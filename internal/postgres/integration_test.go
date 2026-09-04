package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/platform"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/workflow"
)

// 验证真实 PostgreSQL 上发布、幂等启动、领取、fencing 完成与游标续传的垂直链路。
func TestPostgresPublicationAndRuntimeLifecycle(t *testing.T) {
	databaseURL := os.Getenv("SUPAFLOW_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("SUPAFLOW_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	migrations, err := LoadMigrations(os.DirFS("../.."), "migrations")
	if err != nil {
		t.Fatal(err)
	}
	if err := NewMigrator(pool).Up(ctx, migrations); err != nil {
		t.Fatal(err)
	}
	suffix := fmt.Sprintf("%d", time.Now().UTC().UnixNano())
	tenantID := "tenant_" + suffix
	workflowID := "workflow_" + suffix
	if _, err := pool.Exec(ctx, `INSERT INTO tenants (id, name) VALUES ($1, $1)`, tenantID); err != nil {
		t.Fatal(err)
	}
	definition := workflow.WorkflowDefinition{
		SchemaVersion: workflow.DefinitionSchemaV1,
		ID:            workflowID,
		Nodes: []workflow.NodeDefinition{
			{ID: "start", Type: workflow.NodeTypeStart, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
			{ID: "end", Type: workflow.NodeTypeEnd, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
		},
		Edges: []workflow.EdgeDefinition{{
			ID: "edge", FromNode: "start", FromPort: "input", ToNode: "end", ToPort: "result",
		}},
	}
	catalog := NewPlanCatalog(pool, workflow.BuiltinRegistry(), workflow.CompilePolicy{
		AllowedCapabilities: workflow.CapabilitySet{Idempotent: true},
	})
	published, created, err := catalog.Publish(ctx, tenantID, workflowID, definition, "publish_1")
	if err != nil || !created {
		t.Fatalf("Publish() created=%v error=%v", created, err)
	}
	replayed, created, err := catalog.Publish(ctx, tenantID, workflowID, definition, "publish_1")
	if err != nil || created || replayed.ID != published.ID {
		t.Fatalf("Publish() replay=%+v created=%v error=%v", replayed, created, err)
	}
	republished, created, err := catalog.Publish(ctx, tenantID, workflowID, definition, "publish_2")
	if err != nil || !created || republished.Version != published.Version+1 {
		t.Fatalf("Publish() republish=%+v created=%v error=%v", republished, created, err)
	}
	emptyRegistry, err := workflow.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	resolver := NewPlanCatalog(pool, emptyRegistry, workflow.CompilePolicy{})
	resolvedPlan, err := resolver.Resolve(ctx, tenantID, published.ID, published.Hash)
	if err != nil || resolvedPlan.Plan.Hash() != published.Hash {
		t.Fatalf("Resolve() with changed registry plan=%+v error=%v", resolvedPlan, err)
	}
	otherTenantID := "other_" + suffix
	if _, err := pool.Exec(ctx, `INSERT INTO tenants (id, name) VALUES ($1, $1)`, otherTenantID); err != nil {
		t.Fatal(err)
	}
	otherPublished, otherCreated, err := catalog.Publish(ctx, otherTenantID, workflowID, definition, "publish_1")
	if err != nil || !otherCreated || otherPublished.ID == published.ID {
		t.Fatalf("second tenant Publish() plan=%+v created=%v error=%v", otherPublished, otherCreated, err)
	}
	if _, err := catalog.Resolve(ctx, otherTenantID, published.ID, published.Hash); !errors.Is(err, platform.ErrPlanNotFound) {
		t.Fatalf("cross-tenant Resolve() error=%v", err)
	}
	entry, ok := published.Plan.Node("start")
	if !ok {
		t.Fatal("published plan lacks entry")
	}
	store := NewRuntimeStore(pool)
	service := workflowruntime.NewService(store, nil, nil)
	started, err := service.StartRun(ctx, workflowruntime.TenantID(tenantID), workflowruntime.StartRunRequest{
		Plan: workflowruntime.PlanRef{
			ID: published.ID, Hash: published.Hash, EntryNodeID: entry.ID, Executor: entry.Executor.Kind,
		},
		Input: json.RawMessage(`{"value":1}`), IdempotencyKey: "run_1",
	})
	if err != nil || !started.Created {
		t.Fatalf("StartRun() created=%v error=%v", started.Created, err)
	}
	claimAt := time.Now().UTC()
	firstWork, err := store.ClaimAttempt(ctx, workflowruntime.ClaimAttemptRequest{
		TenantID: workflowruntime.TenantID(tenantID), WorkerID: "worker_1",
		Now: claimAt, LeaseDuration: time.Minute,
	})
	if err != nil || firstWork == nil || firstWork.Lease.FencingToken != 1 || firstWork.Takeover {
		t.Fatalf("ClaimAttempt() work=%+v error=%v", firstWork, err)
	}
	if _, err = pool.Exec(ctx, `UPDATE worker_leases SET
		acquired_at=clock_timestamp()-interval '2 seconds',
		expires_at=clock_timestamp()-interval '1 second'
		WHERE tenant_id=$1 AND attempt_id=$2`, tenantID, firstWork.Attempt.ID); err != nil {
		t.Fatal(err)
	}
	takeoverAt := claimAt.Add(time.Minute)
	work, err := store.ClaimAttempt(ctx, workflowruntime.ClaimAttemptRequest{
		TenantID: workflowruntime.TenantID(tenantID), WorkerID: "worker_2",
		Now: takeoverAt, LeaseDuration: time.Minute,
	})
	if err != nil || work == nil || work.Lease.FencingToken <= firstWork.Lease.FencingToken || !work.Takeover {
		t.Fatalf("takeover ClaimAttempt() work=%+v error=%v", work, err)
	}
	suspendAt := takeoverAt.Add(time.Second)
	approvalToken := sha256.Sum256([]byte("integration-approval-token-" + suffix))
	interrupt, err := store.SuspendAttempt(ctx, workflowruntime.SuspendAttemptRequest{
		TenantID: workflowruntime.TenantID(tenantID), RunID: started.Run.ID,
		AttemptID: work.Attempt.ID, WorkerID: "worker_2", FencingToken: work.Lease.FencingToken,
		Now: suspendAt,
		Interrupt: workflowruntime.Interrupt{
			ID: "interrupt_" + suffix, Kind: "approval", TokenDigest: approvalToken,
			ExpiresAt: suspendAt.Add(time.Minute),
		},
		Snapshot: &workflowruntime.Snapshot{ID: "suspended_snapshot_" + suffix, Data: json.RawMessage(`{"waiting":true}`)},
	})
	if err != nil || interrupt.Status != workflowruntime.InterruptPending {
		t.Fatalf("SuspendAttempt() interrupt=%+v error=%v", interrupt, err)
	}
	resolved, err := store.ResolveInterrupt(ctx, workflowruntime.ResolveInterruptRequest{
		TenantID: workflowruntime.TenantID(tenantID), InterruptID: interrupt.ID,
		TokenDigest: approvalToken, DecidedBy: "approver_1", Now: suspendAt.Add(time.Second),
		IdempotencyKey: "approval-integration-1", RequestHash: sha256.Sum256([]byte("approval-request-1")),
		Decision: workflowruntime.ApprovalDecision{Approved: true, Comment: "approved"},
	})
	if err != nil || resolved.Interrupt.Status != workflowruntime.InterruptResolved {
		t.Fatalf("ResolveInterrupt() result=%+v error=%v", resolved, err)
	}
	conflictingHash := sha256.Sum256([]byte("different-approval-request"))
	_, err = store.ResolveInterrupt(ctx, workflowruntime.ResolveInterruptRequest{
		TenantID: workflowruntime.TenantID(tenantID), InterruptID: interrupt.ID,
		TokenDigest: approvalToken, DecidedBy: "approver_1", Now: suspendAt.Add(time.Second),
		IdempotencyKey: "approval-integration-1", RequestHash: conflictingHash,
		Decision: workflowruntime.ApprovalDecision{Approved: false},
	})
	if !workflowruntime.IsCode(err, workflowruntime.CodeIdempotencyConflict) {
		t.Fatalf("ResolveInterrupt() conflicting replay error=%v", err)
	}
	resumed, err := store.ClaimAttempt(ctx, workflowruntime.ClaimAttemptRequest{
		TenantID: workflowruntime.TenantID(tenantID), WorkerID: "worker_3",
		Now: suspendAt.Add(2 * time.Second), LeaseDuration: time.Minute,
	})
	if err != nil || resumed == nil || resumed.Lease.FencingToken <= work.Lease.FencingToken ||
		len(resumed.Attempt.ResumeData) == 0 || resumed.Snapshot == nil {
		t.Fatalf("resumed ClaimAttempt() work=%+v error=%v", resumed, err)
	}
	outputValue := json.RawMessage(`{"ok":true}`)
	digest := sha256.Sum256(outputValue)
	_, err = store.CompleteAttempt(ctx, workflowruntime.CompleteAttemptRequest{
		TenantID: workflowruntime.TenantID(tenantID), RunID: started.Run.ID,
		AttemptID: firstWork.Attempt.ID, WorkerID: "worker_1", FencingToken: firstWork.Lease.FencingToken,
		Now: suspendAt.Add(3 * time.Second), RunStatus: workflowruntime.RunSuccess,
	})
	if !workflowruntime.IsCode(err, workflowruntime.CodeStaleFencingToken) {
		t.Fatalf("stale CompleteAttempt() error=%v", err)
	}
	completed, err := store.CompleteAttempt(ctx, workflowruntime.CompleteAttemptRequest{
		TenantID: workflowruntime.TenantID(tenantID), RunID: started.Run.ID,
		AttemptID: resumed.Attempt.ID, WorkerID: "worker_3", FencingToken: resumed.Lease.FencingToken,
		Now: suspendAt.Add(3 * time.Second), RunStatus: workflowruntime.RunSuccess,
		Output: &workflowruntime.Output{
			ID: "output_" + suffix, ContentType: "application/json", Size: int64(len(outputValue)),
			SHA256: hex.EncodeToString(digest[:]), Inline: outputValue,
		},
		Snapshot: &workflowruntime.Snapshot{ID: "snapshot_" + suffix, Data: json.RawMessage(`{"done":true}`)},
	})
	if err != nil || completed.Status != workflowruntime.RunSuccess {
		t.Fatalf("CompleteAttempt() run=%+v error=%v cause=%v", completed, err, errors.Unwrap(err))
	}
	if completed.Output == nil || !jsonSemanticallyEqual(completed.Output.Inline, outputValue) || completed.Output.External {
		t.Fatalf("CompleteAttempt() output=%+v", completed.Output)
	}
	projected, err := store.GetRun(ctx, workflowruntime.TenantID(tenantID), started.Run.ID)
	if err != nil || projected.Output == nil || !jsonSemanticallyEqual(projected.Output.Inline, outputValue) {
		t.Fatalf("GetRun() run=%+v error=%v", projected, err)
	}
	rerunRequest := workflowruntime.RerunRequest{
		SourceRunID: started.Run.ID, FromNodeID: entry.ID, EntryNodeID: entry.ID,
		EntryExecutor: entry.Executor.Kind, IdempotencyKey: "rerun-integration-1",
	}
	rerun, err := service.Rerun(ctx, workflowruntime.TenantID(tenantID), rerunRequest)
	if err != nil || !rerun.Created || rerun.Run.ID == started.Run.ID ||
		rerun.Run.Rerun == nil || rerun.Run.Rerun.SourceRunID != started.Run.ID {
		t.Fatalf("Rerun() result=%+v error=%v", rerun, err)
	}
	rerunReplay, err := service.Rerun(ctx, workflowruntime.TenantID(tenantID), rerunRequest)
	if err != nil || rerunReplay.Created || rerunReplay.Run.ID != rerun.Run.ID {
		t.Fatalf("Rerun() replay=%+v error=%v", rerunReplay, err)
	}
	conflictingRerun := rerunRequest
	conflictingRerun.FromNodeID = ""
	if _, err = service.Rerun(ctx, workflowruntime.TenantID(tenantID), conflictingRerun); !workflowruntime.IsCode(err, workflowruntime.CodeIdempotencyConflict) {
		t.Fatalf("Rerun() conflict error=%v", err)
	}
	var lineageCount, attemptCount, eventCount, outboxCount int
	err = pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM run_rerun_lineage WHERE tenant_id=$1 AND run_id=$2 AND source_run_id=$3 AND from_node_id=$4),
		(SELECT count(*) FROM node_attempts WHERE tenant_id=$1 AND run_id=$2 AND node_id=$4 AND input='{"value":1}'::jsonb),
		(SELECT count(*) FROM run_events WHERE tenant_id=$1 AND run_id=$2 AND event_type='run.created'),
		(SELECT count(*) FROM outbox_messages WHERE tenant_id=$1 AND aggregate_id=$2 AND topic='run.created')`,
		tenantID, rerun.Run.ID, started.Run.ID, entry.ID).Scan(&lineageCount, &attemptCount, &eventCount, &outboxCount)
	if err != nil || lineageCount != 1 || attemptCount != 1 || eventCount != 1 || outboxCount != 1 {
		t.Fatalf("rerun atomic artifacts lineage=%d attempt=%d event=%d outbox=%d error=%v", lineageCount, attemptCount, eventCount, outboxCount, err)
	}
	if _, err = service.Rerun(ctx, workflowruntime.TenantID(otherTenantID), workflowruntime.RerunRequest{
		SourceRunID: started.Run.ID, EntryNodeID: entry.ID, EntryExecutor: entry.Executor.Kind,
		IdempotencyKey: "rerun-cross-tenant",
	}); !workflowruntime.IsCode(err, workflowruntime.CodeNotFound) {
		t.Fatalf("cross-tenant Rerun() error=%v", err)
	}
	var runOutputID *string
	if err = pool.QueryRow(ctx, `SELECT output_id FROM runs WHERE tenant_id = $1 AND id = $2`, tenantID, started.Run.ID).Scan(&runOutputID); err != nil {
		t.Fatal(err)
	}
	if runOutputID == nil || *runOutputID != "output_"+suffix {
		t.Fatalf("runs.output_id=%v", runOutputID)
	}
	approvalReplay, err := store.ResolveInterrupt(ctx, workflowruntime.ResolveInterruptRequest{
		TenantID: workflowruntime.TenantID(tenantID), InterruptID: interrupt.ID,
		TokenDigest: approvalToken, DecidedBy: "approver_1", Now: suspendAt.Add(4 * time.Second),
		IdempotencyKey: "approval-integration-1", RequestHash: sha256.Sum256([]byte("approval-request-1")),
		Decision: workflowruntime.ApprovalDecision{Approved: true, Comment: "approved"},
	})
	if err != nil || !reflect.DeepEqual(approvalReplay, resolved) {
		t.Fatalf("ResolveInterrupt() replay after run advance=%+v first=%+v error=%v", approvalReplay, resolved, err)
	}
	events, err := store.ListEvents(ctx, workflowruntime.TenantID(tenantID), started.Run.ID, 0, 100)
	if err != nil || len(events) < 3 {
		t.Fatalf("ListEvents() len=%d error=%v", len(events), err)
	}
	for index := 1; index < len(events); index++ {
		if events[index].Cursor != events[index-1].Cursor+1 {
			t.Fatalf("event cursor gap: %+v", events)
		}
	}
	foundSucceeded := false
	eventCounts := map[string]int{}
	for _, event := range events {
		eventCounts[event.Type]++
		if event.Type == "run.success" {
			t.Fatal("obsolete run.success event was persisted")
		}
		foundSucceeded = foundSucceeded || event.Type == "run.succeeded"
	}
	if !foundSucceeded {
		t.Fatalf("run.succeeded event missing: %+v", events)
	}
	if eventCounts["run.started"] != 1 || eventCounts["attempt.claimed"] != 3 || eventCounts["attempt.lease_taken_over"] != 1 {
		t.Fatalf("unexpected claim events: counts=%v events=%+v", eventCounts, events)
	}
	for eventType, want := range map[string]int{
		"run.started": 1, "attempt.claimed": 3, "attempt.lease_taken_over": 1,
	} {
		var eventCount, outboxCount int
		if err = pool.QueryRow(ctx, `
			SELECT
			    (SELECT count(*) FROM run_events
			     WHERE tenant_id = $1 AND run_id = $2 AND event_type = $3),
			    (SELECT count(*) FROM outbox_messages
			     WHERE tenant_id = $1 AND aggregate_id = $2 AND topic = $3)`,
			tenantID, started.Run.ID, eventType).Scan(&eventCount, &outboxCount); err != nil {
			t.Fatal(err)
		}
		if eventCount != want || outboxCount != want {
			t.Fatalf("%s persistence counts: events=%d outbox=%d want=%d", eventType, eventCount, outboxCount, want)
		}
	}
}

func jsonSemanticallyEqual(left, right []byte) bool {
	var leftValue, rightValue any
	if json.Unmarshal(left, &leftValue) != nil || json.Unmarshal(right, &rightValue) != nil {
		return false
	}
	return reflect.DeepEqual(leftValue, rightValue)
}

// 验证真实 PostgreSQL 中取消 intent 赢得并行完成竞态，并清理所有分支的 live work 与 lease。
func TestPostgresCancellationWinsSuccessfulCompletion(t *testing.T) {
	databaseURL := os.Getenv("SUPAFLOW_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("SUPAFLOW_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	migrations, err := LoadMigrations(os.DirFS("../.."), "migrations")
	if err != nil {
		t.Fatal(err)
	}
	if err = NewMigrator(pool).Up(ctx, migrations); err != nil {
		t.Fatal(err)
	}
	suffix := fmt.Sprintf("cancel_%d", time.Now().UTC().UnixNano())
	tenantID := "tenant_" + suffix
	workflowID := "workflow_" + suffix
	if _, err = pool.Exec(ctx, `INSERT INTO tenants (id, name) VALUES ($1, $1)`, tenantID); err != nil {
		t.Fatal(err)
	}
	definition := workflow.WorkflowDefinition{
		SchemaVersion: workflow.DefinitionSchemaV1, ID: workflowID,
		Nodes: []workflow.NodeDefinition{
			{ID: "start", Type: workflow.NodeTypeStart, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
			{ID: "end", Type: workflow.NodeTypeEnd, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
		},
		Edges: []workflow.EdgeDefinition{{
			ID: "edge", FromNode: "start", FromPort: "input", ToNode: "end", ToPort: "result",
		}},
	}
	catalog := NewPlanCatalog(pool, workflow.BuiltinRegistry(), workflow.CompilePolicy{
		AllowedCapabilities: workflow.CapabilitySet{Idempotent: true},
	})
	published, _, err := catalog.Publish(ctx, tenantID, workflowID, definition, "publish_cancel")
	if err != nil {
		t.Fatal(err)
	}
	entry, ok := published.Plan.Node("start")
	if !ok {
		t.Fatal("published plan lacks start")
	}
	store := NewRuntimeStore(pool)
	service := workflowruntime.NewService(store, nil, nil)
	started, err := service.StartRun(ctx, workflowruntime.TenantID(tenantID), workflowruntime.StartRunRequest{
		Plan: workflowruntime.PlanRef{
			ID: published.ID, Hash: published.Hash, EntryNodeID: entry.ID, Executor: entry.Executor.Kind,
		},
		Input: json.RawMessage(`{"value":1}`), IdempotencyKey: "run_cancel",
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	work, err := store.ClaimAttempt(ctx, workflowruntime.ClaimAttemptRequest{
		TenantID: workflowruntime.TenantID(tenantID), WorkerID: "worker_cancel",
		Now: now, LeaseDuration: time.Minute,
	})
	if err != nil || work == nil {
		t.Fatalf("claim: work=%+v err=%v", work, err)
	}
	secondNodeStateID := "parallel_node_" + suffix
	secondAttemptID := "parallel_attempt_" + suffix
	if _, err = pool.Exec(ctx, `INSERT INTO node_states(id,tenant_id,run_id,node_id,status,attempt_count,latest_attempt_id)
		VALUES($1,$2,$3,'parallel','running',1,$4)`,
		secondNodeStateID, tenantID, started.Run.ID, secondAttemptID); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO node_attempts(id,tenant_id,run_id,node_state_id,node_id,executor,input,attempt_number,status,worker_id,fencing_token)
		VALUES($4,$2,$3,$1,'parallel','builtin.transform','{}',1,'running','worker_parallel',1)`,
		secondNodeStateID, tenantID, started.Run.ID, secondAttemptID); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO worker_leases(attempt_id,tenant_id,run_id,worker_id,fencing_token,acquired_at,expires_at)
		VALUES($1,$2,$3,'worker_parallel',1,$4::timestamptz,$4::timestamptz+interval '1 minute')`,
		secondAttemptID, tenantID, started.Run.ID, now); err != nil {
		t.Fatal(err)
	}
	cancelled, err := store.RequestCancel(ctx, workflowruntime.TenantID(tenantID), started.Run.ID, now.Add(time.Millisecond))
	if err != nil || cancelled.Run.Status != workflowruntime.RunCancelling {
		t.Fatalf("cancel intent: result=%+v err=%v", cancelled, err)
	}
	outputValue := json.RawMessage(`{"must":"discard"}`)
	outputHash := sha256.Sum256(outputValue)
	completed, err := store.CompleteAttempt(ctx, workflowruntime.CompleteAttemptRequest{
		TenantID: workflowruntime.TenantID(tenantID), RunID: started.Run.ID,
		AttemptID: work.Attempt.ID, WorkerID: "worker_cancel", FencingToken: work.Lease.FencingToken,
		Now: now.Add(2 * time.Millisecond), RunStatus: workflowruntime.RunSuccess,
		Output: &workflowruntime.Output{
			ID: "discarded_output_" + suffix, ContentType: "application/json",
			Size: int64(len(outputValue)), SHA256: hex.EncodeToString(outputHash[:]), Inline: outputValue,
		},
		Snapshot: &workflowruntime.Snapshot{ID: "discarded_snapshot_" + suffix, Data: json.RawMessage(`{"must":"discard"}`)},
		Next: []workflowruntime.NewAttempt{{
			NodeID: "end", Executor: "builtin.end", Input: json.RawMessage(`null`),
		}},
		Events: []workflowruntime.Event{{Type: "executor.succeeded"}},
	})
	if err != nil || completed.Status != workflowruntime.RunCancelled {
		t.Fatalf("completion: run=%+v err=%v", completed, err)
	}
	var outputCount, snapshotCount, attemptCount, liveAttemptCount, liveNodeCount, leaseCount int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM node_outputs WHERE tenant_id = $1 AND run_id = $2`, tenantID, started.Run.ID).Scan(&outputCount); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM run_snapshots WHERE tenant_id = $1 AND run_id = $2`, tenantID, started.Run.ID).Scan(&snapshotCount); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM node_attempts WHERE tenant_id = $1 AND run_id = $2`, tenantID, started.Run.ID).Scan(&attemptCount); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM node_attempts WHERE tenant_id=$1 AND run_id=$2
		AND status IN ('pending','retry_ready','running','suspended')`, tenantID, started.Run.ID).Scan(&liveAttemptCount); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM node_states WHERE tenant_id=$1 AND run_id=$2
		AND status IN ('pending','ready','running','suspended')`, tenantID, started.Run.ID).Scan(&liveNodeCount); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM worker_leases WHERE tenant_id=$1 AND run_id=$2`, tenantID, started.Run.ID).Scan(&leaseCount); err != nil {
		t.Fatal(err)
	}
	if outputCount != 0 || snapshotCount != 0 || attemptCount != 2 || liveAttemptCount != 0 || liveNodeCount != 0 || leaseCount != 0 {
		t.Fatalf("late completion artifacts: outputs=%d snapshots=%d attempts=%d liveAttempts=%d liveNodes=%d leases=%d",
			outputCount, snapshotCount, attemptCount, liveAttemptCount, liveNodeCount, leaseCount)
	}
	var attemptStatus, nodeStatus string
	var outputID *string
	if err = pool.QueryRow(ctx, `SELECT status FROM node_attempts WHERE tenant_id = $1 AND id = $2`, tenantID, work.Attempt.ID).Scan(&attemptStatus); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT status, output_id FROM node_states WHERE tenant_id = $1 AND run_id = $2 AND node_id = 'start'`, tenantID, started.Run.ID).Scan(&nodeStatus, &outputID); err != nil {
		t.Fatal(err)
	}
	if attemptStatus != "cancelled" || nodeStatus != "cancelled" || outputID != nil {
		t.Fatalf("cancelled state: attempt=%s node=%s outputID=%v", attemptStatus, nodeStatus, outputID)
	}
	var parallelAttemptStatus, parallelNodeStatus string
	if err = pool.QueryRow(ctx, `SELECT status FROM node_attempts WHERE tenant_id=$1 AND id=$2`, tenantID, secondAttemptID).Scan(&parallelAttemptStatus); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT status FROM node_states WHERE tenant_id=$1 AND id=$2`, tenantID, secondNodeStateID).Scan(&parallelNodeStatus); err != nil {
		t.Fatal(err)
	}
	if parallelAttemptStatus != "cancelled" || parallelNodeStatus != "cancelled" {
		t.Fatalf("parallel branch was orphaned: attempt=%s node=%s", parallelAttemptStatus, parallelNodeStatus)
	}
	events, err := store.ListEvents(ctx, workflowruntime.TenantID(tenantID), started.Run.ID, 0, 100)
	if err != nil || len(events) == 0 || events[len(events)-1].Type != "run.cancelled" {
		t.Fatalf("events=%+v err=%v", events, err)
	}
	for _, event := range events {
		if event.Type == "run.succeeded" || event.Type == "executor.succeeded" {
			t.Fatalf("cancelled completion leaked success event: %+v", events)
		}
	}
}
