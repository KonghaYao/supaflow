package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/platformops"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/workflow"
)

// 验证真实 PostgreSQL 中创建、排队、取消、终态释放、优先级晋升和租户隔离属于一个生命周期。
func TestPostgresRunAdmissionLifecycle(t *testing.T) {
	ctx, pool := admissionTestPool(t)
	tenantA, serviceA, planA := admissionTestTenant(t, ctx, pool, "a")
	tenantB, serviceB, planB := admissionTestTenant(t, ctx, pool, "b")
	controller, err := platformops.NewAdmission(pool, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for _, tenantID := range []string{tenantA, tenantB} {
		if err = controller.ConfigureQuota(ctx, platformops.Quota{
			TenantID: tenantID, MaxActiveRuns: 1, MaxActiveWorkers: 2, MaxQueueDepth: 2,
		}); err != nil {
			t.Fatal(err)
		}
	}

	first := startAdmissionRun(t, ctx, serviceA, tenantA, planA, "admission-first", 0)
	second := startAdmissionRun(t, ctx, serviceA, tenantA, planA, "admission-second", 10)
	third := startAdmissionRun(t, ctx, serviceA, tenantA, planA, "admission-third", 20)
	replay, err := serviceA.StartRun(ctx, workflowruntime.TenantID(tenantA), workflowruntime.StartRunRequest{
		Plan: planA, Input: json.RawMessage(`{}`), IdempotencyKey: "admission-second", Priority: 10,
	})
	if err != nil || replay.Created || replay.Run.ID != second.ID {
		t.Fatalf("idempotent replay=%+v err=%v", replay, err)
	}
	assertAdmissionCounters(t, ctx, pool, tenantA, 1, 2)

	store := NewRuntimeStore(pool)
	now := time.Now().UTC()
	work, err := store.ClaimAttempt(ctx, workflowruntime.ClaimAttemptRequest{
		TenantID: workflowruntime.TenantID(tenantA), WorkerID: "worker-a", Now: now, LeaseDuration: time.Minute,
	})
	if err != nil || work == nil || work.Run.ID != first.ID {
		t.Fatalf("first claim=%+v err=%v", work, err)
	}
	if _, err = serviceA.CancelRun(ctx, workflowruntime.TenantID(tenantA), second.ID); err != nil {
		t.Fatal(err)
	}
	assertAdmissionCounters(t, ctx, pool, tenantA, 1, 1)
	if _, err = store.CompleteAttempt(ctx, workflowruntime.CompleteAttemptRequest{
		TenantID: workflowruntime.TenantID(tenantA), RunID: first.ID, AttemptID: work.Attempt.ID,
		WorkerID: "worker-a", FencingToken: work.Lease.FencingToken, Now: now.Add(time.Second),
		RunStatus: workflowruntime.RunRunning,
	}); err != nil {
		t.Fatal(err)
	}
	assertAdmissionCounters(t, ctx, pool, tenantA, 1, 0)
	promoted, err := store.ClaimAttempt(ctx, workflowruntime.ClaimAttemptRequest{
		TenantID: workflowruntime.TenantID(tenantA), WorkerID: "worker-a", Now: now.Add(2 * time.Second), LeaseDuration: time.Minute,
	})
	if err != nil || promoted == nil || promoted.Run.ID != third.ID {
		t.Fatalf("promoted claim=%+v err=%v", promoted, err)
	}

	other := startAdmissionRun(t, ctx, serviceB, tenantB, planB, "admission-other", 0)
	otherWork, err := store.ClaimAttempt(ctx, workflowruntime.ClaimAttemptRequest{
		TenantID: workflowruntime.TenantID(tenantB), WorkerID: "worker-b", Now: time.Now().UTC(), LeaseDuration: time.Minute,
	})
	if err != nil || otherWork == nil || otherWork.Run.ID != other.ID {
		t.Fatalf("tenant-isolated claim=%+v err=%v", otherWork, err)
	}
}

// 验证并发创建严格受 active+queue 上限约束，重放不会重复消耗计数。
func TestPostgresRunAdmissionConcurrentBoundAndReplay(t *testing.T) {
	ctx, pool := admissionTestPool(t)
	tenantID, service, plan := admissionTestTenant(t, ctx, pool, "concurrent")
	controller, err := platformops.NewAdmission(pool, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err = controller.ConfigureQuota(ctx, platformops.Quota{
		TenantID: tenantID, MaxActiveRuns: 1, MaxActiveWorkers: 1, MaxQueueDepth: 2,
	}); err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		key      string
		priority int
		result   workflowruntime.CreateRunResult
		err      error
	}
	results := make(chan outcome, 16)
	var group sync.WaitGroup
	for index := range 16 {
		group.Add(1)
		go func() {
			defer group.Done()
			key := fmt.Sprintf("concurrent-admission-%02d", index)
			priority := index
			result, startErr := service.StartRun(ctx, workflowruntime.TenantID(tenantID), workflowruntime.StartRunRequest{
				Plan: plan, Input: json.RawMessage(`{}`), IdempotencyKey: key, Priority: priority,
			})
			results <- outcome{key: key, priority: priority, result: result, err: startErr}
		}()
	}
	group.Wait()
	close(results)
	accepted := make([]outcome, 0, 3)
	limited := 0
	for result := range results {
		if result.err == nil {
			accepted = append(accepted, result)
			continue
		}
		if workflowruntime.IsCode(result.err, workflowruntime.CodeQuotaExceeded) {
			limited++
			continue
		}
		t.Fatalf("unexpected start error: %v", result.err)
	}
	if len(accepted) != 3 || limited != 13 {
		t.Fatalf("accepted=%d limited=%d", len(accepted), limited)
	}
	assertAdmissionCounters(t, ctx, pool, tenantID, 1, 2)
	for _, item := range accepted {
		replay, replayErr := service.StartRun(ctx, workflowruntime.TenantID(tenantID), workflowruntime.StartRunRequest{
			Plan: plan, Input: json.RawMessage(`{}`), IdempotencyKey: item.key, Priority: item.priority,
		})
		if replayErr != nil || replay.Created || replay.Run.ID != item.result.Run.ID {
			t.Fatalf("replay=%+v err=%v", replay, replayErr)
		}
	}
	assertAdmissionCounters(t, ctx, pool, tenantID, 1, 2)
}

func admissionTestPool(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	databaseURL := os.Getenv("SUPAFLOW_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("SUPAFLOW_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	migrations, err := LoadMigrations(os.DirFS("../.."), "migrations")
	if err != nil {
		t.Fatal(err)
	}
	if err = NewMigrator(pool).Up(ctx, migrations); err != nil {
		t.Fatal(err)
	}
	return ctx, pool
}

func admissionTestTenant(t *testing.T, ctx context.Context, pool *pgxpool.Pool, label string) (string, *workflowruntime.Service, workflowruntime.PlanRef) {
	t.Helper()
	suffix := fmt.Sprintf("%s_%d", label, time.Now().UTC().UnixNano())
	tenantID, workflowID := "tenant_"+suffix, "workflow_"+suffix
	if _, err := pool.Exec(ctx, `INSERT INTO tenants (id, name) VALUES ($1, $1)`, tenantID); err != nil {
		t.Fatal(err)
	}
	definition := workflow.WorkflowDefinition{
		SchemaVersion: workflow.DefinitionSchemaV1, ID: workflowID,
		Nodes: []workflow.NodeDefinition{
			{ID: "start", Type: workflow.NodeTypeStart, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
			{ID: "end", Type: workflow.NodeTypeEnd, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
		},
		Edges: []workflow.EdgeDefinition{{ID: "edge", FromNode: "start", FromPort: "input", ToNode: "end", ToPort: "result"}},
	}
	catalog := NewPlanCatalog(pool, workflow.BuiltinRegistry(), workflow.CompilePolicy{AllowedCapabilities: workflow.CapabilitySet{Idempotent: true}})
	published, created, err := catalog.Publish(ctx, tenantID, workflowID, definition, "publish_"+suffix)
	if err != nil || !created {
		t.Fatalf("publish created=%v err=%v", created, err)
	}
	entry, ok := published.Plan.Node("start")
	if !ok {
		t.Fatal("published plan has no start node")
	}
	return tenantID, workflowruntime.NewService(NewRuntimeStore(pool), nil, nil), workflowruntime.PlanRef{
		ID: published.ID, Hash: published.Hash, EntryNodeID: entry.ID, Executor: entry.Executor.Kind,
	}
}

func startAdmissionRun(t *testing.T, ctx context.Context, service *workflowruntime.Service, tenantID string, plan workflowruntime.PlanRef, key string, priority int) workflowruntime.Run {
	t.Helper()
	result, err := service.StartRun(ctx, workflowruntime.TenantID(tenantID), workflowruntime.StartRunRequest{
		Plan: plan, Input: json.RawMessage(`{}`), IdempotencyKey: key, Priority: priority,
	})
	if err != nil || !result.Created {
		t.Fatalf("start %s result=%+v err=%v", key, result, err)
	}
	return result.Run
}

func assertAdmissionCounters(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tenantID string, active, queued int) {
	t.Helper()
	var gotActive, gotQueued int
	err := pool.QueryRow(ctx, `SELECT active_runs, queued_runs FROM tenant_quota_counters WHERE tenant_id = $1`, tenantID).Scan(&gotActive, &gotQueued)
	if err != nil || gotActive != active || gotQueued != queued {
		t.Fatalf("counters active=%d queued=%d want=%d/%d err=%v", gotActive, gotQueued, active, queued, err)
	}
}
