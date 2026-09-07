package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/workflow"
)

// 验证真实 PostgreSQL 在 Wait deadline 到期时原子恢复原 attempt，并使新 lease 的 fencing token 单调递增。
func TestPostgresAutoResumesDueWait(t *testing.T) {
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

	suffix := fmt.Sprintf("%d", time.Now().UTC().UnixNano())
	organizationID, workflowID := "organization_wait_"+suffix, "wait_"+suffix
	definition := workflow.WorkflowDefinition{
		SchemaVersion: workflow.DefinitionSchemaV1, ID: workflowID,
		Nodes: []workflow.NodeDefinition{
			{ID: "start", Type: workflow.NodeTypeStart, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
			{ID: "wait", Type: workflow.NodeTypeWait, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{"timeoutMs":1000}`)},
			{ID: "end", Type: workflow.NodeTypeEnd, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
		},
		Edges: []workflow.EdgeDefinition{
			{ID: "start-wait", FromNode: "start", FromPort: "input", ToNode: "wait", ToPort: "input"},
			{ID: "wait-end", FromNode: "wait", FromPort: "output", ToNode: "end", ToPort: "result"},
		},
	}
	catalog := NewPlanCatalog(pool, workflow.BuiltinRegistry(), workflow.CompilePolicy{
		AllowedCapabilities: workflow.CapabilitySet{Cancellable: true, Interruptible: true, Idempotent: true},
	})
	published, _, err := catalog.Publish(ctx, organizationID, workflowID, definition, "publish-wait")
	if err != nil {
		t.Fatal(err)
	}
	store := NewRuntimeStore(pool)
	service := workflowruntime.NewService(store, nil, nil)
	created, err := service.StartRun(ctx, workflowruntime.OrganizationID(organizationID), workflowruntime.StartRunRequest{
		Plan:  workflowruntime.PlanRef{ID: published.ID, Hash: published.Hash, EntryNodeID: "start", Executor: "builtin.start"},
		Input: json.RawMessage(`{"value":1}`), IdempotencyKey: "run-wait",
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	start, err := store.ClaimAttempt(ctx, workflowruntime.ClaimAttemptRequest{
		OrganizationID: workflowruntime.OrganizationID(organizationID), WorkerID: "worker", Now: now, LeaseDuration: time.Minute,
	})
	if err != nil || start == nil {
		t.Fatalf("claim start=%+v err=%v", start, err)
	}
	_, err = store.CompleteAttempt(ctx, workflowruntime.CompleteAttemptRequest{
		OrganizationID: workflowruntime.OrganizationID(organizationID), RunID: created.Run.ID, AttemptID: start.Attempt.ID,
		WorkerID: "worker", FencingToken: start.Lease.FencingToken, Now: now,
		RunStatus: workflowruntime.RunRunning, Next: []workflowruntime.NewAttempt{{
			NodeID: "wait", Executor: "builtin.wait", Input: json.RawMessage(`{"value":1}`),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	wait, err := store.ClaimAttempt(ctx, workflowruntime.ClaimAttemptRequest{
		OrganizationID: workflowruntime.OrganizationID(organizationID), WorkerID: "worker", Now: now, LeaseDuration: time.Minute,
	})
	if err != nil || wait == nil || wait.Attempt.NodeID != "wait" {
		t.Fatalf("claim wait=%+v err=%v", wait, err)
	}
	token := sha256.Sum256([]byte("wait-resume-" + suffix))
	_, err = store.SuspendAttempt(ctx, workflowruntime.SuspendAttemptRequest{
		OrganizationID: workflowruntime.OrganizationID(organizationID), RunID: created.Run.ID, AttemptID: wait.Attempt.ID,
		WorkerID: "worker", FencingToken: wait.Lease.FencingToken, Now: now,
		Interrupt: workflowruntime.Interrupt{ID: "wait_" + suffix, Kind: "wait", TokenDigest: token, ExpiresAt: now.Add(time.Second), AutoResume: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE interrupts SET expires_at=clock_timestamp()-interval '1 second'
		WHERE organization_id=$1 AND id=$2`, organizationID, "wait_"+suffix); err != nil {
		t.Fatal(err)
	}
	resumed, err := store.ClaimAttempt(ctx, workflowruntime.ClaimAttemptRequest{
		OrganizationID: workflowruntime.OrganizationID(organizationID), WorkerID: "worker-2", Now: now.Add(time.Second), LeaseDuration: time.Minute,
	})
	if err != nil || resumed == nil || resumed.Attempt.ID != wait.Attempt.ID || !jsonSemanticallyEqual(resumed.Attempt.ResumeData, []byte(`{"expired":true}`)) || resumed.Lease.FencingToken <= wait.Lease.FencingToken {
		t.Fatalf("resumed wait=%+v err=%v", resumed, err)
	}
}
