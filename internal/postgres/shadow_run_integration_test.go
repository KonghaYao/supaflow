package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/executor"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/shadowrun"
	"github.com/pazhou/supaflow/internal/workflow"
)

// 验证 PostgreSQL 持久化 shadow executor，服务重启后领取与后继节点仍只能执行 stub。
func TestPostgresShadowRunPreservesStubModeAcrossClaims(t *testing.T) {
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
	tenantID, workflowID := "shadow_tenant_"+suffix, "shadow_flow_"+suffix
	if _, err = pool.Exec(ctx, `INSERT INTO tenants (id, name) VALUES ($1, $1)`, tenantID); err != nil {
		t.Fatal(err)
	}
	definition := workflow.WorkflowDefinition{
		SchemaVersion: workflow.DefinitionSchemaV1,
		ID:            workflowID,
		Nodes: []workflow.NodeDefinition{
			{ID: "start", Type: workflow.NodeTypeStart, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
			{ID: "request", Type: workflow.NodeTypeHTTP, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{"method":"POST","url":"https://must-not-run.invalid/orders"}`)},
			{ID: "end", Type: workflow.NodeTypeEnd, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
		},
		Edges: []workflow.EdgeDefinition{
			{ID: "to_request", FromNode: "start", FromPort: "input", ToNode: "request", ToPort: "body"},
			{ID: "to_end", FromNode: "request", FromPort: "response", ToNode: "end", ToPort: "result"},
		},
	}
	catalog := NewPlanCatalog(pool, workflow.BuiltinRegistry(), workflow.CompilePolicy{
		AllowedCapabilities: workflow.CapabilitySet{Cancellable: true, Idempotent: true},
		AllowedPermissions:  workflow.PermissionSet{Network: true, Credentials: true},
	})
	published, _, err := catalog.Publish(ctx, tenantID, workflowID, definition, "publish_shadow_"+suffix)
	if err != nil {
		t.Fatal(err)
	}
	store := NewRuntimeStore(pool)
	service, err := shadowrun.NewService(catalog, workflowruntime.NewService(store, nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	started, err := service.Start(ctx, tenantID, shadowrun.StartRequest{
		PlanID: published.ID, PlanHash: published.Hash, Input: json.RawMessage(`{"order":42}`),
		StubBindings:   map[string]string{"request": workflow.ShadowStubExecutorKind},
		IdempotencyKey: "shadow_run_" + suffix,
	})
	if err != nil || started.Run.Plan.Executor != "shadow-v1:builtin.start" {
		t.Fatalf("start=%+v err=%v", started, err)
	}
	planExecutor, err := executor.NewPlanExecutor(catalog, executor.Config{})
	if err != nil {
		t.Fatal(err)
	}
	first, err := NewRuntimeStore(pool).ClaimAttempt(ctx, workflowruntime.ClaimAttemptRequest{
		TenantID: workflowruntime.TenantID(tenantID), WorkerID: "shadow_worker", LeaseDuration: time.Minute,
	})
	if err != nil || first == nil || first.Attempt.Executor != "shadow-v1:builtin.start" {
		t.Fatalf("first claim=%+v err=%v", first, err)
	}
	firstResult, err := planExecutor.Execute(ctx, executionRequestFromClaim(first))
	if err != nil || len(firstResult.Next) != 1 || firstResult.Next[0].Executor != "shadow-v1:builtin.shadow-noop.v1" {
		t.Fatalf("first execute=%+v err=%v", firstResult, err)
	}
	if _, err = store.CompleteAttempt(ctx, workflowruntime.CompleteAttemptRequest{
		TenantID: workflowruntime.TenantID(tenantID), RunID: first.Run.ID, AttemptID: first.Attempt.ID,
		WorkerID: "shadow_worker", FencingToken: first.Lease.FencingToken, Now: first.Lease.UpdatedAt,
		RunStatus: workflowruntime.RunRunning, Output: firstResult.Output, Next: firstResult.Next,
	}); err != nil {
		t.Fatal(err)
	}
	second, err := NewRuntimeStore(pool).ClaimAttempt(ctx, workflowruntime.ClaimAttemptRequest{
		TenantID: workflowruntime.TenantID(tenantID), WorkerID: "shadow_worker_after_restart",
		LeaseDuration: time.Minute,
	})
	if err != nil || second == nil || second.Attempt.NodeID != "request" || second.Attempt.Executor != "shadow-v1:builtin.shadow-noop.v1" {
		t.Fatalf("second claim=%+v err=%v", second, err)
	}
	stubbed, err := planExecutor.Execute(ctx, executionRequestFromClaim(second))
	if err != nil || len(stubbed.Next) != 1 || stubbed.Next[0].Executor != "shadow-v1:builtin.end" {
		t.Fatalf("stub execute=%+v err=%v", stubbed, err)
	}
}

func executionRequestFromClaim(work *workflowruntime.WorkItem) workflowruntime.ExecutionRequest {
	return workflowruntime.ExecutionRequest{
		TenantID: work.Run.TenantID, RunID: work.Run.ID, Plan: work.Run.Plan,
		NodeID: work.Attempt.NodeID, AttemptID: work.Attempt.ID, ExecutionID: work.Attempt.ExecutionID,
		Attempt: work.Attempt.Number, Executor: work.Attempt.Executor, Input: work.Attempt.Input,
		FencingToken: work.Lease.FencingToken, Takeover: work.Takeover,
	}
}
