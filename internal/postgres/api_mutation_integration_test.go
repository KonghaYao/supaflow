package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/apimutation"
	"github.com/pazhou/supaflow/internal/workflow"
)

// 验证 audit fact 冲突会回滚同事务内的发布，且成功时 operation completion 与审计同时可见。
func TestAPIMutationAuditAndDomainCommitAreAtomic(t *testing.T) {
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
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	tenantID, workflowID := "atomic_"+suffix, "workflow_"+suffix
	if _, err = pool.Exec(ctx, `INSERT INTO tenants(id,name) VALUES($1,$1)`, tenantID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM audit_records WHERE tenant_id=$1`, tenantID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM tenants WHERE id=$1`, tenantID)
	})
	definition := workflow.WorkflowDefinition{SchemaVersion: workflow.DefinitionSchemaV1, ID: workflowID,
		Nodes: []workflow.NodeDefinition{
			{ID: "start", Type: workflow.NodeTypeStart, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
			{ID: "end", Type: workflow.NodeTypeEnd, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
		}, Edges: []workflow.EdgeDefinition{{ID: "edge", FromNode: "start", FromPort: "input", ToNode: "end", ToPort: "result"}}}
	catalog := NewPlanCatalog(pool, workflow.BuiltinRegistry(), workflow.CompilePolicy{AllowedCapabilities: workflow.CapabilitySet{Idempotent: true}})
	failed := apimutation.New(tenantID, "publisher", "workflow.publish", "workflow", workflowID, "atomic-failure-key", "POST", "/v1/workflows/x/publish", []byte(`{}`), "")
	if _, err = pool.Exec(ctx, `INSERT INTO audit_records(id,tenant_id,actor_id,action,resource_type,resource_id,outcome,operation_id) VALUES($1,$2,'other','other','workflow',$3,'allowed',$4)`, "audit_conflict_"+suffix, tenantID, workflowID, failed.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err = catalog.Publish(apimutation.WithContext(ctx, failed), tenantID, workflowID, definition, "atomic-failure-key"); err == nil {
		t.Fatal("publication unexpectedly committed despite audit conflict")
	}
	var count int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM workflow_versions WHERE tenant_id=$1 AND publication_key='atomic-failure-key'`, tenantID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("publication survived failed audit count=%d err=%v", count, err)
	}
	succeeded := apimutation.New(tenantID, "publisher", "workflow.publish", "workflow", workflowID, "atomic-success-key", "POST", "/v1/workflows/x/publish", []byte(`{}`), "")
	if _, _, err = catalog.Publish(apimutation.WithContext(ctx, succeeded), tenantID, workflowID, definition, "atomic-success-key"); err != nil {
		t.Fatal(err)
	}
	if !succeeded.Committed() {
		t.Fatal("successful transaction did not expose committed marker")
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM api_mutation_ledger ledger JOIN audit_records audit ON audit.tenant_id=ledger.tenant_id AND audit.operation_id=ledger.operation_id WHERE ledger.tenant_id=$1 AND ledger.operation_id=$2`, tenantID, succeeded.ID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("durable completion/audit count=%d err=%v", count, err)
	}
}
