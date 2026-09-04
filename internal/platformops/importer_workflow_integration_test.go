package platformops

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/postgres"
)

// 验证 workflow 导入原子保留名称与描述，同内容可重放而篡改元数据会冲突。
func TestImportWorkflowPreservesMetadataAndRejectsConflictingReplay(t *testing.T) {
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
	migrations, err := postgres.LoadMigrations(os.DirFS("../.."), "migrations")
	if err != nil {
		t.Fatal(err)
	}
	if err = postgres.NewMigrator(pool).Up(ctx, migrations); err != nil {
		t.Fatal(err)
	}
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	tenantID, workflowID := "workflow_import_tenant_"+suffix, "workflow_import_"+suffix
	if _, err = pool.Exec(ctx, `INSERT INTO tenants(id,name) VALUES($1,$1)`, tenantID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), `DELETE FROM tenants WHERE id=$1`, tenantID) })
	importer, err := NewAdminImporter(pool)
	if err != nil {
		t.Fatal(err)
	}
	request := ImportWorkflowRequest{
		TenantID: tenantID, ActorID: "migration-admin", SourceSystem: "fenix",
		SourceWorkflowID: workflowID, WorkflowID: workflowID,
		Name: "Customer onboarding", Description: "Preserved source description",
	}
	if created, importErr := importer.ImportWorkflow(ctx, request); importErr != nil || !created {
		t.Fatalf("first import created=%v err=%v", created, importErr)
	}
	if created, importErr := importer.ImportWorkflow(ctx, request); importErr != nil || created {
		t.Fatalf("replay created=%v err=%v", created, importErr)
	}
	var name, description, sourceSystem string
	if err = pool.QueryRow(ctx, `
		SELECT workflow.name,workflow.description,marker.source_system
		FROM workflows workflow JOIN workflow_imports marker
		ON marker.tenant_id=workflow.tenant_id AND marker.workflow_id=workflow.id
		WHERE workflow.tenant_id=$1 AND workflow.id=$2`, tenantID, workflowID,
	).Scan(&name, &description, &sourceSystem); err != nil {
		t.Fatal(err)
	}
	if name != request.Name || description != request.Description || sourceSystem != "fenix" {
		t.Fatalf("stored metadata = %q %q %q", name, description, sourceSystem)
	}
	request.Description = "tampered"
	if _, importErr := importer.ImportWorkflow(ctx, request); !errors.Is(importErr, ErrConflict) {
		t.Fatalf("conflicting replay error=%v", importErr)
	}
}
