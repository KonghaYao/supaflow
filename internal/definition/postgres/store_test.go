package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/definition"
	basepostgres "github.com/pazhou/supaflow/internal/postgres"
	"github.com/pazhou/supaflow/internal/workflow"
)

// 验证定义迁移具有租户幂等唯一键、请求摘要和响应快照。
func TestDefinitionMigrationContracts(t *testing.T) {
	contents, err := os.ReadFile("../../../migrations/000004_definition_studio.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(contents)
	for _, contract := range []string{"CREATE TABLE definition_mutations", "UNIQUE (tenant_id, idempotency_key)", "request_hash bytea", "response_snapshot jsonb NOT NULL"} {
		if !strings.Contains(sql, contract) {
			t.Errorf("migration lacks %q", contract)
		}
	}
}

// 验证真实 PostgreSQL 上草稿 revision/幂等与发布版本解析形成完整定义链路。
func TestPostgresDefinitionLifecycle(t *testing.T) {
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
	migrations, err := basepostgres.LoadMigrations(os.DirFS("../../.."), "migrations")
	if err != nil {
		t.Fatal(err)
	}
	if err := basepostgres.NewMigrator(pool).Up(ctx, migrations); err != nil {
		t.Fatal(err)
	}
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	tenant, workflowID := "definition_tenant_"+suffix, "flow_"+suffix
	if _, err := pool.Exec(ctx, `INSERT INTO tenants (id, name) VALUES ($1, $1)`, tenant); err != nil {
		t.Fatal(err)
	}
	registry := workflow.BuiltinRegistry()
	policy := workflow.CompilePolicy{AllowedCapabilities: workflow.CapabilitySet{Idempotent: true}}
	catalog := basepostgres.NewPlanCatalog(pool, registry, policy)
	service, err := definition.NewService(NewStore(pool), definition.CanonicalAdapter{}, catalog, registry, policy)
	if err != nil {
		t.Fatal(err)
	}
	canonical := minimalDefinition(workflowID)
	document, _ := json.Marshal(canonical)
	created, err := service.CreateDraft(ctx, tenant, definition.CreateDraftCommand{
		WorkflowID: workflowID, Document: document, ActorID: "editor", IdempotencyKey: "definition-create-0001",
	})
	if err != nil || created.Revision != 1 {
		t.Fatalf("created=%+v err=%v", created, err)
	}
	replayed, err := service.CreateDraft(ctx, tenant, definition.CreateDraftCommand{
		WorkflowID: workflowID, Document: document, ActorID: "editor", IdempotencyKey: "definition-create-0001",
	})
	if err != nil || replayed.ID != created.ID {
		t.Fatalf("replayed=%+v err=%v", replayed, err)
	}
	_, err = service.UpdateDraft(ctx, tenant, definition.UpdateDraftCommand{
		WorkflowID: workflowID, Document: document, ExpectedRevision: 2, ActorID: "editor", IdempotencyKey: "definition-update-0001",
	})
	if !errors.Is(err, definition.ErrRevisionConflict) {
		t.Fatalf("stale update err=%v", err)
	}
	published, _, err := service.PublishDraft(ctx, tenant, workflowID, "definition-publish-0001")
	if err != nil {
		t.Fatal(err)
	}
	latest, err := service.LatestVersion(ctx, tenant, workflowID)
	if err != nil || latest.VersionID == "" || latest.PlanID != published.ID || latest.PlanHash != published.Hash {
		t.Fatalf("latest=%+v published=%+v err=%v", latest, published, err)
	}
}

func minimalDefinition(id string) workflow.WorkflowDefinition {
	return workflow.WorkflowDefinition{SchemaVersion: workflow.DefinitionSchemaV1, ID: id,
		Nodes: []workflow.NodeDefinition{
			{ID: "start", Type: workflow.NodeTypeStart, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
			{ID: "end", Type: workflow.NodeTypeEnd, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
		},
		Edges: []workflow.EdgeDefinition{{ID: "edge", FromNode: "start", FromPort: "input", ToNode: "end", ToPort: "result"}},
	}
}
