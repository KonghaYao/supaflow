package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/apimutation"
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

// 验证 Studio 列表 SQL 始终分页，并且只读取摘要列而不加载大 JSON 定义。
func TestDefinitionListQueriesAreBoundedSummaryProjections(t *testing.T) {
	contents, err := os.ReadFile("store.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(contents)
	start := strings.Index(source, "func (s *Store) ListDrafts")
	end := strings.Index(source[start:], "// UpdateDraft")
	if start < 0 || end < 0 {
		t.Fatal("could not locate ListDrafts source")
	}
	draftList := source[start : start+end]
	if strings.Contains(draftList, "document") || !strings.Contains(draftList, "LIMIT $3") {
		t.Fatalf("draft list must be a bounded summary projection: %s", draftList)
	}
	if strings.Contains(versionSummarySelect, "v.definition") || !strings.Contains(source, "ORDER BY v.version DESC LIMIT $4") {
		t.Fatalf("version list must be a bounded summary projection: %s", versionSummarySelect)
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

// 验证 publish-draft 响应丢失后即使草稿已 autosave 到下一 revision，原 key 仍重放首次不可变发布结果。
func TestPublishDraftReplaysFirstPublicationAfterDraftChanges(t *testing.T) {
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
	if err = basepostgres.NewMigrator(pool).Up(ctx, migrations); err != nil {
		t.Fatal(err)
	}
	suffix := fmt.Sprintf("publish_replay_%d", time.Now().UnixNano())
	tenantID, workflowID := "tenant_"+suffix, "flow_"+suffix
	if _, err = pool.Exec(ctx, `INSERT INTO tenants (id,name) VALUES ($1,$1)`, tenantID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), `DELETE FROM tenants WHERE id=$1`, tenantID) })
	registry := workflow.BuiltinRegistry()
	policy := workflow.CompilePolicy{AllowedCapabilities: workflow.CapabilitySet{Idempotent: true}}
	catalog := basepostgres.NewPlanCatalog(pool, registry, policy)
	service, err := definition.NewService(NewStore(pool), definition.CanonicalAdapter{}, catalog, registry, policy)
	if err != nil {
		t.Fatal(err)
	}
	firstDefinition := minimalDefinition(workflowID)
	firstDocument, _ := json.Marshal(firstDefinition)
	if _, err = service.CreateDraft(ctx, tenantID, definition.CreateDraftCommand{WorkflowID: workflowID,
		Document: firstDocument, ActorID: "editor", IdempotencyKey: "publish-replay-create"}); err != nil {
		t.Fatal(err)
	}
	key := "publish-replay-stable-key"
	firstOperation := apimutation.New(tenantID, "publisher", "workflow.publish", "workflow", workflowID,
		key, "POST", "/v1/workflows/"+workflowID+"/publish-draft", nil, "")
	first, created, err := service.PublishDraft(apimutation.WithContext(ctx, firstOperation), tenantID, workflowID, key)
	if err != nil || !created || !firstOperation.Committed() {
		t.Fatalf("first=%+v created=%t committed=%t err=%v", first, created, firstOperation.Committed(), err)
	}
	secondDefinition := minimalDefinition(workflowID)
	secondDefinition.Metadata = map[string]json.RawMessage{"revision": json.RawMessage(`2`)}
	secondDocument, _ := json.Marshal(secondDefinition)
	if _, err = service.UpdateDraft(ctx, tenantID, definition.UpdateDraftCommand{WorkflowID: workflowID,
		Document: secondDocument, ExpectedRevision: 1, ActorID: "editor", IdempotencyKey: "publish-replay-update"}); err != nil {
		t.Fatal(err)
	}
	retryOperation := apimutation.New(tenantID, "publisher", "workflow.publish", "workflow", workflowID,
		key, "POST", "/v1/workflows/"+workflowID+"/publish-draft", nil, "")
	replay, replayCreated, err := service.PublishDraft(apimutation.WithContext(ctx, retryOperation), tenantID, workflowID, key)
	if err != nil || replayCreated || replay.ID != first.ID || replay.Hash != first.Hash || replay.Version != first.Version || !retryOperation.Committed() {
		t.Fatalf("first=%+v replay=%+v created=%t committed=%t err=%v", first, replay, replayCreated, retryOperation.Committed(), err)
	}
	var snapshot []byte
	if err = pool.QueryRow(ctx, `SELECT response_snapshot FROM api_mutation_ledger
		WHERE tenant_id=$1 AND action='workflow.publish' AND idempotency_key=$2`, tenantID, key).Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(snapshot, []byte(first.ID)) {
		t.Fatalf("response snapshot does not preserve first publication: %s", snapshot)
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
