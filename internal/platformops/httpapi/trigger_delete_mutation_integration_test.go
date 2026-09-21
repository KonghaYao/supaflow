package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/pazhou/supaflow/internal/api"
	"github.com/pazhou/supaflow/internal/platformops"
	"github.com/pazhou/supaflow/internal/postgres"
	"github.com/pazhou/supaflow/internal/workflow"
)

// 验证「成功且无内容」的响应也能完成幂等账本。
//
// DELETE /ops/triggers/{id} 成功时只写状态码、不写 body，HTTP 中间件于是把 bytes.Buffer 的
// Bytes()（从未写过时为 nil）交给账本；000015 的 CHECK 要求 completed 行同时有 response_status
// 与 response_body，NULL 让完成事务回滚、中间件报 500，而触发器其实已经删掉了。改造前本用例第一
// 个断言就会失败（拿到 500 而不是 204），账本也永远停在 pending。
func TestTriggerDeletionWithEmptyBodyCompletesMutationLedger(t *testing.T) {
	ctx, pool, organizationID := mutationLeaseFixture(t)
	suffix := fmt.Sprintf("%d", time.Now().UTC().UnixNano())
	ledger, err := platformops.NewPostgresMutationLedger(pool)
	if err != nil {
		t.Fatal(err)
	}
	store := platformops.NewPostgresTriggerStore(pool)
	service, err := platformops.NewTriggerService(store, nil, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	handler := &Handler{services: Services{Triggers: service, Mutations: ledger}}
	auth, err := api.NewMasterKeyAuthenticator(httpTestMasterKey)
	if err != nil {
		t.Fatal(err)
	}
	workflowID := "workflow_" + suffix
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
	catalog := postgres.NewPlanCatalog(pool, workflow.BuiltinRegistry(), workflow.CompilePolicy{
		AllowedCapabilities: workflow.CapabilitySet{Idempotent: true},
	})
	if _, created, publishErr := catalog.Publish(ctx, organizationID, workflowID, definition, "publish_"+suffix); publishErr != nil || !created {
		t.Fatalf("publish created=%v err=%v", created, publishErr)
	}
	trigger, err := service.Create(ctx, platformops.CreateTriggerRequest{
		OrganizationID: organizationID, ActorID: "operator", WorkflowID: workflowID,
		Kind: platformops.TriggerEvent, Name: "delete-target-" + suffix,
		Config: json.RawMessage(`{}`), EventType: "delete.target",
	})
	if err != nil {
		t.Fatal(err)
	}
	key := "delete-trigger-" + suffix
	serve := func() *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodDelete, "/ops/triggers/"+trigger.ID+
			"?expectedUpdatedAt="+url.QueryEscape(trigger.UpdatedAt.Format(time.RFC3339Nano)), nil)
		request.Header.Set("Authorization", "Bearer "+httpTestMasterKey)
		request.Header.Set("X-Fenix-Organization-Id", organizationID)
		request.Header.Set("Idempotency-Key", key)
		response := httptest.NewRecorder()
		auth.Middleware(handler.Routes()).ServeHTTP(response, request)
		return response
	}
	first := serve()
	if first.Code != http.StatusNoContent {
		t.Fatalf("删除返回 status=%d body=%s", first.Code, first.Body.String())
	}
	if first.Body.Len() != 0 {
		t.Fatalf("204 响应带了 body: %q", first.Body.String())
	}
	var status, operation string
	var responseBody []byte
	if err = pool.QueryRow(ctx, `SELECT status, response_body, operation FROM ops_mutation_ledger
		WHERE organization_id=$1 AND operation='trigger.mutate' AND idempotency_key=$2`,
		organizationID, key).Scan(&status, &responseBody, &operation); err != nil {
		t.Fatal(err)
	}
	if status != "completed" {
		t.Fatalf("204 之后账本仍是 status=%s operation=%s", status, operation)
	}
	if responseBody == nil {
		t.Fatal("completed 行的 response_body 仍是 NULL")
	}
	var remaining int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM triggers WHERE organization_id=$1 AND id=$2`,
		organizationID, trigger.ID).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Fatalf("删除没有落库，剩余 %d 行", remaining)
	}
	replay := serve()
	if replay.Code != http.StatusNoContent || replay.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatalf("重放 status=%d replayed=%q body=%s", replay.Code, replay.Header().Get("Idempotency-Replayed"), replay.Body.String())
	}
}
