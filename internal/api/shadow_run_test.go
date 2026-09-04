package api

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/pazhou/supaflow/internal/platform"
	"github.com/pazhou/supaflow/internal/workflow"
)

// 验证 HTTP shadow-run 契约对缺失与未审查 stub 使用稳定错误码，完整绑定后才创建运行。
func TestShadowRunHTTPRequiresExplicitReviewedStubs(t *testing.T) {
	handler := testHandler(t)
	definition := workflow.WorkflowDefinition{
		SchemaVersion: workflow.DefinitionSchemaV1,
		ID:            "shadow_http_flow",
		Nodes: []workflow.NodeDefinition{
			{ID: "start", Type: workflow.NodeTypeStart, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
			{ID: "request", Type: workflow.NodeTypeHTTP, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{"method":"POST","url":"https://example.com/orders"}`)},
			{ID: "end", Type: workflow.NodeTypeEnd, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
		},
		Edges: []workflow.EdgeDefinition{
			{ID: "to_request", FromNode: "start", FromPort: "input", ToNode: "request", ToPort: "body"},
			{ID: "to_end", FromNode: "request", FromPort: "response", ToNode: "end", ToPort: "result"},
		},
	}
	publish := requestJSON(t, handler, http.MethodPost, "/v1/workflows/shadow_http_flow/publish", tenantAKey, "publish-shadow-http", definition)
	if publish.Code != http.StatusCreated {
		t.Fatalf("publish status=%d body=%s", publish.Code, publish.Body.String())
	}
	var published platform.PublishedPlan
	if err := json.Unmarshal(publish.Body.Bytes(), &published); err != nil {
		t.Fatal(err)
	}
	body := map[string]any{"planId": published.ID, "planHash": published.Hash, "input": map[string]any{}, "stubBindings": map[string]string{}}
	missing := requestJSON(t, handler, http.MethodPost, "/v1/shadow-runs", tenantAKey, "shadow-http-run-1", body)
	assertProblemCode(t, missing, http.StatusConflict, "shadow_stub_required")
	body["stubBindings"] = map[string]string{"request": "custom.unreviewed"}
	invalid := requestJSON(t, handler, http.MethodPost, "/v1/shadow-runs", tenantAKey, "shadow-http-run-2", body)
	assertProblemCode(t, invalid, http.StatusBadRequest, "invalid_shadow_stub_binding")
	body["stubBindings"] = map[string]string{"request": workflow.ShadowStubExecutorKind}
	crossTenant := requestJSON(t, handler, http.MethodPost, "/v1/shadow-runs", tenantBKey, "shadow-http-run-cross", body)
	assertProblemCode(t, crossTenant, http.StatusNotFound, "not_found")
	started := requestJSON(t, handler, http.MethodPost, "/v1/shadow-runs", tenantAKey, "shadow-http-run-3", body)
	if started.Code != http.StatusAccepted {
		t.Fatalf("start status=%d body=%s", started.Code, started.Body.String())
	}

	// 普通 Run 的既有契约不要求 shadow 字段或 stub 绑定。
	normal := requestJSON(t, handler, http.MethodPost, "/v1/runs", tenantAKey, "normal-http-run-1", map[string]any{
		"planId": published.ID, "planHash": published.Hash, "input": map[string]any{},
	})
	if normal.Code != http.StatusAccepted {
		t.Fatalf("normal start status=%d body=%s", normal.Code, normal.Body.String())
	}
}

func assertProblemCode(t *testing.T, response interface {
	Result() *http.Response
}, status int, code string) {
	t.Helper()
	result := response.Result()
	defer result.Body.Close()
	if result.StatusCode != status {
		t.Fatalf("status=%d want=%d", result.StatusCode, status)
	}
	var body struct {
		Code string `json:"code"`
	}
	if err := json.NewDecoder(result.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Code != code {
		t.Fatalf("code=%q want=%q", body.Code, code)
	}
}
