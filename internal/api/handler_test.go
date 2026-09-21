package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pazhou/supaflow/internal/platform"
	"github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/workflow"
)

const (
	organizationAKey = "organization-a-test-api-key-at-least-24-bytes"
	organizationBKey = "organization-b-test-api-key-at-least-24-bytes"
)

func testHandler(t *testing.T) http.Handler {
	t.Helper()
	return testHandlerWithAudit(t, nil).Routes()
}

// testHandlerWithAudit 构造处理器并注入审计写入器；audit 为 nil 时中间件不介入，
// 与生产环境的装配差异只在「审计是否可用」这一点上。
func testHandlerWithAudit(t *testing.T, audit AuditRecorder) *Handler {
	t.Helper()
	auth, err := NewMasterKeyAuthenticator(organizationAKey)
	if err != nil {
		t.Fatal(err)
	}
	policy := workflow.CompilePolicy{
		AllowedCapabilities: workflow.CapabilitySet{Streaming: true, Cancellable: true, Interruptible: true, Idempotent: true},
		AllowedPermissions:  workflow.PermissionSet{Network: true, Credentials: true},
	}
	catalog := platform.NewMemoryCatalog(workflow.BuiltinRegistry(), policy)
	resumeTokens, err := runtime.NewResumeTokenIssuer([]byte("test-resume-token-secret-32-bytes!"))
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(runtime.NewService(runtime.NewMemoryStore(), nil, nil), catalog, auth, resumeTokens)
	if err != nil {
		t.Fatal(err)
	}
	handler.SetAudit(audit)
	return handler
}

func minimalDefinition(id string) workflow.WorkflowDefinition {
	return workflow.WorkflowDefinition{
		SchemaVersion: workflow.DefinitionSchemaV1,
		ID:            id,
		Nodes: []workflow.NodeDefinition{
			{ID: "start", Type: workflow.NodeTypeStart, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
			{ID: "end", Type: workflow.NodeTypeEnd, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
		},
		Edges: []workflow.EdgeDefinition{{ID: "edge", FromNode: "start", FromPort: "input", ToNode: "end", ToPort: "result"}},
	}
}

func testOrganization(key string) string {
	if key == organizationBKey {
		return "organization-b"
	}
	return "organization-a"
}

func authenticatedTestRequest(request *http.Request, key string) {
	request.Header.Set("Authorization", "Bearer "+organizationAKey)
	request.Header.Set(organizationHeader, testOrganization(key))
}

func requestJSON(t *testing.T, handler http.Handler, method, path, key, idempotency string, body any) *httptest.ResponseRecorder {
	t.Helper()
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequestWithContext(context.Background(), method, path, bytes.NewReader(payload))
	if key != "" {
		authenticatedTestRequest(req, key)
	}
	if idempotency != "" {
		req.Header.Set("Idempotency-Key", idempotency)
	}
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	return res
}

// 验证健康探针不依赖凭据，便于编排系统执行存活检查。
func TestHealthEndpointIsPublic(t *testing.T) {
	res := requestJSON(t, testHandler(t), http.MethodGet, "/health/live", "", "", nil)
	if res.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", res.Code, res.Body.String())
	}
}

// 验证所有租户 API 响应禁止共享缓存，避免 X-API-Key 场景下跨租户复用内容。
func TestAuthenticatedResponsesArePrivateAndNotStored(t *testing.T) {
	response := requestJSON(t, testHandler(t), http.MethodGet, "/v1/runs/missing", organizationAKey, "", nil)
	if response.Header().Get("Cache-Control") != "private, no-store" {
		t.Fatalf("Cache-Control = %q", response.Header().Get("Cache-Control"))
	}
}

// 验证依赖不可用时 readiness 失败且不泄露底层错误。
func TestReadinessReportsDependencyFailure(t *testing.T) {
	auth, err := NewMasterKeyAuthenticator(organizationAKey)
	if err != nil {
		t.Fatal(err)
	}
	catalog := platform.NewMemoryCatalog(workflow.BuiltinRegistry(), workflow.CompilePolicy{})
	resumeTokens, err := runtime.NewResumeTokenIssuer([]byte("test-resume-token-secret-32-bytes!"))
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(runtime.NewService(runtime.NewMemoryStore(), nil, nil), catalog, auth, resumeTokens)
	if err != nil {
		t.Fatal(err)
	}
	handler.SetReadinessProbe(func(context.Context) error { return errors.New("secret database detail") })
	res := requestJSON(t, handler.Routes(), http.MethodGet, "/health/ready", "", "", nil)
	if res.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", res.Code, res.Body.String())
	}
	if strings.Contains(res.Body.String(), "secret database detail") {
		t.Fatalf("readiness leaked dependency error: %s", res.Body.String())
	}
}

// 验证发布与启动形成端到端垂直切片，且运行启动幂等重放返回同一 ID。
func TestPublishAndStartRunIdempotently(t *testing.T) {
	handler := testHandler(t)
	definition := minimalDefinition("flow")
	publish := requestJSON(t, handler, http.MethodPost, "/v1/workflows/flow/publish", organizationAKey, "publish-key-0001", definition)
	if publish.Code != http.StatusCreated {
		t.Fatalf("publish status=%d body=%s", publish.Code, publish.Body.String())
	}
	var published platform.PublishedPlan
	if err := json.Unmarshal(publish.Body.Bytes(), &published); err != nil {
		t.Fatal(err)
	}
	replayedPublish := requestJSON(t, handler, http.MethodPost, "/v1/workflows/flow/publish", organizationAKey, "publish-key-0001", definition)
	if replayedPublish.Code != http.StatusOK {
		t.Fatalf("publish replay status=%d body=%s", replayedPublish.Code, replayedPublish.Body.String())
	}
	var replayedPlan platform.PublishedPlan
	if err := json.Unmarshal(replayedPublish.Body.Bytes(), &replayedPlan); err != nil {
		t.Fatal(err)
	}
	if replayedPlan.ID != published.ID || replayedPlan.Hash != published.Hash || replayedPlan.Version != published.Version {
		t.Fatalf("publish replay differs: first=%+v replay=%+v", published, replayedPlan)
	}
	body := map[string]any{"planId": published.ID, "planHash": published.Hash, "input": map[string]any{"message": "hello"}}
	first := requestJSON(t, handler, http.MethodPost, "/v1/runs", organizationAKey, "run-key-00000001", body)
	second := requestJSON(t, handler, http.MethodPost, "/v1/runs", organizationAKey, "run-key-00000001", body)
	if first.Code != http.StatusAccepted || second.Code != http.StatusOK {
		t.Fatalf("first=%d %s second=%d %s", first.Code, first.Body.String(), second.Code, second.Body.String())
	}
	var firstRun, secondRun runResponse
	if err := json.Unmarshal(first.Body.Bytes(), &firstRun); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(second.Body.Bytes(), &secondRun); err != nil {
		t.Fatal(err)
	}
	if firstRun.ID == "" || firstRun.ID != secondRun.ID {
		t.Fatalf("first=%q second=%q", firstRun.ID, secondRun.ID)
	}
}

// 验证 Run DTO 返回 inline 终态输出，并仅用安全元数据表示对象存储结果。
func TestRunDTOProjectsOutputWithoutObjectReference(t *testing.T) {
	inline := runDTO(runtime.Run{Output: &runtime.RunOutput{
		Inline: json.RawMessage(`{"answer":42}`), ContentType: "application/json",
		Size: 13, SHA256: strings.Repeat("a", 64),
	}})
	if string(inline.Output) != `{"answer":42}` || inline.OutputMetadata == nil || inline.OutputMetadata.Storage != "inline" {
		t.Fatalf("inline DTO = %+v", inline)
	}
	external := runDTO(runtime.Run{Output: &runtime.RunOutput{
		ContentType: "application/json", Size: 1024, SHA256: strings.Repeat("b", 64), External: true, RetentionStatus: "active",
	}})
	payload, err := json.Marshal(external)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(payload, []byte(`"output":`)) || external.OutputMetadata == nil || external.OutputMetadata.Storage != "object" || external.OutputMetadata.DownloadURL == "" {
		t.Fatalf("external DTO = %s", payload)
	}
}

// 验证另一租户即使猜中 plan ID 与 hash 也不能启动该计划。
func TestStartRunRejectsCrossOrganizationPlan(t *testing.T) {
	handler := testHandler(t)
	publish := requestJSON(t, handler, http.MethodPost, "/v1/workflows/flow/publish", organizationAKey, "publish-key-0002", minimalDefinition("flow"))
	var published platform.PublishedPlan
	if err := json.Unmarshal(publish.Body.Bytes(), &published); err != nil {
		t.Fatal(err)
	}
	body := map[string]any{"planId": published.ID, "planHash": published.Hash, "input": map[string]any{}}
	started := requestJSON(t, handler, http.MethodPost, "/v1/runs", organizationBKey, "run-key-00000002", body)
	if started.Code != http.StatusNotFound {
		t.Fatalf("status=%d body=%s", started.Code, started.Body.String())
	}
}
