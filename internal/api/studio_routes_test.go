package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/pazhou/supaflow/internal/definition"
	definitionflowgram "github.com/pazhou/supaflow/internal/definition/flowgram"
	"github.com/pazhou/supaflow/internal/platform"
	"github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/workflow"
)

const viewerKey = "tenant-a-viewer-api-key-at-least-24-bytes"

func studioHandler(t *testing.T) http.Handler {
	t.Helper()
	auth, err := NewAuthenticator([]APIKey{
		{Value: tenantAKey, Principal: Principal{TenantID: "tenant-a", ActorID: "admin-a", Role: RoleAdmin}},
		{Value: tenantBKey, Principal: Principal{TenantID: "tenant-b", ActorID: "admin-b", Role: RoleAdmin}},
		{Value: viewerKey, Principal: Principal{TenantID: "tenant-a", ActorID: "viewer-a", Role: RoleViewer}},
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := workflow.BuiltinRegistry()
	policy := workflow.CompilePolicy{
		AllowedCapabilities: workflow.CapabilitySet{Streaming: true, Cancellable: true, Interruptible: true, Idempotent: true},
		AllowedPermissions:  workflow.PermissionSet{Network: true, Credentials: true},
	}
	catalog := platform.NewMemoryCatalog(registry, policy)
	definitions, err := definition.NewService(definition.NewMemoryStore(), definitionflowgram.Adapter{}, catalog, registry, policy)
	if err != nil {
		t.Fatal(err)
	}
	resumeTokens, err := runtime.NewResumeTokenIssuer([]byte("test-resume-token-secret-32-bytes!"))
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(runtime.NewService(runtime.NewMemoryStore(), nil, nil), catalog, auth, resumeTokens)
	if err != nil {
		t.Fatal(err)
	}
	handler.SetStudio(definitions, registry)
	return handler.Routes()
}

func flowgramDocument(id string) json.RawMessage {
	return json.RawMessage(`{"schemaVersion":"flowgram-v1","workflowId":"` + id + `","nodes":[` +
		`{"id":"start","type":"start","version":"1.0.0","config":{},"position":{"x":0,"y":0}},` +
		`{"id":"end","type":"end","version":"1.0.0","config":{},"position":{"x":200,"y":0}}],` +
		`"edges":[{"id":"edge","sourceNodeId":"start","sourcePortId":"input","targetNodeId":"end","targetPortId":"result"}]}`)
}

// 验证草稿 API 贯穿鉴权、DTO、幂等、乐观锁、编译与租户隔离。
func TestStudioDraftLifecycle(t *testing.T) {
	handler := studioHandler(t)
	body := map[string]any{"workflowId": "flow", "document": flowgramDocument("flow")}
	created := requestJSON(t, handler, http.MethodPost, "/v1/workflows", tenantAKey, "draft-create-0001", body)
	replayed := requestJSON(t, handler, http.MethodPost, "/v1/workflows", tenantAKey, "draft-create-0001", body)
	if created.Code != http.StatusCreated || replayed.Code != http.StatusCreated || created.Body.String() != replayed.Body.String() {
		t.Fatalf("create=%d %s replay=%d %s", created.Code, created.Body.String(), replayed.Code, replayed.Body.String())
	}
	forbidden := requestJSON(t, handler, http.MethodPost, "/v1/workflows", viewerKey, "draft-create-viewer", body)
	if forbidden.Code != http.StatusForbidden {
		t.Fatalf("viewer create status=%d", forbidden.Code)
	}
	otherTenant := requestJSON(t, handler, http.MethodGet, "/v1/workflows/flow", tenantBKey, "", nil)
	if otherTenant.Code != http.StatusNotFound {
		t.Fatalf("cross tenant status=%d body=%s", otherTenant.Code, otherTenant.Body.String())
	}
	compiled := requestJSON(t, handler, http.MethodPost, "/v1/workflows/flow/compile", tenantAKey, "", nil)
	if compiled.Code != http.StatusOK {
		t.Fatalf("compile status=%d body=%s", compiled.Code, compiled.Body.String())
	}
	published := requestJSON(t, handler, http.MethodPost, "/v1/workflows/flow/publish-draft", tenantAKey, "draft-publish-0001", nil)
	if published.Code != http.StatusCreated {
		t.Fatalf("publish draft status=%d body=%s", published.Code, published.Body.String())
	}
	dryRun := requestJSON(t, handler, http.MethodPost, "/v1/workflows/flow/dry-run", tenantAKey, "", map[string]any{"sampleInput": map[string]any{"message": "hello"}})
	if dryRun.Code != http.StatusOK {
		t.Fatalf("dry run status=%d body=%s", dryRun.Code, dryRun.Body.String())
	}
	nodeTest := requestJSON(t, handler, http.MethodPost, "/v1/workflows/flow/node-test", tenantAKey, "", map[string]any{"nodeId": "end", "sampleInput": map[string]any{}})
	if nodeTest.Code != http.StatusOK {
		t.Fatalf("node test status=%d body=%s", nodeTest.Code, nodeTest.Body.String())
	}
}

// 验证最新版本解析契约稳定返回 planId/planHash，且版本历史不会跨租户泄漏。
func TestLatestPublishedVersionContract(t *testing.T) {
	handler := studioHandler(t)
	publish := requestJSON(t, handler, http.MethodPost, "/v1/workflows/flow/publish", tenantAKey, "publish-latest-0001", minimalDefinition("flow"))
	if publish.Code != http.StatusCreated {
		t.Fatalf("publish status=%d body=%s", publish.Code, publish.Body.String())
	}
	latest := requestJSON(t, handler, http.MethodGet, "/v1/workflows/flow/versions/latest", viewerKey, "", nil)
	if latest.Code != http.StatusOK {
		t.Fatalf("latest status=%d body=%s", latest.Code, latest.Body.String())
	}
	var response versionSummaryResponse
	if err := json.Unmarshal(latest.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.WorkflowID != "flow" || response.Version != 1 || response.VersionID == "" || response.PlanID == "" || response.PlanHash == "" {
		t.Fatalf("latest=%+v", response)
	}
	specific := requestJSON(t, handler, http.MethodGet, "/v1/workflows/flow/versions/1", viewerKey, "", nil)
	if specific.Code != http.StatusOK {
		t.Fatalf("specific status=%d body=%s", specific.Code, specific.Body.String())
	}
	var immutable versionResponse
	if err := json.Unmarshal(specific.Body.Bytes(), &immutable); err != nil {
		t.Fatal(err)
	}
	if immutable.WorkflowID != response.WorkflowID || immutable.PlanID != response.PlanID || immutable.Definition.ID != "flow" {
		t.Fatalf("specific=%+v", immutable)
	}
	isolated := requestJSON(t, handler, http.MethodGet, "/v1/workflows/flow/versions/latest", tenantBKey, "", nil)
	if isolated.Code != http.StatusNotFound {
		t.Fatalf("cross tenant latest status=%d body=%s", isolated.Code, isolated.Body.String())
	}
}

// 验证节点类型发现 API 暴露真实注册表而不是前端硬编码的幻影类型。
func TestNodeTypeDiscovery(t *testing.T) {
	response := requestJSON(t, studioHandler(t), http.MethodGet, "/v1/node-types", viewerKey, "", nil)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var entries []workflow.NodeTypeVersion
	if err := json.Unmarshal(response.Body.Bytes(), &entries); err != nil {
		t.Fatal(err)
	}
	if len(entries) < 4 {
		t.Fatalf("entries=%+v", entries)
	}
	for _, entry := range entries {
		if entry.SensitiveFields == nil {
			t.Fatalf("node %s omitted sensitive fields", entry.Type)
		}
	}
}

type nodeTypeProjectionStub struct {
	entries []workflow.NodeTypeVersion
	err     error
	calls   int
}

func (s *nodeTypeProjectionStub) ListNodeTypes(context.Context) ([]workflow.NodeTypeVersion, error) {
	s.calls++
	return s.entries, s.err
}

// 验证节点发现从配置的持久化 projection 读取，而不是从 compiler 内存缓存读取。
func TestNodeTypeDiscoveryReadsConfiguredProjection(t *testing.T) {
	auth, err := NewAuthenticator([]APIKey{{Value: viewerKey, Principal: Principal{TenantID: "tenant-a", ActorID: "viewer-a", Role: RoleViewer}}})
	if err != nil {
		t.Fatal(err)
	}
	registry := workflow.BuiltinRegistry()
	policy := workflow.CompilePolicy{}
	catalog := platform.NewMemoryCatalog(registry, policy)
	definitions, err := definition.NewService(definition.NewMemoryStore(), definitionflowgram.Adapter{}, catalog, registry, policy)
	if err != nil {
		t.Fatal(err)
	}
	resumeTokens, err := runtime.NewResumeTokenIssuer([]byte("test-resume-token-secret-32-bytes!"))
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(runtime.NewService(runtime.NewMemoryStore(), nil, nil), catalog, auth, resumeTokens)
	if err != nil {
		t.Fatal(err)
	}
	projection := &nodeTypeProjectionStub{entries: []workflow.NodeTypeVersion{{
		Type: "db-only", Version: "9.0.0", Executor: workflow.ExecutorBinding{Kind: "db.only", MinimumRunnerVersion: "1.0.0"},
	}}}
	handler.SetStudio(definitions, projection)
	response := requestJSON(t, handler.Routes(), http.MethodGet, "/v1/node-types", viewerKey, "", nil)
	if response.Code != http.StatusOK || projection.calls != 1 {
		t.Fatalf("status=%d calls=%d body=%s", response.Code, projection.calls, response.Body.String())
	}
	var entries []workflow.NodeTypeVersion
	if err = json.Unmarshal(response.Body.Bytes(), &entries); err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Type != "db-only" {
		t.Fatalf("entries=%+v", entries)
	}

	projection.err = errors.New("database unavailable")
	response = requestJSON(t, handler.Routes(), http.MethodGet, "/v1/node-types", viewerKey, "", nil)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("storage error status=%d body=%s", response.Code, response.Body.String())
	}
}
