package api

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/pazhou/supaflow/internal/platform"
	"github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/workflow"
)

const rerunViewerKey = "rerun-viewer-test-api-key-at-least-24-bytes"

func rerunFixture(t *testing.T) (http.Handler, *runtime.Service, *runtime.MemoryStore, platform.PublishedPlan) {
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
	published, _, err := catalog.Publish(context.Background(), "organization-a", "flow", minimalDefinition("flow"), "publish-rerun-key")
	if err != nil {
		t.Fatal(err)
	}
	store := runtime.NewMemoryStore()
	service := runtime.NewService(store, nil, nil)
	tokens, err := runtime.NewResumeTokenIssuer([]byte("test-resume-token-secret-32-bytes!"))
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(service, catalog, auth, tokens)
	if err != nil {
		t.Fatal(err)
	}
	return handler.Routes(), service, store, published
}

func terminalRunWithEndInput(t *testing.T, service *runtime.Service, store *runtime.MemoryStore, published platform.PublishedPlan) runtime.Run {
	t.Helper()
	start, found := published.Plan.Node("start")
	if !found {
		t.Fatal("start node missing")
	}
	created, err := service.StartRun(context.Background(), "organization-a", runtime.StartRunRequest{
		Plan:  runtime.PlanRef{ID: published.ID, Hash: published.Hash, EntryNodeID: start.ID, Executor: start.Executor.Kind},
		Input: json.RawMessage(`{"root":1}`), IdempotencyKey: "source-rerun-key",
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	first, err := store.ClaimAttempt(context.Background(), runtime.ClaimAttemptRequest{
		OrganizationID: "organization-a", WorkerID: "worker", Now: now, LeaseDuration: time.Minute,
	})
	if err != nil || first == nil {
		t.Fatalf("claim start: %+v %v", first, err)
	}
	end, found := published.Plan.Node("end")
	if !found {
		t.Fatal("end node missing")
	}
	_, err = store.CompleteAttempt(context.Background(), runtime.CompleteAttemptRequest{
		OrganizationID: "organization-a", RunID: created.Run.ID, AttemptID: first.Attempt.ID,
		WorkerID: "worker", FencingToken: first.Lease.FencingToken, Now: now.Add(time.Second),
		RunStatus: runtime.RunRunning, Next: []runtime.NewAttempt{{
			NodeID: end.ID, Executor: end.Executor.Kind, Input: json.RawMessage(`{"fromStart":2}`),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.ClaimAttempt(context.Background(), runtime.ClaimAttemptRequest{
		OrganizationID: "organization-a", WorkerID: "worker", Now: now.Add(2 * time.Second), LeaseDuration: time.Minute,
	})
	if err != nil || second == nil {
		t.Fatalf("claim end: %+v %v", second, err)
	}
	finished, err := store.CompleteAttempt(context.Background(), runtime.CompleteAttemptRequest{
		OrganizationID: "organization-a", RunID: created.Run.ID, AttemptID: second.Attempt.ID,
		WorkerID: "worker", FencingToken: second.Lease.FencingToken,
		Now: now.Add(3 * time.Second), RunStatus: runtime.RunSuccess,
	})
	if err != nil {
		t.Fatal(err)
	}
	return finished
}

// 验证 operator 可 from-node 重跑，HTTP 重放返回同一 Run 并携带 lineage。
func TestRerunHTTPFromNodeAndReplay(t *testing.T) {
	handler, service, store, published := rerunFixture(t)
	source := terminalRunWithEndInput(t, service, store, published)
	body := map[string]any{"fromNodeId": "end"}
	first := requestJSON(t, handler, http.MethodPost, "/v1/runs/"+source.ID+"/rerun", organizationAKey, "http-rerun-key", body)
	replay := requestJSON(t, handler, http.MethodPost, "/v1/runs/"+source.ID+"/rerun", organizationAKey, "http-rerun-key", body)
	if first.Code != http.StatusAccepted || replay.Code != http.StatusOK {
		t.Fatalf("first=%d %s replay=%d %s", first.Code, first.Body.String(), replay.Code, replay.Body.String())
	}
	var created, repeated runResponse
	if err := json.Unmarshal(first.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(replay.Body.Bytes(), &repeated); err != nil {
		t.Fatal(err)
	}
	if created.ID == source.ID || created.ID != repeated.ID || created.Rerun == nil || created.Rerun.SourceRunID != source.ID || created.Rerun.FromNodeID != "end" {
		t.Fatalf("created=%+v replay=%+v", created, repeated)
	}
	work, err := store.ClaimAttempt(context.Background(), runtime.ClaimAttemptRequest{
		OrganizationID: "organization-a", WorkerID: "rerun-worker", Now: time.Now().UTC(), LeaseDuration: time.Minute,
	})
	if err != nil || work == nil || work.Attempt.NodeID != "end" || string(work.Attempt.Input) != `{"fromStart":2}` {
		t.Fatalf("work=%+v err=%v", work, err)
	}
}

// 验证已认证组织上下文可以 rerun，且请求体拒绝未知字段与不存在节点。
func TestRerunHTTPAuthorizationAndStrictBody(t *testing.T) {
	handler, service, store, published := rerunFixture(t)
	source := terminalRunWithEndInput(t, service, store, published)
	path := "/v1/runs/" + source.ID + "/rerun"
	denied := requestJSON(t, handler, http.MethodPost, path, rerunViewerKey, "viewer-rerun-key", map[string]any{})
	unknown := requestJSON(t, handler, http.MethodPost, path, organizationAKey, "strict-rerun-key", map[string]any{"unexpected": true})
	missing := requestJSON(t, handler, http.MethodPost, path, organizationAKey, "missing-rerun-key", map[string]any{"fromNodeId": "missing"})
	nullBody := requestJSON(t, handler, http.MethodPost, path, organizationAKey, "null-rerun-key", nil)
	nullNode := requestJSON(t, handler, http.MethodPost, path, organizationAKey, "null-node-key", map[string]any{"fromNodeId": nil})
	if denied.Code != http.StatusAccepted || unknown.Code != http.StatusBadRequest || missing.Code != http.StatusBadRequest || nullBody.Code != http.StatusBadRequest || nullNode.Code != http.StatusBadRequest {
		t.Fatalf("denied=%d unknown=%d missing=%d null=%d nullNode=%d", denied.Code, unknown.Code, missing.Code, nullBody.Code, nullNode.Code)
	}
}
