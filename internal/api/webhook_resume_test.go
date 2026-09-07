package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/pazhou/supaflow/internal/platform"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/workflow"
)

func webhookResumeFixture(t *testing.T) (http.Handler, *workflowruntime.Service, *workflowruntime.MemoryStore, *workflowruntime.ResumeTokenIssuer, workflowruntime.Interrupt) {
	t.Helper()
	auth, err := NewMasterKeyAuthenticator(tenantAKey)
	if err != nil {
		t.Fatal(err)
	}
	catalog := platform.NewMemoryCatalog(workflow.BuiltinRegistry(), workflow.CompilePolicy{})
	store := workflowruntime.NewMemoryStore()
	service := workflowruntime.NewService(store, nil, nil)
	issuer, err := workflowruntime.NewResumeTokenIssuer([]byte("test-resume-token-secret-32-bytes!"))
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(service, catalog, auth, issuer)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	created, err := service.StartRun(context.Background(), "tenant-a", workflowruntime.StartRunRequest{
		Plan:  workflowruntime.PlanRef{ID: "plan", Hash: "hash", EntryNodeID: "wait", Executor: "builtin.webhook-resume"},
		Input: json.RawMessage(`{"order":42}`), IdempotencyKey: "webhook-api-run",
	})
	if err != nil {
		t.Fatal(err)
	}
	claimAt := time.Now().UTC().Add(time.Second)
	work, err := store.ClaimAttempt(context.Background(), workflowruntime.ClaimAttemptRequest{TenantID: "tenant-a", WorkerID: "worker", Now: claimAt, LeaseDuration: time.Minute})
	if err != nil || work == nil {
		t.Fatalf("work=%+v err=%v", work, err)
	}
	interrupt := workflowruntime.Interrupt{ID: "int_webhook", Kind: "webhook", ExpiresAt: now.Add(time.Hour)}
	token, err := issuer.Issue(workflowruntime.Interrupt{TenantID: "tenant-a", ID: interrupt.ID, RunID: created.Run.ID, NodeID: work.Attempt.NodeID, AttemptID: work.Attempt.ID, Kind: "webhook", ExpiresAt: interrupt.ExpiresAt})
	if err != nil {
		t.Fatal(err)
	}
	interrupt.TokenDigest = workflowruntime.ResumeTokenDigest(token)
	stored, err := store.SuspendAttempt(context.Background(), workflowruntime.SuspendAttemptRequest{
		TenantID: "tenant-a", RunID: created.Run.ID, AttemptID: work.Attempt.ID,
		WorkerID: "worker", FencingToken: work.Lease.FencingToken, Now: claimAt, Interrupt: interrupt,
	})
	if err != nil {
		t.Fatal(err)
	}
	return handler.Routes(), service, store, issuer, stored
}

func webhookResumeRequest(handler http.Handler, interruptID, token, key, timestamp string, body []byte) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/hooks/resume/"+interruptID, bytes.NewReader(body))
	request.Header.Set("Authorization", "Resume "+token)
	request.Header.Set("Idempotency-Key", key)
	request.Header.Set("X-Supaflow-Timestamp", timestamp)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

// 验证公网恢复端点无需租户凭据、令牌不出现在 URL/body，并可幂等重放任意 JSON。
func TestWebhookResumePublicEndpointAndReplay(t *testing.T) {
	handler, _, _, issuer, interrupt := webhookResumeFixture(t)
	token, err := issuer.Issue(interrupt)
	if err != nil {
		t.Fatal(err)
	}
	timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
	first := webhookResumeRequest(handler, interrupt.ID, token, "webhook-http-key", timestamp, []byte(`{"paid":true,"items":[1,2]}`))
	replay := webhookResumeRequest(handler, interrupt.ID, token, "webhook-http-key", timestamp, []byte(`{"paid":true,"items":[1,2]}`))
	if first.Code != http.StatusOK || replay.Code != http.StatusOK {
		t.Fatalf("first=%d %s replay=%d %s", first.Code, first.Body.String(), replay.Code, replay.Body.String())
	}
	var firstRun, replayRun runResponse
	if json.Unmarshal(first.Body.Bytes(), &firstRun) != nil || json.Unmarshal(replay.Body.Bytes(), &replayRun) != nil || firstRun.ID != replayRun.ID || firstRun.Status != workflowruntime.RunPending {
		t.Fatalf("first=%+v replay=%+v", firstRun, replayRun)
	}
}

// 验证公网恢复端点拒绝过期 timestamp、缺失 token、路径错绑和超限 body。
func TestWebhookResumePublicEndpointRejectsReplayAndBindingAbuse(t *testing.T) {
	handler, _, _, issuer, interrupt := webhookResumeFixture(t)
	token, _ := issuer.Issue(interrupt)
	stale := strconv.FormatInt(time.Now().Add(-10*time.Minute).Unix(), 10)
	if response := webhookResumeRequest(handler, interrupt.ID, token, "webhook-stale-key", stale, []byte(`{}`)); response.Code != http.StatusUnauthorized {
		t.Fatalf("stale status=%d", response.Code)
	}
	now := strconv.FormatInt(time.Now().Unix(), 10)
	if response := webhookResumeRequest(handler, "different", token, "webhook-bind-key", now, []byte(`{}`)); response.Code != http.StatusUnauthorized {
		t.Fatalf("binding status=%d body=%s", response.Code, response.Body.String())
	}
	oversize := bytes.Repeat([]byte(" "), maxWebhookResumeBody+1)
	if response := webhookResumeRequest(handler, interrupt.ID, token, "webhook-large-key", now, oversize); response.Code != http.StatusBadRequest {
		t.Fatalf("large status=%d", response.Code)
	}
}
