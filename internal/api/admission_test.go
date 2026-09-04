package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pazhou/supaflow/internal/platform"
	"github.com/pazhou/supaflow/internal/runtime"
)

// 验证公开 Run API 接受有界优先级、返回该优先级，并把它纳入幂等语义。
func TestStartRunPriorityContract(t *testing.T) {
	handler := testHandler(t)
	publish := requestJSON(t, handler, http.MethodPost, "/v1/workflows/flow/publish", tenantAKey, "priority-publish-key", minimalDefinition("flow"))
	if publish.Code != http.StatusCreated {
		t.Fatalf("publish=%d %s", publish.Code, publish.Body.String())
	}
	var plan platform.PublishedPlan
	if err := json.Unmarshal(publish.Body.Bytes(), &plan); err != nil {
		t.Fatal(err)
	}
	body := map[string]any{"planId": plan.ID, "planHash": plan.Hash, "input": map[string]any{}, "priority": 42}
	started := requestJSON(t, handler, http.MethodPost, "/v1/runs", tenantAKey, "priority-run-key", body)
	if started.Code != http.StatusAccepted {
		t.Fatalf("start=%d %s", started.Code, started.Body.String())
	}
	var run runResponse
	if err := json.Unmarshal(started.Body.Bytes(), &run); err != nil {
		t.Fatal(err)
	}
	if run.Priority != 42 {
		t.Fatalf("priority=%d", run.Priority)
	}
	body["priority"] = 41
	conflict := requestJSON(t, handler, http.MethodPost, "/v1/runs", tenantAKey, "priority-run-key", body)
	if conflict.Code != http.StatusConflict {
		t.Fatalf("conflict=%d %s", conflict.Code, conflict.Body.String())
	}
	body["priority"] = 1001
	invalid := requestJSON(t, handler, http.MethodPost, "/v1/runs", tenantAKey, "another-priority-key", body)
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid=%d %s", invalid.Code, invalid.Body.String())
	}
}

// 验证队列耗尽映射为可重试 429，且只暴露秒级退避而不泄露内部计数。
func TestQuotaExceededMapsToRetryableProblem(t *testing.T) {
	handler := &Handler{}
	request := httptest.NewRequest(http.MethodPost, "/v1/runs", nil)
	response := httptest.NewRecorder()
	handler.writeError(response, request, &runtime.Error{
		Code: runtime.CodeQuotaExceeded, Message: "tenant run capacity is exhausted", RetryAfter: 5 * time.Second,
	})
	if response.Code != http.StatusTooManyRequests || response.Header().Get("Retry-After") != "5" {
		t.Fatalf("status=%d retry=%q body=%s", response.Code, response.Header().Get("Retry-After"), response.Body.String())
	}
}
