package api

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/pazhou/supaflow/internal/platform"
	"github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/workflow"
)

// failingRunStore 让 CreateRun 返回固定错误，其余行为沿用内存实现，用于验证传输层的错误分类。
type failingRunStore struct {
	runtime.Store
	err error
}

func (s failingRunStore) CreateRun(context.Context, runtime.CreateRunRequest) (runtime.CreateRunResult, error) {
	return runtime.CreateRunResult{}, s.err
}

// 验证运行时错误被翻译成诚实的 HTTP 状态：service_unavailable（服务端这次没做成）是 503 并带
// Retry-After，quota_exceeded 是 429 并带 Retry-After，语义冲突仍是 409，未枚举的码不猜语义而是
// 500 并留下服务端日志。此前所有未列出的运行时码都落成 409，把「这一发 run 根本没建」说成幂等
// 冲突，调用方因此不会重试一个本可以成功的请求。
func TestStartRunMapsRuntimeFailuresToHonestStatuses(t *testing.T) {
	cases := []struct {
		name          string
		err           error
		wantStatus    int
		wantCode      string
		wantRetryHint bool
	}{
		{name: "瞬态失败耗尽重试", err: &runtime.Error{Code: runtime.CodeUnavailable,
			Message: "the database could not complete this transaction", RetryAfter: 1_000_000_000},
			wantStatus: http.StatusServiceUnavailable, wantCode: "service_unavailable", wantRetryHint: true},
		{name: "容量耗尽", err: &runtime.Error{Code: runtime.CodeQuotaExceeded,
			Message: "organization delivery capacity is exhausted", RetryAfter: 5_000_000_000},
			wantStatus: http.StatusTooManyRequests, wantCode: "quota_exceeded", wantRetryHint: true},
		{name: "语义冲突保持 409", err: &runtime.Error{Code: runtime.CodeConflict, Message: "could not admit run"},
			wantStatus: http.StatusConflict, wantCode: "conflict"},
		{name: "未枚举的码不猜语义", err: &runtime.Error{Code: runtime.ErrorCode("brand_new_code"), Message: "boom"},
			wantStatus: http.StatusInternalServerError, wantCode: "internal_error"},
	}
	for _, testCase := range cases {
		handler := handlerWithRuntimeFailure(t, testCase.err)
		definition := minimalDefinition("flow")
		publish := requestJSON(t, handler, http.MethodPost, "/v1/workflows/flow/publish", organizationAKey, "publish-key-0001", definition)
		if publish.Code != http.StatusCreated {
			t.Fatalf("%s publish status=%d body=%s", testCase.name, publish.Code, publish.Body.String())
		}
		var published platform.PublishedPlan
		if err := json.Unmarshal(publish.Body.Bytes(), &published); err != nil {
			t.Fatal(err)
		}
		response := requestJSON(t, handler, http.MethodPost, "/v1/runs", organizationAKey, "run-key-00000001",
			map[string]any{"planId": published.ID, "planHash": published.Hash, "input": map[string]any{"message": "hello"}})
		assertProblemCode(t, response, testCase.wantStatus, testCase.wantCode)
		if hint := response.Header().Get("Retry-After"); (hint != "") != testCase.wantRetryHint {
			t.Fatalf("%s Retry-After=%q want hint=%t", testCase.name, hint, testCase.wantRetryHint)
		}
		// 5xx 的响应体不得带内部诊断；诊断只允许出现在服务端日志里。
		if testCase.wantStatus >= http.StatusInternalServerError && response.Body.String() != "" &&
			!json.Valid(response.Body.Bytes()) {
			t.Fatalf("%s body=%s", testCase.name, response.Body.String())
		}
	}
}

func handlerWithRuntimeFailure(t *testing.T, storeErr error) http.Handler {
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
	service := runtime.NewService(failingRunStore{Store: runtime.NewMemoryStore(), err: storeErr}, nil, nil)
	handler, err := NewHandler(service, catalog, auth, resumeTokens)
	if err != nil {
		t.Fatal(err)
	}
	return handler.Routes()
}
