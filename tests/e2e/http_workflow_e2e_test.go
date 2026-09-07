package e2e

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/pazhou/supaflow/internal/api"
	"github.com/pazhou/supaflow/internal/executor"
	"github.com/pazhou/supaflow/internal/platform"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/workflow"
)

const httpWorkflowAdminKey = "http-workflow-admin-master-key-for-e2e-32"

type approvalInputRecorder struct {
	delegate workflowruntime.Executor
	mu       sync.Mutex
	input    json.RawMessage
}

func (r *approvalInputRecorder) Execute(ctx context.Context, request workflowruntime.ExecutionRequest) (workflowruntime.ExecutionResult, error) {
	if request.Executor == "builtin.human-approval" && len(request.ResumeData) == 0 {
		r.mu.Lock()
		r.input = append(r.input[:0], request.Input...)
		r.mu.Unlock()
	}
	return r.delegate.Execute(ctx, request)
}

func (r *approvalInputRecorder) Cancel(ctx context.Context, request workflowruntime.ExecutionRequest) error {
	return r.delegate.Cancel(ctx, request)
}

func (r *approvalInputRecorder) approvalInput() json.RawMessage {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append(json.RawMessage(nil), r.input...)
}

type httpWorkflowPlan struct {
	ID   string `json:"id"`
	Hash string `json:"hash"`
}

type httpWorkflowRun struct {
	ID     string                    `json:"id"`
	Status workflowruntime.RunStatus `json:"status"`
}

type httpWorkflowInterrupt struct {
	ID          string `json:"id"`
	ResumeToken string `json:"resumeToken"`
}

// 验证零监听端口下 API 能完整推进 Start→HTTP→Approval→End，且审批收到 HTTP 输出绑定。
func TestHTTPApprovalWorkflowThroughAPIWithoutListener(t *testing.T) {
	resumeTokens, err := workflowruntime.NewResumeTokenIssuer([]byte("http-workflow-resume-token-secret!"))
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := api.NewMasterKeyAuthenticator(httpWorkflowAdminKey)
	if err != nil {
		t.Fatal(err)
	}
	catalog := platform.NewMemoryCatalog(workflow.BuiltinRegistry(), workflow.CompilePolicy{
		AllowedCapabilities: workflow.CapabilitySet{Streaming: true, Cancellable: true, Interruptible: true, Idempotent: true},
		AllowedPermissions:  workflow.PermissionSet{Network: true, Credentials: true},
	})
	requestObserved := make(chan struct{}, 1)
	planExecutor, err := executor.NewPlanExecutor(catalog, executor.Config{
		AllowedHosts: []string{"api.example"},
		DNSResolver:  httpWorkflowResolver{},
		DialContext: executor.DialContextFunc(func(_ context.Context, _, _ string) (net.Conn, error) {
			client, server := net.Pipe()
			go func() {
				defer server.Close()
				request, readErr := http.ReadRequest(bufio.NewReader(server))
				if readErr != nil {
					return
				}
				defer request.Body.Close()
				_, _ = io.Copy(io.Discard, request.Body)
				requestObserved <- struct{}{}
				_, _ = io.WriteString(server, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 19\r\n\r\n{\"message\":\"ready\"}")
			}()
			return client, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	recorder := &approvalInputRecorder{delegate: planExecutor}
	store := workflowruntime.NewMemoryStore()
	service := workflowruntime.NewService(store, nil, nil)
	orchestrator, err := workflowruntime.NewOrchestrator(store, recorder, nil, nil, workflowruntime.OrchestratorConfig{
		WorkerID: "http-e2e-worker", LeaseDuration: time.Minute,
		RetryPolicy:  workflowruntime.RetryPolicy{MaxAttempts: 2, BaseDelay: time.Millisecond, MaxDelay: time.Second},
		ResumeTokens: resumeTokens,
	})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := api.NewHandler(service, catalog, authenticator, resumeTokens)
	if err != nil {
		t.Fatal(err)
	}
	routes := handler.Routes()

	publishResponse := httpWorkflowRequest(t, routes, http.MethodPost, "/v1/workflows/http-approval-flow/publish", "publish-http-e2e", httpWorkflowDefinition())
	if publishResponse.Code != http.StatusCreated {
		t.Fatalf("publish status=%d body=%s", publishResponse.Code, publishResponse.Body.String())
	}
	plan := httpWorkflowDecode[httpWorkflowPlan](t, publishResponse)
	startResponse := httpWorkflowRequest(t, routes, http.MethodPost, "/v1/runs", "start-http-e2e", map[string]any{
		"planId": plan.ID, "planHash": plan.Hash, "input": map[string]any{"order": 42},
	})
	if startResponse.Code != http.StatusAccepted {
		t.Fatalf("start status=%d body=%s", startResponse.Code, startResponse.Body.String())
	}
	run := httpWorkflowDecode[httpWorkflowRun](t, startResponse)

	for step := 0; step < 3; step++ {
		result, advanceErr := orchestrator.Advance(context.Background(), "organization-http")
		if advanceErr != nil || !result.Worked {
			t.Fatalf("advance %d: result=%+v err=%v", step, result, advanceErr)
		}
		if step == 2 && result.Interrupt == nil {
			t.Fatalf("approval did not suspend: %+v", result)
		}
	}
	select {
	case <-requestObserved:
	default:
		t.Fatal("HTTP node did not use the controlled dialer")
	}
	var approvalInput struct {
		StatusCode int             `json:"statusCode"`
		Body       json.RawMessage `json:"body"`
	}
	if err := json.Unmarshal(recorder.approvalInput(), &approvalInput); err != nil {
		t.Fatalf("approval input is invalid: %v", err)
	}
	if approvalInput.StatusCode != http.StatusOK || string(approvalInput.Body) != `{"message":"ready"}` {
		t.Fatalf("approval did not receive HTTP output: %s", recorder.approvalInput())
	}

	interruptResponse := httpWorkflowRequest(t, routes, http.MethodGet, "/v1/runs/"+run.ID+"/interrupts", "", nil)
	if interruptResponse.Code != http.StatusOK {
		t.Fatalf("interrupt list status=%d body=%s", interruptResponse.Code, interruptResponse.Body.String())
	}
	interrupts := httpWorkflowDecode[[]httpWorkflowInterrupt](t, interruptResponse)
	if len(interrupts) != 1 || interrupts[0].ResumeToken == "" {
		t.Fatalf("interrupts=%+v", interrupts)
	}
	resolveResponse := httpWorkflowRequest(t, routes, http.MethodPost, "/v1/interrupts/"+interrupts[0].ID+"/resolve", "resolve-http-e2e", map[string]any{
		"resumeToken": interrupts[0].ResumeToken, "decision": "approved",
	})
	if resolveResponse.Code != http.StatusOK {
		t.Fatalf("resolve status=%d body=%s", resolveResponse.Code, resolveResponse.Body.String())
	}
	for step := 0; step < 2; step++ {
		result, advanceErr := orchestrator.Advance(context.Background(), "organization-http")
		if advanceErr != nil || !result.Worked {
			t.Fatalf("resume advance %d: result=%+v err=%v", step, result, advanceErr)
		}
	}
	getResponse := httpWorkflowRequest(t, routes, http.MethodGet, "/v1/runs/"+run.ID, "", nil)
	completed := httpWorkflowDecode[httpWorkflowRun](t, getResponse)
	if getResponse.Code != http.StatusOK || completed.Status != workflowruntime.RunSuccess {
		t.Fatalf("completed status=%d run=%+v", getResponse.Code, completed)
	}
}

type httpWorkflowResolver struct{}

func (httpWorkflowResolver) LookupIPAddr(context.Context, string) ([]net.IPAddr, error) {
	return []net.IPAddr{{IP: net.ParseIP("93.184.216.34")}}, nil
}

func httpWorkflowDefinition() workflow.WorkflowDefinition {
	return workflow.WorkflowDefinition{
		SchemaVersion: workflow.DefinitionSchemaV1, ID: "http-approval-flow",
		Nodes: []workflow.NodeDefinition{
			{ID: "start", Type: workflow.NodeTypeStart, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
			{ID: "http", Type: workflow.NodeTypeHTTP, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{"method":"GET","url":"http://api.example/data"}`)},
			{ID: "approval", Type: workflow.NodeTypeHumanApproval, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{"prompt":"Approve HTTP response?","timeoutMs":60000}`), Inputs: map[string]workflow.InputBinding{
				"request": {Type: workflow.ValueTypeObject, Source: &workflow.OutputReference{NodeID: "http", Port: "response"}},
			}},
			{ID: "end", Type: workflow.NodeTypeEnd, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
		},
		Edges: []workflow.EdgeDefinition{
			{ID: "start_to_http", FromNode: "start", FromPort: "input", ToNode: "http", ToPort: "body"},
			{ID: "http_to_approval", FromNode: "http", FromPort: "response", ToNode: "approval", ToPort: "request"},
			{ID: "approval_to_end", FromNode: "approval", FromPort: "decision", ToNode: "end", ToPort: "result"},
		},
	}
}

func httpWorkflowRequest(t *testing.T, handler http.Handler, method, path, idempotencyKey string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var payload []byte
	if body != nil {
		var err error
		payload, err = json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
	}
	request := httptest.NewRequestWithContext(context.Background(), method, path, bytes.NewReader(payload))
	request.Header.Set("Authorization", "Bearer "+httpWorkflowAdminKey)
	request.Header.Set("X-Fenix-Organization-Id", "organization-http")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func httpWorkflowDecode[T any](t *testing.T, response *httptest.ResponseRecorder) T {
	t.Helper()
	var decoded T
	if err := json.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode status=%d body=%q: %v", response.Code, response.Body.String(), err)
	}
	return decoded
}
