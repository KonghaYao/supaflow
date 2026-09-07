package e2e

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pazhou/supaflow/internal/api"
	"github.com/pazhou/supaflow/internal/executor"
	"github.com/pazhou/supaflow/internal/platform"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/workflow"
)

const (
	tenantAPublisherKey = "tenant-a-publisher-master-key-for-e2e-32"
	tenantAOperatorKey  = "tenant-a-operator-key-for-e2e"
	tenantAApproverKey  = "tenant-a-approver-key-for-e2e"
	tenantAViewerKey    = "tenant-a-viewer-key-for-e2e"
	tenantBPublisherKey = "tenant-b-publisher-key-for-e2e"
	tenantBOperatorKey  = "tenant-b-operator-key-for-e2e"
	tenantBApproverKey  = "tenant-b-approver-key-for-e2e"
	tenantBViewerKey    = "tenant-b-viewer-key-for-e2e"
)

type harness struct {
	handler      http.Handler
	orchestrator *workflowruntime.Orchestrator
	clock        *e2eClock
}

type e2eClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *e2eClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *e2eClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	c.mu.Unlock()
}

type publishedPlan struct {
	ID   string `json:"id"`
	Hash string `json:"hash"`
}

type runResponse struct {
	ID     string                    `json:"id"`
	Status workflowruntime.RunStatus `json:"status"`
}

type interruptResponse struct {
	ID          string `json:"id"`
	RunID       string `json:"runId"`
	NodeID      string `json:"nodeId"`
	Kind        string `json:"kind"`
	ResumeToken string `json:"resumeToken"`
}

type sseEvent struct {
	ID     uint64
	Type   string
	Cursor uint64
}

func newHarness(t *testing.T) harness {
	t.Helper()
	authenticator, err := api.NewMasterKeyAuthenticator(tenantAPublisherKey)
	if err != nil {
		t.Fatal(err)
	}
	catalog := platform.NewMemoryCatalog(workflow.BuiltinRegistry(), workflow.CompilePolicy{
		AllowedCapabilities: workflow.CapabilitySet{Cancellable: true, Interruptible: true, Idempotent: true},
	})
	store := workflowruntime.NewMemoryStore()
	clock := &e2eClock{now: time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)}
	runtimeService := workflowruntime.NewService(store, clock, nil)
	resumeTokens, err := workflowruntime.NewResumeTokenIssuer([]byte("e2e-resume-token-secret-at-least-32-bytes"))
	if err != nil {
		t.Fatal(err)
	}
	planExecutor, err := executor.NewPlanExecutor(catalog, executor.Config{})
	if err != nil {
		t.Fatal(err)
	}
	orchestrator, err := workflowruntime.NewOrchestrator(store, planExecutor, clock, nil, workflowruntime.OrchestratorConfig{
		WorkerID: "e2e-worker", LeaseDuration: time.Minute,
		RetryPolicy: workflowruntime.RetryPolicy{
			MaxAttempts: 2, BaseDelay: time.Millisecond, MaxDelay: time.Second,
		},
		ResumeTokens: resumeTokens,
	})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := api.NewHandler(runtimeService, catalog, authenticator, resumeTokens)
	if err != nil {
		t.Fatal(err)
	}
	return harness{handler: handler.Routes(), orchestrator: orchestrator, clock: clock}
}

func approvalDefinition(id string) workflow.WorkflowDefinition {
	return workflow.WorkflowDefinition{
		SchemaVersion: workflow.DefinitionSchemaV1,
		ID:            id,
		Nodes: []workflow.NodeDefinition{
			{ID: "start", Type: workflow.NodeTypeStart, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
			{ID: "approval", Type: workflow.NodeTypeHumanApproval, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{"prompt":"Approve deployment?","timeoutMs":60000}`)},
			{ID: "end", Type: workflow.NodeTypeEnd, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
		},
		Edges: []workflow.EdgeDefinition{
			{ID: "startToApproval", FromNode: "start", FromPort: "input", ToNode: "approval", ToPort: "request"},
			{ID: "approvalToEnd", FromNode: "approval", FromPort: "decision", ToNode: "end", ToPort: "result"},
		},
	}
}

func authenticatedTestRequest(request *http.Request, key string) {
	request.Header.Set("Authorization", "Bearer "+tenantAPublisherKey)
	organizationID := "tenant-a"
	if strings.Contains(key, "tenant-b") {
		organizationID = "tenant-b"
	}
	request.Header.Set("X-Fenix-Organization-Id", organizationID)
}

func requestJSON(t *testing.T, handler http.Handler, method, path, key, idempotencyKey string, body any) *httptest.ResponseRecorder {
	t.Helper()
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequestWithContext(context.Background(), method, path, bytes.NewReader(payload))
	request.Header.Set("Content-Type", "application/json")
	authenticatedTestRequest(request, key)
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func decodeResponse[T any](t *testing.T, response *httptest.ResponseRecorder) T {
	t.Helper()
	var value T
	if err := json.Unmarshal(response.Body.Bytes(), &value); err != nil {
		t.Fatalf("decode status=%d body=%q: %v", response.Code, response.Body.String(), err)
	}
	return value
}

func publish(t *testing.T, h harness, workflowID, key string) publishedPlan {
	t.Helper()
	return publishAs(t, h, workflowID, key, tenantAPublisherKey)
}

func publishAs(t *testing.T, h harness, workflowID, key, apiKey string) publishedPlan {
	t.Helper()
	return publishDefinitionAs(t, h, workflowID, key, apiKey, approvalDefinition(workflowID))
}

func publishDefinitionAs(t *testing.T, h harness, workflowID, key, apiKey string, definition workflow.WorkflowDefinition) publishedPlan {
	t.Helper()
	response := requestJSON(t, h.handler, http.MethodPost, "/v1/workflows/"+workflowID+"/publish", apiKey, key, definition)
	if response.Code != http.StatusCreated {
		t.Fatalf("publish status=%d body=%s", response.Code, response.Body.String())
	}
	return decodeResponse[publishedPlan](t, response)
}

func start(t *testing.T, h harness, plan publishedPlan, key string) runResponse {
	t.Helper()
	return startAs(t, h, plan, key, tenantAOperatorKey)
}

func startAs(t *testing.T, h harness, plan publishedPlan, key, apiKey string) runResponse {
	t.Helper()
	response := requestJSON(t, h.handler, http.MethodPost, "/v1/runs", apiKey, key, map[string]any{
		"planId": plan.ID, "planHash": plan.Hash, "input": map[string]any{"requestId": "request-42"},
	})
	if response.Code != http.StatusAccepted {
		t.Fatalf("start status=%d body=%s", response.Code, response.Body.String())
	}
	return decodeResponse[runResponse](t, response)
}

func advance(t *testing.T, h harness, tenantID workflowruntime.TenantID) workflowruntime.AdvanceResult {
	t.Helper()
	result, err := h.orchestrator.Advance(context.Background(), tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Worked {
		t.Fatal("worker did not claim an attempt")
	}
	return result
}

func readSSE(t *testing.T, handler http.Handler, path, key, lastEventID string, timeout time.Duration) (*httptest.ResponseRecorder, []sseEvent) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	request := httptest.NewRequestWithContext(ctx, http.MethodGet, path, nil)
	authenticatedTestRequest(request, key)
	if lastEventID != "" {
		request.Header.Set("Last-Event-ID", lastEventID)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response, parseSSE(t, response.Body.String())
}

func parseSSE(t *testing.T, payload string) []sseEvent {
	t.Helper()
	var result []sseEvent
	current := sseEvent{}
	data := ""
	scanner := bufio.NewScanner(strings.NewReader(payload))
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "id: "):
			id, err := strconv.ParseUint(strings.TrimPrefix(line, "id: "), 10, 64)
			if err != nil {
				t.Fatalf("invalid SSE id %q: %v", line, err)
			}
			current.ID = id
		case strings.HasPrefix(line, "event: "):
			current.Type = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			data = strings.TrimPrefix(line, "data: ")
		case line == "" && current.ID != 0:
			var envelope struct {
				Cursor uint64 `json:"cursor"`
				Type   string `json:"type"`
			}
			if err := json.Unmarshal([]byte(data), &envelope); err != nil {
				t.Fatalf("invalid SSE data %q: %v", data, err)
			}
			current.Cursor = envelope.Cursor
			if current.ID != envelope.Cursor || current.Type != envelope.Type {
				t.Fatalf("inconsistent SSE envelope: id=%d event=%q data=%s", current.ID, current.Type, data)
			}
			result = append(result, current)
			current, data = sseEvent{}, ""
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

// 验证发布、幂等启动、worker 推进、人工审批恢复、成功终态和 SSE cursor 形成完整 API 垂直切片。
func TestApprovalWorkflowLifecycleAndSSECursor(t *testing.T) {
	h := newHarness(t)
	plan := publish(t, h, "approval-flow", "publish-e2e-0001")
	publishReplay := requestJSON(t, h.handler, http.MethodPost, "/v1/workflows/approval-flow/publish", tenantAPublisherKey, "publish-e2e-0001", approvalDefinition("approval-flow"))
	if publishReplay.Code != http.StatusOK || decodeResponse[publishedPlan](t, publishReplay).ID != plan.ID {
		t.Fatalf("publish replay status=%d body=%s", publishReplay.Code, publishReplay.Body.String())
	}

	run := start(t, h, plan, "start-e2e-000001")
	startReplay := requestJSON(t, h.handler, http.MethodPost, "/v1/runs", tenantAOperatorKey, "start-e2e-000001", map[string]any{
		"planId": plan.ID, "planHash": plan.Hash, "input": map[string]any{"requestId": "request-42"},
	})
	if startReplay.Code != http.StatusOK || decodeResponse[runResponse](t, startReplay).ID != run.ID {
		t.Fatalf("start replay status=%d body=%s", startReplay.Code, startReplay.Body.String())
	}

	advance(t, h, "tenant-a")
	suspended := advance(t, h, "tenant-a")
	if suspended.Interrupt == nil {
		t.Fatalf("suspension did not create an interrupt: %+v", suspended)
	}

	interruptsResponse := requestJSON(t, h.handler, http.MethodGet, fmt.Sprintf("/v1/runs/%s/interrupts", run.ID), tenantAViewerKey, "", nil)
	if interruptsResponse.Code != http.StatusOK {
		t.Fatalf("list interrupts status=%d body=%s", interruptsResponse.Code, interruptsResponse.Body.String())
	}
	assertPrivateNoStore(t, interruptsResponse)
	interrupts := decodeResponse[[]interruptResponse](t, interruptsResponse)
	if len(interrupts) != 1 || interrupts[0].ID != suspended.Interrupt.ID || interrupts[0].RunID != run.ID || interrupts[0].NodeID != "approval" || interrupts[0].Kind != "approval" {
		t.Fatalf("interrupts=%+v", interrupts)
	}
	if interrupts[0].ResumeToken == "" {
		t.Fatal("authenticated organization interrupt listing omitted the approval token")
	}
	approverInterruptsResponse := requestJSON(t, h.handler, http.MethodGet, fmt.Sprintf("/v1/runs/%s/interrupts", run.ID), tenantAApproverKey, "", nil)
	assertPrivateNoStore(t, approverInterruptsResponse)
	approverInterrupts := decodeResponse[[]interruptResponse](t, approverInterruptsResponse)
	if approverInterruptsResponse.Code != http.StatusOK || len(approverInterrupts) != 1 || approverInterrupts[0].ResumeToken == "" {
		t.Fatalf("approver token delivery status=%d interrupts=%+v", approverInterruptsResponse.Code, approverInterrupts)
	}
	approvalToken := approverInterrupts[0].ResumeToken

	resolveBody := map[string]any{
		"resumeToken": approvalToken, "decision": "approved", "comment": "ship it", "data": map[string]any{"ticket": "OPS-42"},
	}
	resolve := requestJSON(t, h.handler, http.MethodPost, "/v1/interrupts/"+suspended.Interrupt.ID+"/resolve", tenantAApproverKey, "resolve-e2e-0001", resolveBody)
	if resolve.Code != http.StatusOK || decodeResponse[runResponse](t, resolve).Status != workflowruntime.RunPending {
		t.Fatalf("resolve status=%d body=%s", resolve.Code, resolve.Body.String())
	}
	// 模拟服务端已经提交但客户端丢失响应后，使用相同 key 和请求安全重试。
	replayedResolve := requestJSON(t, h.handler, http.MethodPost, "/v1/interrupts/"+suspended.Interrupt.ID+"/resolve", tenantAApproverKey, "resolve-e2e-0001", resolveBody)
	if replayedResolve.Code != http.StatusOK || replayedResolve.Body.String() != resolve.Body.String() {
		t.Fatalf("resolve replay status=%d body=%s first=%s", replayedResolve.Code, replayedResolve.Body.String(), resolve.Body.String())
	}
	conflictingResolve := requestJSON(t, h.handler, http.MethodPost, "/v1/interrupts/"+suspended.Interrupt.ID+"/resolve", tenantAApproverKey, "resolve-e2e-0001", map[string]any{
		"resumeToken": approvalToken, "decision": "rejected", "comment": "different request",
	})
	if conflictingResolve.Code != http.StatusConflict || !strings.Contains(conflictingResolve.Body.String(), "idempotency_conflict") {
		t.Fatalf("resolve conflict status=%d body=%s", conflictingResolve.Code, conflictingResolve.Body.String())
	}
	advance(t, h, "tenant-a")
	completed := advance(t, h, "tenant-a")
	if completed.Run == nil || completed.Run.Status != workflowruntime.RunSuccess {
		t.Fatalf("completed=%+v", completed)
	}

	runResponseRecorder := requestJSON(t, h.handler, http.MethodGet, "/v1/runs/"+run.ID, tenantAViewerKey, "", nil)
	if runResponseRecorder.Code != http.StatusOK || decodeResponse[runResponse](t, runResponseRecorder).Status != workflowruntime.RunSuccess {
		t.Fatalf("get run status=%d body=%s", runResponseRecorder.Code, runResponseRecorder.Body.String())
	}

	stream, events := readSSE(t, h.handler, "/v1/runs/"+run.ID+"/events", tenantAViewerKey, "", time.Second)
	if stream.Code != http.StatusOK || stream.Header().Get("Content-Type") != "text/event-stream" {
		t.Fatalf("SSE status=%d content-type=%q body=%s", stream.Code, stream.Header().Get("Content-Type"), stream.Body.String())
	}
	if len(events) < 2 || events[0].ID != 1 || events[len(events)-1].Type != "run.succeeded" {
		t.Fatalf("events=%+v", events)
	}
	for index := range events {
		if events[index].ID != uint64(index+1) {
			t.Fatalf("event cursors are not monotonic: %+v", events)
		}
	}

	resumeCursor := events[len(events)/2].ID
	_, resumed := readSSE(t, h.handler, "/v1/runs/"+run.ID+"/events", tenantAViewerKey, strconv.FormatUint(resumeCursor, 10), time.Second)
	if len(resumed) == 0 || resumed[0].ID <= resumeCursor || resumed[len(resumed)-1].Type != "run.succeeded" {
		t.Fatalf("resumed events=%+v after=%d", resumed, resumeCursor)
	}
	terminalCursor := events[len(events)-1].ID
	_, afterTerminal := readSSE(t, h.handler, fmt.Sprintf("/v1/runs/%s/events?afterCursor=%d", run.ID, terminalCursor), tenantAViewerKey, "", 30*time.Millisecond)
	if len(afterTerminal) != 0 {
		t.Fatalf("terminal cursor replayed old events: %+v", afterTerminal)
	}
}

func assertPrivateNoStore(t *testing.T, response *httptest.ResponseRecorder) {
	t.Helper()
	if response.Header().Get("Cache-Control") != "private, no-store" {
		t.Fatalf("Cache-Control=%q", response.Header().Get("Cache-Control"))
	}
	if response.Header().Get("Vary") != "X-Fenix-Organization-Id" {
		t.Fatalf("Vary=%q", response.Header().Get("Vary"))
	}
}

// 验证运行、事件、interrupt 和审批 token 始终按认证租户隔离，猜中 ID 也不能跨租户访问。
func TestTenantIsolationAcrossRunEventsAndInterrupt(t *testing.T) {
	h := newHarness(t)
	planA := publishAs(t, h, "isolated-flow", "publish-e2e-0002", tenantAPublisherKey)
	planB := publishAs(t, h, "isolated-flow", "publish-e2e-0002", tenantBPublisherKey)
	if planA.ID == planB.ID {
		t.Fatalf("tenant publications unexpectedly shared plan ID %q", planA.ID)
	}
	run := startAs(t, h, planA, "start-e2e-000002", tenantAOperatorKey)
	runB := startAs(t, h, planB, "start-e2e-000002", tenantBOperatorKey)
	if run.ID == runB.ID {
		t.Fatalf("tenant runs unexpectedly shared run ID %q", run.ID)
	}
	advance(t, h, "tenant-a")
	suspended := advance(t, h, "tenant-a")
	advance(t, h, "tenant-b")
	suspendedB := advance(t, h, "tenant-b")
	if suspended.Interrupt == nil {
		t.Fatal("expected tenant A pending interrupt")
	}
	if suspendedB.Interrupt == nil {
		t.Fatal("expected tenant B pending interrupt")
	}

	for _, testCase := range []struct {
		name   string
		path   string
		apiKey string
	}{
		{name: "tenant B cannot read tenant A run", path: "/v1/runs/" + run.ID, apiKey: tenantBViewerKey},
		{name: "tenant B cannot read tenant A events", path: "/v1/runs/" + run.ID + "/events", apiKey: tenantBViewerKey},
		{name: "tenant B cannot read tenant A interrupts", path: "/v1/runs/" + run.ID + "/interrupts", apiKey: tenantBViewerKey},
		{name: "tenant A cannot read tenant B run", path: "/v1/runs/" + runB.ID, apiKey: tenantAViewerKey},
		{name: "tenant A cannot read tenant B events", path: "/v1/runs/" + runB.ID + "/events", apiKey: tenantAViewerKey},
		{name: "tenant A cannot read tenant B interrupts", path: "/v1/runs/" + runB.ID + "/interrupts", apiKey: tenantAViewerKey},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			response := requestJSON(t, h.handler, http.MethodGet, testCase.path, testCase.apiKey, "", nil)
			if response.Code != http.StatusNotFound {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
		})
	}

	ownerTokenResponse := requestJSON(t, h.handler, http.MethodGet, "/v1/runs/"+run.ID+"/interrupts", tenantAApproverKey, "", nil)
	ownerToken := decodeResponse[[]interruptResponse](t, ownerTokenResponse)[0].ResumeToken
	crossTenantResolve := requestJSON(t, h.handler, http.MethodPost, "/v1/interrupts/"+suspended.Interrupt.ID+"/resolve", tenantBApproverKey, "resolve-e2e-0002", map[string]any{
		"resumeToken": ownerToken, "decision": "approved",
	})
	if crossTenantResolve.Code < 400 || crossTenantResolve.Code >= 500 {
		t.Fatalf("cross-tenant resolve status=%d body=%s", crossTenantResolve.Code, crossTenantResolve.Body.String())
	}

	ownerInterrupts := requestJSON(t, h.handler, http.MethodGet, "/v1/runs/"+run.ID+"/interrupts", tenantAViewerKey, "", nil)
	if ownerInterrupts.Code != http.StatusOK || len(decodeResponse[[]interruptResponse](t, ownerInterrupts)) != 1 {
		t.Fatalf("owner interrupt was changed by cross-tenant request: status=%d body=%s", ownerInterrupts.Code, ownerInterrupts.Body.String())
	}
	tenantBInterrupts := requestJSON(t, h.handler, http.MethodGet, "/v1/runs/"+runB.ID+"/interrupts", tenantBViewerKey, "", nil)
	if tenantBInterrupts.Code != http.StatusOK || len(decodeResponse[[]interruptResponse](t, tenantBInterrupts)) != 1 {
		t.Fatalf("tenant B interrupt status=%d body=%s", tenantBInterrupts.Code, tenantBInterrupts.Body.String())
	}
	_, tenantAEvents := readSSE(t, h.handler, "/v1/runs/"+run.ID+"/events", tenantAViewerKey, "", 30*time.Millisecond)
	_, tenantBEvents := readSSE(t, h.handler, "/v1/runs/"+runB.ID+"/events", tenantBViewerKey, "", 30*time.Millisecond)
	if len(tenantAEvents) == 0 || len(tenantBEvents) == 0 || tenantAEvents[0].ID != 1 || tenantBEvents[0].ID != 1 {
		t.Fatalf("tenant A events=%+v tenant B events=%+v", tenantAEvents, tenantBEvents)
	}
}

// 验证取消 API 持久化 intent、支持重复请求，并让尚未执行的运行直接进入可观察的 CANCELLED 终态。
func TestCancelRunIsIdempotentAndTerminal(t *testing.T) {
	h := newHarness(t)
	run := start(t, h, publish(t, h, "cancel-flow", "publish-e2e-0003"), "start-e2e-000003")
	path := "/v1/runs/" + run.ID + "/cancel"
	first := requestJSON(t, h.handler, http.MethodPost, path, tenantAOperatorKey, "cancel-e2e-0001", map[string]any{})
	second := requestJSON(t, h.handler, http.MethodPost, path, tenantAOperatorKey, "cancel-e2e-0001", map[string]any{})
	if first.Code != http.StatusAccepted || second.Code != http.StatusAccepted {
		t.Fatalf("first=%d %s second=%d %s", first.Code, first.Body.String(), second.Code, second.Body.String())
	}
	if decodeResponse[runResponse](t, first).Status != workflowruntime.RunCancelled || decodeResponse[runResponse](t, second).Status != workflowruntime.RunCancelled {
		t.Fatalf("first=%s second=%s", first.Body.String(), second.Body.String())
	}
	_, events := readSSE(t, h.handler, "/v1/runs/"+run.ID+"/events", tenantAViewerKey, "", time.Second)
	if len(events) == 0 || events[len(events)-1].Type != "run.cancelled" {
		t.Fatalf("events=%+v", events)
	}
}

// 验证 Supaflow 不解释 Fenix 用户角色，组织内写操作可达且猜测的资源仍返回 404。
func TestFenixAuthorizationBoundaryAllowsOrganizationMutations(t *testing.T) {
	h := newHarness(t)
	publishResponse := requestJSON(t, h.handler, http.MethodPost, "/v1/workflows/forbidden-flow/publish", tenantAViewerKey, "publish-e2e-0004", approvalDefinition("forbidden-flow"))
	if publishResponse.Code != http.StatusCreated {
		t.Fatalf("publish status=%d body=%s", publishResponse.Code, publishResponse.Body.String())
	}
	cancelResponse := requestJSON(t, h.handler, http.MethodPost, "/v1/runs/guessed-run/cancel", tenantAViewerKey, "cancel-e2e-0004", map[string]any{})
	if cancelResponse.Code != http.StatusNotFound {
		t.Fatalf("cancel status=%d body=%s", cancelResponse.Code, cancelResponse.Body.String())
	}
}
