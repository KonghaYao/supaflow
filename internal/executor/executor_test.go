package executor

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/pazhou/supaflow/internal/platform"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/workflow"
)

// 验证执行期按真实 NewAttempt 序列化大小再次限制近 1MiB 输出的 fan-out 放大。
func TestSuccessorsRejectAggregateSerializedPayload(t *testing.T) {
	definition := workflow.WorkflowDefinition{SchemaVersion: workflow.DefinitionSchemaV1, ID: "runtime_payload_limit",
		Nodes: []workflow.NodeDefinition{{ID: "start", Type: workflow.NodeTypeStart, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)}},
	}
	for index := 0; index < 16; index++ {
		id := fmt.Sprintf("end_%02d", index)
		definition.Nodes = append(definition.Nodes, workflow.NodeDefinition{ID: id, Type: workflow.NodeTypeEnd, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)})
		definition.Edges = append(definition.Edges, workflow.EdgeDefinition{ID: "to_" + id, FromNode: "start", FromPort: "input", ToNode: id, ToPort: "result"})
	}
	plan, err := workflow.Compile(definition, workflow.BuiltinRegistry(), workflow.CompilePolicy{AllowedCapabilities: workflow.CapabilitySet{Idempotent: true}})
	if err != nil {
		t.Fatal(err)
	}
	payload := json.RawMessage(`"` + strings.Repeat("a", (1<<20)-2) + `"`)
	_, err = successors(plan, "start", payload)
	assertExecutionError(t, err, "successor_payload_limit_exceeded", false)
}

// 验证四节点切片按不可变 plan 逐步执行，并在审批恢复后完成。
func TestPlanExecutorStartHTTPApprovalEnd(t *testing.T) {
	host := "api.example"
	dial := scriptedDialer(func(request *http.Request, connection net.Conn) {
		if request.Header.Get("Idempotency-Key") == "" {
			t.Error("HTTP side effect has no derived idempotency key")
		}
		_, _ = io.WriteString(connection, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nSet-Cookie: session=must-not-leak\r\nX-Visible: yes\r\nContent-Length: 19\r\n\r\n{\"message\":\"ready\"}")
	})
	published := publishPlan(t, workflowDefinition("http://"+host+"/orders", ""))
	executor, err := NewPlanExecutor(published.catalog, Config{
		AllowedHosts: []string{host},
		DNSResolver:  fixedResolver{IP: net.ParseIP("93.184.216.34")}, DialContext: dial,
	})
	if err != nil {
		t.Fatal(err)
	}
	planRef := workflowruntime.PlanRef{ID: published.plan.ID, Hash: published.plan.Hash}
	request := workflowruntime.ExecutionRequest{
		OrganizationID: "organization_a", RunID: "run_1", Plan: planRef, NodeID: "begin",
		AttemptID: "attempt_1", Attempt: 1, Executor: "builtin.start", Input: json.RawMessage(`{"order":42}`), FencingToken: 1,
	}

	start, err := executor.Execute(context.Background(), request)
	if err != nil || len(start.Next) != 1 || start.Next[0].NodeID != "request" || string(start.Next[0].Input) != `{"order":42}` {
		t.Fatalf("start = %+v, err = %v", start, err)
	}
	assertOutputMetadata(t, start.Output)

	request.NodeID, request.AttemptID, request.Executor, request.Input = start.Next[0].NodeID, "attempt_2", start.Next[0].Executor, start.Next[0].Input
	httpResult, err := executor.Execute(context.Background(), request)
	if err != nil || len(httpResult.Next) != 1 || httpResult.Next[0].NodeID != "approval" {
		t.Fatalf("HTTP = %+v, err = %v", httpResult, err)
	}
	assertOutputMetadata(t, httpResult.Output)
	var response struct {
		Headers map[string][]string `json:"headers"`
		Body    json.RawMessage     `json:"body"`
	}
	if err := json.Unmarshal(httpResult.Output.Inline, &response); err != nil {
		t.Fatal(err)
	}
	if response.Headers["Set-Cookie"] != nil || response.Headers["Authorization"] != nil || response.Headers["X-Visible"][0] != "yes" {
		t.Fatalf("unsafe response headers: %#v", response.Headers)
	}

	request.NodeID, request.AttemptID, request.Executor, request.Input = httpResult.Next[0].NodeID, "attempt_3", httpResult.Next[0].Executor, httpResult.Next[0].Input
	suspended, err := executor.Execute(context.Background(), request)
	if err != nil || suspended.Suspend == nil || suspended.Suspend.Kind != "approval" || suspended.Suspend.ExpiresIn != time.Hour {
		t.Fatalf("approval suspend = %+v, err = %v", suspended, err)
	}
	request.ResumeData = json.RawMessage(`{"approved":true,"comment":"ship","decidedBy":"user_1"}`)
	resumed, err := executor.Execute(context.Background(), request)
	if err != nil || len(resumed.Next) != 1 || resumed.Next[0].NodeID != "finish" {
		t.Fatalf("approval resume = %+v, err = %v", resumed, err)
	}
	assertOutputMetadata(t, resumed.Output)

	request.NodeID, request.AttemptID, request.Executor, request.Input = resumed.Next[0].NodeID, "attempt_4", resumed.Next[0].Executor, resumed.Next[0].Input
	request.ResumeData = nil
	finished, err := executor.Execute(context.Background(), request)
	if err != nil || !finished.Final || len(finished.Next) != 0 {
		t.Fatalf("end = %+v, err = %v", finished, err)
	}
	assertOutputMetadata(t, finished.Output)
}

// 验证 credential 节点只能通过 HTTPS 发送密钥，显式 allowlist 也不能放宽传输安全。
func TestPlanExecutorRejectsCredentialOverPlaintextHTTP(t *testing.T) {
	host := "api.example"
	published := publishPlan(t, workflowDefinition("http://"+host+"/orders", "credential_1"))
	executor, err := NewPlanExecutor(published.catalog, Config{
		AllowedHosts: []string{host}, CredentialResolver: staticCredentials{},
		DNSResolver: fixedResolver{IP: net.ParseIP("93.184.216.34")},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = executor.Execute(context.Background(), workflowruntime.ExecutionRequest{
		OrganizationID: "organization_a", RunID: "run_1", Plan: workflowruntime.PlanRef{ID: published.plan.ID, Hash: published.plan.Hash},
		NodeID: "request", AttemptID: "attempt_1", Attempt: 1, Executor: "http.request", Input: json.RawMessage(`{}`), FencingToken: 1,
	})
	assertExecutionError(t, err, "plaintext_credential", false)
}

// 验证 credential header 存在时禁止跟随 redirect，避免跨 origin 复制或 HTTPS 降级。
func TestHTTPExecutorRejectsCredentialedRedirect(t *testing.T) {
	host := "api.example"
	executor, err := newHTTPExecutor(Config{
		AllowedHosts: []string{host}, DNSResolver: fixedResolver{IP: net.ParseIP("93.184.216.34")},
		RequestBodyLimit: 1 << 20, ResponseBodyLimit: 1 << 20, DialTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	original, _ := http.NewRequest(http.MethodGet, "https://"+host+"/start", nil)
	original.Header.Set("Authorization", "Bearer must-not-follow")
	redirect, _ := http.NewRequest(http.MethodGet, "https://"+host+"/next", nil)
	if err = executor.client.CheckRedirect(redirect, []*http.Request{original}); err == nil {
		t.Fatal("credentialed redirect was allowed")
	}
}

// 验证审批拒绝属于不可重试的业务失败且不会生成后继 attempt。
func TestPlanExecutorApprovalRejects(t *testing.T) {
	published := publishPlan(t, workflowDefinition("https://example.com", ""))
	executor, err := NewPlanExecutor(published.catalog, Config{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = executor.Execute(context.Background(), workflowruntime.ExecutionRequest{
		OrganizationID: "organization_a", RunID: "run_1", Plan: workflowruntime.PlanRef{ID: published.plan.ID, Hash: published.plan.Hash},
		NodeID: "approval", AttemptID: "attempt_1", Attempt: 1, Executor: "builtin.human-approval",
		Input: json.RawMessage(`{"message":"ready"}`), ResumeData: json.RawMessage(`{"approved":false,"decidedBy":"user_1"}`), FencingToken: 1,
	})
	assertExecutionError(t, err, "approval_rejected", false)
}

// 验证 attempt 只能使用 plan 固定的 executor binding，未知执行器默认拒绝。
func TestPlanExecutorRejectsBindingMismatch(t *testing.T) {
	published := publishPlan(t, workflowDefinition("https://example.com", ""))
	executor, err := NewPlanExecutor(published.catalog, Config{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = executor.Execute(context.Background(), workflowruntime.ExecutionRequest{
		OrganizationID: "organization_a", RunID: "run_1", Plan: workflowruntime.PlanRef{ID: published.plan.ID, Hash: published.plan.Hash},
		NodeID: "begin", AttemptID: "attempt_1", Executor: "untrusted.executor", Input: json.RawMessage(`null`), FencingToken: 1,
	})
	assertExecutionError(t, err, "executor_binding_mismatch", false)
	_, err = executor.Execute(context.Background(), workflowruntime.ExecutionRequest{
		OrganizationID: "organization_a", RunID: "run_2", Plan: workflowruntime.PlanRef{ID: published.plan.ID, Hash: published.plan.Hash},
		NodeID: "request", AttemptID: "attempt_2", Executor: "shadow-v1:http.request", Input: json.RawMessage(`null`), FencingToken: 1,
	})
	assertExecutionError(t, err, "executor_binding_mismatch", false)
}

// 验证 shadow executor 模式跨后继 attempt 持续存在，且副作用节点不会解析凭据或发起网络调用。
func TestPlanExecutorShadowModeStubsSideEffectsDurably(t *testing.T) {
	published := publishPlan(t, workflowDefinition("https://api.example/orders", "credential_1"))
	dialCalls, credentialCalls := 0, 0
	executor, err := NewPlanExecutor(published.catalog, Config{
		AllowedHosts: []string{"api.example"},
		DNSResolver:  fixedResolver{IP: net.ParseIP("93.184.216.34")},
		DialContext: func(context.Context, string, string) (net.Conn, error) {
			dialCalls++
			return nil, errors.New("network must not be reached")
		},
		CredentialAuthorizer: credentialAuthorizerFunc(func(context.Context, workflowruntime.ExecutionRequest, workflow.PlanNode) error {
			credentialCalls++
			return nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	request := workflowruntime.ExecutionRequest{
		OrganizationID: "organization_a", RunID: "run_shadow", Plan: workflowruntime.PlanRef{ID: published.plan.ID, Hash: published.plan.Hash},
		NodeID: "begin", AttemptID: "attempt_1", Attempt: 1, Executor: "shadow-v1:builtin.start",
		Input: json.RawMessage(`{"secret":"must-not-copy"}`), FencingToken: 1,
	}
	started, err := executor.Execute(context.Background(), request)
	if err != nil || len(started.Next) != 1 || started.Next[0].Executor != "shadow-v1:builtin.shadow-noop.v1" {
		t.Fatalf("shadow start = %+v, err = %v", started, err)
	}
	request.NodeID, request.AttemptID, request.Executor, request.Input = started.Next[0].NodeID, "attempt_2", started.Next[0].Executor, started.Next[0].Input
	stubbed, err := executor.Execute(context.Background(), request)
	if err != nil || len(stubbed.Next) != 1 || stubbed.Next[0].Executor != "shadow-v1:builtin.human-approval" {
		t.Fatalf("shadow HTTP = %+v, err = %v", stubbed, err)
	}
	if dialCalls != 0 || credentialCalls != 0 {
		t.Fatalf("shadow side effects dial=%d credential=%d", dialCalls, credentialCalls)
	}
	if bytes.Contains(stubbed.Output.Inline, []byte("must-not-copy")) || !bytes.Contains(stubbed.Output.Inline, []byte(`"shadowStubbed":true`)) {
		t.Fatalf("unsafe or missing stub evidence: %s", stubbed.Output.Inline)
	}
}

type credentialAuthorizerFunc func(context.Context, workflowruntime.ExecutionRequest, workflow.PlanNode) error

func (f credentialAuthorizerFunc) AuthorizeCredentials(ctx context.Context, request workflowruntime.ExecutionRequest, node workflow.PlanNode) error {
	return f(ctx, request, node)
}

// 验证凭据授权只发生在 immutable plan/node/executor 校验后，失败时外部请求不会启动。
func TestPlanExecutorAuthorizesPinnedNodeCredentialsBeforeExecution(t *testing.T) {
	published := publishPlan(t, workflowDefinition("https://api.example/orders", "credential_1"))
	called := false
	authorizer := credentialAuthorizerFunc(func(_ context.Context, request workflowruntime.ExecutionRequest, node workflow.PlanNode) error {
		called = true
		if request.AttemptID != "attempt_1" || request.FencingToken != 7 || node.ID != "request" || node.Executor.Kind != "http.request" {
			t.Fatalf("authorization scope request=%+v node=%+v", request, node)
		}
		return errors.New("grant rejected")
	})
	executor, err := NewPlanExecutor(published.catalog, Config{CredentialAuthorizer: authorizer})
	if err != nil {
		t.Fatal(err)
	}
	_, err = executor.Execute(context.Background(), workflowruntime.ExecutionRequest{
		OrganizationID: "organization_a", RunID: "run_1", Plan: workflowruntime.PlanRef{ID: published.plan.ID, Hash: published.plan.Hash},
		NodeID: "request", AttemptID: "attempt_1", Attempt: 1, Executor: "http.request", Input: json.RawMessage(`{}`), FencingToken: 7,
	})
	if !called {
		t.Fatal("credential authorizer was not called")
	}
	assertExecutionError(t, err, "credential_grant_failed", false)
}

// 验证线性后继在节点边界解析 literal 与 direct source binding。
func TestPlanExecutorResolvesSuccessorBindings(t *testing.T) {
	definition := workflowDefinition("https://example.com", "")
	definition.Nodes[1].Inputs = map[string]workflow.InputBinding{
		"body": {Type: workflow.ValueTypeObject, Literal: json.RawMessage(`{"fixed":true}`)},
	}
	published := publishPlan(t, definition)
	executor, err := NewPlanExecutor(published.catalog, Config{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.Execute(context.Background(), workflowruntime.ExecutionRequest{
		OrganizationID: "organization_a", RunID: "run_1", Plan: workflowruntime.PlanRef{ID: published.plan.ID, Hash: published.plan.Hash},
		NodeID: "begin", AttemptID: "attempt_1", Executor: "builtin.start", Input: json.RawMessage(`{"original":true}`), FencingToken: 1,
	})
	if err != nil || len(result.Next) != 1 || string(result.Next[0].Input) != `{"fixed":true}` {
		t.Fatalf("literal successor = %+v, err = %v", result, err)
	}

	definition = workflowDefinition("https://example.com", "")
	definition.Nodes[1].Inputs = map[string]workflow.InputBinding{
		"body": {Type: workflow.ValueTypeAny, Source: &workflow.OutputReference{NodeID: "begin", Port: "input"}},
	}
	published = publishPlan(t, definition)
	executor, err = NewPlanExecutor(published.catalog, Config{})
	if err != nil {
		t.Fatal(err)
	}
	result, err = executor.Execute(context.Background(), workflowruntime.ExecutionRequest{
		OrganizationID: "organization_a", RunID: "run_2", Plan: workflowruntime.PlanRef{ID: published.plan.ID, Hash: published.plan.Hash},
		NodeID: "begin", AttemptID: "attempt_2", Executor: "builtin.start", Input: json.RawMessage(`{"original":true}`), FencingToken: 1,
	})
	if err != nil || len(result.Next) != 1 || string(result.Next[0].Input) != `{"original":true}` {
		t.Fatalf("source successor = %+v, err = %v", result, err)
	}
}

// 验证执行器为 fan-out 和 fan-in 产生带不可变 edge barrier 的 durable successor 描述。
func TestPlanExecutorDescribesNonLinearPlans(t *testing.T) {
	definitions := []workflow.WorkflowDefinition{{
		SchemaVersion: workflow.DefinitionSchemaV1, ID: "branched_flow",
		Nodes: []workflow.NodeDefinition{
			{ID: "begin", Type: workflow.NodeTypeStart, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
			{ID: "finish_a", Type: workflow.NodeTypeEnd, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
			{ID: "finish_b", Type: workflow.NodeTypeEnd, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
		},
		Edges: []workflow.EdgeDefinition{
			{ID: "to_a", FromNode: "begin", FromPort: "input", ToNode: "finish_a", ToPort: "result"},
			{ID: "to_b", FromNode: "begin", FromPort: "input", ToNode: "finish_b", ToPort: "result"},
		},
	}, {
		SchemaVersion: workflow.DefinitionSchemaV1, ID: "join_flow",
		Nodes: []workflow.NodeDefinition{
			{ID: "begin", Type: workflow.NodeTypeStart, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
			{ID: "left", Type: workflow.NodeTypeHTTP, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{"method":"GET","url":"https://left.example"}`)},
			{ID: "right", Type: workflow.NodeTypeHTTP, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{"method":"GET","url":"https://right.example"}`)},
			{ID: "finish", Type: workflow.NodeTypeEnd, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
		},
		Edges: []workflow.EdgeDefinition{
			{ID: "to_left", FromNode: "begin", FromPort: "input", ToNode: "left", ToPort: "body"},
			{ID: "to_right", FromNode: "begin", FromPort: "input", ToNode: "right", ToPort: "body"},
			{ID: "left_to_end", FromNode: "left", FromPort: "response", ToNode: "finish", ToPort: "left"},
			{ID: "right_to_end", FromNode: "right", FromPort: "response", ToNode: "finish", ToPort: "right"},
		},
	}}
	for _, definition := range definitions {
		t.Run(definition.ID, func(t *testing.T) {
			published := publishPlan(t, definition)
			executor, err := NewPlanExecutor(published.catalog, Config{})
			if err != nil {
				t.Fatal(err)
			}
			result, err := executor.Execute(context.Background(), workflowruntime.ExecutionRequest{
				OrganizationID: "organization_a", RunID: "run_1", Plan: workflowruntime.PlanRef{ID: published.plan.ID, Hash: published.plan.Hash},
				NodeID: "begin", AttemptID: "attempt_1", Executor: "builtin.start", Input: json.RawMessage(`null`), FencingToken: 1,
			})
			if err != nil || len(result.Next) != 2 {
				t.Fatalf("fan-out result = %+v, err = %v", result, err)
			}
			for _, next := range result.Next {
				if next.EdgeID == "" || len(next.RequiredEdges) == 0 {
					t.Fatalf("successor lacks durable join metadata: %+v", next)
				}
			}
		})
	}
}

type publishedFixture struct {
	catalog *platform.MemoryCatalog
	plan    platform.PublishedPlan
}

func publishPlan(t *testing.T, definition workflow.WorkflowDefinition) publishedFixture {
	t.Helper()
	catalog := platform.NewMemoryCatalog(workflow.BuiltinRegistry(), workflow.CompilePolicy{
		AllowedCapabilities: workflow.CapabilitySet{Streaming: true, Cancellable: true, Interruptible: true, Idempotent: true},
		AllowedPermissions:  workflow.PermissionSet{Network: true, Credentials: true},
	})
	published, _, err := catalog.Publish(context.Background(), "organization_a", definition.ID, definition, "publication_1")
	if err != nil {
		t.Fatal(err)
	}
	return publishedFixture{catalog: catalog, plan: published}
}

func workflowDefinition(target, credentialRef string) workflow.WorkflowDefinition {
	httpConfig := map[string]any{"method": "GET", "url": target, "headers": map[string]string{"X-Request": "visible"}}
	if credentialRef != "" {
		httpConfig["credentialRef"] = credentialRef
	}
	encoded, _ := json.Marshal(httpConfig)
	return workflow.WorkflowDefinition{
		SchemaVersion: workflow.DefinitionSchemaV1, ID: "order_review",
		Nodes: []workflow.NodeDefinition{
			{ID: "begin", Type: workflow.NodeTypeStart, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
			{ID: "request", Type: workflow.NodeTypeHTTP, Version: workflow.BuiltinNodeVersion, Config: encoded},
			{ID: "approval", Type: workflow.NodeTypeHumanApproval, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{"prompt":"Approve response?","timeoutMs":3600000}`)},
			{ID: "finish", Type: workflow.NodeTypeEnd, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
		},
		Edges: []workflow.EdgeDefinition{
			{ID: "begin_to_request", FromNode: "begin", FromPort: "input", ToNode: "request", ToPort: "body"},
			{ID: "request_to_approval", FromNode: "request", FromPort: "response", ToNode: "approval", ToPort: "request"},
			{ID: "approval_to_finish", FromNode: "approval", FromPort: "decision", ToNode: "finish", ToPort: "result"},
		},
	}
}

type staticCredentials struct{}

func (staticCredentials) ResolveHTTPHeaders(context.Context, AttemptScope, string) (http.Header, error) {
	return http.Header{"Authorization": []string{"Bearer injected"}}, nil
}

func assertOutputMetadata(t *testing.T, output *workflowruntime.Output) {
	t.Helper()
	if output == nil || output.ContentType != "application/json" || output.Size != int64(len(output.Inline)) || len(output.SHA256) != 64 {
		t.Fatalf("invalid output metadata: %+v", output)
	}
}

func assertExecutionError(t *testing.T, err error, code string, retryable bool) {
	t.Helper()
	var executionErr *workflowruntime.ExecutionError
	if !errors.As(err, &executionErr) || executionErr.Code != code || executionErr.Retryable != retryable {
		t.Fatalf("error = %#v, want code=%s retryable=%v", err, code, retryable)
	}
}

type fixedResolver struct{ IP net.IP }

func (r fixedResolver) LookupIPAddr(context.Context, string) ([]net.IPAddr, error) {
	return []net.IPAddr{{IP: append(net.IP(nil), r.IP...)}}, nil
}

func scriptedDialer(serve func(*http.Request, net.Conn)) func(context.Context, string, string) (net.Conn, error) {
	return func(_ context.Context, _, _ string) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			request, err := http.ReadRequest(bufio.NewReader(server))
			if err != nil {
				return
			}
			defer request.Body.Close()
			serve(request, server)
		}()
		return client, nil
	}
}
