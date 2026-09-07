package executor

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/pazhou/supaflow/internal/platform"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/workflow"
)

// 验证 Condition、Switch、Transform 和 bounded Loop 产生确定性的控制流与快照。
func TestControlNodesHaveExecutableSemantics(t *testing.T) {
	tests := []struct {
		name, nodeType, config, input, snapshot, wantPort, wantOutput string
	}{
		{"condition", workflow.NodeTypeCondition, `{"path":"$.amount","operator":"gte","value":10}`, `{"amount":12}`, "", "true", `{"amount":12}`},
		{"switch", workflow.NodeTypeSwitch, `{"path":"$.kind","cases":{"vip":"priority"}}`, `{"kind":"vip"}`, "", "priority", `{"kind":"vip"}`},
		{"transform", workflow.NodeTypeTransform, `{"select":{"id":"$.order.id"}}`, `{"order":{"id":42}}`, "", "output", `{"id":42}`},
		{"loop-body", workflow.NodeTypeLoop, `{"maxIterations":2}`, `{"item":1}`, `{"iteration":0}`, "body", `{"item":1}`},
		{"loop-done", workflow.NodeTypeLoop, `{"maxIterations":2}`, `{"item":1}`, `{"iteration":2}`, "done", `{"item":1}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			published := publishNodePlan(t, test.nodeType, test.config, []string{test.wantPort})
			executor, _ := NewPlanExecutor(published.catalog, Config{})
			result, err := executor.Execute(context.Background(), nodeRequest(published, test.input, test.snapshot))
			if err != nil || len(result.Next) != 1 || result.Next[0].SourcePort != test.wantPort || string(result.Output.Inline) != test.wantOutput {
				t.Fatalf("result = %+v, err = %v", result, err)
			}
			if test.nodeType == workflow.NodeTypeLoop && result.Snapshot == nil {
				t.Fatal("loop did not persist bounded iteration state")
			}
		})
	}
}

// 验证 Condition 的互斥出口汇合时只占一个 join barrier 槽，不会等待永远不活跃的分支。
func TestConditionAlternativeMergeDoesNotDeadlockJoin(t *testing.T) {
	definition := workflow.WorkflowDefinition{
		SchemaVersion: workflow.DefinitionSchemaV1, ID: "choice_merge",
		Nodes: []workflow.NodeDefinition{
			{ID: "start", Type: workflow.NodeTypeStart, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
			{ID: "choice", Type: workflow.NodeTypeCondition, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{"path":"$.ok","operator":"eq","value":true}`)},
			{ID: "end", Type: workflow.NodeTypeEnd, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
		},
		Edges: []workflow.EdgeDefinition{
			{ID: "enter", FromNode: "start", FromPort: "input", ToNode: "choice", ToPort: "input"},
			{ID: "yes", FromNode: "choice", FromPort: "true", ToNode: "end", ToPort: "result"},
			{ID: "no", FromNode: "choice", FromPort: "false", ToNode: "end", ToPort: "result"},
		},
	}
	catalog := platform.NewMemoryCatalog(workflow.BuiltinRegistry(), workflow.CompilePolicy{
		AllowedCapabilities: workflow.CapabilitySet{Cancellable: true, Interruptible: true, Idempotent: true},
		AllowedPermissions:  workflow.PermissionSet{Network: true, Credentials: true, Filesystem: true, Compute: true},
	})
	published, _, err := catalog.Publish(context.Background(), "organization_a", "choice_merge", definition, "publish")
	if err != nil {
		t.Fatal(err)
	}
	executor, _ := NewPlanExecutor(catalog, Config{})
	result, err := executor.Execute(context.Background(), workflowruntime.ExecutionRequest{
		OrganizationID: "organization_a", RunID: "run", Plan: workflowruntime.PlanRef{ID: published.ID, Hash: published.Hash},
		NodeID: "choice", AttemptID: "attempt", Attempt: 1, Executor: "builtin.condition",
		Input: json.RawMessage(`{"ok":true}`), FencingToken: 1,
	})
	if err != nil || len(result.Next) != 1 || len(result.Next[0].RequiredEdges) != 1 || result.Next[0].EdgeID != result.Next[0].RequiredEdges[0] {
		t.Fatalf("result = %+v, err = %v", result, err)
	}
}

// 验证 Delay、Wait 与 Webhook Resume 首次 suspend，恢复后才产生后继。
func TestSuspensionNodesResumeDurably(t *testing.T) {
	tests := []struct{ nodeType, config, kind string }{
		{workflow.NodeTypeDelay, `{"durationMs":1000}`, "wait"},
		{workflow.NodeTypeWait, `{"timeoutMs":2000}`, "wait"},
		{workflow.NodeTypeWebhookResume, `{"timeoutMs":2000}`, "webhook"},
	}
	for _, test := range tests {
		t.Run(test.nodeType, func(t *testing.T) {
			published := publishNodePlan(t, test.nodeType, test.config, []string{"output"})
			executor, _ := NewPlanExecutor(published.catalog, Config{})
			request := nodeRequest(published, `{"request":1}`, "")
			first, err := executor.Execute(context.Background(), request)
			if err != nil || first.Suspend == nil || first.Suspend.Kind != test.kind {
				t.Fatalf("first = %+v, err = %v", first, err)
			}
			if (test.nodeType == workflow.NodeTypeDelay || test.nodeType == workflow.NodeTypeWait) && !first.Suspend.AutoResume {
				t.Fatalf("%s must durably auto-resume at its deadline", test.nodeType)
			}
			if test.nodeType == workflow.NodeTypeWebhookResume && first.Suspend.AutoResume {
				t.Fatal("webhook resume must wait for an authenticated callback")
			}
			request.ResumeData = json.RawMessage(`{"resumed":true}`)
			resumed, err := executor.Execute(context.Background(), request)
			if err != nil || len(resumed.Next) != 1 || string(resumed.Output.Inline) != `{"resumed":true}` {
				t.Fatalf("resumed = %+v, err = %v", resumed, err)
			}
		})
	}
}

// 验证 LLM、Agent、Tool 都调用注入 adapter，缺失 adapter 时不会伪造成功。
func TestEinoNodePortsInvokeInjectedAdapters(t *testing.T) {
	model := &fakeModel{}
	agent := &fakeInvocation{output: json.RawMessage(`{"agent":true}`)}
	tool := &fakeInvocation{output: json.RawMessage(`{"tool":true}`)}
	tests := []struct {
		nodeType, config, want string
		cfg                    Config
	}{
		{workflow.NodeTypeLLM, `{"name":"chat"}`, `{"model":true}`, Config{Models: model}},
		{workflow.NodeTypeAgent, `{"name":"agent"}`, `{"agent":true}`, Config{Agents: agent}},
		{workflow.NodeTypeTool, `{"name":"tool"}`, `{"tool":true}`, Config{Tools: tool}},
	}
	for _, test := range tests {
		t.Run(test.nodeType, func(t *testing.T) {
			published := publishNodePlan(t, test.nodeType, test.config, []string{"output"})
			executor, _ := NewPlanExecutor(published.catalog, test.cfg)
			result, err := executor.Execute(context.Background(), nodeRequest(published, `{"prompt":"hello"}`, ""))
			if err != nil || string(result.Output.Inline) != test.want {
				t.Fatalf("result = %+v, err = %v", result, err)
			}
		})
	}
}

// 验证未在 immutable node policy 声明为 retry-safe 的远端副作用默认禁止自动重试。
func TestExternalAdapterRetrySafetyFailsClosed(t *testing.T) {
	published := publishNodePlan(t, workflow.NodeTypeLLM, `{"name":"chat"}`, []string{"output"})
	executor, err := NewPlanExecutor(published.catalog, Config{Models: failingModel{}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = executor.Execute(context.Background(), nodeRequest(published, `{"prompt":"hello"}`, ""))
	assertExecutionError(t, err, "adapter_failed", false)
}

// 验证失联租约接管在任何外部 adapter 调用前拒绝不安全节点，而明确安全的子工作流可继续。
func TestLeaseTakeoverFailsClosedBeforeUnsafeAdapter(t *testing.T) {
	tool := &fakeInvocation{output: json.RawMessage(`{"tool":true}`)}
	published := publishNodePlan(t, workflow.NodeTypeTool, `{"name":"tool"}`, []string{"output"})
	executor, _ := NewPlanExecutor(published.catalog, Config{Tools: tool})
	request := nodeRequest(published, `{"input":1}`, "")
	request.Takeover = true
	_, err := executor.Execute(context.Background(), request)
	assertExecutionError(t, err, "unsafe_lease_takeover", false)
	if tool.calls != 0 {
		t.Fatalf("unsafe adapter calls = %d, want 0", tool.calls)
	}

	subflow := &fakeSubworkflow{}
	published = publishNodePlan(t, workflow.NodeTypeSubworkflow, `{"workflowId":"child","planId":"plan-child","planHash":"hash-child"}`, []string{"output"})
	executor, _ = NewPlanExecutor(published.catalog, Config{Subworkflows: subflow})
	request = nodeRequest(published, `{"input":1}`, "")
	request.Takeover = true
	if _, err = executor.Execute(context.Background(), request); err != nil {
		t.Fatalf("safe takeover error = %v", err)
	}
	if subflow.request.ParentExecution != request.ExecutionID || subflow.request.PlanHash != "hash-child" {
		t.Fatalf("safe takeover scope = %+v", subflow.request)
	}
}

// 验证 Subworkflow 强制注入父 lineage，Shell/Python 强制走 fenced isolation port。
func TestDelegatedNodesPreserveTrustedScope(t *testing.T) {
	subflow := &fakeSubworkflow{}
	published := publishNodePlan(t, workflow.NodeTypeSubworkflow, `{"workflowId":"child","planId":"plan-child","planHash":"hash-child"}`, []string{"output"})
	executor, _ := NewPlanExecutor(published.catalog, Config{Subworkflows: subflow})
	_, err := executor.Execute(context.Background(), nodeRequest(published, `{"child":1}`, ""))
	if err != nil || subflow.request.ParentRun != "run_1" || subflow.request.ParentNode != "node" || subflow.request.PlanHash != "hash-child" {
		t.Fatalf("subworkflow = %+v, err = %v", subflow.request, err)
	}

	for _, nodeType := range []string{workflow.NodeTypeShell, workflow.NodeTypePython} {
		isolated := &fakeIsolation{}
		published = publishNodePlan(t, nodeType, `{"source":"print result"}`, []string{"output"})
		executor, _ = NewPlanExecutor(published.catalog, Config{Isolation: isolated})
		_, err = executor.Execute(context.Background(), nodeRequest(published, `null`, ""))
		if err != nil || isolated.request.Language != nodeType || isolated.request.FencingToken != 1 {
			t.Fatalf("isolation = %+v, err = %v", isolated.request, err)
		}
	}
}

func publishNodePlan(t *testing.T, nodeType, config string, ports []string) publishedFixture {
	t.Helper()
	nodes := []workflow.NodeDefinition{
		{ID: "start", Type: workflow.NodeTypeStart, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
		{ID: "node", Type: nodeType, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(config)},
	}
	edges := []workflow.EdgeDefinition{{ID: "enter", FromNode: "start", FromPort: "input", ToNode: "node", ToPort: "input"}}
	for _, port := range ports {
		endID := "end_" + port
		nodes = append(nodes, workflow.NodeDefinition{ID: endID, Type: workflow.NodeTypeEnd, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)})
		edges = append(edges, workflow.EdgeDefinition{ID: "leave_" + port, FromNode: "node", FromPort: port, ToNode: endID, ToPort: "result"})
	}
	catalog := platform.NewMemoryCatalog(workflow.BuiltinRegistry(), workflow.CompilePolicy{
		AllowedCapabilities: workflow.CapabilitySet{Streaming: true, Cancellable: true, Interruptible: true, Idempotent: true},
		AllowedPermissions:  workflow.PermissionSet{Network: true, Credentials: true, Filesystem: true, Compute: true},
	})
	published, _, err := catalog.Publish(context.Background(), "organization_a", "node_flow", workflow.WorkflowDefinition{SchemaVersion: workflow.DefinitionSchemaV1, ID: "node_flow", Nodes: nodes, Edges: edges}, "publish")
	if err != nil {
		t.Fatal(err)
	}
	return publishedFixture{catalog: catalog, plan: published}
}

func nodeRequest(published publishedFixture, input, snapshot string) workflowruntime.ExecutionRequest {
	return workflowruntime.ExecutionRequest{
		OrganizationID: "organization_a", RunID: "run_1", Plan: workflowruntime.PlanRef{ID: published.plan.ID, Hash: published.plan.Hash},
		NodeID: "node", AttemptID: "attempt_1", ExecutionID: "execution_1", Attempt: 1, Executor: publishedExecutor(published),
		Input: json.RawMessage(input), Snapshot: json.RawMessage(snapshot), FencingToken: 1,
	}
}

func publishedExecutor(published publishedFixture) string {
	node, _ := published.plan.Plan.Node("node")
	return node.Executor.Kind
}

type fakeModel struct{}

func (*fakeModel) Generate(context.Context, AttemptScope, string, string, json.RawMessage) (json.RawMessage, error) {
	return json.RawMessage(`{"model":true}`), nil
}

type failingModel struct{}

func (failingModel) Generate(context.Context, AttemptScope, string, string, json.RawMessage) (json.RawMessage, error) {
	return nil, errors.New("provider response lost")
}

type fakeInvocation struct {
	output json.RawMessage
	calls  int
}

func (f *fakeInvocation) Invoke(context.Context, AttemptScope, string, json.RawMessage) (json.RawMessage, error) {
	f.calls++
	return f.output, nil
}

type fakeSubworkflow struct{ request SubworkflowRequest }

func (f *fakeSubworkflow) Run(_ context.Context, request SubworkflowRequest) (SubworkflowOutput, error) {
	f.request = request
	return SubworkflowOutput{Value: json.RawMessage(`{"childRunId":"run_child"}`)}, nil
}

type fakeIsolation struct{ request IsolationRequest }

func (f *fakeIsolation) Run(_ context.Context, request IsolationRequest) (json.RawMessage, error) {
	f.request = request
	return json.RawMessage(`{"isolated":true}`), nil
}
