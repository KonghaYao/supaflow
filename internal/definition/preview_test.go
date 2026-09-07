package definition

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/pazhou/supaflow/internal/workflow"
)

type previewAdapterFunc func(context.Context, PreviewAdapterRequest) (json.RawMessage, error)

func (f previewAdapterFunc) PreviewNode(ctx context.Context, request PreviewAdapterRequest) (json.RawMessage, error) {
	return f(ctx, request)
}

// 验证 dry-run 真正按拓扑模拟纯节点，并返回最终样例输出而不调用 executor。
func TestDryRunTraversesCompiledDraft(t *testing.T) {
	service, _ := newTestService(t)
	definition := definitionFor("flow")
	_, err := service.CreateDraft(context.Background(), "organization-a", CreateDraftCommand{
		WorkflowID: "flow", ActorID: "editor", IdempotencyKey: "create-preview-1", Document: mustJSON(t, definition),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.DryRun(context.Background(), "organization-a", "flow", json.RawMessage(`{"message":"hello"}`))
	if err != nil || len(result.Steps) != 2 || result.Steps[0].NodeID != "start" || string(result.Output) != `{"message":"hello"}` {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

// 验证 node-test 返回发布时解析出的 executor binding，而不是前端猜测。
func TestNodeTestUsesResolvedExecutor(t *testing.T) {
	service, _ := newTestService(t)
	_, err := service.CreateDraft(context.Background(), "organization-a", CreateDraftCommand{
		WorkflowID: "flow", ActorID: "editor", IdempotencyKey: "create-preview-2", Document: mustJSON(t, definitionFor("flow")),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.TestNode(context.Background(), "organization-a", "flow", "end", json.RawMessage(`{}`))
	if err != nil || result.Executor.Kind == "" || result.Status != "executed" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

// 验证 Condition、Transform 和 Loop 的 node-test 执行与 runtime 共享的确定性语义，而不是回显输入。
func TestNodeTestExecutesPureNodeSemantics(t *testing.T) {
	tests := []struct {
		name, nodeType string
		config         json.RawMessage
		input          json.RawMessage
		output, port   string
	}{
		{name: "condition", nodeType: workflow.NodeTypeCondition, config: json.RawMessage(`{"path":"$.score","operator":"gte","value":10}`), input: json.RawMessage(`{"score":12}`), output: `{"score":12}`, port: "true"},
		{name: "transform", nodeType: workflow.NodeTypeTransform, config: json.RawMessage(`{"select":{"name":"$.user.name"}}`), input: json.RawMessage(`{"user":{"name":"Ada"}}`), output: `{"name":"Ada"}`},
		{name: "loop", nodeType: workflow.NodeTypeLoop, config: json.RawMessage(`{"maxIterations":2}`), input: json.RawMessage(`{"value":1}`), output: `{"value":1}`, port: "body"},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service, _ := newTestService(t)
			workflowID := fmt.Sprintf("preview-%d", index)
			createPreviewDraft(t, service, previewDefinition(workflowID, test.nodeType, test.config))
			result, err := service.TestNode(context.Background(), "organization-a", workflowID, "target", test.input)
			if err != nil || result.Status != "executed" || string(result.Output) != test.output || result.SelectedPort != test.port {
				t.Fatalf("result=%+v err=%v", result, err)
			}
		})
	}
}

// 验证 Agent/Tool preview 只能通过受限测试 adapter 执行，未配置时明确只完成校验。
func TestNodeTestUsesBoundedExternalPreviewAdapter(t *testing.T) {
	for _, nodeType := range []string{workflow.NodeTypeAgent, workflow.NodeTypeTool} {
		t.Run(nodeType, func(t *testing.T) {
			service, _ := newTestService(t)
			workflowID := "external-" + nodeType
			createPreviewDraft(t, service, previewDefinition(workflowID, nodeType, json.RawMessage(`{"name":"sandbox"}`)))
			validation, err := service.TestNode(context.Background(), "organization-a", workflowID, "target", json.RawMessage(`{"id":7}`))
			if err != nil || validation.Status != "validation-only" || validation.Output != nil {
				t.Fatalf("validation result=%+v err=%v", validation, err)
			}
			calls := 0
			service.SetPreviewAdapter(previewAdapterFunc(func(_ context.Context, request PreviewAdapterRequest) (json.RawMessage, error) {
				calls++
				return json.Marshal(map[string]any{"adapter": request.Node.Type, "inputValid": json.Valid(request.Input)})
			}))
			executed, err := service.TestNode(context.Background(), "organization-a", workflowID, "target", json.RawMessage(`{"id":7}`))
			if err != nil || executed.Status != "executed-test-adapter" || calls != 1 || string(executed.Output) != `{"adapter":"`+nodeType+`","inputValid":true}` {
				t.Fatalf("executed result=%+v calls=%d err=%v", executed, calls, err)
			}
		})
	}
}

// 验证测试 adapter 也不能绕过发布节点的输出上限，防止 preview 成为无界资源通道。
func TestNodeTestRejectsPreviewAdapterOutputBeyondNodeLimit(t *testing.T) {
	service, _ := newTestService(t)
	createPreviewDraft(t, service, previewDefinition("bounded-adapter", workflow.NodeTypeTool, json.RawMessage(`{"name":"sandbox"}`)))
	service.SetPreviewAdapter(previewAdapterFunc(func(_ context.Context, request PreviewAdapterRequest) (json.RawMessage, error) {
		return json.Marshal(string(bytes.Repeat([]byte{'x'}, int(request.OutputLimit))))
	}))
	if _, err := service.TestNode(context.Background(), "organization-a", "bounded-adapter", "target", json.RawMessage(`null`)); err == nil {
		t.Fatal("TestNode() error = nil, want bounded output rejection")
	}
}

// 验证 dry-run 会传递 Transform 的真实输出到后续节点和最终结果。
func TestDryRunPropagatesRealTransformOutput(t *testing.T) {
	service, _ := newTestService(t)
	createPreviewDraft(t, service, previewDefinition("transform-flow", workflow.NodeTypeTransform, json.RawMessage(`{"select":{"name":"$.user.name"}}`)))
	result, err := service.DryRun(context.Background(), "organization-a", "transform-flow", json.RawMessage(`{"user":{"name":"Ada"}}`))
	if err != nil || len(result.Steps) != 3 || result.Steps[1].Status != "executed" || string(result.Output) != `{"name":"Ada"}` {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func createPreviewDraft(t *testing.T, service *Service, definition workflow.WorkflowDefinition) {
	t.Helper()
	_, err := service.CreateDraft(context.Background(), "organization-a", CreateDraftCommand{
		WorkflowID: definition.ID, ActorID: "editor", IdempotencyKey: "create-" + definition.ID, Document: mustJSON(t, definition),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func previewDefinition(id, nodeType string, config json.RawMessage) workflow.WorkflowDefinition {
	return workflow.WorkflowDefinition{
		SchemaVersion: workflow.DefinitionSchemaV1, ID: id,
		Nodes: []workflow.NodeDefinition{
			{ID: "start", Type: workflow.NodeTypeStart, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
			{ID: "target", Type: nodeType, Version: workflow.BuiltinNodeVersion, Config: config},
			{ID: "end", Type: workflow.NodeTypeEnd, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
		},
		Edges: []workflow.EdgeDefinition{
			{ID: "to-target", FromNode: "start", FromPort: "input", ToNode: "target", ToPort: "input"},
			{ID: "to-end", FromNode: "target", FromPort: "output", ToNode: "end", ToPort: "result"},
		},
	}
}
