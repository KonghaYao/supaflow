package executor

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/pazhou/supaflow/internal/platform"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/workflow"
)

// 验证所有 executor adapter 都受不可变 plan 的节点超时约束，且超时不会被错误归类为可重试 provider 故障。
func TestPlanExecutorEnforcesPinnedNodeTimeout(t *testing.T) {
	acceptConfig := func(json.RawMessage) error { return nil }
	registry, err := workflow.NewRegistry(workflow.NodeTypeVersion{
		Type: workflow.NodeTypeStart, Version: workflow.BuiltinNodeVersion,
		Capabilities: workflow.CapabilitySet{Idempotent: true}, DefaultTimeoutMS: 1000, MaximumOutputBytes: 1024,
		Executor:     workflow.ExecutorBinding{Kind: "builtin.start", MinimumRunnerVersion: "1.0.0"},
		Outputs:      map[string]workflow.PortDefinition{"input": {Type: workflow.ValueTypeAny}},
		ConfigSchema: json.RawMessage(`{"type":"object"}`), ValidateConfig: acceptConfig,
	}, workflow.NodeTypeVersion{
		Type: workflow.NodeTypeTool, Version: workflow.BuiltinNodeVersion,
		Capabilities:     workflow.CapabilitySet{Cancellable: true, Idempotent: true},
		DefaultTimeoutMS: 20, MaximumOutputBytes: 1024,
		Executor:       workflow.ExecutorBinding{Kind: "eino.tool", MinimumRunnerVersion: "1.0.0"},
		Inputs:         map[string]workflow.PortDefinition{"input": {Type: workflow.ValueTypeAny}},
		Outputs:        map[string]workflow.PortDefinition{"output": {Type: workflow.ValueTypeAny}},
		ConfigSchema:   json.RawMessage(`{"type":"object"}`),
		ValidateConfig: acceptConfig,
	}, workflow.NodeTypeVersion{
		Type: workflow.NodeTypeEnd, Version: workflow.BuiltinNodeVersion,
		Capabilities: workflow.CapabilitySet{Idempotent: true}, DefaultTimeoutMS: 1000, MaximumOutputBytes: 1024,
		Executor:     workflow.ExecutorBinding{Kind: "builtin.end", MinimumRunnerVersion: "1.0.0"},
		Inputs:       map[string]workflow.PortDefinition{"result": {Type: workflow.ValueTypeAny}},
		ConfigSchema: json.RawMessage(`{"type":"object"}`), ValidateConfig: acceptConfig,
	})
	if err != nil {
		t.Fatal(err)
	}
	catalog := platform.NewMemoryCatalog(registry, workflow.CompilePolicy{
		AllowedCapabilities: workflow.CapabilitySet{Cancellable: true, Idempotent: true},
	})
	published, _, err := catalog.Publish(context.Background(), "organization_a", "timeout_flow", workflow.WorkflowDefinition{
		SchemaVersion: workflow.DefinitionSchemaV1, ID: "timeout_flow",
		Nodes: []workflow.NodeDefinition{
			{ID: "start", Type: workflow.NodeTypeStart, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
			{ID: "slow", Type: workflow.NodeTypeTool, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{"name":"slow"}`)},
			{ID: "end", Type: workflow.NodeTypeEnd, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
		},
		Edges: []workflow.EdgeDefinition{
			{ID: "start_slow", FromNode: "start", FromPort: "input", ToNode: "slow", ToPort: "input"},
			{ID: "slow_end", FromNode: "slow", FromPort: "output", ToNode: "end", ToPort: "result"},
		},
	}, "publish-timeout")
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewPlanExecutor(catalog, Config{Tools: blockingTool{}})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	_, err = executor.Execute(context.Background(), workflowruntime.ExecutionRequest{
		OrganizationID: "organization_a", RunID: "run_timeout", Plan: workflowruntime.PlanRef{ID: published.ID, Hash: published.Hash},
		NodeID: "slow", AttemptID: "attempt_timeout", Attempt: 1, Executor: "eino.tool",
		Input: json.RawMessage(`null`), FencingToken: 1,
	})
	var executionErr *workflowruntime.ExecutionError
	if !errors.As(err, &executionErr) || executionErr.Code != "node_timeout" || executionErr.Retryable {
		t.Fatalf("error = %#v", err)
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("node timeout took %s", elapsed)
	}
}

type blockingTool struct{}

func (blockingTool) Invoke(ctx context.Context, _ AttemptScope, _ string, _ json.RawMessage) (json.RawMessage, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
