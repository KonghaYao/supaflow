package executor

import (
	"context"
	"encoding/json"

	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/workflow"
)

type shadowStubOutput struct {
	ShadowStubbed bool   `json:"shadowStubbed"`
	NodeID        string `json:"nodeId"`
	Stub          string `json:"stub"`
}

func (e *PlanExecutor) executeShadowStub(ctx context.Context, req workflowruntime.ExecutionRequest, node workflow.PlanNode, plan *workflow.ExecutionPlan) (workflowruntime.ExecutionResult, error) {
	output, err := json.Marshal(shadowStubOutput{
		ShadowStubbed: true,
		NodeID:        node.ID,
		Stub:          workflow.ShadowStubExecutorKind,
	})
	if err != nil {
		return workflowruntime.ExecutionResult{}, executionError("shadow_stub_failed", false, err)
	}
	next, err := successors(plan, node.ID, output)
	if err != nil {
		return workflowruntime.ExecutionResult{}, err
	}
	// The synthetic output contains no plan input or config. Its reviewed stub
	// contract keeps it inline even when the real node was sensitive or had a
	// tiny output limit, avoiding an object-storage side effect during shadowing.
	node.SensitiveOutput = false
	node.OutputLimit = int64(len(output))
	stored, err := e.makeOutput(ctx, req, node, output)
	if err != nil {
		return workflowruntime.ExecutionResult{}, err
	}
	return withShadowSuccessors(plan, workflowruntime.ExecutionResult{Output: stored, Next: next}, true), nil
}

func withShadowSuccessors(plan *workflow.ExecutionPlan, result workflowruntime.ExecutionResult, shadowMode bool) workflowruntime.ExecutionResult {
	if !shadowMode {
		return result
	}
	for index := range result.Next {
		node, ok := plan.Node(result.Next[index].NodeID)
		if ok {
			result.Next[index].Executor = workflow.ShadowAttemptExecutor(node)
		}
	}
	return result
}
