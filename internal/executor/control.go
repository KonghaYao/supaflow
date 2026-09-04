package executor

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/workflow"
)

func (e *PlanExecutor) executeControl(ctx context.Context, req workflowruntime.ExecutionRequest, node workflow.PlanNode, plan *workflow.ExecutionPlan) (workflowruntime.ExecutionResult, error) {
	evaluated, err := workflow.EvaluateControlNode(node, req.Input, req.Snapshot, req.Attempt)
	if err != nil {
		return workflowruntime.ExecutionResult{}, invalidConfig()
	}
	var snapshot *workflowruntime.Snapshot
	if len(evaluated.Snapshot) > 0 {
		snapshot = &workflowruntime.Snapshot{Data: evaluated.Snapshot}
	}
	next, err := successorsForPort(plan, node.ID, evaluated.SelectedPort, evaluated.Output)
	if err != nil {
		return workflowruntime.ExecutionResult{}, err
	}
	stored, err := e.makeOutput(ctx, req, node, evaluated.Output)
	if err != nil {
		return workflowruntime.ExecutionResult{}, err
	}
	return workflowruntime.ExecutionResult{Output: stored, Snapshot: snapshot, Next: next}, nil
}

func (e *PlanExecutor) executeWait(ctx context.Context, req workflowruntime.ExecutionRequest, node workflow.PlanNode, plan *workflow.ExecutionPlan) (workflowruntime.ExecutionResult, error) {
	var cfg struct {
		DurationMS int64 `json:"durationMs"`
		TimeoutMS  int64 `json:"timeoutMs"`
	}
	if json.Unmarshal(node.Config, &cfg) != nil {
		return workflowruntime.ExecutionResult{}, invalidConfig()
	}
	if len(req.ResumeData) == 0 {
		expires := cfg.TimeoutMS
		kind := "wait"
		if node.Type == workflow.NodeTypeDelay {
			expires = cfg.DurationMS
		}
		if node.Type == workflow.NodeTypeWebhookResume {
			kind = "webhook"
		}
		if expires <= 0 {
			expires = node.TimeoutMS
		}
		return workflowruntime.ExecutionResult{Suspend: &workflowruntime.Suspension{
			Kind: kind, ExpiresIn: time.Duration(expires) * time.Millisecond,
			Snapshot: cloneJSON(req.Input), AutoResume: node.Type == workflow.NodeTypeDelay || node.Type == workflow.NodeTypeWait,
		}}, nil
	}
	if !json.Valid(req.ResumeData) {
		return workflowruntime.ExecutionResult{}, executionError("invalid_resume_data", false, errors.New("resume data is not JSON"))
	}
	next, err := successors(plan, node.ID, req.ResumeData)
	if err != nil {
		return workflowruntime.ExecutionResult{}, err
	}
	stored, err := e.makeOutput(ctx, req, node, req.ResumeData)
	return workflowruntime.ExecutionResult{Output: stored, Next: next}, err
}

func invalidConfig() error {
	return executionError("invalid_node_config", false, errors.New("node configuration is invalid"))
}
