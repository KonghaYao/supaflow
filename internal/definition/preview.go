package definition

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/pazhou/supaflow/internal/workflow"
)

const maximumPreviewAdapterDuration = 5 * time.Second

// PreviewAdapterRequest is the bounded, tenant-scoped command accepted by a
// controlled validation or sandbox adapter. It must never route to production
// credentials or live external destinations.
type PreviewAdapterRequest struct {
	TenantID       string
	Node           workflow.PlanNode
	Input          json.RawMessage
	OutputLimit    int64
	ExecutionLimit time.Duration
}

// PreviewAdapter executes external node types only in a controlled test
// environment. Production side-effect adapters intentionally do not implement
// this interface implicitly.
type PreviewAdapter interface {
	PreviewNode(context.Context, PreviewAdapterRequest) (json.RawMessage, error)
}

// SetPreviewAdapter configures the explicit external-node test seam. It is
// concurrency-safe and nil restores validation-only behavior.
func (s *Service) SetPreviewAdapter(adapter PreviewAdapter) {
	s.previewMu.Lock()
	s.preview = adapter
	s.previewMu.Unlock()
}

// PreviewStep is one resolved node's side-effect-free simulation result.
type PreviewStep struct {
	NodeID       string
	Type         string
	Status       string
	SelectedPort string
	Output       json.RawMessage
}

// DryRunResult is a deterministic traversal of a compiled draft.
type DryRunResult struct {
	PlanHash string
	Steps    []PreviewStep
	Output   json.RawMessage
}

// NodeTestResult validates one resolved node with representative input.
type NodeTestResult struct {
	NodeID       string
	NodeType     string
	Executor     workflow.ExecutorBinding
	Status       string
	SelectedPort string
	Output       json.RawMessage
}

// DryRun compiles and simulates all nodes without invoking external executors.
func (s *Service) DryRun(ctx context.Context, tenantID, workflowID string, sampleInput json.RawMessage) (DryRunResult, error) {
	if !json.Valid(sampleInput) {
		return DryRunResult{}, errors.New("sample input must be valid JSON")
	}
	plan, err := s.CompileDraft(ctx, tenantID, workflowID)
	if err != nil {
		return DryRunResult{}, err
	}
	current := cloneJSON(sampleInput)
	result := DryRunResult{PlanHash: plan.Hash(), Steps: make([]PreviewStep, 0, len(plan.TopologicalOrder()))}
	for _, nodeID := range plan.TopologicalOrder() {
		node, found := plan.Node(nodeID)
		if !found {
			return DryRunResult{}, errors.New("compiled plan topology is incomplete")
		}
		step, err := s.previewNode(ctx, tenantID, node, current)
		if err != nil {
			return DryRunResult{}, err
		}
		result.Steps = append(result.Steps, step)
		if len(step.Output) > 0 {
			current = cloneJSON(step.Output)
		}
	}
	result.Output = current
	return result, nil
}

// TestNode compiles and simulates a selected node without external side effects.
func (s *Service) TestNode(ctx context.Context, tenantID, workflowID, nodeID string, sampleInput json.RawMessage) (NodeTestResult, error) {
	if nodeID == "" || !json.Valid(sampleInput) {
		return NodeTestResult{}, errors.New("node and valid sample input are required")
	}
	plan, err := s.CompileDraft(ctx, tenantID, workflowID)
	if err != nil {
		return NodeTestResult{}, err
	}
	node, found := plan.Node(nodeID)
	if !found {
		return NodeTestResult{}, ErrNotFound
	}
	preview, err := s.previewNode(ctx, tenantID, node, sampleInput)
	if err != nil {
		return NodeTestResult{}, err
	}
	return NodeTestResult{NodeID: node.ID, NodeType: node.Type, Executor: node.Executor, Status: preview.Status, SelectedPort: preview.SelectedPort, Output: preview.Output}, nil
}

func (s *Service) previewNode(ctx context.Context, tenantID string, node workflow.PlanNode, input json.RawMessage) (PreviewStep, error) {
	step := PreviewStep{NodeID: node.ID, Type: node.Type}
	switch node.Type {
	case workflow.NodeTypeCondition, workflow.NodeTypeSwitch, workflow.NodeTypeTransform, workflow.NodeTypeLoop:
		evaluated, err := workflow.EvaluateControlNode(node, input, nil, 1)
		if err != nil {
			return PreviewStep{}, fmt.Errorf("preview node %s: %w", node.ID, err)
		}
		step.Status, step.Output, step.SelectedPort = "executed", evaluated.Output, evaluated.SelectedPort
	case workflow.NodeTypeHumanApproval, workflow.NodeTypeDelay, workflow.NodeTypeWait, workflow.NodeTypeWebhookResume:
		step.Status, step.Output = "would-suspend", nil
	case workflow.NodeTypeHTTP, workflow.NodeTypeLLM, workflow.NodeTypeAgent, workflow.NodeTypeTool,
		workflow.NodeTypeSubworkflow, workflow.NodeTypeShell, workflow.NodeTypePython:
		adapter := s.previewAdapter()
		if adapter == nil {
			step.Status = "validation-only"
			return step, nil
		}
		limit := time.Duration(node.TimeoutMS) * time.Millisecond
		if limit <= 0 || limit > maximumPreviewAdapterDuration {
			limit = maximumPreviewAdapterDuration
		}
		if ctx == nil {
			ctx = context.Background()
		}
		bounded, cancel := context.WithTimeout(ctx, limit)
		defer cancel()
		output, err := adapter.PreviewNode(bounded, PreviewAdapterRequest{
			TenantID: tenantID, Node: node, Input: cloneJSON(input), OutputLimit: node.OutputLimit, ExecutionLimit: limit,
		})
		if err != nil {
			return PreviewStep{}, fmt.Errorf("preview adapter for node %s: %w", node.ID, err)
		}
		if !json.Valid(output) {
			return PreviewStep{}, fmt.Errorf("preview adapter for node %s returned invalid JSON", node.ID)
		}
		if int64(len(output)) > node.OutputLimit {
			return PreviewStep{}, fmt.Errorf("preview adapter for node %s exceeded output limit", node.ID)
		}
		step.Status, step.Output = "executed-test-adapter", cloneJSON(output)
	default:
		step.Status, step.Output = "executed", cloneJSON(input)
	}
	return step, nil
}

func (s *Service) previewAdapter() PreviewAdapter {
	s.previewMu.RLock()
	defer s.previewMu.RUnlock()
	return s.preview
}
