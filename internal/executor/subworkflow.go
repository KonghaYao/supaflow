package executor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/pazhou/supaflow/internal/platform"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/workflow"
)

// RuntimeSubworkflowRunner starts children through the durable runtime service.
type RuntimeSubworkflowRunner struct {
	catalog platform.PlanCatalog
	runtime *workflowruntime.Service
	outputs RunOutputReader
}

// NewRuntimeSubworkflowRunner constructs the production child-run adapter.
func NewRuntimeSubworkflowRunner(catalog platform.PlanCatalog, service *workflowruntime.Service, outputs ...RunOutputReader) (*RuntimeSubworkflowRunner, error) {
	if catalog == nil || service == nil {
		return nil, fmt.Errorf("executor: catalog and runtime service are required")
	}
	var manager RunOutputReader
	if len(outputs) > 0 {
		manager = outputs[0]
	}
	return &RuntimeSubworkflowRunner{catalog: catalog, runtime: service, outputs: manager}, nil
}

// Run creates or observes the same child for every retry/takeover.
func (r *RuntimeSubworkflowRunner) Run(ctx context.Context, request SubworkflowRequest) (SubworkflowOutput, error) {
	published, err := r.catalog.Resolve(ctx, string(request.OrganizationID), request.PlanID, request.PlanHash)
	if err != nil || published.Plan == nil {
		return SubworkflowOutput{}, fmt.Errorf("child plan unavailable")
	}
	order := published.Plan.TopologicalOrder()
	if len(order) == 0 {
		return SubworkflowOutput{}, fmt.Errorf("child plan has no entry")
	}
	entry, ok := published.Plan.Node(order[0])
	if !ok || entry.Type != workflow.NodeTypeStart {
		return SubworkflowOutput{}, fmt.Errorf("child plan entry is invalid")
	}
	canonicalInput, err := canonicalSubworkflowInput(request.Input)
	if err != nil {
		return SubworkflowOutput{}, fmt.Errorf("child input is invalid: %w", err)
	}
	inputDigest := sha256.Sum256(canonicalInput)
	idempotencyKey := subworkflowIdempotencyKey(request, inputDigest)
	result, err := r.runtime.StartSubworkflow(ctx, request.OrganizationID, workflowruntime.StartSubworkflowRequest{
		ParentRunID: request.ParentRun, ParentNodeID: request.ParentNode, ParentAttemptID: request.ParentAttempt, ParentExecutionID: request.ParentExecution,
		Plan:  workflowruntime.PlanRef{ID: request.PlanID, Hash: request.PlanHash, EntryNodeID: entry.ID, Executor: entry.Executor.Kind},
		Input: canonicalInput, IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		return SubworkflowOutput{}, err
	}
	if result.Run.Status == workflowruntime.RunSuccess {
		if result.Run.Output == nil {
			return SubworkflowOutput{Value: json.RawMessage(`null`)}, nil
		}
		if result.Run.Output.External {
			if r.outputs == nil {
				return SubworkflowOutput{}, fmt.Errorf("child output reader unavailable")
			}
			value, readErr := r.outputs.ReadRunOutput(ctx, request.OrganizationID, result.Run.ID, *result.Run.Output)
			if readErr != nil {
				return SubworkflowOutput{}, fmt.Errorf("read child output: %w", readErr)
			}
			return SubworkflowOutput{Value: value, Sensitive: result.Run.Output.Sensitive}, nil
		}
		if len(result.Run.Output.Inline) == 0 {
			return SubworkflowOutput{}, fmt.Errorf("child inline output is unavailable")
		}
		return SubworkflowOutput{Value: append(json.RawMessage(nil), result.Run.Output.Inline...), Sensitive: result.Run.Output.Sensitive}, nil
	}
	value, err := json.Marshal(map[string]any{"childRunId": result.Run.ID, "status": result.Run.Status})
	return SubworkflowOutput{Value: value}, err
}

func subworkflowIdempotencyKey(request SubworkflowRequest, inputDigest [32]byte) string {
	executionID := request.ParentExecution
	if executionID == "" {
		executionID = request.ParentAttempt
	}
	digest := sha256.Sum256([]byte(string(request.OrganizationID) + "\x00" + request.ParentRun + "\x00" + request.ParentNode + "\x00" + executionID + "\x00" + request.PlanHash + "\x00" + hex.EncodeToString(inputDigest[:])))
	return "subflow-" + hex.EncodeToString(digest[:])
}

func canonicalSubworkflowInput(input json.RawMessage) (json.RawMessage, error) {
	if len(input) == 0 {
		return json.RawMessage(`null`), nil
	}
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(canonical), nil
}

func (e *PlanExecutor) executeSubworkflowNode(ctx context.Context, req workflowruntime.ExecutionRequest, node workflow.PlanNode, plan *workflow.ExecutionPlan) (workflowruntime.ExecutionResult, error) {
	output, err := executeSubworkflow(ctx, e.subflow, req, node)
	if err != nil {
		return workflowruntime.ExecutionResult{}, err
	}
	var state struct {
		Status workflowruntime.RunStatus `json:"status"`
	}
	if json.Unmarshal(output.Value, &state) == nil && state.Status != "" {
		switch state.Status {
		case workflowruntime.RunPending, workflowruntime.RunRunning, workflowruntime.RunSuspended, workflowruntime.RunCancelling:
			return workflowruntime.ExecutionResult{Suspend: &workflowruntime.Suspension{Kind: "wait", ExpiresIn: time.Second, AutoResume: true, Snapshot: output.Value}}, nil
		case workflowruntime.RunFailed, workflowruntime.RunError, workflowruntime.RunCancelled:
			return workflowruntime.ExecutionResult{}, executionError("subworkflow_failed", false, errors.New("child workflow did not succeed"))
		}
	}
	next, err := successors(plan, node.ID, output.Value)
	if err != nil {
		return workflowruntime.ExecutionResult{}, err
	}
	node.SensitiveOutput = node.SensitiveOutput || output.Sensitive
	stored, err := e.makeOutput(ctx, req, node, output.Value)
	return workflowruntime.ExecutionResult{Output: stored, Next: next}, err
}

var _ SubworkflowRunner = (*RuntimeSubworkflowRunner)(nil)
