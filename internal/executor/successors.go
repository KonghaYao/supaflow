package executor

import (
	"encoding/json"
	"errors"

	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/workflow"
)

func successors(plan *workflow.ExecutionPlan, nodeID string, input json.RawMessage) ([]workflowruntime.NewAttempt, error) {
	return successorsForPort(plan, nodeID, "", input)
}

func successorsForPort(plan *workflow.ExecutionPlan, nodeID, selectedPort string, input json.RawMessage) ([]workflowruntime.NewAttempt, error) {
	edges := plan.Edges()
	incoming := make(map[string][]workflow.EdgeDefinition)
	active := make([]workflow.EdgeDefinition, 0, 2)
	for _, edge := range edges {
		incoming[edge.ToNode] = append(incoming[edge.ToNode], edge)
		if edge.FromNode == nodeID && (selectedPort == "" || edge.FromPort == selectedPort) {
			active = append(active, edge)
		}
	}
	if len(active) == 0 {
		return nil, executionError("invalid_plan", false, errors.New("non-terminal node has no successor"))
	}
	if len(active) > workflow.MaxNodeOutDegree {
		return nil, executionError("successor_limit_exceeded", false, errors.New("successor count exceeds the immutable execution limit"))
	}
	next := make([]workflowruntime.NewAttempt, 0, len(active))
	requiredByNode := make(map[string][]string)
	var serializedBytes int64
	for _, edge := range active {
		node, ok := plan.Node(edge.ToNode)
		if !ok || node.Executor.Kind == "" {
			return nil, executionError("invalid_plan", false, errors.New("successor has no executable binding"))
		}
		resolved, err := resolveSuccessorInput(node, edge, nodeID, input)
		if err != nil {
			return nil, err
		}
		required := successorBarriers(plan, node, edge, incoming[node.ID], requiredByNode)
		staticInputs := make(map[string]json.RawMessage)
		for port, binding := range node.Inputs {
			if len(binding.Literal) > 0 {
				staticInputs[port] = cloneJSON(binding.Literal)
			}
		}
		attempt := workflowruntime.NewAttempt{
			NodeID: node.ID, Executor: node.Executor.Kind, Input: resolved,
			EdgeID: joinEdgeKey(plan, edge), SourceNodeID: nodeID, SourcePort: edge.FromPort,
			TargetPort: edge.ToPort, RequiredEdges: required,
			StaticInputs: staticInputs, AggregateInput: len(node.Inputs) > 1 || len(staticInputs) > 0,
		}
		encoded, err := json.Marshal(attempt)
		if err != nil {
			return nil, executionError("invalid_plan", false, errors.New("successor attempt could not be encoded"))
		}
		serializedBytes += int64(len(encoded))
		if serializedBytes > workflow.MaxSuccessorPayloadBytes {
			return nil, executionError("successor_payload_limit_exceeded", false, errors.New("successor batch exceeds the immutable execution limit"))
		}
		next = append(next, attempt)
	}
	return next, nil
}

func successorBarriers(plan *workflow.ExecutionPlan, node workflow.PlanNode, edge workflow.EdgeDefinition, incoming []workflow.EdgeDefinition, cache map[string][]string) []string {
	if node.Type == workflow.NodeTypeLoop {
		return []string{joinEdgeKey(plan, edge)}
	}
	if required, exists := cache[node.ID]; exists {
		return required
	}
	required := make([]string, 0, len(incoming))
	seen := make(map[string]struct{}, len(incoming))
	for _, candidate := range incoming {
		key := joinEdgeKey(plan, candidate)
		if _, exists := seen[key]; !exists {
			required = append(required, key)
			seen[key] = struct{}{}
		}
	}
	cache[node.ID] = required
	return required
}

// joinEdgeKey collapses direct Condition/Switch alternatives into one durable
// barrier slot. Exactly one port is active, so waiting for every alternative
// would leave a valid branch merge permanently unschedulable.
func joinEdgeKey(plan *workflow.ExecutionPlan, edge workflow.EdgeDefinition) string {
	source, ok := plan.Node(edge.FromNode)
	if ok && (source.Type == workflow.NodeTypeCondition || source.Type == workflow.NodeTypeSwitch) {
		return "choice:" + edge.FromNode + ":" + edge.ToNode
	}
	return edge.ID
}

func resolveSuccessorInput(node workflow.PlanNode, edge workflow.EdgeDefinition, predecessorID string, output json.RawMessage) (json.RawMessage, error) {
	binding, exists := node.Inputs[edge.ToPort]
	if !exists {
		return cloneJSON(output), nil
	}
	if len(binding.Literal) > 0 && binding.Source == nil {
		return cloneJSON(binding.Literal), nil
	}
	if len(binding.Literal) == 0 && binding.Source != nil && binding.Source.NodeID == predecessorID && binding.Source.Port == edge.FromPort {
		return cloneJSON(output), nil
	}
	return nil, executionError("invalid_input_binding", false, errors.New("successor input binding does not match its control-flow edge"))
}
