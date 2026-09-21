package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/workflow"
)

// AttemptScope is the minimum identity adapters need for organization and fencing checks.
type AttemptScope struct {
	OrganizationID workflowruntime.OrganizationID
	RunID          string
	AttemptID      string
	ExecutionID    string
	FencingToken   uint64
}

func attemptScope(request workflowruntime.ExecutionRequest) AttemptScope {
	executionID := request.ExecutionID
	if executionID == "" {
		executionID = request.AttemptID
	}
	return AttemptScope{OrganizationID: request.OrganizationID, RunID: request.RunID, AttemptID: request.AttemptID, ExecutionID: executionID, FencingToken: request.FencingToken}
}

// ModelRegistry resolves and invokes organization-configured Eino chat models.
// Implementations own provider credentials; plan config only contains a name.
type ModelRegistry interface {
	Generate(context.Context, AttemptScope, string, string, json.RawMessage) (json.RawMessage, error)
}

// StreamingModelRegistry is the optional delta-capable model contract.
type StreamingModelRegistry interface {
	GenerateStream(context.Context, AttemptScope, string, string, json.RawMessage, func(json.RawMessage) error) (json.RawMessage, error)
}

// AgentRegistry invokes a named Eino-backed agent through an injectable port.
type AgentRegistry interface {
	Invoke(context.Context, AttemptScope, string, json.RawMessage) (json.RawMessage, error)
}

// StreamingAgentRegistry is the optional delta-capable agent contract.
type StreamingAgentRegistry interface {
	InvokeStream(context.Context, AttemptScope, string, json.RawMessage, func(json.RawMessage) error) (json.RawMessage, error)
}

// ToolRegistry invokes a named Eino tool through an injectable port.
type ToolRegistry interface {
	Invoke(context.Context, AttemptScope, string, json.RawMessage) (json.RawMessage, error)
}

// SubworkflowRunner starts or observes one idempotent child run. Parent scope is
// always supplied by the executor rather than accepted from workflow input.
type SubworkflowRunner interface {
	Run(context.Context, SubworkflowRequest) (SubworkflowOutput, error)
}

// SubworkflowOutput carries the child value and its confidentiality taint.
type SubworkflowOutput struct {
	Value     json.RawMessage
	Sensitive bool
}

// SubworkflowRequest preserves parent-child lineage across retry and takeover.
type SubworkflowRequest struct {
	OrganizationID  workflowruntime.OrganizationID
	ParentRun       string
	ParentNode      string
	ParentAttempt   string
	ParentExecution string
	PlanID          string
	PlanHash        string
	Input           json.RawMessage
}

// IsolationRunner is the only port through which Shell/Python may execute.
type IsolationRunner interface {
	Run(context.Context, IsolationRequest) (json.RawMessage, error)
}

// IsolationRequest binds isolated work to the current fenced attempt.
type IsolationRequest struct {
	OrganizationID workflowruntime.OrganizationID
	RunID          string
	NodeID         string
	AttemptID      string
	FencingToken   uint64
	Language       string
	Source         string
	Args           []string
	EnvAllowlist   []string
	Network        bool
	Input          json.RawMessage
}

type namedAdapterConfig struct {
	Name   string `json:"name"`
	System string `json:"system,omitempty"`
}

func executeModel(ctx context.Context, registry ModelRegistry, req workflowruntime.ExecutionRequest, node workflow.PlanNode) (json.RawMessage, error) {
	var cfg namedAdapterConfig
	if registry == nil || json.Unmarshal(node.Config, &cfg) != nil || strings.TrimSpace(cfg.Name) == "" {
		return nil, unavailableAdapter("model")
	}
	return invokeAdapter(func() (json.RawMessage, error) {
		return registry.Generate(ctx, attemptScope(req), cfg.Name, cfg.System, req.Input)
	})
}

func executeModelStream(ctx context.Context, registry ModelRegistry, req workflowruntime.ExecutionRequest, node workflow.PlanNode, emit func(json.RawMessage) error) (json.RawMessage, error) {
	var cfg namedAdapterConfig
	streaming, ok := registry.(StreamingModelRegistry)
	if !ok || emit == nil || json.Unmarshal(node.Config, &cfg) != nil || strings.TrimSpace(cfg.Name) == "" {
		return nil, unavailableAdapter("model_stream")
	}
	return invokeAdapter(func() (json.RawMessage, error) {
		return streaming.GenerateStream(ctx, attemptScope(req), cfg.Name, cfg.System, req.Input, emit)
	})
}

func executeAgent(ctx context.Context, registry AgentRegistry, req workflowruntime.ExecutionRequest, node workflow.PlanNode) (json.RawMessage, error) {
	var cfg namedAdapterConfig
	if registry == nil || json.Unmarshal(node.Config, &cfg) != nil || strings.TrimSpace(cfg.Name) == "" {
		return nil, unavailableAdapter("agent")
	}
	return invokeAdapter(func() (json.RawMessage, error) {
		return registry.Invoke(ctx, attemptScope(req), cfg.Name, req.Input)
	})
}

func executeAgentStream(ctx context.Context, registry AgentRegistry, req workflowruntime.ExecutionRequest, node workflow.PlanNode, emit func(json.RawMessage) error) (json.RawMessage, error) {
	var cfg namedAdapterConfig
	streaming, ok := registry.(StreamingAgentRegistry)
	if !ok || emit == nil || json.Unmarshal(node.Config, &cfg) != nil || strings.TrimSpace(cfg.Name) == "" {
		return nil, unavailableAdapter("agent_stream")
	}
	return invokeAdapter(func() (json.RawMessage, error) {
		return streaming.InvokeStream(ctx, attemptScope(req), cfg.Name, req.Input, emit)
	})
}

func executeTool(ctx context.Context, registry ToolRegistry, req workflowruntime.ExecutionRequest, node workflow.PlanNode) (json.RawMessage, error) {
	var cfg namedAdapterConfig
	if registry == nil || json.Unmarshal(node.Config, &cfg) != nil || strings.TrimSpace(cfg.Name) == "" {
		return nil, unavailableAdapter("tool")
	}
	return invokeAdapter(func() (json.RawMessage, error) {
		return registry.Invoke(ctx, attemptScope(req), cfg.Name, req.Input)
	})
}

func invokeAdapter(call func() (json.RawMessage, error)) (json.RawMessage, error) {
	output, err := call()
	if err != nil {
		// Keep the adapter's own error in the cause chain. The public error code stays
		// deliberately coarse, so the cause is the only place the real reason survives:
		// dropping it (as this did) makes "script exited 3", "output exceeded the limit"
		// and "provider is not configured" indistinguishable in every log and every API
		// response. Persistent callers render it; nothing outside this process sees it.
		return nil, executionError("adapter_failed", true, fmt.Errorf("external adapter invocation failed: %w", err))
	}
	if !json.Valid(output) {
		return nil, executionError("invalid_adapter_output", false, errors.New("adapter returned invalid JSON"))
	}
	return cloneJSON(output), nil
}

func unavailableAdapter(kind string) error {
	return executionError(kind+"_adapter_unavailable", false, fmt.Errorf("%s adapter is not configured", kind))
}

func executeSubworkflow(ctx context.Context, runner SubworkflowRunner, req workflowruntime.ExecutionRequest, node workflow.PlanNode) (SubworkflowOutput, error) {
	var cfg struct {
		PlanID   string `json:"planId"`
		PlanHash string `json:"planHash"`
	}
	if runner == nil || json.Unmarshal(node.Config, &cfg) != nil || cfg.PlanID == "" || cfg.PlanHash == "" {
		return SubworkflowOutput{}, unavailableAdapter("subworkflow")
	}
	output, err := runner.Run(ctx, SubworkflowRequest{
		OrganizationID: req.OrganizationID, ParentRun: req.RunID, ParentNode: req.NodeID,
		ParentAttempt: req.AttemptID, ParentExecution: attemptScope(req).ExecutionID,
		PlanID: cfg.PlanID, PlanHash: cfg.PlanHash, Input: cloneJSON(req.Input),
	})
	if err != nil {
		return SubworkflowOutput{}, executionError("adapter_failed", true, fmt.Errorf("external adapter invocation failed: %w", err))
	}
	if !json.Valid(output.Value) {
		return SubworkflowOutput{}, executionError("invalid_adapter_output", false, errors.New("adapter returned invalid JSON"))
	}
	output.Value = cloneJSON(output.Value)
	return output, nil
}

func executeIsolated(ctx context.Context, runner IsolationRunner, req workflowruntime.ExecutionRequest, node workflow.PlanNode) (json.RawMessage, error) {
	var cfg struct {
		Source  string   `json:"source"`
		Args    []string `json:"args"`
		Env     []string `json:"env"`
		Network bool     `json:"network"`
	}
	if runner == nil || json.Unmarshal(node.Config, &cfg) != nil || strings.TrimSpace(cfg.Source) == "" {
		return nil, unavailableAdapter("isolation")
	}
	return invokeAdapter(func() (json.RawMessage, error) {
		return runner.Run(ctx, IsolationRequest{
			OrganizationID: req.OrganizationID, RunID: req.RunID, NodeID: req.NodeID,
			AttemptID: req.AttemptID, FencingToken: req.FencingToken,
			Language: strings.TrimPrefix(node.Executor.Kind, "isolated."), Source: cfg.Source,
			Args: append([]string(nil), cfg.Args...), EnvAllowlist: append([]string(nil), cfg.Env...),
			Network: cfg.Network, Input: cloneJSON(req.Input),
		})
	})
}
