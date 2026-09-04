package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/cloudwego/eino/compose"
)

const maximumAgentSteps = 16

// TrustedToolConfig selects a built-in tool at process assembly time. Workflow
// documents may reference the assigned name but cannot define executable code.
type TrustedToolConfig struct {
	Name string `json:"name"`
	Kind string `json:"kind"`
}

type trustedTool func(json.RawMessage) (json.RawMessage, error)

// TrustedToolRegistry exposes only process-owned, deterministic tools.
type TrustedToolRegistry struct{ tools map[string]trustedTool }

// NewTrustedToolRegistry validates and constructs the assembly-time tool set.
func NewTrustedToolRegistry(configs []TrustedToolConfig) (*TrustedToolRegistry, error) {
	registry := &TrustedToolRegistry{tools: make(map[string]trustedTool, len(configs))}
	for _, cfg := range configs {
		if strings.TrimSpace(cfg.Name) == "" || registry.tools[cfg.Name] != nil {
			return nil, errors.New("executor: trusted tool names must be unique")
		}
		var implementation trustedTool
		switch cfg.Kind {
		case "json.project":
			implementation = projectJSON
		case "json.merge":
			implementation = mergeJSON
		default:
			return nil, fmt.Errorf("executor: unsupported trusted tool kind %q", cfg.Kind)
		}
		registry.tools[cfg.Name] = implementation
	}
	return registry, nil
}

// Has reports whether a trusted process-owned tool is configured.
func (r *TrustedToolRegistry) Has(name string) bool { return r != nil && r.tools[name] != nil }

// Invoke executes one configured trusted tool without reading credentials.
func (r *TrustedToolRegistry) Invoke(_ context.Context, _ AttemptScope, name string, input json.RawMessage) (json.RawMessage, error) {
	if r == nil || r.tools[name] == nil {
		return nil, errors.New("trusted tool is not registered")
	}
	return r.tools[name](input)
}

func projectJSON(input json.RawMessage) (json.RawMessage, error) {
	var request struct {
		Document map[string]json.RawMessage `json:"document"`
		Field    string                     `json:"field"`
	}
	if json.Unmarshal(input, &request) != nil || request.Field == "" || request.Document == nil {
		return nil, errors.New("json.project input is invalid")
	}
	value, exists := request.Document[request.Field]
	if !exists {
		return json.RawMessage("null"), nil
	}
	return cloneJSON(value), nil
}

func mergeJSON(input json.RawMessage) (json.RawMessage, error) {
	var request struct {
		Values []map[string]json.RawMessage `json:"values"`
	}
	if json.Unmarshal(input, &request) != nil || len(request.Values) == 0 {
		return nil, errors.New("json.merge input is invalid")
	}
	result := make(map[string]json.RawMessage)
	for _, value := range request.Values {
		for key, item := range value {
			result[key] = cloneJSON(item)
		}
	}
	return json.Marshal(result)
}

// AgentConfig fixes the model, system prompt, allowlisted tools and finite step
// budget. None of these security-sensitive fields are accepted from a plan.
type AgentConfig struct {
	Name     string   `json:"name"`
	Model    string   `json:"model"`
	Tools    []string `json:"tools"`
	System   string   `json:"system"`
	MaxSteps int      `json:"maxSteps"`
}

// BoundedAgentRegistry runs a finite model/tool-call loop over separate ports.
type BoundedAgentRegistry struct {
	models ModelRegistry
	tools  *TrustedToolRegistry
	agents map[string]AgentConfig
	graphs map[string]compose.Runnable[agentInvocation, json.RawMessage]
}

type agentInvocation struct {
	Scope AttemptScope
	Input json.RawMessage
}

// NewBoundedAgentRegistry validates all references during worker startup.
func NewBoundedAgentRegistry(models ModelRegistry, tools *TrustedToolRegistry, configs []AgentConfig, hasModel func(string) bool) (*BoundedAgentRegistry, error) {
	if models == nil || tools == nil || hasModel == nil {
		return nil, errors.New("executor: agent model and tool registries are required")
	}
	registry := &BoundedAgentRegistry{
		models: models, tools: tools,
		agents: make(map[string]AgentConfig, len(configs)),
		graphs: make(map[string]compose.Runnable[agentInvocation, json.RawMessage], len(configs)),
	}
	for _, cfg := range configs {
		if cfg.Name == "" || cfg.Model == "" || strings.TrimSpace(cfg.System) == "" || cfg.MaxSteps < 1 || cfg.MaxSteps > maximumAgentSteps || registry.agents[cfg.Name].Name != "" || !hasModel(cfg.Model) {
			return nil, errors.New("executor: agent configuration is invalid")
		}
		seen := make(map[string]struct{}, len(cfg.Tools))
		for _, name := range cfg.Tools {
			if !tools.Has(name) {
				return nil, fmt.Errorf("executor: agent tool %q is not configured", name)
			}
			if _, duplicate := seen[name]; duplicate {
				return nil, errors.New("executor: agent tool names must be unique")
			}
			seen[name] = struct{}{}
		}
		registry.agents[cfg.Name] = cfg
		graph := compose.NewGraph[agentInvocation, json.RawMessage]()
		if err := graph.AddLambdaNode("tool-call-loop", compose.InvokableLambda(func(ctx context.Context, invocation agentInvocation) (json.RawMessage, error) {
			return registry.invokeLoop(ctx, cfg, invocation.Scope, invocation.Input)
		})); err != nil {
			return nil, fmt.Errorf("executor: assemble agent graph: %w", err)
		}
		if err := graph.AddEdge(compose.START, "tool-call-loop"); err != nil {
			return nil, fmt.Errorf("executor: assemble agent graph: %w", err)
		}
		if err := graph.AddEdge("tool-call-loop", compose.END); err != nil {
			return nil, fmt.Errorf("executor: assemble agent graph: %w", err)
		}
		runnable, err := graph.Compile(context.Background(), compose.WithGraphName("supaflow-agent-"+cfg.Name))
		if err != nil {
			return nil, fmt.Errorf("executor: compile agent graph: %w", err)
		}
		registry.graphs[cfg.Name] = runnable
	}
	return registry, nil
}

type agentModelResult struct {
	Content   string `json:"content"`
	ToolCalls []struct {
		ID       string `json:"id"`
		Function struct {
			Name      string `json:"name"`
			Arguments string `json:"arguments"`
		} `json:"function"`
	} `json:"toolCalls"`
}

// Invoke runs at most MaxSteps model turns and rejects non-allowlisted calls.
func (r *BoundedAgentRegistry) Invoke(ctx context.Context, scope AttemptScope, name string, input json.RawMessage) (json.RawMessage, error) {
	_, ok := r.agents[name]
	if !ok {
		return nil, errors.New("agent is not registered")
	}
	return r.graphs[name].Invoke(ctx, agentInvocation{Scope: scope, Input: cloneJSON(input)})
}

func (r *BoundedAgentRegistry) invokeLoop(ctx context.Context, cfg AgentConfig, scope AttemptScope, input json.RawMessage) (json.RawMessage, error) {
	allowed := make(map[string]struct{}, len(cfg.Tools))
	for _, toolName := range cfg.Tools {
		allowed[toolName] = struct{}{}
	}
	next := cloneJSON(input)
	for step := 0; step < cfg.MaxSteps; step++ {
		encoded, err := r.models.Generate(ctx, scope, cfg.Model, cfg.System, next)
		if err != nil {
			return nil, err
		}
		var result agentModelResult
		if json.Unmarshal(encoded, &result) != nil {
			return nil, errors.New("agent model output is invalid")
		}
		if len(result.ToolCalls) == 0 {
			return encoded, nil
		}
		toolResults := make([]map[string]any, 0, len(result.ToolCalls))
		for _, call := range result.ToolCalls {
			if _, permitted := allowed[call.Function.Name]; !permitted || !json.Valid([]byte(call.Function.Arguments)) {
				return nil, errors.New("agent requested a forbidden or invalid tool call")
			}
			value, invokeErr := r.tools.Invoke(ctx, scope, call.Function.Name, json.RawMessage(call.Function.Arguments))
			if invokeErr != nil {
				return nil, invokeErr
			}
			toolResults = append(toolResults, map[string]any{"id": call.ID, "name": call.Function.Name, "value": json.RawMessage(value)})
		}
		next, err = json.Marshal(map[string]any{"input": json.RawMessage(input), "assistant": result.Content, "toolResults": toolResults})
		if err != nil {
			return nil, err
		}
	}
	return nil, errors.New("agent exceeded configured maxSteps")
}

// InvokeStream emits the canonical bounded result as a single JSON delta.
func (r *BoundedAgentRegistry) InvokeStream(ctx context.Context, scope AttemptScope, name string, input json.RawMessage, emit func(json.RawMessage) error) (json.RawMessage, error) {
	result, err := r.Invoke(ctx, scope, name, input)
	if err == nil && emit != nil {
		err = emit(cloneJSON(result))
	}
	return result, err
}

var _ ToolRegistry = (*TrustedToolRegistry)(nil)
var _ AgentRegistry = (*BoundedAgentRegistry)(nil)
var _ StreamingAgentRegistry = (*BoundedAgentRegistry)(nil)
