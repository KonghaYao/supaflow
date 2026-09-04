// Package workflow defines Supaflow's transport- and persistence-independent
// workflow model, node registry, validation, and immutable execution plans.
package workflow

import "encoding/json"

const (
	// DefinitionSchemaV1 identifies the first canonical workflow definition schema.
	DefinitionSchemaV1 = "v1"
	// BuiltinNodeVersion is the pinned version of the first built-in node set.
	BuiltinNodeVersion = "1.0.0"
)

const (
	// NodeTypeStart starts a workflow and exposes its run input.
	NodeTypeStart = "start"
	// NodeTypeEnd terminates a successful workflow path.
	NodeTypeEnd = "end"
	// NodeTypeHTTP performs a policy-controlled HTTP request.
	NodeTypeHTTP = "http"
	// NodeTypeHumanApproval suspends a run for a human decision.
	NodeTypeHumanApproval = "human-approval"
	// NodeTypeLLM invokes a configured Eino chat model adapter.
	NodeTypeLLM = "llm"
	// NodeTypeAgent invokes a configured Eino-backed agent adapter.
	NodeTypeAgent = "agent"
	// NodeTypeTool invokes a registered Eino tool adapter.
	NodeTypeTool = "tool"
	// NodeTypeCondition selects a boolean control-flow port.
	NodeTypeCondition = "condition"
	// NodeTypeSwitch selects one named control-flow port.
	NodeTypeSwitch = "switch"
	// NodeTypeLoop bounds repeated execution of its body.
	NodeTypeLoop = "loop"
	// NodeTypeTransform applies a deterministic JSON transformation.
	NodeTypeTransform = "transform"
	// NodeTypeSubworkflow starts a child run with durable lineage.
	NodeTypeSubworkflow = "subworkflow"
	// NodeTypeDelay suspends until a deadline.
	NodeTypeDelay = "delay"
	// NodeTypeWait suspends until an external resume decision.
	NodeTypeWait = "wait"
	// NodeTypeWebhookResume suspends until an authenticated webhook resumes it.
	NodeTypeWebhookResume = "webhook-resume"
	// NodeTypeShell executes a command only through the isolation runner.
	NodeTypeShell = "shell"
	// NodeTypePython executes Python only through the isolation runner.
	NodeTypePython = "python"
)

// ValueType is the JSON value type carried by a port or binding.
type ValueType string

const (
	// ValueTypeAny accepts every JSON value.
	ValueTypeAny ValueType = "any"
	// ValueTypeString accepts JSON strings.
	ValueTypeString ValueType = "string"
	// ValueTypeNumber accepts JSON numbers.
	ValueTypeNumber ValueType = "number"
	// ValueTypeInteger accepts integral JSON numbers.
	ValueTypeInteger ValueType = "integer"
	// ValueTypeBoolean accepts JSON booleans.
	ValueTypeBoolean ValueType = "boolean"
	// ValueTypeObject accepts JSON objects.
	ValueTypeObject ValueType = "object"
	// ValueTypeArray accepts JSON arrays.
	ValueTypeArray ValueType = "array"
)

// WorkflowDefinition is the canonical, editor-independent v1 workflow model.
type WorkflowDefinition struct {
	SchemaVersion string                     `json:"schemaVersion"`
	ID            string                     `json:"id"`
	Nodes         []NodeDefinition           `json:"nodes"`
	Edges         []EdgeDefinition           `json:"edges"`
	Metadata      map[string]json.RawMessage `json:"metadata,omitempty"`
}

// NodeDefinition is one configured use of a registered node type.
type NodeDefinition struct {
	ID       string                     `json:"id"`
	Type     string                     `json:"type"`
	Version  string                     `json:"version"`
	Config   json.RawMessage            `json:"config"`
	Inputs   map[string]InputBinding    `json:"inputs,omitempty"`
	Metadata map[string]json.RawMessage `json:"metadata,omitempty"`
}

// InputBinding supplies a typed literal or an upstream node output to an input port.
// Exactly one of Literal and Source must be present.
type InputBinding struct {
	Type    ValueType        `json:"type"`
	Literal json.RawMessage  `json:"literal,omitempty"`
	Source  *OutputReference `json:"source,omitempty"`
}

// OutputReference identifies a typed output of an upstream node.
type OutputReference struct {
	NodeID string `json:"nodeId"`
	Port   string `json:"port"`
}

// EdgeDefinition is a directed control-flow edge between node ports.
type EdgeDefinition struct {
	ID       string `json:"id"`
	FromNode string `json:"fromNode"`
	FromPort string `json:"fromPort"`
	ToNode   string `json:"toNode"`
	ToPort   string `json:"toPort"`
}
