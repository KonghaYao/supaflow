package workflow

import (
	"encoding/json"
	"fmt"
	"strings"
)

func extendedBuiltinNodeTypes() []NodeTypeVersion {
	return []NodeTypeVersion{
		nodeSpec(NodeTypeLLM, "eino.llm", PermissionSet{Network: true, Credentials: true}, CapabilitySet{Streaming: true, Cancellable: true}, modelSchema, validateNamedAdapter),
		nodeSpec(NodeTypeAgent, "eino.agent", PermissionSet{Network: true, Credentials: true}, CapabilitySet{Streaming: true, Cancellable: true, Interruptible: true}, modelSchema, validateNamedAdapter),
		nodeSpec(NodeTypeTool, "eino.tool", PermissionSet{Network: true, Credentials: true}, CapabilitySet{Cancellable: true}, namedSchema, validateNamedAdapter),
		nodeSpec(NodeTypeCondition, "builtin.condition", PermissionSet{}, CapabilitySet{Idempotent: true, RecoverySafe: true}, conditionSchema, validateExpressionConfig),
		nodeSpec(NodeTypeSwitch, "builtin.switch", PermissionSet{}, CapabilitySet{Idempotent: true, RecoverySafe: true}, switchSchema, validateSwitchConfig),
		nodeSpec(NodeTypeLoop, "builtin.loop", PermissionSet{}, CapabilitySet{Idempotent: true, RecoverySafe: true}, loopSchema, validateLoopConfig),
		nodeSpec(NodeTypeTransform, "builtin.transform", PermissionSet{}, CapabilitySet{Idempotent: true, RecoverySafe: true}, transformSchema, validateTransformConfig),
		nodeSpec(NodeTypeSubworkflow, "builtin.subworkflow", PermissionSet{}, CapabilitySet{Cancellable: true, RecoverySafe: true, RetrySafeErrors: []string{"adapter_failed"}}, subworkflowSchema, validateSubworkflowConfig),
		nodeSpec(NodeTypeDelay, "builtin.delay", PermissionSet{}, CapabilitySet{Cancellable: true, Interruptible: true, RecoverySafe: true}, delaySchema, validateDelayConfig),
		nodeSpec(NodeTypeWait, "builtin.wait", PermissionSet{}, CapabilitySet{Cancellable: true, Interruptible: true, RecoverySafe: true}, waitSchema, validateWaitConfig),
		nodeSpec(NodeTypeWebhookResume, "builtin.webhook-resume", PermissionSet{}, CapabilitySet{Cancellable: true, Interruptible: true, RecoverySafe: true}, waitSchema, validateWaitConfig),
		nodeSpec(NodeTypeShell, "isolated.shell", PermissionSet{Compute: true, Credentials: true}, CapabilitySet{Cancellable: true}, isolationSchema, validateIsolationConfig),
		nodeSpec(NodeTypePython, "isolated.python", PermissionSet{Compute: true, Credentials: true}, CapabilitySet{Cancellable: true}, isolationSchema, validateIsolationConfig),
	}
}

func nodeSpec(nodeType, executor string, permissions PermissionSet, capabilities CapabilitySet, schema string, validator ConfigValidator) NodeTypeVersion {
	return NodeTypeVersion{
		Type: nodeType, Version: BuiltinNodeVersion, Capabilities: capabilities,
		DefaultTimeoutMS: 30000, MaximumOutputBytes: 1 << 20, Permissions: permissions,
		Executor: ExecutorBinding{Kind: executor, MinimumRunnerVersion: "1.0.0"},
		Inputs:   map[string]PortDefinition{"input": {Type: ValueTypeAny}},
		Outputs: map[string]PortDefinition{
			"output": {Type: ValueTypeAny}, "true": {Type: ValueTypeAny},
			"false": {Type: ValueTypeAny}, "body": {Type: ValueTypeAny}, "done": {Type: ValueTypeAny},
		},
		SensitiveFields: []string{},
		ConfigSchema:    json.RawMessage(schema), ValidateConfig: validator,
	}
}

const (
	namedSchema       = `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","required":["name"],"properties":{"name":{"type":"string","minLength":1}},"additionalProperties":true}`
	modelSchema       = `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","required":["name"],"properties":{"name":{"type":"string","minLength":1},"system":{"type":"string"}},"additionalProperties":true}`
	conditionSchema   = `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","required":["path","operator"],"properties":{"path":{"type":"string"},"operator":{"enum":["exists","eq","ne","gt","gte","lt","lte"]},"value":{}},"additionalProperties":false}`
	switchSchema      = `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","required":["path","cases"],"properties":{"path":{"type":"string"},"cases":{"type":"object","minProperties":1}},"additionalProperties":false}`
	loopSchema        = `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","required":["maxIterations"],"properties":{"maxIterations":{"type":"integer","minimum":1,"maximum":10000}},"additionalProperties":false}`
	transformSchema   = `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","required":["select"],"properties":{"select":{"type":"object"}},"additionalProperties":false}`
	subworkflowSchema = `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","required":["workflowId","planId","planHash"],"properties":{"workflowId":{"type":"string"},"planId":{"type":"string"},"planHash":{"type":"string"}},"additionalProperties":false}`
	delaySchema       = `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","required":["durationMs"],"properties":{"durationMs":{"type":"integer","minimum":1,"maximum":2592000000}},"additionalProperties":false}`
	waitSchema        = `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{"timeoutMs":{"type":"integer","minimum":1000,"maximum":2592000000}},"additionalProperties":false}`
	isolationSchema   = `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","required":["source"],"properties":{"source":{"type":"string","minLength":1},"args":{"type":"array","items":{"type":"string"}},"env":{"type":"array","items":{"type":"string"}},"network":{"type":"boolean"}},"additionalProperties":false}`
)

// validateNamedAdapter stays permissive on purpose: llm, agent, and tool publish
// additionalProperties:true schemas because provider adapters carry their own extra
// configuration, so only the required name is checked here.
func validateNamedAdapter(raw json.RawMessage) error {
	var cfg struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(raw, &cfg); err != nil || strings.TrimSpace(cfg.Name) == "" {
		return fmt.Errorf("name is required")
	}
	return nil
}

// conditionOperators is exactly the operator set the published condition schema
// enumerates. compareControlJSON returns false for anything it does not recognize, so a
// misspelled operator would silently keep selecting the false port for every input;
// rejecting it at publication turns that into a definition error instead.
var conditionOperators = map[string]struct{}{
	"exists": {}, "eq": {}, "ne": {}, "gt": {}, "gte": {}, "lt": {}, "lte": {},
}

// The validators below decode strictly. That is what makes each node's published
// ConfigSchema authoritative for key admission: every schema declared with
// additionalProperties:false is enforced by rejecting keys it does not list. The checks
// used to look only at the fields the executor reads, so an author who added
// {"timeoutMs": 2000} to a shell node — a key the shell schema does not declare — got a
// node that ran with the type default and no signal that the setting had been dropped.
// Required-field, range, and enum checks that were already present are kept, now with a
// message that names the offending field. Every property a schema declares must appear
// in the struct below, or strict decoding would reject a legal config.
func validateExpressionConfig(raw json.RawMessage) error {
	var cfg struct {
		Path     string          `json:"path"`
		Operator string          `json:"operator"`
		Value    json.RawMessage `json:"value,omitempty"`
	}
	if err := decodeStrict(raw, &cfg); err != nil {
		return err
	}
	if strings.TrimSpace(cfg.Path) == "" {
		return fmt.Errorf("path is required")
	}
	if _, ok := conditionOperators[cfg.Operator]; !ok {
		return fmt.Errorf("operator must be one of exists, eq, ne, gt, gte, lt, lte")
	}
	return nil
}

func validateSwitchConfig(raw json.RawMessage) error {
	var cfg struct {
		Path  string            `json:"path"`
		Cases map[string]string `json:"cases"`
	}
	if err := decodeStrict(raw, &cfg); err != nil {
		return err
	}
	if strings.TrimSpace(cfg.Path) == "" {
		return fmt.Errorf("path is required")
	}
	if len(cfg.Cases) == 0 {
		return fmt.Errorf("cases must contain at least one port mapping")
	}
	return nil
}

func validateLoopConfig(raw json.RawMessage) error {
	var cfg struct {
		MaxIterations int `json:"maxIterations"`
	}
	if err := decodeStrict(raw, &cfg); err != nil {
		return err
	}
	if cfg.MaxIterations < 1 || cfg.MaxIterations > 10000 {
		return fmt.Errorf("maxIterations must be between 1 and 10000")
	}
	return nil
}

func validateTransformConfig(raw json.RawMessage) error {
	var cfg struct {
		Select map[string]string `json:"select"`
	}
	if err := decodeStrict(raw, &cfg); err != nil {
		return err
	}
	if cfg.Select == nil {
		return fmt.Errorf("select is required")
	}
	return nil
}

func validateSubworkflowConfig(raw json.RawMessage) error {
	var cfg struct {
		WorkflowID string `json:"workflowId"`
		PlanID     string `json:"planId"`
		PlanHash   string `json:"planHash"`
	}
	if err := decodeStrict(raw, &cfg); err != nil {
		return err
	}
	// Checked in declaration order rather than by iterating a map, so the reported field
	// is stable for the same definition.
	switch {
	case strings.TrimSpace(cfg.WorkflowID) == "":
		return fmt.Errorf("workflowId is required")
	case strings.TrimSpace(cfg.PlanID) == "":
		return fmt.Errorf("planId is required")
	case strings.TrimSpace(cfg.PlanHash) == "":
		return fmt.Errorf("planHash is required")
	}
	return nil
}

func validateDelayConfig(raw json.RawMessage) error {
	var cfg struct {
		DurationMS int64 `json:"durationMs"`
	}
	if err := decodeStrict(raw, &cfg); err != nil {
		return err
	}
	if cfg.DurationMS < 1 || cfg.DurationMS > 2592000000 {
		return fmt.Errorf("durationMs must be between 1 and 2592000000")
	}
	return nil
}

func validateWaitConfig(raw json.RawMessage) error {
	var cfg struct {
		TimeoutMS int64 `json:"timeoutMs,omitempty"`
	}
	if err := decodeStrict(raw, &cfg); err != nil {
		return err
	}
	if cfg.TimeoutMS != 0 && (cfg.TimeoutMS < 1000 || cfg.TimeoutMS > 2592000000) {
		return fmt.Errorf("timeoutMs must be between 1000 and 2592000000 when set")
	}
	return nil
}

func validateIsolationConfig(raw json.RawMessage) error {
	var cfg struct {
		Source  string   `json:"source"`
		Args    []string `json:"args,omitempty"`
		Env     []string `json:"env,omitempty"`
		Network bool     `json:"network,omitempty"`
	}
	if err := decodeStrict(raw, &cfg); err != nil {
		return err
	}
	if strings.TrimSpace(cfg.Source) == "" {
		return fmt.Errorf("source is required")
	}
	return nil
}
