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

func validateNamedAdapter(raw json.RawMessage) error {
	var cfg struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(raw, &cfg); err != nil || strings.TrimSpace(cfg.Name) == "" {
		return fmt.Errorf("name is required")
	}
	return nil
}

func validateExpressionConfig(raw json.RawMessage) error {
	var cfg struct{ Path, Operator string }
	if err := json.Unmarshal(raw, &cfg); err != nil || cfg.Operator == "" {
		return fmt.Errorf("path and operator are required")
	}
	return nil
}

func validateSwitchConfig(raw json.RawMessage) error {
	var cfg struct {
		Path  string
		Cases map[string]string
	}
	if err := json.Unmarshal(raw, &cfg); err != nil || len(cfg.Cases) == 0 {
		return fmt.Errorf("path and cases are required")
	}
	return nil
}

func validateLoopConfig(raw json.RawMessage) error {
	var cfg struct {
		MaxIterations int `json:"maxIterations"`
	}
	if err := json.Unmarshal(raw, &cfg); err != nil || cfg.MaxIterations < 1 || cfg.MaxIterations > 10000 {
		return fmt.Errorf("maxIterations must be between 1 and 10000")
	}
	return nil
}

func validateTransformConfig(raw json.RawMessage) error {
	var cfg struct {
		Select map[string]string `json:"select"`
	}
	if err := json.Unmarshal(raw, &cfg); err != nil || cfg.Select == nil {
		return fmt.Errorf("select is required")
	}
	return nil
}

func validateSubworkflowConfig(raw json.RawMessage) error {
	return requireStringFields(raw, "workflowId", "planId", "planHash")
}
func validateDelayConfig(raw json.RawMessage) error {
	var cfg struct {
		DurationMS int64 `json:"durationMs"`
	}
	if err := json.Unmarshal(raw, &cfg); err != nil || cfg.DurationMS < 1 || cfg.DurationMS > 2592000000 {
		return fmt.Errorf("durationMs is invalid")
	}
	return nil
}
func validateWaitConfig(raw json.RawMessage) error {
	var cfg map[string]any
	return json.Unmarshal(raw, &cfg)
}
func validateIsolationConfig(raw json.RawMessage) error { return requireStringFields(raw, "source") }

func requireStringFields(raw json.RawMessage, fields ...string) error {
	var cfg map[string]json.RawMessage
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return err
	}
	for _, field := range fields {
		var value string
		if json.Unmarshal(cfg[field], &value) != nil || strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", field)
		}
	}
	return nil
}
