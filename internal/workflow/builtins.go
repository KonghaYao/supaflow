package workflow

import (
	"encoding/json"
	"fmt"
	"strings"
)

const emptyObjectSchema = `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false}`

// BuiltinRegistry returns the registry for the first durable vertical slice.
func BuiltinRegistry() *Registry {
	registry, err := NewRegistry(builtinNodeTypes()...)
	if err != nil {
		panic("workflow: invalid built-in registry: " + err.Error())
	}
	return registry
}

func builtinNodeTypes() []NodeTypeVersion {
	entries := []NodeTypeVersion{
		{
			Type: NodeTypeStart, Version: BuiltinNodeVersion,
			Capabilities:     CapabilitySet{Idempotent: true, RecoverySafe: true},
			DefaultTimeoutMS: 1000, MaximumOutputBytes: 1 << 20,
			Executor:        ExecutorBinding{Kind: "builtin.start", MinimumRunnerVersion: "1.0.0"},
			Outputs:         map[string]PortDefinition{"input": {Type: ValueTypeAny}},
			SensitiveFields: []string{},
			ConfigSchema:    json.RawMessage(emptyObjectSchema), ValidateConfig: validateEmptyConfig,
		},
		{
			Type: NodeTypeEnd, Version: BuiltinNodeVersion,
			Capabilities:     CapabilitySet{Idempotent: true, RecoverySafe: true},
			DefaultTimeoutMS: 1000, MaximumOutputBytes: 1 << 20,
			Executor:        ExecutorBinding{Kind: "builtin.end", MinimumRunnerVersion: "1.0.0"},
			Inputs:          map[string]PortDefinition{"result": {Type: ValueTypeAny}},
			SensitiveFields: []string{},
			ConfigSchema:    json.RawMessage(emptyObjectSchema), ValidateConfig: validateEmptyConfig,
		},
		{
			Type: NodeTypeHTTP, Version: BuiltinNodeVersion,
			Capabilities:     CapabilitySet{Cancellable: true},
			DefaultTimeoutMS: 30000, MaximumOutputBytes: 4 << 20,
			Permissions:     PermissionSet{Network: true, Credentials: true},
			Executor:        ExecutorBinding{Kind: "http.request", MinimumRunnerVersion: "1.0.0"},
			Inputs:          map[string]PortDefinition{"body": {Type: ValueTypeAny}},
			Outputs:         map[string]PortDefinition{"response": {Type: ValueTypeObject}},
			SensitiveFields: []string{"/config/credentialRef"},
			ConfigSchema:    json.RawMessage(`{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","required":["method","url"],"properties":{"method":{"type":"string","enum":["GET","POST","PUT","PATCH","DELETE","HEAD"]},"url":{"type":"string","minLength":1},"credentialRef":{"type":"string","minLength":1},"timeoutMs":{"type":"integer","minimum":1,"maximum":300000},"headers":{"type":"object","additionalProperties":{"type":"string"}},"retrySafe":{"type":"boolean","description":"Explicitly permits transport-level replay for non-idempotent methods when the destination enforces Idempotency-Key."}},"additionalProperties":false}`),
			ValidateConfig:  validateHTTPConfig,
		},
		{
			Type: NodeTypeHumanApproval, Version: BuiltinNodeVersion,
			Capabilities:     CapabilitySet{Cancellable: true, Interruptible: true, RecoverySafe: true},
			DefaultTimeoutMS: 86400000, MaximumOutputBytes: 64 << 10,
			Executor:        ExecutorBinding{Kind: "builtin.human-approval", MinimumRunnerVersion: "1.0.0"},
			Inputs:          map[string]PortDefinition{"request": {Type: ValueTypeAny}},
			Outputs:         map[string]PortDefinition{"decision": {Type: ValueTypeObject}},
			SensitiveFields: []string{},
			ConfigSchema:    json.RawMessage(`{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","required":["prompt"],"properties":{"prompt":{"type":"string","minLength":1,"maxLength":4000},"timeoutMs":{"type":"integer","minimum":1000,"maximum":2592000000}},"additionalProperties":false}`),
			ValidateConfig:  validateApprovalConfig,
		},
	}
	return append(entries, extendedBuiltinNodeTypes()...)
}

func validateEmptyConfig(config json.RawMessage) error {
	var value map[string]json.RawMessage
	if err := decodeStrict(config, &value); err != nil {
		return err
	}
	if len(value) != 0 {
		return fmt.Errorf("config must be an empty object")
	}
	return nil
}

func validateHTTPConfig(config json.RawMessage) error {
	var value struct {
		Method        string            `json:"method"`
		URL           string            `json:"url"`
		CredentialRef string            `json:"credentialRef,omitempty"`
		TimeoutMS     int64             `json:"timeoutMs,omitempty"`
		Headers       map[string]string `json:"headers,omitempty"`
		RetrySafe     bool              `json:"retrySafe,omitempty"`
	}
	if err := decodeStrict(config, &value); err != nil {
		return err
	}
	allowedMethods := map[string]bool{"GET": true, "POST": true, "PUT": true, "PATCH": true, "DELETE": true, "HEAD": true}
	if !allowedMethods[value.Method] {
		return fmt.Errorf("method must be a supported uppercase HTTP method")
	}
	if strings.TrimSpace(value.URL) == "" {
		return fmt.Errorf("url is required")
	}
	if value.TimeoutMS < 0 || value.TimeoutMS > 300000 {
		return fmt.Errorf("timeoutMs must be between 1 and 300000 when set")
	}
	return nil
}

func validateApprovalConfig(config json.RawMessage) error {
	var value struct {
		Prompt    string `json:"prompt"`
		TimeoutMS int64  `json:"timeoutMs,omitempty"`
	}
	if err := decodeStrict(config, &value); err != nil {
		return err
	}
	if strings.TrimSpace(value.Prompt) == "" || len(value.Prompt) > 4000 {
		return fmt.Errorf("prompt must contain between 1 and 4000 bytes")
	}
	if value.TimeoutMS != 0 && (value.TimeoutMS < 1000 || value.TimeoutMS > 2592000000) {
		return fmt.Errorf("timeoutMs must be between 1000 and 2592000000 when set")
	}
	return nil
}
