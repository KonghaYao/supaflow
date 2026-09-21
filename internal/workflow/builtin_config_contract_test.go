package workflow

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
)

// 验证每个内置节点类型的配置校验器与它已发布的 ConfigSchema 完全一致：
// required 字段缺失必须被拒绝，schema 声明的属性必须被接受，
// additionalProperties:false 的 schema 必须拒绝未声明字段。
// 断言依据直接来自 ConfigSchema，因此校验器或 schema 任一侧漂移都会在此失败。
func TestBuiltinConfigValidatorsMatchPublishedSchema(t *testing.T) {
	specs := builtinNodeTypes()
	if len(specs) == 0 {
		t.Fatal("no built-in node types to check")
	}
	for _, spec := range specs {
		spec := spec
		t.Run(spec.Type, func(t *testing.T) {
			shape := parseConfigSchema(t, spec)
			for _, name := range shape.Required {
				config := configFromKeys(t, spec, shape, shape.Declared)
				delete(config, name)
				if err := spec.ValidateConfig(encodeConfig(t, config)); err == nil {
					t.Fatalf("config without required %q was accepted", name)
				}
			}
			declared := configFromKeys(t, spec, shape, shape.Declared)
			if err := spec.ValidateConfig(encodeConfig(t, declared)); err != nil {
				t.Fatalf("config with every declared property was rejected: %v", err)
			}
			if len(shape.Required) > 0 {
				required := configFromKeys(t, spec, shape, shape.Required)
				if err := spec.ValidateConfig(encodeConfig(t, required)); err != nil {
					t.Fatalf("config with every required property was rejected: %v", err)
				}
			}
			declared["undeclaredKey"] = 1
			err := spec.ValidateConfig(encodeConfig(t, declared))
			if shape.AllowExtra && err != nil {
				t.Fatalf("schema allows additional properties but config was rejected: %v", err)
			}
			if !shape.AllowExtra && err == nil {
				t.Fatal("schema forbids additional properties but an undeclared key was accepted")
			}
		})
	}
}

type configSchemaShape struct {
	Required   []string
	Declared   []string
	Properties map[string]json.RawMessage
	AllowExtra bool
}

func parseConfigSchema(t *testing.T, spec NodeTypeVersion) configSchemaShape {
	t.Helper()
	var decoded struct {
		Required             []string                   `json:"required"`
		Properties           map[string]json.RawMessage `json:"properties"`
		AdditionalProperties json.RawMessage            `json:"additionalProperties"`
	}
	if err := json.Unmarshal(spec.ConfigSchema, &decoded); err != nil {
		t.Fatalf("node %s@%s has an unreadable config schema: %v", spec.Type, spec.Version, err)
	}
	declared := make([]string, 0, len(decoded.Properties))
	for name := range decoded.Properties {
		declared = append(declared, name)
	}
	allowExtra := true
	if string(decoded.AdditionalProperties) == "false" {
		allowExtra = false
	}
	return configSchemaShape{
		Required: decoded.Required, Declared: declared, Properties: decoded.Properties,
		AllowExtra: allowExtra,
	}
}

func configFromKeys(t *testing.T, spec NodeTypeVersion, shape configSchemaShape, keys []string) map[string]any {
	t.Helper()
	config := make(map[string]any, len(keys))
	for _, name := range keys {
		property, ok := shape.Properties[name]
		if !ok {
			t.Fatalf("node %s declares required property %q that its schema does not describe", spec.Type, name)
		}
		config[name] = schemaConfigValue(t, spec, property)
	}
	return config
}

// schemaConfigValue 生成一个满足属性 schema 的最小合法值，用于构造测试配置。
func schemaConfigValue(t *testing.T, spec NodeTypeVersion, property json.RawMessage) any {
	t.Helper()
	var shape struct {
		Type                 string          `json:"type"`
		Enum                 []any           `json:"enum"`
		Minimum              *float64        `json:"minimum"`
		Maximum              *float64        `json:"maximum"`
		MinLength            *int            `json:"minLength"`
		MinProperties        *int            `json:"minProperties"`
		Items                json.RawMessage `json:"items"`
		AdditionalProperties json.RawMessage `json:"additionalProperties"`
	}
	if err := json.Unmarshal(property, &shape); err != nil {
		t.Fatalf("node %s has an unreadable property schema: %v", spec.Type, err)
	}
	if len(shape.Enum) > 0 {
		return shape.Enum[0]
	}
	switch shape.Type {
	case "string":
		length := 1
		if shape.MinLength != nil && *shape.MinLength > length {
			length = *shape.MinLength
		}
		return strings.Repeat("a", length)
	case "integer", "number":
		value := 1.0
		if shape.Minimum != nil && value < *shape.Minimum {
			value = math.Ceil(*shape.Minimum)
		}
		if shape.Maximum != nil && value > *shape.Maximum {
			value = math.Floor(*shape.Maximum)
		}
		return int64(value)
	case "boolean":
		return true
	case "array":
		if len(shape.Items) == 0 {
			return []any{}
		}
		return []any{schemaConfigValue(t, spec, shape.Items)}
	case "object":
		value := "a"
		if len(shape.AdditionalProperties) > 0 && string(shape.AdditionalProperties) != "false" {
			value = fmt.Sprint(schemaConfigValue(t, spec, shape.AdditionalProperties))
		}
		return map[string]any{"key": value}
	default:
		return nil
	}
}

func encodeConfig(t *testing.T, config map[string]any) json.RawMessage {
	t.Helper()
	encoded, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
