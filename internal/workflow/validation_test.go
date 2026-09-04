package workflow

import (
	"encoding/json"
	"errors"
	"testing"
)

// 验证 Fenix 迁移保留的 UUID 即使以数字开头也能作为稳定 Workflow ID 发布。
func TestValidateDefinitionAcceptsImportedUUID(t *testing.T) {
	definition := validDefinition()
	definition.ID = "123e4567-e89b-12d3-a456-426614174000"
	if err := ValidateDefinition(definition, BuiltinRegistry(), builtinPolicy()); err != nil {
		t.Fatalf("ValidateDefinition() error = %v", err)
	}
}

// 验证 canonical definition 对标识、重复节点、未知类型、边端点及 Start/End 约束执行严格校验。
func TestValidateDefinitionRejectsStructuralErrors(t *testing.T) {
	tests := []struct {
		name   string
		code   string
		mutate func(*WorkflowDefinition)
	}{
		{
			name: "invalid workflow id", code: "invalid_id",
			mutate: func(definition *WorkflowDefinition) { definition.ID = "../tenant" },
		},
		{
			name: "duplicate node", code: "duplicate_node",
			mutate: func(definition *WorkflowDefinition) { definition.Nodes = append(definition.Nodes, definition.Nodes[0]) },
		},
		{
			name: "unknown node type", code: "unknown_node_type",
			mutate: func(definition *WorkflowDefinition) { definition.Nodes[1].Type = "missing" },
		},
		{
			name: "unknown edge source", code: "unknown_edge_source",
			mutate: func(definition *WorkflowDefinition) { definition.Edges[0].FromNode = "missing" },
		},
		{
			name: "duplicate edge id", code: "duplicate_edge_id",
			mutate: func(definition *WorkflowDefinition) { definition.Edges[1].ID = definition.Edges[0].ID },
		},
		{
			name: "missing start", code: "invalid_start_count",
			mutate: func(definition *WorkflowDefinition) { definition.Nodes[0].Type = NodeTypeHTTP },
		},
		{
			name: "missing end", code: "missing_end",
			mutate: func(definition *WorkflowDefinition) { definition.Nodes[3].Type = NodeTypeHTTP },
		},
		{
			name: "start incoming", code: "start_has_incoming",
			mutate: func(definition *WorkflowDefinition) {
				definition.Edges = append(definition.Edges, EdgeDefinition{ID: "finish_to_begin", FromNode: "finish", FromPort: "result", ToNode: "begin", ToPort: "input"})
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			definition := validDefinition()
			test.mutate(&definition)
			assertDefinitionCode(t, definition, builtinPolicy(), test.code)
		})
	}
}

// 验证端口、literal 和上游 output binding 的类型约束在编译边界生效。
func TestValidateDefinitionRejectsInvalidTypedBindings(t *testing.T) {
	tests := []struct {
		name   string
		code   string
		mutate func(*WorkflowDefinition)
	}{
		{
			name: "unknown edge port", code: "unknown_output_port",
			mutate: func(definition *WorkflowDefinition) { definition.Edges[0].FromPort = "missing" },
		},
		{
			name: "literal type mismatch", code: "invalid_literal",
			mutate: func(definition *WorkflowDefinition) {
				definition.Nodes[1].Inputs = map[string]InputBinding{"body": {Type: ValueTypeString, Literal: json.RawMessage(`42`)}}
			},
		},
		{
			name: "binding source not adjacent", code: "binding_without_edge",
			mutate: func(definition *WorkflowDefinition) {
				definition.Nodes[2].Inputs = map[string]InputBinding{
					"request": {Type: ValueTypeAny, Source: &OutputReference{NodeID: "begin", Port: "input"}},
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			definition := validDefinition()
			test.mutate(&definition)
			assertDefinitionCode(t, definition, builtinPolicy(), test.code)
		})
	}
}

// 验证合法的直接 output binding 会原样规范化并进入执行计划。
func TestCompilePreservesValidTypedBinding(t *testing.T) {
	definition := validDefinition()
	definition.Nodes[1].Inputs = map[string]InputBinding{
		"body": {Type: ValueTypeAny, Source: &OutputReference{NodeID: "begin", Port: "input"}},
	}
	plan, err := Compile(definition, BuiltinRegistry(), builtinPolicy())
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	node, ok := plan.Node("request")
	if !ok || node.Inputs["body"].Source == nil || node.Inputs["body"].Source.NodeID != "begin" {
		t.Fatalf("Node(request) binding = %#v", node.Inputs["body"])
	}
}

func assertDefinitionCode(t *testing.T, definition WorkflowDefinition, policy CompilePolicy, code string) {
	t.Helper()
	err := ValidateDefinition(definition, BuiltinRegistry(), policy)
	if err == nil {
		t.Fatalf("ValidateDefinition() error = nil, want %q", code)
	}
	var validationErr *ValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("ValidateDefinition() error = %T, want *ValidationError", err)
	}
	for _, issue := range validationErr.Issues {
		if issue.Code == code {
			return
		}
	}
	t.Fatalf("ValidateDefinition() issues = %#v, want %q", validationErr.Issues, code)
}
