package workflow

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

// 验证 shadow 计划仅放行已审查的纯内建节点，并要求所有外部资源节点显式绑定固定 stub。
func TestValidateShadowStubBindingsFailsClosed(t *testing.T) {
	definition := WorkflowDefinition{
		SchemaVersion: DefinitionSchemaV1,
		ID:            "shadow_flow",
		Nodes: []NodeDefinition{
			{ID: "start", Type: NodeTypeStart, Version: BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
			{ID: "request", Type: NodeTypeHTTP, Version: BuiltinNodeVersion, Config: json.RawMessage(`{"method":"POST","url":"https://example.com/orders"}`)},
			{ID: "end", Type: NodeTypeEnd, Version: BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
		},
		Edges: []EdgeDefinition{
			{ID: "to_request", FromNode: "start", FromPort: "input", ToNode: "request", ToPort: "body"},
			{ID: "to_end", FromNode: "request", FromPort: "response", ToNode: "end", ToPort: "result"},
		},
	}
	plan, err := Compile(definition, BuiltinRegistry(), CompilePolicy{
		AllowedCapabilities: CapabilitySet{Cancellable: true, Idempotent: true},
		AllowedPermissions:  PermissionSet{Network: true, Credentials: true},
	})
	if err != nil {
		t.Fatal(err)
	}

	err = ValidateShadowStubBindings(plan, nil)
	var violation *ShadowBindingError
	if !errors.As(err, &violation) || violation.Kind != ShadowBindingMissing ||
		!reflect.DeepEqual(violation.NodeIDs, []string{"request"}) {
		t.Fatalf("missing binding error = %#v", err)
	}
	if err = ValidateShadowStubBindings(plan, map[string]string{"request": ShadowStubExecutorKind}); err != nil {
		t.Fatalf("valid bindings rejected: %v", err)
	}
	if err = ValidateShadowStubBindings(plan, map[string]string{"request": "custom.stub"}); !errors.As(err, &violation) || violation.Kind != ShadowBindingInvalid {
		t.Fatalf("unreviewed stub error = %#v", err)
	}
	if err = ValidateShadowStubBindings(plan, map[string]string{"request": ShadowStubExecutorKind, "start": ShadowStubExecutorKind}); !errors.As(err, &violation) || violation.Kind != ShadowBindingInvalid {
		t.Fatalf("extra safe-node binding error = %#v", err)
	}
}

// 验证未知执行器即使未声明资源权限也默认视为副作用节点，防止注册表漏标造成 shadow 逃逸。
func TestShadowStubRequiredDefaultsUnknownExecutorsToUnsafe(t *testing.T) {
	if !ShadowStubRequired(PlanNode{Executor: ExecutorBinding{Kind: "custom.maybe-side-effect"}}) {
		t.Fatal("unknown executor was treated as side-effect free")
	}
	if ShadowStubRequired(PlanNode{Executor: ExecutorBinding{Kind: "builtin.transform"}}) {
		t.Fatal("reviewed pure executor unexpectedly requires a stub")
	}
	if !ShadowStubRequired(PlanNode{
		Permissions: PermissionSet{Network: true},
		Executor:    ExecutorBinding{Kind: "builtin.transform"},
	}) {
		t.Fatal("resource permission did not override the pure executor allowlist")
	}
}

// 验证完全由纯内建控制节点组成的计划无需伪造空 stub 节点即可进入 shadow 模式。
func TestValidateShadowStubBindingsAllowsSideEffectFreePlan(t *testing.T) {
	plan, err := Compile(WorkflowDefinition{
		SchemaVersion: DefinitionSchemaV1,
		ID:            "pure_shadow_flow",
		Nodes: []NodeDefinition{
			{ID: "start", Type: NodeTypeStart, Version: BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
			{ID: "end", Type: NodeTypeEnd, Version: BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
		},
		Edges: []EdgeDefinition{{ID: "to_end", FromNode: "start", FromPort: "input", ToNode: "end", ToPort: "result"}},
	}, BuiltinRegistry(), CompilePolicy{AllowedCapabilities: CapabilitySet{Idempotent: true}})
	if err != nil {
		t.Fatal(err)
	}
	if err = ValidateShadowStubBindings(plan, nil); err != nil {
		t.Fatalf("side-effect-free plan rejected: %v", err)
	}
}
