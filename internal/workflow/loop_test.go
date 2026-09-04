package workflow

import (
	"encoding/json"
	"testing"
)

// 验证只有闭合到有明确 maxIterations 的 Loop 回边可以通过发布校验。
func TestCompileAllowsOnlyBoundedLoopBackEdge(t *testing.T) {
	definition := WorkflowDefinition{
		SchemaVersion: DefinitionSchemaV1, ID: "bounded_loop",
		Nodes: []NodeDefinition{
			{ID: "start", Type: NodeTypeStart, Version: BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
			{ID: "loop", Type: NodeTypeLoop, Version: BuiltinNodeVersion, Config: json.RawMessage(`{"maxIterations":2}`)},
			{ID: "body", Type: NodeTypeTransform, Version: BuiltinNodeVersion, Config: json.RawMessage(`{"select":{"value":"$.value"}}`)},
			{ID: "end", Type: NodeTypeEnd, Version: BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
		},
		Edges: []EdgeDefinition{
			{ID: "enter", FromNode: "start", FromPort: "input", ToNode: "loop", ToPort: "input"},
			{ID: "body", FromNode: "loop", FromPort: "body", ToNode: "body", ToPort: "input"},
			{ID: "back", FromNode: "body", FromPort: "output", ToNode: "loop", ToPort: "input"},
			{ID: "done", FromNode: "loop", FromPort: "done", ToNode: "end", ToPort: "result"},
		},
	}
	plan, err := Compile(definition, BuiltinRegistry(), CompilePolicy{AllowedCapabilities: CapabilitySet{Cancellable: true, Interruptible: true, Idempotent: true}, AllowedPermissions: PermissionSet{Compute: true}})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	encoded, err := plan.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ParseExecutionPlan(encoded); err != nil {
		t.Fatalf("ParseExecutionPlan() error = %v", err)
	}
}
