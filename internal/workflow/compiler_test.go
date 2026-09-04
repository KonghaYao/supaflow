package workflow

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

// 验证节点与边顺序、JSON 键顺序和 UI 元数据不会改变执行计划哈希。
func TestCompileProducesDeterministicHash(t *testing.T) {
	definition := validDefinition()
	first, err := Compile(definition, BuiltinRegistry(), builtinPolicy())
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	definition.Nodes[1].Config = json.RawMessage(`{ "url": "https://example.test", "method": "GET" }`)
	definition.Nodes[0], definition.Nodes[3] = definition.Nodes[3], definition.Nodes[0]
	definition.Edges[0], definition.Edges[2] = definition.Edges[2], definition.Edges[0]
	definition.Metadata = map[string]json.RawMessage{"viewport": json.RawMessage(`{"x":99}`)}
	definition.Nodes[0].Metadata = map[string]json.RawMessage{"position": json.RawMessage(`{"x":12}`)}
	second, err := Compile(definition, BuiltinRegistry(), builtinPolicy())
	if err != nil {
		t.Fatalf("Compile(reordered) error = %v", err)
	}
	if first.Hash() != second.Hash() {
		t.Fatalf("hashes differ: %s != %s", first.Hash(), second.Hash())
	}
	if len(first.Hash()) != 64 {
		t.Fatalf("hash length = %d, want 64", len(first.Hash()))
	}
}

// 验证敏感输出策略进入不可变计划与内容哈希，发布后不能被 UI 元数据漂移绕过。
func TestCompilePinsSensitiveOutputPolicy(t *testing.T) {
	definition := validDefinition()
	definition.Nodes[0].Metadata = map[string]json.RawMessage{"sensitiveOutput": json.RawMessage(`true`)}
	plan, err := Compile(definition, BuiltinRegistry(), builtinPolicy())
	if err != nil {
		t.Fatal(err)
	}
	node, ok := plan.Node("begin")
	if !ok || !node.SensitiveOutput {
		t.Fatalf("plan node = %+v", node)
	}
	definition.Nodes[0].Metadata["sensitiveOutput"] = json.RawMessage(`false`)
	nonSensitive, err := Compile(definition, BuiltinRegistry(), builtinPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if nonSensitive.Hash() == plan.Hash() {
		t.Fatal("sensitive output policy did not affect immutable plan hash")
	}
}

// 验证保留的敏感输出元数据必须是严格布尔值，避免拼写错误静默降级为内联。
func TestCompileRejectsInvalidSensitiveOutputPolicy(t *testing.T) {
	definition := validDefinition()
	definition.Nodes[0].Metadata = map[string]json.RawMessage{"sensitiveOutput": json.RawMessage(`"true"`)}
	if _, err := Compile(definition, BuiltinRegistry(), builtinPolicy()); err == nil {
		t.Fatal("Compile() error = nil")
	}
}

// 验证已编译计划通过防御性拷贝保持内存不可变。
func TestExecutionPlanIsImmutableThroughAccessors(t *testing.T) {
	plan, err := Compile(validDefinition(), BuiltinRegistry(), builtinPolicy())
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	originalHash := plan.Hash()
	nodes := plan.Nodes()
	nodes[0].Config[0] = '['
	nodes[0].Inputs = make(map[string]InputBinding)
	nodes[0].Inputs["new"] = InputBinding{Type: ValueTypeString, Literal: json.RawMessage(`"changed"`)}
	edges := plan.Edges()
	edges[0].ToNode = "changed"
	order := plan.TopologicalOrder()
	order[0] = "changed"
	encoded, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	if plan.Hash() != originalHash || strings.Contains(string(encoded), "changed") {
		t.Fatalf("plan changed through accessor: %s", encoded)
	}
}

// 验证未知节点版本在发布编译前被拒绝。
func TestCompileRejectsUnknownNodeVersion(t *testing.T) {
	definition := validDefinition()
	definition.Nodes[1].Version = "9.9.9"
	assertValidationCode(t, definition, builtinPolicy(), "unknown_node_version")
}

// 验证 DAG 循环被静态校验拒绝。
func TestCompileRejectsCycle(t *testing.T) {
	definition := validDefinition()
	definition.Edges = append(definition.Edges, EdgeDefinition{
		ID: "approval_to_http", FromNode: "approval", FromPort: "decision", ToNode: "request", ToPort: "body",
	})
	assertValidationCode(t, definition, builtinPolicy(), "cycle_detected")
}

// 验证从 Start 不可达的节点被拒绝，即使它本身能到达 End。
func TestCompileRejectsUnreachableNode(t *testing.T) {
	definition := validDefinition()
	definition.Nodes = append(definition.Nodes, NodeDefinition{
		ID: "orphan", Type: NodeTypeHTTP, Version: BuiltinNodeVersion,
		Config: json.RawMessage(`{"method":"GET","url":"https://example.test/orphan"}`),
	})
	definition.Edges = append(definition.Edges, EdgeDefinition{
		ID: "orphan_to_end", FromNode: "orphan", FromPort: "response", ToNode: "finish", ToPort: "result",
	})
	assertValidationCode(t, definition, builtinPolicy(), "unreachable_node")
}

// 验证明文凭据字段无法进入不可变计划。
func TestCompileRejectsPlaintextSecret(t *testing.T) {
	definition := validDefinition()
	definition.Nodes[1].Config = json.RawMessage(`{"method":"GET","url":"https://example.test","headers":{"Authorization":"Bearer secret"}}`)
	assertValidationCode(t, definition, builtinPolicy(), "plaintext_secret")
}

// 验证节点请求未经发布策略授权的能力时编译失败。
func TestCompileRejectsUnauthorizedCapability(t *testing.T) {
	definition := validDefinition()
	policy := builtinPolicy()
	policy.AllowedCapabilities.Interruptible = false
	assertValidationCode(t, definition, policy, "capability_denied")
}

// 验证需要网络权限的节点无法绕过发布策略。
func TestCompileRejectsUnauthorizedPermission(t *testing.T) {
	assertValidationCode(t, validDefinition(), CompilePolicy{}, "permission_denied")
}

// 验证 HTTP 的失联租约接管能力由不可变方法策略决定，非幂等请求默认拒绝重放。
func TestCompilePinsHTTPLeaseRecoverySafety(t *testing.T) {
	tests := []struct {
		name, config string
		want         bool
	}{
		{name: "get", config: `{"method":"GET","url":"https://example.test"}`, want: true},
		{name: "post-default", config: `{"method":"POST","url":"https://example.test"}`, want: false},
		{name: "post-opt-in", config: `{"method":"POST","url":"https://example.test","retrySafe":true}`, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			definition := validDefinition()
			definition.Nodes[1].Config = json.RawMessage(test.config)
			plan, err := Compile(definition, BuiltinRegistry(), builtinPolicy())
			if err != nil {
				t.Fatal(err)
			}
			node, _ := plan.Node("request")
			if node.Capabilities.RecoverySafe != test.want {
				t.Fatalf("RecoverySafe = %v, want %v", node.Capabilities.RecoverySafe, test.want)
			}
		})
	}
}

func assertValidationCode(t *testing.T, definition WorkflowDefinition, policy CompilePolicy, code string) {
	t.Helper()
	_, err := Compile(definition, BuiltinRegistry(), policy)
	if err == nil {
		t.Fatalf("Compile() error = nil, want code %q", code)
	}
	var validationErr *ValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("Compile() error = %T, want *ValidationError: %v", err, err)
	}
	for _, issue := range validationErr.Issues {
		if issue.Code == code {
			return
		}
	}
	t.Fatalf("Compile() issues = %#v, want code %q", validationErr.Issues, code)
}

func builtinPolicy() CompilePolicy {
	return CompilePolicy{
		AllowedCapabilities: CapabilitySet{Streaming: true, Cancellable: true, Interruptible: true, Idempotent: true},
		AllowedPermissions:  PermissionSet{Network: true, Credentials: true},
	}
}

func validDefinition() WorkflowDefinition {
	return WorkflowDefinition{
		SchemaVersion: DefinitionSchemaV1,
		ID:            "order_review",
		Nodes: []NodeDefinition{
			{ID: "begin", Type: NodeTypeStart, Version: BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
			{ID: "request", Type: NodeTypeHTTP, Version: BuiltinNodeVersion, Config: json.RawMessage(`{"method":"GET","url":"https://example.test"}`)},
			{ID: "approval", Type: NodeTypeHumanApproval, Version: BuiltinNodeVersion, Config: json.RawMessage(`{"prompt":"Approve response?"}`)},
			{ID: "finish", Type: NodeTypeEnd, Version: BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
		},
		Edges: []EdgeDefinition{
			{ID: "begin_to_request", FromNode: "begin", FromPort: "input", ToNode: "request", ToPort: "body"},
			{ID: "request_to_approval", FromNode: "request", FromPort: "response", ToNode: "approval", ToPort: "request"},
			{ID: "approval_to_finish", FromNode: "approval", FromPort: "decision", ToNode: "finish", ToPort: "result"},
		},
	}
}

// 验证已发布计划可在不依赖 Registry 的情况下安全往返并保持防御性副本。
func TestParseExecutionPlanRoundTrip(t *testing.T) {
	plan, err := Compile(validDefinition(), BuiltinRegistry(), builtinPolicy())
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseExecutionPlan(encoded)
	if err != nil {
		t.Fatalf("ParseExecutionPlan() error = %v", err)
	}
	if parsed.Hash() != plan.Hash() || parsed.WorkflowID() != plan.WorkflowID() ||
		!reflect.DeepEqual(parsed.Nodes(), plan.Nodes()) ||
		!reflect.DeepEqual(parsed.Edges(), plan.Edges()) ||
		!reflect.DeepEqual(parsed.TopologicalOrder(), plan.TopologicalOrder()) {
		t.Fatalf("parsed plan differs: parsed=%s original=%s", parsed.Hash(), plan.Hash())
	}
}

// 验证持久化计划任一语义字段被篡改时都会被内容哈希拒绝。
func TestParseExecutionPlanRejectsTampering(t *testing.T) {
	plan, err := Compile(validDefinition(), BuiltinRegistry(), builtinPolicy())
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	var envelope map[string]any
	if err := json.Unmarshal(encoded, &envelope); err != nil {
		t.Fatal(err)
	}
	envelope["workflowId"] = "tampered_workflow"
	tampered, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseExecutionPlan(tampered); err == nil {
		t.Fatal("ParseExecutionPlan() accepted tampered semantics")
	}
}

// 验证即使攻击者重算内容哈希，反序列化仍拒绝与边方向冲突的拓扑顺序。
func TestParseExecutionPlanRejectsInvalidTopologicalOrder(t *testing.T) {
	plan, err := Compile(validDefinition(), BuiltinRegistry(), builtinPolicy())
	if err != nil {
		t.Fatal(err)
	}
	semantics := planSemantics{
		SchemaVersion: plan.SchemaVersion(), WorkflowID: plan.WorkflowID(),
		Nodes: plan.Nodes(), Edges: plan.Edges(), TopologicalOrder: plan.TopologicalOrder(),
	}
	semantics.TopologicalOrder[0], semantics.TopologicalOrder[1] = semantics.TopologicalOrder[1], semantics.TopologicalOrder[0]
	canonical, err := json.Marshal(semantics)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(canonical)
	encoded, err := json.Marshal(planEnvelope{planSemantics: semantics, Hash: hex.EncodeToString(digest[:])})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseExecutionPlan(encoded); err == nil {
		t.Fatal("ParseExecutionPlan() accepted edge-inconsistent topological order")
	}
}
