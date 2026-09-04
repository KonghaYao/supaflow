package workflow

import (
	"encoding/json"
	"testing"
)

// 验证注册表拒绝相同节点类型和版本的重复注册。
func TestRegistryRejectsDuplicateVersion(t *testing.T) {
	entry := builtinNodeTypes()[0]
	if _, err := NewRegistry(entry, entry); err == nil {
		t.Fatal("NewRegistry() error = nil, want duplicate registration error")
	}
}

// 验证注册表返回防御性副本，调用方无法改变固定节点描述。
func TestRegistryResolveReturnsDefensiveCopy(t *testing.T) {
	registry := BuiltinRegistry()
	entry, ok := registry.Resolve(NodeTypeHTTP, BuiltinNodeVersion)
	if !ok {
		t.Fatal("Resolve() ok = false")
	}
	entry.Inputs["body"] = PortDefinition{Type: ValueTypeString, Required: true}
	entry.ConfigSchema[0] = '['
	entry.SensitiveFields[0] = "/mutated"
	again, _ := registry.Resolve(NodeTypeHTTP, BuiltinNodeVersion)
	if again.Inputs["body"].Type != ValueTypeAny || !json.Valid(again.ConfigSchema) || again.SensitiveFields[0] != "/config/credentialRef" {
		t.Fatalf("registry entry mutated: %#v", again)
	}
}

// 验证内建节点投影包含执行能力、权限、限制和 JSON Schema。
func TestBuiltinRegistryProjection(t *testing.T) {
	entries := BuiltinRegistry().Entries()
	if len(entries) != 17 {
		t.Fatalf("Entries() length = %d, want 17", len(entries))
	}
	for _, entry := range entries {
		if entry.Executor.Kind == "" || entry.DefaultTimeoutMS <= 0 || entry.MaximumOutputBytes <= 0 {
			t.Fatalf("incomplete entry: %#v", entry)
		}
		if !json.Valid(entry.ConfigSchema) {
			t.Fatalf("invalid schema for %s: %s", entry.Type, entry.ConfigSchema)
		}
		if entry.SensitiveFields == nil {
			t.Fatalf("node %s omitted its sensitive-field contract", entry.Type)
		}
	}
}

// 验证 compiler adapter cache 在启动时要求与持久化 projection 逐版本完全一致。
func TestRegistryVerifiesDurableProjection(t *testing.T) {
	registry := BuiltinRegistry()
	entries := registry.Entries()
	if err := registry.VerifyProjection(entries); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(entries)
	if err != nil {
		t.Fatal(err)
	}
	var roundTripped []NodeTypeVersion
	if err = json.Unmarshal(encoded, &roundTripped); err != nil {
		t.Fatal(err)
	}
	if err = registry.VerifyProjection(roundTripped); err != nil {
		t.Fatalf("JSON projection round trip: %v", err)
	}
	entries[0].DefaultTimeoutMS++
	if err := registry.VerifyProjection(entries); err == nil {
		t.Fatal("contract drift was accepted")
	}
	if err := registry.VerifyProjection(registry.Entries()[1:]); err == nil {
		t.Fatal("missing durable version was accepted")
	}
}
