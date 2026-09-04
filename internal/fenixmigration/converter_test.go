package fenixmigration

import (
	"errors"
	"testing"

	"github.com/pazhou/supaflow/internal/workflow"
)

// 验证静态 Fenix YAML 被转换为可编译的 canonical definition，并补齐显式 Start。
func TestConvertYAMLPreservesStaticWorkflowSemantics(t *testing.T) {
	source := []byte(`
schema_version: "1"
name: release
description: guarded request
params:
  payload: {type: object, required: true}
nodes:
  - id: request
    type: api
    url: https://example.com/release
    method: POST
    headers: {content-type: application/json}
    body: '{"ok":true}'
  - id: approve
    type: audit
    depends_on: [request]
    display_data: Confirm release
    expires_in: 3600
  - id: finish
    type: end
    depends_on: [approve]
`)
	result, err := ConvertYAML("123e4567-e89b-12d3-a456-426614174000", source, Options{})
	if err != nil {
		t.Fatalf("ConvertYAML() error = %v", err)
	}
	if len(result.Definition.Nodes) != 4 || result.Definition.Nodes[0].Type != workflow.NodeTypeStart {
		t.Fatalf("nodes = %#v", result.Definition.Nodes)
	}
	if result.Definition.Nodes[1].Type != workflow.NodeTypeHTTP || result.Definition.Nodes[2].Type != workflow.NodeTypeHumanApproval {
		t.Fatalf("mapped node types = %#v", result.Definition.Nodes)
	}
	policy := workflow.CompilePolicy{
		AllowedCapabilities: workflow.CapabilitySet{Streaming: true, Cancellable: true, Interruptible: true, Idempotent: true},
		AllowedPermissions:  workflow.PermissionSet{Network: true, Credentials: true, Filesystem: true, Compute: true},
	}
	if _, err = workflow.Compile(result.Definition, workflow.BuiltinRegistry(), policy); err != nil {
		t.Fatalf("Compile() migrated definition error = %v", err)
	}
}

// 验证 argv 形式 Shell 命令会安全引用为 POSIX exec，而不是拼接出新的 shell 语义。
func TestConvertYAMLSafelyQuotesShellArgv(t *testing.T) {
	source := []byte(`
schema_version: "1"
name: argv
nodes:
  - id: command
    type: shell
    command: [printf, "%s", "a b'c"]
`)
	result, err := ConvertYAML("flow", source, Options{})
	if err != nil {
		t.Fatalf("ConvertYAML() error = %v", err)
	}
	if got := string(result.Definition.Nodes[1].Config); got != `{"source":"exec 'printf' '%s' 'a b'\"'\"'c'"}` {
		t.Fatalf("shell config = %s", got)
	}
}

// 验证无法等价迁移的动态表达式和自定义节点会形成确定性 blocker，禁止静默改变行为。
func TestConvertYAMLRejectsUnsupportedSemantics(t *testing.T) {
	source := []byte(`
schema_version: "1"
name: unsafe
nodes:
  - id: request
    type: api
    url: https://example.com/${{ params.path }}
  - id: extension
    type: custom
    tool: slurm
    depends_on: [request]
    outputs: {artifact: {pattern: out.bin, type: file}}
`)
	_, err := ConvertYAML("flow", source, Options{})
	var blocked *BlockedError
	if !errors.As(err, &blocked) {
		t.Fatalf("ConvertYAML() error = %T %v, want *BlockedError", err, err)
	}
	if len(blocked.Blockers) != 3 || blocked.Blockers[0].NodeID != "extension" || blocked.Blockers[2].NodeID != "request" {
		t.Fatalf("blockers = %#v", blocked.Blockers)
	}
}

// 验证未知 YAML 字段直接拒绝，迁移器不得因旧数据拼写错误丢失配置。
func TestConvertYAMLRejectsUnknownFields(t *testing.T) {
	_, err := ConvertYAML("flow", []byte("schema_version: '1'\nname: bad\nunknown: true\nnodes: []\n"), Options{})
	if err == nil {
		t.Fatal("ConvertYAML() error = nil")
	}
}
