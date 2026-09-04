package workflow

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
)

// 验证发布的 v1 节点契约与运行时完整 registry 一致，新增能力或节点不能静默漂移。
func TestBuiltinNodeContractMatchesRegistry(t *testing.T) {
	contents, err := os.ReadFile("../../contracts/schemas/builtin-node-types-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	artifact := struct {
		SchemaVersion string            `json:"schemaVersion"`
		Nodes         []NodeTypeVersion `json:"nodes"`
	}{SchemaVersion: "v1", Nodes: BuiltinRegistry().Entries()}
	expected, err := json.MarshalIndent(artifact, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	expected = append(expected, '\n')
	if !bytes.Equal(contents, expected) {
		t.Fatal("built-in node contract is stale; run go run ./cmd/generate-node-contract")
	}
}
