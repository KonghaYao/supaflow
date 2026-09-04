package fenixmigration

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// 验证全源指纹同时覆盖数据库清理摘要与 YAML 内容，任一变化都会阻断 cutover。
func TestSnapshotFingerprintIncludesDatabaseAndYAML(t *testing.T) {
	database := strings.Repeat("a", 64)
	base := Snapshot{DatabaseFingerprint: database, Workflows: []SourceWorkflow{{
		ID: "flow", Versions: []SourceVersion{{Version: 1, YAML: []byte("name: one\n")}},
	}}}
	got, err := base.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	changedYAML := base
	changedYAML.Workflows = []SourceWorkflow{{ID: "flow", Versions: []SourceVersion{{Version: 1, YAML: []byte("name: two\n")}}}}
	other, err := changedYAML.Fingerprint()
	if err != nil || other == got {
		t.Fatalf("YAML fingerprint did not change: first=%q second=%q err=%v", got, other, err)
	}
	changedDatabase := base
	changedDatabase.DatabaseFingerprint = strings.Repeat("b", 64)
	other, err = changedDatabase.Fingerprint()
	if err != nil || other == got {
		t.Fatalf("database fingerprint did not change: first=%q second=%q err=%v", got, other, err)
	}
}

// 验证 YAML 文件只能从显式配置的 Fenix workflow 根目录读取。
func TestSourceReaderRejectsPathEscape(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "v1.yaml"), []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	reader := &SourceReader{root: root}
	if _, err := reader.readDefinition(outside, "v1.yaml"); err == nil {
		t.Fatal("readDefinition() error = nil")
	}
}

// 验证合法文件按原始字节读取，供迁移 hash 和 YAML 解析共同使用。
func TestSourceReaderReadsBoundedDefinition(t *testing.T) {
	root := t.TempDir()
	directory := filepath.Join(root, "tenant", "flow")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	want := []byte("schema_version: '1'\nname: flow\nnodes: []\n")
	if err := os.WriteFile(filepath.Join(directory, "v1.yaml"), want, 0o600); err != nil {
		t.Fatal(err)
	}
	reader := &SourceReader{root: root}
	got, err := reader.readDefinition(filepath.Join("tenant", "flow"), "v1.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("content = %q", got)
	}
}
