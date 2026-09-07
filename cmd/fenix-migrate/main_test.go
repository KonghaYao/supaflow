package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pazhou/supaflow/internal/fenixmigration"
)

// 验证 mapping 使用严格 JSON，拼写错误不会被迁移命令静默忽略。
func TestReadMappingRejectsUnknownField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mapping.json")
	if err := os.WriteFile(path, []byte(`{"organizations":{},"organizationz":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readMapping(path); err == nil {
		t.Fatal("readMapping() error = nil")
	}
}

// 验证 evidence report 以私有权限完整写入目标路径。
func TestWriteReportIsPrivate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "report.json")
	if err := writeReport(path, fenixmigration.Report{DryRun: true}); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("report mode = %o", info.Mode().Perm())
	}
}
