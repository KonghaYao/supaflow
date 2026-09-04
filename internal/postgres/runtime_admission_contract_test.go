package postgres

import (
	"os"
	"strings"
	"testing"
)

// 验证生产 Run 存储把创建、领取、终态释放和取消都绑定到同一准入状态机。
func TestRuntimeAdmissionSQLContract(t *testing.T) {
	files := map[string]string{
		"create admits atomically":        "runtime_store.go",
		"claim requires active admission": "runtime_claim.go",
		"success releases capacity":       "runtime_complete.go",
		"failure releases capacity":       "runtime_fail.go",
		"idle cancel releases capacity":   "runtime_control.go",
	}
	wants := map[string]string{
		"create admits atomically":        "admitRunTx(ctx, tx",
		"claim requires active admission": "admission_entries",
		"success releases capacity":       "releaseRunAdmissionTx(ctx, tx",
		"failure releases capacity":       "releaseRunAdmissionTx(ctx, tx",
		"idle cancel releases capacity":   "releaseRunAdmissionTx(ctx, tx",
	}
	for name, file := range files {
		content, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(content), wants[name]) {
			t.Errorf("%s missing %q", name, wants[name])
		}
	}
}

// 验证迁移为历史非终态 Run 回填准入，并以单一优先级列约束公开范围。
func TestRuntimeAdmissionMigrationContract(t *testing.T) {
	content, err := os.ReadFile("../../migrations/000010_runtime_admission.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(content)
	for _, want := range []string{
		"ADD COLUMN priority", "priority BETWEEN -1000 AND 1000",
		"INSERT INTO tenant_quotas", "INSERT INTO admission_entries",
		"resource_type = 'run'", "status NOT IN ('success', 'failed', 'error', 'cancelled')",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("migration missing %q", want)
		}
	}
}
