package load

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// 验证发布证据使用 nearest-rank 分位数，避免不同执行环境产生算法歧义。
func TestDurationPercentilesUseNearestRank(t *testing.T) {
	samples := make([]time.Duration, 100)
	for index := range samples {
		samples[index] = time.Duration(index+1) * time.Millisecond
	}
	got := durationPercentiles(samples)
	if got.P50Milliseconds != 50 || got.P95Milliseconds != 95 || got.P99Milliseconds != 99 {
		t.Fatalf("percentiles=%+v", got)
	}
}

// 验证 release profile 绑定精确提交并以私有权限原子写出机器可读 JSON。
func TestWritePressureEvidencePersistsCommitBoundJSON(t *testing.T) {
	directory := t.TempDir()
	commit := "0123456789abcdef0123456789abcdef01234567"
	t.Setenv("SUPAFLOW_RELEASE_EVIDENCE_DIR", directory)
	t.Setenv("SUPAFLOW_RELEASE_COMMIT", commit)
	evidence := pressureEvidence{SchemaVersion: 1, Profile: "postgres-control-plane", Revision: commit}

	path := writePressureEvidence(t, evidence)
	if path != filepath.Join(directory, "postgres-control-plane-"+commit+".json") {
		t.Fatalf("path=%q", path)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("mode=%o", info.Mode().Perm())
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var decoded pressureEvidence
	if err = json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Revision != commit || decoded.Profile != evidence.Profile || decoded.SchemaVersion != 1 {
		t.Fatalf("evidence=%+v", decoded)
	}
}

// 验证配置证据目录时缺失提交绑定会失败，避免生成无法追溯的发布材料。
func TestWritePressureEvidenceRejectsMissingCommit(t *testing.T) {
	t.Setenv("SUPAFLOW_RELEASE_EVIDENCE_DIR", t.TempDir())
	t.Setenv("SUPAFLOW_RELEASE_COMMIT", "")

	if err := validateEvidenceRevision(); err == nil {
		t.Fatal("expected missing revision to be rejected")
	}
}

// 验证同一提交的证据不能覆盖既有文件，保证发布材料可审计且不可变。
func TestPersistPressureEvidenceRejectsDuplicateRevision(t *testing.T) {
	directory := t.TempDir()
	evidence := pressureEvidence{SchemaVersion: 1, Profile: "postgres-control-plane", Revision: "0123456789abcdef0123456789abcdef01234567"}
	if _, err := persistPressureEvidence(directory, evidence); err != nil {
		t.Fatal(err)
	}
	if _, err := persistPressureEvidence(directory, evidence); !errors.Is(err, fs.ErrExist) {
		t.Fatalf("duplicate error=%v", err)
	}
}
