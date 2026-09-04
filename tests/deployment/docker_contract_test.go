package deployment

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime caller unavailable")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

// 验证 control-plane 保持最小镜像，而 worker 镜像实际包含代码节点所需运行时。
func TestDockerfileSeparatesControlAndIsolationRuntimes(t *testing.T) {
	content, err := os.ReadFile(filepath.Join(repositoryRoot(t), "Dockerfile"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(content)
	controlStart := strings.Index(text, " AS control-plane")
	workerStart := strings.Index(text, " AS worker-runtime")
	if controlStart < 0 || workerStart < controlStart {
		t.Fatal("control-plane and worker-runtime targets are required")
	}
	control := text[controlStart:workerStart]
	worker := text[workerStart:]
	if strings.Contains(control, "bubblewrap") || strings.Contains(control, "/out/isolation") {
		t.Fatal("control-plane image contains isolation runtime")
	}
	for _, required := range []string{"bubblewrap", "python3", "/out/isolation", "SUPAFLOW_ISOLATION_CGROUP_ROOT"} {
		if !strings.Contains(worker, required) {
			t.Fatalf("worker target is missing %q", required)
		}
	}
}

// 验证生产 compose 只挂载委派 cgroup 子树，并配置只读 worker 与临时 workspace。
func TestProductionComposeDeclaresIsolationCapabilities(t *testing.T) {
	content, err := os.ReadFile(filepath.Join(repositoryRoot(t), "docker-compose.prod.yml"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(content)
	for _, required := range []string{"target: worker-runtime", "read_only: true", "SUPAFLOW_CGROUP_HOST_PATH", "target: /sys/fs/cgroup/supaflow", "SUPAFLOW_ISOLATION_WORKSPACE_INODES", "tmpfs:"} {
		if !strings.Contains(text, required) {
			t.Fatalf("production compose is missing %q", required)
		}
	}
	if strings.Contains(text, "privileged: true") {
		t.Fatal("worker must not use a privileged container")
	}
}
