//go:build linux

package isolation

import (
	"context"
	"os"
	"testing"
	"time"
)

func requireProductionCapabilities(t *testing.T) {
	t.Helper()
	if os.Getenv(cgroupRootEnv) == "" || os.Getenv(workspaceRootEnv) == "" {
		t.Skip("production cgroup/workspace roots are not configured")
	}
	if err := CheckCapabilities(context.Background()); err != nil {
		t.Skipf("host does not expose the complete production sandbox: %v", err)
	}
}

// 验证真实 launcher 会穿过 bwrap、cgroup v2 和受限 tmpfs，而不是 mock command builder。
func TestProductionLauncherCapabilityPath(t *testing.T) {
	requireProductionCapabilities(t)
	response := Run(context.Background(), capabilityRequest("real-launcher", "shell", `printf '{"ok":true}'`))
	if response.Code != "" || string(response.Output) != `{"ok":true}` {
		t.Fatalf("response=%+v", response)
	}
}

// 验证 pids.max 限制当前 attempt 的整个进程树，fork 耗尽不会影响 worker UID 的其他任务。
func TestProductionLauncherStopsForkExhaustion(t *testing.T) {
	requireProductionCapabilities(t)
	request := capabilityRequest("fork-limit", "shell", `for i in 1 2 3 4 5 6 7 8; do sleep 2 & done; wait; printf null`)
	request.Limits.Processes = 4
	response := Run(context.Background(), request)
	if response.Code != "execution_failed" {
		t.Fatalf("response=%+v", response)
	}
}

// 验证 workspace 的 tmpfs 总字节上限阻止多文件绕过单文件 RLIMIT_FSIZE。
func TestProductionLauncherStopsAggregateDiskExhaustion(t *testing.T) {
	requireProductionCapabilities(t)
	request := capabilityRequest("disk-limit", "shell", `i=0; while [ "$i" -lt 40 ]; do dd if=/dev/zero of="f$i" bs=65536 count=1 2>/dev/null || exit 7; i=$((i+1)); done; printf null`)
	request.OutputLimit = 1 << 20
	request.Limits.FileSizeBytes = 1 << 20
	request.Limits.WorkspaceBytes = 1 << 20
	response := Run(context.Background(), request)
	if response.Code != "execution_failed" {
		t.Fatalf("response=%+v", response)
	}
}

// 验证 inode 监控在有限周期内终止大量空文件创建。
func TestProductionLauncherStopsInodeExhaustion(t *testing.T) {
	requireProductionCapabilities(t)
	request := capabilityRequest("inode-limit", "shell", `i=0; while [ "$i" -lt 100 ]; do : > "f$i"; i=$((i+1)); done; sleep 1; printf null`)
	request.Limits.WorkspaceInodes = 16
	response := Run(context.Background(), request)
	if response.Code != "execution_failed" {
		t.Fatalf("response=%+v", response)
	}
}

// 验证主动取消沿 helper/bwrap 进程树传播并在返回前移除临时 workspace。
func TestProductionLauncherCancellationCleansWorkspace(t *testing.T) {
	requireProductionCapabilities(t)
	before, _ := os.ReadDir(os.Getenv(workspaceRootEnv))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan Response, 1)
	go func() { done <- Run(ctx, capabilityRequest("cancel-cleanup", "shell", `sleep 30`)) }()
	time.Sleep(100 * time.Millisecond)
	cancel()
	if response := <-done; response.Code != "cancelled" {
		t.Fatalf("response=%+v", response)
	}
	after, _ := os.ReadDir(os.Getenv(workspaceRootEnv))
	if len(after) != len(before) {
		t.Fatalf("workspace count before=%d after=%d", len(before), len(after))
	}
}
