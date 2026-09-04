package isolation

import (
	"context"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
)

func validRequest(source string) Request {
	return Request{
		TenantID: "tenant", RunID: "run", AttemptID: "attempt", FencingToken: 1,
		Language: "shell", Source: source, Input: []byte(`null`), Timeout: time.Second, OutputLimit: 1024,
	}
}

// 验证缺省硬限制受请求超时和输出上限约束，而不是给予用户代码无限资源。
func TestResolveResourceLimitsUsesFiniteDefaults(t *testing.T) {
	request := validRequest(`printf null`)
	request.Timeout = 1500 * time.Millisecond
	request.OutputLimit = 4096
	limits, err := resolveResourceLimits(request)
	if err != nil {
		t.Fatalf("resolveResourceLimits() error = %v", err)
	}
	if limits.cpuSeconds != 2 || limits.addressSpaceBytes != 256<<20 || limits.processes != 32 || limits.openFiles != 64 || limits.fileSizeBytes != 4096 || limits.workspaceBytes != 16<<20 || limits.workspaceInodes != 1024 {
		t.Fatalf("limits = %+v", limits)
	}
}

// 验证协议只能收紧生产上限，不能借显式参数扩大 runner 的资源授权。
func TestResolveResourceLimitsRejectsValuesAboveProductionCaps(t *testing.T) {
	tests := []ResourceLimits{
		{CPUTime: 31 * time.Second},
		{AddressSpaceBytes: 256<<20 + 1},
		{Processes: 33},
		{OpenFiles: 65},
		{FileSizeBytes: 1025},
		{WorkspaceBytes: 16<<20 + 1},
		{WorkspaceInodes: 1025},
	}
	for _, limits := range tests {
		request := validRequest(`printf null`)
		request.Limits = limits
		if _, err := resolveResourceLimits(request); err == nil {
			t.Fatalf("resolveResourceLimits(%+v) unexpectedly succeeded", limits)
		}
	}
}

// 验证协议可为单次执行收紧每一种硬资源限制。
func TestResolveResourceLimitsAcceptsStricterValues(t *testing.T) {
	request := validRequest(`printf null`)
	request.Limits = ResourceLimits{
		CPUTime:           time.Second,
		AddressSpaceBytes: 128 << 20,
		Processes:         8,
		OpenFiles:         16,
		FileSizeBytes:     512,
		WorkspaceBytes:    8 << 20,
		WorkspaceInodes:   512,
	}
	limits, err := resolveResourceLimits(request)
	if err != nil {
		t.Fatalf("resolveResourceLimits() error = %v", err)
	}
	if limits.cpuSeconds != 1 || limits.addressSpaceBytes != 128<<20 || limits.processes != 8 || limits.openFiles != 16 || limits.fileSizeBytes != 512 || limits.workspaceBytes != 8<<20 || limits.workspaceInodes != 512 {
		t.Fatalf("limits = %+v", limits)
	}
}

// 验证 Shell 在独立临时 workspace 中执行并返回结构化结果。
func TestRunShellInSandbox(t *testing.T) {
	response := runWithBuilder(context.Background(), validRequest(`printf '{"ok":true}'`), directCommand)
	if response.Code != "" || string(response.Output) != `{"ok":true}` {
		t.Fatalf("response = %+v", response)
	}
}

// 验证 sandbox 默认拒绝读取宿主文件系统。
func TestRunRejectsHostFilesystemEscape(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("当前断言针对 macOS sandbox profile")
	}
	response := Run(context.Background(), validRequest(`cat /etc/passwd`))
	if response.Code == "sandbox_unavailable" || response.Code == "resource_limits_unavailable" {
		t.Skip("当前宿主无法同时提供 sandbox 与完整 hard resource limits")
	}
	if response.Code != "execution_failed" {
		t.Fatalf("response = %+v", response)
	}
}

// 验证无法提供 sandbox-local PID 限制的平台会拒绝执行，而不是只启用部分限制。
func TestRunFailsClosedWithoutCompleteResourceLimitCapability(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("当前断言针对 Darwin 缺少 per-sandbox PID limit 的能力边界")
	}
	response := Run(context.Background(), validRequest(`printf null`))
	if response.Code != "resource_limits_unavailable" {
		t.Fatalf("response = %+v", response)
	}
}

// 验证超时会终止隔离进程而不是占用 worker。
func TestRunEnforcesTimeout(t *testing.T) {
	request := validRequest(`sleep 5`)
	request.Timeout = 30 * time.Millisecond
	response := runWithBuilder(context.Background(), request, directCommand)
	if response.Code != "timeout" {
		t.Fatalf("response = %+v", response)
	}
}

// 验证主动取消会等 helper 退出后清理 attempt workspace，而不是遗留运行目录。
func TestRunCancellationCleansWorkspace(t *testing.T) {
	root := t.TempDir()
	request := validRequest(`sleep 5`)
	ctx, cancel := context.WithCancel(context.Background())
	boundary := func(_ Request, _ resolvedLimits) (*executionBoundary, error) {
		workspace, err := os.MkdirTemp(root, "attempt-")
		if err != nil {
			return nil, err
		}
		return &executionBoundary{
			workspace: workspace, sandboxWorkspace: workspace,
			configure: func(cmd *exec.Cmd) error {
				cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
				return nil
			},
			cleanup: func() { _ = os.RemoveAll(workspace) },
		}, nil
	}
	done := make(chan Response, 1)
	go func() { done <- runWithBoundary(ctx, request, directCommand, boundary) }()
	time.Sleep(30 * time.Millisecond)
	cancel()
	response := <-done
	if response.Code != "cancelled" {
		t.Fatalf("response = %+v", response)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("workspace cleanup entries=%v err=%v", entries, err)
	}
}

// 验证短进程退出后仍执行最终 workspace 配额检查，不能逃逸轮询窗口。
func TestRunChecksWorkspaceQuotaAfterProcessExit(t *testing.T) {
	request := validRequest(`printf null`)
	verified := false
	boundary := func(_ Request, _ resolvedLimits) (*executionBoundary, error) {
		workspace := t.TempDir()
		return &executionBoundary{
			workspace: workspace, sandboxWorkspace: workspace,
			configure: func(cmd *exec.Cmd) error {
				cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
				return nil
			},
			verify: func() error {
				verified = true
				return errWorkspaceQuotaExceeded
			},
			cleanup: func() {},
		}, nil
	}
	response := runWithBoundary(context.Background(), request, directCommand, boundary)
	if !verified || response.Code != "execution_failed" {
		t.Fatalf("verified=%v response=%+v", verified, response)
	}
}

// 验证输出达到上限时进程失败且截断内容不会作为成功结果返回。
func TestRunEnforcesOutputLimit(t *testing.T) {
	request := validRequest(`printf 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'`)
	request.OutputLimit = 64
	response := runWithBuilder(context.Background(), request, directCommand)
	if response.Code != "output_limit_exceeded" {
		t.Fatalf("response = %+v", response)
	}
}

// 验证宿主 secret 不会继承到隔离环境，只有显式注入值可见。
func TestRunEnvironmentAllowlistDoesNotLeakHostSecret(t *testing.T) {
	t.Setenv("SUPAFLOW_HOST_SECRET", "must-not-leak")
	request := validRequest(`printf '%s|%s' "$SAFE_VALUE" "$SUPAFLOW_HOST_SECRET"`)
	request.Environment = map[string]string{"SAFE_VALUE": "visible"}
	response := runWithBuilder(context.Background(), request, directCommand)
	if response.Code != "" || strings.Contains(string(response.Output), "must-not-leak") || string(response.Output) != `"visible|"` {
		t.Fatalf("response = %+v; process env contains secret=%t", response, os.Getenv("SUPAFLOW_HOST_SECRET") != "")
	}
}

// 验证环境凭据不能覆盖固定 sandbox 环境或注入动态加载器、Shell/Python 启动钩子。
func TestRunRejectsReservedEnvironmentBindings(t *testing.T) {
	for _, name := range []string{"HOME", "PATH", "TMPDIR", "LD_PRELOAD", "DYLD_INSERT_LIBRARIES", "BASH_ENV", "ENV", "PYTHONPATH"} {
		request := validRequest(`printf null`)
		request.Environment = map[string]string{name: "attacker-controlled"}
		response := runWithBuilder(context.Background(), request, directCommand)
		if response.Code != "invalid_environment" {
			t.Fatalf("environment %s produced response %+v", name, response)
		}
	}
}

// 验证清洁环境只包含固定边界与排序后的显式安全变量，不继承或重复宿主值。
func TestIsolatedEnvironmentIsDeterministicAndClean(t *testing.T) {
	environment := isolatedEnvironment("/workspace", map[string]string{"TOKEN_B": "b", "TOKEN_A": "a"})
	expected := []string{"HOME=/workspace", "TMPDIR=/workspace", "LANG=C", "LC_ALL=C", "PATH=/usr/bin:/bin", "TOKEN_A=a", "TOKEN_B=b"}
	if strings.Join(environment, "|") != strings.Join(expected, "|") {
		t.Fatalf("environment=%v", environment)
	}
}

func directCommand(_ string, interpreter string, args []string, _ resolvedLimits) ([]string, error) {
	return append([]string{interpreter}, args...), nil
}

// 验证未实现精确 egress policy 前所有用户代码网络访问均 fail-closed。
func TestRunRejectsNetworkCapability(t *testing.T) {
	request := validRequest(`printf null`)
	request.Network = true
	response := Run(context.Background(), request)
	if response.Code != "network_denied" {
		t.Fatalf("response = %+v", response)
	}
}
