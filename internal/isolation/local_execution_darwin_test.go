//go:build darwin

package isolation

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// 验证未设置 opt-in 时 Darwin 降级 runner 保持 fail-closed，绝不静默降级执行用户代码。
func TestLocalExecutionFailsClosedWithoutOptIn(t *testing.T) {
	response := Run(context.Background(), capabilityRequest("local-no-opt-in", "shell", `printf null`))
	if response.Code != "resource_limits_unavailable" {
		t.Fatalf("response = %+v", response)
	}
}

// 验证 opt-in 后 startup capability probe 通过，这是 worker 能在 macOS 启动的关键路径。
func TestLocalExecutionPassesCapabilityProbe(t *testing.T) {
	if pythonInterpreter() == "" {
		t.Skip("host has no python interpreter")
	}
	t.Setenv(allowUnsafeLocalExecutionEnv, "1")
	if err := CheckCapabilities(context.Background()); err != nil {
		t.Fatalf("capability probe failed: %v", err)
	}
}

// 验证 opt-in 后 shell 与 python 穿过真实 launcher 执行并回传输出。
func TestLocalExecutionRunsLanguagesThroughRealLauncher(t *testing.T) {
	t.Setenv(allowUnsafeLocalExecutionEnv, "1")
	cases := []struct {
		language string
		source   string
	}{
		{language: "shell", source: `printf '{"ok":true}'`},
		{language: "python", source: `print('{"ok":true}')`},
	}
	for _, testCase := range cases {
		if testCase.language == "python" && pythonInterpreter() == "" {
			t.Log("host has no python interpreter; skipping python case")
			continue
		}
		response := Run(context.Background(), capabilityRequest("local-"+testCase.language, testCase.language, testCase.source))
		if response.Code != "" || string(response.Output) != `{"ok":true}` {
			t.Fatalf("%s response = %+v", testCase.language, response)
		}
	}
}

// 验证 opt-in 后降级 runner 仍按请求超时终止，并移除本次 attempt 的临时 workspace。
func TestLocalExecutionEnforcesTimeoutAndCleansWorkspace(t *testing.T) {
	t.Setenv(allowUnsafeLocalExecutionEnv, "1")
	root := strings.TrimSpace(os.Getenv(workspaceRootEnv))
	if root == "" {
		root = os.TempDir()
	}
	request := capabilityRequest("local-timeout", "shell", `sleep 30`)
	request.Timeout = 200 * time.Millisecond
	if response := Run(context.Background(), request); response.Code != "timeout" {
		t.Fatalf("response = %+v", response)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read workspace root %s: %v", root, err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "attempt-") {
			t.Fatalf("attempt workspace %q survived a timed out run", entry.Name())
		}
	}
}
