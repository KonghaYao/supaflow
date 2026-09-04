package executor

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/pazhou/supaflow/internal/isolation"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
)

// 验证 helper 缺少真实 sandbox capability 时 worker 装配立即 fail-closed。
func TestProcessIsolationRunnerRejectsFailedCapabilityProbe(t *testing.T) {
	_, err := NewProcessIsolationRunner(ProcessIsolationConfig{
		Binary: os.Args[0], Timeout: time.Second, OutputLimit: 1024,
		CapabilityProbe: func(context.Context, string, []string) error { return errors.New("missing cgroup") },
	})
	if err == nil {
		t.Fatal("NewProcessIsolationRunner() error = nil")
	}
}

// 验证控制面仅通过独立 helper 进程传递 tenant/attempt/fencing 协议。
func TestProcessIsolationRunnerUsesFencedIPC(t *testing.T) {
	t.Setenv("SUPAFLOW_ISOLATION_HELPER", "respond")
	runner, err := NewProcessIsolationRunner(ProcessIsolationConfig{
		Binary: os.Args[0], HelperArgs: []string{"-test.run=TestIsolationHelperProcess"}, Timeout: time.Second, OutputLimit: 1024,
		CapabilityProbe: successfulCapabilityProbe,
	})
	if err != nil {
		t.Fatal(err)
	}
	output, err := runner.Run(context.Background(), IsolationRequest{
		TenantID: "tenant", RunID: "run", NodeID: "node", AttemptID: "attempt", FencingToken: 9,
		Language: "shell", Source: "printf null", Input: json.RawMessage(`null`),
	})
	if err != nil || string(output) != `{"attemptId":"attempt","fencingToken":9}` {
		t.Fatalf("output=%s err=%v", output, err)
	}
}

// 验证取消 context 会终止独立 helper 进程并返回失败。
func TestProcessIsolationRunnerPropagatesCancellation(t *testing.T) {
	t.Setenv("SUPAFLOW_ISOLATION_HELPER", "block")
	runner, _ := NewProcessIsolationRunner(ProcessIsolationConfig{
		Binary: os.Args[0], HelperArgs: []string{"-test.run=TestIsolationHelperProcess"}, Timeout: time.Second, OutputLimit: 1024,
		CapabilityProbe: successfulCapabilityProbe,
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := runner.Run(ctx, IsolationRequest{TenantID: workflowruntime.TenantID("tenant"), RunID: "run", NodeID: "node", AttemptID: "attempt", FencingToken: 1, Language: "shell", Source: "x", Input: json.RawMessage(`null`)})
	if err == nil {
		t.Fatal("Run() error = nil")
	}
}

func successfulCapabilityProbe(context.Context, string, []string) error { return nil }

// TestIsolationHelperProcess 是协议测试专用子进程入口，不在父测试进程执行用户代码。
func TestIsolationHelperProcess(t *testing.T) {
	mode := os.Getenv("SUPAFLOW_ISOLATION_HELPER")
	if mode == "" {
		return
	}
	var request isolation.Request
	if json.NewDecoder(os.Stdin).Decode(&request) != nil {
		os.Exit(2)
	}
	if mode == "block" {
		time.Sleep(5 * time.Second)
		return
	}
	output, _ := json.Marshal(map[string]any{"attemptId": request.AttemptID, "fencingToken": request.FencingToken})
	_ = json.NewEncoder(os.Stdout).Encode(isolation.Response{Output: output})
}
