package runtime

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

// 验证失败日志的 cause 属性会渲染出适配器的真实原因，同时公共错误文本保持粗粒度：
// 否则「脚本退出码非零」与「输出超限」在日志里逐字相同，运维侧无法区分失败原因。
func TestFailureCauseCarriesAdapterReason(t *testing.T) {
	adapter := fmt.Errorf("external adapter invocation failed: %w",
		fmt.Errorf("isolated execution: %s", "output_limit_exceeded"))
	execution := &ExecutionError{Code: "adapter_failed", Retryable: true, Cause: adapter}

	cause := failureCause(execution)
	if cause == nil || !strings.Contains(cause.Error(), "isolated execution: output_limit_exceeded") {
		t.Fatalf("cause=%v", cause)
	}
	// 公共错误文本不得泄露内部原因：调用方只看得到粗粒度错误码。
	if strings.Contains(execution.Error(), "isolated execution") {
		t.Fatalf("public error leaked the cause: %q", execution.Error())
	}
	// 没有 cause 的错误必须原样返回，日志里不能出现 null。
	plain := errors.New("claim lost")
	if got := failureCause(plain); got != plain {
		t.Fatalf("cause=%v want the original error", got)
	}
}

// 验证执行器真实返回的错误形状（审计执行器用 errors.Join 包了一层）也能取出原因：
// errors.Join 只提供 Unwrap() []error，单层 Unwrap 会在包装层停下，日志里就只剩粗粒度错误码。
func TestFailureCauseUnwrapsJoinedExecutionError(t *testing.T) {
	adapter := fmt.Errorf("external adapter invocation failed: %w",
		fmt.Errorf("isolated execution: %s", "execution_failed"))
	joined := errors.Join(&ExecutionError{Code: "adapter_failed", Retryable: true, Cause: adapter}, nil)

	cause := failureCause(joined)
	if cause == nil || !strings.Contains(cause.Error(), "isolated execution: execution_failed") {
		t.Fatalf("cause=%v want the adapter reason", cause)
	}
	// 审计写入同时失败时两条原因都要留下，否则审计故障会被执行故障掩盖。
	auditErr := errors.New("audit record unavailable")
	cause = failureCause(errors.Join(&ExecutionError{Code: "adapter_failed", Retryable: true, Cause: adapter}, auditErr))
	if !strings.Contains(cause.Error(), "isolated execution: execution_failed") || !strings.Contains(cause.Error(), "audit record unavailable") {
		t.Fatalf("cause=%v want both the adapter reason and the audit failure", cause)
	}
	// 只有错误码、没有 cause 的执行错误回退为原错误，日志仍然可读。
	bare := errors.Join(&ExecutionError{Code: "adapter_failed"}, nil)
	if got := failureCause(bare); got == nil || !strings.Contains(got.Error(), "adapter_failed") {
		t.Fatalf("cause=%v want the coarse code", got)
	}
}
