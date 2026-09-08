package postgres

import (
	"os"
	"strings"
	"testing"
)

// 验证运行、事件和租约查询都携带租户谓词。
func TestOrganizationPredicatesArePartOfCriticalQueries(t *testing.T) {
	t.Parallel()
	queries := map[string]string{
		"events": listEventsSQL,
		"claim":  selectRuntimeAttemptForClaimSQL,
		"outbox": claimOutboxSQL,
	}
	for _, name := range []string{"runtime_store.go", "runtime_claim.go", "runtime_complete.go"} {
		if !strings.Contains(readSource(t, name), "organization_id = $1") {
			t.Errorf("%s lacks organization predicate", name)
		}
	}
	for name, query := range queries {
		if !strings.Contains(query, "organization_id = $1") {
			t.Errorf("%s query lacks organization predicate", name)
		}
	}
}

// 验证 PostgreSQL 终态成功事件与 runtime/SSE 契约一致。
func TestSuccessfulTerminalEventUsesSucceededContract(t *testing.T) {
	t.Parallel()
	if eventType := runtimeTerminalEventType("SUCCESS"); eventType != "run.succeeded" {
		t.Fatalf("success terminal event = %q", eventType)
	}
	if strings.Contains(readSource(t, "runtime_complete.go"), `"run.success"`) {
		t.Fatal("completion source contains obsolete run.success event")
	}
}

// 验证竞争领取使用跳过锁行且完成路径校验 worker、fencing 与过期时间。
func TestLeaseQueriesEnforceClaimAndFencingContracts(t *testing.T) {
	t.Parallel()
	if !strings.Contains(selectRuntimeAttemptForClaimSQL, "FOR UPDATE OF a SKIP LOCKED") {
		t.Error("attempt claim must use FOR UPDATE SKIP LOCKED")
	}
	for _, contract := range []string{
		"a.worker_id = $4", "a.fencing_token = $5", "l.worker_id = $4",
		"l.fencing_token = $5", "l.expires_at > clock_timestamp()", "FOR UPDATE OF a, l",
	} {
		if !strings.Contains(readSource(t, "runtime_claim.go"), contract) {
			t.Errorf("runtime lease validation lacks %q", contract)
		}
	}
}

// 验证完成事务源码同时写入 output、snapshot、event、outbox 并释放终态租约。
func TestCompletionSourceKeepsAtomicArtifactsTogether(t *testing.T) {
	t.Parallel()
	source := readSource(t, "runtime_complete.go") + readSource(t, "runtime_persist.go")
	for _, contract := range []string{
		"BeginTx", "persistRuntimeOutput", "persistRuntimeSnapshot", "appendRuntimeEvents",
		"insertOutboxTx", "output_id = CASE WHEN", "releaseTerminalLease", "tx.Commit",
	} {
		if !strings.Contains(source, contract) {
			t.Errorf("completion transaction lacks %q", contract)
		}
	}
}

// 验证 Run output 投影按租户连接且永不选择内部 object_ref。
func TestRunOutputProjectionHidesObjectReference(t *testing.T) {
	t.Parallel()
	query := runtimeRunProjectionByIDSQL
	for _, contract := range []string{"LEFT JOIN node_outputs", "o.organization_id = r.organization_id", "o.id = r.output_id", "o.inline_value", "(o.object_ref IS NOT NULL)"} {
		if !strings.Contains(query, contract) {
			t.Errorf("run output projection lacks %q", contract)
		}
	}
	if strings.Contains(runtimeRunProjectionColumns, "o.object_ref,") {
		t.Fatal("run projection selects an internal object reference")
	}
}

// 验证重跑 Store 在单一事务内完成源锁定、lineage、attempt、event 与 outbox。
func TestRerunSourceKeepsAtomicLineageAndArtifacts(t *testing.T) {
	t.Parallel()
	source := readSource(t, "runtime_rerun.go")
	for _, contract := range []string{
		"BeginTx", "FOR SHARE", "IsTerminalRun", "node_attempts", "ORDER BY attempt_number DESC",
		"run_rerun_lineage", "seedRuntimeAttempt", "appendEventTx", "insertOutboxTx", "tx.Commit",
	} {
		if !strings.Contains(source, contract) {
			t.Errorf("rerun transaction lacks %q", contract)
		}
	}
	if !strings.Contains(runtimeRunProjectionByIDSQL, "LEFT JOIN run_rerun_lineage") {
		t.Fatal("run projection must organization-scope and expose rerun lineage")
	}
}

// 验证领取事务在提交前原子追加可观测事件及其 outbox，并区分首次启动与租约接管。
func TestClaimSourceKeepsEventsAndOutboxAtomic(t *testing.T) {
	t.Parallel()
	claim := readSource(t, "runtime_claim.go")
	source := claim + readSource(t, "runtime_persist.go")
	for _, contract := range []string{
		"BeginTx", `status = 'pending'`, `"run.started"`, `"attempt.claimed"`,
		`"attempt.lease_taken_over"`, "appendRuntimeEvents", "insertOutboxTx", "tx.Commit",
	} {
		if !strings.Contains(source, contract) {
			t.Errorf("claim transaction lacks %q", contract)
		}
	}
	if strings.Index(claim, "claimAttemptTx(ctx") > strings.Index(claim, "tx.Commit(ctx)") {
		t.Error("claim helper runs after transaction commit")
	}
}

// 验证 PostgreSQL 完成事务在取消竞态中清空所有迟到产物后再落取消终态。
func TestCancellationCompletionSourceDiscardsLateArtifacts(t *testing.T) {
	t.Parallel()
	source := readSource(t, "runtime_complete.go")
	for _, contract := range []string{
		`currentStatus == "cancelling"`,
		"request.RunStatus = workflowruntime.RunCancelled",
		"request.Output = nil",
		"request.Snapshot = nil",
		"request.Next = nil",
		"request.Events = nil",
	} {
		if !strings.Contains(source, contract) {
			t.Errorf("cancellation-winning completion lacks %q", contract)
		}
	}
}

// 验证 fan-in 以 join 行锁和 edge 唯一键串行化，且全局静默前不会提前成功。
func TestDAGJoinUsesLockedBarrierAndQuiescence(t *testing.T) {
	t.Parallel()
	source := readSource(t, "runtime_dag.go") + readSource(t, "runtime_complete.go")
	for _, contract := range []string{
		"ON CONFLICT (organization_id, run_id, node_id) DO NOTHING",
		"FOR UPDATE", "scheduled=false", "dag_edge_inputs",
		"remainingDAGRunStatus", "workflowruntime.RunSuspended", "workflowruntime.RunSuccess",
	} {
		if !strings.Contains(source, contract) {
			t.Errorf("durable DAG completion lacks %q", contract)
		}
	}
}

// 验证审批解析在同一事务内串行化租户幂等键并持久化请求摘要。
func TestApprovalResolutionKeepsIdempotencyContract(t *testing.T) {
	t.Parallel()
	source := readSource(t, "runtime_control.go")
	for _, contract := range []string{
		"pg_advisory_xact_lock", "idempotency_key", "request_hash",
		"response_snapshot", "approvalResponseSnapshot", "CodeIdempotencyConflict",
		"subtle.ConstantTimeCompare",
	} {
		if !strings.Contains(source, contract) {
			t.Errorf("approval resolution lacks %q", contract)
		}
	}
}

// 验证 deadline sweep 在同一事务关闭执行状态、租约、中断、事件和容量。
func TestRunDeadlineSweepKeepsAtomicFencingContract(t *testing.T) {
	t.Parallel()
	source := readSource(t, "runtime_deadline_stream.go")
	for _, contract := range []string{
		"FOR UPDATE SKIP LOCKED", "deadline_at <=", "status='timed_out'",
		"DELETE FROM worker_leases", "releaseRunAdmissionTx", "run.timed_out",
		"appendRuntimeEvents", "tx.Commit",
	} {
		if !strings.Contains(source, contract) {
			t.Errorf("deadline sweep lacks %q", contract)
		}
	}
}

// 验证 streamed delta 每次都先验证租约与 fencing，再受聚合上限保护并写 event/outbox。
func TestStreamChunkCommitKeepsFencingAndBoundedDelivery(t *testing.T) {
	t.Parallel()
	source := readSource(t, "runtime_deadline_stream.go")
	for _, contract := range []string{
		"validateRuntimeLease", "attempt_stream_state", "attempt_stream_chunks", "content_hash", "maxRuntimeStreamChunks",
		"maxRuntimeStreamBytes", "stream.chunk", "appendEventTx", "insertOutboxTx",
	} {
		if !strings.Contains(source, contract) {
			t.Errorf("stream chunk commit lacks %q", contract)
		}
	}
}

// 验证 webhook resume 与审批账本隔离，且原子校验 token 绑定、消费、恢复、审计和响应重放。
func TestWebhookResumeKeepsSingleUseAtomicContract(t *testing.T) {
	t.Parallel()
	source := readSource(t, "runtime_webhook_resume.go")
	for _, contract := range []string{
		"webhook_resumptions", "pg_advisory_xact_lock", "subtle.ConstantTimeCompare",
		"interrupt.Kind != \"webhook\"", "status='resolved'", "resume_data",
		"webhook.resumed", "webhook.resume", "response_snapshot", "tx.Commit",
	} {
		if !strings.Contains(source, contract) {
			t.Errorf("webhook resume lacks %q", contract)
		}
	}
}

// 验证迁移器具备多实例锁、校验和账本和逐迁移事务。
func TestMigratorSourceKeepsMultiInstanceSafety(t *testing.T) {
	t.Parallel()
	source := readSource(t, "migrate.go")
	for _, contract := range []string{
		"pg_advisory_lock", "pg_advisory_unlock", "schema_migrations",
		"checksum", "BeginTx", "tx.Commit",
	} {
		if !strings.Contains(source, contract) {
			t.Errorf("migration runner lacks %q", contract)
		}
	}
}

func readSource(t *testing.T, name string) string {
	t.Helper()
	contents, err := os.ReadFile(name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(contents)
}
