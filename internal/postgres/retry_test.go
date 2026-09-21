package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/platformops"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
)

// 验证瞬态失败与容量拒绝被翻译成诚实的运行时码：死锁/序列化失败（40P01/40001）与连接类故障
// （08xxx、57P03）是「服务端这次没做成」，报 service_unavailable 并给出 Retry-After；容量门禁
// （53400）报 quota_exceeded（HTTP 429），不是 conflict。未知存储故障不猜语义，返回普通错误，
// 由 HTTP 面按 500 + 服务端日志回答，而不是继续说成幂等冲突。
func TestRuntimePersistenceErrorClassifiesTransientAndCapacityFailures(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		wantCode  workflowruntime.ErrorCode
		wantRetry bool
		wantPlain bool
	}{
		{name: "死锁 40P01", err: &pgconn.PgError{Code: "40P01", Message: "deadlock detected"}, wantCode: workflowruntime.CodeUnavailable, wantRetry: true},
		{name: "被包装的死锁", err: fmt.Errorf("persist run admission: %w", &pgconn.PgError{Code: "40P01"}), wantCode: workflowruntime.CodeUnavailable, wantRetry: true},
		{name: "序列化失败 40001", err: &pgconn.PgError{Code: "40001"}, wantCode: workflowruntime.CodeUnavailable, wantRetry: true},
		{name: "连接异常 08006", err: &pgconn.PgError{Code: "08006", Message: "connection failure"}, wantCode: workflowruntime.CodeUnavailable, wantRetry: true},
		{name: "数据库正在关闭 57P03", err: &pgconn.PgError{Code: "57P03"}, wantCode: workflowruntime.CodeUnavailable, wantRetry: true},
		{name: "提交结果未知", err: workflowruntime.MarkCommitOutcomeUnknown(&pgconn.PgError{Code: "40P01"}), wantCode: workflowruntime.CodeUnavailable, wantRetry: true},
		{name: "容量门禁 53400", err: &pgconn.PgError{Code: "53400", Message: "tenant pending outbox capacity exceeded"}, wantCode: workflowruntime.CodeQuotaExceeded, wantRetry: true},
		{name: "组织配额未配置 23514", err: &pgconn.PgError{Code: "23514"}, wantPlain: true},
		{name: "非 Postgres 错误", err: errors.New("boom"), wantPlain: true},
	}
	for _, testCase := range cases {
		classified := runtimePersistenceError("could not admit run", testCase.err)
		if testCase.wantPlain {
			var runtimeErr *workflowruntime.Error
			if errors.As(classified, &runtimeErr) {
				t.Fatalf("%s classified=%v want a plain server failure", testCase.name, runtimeErr)
			}
			if !strings.Contains(classified.Error(), "could not admit run") {
				t.Fatalf("%s lost the operation label: %v", testCase.name, classified)
			}
			continue
		}
		var runtimeErr *workflowruntime.Error
		if !errors.As(classified, &runtimeErr) || runtimeErr.Code != testCase.wantCode {
			t.Fatalf("%s classified=%v want code %s", testCase.name, classified, testCase.wantCode)
		}
		if gotRetry := runtimeErr.RetryAfter > 0; gotRetry != testCase.wantRetry {
			t.Fatalf("%s RetryAfter=%v want hint=%t", testCase.name, runtimeErr.RetryAfter, testCase.wantRetry)
		}
		// 对外错误不得泄露内部实现细节。
		if strings.Contains(runtimeErr.Message, "pg_") || strings.Contains(runtimeErr.Message, "organization_quotas") {
			t.Fatalf("%s leaked internals: %s", testCase.name, runtimeErr.Message)
		}
	}
}

// 验证重试有界且只重试瞬态失败：死锁/序列化失败最多尝试 transientRetryAttempts 次并返回最后一次
// 的 cause；提交结果未知、容量拒绝、语义结论与非 Postgres 错误都只尝试一次——一次可能已经提交的
// 写入不能重复执行，而语义结论重试也不会改变。
func TestRetryOnTransientIsBoundedAndNeverRepeatsConclusions(t *testing.T) {
	deadlock := &pgconn.PgError{Code: "40P01", Message: "deadlock detected"}
	cases := []struct {
		name      string
		failures  int
		err       error
		wantCalls int
		wantError bool
	}{
		{name: "一直死锁时耗尽重试", failures: -1, err: deadlock, wantCalls: transientRetryAttempts, wantError: true},
		{name: "第二次尝试成功", failures: 1, err: deadlock, wantCalls: 2},
		{name: "序列化失败同样重试到成功", failures: 2, err: &pgconn.PgError{Code: "40001"}, wantCalls: 3},
		{name: "提交结果未知不重试", failures: -1, err: workflowruntime.MarkCommitOutcomeUnknown(deadlock), wantCalls: 1, wantError: true},
		{name: "容量拒绝不重试", failures: -1, err: &workflowruntime.Error{Code: workflowruntime.CodeQuotaExceeded, Message: "organization delivery capacity is exhausted", Cause: &pgconn.PgError{Code: "53400"}}, wantCalls: 1, wantError: true},
		{name: "语义冲突不重试", failures: -1, err: &workflowruntime.Error{Code: workflowruntime.CodeConflict, Message: "idempotency key was used for a different request"}, wantCalls: 1, wantError: true},
		{name: "非 Postgres 错误不重试", failures: -1, err: errors.New("context deadline exceeded"), wantCalls: 1, wantError: true},
	}
	for _, testCase := range cases {
		calls := 0
		var last error
		started := time.Now()
		err := retryOnTransient(context.Background(), func(context.Context) error {
			calls++
			if testCase.failures < 0 || calls <= testCase.failures {
				last = testCase.err
				return testCase.err
			}
			last = nil
			return nil
		})
		if calls != testCase.wantCalls {
			t.Fatalf("%s calls=%d want=%d", testCase.name, calls, testCase.wantCalls)
		}
		if testCase.wantError != (err != nil) {
			t.Fatalf("%s err=%v", testCase.name, err)
		}
		if err != nil && !errors.Is(err, testCase.err) {
			t.Fatalf("%s lost the cause: %v", testCase.name, err)
		}
		if err != nil && err != last {
			t.Fatalf("%s returned an earlier failure instead of the last one", testCase.name)
		}
		// 有界：尝试与退避必须留在预算内，不能让 HTTP 请求一直挂住。
		if elapsed := time.Since(started); elapsed > transientRetryBudget+250*time.Millisecond {
			t.Fatalf("%s took %v beyond the retry budget", testCase.name, elapsed)
		}
	}

	// 调用方已取消时不再等待，立即把失败交出去。
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	if err := retryOnTransient(canceled, func(context.Context) error { calls++; return deadlock }); err == nil || calls != 1 {
		t.Fatalf("canceled context calls=%d err=%v", calls, err)
	}
}

// 验证 pending outbox 容量耗尽时建 run 报 quota_exceeded（HTTP 429）而不是 conflict，并在容量释放
// 后成功：容量门禁说的是「现在不行」，报 conflict 会让调用方以为幂等键冲突而放弃，实际稍后重试
// 就能成功。此前 insertOutboxTx 撞到 53400 被 createRuntimeRunTx 包成 conflict。
func TestCreateRunReportsCapacityExhaustionAsQuotaExceeded(t *testing.T) {
	ctx, pool := admissionTestPool(t)
	organizationID, service, plan := admissionTestOrganization(t, ctx, pool, "retry_capacity")
	admission, err := platformops.NewAdmission(pool, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	// 建一发 run 会写两条 pending outbox（run.created 与 run.admitted），所以上限 2 允许一发。
	if err = admission.ConfigureQuota(ctx, platformops.Quota{OrganizationID: organizationID, MaxActiveRuns: 100,
		MaxActiveWorkers: 10, MaxQueueDepth: 100, MaxTriggers: 100, MaxPendingTriggerFirings: 100, MaxPendingOutboxMessages: 2}); err != nil {
		t.Fatal(err)
	}
	startAdmissionRun(t, ctx, service, organizationID, plan, "capacity-fill-0001", 0)
	overflow := workflowruntime.StartRunRequest{Plan: plan, Input: []byte(`{}`), IdempotencyKey: "capacity-overflow-0001"}
	if _, err = service.StartRun(ctx, workflowruntime.OrganizationID(organizationID), overflow); !workflowruntime.IsCode(err, workflowruntime.CodeQuotaExceeded) {
		t.Fatalf("overflow err=%v want quota_exceeded", err)
	}
	if _, err = pool.Exec(ctx, `UPDATE outbox_messages SET delivered_at=clock_timestamp() WHERE organization_id=$1`, organizationID); err != nil {
		t.Fatal(err)
	}
	result, err := service.StartRun(ctx, workflowruntime.OrganizationID(organizationID), overflow)
	if err != nil || !result.Created {
		t.Fatalf("after capacity was released result=%+v err=%v", result, err)
	}
}

// 验证同一组织并发建 run 时不会把瞬态失败说成冲突：这条链路在死锁与序列化失败下都会重开事务
// 重试，重试耗尽的结局必须是 service_unavailable（HTTP 503），任何调用方都不该收到 conflict ——
// 收到 conflict 会理解成幂等冲突而放弃重试，而那一发 run 根本没建。并发争用本身是概率事件，
// 所以这里只断言「不许出现 conflict」这个不变量（真出现瞬态失败时同样成立）。
func TestConcurrentCreateRunNeverReportsTransientFailureAsConflict(t *testing.T) {
	ctx, pool := serializableAdmissionPool(t)
	organizationID, service, plan := admissionTestOrganization(t, ctx, pool, "retry_transient")
	const workers = 8
	var waitGroup sync.WaitGroup
	failures := make(chan error, workers)
	for index := range workers {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			key := fmt.Sprintf("concurrent-run-%08d", index)
			if _, err := service.StartRun(ctx, workflowruntime.OrganizationID(organizationID), workflowruntime.StartRunRequest{
				Plan: plan, Input: []byte(`{}`), IdempotencyKey: key}); err != nil {
				failures <- err
			}
		}()
	}
	waitGroup.Wait()
	close(failures)
	for err := range failures {
		if workflowruntime.IsCode(err, workflowruntime.CodeConflict) || workflowruntime.IsCode(err, workflowruntime.CodeIdempotencyConflict) {
			t.Fatalf("transient failure reported as a conflict: %v", err)
		}
		if !workflowruntime.IsCode(err, workflowruntime.CodeUnavailable) && !workflowruntime.IsCode(err, workflowruntime.CodeQuotaExceeded) {
			t.Fatalf("unexpected failure code: %v", err)
		}
	}
}

// serializableAdmissionPool 让同一批连接以 serializable 隔离级别工作，使并发争用产生真实的
// 40001 序列化失败（而不是靠人造错误），用来验证重试与分类在真实数据库上成立。
func serializableAdmissionPool(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	databaseURL := testDatabaseURL(t)
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["default_transaction_isolation"] = "serializable"
	ctx := context.Background()
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	migrations, err := LoadMigrations(os.DirFS("../.."), "migrations")
	if err != nil {
		t.Fatal(err)
	}
	if err = NewMigrator(pool).Up(ctx, migrations); err != nil {
		t.Fatal(err)
	}
	return ctx, pool
}

// testDatabaseURL 返回一次性数据库连接串，未配置时跳过整个用例。
func testDatabaseURL(t *testing.T) string {
	t.Helper()
	databaseURL := os.Getenv("SUPAFLOW_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("SUPAFLOW_TEST_DATABASE_URL is not set")
	}
	return databaseURL
}
