package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/platformops"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
)

// 验证同一组织的「建 run」与「worker 推进」并发时不再形成 ABBA 锁环。
//
// 两个入口原本以相反顺序取同一对资源：建 run 先锁 organization_quotas/organization_quota_counters
// 再经 admission_entries 的工作提示触发器写 organization_work_queue；worker 推进先写 node_attempts
// 触发组织工作提示，再经 outbox_messages 的容量守卫锁 organization_quotas。并发交叉时 PostgreSQL 只能
// 回滚其中一方（40P01）。
//
// 判据是实例级的 pg_stat_database.deadlocks 增量：即使调用方的重试把 40P01 掩盖成一次成功，死锁
// 计数器仍会增长，因此本用例不会因为重试而被「陪跑」通过。改造前该窗口内必然出现死锁。
func TestOrganizationRuntimeGateRemovesDeadlockCycle(t *testing.T) {
	ctx, pool := admissionTestPool(t)
	organizationID, service, plan := admissionTestOrganization(t, ctx, pool, "deadlock")
	controller, err := platformops.NewAdmission(pool, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	// 配额放到远高于并发量，避免容量拒绝混进死锁计数。
	if err = controller.ConfigureQuota(ctx, platformops.Quota{
		OrganizationID: organizationID, MaxActiveRuns: 500, MaxActiveWorkers: 500,
		MaxQueueDepth: 500, MaxTriggers: 1000, MaxPendingTriggerFirings: 5000,
		MaxPendingOutboxMessages: 200000,
	}); err != nil {
		t.Fatal(err)
	}
	store := NewRuntimeStore(pool)
	before := deadlockCounter(t, ctx, pool)

	const creators = 8
	const advancers = 8
	const creatorRounds = 12
	const advanceRounds = 24
	failures := make(chan error, creators*creatorRounds+advancers*advanceRounds+2)
	var group sync.WaitGroup
	for index := range creators {
		group.Add(1)
		go func() {
			defer group.Done()
			for round := range creatorRounds {
				key := fmt.Sprintf("deadlock-run-%02d-%02d", index, round)
				if _, startErr := service.StartRun(ctx, workflowruntime.OrganizationID(organizationID), workflowruntime.StartRunRequest{
					Plan: plan, Input: json.RawMessage(`{}`), IdempotencyKey: key, Priority: round,
				}); startErr != nil {
					failures <- fmt.Errorf("start %s: %w", key, startErr)
				}
			}
		}()
	}
	for index := range advancers {
		group.Add(1)
		go func() {
			defer group.Done()
			workerID := fmt.Sprintf("deadlock-worker-%02d", index)
			for range advanceRounds {
				now := time.Now().UTC()
				work, claimErr := store.ClaimAttempt(ctx, workflowruntime.ClaimAttemptRequest{
					OrganizationID: workflowruntime.OrganizationID(organizationID), WorkerID: workerID,
					Now: now, LeaseDuration: time.Minute,
				})
				if claimErr != nil {
					failures <- fmt.Errorf("claim %s: %w", workerID, claimErr)
					continue
				}
				if work == nil {
					continue
				}
				if _, completeErr := store.CompleteAttempt(ctx, workflowruntime.CompleteAttemptRequest{
					OrganizationID: workflowruntime.OrganizationID(organizationID), RunID: work.Run.ID,
					AttemptID: work.Attempt.ID, WorkerID: workerID, FencingToken: work.Lease.FencingToken,
					Now: now.Add(time.Second), RunStatus: workflowruntime.RunSuccess,
				}); completeErr != nil {
					failures <- fmt.Errorf("complete %s: %w", work.Attempt.ID, completeErr)
				}
			}
		}()
	}
	group.Wait()
	close(failures)
	observed := make([]error, 0, cap(failures))
	for failure := range failures {
		observed = append(observed, failure)
	}
	after := deadlockCounter(t, ctx, pool)
	if after != before {
		t.Fatalf("并发窗口内 PostgreSQL 报告了 %d 次死锁（before=%d after=%d），失败调用=%v",
			after-before, before, after, observed)
	}
	if len(observed) != 0 {
		t.Fatalf("并发窗口内出现 %d 个失败调用: %v", len(observed), observed)
	}
}

// 验证「建 run」与「撤销 cancel」并发时取消既不再被回成 409，也真的落库。
//
// 取消与建 run 争用同一对配额行与工作提示行，是同一 ABBA 环的另一条边；这里的断言面向用户可见
// 结果：并发压力下取消调用要么成功并留下 cancel_requested_at，要么是 run 已被 worker 终结这类
// 语义结论，绝不出现把存储失败说成冲突的 409，窗口内也不能有死锁。
func TestOrganizationRuntimeGateKeepsCancellationDurable(t *testing.T) {
	ctx, pool := admissionTestPool(t)
	organizationID, service, plan := admissionTestOrganization(t, ctx, pool, "cancel")
	controller, err := platformops.NewAdmission(pool, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err = controller.ConfigureQuota(ctx, platformops.Quota{
		OrganizationID: organizationID, MaxActiveRuns: 500, MaxActiveWorkers: 500,
		MaxQueueDepth: 500, MaxTriggers: 1000, MaxPendingTriggerFirings: 5000,
		MaxPendingOutboxMessages: 200000,
	}); err != nil {
		t.Fatal(err)
	}
	store := NewRuntimeStore(pool)
	before := deadlockCounter(t, ctx, pool)

	const creators = 8
	const creatorRounds = 12
	const advancers = 4
	const cancellers = 4
	runIDs := make(chan string, creators*creatorRounds)
	failures := make(chan error, creators*creatorRounds+advancers*creatorRounds+cancellers*creatorRounds+2)
	var group sync.WaitGroup
	for index := range creators {
		group.Add(1)
		go func() {
			defer group.Done()
			for round := range creatorRounds {
				key := fmt.Sprintf("cancel-race-run-%02d-%02d", index, round)
				result, startErr := service.StartRun(ctx, workflowruntime.OrganizationID(organizationID), workflowruntime.StartRunRequest{
					Plan: plan, Input: json.RawMessage(`{}`), IdempotencyKey: key, Priority: 0,
				})
				if startErr != nil {
					failures <- fmt.Errorf("start %s: %w", key, startErr)
					continue
				}
				runIDs <- result.Run.ID
			}
		}()
	}
	for index := range advancers {
		group.Add(1)
		go func() {
			defer group.Done()
			workerID := fmt.Sprintf("cancel-worker-%02d", index)
			for range creatorRounds * 2 {
				now := time.Now().UTC()
				work, claimErr := store.ClaimAttempt(ctx, workflowruntime.ClaimAttemptRequest{
					OrganizationID: workflowruntime.OrganizationID(organizationID), WorkerID: workerID,
					Now: now, LeaseDuration: time.Minute,
				})
				if claimErr != nil {
					failures <- fmt.Errorf("claim %s: %w", workerID, claimErr)
					continue
				}
				if work == nil {
					continue
				}
				if _, completeErr := store.CompleteAttempt(ctx, workflowruntime.CompleteAttemptRequest{
					OrganizationID: workflowruntime.OrganizationID(organizationID), RunID: work.Run.ID,
					AttemptID: work.Attempt.ID, WorkerID: workerID, FencingToken: work.Lease.FencingToken,
					Now: now.Add(time.Second), RunStatus: workflowruntime.RunSuccess,
				}); completeErr != nil {
					failures <- fmt.Errorf("complete %s: %w", work.Attempt.ID, completeErr)
				}
			}
		}()
	}
	for range cancellers {
		group.Add(1)
		go func() {
			defer group.Done()
			for range creatorRounds {
				var runID string
				select {
				case runID = <-runIDs:
				case <-time.After(500 * time.Millisecond):
					continue
				}
				_, cancelErr := service.CancelRun(ctx, workflowruntime.OrganizationID(organizationID), runID)
				if cancelErr != nil {
					if workflowruntime.IsCode(cancelErr, workflowruntime.CodeInvalidTransition) {
						// worker 已经把这个 run 带到终态；取消不再是合法动作，这不是存储失败。
						continue
					}
					failures <- fmt.Errorf("cancel %s: %w", runID, cancelErr)
					continue
				}
				var cancelRequestedAt *time.Time
				if lookupErr := pool.QueryRow(ctx, `SELECT cancel_requested_at FROM runs WHERE organization_id=$1 AND id=$2`,
					organizationID, runID).Scan(&cancelRequestedAt); lookupErr != nil {
					failures <- fmt.Errorf("read cancel intent %s: %w", runID, lookupErr)
					continue
				}
				if cancelRequestedAt == nil {
					failures <- fmt.Errorf("取消返回成功但库里没有 cancel_requested_at: %s", runID)
				}
			}
		}()
	}
	group.Wait()
	close(failures)
	close(runIDs)
	observed := make([]error, 0, cap(failures))
	for failure := range failures {
		observed = append(observed, failure)
	}
	after := deadlockCounter(t, ctx, pool)
	if after != before {
		t.Fatalf("并发窗口内 PostgreSQL 报告了 %d 次死锁（before=%d after=%d），失败调用=%v",
			after-before, before, after, observed)
	}
	if len(observed) != 0 {
		t.Fatalf("并发窗口内出现 %d 个失败调用: %v", len(observed), observed)
	}
}

func deadlockCounter(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `SELECT deadlocks FROM pg_stat_database WHERE datname = current_database()`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
