package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/platformops"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
)

// 验证取消路径把存储层的容量拒绝说成「现在不行」，而不是「你的请求有语义冲突」。
//
// 取消事务会在 organization_work_queue 事件之外向 outbox_messages 写投递行，容量守卫因此可能以
// 53400 拒绝。改造前 runtime_control.go 把这个失败原样包成 CodeConflict，调用方读到的 409 是
// 「别重试，这是你的语义错误」，而真实的结论是「组织投递容量已满，稍后重试」。
//
// 本用例把组织的 pending outbox 上限压到当前积压数，让取消事务里的 outbox 写入必然被拒，
// 然后断言错误码是容量类而不是冲突类：改造前这里会拿到 CodeConflict。
func TestCancelPathReportsCapacityRejectionAsQuotaNotConflict(t *testing.T) {
	ctx, pool := admissionTestPool(t)
	organizationID, service, plan := admissionTestOrganization(t, ctx, pool, "cancel-class")
	controller, err := platformops.NewAdmission(pool, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err = controller.ConfigureQuota(ctx, platformops.Quota{
		OrganizationID: organizationID, MaxActiveRuns: 10, MaxActiveWorkers: 10, MaxQueueDepth: 10,
		MaxTriggers: 1000, MaxPendingTriggerFirings: 5000, MaxPendingOutboxMessages: 1000,
	}); err != nil {
		t.Fatal(err)
	}
	run := startAdmissionRun(t, ctx, service, organizationID, plan, "cancel-class-run", 0)
	if err = controller.ConfigureQuota(ctx, platformops.Quota{
		OrganizationID: organizationID, MaxActiveRuns: 10, MaxActiveWorkers: 10, MaxQueueDepth: 10,
		MaxTriggers: 1000, MaxPendingTriggerFirings: 5000,
		MaxPendingOutboxMessages: pendingOutboxCount(t, ctx, pool, organizationID),
	}); err != nil {
		t.Fatal(err)
	}
	_, cancelErr := service.CancelRun(ctx, workflowruntime.OrganizationID(organizationID), run.ID)
	if cancelErr == nil {
		t.Fatal("容量已满时取消仍然成功，用例前提不成立")
	}
	if workflowruntime.IsCode(cancelErr, workflowruntime.CodeConflict) {
		t.Fatalf("取消把存储容量拒绝报成了 conflict: %v", cancelErr)
	}
	if !workflowruntime.IsCode(cancelErr, workflowruntime.CodeQuotaExceeded) {
		t.Fatalf("取消的错误码不是容量类: %v", cancelErr)
	}
}

// 验证 worker 推进路径做同样的区分。
//
// 完成事务在提交事件时需要写 outbox_messages，容量守卫以 53400 拒绝时，改造前 runtime_complete.go
// 回的是 CodeConflict（第六轮现场日志里的 "conflict: could not persist completion events" 正是
// 这一处把 40P01 说成了冲突）。这里断言同一种存储拒绝被报成容量类，而不是冲突类。
func TestWorkerAdvanceReportsCapacityRejectionAsQuotaNotConflict(t *testing.T) {
	ctx, pool := admissionTestPool(t)
	organizationID, service, plan := admissionTestOrganization(t, ctx, pool, "advance-class")
	controller, err := platformops.NewAdmission(pool, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err = controller.ConfigureQuota(ctx, platformops.Quota{
		OrganizationID: organizationID, MaxActiveRuns: 10, MaxActiveWorkers: 10, MaxQueueDepth: 10,
		MaxTriggers: 1000, MaxPendingTriggerFirings: 5000, MaxPendingOutboxMessages: 1000,
	}); err != nil {
		t.Fatal(err)
	}
	run := startAdmissionRun(t, ctx, service, organizationID, plan, "advance-class-run", 0)
	store := NewRuntimeStore(pool)
	now := time.Now().UTC()
	work, err := store.ClaimAttempt(ctx, workflowruntime.ClaimAttemptRequest{
		OrganizationID: workflowruntime.OrganizationID(organizationID), WorkerID: "advance-class-worker",
		Now: now, LeaseDuration: time.Minute,
	})
	if err != nil || work == nil || work.Run.ID != run.ID {
		t.Fatalf("claim=%+v err=%v", work, err)
	}
	if err = controller.ConfigureQuota(ctx, platformops.Quota{
		OrganizationID: organizationID, MaxActiveRuns: 10, MaxActiveWorkers: 10, MaxQueueDepth: 10,
		MaxTriggers: 1000, MaxPendingTriggerFirings: 5000,
		MaxPendingOutboxMessages: pendingOutboxCount(t, ctx, pool, organizationID),
	}); err != nil {
		t.Fatal(err)
	}
	_, completeErr := store.CompleteAttempt(ctx, workflowruntime.CompleteAttemptRequest{
		OrganizationID: workflowruntime.OrganizationID(organizationID), RunID: run.ID,
		AttemptID: work.Attempt.ID, WorkerID: "advance-class-worker", FencingToken: work.Lease.FencingToken,
		Now: now.Add(time.Second), RunStatus: workflowruntime.RunSuccess,
	})
	if completeErr == nil {
		t.Fatal("容量已满时完成仍然成功，用例前提不成立")
	}
	if workflowruntime.IsCode(completeErr, workflowruntime.CodeConflict) {
		t.Fatalf("推进把存储容量拒绝报成了 conflict: %v", completeErr)
	}
	if !workflowruntime.IsCode(completeErr, workflowruntime.CodeQuotaExceeded) {
		t.Fatalf("推进的错误码不是容量类: %v", completeErr)
	}
	var attemptStatus string
	if err = pool.QueryRow(ctx, `SELECT status FROM node_attempts WHERE organization_id=$1 AND id=$2`,
		organizationID, work.Attempt.ID).Scan(&attemptStatus); err != nil {
		t.Fatal(err)
	}
	if attemptStatus != "running" {
		t.Fatalf("被拒绝的推进留下了部分状态: attempt=%s", attemptStatus)
	}
}

func pendingOutboxCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, organizationID string) int {
	t.Helper()
	var pending int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_messages
		WHERE organization_id = $1 AND delivered_at IS NULL`, organizationID).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if pending <= 0 {
		t.Fatalf("组织 %s 没有待投递消息，用例前提不成立", organizationID)
	}
	return pending
}
