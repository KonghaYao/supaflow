package postgres

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
)

// 验证 claim 后续写入失败会回滚 attempt、lease 和公平 cursor，不存在独立推进窗口。
func TestClaimNextRollsBackCursorWithAttemptClaim(t *testing.T) {
	ctx, pool := admissionTestPool(t)
	organizationID, service, plan := admissionTestOrganization(t, ctx, pool, "dynamic_rollback")
	started := startAdmissionRun(t, ctx, service, organizationID, plan, "dynamic-rollback", 0)
	if _, err := pool.Exec(ctx, `UPDATE organization_work_queue SET ready_at=clock_timestamp()+interval '1 hour' WHERE organization_id<>$1`, organizationID); err != nil {
		t.Fatal(err)
	}
	functionName := fmt.Sprintf("reject_dynamic_claim_event_%d", time.Now().UnixNano())
	if _, err := pool.Exec(ctx, fmt.Sprintf(`CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN IF NEW.run_id=%s AND NEW.event_type='attempt.claimed' THEN RAISE EXCEPTION 'forced claim failure'; END IF; RETURN NEW; END $$;
		CREATE TRIGGER %s BEFORE INSERT ON run_events FOR EACH ROW EXECUTE FUNCTION %s()`,
		pgx.Identifier{functionName}.Sanitize(), "'"+strings.ReplaceAll(started.ID, "'", "''")+"'", pgx.Identifier{functionName}.Sanitize(), pgx.Identifier{functionName}.Sanitize())); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP TRIGGER IF EXISTS "+pgx.Identifier{functionName}.Sanitize()+" ON run_events; DROP FUNCTION IF EXISTS "+pgx.Identifier{functionName}.Sanitize()+"()")
	})
	var before *time.Time
	if err := pool.QueryRow(ctx, `SELECT last_claimed_at FROM organization_work_queue WHERE organization_id=$1`, organizationID).Scan(&before); err != nil {
		t.Fatal(err)
	}
	store := NewRuntimeStore(pool)
	if work, err := store.ClaimNext(ctx, workflowruntime.ClaimNextAttemptRequest{WorkerID: "rollback", LeaseDuration: time.Minute}); err == nil || work != nil {
		t.Fatalf("claim should fail atomically: work=%+v err=%v", work, err)
	}
	var status string
	var leases int
	var after *time.Time
	if err := pool.QueryRow(ctx, `SELECT a.status,(SELECT count(*) FROM worker_leases l WHERE l.attempt_id=a.id),q.last_claimed_at
		FROM node_attempts a JOIN organization_work_queue q USING(organization_id) WHERE a.run_id=$1`, started.ID).Scan(&status, &leases, &after); err != nil {
		t.Fatal(err)
	}
	if status != "pending" || leases != 0 || (before == nil) != (after == nil) || (before != nil && !before.Equal(*after)) {
		t.Fatalf("rollback status=%s leases=%d cursor before=%v after=%v", status, leases, before, after)
	}
}

// 验证动态领取无需静态组织枚举，并在 A/B 两个组织间推进公平 cursor。
func TestClaimNextDiscoversOrganizationsFairly(t *testing.T) {
	ctx, pool := admissionTestPool(t)
	organizationA, serviceA, planA := admissionTestOrganization(t, ctx, pool, "dynamic_a")
	organizationB, serviceB, planB := admissionTestOrganization(t, ctx, pool, "dynamic_b")
	startAdmissionRun(t, ctx, serviceA, organizationA, planA, "dynamic-a", 0)
	startAdmissionRun(t, ctx, serviceB, organizationB, planB, "dynamic-b", 0)
	store := NewRuntimeStore(pool)
	first, err := store.ClaimNext(ctx, workflowruntime.ClaimNextAttemptRequest{WorkerID: "dynamic-1", LeaseDuration: time.Minute})
	if err != nil || first == nil {
		t.Fatalf("first claim=%+v err=%v", first, err)
	}
	second, err := store.ClaimNext(ctx, workflowruntime.ClaimNextAttemptRequest{WorkerID: "dynamic-2", LeaseDuration: time.Minute})
	if err != nil || second == nil || second.Attempt.OrganizationID == first.Attempt.OrganizationID {
		t.Fatalf("second claim=%+v first=%+v err=%v", second, first, err)
	}
}

// 验证多个 worker 并发领取时 SKIP LOCKED 只产生一个 attempt owner。
func TestClaimNextConcurrentSingleOwner(t *testing.T) {
	ctx, pool := admissionTestPool(t)
	organizationID, service, plan := admissionTestOrganization(t, ctx, pool, "dynamic_owner")
	start := startAdmissionRun(t, ctx, service, organizationID, plan, "dynamic-owner", 0)
	if _, err := pool.Exec(ctx, `UPDATE organization_work_queue SET ready_at=clock_timestamp()+interval '1 hour' WHERE organization_id<>$1`, organizationID); err != nil {
		t.Fatal(err)
	}
	store := NewRuntimeStore(pool)
	var wg sync.WaitGroup
	claims := make(chan *workflowruntime.WorkItem, 2)
	for _, worker := range []string{"owner-a", "owner-b"} {
		wg.Add(1)
		go func(worker string) {
			defer wg.Done()
			work, _ := store.ClaimNext(ctx, workflowruntime.ClaimNextAttemptRequest{WorkerID: worker, LeaseDuration: time.Minute})
			claims <- work
		}(worker)
	}
	wg.Wait()
	close(claims)
	owners := 0
	for work := range claims {
		if work != nil && work.Run.ID == start.ID {
			owners++
		}
	}
	if owners != 1 {
		t.Fatalf("owners=%d want=1", owners)
	}
}

// 验证 stale hint 被清理、future hint 不会 hot spin，且有界 reconcile 可修复 false negative。
func TestClaimNextRepairsHintsWithoutClaimingFutureWork(t *testing.T) {
	ctx, pool := admissionTestPool(t)
	organizationID, service, plan := admissionTestOrganization(t, ctx, pool, "dynamic_hint")
	startAdmissionRun(t, ctx, service, organizationID, plan, "dynamic-hint", 0)
	if _, err := pool.Exec(ctx, `DELETE FROM organization_work_queue WHERE organization_id=$1`, organizationID); err != nil {
		t.Fatal(err)
	}
	store := NewRuntimeStore(pool)
	work, err := store.ClaimNext(ctx, workflowruntime.ClaimNextAttemptRequest{WorkerID: "repair", LeaseDuration: time.Minute})
	if err != nil || work == nil {
		t.Fatalf("reconciled claim=%+v err=%v", work, err)
	}
	if _, err = pool.Exec(ctx, `UPDATE organization_work_queue SET ready_at=clock_timestamp()+interval '1 hour' WHERE organization_id<>$1`, organizationID); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO organization_work_queue(organization_id,ready_at) VALUES('stale',clock_timestamp()-interval '1 second')`); err != nil {
		t.Fatal(err)
	}
	for attempts := 0; attempts < 10; attempts++ {
		_, err = store.ClaimNext(ctx, workflowruntime.ClaimNextAttemptRequest{WorkerID: "stale", LeaseDuration: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		var count int
		if err = pool.QueryRow(ctx, `SELECT count(*) FROM organization_work_queue WHERE organization_id='stale'`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count == 0 {
			return
		}
	}
	t.Fatal("stale hint was not cleared within bounded claims")
}
