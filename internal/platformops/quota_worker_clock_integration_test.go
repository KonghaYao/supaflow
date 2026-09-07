package platformops

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// 验证动态下调 run 配额后，release 只自然收敛，不会在仍超限时继续晋升队列。
func TestQuotaDownscaleReleaseConvergesBeforePromotion(t *testing.T) {
	ctx, pool := opsMutationAtomicityFixture(t)
	organizationID := fmt.Sprintf("quota_downscale_%d", time.Now().UnixNano())
	admission, _ := NewAdmission(pool, time.Second)
	if err := admission.ConfigureQuota(ctx, Quota{OrganizationID: organizationID, MaxActiveRuns: 10, MaxActiveWorkers: 1, MaxQueueDepth: 10}); err != nil {
		t.Fatal(err)
	}
	active := make([]string, 0, 10)
	for index := 0; index < 15; index++ {
		resourceID := fmt.Sprintf("run-%02d", index)
		decision, err := admission.AdmitRun(ctx, organizationID, resourceID, index)
		if err != nil {
			t.Fatal(err)
		}
		if decision.Admitted {
			active = append(active, resourceID)
		}
	}
	if err := admission.ConfigureQuota(ctx, Quota{OrganizationID: organizationID, MaxActiveRuns: 5, MaxActiveWorkers: 1, MaxQueueDepth: 10}); err != nil {
		t.Fatal(err)
	}
	for _, resourceID := range active[:5] {
		decision, err := admission.ReleaseRun(ctx, organizationID, resourceID)
		if err != nil || decision.PromotedResourceID != "" {
			t.Fatalf("release while converging resource=%s decision=%+v err=%v", resourceID, decision, err)
		}
	}
	assertRunQuotaCounts(t, ctx, pool, organizationID, 5, 5)
	decision, err := admission.ReleaseRun(ctx, organizationID, active[5])
	if err != nil || decision.PromotedResourceID == "" {
		t.Fatalf("release at capacity decision=%+v err=%v", decision, err)
	}
	assertRunQuotaCounts(t, ctx, pool, organizationID, 5, 4)
}

// 验证网络分区后的旧 worker 只有重新取得 admission 才可复活，配额被其他 worker 占用时保持离线。
func TestWorkerPartitionRecoveryRequiresAdmissionReacquisition(t *testing.T) {
	ctx, pool := opsMutationAtomicityFixture(t)
	organizationID := fmt.Sprintf("worker_partition_%d", time.Now().UnixNano())
	admission, _ := NewAdmission(pool, time.Second)
	if err := admission.ConfigureQuota(ctx, Quota{OrganizationID: organizationID, MaxActiveRuns: 1, MaxActiveWorkers: 1, MaxQueueDepth: 1}); err != nil {
		t.Fatal(err)
	}
	workers := NewWorkerRegistry(pool)
	if err := workers.Heartbeat(ctx, organizationID, "worker-a", 1, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE worker_registrations SET heartbeat_at=clock_timestamp()-interval '1 minute'
		WHERE organization_id=$1 AND worker_id='worker-a'`, organizationID); err != nil {
		t.Fatal(err)
	}
	if reaped, err := workers.ReapStale(ctx, organizationID, 30*time.Second, 10); err != nil || reaped != 1 {
		t.Fatalf("reaped=%d err=%v", reaped, err)
	}
	if err := workers.Heartbeat(ctx, organizationID, "worker-b", 1, 0); err != nil {
		t.Fatal(err)
	}
	if err := workers.Heartbeat(ctx, organizationID, "worker-a", 1, 0); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("partitioned worker reacquisition error=%v", err)
	}
	if allowed, err := workers.MayClaim(ctx, organizationID, "worker-a", 30*time.Second); err != nil || allowed {
		t.Fatalf("released worker mayClaim=%t err=%v", allowed, err)
	}
	if allowed, err := workers.MayClaim(ctx, organizationID, "worker-b", 30*time.Second); err != nil || !allowed {
		t.Fatalf("active worker mayClaim=%t err=%v", allowed, err)
	}
	var activeWorkers int
	if err := pool.QueryRow(ctx, `SELECT active_workers FROM organization_quota_counters WHERE organization_id=$1`, organizationID).Scan(&activeWorkers); err != nil || activeWorkers != 1 {
		t.Fatalf("activeWorkers=%d err=%v", activeWorkers, err)
	}
}

// 验证没有 registration 的遗留 active admission 可由 supervisor 有界回收。
func TestWorkerReaperReleasesOrphanAdmission(t *testing.T) {
	ctx, pool := opsMutationAtomicityFixture(t)
	organizationID := fmt.Sprintf("worker_orphan_%d", time.Now().UnixNano())
	admission, _ := NewAdmission(pool, time.Second)
	if err := admission.ConfigureQuota(ctx, Quota{OrganizationID: organizationID, MaxActiveRuns: 1, MaxActiveWorkers: 1, MaxQueueDepth: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := admission.AdmitWorker(ctx, organizationID, "orphan"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE admission_entries SET created_at=clock_timestamp()-interval '1 minute'
		WHERE organization_id=$1 AND resource_type='worker' AND resource_id='orphan'`, organizationID); err != nil {
		t.Fatal(err)
	}
	if reaped, err := NewWorkerRegistry(pool).ReapStale(ctx, organizationID, 30*time.Second, 10); err != nil || reaped != 1 {
		t.Fatalf("reaped=%d err=%v", reaped, err)
	}
	var activeWorkers int
	if err := pool.QueryRow(ctx, `SELECT active_workers FROM organization_quota_counters WHERE organization_id=$1`, organizationID).Scan(&activeWorkers); err != nil || activeWorkers != 0 {
		t.Fatalf("activeWorkers=%d err=%v", activeWorkers, err)
	}
}

// 验证 draining worker 崩溃后仍会被 fenced offline，并释放其 worker quota。
func TestWorkerReaperReleasesStaleDrainingWorker(t *testing.T) {
	ctx, pool := opsMutationAtomicityFixture(t)
	organizationID := fmt.Sprintf("worker_draining_%d", time.Now().UnixNano())
	admission, _ := NewAdmission(pool, time.Second)
	if err := admission.ConfigureQuota(ctx, Quota{OrganizationID: organizationID, MaxActiveRuns: 1, MaxActiveWorkers: 1, MaxQueueDepth: 1}); err != nil {
		t.Fatal(err)
	}
	workers := NewWorkerRegistry(pool)
	if err := workers.Heartbeat(ctx, organizationID, "worker-draining", 1, 0); err != nil {
		t.Fatal(err)
	}
	if err := workers.BeginDrain(ctx, organizationID, "worker-draining"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE worker_registrations SET heartbeat_at=clock_timestamp()-interval '1 minute'
		WHERE organization_id=$1 AND worker_id='worker-draining'`, organizationID); err != nil {
		t.Fatal(err)
	}
	if reaped, err := workers.ReapStale(ctx, organizationID, 30*time.Second, 10); err != nil || reaped != 1 {
		t.Fatalf("reaped=%d err=%v", reaped, err)
	}
	var status, admissionStatus string
	var activeWorkers int
	if err := pool.QueryRow(ctx, `SELECT w.status,a.status,c.active_workers
		FROM worker_registrations w
		JOIN admission_entries a ON a.organization_id=w.organization_id AND a.resource_type='worker' AND a.resource_id=w.worker_id
		JOIN organization_quota_counters c ON c.organization_id=w.organization_id
		WHERE w.organization_id=$1 AND w.worker_id='worker-draining'`, organizationID).Scan(&status, &admissionStatus, &activeWorkers); err != nil {
		t.Fatal(err)
	}
	if status != "offline" || admissionStatus != "released" || activeWorkers != 0 {
		t.Fatalf("worker=%s admission=%s activeWorkers=%d", status, admissionStatus, activeWorkers)
	}
	if err := workers.Heartbeat(ctx, organizationID, "worker-draining", 1, 0); err != nil {
		t.Fatalf("restart reacquire: %v", err)
	}
	if allowed, err := workers.MayClaim(ctx, organizationID, "worker-draining", 30*time.Second); err != nil || !allowed {
		t.Fatalf("restarted worker mayClaim=%t err=%v", allowed, err)
	}
}

// 验证 outbox lease 的资格与 fencing 完全依据数据库时钟，不受调用进程时钟偏移影响。
func TestOutboxLeaseUsesDatabaseClockAcrossSkewedCallers(t *testing.T) {
	ctx, pool := opsMutationAtomicityFixture(t)
	organizationID := fmt.Sprintf("outbox_clock_%d", time.Now().UnixNano())
	if _, err := pool.Exec(ctx, `INSERT INTO workflows(organization_id,id,slug,name,created_by)
		VALUES($1,'outbox-clock','outbox-clock','outbox-clock','test')`, organizationID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO outbox_messages
		(id,organization_id,aggregate_type,aggregate_id,topic,payload,available_at)
		VALUES ('clock-message',$1,'test','aggregate','test.clock','{}',clock_timestamp())`, organizationID); err != nil {
		t.Fatal(err)
	}
	store := NewPostgresDeliveryStore(pool)
	message, err := store.ClaimDelivery(ctx, organizationID, "worker-a", time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.ClaimDelivery(ctx, organizationID, "worker-b", time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC), time.Minute); !errors.Is(err, ErrNotFound) {
		t.Fatalf("future-skew takeover error=%v", err)
	}
	if _, err = pool.Exec(ctx, `UPDATE outbox_messages SET claim_expires_at=clock_timestamp()-interval '1 second'
		WHERE organization_id=$1 AND id=$2`, organizationID, message.ID); err != nil {
		t.Fatal(err)
	}
	message, err = store.ClaimDelivery(ctx, organizationID, "worker-b", time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.AcknowledgeDelivery(ctx, message, "worker-b", time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("past-skew acknowledgement: %v", err)
	}
}

// 验证数据库 lease 已过期时，慢钟 worker 既不能新建 credential grant，也不能继续解析既有 grant。
func TestCredentialAccessRejectsExpiredDatabaseLeaseDespiteSlowCallerClock(t *testing.T) {
	ctx, pool := opsMutationAtomicityFixture(t)
	suffix := fmt.Sprintf("credential_clock_%d", time.Now().UnixNano())
	organizationID, workflowID := "organization_"+suffix, "workflow_"+suffix
	if err := seedPlatformOrganization(ctx, pool, organizationID, workflowID); err != nil {
		t.Fatal(err)
	}
	attemptID, runID, err := seedRunningAttempt(ctx, pool, organizationID, workflowID, suffix)
	if err != nil {
		t.Fatal(err)
	}
	provider, _ := NewLocalAESProvider("credential-clock", bytes.Repeat([]byte{8}, 32))
	credentials, _ := NewCredentialService(NewPostgresCredentialStore(pool), provider)
	credential, err := credentials.Create(ctx, CreateCredentialRequest{
		OrganizationID: organizationID, ActorID: "admin", Name: "database-clock", Purpose: "runtime", Secret: []byte("secret"),
	})
	if err != nil {
		t.Fatal(err)
	}
	liveRequest := GrantCredentialRequest{OrganizationID: organizationID, RunID: runID, AttemptID: attemptID,
		CredentialID: credential.ID, Purpose: "runtime", FencingToken: 1, Now: time.Now().UTC()}
	if err = credentials.GrantForAttempt(ctx, liveRequest); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE worker_leases
		SET acquired_at=clock_timestamp()-interval '2 seconds',expires_at=clock_timestamp()-interval '1 second'
		WHERE organization_id=$1 AND attempt_id=$2`, organizationID, attemptID); err != nil {
		t.Fatal(err)
	}
	slowNow := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	liveRequest.Now = slowNow
	if err = credentials.GrantForAttempt(ctx, liveRequest); !errors.Is(err, ErrCredentialForbidden) {
		t.Fatalf("expired lease grant error=%v", err)
	}
	if _, err = credentials.ResolveForAttempt(ctx, ResolveCredentialRequest{OrganizationID: organizationID, RunID: runID,
		AttemptID: attemptID, CredentialID: credential.ID, Purpose: "runtime", FencingToken: 1, Now: slowNow}); !errors.Is(err, ErrCredentialForbidden) {
		t.Fatalf("expired lease resolution error=%v", err)
	}
}

func assertRunQuotaCounts(t *testing.T, ctx context.Context, pool *pgxpool.Pool, organizationID string, wantActive, wantQueued int) {
	t.Helper()
	var active, queued int
	if err := pool.QueryRow(ctx, `SELECT active_runs,queued_runs FROM organization_quota_counters WHERE organization_id=$1`, organizationID).Scan(&active, &queued); err != nil {
		t.Fatal(err)
	}
	if active != wantActive || queued != wantQueued {
		t.Fatalf("active=%d queued=%d want=%d/%d", active, queued, wantActive, wantQueued)
	}
}
