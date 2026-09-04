package platformops

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// 验证 ConfigureQuota 与 ReleaseRun 并发时共享 quota/counter 锁，完成后计数一致并按新上限自然收敛。
func TestConfigureQuotaConcurrentWithReleaseKeepsDownscaleInvariant(t *testing.T) {
	ctx, pool := opsMutationAtomicityFixture(t)
	tenantID := fmt.Sprintf("quota_concurrent_release_%d", time.Now().UnixNano())
	if _, err := pool.Exec(ctx, `INSERT INTO tenants(id,name) VALUES ($1,$1)`, tenantID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), `DELETE FROM tenants WHERE id=$1`, tenantID) })
	admission, _ := NewAdmission(pool, time.Second)
	if err := admission.ConfigureQuota(ctx, Quota{TenantID: tenantID, MaxActiveRuns: 10, MaxActiveWorkers: 1, MaxQueueDepth: 10}); err != nil {
		t.Fatal(err)
	}
	activeIDs := make([]string, 0, 10)
	for index := 0; index < 15; index++ {
		resourceID := fmt.Sprintf("concurrent-run-%02d", index)
		decision, err := admission.AdmitRun(ctx, tenantID, resourceID, index)
		if err != nil {
			t.Fatal(err)
		}
		if decision.Admitted {
			activeIDs = append(activeIDs, resourceID)
		}
	}
	start := make(chan struct{})
	errorsCh := make(chan error, 2)
	go func() {
		<-start
		errorsCh <- admission.ConfigureQuota(ctx, Quota{TenantID: tenantID, MaxActiveRuns: 5, MaxActiveWorkers: 1, MaxQueueDepth: 10})
	}()
	go func() {
		<-start
		_, err := admission.ReleaseRun(ctx, tenantID, activeIDs[0])
		errorsCh <- err
	}()
	close(start)
	for range 2 {
		if err := <-errorsCh; err != nil {
			t.Fatal(err)
		}
	}
	var maxActive, counterActive, counterQueued, actualActive, actualQueued int
	if err := pool.QueryRow(ctx, `SELECT q.max_active_runs,c.active_runs,c.queued_runs,
		(SELECT count(*) FROM admission_entries a WHERE a.tenant_id=q.tenant_id AND a.resource_type='run' AND a.status='active'),
		(SELECT count(*) FROM admission_entries a WHERE a.tenant_id=q.tenant_id AND a.resource_type='run' AND a.status='queued')
		FROM tenant_quotas q JOIN tenant_quota_counters c ON c.tenant_id=q.tenant_id WHERE q.tenant_id=$1`, tenantID).
		Scan(&maxActive, &counterActive, &counterQueued, &actualActive, &actualQueued); err != nil {
		t.Fatal(err)
	}
	if maxActive != 5 || counterActive != actualActive || counterQueued != actualQueued {
		t.Fatalf("max=%d counters=%d/%d actual=%d/%d", maxActive, counterActive, counterQueued, actualActive, actualQueued)
	}
	decision, err := admission.ReleaseRun(ctx, tenantID, activeIDs[1])
	if err != nil || decision.PromotedResourceID != "" {
		t.Fatalf("post-concurrency convergence decision=%+v err=%v", decision, err)
	}
}
