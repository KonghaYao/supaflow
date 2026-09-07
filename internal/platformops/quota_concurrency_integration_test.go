package platformops

import (
	"fmt"
	"testing"
	"time"
)

// 验证 ConfigureQuota 与 ReleaseRun 并发时共享 quota/counter 锁，完成后计数一致并按新上限自然收敛。
func TestConfigureQuotaConcurrentWithReleaseKeepsDownscaleInvariant(t *testing.T) {
	ctx, pool := opsMutationAtomicityFixture(t)
	organizationID := fmt.Sprintf("quota_concurrent_release_%d", time.Now().UnixNano())
	admission, _ := NewAdmission(pool, time.Second)
	if err := admission.ConfigureQuota(ctx, Quota{OrganizationID: organizationID, MaxActiveRuns: 10, MaxActiveWorkers: 1, MaxQueueDepth: 10}); err != nil {
		t.Fatal(err)
	}
	activeIDs := make([]string, 0, 10)
	for index := 0; index < 15; index++ {
		resourceID := fmt.Sprintf("concurrent-run-%02d", index)
		decision, err := admission.AdmitRun(ctx, organizationID, resourceID, index)
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
		errorsCh <- admission.ConfigureQuota(ctx, Quota{OrganizationID: organizationID, MaxActiveRuns: 5, MaxActiveWorkers: 1, MaxQueueDepth: 10})
	}()
	go func() {
		<-start
		_, err := admission.ReleaseRun(ctx, organizationID, activeIDs[0])
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
		(SELECT count(*) FROM admission_entries a WHERE a.organization_id=q.organization_id AND a.resource_type='run' AND a.status='active'),
		(SELECT count(*) FROM admission_entries a WHERE a.organization_id=q.organization_id AND a.resource_type='run' AND a.status='queued')
		FROM organization_quotas q JOIN organization_quota_counters c ON c.organization_id=q.organization_id WHERE q.organization_id=$1`, organizationID).
		Scan(&maxActive, &counterActive, &counterQueued, &actualActive, &actualQueued); err != nil {
		t.Fatal(err)
	}
	if maxActive != 5 || counterActive != actualActive || counterQueued != actualQueued {
		t.Fatalf("max=%d counters=%d/%d actual=%d/%d", maxActive, counterActive, counterQueued, actualActive, actualQueued)
	}
	decision, err := admission.ReleaseRun(ctx, organizationID, activeIDs[1])
	if err != nil || decision.PromotedResourceID != "" {
		t.Fatalf("post-concurrency convergence decision=%+v err=%v", decision, err)
	}
}
