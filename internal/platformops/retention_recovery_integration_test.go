package platformops

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"sync"
	"testing"
	"time"
)

// 验证并发 retention 删除只有一个事务取得 intent，且进程在物理删除前崩溃也已留下唯一审计。
func TestRetentionDeletionIntentAndAuditAreAtomic(t *testing.T) {
	ctx, pool, objects, tenantID, _ := objectRecoveryFixture(t)
	value := []byte("delete after crash")
	digest := sha256.Sum256(value)
	if _, err := objects.Put(ctx, tenantID, "atomic-delete", bytes.NewReader(value), 1024); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO managed_objects
		(tenant_id,id,kind,object_key,content_type,size_bytes,content_hash,status,archived_at)
		VALUES($1,'atomic-delete','output','atomic-delete','application/octet-stream',$2,$3,'archived',now()-interval '1 hour')`,
		tenantID, len(value), digest[:]); err != nil {
		t.Fatal(err)
	}
	audit := func() AuditRecord {
		return AuditRecord{TenantID: tenantID, ActorID: "system:retention", Action: "retention.objects.delete",
			ResourceType: "managed_object", ResourceID: "atomic-delete", Outcome: AuditAllowed}
	}
	var claimed int
	var failures []error
	var mutex sync.Mutex
	var workers sync.WaitGroup
	for range 2 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			record := audit()
			_, won, err := markObjectDeleting(ctx, pool, tenantID, "atomic-delete", "output", &record)
			mutex.Lock()
			defer mutex.Unlock()
			if won {
				claimed++
			}
			if err != nil {
				failures = append(failures, err)
			}
		}()
	}
	workers.Wait()
	if claimed != 1 || len(failures) != 0 {
		t.Fatalf("claimed=%d failures=%v", claimed, failures)
	}
	var status string
	var audits int
	if err := pool.QueryRow(ctx, `SELECT status FROM managed_objects WHERE tenant_id=$1 AND id='atomic-delete'`, tenantID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM audit_records
		WHERE tenant_id=$1 AND action='retention.objects.delete' AND resource_id='atomic-delete'`, tenantID).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if status != "deleting" || audits != 1 {
		t.Fatalf("status=%s audits=%d", status, audits)
	}
	retention, _ := NewRetention(pool, objects, 1024)
	if recovered, err := retention.RecoverObjects(ctx, tenantID, 10); err != nil || recovered != 1 {
		t.Fatalf("recovered=%d err=%v", recovered, err)
	}
}

// 验证 legacy NULL 回填与 SetPolicy 共用 tenant lock，竞态结束后 deadline 绝不超过新上限。
func TestRetentionBackfillSerializesWithPolicyTightening(t *testing.T) {
	ctx, pool, objects, tenantID, _ := objectRecoveryFixture(t)
	if _, err := pool.Exec(ctx, `UPDATE retention_policies SET output_days=30 WHERE tenant_id=$1`, tenantID); err != nil {
		t.Fatal(err)
	}
	value := []byte("legacy")
	digest := sha256.Sum256(value)
	if _, err := pool.Exec(ctx, `INSERT INTO managed_objects
		(tenant_id,id,kind,object_key,content_type,size_bytes,content_hash,status,retention_until)
		VALUES($1,'legacy-null','output','legacy-null','application/octet-stream',$2,$3,'active',NULL)`,
		tenantID, len(value), digest[:]); err != nil {
		t.Fatal(err)
	}
	functionName := fmt.Sprintf("delay_backfill_%d", time.Now().UnixNano())
	triggerName := functionName + "_trigger"
	if _, err := pool.Exec(ctx, fmt.Sprintf(`CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN PERFORM pg_sleep(0.4); RETURN NEW; END $$;
		CREATE TRIGGER %s BEFORE UPDATE ON managed_objects FOR EACH ROW
		WHEN (NEW.tenant_id='%s' AND OLD.retention_until IS NULL) EXECUTE FUNCTION %s()`,
		functionName, triggerName, tenantID, functionName)); err != nil {
		t.Fatal(err)
	}
	retention, _ := NewRetention(pool, objects, 1024)
	backfillDone := make(chan error, 1)
	go func() {
		_, err := retention.backfillOutputRetention(ctx, tenantID, 10)
		backfillDone <- err
	}()
	waitForRetentionLock(t, ctx, pool, tenantID)
	policyDone := make(chan error, 1)
	go func() {
		policyDone <- retention.SetPolicy(ctx, RetentionPolicy{TenantID: tenantID, EventDays: 7, OutputDays: 1, ArchiveBeforeDelete: true})
	}()
	if err := <-backfillDone; err != nil {
		t.Fatal(err)
	}
	if err := <-policyDone; err != nil {
		t.Fatal(err)
	}
	var exceeds bool
	if err := pool.QueryRow(ctx, `SELECT retention_until > created_at + interval '1 day'
		FROM managed_objects WHERE tenant_id=$1 AND id='legacy-null'`, tenantID).Scan(&exceeds); err != nil {
		t.Fatal(err)
	}
	if exceeds {
		t.Fatal("concurrent backfill extended retention beyond tightened policy")
	}
}

// 验证事件清理从读取策略到不可逆删除始终持有 tenant lock，新策略不能在中途提交。
func TestRetentionEventSweepSerializesWithPolicyUpdate(t *testing.T) {
	ctx, pool, objects, tenantA, tenantB := objectRecoveryFixture(t)
	for index, archive := range []bool{false, true} {
		tenantID := []string{tenantA, tenantB}[index]
		workflowID := fmt.Sprintf("retention_workflow_%d", index)
		if _, err := pool.Exec(ctx, `INSERT INTO workflows (tenant_id,id,slug,name,created_by)
			VALUES ($1,$2,$2,$2,'integration')`, tenantID, workflowID); err != nil {
			t.Fatal(err)
		}
		_, runID, err := seedRunningAttempt(ctx, pool, tenantID, workflowID, fmt.Sprintf("retention_%d", index))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = pool.Exec(ctx, `UPDATE runs SET status='success',finished_at=now() WHERE tenant_id=$1 AND id=$2`, tenantID, runID); err != nil {
			t.Fatal(err)
		}
		if _, err = pool.Exec(ctx, `INSERT INTO run_events(id,tenant_id,run_id,sequence,event_type,payload,created_at)
			VALUES($3,$1,$2,1,'completed','{}',now()-interval '2 days')`,
			tenantID, runID, fmt.Sprintf("event_%d", index)); err != nil {
			t.Fatal(err)
		}
		retention, err := NewRetention(pool, objects, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		if err = retention.SetPolicy(ctx, RetentionPolicy{
			TenantID: tenantID, EventDays: 1, OutputDays: 30, ArchiveBeforeDelete: archive,
		}); err != nil {
			t.Fatal(err)
		}
		functionName := fmt.Sprintf("delay_event_sweep_%d_%d", index, time.Now().UnixNano())
		triggerName := functionName + "_trigger"
		if _, err = pool.Exec(ctx, fmt.Sprintf(`CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN PERFORM pg_sleep(0.4); RETURN OLD; END $$;
			CREATE TRIGGER %s BEFORE DELETE ON run_events FOR EACH ROW
			WHEN (OLD.tenant_id='%s') EXECUTE FUNCTION %s()`,
			functionName, triggerName, tenantID, functionName)); err != nil {
			t.Fatal(err)
		}
		sweepDone := make(chan error, 1)
		go func() {
			_, sweepErr := retention.SweepTenant(ctx, tenantID, time.Now().UTC(), 10)
			sweepDone <- sweepErr
		}()
		waitForRetentionLock(t, ctx, pool, tenantID)
		policyDone := make(chan error, 1)
		go func() {
			policyDone <- retention.SetPolicy(ctx, RetentionPolicy{
				TenantID: tenantID, EventDays: 30, OutputDays: 30, ArchiveBeforeDelete: archive,
			})
		}()
		select {
		case policyErr := <-policyDone:
			t.Fatalf("archive=%t policy update bypassed in-flight event sweep: %v", archive, policyErr)
		case <-time.After(100 * time.Millisecond):
		}
		if err = <-sweepDone; err != nil {
			t.Fatal(err)
		}
		if err = <-policyDone; err != nil {
			t.Fatal(err)
		}
		var eventDays int
		if err = pool.QueryRow(ctx, `SELECT event_days FROM retention_policies WHERE tenant_id=$1`, tenantID).Scan(&eventDays); err != nil || eventDays != 30 {
			t.Fatalf("archive=%t eventDays=%d err=%v", archive, eventDays, err)
		}
	}
}
