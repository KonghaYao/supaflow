package platformops

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/postgres"
)

// 验证业务已提交但 HTTP ledger 尚未完成的崩溃窗口可安全接管，且业务状态与成功审计均只保留一份。
func TestOpsMutationKillWindowTakeoverKeepsBusinessAndSingleAudit(t *testing.T) {
	ctx, pool := opsMutationAtomicityFixture(t)
	organizationID := fmt.Sprintf("ops_mutation_takeover_%d", time.Now().UnixNano())
	ledger, err := NewPostgresMutationLedger(pool)
	if err != nil {
		t.Fatal(err)
	}
	hash := bytes.Repeat([]byte{7}, 32)
	first, err := ledger.Claim(ctx, organizationID, "quota.configure", "quota-kill-window", hash)
	if err != nil {
		t.Fatal(err)
	}
	admission, err := NewAdmission(pool, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	mutation := MutationOperation{ID: first.OperationID, OrganizationID: organizationID, ActorID: "admin",
		Action: "quota.configure", ResourceType: "organization_quota", ResourceID: "organization"}
	firstContext := ContextWithMutation(ctx, mutation)
	quota := Quota{OrganizationID: organizationID, MaxActiveRuns: 3, MaxActiveWorkers: 2, MaxQueueDepth: 5}
	if err = admission.ConfigureQuota(firstContext, quota); err != nil {
		t.Fatal(err)
	}
	if !MutationCommitted(firstContext) {
		t.Fatal("successful domain transaction did not mark mutation committed")
	}
	assertQuotaAndAudit(t, ctx, pool, organizationID, quota, 1)

	// Simulate process death here: no ledger.Complete call from the first owner.
	if _, err = pool.Exec(ctx, `UPDATE ops_mutation_ledger SET lease_expires_at=clock_timestamp()-interval '1 second'
		WHERE organization_id=$1 AND operation=$2 AND idempotency_key=$3`, organizationID, "quota.configure", "quota-kill-window"); err != nil {
		t.Fatalf("expire first owner lease: %v", err)
	}
	second, err := ledger.Claim(ctx, organizationID, "quota.configure", "quota-kill-window", hash)
	if err != nil || second.Owner == first.Owner || second.OperationID != first.OperationID {
		t.Fatalf("takeover first=%+v second=%+v err=%v", first, second, err)
	}
	secondContext := ContextWithMutation(ctx, mutation)
	if err = admission.ConfigureQuota(secondContext, quota); err != nil {
		t.Fatal(err)
	}
	assertQuotaAndAudit(t, ctx, pool, organizationID, quota, 1)
	if err = ledger.Complete(ctx, organizationID, "quota.configure", "quota-kill-window", MutationCompletion{
		Owner: second.Owner, StatusCode: 200, ContentType: "application/json", Body: []byte(`{"configured":true}`),
	}); err != nil {
		t.Fatal(err)
	}
	replay, err := ledger.Claim(ctx, organizationID, "quota.configure", "quota-kill-window", hash)
	if err != nil || !replay.Replay || replay.OperationID != first.OperationID {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
}

// 验证 organization export 首次提交和崩溃后重放复用同一对象，并只写 HTTP 契约对应的 retention.export 审计。
func TestOpsMutationExportReplayKeepsSingleHTTPAudit(t *testing.T) {
	ctx, pool := opsMutationAtomicityFixture(t)
	organizationID := fmt.Sprintf("ops_export_takeover_%d", time.Now().UnixNano())
	objects, err := NewFileObjectStore(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		t.Fatal(err)
	}
	retention, err := NewRetention(pool, objects, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	operation := MutationOperation{ID: "operation_export_takeover_" + fmt.Sprint(time.Now().UnixNano()), OrganizationID: organizationID, ActorID: "admin",
		Action: "retention.export", ResourceType: "organization_export", ResourceID: "organization"}
	expiresAt := time.Now().UTC().Add(24 * time.Hour)
	firstContext := ContextWithMutation(ctx, operation)
	first, err := retention.ExportOrganization(firstContext, organizationID, "admin", expiresAt)
	if err != nil || !MutationCommitted(firstContext) {
		t.Fatalf("first export=%+v committed=%t err=%v", first, MutationCommitted(firstContext), err)
	}
	secondContext := ContextWithMutation(ctx, operation)
	second, err := retention.ExportOrganization(secondContext, organizationID, "admin", expiresAt)
	if err != nil || second.ID != first.ID || second.ObjectID != first.ObjectID || !MutationCommitted(secondContext) {
		t.Fatalf("replay export=%+v first=%+v committed=%t err=%v", second, first, MutationCommitted(secondContext), err)
	}
	var exports, objectsCount, httpAudits, legacyAudits int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM organization_exports WHERE organization_id=$1`, organizationID).Scan(&exports); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM managed_objects WHERE organization_id=$1 AND kind='organization_export'`, organizationID).Scan(&objectsCount); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE action='retention.export'),
		count(*) FILTER (WHERE action='organization.export') FROM audit_records WHERE organization_id=$1`, organizationID).Scan(&httpAudits, &legacyAudits); err != nil {
		t.Fatal(err)
	}
	if exports != 1 || objectsCount != 1 || httpAudits != 1 || legacyAudits != 0 {
		t.Fatalf("exports=%d objects=%d httpAudits=%d legacyAudits=%d", exports, objectsCount, httpAudits, legacyAudits)
	}
}

// 验证 trigger firing 的 created=false 重放仍确认同一成功审计，不重复 firing 或 outbox 副作用。
func TestOpsMutationTriggerFireReplayKeepsSingleAuditAndFiring(t *testing.T) {
	ctx, pool := opsMutationAtomicityFixture(t)
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	organizationID, workflowID := "ops_trigger_"+suffix, "workflow_"+suffix
	if err := seedPlatformOrganization(ctx, pool, organizationID, workflowID); err != nil {
		t.Fatal(err)
	}
	if err := seedTriggerPublication(ctx, pool, organizationID, workflowID, suffix); err != nil {
		t.Fatal(err)
	}
	store := NewPostgresTriggerStore(pool)
	service, err := NewTriggerService(store, nil, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	createOperation := MutationOperation{ID: "operation_trigger_create_" + suffix, OrganizationID: organizationID, ActorID: "operator",
		Action: "trigger.create", ResourceType: "trigger", ResourceID: "organization"}
	createRequest := CreateTriggerRequest{OrganizationID: organizationID, ActorID: "operator", WorkflowID: workflowID,
		Kind: TriggerAPI, Name: "api", IdempotencyKey: "trigger-create-atomic"}
	createdContext := ContextWithMutation(ctx, createOperation)
	trigger, err := service.Create(createdContext, createRequest)
	if err != nil || !MutationCommitted(createdContext) {
		t.Fatalf("create trigger=%+v committed=%t err=%v", trigger, MutationCommitted(createdContext), err)
	}
	replayedCreateContext := ContextWithMutation(ctx, createOperation)
	replayedTrigger, err := service.Create(replayedCreateContext, createRequest)
	if err != nil || replayedTrigger.ID != trigger.ID || !MutationCommitted(replayedCreateContext) {
		t.Fatalf("replay trigger=%+v committed=%t err=%v", replayedTrigger, MutationCommitted(replayedCreateContext), err)
	}
	stateRequest := ChangeTriggerStateRequest{OrganizationID: organizationID, ActorID: "operator", TriggerID: trigger.ID,
		Enabled: true, ExpectedUpdatedAt: trigger.UpdatedAt, IdempotencyKey: "trigger-state-stable", Now: time.Now().UTC()}
	firstState, err := service.ChangeState(ctx, stateRequest)
	if err != nil {
		t.Fatal(err)
	}
	stateRequest.Now = stateRequest.Now.Add(time.Minute)
	replayedState, err := service.ChangeState(ctx, stateRequest)
	if err != nil || !replayedState.UpdatedAt.Equal(firstState.UpdatedAt) {
		t.Fatalf("first state=%+v replay=%+v err=%v", firstState, replayedState, err)
	}
	fireOperation := MutationOperation{ID: "operation_trigger_fire_" + suffix, OrganizationID: organizationID, ActorID: "operator",
		Action: "trigger.fire", ResourceType: "trigger", ResourceID: trigger.ID}
	fireRequest := FireRequest{OrganizationID: organizationID, TriggerID: trigger.ID, ExpectedKind: TriggerAPI,
		DeduplicationKey: "trigger-fire-atomic", Payload: []byte(`{"value":1}`)}
	firstFireContext := ContextWithMutation(ctx, fireOperation)
	_, firstCreated, err := service.Fire(firstFireContext, fireRequest)
	if err != nil || !firstCreated || !MutationCommitted(firstFireContext) {
		t.Fatalf("first firing created=%t committed=%t err=%v", firstCreated, MutationCommitted(firstFireContext), err)
	}
	replayFireContext := ContextWithMutation(ctx, fireOperation)
	_, replayCreated, err := service.Fire(replayFireContext, fireRequest)
	if err != nil || replayCreated || !MutationCommitted(replayFireContext) {
		t.Fatalf("replay firing created=%t committed=%t err=%v", replayCreated, MutationCommitted(replayFireContext), err)
	}
	var firings, outboxMessages, createAudits, fireAudits int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM trigger_firings WHERE organization_id=$1 AND trigger_id=$2`, organizationID, trigger.ID).Scan(&firings); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM outbox_messages WHERE organization_id=$1 AND aggregate_id=$2 AND topic='trigger.fired'`, organizationID, trigger.ID).Scan(&outboxMessages); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE action='trigger.create'),
		count(*) FILTER (WHERE action='trigger.fire') FROM audit_records WHERE organization_id=$1`, organizationID).Scan(&createAudits, &fireAudits); err != nil {
		t.Fatal(err)
	}
	if firings != 1 || outboxMessages != 1 || createAudits != 1 || fireAudits != 1 {
		t.Fatalf("firings=%d outbox=%d createAudits=%d fireAudits=%d", firings, outboxMessages, createAudits, fireAudits)
	}
}

// 验证成功审计写入失败会回滚同一事务内的 quota 业务变更，而不是留下无审计状态。
func TestOpsMutationAuditFailureRollsBackBusinessState(t *testing.T) {
	ctx, pool := opsMutationAtomicityFixture(t)
	organizationID := fmt.Sprintf("ops_mutation_rollback_%d", time.Now().UnixNano())
	functionName := fmt.Sprintf("reject_ops_audit_%d", time.Now().UnixNano())
	triggerName := functionName + "_trigger"
	if _, err := pool.Exec(ctx, fmt.Sprintf(`CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'injected audit failure'; END $$;
		CREATE TRIGGER %s BEFORE INSERT ON audit_records FOR EACH ROW
		WHEN (NEW.organization_id='%s') EXECUTE FUNCTION %s()`, functionName, triggerName, organizationID, functionName)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON audit_records", triggerName))
		_, _ = pool.Exec(context.Background(), fmt.Sprintf("DROP FUNCTION IF EXISTS %s()", functionName))
	})
	admission, err := NewAdmission(pool, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err = admission.EnsureQuota(ctx, Quota{OrganizationID: organizationID, MaxActiveRuns: 100, MaxActiveWorkers: 100, MaxQueueDepth: 1000}); err != nil {
		t.Fatal(err)
	}
	var beforeRuns, beforeWorkers, beforeQueue int
	if err = pool.QueryRow(ctx, `SELECT max_active_runs,max_active_workers,max_queue_depth FROM organization_quotas
		WHERE organization_id=$1`, organizationID).Scan(&beforeRuns, &beforeWorkers, &beforeQueue); err != nil {
		t.Fatal(err)
	}
	mutationContext := ContextWithMutation(ctx, MutationOperation{ID: "operation_audit_rollback_" + fmt.Sprint(time.Now().UnixNano()), OrganizationID: organizationID,
		ActorID: "admin", Action: "quota.configure", ResourceType: "organization_quota", ResourceID: "organization"})
	err = admission.ConfigureQuota(mutationContext, Quota{
		OrganizationID: organizationID, MaxActiveRuns: 4, MaxActiveWorkers: 2, MaxQueueDepth: 6,
	})
	if err == nil {
		t.Fatal("expected injected audit failure")
	}
	var afterRuns, afterWorkers, afterQueue, audits int
	if queryErr := pool.QueryRow(ctx, `SELECT max_active_runs,max_active_workers,max_queue_depth FROM organization_quotas
		WHERE organization_id=$1`, organizationID).Scan(&afterRuns, &afterWorkers, &afterQueue); queryErr != nil {
		t.Fatal(queryErr)
	}
	if queryErr := pool.QueryRow(ctx, `SELECT count(*) FROM audit_records WHERE organization_id=$1`, organizationID).Scan(&audits); queryErr != nil {
		t.Fatal(queryErr)
	}
	if afterRuns != beforeRuns || afterWorkers != beforeWorkers || afterQueue != beforeQueue || audits != 0 || MutationCommitted(mutationContext) {
		t.Fatalf("quota before=%d/%d/%d after=%d/%d/%d audit=%d committed=%t",
			beforeRuns, beforeWorkers, beforeQueue, afterRuns, afterWorkers, afterQueue, audits, MutationCommitted(mutationContext))
	}
}

func opsMutationAtomicityFixture(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	databaseURL := os.Getenv("SUPAFLOW_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("SUPAFLOW_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	migrations, err := postgres.LoadMigrations(os.DirFS("../.."), "migrations")
	if err != nil {
		t.Fatal(err)
	}
	if err = postgres.NewMigrator(pool).Up(ctx, migrations); err != nil {
		t.Fatal(err)
	}
	return ctx, pool
}

func assertQuotaAndAudit(t *testing.T, ctx context.Context, pool *pgxpool.Pool, organizationID string, quota Quota, auditCount int) {
	t.Helper()
	var activeRuns, activeWorkers, queueDepth, audits int
	if err := pool.QueryRow(ctx, `SELECT max_active_runs,max_active_workers,max_queue_depth
		FROM organization_quotas WHERE organization_id=$1`, organizationID).Scan(&activeRuns, &activeWorkers, &queueDepth); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM audit_records
		WHERE organization_id=$1 AND action='quota.configure' AND outcome='allowed'`, organizationID).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if activeRuns != quota.MaxActiveRuns || activeWorkers != quota.MaxActiveWorkers || queueDepth != quota.MaxQueueDepth || audits != auditCount {
		t.Fatalf("quota=(%d,%d,%d) audit=%d", activeRuns, activeWorkers, queueDepth, audits)
	}
}
