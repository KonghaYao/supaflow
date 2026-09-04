package platformops

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/postgres"
)

// 验证真实 PostgreSQL 上 webhook replay、schedule 去重、quota 竞争、credential 隔离轮换、DLQ 与 retention/export 全链路。
func TestPostgresPlatformOperationsLifecycle(t *testing.T) {
	databaseURL := os.Getenv("SUPAFLOW_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("SUPAFLOW_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	migrations, err := postgres.LoadMigrations(os.DirFS("../.."), "migrations")
	if err != nil {
		t.Fatal(err)
	}
	if err = postgres.NewMigrator(pool).Up(ctx, migrations); err != nil {
		t.Fatal(err)
	}
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	tenantA, tenantB := "ops_a_"+suffix, "ops_b_"+suffix
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM tenants WHERE id=ANY($1)`, []string{tenantA, tenantB})
	})
	workflowA, workflowB := "workflow_a_"+suffix, "workflow_b_"+suffix
	if err = seedPlatformTenant(ctx, pool, tenantA, workflowA); err != nil {
		t.Fatal(err)
	}
	if err = seedPlatformTenant(ctx, pool, tenantB, workflowB); err != nil {
		t.Fatal(err)
	}

	provider, err := NewLocalAESProvider("integration-key", bytes.Repeat([]byte{3}, 32))
	if err != nil {
		t.Fatal(err)
	}
	credentialStore := NewPostgresCredentialStore(pool)
	credentials, err := NewCredentialService(credentialStore, provider)
	if err != nil {
		t.Fatal(err)
	}
	secretValue := "webhook-secret-" + suffix
	credential, err := credentials.Create(ctx, CreateCredentialRequest{
		TenantID: tenantA, ActorID: "admin_a", Name: "webhook", Purpose: "webhook_signing", Secret: []byte(secretValue),
	})
	if err != nil {
		t.Fatal(err)
	}
	triggerStore := NewPostgresTriggerStore(pool)
	triggers, err := NewTriggerService(triggerStore, credentials, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	webhook, err := triggers.Create(ctx, CreateTriggerRequest{
		TenantID: tenantA, ActorID: "admin_a", WorkflowID: workflowA,
		Kind: TriggerWebhook, Name: "webhook", SigningCredentialID: credential.ID,
	})
	if err != nil || webhook.PublicHash == "" {
		t.Fatalf("create webhook=%+v err=%v", webhook, err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	body := []byte(`{"message":"hello"}`)
	webhookRequest := PublicWebhookRequest{
		PublicHash: webhook.PublicHash, RequestID: "webhook-request-1", Timestamp: now,
		Signature: SignWebhook([]byte(secretValue), "webhook-request-1", now, body), Body: body, Now: now,
	}
	if _, created, receiveErr := triggers.ReceivePublicWebhook(ctx, webhookRequest); receiveErr != nil || !created {
		t.Fatalf("receive webhook created=%v err=%v", created, receiveErr)
	}
	if _, _, receiveErr := triggers.ReceivePublicWebhook(ctx, webhookRequest); !errors.Is(receiveErr, ErrReplay) {
		t.Fatalf("webhook replay error=%v", receiveErr)
	}
	if _, _, receiveErr := triggers.ReceiveWebhook(ctx, WebhookRequest{
		TenantID: tenantB, TriggerID: webhook.ID, RequestID: "cross-tenant", Timestamp: now,
		Signature: webhookRequest.Signature, Body: body, Now: now,
	}); !errors.Is(receiveErr, ErrCredentialForbidden) {
		t.Fatalf("cross-tenant webhook error=%v", receiveErr)
	}
	if purged, purgeErr := triggerStore.PurgeExpiredWebhookReplays(ctx, now.Add(10*time.Minute), 100); purgeErr != nil || purged != 1 {
		t.Fatalf("purged webhook replay guards=%d err=%v", purged, purgeErr)
	}
	for _, kind := range []TriggerKind{TriggerAPI, TriggerManual} {
		trigger, createErr := triggers.Create(ctx, CreateTriggerRequest{
			TenantID: tenantA, ActorID: "admin_a", WorkflowID: workflowA,
			Kind: kind, Name: string(kind) + "-trigger",
		})
		if createErr != nil {
			t.Fatal(createErr)
		}
		fireRequest := FireRequest{
			TenantID: tenantA, TriggerID: trigger.ID, ExpectedKind: kind,
			DeduplicationKey: string(kind) + "-request-1", Payload: json.RawMessage(`{"value":1}`),
		}
		if _, created, fireErr := triggers.Fire(ctx, fireRequest); fireErr != nil || !created {
			t.Fatalf("%s first firing created=%v err=%v", kind, created, fireErr)
		}
		if _, created, fireErr := triggers.Fire(ctx, fireRequest); fireErr != nil || created {
			t.Fatalf("%s firing replay created=%v err=%v", kind, created, fireErr)
		}
		fireRequest.Payload = json.RawMessage(`{"value":2}`)
		if _, _, fireErr := triggers.Fire(ctx, fireRequest); !errors.Is(fireErr, ErrConflict) {
			t.Fatalf("%s firing conflicting payload error=%v", kind, fireErr)
		}
	}
	eventTrigger, err := triggers.Create(ctx, CreateTriggerRequest{
		TenantID: tenantA, ActorID: "admin_a", WorkflowID: workflowA,
		Kind: TriggerEvent, Name: "event-trigger", EventType: "document.ready",
	})
	if err != nil {
		t.Fatal(err)
	}
	eventFirings, err := triggers.ReceiveEvent(ctx, tenantA, "document.ready", "event-1", json.RawMessage(`{"documentId":"doc-1"}`), now)
	if err != nil || len(eventFirings) != 1 || eventFirings[0].TriggerID != eventTrigger.ID {
		t.Fatalf("event firings=%+v err=%v", eventFirings, err)
	}
	eventFirings, err = triggers.ReceiveEvent(ctx, tenantA, "document.ready", "event-1", json.RawMessage(`{"documentId":"doc-1"}`), now)
	if err != nil || len(eventFirings) != 0 {
		t.Fatalf("event replay firings=%+v err=%v", eventFirings, err)
	}
	if _, err = triggers.ReceiveEvent(ctx, tenantA, "document.ready", "event-1", json.RawMessage(`{"documentId":"doc-2"}`), now); !errors.Is(err, ErrConflict) {
		t.Fatalf("event conflicting payload error=%v", err)
	}

	firstDue := now.Add(-3 * time.Minute)
	_, err = triggers.Create(ctx, CreateTriggerRequest{
		TenantID: tenantA, ActorID: "admin_a", WorkflowID: workflowA, Kind: TriggerSchedule,
		Name: "catch-up", ScheduleInterval: time.Minute, MisfirePolicy: MisfireCatchUp, FirstFireAt: &firstDue,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = triggers.Create(ctx, CreateTriggerRequest{
		TenantID: tenantA, ActorID: "admin_a", WorkflowID: workflowA, Kind: TriggerSchedule,
		Name: "skip-misfire", ScheduleInterval: time.Minute, MisfirePolicy: MisfireSkip, FirstFireAt: &firstDue,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = triggers.Create(ctx, CreateTriggerRequest{
		TenantID: tenantA, ActorID: "admin_a", WorkflowID: workflowA, Kind: TriggerSchedule,
		Name: "fire-once-misfire", ScheduleInterval: time.Minute, MisfirePolicy: MisfireFireOnce, FirstFireAt: &firstDue,
	})
	if err != nil {
		t.Fatal(err)
	}
	firings, err := triggers.Tick(ctx, ScheduleOptions{Now: now, Limit: 1000, MaxCatchUp: 2, MisfireGrace: time.Second})
	if err != nil || countTenantFirings(firings, tenantA) != 3 {
		t.Fatalf("first schedule firings=%+v err=%v", firings, err)
	}
	firings, err = triggers.Tick(ctx, ScheduleOptions{Now: now, Limit: 1000, MaxCatchUp: 2, MisfireGrace: time.Second})
	if err != nil || countTenantFirings(firings, tenantA) != 2 {
		t.Fatalf("bounded catch-up continuation=%+v err=%v", firings, err)
	}
	firings, err = triggers.Tick(ctx, ScheduleOptions{Now: now, Limit: 1000, MaxCatchUp: 2, MisfireGrace: time.Second})
	if err != nil || countTenantFirings(firings, tenantA) != 0 {
		t.Fatalf("completed schedule catch-up fired duplicates=%+v err=%v", firings, err)
	}

	admission, err := NewAdmission(pool, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err = admission.ConfigureQuota(ctx, Quota{TenantID: tenantA, MaxActiveRuns: 3, MaxActiveWorkers: 2, MaxQueueDepth: 2}); err != nil {
		t.Fatal(err)
	}
	var admitted, queued, limited atomic.Int64
	var wait sync.WaitGroup
	for index := 0; index < 20; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			decision, admitErr := admission.AdmitRun(ctx, tenantA, fmt.Sprintf("run-admission-%02d", index), index%3)
			switch {
			case admitErr == nil && decision.Admitted:
				admitted.Add(1)
			case admitErr == nil && decision.Queued:
				queued.Add(1)
			case errors.Is(admitErr, ErrRateLimited):
				limited.Add(1)
			default:
				t.Errorf("unexpected admission decision=%+v err=%v", decision, admitErr)
			}
		}(index)
	}
	wait.Wait()
	if admitted.Load() != 3 || queued.Load() != 2 || limited.Load() != 15 {
		t.Fatalf("quota results admitted=%d queued=%d limited=%d", admitted.Load(), queued.Load(), limited.Load())
	}
	var activeResource, highestPriorityQueued string
	if err = pool.QueryRow(ctx, `SELECT resource_id FROM admission_entries WHERE tenant_id=$1 AND resource_type='run' AND status='active' ORDER BY resource_id LIMIT 1`, tenantA).Scan(&activeResource); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT resource_id FROM admission_entries WHERE tenant_id=$1 AND resource_type='run' AND status='queued' ORDER BY priority DESC,created_at,id LIMIT 1`, tenantA).Scan(&highestPriorityQueued); err != nil {
		t.Fatal(err)
	}
	released, err := admission.ReleaseRun(ctx, tenantA, activeResource)
	if err != nil || released.PromotedResourceID != highestPriorityQueued {
		t.Fatalf("priority promotion=%+v want=%s err=%v", released, highestPriorityQueued, err)
	}
	for index := 0; index < 2; index++ {
		decision, workerErr := admission.AdmitWorker(ctx, tenantA, fmt.Sprintf("worker-%d", index))
		if workerErr != nil || !decision.Admitted {
			t.Fatalf("worker admission=%+v err=%v", decision, workerErr)
		}
	}
	if _, err = admission.AdmitWorker(ctx, tenantA, "worker-over-quota"); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("worker quota error=%v", err)
	}
	workers := NewWorkerRegistry(pool)
	if err = workers.Heartbeat(ctx, tenantA, "worker-0", 1, 0, now.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err = workers.Heartbeat(ctx, tenantA, "worker-1", 1, 0, now); err != nil {
		t.Fatal(err)
	}
	if reaped, reapErr := workers.ReapStale(ctx, tenantA, now.Add(-30*time.Second), 10); reapErr != nil || reaped != 1 {
		t.Fatalf("reaped workers=%d err=%v", reaped, reapErr)
	}
	var activeWorkers int
	if err = pool.QueryRow(ctx, `SELECT active_workers FROM tenant_quota_counters WHERE tenant_id=$1`, tenantA).Scan(&activeWorkers); err != nil || activeWorkers != 1 {
		t.Fatalf("active worker counter=%d err=%v", activeWorkers, err)
	}

	attemptID, runID, err := seedRunningAttempt(ctx, pool, tenantA, workflowA, suffix)
	if err != nil {
		t.Fatal(err)
	}
	importer, err := NewAdminImporter(pool)
	if err != nil {
		t.Fatal(err)
	}
	importedTriggerRequest := ImportTriggerRequest{
		TenantID: tenantA, ActorID: "migration_admin", SourceSystem: "fenix",
		TriggerID: "legacy_trigger_" + suffix, PublicHash: fmt.Sprintf("%x", sha256.Sum256([]byte("legacy-"+suffix)))[:16],
		WorkflowID: workflowA, Kind: TriggerManual, Name: "legacy-manual", Enabled: true,
	}
	importedTrigger, imported, err := importer.ImportTrigger(ctx, importedTriggerRequest)
	if err != nil || !imported || importedTrigger.ID != importedTriggerRequest.TriggerID || importedTrigger.PublicHash != importedTriggerRequest.PublicHash {
		t.Fatalf("import trigger=%+v created=%v err=%v", importedTrigger, imported, err)
	}
	if _, imported, err = importer.ImportTrigger(ctx, importedTriggerRequest); err != nil || imported {
		t.Fatalf("replay trigger import created=%v err=%v", imported, err)
	}
	importedRunRequest := ImportRunProjectionRequest{
		TenantID: tenantA, ActorID: "migration_admin", SourceSystem: "fenix",
		RunID: "legacy_run_" + suffix, WorkflowID: workflowA,
		WorkflowVersionID: "version_" + suffix, ExecutionPlanID: "plan_" + suffix,
		PlanHash: fmt.Sprintf("%x", sha256.Sum256([]byte("seed-"+suffix))), Status: "success",
		Input: json.RawMessage(`{"legacy":true}`), CreatedAt: now.Add(-time.Hour),
		Output:    integrationRunOutput(),
		StartedAt: timePointer(now.Add(-time.Hour)), FinishedAt: now.Add(-30 * time.Minute),
		Events: []ImportedRunEvent{
			{ID: "legacy_event_1_" + suffix, Sequence: 1, Type: "run.created", Payload: json.RawMessage(`{}`), CreatedAt: now.Add(-time.Hour)},
			{ID: "legacy_event_2_" + suffix, Sequence: 2, Type: "run.succeeded", Payload: json.RawMessage(`{"token":"must-redact"}`), CreatedAt: now.Add(-30 * time.Minute)},
		},
	}
	if imported, err = importer.ImportRunProjection(ctx, importedRunRequest); err != nil || !imported {
		t.Fatalf("import run projection created=%v err=%v", imported, err)
	}
	if imported, err = importer.ImportRunProjection(ctx, importedRunRequest); err != nil || imported {
		t.Fatalf("replay run projection created=%v err=%v", imported, err)
	}
	var importedPayload []byte
	if err = pool.QueryRow(ctx, `SELECT payload FROM run_events WHERE tenant_id=$1 AND run_id=$2 AND sequence=2`, tenantA, importedRunRequest.RunID).Scan(&importedPayload); err != nil || bytes.Contains(importedPayload, []byte("must-redact")) {
		t.Fatalf("imported event payload=%s err=%v", importedPayload, err)
	}
	if err = credentials.GrantForAttempt(ctx, GrantCredentialRequest{
		TenantID: tenantA, RunID: runID, AttemptID: attemptID, CredentialID: credential.ID,
		Purpose: "webhook_signing", FencingToken: 1, Now: now,
	}); err != nil {
		t.Fatal(err)
	}
	resolved, err := credentials.ResolveForAttempt(ctx, ResolveCredentialRequest{
		TenantID: tenantA, RunID: runID, AttemptID: attemptID, CredentialID: credential.ID,
		Purpose: "webhook_signing", FencingToken: 1, Now: now,
	})
	if err != nil || string(resolved) != secretValue {
		t.Fatalf("resolve credential=%q err=%v", resolved, err)
	}
	rotatedValue := "rotated-secret-" + suffix
	rotated, err := credentials.Rotate(ctx, tenantA, credential.ID, "admin_a", 2, []byte(rotatedValue))
	if err != nil || rotated.CurrentVersion != 2 {
		t.Fatalf("rotate credential=%+v err=%v", rotated, err)
	}
	resolved, err = credentials.ResolveForAttempt(ctx, ResolveCredentialRequest{
		TenantID: tenantA, RunID: runID, AttemptID: attemptID, CredentialID: credential.ID,
		Purpose: "webhook_signing", FencingToken: 1, Now: now,
	})
	if err != nil || string(resolved) != rotatedValue {
		t.Fatalf("resolve rotated credential=%q err=%v", resolved, err)
	}
	if _, err = pool.Exec(ctx, `UPDATE node_attempts SET fencing_token=2 WHERE tenant_id=$1 AND id=$2`, tenantA, attemptID); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE worker_leases SET fencing_token=2,expires_at=$3 WHERE tenant_id=$1 AND attempt_id=$2`, tenantA, attemptID, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err = credentials.ResolveForAttempt(ctx, ResolveCredentialRequest{
		TenantID: tenantA, RunID: runID, AttemptID: attemptID, CredentialID: credential.ID,
		Purpose: "webhook_signing", FencingToken: 1, Now: now,
	}); !errors.Is(err, ErrCredentialForbidden) {
		t.Fatalf("stale fencing resolve error=%v", err)
	}
	if _, err = credentials.Revoke(ctx, tenantA, credential.ID, "admin_a", now); err != nil {
		t.Fatal(err)
	}
	if _, err = credentials.ResolveForAttempt(ctx, ResolveCredentialRequest{
		TenantID: tenantA, RunID: runID, AttemptID: attemptID, CredentialID: credential.ID,
		Purpose: "webhook_signing", FencingToken: 2, Now: now,
	}); !errors.Is(err, ErrCredentialForbidden) {
		t.Fatalf("revoked resolve error=%v", err)
	}

	var deliveryMessageID string
	if err = pool.QueryRow(ctx, `SELECT id FROM outbox_messages WHERE tenant_id=$1 AND delivered_at IS NULL ORDER BY created_at,id LIMIT 1`, tenantA).Scan(&deliveryMessageID); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `
		UPDATE outbox_messages SET delivered_at=CASE WHEN id=$2 THEN NULL ELSE now() END,
		       max_attempts=CASE WHEN id=$2 THEN 2 ELSE max_attempts END
		WHERE tenant_id=$1 AND delivered_at IS NULL`, tenantA, deliveryMessageID); err != nil {
		t.Fatal(err)
	}
	deliveryStore := NewPostgresDeliveryStore(pool)
	dispatcher, err := NewOutboxDispatcher(deliveryStore, failingDelivery{}, "dispatcher", time.Minute, time.Millisecond, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	dispatchNow := time.Now().UTC().Add(time.Second)
	firstFailure, err := dispatcher.DispatchOne(ctx, tenantA, dispatchNow)
	if err != nil || firstFailure.RetryAt == nil {
		t.Fatalf("first delivery failure=%+v err=%v", firstFailure, err)
	}
	secondFailure, err := dispatcher.DispatchOne(ctx, tenantA, *firstFailure.RetryAt)
	if err != nil || !secondFailure.DeadLetter || secondFailure.MessageID != firstFailure.MessageID {
		t.Fatalf("second delivery failure=%+v err=%v", secondFailure, err)
	}
	letters, err := deliveryStore.ListDeadLetters(ctx, tenantA, 10)
	if err != nil || len(letters) != 1 || stringsContainSecret(letters[0].LastError) {
		t.Fatalf("dead letters=%+v err=%v", letters, err)
	}

	objects, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	outputs, err := NewOutputManager(pool, objects, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE retention_policies SET event_days=7,output_days=2,archive_before_delete=true WHERE tenant_id=$1`, tenantA); err != nil {
		t.Fatal(err)
	}
	downloadValue := []byte(`{"download":true}`)
	downloadable, err := outputs.Store(ctx, tenantA, runID, "application/json", nil, bytes.NewReader(downloadValue))
	if err != nil {
		t.Fatal(err)
	}
	downloadDigest, _ := hex.DecodeString(downloadable.SHA256)
	if _, err = pool.Exec(ctx, `
		INSERT INTO node_outputs(id,tenant_id,run_id,node_state_id,attempt_id,content_type,object_ref,size_bytes,content_hash,sensitivity,retention_until)
		SELECT $1,$2,$3,id,$4,'application/json',$5,$6,$7,'internal',now()+interval '2 days'
		FROM node_states WHERE tenant_id=$2 AND run_id=$3 LIMIT 1`, "download_output_"+suffix, tenantA, runID, attemptID,
		downloadable.ID, downloadable.Size, downloadDigest); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE runs SET output_id=$3 WHERE tenant_id=$1 AND id=$2`, tenantA, runID, "download_output_"+suffix); err != nil {
		t.Fatal(err)
	}
	downloadReader, downloadMetadata, err := outputs.OpenRunOutput(ctx, tenantA, runID)
	if err != nil || downloadMetadata.ID != downloadable.ID {
		t.Fatalf("open run output metadata=%+v err=%v", downloadMetadata, err)
	}
	downloaded, err := io.ReadAll(downloadReader)
	_ = downloadReader.Close()
	if err != nil || !bytes.Equal(downloaded, downloadValue) {
		t.Fatalf("downloaded=%s err=%v", downloaded, err)
	}
	if _, _, err = outputs.OpenRunOutput(ctx, tenantB, runID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant output read error=%v", err)
	}
	if _, err = pool.Exec(ctx, `UPDATE node_outputs SET retention_until=now()-interval '1 second' WHERE tenant_id=$1 AND id=$2`, tenantA, "download_output_"+suffix); err != nil {
		t.Fatal(err)
	}
	if _, _, err = outputs.OpenRunOutput(ctx, tenantA, runID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expired output read error=%v", err)
	}
	expired := now.Add(-time.Minute)
	managed, err := outputs.Store(ctx, tenantA, runID, "application/json", &expired, bytes.NewReader([]byte(`{"large":true}`)))
	if err != nil {
		t.Fatal(err)
	}
	compensated, err := outputs.Store(ctx, tenantA, runID, "application/json", nil, bytes.NewReader([]byte(`{"uncommitted":true}`)))
	if err != nil {
		t.Fatal(err)
	}
	if err = outputs.Discard(ctx, tenantA, compensated.ID); err != nil {
		t.Fatal(err)
	}
	if compensated.RetentionUntil == nil || compensated.RetentionUntil.Before(now.Add(47*time.Hour)) || compensated.RetentionUntil.After(now.Add(49*time.Hour)) {
		t.Fatalf("policy-derived retention=%v", compensated.RetentionUntil)
	}
	if _, _, err = outputs.Open(ctx, tenantA, compensated.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("discarded output open error=%v", err)
	}
	retention, err := NewRetention(pool, objects, 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	eventArchive, archivedEvents, err := retention.ArchiveEvents(ctx, tenantA, "admin_a", now.Add(time.Minute), 10)
	if err != nil || archivedEvents != 2 {
		t.Fatalf("event archive=%+v count=%d err=%v", eventArchive, archivedEvents, err)
	}
	archiveReader, _, err := outputs.Open(ctx, tenantA, eventArchive.ID)
	if err != nil {
		t.Fatal(err)
	}
	archiveBytes, err := io.ReadAll(archiveReader)
	_ = archiveReader.Close()
	if err != nil || bytes.Contains(archiveBytes, []byte("must-redact")) {
		t.Fatalf("event archive leaked secret or failed: err=%v", err)
	}
	if count, archiveErr := retention.ArchiveExpired(ctx, tenantA, now, 10); archiveErr != nil || count != 1 {
		t.Fatalf("archive count=%d err=%v", count, archiveErr)
	}
	if count, deleteErr := retention.DeleteArchived(ctx, tenantA, now, 10); deleteErr != nil || count != 1 {
		t.Fatalf("delete count=%d err=%v", count, deleteErr)
	}
	if _, _, err = outputs.Open(ctx, tenantA, managed.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted output open error=%v", err)
	}
	exported, err := retention.ExportTenant(ctx, tenantA, "admin_a", now.Add(time.Hour))
	if err != nil || exported.Status != "ready" {
		t.Fatalf("tenant export=%+v err=%v", exported, err)
	}
	exportReader, _, err := outputs.Open(ctx, tenantA, exported.ObjectID)
	if err != nil {
		t.Fatal(err)
	}
	exportBytes, err := io.ReadAll(exportReader)
	_ = exportReader.Close()
	if err != nil || bytes.Contains(exportBytes, []byte(secretValue)) || bytes.Contains(exportBytes, []byte(rotatedValue)) {
		t.Fatalf("tenant export leaked secret or failed: err=%v", err)
	}
	var pendingUploads int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM object_upload_intents WHERE tenant_id=$1`, tenantA).Scan(&pendingUploads); err != nil || pendingUploads != 0 {
		t.Fatalf("completed object operations left upload intents=%d err=%v", pendingUploads, err)
	}
	audit, err := NewAuditService(pool)
	if err != nil {
		t.Fatal(err)
	}
	records, err := audit.Query(ctx, AuditQuery{TenantID: tenantA, CanViewAudit: true, Limit: 100})
	if err != nil || len(records) < 5 {
		t.Fatalf("audit records=%d err=%v", len(records), err)
	}
	crossTenant, err := audit.Query(ctx, AuditQuery{TenantID: tenantB, CanViewAudit: true, Limit: 100})
	if err != nil || len(crossTenant) != 0 {
		t.Fatalf("cross-tenant audit records=%d err=%v", len(crossTenant), err)
	}
}
