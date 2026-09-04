package platformops

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/postgres"
)

type historicalRetentionFixture struct {
	managedID   string
	objectRunID string
	objectNode  string
	inlineNode  string
	value       []byte
}

// 验证 retention 策略只收紧本租户历史 sensitive/object/inline 输出，且后续放宽不会复活或延长数据。
func TestPostgresRetentionPolicyTightensHistoricalOutputs(t *testing.T) {
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
	tenantA, tenantB := "policy_a_"+suffix, "policy_b_"+suffix
	historical := time.Now().UTC().Truncate(time.Second).Add(-5 * 24 * time.Hour)
	objects, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewOutputManager(pool, objects, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	fixtureA := seedHistoricalRetentionFixture(t, ctx, pool, manager, tenantA, suffix+"_a", historical)
	fixtureB := seedHistoricalRetentionFixture(t, ctx, pool, manager, tenantB, suffix+"_b", historical)
	retention, err := NewRetention(pool, objects, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = retention.SetPolicy(ctx, RetentionPolicy{
		TenantID: tenantA, EventDays: 7, OutputDays: 1, ArchiveBeforeDelete: true,
	}); err != nil {
		t.Fatal(err)
	}

	tightened := historical.Add(24 * time.Hour)
	original := historical.Add(30 * 24 * time.Hour)
	assertRetentionDeadline(t, ctx, pool, "managed_objects", tenantA, fixtureA.managedID, tightened)
	assertRetentionDeadline(t, ctx, pool, "node_outputs", tenantA, fixtureA.objectNode, tightened)
	assertRetentionDeadline(t, ctx, pool, "node_outputs", tenantA, fixtureA.inlineNode, tightened)
	assertRetentionDeadline(t, ctx, pool, "managed_objects", tenantB, fixtureB.managedID, original)
	assertRetentionDeadline(t, ctx, pool, "node_outputs", tenantB, fixtureB.objectNode, original)
	assertRetentionDeadline(t, ctx, pool, "node_outputs", tenantB, fixtureB.inlineNode, original)

	if _, _, openErr := manager.OpenRunOutput(ctx, tenantA, fixtureA.objectRunID); !errors.Is(openErr, ErrNotFound) {
		t.Fatalf("tightened tenant output remained readable: %v", openErr)
	}
	reader, metadata, err := manager.OpenRunOutput(ctx, tenantB, fixtureB.objectRunID)
	if err != nil {
		t.Fatal(err)
	}
	read, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || !bytes.Equal(read, fixtureB.value) || metadata.TenantID != tenantB {
		t.Fatalf("isolated object read bytes=%q metadata=%+v readErr=%v closeErr=%v", read, metadata, readErr, closeErr)
	}

	if _, err = pool.Exec(ctx, `UPDATE node_outputs SET retention_status='archived',archived_at=now() WHERE tenant_id=$1 AND id=$2`, tenantA, fixtureA.inlineNode); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE managed_objects SET status='archived',archived_at=now() WHERE tenant_id=$1 AND id=$2`, tenantA, fixtureA.managedID); err != nil {
		t.Fatal(err)
	}
	if err = retention.SetPolicy(ctx, RetentionPolicy{
		TenantID: tenantA, EventDays: 60, OutputDays: 60, ArchiveBeforeDelete: false,
	}); err != nil {
		t.Fatal(err)
	}
	assertRetentionDeadline(t, ctx, pool, "managed_objects", tenantA, fixtureA.managedID, tightened)
	assertRetentionDeadline(t, ctx, pool, "node_outputs", tenantA, fixtureA.objectNode, tightened)
	assertRetentionDeadline(t, ctx, pool, "node_outputs", tenantA, fixtureA.inlineNode, tightened)
	var nodeStatus, objectStatus string
	if err = pool.QueryRow(ctx, `SELECT retention_status FROM node_outputs WHERE tenant_id=$1 AND id=$2`, tenantA, fixtureA.inlineNode).Scan(&nodeStatus); err != nil || nodeStatus != "archived" {
		t.Fatalf("archived node output was resurrected: status=%q err=%v", nodeStatus, err)
	}
	if err = pool.QueryRow(ctx, `SELECT status FROM managed_objects WHERE tenant_id=$1 AND id=$2`, tenantA, fixtureA.managedID).Scan(&objectStatus); err != nil || objectStatus != "archived" {
		t.Fatalf("archived managed output was resurrected: status=%q err=%v", objectStatus, err)
	}
}

// 验证输出登记与策略收紧共享租户锁，旧策略下已计算的并发写入不会越过新的最长保留期。
func TestPostgresRetentionPolicySerializesConcurrentOutputRegistration(t *testing.T) {
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
	tenantID, workflowID := "policy_race_"+suffix, "workflow_"+suffix
	if err = seedPlatformTenant(ctx, pool, tenantID, workflowID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), `DELETE FROM tenants WHERE id=$1`, tenantID) })
	_, runID, err := seedRunningAttempt(ctx, pool, tenantID, workflowID, "race_"+suffix)
	if err != nil {
		t.Fatal(err)
	}

	functionName, triggerName := "delay_output_"+suffix, "delay_output_trigger_"+suffix
	quotedTenant := "'" + tenantID + "'"
	if _, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN PERFORM pg_sleep(0.5); RETURN NEW; END $$;
		CREATE TRIGGER %s BEFORE INSERT ON managed_objects
		FOR EACH ROW WHEN (NEW.tenant_id = %s) EXECUTE FUNCTION %s()`,
		functionName, triggerName, quotedTenant, functionName)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), fmt.Sprintf(`DROP TRIGGER IF EXISTS %s ON managed_objects`, triggerName))
		_, _ = pool.Exec(context.Background(), fmt.Sprintf(`DROP FUNCTION IF EXISTS %s()`, functionName))
	})

	objects, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewOutputManager(pool, objects, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	retention, err := NewRetention(pool, objects, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	storeResult := make(chan error, 1)
	go func() {
		_, storeErr := manager.Store(ctx, tenantID, runID, "application/json", nil, bytes.NewReader([]byte(`{"race":true}`)))
		storeResult <- storeErr
	}()
	waitForRetentionLock(t, ctx, pool, tenantID)
	policyResult := make(chan error, 1)
	go func() {
		policyResult <- retention.SetPolicy(ctx, RetentionPolicy{
			TenantID: tenantID, EventDays: 7, OutputDays: 1, ArchiveBeforeDelete: true,
		})
	}()
	select {
	case policyErr := <-policyResult:
		t.Fatalf("policy update bypassed in-flight output registration: %v", policyErr)
	case <-time.After(100 * time.Millisecond):
	}
	if err = <-storeResult; err != nil {
		t.Fatal(err)
	}
	if err = <-policyResult; err != nil {
		t.Fatal(err)
	}
	var violations int
	if err = pool.QueryRow(ctx, `
		SELECT count(*) FROM managed_objects
		WHERE tenant_id=$1 AND kind='output'
		  AND retention_until > created_at + interval '1 day'`, tenantID).Scan(&violations); err != nil {
		t.Fatal(err)
	}
	if violations != 0 {
		t.Fatalf("concurrent output escaped tightened retention: %d", violations)
	}
}

func waitForRetentionLock(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tenantID string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var acquired bool
		err = tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended('supaflow:retention:' || $1, 0))`, tenantID).Scan(&acquired)
		_ = tx.Rollback(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !acquired {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("output registration never acquired tenant retention lock")
}

func seedHistoricalRetentionFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool, manager *OutputManager, tenantID, suffix string, createdAt time.Time) historicalRetentionFixture {
	t.Helper()
	objectWorkflow := "workflow_object_" + suffix
	if err := seedPlatformTenant(ctx, pool, tenantID, objectWorkflow); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE retention_policies SET event_days=30,output_days=30,archive_before_delete=true WHERE tenant_id=$1`, tenantID); err != nil {
		t.Fatal(err)
	}
	objectAttempt, objectRun, err := seedRunningAttempt(ctx, pool, tenantID, objectWorkflow, "object_"+suffix)
	if err != nil {
		t.Fatal(err)
	}
	value := []byte(`{"secret":"retained"}`)
	managed, err := manager.Store(ctx, tenantID, objectRun, "application/json", nil, bytes.NewReader(value))
	if err != nil {
		t.Fatal(err)
	}
	originalDeadline := createdAt.Add(30 * 24 * time.Hour)
	if _, err = pool.Exec(ctx, `UPDATE managed_objects SET created_at=$3,retention_until=$4 WHERE tenant_id=$1 AND id=$2`, tenantID, managed.ID, createdAt, originalDeadline); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(value)
	objectNode := "output_object_" + suffix
	if _, err = pool.Exec(ctx, `
		INSERT INTO node_outputs
		(id,tenant_id,run_id,node_state_id,attempt_id,content_type,object_ref,size_bytes,content_hash,sensitivity,created_at,retention_until)
		VALUES ($1,$2,$3,$4,$5,'application/json',$6,$7,$8,'sensitive',$9,$10)`,
		objectNode, tenantID, objectRun, "node_object_"+suffix, objectAttempt, managed.ID,
		len(value), digest[:], createdAt, originalDeadline); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE runs SET output_id=$3 WHERE tenant_id=$1 AND id=$2`, tenantID, objectRun, objectNode); err != nil {
		t.Fatal(err)
	}

	inlineWorkflow := "workflow_inline_" + suffix
	if _, err = pool.Exec(ctx, `INSERT INTO workflows (tenant_id,id,slug,name,created_by) VALUES ($1,$2,$2,$2,'integration')`, tenantID, inlineWorkflow); err != nil {
		t.Fatal(err)
	}
	inlineAttempt, inlineRun, err := seedRunningAttempt(ctx, pool, tenantID, inlineWorkflow, "inline_"+suffix)
	if err != nil {
		t.Fatal(err)
	}
	inlineNode := "output_inline_" + suffix
	if _, err = pool.Exec(ctx, `
		INSERT INTO node_outputs
		(id,tenant_id,run_id,node_state_id,attempt_id,content_type,inline_value,size_bytes,content_hash,sensitivity,created_at,retention_until)
		VALUES ($1,$2,$3,$4,$5,'application/json',$6,$7,$8,'sensitive',$9,$10)`,
		inlineNode, tenantID, inlineRun, "node_inline_"+suffix, inlineAttempt, value,
		len(value), digest[:], createdAt, originalDeadline); err != nil {
		t.Fatal(err)
	}
	return historicalRetentionFixture{managedID: managed.ID, objectRunID: objectRun, objectNode: objectNode, inlineNode: inlineNode, value: value}
}

func assertRetentionDeadline(t *testing.T, ctx context.Context, pool *pgxpool.Pool, table, tenantID, id string, expected time.Time) {
	t.Helper()
	if table != "managed_objects" && table != "node_outputs" {
		t.Fatalf("unsupported retention table %q", table)
	}
	var actual time.Time
	query := `SELECT retention_until FROM ` + table + ` WHERE tenant_id=$1 AND id=$2`
	if err := pool.QueryRow(ctx, query, tenantID, id).Scan(&actual); err != nil || !actual.Equal(expected) {
		t.Fatalf("%s/%s retention=%v want=%v err=%v", table, id, actual, expected, err)
	}
}
