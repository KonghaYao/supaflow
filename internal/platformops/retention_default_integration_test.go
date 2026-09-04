package platformops

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/postgres"
)

// 验证迁移后新增 tenant 会原子获得默认策略，且 supervisor 路径能实际归档过期事件与输出。
func TestPostgresNewTenantReceivesEnforceableDefaultRetention(t *testing.T) {
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
	tenantID, workflowID := "default_policy_"+suffix, "workflow_"+suffix
	if _, err = pool.Exec(ctx, `INSERT INTO tenants(id,name) VALUES($1,$1)`, tenantID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), `DELETE FROM tenants WHERE id=$1`, tenantID) })
	var eventDays, outputDays int
	var archive bool
	if err = pool.QueryRow(ctx, `SELECT event_days,output_days,archive_before_delete FROM retention_policies WHERE tenant_id=$1`, tenantID).
		Scan(&eventDays, &outputDays, &archive); err != nil || eventDays != 30 || outputDays != 30 || !archive {
		t.Fatalf("default retention event=%d output=%d archive=%v err=%v", eventDays, outputDays, archive, err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO workflows(tenant_id,id,slug,name,created_by) VALUES($1,$2,$2,$2,'test')`, tenantID, workflowID); err != nil {
		t.Fatal(err)
	}
	attemptID, runID, err := seedRunningAttempt(ctx, pool, tenantID, workflowID, suffix)
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().UTC().Add(-31 * 24 * time.Hour)
	if _, err = pool.Exec(ctx, `UPDATE runs SET status='success' WHERE tenant_id=$1 AND id=$2`, tenantID, runID); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO run_events(id,tenant_id,run_id,sequence,event_type,payload,created_at) VALUES($1,$2,$3,1,'run.succeeded','{}',$4)`, "event_"+suffix, tenantID, runID, old); err != nil {
		t.Fatal(err)
	}
	objects, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewOutputManager(pool, objects, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	value := []byte(`{"expired":true}`)
	expired := time.Now().UTC().Add(-time.Hour)
	managed, err := manager.Store(ctx, tenantID, runID, "application/json", &expired, bytes.NewReader(value))
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(value)
	outputID := "inline_" + suffix
	if _, err = pool.Exec(ctx, `
		INSERT INTO node_outputs
		(id,tenant_id,run_id,node_state_id,attempt_id,content_type,inline_value,size_bytes,content_hash,sensitivity,retention_until)
		VALUES($1,$2,$3,$4,$5,'application/json',$6,$7,$8,'internal',$9)`,
		outputID, tenantID, runID, "node_"+suffix, attemptID, value, len(value), digest[:], expired); err != nil {
		t.Fatal(err)
	}
	retention, err := NewRetention(pool, objects, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	sweep, err := retention.SweepTenant(ctx, tenantID, time.Now().UTC(), 100)
	if err != nil || sweep.EventsRemoved != 1 || sweep.ObjectsArchived != 1 || sweep.InlineOutputsArchived != 1 {
		t.Fatalf("default retention sweep=%+v err=%v managed=%s", sweep, err, managed.ID)
	}
}
