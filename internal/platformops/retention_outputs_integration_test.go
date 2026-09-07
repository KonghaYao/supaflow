package platformops

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/postgres"
)

// 验证真实 PostgreSQL 中 inline node output 经 deadline、archive、tombstone 流程并保留同事务审计。
func TestPostgresInlineOutputRetentionLifecycle(t *testing.T) {
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
	organizationID, workflowID := "retention_"+suffix, "workflow_"+suffix
	if err = seedPlatformOrganization(ctx, pool, organizationID, workflowID); err != nil {
		t.Fatal(err)
	}
	attemptID, runID, err := seedRunningAttempt(ctx, pool, organizationID, workflowID, suffix)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	value := []byte(`{"answer":42}`)
	digest := sha256.Sum256(value)
	outputID := "output_" + suffix
	if _, err = pool.Exec(ctx, `
		INSERT INTO node_outputs
		(id,organization_id,run_id,node_state_id,attempt_id,content_type,inline_value,size_bytes,content_hash,retention_until)
		VALUES ($1,$2,$3,$4,$5,'application/json',$6,$7,$8,$9)`, outputID, organizationID, runID,
		"node_"+suffix, attemptID, value, len(value), digest[:], now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	objects, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	retention, err := NewRetention(pool, objects, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if count, archiveErr := retention.ArchiveNodeOutputs(ctx, organizationID, "retention-worker", 10); archiveErr != nil || count != 1 {
		t.Fatalf("archive count=%d err=%v", count, archiveErr)
	}
	if count, deleteErr := retention.DeleteArchivedNodeOutputs(ctx, organizationID, "retention-worker", 0, 10); deleteErr != nil || count != 1 {
		t.Fatalf("delete count=%d err=%v", count, deleteErr)
	}
	var status string
	var inlineValue []byte
	var archivedAt, deletedAt *time.Time
	if err = pool.QueryRow(ctx, `SELECT retention_status,inline_value,archived_at,deleted_at FROM node_outputs WHERE organization_id=$1 AND id=$2`, organizationID, outputID).Scan(&status, &inlineValue, &archivedAt, &deletedAt); err != nil {
		t.Fatal(err)
	}
	if status != "deleted" || inlineValue != nil || archivedAt == nil || deletedAt == nil {
		t.Fatalf("status=%q inline=%q archived=%v deleted=%v", status, inlineValue, archivedAt, deletedAt)
	}
	var audits int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM audit_records WHERE organization_id=$1 AND action IN ('retention.outputs.archive','retention.outputs.delete')`, organizationID).Scan(&audits); err != nil || audits != 2 {
		t.Fatalf("audits=%d err=%v", audits, err)
	}
}
