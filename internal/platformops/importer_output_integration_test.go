package platformops

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/postgres"
)

// 验证真实 PostgreSQL 中 Fenix 终态 output 成为 node_outputs 与 runs.output_id 投影，事件仅保留 metadata。
func TestPostgresImportRunProjectsClassifiedOutput(t *testing.T) {
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
	tenantID, workflowID := "import_output_"+suffix, "workflow_"+suffix
	if err = seedPlatformTenant(ctx, pool, tenantID, workflowID); err != nil {
		t.Fatal(err)
	}
	if _, _, err = seedRunningAttempt(ctx, pool, tenantID, workflowID, suffix); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	value := json.RawMessage(`{"result":"legacy-value","token":"classified"}`)
	digest := sha256.Sum256(value)
	request := ImportRunProjectionRequest{
		TenantID: tenantID, ActorID: "migration", SourceSystem: "fenix", RunID: "legacy_" + suffix,
		WorkflowID: workflowID, WorkflowVersionID: "version_" + suffix, ExecutionPlanID: "plan_" + suffix,
		PlanHash: hex.EncodeToString(sha256Sum("seed-" + suffix)), Status: "success", Input: json.RawMessage(`{}`),
		Output:    &ImportedRunOutput{Value: value, ContentType: "application/json", Size: int64(len(value)), SHA256: hex.EncodeToString(digest[:]), Sensitive: true},
		CreatedAt: now.Add(-time.Hour), StartedAt: timePointer(now.Add(-time.Hour)), FinishedAt: now,
		Events: []ImportedRunEvent{{ID: "event_" + suffix, Sequence: 1, Type: "run.migrated", Payload: json.RawMessage(`{"outputMetadata":{"sensitive":true}}`), CreatedAt: now}},
	}
	importer, err := NewAdminImporter(pool)
	if err != nil {
		t.Fatal(err)
	}
	if created, importErr := importer.ImportRunProjection(ctx, request); importErr != nil || !created {
		t.Fatalf("created=%v err=%v", created, importErr)
	}
	var runOutputID, outputID, contentType, sensitivity, retentionStatus string
	var inline []byte
	var size int64
	var storedHash []byte
	var retentionUntil *time.Time
	err = pool.QueryRow(ctx, `
		SELECT r.output_id,o.id,o.content_type,o.inline_value,o.size_bytes,o.content_hash,
		       o.sensitivity,o.retention_status,o.retention_until
		FROM runs r JOIN node_outputs o ON o.tenant_id=r.tenant_id AND o.id=r.output_id
		WHERE r.tenant_id=$1 AND r.id=$2`, tenantID, request.RunID).Scan(
		&runOutputID, &outputID, &contentType, &inline, &size, &storedHash, &sensitivity, &retentionStatus, &retentionUntil)
	if err != nil {
		t.Fatal(err)
	}
	var expectedJSON, storedJSON any
	jsonMatches := json.Unmarshal(value, &expectedJSON) == nil && json.Unmarshal(inline, &storedJSON) == nil && reflect.DeepEqual(expectedJSON, storedJSON)
	if runOutputID != outputID || contentType != "application/json" || !jsonMatches || size != int64(len(value)) || !bytes.Equal(storedHash, digest[:]) || sensitivity != "sensitive" || retentionStatus != "active" || retentionUntil == nil {
		t.Fatalf("runOutput=%q output=%q type=%q size=%d sensitivity=%q status=%q retention=%v", runOutputID, outputID, contentType, size, sensitivity, retentionStatus, retentionUntil)
	}
}

func sha256Sum(value string) []byte {
	digest := sha256.Sum256([]byte(value))
	return digest[:]
}
