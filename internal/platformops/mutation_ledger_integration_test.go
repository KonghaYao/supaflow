package platformops

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/postgres"
)

// 验证真实 PostgreSQL 上 lease 接管会保留稳定 operation identity、拒绝旧 owner，并恢复完成结果与审计。
func TestPostgresMutationLedgerLeaseTakeoverAndReplay(t *testing.T) {
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
	organizationID := fmt.Sprintf("mutation_%d", time.Now().UnixNano())
	ledger, err := NewPostgresMutationLedger(pool)
	if err != nil {
		t.Fatal(err)
	}
	hash := bytes.Repeat([]byte{4}, 32)
	first, err := ledger.Claim(ctx, organizationID, "credential.create", "stable-key", hash)
	if err != nil || first.Owner == "" || first.OperationID == "" {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	if _, err = ledger.Claim(ctx, organizationID, "credential.create", "stable-key", hash); !errors.Is(err, ErrConflict) {
		t.Fatalf("active lease error=%v", err)
	}
	if _, err = pool.Exec(ctx, `UPDATE ops_mutation_ledger SET lease_expires_at=clock_timestamp()-interval '1 second'
		WHERE organization_id=$1 AND operation=$2 AND idempotency_key=$3`, organizationID, "credential.create", "stable-key"); err != nil {
		t.Fatalf("expire first owner lease: %v", err)
	}
	second, err := ledger.Claim(ctx, organizationID, "credential.create", "stable-key", hash)
	if err != nil || second.Owner == first.Owner || second.OperationID != first.OperationID {
		t.Fatalf("takeover first=%+v second=%+v err=%v", first, second, err)
	}
	completion := MutationCompletion{Owner: first.Owner, StatusCode: 201, ContentType: "application/json", Body: []byte(`{"id":"cred"}`)}
	if err = ledger.Complete(ctx, organizationID, "credential.create", "stable-key", completion); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale owner completion error=%v", err)
	}
	completion.Owner = second.Owner
	completion.Audit = &AuditRecord{OrganizationID: organizationID, ActorID: "admin", Action: "credential.create", ResourceType: "credential", ResourceID: "cred", Outcome: AuditAllowed}
	if err = ledger.Complete(ctx, organizationID, "credential.create", "stable-key", completion); err != nil {
		t.Fatal(err)
	}
	replay, err := ledger.Claim(ctx, organizationID, "credential.create", "stable-key", hash)
	if err != nil || !replay.Replay || replay.OperationID != first.OperationID || replay.StatusCode != 201 || !bytes.Equal(replay.Body, completion.Body) {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	var auditCount int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM audit_records WHERE organization_id=$1 AND action='credential.create'`, organizationID).Scan(&auditCount); err != nil || auditCount != 1 {
		t.Fatalf("audit count=%d err=%v", auditCount, err)
	}
}

// 验证 retryable 结果立即交还 lease 且保留 operation identity，避免部分提交后的同键重试冲突。
func TestPostgresMutationLedgerRetryPreservesOperationIdentity(t *testing.T) {
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
	organizationID := fmt.Sprintf("mutation_retry_%d", time.Now().UnixNano())
	ledger, _ := NewPostgresMutationLedger(pool)
	hash := bytes.Repeat([]byte{5}, 32)
	first, err := ledger.Claim(ctx, organizationID, "trigger.event.receive", "event-retry-key", hash)
	if err != nil {
		t.Fatal(err)
	}
	if err = ledger.Retry(ctx, organizationID, "trigger.event.receive", "event-retry-key", first.Owner); err != nil {
		t.Fatal(err)
	}
	second, err := ledger.Claim(ctx, organizationID, "trigger.event.receive", "event-retry-key", hash)
	if err != nil || second.Owner == first.Owner || second.OperationID != first.OperationID {
		t.Fatalf("first=%+v second=%+v err=%v", first, second, err)
	}
}
