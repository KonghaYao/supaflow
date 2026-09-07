package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/platformops"
	"github.com/pazhou/supaflow/internal/postgres"
)

// 验证多租户 worker 启动在首个或后续 organization 失败时补偿全部已取得名额，重启可重新 admission。
func TestConfigureWorkerOperationsCompensatesPartialOrganizationAcquisition(t *testing.T) {
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
	suffix := fmt.Sprint(time.Now().UnixNano())
	organizationA, organizationB := "worker_start_a_"+suffix, "worker_start_b_"+suffix
	admission, _ := platformops.NewAdmission(pool, time.Second)
	for _, organizationID := range []string{organizationA, organizationB} {
		if err = admission.ConfigureQuota(ctx, platformops.Quota{OrganizationID: organizationID, MaxActiveRuns: 1, MaxActiveWorkers: 1, MaxQueueDepth: 1}); err != nil {
			t.Fatal(err)
		}
	}
	registry := platformops.NewWorkerRegistry(pool)
	if err = registry.Heartbeat(ctx, organizationA, "blocker-a", 1, 0); err != nil {
		t.Fatal(err)
	}
	if _, err = configureWorkerOperations(ctx, pool, "worker-restart", []string{organizationA, organizationB}); err == nil {
		t.Fatal("expected first organization acquisition failure")
	}
	assertWorkerCount(t, ctx, pool, organizationA, 1)
	if err = admission.ReleaseWorker(ctx, organizationA, "blocker-a"); err != nil {
		t.Fatal(err)
	}
	if err = registry.Heartbeat(ctx, organizationB, "blocker-b", 1, 0); err != nil {
		t.Fatal(err)
	}
	if _, err = configureWorkerOperations(ctx, pool, "worker-restart", []string{organizationA, organizationB}); err == nil {
		t.Fatal("expected second organization acquisition failure")
	}
	assertWorkerCount(t, ctx, pool, organizationA, 0)
	assertWorkerCount(t, ctx, pool, organizationB, 1)
	operations, err := configureWorkerOperations(ctx, pool, "worker-restart", []string{organizationA})
	if err != nil {
		t.Fatalf("restart admission: %v", err)
	}
	assertWorkerCount(t, ctx, pool, organizationA, 1)
	if err = operations.close(); err != nil {
		t.Fatal(err)
	}
}

func assertWorkerCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, organizationID string, want int) {
	t.Helper()
	var got int
	if err := pool.QueryRow(ctx, `SELECT active_workers FROM organization_quota_counters WHERE organization_id=$1`, organizationID).Scan(&got); err != nil || got != want {
		t.Fatalf("organization=%s activeWorkers=%d want=%d err=%v", organizationID, got, want, err)
	}
}
