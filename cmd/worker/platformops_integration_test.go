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

// 验证多租户 worker 启动在首个或后续 tenant 失败时补偿全部已取得名额，重启可重新 admission。
func TestConfigureWorkerOperationsCompensatesPartialTenantAcquisition(t *testing.T) {
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
	tenantA, tenantB := "worker_start_a_"+suffix, "worker_start_b_"+suffix
	if _, err = pool.Exec(ctx, `INSERT INTO tenants(id,name) VALUES ($1,$1),($2,$2)`, tenantA, tenantB); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM tenants WHERE id=ANY($1)`, []string{tenantA, tenantB})
	})
	admission, _ := platformops.NewAdmission(pool, time.Second)
	for _, tenantID := range []string{tenantA, tenantB} {
		if err = admission.ConfigureQuota(ctx, platformops.Quota{TenantID: tenantID, MaxActiveRuns: 1, MaxActiveWorkers: 1, MaxQueueDepth: 1}); err != nil {
			t.Fatal(err)
		}
	}
	registry := platformops.NewWorkerRegistry(pool)
	if err = registry.Heartbeat(ctx, tenantA, "blocker-a", 1, 0); err != nil {
		t.Fatal(err)
	}
	if _, err = configureWorkerOperations(ctx, pool, "worker-restart", []string{tenantA, tenantB}); err == nil {
		t.Fatal("expected first tenant acquisition failure")
	}
	assertWorkerCount(t, ctx, pool, tenantA, 1)
	if err = admission.ReleaseWorker(ctx, tenantA, "blocker-a"); err != nil {
		t.Fatal(err)
	}
	if err = registry.Heartbeat(ctx, tenantB, "blocker-b", 1, 0); err != nil {
		t.Fatal(err)
	}
	if _, err = configureWorkerOperations(ctx, pool, "worker-restart", []string{tenantA, tenantB}); err == nil {
		t.Fatal("expected second tenant acquisition failure")
	}
	assertWorkerCount(t, ctx, pool, tenantA, 0)
	assertWorkerCount(t, ctx, pool, tenantB, 1)
	operations, err := configureWorkerOperations(ctx, pool, "worker-restart", []string{tenantA})
	if err != nil {
		t.Fatalf("restart admission: %v", err)
	}
	assertWorkerCount(t, ctx, pool, tenantA, 1)
	if err = operations.close(); err != nil {
		t.Fatal(err)
	}
}

func assertWorkerCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tenantID string, want int) {
	t.Helper()
	var got int
	if err := pool.QueryRow(ctx, `SELECT active_workers FROM tenant_quota_counters WHERE tenant_id=$1`, tenantID).Scan(&got); err != nil || got != want {
		t.Fatalf("tenant=%s activeWorkers=%d want=%d err=%v", tenantID, got, want, err)
	}
}
