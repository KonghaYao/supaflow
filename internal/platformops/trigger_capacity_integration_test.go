package platformops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/postgres"
)

// 验证数据库门禁同时限制 trigger、pending firing 与 pending outbox，并让终态容量可复用。
func TestTriggerIngressQuotaIsTenantHardBound(t *testing.T) {
	ctx, pool := triggerCapacityPool(t)
	tenantID, workflowID := seedCapacityTenant(t, ctx, pool, "hard_bound")
	admission, _ := NewAdmission(pool, time.Second)
	err := admission.ConfigureQuota(ctx, Quota{TenantID: tenantID, MaxActiveRuns: 10, MaxActiveWorkers: 2,
		MaxQueueDepth: 10, MaxTriggers: 1, MaxPendingTriggerFirings: 1, MaxPendingOutboxMessages: 1})
	if err != nil {
		t.Fatal(err)
	}
	store := NewPostgresTriggerStore(pool)
	service, _ := NewTriggerService(store, nil, time.Minute)
	trigger, err := service.Create(ctx, CreateTriggerRequest{TenantID: tenantID, ActorID: "operator",
		WorkflowID: workflowID, Kind: TriggerAPI, Name: "only"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Create(ctx, CreateTriggerRequest{TenantID: tenantID, ActorID: "operator",
		WorkflowID: workflowID, Kind: TriggerAPI, Name: "overflow"}); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("trigger quota error=%v", err)
	}
	first, created, err := service.Fire(ctx, FireRequest{TenantID: tenantID, TriggerID: trigger.ID,
		ExpectedKind: TriggerAPI, DeduplicationKey: "request-one", Payload: json.RawMessage(`{"n":1}`)})
	if err != nil || !created {
		t.Fatalf("first firing=%+v created=%t err=%v", first, created, err)
	}
	if _, _, err = service.Fire(ctx, FireRequest{TenantID: tenantID, TriggerID: trigger.ID,
		ExpectedKind: TriggerAPI, DeduplicationKey: "request-two", Payload: json.RawMessage(`{"n":2}`)}); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("pending quota error=%v", err)
	}
	if _, err = pool.Exec(ctx, `UPDATE outbox_messages SET delivered_at=clock_timestamp()
		WHERE tenant_id=$1 AND payload->>'firingId'=$2`, tenantID, first.ID); err != nil {
		t.Fatal(err)
	}
	if _, created, err = service.Fire(ctx, FireRequest{TenantID: tenantID, TriggerID: trigger.ID,
		ExpectedKind: TriggerAPI, DeduplicationKey: "request-two", Payload: json.RawMessage(`{"n":2}`)}); err != nil || !created {
		t.Fatalf("reused capacity created=%t err=%v", created, err)
	}
}

// 验证历史 catch-up 已饱和的 noisy tenant 只让出自己的轮次，其他租户仍能在同一 Tick 提交。
func TestScheduleTickIsFairAcrossSaturatedTenants(t *testing.T) {
	ctx, pool := triggerCapacityPool(t)
	tenantA, workflowA := seedCapacityTenant(t, ctx, pool, "fair_a")
	tenantB, workflowB := seedCapacityTenant(t, ctx, pool, "fair_b")
	store := NewPostgresTriggerStore(pool)
	service, _ := NewTriggerService(store, nil, time.Minute)
	admission, _ := NewAdmission(pool, time.Second)
	for _, tenantID := range []string{tenantA, tenantB} {
		if err := admission.ConfigureQuota(ctx, Quota{TenantID: tenantID, MaxActiveRuns: 10, MaxActiveWorkers: 2,
			MaxQueueDepth: 10, MaxTriggers: 10, MaxPendingTriggerFirings: 1, MaxPendingOutboxMessages: 1}); err != nil {
			t.Fatal(err)
		}
	}
	filler, err := service.Create(ctx, CreateTriggerRequest{TenantID: tenantA, ActorID: "operator",
		WorkflowID: workflowA, Kind: TriggerAPI, Name: "capacity-filler"})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = service.Fire(ctx, FireRequest{TenantID: tenantA, TriggerID: filler.ID,
		ExpectedKind: TriggerAPI, DeduplicationKey: "fill-capacity", Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	databaseNow := databaseClock(t, ctx, pool)
	oldA, dueB := databaseNow.Add(-time.Hour), databaseNow.Add(-time.Minute)
	if _, err = service.Create(ctx, CreateTriggerRequest{TenantID: tenantA, ActorID: "operator", WorkflowID: workflowA,
		Kind: TriggerSchedule, Name: "noisy-catch-up", ScheduleInterval: time.Minute,
		MisfirePolicy: MisfireCatchUp, FirstFireAt: &oldA}); err != nil {
		t.Fatal(err)
	}
	triggerB, err := service.Create(ctx, CreateTriggerRequest{TenantID: tenantB, ActorID: "operator", WorkflowID: workflowB,
		Kind: TriggerSchedule, Name: "healthy", ScheduleInterval: time.Minute,
		MisfirePolicy: MisfireFireOnce, FirstFireAt: &dueB})
	if err != nil {
		t.Fatal(err)
	}
	firings, tickErr := service.Tick(ctx, ScheduleOptions{Now: time.Unix(1, 0), Limit: 2, MaxCatchUp: 100, MisfireGrace: time.Minute})
	if !errors.Is(tickErr, ErrRateLimited) || len(firings) != 1 || firings[0].TenantID != tenantB || firings[0].TriggerID != triggerB.ID {
		t.Fatalf("firings=%+v err=%v", firings, tickErr)
	}
}

// 验证 100 条边界以上的事件 fanout 使用多个数据库事务且返回稳定的首次 firing。
func TestFireEventUsesBoundedTransactionPages(t *testing.T) {
	ctx, pool := triggerCapacityPool(t)
	tenantID, workflowID := seedCapacityTenant(t, ctx, pool, "event_pages")
	admission, _ := NewAdmission(pool, time.Second)
	if err := admission.ConfigureQuota(ctx, Quota{TenantID: tenantID, MaxActiveRuns: 10, MaxActiveWorkers: 2,
		MaxQueueDepth: 10, MaxTriggers: 200, MaxPendingTriggerFirings: 200, MaxPendingOutboxMessages: 200}); err != nil {
		t.Fatal(err)
	}
	store := NewPostgresTriggerStore(pool)
	service, _ := NewTriggerService(store, nil, time.Minute)
	for index := 0; index < eventFanoutPageSize+1; index++ {
		if _, err := service.Create(ctx, CreateTriggerRequest{TenantID: tenantID, ActorID: "operator",
			WorkflowID: workflowID, Kind: TriggerEvent, Name: fmt.Sprintf("event-%03d", index), EventType: "document.ready"}); err != nil {
			t.Fatal(err)
		}
	}
	at := databaseClock(t, ctx, pool)
	firings, err := service.ReceiveEvent(ctx, tenantID, "document.ready", "event-page-request", json.RawMessage(`{"id":1}`), at)
	if err != nil || len(firings) != eventFanoutPageSize+1 {
		t.Fatalf("firings=%d err=%v", len(firings), err)
	}
	var transactions int
	if err = pool.QueryRow(ctx, `SELECT count(DISTINCT xmin::text) FROM trigger_firings
		WHERE tenant_id=$1 AND deduplication_key=$2`, tenantID, "event:event-page-request").Scan(&transactions); err != nil {
		t.Fatal(err)
	}
	if transactions < 2 {
		t.Fatalf("event fanout transaction count=%d", transactions)
	}
	replayed, err := service.ReceiveEvent(ctx, tenantID, "document.ready", "event-page-request", json.RawMessage(`{"id":1}`), at.Add(time.Minute))
	if err != nil || len(replayed) != len(firings) || replayed[0].ID != firings[0].ID {
		t.Fatalf("replay=%d first=%v err=%v", len(replayed), replayed[0].ID == firings[0].ID, err)
	}
}

// 验证分页 fanout 部分提交后遇到容量背压，同一事件重试会复用前缀并最终补齐全部 trigger。
func TestFireEventResumesAfterPartialCapacityBackpressure(t *testing.T) {
	ctx, pool := triggerCapacityPool(t)
	tenantID, workflowID := seedCapacityTenant(t, ctx, pool, "event_resume")
	admission, _ := NewAdmission(pool, time.Second)
	if err := admission.ConfigureQuota(ctx, Quota{TenantID: tenantID, MaxActiveRuns: 10, MaxActiveWorkers: 2,
		MaxQueueDepth: 10, MaxTriggers: 3, MaxPendingTriggerFirings: 1, MaxPendingOutboxMessages: 1}); err != nil {
		t.Fatal(err)
	}
	store := NewPostgresTriggerStore(pool)
	service, _ := NewTriggerService(store, nil, time.Minute)
	for index := 0; index < 3; index++ {
		if _, err := service.Create(ctx, CreateTriggerRequest{TenantID: tenantID, ActorID: "operator",
			WorkflowID: workflowID, Kind: TriggerEvent, Name: fmt.Sprintf("resume-%d", index), EventType: "document.ready"}); err != nil {
			t.Fatal(err)
		}
	}
	at := databaseClock(t, ctx, pool)
	for expected := 1; expected <= 3; expected++ {
		firings, err := service.ReceiveEvent(ctx, tenantID, "document.ready", "event-resume-request", json.RawMessage(`{"id":1}`), at)
		if len(firings) != expected || (expected < 3 && !errors.Is(err, ErrRateLimited)) || (expected == 3 && err != nil) {
			t.Fatalf("attempt=%d firings=%d err=%v", expected, len(firings), err)
		}
		if expected < 3 {
			if _, err = pool.Exec(ctx, `UPDATE trigger_firings SET status='dispatched'
				WHERE tenant_id=$1 AND deduplication_key=$2 AND status='pending'`, tenantID, "event:event-resume-request"); err != nil {
				t.Fatal(err)
			}
			if _, err = pool.Exec(ctx, `UPDATE outbox_messages SET delivered_at=clock_timestamp()
				WHERE tenant_id=$1 AND payload->>'firingId'=ANY($2)`, tenantID, firingIDs(firings)); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func firingIDs(firings []Firing) []string {
	ids := make([]string, 0, len(firings))
	for _, firing := range firings {
		ids = append(ids, firing.ID)
	}
	return ids
}

// 验证 delivered outbox 与 completed firing 仅在七天幂等窗口之后按租户有界清理。
func TestCompletedTriggerHistoryRetentionPreservesIdempotencyWindow(t *testing.T) {
	ctx, pool := triggerCapacityPool(t)
	tenantID, workflowID := seedCapacityTenant(t, ctx, pool, "history")
	store := NewPostgresTriggerStore(pool)
	service, _ := NewTriggerService(store, nil, time.Minute)
	trigger, err := service.Create(ctx, CreateTriggerRequest{TenantID: tenantID, ActorID: "operator",
		WorkflowID: workflowID, Kind: TriggerAPI, Name: "api"})
	if err != nil {
		t.Fatal(err)
	}
	old, _, err := service.Fire(ctx, FireRequest{TenantID: tenantID, TriggerID: trigger.ID,
		ExpectedKind: TriggerAPI, DeduplicationKey: "old-request", Payload: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	recent, _, err := service.Fire(ctx, FireRequest{TenantID: tenantID, TriggerID: trigger.ID,
		ExpectedKind: TriggerAPI, DeduplicationKey: "recent-request", Payload: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE trigger_firings SET status='dispatched',
		created_at=CASE WHEN id=$2 THEN clock_timestamp()-interval '8 days' ELSE created_at END
		WHERE tenant_id=$1 AND id=ANY($3)`, tenantID, old.ID, []string{old.ID, recent.ID}); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE outbox_messages SET delivered_at=CASE
		WHEN payload->>'firingId'=$2 THEN clock_timestamp()-interval '8 days' ELSE clock_timestamp() END
		WHERE tenant_id=$1 AND payload->>'firingId'=ANY($3)`, tenantID, old.ID, []string{old.ID, recent.ID}); err != nil {
		t.Fatal(err)
	}
	result, err := store.PurgeCompletedTriggerHistory(ctx, tenantID, 1)
	if err != nil || result.DeliveredOutbox != 1 || result.CompletedFirings != 1 {
		t.Fatalf("purge=%+v err=%v", result, err)
	}
	var recentRows int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM trigger_firings WHERE tenant_id=$1 AND id=$2`, tenantID, recent.ID).Scan(&recentRows); err != nil || recentRows != 1 {
		t.Fatalf("recent rows=%d err=%v", recentRows, err)
	}
	if replacement, created, fireErr := service.Fire(ctx, FireRequest{TenantID: tenantID, TriggerID: trigger.ID,
		ExpectedKind: TriggerAPI, DeduplicationKey: "old-request", Payload: json.RawMessage(`{}`)}); fireErr != nil || !created || replacement.ID == old.ID {
		t.Fatalf("replacement=%+v created=%t err=%v", replacement, created, fireErr)
	}
}

func triggerCapacityPool(t *testing.T) (context.Context, *pgxpool.Pool) {
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
		t.Fatalf("migrate trigger capacity database: %v", err)
	}
	if err = postgres.NewMigrator(pool).Up(ctx, migrations); err != nil {
		t.Fatalf("migrate trigger capacity database: %v", err)
	}
	return ctx, pool
}

func seedCapacityTenant(t *testing.T, ctx context.Context, pool *pgxpool.Pool, label string) (string, string) {
	t.Helper()
	suffix := fmt.Sprintf("%s_%d", label, time.Now().UnixNano())
	tenantID, workflowID := "capacity_tenant_"+suffix, "capacity_workflow_"+suffix
	if err := seedPlatformTenant(ctx, pool, tenantID, workflowID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), `DELETE FROM tenants WHERE id=$1`, tenantID) })
	if err := seedTriggerPublication(ctx, pool, tenantID, workflowID, suffix); err != nil {
		t.Fatal(err)
	}
	return tenantID, workflowID
}

func databaseClock(t *testing.T, ctx context.Context, pool *pgxpool.Pool) time.Time {
	t.Helper()
	var now time.Time
	if err := pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		t.Fatal(err)
	}
	return now
}
