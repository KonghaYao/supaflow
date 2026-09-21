package httpapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/api"
	"github.com/pazhou/supaflow/internal/platformops"
	"github.com/pazhou/supaflow/internal/postgres"
)

// 验证短 lease 的长请求由 HTTP middleware 持续续租，第二 owner 不能在 handler 仍运行时接管。
func TestMutationMiddlewareRenewsLeaseUntilHandlerCompletes(t *testing.T) {
	ctx, pool, organizationID := mutationLeaseFixture(t)
	ledger, err := platformops.NewPostgresMutationLedgerWithLease(pool, 60*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	audit, err := platformops.NewAuditService(pool)
	if err != nil {
		t.Fatal(err)
	}
	handler := &Handler{services: Services{Mutations: ledger, Audit: audit}}
	started := make(chan struct{})
	release := make(chan struct{})
	protected := handler.idempotentMutations(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		select {
		case <-release:
			writeJSON(w, http.StatusOK, map[string]bool{"configured": true})
		case <-r.Context().Done():
			writeProblem(w, r, http.StatusConflict, "lease_lost")
		}
	}))
	auth, err := api.NewMasterKeyAuthenticator(httpTestMasterKey)
	if err != nil {
		t.Fatal(err)
	}
	responseDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		request := httptest.NewRequest(http.MethodPost, "/ops/quotas", bytes.NewReader([]byte(`{}`)))
		request.Header.Set("Authorization", "Bearer "+httpTestMasterKey)
		request.Header.Set("X-Fenix-Organization-Id", organizationID)
		request.Header.Set("Idempotency-Key", "mutation-lease-long-handler")
		response := httptest.NewRecorder()
		auth.Middleware(protected).ServeHTTP(response, request)
		responseDone <- response
	}()
	select {
	case <-started:
	case response := <-responseDone:
		t.Fatalf("request returned before handler start: status=%d body=%s", response.Code, response.Body.String())
	case <-time.After(5 * time.Second):
		t.Fatal("request did not reach handler within five seconds")
	}
	time.Sleep(130 * time.Millisecond)
	fingerprint := sha256.Sum256([]byte("POST\n/ops/quotas\n{}"))
	if _, err = ledger.Claim(ctx, organizationID, "quota.configure", "mutation-lease-long-handler", fingerprint[:]); !errors.Is(err, platformops.ErrConflict) {
		t.Fatalf("live heartbeat allowed takeover: %v", err)
	}
	close(release)
	response := <-responseDone
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	replay, err := ledger.Claim(ctx, organizationID, "quota.configure", "mutation-lease-long-handler", fingerprint[:])
	if err != nil || !replay.Replay {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
}

// 验证 heartbeat 停止后 lease 按数据库时钟到期，新的 owner 可以接管且保留 operation identity。
func TestMutationLedgerAllowsTakeoverAfterHeartbeatStops(t *testing.T) {
	ctx, pool, organizationID := mutationLeaseFixture(t)
	ledger, err := platformops.NewPostgresMutationLedgerWithLease(pool, 40*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256([]byte("stopped-heartbeat"))
	first, err := ledger.Claim(ctx, organizationID, "quota.configure", "mutation-lease-stopped", hash[:])
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(90 * time.Millisecond)
	second, err := ledger.Claim(ctx, organizationID, "quota.configure", "mutation-lease-stopped", hash[:])
	if err != nil || second.Owner == first.Owner || second.OperationID != first.OperationID {
		t.Fatalf("first=%+v second=%+v err=%v", first, second, err)
	}
}

func mutationLeaseFixture(t *testing.T) (context.Context, *pgxpool.Pool, string) {
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
	migrations, err := postgres.LoadMigrations(os.DirFS("../../.."), "migrations")
	if err != nil {
		t.Fatal(err)
	}
	if err = postgres.NewMigrator(pool).Up(ctx, migrations); err != nil {
		t.Fatal(err)
	}
	organizationID := fmt.Sprintf("mutation_lease_%d", time.Now().UnixNano())
	return ctx, pool, organizationID
}
