package identity_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/api"
	"github.com/pazhou/supaflow/internal/identity"
	pgstore "github.com/pazhou/supaflow/internal/postgres"
)

// 验证真实 PostgreSQL + HTTP 中 DB API key/session 的租户隔离、过期和撤销契约。
func TestPostgresIdentityAtHTTPBoundary(t *testing.T) {
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
	migrations, err := pgstore.LoadMigrations(os.DirFS("../.."), "migrations")
	if err != nil {
		t.Fatal(err)
	}
	if err = pgstore.NewMigrator(pool).Up(ctx, migrations); err != nil {
		t.Fatal(err)
	}
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	tenantA, tenantB, serviceID, userID := "identity_a_"+suffix, "identity_b_"+suffix, "svc_"+suffix, "usr_"+suffix
	for _, tenant := range []string{tenantA, tenantB} {
		if _, err = pool.Exec(ctx, `INSERT INTO tenants (id,name) VALUES ($1,$1)`, tenant); err != nil {
			t.Fatal(err)
		}
	}
	store, err := identity.NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	rawKey := "database-integration-api-key-" + suffix
	if err = store.BootstrapAPIKey(ctx, identity.BootstrapAPIKey{Value: rawKey, TenantID: tenantA, ActorID: serviceID, Name: "integration", Role: identity.RoleViewer}); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO oidc_identities (id,issuer,subject) VALUES ($1,'https://issuer.example',$1)`, userID); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO tenant_memberships (tenant_id,actor_id,actor_type,role) VALUES ($1,$2,'user','admin')`, tenantA, userID); err != nil {
		t.Fatal(err)
	}
	rawSession := "database-integration-session-" + suffix
	if _, err = store.CreateSession(ctx, identity.Principal{TenantID: tenantA, ActorID: userID, Role: identity.RoleAdmin}, identity.DigestSecret(rawSession), time.Now().Add(time.Hour), time.Now()); err != nil {
		t.Fatal(err)
	}
	authenticator, err := api.NewDatabaseAuthenticator(store)
	if err != nil {
		t.Fatal(err)
	}
	var principal api.Principal
	handler := authenticator.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, _ = api.PrincipalFromContext(r.Context())
		w.WriteHeader(http.StatusNoContent)
	}))
	request := httptest.NewRequest(http.MethodGet, "/v1/runs", nil)
	request.Header.Set("X-API-Key", rawKey)
	request.Header.Set("X-Tenant-ID", tenantB)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent || principal.TenantID != tenantA || principal.Role != api.RoleViewer {
		t.Fatalf("status=%d principal=%+v", response.Code, principal)
	}
	if _, err = pool.Exec(ctx, `UPDATE api_keys SET revoked_at=now() WHERE tenant_id=$1`, tenantA); err != nil {
		t.Fatal(err)
	}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("revoked key status=%d", response.Code)
	}
	sessionRequest := httptest.NewRequest(http.MethodGet, "/v1/runs", nil)
	sessionRequest.AddCookie(&http.Cookie{Name: api.SessionCookieName, Value: rawSession})
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, sessionRequest)
	if response.Code != http.StatusNoContent || principal.ActorID != userID {
		t.Fatalf("session status=%d principal=%+v", response.Code, principal)
	}
	if _, err = pool.Exec(ctx, `UPDATE identity_sessions SET expires_at=now()-interval '1 second' WHERE tenant_id=$1`, tenantA); err != nil {
		t.Fatal(err)
	}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, sessionRequest)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("expired session status=%d", response.Code)
	}
}
