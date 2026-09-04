package api

import (
	"context"
	"crypto/sha256"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pazhou/supaflow/internal/identity"
)

type resolverCredential struct {
	digest    [sha256.Size]byte
	principal Principal
	expiresAt time.Time
	revoked   bool
}

type testIdentityResolver struct{ apiKey, session resolverCredential }

func (r *testIdentityResolver) ResolveAPIKey(_ context.Context, digest [sha256.Size]byte, now time.Time) (identity.Principal, error) {
	return resolveCredential(r.apiKey, digest, now)
}
func (r *testIdentityResolver) ResolveSession(_ context.Context, digest [sha256.Size]byte, now time.Time) (identity.Principal, error) {
	return resolveCredential(r.session, digest, now)
}
func resolveCredential(credential resolverCredential, digest [sha256.Size]byte, now time.Time) (Principal, error) {
	if credential.revoked || credential.digest != digest || !credential.expiresAt.After(now) {
		return Principal{}, identity.ErrUnauthenticated
	}
	return credential.principal, nil
}

// 验证认证上下文只使用服务端 API key 映射的租户，忽略调用方伪造的租户头。
func TestAuthenticatorInjectsServerOwnedTenant(t *testing.T) {
	auth, err := NewAuthenticator([]APIKey{{
		Value:     "test-key-at-least-24-bytes-long",
		Principal: Principal{TenantID: "tenant-a", ActorID: "service-a", Role: RoleOperator},
	}})
	if err != nil {
		t.Fatal(err)
	}

	var received Principal
	handler := auth.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received, _ = PrincipalFromContext(r.Context())
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodGet, "/v1/runs", nil)
	req.Header.Set("X-API-Key", "test-key-at-least-24-bytes-long")
	req.Header.Set("X-Tenant-ID", "tenant-b")
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)

	if res.Code != http.StatusNoContent || received.TenantID != "tenant-a" {
		t.Fatalf("status=%d tenant=%q", res.Code, received.TenantID)
	}
}

// 验证 identity-aware middleware 在成功认证后读取 principal，供 tenant 级观测使用。
func TestAuthenticatorRunsRequestMiddlewareWithResolvedPrincipal(t *testing.T) {
	auth, err := NewAuthenticator([]APIKey{{
		Value:     "test-key-at-least-24-bytes-long",
		Principal: Principal{TenantID: "tenant-a", ActorID: "service-a", Role: RoleOperator},
	}})
	if err != nil {
		t.Fatal(err)
	}
	observedTenant := ""
	auth.SetRequestMiddleware(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			principal, _ := PrincipalFromContext(r.Context())
			observedTenant = principal.TenantID
			next.ServeHTTP(w, r)
		})
	})
	handler := auth.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	request := httptest.NewRequest(http.MethodGet, "/v1/runs", nil)
	request.Header.Set("X-API-Key", "test-key-at-least-24-bytes-long")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent || observedTenant != "tenant-a" {
		t.Fatalf("status=%d tenant=%q", response.Code, observedTenant)
	}
}

// 验证未知凭据被拒绝且不会进入业务处理器。
func TestAuthenticatorRejectsUnknownKey(t *testing.T) {
	auth, err := NewAuthenticator([]APIKey{{
		Value:     "test-key-at-least-24-bytes-long",
		Principal: Principal{TenantID: "tenant-a", ActorID: "service-a", Role: RoleOperator},
	}})
	if err != nil {
		t.Fatal(err)
	}

	called := false
	handler := auth.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
	req := httptest.NewRequest(http.MethodGet, "/v1/runs", nil)
	req.Header.Set("X-API-Key", "wrong-key-at-least-24-bytes")
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)

	if res.Code != http.StatusUnauthorized || called {
		t.Fatalf("status=%d called=%v", res.Code, called)
	}
}

// 验证数据库 API key 解析只注入服务端角色和租户，调用方头不能覆盖身份。
func TestDatabaseAuthenticatorUsesResolvedAPIKeyPrincipal(t *testing.T) {
	now := time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC)
	resolver := &testIdentityResolver{apiKey: resolverCredential{
		digest:    identity.DigestSecret("database-api-key-at-least-24-bytes"),
		principal: Principal{TenantID: "tenant-a", ActorID: "service-a", Role: RoleViewer}, expiresAt: now.Add(time.Hour),
	}}
	auth, err := NewDatabaseAuthenticator(resolver)
	if err != nil {
		t.Fatal(err)
	}
	auth.now = func() time.Time { return now }
	var received Principal
	handler := auth.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received, _ = PrincipalFromContext(r.Context())
		w.WriteHeader(http.StatusNoContent)
	}))
	request := httptest.NewRequest(http.MethodGet, "/v1/runs", nil)
	request.Header.Set("X-API-Key", "database-api-key-at-least-24-bytes")
	request.Header.Set("X-Tenant-ID", "tenant-b")
	request.Header.Set("X-Role", "admin")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent || received.TenantID != "tenant-a" || received.Role != RoleViewer {
		t.Fatalf("status=%d principal=%+v", response.Code, received)
	}
}

// 验证 HttpOnly session 可认证，而过期或撤销的 session 在 API 边界统一拒绝。
func TestDatabaseAuthenticatorRejectsExpiredAndRevokedSessions(t *testing.T) {
	now := time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC)
	credential := resolverCredential{digest: identity.DigestSecret("opaque-session"), principal: Principal{TenantID: "tenant-a", ActorID: "user-a", Role: RoleAdmin}, expiresAt: now.Add(time.Minute)}
	resolver := &testIdentityResolver{session: credential}
	auth, err := NewDatabaseAuthenticator(resolver)
	if err != nil {
		t.Fatal(err)
	}
	auth.now = func() time.Time { return now }
	handler := auth.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	request := httptest.NewRequest(http.MethodGet, "/v1/runs", nil)
	request.AddCookie(&http.Cookie{Name: SessionCookieName, Value: "opaque-session", HttpOnly: true})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("live session status=%d", response.Code)
	}
	resolver.session.revoked = true
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("revoked session status=%d", response.Code)
	}
	resolver.session.revoked = false
	auth.now = func() time.Time { return now.Add(2 * time.Minute) }
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("expired session status=%d", response.Code)
	}
}
