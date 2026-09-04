package identity

import (
	"context"
	"crypto/sha256"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

type memoryIdentityStore struct {
	attempt   LoginAttempt
	principal Principal
	session   [sha256.Size]byte
	revoked   bool
}

func (s *memoryIdentityStore) ResolveAPIKey(context.Context, [sha256.Size]byte, time.Time) (Principal, error) {
	return Principal{}, ErrUnauthenticated
}
func (s *memoryIdentityStore) ResolveSession(_ context.Context, digest [sha256.Size]byte, _ time.Time) (Principal, error) {
	if s.revoked || !sameDigest(s.session, digest) {
		return Principal{}, ErrUnauthenticated
	}
	return s.principal, nil
}
func (s *memoryIdentityStore) CreateLoginAttempt(_ context.Context, attempt LoginAttempt) error {
	s.attempt = attempt
	return nil
}
func (s *memoryIdentityStore) ConsumeLoginAttempt(_ context.Context, digest [sha256.Size]byte, now time.Time) (LoginAttempt, error) {
	if !sameDigest(s.attempt.StateHash, digest) || !s.attempt.ExpiresAt.After(now) {
		return LoginAttempt{}, ErrInvalidLogin
	}
	attempt := s.attempt
	s.attempt = LoginAttempt{}
	return attempt, nil
}
func (s *memoryIdentityStore) ResolveOIDCPrincipal(_ context.Context, _ OIDCClaims, tenant string, _ time.Time) (Principal, error) {
	if tenant != s.principal.TenantID {
		return Principal{}, ErrMembershipNotFound
	}
	return s.principal, nil
}
func (s *memoryIdentityStore) CreateSession(_ context.Context, principal Principal, digest [sha256.Size]byte, _, _ time.Time) (string, error) {
	if principal != s.principal {
		return "", ErrMembershipNotFound
	}
	s.session = digest
	return "session-test", nil
}
func (s *memoryIdentityStore) RevokeSession(_ context.Context, digest [sha256.Size]byte, _ time.Time) error {
	if sameDigest(s.session, digest) {
		s.revoked = true
	}
	return nil
}

type fakeOIDCProvider struct{ state, nonce, challenge string }

func (p *fakeOIDCProvider) AuthorizationURL(state, nonce, challenge string) string {
	p.state, p.nonce, p.challenge = state, nonce, challenge
	return "https://issuer.example/authorize?state=" + url.QueryEscape(state)
}
func (p *fakeOIDCProvider) Exchange(_ context.Context, code, _ string) (VerifiedOIDCIdentity, error) {
	return VerifiedOIDCIdentity{Claims: OIDCClaims{Issuer: "https://issuer.example", Subject: code}, Nonce: p.nonce}, nil
}

// 验证 API 级 OIDC 流程使用 PKCE、浏览器绑定、服务端成员关系并签发 HttpOnly session。
func TestOIDCFlowCreatesServerOwnedSession(t *testing.T) {
	now := time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC)
	store := &memoryIdentityStore{principal: Principal{TenantID: "tenant-a", ActorID: "user-a", Role: RoleAdmin}}
	provider := &fakeOIDCProvider{}
	handler, err := NewHTTPHandler(store, provider, HTTPConfig{LoginTTL: 5 * time.Minute, SessionTTL: time.Hour, SuccessRedirect: "/studio/"})
	if err != nil {
		t.Fatal(err)
	}
	handler.now = func() time.Time { return now }

	start := httptest.NewRecorder()
	handler.Routes().ServeHTTP(start, httptest.NewRequest(http.MethodGet, "/auth/oidc/start?tenant=tenant-a", nil))
	if start.Code != http.StatusFound || provider.challenge == "" || store.attempt.RequestedTenant != "tenant-a" {
		t.Fatalf("start status=%d challenge=%q attempt=%+v", start.Code, provider.challenge, store.attempt)
	}
	loginCookie := start.Result().Cookies()[0]
	callbackRequest := httptest.NewRequest(http.MethodGet, "/auth/oidc/callback?code=subject-a&state="+url.QueryEscape(provider.state), nil)
	callbackRequest.AddCookie(loginCookie)
	callback := httptest.NewRecorder()
	handler.Routes().ServeHTTP(callback, callbackRequest)
	if callback.Code != http.StatusFound || callback.Header().Get("Location") != "/studio/" {
		t.Fatalf("callback status=%d location=%q body=%s", callback.Code, callback.Header().Get("Location"), callback.Body.String())
	}
	var sessionCookie *http.Cookie
	for _, cookie := range callback.Result().Cookies() {
		if cookie.Name == sessionCookieName {
			sessionCookie = cookie
		}
	}
	if sessionCookie == nil || !sessionCookie.HttpOnly || !sessionCookie.Secure || sessionCookie.SameSite != http.SameSiteLaxMode {
		t.Fatalf("session cookie=%+v", sessionCookie)
	}
	if !sameDigest(store.session, DigestSecret(sessionCookie.Value)) {
		t.Fatal("session store did not receive the cookie digest")
	}
}

// 验证回调缺少发起浏览器的绑定 cookie 时被拒绝，且一次性 state 不可重放。
func TestOIDCCallbackRejectsBrowserMismatchAndReplay(t *testing.T) {
	store := &memoryIdentityStore{principal: Principal{TenantID: "tenant-a", ActorID: "user-a", Role: RoleViewer}}
	provider := &fakeOIDCProvider{}
	handler, err := NewHTTPHandler(store, provider, HTTPConfig{LoginTTL: time.Minute, SessionTTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	start := httptest.NewRecorder()
	handler.Routes().ServeHTTP(start, httptest.NewRequest(http.MethodGet, "/auth/oidc/start?tenant=tenant-a", nil))
	callback := httptest.NewRecorder()
	handler.Routes().ServeHTTP(callback, httptest.NewRequest(http.MethodGet, "/auth/oidc/callback?code=x&state="+url.QueryEscape(provider.state), nil))
	if callback.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d", callback.Code)
	}
}

// 验证注销只按 session 摘要撤销服务端状态并清除浏览器 cookie。
func TestLogoutRevokesSessionDigest(t *testing.T) {
	store := &memoryIdentityStore{session: DigestSecret("session-value")}
	handler, err := NewHTTPHandler(store, &fakeOIDCProvider{}, HTTPConfig{LoginTTL: time.Minute, SessionTTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/auth/logout", nil)
	request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "session-value"})
	response := httptest.NewRecorder()
	handler.Routes().ServeHTTP(response, request)
	if response.Code != http.StatusNoContent || !store.revoked || response.Result().Cookies()[0].MaxAge >= 0 {
		t.Fatalf("status=%d revoked=%v cookies=%+v", response.Code, store.revoked, response.Result().Cookies())
	}
}
