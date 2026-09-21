package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const testMasterKey = "test-platform-master-key-at-least-32-bytes"

func authenticatedRequest(organizationID string) *http.Request {
	request := httptest.NewRequest(http.MethodGet, "/v1/runs", nil)
	request.Header.Set("Authorization", "Bearer "+testMasterKey)
	if organizationID != "" {
		request.Header.Set(organizationHeader, organizationID)
	}
	return request
}

func testAuthenticator(t *testing.T) *Authenticator {
	t.Helper()
	authenticator, err := NewMasterKeyAuthenticator(testMasterKey)
	if err != nil {
		t.Fatal(err)
	}
	return authenticator
}

// 验证可信 Fenix master key 与兼容非 UUID 的组织 ID 会形成中立组织 Principal，且不伪造 Supaflow role。
func TestAuthenticatorInjectsFenixOrganization(t *testing.T) {
	authenticator := testAuthenticator(t)
	var received Principal
	handler := authenticator.Middleware(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		received, _ = PrincipalFromContext(request.Context())
		w.WriteHeader(http.StatusNoContent)
	}))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, authenticatedRequest("org_xxx"))
	if response.Code != http.StatusNoContent || received.OrganizationID != "org_xxx" || received.ActorID != "fenix" {
		t.Fatalf("status=%d organization=%q actor=%q", response.Code, received.OrganizationID, received.ActorID)
	}
}

// 验证缺失或错误 master key 均稳定返回 401，且旧 X-API-Key 不再 fallback。
func TestAuthenticatorRejectsMissingWrongAndLegacyKeys(t *testing.T) {
	handler := testAuthenticator(t).Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("unauthenticated request reached handler")
	}))
	for name, request := range map[string]*http.Request{
		"missing": httptest.NewRequest(http.MethodGet, "/v1/runs", nil),
		"wrong":   authenticatedRequest("org-a"),
		"legacy":  httptest.NewRequest(http.MethodGet, "/v1/runs", nil),
	} {
		if name == "wrong" {
			request.Header.Set("Authorization", "Bearer wrong-platform-master-key-at-least-32")
		}
		if name == "legacy" {
			request.Header.Set("X-API-Key", testMasterKey)
			request.Header.Set(organizationHeader, "org-a")
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusUnauthorized {
			t.Fatalf("%s status=%d", name, response.Code)
		}
	}
}

// 验证已认证请求缺失或携带非法组织 header 时稳定返回 400。
func TestAuthenticatorRejectsInvalidOrganization(t *testing.T) {
	handler := testAuthenticator(t).Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("invalid organization reached handler")
	}))
	invalid := []string{"", "org/a", `org\\a`, "org a", "org\ncontrol", strings.Repeat("a", maxOrganizationID+1)}
	for _, organizationID := range invalid {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, authenticatedRequest(organizationID))
		if response.Code != http.StatusBadRequest {
			t.Fatalf("organization=%q status=%d", organizationID, response.Code)
		}
	}
}

// 验证组织 header 里的非法 UTF-8 在鉴权层就被拒：它能通过形状校验（range 把它读成 U+FFFD），
// 却是 Postgres 存不下的字节（22021）。此前它只会在写审计时失败，变更请求被渲染成
// 500 audit_unavailable，而同一个请求的只读端点却是 400。
func TestAuthenticatorRejectsUnstorableOrganization(t *testing.T) {
	handler := testAuthenticator(t).Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("unstorable organization reached handler")
	}))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, authenticatedRequest("organization-a\xff"))
	assertProblemCode(t, response, http.StatusBadRequest, "invalid_argument")
}

// 验证 current/previous 重叠轮换期间两把 master key 均可认证。
func TestAuthenticatorAcceptsCurrentAndPreviousKeys(t *testing.T) {
	previous := "previous-platform-master-key-at-least-32-bytes"
	authenticator, err := NewMasterKeyAuthenticator(testMasterKey, previous)
	if err != nil {
		t.Fatal(err)
	}
	handler := authenticator.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	for _, key := range []string{testMasterKey, previous} {
		request := authenticatedRequest("org-a")
		request.Header.Set("Authorization", "Bearer "+key)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusNoContent {
			t.Fatalf("status=%d", response.Code)
		}
	}
}

// 验证 master key 配置至少包含 32 字节且错误不会回显 secret。
func TestAuthenticatorValidatesAndRedactsMasterKey(t *testing.T) {
	secret := "short-secret"
	_, err := NewMasterKeyAuthenticator(secret)
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("err=%v", err)
	}
}
