package identity

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// 验证 OIDC discovery、Authorization Code + PKCE 交换及签名/issuer/audience/nonce 校验链路。
func TestDiscoveryOIDCProviderExchangesAndVerifiesIDToken(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	const issuer = "https://issuer.example"
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		status := http.StatusOK
		var payload any
		switch request.URL.Path {
		case "/.well-known/openid-configuration":
			payload = map[string]string{"issuer": issuer, "authorization_endpoint": issuer + "/authorize", "token_endpoint": issuer + "/token", "jwks_uri": issuer + "/keys"}
		case "/token":
			if parseErr := request.ParseForm(); parseErr != nil || request.Form.Get("code_verifier") != "pkce-verifier" || request.Form.Get("code") != "authorization-code" {
				status, payload = http.StatusBadRequest, map[string]string{"error": "invalid_request"}
				break
			}
			claims := jwt.MapClaims{"iss": issuer, "sub": "subject-a", "aud": "client-a", "nonce": "nonce-a", "exp": time.Now().Add(time.Minute).Unix(), "iat": time.Now().Unix()}
			token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
			token.Header["kid"] = "key-a"
			raw, signErr := token.SignedString(privateKey)
			if signErr != nil {
				return nil, signErr
			}
			payload = map[string]string{"id_token": raw}
		case "/keys":
			exponent := big.NewInt(int64(privateKey.PublicKey.E)).Bytes()
			payload = map[string]any{"keys": []map[string]string{{"kid": "key-a", "kty": "RSA", "n": base64.RawURLEncoding.EncodeToString(privateKey.PublicKey.N.Bytes()), "e": base64.RawURLEncoding.EncodeToString(exponent)}}}
		default:
			status, payload = http.StatusNotFound, map[string]string{"error": "not_found"}
		}
		encoded, marshalErr := json.Marshal(payload)
		if marshalErr != nil {
			return nil, marshalErr
		}
		return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(string(encoded)))}, nil
	})}
	provider, err := DiscoverOIDCProvider(context.Background(), OIDCConfig{IssuerURL: issuer, ClientID: "client-a", ClientSecret: "secret-a", RedirectURL: "https://supaflow.example/auth/oidc/callback", HTTPClient: client})
	if err != nil {
		t.Fatal(err)
	}
	authorizationURL, err := url.Parse(provider.AuthorizationURL("state-a", "nonce-a", "challenge-a"))
	if err != nil {
		t.Fatal(err)
	}
	if authorizationURL.Query().Get("code_challenge_method") != "S256" || authorizationURL.Query().Get("nonce") != "nonce-a" {
		t.Fatalf("authorization URL=%s", authorizationURL)
	}
	verified, err := provider.Exchange(context.Background(), "authorization-code", "pkce-verifier")
	if err != nil || verified.Claims.Subject != "subject-a" || verified.Nonce != "nonce-a" {
		t.Fatalf("identity=%+v err=%v", verified, err)
	}
}

// 验证 OIDC 配置缺失或使用明文端点时启动直接失败而非降级。
func TestDiscoverOIDCProviderFailsClosedForUnsafeConfiguration(t *testing.T) {
	t.Parallel()
	for _, config := range []OIDCConfig{{}, {IssuerURL: "http://issuer.example", ClientID: "client", RedirectURL: "https://app.example/callback"}, {IssuerURL: "https://issuer.example", ClientID: "client", RedirectURL: "http://app.example/callback"}} {
		if _, err := DiscoverOIDCProvider(context.Background(), config); err == nil {
			t.Fatalf("config=%+v unexpectedly accepted", config)
		}
	}
}

// 验证 discovery、token 和 JWKS 均不能跟随重定向逃逸已校验的 HTTPS 端点。
func TestDiscoverOIDCProviderRejectsRedirects(t *testing.T) {
	const issuer = "https://issuer.example"
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		body := `{"issuer":"https://issuer.example","authorization_endpoint":"https://issuer.example/authorize","token_endpoint":"https://issuer.example/token","jwks_uri":"https://issuer.example/keys"}`
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
	})}
	provider, err := DiscoverOIDCProvider(context.Background(), OIDCConfig{
		IssuerURL: issuer, ClientID: "client", RedirectURL: "https://app.example/callback", HTTPClient: client,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = provider.config.HTTPClient.CheckRedirect(&http.Request{}, nil); err == nil || !strings.Contains(err.Error(), "redirects are disabled") {
		t.Fatalf("OIDC redirect policy was not installed: %v", err)
	}
}

// 验证 provider token endpoint 错误不会把响应正文或 client secret 泄露给上层。
func TestOIDCExchangeReturnsSanitizedError(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		body := `{"error":"invalid_client","client_secret":"must-not-leak"}`
		return &http.Response{StatusCode: http.StatusUnauthorized, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
	})}
	provider := &DiscoveryOIDCProvider{config: OIDCConfig{ClientID: "client", ClientSecret: "must-not-leak", RedirectURL: "https://app.example/callback", HTTPClient: client}, discovery: oidcDiscovery{Issuer: "https://issuer.example", TokenEndpoint: "https://issuer.example/token"}}
	_, err := provider.Exchange(context.Background(), "code", "verifier")
	if err == nil || strings.Contains(err.Error(), "must-not-leak") {
		t.Fatalf("error=%v", err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }
