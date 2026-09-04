package identity

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
)

const (
	loginCookieName   = "__Host-supaflow_oidc"
	sessionCookieName = "__Host-supaflow_session"
)

// HTTPConfig controls OIDC transaction and session lifetimes.
type HTTPConfig struct {
	LoginTTL        time.Duration
	SessionTTL      time.Duration
	SuccessRedirect string
}

// HTTPHandler serves independent OIDC login, callback, and session logout routes.
type HTTPHandler struct {
	store    Store
	provider OIDCProvider
	config   HTTPConfig
	now      func() time.Time
}

// NewHTTPHandler creates fail-closed identity routes.
func NewHTTPHandler(store Store, provider OIDCProvider, config HTTPConfig) (*HTTPHandler, error) {
	if store == nil || provider == nil {
		return nil, errors.New("identity store and OIDC provider are required")
	}
	if config.LoginTTL <= 0 || config.LoginTTL > 15*time.Minute || config.SessionTTL <= 0 {
		return nil, errors.New("identity login and session TTLs are invalid")
	}
	if config.SuccessRedirect == "" {
		config.SuccessRedirect = "/studio/"
	}
	if !strings.HasPrefix(config.SuccessRedirect, "/") || strings.HasPrefix(config.SuccessRedirect, "//") {
		return nil, errors.New("identity success redirect must be a local absolute path")
	}
	return &HTTPHandler{store: store, provider: provider, config: config, now: time.Now}, nil
}

// Routes returns public login/callback routes and a credential-bound logout endpoint.
func (h *HTTPHandler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /auth/oidc/start", h.start)
	mux.HandleFunc("GET /auth/oidc/callback", h.callback)
	mux.HandleFunc("POST /auth/logout", h.logout)
	return mux
}

func (h *HTTPHandler) start(w http.ResponseWriter, r *http.Request) {
	requestedTenant := strings.TrimSpace(r.URL.Query().Get("tenant"))
	if len(requestedTenant) > 255 {
		h.problem(w, http.StatusBadRequest, "invalid tenant selector")
		return
	}
	state, err := randomSecret(32)
	if err != nil {
		h.internalError(w)
		return
	}
	browser, err := randomSecret(32)
	if err != nil {
		h.internalError(w)
		return
	}
	nonce, err := randomSecret(32)
	if err != nil {
		h.internalError(w)
		return
	}
	verifier, err := randomSecret(32)
	if err != nil {
		h.internalError(w)
		return
	}
	now := h.now().UTC()
	attempt := LoginAttempt{
		StateHash: DigestSecret(state), BrowserHash: DigestSecret(browser), NonceHash: DigestSecret(nonce),
		PKCEVerifier: verifier, RequestedTenant: requestedTenant, ExpiresAt: now.Add(h.config.LoginTTL),
	}
	if err = h.store.CreateLoginAttempt(r.Context(), attempt); err != nil {
		h.internalError(w)
		return
	}
	h.setCookie(w, loginCookieName, browser, h.config.LoginTTL)
	challenge := sha256.Sum256([]byte(verifier))
	http.Redirect(w, r, h.provider.AuthorizationURL(state, nonce, base64.RawURLEncoding.EncodeToString(challenge[:])), http.StatusFound)
}

func (h *HTTPHandler) callback(w http.ResponseWriter, r *http.Request) {
	state, code := strings.TrimSpace(r.URL.Query().Get("state")), strings.TrimSpace(r.URL.Query().Get("code"))
	cookie, cookieErr := r.Cookie(loginCookieName)
	if !validBearerPart(state, 512) || !validBearerPart(code, 4096) || cookieErr != nil || !validBearerPart(cookie.Value, 512) {
		h.clearCookie(w, loginCookieName)
		h.unauthorized(w)
		return
	}
	now := h.now().UTC()
	attempt, err := h.store.ConsumeLoginAttempt(r.Context(), DigestSecret(state), now)
	if err != nil || !sameDigest(attempt.BrowserHash, DigestSecret(cookie.Value)) {
		h.clearCookie(w, loginCookieName)
		h.unauthorized(w)
		return
	}
	verified, err := h.provider.Exchange(r.Context(), code, attempt.PKCEVerifier)
	if err != nil || !sameDigest(attempt.NonceHash, DigestSecret(verified.Nonce)) {
		h.clearCookie(w, loginCookieName)
		h.unauthorized(w)
		return
	}
	principal, err := h.store.ResolveOIDCPrincipal(r.Context(), verified.Claims, attempt.RequestedTenant, now)
	if err != nil {
		h.clearCookie(w, loginCookieName)
		h.unauthorized(w)
		return
	}
	rawSession, err := randomSecret(32)
	if err != nil {
		h.internalError(w)
		return
	}
	if _, err = h.store.CreateSession(r.Context(), principal, DigestSecret(rawSession), now.Add(h.config.SessionTTL), now); err != nil {
		h.internalError(w)
		return
	}
	h.clearCookie(w, loginCookieName)
	h.setCookie(w, sessionCookieName, rawSession, h.config.SessionTTL)
	http.Redirect(w, r, h.config.SuccessRedirect, http.StatusFound)
}

func (h *HTTPHandler) logout(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie(sessionCookieName)
	if err == nil && validBearerPart(cookie.Value, 512) {
		if err = h.store.RevokeSession(r.Context(), DigestSecret(cookie.Value), h.now().UTC()); err != nil {
			h.internalError(w)
			return
		}
	}
	h.clearCookie(w, sessionCookieName)
	w.WriteHeader(http.StatusNoContent)
}

func (h *HTTPHandler) setCookie(w http.ResponseWriter, name, value string, ttl time.Duration) {
	http.SetCookie(w, &http.Cookie{Name: name, Value: value, Path: "/", MaxAge: int(ttl.Seconds()), HttpOnly: true, Secure: true, SameSite: http.SameSiteLaxMode})
}

func (h *HTTPHandler) clearCookie(w http.ResponseWriter, name string) {
	http.SetCookie(w, &http.Cookie{Name: name, Path: "/", MaxAge: -1, HttpOnly: true, Secure: true, SameSite: http.SameSiteLaxMode})
}

func (h *HTTPHandler) unauthorized(w http.ResponseWriter) {
	h.problem(w, http.StatusUnauthorized, "authentication failed")
}
func (h *HTTPHandler) internalError(w http.ResponseWriter) {
	h.problem(w, http.StatusInternalServerError, "identity service unavailable")
}
func (h *HTTPHandler) problem(w http.ResponseWriter, status int, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"status": status, "detail": detail})
}

func randomSecret(size int) (string, error) {
	value := make([]byte, size)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}

func sameDigest(left, right [sha256.Size]byte) bool {
	return subtle.ConstantTimeCompare(left[:], right[:]) == 1
}

func validBearerPart(value string, maximum int) bool {
	return value != "" && len(value) <= maximum && !strings.ContainsAny(value, "\r\n\x00")
}
