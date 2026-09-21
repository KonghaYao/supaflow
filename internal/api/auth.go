package api

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"
	"unicode"

	"github.com/pazhou/supaflow/internal/organization"
	"github.com/pazhou/supaflow/internal/platformops"
)

const (
	organizationHeader = "X-Fenix-Organization-Id"
	maxOrganizationID  = 128
)

// Principal is the authenticated organization context injected into requests.
type Principal = organization.Principal

// Authenticator validates platform master keys and injects Fenix organization scope.
type Authenticator struct {
	masterKeyDigests  [][sha256.Size]byte
	requestMiddleware func(http.Handler) http.Handler
}

// NewMasterKeyAuthenticator stores only digests of current and optional previous keys.
func NewMasterKeyAuthenticator(keys ...string) (*Authenticator, error) {
	digests := make([][sha256.Size]byte, 0, len(keys))
	for _, key := range keys {
		if key == "" {
			continue
		}
		if len([]byte(key)) < 32 {
			return nil, errInvalidAuthConfig
		}
		digests = append(digests, sha256.Sum256([]byte(key)))
	}
	if len(digests) == 0 {
		return nil, errInvalidAuthConfig
	}
	return &Authenticator{masterKeyDigests: digests}, nil
}

// Middleware validates Bearer master key before validating the organization header.
func (a *Authenticator) Middleware(next http.Handler) http.Handler {
	protected := next
	if a.requestMiddleware != nil {
		protected = a.requestMiddleware(protected)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, ok := bearerToken(r.Header.Get("Authorization"))
		if !ok || !a.matches(key) {
			writeProblem(w, r, http.StatusUnauthorized, "unauthorized", "authentication required")
			return
		}
		organizationID := r.Header.Get(organizationHeader)
		if !validOrganizationID(organizationID) {
			writeProblem(w, r, http.StatusBadRequest, "invalid_organization", organizationHeader+" is required and must be valid")
			return
		}
		// 组织标识会写进每一条审计记录，也是所有查询的过滤条件。非法 UTF-8 能通过上面的形状
		// 校验（range 把它们读成 U+FFFD），Postgres 则直接拒收（22021）：变更请求因此在审计
		// 分支被渲染成 500 audit_unavailable，只读端点走 writeError 却是 400。同一个根因两种
		// 结局，所以在边缘统一挡住。/v1 与 /ops 共用这一层，两边一起覆盖。
		if !platformops.StorableText(organizationID) {
			writeProblem(w, r, http.StatusBadRequest, "invalid_argument", organizationHeader+" must be valid UTF-8 without NUL bytes")
			return
		}
		principal := Principal{OrganizationID: organizationID, ActorID: "fenix"}
		protected.ServeHTTP(w, r.WithContext(organization.ContextWithPrincipal(r.Context(), principal)))
	})
}

func bearerToken(value string) (string, bool) {
	const prefix = "Bearer "
	if !strings.HasPrefix(value, prefix) {
		return "", false
	}
	key := strings.TrimSpace(strings.TrimPrefix(value, prefix))
	return key, key != ""
}

func (a *Authenticator) matches(key string) bool {
	candidate := sha256.Sum256([]byte(key))
	matched := 0
	for _, expected := range a.masterKeyDigests {
		matched |= subtle.ConstantTimeCompare(candidate[:], expected[:])
	}
	return matched == 1
}

func validOrganizationID(value string) bool {
	if value == "" || len([]byte(value)) > maxOrganizationID || strings.ContainsAny(value, "/\\") {
		return false
	}
	for _, character := range value {
		if unicode.IsSpace(character) || unicode.IsControl(character) {
			return false
		}
	}
	return true
}

// SetRequestMiddleware configures organization-aware middleware around authenticated requests.
func (a *Authenticator) SetRequestMiddleware(middleware func(http.Handler) http.Handler) {
	a.requestMiddleware = middleware
}

// PrincipalFromContext returns the authenticated principal.
func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	return organization.PrincipalFromContext(ctx)
}
