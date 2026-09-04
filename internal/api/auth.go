package api

import (
	"context"
	"crypto/sha256"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/pazhou/supaflow/internal/identity"
)

// Role is a tenant-scoped platform permission loaded from identity storage.
type Role = identity.Role

const (
	RoleViewer    = identity.RoleViewer
	RoleEditor    = identity.RoleEditor
	RolePublisher = identity.RolePublisher
	RoleOperator  = identity.RoleOperator
	RoleApprover  = identity.RoleApprover
	RoleAdmin     = identity.RoleAdmin
)

// Principal is the authenticated tenant and actor injected into request context.
type Principal = identity.Principal

// APIKey registers an opaque key without retaining it after construction.
type APIKey struct {
	Value     string
	Principal Principal
}

// Authenticator resolves opaque API keys to tenant principals.
type Authenticator struct {
	byDigest          map[[sha256.Size]byte]Principal
	resolver          identity.Resolver
	now               func() time.Time
	requestMiddleware func(http.Handler) http.Handler
}

// NewAuthenticator hashes configured keys so raw credentials are not retained.
func NewAuthenticator(keys []APIKey) (*Authenticator, error) {
	a := &Authenticator{byDigest: make(map[[sha256.Size]byte]Principal, len(keys)), now: time.Now}
	for _, key := range keys {
		if len(key.Value) < 24 || key.Principal.TenantID == "" || key.Principal.ActorID == "" {
			return nil, errInvalidAuthConfig
		}
		digest := identity.DigestSecret(key.Value)
		if _, exists := a.byDigest[digest]; exists {
			return nil, errInvalidAuthConfig
		}
		a.byDigest[digest] = key.Principal
	}
	return a, nil
}

// NewDatabaseAuthenticator creates the production authenticator backed by durable identity state.
func NewDatabaseAuthenticator(resolver identity.Resolver) (*Authenticator, error) {
	if resolver == nil {
		return nil, errInvalidAuthConfig
	}
	return &Authenticator{resolver: resolver, now: time.Now}, nil
}

// Middleware authenticates an API key and injects its server-owned tenant.
func (a *Authenticator) Middleware(next http.Handler) http.Handler {
	protected := next
	unauthorized := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeProblem(w, r, http.StatusUnauthorized, "unauthorized", "authentication required")
	})
	if a.requestMiddleware != nil {
		protected = a.requestMiddleware(protected)
		unauthorized = a.requestMiddleware(unauthorized).ServeHTTP
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		value := strings.TrimSpace(r.Header.Get("X-API-Key"))
		if value == "" {
			const bearer = "Bearer "
			authorization := r.Header.Get("Authorization")
			if strings.HasPrefix(authorization, bearer) {
				value = strings.TrimSpace(strings.TrimPrefix(authorization, bearer))
			}
		}
		principal, err := a.authenticate(r.Context(), value, r)
		if err != nil {
			unauthorized.ServeHTTP(w, r)
			return
		}
		ctx := identity.ContextWithPrincipal(r.Context(), principal)
		protected.ServeHTTP(w, r.WithContext(ctx))
	})
}

// SetRequestMiddleware configures identity-aware middleware around authentication outcomes.
// Successful requests expose Principal in context; rejected requests remain anonymous.
// Configure it before constructing route handlers or serving requests.
func (a *Authenticator) SetRequestMiddleware(middleware func(http.Handler) http.Handler) {
	a.requestMiddleware = middleware
}

func (a *Authenticator) authenticate(ctx context.Context, apiKey string, r *http.Request) (Principal, error) {
	if apiKey != "" {
		digest := identity.DigestSecret(apiKey)
		if a.resolver != nil {
			return a.resolver.ResolveAPIKey(ctx, digest, a.now().UTC())
		}
		principal, ok := a.byDigest[digest]
		if !ok {
			return Principal{}, identity.ErrUnauthenticated
		}
		return principal, nil
	}
	if a.resolver == nil {
		return Principal{}, identity.ErrUnauthenticated
	}
	cookie, err := r.Cookie(SessionCookieName)
	if err != nil || strings.TrimSpace(cookie.Value) == "" {
		return Principal{}, identity.ErrUnauthenticated
	}
	principal, err := a.resolver.ResolveSession(ctx, identity.DigestSecret(cookie.Value), a.now().UTC())
	if errors.Is(err, identity.ErrUnauthenticated) {
		return Principal{}, identity.ErrUnauthenticated
	}
	return principal, err
}

// PrincipalFromContext returns the authenticated principal.
func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	return identity.PrincipalFromContext(ctx)
}

// SessionCookieName is the private cookie carrying an opaque database session credential.
const SessionCookieName = "__Host-supaflow_session"
