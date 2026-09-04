// Package identity owns authenticated actors, tenant memberships, and login sessions.
package identity

import (
	"context"
	"crypto/sha256"
	"errors"
	"time"
)

// Role is a tenant-scoped platform permission loaded from server-side membership state.
type Role string

const (
	RoleViewer    Role = "viewer"
	RoleEditor    Role = "editor"
	RolePublisher Role = "publisher"
	RoleOperator  Role = "operator"
	RoleApprover  Role = "approver"
	RoleAdmin     Role = "admin"
)

var (
	// ErrUnauthenticated means a credential did not resolve to an active principal.
	ErrUnauthenticated = errors.New("identity: unauthenticated")
	// ErrInvalidLogin means an OIDC login transaction is invalid, expired, or already consumed.
	ErrInvalidLogin = errors.New("identity: invalid login transaction")
	// ErrMembershipNotFound means an OIDC subject has no active membership in the requested tenant.
	ErrMembershipNotFound = errors.New("identity: membership not found")
)

// Principal is the authenticated tenant and actor resolved from server-side state.
type Principal struct {
	TenantID string
	ActorID  string
	Role     Role
}

// LoginAttempt is the one-time server-side state for an OIDC authorization flow.
type LoginAttempt struct {
	StateHash       [sha256.Size]byte
	BrowserHash     [sha256.Size]byte
	NonceHash       [sha256.Size]byte
	PKCEVerifier    string
	RequestedTenant string
	ExpiresAt       time.Time
}

// OIDCClaims is the verified identity projection accepted from an OIDC provider.
type OIDCClaims struct {
	Issuer      string
	Subject     string
	Email       string
	DisplayName string
}

// BootstrapAPIKey is an explicit first-install service-account credential.
// Value is hashed before persistence and must never be logged.
type BootstrapAPIKey struct {
	Value    string
	TenantID string
	ActorID  string
	Name     string
	Role     Role
}

// Resolver resolves credential digests against durable server-side identity state.
type Resolver interface {
	ResolveAPIKey(context.Context, [sha256.Size]byte, time.Time) (Principal, error)
	ResolveSession(context.Context, [sha256.Size]byte, time.Time) (Principal, error)
}

// Store persists login transactions, OIDC identities, memberships, and sessions.
type Store interface {
	Resolver
	CreateLoginAttempt(context.Context, LoginAttempt) error
	ConsumeLoginAttempt(context.Context, [sha256.Size]byte, time.Time) (LoginAttempt, error)
	ResolveOIDCPrincipal(context.Context, OIDCClaims, string, time.Time) (Principal, error)
	CreateSession(context.Context, Principal, [sha256.Size]byte, time.Time, time.Time) (string, error)
	RevokeSession(context.Context, [sha256.Size]byte, time.Time) error
}

// DigestSecret converts a bearer credential to the only representation persisted by identity stores.
func DigestSecret(value string) [sha256.Size]byte { return sha256.Sum256([]byte(value)) }

// ValidRole reports whether role belongs to the stable platform RBAC vocabulary.
func ValidRole(role Role) bool {
	switch role {
	case RoleViewer, RoleEditor, RolePublisher, RoleOperator, RoleApprover, RoleAdmin:
		return true
	default:
		return false
	}
}
