// Package organization defines the authenticated organization scope shared by transport adapters.
package organization

import "context"

type principalContextKey struct{}

// Principal is the opaque Fenix organization and server actor authenticated at the service boundary.
type Principal struct {
	OrganizationID string
	ActorID        string
}

// ContextWithPrincipal attaches one server-authenticated organization principal to a context.
func ContextWithPrincipal(ctx context.Context, principal Principal) context.Context {
	return context.WithValue(ctx, principalContextKey{}, principal)
}

// PrincipalFromContext returns the server-authenticated organization principal, when present.
func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	principal, ok := ctx.Value(principalContextKey{}).(Principal)
	return principal, ok
}
