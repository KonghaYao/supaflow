package platformops

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sync/atomic"

	"github.com/jackc/pgx/v5"
)

type mutationOperationContextKey struct{}

// MutationOperation is the authenticated identity shared by the HTTP ledger
// and the PostgreSQL transaction that applies one operations mutation.
type MutationOperation struct {
	ID             string
	OrganizationID string
	ActorID        string
	Action         string
	ResourceType   string
	ResourceID     string
}

type mutationContext struct {
	operation MutationOperation
	committed atomic.Bool
}

// ContextWithMutation attaches the complete authenticated mutation identity.
// Callers must construct it only after authorization and durable ledger claim.
func ContextWithMutation(ctx context.Context, operation MutationOperation) context.Context {
	if operation.ID == "" {
		return ctx
	}
	return context.WithValue(ctx, mutationOperationContextKey{}, &mutationContext{operation: operation})
}

// ContextWithMutationOperation attaches the durable operation identity claimed
// by the HTTP mutation ledger. Domain code uses it to derive stable resource
// identities that survive a crash between the side effect and response commit.
func ContextWithMutationOperation(ctx context.Context, operationID string) context.Context {
	if operationID == "" {
		return ctx
	}
	return ContextWithMutation(ctx, MutationOperation{ID: operationID})
}

// MutationResourceID deterministically derives an opaque resource identity for
// one logical mutation. Direct non-HTTP callers receive ok=false and retain
// their normal random-ID behavior.
func MutationResourceID(ctx context.Context, prefix, discriminator string) (id string, ok bool) {
	operation, ok := mutationOperationFromContext(ctx)
	if !ok || prefix == "" {
		return "", false
	}
	digest := sha256.Sum256([]byte(operation.operation.ID + "\x00" + prefix + "\x00" + discriminator))
	return prefix + "_" + hex.EncodeToString(digest[:16]), true
}

// MutationCommitted reports whether the business state and its allowed audit
// committed atomically for the current operation.
func MutationCommitted(ctx context.Context) bool {
	operation, ok := mutationOperationFromContext(ctx)
	return ok && operation.committed.Load()
}

func mutationOperationFromContext(ctx context.Context) (*mutationContext, bool) {
	operation, ok := ctx.Value(mutationOperationContextKey{}).(*mutationContext)
	return operation, ok && operation != nil && operation.operation.ID != ""
}

func recordAllowedMutationTx(ctx context.Context, tx pgx.Tx, resourceID string) (bool, error) {
	operation, ok := mutationOperationFromContext(ctx)
	if !ok {
		return false, nil
	}
	identity := operation.operation
	if identity.OrganizationID == "" || identity.ActorID == "" || identity.Action == "" || identity.ResourceType == "" {
		return false, nil
	}
	if resourceID == "" {
		resourceID = identity.ResourceID
	}
	metadata, err := json.Marshal(map[string]string{"operationIdHash": AuditIdentifier(identity.ID)})
	if err != nil {
		return false, err
	}
	auditID, _ := MutationResourceID(ctx, "audit", "allowed")
	err = insertAuditOnceTx(ctx, tx, AuditRecord{ID: auditID, OrganizationID: identity.OrganizationID,
		ActorID: identity.ActorID, Action: identity.Action, ResourceType: identity.ResourceType,
		ResourceID: resourceID, Outcome: AuditAllowed, Metadata: metadata})
	return true, err
}

func markMutationCommitted(ctx context.Context) {
	if operation, ok := mutationOperationFromContext(ctx); ok {
		operation.committed.Store(true)
	}
}

func mutationResourceIDOrRandom(ctx context.Context, prefix, discriminator string) (string, error) {
	if id, ok := MutationResourceID(ctx, prefix, discriminator); ok {
		return id, nil
	}
	return newID(prefix)
}
