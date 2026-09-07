// Package apimutation carries authenticated mutation identity from HTTP into
// PostgreSQL transactions without coupling domain ports to transport models.
package apimutation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sync/atomic"
)

// Operation is the immutable identity and audit subject of one API mutation.
type Operation struct {
	ID             string
	OrganizationID string
	ActorID        string
	Action         string
	ResourceType   string
	ResourceID     string
	IdempotencyKey string
	RequestHash    [sha256.Size]byte
	RequestIDHash  string
	committed      atomic.Bool
}

type contextKey struct{}

// New creates a stable operation identity for a organization/action/idempotency tuple.
func New(organizationID, actorID, action, resourceType, resourceID, key, method, path string, body []byte, requestIDHash string) *Operation {
	fingerprint := sha256.New()
	_, _ = fingerprint.Write([]byte(method))
	_, _ = fingerprint.Write([]byte{0})
	_, _ = fingerprint.Write([]byte(path))
	_, _ = fingerprint.Write([]byte{0})
	_, _ = fingerprint.Write(body)
	var requestHash [sha256.Size]byte
	copy(requestHash[:], fingerprint.Sum(nil))
	identity := sha256.Sum256([]byte(organizationID + "\x00" + action + "\x00" + key))
	return &Operation{ID: "apiop_" + hex.EncodeToString(identity[:]), OrganizationID: organizationID, ActorID: actorID,
		Action: action, ResourceType: resourceType, ResourceID: resourceID, IdempotencyKey: key,
		RequestHash: requestHash, RequestIDHash: requestIDHash}
}

// WithContext attaches an operation to downstream domain calls.
func WithContext(ctx context.Context, operation *Operation) context.Context {
	if operation == nil {
		return ctx
	}
	return context.WithValue(ctx, contextKey{}, operation)
}

// FromContext returns the current authenticated mutation, when any.
func FromContext(ctx context.Context) (*Operation, bool) {
	operation, ok := ctx.Value(contextKey{}).(*Operation)
	return operation, ok && operation != nil
}

// MarkCommitted signals that the transaction containing the durable fact committed.
func MarkCommitted(ctx context.Context) {
	if operation, ok := FromContext(ctx); ok {
		operation.committed.Store(true)
	}
}

// Committed reports whether the durable domain/audit transaction committed.
func (o *Operation) Committed() bool { return o != nil && o.committed.Load() }
