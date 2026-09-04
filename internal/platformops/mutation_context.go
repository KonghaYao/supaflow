package platformops

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
)

type mutationOperationContextKey struct{}

// ContextWithMutationOperation attaches the durable operation identity claimed
// by the HTTP mutation ledger. Domain code uses it to derive stable resource
// identities that survive a crash between the side effect and response commit.
func ContextWithMutationOperation(ctx context.Context, operationID string) context.Context {
	if operationID == "" {
		return ctx
	}
	return context.WithValue(ctx, mutationOperationContextKey{}, operationID)
}

// MutationResourceID deterministically derives an opaque resource identity for
// one logical mutation. Direct non-HTTP callers receive ok=false and retain
// their normal random-ID behavior.
func MutationResourceID(ctx context.Context, prefix, discriminator string) (id string, ok bool) {
	operationID, ok := ctx.Value(mutationOperationContextKey{}).(string)
	if !ok || operationID == "" || prefix == "" {
		return "", false
	}
	digest := sha256.Sum256([]byte(operationID + "\x00" + prefix + "\x00" + discriminator))
	return prefix + "_" + hex.EncodeToString(digest[:16]), true
}

func mutationResourceIDOrRandom(ctx context.Context, prefix, discriminator string) (string, error) {
	if id, ok := MutationResourceID(ctx, prefix, discriminator); ok {
		return id, nil
	}
	return newID(prefix)
}
