// Package platformops implements Supaflow's durable platform and operations capabilities.
package platformops

import (
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// Stable service errors are safe for transport adapters to classify.
var (
	ErrInvalidArgument     = errors.New("invalid platform operation")
	ErrNotFound            = errors.New("platform resource not found")
	ErrConflict            = errors.New("platform resource conflict")
	ErrReplay              = errors.New("request was already consumed")
	ErrSignature           = errors.New("webhook signature is invalid")
	ErrCredentialRevoked   = errors.New("credential is revoked")
	ErrCredentialForbidden = errors.New("credential is not granted to attempt")
	ErrRateLimited         = errors.New("organization quota exceeded")
	ErrPermissionDenied    = errors.New("permission denied")
	ErrStaleLease          = errors.New("delivery lease is stale")
)

// integrityViolationCodes are the two SQLSTATEs a caller's own values raise inside a write:
// a unique key that is already taken, and a reference to a parent row that does not exist.
const (
	uniqueViolationCode     = "23505"
	foreignKeyViolationCode = "23503"
)

// integrityViolationError translates an integrity violation caused by the caller's values into
// the stable service error that describes it, and returns nil for every other failure.
//
// Both cases are the caller's to fix, so they belong on the 4xx side of the HTTP edge: a taken
// key is a conflict, a reference to a missing parent is a bad argument. Reporting them as an
// opaque storage failure told the caller to retry a request that can never succeed — which is how
// a duplicate trigger name and a trigger pointing at a non-existent workflow both surfaced as
// 500 internal_error. Neither branch carries the constraint, table, or column name: the caller
// learns what to correct from the endpoint it called, not from the server's schema.
//
// The translation stays at the statement that raised it: a 23505 can only mean "this unique key
// is taken" for the row being written, and a 23503 can only mean "one of the values in this
// statement names a row that is not there". Callers must therefore only apply it to statements
// whose constraints are satisfied by request-supplied values.
func integrityViolationError(err error) error {
	var databaseError *pgconn.PgError
	if !errors.As(err, &databaseError) {
		return nil
	}
	switch databaseError.Code {
	case uniqueViolationCode:
		return ErrConflict
	case foreignKeyViolationCode:
		return ErrInvalidArgument
	}
	return nil
}

// RateLimitError carries a safe retry hint for HTTP Retry-After.
type RateLimitError struct {
	RetryAfter time.Duration
	Reason     string
}

// Error returns a stable rate-limit description without internal quota details.
func (e *RateLimitError) Error() string { return ErrRateLimited.Error() + ": " + e.Reason }

// Unwrap supports errors.Is(err, ErrRateLimited).
func (e *RateLimitError) Unwrap() error { return ErrRateLimited }
