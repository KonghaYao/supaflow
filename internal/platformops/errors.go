// Package platformops implements Supaflow's durable platform and operations capabilities.
package platformops

import (
	"errors"
	"time"
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

// RateLimitError carries a safe retry hint for HTTP Retry-After.
type RateLimitError struct {
	RetryAfter time.Duration
	Reason     string
}

// Error returns a stable rate-limit description without internal quota details.
func (e *RateLimitError) Error() string { return ErrRateLimited.Error() + ": " + e.Reason }

// Unwrap supports errors.Is(err, ErrRateLimited).
func (e *RateLimitError) Unwrap() error { return ErrRateLimited }
