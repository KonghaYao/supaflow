package runtime

import (
	"errors"
	"fmt"
	"time"
)

// ErrorCode is a stable, transport-independent runtime error code.
type ErrorCode string

const (
	CodeInvalidArgument     ErrorCode = "invalid_argument"
	CodeNotFound            ErrorCode = "not_found"
	CodeConflict            ErrorCode = "conflict"
	CodeIdempotencyConflict ErrorCode = "idempotency_conflict"
	CodeInvalidTransition   ErrorCode = "invalid_transition"
	CodeLeaseLost           ErrorCode = "lease_lost"
	CodeStaleFencingToken   ErrorCode = "stale_fencing_token"
	CodeApprovalInvalid     ErrorCode = "approval_invalid"
	CodeApprovalExpired     ErrorCode = "approval_expired"
	CodeApprovalConsumed    ErrorCode = "approval_consumed"
	CodeExecutorFailed      ErrorCode = "executor_failed"
	CodeQuotaExceeded       ErrorCode = "quota_exceeded"
	// CodeUnavailable means the server could not complete the operation for a reason that is
	// expected to pass: the transaction was rolled back as a deadlock victim or a serialization
	// failure, the connection failed, or a commit's acknowledgement was lost. It is not a verdict
	// about the request, so it must not be reported as a conflict.
	CodeUnavailable ErrorCode = "service_unavailable"
)

// Error is a typed runtime error safe to map to an external protocol.
// Message must not contain credentials, tokens, input payloads, or SQL details.
type Error struct {
	Code    ErrorCode
	Message string
	Cause   error
	// RetryAfter is set only for retryable capacity exhaustion.
	RetryAfter time.Duration
}

// Error implements error.
func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// Unwrap exposes the diagnostic cause to trusted callers and logs.
func (e *Error) Unwrap() error { return e.Cause }

// IsCode reports whether err or a wrapped runtime Error has code.
func IsCode(err error, code ErrorCode) bool {
	var target *Error
	return errors.As(err, &target) && target.Code == code
}

type commitOutcomeUnknownError struct{ cause error }

func (e *commitOutcomeUnknownError) Error() string { return "database commit outcome is unknown" }
func (e *commitOutcomeUnknownError) Unwrap() error { return e.cause }

// MarkCommitOutcomeUnknown preserves the fact that a database commit may have
// succeeded even though its acknowledgement was lost.
func MarkCommitOutcomeUnknown(err error) error {
	if err == nil {
		return nil
	}
	return &commitOutcomeUnknownError{cause: err}
}

// IsCommitOutcomeUnknown reports whether compensating deletion would be unsafe.
func IsCommitOutcomeUnknown(err error) bool {
	var target *commitOutcomeUnknownError
	return errors.As(err, &target)
}

func errorf(code ErrorCode, format string, args ...any) error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}
