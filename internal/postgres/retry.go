package postgres

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
)

// SQLSTATEs whose meaning is decided by Postgres rather than by the request. Everything here is
// matched by code alone: a message is free text that changes with the server version and locale.
const (
	// deadlockDetectedCode is 40P01: the server broke a lock cycle by rolling one transaction back.
	deadlockDetectedCode = "40P01"
	// serializationFailureCode is 40001: a concurrent transaction invalidated this snapshot.
	serializationFailureCode = "40001"
	// connectionExceptionClass is 08: the connection failed. The transaction never committed.
	connectionExceptionClass = "08"
	// cannotConnectNowCode is 57P03: the server is starting up or shutting down and refuses work.
	cannotConnectNowCode = "57P03"
	// capacityExceededCode is 53400: an organization capacity guard refused another pending row.
	capacityExceededCode = "53400"
)

const (
	// transientRetryAttempts bounds how many times one transaction is attempted. Two retries are
	// enough to survive a lock cycle or a lost serialization race, which is what a deadlock storm
	// actually produces; more attempts would hold an HTTP request open while the storm continues.
	transientRetryAttempts = 3
	// transientRetryBudget bounds how long one retrying call may keep deciding to try again.
	//
	// It has to outlast one deadlock-detection window. A 40P01 reaches the client only after the
	// server has already waited deadlock_timeout (1s by default) on the contested lock, so a
	// one-second budget is spent while the first attempt is still being unblocked and the wrapper
	// returns that failure without calling fn a second time — the retry exists but never fires for
	// the exact case it was written for. Four seconds covers two detection windows plus the backoff
	// between them; with transientRetryAttempts at 3 the worst case is three unblock waits, so a
	// request that keeps deadlocking gives up after roughly 3.5s instead of never retrying at all.
	transientRetryBudget = 4 * time.Second
	// transientRetryBaseDelay is the first backoff; it doubles per attempt and carries jitter so
	// callers that collided once do not line up again.
	transientRetryBaseDelay = 20 * time.Millisecond
	// transientRetryHint is what a 503 tells the caller to wait before trying again. The server
	// already retried internally within transientRetryBudget, so the caller waits one second
	// rather than immediately re-entering the same contention.
	transientRetryHint = time.Second
)

// retryOnTransient runs fn until it succeeds, until the failure stops being a transient Postgres
// one, or until the attempt or wall-clock bound is reached, and returns the last failure.
//
// fn must open a new transaction on every call. The failure it retries means the server already
// rolled the previous transaction back, so continuing on that transaction would only produce 25P02;
// the whole unit — begin, lock, domain writes, commit — is what gets retried.
//
// Only 40P01 and 40001 are retried. A failure whose outcome is unknown is never retried, because the
// transaction may have committed and a second attempt would create a second durable fact
// (isTransientDatabaseFailure). A semantic conclusion — a key reused for a different body, a missing
// resource, exhausted capacity — is produced by the caller's own values and is returned unchanged:
// retrying it would repeat a verdict that cannot change.
//
// The budget bounds the decision to retry, not an attempt already under way: the loop starts an
// attempt before consulting the clock, so the worst case is transientRetryBudget plus the unblock
// wait of whichever attempt was last.
func retryOnTransient(ctx context.Context, fn func(context.Context) error) error {
	deadline := time.Now().Add(transientRetryBudget)
	var err error
	for attempt := 1; ; attempt++ {
		err = fn(ctx)
		if err == nil || !isTransientDatabaseFailure(err) || attempt >= transientRetryAttempts {
			return err
		}
		delay := transientRetryDelay(attempt)
		if time.Now().Add(delay).After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(delay):
		}
	}
}

// transientRetryDelay is exponential backoff with full jitter on the exponential part.
func transientRetryDelay(attempt int) time.Duration {
	base := transientRetryBaseDelay << (attempt - 1)
	return base + time.Duration(rand.Int63n(int64(base)+1))
}

// isTransientDatabaseFailure reports whether err is a Postgres-level transient failure that a fresh
// transaction may survive.
//
// The commit-outcome check comes first and is not redundant: MarkCommitOutcomeUnknown keeps the
// driver error as its cause, so a deadlock that was reported while committing would still be found
// by errors.As. That failure may have committed, and repeating it would create a second run.
func isTransientDatabaseFailure(err error) bool {
	if err == nil || workflowruntime.IsCommitOutcomeUnknown(err) {
		return false
	}
	var databaseError *pgconn.PgError
	if !errors.As(err, &databaseError) {
		return false
	}
	return databaseError.Code == deadlockDetectedCode || databaseError.Code == serializationFailureCode
}

// runtimePersistenceError wraps one failed persistence statement in the runtime error that
// describes it honestly, or returns the operation-labelled failure unchanged when all the server
// said was "no".
//
// The adapter used to report every one of these as CodeConflict, which told a caller whose run was
// never created that it had an idempotency conflict: the honest answers are "the server rolled this
// transaction back, try again" (503), "this organization has no delivery capacity left" (429), and
// — when nothing is known — a plain server-side failure that the transport answers with 500 and
// records under its request id. None of them carries the constraint, table, or column name: the
// caller learns what to correct from the endpoint it called, not from the server's schema.
func runtimePersistenceError(operation string, err error) error {
	if workflowruntime.IsCommitOutcomeUnknown(err) {
		return &workflowruntime.Error{
			Code: workflowruntime.CodeUnavailable, Message: "the outcome of this request could not be confirmed",
			RetryAfter: transientRetryHint, Cause: err,
		}
	}
	var databaseError *pgconn.PgError
	if errors.As(err, &databaseError) {
		switch {
		case databaseError.Code == deadlockDetectedCode || databaseError.Code == serializationFailureCode:
			return &workflowruntime.Error{
				Code: workflowruntime.CodeUnavailable, Message: "the database could not complete this transaction",
				RetryAfter: transientRetryHint, Cause: err,
			}
		case databaseError.Code == capacityExceededCode:
			// 容量门禁（pending outbox / firing 上限）拒绝的是「现在不行」，不是「这个请求有错」：
			// 报 conflict 会让调用方以为幂等键冲突而放弃，实际稍后重试就能成功。
			return &workflowruntime.Error{
				Code: workflowruntime.CodeQuotaExceeded, Message: "organization delivery capacity is exhausted",
				RetryAfter: quotaRetryAfter, Cause: err,
			}
		case databaseError.Code == cannotConnectNowCode || strings.HasPrefix(databaseError.Code, connectionExceptionClass):
			return &workflowruntime.Error{
				Code: workflowruntime.CodeUnavailable, Message: "the database is temporarily unavailable",
				RetryAfter: transientRetryHint, Cause: err,
			}
		}
	}
	return fmt.Errorf("%s: %w", operation, err)
}
