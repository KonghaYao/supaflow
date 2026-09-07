package platformops

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// MutationRecord is the durable result of one organization-scoped operation key.
type MutationRecord struct {
	Replay      bool
	Owner       string
	OperationID string
	RenewAfter  time.Duration
	StatusCode  int
	ContentType string
	Body        []byte
}

// MutationCompletion is the exact response and audit evidence committed by
// the current lease owner. Audit is nil only when the domain mutation already
// persisted equivalent evidence atomically.
type MutationCompletion struct {
	Owner       string
	StatusCode  int
	ContentType string
	Body        []byte
	Audit       *AuditRecord
}

// MutationLedger claims and completes operator mutations before a response is returned.
type MutationLedger interface {
	Claim(context.Context, string, string, string, []byte) (MutationRecord, error)
	Renew(context.Context, string, string, string, string) error
	Retry(context.Context, string, string, string, string) error
	Complete(context.Context, string, string, string, MutationCompletion) error
}

// PostgresMutationLedger persists operator idempotency across replicas and restarts.
type PostgresMutationLedger struct {
	pool          *pgxpool.Pool
	leaseDuration time.Duration
}

// NewPostgresMutationLedger creates a durable ledger adapter.
func NewPostgresMutationLedger(pool *pgxpool.Pool) (*PostgresMutationLedger, error) {
	if pool == nil {
		return nil, ErrInvalidArgument
	}
	return NewPostgresMutationLedgerWithLease(pool, 30*time.Second)
}

// NewPostgresMutationLedgerWithLease creates a durable ledger with an explicit
// lease duration. Production callers should use NewPostgresMutationLedger;
// the explicit form supports deterministic short-lease integration tests.
func NewPostgresMutationLedgerWithLease(pool *pgxpool.Pool, leaseDuration time.Duration) (*PostgresMutationLedger, error) {
	if pool == nil || leaseDuration <= 0 {
		return nil, ErrInvalidArgument
	}
	return &PostgresMutationLedger{pool: pool, leaseDuration: leaseDuration}, nil
}

// Claim reserves a key or returns the completed response for an identical request.
func (l *PostgresMutationLedger) Claim(ctx context.Context, organizationID, operation, key string, requestHash []byte) (MutationRecord, error) {
	if organizationID == "" || operation == "" || !validMutationKey(key) || len(requestHash) != 32 {
		return MutationRecord{}, ErrInvalidArgument
	}
	owner, err := newID("mutation")
	if err != nil {
		return MutationRecord{}, err
	}
	operationID, err := newID("operation")
	if err != nil {
		return MutationRecord{}, err
	}
	var status string
	var statusCode *int
	var contentType *string
	var body []byte
	var leaseExpiresAt *time.Time
	err = l.pool.QueryRow(ctx, `
		INSERT INTO ops_mutation_ledger
		(organization_id,operation,idempotency_key,request_hash,status,operation_id,lease_owner,lease_expires_at,attempt_count,updated_at)
		VALUES ($1,$2,$3,$4,'pending',$5,$6,clock_timestamp()+make_interval(secs => $7),1,clock_timestamp())
		ON CONFLICT (organization_id,operation,idempotency_key) DO UPDATE SET
		  lease_owner=EXCLUDED.lease_owner,lease_expires_at=EXCLUDED.lease_expires_at,
		  attempt_count=ops_mutation_ledger.attempt_count+1,updated_at=clock_timestamp()
		WHERE ops_mutation_ledger.status='pending'
		  AND ops_mutation_ledger.request_hash=EXCLUDED.request_hash
		  AND ops_mutation_ledger.lease_expires_at <= clock_timestamp()
		RETURNING status,operation_id,response_status,response_content_type,response_body,lease_expires_at`,
		organizationID, operation, key, requestHash, operationID, owner, l.leaseDuration.Seconds()).
		Scan(&status, &operationID, &statusCode, &contentType, &body, &leaseExpiresAt)
	if err == nil {
		return MutationRecord{Owner: owner, OperationID: operationID, RenewAfter: l.leaseDuration / 3}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return MutationRecord{}, fmt.Errorf("claim operation mutation: %w", err)
	}
	var storedHash []byte
	err = l.pool.QueryRow(ctx, `
		SELECT request_hash,status,operation_id,response_status,response_content_type,response_body
		FROM ops_mutation_ledger WHERE organization_id=$1 AND operation=$2 AND idempotency_key=$3`, organizationID, operation, key).
		Scan(&storedHash, &status, &operationID, &statusCode, &contentType, &body)
	if err != nil {
		return MutationRecord{}, fmt.Errorf("read operation mutation: %w", err)
	}
	if !bytes.Equal(storedHash, requestHash) {
		return MutationRecord{}, ErrConflict
	}
	if status != "completed" || statusCode == nil {
		return MutationRecord{}, ErrConflict
	}
	record := MutationRecord{Replay: true, OperationID: operationID, StatusCode: *statusCode, Body: append([]byte(nil), body...)}
	if contentType != nil {
		record.ContentType = *contentType
	}
	return record, nil
}

// Renew extends only the live lease owned by the caller, using the database
// clock so application-host skew cannot resurrect an expired owner.
func (l *PostgresMutationLedger) Renew(ctx context.Context, organizationID, operation, key, owner string) error {
	if organizationID == "" || operation == "" || !validMutationKey(key) || owner == "" {
		return ErrInvalidArgument
	}
	tag, err := l.pool.Exec(ctx, `UPDATE ops_mutation_ledger
		SET lease_expires_at=clock_timestamp()+make_interval(secs => $5),updated_at=clock_timestamp()
		WHERE organization_id=$1 AND operation=$2 AND idempotency_key=$3 AND status='pending'
		  AND lease_owner=$4 AND lease_expires_at > clock_timestamp()`,
		organizationID, operation, key, owner, l.leaseDuration.Seconds())
	if err != nil {
		return fmt.Errorf("renew operation mutation: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrConflict
	}
	return nil
}

// Retry relinquishes a live pending lease without discarding its stable
// operation identity. This is used only when an idempotent domain operation
// reports retryable backpressure after committing a resumable prefix.
func (l *PostgresMutationLedger) Retry(ctx context.Context, organizationID, operation, key, owner string) error {
	if organizationID == "" || operation == "" || !validMutationKey(key) || owner == "" {
		return ErrInvalidArgument
	}
	tag, err := l.pool.Exec(ctx, `UPDATE ops_mutation_ledger
		SET lease_expires_at=clock_timestamp(),updated_at=clock_timestamp()
		WHERE organization_id=$1 AND operation=$2 AND idempotency_key=$3 AND status='pending'
		  AND lease_owner=$4 AND lease_expires_at > clock_timestamp()`, organizationID, operation, key, owner)
	if err != nil {
		return fmt.Errorf("release retryable operation mutation: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrConflict
	}
	return nil
}

// Complete stores the exact bounded HTTP result for deterministic replay.
func (l *PostgresMutationLedger) Complete(ctx context.Context, organizationID, operation, key string, completion MutationCompletion) error {
	if organizationID == "" || operation == "" || !validMutationKey(key) || completion.Owner == "" || completion.StatusCode < 100 || completion.StatusCode > 599 || len(completion.Body) > 2<<20 {
		return ErrInvalidArgument
	}
	tx, err := l.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin operation completion: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if completion.Audit != nil {
		if _, err = insertAuditTx(ctx, tx, *completion.Audit); err != nil {
			return fmt.Errorf("persist operation audit: %w", err)
		}
	}
	tag, err := tx.Exec(ctx, `
		UPDATE ops_mutation_ledger SET status='completed',response_status=$4,response_content_type=$5,
		response_body=$6,completed_at=clock_timestamp(),updated_at=clock_timestamp(),lease_expires_at=NULL
		WHERE organization_id=$1 AND operation=$2 AND idempotency_key=$3 AND status='pending' AND lease_owner=$7
		  AND lease_expires_at > clock_timestamp()`,
		organizationID, operation, key, completion.StatusCode, completion.ContentType, completion.Body, completion.Owner)
	if err != nil {
		return fmt.Errorf("complete operation mutation: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrConflict
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit operation completion: %w", err)
	}
	return nil
}

var _ MutationLedger = (*PostgresMutationLedger)(nil)
