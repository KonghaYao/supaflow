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

// MutationRecord is the durable result of one tenant-scoped operation key.
type MutationRecord struct {
	Replay      bool
	Owner       string
	OperationID string
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
	Complete(context.Context, string, string, string, MutationCompletion) error
}

// PostgresMutationLedger persists operator idempotency across replicas and restarts.
type PostgresMutationLedger struct {
	pool          *pgxpool.Pool
	now           func() time.Time
	leaseDuration time.Duration
}

// NewPostgresMutationLedger creates a durable ledger adapter.
func NewPostgresMutationLedger(pool *pgxpool.Pool) (*PostgresMutationLedger, error) {
	if pool == nil {
		return nil, ErrInvalidArgument
	}
	return &PostgresMutationLedger{pool: pool, now: func() time.Time { return time.Now().UTC() }, leaseDuration: 30 * time.Second}, nil
}

// Claim reserves a key or returns the completed response for an identical request.
func (l *PostgresMutationLedger) Claim(ctx context.Context, tenantID, operation, key string, requestHash []byte) (MutationRecord, error) {
	if tenantID == "" || operation == "" || !validMutationKey(key) || len(requestHash) != 32 {
		return MutationRecord{}, ErrInvalidArgument
	}
	owner, err := newID("mutation")
	if err != nil {
		return MutationRecord{}, err
	}
	now := l.now()
	operationID, err := newID("operation")
	if err != nil {
		return MutationRecord{}, err
	}
	var status string
	var statusCode *int
	var contentType *string
	var body []byte
	err = l.pool.QueryRow(ctx, `
		INSERT INTO ops_mutation_ledger
		(tenant_id,operation,idempotency_key,request_hash,status,operation_id,lease_owner,lease_expires_at,attempt_count,updated_at)
		VALUES ($1,$2,$3,$4,'pending',$5,$6,$7,1,$8)
		ON CONFLICT (tenant_id,operation,idempotency_key) DO UPDATE SET
		  lease_owner=EXCLUDED.lease_owner,lease_expires_at=EXCLUDED.lease_expires_at,
		  attempt_count=ops_mutation_ledger.attempt_count+1,updated_at=EXCLUDED.updated_at
		WHERE ops_mutation_ledger.status='pending'
		  AND ops_mutation_ledger.request_hash=EXCLUDED.request_hash
		  AND ops_mutation_ledger.lease_expires_at <= EXCLUDED.updated_at
		RETURNING status,operation_id,response_status,response_content_type,response_body`,
		tenantID, operation, key, requestHash, operationID, owner, now.Add(l.leaseDuration), now).
		Scan(&status, &operationID, &statusCode, &contentType, &body)
	if err == nil {
		return MutationRecord{Owner: owner, OperationID: operationID}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return MutationRecord{}, fmt.Errorf("claim operation mutation: %w", err)
	}
	var storedHash []byte
	err = l.pool.QueryRow(ctx, `
		SELECT request_hash,status,operation_id,response_status,response_content_type,response_body
		FROM ops_mutation_ledger WHERE tenant_id=$1 AND operation=$2 AND idempotency_key=$3`, tenantID, operation, key).
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

// Complete stores the exact bounded HTTP result for deterministic replay.
func (l *PostgresMutationLedger) Complete(ctx context.Context, tenantID, operation, key string, completion MutationCompletion) error {
	if tenantID == "" || operation == "" || !validMutationKey(key) || completion.Owner == "" || completion.StatusCode < 100 || completion.StatusCode > 599 || len(completion.Body) > 2<<20 {
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
		response_body=$6,completed_at=now(),updated_at=now(),lease_expires_at=NULL
		WHERE tenant_id=$1 AND operation=$2 AND idempotency_key=$3 AND status='pending' AND lease_owner=$7`,
		tenantID, operation, key, completion.StatusCode, completion.ContentType, completion.Body, completion.Owner)
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
