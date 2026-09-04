package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// OutboxMessage is a transactionally persisted delivery request.
type OutboxMessage struct {
	ID            string
	TenantID      string
	AggregateType string
	AggregateID   string
	Topic         string
	Payload       json.RawMessage
	Attempts      int
	CreatedAt     time.Time
}

func insertOutboxTx(ctx context.Context, tx pgx.Tx, tenantID, aggregateType, aggregateID, topic string, payload json.RawMessage) error {
	id, err := NewID("out")
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_messages (
		    id, tenant_id, aggregate_type, aggregate_id, topic, payload
		) VALUES ($1, $2, $3, $4, $5, $6)`,
		id, tenantID, aggregateType, aggregateID, topic, payload,
	)
	if err != nil {
		return fmt.Errorf("insert outbox message: %w", err)
	}
	return nil
}

const claimOutboxSQL = `
WITH candidate AS (
    SELECT id
    FROM outbox_messages
    WHERE tenant_id = $1
      AND delivered_at IS NULL
      AND available_at <= clock_timestamp()
      AND (claimed_by IS NULL OR claim_expires_at <= clock_timestamp())
    ORDER BY available_at, created_at, id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE outbox_messages o
SET claimed_by = $2,
    claim_expires_at = clock_timestamp() + ($3 * interval '1 millisecond'),
    attempts = attempts + 1
FROM candidate
WHERE o.tenant_id = $1 AND o.id = candidate.id
RETURNING o.id, o.tenant_id, o.aggregate_type, o.aggregate_id, o.topic,
          o.payload, o.attempts, o.created_at`

// ClaimOutbox claims one tenant-scoped message for at-least-once delivery.
func (r *Repository) ClaimOutbox(ctx context.Context, tenantID, workerID string, duration time.Duration) (OutboxMessage, error) {
	if err := r.requirePool(); err != nil {
		return OutboxMessage{}, err
	}
	if tenantID == "" || workerID == "" || duration <= 0 {
		return OutboxMessage{}, errors.New("claim outbox requires tenant, worker, and positive duration")
	}
	var message OutboxMessage
	err := r.pool.QueryRow(ctx, claimOutboxSQL, tenantID, workerID, duration.Milliseconds()).Scan(
		&message.ID, &message.TenantID, &message.AggregateType, &message.AggregateID,
		&message.Topic, &message.Payload, &message.Attempts, &message.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return OutboxMessage{}, ErrNotFound
	}
	if err != nil {
		return OutboxMessage{}, fmt.Errorf("claim outbox message: %w", err)
	}
	return message, nil
}

// MarkOutboxDelivered atomically acknowledges a message owned by workerID.
func (r *Repository) MarkOutboxDelivered(ctx context.Context, tenantID, messageID, workerID string) error {
	if err := r.requirePool(); err != nil {
		return err
	}
	tag, err := r.pool.Exec(ctx, `
		UPDATE outbox_messages
		SET delivered_at = now(), claimed_by = NULL, claim_expires_at = NULL, last_error = NULL
		WHERE tenant_id = $1 AND id = $2 AND claimed_by = $3
		  AND delivered_at IS NULL AND claim_expires_at > clock_timestamp()`,
		tenantID, messageID, workerID,
	)
	if err != nil {
		return fmt.Errorf("acknowledge outbox message: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrStaleLease
	}
	return nil
}

// ReleaseOutbox records a bounded diagnostic and schedules a retry. The caller
// must redact external errors before passing lastError.
func (r *Repository) ReleaseOutbox(ctx context.Context, tenantID, messageID, workerID, lastError string, retryAt time.Time) error {
	if err := r.requirePool(); err != nil {
		return err
	}
	if len(lastError) > 2000 {
		lastError = lastError[:2000]
	}
	tag, err := r.pool.Exec(ctx, `
		UPDATE outbox_messages
		SET claimed_by = NULL, claim_expires_at = NULL, last_error = $4, available_at = $5
		WHERE tenant_id = $1 AND id = $2 AND claimed_by = $3 AND delivered_at IS NULL`,
		tenantID, messageID, workerID, lastError, retryAt,
	)
	if err != nil {
		return fmt.Errorf("release outbox message: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrStaleLease
	}
	return nil
}
