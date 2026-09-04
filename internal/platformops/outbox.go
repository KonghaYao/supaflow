package platformops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/propagation"
)

// DeliveryMessage is one leased at-least-once outbox delivery.
type DeliveryMessage struct {
	ID             string
	TenantID       string
	AggregateType  string
	AggregateID    string
	Topic          string
	Payload        json.RawMessage
	Attempts       int
	MaxAttempts    int
	LeaseExpiresAt time.Time
	TraceHeaders   map[string]string
}

// DeadLetter is a terminally failed outbox delivery safe for operations APIs.
type DeadLetter struct {
	ID                string          `json:"id"`
	TenantID          string          `json:"-"`
	OriginalMessageID string          `json:"originalMessageId"`
	AggregateType     string          `json:"aggregateType"`
	AggregateID       string          `json:"aggregateId"`
	Topic             string          `json:"topic"`
	Payload           json.RawMessage `json:"payload"`
	Attempts          int             `json:"attempts"`
	LastError         string          `json:"lastError"`
	FailedAt          time.Time       `json:"failedAt"`
	RequeuedAt        *time.Time      `json:"requeuedAt,omitempty"`
}

// DeliveryStore owns durable claim, acknowledgement, retry, and DLQ transitions.
type DeliveryStore interface {
	ClaimDelivery(context.Context, string, string, time.Time, time.Duration) (DeliveryMessage, error)
	RenewDeliveryLease(context.Context, DeliveryMessage, string, time.Time, time.Duration) (time.Time, error)
	AcknowledgeDelivery(context.Context, DeliveryMessage, string, time.Time) error
	FailDelivery(context.Context, DeliveryMessage, string, string, time.Time, time.Time) (bool, error)
	ListDeadLetters(context.Context, string, int) ([]DeadLetter, error)
	RequeueDeadLetter(context.Context, string, string, time.Time) error
}

// DeliveryClient performs the external side effect at the system boundary.
type DeliveryClient interface {
	Deliver(context.Context, DeliveryMessage) error
}

// DispatchResult reports one bounded dispatcher action.
type DispatchResult struct {
	MessageID  string
	Delivered  bool
	DeadLetter bool
	RetryAt    *time.Time
}

// OutboxDispatcher provides at-least-once delivery with bounded retry and DLQ.
type OutboxDispatcher struct {
	store       DeliveryStore
	client      DeliveryClient
	workerID    string
	lease       time.Duration
	baseBackoff time.Duration
	maxBackoff  time.Duration
	now         func() time.Time
	telemetry   OperationTelemetry
}

// SetTelemetry enables tenant-hashed tracing and outbound W3C propagation.
func (d *OutboxDispatcher) SetTelemetry(telemetry OperationTelemetry) { d.telemetry = telemetry }

// NewOutboxDispatcher creates a bounded transactional outbox dispatcher.
func NewOutboxDispatcher(store DeliveryStore, client DeliveryClient, workerID string, lease, baseBackoff, maxBackoff time.Duration) (*OutboxDispatcher, error) {
	if store == nil || client == nil || workerID == "" || lease <= 0 || baseBackoff <= 0 || maxBackoff < baseBackoff {
		return nil, ErrInvalidArgument
	}
	return &OutboxDispatcher{store: store, client: client, workerID: workerID, lease: lease, baseBackoff: baseBackoff, maxBackoff: maxBackoff, now: func() time.Time { return time.Now().UTC() }}, nil
}

// DispatchOne claims and processes at most one tenant-scoped message.
func (d *OutboxDispatcher) DispatchOne(ctx context.Context, tenantID string, now time.Time) (DispatchResult, error) {
	if tenantID == "" || now.IsZero() {
		return DispatchResult{}, ErrInvalidArgument
	}
	message, err := d.store.ClaimDelivery(ctx, tenantID, d.workerID, now, d.lease)
	if errors.Is(err, ErrNotFound) {
		return DispatchResult{}, nil
	}
	if err != nil {
		return DispatchResult{}, err
	}
	result := DispatchResult{MessageID: message.ID}
	deliveryCtx, cancelDelivery := context.WithCancel(ctx)
	completeTelemetry := func(error) {}
	if d.telemetry != nil {
		deliveryCtx, completeTelemetry = d.telemetry.Start(deliveryCtx, "outbox.delivery", message.TenantID)
		message.TraceHeaders = make(map[string]string)
		d.telemetry.Inject(deliveryCtx, propagation.MapCarrier(message.TraceHeaders))
	}
	renewalDone := make(chan error, 1)
	go func() { renewalDone <- d.renewLease(deliveryCtx, message) }()
	err = d.client.Deliver(deliveryCtx, message)
	completeTelemetry(err)
	cancelDelivery()
	renewalErr := <-renewalDone
	if renewalErr != nil {
		return result, renewalErr
	}
	if err == nil {
		completedAt := d.now()
		if completedAt.Before(now) {
			completedAt = now
		}
		if err = d.store.AcknowledgeDelivery(ctx, message, d.workerID, completedAt); err != nil {
			return result, err
		}
		result.Delivered = true
		return result, nil
	}
	completedAt := d.now()
	if completedAt.Before(now) {
		completedAt = now
	}
	retryAt := completedAt.Add(d.retryDelay(message.Attempts))
	dead, storeErr := d.store.FailDelivery(ctx, message, d.workerID, RedactDiagnostic(err.Error()), retryAt, completedAt)
	if storeErr != nil {
		return result, storeErr
	}
	result.DeadLetter = dead
	if !dead {
		result.RetryAt = &retryAt
	}
	return result, nil
}

func (d *OutboxDispatcher) renewLease(ctx context.Context, message DeliveryMessage) error {
	interval := d.lease / 3
	if interval <= 0 {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			now := d.now()
			expiresAt, err := d.store.RenewDeliveryLease(ctx, message, d.workerID, now, d.lease)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
			message.LeaseExpiresAt = expiresAt
		}
	}
}

func (d *OutboxDispatcher) retryDelay(attempt int) time.Duration {
	exponent := attempt - 1
	if exponent < 0 {
		exponent = 0
	}
	if exponent > 30 {
		exponent = 30
	}
	delay := time.Duration(float64(d.baseBackoff) * math.Pow(2, float64(exponent)))
	if delay > d.maxBackoff || delay < 0 {
		return d.maxBackoff
	}
	return delay
}

// PostgresDeliveryStore coordinates dispatcher replicas with row leases.
type PostgresDeliveryStore struct {
	pool   *pgxpool.Pool
	topics []string
}

// NewPostgresDeliveryStore creates a durable delivery store.
func NewPostgresDeliveryStore(pool *pgxpool.Pool) *PostgresDeliveryStore {
	return &PostgresDeliveryStore{pool: pool}
}

// NewPostgresTopicDeliveryStore restricts claims to topics owned by one dispatcher.
func NewPostgresTopicDeliveryStore(pool *pgxpool.Pool, topics ...string) *PostgresDeliveryStore {
	return &PostgresDeliveryStore{pool: pool, topics: append([]string(nil), topics...)}
}

// ClaimDelivery leases one due message with SKIP LOCKED.
func (s *PostgresDeliveryStore) ClaimDelivery(ctx context.Context, tenantID, workerID string, now time.Time, lease time.Duration) (DeliveryMessage, error) {
	var message DeliveryMessage
	message.LeaseExpiresAt = now.Add(lease)
	err := s.pool.QueryRow(ctx, `
		WITH candidate AS (
		    SELECT id FROM outbox_messages
		    WHERE tenant_id=$1 AND delivered_at IS NULL AND available_at <= $3
		      AND (claimed_by IS NULL OR claim_expires_at <= $3)
		      AND (coalesce(cardinality($5::text[]),0) = 0 OR topic = ANY($5::text[]))
		    ORDER BY available_at,created_at,id FOR UPDATE SKIP LOCKED LIMIT 1
		)
		UPDATE outbox_messages o SET claimed_by=$2,claim_expires_at=$4,attempts=o.attempts+1
		FROM candidate WHERE o.tenant_id=$1 AND o.id=candidate.id
		RETURNING o.id,o.tenant_id,o.aggregate_type,o.aggregate_id,o.topic,o.payload,o.attempts,o.max_attempts`,
		tenantID, workerID, now, message.LeaseExpiresAt, s.topics).Scan(&message.ID, &message.TenantID,
		&message.AggregateType, &message.AggregateID, &message.Topic, &message.Payload,
		&message.Attempts, &message.MaxAttempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return DeliveryMessage{}, ErrNotFound
	}
	if err != nil {
		return DeliveryMessage{}, fmt.Errorf("claim outbox delivery: %w", err)
	}
	return message, nil
}

// RenewDeliveryLease extends only the currently owned, unexpired row lease.
func (s *PostgresDeliveryStore) RenewDeliveryLease(ctx context.Context, message DeliveryMessage, workerID string, now time.Time, lease time.Duration) (time.Time, error) {
	if lease <= 0 {
		return time.Time{}, ErrInvalidArgument
	}
	expiresAt := now.Add(lease)
	tag, err := s.pool.Exec(ctx, `UPDATE outbox_messages SET claim_expires_at=$5
		WHERE tenant_id=$1 AND id=$2 AND claimed_by=$3 AND delivered_at IS NULL AND claim_expires_at>$4`,
		message.TenantID, message.ID, workerID, now, expiresAt)
	if err != nil {
		return time.Time{}, fmt.Errorf("renew outbox delivery lease: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return time.Time{}, ErrStaleLease
	}
	return expiresAt, nil
}

// AcknowledgeDelivery marks a currently owned lease delivered.
func (s *PostgresDeliveryStore) AcknowledgeDelivery(ctx context.Context, message DeliveryMessage, workerID string, now time.Time) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE outbox_messages SET delivered_at=$4,claimed_by=NULL,claim_expires_at=NULL,last_error=NULL
		WHERE tenant_id=$1 AND id=$2 AND claimed_by=$3 AND delivered_at IS NULL AND claim_expires_at > $4`,
		message.TenantID, message.ID, workerID, now)
	if err != nil {
		return fmt.Errorf("acknowledge outbox delivery: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrStaleLease
	}
	return nil
}

// FailDelivery schedules retry or atomically moves an exhausted message to DLQ.
func (s *PostgresDeliveryStore) FailDelivery(ctx context.Context, message DeliveryMessage, workerID, diagnostic string, retryAt, now time.Time) (bool, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, fmt.Errorf("begin outbox failure: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var attempts, maxAttempts int
	err = tx.QueryRow(ctx, `
		SELECT attempts,max_attempts FROM outbox_messages
		WHERE tenant_id=$1 AND id=$2 AND claimed_by=$3 AND delivered_at IS NULL AND claim_expires_at > $4
		FOR UPDATE`, message.TenantID, message.ID, workerID, now).Scan(&attempts, &maxAttempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, ErrStaleLease
	}
	if err != nil {
		return false, fmt.Errorf("lock failed outbox delivery: %w", err)
	}
	diagnostic = RedactDiagnostic(diagnostic)
	dead := attempts >= maxAttempts
	if dead {
		deadID, idErr := newID("dlq")
		if idErr != nil {
			return false, idErr
		}
		if _, err = tx.Exec(ctx, `
			INSERT INTO outbox_dead_letters (
			    tenant_id,id,original_message_id,aggregate_type,aggregate_id,topic,payload,attempts,last_error,failed_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, message.TenantID, deadID,
			message.ID, message.AggregateType, message.AggregateID, message.Topic, message.Payload,
			attempts, diagnostic, now); err != nil {
			return false, fmt.Errorf("persist dead letter: %w", err)
		}
		if _, err = tx.Exec(ctx, `
			UPDATE outbox_messages SET delivered_at=$4,claimed_by=NULL,claim_expires_at=NULL,last_error=$5
			WHERE tenant_id=$1 AND id=$2 AND claimed_by=$3`, message.TenantID, message.ID, workerID, now, diagnostic); err != nil {
			return false, fmt.Errorf("finalize dead letter source: %w", err)
		}
	} else if _, err = tx.Exec(ctx, `
		UPDATE outbox_messages SET claimed_by=NULL,claim_expires_at=NULL,last_error=$4,available_at=$5
		WHERE tenant_id=$1 AND id=$2 AND claimed_by=$3`, message.TenantID, message.ID, workerID, diagnostic, retryAt); err != nil {
		return false, fmt.Errorf("schedule outbox retry: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit outbox failure: %w", err)
	}
	return dead, nil
}

// ListDeadLetters lists bounded tenant-scoped delivery failures.
func (s *PostgresDeliveryStore) ListDeadLetters(ctx context.Context, tenantID string, limit int) ([]DeadLetter, error) {
	if tenantID == "" || limit <= 0 || limit > 500 {
		return nil, ErrInvalidArgument
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id,tenant_id,original_message_id,aggregate_type,aggregate_id,topic,payload,
		       attempts,last_error,failed_at,requeued_at
		FROM outbox_dead_letters WHERE tenant_id=$1 ORDER BY failed_at DESC,id LIMIT $2`, tenantID, limit)
	if err != nil {
		return nil, fmt.Errorf("list dead letters: %w", err)
	}
	defer rows.Close()
	letters := make([]DeadLetter, 0, limit)
	for rows.Next() {
		var letter DeadLetter
		if err = rows.Scan(&letter.ID, &letter.TenantID, &letter.OriginalMessageID,
			&letter.AggregateType, &letter.AggregateID, &letter.Topic, &letter.Payload,
			&letter.Attempts, &letter.LastError, &letter.FailedAt, &letter.RequeuedAt); err != nil {
			return nil, fmt.Errorf("scan dead letter: %w", err)
		}
		letter.Payload, err = redactJSON(letter.Payload)
		if err != nil {
			return nil, fmt.Errorf("redact dead letter payload: %w", err)
		}
		letters = append(letters, letter)
	}
	return letters, rows.Err()
}

// RequeueDeadLetter creates a fresh delivery once and marks the DLQ record requeued atomically.
func (s *PostgresDeliveryStore) RequeueDeadLetter(ctx context.Context, tenantID, deadLetterID string, now time.Time) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin dead letter requeue: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var letter DeadLetter
	err = tx.QueryRow(ctx, `
		SELECT original_message_id,aggregate_type,aggregate_id,topic,payload,requeued_at
		FROM outbox_dead_letters WHERE tenant_id=$1 AND id=$2 FOR UPDATE`, tenantID, deadLetterID).Scan(
		&letter.OriginalMessageID, &letter.AggregateType, &letter.AggregateID,
		&letter.Topic, &letter.Payload, &letter.RequeuedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("lock dead letter: %w", err)
	}
	if letter.RequeuedAt != nil {
		if _, recovery := MutationResourceID(ctx, "out", "dead-letter:"+deadLetterID); recovery {
			if commitErr := tx.Commit(ctx); commitErr != nil {
				return fmt.Errorf("commit dead letter replay: %w", commitErr)
			}
			return nil
		}
		return ErrConflict
	}
	messageID, err := mutationResourceIDOrRandom(ctx, "out", "dead-letter:"+deadLetterID)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO outbox_messages (id,tenant_id,aggregate_type,aggregate_id,topic,payload,available_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`, messageID, tenantID, letter.AggregateType,
		letter.AggregateID, letter.Topic, letter.Payload, now); err != nil {
		return fmt.Errorf("requeue dead letter: %w", err)
	}
	if _, err = tx.Exec(ctx, `UPDATE outbox_dead_letters SET requeued_at=$3 WHERE tenant_id=$1 AND id=$2`, tenantID, deadLetterID, now); err != nil {
		return fmt.Errorf("mark dead letter requeued: %w", err)
	}
	return tx.Commit(ctx)
}

// RedactDiagnostic removes common secret-bearing key/value material and bounds storage.
func RedactDiagnostic(value string) string {
	redacted := value
	for _, marker := range []string{"authorization", "password", "secret", "token", "api_key", "apikey"} {
		searchFrom := 0
		for {
			lower := strings.ToLower(redacted)
			relative := strings.Index(lower[searchFrom:], marker)
			if relative < 0 {
				break
			}
			index := searchFrom + relative
			valueStart := index + len(marker)
			for valueStart < len(redacted) && (redacted[valueStart] == ' ' || redacted[valueStart] == ':' || redacted[valueStart] == '=') {
				valueStart++
			}
			end := valueStart
			for end < len(redacted) && redacted[end] != ' ' && redacted[end] != ',' && redacted[end] != ';' {
				end++
			}
			replacement := marker + "=[REDACTED]"
			redacted = redacted[:index] + replacement + redacted[end:]
			searchFrom = index + len(replacement)
			if len(redacted) > 2000 {
				break
			}
		}
	}
	if len(redacted) > 2000 {
		redacted = redacted[:2000]
	}
	return redacted
}
