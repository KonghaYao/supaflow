package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Event is an ordered durable run event. Sequence is the SSE cursor.
type Event struct {
	ID        string
	TenantID  string
	RunID     string
	Sequence  int64
	Type      string
	Payload   json.RawMessage
	CreatedAt time.Time
}

const nextEventSequenceSQL = `
UPDATE runs
SET next_event_sequence = next_event_sequence + 1, updated_at = now()
WHERE tenant_id = $1 AND id = $2
RETURNING next_event_sequence`

const insertEventSQL = `
INSERT INTO run_events (id, tenant_id, run_id, sequence, event_type, payload)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING created_at`

func appendEventTx(ctx context.Context, tx pgx.Tx, tenantID, runID, eventType string, payload json.RawMessage) (Event, error) {
	var event Event
	if tenantID == "" || runID == "" || eventType == "" {
		return event, errors.New("append event requires tenant, run, and event type")
	}
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	if err := tx.QueryRow(ctx, nextEventSequenceSQL, tenantID, runID).Scan(&event.Sequence); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return event, ErrNotFound
		}
		return event, fmt.Errorf("allocate run event sequence: %w", err)
	}
	eventID, err := NewID("evt")
	if err != nil {
		return event, err
	}
	event.ID, event.TenantID, event.RunID, event.Type, event.Payload =
		eventID, tenantID, runID, eventType, payload
	if err := tx.QueryRow(ctx, insertEventSQL,
		event.ID, tenantID, runID, event.Sequence, eventType, payload,
	).Scan(&event.CreatedAt); err != nil {
		return Event{}, fmt.Errorf("insert run event: %w", err)
	}
	return event, nil
}

const listEventsSQL = `
SELECT id, tenant_id, run_id, sequence, event_type, payload, created_at
FROM run_events
WHERE tenant_id = $1 AND run_id = $2 AND sequence > $3
ORDER BY sequence ASC
LIMIT $4`

// ListEvents returns events strictly after cursor for SSE resume.
func (r *Repository) ListEvents(ctx context.Context, tenantID, runID string, after int64, limit int) ([]Event, error) {
	if err := r.requirePool(); err != nil {
		return nil, err
	}
	if tenantID == "" || runID == "" {
		return nil, errors.New("list events requires tenant and run")
	}
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	rows, err := r.pool.Query(ctx, listEventsSQL, tenantID, runID, after, limit)
	if err != nil {
		return nil, fmt.Errorf("query run events: %w", err)
	}
	defer rows.Close()
	events := make([]Event, 0)
	for rows.Next() {
		var event Event
		if err := rows.Scan(&event.ID, &event.TenantID, &event.RunID, &event.Sequence,
			&event.Type, &event.Payload, &event.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan run event: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate run events: %w", err)
	}
	return events, nil
}
