package platformops

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	eventFanoutPageSize = 100
	maxEventFanout      = 10000
)

type eventTriggerTarget struct {
	id       string
	created  time.Time
	existing bool
}

// FireEvent fans out through bounded independently committed pages. A full
// tenant applies backpressure without holding an unbounded trigger result set
// or one transaction open across the complete fanout.
func (s *PostgresTriggerStore) FireEvent(ctx context.Context, tenantID, eventType, eventID string, payload json.RawMessage, at time.Time) ([]Firing, error) {
	firings := make([]Firing, 0, eventFanoutPageSize)
	var cursorTime *time.Time
	cursorID := ""
	for len(firings) < maxEventFanout {
		page, nextTime, nextID, done, err := s.fireEventPage(ctx, tenantID, eventType, eventID, payload, at, cursorTime, cursorID, maxEventFanout-len(firings))
		if err != nil {
			return firings, err
		}
		firings = append(firings, page...)
		if done {
			return firings, nil
		}
		cursorTime, cursorID = &nextTime, nextID
	}
	remaining, err := s.hasEventTargetsAfter(ctx, tenantID, eventType, cursorTime, cursorID)
	if err != nil {
		return firings, err
	}
	if !remaining {
		return firings, nil
	}
	return firings, triggerCapacityError()
}

func (s *PostgresTriggerStore) fireEventPage(
	ctx context.Context,
	tenantID, eventType, eventID string,
	payload json.RawMessage,
	at time.Time,
	cursorTime *time.Time,
	cursorID string,
	remaining int,
) ([]Firing, time.Time, string, bool, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, time.Time{}, "", false, triggerError("begin event firing page", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	pageLimit := min(eventFanoutPageSize, remaining)
	targets, err := selectEventTriggerTargets(ctx, tx, tenantID, eventType, eventID, cursorTime, cursorID, pageLimit)
	if err != nil {
		return nil, time.Time{}, "", false, err
	}
	if len(targets) == 0 {
		audited, auditErr := recordAllowedMutationTx(ctx, tx, eventType)
		if auditErr != nil {
			return nil, time.Time{}, "", false, triggerError("audit empty event firing", auditErr)
		}
		if err = tx.Commit(ctx); err != nil {
			return nil, time.Time{}, "", false, triggerError("commit empty event firing", err)
		}
		if audited {
			markMutationCommitted(ctx)
		}
		return nil, time.Time{}, "", true, nil
	}
	available, err := tenantTriggerIngressCapacityTx(ctx, tx, tenantID)
	if err != nil {
		return nil, time.Time{}, "", false, err
	}
	selected := targets[:0]
	newFirings := 0
	for _, target := range targets {
		if !target.existing {
			if newFirings >= available {
				break
			}
			newFirings++
		}
		selected = append(selected, target)
	}
	if len(selected) == 0 {
		return nil, time.Time{}, "", false, triggerCapacityError()
	}
	firings := make([]Firing, 0, len(selected))
	for _, target := range selected {
		firing, _, fireErr := insertFiringTx(ctx, tx, FireRequest{
			TenantID: tenantID, TriggerID: target.id, ExpectedKind: TriggerEvent,
			DeduplicationKey: "event:" + eventID, SourceTime: &at, Payload: payload,
			StableReplaySourceTime: true,
		})
		if fireErr != nil {
			return nil, time.Time{}, "", false, fireErr
		}
		firings = append(firings, firing)
	}
	audited, err := recordAllowedMutationTx(ctx, tx, eventType)
	if err != nil {
		return nil, time.Time{}, "", false, triggerError("audit event firing page", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, time.Time{}, "", false, triggerError("commit event firing page", err)
	}
	if audited {
		markMutationCommitted(ctx)
	}
	last := selected[len(selected)-1]
	done := len(targets) < pageLimit && len(selected) == len(targets)
	return firings, last.created, last.id, done, nil
}

func (s *PostgresTriggerStore) hasEventTargetsAfter(ctx context.Context, tenantID, eventType string, cursorTime *time.Time, cursorID string) (bool, error) {
	var found bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM triggers trigger
		WHERE trigger.tenant_id=$1 AND trigger.kind='event' AND trigger.status='enabled'
		  AND trigger.event_type=$2
		  AND ($3::timestamptz IS NULL OR (trigger.created_at,trigger.id)>($3,CAST($4 AS text)))
	)`, tenantID, eventType, cursorTime, cursorID).Scan(&found)
	if err != nil {
		return false, triggerError("check remaining event triggers", err)
	}
	return found, nil
}

func selectEventTriggerTargets(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, eventType, eventID string,
	cursorTime *time.Time,
	cursorID string,
	limit int,
) ([]eventTriggerTarget, error) {
	rows, err := tx.Query(ctx, `
		SELECT trigger.id,trigger.created_at,(firing.id IS NOT NULL)
		FROM triggers trigger
		LEFT JOIN trigger_firings firing
		  ON firing.tenant_id=trigger.tenant_id AND firing.trigger_id=trigger.id
		 AND firing.deduplication_key=$3
		WHERE trigger.tenant_id=$1 AND trigger.kind='event' AND trigger.status='enabled'
		  AND trigger.event_type=$2
		  AND ($4::timestamptz IS NULL OR (trigger.created_at,trigger.id)>($4,CAST($5 AS text)))
		ORDER BY trigger.created_at,trigger.id FOR UPDATE OF trigger LIMIT $6`,
		tenantID, eventType, "event:"+eventID, cursorTime, cursorID, limit)
	if err != nil {
		return nil, triggerError("select event trigger page", err)
	}
	defer rows.Close()
	targets := make([]eventTriggerTarget, 0, limit)
	for rows.Next() {
		var target eventTriggerTarget
		if err = rows.Scan(&target.id, &target.created, &target.existing); err != nil {
			return nil, triggerError("scan event trigger page", err)
		}
		targets = append(targets, target)
	}
	if err = rows.Err(); err != nil {
		return nil, triggerError("read event trigger page", err)
	}
	return targets, nil
}
