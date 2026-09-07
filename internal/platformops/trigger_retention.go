package platformops

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

const triggerIdempotencyRetentionDays = 7

// TriggerRetentionResult reports one bounded organization history purge.
type TriggerRetentionResult struct {
	DeliveredOutbox  int
	CompletedFirings int
}

// PurgeCompletedTriggerHistory removes one organization's terminal delivery history
// only after the documented firing-idempotency window has elapsed.
func (s *PostgresTriggerStore) PurgeCompletedTriggerHistory(ctx context.Context, organizationID string, limit int) (TriggerRetentionResult, error) {
	if organizationID == "" || limit <= 0 || limit > 10000 {
		return TriggerRetentionResult{}, ErrInvalidArgument
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return TriggerRetentionResult{}, fmt.Errorf("begin trigger history purge: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	result := TriggerRetentionResult{}
	outboxTag, err := tx.Exec(ctx, `
		WITH expired AS (
		    SELECT organization_id,id FROM outbox_messages
		    WHERE organization_id=$1 AND delivered_at IS NOT NULL
		      AND delivered_at < clock_timestamp()-make_interval(days => $2)
		    ORDER BY delivered_at,id FOR UPDATE SKIP LOCKED LIMIT $3
		)
		DELETE FROM outbox_messages message USING expired
		WHERE message.organization_id=expired.organization_id AND message.id=expired.id`,
		organizationID, triggerIdempotencyRetentionDays, limit)
	if err != nil {
		return TriggerRetentionResult{}, fmt.Errorf("purge delivered outbox: %w", err)
	}
	result.DeliveredOutbox = int(outboxTag.RowsAffected())
	firingTag, err := tx.Exec(ctx, `
		WITH expired AS (
		    SELECT firing.organization_id,firing.id FROM trigger_firings firing
		    WHERE firing.organization_id=$1 AND firing.status IN ('dispatched','failed')
		      AND firing.created_at < clock_timestamp()-make_interval(days => $2)
		      AND NOT EXISTS (
		          SELECT 1 FROM outbox_messages message
		          WHERE message.organization_id=firing.organization_id AND message.delivered_at IS NULL
		            AND message.topic='trigger.fired' AND message.payload->>'firingId'=firing.id
		      )
		    ORDER BY firing.created_at,firing.id FOR UPDATE SKIP LOCKED LIMIT $3
		)
		DELETE FROM trigger_firings firing USING expired
		WHERE firing.organization_id=expired.organization_id AND firing.id=expired.id`,
		organizationID, triggerIdempotencyRetentionDays, limit)
	if err != nil {
		return TriggerRetentionResult{}, fmt.Errorf("purge completed trigger firings: %w", err)
	}
	result.CompletedFirings = int(firingTag.RowsAffected())
	if err = tx.Commit(ctx); err != nil {
		return TriggerRetentionResult{}, fmt.Errorf("commit trigger history purge: %w", err)
	}
	return result, nil
}
