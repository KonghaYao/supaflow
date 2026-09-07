package platformops

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

func organizationTriggerIngressCapacityTx(ctx context.Context, tx pgx.Tx, organizationID string) (int, error) {
	var available int
	err := tx.QueryRow(ctx, `
		SELECT GREATEST(0, LEAST(
		    quota.max_pending_trigger_firings-(
		        SELECT count(*) FROM trigger_firings firing
		        WHERE firing.organization_id=quota.organization_id AND firing.status='pending'
		    ),
		    quota.max_pending_outbox_messages-(
		        SELECT count(*) FROM outbox_messages message
		        WHERE message.organization_id=quota.organization_id AND message.delivered_at IS NULL
		    )
		))::integer
		FROM organization_quotas quota WHERE quota.organization_id=$1 FOR UPDATE OF quota`, organizationID).Scan(&available)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, triggerError("read organization trigger ingress capacity", err)
	}
	return available, nil
}

func triggerCapacityError() error {
	return &RateLimitError{RetryAfter: 5 * time.Second, Reason: "trigger ingress capacity exhausted"}
}
