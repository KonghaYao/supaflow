package platformops

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// sweepEventsAndBackfill holds the tenant retention lock from policy read
// through the irreversible event operation. A concurrently relaxed policy
// therefore either commits before this sweep reads it or waits until the old
// policy has been completely enforced.
func (r *Retention) sweepEventsAndBackfill(ctx context.Context, tenantID string, limit int) (int, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, fmt.Errorf("begin retention sweep: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err = lockTenantRetention(ctx, tx, tenantID); err != nil {
		return 0, err
	}
	var now time.Time
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return 0, fmt.Errorf("read database clock for retention sweep: %w", err)
	}
	policy, err := r.backfillOutputRetentionTx(ctx, tx, tenantID, limit)
	if err != nil {
		return 0, err
	}
	before := now.AddDate(0, 0, -policy.EventDays)
	removed := 0
	if policy.ArchiveBeforeDelete {
		// ArchiveEvents owns the archive/object transaction. The outer transaction
		// intentionally stays open only to retain the tenant advisory lock until
		// the archive and event deletion have durably committed.
		if _, removed, err = r.ArchiveEvents(ctx, tenantID, "system:retention", before, limit); err != nil {
			return removed, err
		}
	} else {
		tag, deleteErr := tx.Exec(ctx, `DELETE FROM run_events WHERE (tenant_id,id) IN (
			SELECT e.tenant_id,e.id FROM run_events e JOIN runs run ON run.tenant_id=e.tenant_id AND run.id=e.run_id
			WHERE e.tenant_id=$1 AND e.created_at < $2 AND run.status IN ('success','failed','error','cancelled')
			ORDER BY e.created_at,e.id LIMIT $3)`, tenantID, before, limit)
		if deleteErr != nil {
			return 0, fmt.Errorf("delete expired events: %w", deleteErr)
		}
		removed = int(tag.RowsAffected())
	}
	if err = tx.Commit(ctx); err != nil {
		return removed, fmt.Errorf("commit retention sweep: %w", err)
	}
	return removed, nil
}
