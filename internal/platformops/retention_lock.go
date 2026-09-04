package platformops

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

const tenantRetentionLockSQL = `SELECT pg_advisory_xact_lock(hashtextextended('supaflow:retention:' || $1, 0))`

// lockTenantRetention serializes policy changes with every output deadline
// calculation so a concurrent writer cannot retain data beyond a new maximum.
func lockTenantRetention(ctx context.Context, tx pgx.Tx, tenantID string) error {
	if _, err := tx.Exec(ctx, tenantRetentionLockSQL, tenantID); err != nil {
		return fmt.Errorf("lock tenant retention policy: %w", err)
	}
	return nil
}
