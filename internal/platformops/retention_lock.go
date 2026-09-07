package platformops

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

const organizationRetentionLockSQL = `SELECT pg_advisory_xact_lock(hashtextextended('supaflow:retention:' || $1, 0))`

// lockOrganizationRetention serializes policy changes with every output deadline
// calculation so a concurrent writer cannot retain data beyond a new maximum.
func lockOrganizationRetention(ctx context.Context, tx pgx.Tx, organizationID string) error {
	if _, err := tx.Exec(ctx, organizationRetentionLockSQL, organizationID); err != nil {
		return fmt.Errorf("lock organization retention policy: %w", err)
	}
	return nil
}
