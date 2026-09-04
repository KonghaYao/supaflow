package platformops

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// SetPolicy atomically installs a tenant retention policy and tightens all
// active output deadlines that exceed the new maximum lifetime.
func (r *Retention) SetPolicy(ctx context.Context, policy RetentionPolicy) error {
	if policy.TenantID == "" || policy.EventDays <= 0 || policy.OutputDays <= 0 {
		return ErrInvalidArgument
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin retention policy update: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err = lockTenantRetention(ctx, tx, policy.TenantID); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO retention_policies (tenant_id,event_days,output_days,archive_before_delete)
		VALUES ($1,$2,$3,$4) ON CONFLICT (tenant_id) DO UPDATE SET
		event_days=EXCLUDED.event_days,output_days=EXCLUDED.output_days,
		archive_before_delete=EXCLUDED.archive_before_delete,updated_at=now()`,
		policy.TenantID, policy.EventDays, policy.OutputDays, policy.ArchiveBeforeDelete); err != nil {
		return fmt.Errorf("persist retention policy: %w", err)
	}
	// Retention is a maximum lifetime, so policy updates may shorten existing
	// active output deadlines but never extend or resurrect retained data.
	if _, err = tx.Exec(ctx, `
		UPDATE managed_objects
		SET retention_until = LEAST(
			COALESCE(retention_until, created_at + $2 * interval '1 day'),
			created_at + $2 * interval '1 day'
		)
		WHERE tenant_id=$1 AND kind='output' AND status='active'`,
		policy.TenantID, policy.OutputDays); err != nil {
		return fmt.Errorf("tighten managed output retention: %w", err)
	}
	if _, err = tx.Exec(ctx, `
		UPDATE node_outputs
		SET retention_until = LEAST(
			COALESCE(retention_until, created_at + $2 * interval '1 day'),
			created_at + $2 * interval '1 day'
		)
		WHERE tenant_id=$1 AND retention_status='active'`,
		policy.TenantID, policy.OutputDays); err != nil {
		return fmt.Errorf("tighten node output retention: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit retention policy update: %w", err)
	}
	return nil
}

// backfillOutputRetention serializes legacy NULL deadline materialization with
// policy changes. The policy is read only after taking the same tenant lock,
// so a stale sweep can never extend data beyond a concurrently tightened rule.
func (r *Retention) backfillOutputRetention(ctx context.Context, tenantID string, limit int) (RetentionPolicy, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return RetentionPolicy{}, fmt.Errorf("begin output retention backfill: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err = lockTenantRetention(ctx, tx, tenantID); err != nil {
		return RetentionPolicy{}, err
	}
	policy := RetentionPolicy{TenantID: tenantID}
	err = tx.QueryRow(ctx, `SELECT event_days,output_days,archive_before_delete
		FROM retention_policies WHERE tenant_id=$1`, tenantID).Scan(
		&policy.EventDays, &policy.OutputDays, &policy.ArchiveBeforeDelete)
	if errors.Is(err, pgx.ErrNoRows) {
		return RetentionPolicy{}, ErrNotFound
	}
	if err != nil {
		return RetentionPolicy{}, fmt.Errorf("read retention policy: %w", err)
	}
	if _, err = tx.Exec(ctx, `
		WITH targets AS (
			SELECT tenant_id,id FROM managed_objects
			WHERE tenant_id=$1 AND kind='output' AND status='active' AND retention_until IS NULL
			ORDER BY created_at,id FOR UPDATE SKIP LOCKED LIMIT $3
		)
		UPDATE managed_objects output
		SET retention_until=LEAST(
			COALESCE(output.retention_until,output.created_at + $2 * interval '1 day'),
			output.created_at + $2 * interval '1 day')
		FROM targets WHERE output.tenant_id=targets.tenant_id AND output.id=targets.id`, tenantID, policy.OutputDays, limit); err != nil {
		return RetentionPolicy{}, fmt.Errorf("backfill output retention deadline: %w", err)
	}
	if _, err = tx.Exec(ctx, `
		WITH targets AS (
			SELECT tenant_id,id FROM node_outputs
			WHERE tenant_id=$1 AND retention_status='active' AND retention_until IS NULL
			ORDER BY created_at,id FOR UPDATE SKIP LOCKED LIMIT $3
		)
		UPDATE node_outputs output
		SET retention_until=LEAST(
			COALESCE(output.retention_until,output.created_at + $2 * interval '1 day'),
			output.created_at + $2 * interval '1 day')
		FROM targets WHERE output.tenant_id=targets.tenant_id AND output.id=targets.id`, tenantID, policy.OutputDays, limit); err != nil {
		return RetentionPolicy{}, fmt.Errorf("backfill inline output retention deadline: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return RetentionPolicy{}, fmt.Errorf("commit output retention backfill: %w", err)
	}
	return policy, nil
}
