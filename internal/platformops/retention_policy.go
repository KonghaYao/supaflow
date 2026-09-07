package platformops

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// SetPolicy atomically installs a organization retention policy and tightens all
// active output deadlines that exceed the new maximum lifetime.
func (r *Retention) SetPolicy(ctx context.Context, policy RetentionPolicy) error {
	if policy.OrganizationID == "" || policy.EventDays <= 0 || policy.OutputDays <= 0 {
		return ErrInvalidArgument
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin retention policy update: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err = lockOrganizationRetention(ctx, tx, policy.OrganizationID); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO retention_policies (organization_id,event_days,output_days,archive_before_delete)
		VALUES ($1,$2,$3,$4) ON CONFLICT (organization_id) DO UPDATE SET
		event_days=EXCLUDED.event_days,output_days=EXCLUDED.output_days,
		archive_before_delete=EXCLUDED.archive_before_delete,updated_at=now()`,
		policy.OrganizationID, policy.EventDays, policy.OutputDays, policy.ArchiveBeforeDelete); err != nil {
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
		WHERE organization_id=$1 AND kind='output' AND status='active'`,
		policy.OrganizationID, policy.OutputDays); err != nil {
		return fmt.Errorf("tighten managed output retention: %w", err)
	}
	if _, err = tx.Exec(ctx, `
		UPDATE node_outputs
		SET retention_until = LEAST(
			COALESCE(retention_until, created_at + $2 * interval '1 day'),
			created_at + $2 * interval '1 day'
		)
		WHERE organization_id=$1 AND retention_status='active'`,
		policy.OrganizationID, policy.OutputDays); err != nil {
		return fmt.Errorf("tighten node output retention: %w", err)
	}
	audited, err := recordAllowedMutationTx(ctx, tx, "")
	if err != nil {
		return fmt.Errorf("audit retention policy update: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit retention policy update: %w", err)
	}
	if audited {
		markMutationCommitted(ctx)
	}
	return nil
}

// backfillOutputRetention serializes legacy NULL deadline materialization with
// policy changes. The policy is read only after taking the same organization lock,
// so a stale sweep can never extend data beyond a concurrently tightened rule.
func (r *Retention) backfillOutputRetention(ctx context.Context, organizationID string, limit int) (RetentionPolicy, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return RetentionPolicy{}, fmt.Errorf("begin output retention backfill: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err = lockOrganizationRetention(ctx, tx, organizationID); err != nil {
		return RetentionPolicy{}, err
	}
	policy, err := r.backfillOutputRetentionTx(ctx, tx, organizationID, limit)
	if err != nil {
		return RetentionPolicy{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return RetentionPolicy{}, fmt.Errorf("commit output retention backfill: %w", err)
	}
	return policy, nil
}

// backfillOutputRetentionTx reads the policy and materializes legacy output
// deadlines inside a caller-owned transaction that already holds the organization
// retention lock.
func (r *Retention) backfillOutputRetentionTx(ctx context.Context, tx pgx.Tx, organizationID string, limit int) (RetentionPolicy, error) {
	policy := RetentionPolicy{OrganizationID: organizationID}
	err := tx.QueryRow(ctx, `SELECT event_days,output_days,archive_before_delete
		FROM retention_policies WHERE organization_id=$1`, organizationID).Scan(
		&policy.EventDays, &policy.OutputDays, &policy.ArchiveBeforeDelete)
	if errors.Is(err, pgx.ErrNoRows) {
		return RetentionPolicy{}, ErrNotFound
	}
	if err != nil {
		return RetentionPolicy{}, fmt.Errorf("read retention policy: %w", err)
	}
	if _, err = tx.Exec(ctx, `
		WITH targets AS (
			SELECT organization_id,id FROM managed_objects
			WHERE organization_id=$1 AND kind='output' AND status='active' AND retention_until IS NULL
			ORDER BY created_at,id FOR UPDATE SKIP LOCKED LIMIT $3
		)
		UPDATE managed_objects output
		SET retention_until=LEAST(
			COALESCE(output.retention_until,output.created_at + $2 * interval '1 day'),
			output.created_at + $2 * interval '1 day')
		FROM targets WHERE output.organization_id=targets.organization_id AND output.id=targets.id`, organizationID, policy.OutputDays, limit); err != nil {
		return RetentionPolicy{}, fmt.Errorf("backfill output retention deadline: %w", err)
	}
	if _, err = tx.Exec(ctx, `
		WITH targets AS (
			SELECT organization_id,id FROM node_outputs
			WHERE organization_id=$1 AND retention_status='active' AND retention_until IS NULL
			ORDER BY created_at,id FOR UPDATE SKIP LOCKED LIMIT $3
		)
		UPDATE node_outputs output
		SET retention_until=LEAST(
			COALESCE(output.retention_until,output.created_at + $2 * interval '1 day'),
			output.created_at + $2 * interval '1 day')
		FROM targets WHERE output.organization_id=targets.organization_id AND output.id=targets.id`, organizationID, policy.OutputDays, limit); err != nil {
		return RetentionPolicy{}, fmt.Errorf("backfill inline output retention deadline: %w", err)
	}
	return policy, nil
}
