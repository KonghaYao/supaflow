package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
)

const dynamicClaimScanLimit = 100

const reconcileWorkHintsSQL = `
WITH candidates AS (
    SELECT a.organization_id,
           min(CASE WHEN a.status='running' THEN l.expires_at ELSE a.available_at END) AS ready_at
    FROM node_attempts a
    JOIN runs r ON r.organization_id=a.organization_id AND r.id=a.run_id
    JOIN admission_entries admission ON admission.organization_id=r.organization_id
      AND admission.resource_type='run' AND admission.resource_id=r.id AND admission.status='active'
    LEFT JOIN worker_leases l ON l.organization_id=a.organization_id AND l.attempt_id=a.id
    WHERE (r.deadline_at IS NULL OR r.deadline_at > $1)
      AND ((r.status IN ('pending','running') AND a.status IN ('pending','retry_ready'))
        OR (r.status IN ('running','cancelling') AND a.status='running' AND l.expires_at IS NOT NULL))
    GROUP BY a.organization_id
    ORDER BY min(CASE WHEN a.status='running' THEN l.expires_at ELSE a.available_at END)
    LIMIT 100
)
SELECT organization_id, ready_at FROM candidates`

// upsertWorkHintSQL installs one organization's reconciled hint. It is the write half of
// reconcileWorkHintsSQL: the candidate scan reads without a gate, and each candidate is installed
// by its own single-organization transaction so no transaction holds two organization gates.
const upsertWorkHintSQL = `
INSERT INTO organization_work_queue(organization_id,ready_at,updated_at)
VALUES ($1,$2,$3)
ON CONFLICT (organization_id) DO UPDATE SET ready_at=EXCLUDED.ready_at,updated_at=EXCLUDED.updated_at`

// nextReadyWorkOrganizationSQL proposes one organization without taking a row lock or a gate, so
// the claim can decide which organization to serialize on before it holds anything. Ordering is the
// fair-share cursor order: least recently claimed first, then whatever is due soonest.
const nextReadyWorkOrganizationSQL = `
SELECT organization_id FROM organization_work_queue
WHERE ready_at <= clock_timestamp()
ORDER BY last_claimed_at NULLS FIRST,ready_at,organization_id
LIMIT 1`

// claimWorkOrganizationSQL re-validates the candidate inside the organization's own transaction.
// SKIP LOCKED means a candidate another worker is already claiming is skipped rather than waited
// for, which is what keeps two workers off the same organization instead of queueing them.
const claimWorkOrganizationSQL = `
SELECT true FROM organization_work_queue
WHERE organization_id = $1 AND ready_at <= clock_timestamp()
FOR UPDATE SKIP LOCKED`

// ClaimNext atomically selects a fair organization hint and claims one authoritative attempt.
// Queue eligibility, leases, and cursor use one DB clock.
//
// Every candidate gets its own transaction that takes that organization's gate first, because the
// claim writes node_attempts, runs, worker_leases, and outbox_messages and would otherwise be able
// to form a lock cycle with a run-creation or cancellation transaction for the same organization.
// Nothing is locked while the next candidate is chosen, so no transaction here ever waits for a
// second organization's gate.
func (s *RuntimeStore) ClaimNext(ctx context.Context, request workflowruntime.ClaimNextAttemptRequest) (*workflowruntime.WorkItem, error) {
	reconciled := false
	for scanned := 0; scanned < dynamicClaimScanLimit; scanned++ {
		var organizationID string
		err := s.repository.pool.QueryRow(ctx, nextReadyWorkOrganizationSQL).Scan(&organizationID)
		if errors.Is(err, pgx.ErrNoRows) {
			if reconciled {
				return nil, nil
			}
			if reconcileErr := s.reconcileWorkHints(ctx); reconcileErr != nil {
				return nil, reconcileErr
			}
			reconciled = true
			continue
		}
		if err != nil {
			return nil, runtimePersistenceError("could not select work organization", err)
		}
		work, claimErr := s.claimFromWorkOrganization(ctx, organizationID, request)
		if claimErr != nil {
			return nil, claimErr
		}
		if work == nil {
			continue
		}
		return work, nil
	}
	return nil, nil
}

// claimFromWorkOrganization claims one attempt for a single organization. It returns a nil work
// item when the candidate stopped being claimable (another worker took it, or the hint was stale),
// and the caller then moves on to the next candidate.
func (s *RuntimeStore) claimFromWorkOrganization(ctx context.Context, organizationID string, request workflowruntime.ClaimNextAttemptRequest) (*workflowruntime.WorkItem, error) {
	tx, err := beginOrganizationRuntimeTx(ctx, s.repository.pool, organizationID)
	if err != nil {
		return nil, runtimePersistenceError("could not start next attempt claim", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var now time.Time
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return nil, runtimePersistenceError("could not read database clock", err)
	}
	var ready bool
	err = tx.QueryRow(ctx, claimWorkOrganizationSQL, organizationID).Scan(&ready)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, runtimePersistenceError("could not lock work organization", err)
	}
	work, err := s.claimAttemptTx(ctx, tx, workflowruntime.ClaimAttemptRequest{
		OrganizationID: workflowruntime.OrganizationID(organizationID), WorkerID: request.WorkerID,
		LeaseDuration: request.LeaseDuration,
	}, now)
	if err != nil {
		return nil, err
	}
	if work == nil {
		// The hint named an organization with nothing claimable left; drop it so the next scan does
		// not propose it again.
		if _, err = tx.Exec(ctx, `SELECT refresh_organization_work_hint($1)`, organizationID); err != nil {
			return nil, runtimePersistenceError("could not clear stale work hint", err)
		}
		if err = tx.Commit(ctx); err != nil {
			return nil, runtimePersistenceError("could not commit stale work hint cleanup", err)
		}
		return nil, nil
	}
	if _, err = tx.Exec(ctx, `SELECT refresh_organization_work_hint($1)`, organizationID); err != nil {
		return nil, runtimePersistenceError("could not refresh work hint", err)
	}
	if _, err = tx.Exec(ctx, `UPDATE organization_work_queue SET last_claimed_at=$2,updated_at=$2 WHERE organization_id=$1`, organizationID, now); err != nil {
		return nil, runtimePersistenceError("could not advance work cursor", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, runtimePersistenceError("could not commit next attempt claim", workflowruntime.MarkCommitOutcomeUnknown(err))
	}
	return work, nil
}

// reconcileWorkHints repairs drifted hints by rescanning authoritative work and installing one hint
// per organization.
//
// The scan is read-only and ungated; each candidate is then written by its own transaction that
// takes that candidate's gate. One transaction never holds two gates, and the writes touch only
// organization_work_queue, so the repair cannot deadlock against a claim or a run creation.
func (s *RuntimeStore) reconcileWorkHints(ctx context.Context) error {
	var now time.Time
	if err := s.repository.pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return runtimePersistenceError("could not read reconciliation clock", err)
	}
	rows, err := s.repository.pool.Query(ctx, reconcileWorkHintsSQL, now)
	if err != nil {
		return runtimePersistenceError("could not scan work hints", err)
	}
	type candidate struct {
		organizationID string
		readyAt        time.Time
	}
	candidates := make([]candidate, 0, 100)
	for rows.Next() {
		var item candidate
		if err = rows.Scan(&item.organizationID, &item.readyAt); err != nil {
			rows.Close()
			return runtimePersistenceError("could not decode work hint candidate", err)
		}
		candidates = append(candidates, item)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return runtimePersistenceError("could not iterate work hint candidates", err)
	}
	for _, item := range candidates {
		if err = s.installWorkHint(ctx, item.organizationID, item.readyAt, now); err != nil {
			return err
		}
	}
	return nil
}

func (s *RuntimeStore) installWorkHint(ctx context.Context, organizationID string, readyAt, now time.Time) error {
	tx, err := beginOrganizationRuntimeTx(ctx, s.repository.pool, organizationID)
	if err != nil {
		return runtimePersistenceError("could not start work hint reconciliation", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err = tx.Exec(ctx, upsertWorkHintSQL, organizationID, readyAt, now); err != nil {
		return runtimePersistenceError("could not reconcile work hints", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return runtimePersistenceError("could not commit work hint reconciliation", err)
	}
	return nil
}
