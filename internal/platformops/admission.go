package platformops

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Quota defines durable per-tenant execution and trigger-ingress bounds.
type Quota struct {
	TenantID                 string `json:"-"`
	MaxActiveRuns            int    `json:"maxActiveRuns"`
	MaxActiveWorkers         int    `json:"maxActiveWorkers"`
	MaxQueueDepth            int    `json:"maxQueueDepth"`
	MaxTriggers              int    `json:"maxTriggers"`
	MaxPendingTriggerFirings int    `json:"maxPendingTriggerFirings"`
	MaxPendingOutboxMessages int    `json:"maxPendingOutboxMessages"`
}

const (
	defaultMaxTriggers              = 1000
	defaultMaxPendingTriggerFirings = 5000
	defaultMaxPendingOutboxMessages = 20000
)

// AdmissionDecision describes whether work may start or remains durably queued.
type AdmissionDecision struct {
	EntryID    string        `json:"entryId"`
	Admitted   bool          `json:"admitted"`
	Queued     bool          `json:"queued"`
	Priority   int           `json:"priority"`
	RetryAfter time.Duration `json:"retryAfter,omitempty"`
}

// ReleaseDecision identifies a queued run promoted by released capacity.
type ReleaseDecision struct {
	PromotedResourceID string `json:"promotedResourceId,omitempty"`
}

// Admission enforces tenant quotas using PostgreSQL row locks and counters.
type Admission struct {
	pool       *pgxpool.Pool
	retryAfter time.Duration
}

// NewAdmission creates a database-coordinated admission controller.
func NewAdmission(pool *pgxpool.Pool, retryAfter time.Duration) (*Admission, error) {
	if pool == nil || retryAfter <= 0 {
		return nil, ErrInvalidArgument
	}
	return &Admission{pool: pool, retryAfter: retryAfter}, nil
}

// ConfigureQuota atomically installs limits and initializes counters.
func (a *Admission) ConfigureQuota(ctx context.Context, quota Quota) error {
	quota = normalizeTriggerQuota(quota)
	if !validQuota(quota) {
		return ErrInvalidArgument
	}
	tx, err := a.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin quota configuration: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err = tx.Exec(ctx, `
		INSERT INTO tenant_quotas (
		    tenant_id,max_active_runs,max_active_workers,max_queue_depth,
		    max_triggers,max_pending_trigger_firings,max_pending_outbox_messages
		) VALUES ($1,$2,$3,$4,$5,$6,$7)
		ON CONFLICT (tenant_id) DO UPDATE SET
		max_active_runs=EXCLUDED.max_active_runs,max_active_workers=EXCLUDED.max_active_workers,
		max_queue_depth=EXCLUDED.max_queue_depth,max_triggers=EXCLUDED.max_triggers,
		max_pending_trigger_firings=EXCLUDED.max_pending_trigger_firings,
		max_pending_outbox_messages=EXCLUDED.max_pending_outbox_messages,updated_at=clock_timestamp()`,
		quota.TenantID, quota.MaxActiveRuns, quota.MaxActiveWorkers, quota.MaxQueueDepth,
		quota.MaxTriggers, quota.MaxPendingTriggerFirings, quota.MaxPendingOutboxMessages); err != nil {
		return fmt.Errorf("persist tenant quota: %w", err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO tenant_quota_counters (tenant_id) VALUES ($1) ON CONFLICT DO NOTHING`, quota.TenantID); err != nil {
		return fmt.Errorf("initialize quota counters: %w", err)
	}
	audited, err := recordAllowedMutationTx(ctx, tx, "")
	if err != nil {
		return fmt.Errorf("audit quota configuration: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit quota configuration: %w", err)
	}
	if audited {
		markMutationCommitted(ctx)
	}
	return nil
}

// EnsureQuota installs bootstrap limits without overwriting an administrator's policy.
func (a *Admission) EnsureQuota(ctx context.Context, quota Quota) error {
	quota = normalizeTriggerQuota(quota)
	if !validQuota(quota) {
		return ErrInvalidArgument
	}
	tx, err := a.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin quota bootstrap: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err = tx.Exec(ctx, `INSERT INTO tenant_quotas (
		tenant_id,max_active_runs,max_active_workers,max_queue_depth,
		max_triggers,max_pending_trigger_firings,max_pending_outbox_messages
	) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING`, quota.TenantID,
		quota.MaxActiveRuns, quota.MaxActiveWorkers, quota.MaxQueueDepth, quota.MaxTriggers,
		quota.MaxPendingTriggerFirings, quota.MaxPendingOutboxMessages); err != nil {
		return fmt.Errorf("bootstrap tenant quota: %w", err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO tenant_quota_counters (tenant_id) VALUES ($1) ON CONFLICT DO NOTHING`, quota.TenantID); err != nil {
		return fmt.Errorf("bootstrap quota counters: %w", err)
	}
	return tx.Commit(ctx)
}

func normalizeTriggerQuota(quota Quota) Quota {
	if quota.MaxTriggers == 0 {
		quota.MaxTriggers = defaultMaxTriggers
	}
	if quota.MaxPendingTriggerFirings == 0 {
		quota.MaxPendingTriggerFirings = defaultMaxPendingTriggerFirings
	}
	if quota.MaxPendingOutboxMessages == 0 {
		quota.MaxPendingOutboxMessages = defaultMaxPendingOutboxMessages
	}
	return quota
}

func validQuota(quota Quota) bool {
	return quota.TenantID != "" && quota.MaxActiveRuns > 0 && quota.MaxActiveWorkers > 0 &&
		quota.MaxQueueDepth >= 0 && quota.MaxTriggers > 0 && quota.MaxTriggers <= 10000 &&
		quota.MaxPendingTriggerFirings > 0 && quota.MaxPendingTriggerFirings <= 100000 &&
		quota.MaxPendingOutboxMessages > 0 && quota.MaxPendingOutboxMessages <= 500000
}

// AdmitRun starts a run when capacity exists or durably queues it by priority.
func (a *Admission) AdmitRun(ctx context.Context, tenantID, runID string, priority int) (AdmissionDecision, error) {
	return a.admit(ctx, tenantID, "run", runID, priority, true)
}

// AdmitWorker reserves one tenant worker slot or returns a retryable limit error.
func (a *Admission) AdmitWorker(ctx context.Context, tenantID, workerID string) (AdmissionDecision, error) {
	return a.admit(ctx, tenantID, "worker", workerID, 0, false)
}

// ReleaseRun releases active/queued capacity and promotes the highest-priority queued run.
func (a *Admission) ReleaseRun(ctx context.Context, tenantID, runID string) (ReleaseDecision, error) {
	return a.release(ctx, tenantID, "run", runID)
}

// ReleaseWorker releases one active worker slot.
func (a *Admission) ReleaseWorker(ctx context.Context, tenantID, workerID string) error {
	_, err := a.release(ctx, tenantID, "worker", workerID)
	return err
}

func (a *Admission) admit(ctx context.Context, tenantID, resourceType, resourceID string, priority int, allowQueue bool) (AdmissionDecision, error) {
	if tenantID == "" || resourceID == "" || priority < -1000 || priority > 1000 {
		return AdmissionDecision{}, ErrInvalidArgument
	}
	tx, err := a.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return AdmissionDecision{}, fmt.Errorf("begin admission: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var quota Quota
	var activeRuns, activeWorkers, queuedRuns int
	err = tx.QueryRow(ctx, `
		SELECT q.max_active_runs,q.max_active_workers,q.max_queue_depth,
		       c.active_runs,c.active_workers,c.queued_runs
		FROM tenant_quotas q JOIN tenant_quota_counters c ON c.tenant_id=q.tenant_id
		WHERE q.tenant_id=$1 FOR UPDATE OF q,c`, tenantID).Scan(&quota.MaxActiveRuns,
		&quota.MaxActiveWorkers, &quota.MaxQueueDepth, &activeRuns, &activeWorkers, &queuedRuns)
	if errors.Is(err, pgx.ErrNoRows) {
		return AdmissionDecision{}, ErrNotFound
	}
	if err != nil {
		return AdmissionDecision{}, fmt.Errorf("lock tenant quota: %w", err)
	}
	var existing AdmissionDecision
	var status string
	err = tx.QueryRow(ctx, `SELECT id,status,priority FROM admission_entries WHERE tenant_id=$1 AND resource_type=$2 AND resource_id=$3`, tenantID, resourceType, resourceID).Scan(&existing.EntryID, &status, &existing.Priority)
	if err == nil {
		if resourceType == "worker" && status == "released" {
			if activeWorkers >= quota.MaxActiveWorkers {
				return AdmissionDecision{}, &RateLimitError{RetryAfter: a.retryAfter, Reason: "worker capacity exhausted"}
			}
			if _, err = tx.Exec(ctx, `UPDATE admission_entries SET status='active',admitted_at=clock_timestamp(),released_at=NULL
				WHERE tenant_id=$1 AND id=$2 AND status='released'`, tenantID, existing.EntryID); err != nil {
				return AdmissionDecision{}, fmt.Errorf("reactivate worker admission: %w", err)
			}
			activeWorkers++
			if _, err = tx.Exec(ctx, `UPDATE tenant_quota_counters SET active_workers=$2,updated_at=clock_timestamp()
				WHERE tenant_id=$1`, tenantID, activeWorkers); err != nil {
				return AdmissionDecision{}, fmt.Errorf("update reactivated worker counter: %w", err)
			}
			existing.Admitted = true
			if err = tx.Commit(ctx); err != nil {
				return AdmissionDecision{}, fmt.Errorf("commit worker readmission: %w", err)
			}
			return existing, nil
		}
		existing.Admitted, existing.Queued = status == "active", status == "queued"
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return AdmissionDecision{}, fmt.Errorf("commit admission replay: %w", commitErr)
		}
		return existing, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return AdmissionDecision{}, fmt.Errorf("inspect admission replay: %w", err)
	}
	canAdmit := (resourceType == "run" && activeRuns < quota.MaxActiveRuns) ||
		(resourceType == "worker" && activeWorkers < quota.MaxActiveWorkers)
	if !canAdmit && (!allowQueue || queuedRuns >= quota.MaxQueueDepth) {
		return AdmissionDecision{}, &RateLimitError{RetryAfter: a.retryAfter, Reason: resourceType + " capacity exhausted"}
	}
	id, err := newID("adm")
	if err != nil {
		return AdmissionDecision{}, err
	}
	decision := AdmissionDecision{EntryID: id, Admitted: canAdmit, Queued: !canAdmit, Priority: priority}
	status = "active"
	if !canAdmit {
		status = "queued"
		decision.RetryAfter = a.retryAfter
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO admission_entries (tenant_id,id,resource_type,resource_id,priority,status,admitted_at)
		VALUES ($1,$2,$3,$4,$5,$6,CASE WHEN $6='active' THEN now() ELSE NULL END)`,
		tenantID, id, resourceType, resourceID, priority, status); err != nil {
		return AdmissionDecision{}, fmt.Errorf("persist admission: %w", err)
	}
	if resourceType == "run" {
		if canAdmit {
			activeRuns++
		} else {
			queuedRuns++
		}
	} else {
		activeWorkers++
	}
	if _, err = tx.Exec(ctx, `
		UPDATE tenant_quota_counters SET active_runs=$2,active_workers=$3,queued_runs=$4,updated_at=now()
		WHERE tenant_id=$1`, tenantID, activeRuns, activeWorkers, queuedRuns); err != nil {
		return AdmissionDecision{}, fmt.Errorf("update quota counters: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return AdmissionDecision{}, fmt.Errorf("commit admission: %w", err)
	}
	return decision, nil
}

func (a *Admission) release(ctx context.Context, tenantID, resourceType, resourceID string) (ReleaseDecision, error) {
	if tenantID == "" || resourceID == "" {
		return ReleaseDecision{}, ErrInvalidArgument
	}
	tx, err := a.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ReleaseDecision{}, fmt.Errorf("begin admission release: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var maxActiveRuns, activeRuns, activeWorkers, queuedRuns int
	if err = tx.QueryRow(ctx, `
		SELECT q.max_active_runs,c.active_runs,c.active_workers,c.queued_runs
		FROM tenant_quotas q JOIN tenant_quota_counters c ON c.tenant_id=q.tenant_id
		WHERE q.tenant_id=$1 FOR UPDATE OF q,c`, tenantID).
		Scan(&maxActiveRuns, &activeRuns, &activeWorkers, &queuedRuns); errors.Is(err, pgx.ErrNoRows) {
		return ReleaseDecision{}, ErrNotFound
	} else if err != nil {
		return ReleaseDecision{}, fmt.Errorf("lock quota counters: %w", err)
	}
	var entryID, status string
	err = tx.QueryRow(ctx, `SELECT id,status FROM admission_entries WHERE tenant_id=$1 AND resource_type=$2 AND resource_id=$3 FOR UPDATE`, tenantID, resourceType, resourceID).Scan(&entryID, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return ReleaseDecision{}, ErrNotFound
	}
	if err != nil {
		return ReleaseDecision{}, fmt.Errorf("lock admission entry: %w", err)
	}
	if status == "released" {
		if err = tx.Commit(ctx); err != nil {
			return ReleaseDecision{}, fmt.Errorf("commit release replay: %w", err)
		}
		return ReleaseDecision{}, nil
	}
	if _, err = tx.Exec(ctx, `UPDATE admission_entries SET status='released',released_at=now() WHERE tenant_id=$1 AND id=$2`, tenantID, entryID); err != nil {
		return ReleaseDecision{}, fmt.Errorf("release admission entry: %w", err)
	}
	if resourceType == "worker" {
		activeWorkers--
	} else if status == "queued" {
		queuedRuns--
	} else {
		activeRuns--
	}
	decision := ReleaseDecision{}
	if resourceType == "run" && status == "active" && queuedRuns > 0 && activeRuns < maxActiveRuns {
		var promotedID string
		err = tx.QueryRow(ctx, `
			UPDATE admission_entries SET status='active',admitted_at=now()
			WHERE (tenant_id,id) = (
			    SELECT tenant_id,id FROM admission_entries
			    WHERE tenant_id=$1 AND resource_type='run' AND status='queued'
			    ORDER BY priority DESC,created_at,id FOR UPDATE SKIP LOCKED LIMIT 1
			) RETURNING resource_id,id`, tenantID).Scan(&decision.PromotedResourceID, &promotedID)
		if err == nil {
			queuedRuns--
			activeRuns++
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return ReleaseDecision{}, fmt.Errorf("promote queued run: %w", err)
		}
	}
	if activeRuns < 0 || activeWorkers < 0 || queuedRuns < 0 {
		return ReleaseDecision{}, ErrConflict
	}
	if _, err = tx.Exec(ctx, `UPDATE tenant_quota_counters SET active_runs=$2,active_workers=$3,queued_runs=$4,updated_at=now() WHERE tenant_id=$1`, tenantID, activeRuns, activeWorkers, queuedRuns); err != nil {
		return ReleaseDecision{}, fmt.Errorf("persist quota release: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return ReleaseDecision{}, fmt.Errorf("commit admission release: %w", err)
	}
	return decision, nil
}
