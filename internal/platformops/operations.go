package platformops

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ReadinessCheck is one named dependency readiness probe.
type ReadinessCheck struct {
	Name  string
	Check func(context.Context) error
}

// Readiness runs bounded dependency checks and reports safe names only.
type Readiness struct{ checks []ReadinessCheck }

// NewReadiness validates and installs dependency probes.
func NewReadiness(checks ...ReadinessCheck) (*Readiness, error) {
	if len(checks) == 0 {
		return nil, ErrInvalidArgument
	}
	for _, check := range checks {
		if check.Name == "" || check.Check == nil {
			return nil, ErrInvalidArgument
		}
	}
	return &Readiness{checks: append([]ReadinessCheck(nil), checks...)}, nil
}

// Check returns a map of failed component names and a joined diagnostic error.
func (r *Readiness) Check(ctx context.Context) (map[string]string, error) {
	failures := make(map[string]string)
	joined := make([]error, 0)
	for _, check := range r.checks {
		if err := check.Check(ctx); err != nil {
			failures[check.Name] = "unavailable"
			joined = append(joined, fmt.Errorf("%s unavailable: %w", check.Name, err))
		}
	}
	return failures, errors.Join(joined...)
}

// DrainController rejects new work and waits for tracked work to finish.
type DrainController struct {
	mu        sync.Mutex
	accepting bool
	active    sync.WaitGroup
}

// NewDrainController creates an accepting controller.
func NewDrainController() *DrainController { return &DrainController{accepting: true} }

// Begin registers in-flight work and returns a mandatory completion callback.
func (d *DrainController) Begin() (func(), bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.accepting {
		return nil, false
	}
	d.active.Add(1)
	var once sync.Once
	return func() { once.Do(d.active.Done) }, true
}

// Drain rejects new work and waits until completion or context deadline.
func (d *DrainController) Drain(ctx context.Context) error {
	d.mu.Lock()
	d.accepting = false
	d.mu.Unlock()
	done := make(chan struct{})
	go func() {
		d.active.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// WorkerRegistry persists readiness/drain state shared by worker replicas.
type WorkerRegistry struct{ pool *pgxpool.Pool }

// NewWorkerRegistry creates a durable worker registry.
func NewWorkerRegistry(pool *pgxpool.Pool) *WorkerRegistry { return &WorkerRegistry{pool: pool} }

// Heartbeat registers capacity while preserving a previously requested drain.
// An offline worker must atomically reacquire quota before it may become ready.
func (r *WorkerRegistry) Heartbeat(ctx context.Context, organizationID, workerID string, capacity, active int) error {
	if organizationID == "" || workerID == "" || capacity <= 0 || active < 0 || active > capacity {
		return ErrInvalidArgument
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin worker heartbeat: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var maxWorkers, activeWorkers int
	err = tx.QueryRow(ctx, `SELECT q.max_active_workers,c.active_workers
		FROM organization_quotas q JOIN organization_quota_counters c ON c.organization_id=q.organization_id
		WHERE q.organization_id=$1 FOR UPDATE OF q,c`, organizationID).Scan(&maxWorkers, &activeWorkers)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("lock worker quota: %w", err)
	}
	var admissionID, admissionStatus string
	err = tx.QueryRow(ctx, `SELECT id,status FROM admission_entries
		WHERE organization_id=$1 AND resource_type='worker' AND resource_id=$2 FOR UPDATE`, organizationID, workerID).
		Scan(&admissionID, &admissionStatus)
	if errors.Is(err, pgx.ErrNoRows) {
		admissionID, err = newID("adm")
		if err != nil {
			return err
		}
		admissionStatus = "released"
	} else if err != nil {
		return fmt.Errorf("lock worker admission: %w", err)
	}
	reacquired := admissionStatus != "active"
	if reacquired {
		if admissionStatus != "released" || activeWorkers >= maxWorkers {
			return &RateLimitError{RetryAfter: 5 * time.Second, Reason: "worker capacity exhausted"}
		}
		if _, err = tx.Exec(ctx, `INSERT INTO admission_entries
			(organization_id,id,resource_type,resource_id,priority,status,admitted_at)
			VALUES ($1,$2,'worker',$3,0,'active',clock_timestamp())
			ON CONFLICT (organization_id,resource_type,resource_id) DO UPDATE SET
			  status='active',admitted_at=clock_timestamp(),released_at=NULL`, organizationID, admissionID, workerID); err != nil {
			return fmt.Errorf("reacquire worker admission: %w", err)
		}
		activeWorkers++
		if _, err = tx.Exec(ctx, `UPDATE organization_quota_counters
			SET active_workers=$2,updated_at=clock_timestamp() WHERE organization_id=$1`, organizationID, activeWorkers); err != nil {
			return fmt.Errorf("update worker quota counter: %w", err)
		}
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO worker_registrations (organization_id,worker_id,status,capacity,active_attempts,heartbeat_at)
		VALUES ($1,$2,'ready',$3,$4,clock_timestamp())
		ON CONFLICT (organization_id,worker_id) DO UPDATE SET
		status=CASE WHEN worker_registrations.status='draining' AND NOT $5 THEN 'draining' ELSE 'ready' END,
		capacity=EXCLUDED.capacity,active_attempts=EXCLUDED.active_attempts,
		heartbeat_at=EXCLUDED.heartbeat_at,updated_at=clock_timestamp()`, organizationID, workerID, capacity, active, reacquired)
	if err != nil {
		return fmt.Errorf("persist worker heartbeat: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit worker heartbeat: %w", err)
	}
	return nil
}

// BeginDrain persistently prevents new claims for a worker.
func (r *WorkerRegistry) BeginDrain(ctx context.Context, organizationID, workerID string) error {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin worker drain: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	tag, err := tx.Exec(ctx, `UPDATE worker_registrations SET status='draining',updated_at=clock_timestamp()
		WHERE organization_id=$1 AND worker_id=$2`, organizationID, workerID)
	if err != nil {
		return fmt.Errorf("begin worker drain: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrNotFound
	}
	audited, err := recordAllowedMutationTx(ctx, tx, workerID)
	if err != nil {
		return fmt.Errorf("audit worker drain: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit worker drain: %w", err)
	}
	if audited {
		markMutationCommitted(ctx)
	}
	return nil
}

// MayClaim reports whether a fresh, non-draining worker may claim new work.
func (r *WorkerRegistry) MayClaim(ctx context.Context, organizationID, workerID string, maxHeartbeatAge time.Duration) (bool, error) {
	if organizationID == "" || workerID == "" || maxHeartbeatAge <= 0 {
		return false, ErrInvalidArgument
	}
	var allowed bool
	err := r.pool.QueryRow(ctx, `
		SELECT w.status='ready'
		  AND w.heartbeat_at >= clock_timestamp()-make_interval(secs => $3)
		  AND w.active_attempts < w.capacity
		  AND EXISTS (SELECT 1 FROM admission_entries a
		      WHERE a.organization_id=w.organization_id AND a.resource_type='worker'
		        AND a.resource_id=w.worker_id AND a.status='active')
		FROM worker_registrations w WHERE w.organization_id=$1 AND w.worker_id=$2`, organizationID, workerID, maxHeartbeatAge.Seconds()).Scan(&allowed)
	if err != nil {
		return false, ErrNotFound
	}
	return allowed, nil
}

// ReapStale marks crashed workers offline and atomically releases their quota reservations.
func (r *WorkerRegistry) ReapStale(ctx context.Context, organizationID string, maxHeartbeatAge time.Duration, limit int) (int, error) {
	if organizationID == "" || maxHeartbeatAge <= 0 || limit <= 0 || limit > 1000 {
		return 0, ErrInvalidArgument
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, fmt.Errorf("begin reap stale workers: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var ignored int
	if err = tx.QueryRow(ctx, `SELECT c.active_workers FROM organization_quotas q
		JOIN organization_quota_counters c ON c.organization_id=q.organization_id
		WHERE q.organization_id=$1 FOR UPDATE OF q,c`, organizationID).Scan(&ignored); errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrNotFound
	} else if err != nil {
		return 0, fmt.Errorf("lock worker quota for reap: %w", err)
	}
	var released int
	err = tx.QueryRow(ctx, `
		WITH stale AS (
		    SELECT organization_id,worker_id FROM worker_registrations
		    WHERE organization_id=$1 AND status IN ('ready','draining')
		      AND heartbeat_at < clock_timestamp()-make_interval(secs => $2)
		    ORDER BY heartbeat_at,worker_id FOR UPDATE SKIP LOCKED LIMIT $3
		), offline AS (
		    UPDATE worker_registrations w SET status='offline',updated_at=clock_timestamp()
		    FROM stale s WHERE w.organization_id=s.organization_id AND w.worker_id=s.worker_id
		    RETURNING w.worker_id
		), orphan AS (
		    SELECT a.resource_id AS worker_id FROM admission_entries a
		    WHERE a.organization_id=$1 AND a.resource_type='worker' AND a.status='active'
		      AND a.created_at < clock_timestamp()-make_interval(secs => $2)
		      AND NOT EXISTS (SELECT 1 FROM worker_registrations w
		          WHERE w.organization_id=a.organization_id AND w.worker_id=a.resource_id)
		    ORDER BY a.created_at,a.resource_id FOR UPDATE OF a SKIP LOCKED LIMIT $3
		), released AS (
		    UPDATE admission_entries a SET status='released',released_at=clock_timestamp()
		    WHERE a.organization_id=$1 AND a.resource_type='worker' AND a.status='active'
		      AND a.resource_id IN (SELECT worker_id FROM offline UNION SELECT worker_id FROM orphan)
		    RETURNING 1
		), counter AS (
		    UPDATE organization_quota_counters SET
		      active_workers=greatest(0,active_workers-(SELECT count(*) FROM released)),updated_at=clock_timestamp()
		    WHERE organization_id=$1
		)
		SELECT count(*) FROM released`, organizationID, maxHeartbeatAge.Seconds(), limit).Scan(&released)
	if err != nil {
		return 0, fmt.Errorf("reap stale workers: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit stale worker reap: %w", err)
	}
	return released, nil
}

// CapacitySnapshot contains organization-safe operational saturation signals.
type CapacitySnapshot struct {
	OrganizationID           string        `json:"-"`
	ActiveRuns               int           `json:"activeRuns"`
	QueuedRuns               int           `json:"queuedRuns"`
	ActiveWorkers            int           `json:"activeWorkers"`
	TriggerCount             int           `json:"triggerCount"`
	PendingTriggerFirings    int           `json:"pendingTriggerFirings"`
	OutboxBacklog            int           `json:"outboxBacklog"`
	MaxTriggers              int           `json:"maxTriggers"`
	MaxPendingTriggerFirings int           `json:"maxPendingTriggerFirings"`
	MaxPendingOutboxMessages int           `json:"maxPendingOutboxMessages"`
	OldestOutboxAge          time.Duration `json:"oldestOutboxAge"`
	DLQDepth                 int           `json:"dlqDepth"`
}

// CapacityMonitor reads durable queue and delivery capacity signals.
type CapacityMonitor struct{ pool *pgxpool.Pool }

// NewCapacityMonitor creates a PostgreSQL capacity monitor.
func NewCapacityMonitor(pool *pgxpool.Pool) *CapacityMonitor { return &CapacityMonitor{pool: pool} }

// Snapshot returns one organization's current durable saturation state.
func (m *CapacityMonitor) Snapshot(ctx context.Context, organizationID string, now time.Time) (CapacitySnapshot, error) {
	var snapshot CapacitySnapshot
	snapshot.OrganizationID = organizationID
	var oldest *time.Time
	err := m.pool.QueryRow(ctx, `
		SELECT c.active_runs,c.queued_runs,c.active_workers,
		       (SELECT count(*) FROM triggers t WHERE t.organization_id=c.organization_id),
		       (SELECT count(*) FROM trigger_firings f WHERE f.organization_id=c.organization_id AND f.status='pending'),
		       (SELECT count(*) FROM outbox_messages o WHERE o.organization_id=c.organization_id AND o.delivered_at IS NULL),
		       q.max_triggers,q.max_pending_trigger_firings,q.max_pending_outbox_messages,
		       (SELECT min(created_at) FROM outbox_messages o WHERE o.organization_id=c.organization_id AND o.delivered_at IS NULL),
		       (SELECT count(*) FROM outbox_dead_letters d WHERE d.organization_id=c.organization_id AND d.requeued_at IS NULL)
		FROM organization_quota_counters c JOIN organization_quotas q ON q.organization_id=c.organization_id
		WHERE c.organization_id=$1`, organizationID).Scan(&snapshot.ActiveRuns, &snapshot.QueuedRuns,
		&snapshot.ActiveWorkers, &snapshot.TriggerCount, &snapshot.PendingTriggerFirings,
		&snapshot.OutboxBacklog, &snapshot.MaxTriggers, &snapshot.MaxPendingTriggerFirings,
		&snapshot.MaxPendingOutboxMessages, &oldest, &snapshot.DLQDepth)
	if err != nil {
		return CapacitySnapshot{}, ErrNotFound
	}
	if oldest != nil && now.After(*oldest) {
		snapshot.OldestOutboxAge = now.Sub(*oldest)
	}
	return snapshot, nil
}

// Alerts returns stable alert codes for configured thresholds.
func (m *CapacityMonitor) Alerts(snapshot CapacitySnapshot, maxQueue, maxDLQ int, maxOutboxAge time.Duration) []string {
	alerts := make([]string, 0, 3)
	if snapshot.QueuedRuns >= maxQueue && maxQueue >= 0 {
		alerts = append(alerts, "run_queue_saturated")
	}
	if snapshot.DLQDepth > maxDLQ {
		alerts = append(alerts, "outbox_dlq_nonempty")
	}
	if snapshot.OldestOutboxAge > maxOutboxAge {
		alerts = append(alerts, "outbox_delivery_lag")
	}
	return alerts
}
