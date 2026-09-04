package platformops

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

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
func (r *WorkerRegistry) Heartbeat(ctx context.Context, tenantID, workerID string, capacity, active int, now time.Time) error {
	if tenantID == "" || workerID == "" || capacity <= 0 || active < 0 || active > capacity || now.IsZero() {
		return ErrInvalidArgument
	}
	_, err := r.pool.Exec(ctx, `
		INSERT INTO worker_registrations (tenant_id,worker_id,status,capacity,active_attempts,heartbeat_at)
		VALUES ($1,$2,'ready',$3,$4,$5)
		ON CONFLICT (tenant_id,worker_id) DO UPDATE SET
		status=CASE WHEN worker_registrations.status='draining' THEN 'draining' ELSE 'ready' END,
		capacity=EXCLUDED.capacity,active_attempts=EXCLUDED.active_attempts,
		heartbeat_at=EXCLUDED.heartbeat_at,updated_at=now()`, tenantID, workerID, capacity, active, now)
	if err != nil {
		return fmt.Errorf("persist worker heartbeat: %w", err)
	}
	return nil
}

// BeginDrain persistently prevents new claims for a worker.
func (r *WorkerRegistry) BeginDrain(ctx context.Context, tenantID, workerID string, now time.Time) error {
	tag, err := r.pool.Exec(ctx, `UPDATE worker_registrations SET status='draining',updated_at=$3 WHERE tenant_id=$1 AND worker_id=$2`, tenantID, workerID, now)
	if err != nil {
		return fmt.Errorf("begin worker drain: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrNotFound
	}
	return nil
}

// MayClaim reports whether a fresh, non-draining worker may claim new work.
func (r *WorkerRegistry) MayClaim(ctx context.Context, tenantID, workerID string, freshAfter time.Time) (bool, error) {
	var allowed bool
	err := r.pool.QueryRow(ctx, `
		SELECT status='ready' AND heartbeat_at >= $3 AND active_attempts < capacity
		FROM worker_registrations WHERE tenant_id=$1 AND worker_id=$2`, tenantID, workerID, freshAfter).Scan(&allowed)
	if err != nil {
		return false, ErrNotFound
	}
	return allowed, nil
}

// ReapStale marks crashed workers offline and atomically releases their quota reservations.
func (r *WorkerRegistry) ReapStale(ctx context.Context, tenantID string, staleBefore time.Time, limit int) (int, error) {
	if tenantID == "" || staleBefore.IsZero() || limit <= 0 || limit > 1000 {
		return 0, ErrInvalidArgument
	}
	var released int
	err := r.pool.QueryRow(ctx, `
		WITH stale AS (
		    SELECT tenant_id,worker_id FROM worker_registrations
		    WHERE tenant_id=$1 AND status='ready' AND heartbeat_at < $2
		    ORDER BY heartbeat_at,worker_id FOR UPDATE SKIP LOCKED LIMIT $3
		), offline AS (
		    UPDATE worker_registrations w SET status='offline',updated_at=now()
		    FROM stale s WHERE w.tenant_id=s.tenant_id AND w.worker_id=s.worker_id
		    RETURNING w.worker_id
		), released AS (
		    UPDATE admission_entries a SET status='released',released_at=now()
		    WHERE a.tenant_id=$1 AND a.resource_type='worker' AND a.status='active'
		      AND a.resource_id IN (SELECT worker_id FROM offline)
		    RETURNING 1
		), counter AS (
		    UPDATE tenant_quota_counters SET
		      active_workers=greatest(0,active_workers-(SELECT count(*) FROM released)),updated_at=now()
		    WHERE tenant_id=$1
		)
		SELECT count(*) FROM released`, tenantID, staleBefore, limit).Scan(&released)
	if err != nil {
		return 0, fmt.Errorf("reap stale workers: %w", err)
	}
	return released, nil
}

// CapacitySnapshot contains tenant-safe operational saturation signals.
type CapacitySnapshot struct {
	TenantID        string        `json:"-"`
	ActiveRuns      int           `json:"activeRuns"`
	QueuedRuns      int           `json:"queuedRuns"`
	ActiveWorkers   int           `json:"activeWorkers"`
	OutboxBacklog   int           `json:"outboxBacklog"`
	OldestOutboxAge time.Duration `json:"oldestOutboxAge"`
	DLQDepth        int           `json:"dlqDepth"`
}

// CapacityMonitor reads durable queue and delivery capacity signals.
type CapacityMonitor struct{ pool *pgxpool.Pool }

// NewCapacityMonitor creates a PostgreSQL capacity monitor.
func NewCapacityMonitor(pool *pgxpool.Pool) *CapacityMonitor { return &CapacityMonitor{pool: pool} }

// Snapshot returns one tenant's current durable saturation state.
func (m *CapacityMonitor) Snapshot(ctx context.Context, tenantID string, now time.Time) (CapacitySnapshot, error) {
	var snapshot CapacitySnapshot
	snapshot.TenantID = tenantID
	var oldest *time.Time
	err := m.pool.QueryRow(ctx, `
		SELECT c.active_runs,c.queued_runs,c.active_workers,
		       (SELECT count(*) FROM outbox_messages o WHERE o.tenant_id=c.tenant_id AND o.delivered_at IS NULL),
		       (SELECT min(created_at) FROM outbox_messages o WHERE o.tenant_id=c.tenant_id AND o.delivered_at IS NULL),
		       (SELECT count(*) FROM outbox_dead_letters d WHERE d.tenant_id=c.tenant_id AND d.requeued_at IS NULL)
		FROM tenant_quota_counters c WHERE c.tenant_id=$1`, tenantID).Scan(&snapshot.ActiveRuns,
		&snapshot.QueuedRuns, &snapshot.ActiveWorkers, &snapshot.OutboxBacklog, &oldest, &snapshot.DLQDepth)
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

// CommandRunner is the process boundary for backup/restore drills.
type CommandRunner interface {
	Run(context.Context, string, []string, []string) error
}

// ExecCommandRunner executes PostgreSQL tools with bounded redacted diagnostics.
type ExecCommandRunner struct{}

// Run executes one fixed tool command supplied by BackupRestore.
func (ExecCommandRunner) Run(ctx context.Context, name string, args, environment []string) error {
	command := exec.CommandContext(ctx, name, args...)
	command.Env = append(os.Environ(), environment...)
	var stderr bytes.Buffer
	command.Stderr = &limitedWriter{writer: &stderr, remaining: 4096}
	if err := command.Run(); err != nil {
		return fmt.Errorf("%s failed: %s: %w", filepath.Base(name), RedactDiagnostic(stderr.String()), err)
	}
	return nil
}

// BackupRestore safely constructs executable PostgreSQL backup/restore drills.
type BackupRestore struct {
	runner CommandRunner
	root   string
}

// NewBackupRestore restricts backup artifacts below root.
func NewBackupRestore(root string, runner CommandRunner) (*BackupRestore, error) {
	absolute, err := filepath.Abs(root)
	if err != nil || root == "" || runner == nil {
		return nil, ErrInvalidArgument
	}
	if err = os.MkdirAll(absolute, 0o700); err != nil {
		return nil, fmt.Errorf("create backup root: %w", err)
	}
	return &BackupRestore{runner: runner, root: absolute}, nil
}

// Backup runs pg_dump in custom format with the connection secret supplied only via environment.
func (b *BackupRestore) Backup(ctx context.Context, databaseURL, name string) (string, error) {
	path, err := b.artifactPath(name)
	if err != nil || databaseURL == "" {
		return "", ErrInvalidArgument
	}
	err = b.runner.Run(ctx, "pg_dump", []string{"--format=custom", "--no-owner", "--file", path}, []string{"PGDATABASE=" + databaseURL})
	if err != nil {
		return "", err
	}
	return path, nil
}

// RestoreDrill restores an artifact into an explicitly supplied scratch database.
func (b *BackupRestore) RestoreDrill(ctx context.Context, databaseURL, name string) error {
	path, err := b.artifactPath(name)
	if err != nil || databaseURL == "" {
		return ErrInvalidArgument
	}
	return b.runner.Run(ctx, "pg_restore", []string{"--exit-on-error", "--no-owner", path}, []string{"PGDATABASE=" + databaseURL})
}

func (b *BackupRestore) artifactPath(name string) (string, error) {
	if !objectPathPart.MatchString(name) {
		return "", ErrInvalidArgument
	}
	path := filepath.Join(b.root, name)
	if filepath.Dir(path) != b.root {
		return "", ErrInvalidArgument
	}
	return path, nil
}

type limitedWriter struct {
	writer    *bytes.Buffer
	remaining int
}

func (w *limitedWriter) Write(value []byte) (int, error) {
	original := len(value)
	if len(value) > w.remaining {
		value = value[:w.remaining]
	}
	_, _ = w.writer.Write(value)
	w.remaining -= len(value)
	return original, nil
}
