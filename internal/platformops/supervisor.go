package platformops

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
)

// RuntimeExpirer is the maintenance boundary for durable run deadlines.
type RuntimeExpirer interface {
	ExpireRuns(context.Context, workflowruntime.TenantID, time.Time, int) (int, error)
}

// SupervisorConfig bounds every maintenance iteration and prevents busy loops.
type SupervisorConfig struct {
	ScheduleInterval       time.Duration
	DeadlineInterval       time.Duration
	DeliveryInterval       time.Duration
	RetentionInterval      time.Duration
	ObjectRecoveryInterval time.Duration
	DeliveryTimeout        time.Duration
	ScheduleBatch          int
	DeliveryBatch          int
	DeliveryConcurrency    int
	RetentionBatch         int
}

// Supervisor runs database-coordinated schedule, outbox, replay-TTL, and retention work.
type Supervisor struct {
	pool          *pgxpool.Pool
	triggers      *TriggerService
	triggerStore  *PostgresTriggerStore
	dispatcher    *OutboxDispatcher
	retention     *Retention
	workers       *WorkerRegistry
	config        SupervisorConfig
	logger        *slog.Logger
	runtime       RuntimeExpirer
	cursorMu      sync.Mutex
	tenantCursors map[string]string
}

// SetRuntimeExpirer enables deadline enforcement during every schedule tick.
// Configure it before Run; nil keeps trigger-only maintenance for unit tests.
func (s *Supervisor) SetRuntimeExpirer(expirer RuntimeExpirer) { s.runtime = expirer }

// NewSupervisor validates production maintenance dependencies.
func NewSupervisor(pool *pgxpool.Pool, triggers *TriggerService, triggerStore *PostgresTriggerStore, dispatcher *OutboxDispatcher, retention *Retention, config SupervisorConfig, logger *slog.Logger) (*Supervisor, error) {
	if pool == nil || triggers == nil || triggerStore == nil || dispatcher == nil || retention == nil ||
		config.ScheduleInterval <= 0 || config.DeliveryInterval <= 0 || config.RetentionInterval <= 0 ||
		config.ScheduleBatch <= 0 || config.DeliveryBatch <= 0 || config.RetentionBatch <= 0 {
		return nil, ErrInvalidArgument
	}
	if logger == nil {
		logger = slog.Default()
	}
	if config.DeadlineInterval <= 0 {
		config.DeadlineInterval = config.ScheduleInterval
	}
	if config.ObjectRecoveryInterval <= 0 {
		config.ObjectRecoveryInterval = time.Minute
	}
	if config.DeliveryTimeout <= 0 {
		config.DeliveryTimeout = 20 * time.Second
	}
	if config.DeliveryConcurrency <= 0 {
		config.DeliveryConcurrency = 4
	}
	if config.DeliveryConcurrency > 64 || config.DeliveryBatch > 1000 || config.DeliveryTimeout > 30*time.Second {
		return nil, ErrInvalidArgument
	}
	return &Supervisor{pool: pool, triggers: triggers, triggerStore: triggerStore, dispatcher: dispatcher, retention: retention, workers: NewWorkerRegistry(pool), config: config, logger: logger, tenantCursors: make(map[string]string)}, nil
}

// Run executes bounded maintenance ticks until cancellation and never busy-loops on failures.
func (s *Supervisor) Run(ctx context.Context) {
	loops := []struct {
		name     string
		interval time.Duration
		run      func(context.Context, time.Time) error
	}{
		{name: "schedule", interval: s.config.ScheduleInterval, run: s.ScheduleOnce},
		{name: "deadline", interval: s.config.DeadlineInterval, run: s.DeadlineOnce},
		{name: "delivery", interval: s.config.DeliveryInterval, run: s.DeliverOnce},
		{name: "object-recovery", interval: s.config.ObjectRecoveryInterval, run: s.ObjectRecoveryOnce},
		{name: "retention", interval: s.config.RetentionInterval, run: s.RetentionOnce},
	}
	var wait sync.WaitGroup
	for _, loop := range loops {
		loop := loop
		wait.Add(1)
		go func() {
			defer wait.Done()
			s.runLoop(ctx, loop.name, loop.interval, loop.run)
		}()
	}
	wait.Wait()
}

// ObjectRecoveryOnce reclaims a bounded page of expired upload intents. It is
// independent from retention policies so a tenant without a custom policy is
// still protected from files left by a killed uploader.
func (s *Supervisor) ObjectRecoveryOnce(ctx context.Context, _ time.Time) error {
	tenants, err := s.tenantPage(ctx, "object-recovery", s.config.RetentionBatch)
	if err != nil {
		return err
	}
	var failures []error
	for _, tenantID := range tenants {
		if _, recoveryErr := s.retention.RecoverObjects(ctx, tenantID, s.config.RetentionBatch); recoveryErr != nil {
			failures = append(failures, fmt.Errorf("tenant object recovery: %w", recoveryErr))
		}
	}
	return errors.Join(failures...)
}

func (s *Supervisor) runLoop(ctx context.Context, name string, interval time.Duration, run func(context.Context, time.Time) error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if err := run(ctx, now.UTC()); err != nil {
				s.logger.Error("platform maintenance tick failed", "loop", name, "error", RedactDiagnostic(err.Error()))
			}
		}
	}
}

// ScheduleOnce claims a finite set of schedules and purges expired replay identities.
func (s *Supervisor) ScheduleOnce(ctx context.Context, now time.Time) error {
	_, tickErr := s.triggers.Tick(ctx, ScheduleOptions{Now: now, Limit: s.config.ScheduleBatch, MaxCatchUp: 100, MisfireGrace: time.Minute})
	_, purgeErr := s.triggerStore.PurgeExpiredWebhookReplays(ctx, now, s.config.ScheduleBatch)
	tenants, tenantErr := s.tenantPage(ctx, "schedule", s.config.ScheduleBatch)
	failures := []error{tickErr, purgeErr, tenantErr}
	for _, tenantID := range tenants {
		_, reapErr := s.workers.ReapStale(ctx, tenantID, now.Add(-30*time.Second), s.config.ScheduleBatch)
		failures = append(failures, reapErr)
	}
	return errors.Join(failures...)
}

// DeadlineOnce expires a finite batch independently from scheduling and delivery.
func (s *Supervisor) DeadlineOnce(ctx context.Context, now time.Time) error {
	if s.runtime == nil {
		return nil
	}
	tenants, err := s.tenantPage(ctx, "deadline", s.config.ScheduleBatch)
	if err != nil {
		return err
	}
	var failures []error
	for _, tenantID := range tenants {
		_, expiryErr := s.runtime.ExpireRuns(ctx, workflowruntime.TenantID(tenantID), now, s.config.ScheduleBatch)
		failures = append(failures, expiryErr)
	}
	return errors.Join(failures...)
}

// DeliverOnce dispatches a finite per-tenant batch of owned outbox topics.
func (s *Supervisor) DeliverOnce(ctx context.Context, now time.Time) error {
	tenants, err := s.tenantPage(ctx, "delivery", s.config.DeliveryBatch)
	if err != nil {
		return err
	}
	var failures []error
	remaining := s.config.DeliveryBatch
	for remaining > 0 {
		count := min(remaining, len(tenants))
		if count == 0 {
			break
		}
		type outcome struct {
			result DispatchResult
			err    error
		}
		outcomes := make(chan outcome, count)
		semaphore := make(chan struct{}, s.config.DeliveryConcurrency)
		var wait sync.WaitGroup
		for _, tenantID := range tenants[:count] {
			tenantID := tenantID
			wait.Add(1)
			go func() {
				defer wait.Done()
				select {
				case semaphore <- struct{}{}:
				case <-ctx.Done():
					outcomes <- outcome{err: ctx.Err()}
					return
				}
				defer func() { <-semaphore }()
				deliveryCtx, cancel := context.WithTimeout(ctx, s.config.DeliveryTimeout)
				defer cancel()
				result, dispatchErr := s.dispatcher.DispatchOne(deliveryCtx, tenantID, now)
				outcomes <- outcome{result: result, err: dispatchErr}
			}()
		}
		wait.Wait()
		close(outcomes)
		worked := false
		for item := range outcomes {
			if item.err != nil {
				failures = append(failures, fmt.Errorf("tenant delivery: %w", item.err))
			}
			worked = worked || item.result.MessageID != ""
		}
		remaining -= count
		if !worked {
			break
		}
	}
	return errors.Join(failures...)
}

// RetentionOnce applies every tenant policy with a finite batch.
func (s *Supervisor) RetentionOnce(ctx context.Context, now time.Time) error {
	tenants, err := s.tenantPage(ctx, "retention", s.config.RetentionBatch)
	if err != nil {
		return err
	}
	var failures []error
	for _, tenantID := range tenants {
		if _, sweepErr := s.retention.SweepTenant(ctx, tenantID, now, s.config.RetentionBatch); sweepErr != nil && !errors.Is(sweepErr, ErrNotFound) {
			failures = append(failures, fmt.Errorf("tenant retention: %w", sweepErr))
		}
	}
	return errors.Join(failures...)
}

func (s *Supervisor) tenantPage(ctx context.Context, loop string, limit int) ([]string, error) {
	s.cursorMu.Lock()
	cursor := s.tenantCursors[loop]
	s.cursorMu.Unlock()
	result, err := s.queryTenantPage(ctx, cursor, limit)
	if err != nil {
		return nil, err
	}
	if len(result) == 0 && cursor != "" {
		result, err = s.queryTenantPage(ctx, "", limit)
		if err != nil {
			return nil, err
		}
	}
	s.cursorMu.Lock()
	if len(result) == 0 {
		s.tenantCursors[loop] = ""
	} else {
		s.tenantCursors[loop] = result[len(result)-1]
	}
	s.cursorMu.Unlock()
	return result, nil
}

func (s *Supervisor) queryTenantPage(ctx context.Context, after string, limit int) ([]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT id FROM tenants
		WHERE status='active' AND id > $1 ORDER BY id LIMIT $2`, after, limit)
	if err != nil {
		return nil, fmt.Errorf("list maintenance tenants: %w", err)
	}
	defer rows.Close()
	result := make([]string, 0)
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan maintenance tenant: %w", err)
		}
		result = append(result, id)
	}
	return result, rows.Err()
}
