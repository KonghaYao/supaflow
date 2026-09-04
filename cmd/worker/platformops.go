package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/platformops"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
)

type workerOperations struct {
	admission *platformops.Admission
	registry  *platformops.WorkerRegistry
	workerID  string
	tenants   []string
	capacity  int
	active    atomic.Int64
}

func configureWorkerOperations(ctx context.Context, pool *pgxpool.Pool, workerID string, tenants []string) (*workerOperations, error) {
	admission, err := platformops.NewAdmission(pool, 5*time.Second)
	if err != nil {
		return nil, err
	}
	operations := &workerOperations{admission: admission, registry: platformops.NewWorkerRegistry(pool), workerID: workerID, tenants: tenants, capacity: 1}
	acquired := make([]string, 0, len(tenants))
	cleanup := func() error {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var failures []error
		for _, acquiredTenantID := range acquired {
			failures = append(failures,
				operations.registry.BeginDrain(cleanupCtx, acquiredTenantID, workerID),
				admission.ReleaseWorker(cleanupCtx, acquiredTenantID, workerID))
		}
		return errors.Join(failures...)
	}
	for _, tenantID := range tenants {
		if err = admission.EnsureQuota(ctx, platformops.Quota{
			TenantID: tenantID, MaxActiveRuns: workflowruntime.DefaultTenantMaxActiveRuns,
			MaxActiveWorkers: workflowruntime.DefaultTenantMaxActiveWorkers,
			MaxQueueDepth:    workflowruntime.DefaultTenantMaxQueueDepth,
		}); err != nil {
			return nil, errors.Join(fmt.Errorf("ensure tenant worker quota: %w", err), cleanup())
		}
		if err = operations.registry.Heartbeat(ctx, tenantID, workerID, operations.capacity, 0); err != nil {
			return nil, errors.Join(fmt.Errorf("register worker heartbeat: %w", err), cleanup())
		}
		acquired = append(acquired, tenantID)
	}
	return operations, nil
}

func (o *workerOperations) mayClaim(ctx context.Context, tenantID string) bool {
	allowed, err := o.registry.MayClaim(ctx, tenantID, o.workerID, 15*time.Second)
	return err == nil && allowed
}

func (o *workerOperations) heartbeat(ctx context.Context, active int) {
	for _, tenantID := range o.tenants {
		o.heartbeatTenant(ctx, tenantID, active)
	}
}

func (o *workerOperations) runHeartbeat(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			o.heartbeat(ctx, int(o.active.Load()))
		}
	}
}

func (o *workerOperations) setActive(value int64) { o.active.Store(value) }

func (o *workerOperations) heartbeatTenant(ctx context.Context, tenantID string, active int) {
	if err := o.registry.Heartbeat(ctx, tenantID, o.workerID, o.capacity, active); err != nil {
		slog.Error("worker heartbeat failed", "error", platformops.RedactDiagnostic(err.Error()))
	}
}

func (o *workerOperations) close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var failures []error
	for _, tenantID := range o.tenants {
		failures = append(failures,
			o.registry.BeginDrain(ctx, tenantID, o.workerID),
			o.admission.ReleaseWorker(ctx, tenantID, o.workerID))
	}
	return errors.Join(failures...)
}
