package main

import (
	"context"
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
	for _, tenantID := range tenants {
		if err = admission.EnsureQuota(ctx, platformops.Quota{
			TenantID: tenantID, MaxActiveRuns: workflowruntime.DefaultTenantMaxActiveRuns,
			MaxActiveWorkers: workflowruntime.DefaultTenantMaxActiveWorkers,
			MaxQueueDepth:    workflowruntime.DefaultTenantMaxQueueDepth,
		}); err != nil {
			return nil, fmt.Errorf("ensure tenant worker quota: %w", err)
		}
		if _, err = admission.AdmitWorker(ctx, tenantID, workerID); err != nil {
			return nil, fmt.Errorf("admit worker for tenant: %w", err)
		}
		if err = operations.registry.Heartbeat(ctx, tenantID, workerID, operations.capacity, 0, time.Now().UTC()); err != nil {
			return nil, fmt.Errorf("register worker heartbeat: %w", err)
		}
	}
	return operations, nil
}

func (o *workerOperations) mayClaim(ctx context.Context, tenantID string) bool {
	allowed, err := o.registry.MayClaim(ctx, tenantID, o.workerID, time.Now().UTC().Add(-15*time.Second))
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
	if err := o.registry.Heartbeat(ctx, tenantID, o.workerID, o.capacity, active, time.Now().UTC()); err != nil {
		slog.Error("worker heartbeat failed", "error", platformops.RedactDiagnostic(err.Error()))
	}
}

func (o *workerOperations) close() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, tenantID := range o.tenants {
		_ = o.registry.BeginDrain(ctx, tenantID, o.workerID, time.Now().UTC())
		_ = o.admission.ReleaseWorker(ctx, tenantID, o.workerID)
	}
}
