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
	admission     *platformops.Admission
	registry      *platformops.WorkerRegistry
	workerID      string
	organizations []string
	capacity      int
	active        atomic.Int64
}

func configureWorkerOperations(ctx context.Context, pool *pgxpool.Pool, workerID string, organizations []string) (*workerOperations, error) {
	admission, err := platformops.NewAdmission(pool, 5*time.Second)
	if err != nil {
		return nil, err
	}
	operations := &workerOperations{admission: admission, registry: platformops.NewWorkerRegistry(pool), workerID: workerID, organizations: organizations, capacity: 1}
	acquired := make([]string, 0, len(organizations))
	cleanup := func() error {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var failures []error
		for _, acquiredOrganizationID := range acquired {
			failures = append(failures,
				operations.registry.BeginDrain(cleanupCtx, acquiredOrganizationID, workerID),
				admission.ReleaseWorker(cleanupCtx, acquiredOrganizationID, workerID))
		}
		return errors.Join(failures...)
	}
	for _, organizationID := range organizations {
		if err = admission.EnsureQuota(ctx, platformops.Quota{
			OrganizationID: organizationID, MaxActiveRuns: workflowruntime.DefaultOrganizationMaxActiveRuns,
			MaxActiveWorkers: workflowruntime.DefaultOrganizationMaxActiveWorkers,
			MaxQueueDepth:    workflowruntime.DefaultOrganizationMaxQueueDepth,
		}); err != nil {
			return nil, errors.Join(fmt.Errorf("ensure organization worker quota: %w", err), cleanup())
		}
		if err = operations.registry.Heartbeat(ctx, organizationID, workerID, operations.capacity, 0); err != nil {
			return nil, errors.Join(fmt.Errorf("register worker heartbeat: %w", err), cleanup())
		}
		acquired = append(acquired, organizationID)
	}
	return operations, nil
}

func (o *workerOperations) mayClaim(ctx context.Context, organizationID string) bool {
	allowed, err := o.registry.MayClaim(ctx, organizationID, o.workerID, 15*time.Second)
	return err == nil && allowed
}

func (o *workerOperations) heartbeat(ctx context.Context, active int) {
	for _, organizationID := range o.organizations {
		o.heartbeatOrganization(ctx, organizationID, active)
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

func (o *workerOperations) heartbeatOrganization(ctx context.Context, organizationID string, active int) {
	if err := o.registry.Heartbeat(ctx, organizationID, o.workerID, o.capacity, active); err != nil {
		slog.Error("worker heartbeat failed", "error", platformops.RedactDiagnostic(err.Error()))
	}
}

func (o *workerOperations) close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var failures []error
	for _, organizationID := range o.organizations {
		failures = append(failures,
			o.registry.BeginDrain(ctx, organizationID, o.workerID),
			o.admission.ReleaseWorker(ctx, organizationID, o.workerID))
	}
	return errors.Join(failures...)
}
