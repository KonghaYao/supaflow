// Command worker claims and executes durable Supaflow node attempts.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/executor"
	"github.com/pazhou/supaflow/internal/platform"
	"github.com/pazhou/supaflow/internal/platformops"
	"github.com/pazhou/supaflow/internal/postgres"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/workflow"
)

func main() {
	if err := run(); err != nil {
		slog.Error("worker stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	slog.SetDefault(slog.New(platformops.NewRedactingHandler(slog.NewJSONHandler(os.Stderr, nil))))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	databaseURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if databaseURL == "" {
		return errors.New("DATABASE_URL is required")
	}
	resumeTokens, err := workflowruntime.NewResumeTokenIssuer([]byte(os.Getenv("SUPAFLOW_RESUME_TOKEN_SECRET")))
	if err != nil {
		return errors.New("SUPAFLOW_RESUME_TOKEN_SECRET must contain at least 32 bytes")
	}
	allowedHosts := parseList(os.Getenv("SUPAFLOW_HTTP_ALLOWED_HOSTS"))
	if len(allowedHosts) == 0 {
		return errors.New("SUPAFLOW_HTTP_ALLOWED_HOSTS requires an explicit egress allowlist")
	}
	workerID := strings.TrimSpace(os.Getenv("SUPAFLOW_WORKER_ID"))
	if workerID == "" {
		generated, err := (workflowruntime.CryptoIDGenerator{}).NewID("worker")
		if err != nil {
			return errors.New("could not allocate worker id")
		}
		workerID = generated
	}
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return errors.New("DATABASE_URL is invalid")
	}
	poolConfig.MaxConns = 16
	poolConfig.MinConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return err
	}
	telemetry, err := configureWorkerTelemetry(ctx)
	if err != nil {
		return err
	}
	if telemetry != nil {
		defer func() {
			flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if shutdownErr := telemetry.Shutdown(flushCtx); shutdownErr != nil {
				slog.Error("worker telemetry shutdown failed", "error", shutdownErr)
			}
		}()
	}
	registry := workflow.BuiltinRegistry()
	nodeRegistry := postgres.NewNodeRegistry(pool)
	if err := nodeRegistry.EnsureRegistry(ctx, registry); err != nil {
		return fmt.Errorf("seed durable node registry: %w", err)
	}
	if err := validateWorkerNodeRegistry(ctx, nodeRegistry, registry); err != nil {
		return fmt.Errorf("validate durable node registry: %w", err)
	}
	policy := workflow.CompilePolicy{
		AllowedCapabilities: workflow.CapabilitySet{Streaming: true, Cancellable: true, Interruptible: true, Idempotent: true},
		AllowedPermissions:  workflow.PermissionSet{Network: true, Credentials: true, Compute: true},
	}
	catalog := postgres.NewPlanCatalog(pool, registry, policy)
	baseRuntimeStore := postgres.NewRuntimeStore(pool)
	outputs, err := configureWorkerOutputs(pool)
	if err != nil {
		return err
	}
	runtimeStore, err := executor.NewManagedOutputStore(baseRuntimeStore, outputs)
	if err != nil {
		return err
	}
	runtimeService := workflowruntime.NewService(runtimeStore, nil, nil)
	runtimeService.SetTelemetry(telemetry)
	executionConfig, err := configureExecution(pool, catalog, runtimeService, allowedHosts, outputs, nil)
	if err != nil {
		return err
	}
	planExecutor, err := executor.NewPlanExecutor(catalog, executionConfig)
	if err != nil {
		return err
	}
	audit, err := platformops.NewAuditService(pool)
	if err != nil {
		return err
	}
	auditedExecutor, err := platformops.NewAuditedExecutor(planExecutor, audit)
	if err != nil {
		return err
	}
	orchestrator, err := workflowruntime.NewOrchestrator(
		runtimeStore, auditedExecutor, nil, nil,
		workflowruntime.OrchestratorConfig{
			WorkerID: workerID, LeaseDuration: 30 * time.Second,
			RetryPolicy:  workflowruntime.RetryPolicy{MaxAttempts: 3, BaseDelay: time.Second, MaxDelay: 30 * time.Second, Jitter: 0.2},
			ResumeTokens: resumeTokens,
			Telemetry:    telemetry,
		},
	)
	if err != nil {
		return err
	}
	slog.Info("worker started", "workerId", workerID)
	idle := time.NewTicker(100 * time.Millisecond)
	defer idle.Stop()
	for {
		result, advanceErr := orchestrator.AdvanceNext(ctx)
		if advanceErr != nil {
			slog.Error("attempt advance failed", "error", advanceErr)
		}
		if result.Worked {
			continue
		}
		select {
		case <-ctx.Done():
			slog.Info("worker drained", "workerId", workerID)
			return nil
		case <-idle.C:
		}
	}
}

func validateWorkerNodeRegistry(ctx context.Context, catalog platform.NodeTypeCatalog, cache *workflow.Registry) error {
	entries, err := catalog.ListNodeTypes(ctx)
	if err != nil {
		return err
	}
	return executor.ValidateNodeRegistryCompatibility(entries, cache, executor.WorkerRunnerVersion)
}

func parseList(value string) []string {
	seen := make(map[string]bool)
	items := make([]string, 0)
	for _, raw := range strings.Split(value, ",") {
		item := strings.TrimSpace(raw)
		if item == "" || seen[item] {
			continue
		}
		seen[item] = true
		items = append(items, item)
	}
	return items
}
