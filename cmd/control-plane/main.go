// Command control-plane serves Supaflow's authenticated publication and run APIs.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/api"
	"github.com/pazhou/supaflow/internal/definition"
	definitionflowgram "github.com/pazhou/supaflow/internal/definition/flowgram"
	definitionpostgres "github.com/pazhou/supaflow/internal/definition/postgres"
	"github.com/pazhou/supaflow/internal/platformops"
	"github.com/pazhou/supaflow/internal/postgres"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/workflow"
	studio "github.com/pazhou/supaflow/web"
)

func main() {
	if err := run(); err != nil {
		slog.Error("control plane stopped", "error", err)
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
		return fmt.Errorf("SUPAFLOW_RESUME_TOKEN_SECRET is invalid: %w", err)
	}
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return errors.New("DATABASE_URL is invalid")
	}
	poolConfig.MaxConns = 32
	poolConfig.MinConns = 2
	poolConfig.MaxConnLifetime = 30 * time.Minute
	poolConfig.MaxConnIdleTime = 5 * time.Minute
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return fmt.Errorf("configure database pool: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("database readiness check failed: %w", err)
	}
	auth, err := configureAuthentication()
	if err != nil {
		return fmt.Errorf("configure authentication: %w", err)
	}

	registry := workflow.BuiltinRegistry()
	nodeRegistry := postgres.NewNodeRegistry(pool)
	if err = nodeRegistry.EnsureRegistry(ctx, registry); err != nil {
		return fmt.Errorf("seed durable node registry: %w", err)
	}
	persistedNodeTypes, err := nodeRegistry.ListNodeTypes(ctx)
	if err != nil {
		return fmt.Errorf("read durable node registry: %w", err)
	}
	if err = registry.VerifyProjection(persistedNodeTypes); err != nil {
		return fmt.Errorf("validate durable node registry: %w", err)
	}
	policy := workflow.CompilePolicy{
		AllowedCapabilities: workflow.CapabilitySet{Streaming: true, Cancellable: true, Interruptible: true, Idempotent: true},
		AllowedPermissions:  workflow.PermissionSet{Network: true, Credentials: true, Compute: true},
	}
	catalog := postgres.NewPlanCatalog(pool, registry, policy)
	runtimeStore := postgres.NewRuntimeStore(pool)
	runtimeService := workflowruntime.NewService(runtimeStore, nil, nil)
	handler, err := api.NewHandler(runtimeService, catalog, auth, resumeTokens)
	if err != nil {
		return fmt.Errorf("configure HTTP API: %w", err)
	}
	streamConfig, err := eventStreamConfigFromEnv()
	if err != nil {
		return err
	}
	if err = handler.SetEventStreamConfig(streamConfig); err != nil {
		return fmt.Errorf("configure event streams: %w", err)
	}
	definitions, err := definition.NewService(definitionpostgres.NewStore(pool), definitionflowgram.Adapter{}, catalog, registry, policy)
	if err != nil {
		return fmt.Errorf("configure definition studio: %w", err)
	}
	handler.SetStudio(definitions, nodeRegistry)
	handler.SetStudioAssets(studio.Assets())
	handler.SetReadinessProbe(pool.Ping)
	operations, err := configureOperations(ctx, pool, catalog, definitions, runtimeService)
	if err != nil {
		return fmt.Errorf("configure platform operations: %w", err)
	}
	if operations.telemetry != nil {
		runtimeService.SetTelemetry(operations.telemetry)
	}
	operations.supervisor.SetRuntimeExpirer(runtimeStore)
	handler.SetAudit(operations.audit)
	handler.SetOutputReader(operations.outputs)
	if operations.telemetry != nil {
		auth.SetRequestMiddleware(operations.telemetry.HTTPMiddleware)
		defer func() {
			flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if shutdownErr := operations.telemetry.Shutdown(flushCtx); shutdownErr != nil {
				slog.Error("telemetry shutdown failed", "error", shutdownErr)
			}
		}()
	}
	supervisorDone := make(chan struct{})
	go func() {
		defer close(supervisorDone)
		operations.supervisor.Run(ctx)
	}()
	defer func() {
		stop()
		<-supervisorDone
	}()
	address := strings.TrimSpace(os.Getenv("SUPAFLOW_ADDR"))
	if address == "" {
		address = "127.0.0.1:8080"
	}
	rootHandler := mountHandlers(handler.Routes(), operations.handler, auth, operations.telemetry)
	server := &http.Server{
		Addr: address, Handler: rootHandler, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10,
	}
	serverErrors := make(chan error, 1)
	go func() {
		slog.Info("control plane listening", "address", address)
		serverErrors <- server.ListenAndServe()
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("graceful shutdown: %w", err)
		}
		return nil
	case err := <-serverErrors:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve HTTP: %w", err)
	}
}
