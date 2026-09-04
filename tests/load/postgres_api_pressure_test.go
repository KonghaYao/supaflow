package load

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/api"
	"github.com/pazhou/supaflow/internal/executor"
	"github.com/pazhou/supaflow/internal/identity"
	"github.com/pazhou/supaflow/internal/postgres"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/workflow"
)

// 验证真实 TCP、数据库认证、PostgreSQL catalog/runtime/admission 和连接池承受并发 Run API 压力。
func TestPostgresControlPlanePressure(t *testing.T) {
	databaseURL := os.Getenv("SUPAFLOW_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("SUPAFLOW_TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	poolConfig.MaxConns = 32
	poolConfig.MinConns = 2
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	migrations, err := postgres.LoadMigrations(os.DirFS("../.."), "migrations")
	if err != nil {
		t.Fatal(err)
	}
	if err = postgres.NewMigrator(pool).Up(ctx, migrations); err != nil {
		t.Fatal(err)
	}

	suffix := fmt.Sprintf("%d", time.Now().UTC().UnixNano())
	tenantID := "pressure_tenant_" + suffix
	workflowID := "pressure_workflow_" + suffix
	actorID := "pressure_actor_" + suffix
	apiKey := "pressure-api-key-at-least-24-bytes-" + suffix
	if _, err = pool.Exec(ctx, `INSERT INTO tenants (id,name) VALUES ($1,$1)`, tenantID); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = pool.Exec(context.Background(), `DELETE FROM tenants WHERE id=$1`, tenantID) }()
	identityStore, err := identity.NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	if err = identityStore.BootstrapAPIKey(ctx, identity.BootstrapAPIKey{
		Value: apiKey, TenantID: tenantID, ActorID: actorID, Name: "pressure", Role: identity.RoleAdmin,
	}); err != nil {
		t.Fatal(err)
	}
	auth, err := api.NewDatabaseAuthenticator(identityStore)
	if err != nil {
		t.Fatal(err)
	}
	registry := workflow.BuiltinRegistry()
	catalog := postgres.NewPlanCatalog(pool, registry, workflow.CompilePolicy{
		AllowedCapabilities: workflow.CapabilitySet{Idempotent: true},
	})
	published, _, err := catalog.Publish(ctx, tenantID, workflowID, pressureDefinition(workflowID), "pressure-publish")
	if err != nil {
		t.Fatal(err)
	}
	issuer, err := workflowruntime.NewResumeTokenIssuer([]byte("pressure-resume-token-secret-at-least-32-bytes"))
	if err != nil {
		t.Fatal(err)
	}
	handler, err := api.NewHandler(workflowruntime.NewService(postgres.NewRuntimeStore(pool), nil, nil), catalog, auth, issuer)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler.Routes())
	defer server.Close()
	transport := &http.Transport{
		MaxIdleConns: 128, MaxIdleConnsPerHost: 128, MaxConnsPerHost: 1000, IdleConnTimeout: 30 * time.Second,
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 45 * time.Second}

	payload, err := json.Marshal(map[string]any{
		"planId": published.ID, "planHash": published.Hash, "input": map[string]any{"pressure": true},
	})
	if err != nil {
		t.Fatal(err)
	}
	const requests = 1000
	const concurrency = requests
	startGate := make(chan struct{})
	failures := make(chan string, requests)
	var succeeded atomic.Int64
	started := time.Now()
	var workers sync.WaitGroup
	for index := 0; index < requests; index++ {
		workers.Add(1)
		go func(index int) {
			defer workers.Done()
			<-startGate
			req, requestErr := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/runs", bytes.NewReader(payload))
			if requestErr != nil {
				failures <- requestErr.Error()
				return
			}
			req.Header.Set("X-API-Key", apiKey)
			req.Header.Set("Idempotency-Key", fmt.Sprintf("postgres-pressure-%08d", index))
			response, requestErr := client.Do(req)
			if requestErr != nil {
				failures <- fmt.Sprintf("request %d: %v", index, requestErr)
				return
			}
			_ = response.Body.Close()
			if response.StatusCode != http.StatusAccepted {
				failures <- fmt.Sprintf("request %d status=%d", index, response.StatusCode)
				return
			}
			succeeded.Add(1)
		}(index)
	}
	close(startGate)
	workers.Wait()
	close(failures)
	for failure := range failures {
		t.Error(failure)
	}
	if succeeded.Load() != requests {
		t.Fatalf("successful requests=%d want=%d", succeeded.Load(), requests)
	}
	var durableRuns int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM runs WHERE tenant_id=$1`, tenantID).Scan(&durableRuns); err != nil {
		t.Fatal(err)
	}
	if durableRuns != requests {
		t.Fatalf("durable runs=%d want=%d", durableRuns, requests)
	}
	elapsed := time.Since(started)
	t.Logf("profile=postgres-tcp requests=%d concurrency=%d elapsed=%s throughput=%.0f req/s", requests, concurrency, elapsed, requests/elapsed.Seconds())

	planExecutor, err := executor.NewPlanExecutor(catalog, executor.Config{})
	if err != nil {
		t.Fatal(err)
	}
	orchestrator, err := workflowruntime.NewOrchestrator(postgres.NewRuntimeStore(pool), planExecutor, nil, nil, workflowruntime.OrchestratorConfig{
		WorkerID:      "pressure-worker-" + suffix,
		LeaseDuration: time.Minute,
		RetryPolicy: workflowruntime.RetryPolicy{
			MaxAttempts: 2,
			BaseDelay:   time.Millisecond,
			MaxDelay:    time.Second,
		},
		ResumeTokens: issuer,
	})
	if err != nil {
		t.Fatal(err)
	}
	drainStarted := time.Now()
	for advances := 0; advances < requests*2; advances++ {
		result, advanceErr := orchestrator.Advance(ctx, workflowruntime.TenantID(tenantID))
		if advanceErr != nil || !result.Worked {
			t.Fatalf("advance %d worked=%t error=%v", advances, result.Worked, advanceErr)
		}
	}
	result, err := orchestrator.Advance(ctx, workflowruntime.TenantID(tenantID))
	if err != nil || result.Worked {
		t.Fatalf("queue was not drained: result=%+v error=%v", result, err)
	}
	var successfulRuns, activeRuns, queuedRuns int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM runs WHERE tenant_id=$1 AND status='success'`, tenantID).Scan(&successfulRuns); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT active_runs,queued_runs FROM tenant_quota_counters WHERE tenant_id=$1`, tenantID).Scan(&activeRuns, &queuedRuns); err != nil {
		t.Fatal(err)
	}
	if successfulRuns != requests || activeRuns != 0 || queuedRuns != 0 {
		t.Fatalf("terminal projection success=%d active=%d queued=%d", successfulRuns, activeRuns, queuedRuns)
	}
	t.Logf("profile=postgres-worker-drain runs=%d advances=%d elapsed=%s", requests, requests*2, time.Since(drainStarted))
}

func pressureDefinition(workflowID string) workflow.WorkflowDefinition {
	return workflow.WorkflowDefinition{
		SchemaVersion: workflow.DefinitionSchemaV1,
		ID:            workflowID,
		Nodes: []workflow.NodeDefinition{
			{ID: "start", Type: workflow.NodeTypeStart, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
			{ID: "end", Type: workflow.NodeTypeEnd, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
		},
		Edges: []workflow.EdgeDefinition{{ID: "edge", FromNode: "start", FromPort: "input", ToNode: "end", ToPort: "result"}},
	}
}
