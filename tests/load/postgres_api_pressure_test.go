package load

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
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
	if os.Getenv("SUPAFLOW_RELEASE_EVIDENCE_DIR") != "" {
		if err := validateEvidenceRevision(); err != nil {
			t.Fatal(err)
		}
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
	var postgresVersion string
	if err = pool.QueryRow(ctx, `SHOW server_version`).Scan(&postgresVersion); err != nil {
		t.Fatal(err)
	}
	migrations, err := postgres.LoadMigrations(os.DirFS("../.."), "migrations")
	if err != nil {
		t.Fatal(err)
	}
	if err = postgres.NewMigrator(pool).Up(ctx, migrations); err != nil {
		t.Fatal(err)
	}

	suffix := fmt.Sprintf("%d", time.Now().UTC().UnixNano())
	organizationID := "pressure_organization_" + suffix
	workflowID := "pressure_workflow_" + suffix
	apiKey := "pressure-api-key-at-least-24-bytes-" + suffix
	auth, err := api.NewMasterKeyAuthenticator(apiKey)
	if err != nil {
		t.Fatal(err)
	}
	registry := workflow.BuiltinRegistry()
	catalog := postgres.NewPlanCatalog(pool, registry, workflow.CompilePolicy{
		AllowedCapabilities: workflow.CapabilitySet{Idempotent: true},
	})
	published, _, err := catalog.Publish(ctx, organizationID, workflowID, pressureDefinition(workflowID), "pressure-publish")
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
	startLatencies := make([]time.Duration, requests)
	startGate := make(chan struct{})
	failures := make(chan string, requests)
	var succeeded atomic.Int64
	var ready sync.WaitGroup
	ready.Add(requests)
	var workers sync.WaitGroup
	for index := 0; index < requests; index++ {
		workers.Add(1)
		go func(index int) {
			defer workers.Done()
			ready.Done()
			<-startGate
			req, requestErr := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/runs", bytes.NewReader(payload))
			if requestErr != nil {
				failures <- requestErr.Error()
				return
			}
			req.Header.Set("Authorization", "Bearer "+apiKey)
			req.Header.Set("X-Fenix-Organization-Id", organizationID)
			req.Header.Set("Idempotency-Key", fmt.Sprintf("postgres-pressure-%08d", index))
			requestStarted := time.Now()
			response, requestErr := client.Do(req)
			if requestErr != nil {
				failures <- fmt.Sprintf("request %d: %v", index, requestErr)
				return
			}
			_, readErr := io.Copy(io.Discard, response.Body)
			closeErr := response.Body.Close()
			startLatencies[index] = time.Since(requestStarted)
			if readErr != nil || closeErr != nil {
				failures <- fmt.Sprintf("request %d read=%v close=%v", index, readErr, closeErr)
				return
			}
			if response.StatusCode != http.StatusAccepted {
				failures <- fmt.Sprintf("request %d status=%d", index, response.StatusCode)
				return
			}
			succeeded.Add(1)
		}(index)
	}
	ready.Wait()
	started := time.Now()
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
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM runs WHERE organization_id=$1`, organizationID).Scan(&durableRuns); err != nil {
		t.Fatal(err)
	}
	if durableRuns != requests {
		t.Fatalf("durable runs=%d want=%d", durableRuns, requests)
	}
	elapsed := time.Since(started)
	startPercentiles := durationPercentiles(startLatencies)
	t.Logf("profile=postgres-tcp requests=%d concurrency=%d elapsed=%s throughput=%.0f req/s latency_p50_ms=%.3f latency_p95_ms=%.3f latency_p99_ms=%.3f",
		requests, concurrency, elapsed, requests/elapsed.Seconds(), startPercentiles.P50Milliseconds,
		startPercentiles.P95Milliseconds, startPercentiles.P99Milliseconds)

	planExecutor, err := executor.NewPlanExecutor(catalog, executor.Config{})
	if err != nil {
		t.Fatal(err)
	}
	const workerCount = 8
	orchestrators := make([]*workflowruntime.Orchestrator, workerCount)
	for index := range orchestrators {
		orchestrators[index], err = workflowruntime.NewOrchestrator(postgres.NewRuntimeStore(pool), planExecutor, nil, nil, workflowruntime.OrchestratorConfig{
			WorkerID:      fmt.Sprintf("pressure-worker-%s-%02d", suffix, index),
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
	}
	var queuedRunsBeforeDrain int
	var oldestQueueAgeBeforeDrainMS float64
	if err = pool.QueryRow(ctx, `SELECT count(*), COALESCE((EXTRACT(EPOCH FROM (clock_timestamp()-min(created_at)))*1000)::double precision,0)
		FROM admission_entries WHERE organization_id=$1 AND resource_type='run' AND status='queued'`, organizationID).
		Scan(&queuedRunsBeforeDrain, &oldestQueueAgeBeforeDrainMS); err != nil {
		t.Fatal(err)
	}
	if queuedRunsBeforeDrain == 0 || oldestQueueAgeBeforeDrainMS <= 0 {
		t.Fatalf("queue pressure was not exercised: queued=%d oldest_age_ms=%.3f", queuedRunsBeforeDrain, oldestQueueAgeBeforeDrainMS)
	}
	var eventsBeforeDrain, outboxBeforeDrain int64
	if err = pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM run_events WHERE organization_id=$1),
		(SELECT count(*) FROM outbox_messages WHERE organization_id=$1)`, organizationID).
		Scan(&eventsBeforeDrain, &outboxBeforeDrain); err != nil {
		t.Fatal(err)
	}
	drainStarted := time.Now()
	const expectedAdvances = int64(requests * 2)
	const maximumAdvanceCalls = expectedAdvances * 4
	var advances, advanceCalls, emptyAdvanceCalls atomic.Int64
	advanceLatencies := make([]time.Duration, 0, maximumAdvanceCalls)
	var latencyMu sync.Mutex
	drainCtx, stopDrain := context.WithCancel(ctx)
	defer stopDrain()
	var drainErr error
	var failOnce sync.Once
	fail := func(reason error) {
		failOnce.Do(func() {
			drainErr = reason
			stopDrain()
		})
	}
	var drainWorkers sync.WaitGroup
	for _, orchestrator := range orchestrators {
		drainWorkers.Add(1)
		go func(orchestrator *workflowruntime.Orchestrator) {
			defer drainWorkers.Done()
			for advances.Load() < expectedAdvances && drainCtx.Err() == nil {
				call := advanceCalls.Add(1)
				if call > maximumAdvanceCalls {
					fail(fmt.Errorf("worker drain exceeded bounded call budget: %d", call))
					return
				}
				callStarted := time.Now()
				result, advanceErr := orchestrator.Advance(drainCtx, workflowruntime.OrganizationID(organizationID))
				callElapsed := time.Since(callStarted)
				latencyMu.Lock()
				advanceLatencies = append(advanceLatencies, callElapsed)
				latencyMu.Unlock()
				if advanceErr != nil {
					fail(advanceErr)
					return
				}
				if result.Worked {
					advances.Add(1)
				} else {
					emptyAdvanceCalls.Add(1)
					time.Sleep(time.Millisecond)
				}
			}
		}(orchestrator)
	}
	drainWorkers.Wait()
	if drainErr != nil {
		t.Fatal(drainErr)
	}
	if advances.Load() != expectedAdvances {
		t.Fatalf("worker advances=%d want=%d", advances.Load(), expectedAdvances)
	}
	drainElapsed := time.Since(drainStarted)
	result, err := orchestrators[0].Advance(ctx, workflowruntime.OrganizationID(organizationID))
	if err != nil || result.Worked {
		t.Fatalf("queue was not drained: result=%+v error=%v", result, err)
	}
	var successfulRuns, activeRuns, queuedRuns int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM runs WHERE organization_id=$1 AND status='success'`, organizationID).Scan(&successfulRuns); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT active_runs,queued_runs FROM organization_quota_counters WHERE organization_id=$1`, organizationID).Scan(&activeRuns, &queuedRuns); err != nil {
		t.Fatal(err)
	}
	if successfulRuns != requests || activeRuns != 0 || queuedRuns != 0 {
		t.Fatalf("terminal projection success=%d active=%d queued=%d", successfulRuns, activeRuns, queuedRuns)
	}
	var eventsAfterDrain, outboxAfterDrain, leaseTakeovers int64
	var maximumQueueAdmissionDelayMS float64
	if err = pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM run_events WHERE organization_id=$1),
		(SELECT count(*) FROM outbox_messages WHERE organization_id=$1),
		(SELECT count(*) FROM run_events WHERE organization_id=$1 AND event_type='attempt.lease_taken_over'),
		(SELECT COALESCE((max(EXTRACT(EPOCH FROM (admitted_at-created_at))*1000))::double precision,0)
		 FROM admission_entries WHERE organization_id=$1 AND resource_type='run')`, organizationID).
		Scan(&eventsAfterDrain, &outboxAfterDrain, &leaseTakeovers, &maximumQueueAdmissionDelayMS); err != nil {
		t.Fatal(err)
	}
	committedEvents := eventsAfterDrain - eventsBeforeDrain
	committedOutbox := outboxAfterDrain - outboxBeforeDrain
	if committedEvents <= 0 || committedOutbox <= 0 || maximumQueueAdmissionDelayMS <= 0 {
		t.Fatalf("bounded runtime metrics missing: events=%d outbox=%d queue_delay_ms=%.3f",
			committedEvents, committedOutbox, maximumQueueAdmissionDelayMS)
	}
	advancePercentiles := durationPercentiles(advanceLatencies)
	t.Logf("profile=postgres-worker-drain workers=%d runs=%d advances=%d calls=%d empty_calls=%d elapsed=%s throughput=%.0f advances/s advance_p95_ms=%.3f events=%d event_throughput=%.0f events/s outbox=%d outbox_throughput=%.0f messages/s queue_oldest_ms=%.3f queue_admission_max_ms=%.3f lease_takeovers=%d",
		workerCount, requests, advances.Load(), advanceCalls.Load(), emptyAdvanceCalls.Load(), drainElapsed,
		float64(advances.Load())/drainElapsed.Seconds(), advancePercentiles.P95Milliseconds,
		committedEvents, float64(committedEvents)/drainElapsed.Seconds(), committedOutbox,
		float64(committedOutbox)/drainElapsed.Seconds(), oldestQueueAgeBeforeDrainMS,
		maximumQueueAdmissionDelayMS, leaseTakeovers)
	revision := os.Getenv("SUPAFLOW_RELEASE_COMMIT")
	if revision == "" {
		revision = "unbound-local-run"
	}
	evidence := newPressureEvidence(revision)
	evidence.PostgreSQL = postgresVersion
	evidence.PoolMaxConns = poolConfig.MaxConns
	evidence.PoolMinConns = poolConfig.MinConns
	evidence.Start = startPressureEvidence{
		Requests: requests, Concurrency: concurrency, ElapsedSeconds: elapsed.Seconds(),
		Throughput: float64(requests) / elapsed.Seconds(), Latency: startPercentiles,
	}
	evidence.Runtime = runtimePressureEvidence{
		Workers: workerCount, Advances: advances.Load(), AdvanceCallLimit: maximumAdvanceCalls,
		AdvanceCalls:      advanceCalls.Load(),
		EmptyAdvanceCalls: emptyAdvanceCalls.Load(), ElapsedSeconds: drainElapsed.Seconds(),
		AdvanceThroughput: float64(advances.Load()) / drainElapsed.Seconds(), AdvanceLatency: advancePercentiles,
		CommittedEvents: committedEvents, EventCommitThroughput: float64(committedEvents) / drainElapsed.Seconds(),
		CommittedOutboxMessages: committedOutbox, OutboxEnqueueThroughput: float64(committedOutbox) / drainElapsed.Seconds(),
		QueuedRunsBeforeDrain: queuedRunsBeforeDrain, OldestQueueAgeBeforeDrainMS: oldestQueueAgeBeforeDrainMS,
		MaximumQueueAdmissionDelayMS: maximumQueueAdmissionDelayMS, LeaseTakeovers: leaseTakeovers,
	}
	if path := writePressureEvidence(t, evidence); path != "" {
		t.Logf("release_evidence_path=%s", path)
	}
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
