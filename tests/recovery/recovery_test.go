package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
)

// 验证 worker 在持有 lease 时崩溃后，新 worker 只能在租约到期后用更高 fencing token 接管并完成。
func TestWorkerCrashLeaseTakeover(t *testing.T) {
	now := time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)
	clock := &mutableClock{now: now}
	store := workflowruntime.NewMemoryStore()
	service := workflowruntime.NewService(store, clock, nil)
	created := startRun(t, service, "crash-takeover")
	dead, err := store.ClaimAttempt(context.Background(), workflowruntime.ClaimAttemptRequest{
		TenantID: "tenant", WorkerID: "dead-worker", Now: now, LeaseDuration: time.Second,
	})
	if err != nil || dead == nil {
		t.Fatalf("initial claim = %#v, %v", dead, err)
	}
	clock.Set(now.Add(2 * time.Second))
	orchestrator := newOrchestrator(t, store, finalExecutor{}, clock, "recovery-worker", 2)
	result, err := orchestrator.Advance(context.Background(), "tenant")
	if err != nil || result.Run == nil || result.Run.ID != created.ID || result.Run.Status != workflowruntime.RunSuccess {
		t.Fatalf("recovery advance = %#v, %v", result, err)
	}
	events, err := service.ListEvents(context.Background(), "tenant", created.ID, 0, 100)
	if err != nil || !hasEvent(events, "attempt.lease_taken_over") {
		t.Fatalf("takeover events = %#v, %v", events, err)
	}
}

// 验证持久化适配器短暂失败不会吞掉待执行 attempt，恢复后仍能正常完成。
func TestTransientStoreFailurePreservesWork(t *testing.T) {
	now := time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)
	clock := &mutableClock{now: now}
	base := workflowruntime.NewMemoryStore()
	store := &transientClaimStore{Store: base}
	service := workflowruntime.NewService(store, clock, nil)
	created := startRun(t, service, "store-fault")
	orchestrator := newOrchestrator(t, store, finalExecutor{}, clock, "worker", 1)
	if _, err := orchestrator.Advance(context.Background(), "tenant"); err == nil {
		t.Fatal("first Advance() error = nil")
	}
	result, err := orchestrator.Advance(context.Background(), "tenant")
	if err != nil || result.Run == nil || result.Run.ID != created.ID || result.Run.Status != workflowruntime.RunSuccess {
		t.Fatalf("second advance = %#v, %v", result, err)
	}
}

// 验证外部网络短暂中断只触发有界 retry，恢复后使用新 attempt 完成且不重建 Run。
func TestTransientExecutorPartitionRetriesBoundedly(t *testing.T) {
	now := time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)
	clock := &mutableClock{now: now}
	store := workflowruntime.NewMemoryStore()
	service := workflowruntime.NewService(store, clock, nil)
	created := startRun(t, service, "network-fault")
	executor := &failOnceExecutor{}
	orchestrator := newOrchestrator(t, store, executor, clock, "worker", 2)
	first, err := orchestrator.Advance(context.Background(), "tenant")
	if err != nil || first.Run == nil || first.Run.Status != workflowruntime.RunRunning {
		t.Fatalf("first advance = %#v, %v", first, err)
	}
	clock.Set(now.Add(2 * time.Millisecond))
	second, err := orchestrator.Advance(context.Background(), "tenant")
	if err != nil || second.Run == nil || second.Run.ID != created.ID || second.Run.Status != workflowruntime.RunSuccess {
		t.Fatalf("second advance = %#v, %v", second, err)
	}
	if executor.calls.Load() != 2 {
		t.Fatalf("executor calls = %d", executor.calls.Load())
	}
}

func startRun(t *testing.T, service *workflowruntime.Service, key string) workflowruntime.Run {
	t.Helper()
	result, err := service.StartRun(context.Background(), "tenant", workflowruntime.StartRunRequest{
		Plan:  workflowruntime.PlanRef{ID: "plan", Hash: "hash", EntryNodeID: "end", Executor: "builtin.end"},
		Input: json.RawMessage(`{"value":1}`), IdempotencyKey: key + "-00000001",
	})
	if err != nil {
		t.Fatal(err)
	}
	return result.Run
}

func newOrchestrator(t *testing.T, store workflowruntime.Store, executor workflowruntime.Executor, clock workflowruntime.Clock, worker string, attempts int) *workflowruntime.Orchestrator {
	t.Helper()
	issuer, err := workflowruntime.NewResumeTokenIssuer([]byte("recovery-test-resume-token-secret-32-bytes"))
	if err != nil {
		t.Fatal(err)
	}
	orchestrator, err := workflowruntime.NewOrchestrator(store, executor, clock, nil, workflowruntime.OrchestratorConfig{
		WorkerID: worker, LeaseDuration: time.Second, ResumeTokens: issuer,
		RetryPolicy: workflowruntime.RetryPolicy{MaxAttempts: attempts, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	return orchestrator
}

type mutableClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *mutableClock) Now() time.Time    { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *mutableClock) Set(now time.Time) { c.mu.Lock(); c.now = now; c.mu.Unlock() }

type transientClaimStore struct {
	workflowruntime.Store
	failed atomic.Bool
}

func (s *transientClaimStore) ClaimAttempt(ctx context.Context, request workflowruntime.ClaimAttemptRequest) (*workflowruntime.WorkItem, error) {
	if s.failed.CompareAndSwap(false, true) {
		return nil, errors.New("temporary database partition")
	}
	return s.Store.ClaimAttempt(ctx, request)
}

type finalExecutor struct{}

func (finalExecutor) Execute(context.Context, workflowruntime.ExecutionRequest) (workflowruntime.ExecutionResult, error) {
	return workflowruntime.ExecutionResult{Final: true, Output: &workflowruntime.Output{
		ContentType: "application/json", Size: 11, Inline: json.RawMessage(`{"value":1}`),
	}}, nil
}
func (finalExecutor) Cancel(context.Context, workflowruntime.ExecutionRequest) error { return nil }

type failOnceExecutor struct{ calls atomic.Int64 }

func (e *failOnceExecutor) Execute(context.Context, workflowruntime.ExecutionRequest) (workflowruntime.ExecutionResult, error) {
	if e.calls.Add(1) == 1 {
		return workflowruntime.ExecutionResult{}, &workflowruntime.ExecutionError{Code: "network_partition", Retryable: true}
	}
	return finalExecutor{}.Execute(context.Background(), workflowruntime.ExecutionRequest{})
}
func (*failOnceExecutor) Cancel(context.Context, workflowruntime.ExecutionRequest) error { return nil }

func hasEvent(events []workflowruntime.Event, eventType string) bool {
	for _, event := range events {
		if event.Type == eventType {
			return true
		}
	}
	return false
}
