package runtime

import (
	"context"
	"testing"
	"time"
)

// 验证 worker 将失联租约接管事实传给执行器，并把不安全重放持久化为不可重试终态。
func TestOrchestratorPersistsUnsafeLeaseTakeoverAsTerminalFailure(t *testing.T) {
	service, store, clock := testService(time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC))
	created, err := service.StartRun(context.Background(), "tenant_a", startRequest("unsafe-takeover"))
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.ClaimAttempt(context.Background(), ClaimAttemptRequest{
		TenantID: "tenant_a", WorkerID: "lost-worker", Now: clock.Now(), LeaseDuration: time.Minute,
	})
	if err != nil || first == nil || first.Takeover {
		t.Fatalf("first claim = %+v, err = %v", first, err)
	}

	var calls int
	executor := executorFunc{execute: func(_ context.Context, request ExecutionRequest) (ExecutionResult, error) {
		calls++
		if !request.Takeover || request.ExecutionID != first.Attempt.ExecutionID {
			t.Fatalf("takeover request = %+v", request)
		}
		return ExecutionResult{}, &ExecutionError{Code: "unsafe_lease_takeover", Retryable: false}
	}}
	orchestrator, err := NewOrchestrator(store, executor, clock, &sequenceIDs{}, OrchestratorConfig{
		WorkerID: "recovery-worker", LeaseDuration: time.Minute,
		RetryPolicy:  RetryPolicy{MaxAttempts: 5, BaseDelay: time.Second, MaxDelay: time.Minute},
		ResumeTokens: testResumeTokens(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	clock.Set(clock.Now().Add(time.Minute))
	result, err := orchestrator.Advance(context.Background(), "tenant_a")
	if err != nil || result.Run == nil || result.Run.Status != RunFailed || calls != 1 {
		t.Fatalf("advance = %+v, calls = %d, err = %v", result, calls, err)
	}
	details, err := store.GetRunDetails(context.Background(), "tenant_a", created.Run.ID)
	if err != nil || len(details.Nodes) != 1 || len(details.Nodes[0].Attempts) != 1 ||
		details.Nodes[0].Attempts[0].Attempt.ErrorCode != "unsafe_lease_takeover" {
		t.Fatalf("terminal details = %+v, err = %v", details, err)
	}
	next, err := store.ClaimAttempt(context.Background(), ClaimAttemptRequest{
		TenantID: "tenant_a", WorkerID: "another-worker", Now: clock.Now().Add(time.Hour), LeaseDuration: time.Minute,
	})
	if err != nil || next != nil {
		t.Fatalf("terminal run was reclaimable: work=%+v err=%v", next, err)
	}
}
