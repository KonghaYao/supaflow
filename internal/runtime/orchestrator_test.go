package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type renewalStore struct {
	Store
	calls atomic.Int64
	fail  bool
}

func testResumeTokens(t *testing.T) *ResumeTokenIssuer {
	t.Helper()
	issuer, err := NewResumeTokenIssuer([]byte("test-resume-token-secret-32-bytes!"))
	if err != nil {
		t.Fatal(err)
	}
	return issuer
}

func (s *renewalStore) RenewLease(ctx context.Context, req RenewLeaseRequest) (Lease, error) {
	s.calls.Add(1)
	if s.fail {
		return Lease{}, errorf(CodeLeaseLost, "test lease renewal failed")
	}
	return s.Store.RenewLease(ctx, req)
}

// 验证审批 token 只保存摘要、仅能消费一次，并在租户边界内恢复运行。
func TestApprovalSuspendAndSingleUseResume(t *testing.T) {
	service, store, clock := testService(time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC))
	created, err := service.StartRun(context.Background(), "tenant_a", startRequest("approval"))
	if err != nil {
		t.Fatal(err)
	}
	var executeCalls atomic.Int64
	executor := executorFunc{execute: func(_ context.Context, req ExecutionRequest) (ExecutionResult, error) {
		if executeCalls.Add(1) == 1 {
			return ExecutionResult{Suspend: &Suspension{
				Kind: "human_approval", ExpiresIn: time.Hour, Snapshot: json.RawMessage(`{"step":1}`),
			}}, nil
		}
		var decision ApprovalDecision
		if err := json.Unmarshal(req.ResumeData, &decision); err != nil {
			t.Fatalf("resume decision is not valid JSON: %v", err)
		}
		if !decision.Approved || decision.DecidedBy != "user_approver" {
			t.Fatalf("executor received wrong resume decision: %+v", decision)
		}
		if string(req.Snapshot) != `{"step":1}` {
			t.Fatalf("executor received wrong snapshot: %s", req.Snapshot)
		}
		if req.Plan.ID != "plan_1" || req.Plan.Hash != "sha256:one" {
			t.Fatalf("executor lost immutable plan binding: %+v", req.Plan)
		}
		return ExecutionResult{Final: true}, nil
	}}
	orchestrator, err := NewOrchestrator(store, executor, clock, &sequenceIDs{}, OrchestratorConfig{
		WorkerID: "worker_1", LeaseDuration: time.Minute,
		RetryPolicy:  RetryPolicy{MaxAttempts: 2, BaseDelay: time.Second, MaxDelay: time.Minute},
		ResumeTokens: testResumeTokens(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	advanced, err := orchestrator.Advance(context.Background(), "tenant_a")
	if err != nil || advanced.Interrupt == nil {
		t.Fatalf("suspend: result=%+v err=%v", advanced, err)
	}
	approvalToken, err := testResumeTokens(t).Issue(*advanced.Interrupt)
	if err != nil {
		t.Fatal(err)
	}
	run, err := service.GetRun(context.Background(), "tenant_a", created.Run.ID)
	if err != nil || run.Status != RunSuspended {
		t.Fatalf("suspended run: run=%+v err=%v", run, err)
	}
	pending, err := service.ListInterrupts(context.Background(), "tenant_a", run.ID)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending interrupts: %v err=%v", pending, err)
	}
	if pending[0].TokenDigest != ([32]byte{}) {
		t.Fatal("ListInterrupts leaked token digest")
	}
	if _, err := service.ListInterrupts(context.Background(), "tenant_b", run.ID); !IsCode(err, CodeNotFound) {
		t.Fatalf("cross-tenant interrupt list returned %v", err)
	}
	if _, err := service.ResolveApproval(context.Background(), "tenant_a", ResolveApprovalCommand{ActorID: "user_approver", InterruptID: advanced.Interrupt.ID, ResumeToken: "wrong", IdempotencyKey: "approval-wrong-0001", Decision: ApprovalDecision{Approved: true}}); !IsCode(err, CodeApprovalInvalid) {
		t.Fatalf("wrong token returned %v", err)
	}
	command := ResolveApprovalCommand{ActorID: "user_approver", InterruptID: advanced.Interrupt.ID, ResumeToken: approvalToken, IdempotencyKey: "approval-resolve-0001", Decision: ApprovalDecision{Approved: true}}
	resolved, err := service.ResolveApproval(context.Background(), "tenant_a", command)
	if err != nil || resolved.Run.Status != RunPending {
		t.Fatalf("resolve: result=%+v err=%v", resolved, err)
	}
	var storedDecision ApprovalDecision
	if err := json.Unmarshal(resolved.Interrupt.Decision, &storedDecision); err != nil || storedDecision.DecidedBy != "user_approver" {
		t.Fatalf("approval actor was not audited: decision=%+v err=%v", storedDecision, err)
	}
	replayed, err := service.ResolveApproval(context.Background(), "tenant_a", command)
	if err != nil || replayed.Run.ID != resolved.Run.ID || replayed.Run.Version != resolved.Run.Version {
		t.Fatalf("lost-response replay: result=%+v err=%v", replayed, err)
	}
	conflict := command
	conflict.Decision.Comment = "different request"
	if _, err := service.ResolveApproval(context.Background(), "tenant_a", conflict); !IsCode(err, CodeIdempotencyConflict) {
		t.Fatalf("idempotency conflict returned %v", err)
	}
	consumed := command
	consumed.IdempotencyKey = "approval-resolve-0002"
	if _, err := service.ResolveApproval(context.Background(), "tenant_a", consumed); !IsCode(err, CodeApprovalConsumed) {
		t.Fatalf("second consumption with a new key returned %v", err)
	}
	pending, err = service.ListInterrupts(context.Background(), "tenant_a", run.ID)
	if err != nil || len(pending) != 0 {
		t.Fatalf("resolved interrupt remained pending: %v err=%v", pending, err)
	}
	resumed, err := orchestrator.Advance(context.Background(), "tenant_a")
	if err != nil || resumed.Run == nil || resumed.Run.Status != RunSuccess {
		t.Fatalf("resumed execution: result=%+v err=%v", resumed, err)
	}
	if executeCalls.Load() != 2 {
		t.Fatalf("executor calls = %d, want suspend then resume", executeCalls.Load())
	}
}

// 验证过期审批 token 不会恢复已暂停的运行。
func TestApprovalExpiry(t *testing.T) {
	service, store, clock := testService(time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC))
	_, err := service.StartRun(context.Background(), "tenant_a", startRequest("expired-approval"))
	if err != nil {
		t.Fatal(err)
	}
	executor := executorFunc{execute: func(context.Context, ExecutionRequest) (ExecutionResult, error) {
		return ExecutionResult{Suspend: &Suspension{Kind: "human_approval", ExpiresIn: time.Minute}}, nil
	}}
	orchestrator, err := NewOrchestrator(store, executor, clock, &sequenceIDs{}, OrchestratorConfig{
		WorkerID: "worker_1", LeaseDuration: time.Minute,
		RetryPolicy:  RetryPolicy{MaxAttempts: 1, BaseDelay: time.Second, MaxDelay: time.Second},
		ResumeTokens: testResumeTokens(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	advanced, err := orchestrator.Advance(context.Background(), "tenant_a")
	if err != nil {
		t.Fatal(err)
	}
	token, err := testResumeTokens(t).Issue(*advanced.Interrupt)
	if err != nil {
		t.Fatal(err)
	}
	clock.Set(clock.Now().Add(time.Minute))
	if _, err := service.ResolveApproval(context.Background(), "tenant_a", ResolveApprovalCommand{ActorID: "user_approver", InterruptID: advanced.Interrupt.ID, ResumeToken: token, IdempotencyKey: "approval-expired-0001", Decision: ApprovalDecision{Approved: true}}); !IsCode(err, CodeApprovalExpired) {
		t.Fatalf("expired token returned %v", err)
	}
}

// 验证取消已暂停运行会关闭待审批项，旧 token 不能再次恢复运行。
func TestCancelSuspendedRunClosesApproval(t *testing.T) {
	service, store, clock := testService(time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC))
	created, err := service.StartRun(context.Background(), "tenant_a", startRequest("cancel-approval"))
	if err != nil {
		t.Fatal(err)
	}
	executor := executorFunc{execute: func(context.Context, ExecutionRequest) (ExecutionResult, error) {
		return ExecutionResult{Suspend: &Suspension{Kind: "human_approval", ExpiresIn: time.Hour}}, nil
	}}
	orchestrator, err := NewOrchestrator(store, executor, clock, &sequenceIDs{}, OrchestratorConfig{
		WorkerID: "worker_1", LeaseDuration: time.Minute,
		RetryPolicy:  RetryPolicy{MaxAttempts: 1, BaseDelay: time.Second, MaxDelay: time.Second},
		ResumeTokens: testResumeTokens(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	advanced, err := orchestrator.Advance(context.Background(), "tenant_a")
	if err != nil {
		t.Fatal(err)
	}
	token, err := testResumeTokens(t).Issue(*advanced.Interrupt)
	if err != nil {
		t.Fatal(err)
	}
	cancelled, err := service.CancelRun(context.Background(), "tenant_a", created.Run.ID)
	if err != nil || cancelled.Run.Status != RunCancelled {
		t.Fatalf("cancel suspended run: result=%+v err=%v", cancelled, err)
	}
	pending, err := service.ListInterrupts(context.Background(), "tenant_a", created.Run.ID)
	if err != nil || len(pending) != 0 {
		t.Fatalf("cancelled run has pending approvals: %v err=%v", pending, err)
	}
	if _, err := service.ResolveApproval(context.Background(), "tenant_a", ResolveApprovalCommand{ActorID: "user_approver", InterruptID: advanced.Interrupt.ID, ResumeToken: token, IdempotencyKey: "approval-cancelled-0001", Decision: ApprovalDecision{Approved: true}}); !IsCode(err, CodeApprovalConsumed) {
		t.Fatalf("cancelled approval token returned %v", err)
	}
}

// 验证事件 cursor 单调递增，afterCursor 分页不会重复或跳过事件。
func TestEventCursorResume(t *testing.T) {
	service, store, clock := testService(time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC))
	created, err := service.StartRun(context.Background(), "tenant_a", startRequest("events"))
	if err != nil {
		t.Fatal(err)
	}
	executor := executorFunc{execute: func(context.Context, ExecutionRequest) (ExecutionResult, error) {
		return ExecutionResult{Final: true}, nil
	}}
	orchestrator, err := NewOrchestrator(store, executor, clock, &sequenceIDs{}, OrchestratorConfig{
		WorkerID: "worker_1", LeaseDuration: time.Minute,
		RetryPolicy:  RetryPolicy{MaxAttempts: 1, BaseDelay: time.Second, MaxDelay: time.Second},
		ResumeTokens: testResumeTokens(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orchestrator.Advance(context.Background(), "tenant_a"); err != nil {
		t.Fatal(err)
	}
	first, err := service.ListEvents(context.Background(), "tenant_a", created.Run.ID, 0, 2)
	if err != nil || len(first) != 2 {
		t.Fatalf("first event page: %v err=%v", first, err)
	}
	second, err := service.ListEvents(context.Background(), "tenant_a", created.Run.ID, first[len(first)-1].Cursor, 100)
	if err != nil || len(second) == 0 {
		t.Fatalf("second event page: %v err=%v", second, err)
	}
	all := append(first, second...)
	for i, event := range all {
		want := uint64(i + 1)
		if event.Cursor != want {
			t.Fatalf("cursor[%d] = %d, want %d", i, event.Cursor, want)
		}
		if event.TenantID != "tenant_a" || event.RunID != created.Run.ID {
			t.Fatalf("event scope is wrong: %+v", event)
		}
	}
	remaining, err := service.ListEvents(context.Background(), "tenant_a", created.Run.ID, all[len(all)-1].Cursor, 100)
	if err != nil || len(remaining) != 0 {
		t.Fatalf("events after tail: %v err=%v", remaining, err)
	}
}

// 验证可重试错误采用有限次数和延迟退避，达到上限后终结为 FAILED。
func TestBoundedRetry(t *testing.T) {
	service, store, clock := testService(time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC))
	created, err := service.StartRun(context.Background(), "tenant_a", startRequest("retry"))
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	var executionID string
	executor := executorFunc{execute: func(_ context.Context, request ExecutionRequest) (ExecutionResult, error) {
		calls.Add(1)
		if executionID == "" {
			executionID = request.ExecutionID
		} else if request.ExecutionID != executionID {
			t.Fatalf("retry execution id = %q, want stable %q", request.ExecutionID, executionID)
		}
		return ExecutionResult{}, &ExecutionError{Code: "temporary", Retryable: true}
	}}
	orchestrator, err := NewOrchestrator(store, executor, clock, &sequenceIDs{}, OrchestratorConfig{
		WorkerID: "worker_1", LeaseDuration: time.Minute,
		RetryPolicy:  RetryPolicy{MaxAttempts: 3, BaseDelay: time.Second, MaxDelay: 4 * time.Second},
		ResumeTokens: testResumeTokens(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= 3; attempt++ {
		advanced, err := orchestrator.Advance(context.Background(), "tenant_a")
		if err != nil || !advanced.Worked {
			t.Fatalf("advance %d: result=%+v err=%v", attempt, advanced, err)
		}
		if attempt < 3 {
			idle, err := orchestrator.Advance(context.Background(), "tenant_a")
			if err != nil || idle.Worked {
				t.Fatalf("retry ran before backoff: result=%+v err=%v", idle, err)
			}
			clock.Set(clock.Now().Add(time.Duration(1<<(attempt-1)) * time.Second))
		}
	}
	run, err := service.GetRun(context.Background(), "tenant_a", created.Run.ID)
	if err != nil || run.Status != RunFailed {
		t.Fatalf("final run: %+v err=%v", run, err)
	}
	if calls.Load() != 3 {
		t.Fatalf("executor calls = %d, want 3", calls.Load())
	}
}

// 验证长节点执行期间 worker 会主动续租，避免健康执行被另一 worker 接管。
func TestOrchestratorRenewsLeaseDuringExecution(t *testing.T) {
	base := NewMemoryStore()
	service := NewService(base, nil, &sequenceIDs{})
	created, err := service.StartRun(context.Background(), "tenant_a", startRequest("heartbeat"))
	if err != nil {
		t.Fatal(err)
	}
	store := &renewalStore{Store: base}
	executor := executorFunc{execute: func(context.Context, ExecutionRequest) (ExecutionResult, error) {
		time.Sleep(80 * time.Millisecond)
		return ExecutionResult{Final: true}, nil
	}}
	orchestrator, err := NewOrchestrator(store, executor, nil, &sequenceIDs{}, OrchestratorConfig{
		WorkerID: "worker_heartbeat", LeaseDuration: 30 * time.Millisecond,
		RetryPolicy:  RetryPolicy{MaxAttempts: 1, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond},
		ResumeTokens: testResumeTokens(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := orchestrator.Advance(context.Background(), "tenant_a")
	if err != nil || result.Run == nil || result.Run.ID != created.Run.ID || result.Run.Status != RunSuccess {
		t.Fatalf("advance=%+v err=%v", result, err)
	}
	if store.calls.Load() < 2 {
		t.Fatalf("renew calls=%d, want at least 2", store.calls.Load())
	}
}

// 验证续租失败会取消 executor 且不会用失效 fencing token 提交结果。
func TestOrchestratorCancelsExecutionWhenLeaseRenewalFails(t *testing.T) {
	base := NewMemoryStore()
	service := NewService(base, nil, &sequenceIDs{})
	created, err := service.StartRun(context.Background(), "tenant_a", startRequest("heartbeat-failure"))
	if err != nil {
		t.Fatal(err)
	}
	store := &renewalStore{Store: base, fail: true}
	var cancelled atomic.Bool
	executor := executorFunc{execute: func(ctx context.Context, _ ExecutionRequest) (ExecutionResult, error) {
		<-ctx.Done()
		cancelled.Store(true)
		return ExecutionResult{}, ctx.Err()
	}}
	orchestrator, err := NewOrchestrator(store, executor, nil, &sequenceIDs{}, OrchestratorConfig{
		WorkerID: "worker_heartbeat", LeaseDuration: 15 * time.Millisecond,
		RetryPolicy:  RetryPolicy{MaxAttempts: 1, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond},
		ResumeTokens: testResumeTokens(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := orchestrator.Advance(context.Background(), "tenant_a")
	if err == nil || !IsCode(err, CodeLeaseLost) || !result.Worked || !cancelled.Load() {
		t.Fatalf("advance=%+v cancelled=%v err=%v", result, cancelled.Load(), err)
	}
	run, getErr := service.GetRun(context.Background(), "tenant_a", created.Run.ID)
	if getErr != nil {
		t.Fatal(getErr)
	}
	if run.Status != RunRunning {
		t.Fatalf("run status=%s, stale worker mutated terminal state", run.Status)
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("expected lease error, got executor cancellation: %v", err)
	}
}

// 验证运行中收到取消意图后 heartbeat 会显式通知 executor、终止执行并原子落为 CANCELLED。
func TestOrchestratorPropagatesActiveCancellationWithoutRetry(t *testing.T) {
	store := NewMemoryStore()
	service := NewService(store, nil, &sequenceIDs{})
	created, err := service.StartRun(context.Background(), "tenant_a", startRequest("active-cancel"))
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	executionCancelled := make(chan struct{})
	var executeCalls atomic.Int64
	var cancelCalls atomic.Int64
	executor := executorFunc{
		execute: func(ctx context.Context, _ ExecutionRequest) (ExecutionResult, error) {
			executeCalls.Add(1)
			close(started)
			<-ctx.Done()
			close(executionCancelled)
			return ExecutionResult{}, ctx.Err()
		},
		cancel: func(context.Context, ExecutionRequest) error {
			cancelCalls.Add(1)
			return errors.New("remote runner already stopped")
		},
	}
	resumeTokens, err := NewResumeTokenIssuer([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	orchestrator, err := NewOrchestrator(store, executor, nil, &sequenceIDs{}, OrchestratorConfig{
		WorkerID: "worker_active_cancel", LeaseDuration: 30 * time.Millisecond,
		RetryPolicy:  RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: time.Second},
		ResumeTokens: resumeTokens,
	})
	if err != nil {
		t.Fatal(err)
	}
	advanced := make(chan AdvanceResult, 1)
	advanceErrors := make(chan error, 1)
	go func() {
		result, advanceErr := orchestrator.Advance(context.Background(), "tenant_a")
		advanced <- result
		advanceErrors <- advanceErr
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("executor did not start")
	}
	cancelled, err := service.CancelRun(context.Background(), "tenant_a", created.Run.ID)
	if err != nil || cancelled.Run.Status != RunCancelling {
		t.Fatalf("cancel intent: result=%+v err=%v", cancelled, err)
	}
	select {
	case result := <-advanced:
		if advanceErr := <-advanceErrors; advanceErr != nil || result.Run == nil || result.Run.Status != RunCancelled {
			t.Fatalf("advance=%+v err=%v", result, advanceErr)
		}
	case <-time.After(time.Second):
		t.Fatal("active cancellation did not finish")
	}
	select {
	case <-executionCancelled:
	default:
		t.Fatal("executor context was not cancelled")
	}
	if cancelCalls.Load() != 1 || executeCalls.Load() != 1 {
		t.Fatalf("Cancel calls=%d Execute calls=%d, want one each", cancelCalls.Load(), executeCalls.Load())
	}
	idle, err := orchestrator.Advance(context.Background(), "tenant_a")
	if err != nil || idle.Worked {
		t.Fatalf("cancelled run retried: result=%+v err=%v", idle, err)
	}
}
