package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Set(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = now
}

type sequenceIDs struct {
	next atomic.Uint64
}

func (g *sequenceIDs) NewID(prefix string) (string, error) {
	return fmt.Sprintf("%s_%d", prefix, g.next.Add(1)), nil
}

type executorFunc struct {
	execute func(context.Context, ExecutionRequest) (ExecutionResult, error)
	cancel  func(context.Context, ExecutionRequest) error
}

func (e executorFunc) Execute(ctx context.Context, req ExecutionRequest) (ExecutionResult, error) {
	return e.execute(ctx, req)
}

func (e executorFunc) Cancel(ctx context.Context, req ExecutionRequest) error {
	if e.cancel == nil {
		return nil
	}
	return e.cancel(ctx, req)
}

func testService(now time.Time) (*Service, *MemoryStore, *fakeClock) {
	store := NewMemoryStore()
	clock := &fakeClock{now: now}
	return NewService(store, clock, &sequenceIDs{}), store, clock
}

func startRequest(key string) StartRunRequest {
	return StartRunRequest{
		Plan:  PlanRef{ID: "plan_1", Hash: "sha256:one", EntryNodeID: "start", Executor: "test"},
		Input: json.RawMessage(`{"message":"hello"}`), IdempotencyKey: key,
	}
}

// 验证 shadow 与普通运行共享幂等键时冲突，而普通运行的既有重放指纹保持不变。
func TestStartRunSeparatesShadowIdempotencySemantics(t *testing.T) {
	service, _, _ := testService(time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC))
	normal := startRequest("shared-shadow-key")
	first, err := service.StartRun(context.Background(), "organization_a", normal)
	if err != nil || !first.Created {
		t.Fatalf("normal start=%+v err=%v", first, err)
	}
	replay, err := service.StartRun(context.Background(), "organization_a", normal)
	if err != nil || replay.Created || replay.Run.ID != first.Run.ID {
		t.Fatalf("normal replay=%+v err=%v", replay, err)
	}
	shadow := normal
	shadow.Plan.Executor = "shadow-v1:builtin.start"
	if _, err = service.StartRun(context.Background(), "organization_a", shadow); !IsCode(err, CodeIdempotencyConflict) {
		t.Fatalf("normal/shadow idempotency error=%v", err)
	}
}

// 验证子工作流创建失败时不会遗留可执行子运行或占用幂等键。
func TestStartSubworkflowCreatesChildAndLineageAtomically(t *testing.T) {
	service, store, clock := testService(time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC))
	parent, err := service.StartRun(context.Background(), "organization_a", startRequest("parent-atomic"))
	if err != nil {
		t.Fatal(err)
	}
	work, err := store.ClaimAttempt(context.Background(), ClaimAttemptRequest{OrganizationID: "organization_a", WorkerID: "worker", Now: clock.Now(), LeaseDuration: time.Minute})
	if err != nil || work == nil {
		t.Fatalf("claim parent: work=%v err=%v", work, err)
	}
	request := StartSubworkflowRequest{ParentRunID: parent.Run.ID, ParentNodeID: work.Attempt.NodeID,
		ParentAttemptID: "missing-attempt", ParentExecutionID: work.Attempt.ExecutionID, Plan: startRequest("unused").Plan,
		Input: json.RawMessage(`{"child":true}`), IdempotencyKey: "child-atomic"}
	if _, err = service.StartSubworkflow(context.Background(), "organization_a", request); !IsCode(err, CodeNotFound) {
		t.Fatalf("expected missing parent attempt, got %v", err)
	}
	standalone := startRequest("child-atomic")
	standalone.Input = request.Input
	created, err := service.StartRun(context.Background(), "organization_a", standalone)
	if err != nil || !created.Created {
		t.Fatalf("failed child must not consume idempotency key: created=%v err=%v", created.Created, err)
	}
}

// 验证父运行取消会在同一租户内递归取消子运行并使其无法继续执行。
func TestParentCancellationPropagatesToSubworkflow(t *testing.T) {
	service, store, clock := testService(time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC))
	parent, err := service.StartRun(context.Background(), "organization_a", startRequest("parent-cancel"))
	if err != nil {
		t.Fatal(err)
	}
	work, _ := store.ClaimAttempt(context.Background(), ClaimAttemptRequest{OrganizationID: "organization_a", WorkerID: "worker", Now: clock.Now(), LeaseDuration: time.Minute})
	child, err := service.StartSubworkflow(context.Background(), "organization_a", StartSubworkflowRequest{
		ParentRunID: parent.Run.ID, ParentNodeID: work.Attempt.NodeID, ParentAttemptID: work.Attempt.ID, ParentExecutionID: work.Attempt.ExecutionID,
		Plan: startRequest("unused").Plan, Input: json.RawMessage(`null`), IdempotencyKey: "child-cancel",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.CancelRun(context.Background(), "organization_a", parent.Run.ID); err != nil {
		t.Fatal(err)
	}
	childRun, err := service.GetRun(context.Background(), "organization_a", child.Run.ID)
	if err != nil || childRun.Status != RunCancelled || childRun.CancelRequestedAt == nil {
		t.Fatalf("child cancellation did not propagate: run=%+v err=%v", childRun, err)
	}
}

// 验证子运行继承父截止时间且父级定点过期会同步 fencing 所有后代。
func TestParentDeadlinePropagatesToSubworkflow(t *testing.T) {
	service, store, clock := testService(time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC))
	request := startRequest("parent-deadline")
	request.Timeout = time.Minute
	parent, err := service.StartRun(context.Background(), "organization_a", request)
	if err != nil {
		t.Fatal(err)
	}
	work, _ := store.ClaimAttempt(context.Background(), ClaimAttemptRequest{OrganizationID: "organization_a", WorkerID: "worker", Now: clock.Now(), LeaseDuration: time.Minute})
	child, err := service.StartSubworkflow(context.Background(), "organization_a", StartSubworkflowRequest{
		ParentRunID: parent.Run.ID, ParentNodeID: work.Attempt.NodeID, ParentAttemptID: work.Attempt.ID, ParentExecutionID: work.Attempt.ExecutionID,
		Plan: startRequest("unused").Plan, Input: json.RawMessage(`null`), IdempotencyKey: "child-deadline",
	})
	if err != nil {
		t.Fatal(err)
	}
	if child.Run.DeadlineAt == nil || !child.Run.DeadlineAt.Equal(*parent.Run.DeadlineAt) {
		t.Fatalf("child deadline=%v want %v", child.Run.DeadlineAt, parent.Run.DeadlineAt)
	}
	if changed, expireErr := store.ExpireRun(context.Background(), "organization_a", parent.Run.ID, *parent.Run.DeadlineAt); expireErr != nil || !changed {
		t.Fatalf("expire parent: changed=%v err=%v", changed, expireErr)
	}
	childRun, _ := service.GetRun(context.Background(), "organization_a", child.Run.ID)
	if childRun.Status != RunError {
		t.Fatalf("child status=%s want ERROR", childRun.Status)
	}
}

// 验证运行状态机只接受设计允许的 CAS 状态转移。
func TestRunStateMachine(t *testing.T) {
	allowed := []struct{ from, to RunStatus }{
		{RunPending, RunRunning}, {RunPending, RunCancelled},
		{RunRunning, RunSuspended}, {RunRunning, RunSuccess}, {RunRunning, RunCancelling},
		{RunSuspended, RunPending}, {RunSuspended, RunCancelled},
		{RunCancelling, RunCancelled},
	}
	for _, transition := range allowed {
		if !CanTransitionRun(transition.from, transition.to) {
			t.Fatalf("expected %s -> %s to be allowed", transition.from, transition.to)
		}
	}
	for _, terminal := range []RunStatus{RunSuccess, RunFailed, RunError, RunCancelled} {
		if !IsTerminalRun(terminal) {
			t.Fatalf("expected %s to be terminal", terminal)
		}
		for _, next := range []RunStatus{RunPending, RunRunning, RunSuspended, RunCancelling, RunSuccess, RunFailed, RunError, RunCancelled} {
			if CanTransitionRun(terminal, next) {
				t.Fatalf("terminal transition %s -> %s must be rejected", terminal, next)
			}
		}
	}
}

// 验证同一租户并发重放幂等键时只创建一个运行实例。
func TestStartRunConcurrentIdempotency(t *testing.T) {
	service, _, _ := testService(time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC))
	const goroutines = 64
	results := make(chan CreateRunResult, goroutines)
	errors := make(chan error, goroutines)
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := service.StartRun(context.Background(), "organization_a", startRequest("same-key"))
			results <- result
			errors <- err
		}()
	}
	wg.Wait()
	close(results)
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatalf("StartRun returned error: %v", err)
		}
	}
	var runID string
	created := 0
	for result := range results {
		if runID == "" {
			runID = result.Run.ID
		}
		if result.Run.ID != runID {
			t.Fatalf("idempotent requests returned different runs: %s and %s", runID, result.Run.ID)
		}
		if result.Created {
			created++
		}
	}
	if created != 1 {
		t.Fatalf("created %d runs, want 1", created)
	}
	conflict := startRequest("same-key")
	conflict.Input = json.RawMessage(`{"message":"different"}`)
	if _, err := service.StartRun(context.Background(), "organization_a", conflict); !IsCode(err, CodeIdempotencyConflict) {
		t.Fatalf("expected idempotency conflict, got %v", err)
	}
}

// 验证相同幂等键按租户隔离，跨租户读取不会泄漏运行数据。
func TestOrganizationIsolation(t *testing.T) {
	service, _, _ := testService(time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC))
	a, err := service.StartRun(context.Background(), "organization_a", startRequest("key"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := service.StartRun(context.Background(), "organization_b", startRequest("key"))
	if err != nil {
		t.Fatal(err)
	}
	if a.Run.ID == b.Run.ID {
		t.Fatal("different organizations unexpectedly shared a run")
	}
	if _, err := service.GetRun(context.Background(), "organization_b", a.Run.ID); !IsCode(err, CodeNotFound) {
		t.Fatalf("cross-organization get returned %v", err)
	}
	if _, err := service.ListInterrupts(context.Background(), "organization_b", a.Run.ID); !IsCode(err, CodeNotFound) {
		t.Fatalf("cross-organization interrupt list returned %v", err)
	}
}

// 验证租约过期后新 worker 接管并以 fencing token 拒绝旧 worker 提交。
func TestLeaseExpiryTakeoverRejectsStaleFencingToken(t *testing.T) {
	service, store, clock := testService(time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC))
	created, err := service.StartRun(context.Background(), "organization_a", startRequest("lease"))
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.ClaimAttempt(context.Background(), ClaimAttemptRequest{
		OrganizationID: "organization_a", WorkerID: "worker_1", Now: clock.Now(), LeaseDuration: time.Minute,
	})
	if err != nil || first == nil {
		t.Fatalf("first claim: work=%v err=%v", first, err)
	}
	if first.Takeover {
		t.Fatal("initial claim was marked as a takeover")
	}
	beforeExpiry, err := store.ClaimAttempt(context.Background(), ClaimAttemptRequest{
		OrganizationID: "organization_a", WorkerID: "worker_2", Now: clock.Now().Add(30 * time.Second), LeaseDuration: time.Minute,
	})
	if err != nil || beforeExpiry != nil {
		t.Fatalf("claim before expiry: work=%v err=%v", beforeExpiry, err)
	}
	takeoverAt := clock.Now().Add(time.Minute)
	second, err := store.ClaimAttempt(context.Background(), ClaimAttemptRequest{
		OrganizationID: "organization_a", WorkerID: "worker_2", Now: takeoverAt, LeaseDuration: time.Minute,
	})
	if err != nil || second == nil {
		t.Fatalf("takeover claim: work=%v err=%v", second, err)
	}
	if !second.Takeover {
		t.Fatal("expired running lease was not marked as a takeover")
	}
	if second.Lease.FencingToken <= first.Lease.FencingToken {
		t.Fatal("takeover did not advance fencing token")
	}
	events, err := store.ListEvents(context.Background(), "organization_a", created.Run.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	eventCounts := map[string]int{}
	for index, event := range events {
		eventCounts[event.Type]++
		if index > 0 && event.Cursor != events[index-1].Cursor+1 {
			t.Fatalf("event cursors are not monotonic: %+v", events)
		}
	}
	if eventCounts["run.started"] != 1 || eventCounts["attempt.claimed"] != 2 || eventCounts["attempt.lease_taken_over"] != 1 {
		t.Fatalf("unexpected claim events: counts=%v events=%+v", eventCounts, events)
	}
	_, err = store.CompleteAttempt(context.Background(), CompleteAttemptRequest{
		OrganizationID: "organization_a", RunID: created.Run.ID, AttemptID: first.Attempt.ID,
		WorkerID: "worker_1", FencingToken: first.Lease.FencingToken,
		Now: takeoverAt.Add(time.Second), RunStatus: RunSuccess,
	})
	if !IsCode(err, CodeStaleFencingToken) {
		t.Fatalf("stale worker completion returned %v", err)
	}
	completed, err := store.CompleteAttempt(context.Background(), CompleteAttemptRequest{
		OrganizationID: "organization_a", RunID: created.Run.ID, AttemptID: second.Attempt.ID,
		WorkerID: "worker_2", FencingToken: second.Lease.FencingToken,
		Now: takeoverAt.Add(time.Second), RunStatus: RunSuccess,
	})
	if err != nil || completed.Status != RunSuccess {
		t.Fatalf("current worker completion: run=%+v err=%v", completed, err)
	}
}

// 验证运行中取消先进入 CANCELLING，并由 worker 调用 executor cancel 后终结。
func TestCancellationIntentAndWorkerFinalization(t *testing.T) {
	service, store, clock := testService(time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC))
	created, err := service.StartRun(context.Background(), "organization_a", startRequest("cancel"))
	if err != nil {
		t.Fatal(err)
	}
	work, err := store.ClaimAttempt(context.Background(), ClaimAttemptRequest{
		OrganizationID: "organization_a", WorkerID: "worker_1", Now: clock.Now(), LeaseDuration: time.Minute,
	})
	if err != nil || work == nil {
		t.Fatalf("claim: work=%v err=%v", work, err)
	}
	cancelled, err := service.CancelRun(context.Background(), "organization_a", created.Run.ID)
	if err != nil || cancelled.Run.Status != RunCancelling {
		t.Fatalf("cancel intent: result=%+v err=%v", cancelled, err)
	}
	var cancelCalls atomic.Int64
	executor := executorFunc{
		execute: func(context.Context, ExecutionRequest) (ExecutionResult, error) {
			t.Fatal("Execute must not run for a cancelling run")
			return ExecutionResult{}, nil
		},
		cancel: func(context.Context, ExecutionRequest) error { cancelCalls.Add(1); return nil },
	}
	orchestrator, err := NewOrchestrator(store, executor, clock, &sequenceIDs{}, OrchestratorConfig{
		WorkerID: "worker_2", LeaseDuration: time.Minute,
		RetryPolicy:  RetryPolicy{MaxAttempts: 1, BaseDelay: time.Second, MaxDelay: time.Second},
		ResumeTokens: testResumeTokens(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	clock.Set(clock.Now().Add(time.Minute))
	advanced, err := orchestrator.Advance(context.Background(), "organization_a")
	if err != nil || advanced.Run == nil || advanced.Run.Status != RunCancelled {
		t.Fatalf("cancel finalization: result=%+v err=%v", advanced, err)
	}
	if cancelCalls.Load() != 1 {
		t.Fatalf("Cancel calls = %d, want 1", cancelCalls.Load())
	}
	repeated, err := service.CancelRun(context.Background(), "organization_a", created.Run.ID)
	if err != nil || !repeated.AlreadyRequested {
		t.Fatalf("repeated cancellation: result=%+v err=%v", repeated, err)
	}
}

// 验证取消 intent 与成功结果竞争时，内存适配器丢弃结果和后继并原子终结为 CANCELLED。
func TestMemoryStoreCancellationWinsSuccessfulCompletion(t *testing.T) {
	service, store, clock := testService(time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC))
	created, err := service.StartRun(context.Background(), "organization_a", startRequest("cancel-wins-completion"))
	if err != nil {
		t.Fatal(err)
	}
	work, err := store.ClaimAttempt(context.Background(), ClaimAttemptRequest{
		OrganizationID: "organization_a", WorkerID: "worker_1", Now: clock.Now(), LeaseDuration: time.Minute,
	})
	if err != nil || work == nil {
		t.Fatalf("claim: work=%+v err=%v", work, err)
	}
	if _, err = service.CancelRun(context.Background(), "organization_a", created.Run.ID); err != nil {
		t.Fatal(err)
	}
	completed, err := store.CompleteAttempt(context.Background(), CompleteAttemptRequest{
		OrganizationID: "organization_a", RunID: created.Run.ID, AttemptID: work.Attempt.ID,
		WorkerID: "worker_1", FencingToken: work.Lease.FencingToken, Now: clock.Now(),
		RunStatus: RunSuccess,
		Output:    &Output{Inline: json.RawMessage(`{"must":"discard"}`)},
		Snapshot:  &Snapshot{Data: json.RawMessage(`{"must":"discard"}`)},
		Next: []NewAttempt{{
			NodeID: "must_not_run", Executor: "test", Input: json.RawMessage(`null`),
		}},
		Events: []Event{{Type: "executor.succeeded"}},
	})
	if err != nil || completed.Status != RunCancelled {
		t.Fatalf("completion: run=%+v err=%v", completed, err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.outputs) != 0 || len(store.snapshots[scoped("organization_a", created.Run.ID)]) != 0 {
		t.Fatalf("cancelled completion persisted output/snapshot: outputs=%d snapshots=%d", len(store.outputs), len(store.snapshots))
	}
	if len(store.attemptOrder) != 1 || store.attempts[scoped("organization_a", work.Attempt.ID)].Status != AttemptCancelled {
		t.Fatalf("cancelled completion created successor or kept active attempt: order=%v attempts=%v", store.attemptOrder, store.attempts)
	}
	node := store.nodes[nodeScoped("organization_a", created.Run.ID, work.Attempt.NodeID)]
	if node.Status != NodeCancelled {
		t.Fatalf("node status=%s", node.Status)
	}
	events := store.events[scoped("organization_a", created.Run.ID)]
	if events[len(events)-1].Type != "run.cancelled" {
		t.Fatalf("terminal event=%q events=%+v", events[len(events)-1].Type, events)
	}
	for _, event := range events {
		if event.Type == "run.succeeded" || event.Type == "executor.succeeded" {
			t.Fatalf("cancelled completion leaked success event: %+v", events)
		}
	}
}

// 验证 Run 优先级只接受平台约定的有界范围，避免队列排序被极端输入操纵。
func TestStartRunRejectsOutOfRangePriority(t *testing.T) {
	service := NewService(NewMemoryStore(), &fakeClock{now: time.Unix(100, 0)}, &sequenceIDs{})
	_, err := service.StartRun(context.Background(), "organization_a", StartRunRequest{
		Plan:  PlanRef{ID: "plan_1", Hash: "hash_1", EntryNodeID: "start", Executor: "builtin"},
		Input: json.RawMessage(`{}`), IdempotencyKey: "priority-key", Priority: 1001,
	})
	if !IsCode(err, CodeInvalidArgument) {
		t.Fatalf("StartRun() error = %v", err)
	}
}

// 验证相同幂等键不能通过修改优先级改变已经接受的排队请求。
func TestStartRunPriorityParticipatesInIdempotency(t *testing.T) {
	service := NewService(NewMemoryStore(), &fakeClock{now: time.Unix(100, 0)}, &sequenceIDs{})
	request := StartRunRequest{
		Plan:  PlanRef{ID: "plan_1", Hash: "hash_1", EntryNodeID: "start", Executor: "builtin"},
		Input: json.RawMessage(`{}`), IdempotencyKey: "priority-key", Priority: 10,
	}
	if _, err := service.StartRun(context.Background(), "organization_a", request); err != nil {
		t.Fatal(err)
	}
	request.Priority = 20
	if _, err := service.StartRun(context.Background(), "organization_a", request); !IsCode(err, CodeIdempotencyConflict) {
		t.Fatalf("StartRun() replay error = %v", err)
	}
}
