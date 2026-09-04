package runtime

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

type targetedExpiryStore struct {
	*MemoryStore
	mu          sync.Mutex
	targets     []string
	batchTarget string
}

func (s *targetedExpiryStore) ExpireRun(ctx context.Context, tenantID TenantID, runID string, now time.Time) (bool, error) {
	s.mu.Lock()
	s.targets = append(s.targets, runID)
	s.mu.Unlock()
	return s.MemoryStore.ExpireRun(ctx, tenantID, runID, now)
}

func (s *targetedExpiryStore) ExpireRuns(ctx context.Context, tenantID TenantID, now time.Time, limit int) (int, error) {
	if s.batchTarget != "" {
		expired, err := s.MemoryStore.ExpireRun(ctx, tenantID, s.batchTarget, now)
		if expired {
			return 1, err
		}
		return 0, err
	}
	return s.MemoryStore.ExpireRuns(ctx, tenantID, now, limit)
}

// 验证运行超时被固化为绝对 deadline，且进入幂等指纹并受上下限约束。
func TestStartRunPersistsBoundedDeadline(t *testing.T) {
	now := time.Date(2026, 9, 3, 8, 0, 0, 0, time.UTC)
	service, _, _ := testService(now)
	request := startRequest("deadline-key")
	request.Timeout = 5 * time.Minute
	created, err := service.StartRun(context.Background(), "tenant_a", request)
	if err != nil || created.Run.DeadlineAt == nil || !created.Run.DeadlineAt.Equal(now.Add(5*time.Minute)) {
		t.Fatalf("created=%+v err=%v", created, err)
	}
	conflict := request
	conflict.Timeout = 6 * time.Minute
	if _, err = service.StartRun(context.Background(), "tenant_a", conflict); !IsCode(err, CodeIdempotencyConflict) {
		t.Fatalf("timeout must affect idempotency fingerprint: %v", err)
	}
	tooShort := startRequest("deadline-short")
	tooShort.Timeout = time.Millisecond
	if _, err = service.StartRun(context.Background(), "tenant_a", tooShort); !IsCode(err, CodeInvalidArgument) {
		t.Fatalf("short timeout error=%v", err)
	}
	tooLong := startRequest("deadline-long")
	tooLong.Timeout = maximumRunTimeout + time.Second
	if _, err = service.StartRun(context.Background(), "tenant_a", tooLong); !IsCode(err, CodeInvalidArgument) {
		t.Fatalf("long timeout error=%v", err)
	}
}

// 验证到期扫描原子终止运行、失效租约、关闭中断，并可安全重复执行。
func TestExpireRunsIsTerminalAndIdempotent(t *testing.T) {
	now := time.Date(2026, 9, 3, 8, 0, 0, 0, time.UTC)
	service, store, _ := testService(now)
	request := startRequest("expire-key")
	request.Timeout = time.Minute
	created, err := service.StartRun(context.Background(), "tenant_a", request)
	if err != nil {
		t.Fatal(err)
	}
	work, err := store.ClaimAttempt(context.Background(), ClaimAttemptRequest{TenantID: "tenant_a", WorkerID: "worker", Now: now, LeaseDuration: time.Hour})
	if err != nil || work == nil {
		t.Fatalf("claim=%+v err=%v", work, err)
	}
	count, err := store.ExpireRuns(context.Background(), "tenant_a", now.Add(time.Minute), 10)
	if err != nil || count != 1 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	run, _ := service.GetRun(context.Background(), "tenant_a", created.Run.ID)
	if run.Status != RunError {
		t.Fatalf("status=%s", run.Status)
	}
	if _, err = store.RenewLease(context.Background(), RenewLeaseRequest{TenantID: "tenant_a", RunID: run.ID, AttemptID: work.Attempt.ID, WorkerID: "worker", FencingToken: work.Lease.FencingToken, Now: now.Add(time.Minute), LeaseDuration: time.Minute}); !IsCode(err, CodeLeaseLost) {
		t.Fatalf("expired lease remained valid: %v", err)
	}
	if count, err = store.ExpireRuns(context.Background(), "tenant_a", now.Add(2*time.Minute), 10); err != nil || count != 0 {
		t.Fatalf("repeat count=%d err=%v", count, err)
	}
	events, _ := service.ListEvents(context.Background(), "tenant_a", run.ID, 0, 100)
	if events[len(events)-2].Type != "run.timed_out" || events[len(events)-1].Type != "run.error" {
		t.Fatalf("events=%+v", events)
	}
}

// 验证执行中的 deadline 只终止当前 run，不会误伤同租户更早到期的其他 run。
func TestOrchestratorDeadlineExpiresClaimedRunByID(t *testing.T) {
	now := time.Now().UTC()
	base := NewMemoryStore()
	store := &targetedExpiryStore{MemoryStore: base}
	clock := &fakeClock{now: now}
	service := NewService(store, clock, &sequenceIDs{})
	claimed := startRequest("deadline-claimed")
	claimed.Timeout = minimumRunTimeout + time.Second
	created, err := service.StartRun(context.Background(), "tenant_a", claimed)
	if err != nil {
		t.Fatal(err)
	}
	other := startRequest("deadline-other")
	other.Timeout = minimumRunTimeout
	otherCreated, err := service.StartRun(context.Background(), "tenant_a", other)
	if err != nil {
		t.Fatal(err)
	}
	store.batchTarget = otherCreated.Run.ID
	executor := executorFunc{execute: func(ctx context.Context, _ ExecutionRequest) (ExecutionResult, error) {
		clock.Set(now.Add(minimumRunTimeout + time.Second))
		<-ctx.Done()
		return ExecutionResult{}, ctx.Err()
	}}
	orchestrator, err := NewOrchestrator(store, executor, clock, &sequenceIDs{}, OrchestratorConfig{
		WorkerID: "worker", LeaseDuration: 30 * time.Millisecond,
		RetryPolicy: RetryPolicy{MaxAttempts: 1, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond}, ResumeTokens: testResumeTokens(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := orchestrator.Advance(context.Background(), "tenant_a")
	if err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	targets := append([]string(nil), store.targets...)
	store.mu.Unlock()
	if len(targets) == 0 || targets[len(targets)-1] != created.Run.ID {
		t.Fatalf("targeted expiries=%v, want final target %s", targets, created.Run.ID)
	}
	if result.Run == nil || result.Run.ID != created.Run.ID || result.Run.Status != RunError {
		t.Fatalf("result=%+v", result)
	}
}

type streamingExecutorFunc struct {
	executorFunc
	stream func(context.Context, ExecutionRequest, func(json.RawMessage) error) (ExecutionResult, error)
}

func (e streamingExecutorFunc) ExecuteStream(ctx context.Context, req ExecutionRequest, emit func(json.RawMessage) error) (ExecutionResult, error) {
	return e.stream(ctx, req, emit)
}

// 验证流式 delta 经过 fencing 进入可游标回放事件，最终结果仍由同一次执行提交。
func TestOrchestratorPersistsStreamChunksBeforeCompletion(t *testing.T) {
	now := time.Date(2026, 9, 3, 8, 0, 0, 0, time.UTC)
	service, store, clock := testService(now)
	created, err := service.StartRun(context.Background(), "tenant_a", startRequest("stream-key"))
	if err != nil {
		t.Fatal(err)
	}
	executor := streamingExecutorFunc{
		executorFunc: executorFunc{execute: func(context.Context, ExecutionRequest) (ExecutionResult, error) {
			return ExecutionResult{}, nil
		}},
		stream: func(_ context.Context, _ ExecutionRequest, emit func(json.RawMessage) error) (ExecutionResult, error) {
			if err := emit(json.RawMessage(`{"content":"hel"}`)); err != nil {
				return ExecutionResult{}, err
			}
			if err := emit(json.RawMessage(`{"content":"lo"}`)); err != nil {
				return ExecutionResult{}, err
			}
			return ExecutionResult{Final: true}, nil
		},
	}
	orchestrator, err := NewOrchestrator(store, executor, clock, &sequenceIDs{}, OrchestratorConfig{
		WorkerID: "worker", LeaseDuration: time.Minute,
		RetryPolicy: RetryPolicy{MaxAttempts: 1, BaseDelay: time.Second, MaxDelay: time.Second}, ResumeTokens: testResumeTokens(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := orchestrator.Advance(context.Background(), "tenant_a")
	if err != nil || result.Run == nil || result.Run.Status != RunSuccess {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	events, err := service.ListEvents(context.Background(), "tenant_a", created.Run.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	chunks := 0
	for _, event := range events {
		if event.Type == "stream.chunk" {
			chunks++
			var payload struct {
				AttemptID string          `json:"attemptId"`
				Sequence  int             `json:"sequence"`
				Chunk     json.RawMessage `json:"chunk"`
			}
			if json.Unmarshal(event.Data, &payload) != nil || payload.AttemptID == "" || payload.Sequence != chunks || !json.Valid(payload.Chunk) {
				t.Fatalf("chunk payload=%s", event.Data)
			}
		}
	}
	if chunks != 2 {
		t.Fatalf("stream chunks=%d events=%+v", chunks, events)
	}
}

// 验证同一 attempt 的流式序列可安全重放，但不允许同序列携带不同内容。
func TestAppendStreamChunkDeduplicatesByAttemptSequence(t *testing.T) {
	now := time.Date(2026, 9, 3, 8, 0, 0, 0, time.UTC)
	service, store, _ := testService(now)
	created, err := service.StartRun(context.Background(), "tenant_a", startRequest("stream-dedup-key"))
	if err != nil {
		t.Fatal(err)
	}
	work, err := store.ClaimAttempt(context.Background(), ClaimAttemptRequest{
		TenantID: "tenant_a", WorkerID: "worker", Now: now, LeaseDuration: time.Minute,
	})
	if err != nil || work == nil {
		t.Fatalf("claim=%+v err=%v", work, err)
	}
	request := AppendStreamChunkRequest{
		TenantID: "tenant_a", RunID: created.Run.ID, AttemptID: work.Attempt.ID,
		WorkerID: "worker", FencingToken: work.Lease.FencingToken, Sequence: 1,
		Now: now, Chunk: json.RawMessage(`{"content":"hello"}`),
	}
	first, err := store.AppendStreamChunk(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := store.AppendStreamChunk(context.Background(), request)
	if err != nil || replayed.Cursor != first.Cursor {
		t.Fatalf("replayed=%+v first=%+v err=%v", replayed, first, err)
	}
	request.Chunk = json.RawMessage(`{"content":"different"}`)
	if _, err = store.AppendStreamChunk(context.Background(), request); !IsCode(err, CodeConflict) {
		t.Fatalf("conflicting sequence error=%v", err)
	}
	events, err := service.ListEvents(context.Background(), "tenant_a", created.Run.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	chunks := 0
	for _, event := range events {
		if event.Type == "stream.chunk" {
			chunks++
		}
	}
	if chunks != 1 {
		t.Fatalf("stream chunk events=%d", chunks)
	}
}
