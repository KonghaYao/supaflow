package runtime

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

// 验证两个并发分支同时到达 fan-in 时 CAS 只生成一个 join attempt。
func TestMemoryStoreConcurrentFanInCreatesOneAttempt(t *testing.T) {
	store, now := NewMemoryStore(), time.Unix(50, 0).UTC()
	_, _ = store.CreateRun(context.Background(), CreateRunRequest{Run: Run{
		OrganizationID: "organization", ID: "run", Plan: PlanRef{ID: "plan", Hash: "hash", EntryNodeID: "start", Executor: "start"},
		Status: RunPending, Input: json.RawMessage(`null`), IdempotencyKey: "idem", RequestFingerprint: "fingerprint", CreatedAt: now, UpdatedAt: now,
	}})
	start := claimDAGAttempt(t, store, now)
	completeDAGAttempt(t, store, start, now, []NewAttempt{{NodeID: "left", Executor: "work"}, {NodeID: "right", Executor: "work"}})
	left, right := claimDAGAttempt(t, store, now), claimDAGAttempt(t, store, now)
	works := []*WorkItem{left, right}
	var wait sync.WaitGroup
	errors := make(chan error, 2)
	for index, work := range works {
		wait.Add(1)
		go func(index int, work *WorkItem) {
			defer wait.Done()
			edge := []string{"left_join", "right_join"}[index]
			_, err := store.CompleteAttempt(context.Background(), CompleteAttemptRequest{
				OrganizationID: "organization", RunID: "run", AttemptID: work.Attempt.ID, WorkerID: "worker", FencingToken: work.Lease.FencingToken, Now: now,
				RunStatus: RunRunning, Next: []NewAttempt{{NodeID: "join", Executor: "join", Input: json.RawMessage(`1`), EdgeID: edge, SourceNodeID: work.Attempt.NodeID, TargetPort: work.Attempt.NodeID, RequiredEdges: []string{"left_join", "right_join"}}},
			})
			errors <- err
		}(index, work)
	}
	wait.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	if countAttemptsForNode(store, "join") != 1 {
		t.Fatalf("join attempts = %d", countAttemptsForNode(store, "join"))
	}
}

// 验证 fan-out/fan-in 只在全部持久化贡献到齐后创建一次 join attempt。
func TestMemoryStoreDurableFanInCAS(t *testing.T) {
	store, now := NewMemoryStore(), time.Unix(100, 0).UTC()
	created, err := store.CreateRun(context.Background(), CreateRunRequest{Run: Run{
		OrganizationID: "organization", ID: "run", Plan: PlanRef{ID: "plan", Hash: "hash", EntryNodeID: "start", Executor: "start"},
		Status: RunPending, Input: json.RawMessage(`{"seed":1}`), IdempotencyKey: "idem", RequestFingerprint: "fingerprint", CreatedAt: now, UpdatedAt: now,
	}})
	if err != nil || !created.Created {
		t.Fatalf("CreateRun() = %+v, %v", created, err)
	}
	start := claimDAGAttempt(t, store, now)
	completeDAGAttempt(t, store, start, now, []NewAttempt{
		{NodeID: "left", Executor: "left", Input: json.RawMessage(`1`)},
		{NodeID: "right", Executor: "right", Input: json.RawMessage(`2`)},
	})
	left := claimDAGAttempt(t, store, now)
	run := completeDAGAttempt(t, store, left, now, []NewAttempt{{
		NodeID: "join", Executor: "join", Input: json.RawMessage(`{"v":"left"}`), EdgeID: "left_join",
		SourceNodeID: "left", SourcePort: "output", TargetPort: "left", RequiredEdges: []string{"left_join", "right_join"},
		StaticInputs: map[string]json.RawMessage{"policy": json.RawMessage(`"strict"`)}, AggregateInput: true,
	}})
	if run.Status != RunRunning {
		t.Fatalf("first join contribution status = %s", run.Status)
	}
	right := claimDAGAttempt(t, store, now)
	completeDAGAttempt(t, store, right, now, []NewAttempt{{
		NodeID: "join", Executor: "join", Input: json.RawMessage(`{"v":"right"}`), EdgeID: "right_join",
		SourceNodeID: "right", SourcePort: "output", TargetPort: "right", RequiredEdges: []string{"left_join", "right_join"},
		StaticInputs: map[string]json.RawMessage{"policy": json.RawMessage(`"strict"`)}, AggregateInput: true,
	}})
	join := claimDAGAttempt(t, store, now)
	var joined map[string]json.RawMessage
	if json.Unmarshal(join.Attempt.Input, &joined) != nil || string(joined["left"]) != `{"v":"left"}` || string(joined["right"]) != `{"v":"right"}` {
		t.Fatalf("joined input = %s", join.Attempt.Input)
	}
	if string(joined["policy"]) != `"strict"` {
		t.Fatalf("literal binding was not joined: %s", join.Attempt.Input)
	}
	if countAttemptsForNode(store, "join") != 1 {
		t.Fatalf("join attempts = %d", countAttemptsForNode(store, "join"))
	}
}

// 验证多个 End 中任一路径先结束都不能提前把整个 run 标记成功。
func TestMemoryStoreMultiEndWaitsForAllActivePaths(t *testing.T) {
	store, now := NewMemoryStore(), time.Unix(200, 0).UTC()
	_, _ = store.CreateRun(context.Background(), CreateRunRequest{Run: Run{
		OrganizationID: "organization", ID: "run", Plan: PlanRef{ID: "plan", Hash: "hash", EntryNodeID: "start", Executor: "start"},
		Status: RunPending, Input: json.RawMessage(`null`), IdempotencyKey: "idem", RequestFingerprint: "fingerprint", CreatedAt: now, UpdatedAt: now,
	}})
	start := claimDAGAttempt(t, store, now)
	completeDAGAttempt(t, store, start, now, []NewAttempt{{NodeID: "end_a", Executor: "end"}, {NodeID: "end_b", Executor: "end"}})
	first := claimDAGAttempt(t, store, now)
	run := completeDAGAttempt(t, store, first, now, nil)
	if run.Status != RunRunning {
		t.Fatalf("first End status = %s", run.Status)
	}
	second := claimDAGAttempt(t, store, now)
	run = completeDAGAttempt(t, store, second, now, nil)
	if run.Status != RunSuccess {
		t.Fatalf("last End status = %s", run.Status)
	}
}

// 验证内存存储只在成功终态原子投影最后完成的 End 输出，运行中节点输出不可见。
func TestMemoryStoreProjectsTerminalOutput(t *testing.T) {
	store, now := NewMemoryStore(), time.Unix(250, 0).UTC()
	_, _ = store.CreateRun(context.Background(), CreateRunRequest{Run: Run{
		OrganizationID: "organization", ID: "run", Plan: PlanRef{ID: "plan", Hash: "hash", EntryNodeID: "start", Executor: "start"},
		Status: RunPending, Input: json.RawMessage(`null`), IdempotencyKey: "idem", RequestFingerprint: "fingerprint", CreatedAt: now, UpdatedAt: now,
	}})
	start := claimDAGAttempt(t, store, now)
	completeDAGAttempt(t, store, start, now, []NewAttempt{{NodeID: "end", Executor: "end"}})
	end := claimDAGAttempt(t, store, now)
	value := json.RawMessage(`{"answer":42}`)
	completed, err := store.CompleteAttempt(context.Background(), CompleteAttemptRequest{
		OrganizationID: "organization", RunID: "run", AttemptID: end.Attempt.ID, WorkerID: "worker",
		FencingToken: end.Lease.FencingToken, Now: now, RunStatus: RunRunning,
		Output: &Output{ContentType: "application/json", Size: int64(len(value)), SHA256: "digest", Inline: value},
	})
	if err != nil || completed.Status != RunSuccess || completed.Output == nil {
		t.Fatalf("CompleteAttempt() = %+v, %v", completed, err)
	}
	if string(completed.Output.Inline) != string(value) || completed.Output.External {
		t.Fatalf("terminal output = %+v", completed.Output)
	}
	stored, err := store.GetRun(context.Background(), "organization", "run")
	if err != nil || stored.Output == nil || string(stored.Output.Inline) != string(value) {
		t.Fatalf("GetRun() = %+v, %v", stored, err)
	}
}

// 验证 Wait 的 auto-resume deadline 到达后原 attempt 以更高 fencing 重新可领取。
func TestMemoryStoreAutoResumesDueWait(t *testing.T) {
	store, now := NewMemoryStore(), time.Unix(300, 0).UTC()
	_, _ = store.CreateRun(context.Background(), CreateRunRequest{Run: Run{
		OrganizationID: "organization", ID: "run", Plan: PlanRef{ID: "plan", Hash: "hash", EntryNodeID: "delay", Executor: "delay"},
		Status: RunPending, Input: json.RawMessage(`null`), IdempotencyKey: "idem", RequestFingerprint: "fingerprint", CreatedAt: now, UpdatedAt: now,
	}})
	work := claimDAGAttempt(t, store, now)
	digest := [32]byte{1}
	_, err := store.SuspendAttempt(context.Background(), SuspendAttemptRequest{
		OrganizationID: "organization", RunID: "run", AttemptID: work.Attempt.ID, WorkerID: "worker", FencingToken: work.Lease.FencingToken, Now: now,
		Interrupt: Interrupt{ID: "timer", Kind: "wait", TokenDigest: digest, ExpiresAt: now.Add(time.Second), AutoResume: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.ClaimAttempt(context.Background(), ClaimAttemptRequest{OrganizationID: "organization", WorkerID: "worker", Now: now.Add(time.Millisecond), LeaseDuration: time.Minute})
	if err != nil || before != nil {
		t.Fatalf("early claim = %+v, err = %v", before, err)
	}
	after, err := store.ClaimAttempt(context.Background(), ClaimAttemptRequest{OrganizationID: "organization", WorkerID: "worker", Now: now.Add(time.Second), LeaseDuration: time.Minute})
	if err != nil || after == nil || string(after.Attempt.ResumeData) != `{"expired":true}` || after.Lease.FencingToken <= work.Lease.FencingToken {
		t.Fatalf("due claim = %+v, err = %v", after, err)
	}
}

// 验证审批节点自己的 deadline 到期会原子终结 attempt、node 和 run，而不依赖可选的 Run deadline。
func TestMemoryStoreFinalizesExpiredApprovalNode(t *testing.T) {
	store, now := NewMemoryStore(), time.Unix(350, 0).UTC()
	_, _ = store.CreateRun(context.Background(), CreateRunRequest{Run: Run{
		OrganizationID: "organization", ID: "run", Plan: PlanRef{ID: "plan", Hash: "hash", EntryNodeID: "approval", Executor: "approval"},
		Status: RunPending, Input: json.RawMessage(`null`), IdempotencyKey: "idem", RequestFingerprint: "fingerprint", CreatedAt: now, UpdatedAt: now,
	}})
	work := claimDAGAttempt(t, store, now)
	_, err := store.SuspendAttempt(context.Background(), SuspendAttemptRequest{
		OrganizationID: "organization", RunID: "run", AttemptID: work.Attempt.ID, WorkerID: "worker", FencingToken: work.Lease.FencingToken, Now: now,
		Interrupt: Interrupt{ID: "approval", Kind: "approval", TokenDigest: [32]byte{1}, ExpiresAt: now.Add(time.Second)},
	})
	if err != nil {
		t.Fatal(err)
	}
	count, err := store.ExpireRuns(context.Background(), "organization", now.Add(time.Second), 10)
	if err != nil || count != 1 {
		t.Fatalf("ExpireRuns() count=%d err=%v", count, err)
	}
	details, err := store.GetRunDetails(context.Background(), "organization", "run")
	if err != nil || details.Run.Status != RunError || len(details.Nodes) != 1 || details.Nodes[0].Node.Status != NodeError ||
		len(details.Nodes[0].Attempts) != 1 || details.Nodes[0].Attempts[0].Attempt.Status != AttemptError ||
		details.Nodes[0].Attempts[0].Attempt.ErrorCode != "node_timeout" {
		t.Fatalf("expired approval details=%+v err=%v", details, err)
	}
	interrupts, err := store.ListInterrupts(context.Background(), "organization", "run")
	if err != nil || len(interrupts) != 0 {
		t.Fatalf("expired approval interrupts=%+v err=%v", interrupts, err)
	}
	events, err := store.ListEvents(context.Background(), "organization", "run", 0, 100)
	if err != nil || !eventTypeExists(events, "node.timed_out") || !eventTypeExists(events, "run.error") {
		t.Fatalf("expired approval events=%+v err=%v", events, err)
	}
}

func eventTypeExists(events []Event, expected string) bool {
	for _, event := range events {
		if event.Type == expected {
			return true
		}
	}
	return false
}

// 验证一个并行分支等待审批时其他分支仍可推进，最后一个可运行分支结束后 run 才进入挂起。
func TestMemoryStoreSuspendedBranchDoesNotBlockFanOut(t *testing.T) {
	store, now := NewMemoryStore(), time.Unix(400, 0).UTC()
	_, _ = store.CreateRun(context.Background(), CreateRunRequest{Run: Run{
		OrganizationID: "organization", ID: "run", Plan: PlanRef{ID: "plan", Hash: "hash", EntryNodeID: "start", Executor: "start"},
		Status: RunPending, Input: json.RawMessage(`null`), IdempotencyKey: "idem", RequestFingerprint: "fingerprint", CreatedAt: now, UpdatedAt: now,
	}})
	start := claimDAGAttempt(t, store, now)
	completeDAGAttempt(t, store, start, now, []NewAttempt{{NodeID: "approval", Executor: "approval"}, {NodeID: "work", Executor: "work"}})
	approval := claimDAGAttempt(t, store, now)
	digest := [32]byte{9}
	_, err := store.SuspendAttempt(context.Background(), SuspendAttemptRequest{
		OrganizationID: "organization", RunID: "run", AttemptID: approval.Attempt.ID, WorkerID: "worker", FencingToken: approval.Lease.FencingToken, Now: now,
		Interrupt: Interrupt{ID: "approval", Kind: "approval", TokenDigest: digest, ExpiresAt: now.Add(time.Hour)},
	})
	if err != nil {
		t.Fatal(err)
	}
	run, _ := store.GetRun(context.Background(), "organization", "run")
	if run.Status != RunRunning {
		t.Fatalf("run status while sibling is runnable = %s", run.Status)
	}
	work := claimDAGAttempt(t, store, now)
	run = completeDAGAttempt(t, store, work, now, nil)
	if run.Status != RunSuspended {
		t.Fatalf("run status after sibling completed = %s", run.Status)
	}
	resolved, err := store.ResolveInterrupt(context.Background(), ResolveInterruptRequest{
		OrganizationID: "organization", InterruptID: "approval", TokenDigest: digest, IdempotencyKey: "approval-1", DecidedBy: "user",
		Decision: ApprovalDecision{Approved: true, DecidedBy: "user"}, Now: now.Add(time.Second),
	})
	if err != nil || resolved.Run.Status != RunPending {
		t.Fatalf("ResolveInterrupt() = %+v, %v", resolved, err)
	}
}

func claimDAGAttempt(t *testing.T, store *MemoryStore, now time.Time) *WorkItem {
	t.Helper()
	work, err := store.ClaimAttempt(context.Background(), ClaimAttemptRequest{OrganizationID: "organization", WorkerID: "worker", Now: now, LeaseDuration: time.Minute})
	if err != nil || work == nil {
		t.Fatalf("ClaimAttempt() = %+v, %v", work, err)
	}
	return work
}

func completeDAGAttempt(t *testing.T, store *MemoryStore, work *WorkItem, now time.Time, next []NewAttempt) Run {
	t.Helper()
	run, err := store.CompleteAttempt(context.Background(), CompleteAttemptRequest{
		OrganizationID: "organization", RunID: "run", AttemptID: work.Attempt.ID, WorkerID: "worker",
		FencingToken: work.Lease.FencingToken, Now: now, RunStatus: RunRunning, Next: next,
	})
	if err != nil {
		t.Fatalf("CompleteAttempt() error = %v", err)
	}
	return run
}

func countAttemptsForNode(store *MemoryStore, nodeID string) int {
	store.mu.Lock()
	defer store.mu.Unlock()
	count := 0
	for _, attempt := range store.attempts {
		if attempt.NodeID == nodeID {
			count++
		}
	}
	return count
}
