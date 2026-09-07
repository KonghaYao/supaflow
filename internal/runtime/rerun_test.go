package runtime

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func completeTestRun(t *testing.T, store *MemoryStore, organizationID OrganizationID, runID string, now time.Time, next []NewAttempt, status RunStatus) {
	t.Helper()
	work, err := store.ClaimAttempt(context.Background(), ClaimAttemptRequest{
		OrganizationID: organizationID, WorkerID: "worker", Now: now, LeaseDuration: time.Minute,
	})
	if err != nil || work == nil || work.Run.ID != runID {
		t.Fatalf("claim run %s: work=%+v err=%v", runID, work, err)
	}
	_, err = store.CompleteAttempt(context.Background(), CompleteAttemptRequest{
		OrganizationID: organizationID, RunID: runID, AttemptID: work.Attempt.ID, WorkerID: "worker",
		FencingToken: work.Lease.FencingToken, Now: now.Add(time.Second), RunStatus: status, Next: next,
	})
	if err != nil {
		t.Fatalf("complete run %s: %v", runID, err)
	}
}

// 验证完整重跑创建新 Run、保留源历史，并投影不可变 lineage。
func TestRerunFullCreatesFreshRunWithLineage(t *testing.T) {
	now := time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)
	service, store, _ := testService(now)
	source, err := service.StartRun(context.Background(), "organization_a", startRequest("source-full"))
	if err != nil {
		t.Fatal(err)
	}
	completeTestRun(t, store, "organization_a", source.Run.ID, now, nil, RunSuccess)
	result, err := service.Rerun(context.Background(), "organization_a", RerunRequest{
		SourceRunID: source.Run.ID, EntryNodeID: "start", EntryExecutor: "test", IdempotencyKey: "rerun-full-key",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Run.ID == source.Run.ID || result.Run.Rerun == nil || result.Run.Rerun.SourceRunID != source.Run.ID {
		t.Fatalf("rerun=%+v source=%+v", result.Run, source.Run)
	}
	work, err := store.ClaimAttempt(context.Background(), ClaimAttemptRequest{
		OrganizationID: "organization_a", WorkerID: "rerun-worker", Now: now.Add(2 * time.Second), LeaseDuration: time.Minute,
	})
	if err != nil || work == nil || work.Attempt.NodeID != "start" || string(work.Attempt.Input) != `{"message":"hello"}` {
		t.Fatalf("rerun work=%+v err=%v", work, err)
	}
	sourceAfter, err := service.GetRun(context.Background(), "organization_a", source.Run.ID)
	if err != nil || sourceAfter.Status != RunSuccess {
		t.Fatalf("source mutated: %+v err=%v", sourceAfter, err)
	}
}

// 验证 from-node 使用源节点最后一次 attempt 的原始输入和计划 executor。
func TestRerunFromNodeUsesLatestAttemptInput(t *testing.T) {
	now := time.Date(2026, 9, 3, 1, 0, 0, 0, time.UTC)
	service, store, _ := testService(now)
	source, err := service.StartRun(context.Background(), "organization_a", startRequest("source-node"))
	if err != nil {
		t.Fatal(err)
	}
	completeTestRun(t, store, "organization_a", source.Run.ID, now, []NewAttempt{{
		NodeID: "step", Executor: "http", Input: json.RawMessage(`{"original":42}`),
	}}, RunRunning)
	completeTestRun(t, store, "organization_a", source.Run.ID, now.Add(2*time.Second), nil, RunSuccess)
	result, err := service.Rerun(context.Background(), "organization_a", RerunRequest{
		SourceRunID: source.Run.ID, FromNodeID: "step", EntryNodeID: "step",
		EntryExecutor: "http", IdempotencyKey: "rerun-node-key",
	})
	if err != nil {
		t.Fatal(err)
	}
	work, err := store.ClaimAttempt(context.Background(), ClaimAttemptRequest{
		OrganizationID: "organization_a", WorkerID: "rerun-worker", Now: now.Add(4 * time.Second), LeaseDuration: time.Minute,
	})
	if err != nil || work == nil || work.Attempt.Executor != "http" || string(work.Attempt.Input) != `{"original":42}` {
		t.Fatalf("from-node work=%+v err=%v", work, err)
	}
	if result.Run.Rerun == nil || result.Run.Rerun.FromNodeID != "step" {
		t.Fatalf("lineage=%+v", result.Run.Rerun)
	}
}

// 验证仅终态源可重跑，且幂等重放、冲突和租户隔离遵循稳定语义。
func TestRerunGuardsTerminalIdempotencyAndOrganization(t *testing.T) {
	now := time.Date(2026, 9, 3, 2, 0, 0, 0, time.UTC)
	service, store, _ := testService(now)
	source, err := service.StartRun(context.Background(), "organization_a", startRequest("source-guards"))
	if err != nil {
		t.Fatal(err)
	}
	request := RerunRequest{SourceRunID: source.Run.ID, EntryNodeID: "start", EntryExecutor: "test", IdempotencyKey: "rerun-guard-key"}
	if _, err = service.Rerun(context.Background(), "organization_a", request); !IsCode(err, CodeInvalidTransition) {
		t.Fatalf("non-terminal rerun error=%v", err)
	}
	completeTestRun(t, store, "organization_a", source.Run.ID, now, nil, RunSuccess)
	first, err := service.Rerun(context.Background(), "organization_a", request)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := service.Rerun(context.Background(), "organization_a", request)
	if err != nil || replay.Created || replay.Run.ID != first.Run.ID {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	request.FromNodeID, request.EntryNodeID = "start", "start"
	if _, err = service.Rerun(context.Background(), "organization_a", request); !IsCode(err, CodeIdempotencyConflict) {
		t.Fatalf("conflicting rerun error=%v", err)
	}
	if _, err = service.Rerun(context.Background(), "organization_b", RerunRequest{
		SourceRunID: source.Run.ID, EntryNodeID: "start", EntryExecutor: "test", IdempotencyKey: "organization-cross-key",
	}); !IsCode(err, CodeNotFound) {
		t.Fatalf("cross-organization rerun error=%v", err)
	}
}
