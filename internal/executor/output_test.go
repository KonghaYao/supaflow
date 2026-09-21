package executor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/workflow"
)

type recordingOutputManager struct {
	stored       []byte
	discarded    []string
	blockDiscard bool
}

func (m *recordingOutputManager) StoreOutput(_ context.Context, _ workflowruntime.OrganizationID, _, _ string, value []byte) (ExternalOutput, error) {
	m.stored = append([]byte(nil), value...)
	digest := sha256.Sum256(value)
	return ExternalOutput{ObjectRef: "obj_safe_reference", Size: int64(len(value)), SHA256: hex.EncodeToString(digest[:])}, nil
}

func (m *recordingOutputManager) DiscardOutput(ctx context.Context, _ workflowruntime.OrganizationID, objectRef string) error {
	if m.blockDiscard {
		<-ctx.Done()
		return ctx.Err()
	}
	m.discarded = append(m.discarded, objectRef)
	return nil
}

func (m *recordingOutputManager) ReadRunOutput(_ context.Context, _ workflowruntime.OrganizationID, _ string, expected workflowruntime.RunOutput) (json.RawMessage, error) {
	return append(json.RawMessage(nil), expected.Inline...), nil
}

// 验证超过节点内联上限的 JSON 输出被外置，runtime 仅持安全引用和完整性元数据。
func TestPlanExecutorExternalizesOversizedOutput(t *testing.T) {
	published := publishPlan(t, workflowDefinition("https://example.com", ""))
	manager := &recordingOutputManager{}
	executor, err := NewPlanExecutor(published.catalog, Config{Outputs: manager})
	if err != nil {
		t.Fatal(err)
	}
	input := json.RawMessage(`"` + strings.Repeat("x", (1<<20)+1) + `"`)
	result, err := executor.Execute(context.Background(), workflowruntime.ExecutionRequest{
		OrganizationID: "organization_a", RunID: "run_1", Plan: workflowruntime.PlanRef{ID: published.plan.ID, Hash: published.plan.Hash},
		NodeID: "begin", AttemptID: "attempt_1", Executor: "builtin.start", Input: input, FencingToken: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Output == nil || result.Output.ObjectRef != "obj_safe_reference" || result.Output.Inline != nil || result.Output.Size != int64(len(input)) {
		t.Fatalf("external output = %+v", result.Output)
	}
	if string(manager.stored) != string(input) || len(result.Next) != 1 || string(result.Next[0].Input) != string(input) {
		t.Fatal("externalization changed the durable output or downstream value")
	}
}

// 验证发布计划标记敏感的输出即使很小也不会以内联明文进入 runtime。
func TestPlanExecutorExternalizesSensitiveOutput(t *testing.T) {
	definition := workflowDefinition("https://example.com", "")
	definition.Nodes[0].Metadata = map[string]json.RawMessage{"sensitiveOutput": json.RawMessage(`true`)}
	published := publishPlan(t, definition)
	manager := &recordingOutputManager{}
	executor, err := NewPlanExecutor(published.catalog, Config{Outputs: manager})
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.Execute(context.Background(), workflowruntime.ExecutionRequest{
		OrganizationID: "organization_a", RunID: "run_1", Plan: workflowruntime.PlanRef{ID: published.plan.ID, Hash: published.plan.Hash},
		NodeID: "begin", AttemptID: "attempt_1", Executor: "builtin.start", Input: json.RawMessage(`{"secret":"value"}`), FencingToken: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Output == nil || !result.Output.Sensitive || result.Output.Inline != nil || result.Output.ObjectRef == "" {
		t.Fatalf("sensitive output = %+v", result.Output)
	}
}

// 验证需要外置的输出在未装配对象存储时保守失败，绝不回退为数据库明文。
func TestPlanExecutorFailsClosedWithoutExternalOutputStorage(t *testing.T) {
	definition := workflowDefinition("https://example.com", "")
	definition.Nodes[0].Metadata = map[string]json.RawMessage{"sensitiveOutput": json.RawMessage(`true`)}
	published := publishPlan(t, definition)
	executor, err := NewPlanExecutor(published.catalog, Config{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = executor.Execute(context.Background(), workflowruntime.ExecutionRequest{
		OrganizationID: "organization_a", RunID: "run_1", Plan: workflowruntime.PlanRef{ID: published.plan.ID, Hash: published.plan.Hash},
		NodeID: "begin", AttemptID: "attempt_1", Executor: "builtin.start", Input: json.RawMessage(`{"secret":"value"}`), FencingToken: 1,
	})
	assertExecutionError(t, err, "sensitive_output_unavailable", false)
}

type sensitiveStreamingModel struct{}

func (sensitiveStreamingModel) Generate(context.Context, AttemptScope, string, string, json.RawMessage) (json.RawMessage, error) {
	return json.RawMessage(`{"secret":"provider-output"}`), nil
}

func (sensitiveStreamingModel) GenerateStream(_ context.Context, _ AttemptScope, _, _ string, _ json.RawMessage, emit func(json.RawMessage) error) (json.RawMessage, error) {
	chunk := json.RawMessage(`{"secret":"stream-chunk"}`)
	if err := emit(chunk); err != nil {
		return nil, err
	}
	return chunk, nil
}

// 验证显式敏感的 streaming 节点降级为受保护的最终输出，不向普通 SSE/outbox emit 明文 delta。
func TestPlanExecutorSuppressesSensitiveStreamChunks(t *testing.T) {
	definition := workflow.WorkflowDefinition{
		SchemaVersion: workflow.DefinitionSchemaV1,
		ID:            "sensitive_stream",
		Nodes: []workflow.NodeDefinition{
			{ID: "begin", Type: workflow.NodeTypeStart, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
			{ID: "model", Type: workflow.NodeTypeLLM, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{"name":"primary"}`), Metadata: map[string]json.RawMessage{"sensitiveOutput": json.RawMessage(`true`)}},
			{ID: "finish", Type: workflow.NodeTypeEnd, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
		},
		Edges: []workflow.EdgeDefinition{
			{ID: "begin_to_model", FromNode: "begin", FromPort: "input", ToNode: "model", ToPort: "input"},
			{ID: "model_to_end", FromNode: "model", FromPort: "output", ToNode: "finish", ToPort: "result"},
		},
	}
	published := publishPlan(t, definition)
	manager := &recordingOutputManager{}
	executor, err := NewPlanExecutor(published.catalog, Config{Models: sensitiveStreamingModel{}, Outputs: manager})
	if err != nil {
		t.Fatal(err)
	}
	emitted := 0
	result, err := executor.ExecuteStream(context.Background(), workflowruntime.ExecutionRequest{
		OrganizationID: "organization_a", RunID: "run_1", Plan: workflowruntime.PlanRef{ID: published.plan.ID, Hash: published.plan.Hash},
		NodeID: "model", AttemptID: "attempt_1", Executor: "eino.llm", Input: json.RawMessage(`{"prompt":"hello"}`), FencingToken: 1,
	}, func(json.RawMessage) error {
		emitted++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if emitted != 0 || result.Output == nil || !result.Output.Sensitive || result.Output.ObjectRef == "" {
		t.Fatalf("sensitive stream leaked or was not protected: emitted=%d output=%+v", emitted, result.Output)
	}
}

type failingCompletionStore struct {
	workflowruntime.Store
	err    error
	status workflowruntime.RunStatus
}

func (s failingCompletionStore) CompleteAttempt(_ context.Context, req workflowruntime.CompleteAttemptRequest) (workflowruntime.Run, error) {
	return workflowruntime.Run{OrganizationID: req.OrganizationID, ID: req.RunID, Status: s.status}, s.err
}

// 验证对象写入成功但 runtime 原子提交失败时立即补偿删除，避免不可达对象泄漏。
func TestManagedOutputStoreCompensatesFailedCompletion(t *testing.T) {
	manager := &recordingOutputManager{}
	store, err := NewManagedOutputStore(failingCompletionStore{err: errors.New("commit failed")}, manager)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.CompleteAttempt(context.Background(), workflowruntime.CompleteAttemptRequest{
		OrganizationID: "organization_a", RunID: "run_1", Output: &workflowruntime.Output{ObjectRef: "obj_safe_reference"},
	})
	if err == nil || len(manager.discarded) != 1 || manager.discarded[0] != "obj_safe_reference" {
		t.Fatalf("error=%v discarded=%v", err, manager.discarded)
	}
}

// 验证数据库 commit 回执丢失时保留可能已被成功 Run 引用的对象，禁止破坏性补偿。
func TestManagedOutputStorePreservesObjectWhenCommitOutcomeIsUnknown(t *testing.T) {
	manager := &recordingOutputManager{}
	commitErr := workflowruntime.MarkCommitOutcomeUnknown(errors.New("connection lost after commit"))
	store, err := NewManagedOutputStore(failingCompletionStore{err: commitErr}, manager)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.CompleteAttempt(context.Background(), workflowruntime.CompleteAttemptRequest{
		OrganizationID: "organization_a", RunID: "run_1", Output: &workflowruntime.Output{ObjectRef: "obj_maybe_committed"},
	})
	if !workflowruntime.IsCommitOutcomeUnknown(err) || len(manager.discarded) != 0 {
		t.Fatalf("error=%v discarded=%v", err, manager.discarded)
	}
}

// 验证取消竞态已由 runtime 原子落为 CANCELLED 时，executor 预写对象同样被清理。
func TestManagedOutputStoreDiscardsCancelledCompletion(t *testing.T) {
	manager := &recordingOutputManager{}
	store, err := NewManagedOutputStore(failingCompletionStore{status: workflowruntime.RunCancelled}, manager)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.CompleteAttempt(context.Background(), workflowruntime.CompleteAttemptRequest{
		OrganizationID: "organization_a", RunID: "run_1", Output: &workflowruntime.Output{ObjectRef: "obj_safe_reference"},
	})
	if err != nil || len(manager.discarded) != 1 {
		t.Fatalf("error=%v discarded=%v", err, manager.discarded)
	}
}

// 验证 runtime 拒绝输出后的对象补偿有独立超时，存储故障不能永久阻塞 worker 退出。
func TestManagedOutputStoreBoundsCompensation(t *testing.T) {
	manager := &recordingOutputManager{blockDiscard: true}
	store, err := NewManagedOutputStore(failingCompletionStore{err: errors.New("commit failed")}, manager)
	if err != nil {
		t.Fatal(err)
	}
	store.cleanupWait = 20 * time.Millisecond
	started := time.Now()
	_, err = store.CompleteAttempt(context.Background(), workflowruntime.CompleteAttemptRequest{
		OrganizationID: "organization_a", RunID: "run_1", Output: &workflowruntime.Output{ObjectRef: "obj_safe_reference"},
	})
	if err == nil || time.Since(started) > 250*time.Millisecond {
		t.Fatalf("output compensation was not bounded: elapsed=%s err=%v", time.Since(started), err)
	}
}

// claimingStore implements the optional DynamicStore capability the worker relies on.
type claimingStore struct {
	failingCompletionStore
	calls int
}

func (s *claimingStore) ClaimNext(_ context.Context, _ workflowruntime.ClaimNextAttemptRequest) (*workflowruntime.WorkItem, error) {
	s.calls++
	return &workflowruntime.WorkItem{Attempt: workflowruntime.Attempt{ID: "attempt_1"}}, nil
}

// 验证装饰器保留 DynamicStore 能力：Orchestrator.AdvanceNext 依赖该类型断言，丢失即 worker 永远领不到工作。
func TestManagedOutputStoreForwardsDynamicClaiming(t *testing.T) {
	inner := &claimingStore{}
	store, err := NewManagedOutputStore(inner, &recordingOutputManager{})
	if err != nil {
		t.Fatal(err)
	}
	dynamic, ok := workflowruntime.Store(store).(workflowruntime.DynamicStore)
	if !ok {
		t.Fatal("decorated store lost the DynamicStore capability")
	}
	work, err := dynamic.ClaimNext(context.Background(), workflowruntime.ClaimNextAttemptRequest{WorkerID: "worker_1"})
	if err != nil || work == nil || work.Attempt.ID != "attempt_1" || inner.calls != 1 {
		t.Fatalf("work=%+v err=%v calls=%d", work, err, inner.calls)
	}
}

// 验证被装饰的 store 不支持动态领取时显式报错，而不是伪装成一次空领取并让 worker 空转。
func TestManagedOutputStoreRejectsUndecoratedDynamicClaiming(t *testing.T) {
	store, err := NewManagedOutputStore(failingCompletionStore{}, &recordingOutputManager{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.ClaimNext(context.Background(), workflowruntime.ClaimNextAttemptRequest{WorkerID: "worker_1"}); err == nil {
		t.Fatal("expected an explicit error for a store without dynamic claiming")
	}
}

var _ workflowruntime.Store = failingCompletionStore{}
var _ workflowruntime.DynamicStore = (*claimingStore)(nil)
var _ OutputManager = (*recordingOutputManager)(nil)
