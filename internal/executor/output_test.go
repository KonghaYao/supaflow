package executor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
)

type recordingOutputManager struct {
	stored    []byte
	discarded []string
}

func (m *recordingOutputManager) StoreOutput(_ context.Context, _ workflowruntime.TenantID, _, _ string, value []byte) (ExternalOutput, error) {
	m.stored = append([]byte(nil), value...)
	digest := sha256.Sum256(value)
	return ExternalOutput{ObjectRef: "obj_safe_reference", Size: int64(len(value)), SHA256: hex.EncodeToString(digest[:])}, nil
}

func (m *recordingOutputManager) DiscardOutput(_ context.Context, _ workflowruntime.TenantID, objectRef string) error {
	m.discarded = append(m.discarded, objectRef)
	return nil
}

func (m *recordingOutputManager) ReadRunOutput(_ context.Context, _ workflowruntime.TenantID, _ string, expected workflowruntime.RunOutput) (json.RawMessage, error) {
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
		TenantID: "tenant_a", RunID: "run_1", Plan: workflowruntime.PlanRef{ID: published.plan.ID, Hash: published.plan.Hash},
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
		TenantID: "tenant_a", RunID: "run_1", Plan: workflowruntime.PlanRef{ID: published.plan.ID, Hash: published.plan.Hash},
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
		TenantID: "tenant_a", RunID: "run_1", Plan: workflowruntime.PlanRef{ID: published.plan.ID, Hash: published.plan.Hash},
		NodeID: "begin", AttemptID: "attempt_1", Executor: "builtin.start", Input: json.RawMessage(`{"secret":"value"}`), FencingToken: 1,
	})
	assertExecutionError(t, err, "sensitive_output_unavailable", false)
}

type failingCompletionStore struct {
	workflowruntime.Store
	err    error
	status workflowruntime.RunStatus
}

func (s failingCompletionStore) CompleteAttempt(_ context.Context, req workflowruntime.CompleteAttemptRequest) (workflowruntime.Run, error) {
	return workflowruntime.Run{TenantID: req.TenantID, ID: req.RunID, Status: s.status}, s.err
}

// 验证对象写入成功但 runtime 原子提交失败时立即补偿删除，避免不可达对象泄漏。
func TestManagedOutputStoreCompensatesFailedCompletion(t *testing.T) {
	manager := &recordingOutputManager{}
	store, err := NewManagedOutputStore(failingCompletionStore{err: errors.New("commit failed")}, manager)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.CompleteAttempt(context.Background(), workflowruntime.CompleteAttemptRequest{
		TenantID: "tenant_a", RunID: "run_1", Output: &workflowruntime.Output{ObjectRef: "obj_safe_reference"},
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
		TenantID: "tenant_a", RunID: "run_1", Output: &workflowruntime.Output{ObjectRef: "obj_maybe_committed"},
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
		TenantID: "tenant_a", RunID: "run_1", Output: &workflowruntime.Output{ObjectRef: "obj_safe_reference"},
	})
	if err != nil || len(manager.discarded) != 1 {
		t.Fatalf("error=%v discarded=%v", err, manager.discarded)
	}
}

var _ workflowruntime.Store = failingCompletionStore{}
var _ OutputManager = (*recordingOutputManager)(nil)
