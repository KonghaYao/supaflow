package executor

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pazhou/supaflow/internal/platform"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/workflow"
)

type subworkflowIDs struct{ next atomic.Uint64 }

type recordingChildOutputReader struct {
	value    json.RawMessage
	tenantID workflowruntime.TenantID
	runID    string
	expected workflowruntime.RunOutput
}

func (r *recordingChildOutputReader) ReadRunOutput(_ context.Context, tenantID workflowruntime.TenantID, runID string, expected workflowruntime.RunOutput) (json.RawMessage, error) {
	r.tenantID, r.runID, r.expected = tenantID, runID, expected
	return append(json.RawMessage(nil), r.value...), nil
}

func (g *subworkflowIDs) NewID(prefix string) (string, error) {
	return fmt.Sprintf("%s_%d", prefix, g.next.Add(1)), nil
}

// 验证子工作流成功后返回子 Run 的真实结果，重试不会只得到状态元数据。
func TestRuntimeSubworkflowRunnerReturnsSuccessfulChildOutput(t *testing.T) {
	runner, service, store, published, parentRunID, parentAttemptID := subworkflowFixture(t)
	request := subworkflowRequest(published, parentRunID, parentAttemptID, json.RawMessage(`{"customer":"one"}`))

	initial, err := runner.Run(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	var child struct {
		ChildRunID string                    `json:"childRunId"`
		Status     workflowruntime.RunStatus `json:"status"`
	}
	if err = json.Unmarshal(initial.Value, &child); err != nil || child.ChildRunID == "" || child.Status != workflowruntime.RunPending {
		t.Fatalf("initial child projection = %s, error = %v", initial.Value, err)
	}
	finishSubworkflow(t, store, child.ChildRunID, json.RawMessage(`{"approved":true}`))

	output, err := runner.Run(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if string(output.Value) != `{"approved":true}` {
		t.Fatalf("Run() output = %s", output.Value)
	}
	observed, err := service.GetRun(context.Background(), "tenant_a", child.ChildRunID)
	if err != nil || observed.Status != workflowruntime.RunSuccess {
		t.Fatalf("GetRun() = %+v, error = %v", observed, err)
	}
}

// 验证 object-backed 子 Run 输出通过租户内受控读取端口恢复，并将敏感标记传播给父节点。
func TestRuntimeSubworkflowRunnerReadsExternalChildOutputAndPreservesSensitivity(t *testing.T) {
	runner, service, store, published, parentRunID, parentAttemptID := subworkflowFixture(t)
	reader := &recordingChildOutputReader{value: json.RawMessage(`{"secret":"approved"}`)}
	runner.outputs = reader
	request := subworkflowRequest(published, parentRunID, parentAttemptID, json.RawMessage(`{"customer":"one"}`))

	initial, err := runner.Run(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	var child struct {
		ChildRunID string `json:"childRunId"`
	}
	if json.Unmarshal(initial.Value, &child) != nil || child.ChildRunID == "" {
		t.Fatalf("initial child projection = %s", initial.Value)
	}
	finishExternalSubworkflow(t, store, child.ChildRunID, reader.value, true)

	output, err := runner.Run(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if string(output.Value) != string(reader.value) || !output.Sensitive {
		t.Fatalf("external child output = %+v", output)
	}
	if reader.tenantID != "tenant_a" || reader.runID != child.ChildRunID || !reader.expected.External || !reader.expected.Sensitive {
		t.Fatalf("reader scope/metadata = tenant=%s run=%s output=%+v", reader.tenantID, reader.runID, reader.expected)
	}
	observed, err := service.GetRun(context.Background(), "tenant_a", child.ChildRunID)
	if err != nil || observed.Output == nil || !observed.Output.External {
		t.Fatalf("GetRun() = %+v, error = %v", observed, err)
	}
}

// 验证子工作流幂等范围绑定逻辑执行与稳定输入摘要，物理重试复用而新激活不会误复用子 Run。
func TestRuntimeSubworkflowRunnerScopesReplayByAttemptAndCanonicalInput(t *testing.T) {
	runner, _, store, published, parentRunID, parentAttemptID := subworkflowFixture(t)
	first := subworkflowRequest(published, parentRunID, parentAttemptID, json.RawMessage(`{"a":1,"b":2}`))
	first.ParentExecution = parentAttemptID
	firstID := childRunID(t, runner, first)

	whitespaceEquivalent := first
	whitespaceEquivalent.Input = json.RawMessage("{ \"b\": 2, \"a\": 1 }")
	if replayedID := childRunID(t, runner, whitespaceEquivalent); replayedID != firstID {
		t.Fatalf("canonical input replay child = %s, want %s", replayedID, firstID)
	}
	failAt := time.Now().UTC()
	retryAt := failAt
	if _, err := store.FailAttempt(context.Background(), workflowruntime.FailAttemptRequest{
		TenantID: "tenant_a", RunID: parentRunID, AttemptID: parentAttemptID, WorkerID: "parent-worker", FencingToken: 1,
		Now: failAt, ErrorCode: "adapter_failed", RetryAt: &retryAt,
	}); err != nil {
		t.Fatal(err)
	}
	var retryWork *workflowruntime.WorkItem
	for index := 0; index < 2; index++ {
		claimed, claimErr := store.ClaimAttempt(context.Background(), workflowruntime.ClaimAttemptRequest{
			TenantID: "tenant_a", WorkerID: fmt.Sprintf("retry-worker-%d", index), Now: failAt.Add(time.Second), LeaseDuration: time.Minute,
		})
		if claimErr != nil {
			t.Fatal(claimErr)
		}
		if claimed != nil && claimed.Attempt.RunID == parentRunID {
			retryWork = claimed
			break
		}
	}
	if retryWork == nil || retryWork.Attempt.ExecutionID != parentAttemptID {
		t.Fatalf("retry work = %+v", retryWork)
	}
	physicalRetry := first
	physicalRetry.ParentAttempt = retryWork.Attempt.ID
	if replayedID := childRunID(t, runner, physicalRetry); replayedID != firstID {
		t.Fatalf("physical retry child = %s, want %s", replayedID, firstID)
	}

	firstDigest := sha256.Sum256(json.RawMessage(`{"a":1,"b":2}`))
	nextAttempt := physicalRetry
	if subworkflowIdempotencyKey(nextAttempt, firstDigest) != subworkflowIdempotencyKey(first, firstDigest) {
		t.Fatal("physical retry changed the logical execution idempotency key")
	}

	nextExecution := nextAttempt
	nextExecution.ParentExecution = parentAttemptID + "-next-execution"
	if subworkflowIdempotencyKey(nextExecution, firstDigest) == subworkflowIdempotencyKey(first, firstDigest) {
		t.Fatal("different logical execution produced the same idempotency key")
	}

	nextInput := first
	nextDigest := sha256.Sum256(json.RawMessage(`{"a":2,"b":2}`))
	if subworkflowIdempotencyKey(nextInput, nextDigest) == subworkflowIdempotencyKey(first, firstDigest) {
		t.Fatal("different input produced the same idempotency key")
	}
}

func subworkflowFixture(t *testing.T) (*RuntimeSubworkflowRunner, *workflowruntime.Service, *workflowruntime.MemoryStore, platform.PublishedPlan, string, string) {
	t.Helper()
	catalog := platform.NewMemoryCatalog(workflow.BuiltinRegistry(), workflow.CompilePolicy{
		AllowedCapabilities: workflow.CapabilitySet{Streaming: true, Cancellable: true, Interruptible: true, Idempotent: true},
		AllowedPermissions:  workflow.PermissionSet{Network: true, Credentials: true},
	})
	definition := workflow.WorkflowDefinition{
		SchemaVersion: workflow.DefinitionSchemaV1, ID: "child-flow",
		Nodes: []workflow.NodeDefinition{
			{ID: "start", Type: workflow.NodeTypeStart, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
			{ID: "end", Type: workflow.NodeTypeEnd, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
		},
		Edges: []workflow.EdgeDefinition{{ID: "start-end", FromNode: "start", FromPort: "input", ToNode: "end", ToPort: "result"}},
	}
	published, _, err := catalog.Publish(context.Background(), "tenant_a", definition.ID, definition, "publish-child")
	if err != nil {
		t.Fatal(err)
	}
	store := workflowruntime.NewMemoryStore()
	service := workflowruntime.NewService(store, nil, &subworkflowIDs{})
	parent, err := service.StartRun(context.Background(), "tenant_a", workflowruntime.StartRunRequest{
		Plan:  workflowruntime.PlanRef{ID: published.ID, Hash: published.Hash, EntryNodeID: "start", Executor: "builtin.start"},
		Input: json.RawMessage(`null`), IdempotencyKey: "parent-run",
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	parentAttempt, err := store.ClaimAttempt(context.Background(), workflowruntime.ClaimAttemptRequest{
		TenantID: "tenant_a", WorkerID: "parent-worker", Now: now, LeaseDuration: time.Minute,
	})
	if err != nil || parentAttempt == nil {
		t.Fatalf("claim parent: %+v, error = %v", parentAttempt, err)
	}
	runner, err := NewRuntimeSubworkflowRunner(catalog, service)
	if err != nil {
		t.Fatal(err)
	}
	return runner, service, store, published, parent.Run.ID, parentAttempt.Attempt.ID
}

func subworkflowRequest(published platform.PublishedPlan, parentRunID, attempt string, input json.RawMessage) SubworkflowRequest {
	return SubworkflowRequest{
		TenantID: "tenant_a", ParentRun: parentRunID, ParentNode: "start", ParentAttempt: attempt, ParentExecution: attempt,
		PlanID: published.ID, PlanHash: published.Hash, Input: input,
	}
}

func childRunID(t *testing.T, runner *RuntimeSubworkflowRunner, request SubworkflowRequest) string {
	t.Helper()
	output, err := runner.Run(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	var value struct {
		ChildRunID string `json:"childRunId"`
	}
	if err = json.Unmarshal(output.Value, &value); err != nil || value.ChildRunID == "" {
		t.Fatalf("child metadata = %s, error = %v", output.Value, err)
	}
	return value.ChildRunID
}

func finishExternalSubworkflow(t *testing.T, store *workflowruntime.MemoryStore, runID string, output json.RawMessage, sensitive bool) {
	t.Helper()
	now := time.Now().UTC()
	claimed, err := store.ClaimAttempt(context.Background(), workflowruntime.ClaimAttemptRequest{
		TenantID: "tenant_a", WorkerID: "child-worker", Now: now, LeaseDuration: time.Minute,
	})
	if err != nil || claimed == nil || claimed.Attempt.RunID != runID {
		t.Fatalf("ClaimAttempt() = %+v, error = %v", claimed, err)
	}
	digest := sha256.Sum256(output)
	_, err = store.CompleteAttempt(context.Background(), workflowruntime.CompleteAttemptRequest{
		TenantID: "tenant_a", RunID: runID, AttemptID: claimed.Attempt.ID, WorkerID: "child-worker",
		FencingToken: claimed.Lease.FencingToken, Now: now.Add(time.Second), RunStatus: workflowruntime.RunSuccess,
		Output: &workflowruntime.Output{ContentType: "application/json", Size: int64(len(output)), SHA256: fmt.Sprintf("%x", digest[:]), Sensitive: sensitive, ObjectRef: "obj-child"},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func finishSubworkflow(t *testing.T, store *workflowruntime.MemoryStore, runID string, output json.RawMessage) {
	t.Helper()
	now := time.Now().UTC()
	claimed, err := store.ClaimAttempt(context.Background(), workflowruntime.ClaimAttemptRequest{
		TenantID: "tenant_a", WorkerID: "child-worker", Now: now, LeaseDuration: time.Minute,
	})
	if err != nil || claimed == nil || claimed.Attempt.RunID != runID {
		t.Fatalf("ClaimAttempt() = %+v, error = %v", claimed, err)
	}
	_, err = store.CompleteAttempt(context.Background(), workflowruntime.CompleteAttemptRequest{
		TenantID: "tenant_a", RunID: runID, AttemptID: claimed.Attempt.ID, WorkerID: "child-worker",
		FencingToken: claimed.Lease.FencingToken, Now: now.Add(time.Second), RunStatus: workflowruntime.RunSuccess,
		Output: &workflowruntime.Output{ContentType: "application/json", Size: int64(len(output)), Inline: output},
	})
	if err != nil {
		t.Fatal(err)
	}
}
