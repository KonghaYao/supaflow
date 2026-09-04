package e2e

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/workflow"
)

// 验证 Wait 节点到期后无需外部请求即可持久恢复，并通过 API 观测到成功终态。
func TestWaitAutoResumesThroughAPIWithoutBrowser(t *testing.T) {
	h := newHarness(t)
	plan := publishDefinitionAs(t, h, "wait-flow", "publish-wait-e2e", tenantAPublisherKey, waitDefinition("wait-flow"))
	run := start(t, h, plan, "start-wait-e2e")

	advance(t, h, "tenant-a")
	suspended := advance(t, h, "tenant-a")
	if suspended.Interrupt == nil || !suspended.Interrupt.AutoResume || suspended.Interrupt.Kind != "wait" {
		t.Fatalf("wait suspension = %+v", suspended)
	}
	before := requestJSON(t, h.handler, http.MethodGet, "/v1/runs/"+run.ID, tenantAViewerKey, "", nil)
	if before.Code != http.StatusOK || decodeResponse[runResponse](t, before).Status != workflowruntime.RunSuspended {
		t.Fatalf("before expiry status=%d body=%s", before.Code, before.Body.String())
	}

	h.clock.Advance(time.Second)
	advance(t, h, "tenant-a")
	completed := advance(t, h, "tenant-a")
	if completed.Run == nil || completed.Run.Status != workflowruntime.RunSuccess {
		t.Fatalf("completed=%+v", completed)
	}
	after := requestJSON(t, h.handler, http.MethodGet, "/v1/runs/"+run.ID, tenantAViewerKey, "", nil)
	if after.Code != http.StatusOK || decodeResponse[runResponse](t, after).Status != workflowruntime.RunSuccess {
		t.Fatalf("after expiry status=%d body=%s", after.Code, after.Body.String())
	}
	_, events := readSSE(t, h.handler, "/v1/runs/"+run.ID+"/events", tenantAViewerKey, "", time.Second)
	foundResume := false
	for _, event := range events {
		foundResume = foundResume || event.Type == "wait.resumed"
	}
	if !foundResume {
		t.Fatalf("events=%+v", events)
	}
}

func waitDefinition(id string) workflow.WorkflowDefinition {
	return workflow.WorkflowDefinition{
		SchemaVersion: workflow.DefinitionSchemaV1,
		ID:            id,
		Nodes: []workflow.NodeDefinition{
			{ID: "start", Type: workflow.NodeTypeStart, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
			{ID: "wait", Type: workflow.NodeTypeWait, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{"timeoutMs":1000}`)},
			{ID: "end", Type: workflow.NodeTypeEnd, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
		},
		Edges: []workflow.EdgeDefinition{
			{ID: "startToWait", FromNode: "start", FromPort: "input", ToNode: "wait", ToPort: "input"},
			{ID: "waitToEnd", FromNode: "wait", FromPort: "output", ToNode: "end", ToPort: "result"},
		},
	}
}
