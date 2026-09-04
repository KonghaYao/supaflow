package shadowrun

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/pazhou/supaflow/internal/platform"
	"github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/workflow"
)

// 验证应用服务在创建 durable run 前校验完整 stub 清单，并把 shadow 模式写入运行绑定。
func TestServiceStartsOnlyFullyStubbedShadowRun(t *testing.T) {
	catalog := platform.NewMemoryCatalog(workflow.BuiltinRegistry(), workflow.CompilePolicy{
		AllowedCapabilities: workflow.CapabilitySet{Cancellable: true, Idempotent: true},
		AllowedPermissions:  workflow.PermissionSet{Network: true, Credentials: true},
	})
	definition := workflow.WorkflowDefinition{
		SchemaVersion: workflow.DefinitionSchemaV1,
		ID:            "shadow_flow",
		Nodes: []workflow.NodeDefinition{
			{ID: "start", Type: workflow.NodeTypeStart, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
			{ID: "request", Type: workflow.NodeTypeHTTP, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{"method":"POST","url":"https://example.com/orders"}`)},
			{ID: "end", Type: workflow.NodeTypeEnd, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
		},
		Edges: []workflow.EdgeDefinition{
			{ID: "to_request", FromNode: "start", FromPort: "input", ToNode: "request", ToPort: "body"},
			{ID: "to_end", FromNode: "request", FromPort: "response", ToNode: "end", ToPort: "result"},
		},
	}
	published, _, err := catalog.Publish(context.Background(), "tenant_a", definition.ID, definition, "publish-shadow-1")
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(catalog, runtime.NewService(runtime.NewMemoryStore(), nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	request := StartRequest{
		PlanID: published.ID, PlanHash: published.Hash, Input: json.RawMessage(`{"order":42}`),
		IdempotencyKey: "shadow-run-key-1",
	}
	_, err = service.Start(context.Background(), "tenant_a", request)
	var shadowErr *Error
	if !errors.As(err, &shadowErr) || shadowErr.Code != CodeShadowStubRequired {
		t.Fatalf("missing stub error = %#v", err)
	}
	request.StubBindings = map[string]string{"request": workflow.ShadowStubExecutorKind}
	created, err := service.Start(context.Background(), "tenant_a", request)
	if err != nil || !created.Created || created.Run.Plan.Executor != "shadow-v1:builtin.start" {
		t.Fatalf("created = %+v, err = %v", created, err)
	}
	replay, err := service.Start(context.Background(), "tenant_a", request)
	if err != nil || replay.Created || replay.Run.ID != created.Run.ID {
		t.Fatalf("replay = %+v, err = %v", replay, err)
	}
}
