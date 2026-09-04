package platform

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/pazhou/supaflow/internal/workflow"
)

func minimalDefinition(id string) workflow.WorkflowDefinition {
	return workflow.WorkflowDefinition{
		SchemaVersion: workflow.DefinitionSchemaV1,
		ID:            id,
		Nodes: []workflow.NodeDefinition{
			{ID: "start", Type: workflow.NodeTypeStart, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
			{ID: "end", Type: workflow.NodeTypeEnd, Version: workflow.BuiltinNodeVersion, Config: json.RawMessage(`{}`)},
		},
		Edges: []workflow.EdgeDefinition{{ID: "edge", FromNode: "start", FromPort: "input", ToNode: "end", ToPort: "result"}},
	}
}

// 验证发布版本不可变且同一租户幂等重放不会增加版本。
func TestMemoryCatalogPublishesIdempotently(t *testing.T) {
	catalog := NewMemoryCatalog(workflow.BuiltinRegistry(), workflow.CompilePolicy{
		AllowedCapabilities: workflow.CapabilitySet{Idempotent: true},
	})
	first, created, err := catalog.Publish(context.Background(), "tenant-a", "flow", minimalDefinition("flow"), "publish-key")
	if err != nil || !created {
		t.Fatalf("publish: created=%v err=%v", created, err)
	}
	replayed, created, err := catalog.Publish(context.Background(), "tenant-a", "flow", minimalDefinition("flow"), "publish-key")
	if err != nil || created || replayed.ID != first.ID || replayed.Version != 1 {
		t.Fatalf("replay=%+v created=%v err=%v", replayed, created, err)
	}
}

// 验证计划查找同时校验租户与内容哈希，避免跨租户枚举。
func TestMemoryCatalogResolveIsTenantScoped(t *testing.T) {
	catalog := NewMemoryCatalog(workflow.BuiltinRegistry(), workflow.CompilePolicy{
		AllowedCapabilities: workflow.CapabilitySet{Idempotent: true},
	})
	published, _, err := catalog.Publish(context.Background(), "tenant-a", "flow", minimalDefinition("flow"), "publish-key")
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.Resolve(context.Background(), "tenant-b", published.ID, published.Hash)
	if !errors.Is(err, ErrPlanNotFound) {
		t.Fatalf("expected plan not found, got %v", err)
	}
}
