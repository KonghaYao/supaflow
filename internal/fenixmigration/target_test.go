package fenixmigration

import (
	"context"
	"errors"
	"testing"

	"github.com/pazhou/supaflow/internal/definition"
	definitionflowgram "github.com/pazhou/supaflow/internal/definition/flowgram"
	"github.com/pazhou/supaflow/internal/platform"
	"github.com/pazhou/supaflow/internal/workflow"
)

type unusedPublisher struct{}

func (unusedPublisher) Publish(context.Context, string, string, workflow.WorkflowDefinition, string) (platform.PublishedPlan, bool, error) {
	return platform.PublishedPlan{}, false, errors.New("publisher is not used by draft import")
}

// 验证迁移草稿经过 FlowGram adapter 往返后仍保持 canonical 执行语义。
func TestFlowgramDocumentRoundTrip(t *testing.T) {
	definition := workflow.WorkflowDefinition{
		SchemaVersion: workflow.DefinitionSchemaV1,
		ID:            "flow",
		Nodes: []workflow.NodeDefinition{
			{ID: "start", Type: workflow.NodeTypeStart, Version: workflow.BuiltinNodeVersion, Config: []byte(`{}`)},
			{ID: "end", Type: workflow.NodeTypeEnd, Version: workflow.BuiltinNodeVersion, Config: []byte(`{}`)},
		},
		Edges: []workflow.EdgeDefinition{{ID: "edge", FromNode: "start", FromPort: "input", ToNode: "end", ToPort: "result"}},
	}
	document, err := flowgramDocument(definition)
	if err != nil {
		t.Fatal(err)
	}
	got, err := (definitionflowgram.Adapter{}).ToDefinition(document)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != definition.ID || len(got.Nodes) != 2 || len(got.Edges) != 1 {
		t.Fatalf("round trip = %#v", got)
	}
}

// 验证迁移草稿首次导入计为创建，同一幂等键重跑只计为重放。
func TestImportDraftReportsAtomicCreateOutcome(t *testing.T) {
	registry := workflow.BuiltinRegistry()
	service, err := definition.NewService(definition.NewMemoryStore(), definitionflowgram.Adapter{}, unusedPublisher{}, registry, migrationCompilePolicy())
	if err != nil {
		t.Fatal(err)
	}
	target := &PostgresTarget{definitions: service}
	value := workflow.WorkflowDefinition{
		SchemaVersion: workflow.DefinitionSchemaV1,
		ID:            "flow",
		Nodes: []workflow.NodeDefinition{
			{ID: "start", Type: workflow.NodeTypeStart, Version: workflow.BuiltinNodeVersion, Config: []byte(`{}`)},
			{ID: "end", Type: workflow.NodeTypeEnd, Version: workflow.BuiltinNodeVersion, Config: []byte(`{}`)},
		},
		Edges: []workflow.EdgeDefinition{{ID: "edge", FromNode: "start", FromPort: "input", ToNode: "end", ToPort: "result"}},
	}
	created, err := target.ImportDraft(context.Background(), "tenant", "flow", "migration-admin", value, "draft-import-stable-key")
	if err != nil || !created {
		t.Fatalf("first created=%t err=%v", created, err)
	}
	replayed, err := target.ImportDraft(context.Background(), "tenant", "flow", "migration-admin", value, "draft-import-stable-key")
	if err != nil || replayed {
		t.Fatalf("replay created=%t err=%v", replayed, err)
	}
}
