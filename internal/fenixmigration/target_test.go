package fenixmigration

import (
	"testing"

	definitionflowgram "github.com/pazhou/supaflow/internal/definition/flowgram"
	"github.com/pazhou/supaflow/internal/workflow"
)

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
