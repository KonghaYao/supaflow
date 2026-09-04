package flowgram

import (
	"encoding/json"
	"testing"

	"github.com/pazhou/supaflow/internal/workflow"
)

// 验证 FlowGram 的画布坐标、分组和展示数据不会泄漏到可执行定义。
func TestAdapterSeparatesEditorAndCanonicalModels(t *testing.T) {
	document := json.RawMessage(`{
		"schemaVersion":"flowgram-v1","workflowId":"flow",
		"nodes":[
			{"id":"start","type":"start","version":"1.0.0","config":{},"inputs":{},"position":{"x":10,"y":20},"groupId":"group-a","ui":{"collapsed":true}},
			{"id":"end","type":"end","version":"1.0.0","config":{},"inputs":{},"position":{"x":300,"y":20}}
		],
		"edges":[{"id":"edge","sourceNodeId":"start","sourcePortId":"input","targetNodeId":"end","targetPortId":"result"}],
		"groups":[{"id":"group-a","title":"Main","nodeIds":["start"]}],
		"viewport":{"x":1,"y":2,"zoom":0.8}
	}`)
	definition, err := (Adapter{}).ToDefinition(document)
	if err != nil {
		t.Fatal(err)
	}
	if definition.ID != "flow" || definition.SchemaVersion != workflow.DefinitionSchemaV1 {
		t.Fatalf("definition=%+v", definition)
	}
	if len(definition.Nodes) != 2 || len(definition.Edges) != 1 {
		t.Fatalf("nodes=%+v edges=%+v", definition.Nodes, definition.Edges)
	}
	encoded, _ := json.Marshal(definition)
	for _, forbidden := range []string{"position", "groupId", "viewport", "collapsed"} {
		if json.Valid(encoded) && containsJSONKey(encoded, forbidden) {
			t.Fatalf("canonical definition leaked %q: %s", forbidden, encoded)
		}
	}
}

// 验证转换器拒绝未知字段，防止前后端 schema 漂移被静默吞掉。
func TestAdapterRejectsUnknownFields(t *testing.T) {
	_, err := (Adapter{}).ToDefinition(json.RawMessage(`{"schemaVersion":"flowgram-v1","workflowId":"flow","nodes":[],"edges":[],"unexpected":true}`))
	if err == nil {
		t.Fatal("expected unknown field error")
	}
}

func containsJSONKey(document []byte, key string) bool {
	var value any
	_ = json.Unmarshal(document, &value)
	return walkForKey(value, key)
}

func walkForKey(value any, key string) bool {
	switch typed := value.(type) {
	case map[string]any:
		for current, nested := range typed {
			if current == key || walkForKey(nested, key) {
				return true
			}
		}
	case []any:
		for _, nested := range typed {
			if walkForKey(nested, key) {
				return true
			}
		}
	}
	return false
}
