package flowgram

import (
	"encoding/json"
	"strings"
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

// 验证文档形状错误只描述 JSON 形状与字段：message 会原样进入对外的校验问题，
// 不能带上 Go 包名与类型名（"flowgram.document"、"of type flowgram.viewport"）。
func TestAdapterReportsDocumentShapeWithoutGoTypeNames(t *testing.T) {
	cases := []struct {
		name  string
		body  string
		field string
	}{
		{name: "顶层不是对象", body: `[1,2]`, field: ""},
		{name: "viewport 形状错误", body: `{"schemaVersion":"flowgram-v1","workflowId":"flow","nodes":[],"edges":[],"viewport":[]}`, field: "viewport"},
	}
	for _, testCase := range cases {
		_, err := (Adapter{}).ToDefinition(json.RawMessage(testCase.body))
		if err == nil {
			t.Fatalf("%s: expected an error", testCase.name)
		}
		message := err.Error()
		if strings.Contains(message, "Go struct field") || strings.Contains(message, "of type") {
			t.Fatalf("%s leaked Go internals: %s", testCase.name, message)
		}
		if testCase.field != "" && !strings.Contains(message, testCase.field) {
			t.Fatalf("%s lost the offending field: %s", testCase.name, message)
		}
	}
}

// 验证转换器接受编辑器实际保存的文档形状，label 作为展示字段被丢弃。
// 这里的文档逐字对应当前端的 web/src/domain/default-document.ts，改动前端默认文档时
// 必须同步本用例：严格解码下未声明的字段会让编辑器保存的每一份草稿都无法编译、
// 试运行或发布。
func TestAdapterAcceptsEditorDefaultDocument(t *testing.T) {
	document := json.RawMessage(`{
		"schemaVersion":"flowgram-v1","workflowId":"flow",
		"nodes":[
			{"id":"start","type":"start","version":"1.0.0","label":"Start","config":{},"inputs":{},"position":{"x":80,"y":180}},
			{"id":"end","type":"end","version":"1.0.0","label":"End","config":{},"inputs":{},"position":{"x":520,"y":180}}
		],
		"edges":[{"id":"start-end","sourceNodeId":"start","sourcePortId":"input","targetNodeId":"end","targetPortId":"result"}],
		"groups":[],
		"viewport":{"x":0,"y":0,"zoom":1}
	}`)
	definition, err := (Adapter{}).ToDefinition(document)
	if err != nil {
		t.Fatalf("editor document was rejected: %v", err)
	}
	if len(definition.Nodes) != 2 || definition.Nodes[0].ID != "start" {
		t.Fatalf("nodes=%+v", definition.Nodes)
	}
	encoded, _ := json.Marshal(definition)
	if containsJSONKey(encoded, "label") {
		t.Fatalf("canonical definition leaked label: %s", encoded)
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
