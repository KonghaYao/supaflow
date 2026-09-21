package api

import (
	"encoding/json"
	"net/http"
	"testing"
)

// editorDocument 是编辑器保存的文档形状：每个节点都带画布标签，以及分组与视口元数据。
func editorDocument(id string) json.RawMessage {
	return json.RawMessage(`{"schemaVersion":"flowgram-v1","workflowId":"` + id + `","nodes":[` +
		`{"id":"start","type":"start","version":"1.0.0","label":"Start","config":{},"inputs":{},"position":{"x":80,"y":180}},` +
		`{"id":"end","type":"end","version":"1.0.0","label":"End","config":{},"inputs":{},"position":{"x":520,"y":180}}],` +
		`"edges":[{"id":"start-end","sourceNodeId":"start","sourcePortId":"input","targetNodeId":"end","targetPortId":"result"}],` +
		`"groups":[],"viewport":{"x":0,"y":0,"zoom":1}}`)
}

// 验证编辑器保存的文档能直接编译与发布。严格解码缺少 label 声明时，前端保存的每一份
// 草稿都会在编译、试运行和发布草稿三个入口失败，这是阻断前端主流程的缺陷。
func TestStudioAcceptsEditorSavedDocument(t *testing.T) {
	handler := studioHandler(t)
	created := requestJSON(t, handler, http.MethodPost, "/v1/workflows", organizationAKey, "editor-create-0001",
		map[string]any{"workflowId": "editor-flow", "document": editorDocument("editor-flow")})
	if created.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", created.Code, created.Body.String())
	}
	compiled := requestJSON(t, handler, http.MethodPost, "/v1/workflows/editor-flow/compile", organizationAKey, "", nil)
	if compiled.Code != http.StatusOK {
		t.Fatalf("compile status=%d body=%s", compiled.Code, compiled.Body.String())
	}
	published := requestJSON(t, handler, http.MethodPost, "/v1/workflows/editor-flow/publish-draft", organizationAKey, "editor-publish-0001", nil)
	if published.Code != http.StatusCreated {
		t.Fatalf("publish status=%d body=%s", published.Code, published.Body.String())
	}
}

// 验证无法编译的文档在写入边界就被拒绝，而不是先存下来、之后每次编译或发布都失败。
func TestStudioRejectsUnusableDocumentAtWrite(t *testing.T) {
	handler := studioHandler(t)

	mismatched := requestJSON(t, handler, http.MethodPost, "/v1/workflows", organizationAKey, "unusable-create-0001",
		map[string]any{"workflowId": "unusable-flow", "document": editorDocument("other-flow")})
	assertProblemCode(t, mismatched, http.StatusBadRequest, "invalid_workflow")

	stored := requestJSON(t, handler, http.MethodGet, "/v1/workflows/unusable-flow", organizationAKey, "", nil)
	if stored.Code != http.StatusNotFound {
		t.Fatalf("rejected create stored a draft: status=%d body=%s", stored.Code, stored.Body.String())
	}

	valid := requestJSON(t, handler, http.MethodPost, "/v1/workflows", organizationAKey, "unusable-create-0002",
		map[string]any{"workflowId": "unusable-flow", "document": editorDocument("unusable-flow")})
	if valid.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", valid.Code, valid.Body.String())
	}
	undecodable := requestJSON(t, handler, http.MethodPut, "/v1/workflows/unusable-flow", organizationAKey, "unusable-update-0001",
		map[string]any{"document": json.RawMessage(`{"schemaVersion":"flowgram-v1","workflowId":"unusable-flow","nodes":[],"edges":[],"unexpected":true}`), "expectedRevision": 1})
	assertProblemCode(t, undecodable, http.StatusBadRequest, "invalid_workflow")
}

// 验证缺少 expectedRevision 的自动保存请求返回 400：这是调用方可以修正的请求形状问题。
func TestStudioUpdateRequiresExpectedRevision(t *testing.T) {
	handler := studioHandler(t)
	created := requestJSON(t, handler, http.MethodPost, "/v1/workflows", organizationAKey, "revision-create-0001",
		map[string]any{"workflowId": "revision-flow", "document": editorDocument("revision-flow")})
	if created.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", created.Code, created.Body.String())
	}
	response := requestJSON(t, handler, http.MethodPut, "/v1/workflows/revision-flow", organizationAKey, "revision-update-0001",
		map[string]any{"document": editorDocument("revision-flow")})
	assertProblemCode(t, response, http.StatusBadRequest, "invalid_argument")
}
