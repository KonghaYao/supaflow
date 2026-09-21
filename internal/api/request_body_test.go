package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// 验证请求体里 jsonb 存不下的转义在进入处理器前返回 400：\u0000 与孤立代理此前会一路写进
// jsonb 列，被 Postgres 以 22P05 / 22P02 拒绝后渲染成 500 internal_error，调用方既看不到
// 是哪个字段的问题，也被引导去重试一个永远不会成功的请求。
func TestUnstorableRequestBodyIsRejectedBeforeHandler(t *testing.T) {
	handler := studioHandler(t)
	nul := requestJSON(t, handler, http.MethodPost, "/v1/workflows", organizationAKey, "body-escapes-create-1",
		map[string]any{"workflowId": "body-escapes", "document": documentWithLabel("body-escapes", `\u0000`)})
	assertProblemCode(t, nul, http.StatusBadRequest, "invalid_argument")

	surrogate := requestJSON(t, handler, http.MethodPost, "/v1/workflows", organizationAKey, "body-escapes-create-2",
		map[string]any{"workflowId": "body-escapes", "document": documentWithLabel("body-escapes", `\ud800`)})
	assertProblemCode(t, surrogate, http.StatusBadRequest, "invalid_argument")

	// 对照组：合法规避——代理对与非 ASCII 文本必须照常写下来，边缘校验不能把能存的文档挡在门外。
	accepted := requestJSON(t, handler, http.MethodPost, "/v1/workflows", organizationAKey, "body-escapes-create-3",
		map[string]any{"workflowId": "body-escapes", "document": documentWithLabel("body-escapes", `\ud83d\ude00`)})
	if accepted.Code != http.StatusCreated {
		t.Fatalf("accepted status=%d body=%s", accepted.Code, accepted.Body.String())
	}

	// 更新路径同样在解码边界拦住，不需要先写坏一次草稿。
	updated := requestJSON(t, handler, http.MethodPut, "/v1/workflows/body-escapes", organizationAKey, "body-escapes-update-1",
		map[string]any{"document": documentWithLabel("body-escapes", `\u0000`), "expectedRevision": 1})
	assertProblemCode(t, updated, http.StatusBadRequest, "invalid_argument")
}

// 验证语法错误仍然按 invalid_json 回答：非法 JSON 先于可存性校验失败，两者的语义不能混。
func TestInvalidJSONKeepsItsOwnCode(t *testing.T) {
	handler := studioHandler(t)
	request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/workflows",
		strings.NewReader(`{"workflowId":"body-invalid","document":`))
	authenticatedTestRequest(request, organizationAKey)
	request.Header.Set("Idempotency-Key", "body-invalid-json-1")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	assertProblemCode(t, response, http.StatusBadRequest, "invalid_json")
}

// 验证请求体里的裸非法 UTF-8 字节在解码边界返回 400：JSON 文档按 RFC 8259 必须是 UTF-8，
// Go 的解码器却会把它静默换成 U+FFFD——两个只差最后一个字节的名字因此在库里合并成一个，
// 第一个 201、第二个 409 conflict，调用方被告知「你已经建过了」，而它真正要的名字永远建不出来。
// 这是有意的行为变更：此前这类 body 会被接受。
func TestInvalidUTF8RequestBodyIsRejectedBeforeHandler(t *testing.T) {
	handler := studioHandler(t)
	// 裸字节必须手工拼：json.Marshal 会把非法序列预先换成 \ufffd，那正是要拦掉的那种静默替换。
	request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/workflows",
		strings.NewReader(`{"workflowId":"body-utf8`+"\xff"+`","document":`+string(documentWithLabel("body-utf8", "ok"))+`}`))
	authenticatedTestRequest(request, organizationAKey)
	request.Header.Set("Idempotency-Key", "body-utf8-create-1")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	assertProblemCode(t, response, http.StatusBadRequest, "invalid_argument")

	// 对照组：合法 UTF-8 文本必须照常写下来，边缘校验不能把能存的文档挡在门外。
	accepted := requestJSON(t, handler, http.MethodPost, "/v1/workflows", organizationAKey, "body-utf8-create-2",
		map[string]any{"workflowId": "body-utf8", "document": documentWithLabel("body-utf8", "名称 🚀")})
	if accepted.Code != http.StatusCreated {
		t.Fatalf("accepted status=%d body=%s", accepted.Code, accepted.Body.String())
	}
}

// documentWithLabel 构造编辑器形状的文档，label 原样嵌入，用于精确输送某个转义序列。
func documentWithLabel(id, label string) json.RawMessage {
	return json.RawMessage(`{"schemaVersion":"flowgram-v1","workflowId":"` + id + `","nodes":[` +
		`{"id":"start","type":"start","version":"1.0.0","label":"` + label + `","config":{},"inputs":{},"position":{"x":80,"y":180}},` +
		`{"id":"end","type":"end","version":"1.0.0","label":"End","config":{},"inputs":{},"position":{"x":520,"y":180}}],` +
		`"edges":[{"id":"start-end","sourceNodeId":"start","sourcePortId":"input","targetNodeId":"end","targetPortId":"result"}],` +
		`"groups":[],"viewport":{"x":0,"y":0,"zoom":1}}`)
}
