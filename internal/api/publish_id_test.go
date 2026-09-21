package api

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/pazhou/supaflow/internal/workflow"
)

// 验证发布时 body 定义的 id 必须与路径 id 一致：缺 id 或 id 为空此前落进 catalog 守卫，
// 被 writeError 的 default 分支渲染成 500，调用方无法区分「参数传错」与「服务异常」。
func TestPublishRejectsDefinitionIDThatDoesNotMatchPath(t *testing.T) {
	handler := testHandler(t)
	cases := []struct {
		name       string
		path       string
		key        string
		definition workflow.WorkflowDefinition
	}{
		{name: "missing", path: "/v1/workflows/flow-missing-id/publish", key: "publish-id-missing", definition: minimalDefinition("")},
		{name: "mismatch", path: "/v1/workflows/flow-path/publish", key: "publish-id-mismatch", definition: minimalDefinition("flow-body")},
	}
	for _, testCase := range cases {
		response := requestJSON(t, handler, http.MethodPost, testCase.path, organizationAKey, testCase.key, testCase.definition)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("%s status=%d body=%s", testCase.name, response.Code, response.Body.String())
		}
		// 问题码要指出是参数问题，而不是把客户端错误伪装成服务端故障。
		var failure problem
		if err := json.Unmarshal(response.Body.Bytes(), &failure); err != nil {
			t.Fatal(err)
		}
		if failure.Code != "invalid_argument" {
			t.Fatalf("%s code=%s body=%s", testCase.name, failure.Code, response.Body.String())
		}
	}
	// 合法的定义必须照常发布，避免守卫把正常请求也拦下。
	published := requestJSON(t, handler, http.MethodPost, "/v1/workflows/flow-ok/publish", organizationAKey, "publish-id-ok", minimalDefinition("flow-ok"))
	if published.Code != http.StatusCreated {
		t.Fatalf("valid publish status=%d body=%s", published.Code, published.Body.String())
	}
}
