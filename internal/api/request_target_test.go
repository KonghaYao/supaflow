package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// 验证请求目标里的非法字节在进入处理器前返回 400：此前这类路径会一路走到审计写入，
// Postgres 以 22021 拒绝后整个响应被替换成 500 audit_unavailable，既掩盖了客户端的
// 参数错误，又谎称审计证据丢失。
func TestUnstorableRequestTargetIsRejectedBeforeHandler(t *testing.T) {
	handler := testHandler(t)
	cases := []struct {
		name   string
		method string
		target string
	}{
		{name: "nul in path", method: http.MethodPost, target: "/v1/workflows/flow%00-1/publish"},
		{name: "invalid utf8 in path", method: http.MethodGet, target: "/v1/workflows/flow%FF-1"},
		{name: "nul in cursor", method: http.MethodGet, target: "/v1/workflows?cursor=%00"},
		{name: "nul in run id", method: http.MethodPost, target: "/v1/runs/run_%00x/cancel"},
	}
	for _, testCase := range cases {
		response := requestJSON(t, handler, testCase.method, testCase.target, organizationAKey, "target-key-0001", nil)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("%s status=%d body=%s", testCase.name, response.Code, response.Body.String())
		}
	}
}

// 验证「数据库无法存储这些字节」被归类为调用方参数错误，而其他存储错误仍是服务端故障。
func TestWriteErrorClassifiesUnstorableStoredBytes(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/v1/runs", nil)
	recorder := httptest.NewRecorder()
	(&Handler{}).writeError(recorder, request, fmt.Errorf("resolve execution plan: %w", &pgconn.PgError{Code: "22021"}))
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	failed := httptest.NewRecorder()
	(&Handler{}).writeError(failed, request, &pgconn.PgError{Code: "57P03", Message: "the database system is starting up"})
	if failed.Code != http.StatusInternalServerError {
		t.Fatalf("storage outage status=%d body=%s", failed.Code, failed.Body.String())
	}
}
