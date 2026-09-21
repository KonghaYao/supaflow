package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// 验证未预期的失败会返回 500 并留下同 requestId 的服务端日志：
// 此前这条路径不写任何日志，调用方拿到的 requestId 无处可查，故障无法归因。
func TestWriteInternalErrorLogsTheSameRequestID(t *testing.T) {
	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&output, nil)))
	defer slog.SetDefault(previous)

	request := httptest.NewRequest(http.MethodPost, "/v1/workflows/flow-x/publish", nil)
	response := httptest.NewRecorder()
	writeInternalError(response, request, errors.New("token=super-secret-value rejected"))

	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d", response.Code)
	}
	var body struct {
		RequestID string `json:"requestId"`
		Code      string `json:"code"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Code != "internal_error" || body.RequestID == "" {
		t.Fatalf("body=%+v", body)
	}
	line := output.String()
	// 日志里的 requestId 必须就是响应里返回的那个，否则运维按 id 检索不到任何东西。
	if !strings.Contains(line, body.RequestID) {
		t.Fatalf("log=%s missing requestId=%s", line, body.RequestID)
	}
	// 诊断信息记入日志前必须脱敏，且响应体不得泄露内部细节。
	if strings.Contains(line, "super-secret-value") || !strings.Contains(line, "REDACTED") {
		t.Fatalf("log=%s", line)
	}
	if strings.Contains(response.Body.String(), "super-secret-value") {
		t.Fatalf("response leaked the diagnostic: %s", response.Body.String())
	}
}

// 验证调用方自带 requestId 时响应与日志沿用该值，不另行生成：
// 否则关联客户端日志与服务端日志时会对不上。
func TestWriteInternalErrorKeepsCallerRequestID(t *testing.T) {
	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&output, nil)))
	defer slog.SetDefault(previous)

	request := httptest.NewRequest(http.MethodGet, "/v1/runs/run-1", nil)
	request.Header.Set("X-Request-ID", "caller-supplied-id")
	response := httptest.NewRecorder()
	writeInternalError(response, request, errors.New("boom"))

	var body struct {
		RequestID string `json:"requestId"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.RequestID != "caller-supplied-id" || !strings.Contains(output.String(), "caller-supplied-id") {
		t.Fatalf("requestId=%s log=%s", body.RequestID, output.String())
	}
}
