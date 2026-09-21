package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pazhou/supaflow/internal/platformops"
	"github.com/pazhou/supaflow/internal/workflow"
)

// failingAuditRecorder 让审计写入必然失败，用于验证「审计证据写不进去」这条 500 通路。
type failingAuditRecorder struct{ err error }

func (r failingAuditRecorder) Record(context.Context, platformops.AuditRecord) (platformops.AuditRecord, error) {
	return platformops.AuditRecord{}, r.err
}

// 验证审计写入失败时返回 500 audit_unavailable，并留下与响应同 requestId、同问题码的服务端日志。
// 这条通路不经过 writeError 的兜底分支，上一轮给它补日志时被漏掉：调用方拿到 requestId 报障，
// 运维在日志里搜不到任何一行。触发它需要一个本身就被拒的变更请求（校验失败的定义），
// 这样中间件才会去记录失败证据；成功路径在内存目录下走的是 mutation_not_durable 分支。
func TestAuditFailureLogsTheSameRequestID(t *testing.T) {
	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&output, nil)))
	defer slog.SetDefault(previous)

	rejected := workflow.WorkflowDefinition{SchemaVersion: workflow.DefinitionSchemaV1, ID: "flow-audit"}
	handler := testHandlerWithAudit(t, failingAuditRecorder{err: errors.New("append audit record: connection refused")}).Routes()
	response := requestJSON(t, handler, http.MethodPost, "/v1/workflows/flow-audit/publish", organizationAKey, "audit-log-key-1", rejected)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var failure problem
	if err := json.Unmarshal(response.Body.Bytes(), &failure); err != nil {
		t.Fatal(err)
	}
	if failure.Code != "audit_unavailable" || failure.RequestID == "" {
		t.Fatalf("body=%+v", failure)
	}
	line := output.String()
	if !strings.Contains(line, failure.RequestID) || !strings.Contains(line, "audit_unavailable") {
		t.Fatalf("log=%s missing requestId=%s code=audit_unavailable", line, failure.RequestID)
	}
	// 诊断信息只进日志：响应体不得泄露底层存储细节。
	if strings.Contains(response.Body.String(), "connection refused") {
		t.Fatalf("response leaked the diagnostic: %s", response.Body.String())
	}
}

// 验证审计写入因调用方字节失败时返回 400 而不是 500 audit_unavailable：审计记录的每个字段
// 都来自请求，字节存不进 column 是调用方可改的输入问题。报成「审计不可用」会让运维去查
// 审计系统，也让调用方重试一个永远不会成功的请求。
func TestAuditUnstorableBytesAreAClientError(t *testing.T) {
	rejected := workflow.WorkflowDefinition{SchemaVersion: workflow.DefinitionSchemaV1, ID: "flow-audit-bytes"}
	unstorable := fmt.Errorf("append audit record: %w",
		&pgconn.PgError{Code: "22021", Message: `invalid byte sequence for encoding "UTF8": 0xff`})
	handler := testHandlerWithAudit(t, failingAuditRecorder{err: unstorable}).Routes()
	response := requestJSON(t, handler, http.MethodPost, "/v1/workflows/flow-audit-bytes/publish", organizationAKey, "audit-bytes-key-1", rejected)
	assertProblemCode(t, response, http.StatusBadRequest, "invalid_argument")
}
