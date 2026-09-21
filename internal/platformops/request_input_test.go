package platformops

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// 验证只接受数据库可以存储的请求目标：NUL 与非法 UTF-8 必须在进入处理器前被识别。
func TestValidRequestTargetRefusesUnstorableBytes(t *testing.T) {
	cases := []struct {
		name   string
		target string
		valid  bool
	}{
		{name: "plain", target: "/v1/workflows/flow-1/publish", valid: true},
		{name: "escaped unicode", target: "/v1/workflows/%E6%B5%81%E7%A8%8B/publish", valid: true},
		{name: "nul in path", target: "/v1/workflows/flow%00-1/publish", valid: false},
		{name: "invalid utf8 in path", target: "/v1/workflows/flow%FF-1/publish", valid: false},
		{name: "nul in query", target: "/v1/workflows?cursor=%00", valid: false},
		{name: "invalid utf8 in query", target: "/v1/runs?workflowId=%FF", valid: false},
		{name: "non ascii query", target: "/v1/runs?workflowId=%E6%B5%81%E7%A8%8B", valid: true},
	}
	for _, testCase := range cases {
		request := httptest.NewRequest(http.MethodGet, testCase.target, nil)
		if got := ValidRequestTarget(request); got != testCase.valid {
			t.Fatalf("%s ValidRequestTarget=%v want=%v", testCase.name, got, testCase.valid)
		}
	}
}

// 验证只有 Postgres 的「无法存储字符」错误被归类为调用方问题，其他存储错误仍属服务端故障。
func TestIsUnstorableTextOnlyMatchesUntranslatableCharacter(t *testing.T) {
	unstorable := fmt.Errorf("append audit record: %w", &pgconn.PgError{Code: "22021", Message: "invalid byte sequence for encoding \"UTF8\": 0x00"})
	if !IsUnstorableText(unstorable) {
		t.Fatal("expected 22021 to be classified as unstorable text")
	}
	other := &pgconn.PgError{Code: "23505", Message: "duplicate key value"}
	if IsUnstorableText(other) {
		t.Fatal("expected unique violation to stay a server-side failure")
	}
	if IsUnstorableText(fmt.Errorf("dial tcp: connection refused")) {
		t.Fatal("expected a non-postgres error to stay a server-side failure")
	}
}
