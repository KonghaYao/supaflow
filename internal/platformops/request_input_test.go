package platformops

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
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

// 验证请求体里「JSON 语法合法、存下去却会变形或存不下」的字节在边缘被识别：\u0000 与孤立代理、
// 以及裸的非法 UTF-8 字节都拒绝，合法代理对与字面量 \\u0000 放行。放宽这一格会让调用方拿到 500，
// 或让两个不同的名字在库里合并成一个（解码器把非法字节换成 U+FFFD），收紧则会把能存的文档挡在门外。
func TestStorableJSONMatchesOnlyEscapesJSONBRejects(t *testing.T) {
	cases := []struct {
		name     string
		body     string
		storable bool
	}{
		{name: "转义形式的 NUL 被拒", body: `{"label":"\u0000"}`, storable: false},
		{name: "字符串中间的 NUL 被拒", body: `{"label":"a\u0000b"}`, storable: false},
		{name: "键名里的 NUL 被拒", body: `{"\u0000":1}`, storable: false},
		{name: "孤立高位代理被拒", body: `{"label":"\ud800"}`, storable: false},
		{name: "孤立低位代理被拒", body: `{"label":"\udc00"}`, storable: false},
		{name: "两个高位代理相连被拒", body: `{"label":"\ud800\ud800"}`, storable: false},
		{name: "高位代理后跟普通转义被拒", body: `{"label":"\ud800\n"}`, storable: false},
		{name: "代理对在字符串末尾截断被拒", body: `{"label":"\ud83d"}`, storable: false},
		{name: "合法代理对放行", body: `{"label":"\ud83d\ude00"}`, storable: true},
		{name: "大写十六进制的代理对放行", body: `{"label":"\uD83D\uDE00"}`, storable: true},
		{name: "双写反斜杠是字面量 u0000，放行", body: `{"label":"\\u0000"}`, storable: true},
		{name: "控制字符转义放行", body: `{"label":"\u001f"}`, storable: true},
		{name: "非字符 U+FFFF 放行", body: `{"label":"\uffff"}`, storable: true},
		{name: "引号与换行等普通转义放行", body: `{"label":"\"quoted\"\n\t\\"}`, storable: true},
		{name: "字符串以转义反斜杠结尾放行", body: `{"label":"ends with \\"}`, storable: true},
		{name: "非 ASCII 与 emoji 原文放行", body: `{"label":"流程 🚀"}`, storable: true},
		{name: "不含转义的大 body 放行", body: `{"document":{` + strings.Repeat(`"node":"value",`, 4096) + `"end":1}}`, storable: true},
		{name: "字符串外的转义不由本函数判定，语法错误交给解码器", body: `{"label": \u0000}`, storable: true},
		{name: "字符串里的裸非法字节被拒", body: `{"label":"name-a` + "\xff" + `"}`, storable: false},
		{name: "另一个裸非法字节同样被拒", body: `{"label":"name-a` + "\xfe" + `"}`, storable: false},
		{name: "字符串外的裸非法字节也被拒", body: `{` + "\xf5" + `"label":"ok"}`, storable: false},
		{name: "截断的多字节序列被拒", body: `{"label":"` + "\xe4\xb8" + `"}`, storable: false},
		{name: "合法 UTF-8 原文放行", body: `{"label":"流程` + "\xf0\x9f\x9a\x80" + `"}`, storable: true},
	}
	for _, testCase := range cases {
		if got := StorableJSON([]byte(testCase.body)); got != testCase.storable {
			t.Fatalf("%s StorableJSON=%v want=%v", testCase.name, got, testCase.storable)
		}
	}
}

// 验证「Postgres 说这些字节存不下」只对调用方输入成立：22021（text 编码）、22P05（jsonb
// 转义）与指向 json 的 22P02 归调用方，其余存储故障仍属服务端，否则内部 bug 会被说成 400。
func TestIsUnstorableTextMatchesOnlyUnstorableInput(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "text 无法编码 22021", err: &pgconn.PgError{Code: "22021", Message: `invalid byte sequence for encoding "UTF8": 0xff`}, want: true},
		{name: "jsonb 转义无法转换 22P05", err: &pgconn.PgError{Code: "22P05", Message: "unsupported Unicode escape sequence"}, want: true},
		{name: "json 语法错误 22P02", err: &pgconn.PgError{Code: "22P02", Message: "invalid input syntax for type json"}, want: true},
		{name: "整数输入错误 22P02 不算调用方字节问题", err: &pgconn.PgError{Code: "22P02", Message: `invalid input syntax for type integer: "x"`}, want: false},
		{name: "22P02 无消息时保守判为服务端故障", err: &pgconn.PgError{Code: "22P02"}, want: false},
		{name: "唯一约束冲突仍是服务端故障", err: &pgconn.PgError{Code: "23505", Message: "duplicate key value"}, want: false},
		{name: "数据库启动中仍是服务端故障", err: &pgconn.PgError{Code: "57P03", Message: "the database system is starting up"}, want: false},
		{name: "非 Postgres 错误仍是服务端故障", err: fmt.Errorf("dial tcp: connection refused"), want: false},
	}
	for _, testCase := range cases {
		if got := IsUnstorableText(testCase.err); got != testCase.want {
			t.Fatalf("%s IsUnstorableText=%v want=%v", testCase.name, got, testCase.want)
		}
	}
	// 服务层用 %w 包过再返回的错误同样要能识别，否则归类防线在真实调用链上失效。
	wrapped := fmt.Errorf("append audit record: %w", &pgconn.PgError{Code: "22021", Message: "invalid byte sequence for encoding \"UTF8\": 0xff"})
	if !IsUnstorableText(wrapped) {
		t.Fatal("expected a wrapped unstorable-text error to stay classified")
	}
}
