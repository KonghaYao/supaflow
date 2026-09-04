package openapi

import (
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// 验证稳定契约是可被 OpenAPI 工具链解析的 YAML 文档。
func TestOpenAPIDocumentParses(t *testing.T) {
	contents, err := os.ReadFile("openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err = yaml.Unmarshal(contents, &document); err != nil {
		t.Fatal(err)
	}
	if document["openapi"] != "3.1.0" {
		t.Fatalf("unexpected OpenAPI version %v", document["openapi"])
	}
}

// 验证 compile 与 publish 的成功响应 schema 和实际 Handler 返回保持一致。
func TestWorkflowSuccessResponseContracts(t *testing.T) {
	contents, err := os.ReadFile("openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	document := string(contents)
	compile := contractSection(t, document, "  /workflows/compile:", "  /workflows/{workflowId}/publish:")
	if !strings.Contains(compile, "$ref: '#/components/schemas/ExecutionPlan'") ||
		strings.Contains(compile, "$ref: '#/components/schemas/PublishedPlan'") {
		t.Fatal("compile success response must be ExecutionPlan")
	}
	publish := contractSection(t, document, "  /workflows/{workflowId}/publish:", "  /runs:")
	if !strings.Contains(publish, "        '200':") || !strings.Contains(publish, "        '201':") {
		t.Fatal("publish must document both idempotent 200 and created 201")
	}
	if strings.Count(publish, "$ref: '#/components/schemas/PublishedPlan'") != 2 {
		t.Fatal("publish 200 and 201 must both return PublishedPlan")
	}
}

// 验证节点发现契约显式暴露持久化版本与配置迁移 metadata。
func TestNodeTypeRegistryContract(t *testing.T) {
	contents, err := os.ReadFile("openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	document := string(contents)
	nodeType := contractSection(t, document, "    NodeType:", "    ExecutionPlan:")
	for _, required := range []string{"minimumRunnerVersion", "migrations", "fromVersion", "toVersion", "binding"} {
		if !strings.Contains(nodeType, required) {
			t.Fatalf("NodeType contract missing %q", required)
		}
	}
}

// 验证 Run 契约包含可选终态输出，并用不泄露对象引用的元数据建模外部存储。
func TestRunOutputContract(t *testing.T) {
	contents, err := os.ReadFile("openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	document := string(contents)
	run := contractSection(t, document, "    Run:", "    RunOutputMetadata:")
	metadata := contractSection(t, document, "    RunOutputMetadata:", "    Interrupt:")
	if !strings.Contains(run, "output:") || !strings.Contains(run, "outputMetadata:") {
		t.Fatal("Run contract must expose optional terminal output and metadata")
	}
	for _, required := range []string{"contentType", "sizeBytes", "sha256", "sensitive", "storage", "enum: [inline, object]"} {
		if !strings.Contains(metadata, required) {
			t.Fatalf("RunOutputMetadata missing %q", required)
		}
	}
	if strings.Contains(metadata, "objectRef") {
		t.Fatal("RunOutputMetadata must not expose internal object references")
	}
}

// 验证重跑契约要求幂等键、严格可选 fromNodeId，并返回 lineage 投影。
func TestRerunContract(t *testing.T) {
	contents, err := os.ReadFile("openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	document := string(contents)
	rerun := contractSection(t, document, "  /runs/{runId}/rerun:", "  /runs/{runId}/cancel:")
	for _, required := range []string{"operationId: rerunRun", "IdempotencyKey", "additionalProperties: false", "fromNodeId", "timeoutMs", "'202'", "'409'"} {
		if !strings.Contains(rerun, required) {
			t.Fatalf("rerun contract missing %q", required)
		}
	}
	run := contractSection(t, document, "    Run:", "    RunOutputMetadata:")
	if !strings.Contains(run, "rerun: { $ref: '#/components/schemas/RerunLineage' }") {
		t.Fatal("Run contract must expose rerun lineage")
	}
}

// 验证 durable run deadline 与公开 webhook resume 的安全边界进入稳定契约。
func TestDeadlineAndWebhookResumeContracts(t *testing.T) {
	contents, err := os.ReadFile("openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	document := string(contents)
	start := contractSection(t, document, "  /runs:", "  /runs/{runId}:")
	for _, required := range []string{"timeoutMs", "minimum: 1000", "maximum: 2592000000", "additionalProperties: false"} {
		if !strings.Contains(start, required) {
			t.Fatalf("start run deadline contract missing %q", required)
		}
	}
	resume := contractSection(t, document, "  /hooks/resume/{interruptId}:", "  /ops/health:")
	for _, required := range []string{"security: []", "Authorization", "^Resume", "X-Supaflow-Timestamp", "IdempotencyKey", "'401'", "resumeWebhookInterrupt"} {
		if !strings.Contains(resume, required) {
			t.Fatalf("webhook resume contract missing %q", required)
		}
	}
}

// 验证 Phase 3 运维、公网 webhook 与安全凭据 DTO 均进入稳定契约。
func TestPlatformOperationsContracts(t *testing.T) {
	contents, err := os.ReadFile("openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	document := string(contents)
	paths := []string{
		"  /ops/health:", "  /ops/ready:", "  /ops/metrics:", "  /ops/triggers:",
		"  /hooks/{publicHash}:", "  /ops/credentials:", "  /ops/quotas:",
		"  /ops/events/{eventType}:",
		"  /ops/audit:", "  /ops/retention:", "  /ops/exports:",
		"  /ops/workers/{workerId}/drain:", "  /ops/dlq:",
		"  /ops/webhook-subscriptions:",
	}
	for _, path := range paths {
		if !strings.Contains(document, path) {
			t.Fatalf("missing Phase 3 contract path %s", path)
		}
	}
	hook := contractSection(t, document, "  /hooks/{publicHash}:", "  /ops/credentials:")
	for _, required := range []string{"security: []", "X-Supaflow-Request-ID", "X-Supaflow-Timestamp", "X-Supaflow-Signature", "minLength: 16", "maxLength: 128"} {
		if !strings.Contains(hook, required) {
			t.Fatalf("webhook contract missing %q", required)
		}
	}
	credential := contractSection(t, document, "    Credential:", "    Quota:")
	for _, forbidden := range []string{"secret:", "ciphertext", "wrappedDataKey", "nonce:"} {
		if strings.Contains(credential, forbidden) {
			t.Fatalf("credential response leaks %q", forbidden)
		}
	}
	writeCredential := contractSection(t, document, "    WriteCredential:", "    Credential:")
	if !strings.Contains(writeCredential, "writeOnly: true") {
		t.Fatal("credential secret input must be writeOnly")
	}
}

// 验证事件 fanout 的稳定 429 契约要求同一幂等身份续传，并明确已提交前缀不会重复创建。
func TestEventFanoutRateLimitRetryContract(t *testing.T) {
	contents, err := os.ReadFile("openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	event := contractSection(t, string(contents), "  /ops/events/{eventType}:", "  /ops/credentials:")
	for _, required := range []string{
		"        '429':", "Retry-After:", "minimum: 1", "application/problem+json",
		"same `Idempotency-Key`", "same `eventId`", "identical request", "Previously committed firings replay",
		"fan-out resumes", "$ref: '#/components/schemas/Problem'",
	} {
		if !strings.Contains(event, required) {
			t.Fatalf("event fanout 429 contract missing %q", required)
		}
	}
}

// 验证 shadow run 契约要求逐节点显式绑定唯一受审 stub，并公开稳定失败码。
func TestShadowRunContract(t *testing.T) {
	contents, err := os.ReadFile("openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	shadow := contractSection(t, string(contents), "  /shadow-runs:", "  /runs/{runId}:")
	for _, required := range []string{
		"operationId: startShadowRun", "stubBindings", "builtin.shadow-noop.v1",
		"shadow_stub_required", "invalid_shadow_stub_binding", "additionalProperties: false",
		"IdempotencyKey", "'202'", "'409'",
	} {
		if !strings.Contains(shadow, required) {
			t.Fatalf("shadow run contract missing %q", required)
		}
	}
}

// 验证 outbound webhook 契约仅接受凭据引用并完整描述生命周期。
func TestOutboundWebhookContracts(t *testing.T) {
	contents, err := os.ReadFile("openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	document := string(contents)
	section := contractSection(t, document, "  /ops/webhook-subscriptions:", "components:")
	for _, required := range []string{"listOutboundWebhookSubscriptions", "createOutboundWebhookSubscription", "enableOutboundWebhookSubscription", "disableOutboundWebhookSubscription", "deleteOutboundWebhookSubscription"} {
		if !strings.Contains(section, required) {
			t.Fatalf("outbound webhook contract missing %q", required)
		}
	}
	schema := contractSection(t, document, "    CreateOutboundWebhookSubscription:", "    OutboundWebhookSubscription:")
	if strings.Contains(schema, "secret:") || !strings.Contains(schema, "signingCredentialId") || !strings.Contains(schema, "pattern: '^https://'") {
		t.Fatalf("unsafe outbound webhook write schema: %s", schema)
	}
}

func contractSection(t *testing.T, document, start, end string) string {
	t.Helper()
	startIndex := strings.Index(document, start)
	endIndex := strings.Index(document, end)
	if startIndex < 0 || endIndex <= startIndex {
		t.Fatalf("OpenAPI section %q..%q not found", start, end)
	}
	return document[startIndex:endIndex]
}
