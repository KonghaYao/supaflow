package openapi

import (
	"os"
	"testing"

	"gopkg.in/yaml.v3"
)

type semanticDocument struct {
	OpenAPI    string                  `yaml:"openapi"`
	Paths      map[string]semanticPath `yaml:"paths"`
	Components struct {
		Schemas map[string]yaml.Node `yaml:"schemas"`
	} `yaml:"components"`
}

type semanticPath struct {
	Get    *semanticOperation `yaml:"get"`
	Post   *semanticOperation `yaml:"post"`
	Put    *semanticOperation `yaml:"put"`
	Delete *semanticOperation `yaml:"delete"`
}

type semanticOperation struct {
	OperationID string              `yaml:"operationId"`
	Parameters  []semanticParameter `yaml:"parameters"`
	Responses   map[string]struct {
		Content map[string]struct {
			Schema semanticSchema `yaml:"schema"`
		} `yaml:"content"`
	} `yaml:"responses"`
}

type semanticParameter struct {
	Ref  string `yaml:"$ref"`
	Name string `yaml:"name"`
}

type semanticSchema struct {
	Ref   string          `yaml:"$ref"`
	Type  string          `yaml:"type"`
	Items *semanticSchema `yaml:"items"`
}

// 验证 OpenAPI 没有重复 mapping key，避免 YAML 解析器静默覆盖契约。
func TestOpenAPIHasNoDuplicateMappingKeys(t *testing.T) {
	contents, err := os.ReadFile("openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var root yaml.Node
	if err = yaml.Unmarshal(contents, &root); err != nil {
		t.Fatal(err)
	}
	assertUniqueMappingKeys(t, &root, "document")
}

// 验证每个 HTTP operation 都有唯一 operationId，保证生成 SDK 时不会覆盖方法。
func TestOpenAPIOperationIDsArePresentAndUnique(t *testing.T) {
	document := readSemanticDocument(t)
	seen := make(map[string]string)
	for path, item := range document.Paths {
		operations := map[string]*semanticOperation{"get": item.Get, "post": item.Post, "put": item.Put, "delete": item.Delete}
		for method, operation := range operations {
			if operation == nil {
				continue
			}
			if operation.OperationID == "" {
				t.Fatalf("%s %s has no operationId", method, path)
			}
			if previous, found := seen[operation.OperationID]; found {
				t.Fatalf("operationId %q is shared by %s and %s %s", operation.OperationID, previous, method, path)
			}
			seen[operation.OperationID] = method + " " + path
		}
	}
}

// 验证 Run monitor 和 Trigger lifecycle 的响应引用、幂等参数与 schema 均形成可生成的语义契约。
func TestMonitorAndTriggerLifecycleSemanticContracts(t *testing.T) {
	document := readSemanticDocument(t)
	listRuns := requireOperation(t, document, "/runs", "get")
	if got := listRuns.Responses["200"].Content["application/json"].Schema.Items.Ref; got != "#/components/schemas/RunSummary" {
		t.Fatalf("list runs item schema = %q", got)
	}
	details := requireOperation(t, document, "/runs/{runId}/details", "get")
	if got := details.Responses["200"].Content["application/json"].Schema.Ref; got != "#/components/schemas/RunDetails" {
		t.Fatalf("run details schema = %q", got)
	}
	for _, target := range []struct{ path, method string }{
		{"/ops/triggers", "post"}, {"/ops/triggers/{triggerId}", "put"},
		{"/ops/triggers/{triggerId}", "delete"}, {"/ops/triggers/{triggerId}/state", "post"},
		{"/ops/triggers/{triggerId}/fire", "post"}, {"/ops/events/{eventType}", "post"},
		{"/ops/credentials", "post"}, {"/ops/credentials/{credentialId}/rotate", "post"},
		{"/ops/credentials/{credentialId}/revoke", "post"}, {"/ops/quotas", "post"},
		{"/ops/retention", "post"}, {"/ops/exports", "post"},
		{"/ops/workers/{workerId}/drain", "post"}, {"/ops/dlq/{messageId}/requeue", "post"},
		{"/ops/webhook-subscriptions", "post"}, {"/ops/webhook-subscriptions/{subscriptionId}/enable", "post"},
		{"/ops/webhook-subscriptions/{subscriptionId}/disable", "post"}, {"/ops/webhook-subscriptions/{subscriptionId}", "delete"},
	} {
		operation := requireOperation(t, document, target.path, target.method)
		if !hasParameterRef(operation, "#/components/parameters/IdempotencyKey") {
			t.Fatalf("%s %s must require Idempotency-Key", target.method, target.path)
		}
	}
	for _, schema := range []string{"RunSummary", "RunDetails", "NodeDiagnostic", "AttemptDiagnostic", "Trigger", "UpdateTrigger"} {
		if _, found := document.Components.Schemas[schema]; !found {
			t.Fatalf("schema %s is missing", schema)
		}
	}
}

func readSemanticDocument(t *testing.T) semanticDocument {
	t.Helper()
	contents, err := os.ReadFile("openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var document semanticDocument
	if err = yaml.Unmarshal(contents, &document); err != nil {
		t.Fatal(err)
	}
	return document
}

func requireOperation(t *testing.T, document semanticDocument, path, method string) *semanticOperation {
	t.Helper()
	item, found := document.Paths[path]
	if !found {
		t.Fatalf("path %s is missing", path)
	}
	operations := map[string]*semanticOperation{"get": item.Get, "post": item.Post, "put": item.Put, "delete": item.Delete}
	operation := operations[method]
	if operation == nil {
		t.Fatalf("operation %s %s is missing", method, path)
	}
	return operation
}

func hasParameterRef(operation *semanticOperation, ref string) bool {
	for _, parameter := range operation.Parameters {
		if parameter.Ref == ref {
			return true
		}
	}
	return false
}

func assertUniqueMappingKeys(t *testing.T, node *yaml.Node, path string) {
	t.Helper()
	if node.Kind == yaml.MappingNode {
		seen := make(map[string]bool)
		for index := 0; index < len(node.Content); index += 2 {
			key, value := node.Content[index], node.Content[index+1]
			if seen[key.Value] {
				t.Fatalf("duplicate mapping key %q at %s", key.Value, path)
			}
			seen[key.Value] = true
			assertUniqueMappingKeys(t, value, path+"."+key.Value)
		}
		return
	}
	for _, child := range node.Content {
		assertUniqueMappingKeys(t, child, path)
	}
}
