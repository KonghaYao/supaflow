package supaflow

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

// 验证 Workflow、Approval、Trigger 和 Ops SDK 固定稳定路径且写操作携带幂等键。
func TestPlatformSurfacePathsAndIdempotency(t *testing.T) {
	requests := make([]string, 0)
	client := testClient(t, func(request *http.Request) (*http.Response, error) {
		if request.URL.Query().Has("tenantId") || request.Header.Get("X-Tenant-ID") != "" {
			t.Fatal("SDK must never accept or forward tenant identity")
		}
		requests = append(requests, request.Method+" "+request.URL.Path)
		if (request.URL.Path == "/v1/workflows" || strings.Contains(request.URL.Path, "/ops/triggers")) &&
			(request.Method == http.MethodPost || request.Method == http.MethodPut || request.Method == http.MethodDelete) &&
			!strings.HasSuffix(request.URL.Path, "/fire") && request.Header.Get("Idempotency-Key") == "" {
			t.Fatalf("mutation %s lacks Idempotency-Key", request.URL.Path)
		}
		switch {
		case request.URL.Path == "/v1/workflows":
			return response(http.StatusOK, "application/json", `[]`), nil
		case request.URL.Path == "/v1/runs/run-1/interrupts":
			return response(http.StatusOK, "application/json", `[]`), nil
		case request.URL.Path == "/ops/triggers":
			return response(http.StatusOK, "application/json", `[]`), nil
		case request.URL.Path == "/ops/capacity":
			return response(http.StatusOK, "application/json", `{"activeRuns":0,"queuedRuns":0,"activeWorkers":0,"outboxBacklog":0,"oldestOutboxAge":0,"dlqDepth":0}`), nil
		default:
			return response(http.StatusNoContent, "application/json", ``), nil
		}
	})
	ctx := context.Background()
	if _, err := client.ListWorkflowDrafts(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ListInterrupts(ctx, "run-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ListTriggers(ctx, TriggerFilter{WorkflowID: "flow", Limit: 20}); err != nil {
		t.Fatal(err)
	}
	if err := client.DeleteTrigger(ctx, "trigger-1", time.Unix(10, 0).UTC(), "delete-key"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Capacity(ctx); err != nil {
		t.Fatal(err)
	}
	if len(requests) != 5 {
		t.Fatalf("requests=%v", requests)
	}
}

// 验证 Go SDK 覆盖 Studio 已公开的版本 diff、republish、dry-run、node-test 与节点注册表契约。
func TestStudioSDKSurfaceMatchesStableAPI(t *testing.T) {
	requests := make([]string, 0)
	client := testClient(t, func(request *http.Request) (*http.Response, error) {
		requests = append(requests, request.Method+" "+request.URL.RequestURI())
		switch {
		case strings.HasSuffix(request.URL.Path, "/versions/2"):
			return response(http.StatusOK, "application/json", `{"workflowId":"flow","version":2,"versionId":"version-2","planId":"plan-2","planHash":"hash-2","publishedAt":"2026-01-01T00:00:00Z","definition":{"schemaVersion":"v1","id":"flow","nodes":[],"edges":[]}}`), nil
		case strings.HasSuffix(request.URL.Path, "/versions/diff"):
			return response(http.StatusOK, "application/json", `{"workflowId":"flow","from":1,"to":2,"changes":[]}`), nil
		case strings.HasSuffix(request.URL.Path, "/republish"):
			return response(http.StatusOK, "application/json", `{"id":"plan-3","workflowId":"flow","version":3,"hash":"hash-3","plan":{},"publishedAt":"2026-01-01T00:00:00Z"}`), nil
		case strings.HasSuffix(request.URL.Path, "/dry-run"):
			return response(http.StatusOK, "application/json", `{"mode":"simulated","planHash":"hash","steps":[],"output":{"ok":true}}`), nil
		case strings.HasSuffix(request.URL.Path, "/node-test"):
			return response(http.StatusOK, "application/json", `{"mode":"simulated","nodeId":"node","nodeType":"transform","executor":{"kind":"builtin.transform","minimumRunnerVersion":"1.0.0"},"status":"validation-only","output":{"ok":true}}`), nil
		case request.URL.Path == "/v1/node-types":
			return response(http.StatusOK, "application/json", `[]`), nil
		default:
			return response(http.StatusNoContent, "application/json", ``), nil
		}
	})
	ctx := context.Background()
	if _, err := client.GetWorkflowVersion(ctx, "flow", 2); err != nil {
		t.Fatal(err)
	}
	if _, err := client.DiffWorkflowVersions(ctx, "flow", 1, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := client.RepublishWorkflowVersion(ctx, "flow", 2, "republish-key"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.DryRunWorkflowDraft(ctx, "flow", json.RawMessage(`{"value":1}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := client.TestWorkflowNode(ctx, "flow", "node", json.RawMessage(`{"value":1}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ListNodeTypes(ctx); err != nil {
		t.Fatal(err)
	}
	if len(requests) != 6 || !strings.Contains(requests[1], "from=1&to=2") {
		t.Fatalf("requests=%v", requests)
	}
}

// 验证 Run monitor SDK 正确编码 filter 并读取详情路径。
func TestRunMonitorSDKContract(t *testing.T) {
	client := testClient(t, func(request *http.Request) (*http.Response, error) {
		if request.URL.Path == "/v1/runs" {
			if request.URL.Query().Get("workflowId") != "flow/a" || request.URL.Query().Get("status") != "FAILED" || request.URL.Query().Get("limit") != "25" {
				t.Fatalf("query=%s", request.URL.RawQuery)
			}
			return response(http.StatusOK, "application/json", `[]`), nil
		}
		if request.URL.Path != "/v1/runs/run%2F1/details" && request.URL.Path != "/v1/runs/run/1/details" {
			t.Fatalf("details path=%s", request.URL.Path)
		}
		return response(http.StatusOK, "application/json", `{"run":{"id":"run-1","planId":"plan","planHash":"hash","status":"FAILED","createdAt":"2026-01-01T00:00:00Z","updatedAt":"2026-01-01T00:00:00Z"},"input":{},"nodes":[],"lastEventCursor":3}`), nil
	})
	ctx := context.Background()
	if _, err := client.ListRuns(ctx, RunFilter{WorkflowID: "flow/a", Status: "FAILED", Limit: 25}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetRunDetails(ctx, "run/1"); err != nil {
		t.Fatal(err)
	}
}
