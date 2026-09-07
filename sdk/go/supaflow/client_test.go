package supaflow

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const testKey = "sdk-test-master-key-at-least-32-bytes-long"

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func testClient(t *testing.T, transport roundTripFunc) *Client {
	t.Helper()
	client, err := NewClient("https://supaflow.test", testKey, "org-a", &http.Client{Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// 验证 SDK 最后写入可信认证与组织 header，并删除旧 X-API-Key。
func TestClientTrustedHeadersCannotBeOverridden(t *testing.T) {
	client := testClient(t, func(*http.Request) (*http.Response, error) {
		return response(http.StatusOK, "application/json", `{}`), nil
	})
	request, err := http.NewRequest(http.MethodGet, "https://supaflow.test/v1/runs", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer attacker")
	request.Header.Set("X-Fenix-Organization-Id", "org-b")
	request.Header.Set("X-API-Key", "legacy")
	client.setTrustedHeaders(request)
	if request.Header.Get("Authorization") != "Bearer "+testKey || request.Header.Get("X-Fenix-Organization-Id") != "org-a" || request.Header.Get("X-API-Key") != "" {
		t.Fatalf("headers=%v", request.Header)
	}
}

func response(status int, contentType, body string) *http.Response {
	recorder := httptest.NewRecorder()
	recorder.Header().Set("Content-Type", contentType)
	recorder.WriteHeader(status)
	_, _ = io.Copy(recorder, strings.NewReader(body))
	return recorder.Result()
}

// 验证版本解析请求只携带 API key，不允许 SDK 注入 tenant 身份。
func TestResolveLatestVersionContract(t *testing.T) {
	client := testClient(t, func(request *http.Request) (*http.Response, error) {
		if request.URL.Path != "/v1/workflows/flow/versions/latest" || request.Header.Get("Authorization") != "Bearer "+testKey || request.Header.Get("X-Fenix-Organization-Id") != "org-a" || request.Header.Get("X-API-Key") != "" {
			t.Fatalf("request=%s headers=%v", request.URL, request.Header)
		}
		if request.Header.Get("X-Tenant-ID") != "" || request.URL.Query().Has("tenantId") {
			t.Fatal("SDK must not send caller-controlled tenant identity")
		}
		return response(http.StatusOK, "application/json", `{"workflowId":"flow","version":3,"versionId":"version-3","planId":"plan-3","planHash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","publishedAt":"2026-01-01T00:00:00Z"}`), nil
	})
	version, err := client.ResolveLatestVersion(context.Background(), "flow")
	if err != nil || version.Version != 3 || version.PlanID != "plan-3" {
		t.Fatalf("version=%+v err=%v", version, err)
	}
}

// 验证 SDK 拒绝非本机明文地址，避免 tenant API key 在传输中泄漏。
func TestClientRejectsPlaintextRemoteBaseURL(t *testing.T) {
	if _, err := NewClient("http://supaflow.internal", testKey, "org-a", nil); err == nil {
		t.Fatal("plaintext remote base URL unexpectedly accepted")
	}
	if _, err := NewClient("http://127.0.0.1:8080", testKey, "org-a", nil); err != nil {
		t.Fatalf("loopback development URL rejected: %v", err)
	}
}

// 验证 SDK 覆盖调用方 client 的 redirect 策略，禁止 X-API-Key 跟随 Location 外泄。
func TestClientDisablesRedirects(t *testing.T) {
	client, err := NewClient("https://supaflow.test", testKey, "org-a", &http.Client{})
	if err != nil {
		t.Fatal(err)
	}
	if err = client.http.CheckRedirect(&http.Request{}, nil); err == nil || !strings.Contains(err.Error(), "redirects are disabled") {
		t.Fatalf("redirect was not rejected: %v", err)
	}
}

// 验证 SDK 可按明确版本号解析不可变 plan，避免迁移和回放错误地漂移到 latest。
func TestResolveSpecificVersion(t *testing.T) {
	client := testClient(t, func(request *http.Request) (*http.Response, error) {
		if request.URL.Path != "/v1/workflows/flow/versions/7" {
			t.Fatalf("path=%s", request.URL.Path)
		}
		return response(http.StatusOK, "application/json", `{"workflowId":"flow","version":7,"versionId":"version-7","planId":"plan-7","planHash":"hash-7","publishedAt":"2026-01-01T00:00:00Z"}`), nil
	})
	version, err := client.ResolveVersion(context.Background(), "flow", 7)
	if err != nil || version.Version != 7 || version.PlanID != "plan-7" || version.PlanHash != "hash-7" {
		t.Fatalf("version=%+v err=%v", version, err)
	}
}

// 验证启动、读取、重跑、取消方法固定路径、幂等头和稳定运行 DTO。
func TestRunControlContracts(t *testing.T) {
	requests := 0
	client := testClient(t, func(request *http.Request) (*http.Response, error) {
		requests++
		if request.Header.Get("Authorization") != "Bearer "+testKey || request.Header.Get("X-Fenix-Organization-Id") != "org-a" || request.Header.Get("X-API-Key") != "" {
			t.Fatal("missing API key")
		}
		if request.Method == http.MethodPost && request.Header.Get("Idempotency-Key") == "" {
			t.Fatal("mutation missing idempotency key")
		}
		if strings.HasSuffix(request.URL.Path, "/rerun") && request.URL.Path != "/v1/runs/run-1/rerun" {
			t.Fatalf("rerun path=%s", request.URL.Path)
		}
		return response(http.StatusOK, "application/json", `{"id":"run-1","planId":"plan-1","planHash":"hash","status":"RUNNING","createdAt":"2026-01-01T00:00:00Z","updatedAt":"2026-01-01T00:00:00Z"}`), nil
	})
	if _, err := client.StartRun(context.Background(), StartRunRequest{PlanID: "plan-1", PlanHash: "hash", Input: []byte(`{}`)}, "start-key"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetRun(context.Background(), "run-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Rerun(context.Background(), "run-1", RerunRequest{FromNodeID: "step"}, "rerun-key"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CancelRun(context.Background(), "run-1", "cancel-key"); err != nil {
		t.Fatal(err)
	}
	if requests != 4 {
		t.Fatalf("requests=%d", requests)
	}
}

// 验证 SDK 解码终态 inline output 及安全存储元数据。
func TestRunOutputContract(t *testing.T) {
	client := testClient(t, func(*http.Request) (*http.Response, error) {
		return response(http.StatusOK, "application/json", `{"id":"run-1","planId":"plan-1","planHash":"hash","status":"SUCCESS","output":{"answer":42},"outputMetadata":{"contentType":"application/json","sizeBytes":13,"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","sensitive":false,"storage":"inline"},"createdAt":"2026-01-01T00:00:00Z","updatedAt":"2026-01-01T00:00:01Z"}`), nil
	})
	run, err := client.GetRun(context.Background(), "run-1")
	if err != nil || string(run.Output) != `{"answer":42}` || run.OutputMetadata == nil || run.OutputMetadata.Storage != OutputStorageInline {
		t.Fatalf("run=%+v err=%v", run, err)
	}
}

// 验证 SSE 从 afterCursor 恢复且不会向调用方重放旧 cursor。
func TestStreamEventsCursorContract(t *testing.T) {
	client := testClient(t, func(request *http.Request) (*http.Response, error) {
		if request.URL.Query().Get("afterCursor") != "4" || request.Header.Get("Accept") != "text/event-stream" {
			t.Fatalf("request=%s headers=%v", request.URL, request.Header)
		}
		body := "id: 4\nevent: stale\ndata: {\"cursor\":4,\"type\":\"stale\",\"at\":\"2026-01-01T00:00:00Z\"}\n\n" +
			"id: 5\nevent: run.succeeded\ndata: {\"cursor\":5,\"type\":\"run.succeeded\",\"at\":\"2026-01-01T00:00:01Z\"}\n\n"
		return response(http.StatusOK, "text/event-stream", body), nil
	})
	var cursors []uint64
	err := client.StreamEvents(context.Background(), "run-1", 4, func(event Event) error {
		cursors = append(cursors, event.Cursor)
		return nil
	})
	if err != nil || len(cursors) != 1 || cursors[0] != 5 {
		t.Fatalf("cursors=%v err=%v", cursors, err)
	}
}

// 验证 SDK 覆盖 outbound webhook 创建、查询、启停和删除且不发送 tenant 或 secret。
func TestOutboundWebhookLifecycleContract(t *testing.T) {
	client := testClient(t, func(request *http.Request) (*http.Response, error) {
		if request.URL.Query().Has("tenantId") || request.Header.Get("X-Tenant-ID") != "" {
			t.Fatal("SDK must not send tenant identity")
		}
		if strings.Contains(request.URL.RawQuery, "secret") {
			t.Fatal("SDK leaked secret")
		}
		if request.Method == http.MethodDelete {
			return response(http.StatusOK, "application/json", `{"deleted":true}`), nil
		}
		if request.Method == http.MethodGet {
			return response(http.StatusOK, "application/json", `[]`), nil
		}
		return response(http.StatusOK, "application/json", `{"id":"sub-1","name":"events","endpointUrl":"https://events.example.test/hook","eventTypes":["run.succeeded"],"signingCredentialId":"cred-1","status":"enabled","timeoutMs":1000,"createdBy":"admin","createdAt":"2026-01-01T00:00:00Z","updatedAt":"2026-01-01T00:00:00Z"}`), nil
	})
	ctx := context.Background()
	if _, err := client.CreateOutboundWebhookSubscription(ctx, CreateOutboundWebhookSubscription{Name: "events", EndpointURL: "https://events.example.test/hook", EventTypes: []string{"run.succeeded"}, SigningCredentialID: "cred-1", TimeoutMS: 1000}, "create-subscription"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ListOutboundWebhookSubscriptions(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.SetOutboundWebhookSubscriptionEnabled(ctx, "sub-1", false, "disable-subscription"); err != nil {
		t.Fatal(err)
	}
	if err := client.DeleteOutboundWebhookSubscription(ctx, "sub-1", "delete-subscription"); err != nil {
		t.Fatal(err)
	}
}
