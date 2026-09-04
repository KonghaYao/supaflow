package executor

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
)

// 验证生产默认策略拒绝 loopback、private、link-local、multicast 和 unspecified 地址。
func TestHTTPExecutorRejectsNonPublicDestinations(t *testing.T) {
	cases := []string{"127.0.0.1", "10.0.0.1", "100.64.0.1", "169.254.1.1", "198.18.0.1", "224.0.0.1", "0.0.0.0", "::1", "fe80::1", "ff02::1", "::"}
	for _, address := range cases {
		t.Run(address, func(t *testing.T) {
			target := "http://" + address
			if net.ParseIP(address).To4() == nil {
				target = "http://[" + address + "]"
			}
			executor, request := httpRequestFixture(t, target, Config{})
			_, err := executor.Execute(context.Background(), request)
			assertExecutionError(t, err, "http_egress_denied", false)
		})
	}
}

// 验证 DNS 返回的每个地址都必须通过策略，而不是只信任第一个公网地址。
func TestHTTPExecutorRejectsMixedDNSAnswers(t *testing.T) {
	resolver := &sequenceResolver{answers: [][]net.IPAddr{{
		{IP: net.ParseIP("93.184.216.34")}, {IP: net.ParseIP("10.0.0.4")},
	}}}
	executor, request := httpRequestFixture(t, "http://mixed.example", Config{DNSResolver: resolver})
	_, err := executor.Execute(context.Background(), request)
	assertExecutionError(t, err, "http_egress_denied", false)
}

// 验证重定向后重新解析并在连接前阻止 DNS rebinding 到 loopback。
func TestHTTPExecutorRejectsRedirectDNSRebinding(t *testing.T) {
	resolver := &hostSequenceResolver{answers: map[string][][]net.IPAddr{
		"source.example": {{{IP: net.ParseIP("93.184.216.34")}}},
		"rebind.example": {
			{{IP: net.ParseIP("93.184.216.34")}},
			{{IP: net.ParseIP("127.0.0.1")}},
		},
	}}
	executor, request := httpRequestFixture(t, "http://source.example/start", Config{
		AllowedHosts: []string{"source.example", "rebind.example"}, DNSResolver: resolver,
		DialContext: scriptedDialer(func(_ *http.Request, connection net.Conn) {
			_, _ = io.WriteString(connection, "HTTP/1.1 302 Found\r\nLocation: http://rebind.example/target\r\nContent-Length: 0\r\n\r\n")
		}),
	})
	_, err := executor.Execute(context.Background(), request)
	assertExecutionError(t, err, "http_egress_denied", false)
}

// 验证 HTTP 响应及其 JSON 封装都受 plan output limit 约束。
func TestHTTPExecutorRejectsOversizeResponse(t *testing.T) {
	executor, request := httpRequestFixture(t, "http://large.example/data", Config{
		AllowedHosts: []string{"large.example"}, ResponseBodyLimit: 1024,
		DNSResolver: fixedResolver{IP: net.ParseIP("93.184.216.34")},
		DialContext: scriptedDialer(func(_ *http.Request, connection net.Conn) {
			_, _ = io.WriteString(connection, "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: 131072\r\n\r\n")
			_, _ = connection.Write(make([]byte, 128*1024))
		}),
	})
	_, err := executor.Execute(context.Background(), request)
	assertExecutionError(t, err, "output_limit_exceeded", false)
}

// 验证请求 body 与注入 header 共用有限的请求预算。
func TestHTTPExecutorRejectsOversizeRequest(t *testing.T) {
	executor, request := httpRequestFixture(t, "http://request.example/data", Config{
		AllowedHosts: []string{"request.example"}, RequestBodyLimit: 8,
		DNSResolver: fixedResolver{IP: net.ParseIP("93.184.216.34")},
		DialContext: func(context.Context, string, string) (net.Conn, error) {
			t.Fatal("oversize request reached the network")
			return nil, nil
		},
	})
	_, err := executor.Execute(context.Background(), request)
	assertExecutionError(t, err, "request_limit_exceeded", false)
}

// 验证 Cancel 只取消目标 attempt，且执行完成后 registry 会释放该 attempt。
func TestPlanExecutorCancelsActiveAttemptAndReleasesRegistry(t *testing.T) {
	started := make(chan struct{})
	executor, request := httpRequestFixture(t, "http://slow.example/wait", Config{
		AllowedHosts: []string{"slow.example"}, DNSResolver: fixedResolver{IP: net.ParseIP("93.184.216.34")},
		DialContext: scriptedDialer(func(_ *http.Request, connection net.Conn) {
			close(started)
			_, _ = io.Copy(io.Discard, connection)
		}),
	})
	result := make(chan error, 1)
	go func() {
		_, err := executor.Execute(context.Background(), request)
		result <- err
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP attempt did not start")
	}
	if err := executor.Cancel(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		assertExecutionError(t, err, "cancelled", false)
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled HTTP attempt did not stop")
	}
	if err := executor.Cancel(context.Background(), request); err != nil {
		t.Fatal(err)
	}
}

// 验证外部副作用幂等键绑定逻辑执行而非物理 attempt，响应丢失后的 retry 可由远端去重。
func TestHTTPIdempotencyKeyBindsLogicalExecutionAndInput(t *testing.T) {
	base := workflowruntime.ExecutionRequest{
		TenantID: "tenant_a", RunID: "run_1", NodeID: "request", AttemptID: "attempt_1", ExecutionID: "execution_1",
		Plan: workflowruntime.PlanRef{ID: "plan_1", Hash: "sha256:one"}, FencingToken: 1, Input: json.RawMessage(`{"item":1}`),
	}
	takeover := base
	takeover.FencingToken = 9
	if attemptIdempotencyKey(base) != attemptIdempotencyKey(takeover) {
		t.Fatal("lease takeover changed the HTTP idempotency key")
	}
	retry := base
	retry.AttemptID = "attempt_2"
	if attemptIdempotencyKey(base) != attemptIdempotencyKey(retry) {
		t.Fatal("physical retry changed the logical HTTP idempotency key")
	}
	nextActivation := retry
	nextActivation.ExecutionID = "execution_2"
	if attemptIdempotencyKey(base) == attemptIdempotencyKey(nextActivation) {
		t.Fatal("different logical executions shared an HTTP idempotency key")
	}
	differentInput := base
	differentInput.Input = json.RawMessage(`{"item":2}`)
	if attemptIdempotencyKey(base) == attemptIdempotencyKey(differentInput) {
		t.Fatal("different activation inputs shared an HTTP idempotency key")
	}
	differentRun := base
	differentRun.RunID = "run_2"
	if attemptIdempotencyKey(base) == attemptIdempotencyKey(differentRun) {
		t.Fatal("different runs shared an HTTP idempotency key")
	}
	differentNode := base
	differentNode.NodeID = "request_2"
	if attemptIdempotencyKey(base) == attemptIdempotencyKey(differentNode) {
		t.Fatal("different logical nodes shared an HTTP idempotency key")
	}
}

// 验证 POST/PATCH 等非幂等方法未显式声明远端去重契约时，即使传输失败也禁止自动重试。
func TestHTTPPostResponseLostFailsClosedWithoutRetrySafeDeclaration(t *testing.T) {
	executor, request := httpMethodRequestFixture(t, "http://post.example/orders", http.MethodPost, false, Config{
		AllowedHosts: []string{"post.example"}, DNSResolver: fixedResolver{IP: net.ParseIP("93.184.216.34")},
		DialContext: scriptedDialer(func(_ *http.Request, connection net.Conn) { _ = connection.Close() }),
	})
	_, err := executor.Execute(context.Background(), request)
	assertExecutionError(t, err, "http_transport_failed", false)
}

// 验证远端已提交副作用但响应丢失时，物理 retry 复用稳定键且远端只应用一次。
func TestHTTPResponseLostRetryDeduplicatesCommittedEffect(t *testing.T) {
	var mu sync.Mutex
	seen := make(map[string]struct{})
	calls, effects := 0, 0
	dial := scriptedDialer(func(request *http.Request, connection net.Conn) {
		key := request.Header.Get("Idempotency-Key")
		mu.Lock()
		calls++
		if _, found := seen[key]; !found {
			seen[key] = struct{}{}
			effects++
		}
		call := calls
		mu.Unlock()
		if call == 1 {
			_ = connection.Close()
			return
		}
		_, _ = io.WriteString(connection, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 16\r\n\r\n{\"applied\":true}")
	})
	executor, first := httpMethodRequestFixture(t, "http://dedupe.example/orders", http.MethodPost, true, Config{
		AllowedHosts: []string{"dedupe.example"}, DNSResolver: fixedResolver{IP: net.ParseIP("93.184.216.34")}, DialContext: dial,
	})
	_, err := executor.Execute(context.Background(), first)
	assertExecutionError(t, err, "http_transport_failed", true)

	retry := first
	retry.AttemptID = "attempt_2"
	retry.Attempt = 2
	result, err := executor.Execute(context.Background(), retry)
	if err != nil {
		t.Fatal(err)
	}
	if result.Output == nil || !json.Valid(result.Output.Inline) {
		t.Fatalf("retry output = %+v", result.Output)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 2 || effects != 1 || len(seen) != 1 {
		t.Fatalf("calls=%d effects=%d keys=%d", calls, effects, len(seen))
	}
}

func httpMethodRequestFixture(t *testing.T, target, method string, retrySafe bool, cfg Config) (*PlanExecutor, workflowruntime.ExecutionRequest) {
	t.Helper()
	definition := workflowDefinition(target, "")
	config, err := json.Marshal(map[string]any{"method": method, "url": target, "retrySafe": retrySafe})
	if err != nil {
		t.Fatal(err)
	}
	definition.Nodes[1].Config = config
	published := publishPlan(t, definition)
	executor, err := NewPlanExecutor(published.catalog, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return executor, workflowruntime.ExecutionRequest{
		TenantID: "tenant_a", RunID: "run_1", Plan: workflowruntime.PlanRef{ID: published.plan.ID, Hash: published.plan.Hash},
		NodeID: "request", AttemptID: "attempt_1", ExecutionID: "execution_1", Attempt: 1, Executor: "http.request",
		Input: json.RawMessage(`{"value":1}`), FencingToken: 1,
	}
}

func httpRequestFixture(t *testing.T, target string, cfg Config) (*PlanExecutor, workflowruntime.ExecutionRequest) {
	t.Helper()
	published := publishPlan(t, workflowDefinition(target, ""))
	executor, err := NewPlanExecutor(published.catalog, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return executor, workflowruntime.ExecutionRequest{
		TenantID: "tenant_a", RunID: "run_1", Plan: workflowruntime.PlanRef{ID: published.plan.ID, Hash: published.plan.Hash},
		NodeID: "request", AttemptID: "attempt_1", ExecutionID: "execution_1", Attempt: 1, Executor: "http.request",
		Input: json.RawMessage(`{"value":1}`), FencingToken: 1,
	}
}

type sequenceResolver struct {
	mu      sync.Mutex
	answers [][]net.IPAddr
	index   int
}

func (r *sequenceResolver) LookupIPAddr(context.Context, string) ([]net.IPAddr, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	index := r.index
	if index >= len(r.answers) {
		index = len(r.answers) - 1
	}
	r.index++
	return append([]net.IPAddr(nil), r.answers[index]...), nil
}

type hostSequenceResolver struct {
	mu      sync.Mutex
	answers map[string][][]net.IPAddr
	indexes map[string]int
}

func (r *hostSequenceResolver) LookupIPAddr(_ context.Context, host string) ([]net.IPAddr, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.indexes == nil {
		r.indexes = make(map[string]int)
	}
	answers := r.answers[host]
	index := r.indexes[host]
	if index >= len(answers) {
		index = len(answers) - 1
	}
	r.indexes[host]++
	return append([]net.IPAddr(nil), answers[index]...), nil
}
