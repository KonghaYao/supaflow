package platformops

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pazhou/supaflow/internal/identity"
	"go.opentelemetry.io/otel/propagation"
)

type recordingOperationTelemetry struct {
	started   bool
	starts    int
	injected  bool
	extracted bool
	tenant    string
	completed error
	status    int
}

func (t *recordingOperationTelemetry) Extract(ctx context.Context, _ propagation.TextMapCarrier) context.Context {
	t.extracted = true
	return ctx
}

func (t *recordingOperationTelemetry) Start(ctx context.Context, _ string, tenant string) (context.Context, func(error)) {
	t.started, t.tenant = true, tenant
	return ctx, func(err error) { t.completed = err }
}

func (t *recordingOperationTelemetry) StartHTTP(ctx context.Context, _ string, tenant string) (context.Context, func(int, error)) {
	t.started, t.tenant, t.starts = true, tenant, t.starts+1
	return ctx, func(status int, err error) { t.status, t.completed = status, err }
}

func (t *recordingOperationTelemetry) StartDeferredHTTP(ctx context.Context, _ string) (context.Context, func(string, int, error)) {
	t.started, t.starts = true, t.starts+1
	return ctx, func(tenant string, status int, err error) {
		t.tenant, t.status, t.completed = tenant, status, err
	}
}

func (t *recordingOperationTelemetry) Inject(_ context.Context, carrier propagation.TextMapCarrier) {
	t.injected = true
	carrier.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
}

type traceCapturingDelivery struct{ message DeliveryMessage }

func (d *traceCapturingDelivery) Deliver(_ context.Context, message DeliveryMessage) error {
	d.message = message
	return nil
}

// 验证 outbox delivery 启动 tenant span 并把 W3C 上下文注入外发信封。
func TestOutboxDispatcherStartsAndInjectsTelemetry(t *testing.T) {
	now := time.Now().UTC()
	store := &deliveryTestStore{message: DeliveryMessage{ID: "out_trace", TenantID: "tenant_a", Attempts: 1, MaxAttempts: 2}}
	delivery := &traceCapturingDelivery{}
	dispatcher, err := NewOutboxDispatcher(store, delivery, "dispatcher", time.Minute, time.Second, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher.now = func() time.Time { return now }
	telemetry := &recordingOperationTelemetry{}
	dispatcher.SetTelemetry(telemetry)
	result, err := dispatcher.DispatchOne(context.Background(), "tenant_a", now)
	if err != nil || !result.Delivered || !telemetry.started || !telemetry.injected || telemetry.tenant != "tenant_a" || delivery.message.TraceHeaders["traceparent"] == "" {
		t.Fatalf("result=%+v telemetry=%+v headers=%v err=%v", result, telemetry, delivery.message.TraceHeaders, err)
	}
}

// 验证 control-plane HTTP 入站从认证上下文归属 tenant，并按响应状态记录失败。
func TestTelemetryHTTPMiddlewareAttributesAuthenticatedTenantAndFailure(t *testing.T) {
	telemetry := &recordingOperationTelemetry{}
	handler := NewTelemetryHTTPMiddleware(telemetry, http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusServiceUnavailable)
	}))
	request := httptest.NewRequest(http.MethodGet, "/v1/runs", nil)
	request = request.WithContext(identity.ContextWithPrincipal(request.Context(), identity.Principal{TenantID: "tenant_a", ActorID: "actor_a", Role: identity.RoleOperator}))
	handler.ServeHTTP(httptest.NewRecorder(), request)
	if !telemetry.extracted || !telemetry.started || telemetry.tenant != "tenant_a" || telemetry.status != http.StatusServiceUnavailable {
		t.Fatalf("telemetry=%+v", telemetry)
	}
}

// 验证 public webhook 在 handler 解析 tenant 后补充归属，并且整个请求只生成一个 span。
func TestDeferredTenantHTTPMiddlewareAttributesResolvedTenantOnce(t *testing.T) {
	telemetry := &recordingOperationTelemetry{}
	handler := NewDeferredTenantTelemetryHTTPMiddleware(telemetry, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		AttributeTelemetryTenant(request.Context(), "tenant_from_public_hash")
		response.WriteHeader(http.StatusAccepted)
	}))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/hooks/hash", nil))
	if telemetry.starts != 1 || telemetry.tenant != "tenant_from_public_hash" || telemetry.status != http.StatusAccepted {
		t.Fatalf("telemetry=%+v", telemetry)
	}
}

// 验证无法解析 publicHash 的请求仍记录匿名失败，不伪造 tenant 归属。
func TestDeferredTenantHTTPMiddlewareKeepsUnknownTenantAnonymous(t *testing.T) {
	telemetry := &recordingOperationTelemetry{}
	handler := NewDeferredTenantTelemetryHTTPMiddleware(telemetry, http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusNotFound)
	}))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/hooks/missing", nil))
	if telemetry.starts != 1 || telemetry.tenant != "" || telemetry.status != http.StatusNotFound {
		t.Fatalf("telemetry=%+v", telemetry)
	}
}
