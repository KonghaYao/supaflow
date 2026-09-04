package platformops

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/pazhou/supaflow/internal/identity"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.30.0"
	"go.opentelemetry.io/otel/trace"
)

// OperationTelemetry is the minimal tracing boundary used by runtime adapters.
type OperationTelemetry interface {
	Start(context.Context, string, string) (context.Context, func(error))
	Inject(context.Context, propagation.TextMapCarrier)
}

// InboundTelemetry restores an upstream context before starting a server span.
type InboundTelemetry interface {
	StartHTTP(context.Context, string, string) (context.Context, func(int, error))
	Extract(context.Context, propagation.TextMapCarrier) context.Context
}

// TelemetryConfig configures OTLP exporters and privacy-preserving attribution.
type TelemetryConfig struct {
	ServiceName    string
	Endpoint       string
	Insecure       bool
	TenantHashKey  []byte
	MetricInterval time.Duration
}

// Telemetry owns real OpenTelemetry trace and metric providers.
type Telemetry struct {
	tracerProvider *sdktrace.TracerProvider
	meterProvider  *sdkmetric.MeterProvider
	tracer         trace.Tracer
	operations     metric.Int64Counter
	duration       metric.Float64Histogram
	tenantHashKey  []byte
	propagator     propagation.TextMapPropagator
}

// NewTelemetry creates OTLP gRPC trace/metric exporters and installs W3C propagation.
func NewTelemetry(ctx context.Context, config TelemetryConfig) (*Telemetry, error) {
	if config.ServiceName == "" || config.Endpoint == "" || len(config.TenantHashKey) < 32 {
		return nil, ErrInvalidArgument
	}
	if config.MetricInterval <= 0 {
		config.MetricInterval = 15 * time.Second
	}
	traceOptions := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(config.Endpoint)}
	metricOptions := []otlpmetricgrpc.Option{otlpmetricgrpc.WithEndpoint(config.Endpoint)}
	if config.Insecure {
		traceOptions = append(traceOptions, otlptracegrpc.WithInsecure())
		metricOptions = append(metricOptions, otlpmetricgrpc.WithInsecure())
	}
	traceExporter, err := otlptracegrpc.New(ctx, traceOptions...)
	if err != nil {
		return nil, fmt.Errorf("configure OTLP trace exporter: %w", err)
	}
	metricExporter, err := otlpmetricgrpc.New(ctx, metricOptions...)
	if err != nil {
		_ = traceExporter.Shutdown(context.WithoutCancel(ctx))
		return nil, fmt.Errorf("configure OTLP metric exporter: %w", err)
	}
	res, err := resource.New(ctx, resource.WithAttributes(semconv.ServiceName(config.ServiceName)))
	if err != nil {
		_ = traceExporter.Shutdown(context.WithoutCancel(ctx))
		_ = metricExporter.Shutdown(context.WithoutCancel(ctx))
		return nil, fmt.Errorf("configure telemetry resource: %w", err)
	}
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithBatcher(traceExporter), sdktrace.WithResource(res))
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter, sdkmetric.WithInterval(config.MetricInterval))), sdkmetric.WithResource(res))
	propagator := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
	otel.SetTracerProvider(tracerProvider)
	otel.SetMeterProvider(meterProvider)
	otel.SetTextMapPropagator(propagator)
	meter := meterProvider.Meter("github.com/pazhou/supaflow/internal/platformops")
	operations, err := meter.Int64Counter("supaflow.platform.operations")
	if err != nil {
		_ = meterProvider.Shutdown(context.WithoutCancel(ctx))
		_ = tracerProvider.Shutdown(context.WithoutCancel(ctx))
		return nil, fmt.Errorf("create operation counter: %w", err)
	}
	duration, err := meter.Float64Histogram("supaflow.platform.operation.duration", metric.WithUnit("s"))
	if err != nil {
		_ = meterProvider.Shutdown(context.WithoutCancel(ctx))
		_ = tracerProvider.Shutdown(context.WithoutCancel(ctx))
		return nil, fmt.Errorf("create operation histogram: %w", err)
	}
	return &Telemetry{
		tracerProvider: tracerProvider, meterProvider: meterProvider,
		tracer:     tracerProvider.Tracer("github.com/pazhou/supaflow/internal/platformops"),
		operations: operations, duration: duration, tenantHashKey: append([]byte(nil), config.TenantHashKey...),
		propagator: propagator,
	}, nil
}

// Start begins a span and returns a completion function that records safe metrics.
func (t *Telemetry) Start(ctx context.Context, operation, tenantID string) (context.Context, func(error)) {
	hash := t.tenantHash(tenantID)
	started := time.Now()
	ctx, span := t.tracer.Start(ctx, operation, trace.WithAttributes(attribute.String("supaflow.tenant_hash", hash)))
	return ctx, func(err error) {
		outcome := "success"
		if err != nil {
			outcome = "error"
			span.SetStatus(codes.Error, "operation failed")
		}
		attributes := metric.WithAttributes(attribute.String("operation", operation), attribute.String("outcome", outcome), attribute.String("supaflow.tenant_hash", hash))
		t.operations.Add(ctx, 1, attributes)
		t.duration.Record(ctx, time.Since(started).Seconds(), attributes)
		span.SetAttributes(attribute.String("outcome", outcome))
		span.End()
	}
}

// StartHTTP begins an inbound span whose outcome and status come from the HTTP response boundary.
func (t *Telemetry) StartHTTP(ctx context.Context, operation, tenantID string) (context.Context, func(int, error)) {
	hash := t.tenantHash(tenantID)
	started := time.Now()
	ctx, span := t.tracer.Start(ctx, operation, trace.WithAttributes(attribute.String("supaflow.tenant_hash", hash)))
	return ctx, func(status int, err error) {
		outcome := "success"
		if err != nil || status >= http.StatusBadRequest {
			outcome = "error"
			span.SetStatus(codes.Error, "HTTP operation failed")
		}
		attributes := metric.WithAttributes(attribute.String("operation", operation), attribute.String("outcome", outcome), attribute.String("supaflow.tenant_hash", hash))
		t.operations.Add(ctx, 1, attributes)
		t.duration.Record(ctx, time.Since(started).Seconds(), attributes)
		span.SetAttributes(semconv.HTTPResponseStatusCode(status), attribute.String("outcome", outcome))
		span.End()
	}
}

// StartDeferredHTTP begins a public HTTP span and applies tenant attribution
// only after the handler resolves it from server-owned routing state.
func (t *Telemetry) StartDeferredHTTP(ctx context.Context, operation string) (context.Context, func(string, int, error)) {
	started := time.Now()
	ctx, span := t.tracer.Start(ctx, operation)
	return ctx, func(tenantID string, status int, err error) {
		outcome := "success"
		if err != nil || status >= http.StatusBadRequest {
			outcome = "error"
			span.SetStatus(codes.Error, "HTTP operation failed")
		}
		metricAttributes := []attribute.KeyValue{
			attribute.String("operation", operation), attribute.String("outcome", outcome),
			attribute.Int("http.response.status_code", status),
		}
		spanAttributes := []attribute.KeyValue{semconv.HTTPResponseStatusCode(status), attribute.String("outcome", outcome)}
		if tenantID != "" {
			tenantAttribute := attribute.String("supaflow.tenant_hash", t.tenantHash(tenantID))
			metricAttributes = append(metricAttributes, tenantAttribute)
			spanAttributes = append(spanAttributes, tenantAttribute)
		}
		attributes := metric.WithAttributes(metricAttributes...)
		t.operations.Add(ctx, 1, attributes)
		t.duration.Record(ctx, time.Since(started).Seconds(), attributes)
		span.SetAttributes(spanAttributes...)
		span.End()
	}
}

// Extract restores W3C trace context from an inbound carrier.
func (t *Telemetry) Extract(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	return t.propagator.Extract(ctx, carrier)
}

// Inject writes W3C trace context to an outbound carrier.
func (t *Telemetry) Inject(ctx context.Context, carrier propagation.TextMapCarrier) {
	t.propagator.Inject(ctx, carrier)
}

// HTTPMiddleware extracts W3C context and starts one inbound server operation.
func (t *Telemetry) HTTPMiddleware(next http.Handler) http.Handler {
	return NewTelemetryHTTPMiddleware(t, next)
}

// NewTelemetryHTTPMiddleware instruments HTTP with an injectable telemetry port.
func NewTelemetryHTTPMiddleware(telemetry InboundTelemetry, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		ctx := telemetry.Extract(request.Context(), propagation.HeaderCarrier(request.Header))
		principal, _ := identity.PrincipalFromContext(ctx)
		ctx, complete := telemetry.StartHTTP(ctx, "control_plane.http.inbound", principal.TenantID)
		tracked := &telemetryResponseWriter{ResponseWriter: w}
		defer func() {
			if recovered := recover(); recovered != nil {
				complete(tracked.statusOr(http.StatusInternalServerError), errors.New("HTTP handler panicked"))
				panic(recovered)
			}
			complete(tracked.statusOr(http.StatusOK), nil)
		}()
		next.ServeHTTP(tracked, request.WithContext(ctx))
	})
}

type telemetryResponseWriter struct {
	http.ResponseWriter
	status int
}

func (w *telemetryResponseWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *telemetryResponseWriter) Write(value []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(value)
}

func (w *telemetryResponseWriter) Flush() {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}

func (w *telemetryResponseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *telemetryResponseWriter) statusOr(fallback int) int {
	if w.status == 0 {
		return fallback
	}
	return w.status
}

var _ http.Flusher = (*telemetryResponseWriter)(nil)

// Shutdown flushes both telemetry pipelines within the caller deadline.
func (t *Telemetry) Shutdown(ctx context.Context) error {
	return errors.Join(t.meterProvider.Shutdown(ctx), t.tracerProvider.Shutdown(ctx))
}

func (t *Telemetry) tenantHash(tenantID string) string {
	mac := hmac.New(sha256.New, t.tenantHashKey)
	_, _ = mac.Write([]byte(tenantID))
	return hex.EncodeToString(mac.Sum(nil)[:16])
}

// RedactingHandler removes structured secret fields before records reach a sink.
type RedactingHandler struct{ next slog.Handler }

// NewRedactingHandler wraps a structured logging handler.
func NewRedactingHandler(next slog.Handler) *RedactingHandler { return &RedactingHandler{next: next} }

// Enabled delegates level filtering.
func (h *RedactingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

// Handle recursively replaces secret-like attribute values.
func (h *RedactingHandler) Handle(ctx context.Context, record slog.Record) error {
	copyRecord := slog.NewRecord(record.Time, record.Level, record.Message, record.PC)
	record.Attrs(func(attribute slog.Attr) bool {
		copyRecord.AddAttrs(redactLogAttr(attribute))
		return true
	})
	return h.next.Handle(ctx, copyRecord)
}

// WithAttrs returns a handler whose fixed attributes are already redacted.
func (h *RedactingHandler) WithAttrs(attributes []slog.Attr) slog.Handler {
	redacted := make([]slog.Attr, len(attributes))
	for index, attribute := range attributes {
		redacted[index] = redactLogAttr(attribute)
	}
	return &RedactingHandler{next: h.next.WithAttrs(redacted)}
}

// WithGroup preserves structured logging groups.
func (h *RedactingHandler) WithGroup(name string) slog.Handler {
	return &RedactingHandler{next: h.next.WithGroup(name)}
}

func redactLogAttr(attr slog.Attr) slog.Attr {
	attr.Value = attr.Value.Resolve()
	if sensitiveKey(attr.Key) {
		return slog.String(attr.Key, "[REDACTED]")
	}
	if attr.Value.Kind() == slog.KindGroup {
		group := attr.Value.Group()
		for index := range group {
			group[index] = redactLogAttr(group[index])
		}
		return slog.Group(attr.Key, attrsToAny(group)...)
	}
	if attr.Value.Kind() == slog.KindString {
		return slog.String(attr.Key, RedactDiagnostic(attr.Value.String()))
	}
	return attr
}

func attrsToAny(attributes []slog.Attr) []any {
	result := make([]any, len(attributes))
	for index := range attributes {
		result[index] = attributes[index]
	}
	return result
}
