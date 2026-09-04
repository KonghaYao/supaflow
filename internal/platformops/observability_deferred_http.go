package platformops

import (
	"context"
	"errors"
	"net/http"

	"go.opentelemetry.io/otel/propagation"
)

// DeferredInboundTelemetry starts a public HTTP operation before its tenant is
// known and binds the server-resolved tenant when the operation completes.
type DeferredInboundTelemetry interface {
	StartDeferredHTTP(context.Context, string) (context.Context, func(string, int, error))
	Extract(context.Context, propagation.TextMapCarrier) context.Context
}

type telemetryTenantAttribution struct{ tenantID string }
type telemetryTenantAttributionKey struct{}

// AttributeTelemetryTenant binds a server-resolved tenant to the current
// deferred public operation. Calls outside that middleware are harmless.
func AttributeTelemetryTenant(ctx context.Context, tenantID string) {
	if tenantID == "" {
		return
	}
	attribution, _ := ctx.Value(telemetryTenantAttributionKey{}).(*telemetryTenantAttribution)
	if attribution != nil && attribution.tenantID == "" {
		attribution.tenantID = tenantID
	}
}

// NewDeferredTenantTelemetryHTTPMiddleware records exactly one HTTP operation
// while allowing a public handler to attribute its tenant after route lookup.
func NewDeferredTenantTelemetryHTTPMiddleware(telemetry DeferredInboundTelemetry, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		ctx := telemetry.Extract(request.Context(), propagation.HeaderCarrier(request.Header))
		ctx, complete := telemetry.StartDeferredHTTP(ctx, "control_plane.http.inbound")
		attribution := &telemetryTenantAttribution{}
		ctx = context.WithValue(ctx, telemetryTenantAttributionKey{}, attribution)
		tracked := &telemetryResponseWriter{ResponseWriter: w}
		defer func() {
			if recovered := recover(); recovered != nil {
				complete(attribution.tenantID, tracked.statusOr(http.StatusInternalServerError), errors.New("HTTP handler panicked"))
				panic(recovered)
			}
			complete(attribution.tenantID, tracked.statusOr(http.StatusOK), nil)
		}()
		next.ServeHTTP(tracked, request.WithContext(ctx))
	})
}
