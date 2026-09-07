package platformops

import (
	"context"
	"errors"
	"net/http"

	"go.opentelemetry.io/otel/propagation"
)

// DeferredInboundTelemetry starts a public HTTP operation before its organization is
// known and binds the server-resolved organization when the operation completes.
type DeferredInboundTelemetry interface {
	StartDeferredHTTP(context.Context, string) (context.Context, func(string, int, error))
	Extract(context.Context, propagation.TextMapCarrier) context.Context
}

type telemetryOrganizationAttribution struct{ organizationID string }
type telemetryOrganizationAttributionKey struct{}

// AttributeTelemetryOrganization binds a server-resolved organization to the current
// deferred public operation. Calls outside that middleware are harmless.
func AttributeTelemetryOrganization(ctx context.Context, organizationID string) {
	if organizationID == "" {
		return
	}
	attribution, _ := ctx.Value(telemetryOrganizationAttributionKey{}).(*telemetryOrganizationAttribution)
	if attribution != nil && attribution.organizationID == "" {
		attribution.organizationID = organizationID
	}
}

// NewDeferredOrganizationTelemetryHTTPMiddleware records exactly one HTTP operation
// while allowing a public handler to attribute its organization after route lookup.
func NewDeferredOrganizationTelemetryHTTPMiddleware(telemetry DeferredInboundTelemetry, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		ctx := telemetry.Extract(request.Context(), propagation.HeaderCarrier(request.Header))
		ctx, complete := telemetry.StartDeferredHTTP(ctx, "control_plane.http.inbound")
		attribution := &telemetryOrganizationAttribution{}
		ctx = context.WithValue(ctx, telemetryOrganizationAttributionKey{}, attribution)
		tracked := &telemetryResponseWriter{ResponseWriter: w}
		defer func() {
			if recovered := recover(); recovered != nil {
				complete(attribution.organizationID, tracked.statusOr(http.StatusInternalServerError), errors.New("HTTP handler panicked"))
				panic(recovered)
			}
			complete(attribution.organizationID, tracked.statusOr(http.StatusOK), nil)
		}()
		next.ServeHTTP(tracked, request.WithContext(ctx))
	})
}
