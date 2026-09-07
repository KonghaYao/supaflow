package main

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/pazhou/supaflow/internal/platformops"
)

func configureWorkerTelemetry(ctx context.Context) (*platformops.Telemetry, error) {
	endpoint := strings.TrimSpace(envValue("SUPAFLOW_OTEL_ENDPOINT"))
	if endpoint == "" {
		return nil, nil
	}
	key, err := decodeWorkerKey(strings.TrimSpace(envValue("SUPAFLOW_ORGANIZATION_HASH_KEY")))
	if err != nil {
		return nil, errors.New("SUPAFLOW_ORGANIZATION_HASH_KEY must encode exactly 32 bytes when telemetry is enabled")
	}
	defer clear(key)
	return platformops.NewTelemetry(ctx, platformops.TelemetryConfig{
		ServiceName: "supaflow-worker", Endpoint: endpoint,
		Insecure:            strings.EqualFold(strings.TrimSpace(envValue("SUPAFLOW_OTEL_INSECURE")), "true"),
		OrganizationHashKey: key, MetricInterval: 15 * time.Second,
	})
}
