package runtime

import (
	"context"
	"testing"
)

type runtimeTelemetry struct{ operations []string }

func (t *runtimeTelemetry) Start(ctx context.Context, operation, _ string) (context.Context, func(error)) {
	t.operations = append(t.operations, operation)
	return ctx, func(error) {}
}

// 验证 StartRun 确实经过 runtime.start telemetry 边界。
func TestStartRunCallsTelemetry(t *testing.T) {
	telemetry := &runtimeTelemetry{}
	service := NewService(NewMemoryStore(), nil, nil)
	service.SetTelemetry(telemetry)
	if _, err := service.StartRun(context.Background(), "organization", startRequest("telemetry-start")); err != nil {
		t.Fatal(err)
	}
	if len(telemetry.operations) != 1 || telemetry.operations[0] != "runtime.start" {
		t.Fatalf("operations=%v", telemetry.operations)
	}
}
