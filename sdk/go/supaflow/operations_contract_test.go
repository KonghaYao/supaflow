package supaflow

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
)

// 验证稳定 ops/credential SDK 的每个写方法透传调用方提供的幂等键。
func TestOperationsMutationsSendIdempotencyKeys(t *testing.T) {
	keys := make([]string, 0, 12)
	client := testClient(t, func(request *http.Request) (*http.Response, error) {
		if key := request.Header.Get("Idempotency-Key"); key == "" {
			t.Fatalf("%s %s omitted Idempotency-Key", request.Method, request.URL.Path)
		} else {
			keys = append(keys, key)
		}
		return response(http.StatusOK, "application/json", `{}`), nil
	})
	ctx := context.Background()
	_, _ = client.CreateCredential(ctx, WriteCredential{Name: "name", Purpose: "purpose", Secret: "secret"}, "credential-create")
	_, _ = client.RotateCredential(ctx, "credential", 1, "secret", "credential-rotate")
	_, _ = client.RevokeCredential(ctx, "credential", "credential-revoke")
	_ = client.ConfigureQuota(ctx, Quota{MaxActiveRuns: 1, MaxActiveWorkers: 1}, "quota-configure")
	_ = client.ConfigureRetention(ctx, RetentionPolicy{EventDays: 7, OutputDays: 7}, "retention-configure")
	_, _ = client.ExportTenant(ctx, "tenant-export")
	_ = client.DrainWorker(ctx, "worker", "worker-drain")
	_ = client.RequeueDeadLetter(ctx, "letter", "letter-requeue")
	_, _ = client.CreateOutboundWebhookSubscription(ctx, CreateOutboundWebhookSubscription{Name: "events"}, "webhook-create")
	_, _ = client.SetOutboundWebhookSubscriptionEnabled(ctx, "subscription", true, "webhook-enable")
	_ = client.DeleteOutboundWebhookSubscription(ctx, "subscription", "webhook-delete")
	_ = client.ReceivePlatformEvent(ctx, "document.ready", "event", json.RawMessage(`{}`), "event-receive")
	if len(keys) != 12 {
		t.Fatalf("keys=%v", keys)
	}
}
