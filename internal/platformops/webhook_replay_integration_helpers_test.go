package platformops

import (
	"context"
	"errors"
	"testing"
	"time"
)

func verifyFutureWebhookReplayWindow(
	t *testing.T,
	ctx context.Context,
	triggers *TriggerService,
	store *PostgresTriggerStore,
	organizationID string,
	triggerID string,
	body []byte,
	secret []byte,
	now time.Time,
) {
	t.Helper()
	futureTimestamp := now.Add(5 * time.Minute)
	request := WebhookRequest{
		OrganizationID: organizationID, TriggerID: triggerID, RequestID: "future-skew-request",
		Timestamp: futureTimestamp, Body: body, Now: now,
		Signature: SignWebhook(secret, "future-skew-request", futureTimestamp, body),
	}
	if _, created, err := triggers.ReceiveWebhook(ctx, request); err != nil || !created {
		t.Fatalf("future-skew webhook created=%v err=%v", created, err)
	}
	if _, err := store.PurgeExpiredWebhookReplays(ctx, now.Add(5*time.Minute), 100); err != nil {
		t.Fatalf("premature replay purge error=%v", err)
	}
	var remaining int
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM webhook_replays
		WHERE organization_id=$1 AND trigger_id=$2 AND request_id IN ('webhook-request-1','future-skew-request')`,
		organizationID, triggerID).Scan(&remaining); err != nil || remaining != 2 {
		t.Fatalf("replay guards before full window=%d err=%v", remaining, err)
	}
	request.Now = now.Add(5*time.Minute + time.Second)
	if _, _, err := triggers.ReceiveWebhook(ctx, request); !errors.Is(err, ErrReplay) {
		t.Fatalf("future-skew replay after original receive error=%v", err)
	}
	// PostgreSQL timestamptz has microsecond precision, so advance by one full
	// storage tick to make the strict expiry boundary deterministic.
	if _, err := store.PurgeExpiredWebhookReplays(ctx, now.Add(10*time.Minute+time.Microsecond), 100); err != nil {
		t.Fatalf("purge webhook replay guards error=%v", err)
	}
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM webhook_replays
		WHERE organization_id=$1 AND trigger_id=$2 AND request_id IN ('webhook-request-1','future-skew-request')`,
		organizationID, triggerID).Scan(&remaining); err != nil || remaining != 0 {
		t.Fatalf("replay guards after full window=%d err=%v", remaining, err)
	}
}
