package platformops

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/postgres"
)

// 验证真实 PostgreSQL 中订阅、加密凭据、outbox、幂等收据和租户隔离形成闭环。
func TestPostgresOutboundWebhookDeliveryLifecycle(t *testing.T) {
	databaseURL := os.Getenv("SUPAFLOW_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("SUPAFLOW_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	migrations, err := postgres.LoadMigrations(os.DirFS("../.."), "migrations")
	if err != nil {
		t.Fatal(err)
	}
	if err = postgres.NewMigrator(pool).Up(ctx, migrations); err != nil {
		t.Fatal(err)
	}
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	organizationA, organizationB := "outbound_a_"+suffix, "outbound_b_"+suffix
	provider, err := NewLocalAESProvider("outbound-test", bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}
	credentials, err := NewCredentialService(NewPostgresCredentialStore(pool), provider)
	if err != nil {
		t.Fatal(err)
	}
	secret := []byte("outbound-secret")
	credential, err := credentials.Create(ctx, CreateCredentialRequest{
		OrganizationID: organizationA, ActorID: "admin-a", Name: "outbound-" + suffix,
		Purpose: "outbound_webhook_signing", Secret: secret,
	})
	if err != nil {
		t.Fatal(err)
	}
	repository := NewOutboundWebhookRepository(pool)
	subscription, err := repository.Create(ctx, CreateOutboundWebhookRequest{
		OrganizationID: organizationA, ActorID: "admin-a", Name: "events-" + suffix,
		EndpointURL: "https://events.example.test/hook", EventTypes: []string{"run.succeeded"},
		SigningCredentialID: credential.ID, Timeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	other, err := repository.List(ctx, organizationB, 10)
	if err != nil || len(other) != 0 {
		t.Fatalf("cross-organization subscriptions=%+v err=%v", other, err)
	}
	payload := json.RawMessage(`{"result":"ok"}`)
	messageID := "outbound_message_" + suffix
	if _, err = pool.Exec(ctx, `
		INSERT INTO outbox_messages (id,organization_id,aggregate_type,aggregate_id,topic,payload)
		VALUES ($1,$2,'run',$3,'run.succeeded',$4)`, messageID, organizationA, "run-"+suffix, payload); err != nil {
		t.Fatal(err)
	}
	client := &recordingWebhookClient{}
	delivery, err := NewOutboundWebhookDelivery(repository, credentials, client)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher, err := NewOutboxDispatcher(NewPostgresTopicDeliveryStore(pool, "run.succeeded"), delivery, "outbound-worker", time.Minute, time.Second, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	result, err := dispatcher.DispatchOne(ctx, organizationA, time.Now().UTC())
	if err != nil || !result.Delivered || len(client.requests) != 1 {
		t.Fatalf("result=%+v requests=%d err=%v", result, len(client.requests), err)
	}
	var delivered, receipt int
	if err = pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM outbox_messages WHERE organization_id=$1 AND id=$2 AND delivered_at IS NOT NULL),
		       (SELECT count(*) FROM outbound_webhook_receipts WHERE organization_id=$1 AND subscription_id=$3 AND message_id=$2 AND status='delivered')`,
		organizationA, messageID, subscription.ID).Scan(&delivered, &receipt); err != nil {
		t.Fatal(err)
	}
	if delivered != 1 || receipt != 1 {
		t.Fatalf("delivered=%d receipt=%d", delivered, receipt)
	}
}
