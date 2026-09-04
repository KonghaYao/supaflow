package platformops

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"
)

// 验证 outbound webhook 与 HTTP node 共用特殊地址拒绝策略，CGNAT/benchmark DNS 不能穿透。
func TestOutboundWebhookRejectsSpecialUseAddresses(t *testing.T) {
	for _, value := range []string{"100.64.0.1", "198.18.0.1", "192.0.2.1", "2001:db8::1"} {
		if isPublicIP(net.ParseIP(value)) {
			t.Fatalf("special-use address %s was allowed", value)
		}
	}
}

// 验证运行事件按租户和 topic 投递、签名且重试时跳过已完成订阅。
func TestOutboundWebhookDeliveryIsTenantScopedSignedAndReplaySafe(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	store := &webhookDeliveryStore{subscriptions: []OutboundWebhookSubscription{
		{TenantID: "tenant-a", ID: "sub-a", EndpointURL: "https://events.example.test/hook", EventTypes: []string{"run.succeeded"}, SigningCredentialID: "cred-a", Timeout: time.Second},
	}}
	client := &recordingWebhookClient{}
	delivery, err := NewOutboundWebhookDelivery(store, staticWebhookSecrets{value: []byte("signing-secret")}, client)
	if err != nil {
		t.Fatal(err)
	}
	delivery.now = func() time.Time { return now }
	message := DeliveryMessage{ID: "out-1", TenantID: "tenant-a", AggregateType: "run", AggregateID: "run-1", Topic: "run.succeeded", Payload: json.RawMessage(`{"ok":true}`)}
	if err = delivery.Deliver(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if len(client.requests) != 1 || client.requests[0].RequestID == "" || client.requests[0].Signature == "" {
		t.Fatalf("requests=%+v", client.requests)
	}
	if !VerifyOutboundWebhookSignature([]byte("signing-secret"), now, client.requests[0].RequestID, client.requests[0].Body, client.requests[0].Signature) {
		t.Fatal("delivery signature did not verify")
	}
	if !bytes.Contains(client.requests[0].Body, []byte(`"type":"run.succeeded"`)) || bytes.Contains(client.requests[0].Body, []byte("tenant-a")) {
		t.Fatalf("unsafe or incomplete envelope: %s", client.requests[0].Body)
	}
	if err = delivery.Deliver(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if len(client.requests) != 1 {
		t.Fatalf("delivered subscription replayed: %d", len(client.requests))
	}
}

// 验证任一订阅失败会保留稳定 request id 并交由 outbox 有界重试。
func TestOutboundWebhookDeliveryPreservesStableRequestIDOnFailure(t *testing.T) {
	store := &webhookDeliveryStore{subscriptions: []OutboundWebhookSubscription{{TenantID: "tenant-a", ID: "sub-a", EndpointURL: "https://events.example.test/hook", EventTypes: []string{"node.failed"}, SigningCredentialID: "cred-a", Timeout: time.Second}}}
	client := &recordingWebhookClient{err: errors.New("authorization=never-log-this connection refused")}
	delivery, err := NewOutboundWebhookDelivery(store, staticWebhookSecrets{value: []byte("secret")}, client)
	if err != nil {
		t.Fatal(err)
	}
	message := DeliveryMessage{ID: "out-2", TenantID: "tenant-a", AggregateType: "run", AggregateID: "run-2", Topic: "node.failed", Payload: json.RawMessage(`{}`)}
	if err = delivery.Deliver(context.Background(), message); err == nil {
		t.Fatal("expected external delivery failure")
	}
	if err = delivery.Deliver(context.Background(), message); err == nil {
		t.Fatal("expected retry failure")
	}
	if len(client.requests) != 2 || client.requests[0].RequestID != client.requests[1].RequestID {
		t.Fatalf("request ids=%+v", client.requests)
	}
	if bytes.Contains([]byte(store.lastError), []byte("never-log-this")) {
		t.Fatalf("diagnostic leaked secret: %q", store.lastError)
	}
}

type webhookDeliveryStore struct {
	subscriptions []OutboundWebhookSubscription
	delivered     map[string]bool
	lastError     string
}

func (s *webhookDeliveryStore) ListEnabled(_ context.Context, tenantID, topic string) ([]OutboundWebhookSubscription, error) {
	result := make([]OutboundWebhookSubscription, 0)
	for _, subscription := range s.subscriptions {
		if subscription.TenantID == tenantID && containsEventType(subscription.EventTypes, topic) {
			result = append(result, subscription)
		}
	}
	return result, nil
}
func (s *webhookDeliveryStore) BeginDelivery(_ context.Context, tenantID, subscriptionID, messageID, requestID string, _ time.Time) (bool, error) {
	if s.delivered == nil {
		s.delivered = map[string]bool{}
	}
	return s.delivered[tenantID+"/"+subscriptionID+"/"+messageID], nil
}
func (s *webhookDeliveryStore) CompleteDelivery(_ context.Context, tenantID, subscriptionID, messageID string, _ int, _ time.Time) error {
	s.delivered[tenantID+"/"+subscriptionID+"/"+messageID] = true
	return nil
}
func (s *webhookDeliveryStore) FailWebhookDelivery(_ context.Context, _, _, _ string, diagnostic string, _ time.Time) error {
	s.lastError = diagnostic
	return nil
}

type staticWebhookSecrets struct{ value []byte }

func (s staticWebhookSecrets) ResolveOutboundWebhookSecret(context.Context, string, string) ([]byte, error) {
	return append([]byte(nil), s.value...), nil
}

type recordingWebhookClient struct {
	requests []OutboundWebhookRequest
	err      error
}

func (c *recordingWebhookClient) Send(_ context.Context, request OutboundWebhookRequest) (int, error) {
	request.Body = append([]byte(nil), request.Body...)
	c.requests = append(c.requests, request)
	return 204, c.err
}
