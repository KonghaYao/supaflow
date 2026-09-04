package platformops

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
)

// 验证 Webhook 仅在签名时间窗内通过，并把重复请求交给持久化 replay guard 拒绝。
func TestTriggerServiceVerifiesWebhookWindowAndReplay(t *testing.T) {
	now := time.Date(2026, 9, 3, 8, 0, 0, 0, time.UTC)
	secret := []byte("01234567890123456789012345678901")
	store := &triggerTestStore{}
	service, err := NewTriggerService(store, triggerSecrets{secret: secret}, fiveMinutes)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"value":1}`)
	request := WebhookRequest{
		TenantID: "tenant_a", TriggerID: "trigger_a", RequestID: "request_a",
		Timestamp: now, Now: now, Body: body, Signature: SignWebhook(secret, "request_a", now, body),
	}
	firing, created, err := service.ReceiveWebhook(context.Background(), request)
	if err != nil || !created || firing.TriggerID != "trigger_a" {
		t.Fatalf("first webhook: firing=%+v created=%v err=%v", firing, created, err)
	}
	store.replay = true
	if _, _, err = service.ReceiveWebhook(context.Background(), request); !errors.Is(err, ErrReplay) {
		t.Fatalf("replayed webhook error=%v", err)
	}
	request.RequestID = "request_b"
	request.Timestamp = now.Add(-fiveMinutes - time.Second)
	request.Signature = SignWebhook(secret, request.RequestID, request.Timestamp, body)
	if _, _, err = service.ReceiveWebhook(context.Background(), request); !errors.Is(err, ErrSignature) {
		t.Fatalf("expired webhook error=%v", err)
	}
}

// 验证 request-id 属于 webhook 签名信封，攻击者不能换 ID 重放同一正文。
func TestWebhookSignatureBindsRequestID(t *testing.T) {
	now := time.Date(2026, 9, 3, 8, 0, 0, 0, time.UTC)
	secret := []byte("01234567890123456789012345678901")
	store := &triggerTestStore{}
	service, err := NewTriggerService(store, triggerSecrets{secret: secret}, fiveMinutes)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"event":"created"}`)
	_, _, err = service.ReceiveWebhook(context.Background(), WebhookRequest{
		TenantID: "tenant_a", TriggerID: "trigger_a", RequestID: "request_b",
		Timestamp: now, Now: now, Body: body, Signature: SignWebhook(secret, "request_a", now, body),
	})
	if !errors.Is(err, ErrSignature) {
		t.Fatalf("changed request id error=%v", err)
	}
	if store.webhookCalls != 0 {
		t.Fatalf("store received %d unauthenticated webhook calls", store.webhookCalls)
	}
}

// 验证触发器 API/manual firing 对幂等 key 和 JSON object 做边界校验。
func TestTriggerServiceRejectsInvalidFireEnvelope(t *testing.T) {
	service, err := NewTriggerService(&triggerTestStore{}, nil, fiveMinutes)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = service.Fire(context.Background(), FireRequest{
		TenantID: "tenant_a", TriggerID: "trigger_a", ExpectedKind: TriggerAPI,
		Payload: json.RawMessage(`[]`),
	})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("invalid fire error=%v", err)
	}
}

// 验证 catch-up 达到批次上限时保留剩余欠账而不是静默跳过。
func TestScheduleCatchUpKeepsBacklogAfterBatchLimit(t *testing.T) {
	now := time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC)
	due := now.Add(-5 * time.Minute)
	occurrences, next := scheduleOccurrences(due, time.Minute, MisfireCatchUp, ScheduleOptions{Now: now, MaxCatchUp: 2})
	if len(occurrences) != 2 || !next.Equal(due.Add(2*time.Minute)) || next.After(now) {
		t.Fatalf("occurrences=%v next=%v", occurrences, next)
	}
}

// 验证 credential 明文被 envelope 加密、调用缓冲区被清零且只能由相同租户上下文解密。
func TestCredentialServiceEnvelopeRoundTripAndTenantBinding(t *testing.T) {
	provider, err := NewLocalAESProvider("local-key-1", bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}
	store := &credentialTestStore{}
	service, err := NewCredentialService(store, provider)
	if err != nil {
		t.Fatal(err)
	}
	secret := []byte("top-secret-value")
	credential, err := service.Create(context.Background(), CreateCredentialRequest{
		TenantID: "tenant_a", ActorID: "admin_a", Name: "signer", Purpose: "webhook_signing", Secret: secret,
	})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(store.encrypted.Ciphertext, []byte("top-secret-value")) || !bytes.Equal(secret, make([]byte, len(secret))) {
		t.Fatal("secret remained visible after credential creation")
	}
	store.encrypted.Credential = credential
	resolved, err := service.ResolveForAttempt(context.Background(), ResolveCredentialRequest{
		TenantID: "tenant_a", RunID: "run_a", AttemptID: "attempt_a", CredentialID: credential.ID,
		Purpose: "webhook_signing", FencingToken: 1, Now: time.Now().UTC(),
	})
	if err != nil || string(resolved) != "top-secret-value" {
		t.Fatalf("resolved=%q err=%v", resolved, err)
	}
	tampered := store.encrypted
	tampered.Credential.TenantID = "tenant_b"
	store.encrypted = tampered
	if _, err = service.ResolveForAttempt(context.Background(), ResolveCredentialRequest{
		TenantID: "tenant_b", RunID: "run_a", AttemptID: "attempt_a", CredentialID: credential.ID,
		Purpose: "webhook_signing", FencingToken: 1, Now: time.Now().UTC(),
	}); !errors.Is(err, ErrCredentialForbidden) {
		t.Fatalf("cross-tenant ciphertext error=%v", err)
	}
}

// 验证 dispatcher 对失败执行指数退避并在达到上限时进入 DLQ。
func TestOutboxDispatcherRetriesThenDeadLetters(t *testing.T) {
	now := time.Date(2026, 9, 3, 9, 0, 0, 0, time.UTC)
	store := &deliveryTestStore{message: DeliveryMessage{
		ID: "out_a", TenantID: "tenant_a", Attempts: 1, MaxAttempts: 2,
	}}
	dispatcher, err := NewOutboxDispatcher(store, failingDelivery{}, "dispatcher_a", time.Minute, time.Second, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	completedAt := now
	dispatcher.now = func() time.Time { return completedAt }
	result, err := dispatcher.DispatchOne(context.Background(), "tenant_a", now)
	if err != nil || result.DeadLetter || result.RetryAt == nil || !result.RetryAt.Equal(now.Add(time.Second)) {
		t.Fatalf("retry result=%+v err=%v", result, err)
	}
	store.message.Attempts = 2
	completedAt = now.Add(time.Second)
	result, err = dispatcher.DispatchOne(context.Background(), "tenant_a", now.Add(time.Second))
	if err != nil || !result.DeadLetter || result.RetryAt != nil {
		t.Fatalf("DLQ result=%+v err=%v", result, err)
	}
}

type slowDelivery struct{ delay time.Duration }

func (d slowDelivery) Deliver(ctx context.Context, _ DeliveryMessage) error {
	select {
	case <-time.After(d.delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// 验证慢 webhook 处理期间持续续租，避免另一副本抢占同一 outbox 消息。
func TestOutboxDispatcherRenewsLeaseDuringSlowDelivery(t *testing.T) {
	now := time.Now().UTC()
	store := &deliveryTestStore{message: DeliveryMessage{ID: "out_slow", TenantID: "tenant_a", Attempts: 1, MaxAttempts: 2}}
	dispatcher, err := NewOutboxDispatcher(store, slowDelivery{delay: 90 * time.Millisecond}, "dispatcher_a", 30*time.Millisecond, time.Second, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher.now = func() time.Time { return time.Now().UTC() }
	result, err := dispatcher.DispatchOne(context.Background(), "tenant_a", now)
	if err != nil || !result.Delivered {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	store.mu.Lock()
	renewals := store.renewals
	store.mu.Unlock()
	if renewals < 2 {
		t.Fatalf("renewals=%d", renewals)
	}
}

// 验证本地 object adapter 阻止路径穿越、限制大小并保持租户隔离。
func TestFileObjectStoreBoundsAndIsolation(t *testing.T) {
	store, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Put(context.Background(), "../tenant", "object", strings.NewReader("x"), 10); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("traversal error=%v", err)
	}
	if _, err = store.Put(context.Background(), "tenant_a", "object", strings.NewReader("too-large"), 3); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("size error=%v", err)
	}
	stored, err := store.Put(context.Background(), "tenant_a", "object", strings.NewReader("safe"), 10)
	if err != nil || stored.Size != 4 {
		t.Fatalf("stored=%+v err=%v", stored, err)
	}
	if _, err = store.Open(context.Background(), "tenant_b", "object"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant open error=%v", err)
	}
}

// 验证结构化日志递归遮蔽 secret 字段和 bearer token。
func TestRedactingHandlerRemovesStructuredSecrets(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(NewRedactingHandler(slog.NewJSONHandler(&output, nil)))
	logger.Info("request", "authorization", "Bearer abc", "nested", slog.GroupValue(slog.String("password", "hidden")))
	text := output.String()
	if strings.Contains(text, "abc") || strings.Contains(text, "hidden") || strings.Count(text, "[REDACTED]") != 2 {
		t.Fatalf("redacted log=%s", text)
	}
}

// 验证 graceful drain 拒绝新工作并等待已有工作完成。
func TestDrainControllerWaitsForInFlightWork(t *testing.T) {
	drain := NewDrainController()
	done, ok := drain.Begin()
	if !ok {
		t.Fatal("initial work was rejected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- drain.Drain(ctx) }()
	time.Sleep(10 * time.Millisecond)
	if _, accepted := drain.Begin(); accepted {
		t.Fatal("draining controller accepted new work")
	}
	done()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

// 验证高风险执行在审计不可用时失败关闭，并把策略拒绝记录为 denied。
func TestAuditedExecutorFailsClosedAndClassifiesDenial(t *testing.T) {
	recorder := &auditTestRecorder{failFirst: true}
	next := &executorTestDouble{err: &workflowruntime.ExecutionError{Code: "policy_denied"}}
	executor, err := NewAuditedExecutor(next, recorder)
	if err != nil {
		t.Fatal(err)
	}
	request := workflowruntime.ExecutionRequest{TenantID: "tenant_a", RunID: "run_a", NodeID: "node_a", AttemptID: "attempt_a", Executor: "http", FencingToken: 2}
	if _, err = executor.Execute(context.Background(), request); err == nil || next.calls != 0 {
		t.Fatalf("audit failure must prevent execution: calls=%d err=%v", next.calls, err)
	}
	recorder.failFirst = false
	if _, err = executor.Execute(context.Background(), request); err == nil || next.calls != 1 {
		t.Fatalf("denied execution calls=%d err=%v", next.calls, err)
	}
	if len(recorder.records) != 2 || recorder.records[0].Outcome != AuditAllowed || recorder.records[1].Outcome != AuditDenied {
		t.Fatalf("audit outcomes=%+v", recorder.records)
	}
}

const fiveMinutes = 5 * time.Minute

type triggerSecrets struct{ secret []byte }

func (s triggerSecrets) ResolveTriggerSecret(context.Context, string, string) ([]byte, error) {
	return append([]byte(nil), s.secret...), nil
}

type triggerTestStore struct {
	replay       bool
	webhookCalls int
}

func (s *triggerTestStore) CreateTrigger(context.Context, CreateTriggerRequest) (Trigger, error) {
	return Trigger{}, nil
}
func (s *triggerTestStore) GetTrigger(context.Context, string, string) (Trigger, error) {
	return Trigger{}, nil
}
func (s *triggerTestStore) ResolvePublicWebhook(context.Context, string) (Trigger, error) {
	return Trigger{TenantID: "tenant_a", ID: "trigger_a", Kind: TriggerWebhook}, nil
}
func (s *triggerTestStore) SetTriggerEnabled(context.Context, string, string, bool, time.Time) (Trigger, error) {
	return Trigger{}, nil
}
func (s *triggerTestStore) Fire(_ context.Context, request FireRequest) (Firing, bool, error) {
	return Firing{TenantID: request.TenantID, TriggerID: request.TriggerID}, true, nil
}
func (s *triggerTestStore) FireWebhook(_ context.Context, request FireRequest, _ string, _ [sha256.Size]byte, _ time.Time) (Firing, bool, error) {
	s.webhookCalls++
	if s.replay {
		return Firing{}, false, ErrReplay
	}
	return Firing{TenantID: request.TenantID, TriggerID: request.TriggerID}, true, nil
}
func (s *triggerTestStore) FireDueSchedules(context.Context, ScheduleOptions) ([]Firing, error) {
	return nil, nil
}
func (s *triggerTestStore) FireEvent(context.Context, string, string, string, json.RawMessage, time.Time) ([]Firing, error) {
	return nil, nil
}

type credentialTestStore struct{ encrypted EncryptedCredential }

func (s *credentialTestStore) CreateCredential(_ context.Context, encrypted EncryptedCredential, _ string) (Credential, error) {
	s.encrypted = encrypted
	return encrypted.Credential, nil
}
func (s *credentialTestStore) RotateCredential(_ context.Context, encrypted EncryptedCredential, _ string) (Credential, error) {
	s.encrypted = encrypted
	return encrypted.Credential, nil
}
func (s *credentialTestStore) RevokeCredential(context.Context, string, string, string, time.Time) (Credential, error) {
	return Credential{}, nil
}
func (s *credentialTestStore) GrantCredential(context.Context, GrantCredentialRequest) error {
	return nil
}
func (s *credentialTestStore) EncryptedForAttempt(context.Context, ResolveCredentialRequest) (EncryptedCredential, error) {
	return s.encrypted, nil
}
func (s *credentialTestStore) EncryptedForTrigger(context.Context, string, string) (EncryptedCredential, error) {
	return s.encrypted, nil
}

type failingDelivery struct{}

func (failingDelivery) Deliver(context.Context, DeliveryMessage) error {
	return errors.New("authorization=Bearer-secret")
}

type deliveryTestStore struct {
	mu       sync.Mutex
	message  DeliveryMessage
	renewals int
}

func (s *deliveryTestStore) RenewDeliveryLease(_ context.Context, _ DeliveryMessage, _ string, _ time.Time, _ time.Duration) (time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.renewals++
	return time.Now().UTC().Add(time.Minute), nil
}

func (s *deliveryTestStore) ClaimDelivery(context.Context, string, string, time.Time, time.Duration) (DeliveryMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.message, nil
}
func (s *deliveryTestStore) AcknowledgeDelivery(context.Context, DeliveryMessage, string, time.Time) error {
	return nil
}
func (s *deliveryTestStore) FailDelivery(_ context.Context, message DeliveryMessage, _, diagnostic string, _, _ time.Time) (bool, error) {
	if strings.Contains(diagnostic, "Bearer-secret") {
		return false, errors.New("diagnostic was not redacted")
	}
	return message.Attempts >= message.MaxAttempts, nil
}
func (s *deliveryTestStore) ListDeadLetters(context.Context, string, int) ([]DeadLetter, error) {
	return nil, nil
}
func (s *deliveryTestStore) RequeueDeadLetter(context.Context, string, string, time.Time) error {
	return nil
}

type auditTestRecorder struct {
	failFirst bool
	records   []AuditRecord
}

func (r *auditTestRecorder) Record(_ context.Context, record AuditRecord) (AuditRecord, error) {
	if r.failFirst {
		return AuditRecord{}, errors.New("audit unavailable")
	}
	r.records = append(r.records, record)
	return record, nil
}

type executorTestDouble struct {
	calls int
	err   error
}

func (e *executorTestDouble) Execute(context.Context, workflowruntime.ExecutionRequest) (workflowruntime.ExecutionResult, error) {
	e.calls++
	return workflowruntime.ExecutionResult{}, e.err
}

func (e *executorTestDouble) Cancel(context.Context, workflowruntime.ExecutionRequest) error {
	return e.err
}
