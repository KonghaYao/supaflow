package httpapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pazhou/supaflow/internal/api"
	"github.com/pazhou/supaflow/internal/organization"
	"github.com/pazhou/supaflow/internal/platformops"
	"go.opentelemetry.io/otel/propagation"
)

// 验证公网 hook 使用固定 headers、原始 body 验签且无需接受客户端 organization。
func TestWebhookRoutePreservesRawBodyAndResolvesOrganizationServerSide(t *testing.T) {
	secret := []byte("01234567890123456789012345678901")
	store := &httpTriggerStore{}
	service, err := platformops.NewTriggerService(store, httpSecrets{secret: secret}, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	handler := &Handler{services: Services{Triggers: service}}
	now := time.Now().UTC().Truncate(time.Second)
	body := []byte("{ \"exact\" : true }")
	request := httptest.NewRequest(http.MethodPost, "/hooks/public-hook-value", bytes.NewReader(body))
	request.Header.Set("X-Supaflow-Request-ID", "request-1")
	request.Header.Set("X-Supaflow-Timestamp", strconv.FormatInt(now.Unix(), 10))
	request.Header.Set("X-Supaflow-Signature", platformops.SignWebhook(secret, "request-1", now, body))
	response := httptest.NewRecorder()
	handler.WebhookRoutes().ServeHTTP(response, request)
	if response.Code != http.StatusAccepted || store.receivedOrganization != "organization_from_store" || !bytes.Equal(store.receivedBody, body) {
		t.Fatalf("status=%d organization=%q body=%q response=%s", response.Code, store.receivedOrganization, store.receivedBody, response.Body.String())
	}
}

// 验证 publicHash 解析出的服务端 organization 会归属到唯一 webhook span，而不依赖请求传入 organization。
func TestWebhookRouteAttributesResolvedOrganizationToTelemetry(t *testing.T) {
	secret := []byte("01234567890123456789012345678901")
	store := &httpTriggerStore{}
	service, err := platformops.NewTriggerService(store, httpSecrets{secret: secret}, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	handler := &Handler{services: Services{Triggers: service}}
	telemetry := &recordingWebhookTelemetry{}
	route := platformops.NewDeferredOrganizationTelemetryHTTPMiddleware(telemetry, handler.WebhookRoutes())
	now := time.Now().UTC().Truncate(time.Second)
	body := []byte(`{"value":true}`)
	request := httptest.NewRequest(http.MethodPost, "/hooks/public-hook-value", bytes.NewReader(body))
	request.Header.Set("X-Supaflow-Request-ID", "request-telemetry")
	request.Header.Set("X-Supaflow-Timestamp", strconv.FormatInt(now.Unix(), 10))
	request.Header.Set("X-Supaflow-Signature", platformops.SignWebhook(secret, "request-telemetry", now, body))
	response := httptest.NewRecorder()
	route.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted || telemetry.starts != 1 || telemetry.organization != "organization_from_store" || telemetry.status != http.StatusAccepted {
		t.Fatalf("status=%d telemetry=%+v", response.Code, telemetry)
	}
}

type recordingWebhookTelemetry struct {
	starts       int
	organization string
	status       int
}

func (t *recordingWebhookTelemetry) Extract(ctx context.Context, _ propagation.TextMapCarrier) context.Context {
	return ctx
}

func (t *recordingWebhookTelemetry) StartDeferredHTTP(ctx context.Context, _ string) (context.Context, func(string, int, error)) {
	t.starts++
	return ctx, func(organization string, status int, _ error) { t.organization, t.status = organization, status }
}

// 验证 credential 管理 API 从认证上下文取 organization 且响应绝不回显 secret。
const httpTestMasterKey = "platformops-test-master-key-at-least-32-bytes"

func authenticatedTestRequest(request *http.Request, organizationID string) {
	request.Header.Set("Authorization", "Bearer "+httpTestMasterKey)
	request.Header.Set("X-Fenix-Organization-Id", organizationID)
}

func TestCredentialRouteNeverReturnsSecret(t *testing.T) {
	provider, err := platformops.NewLocalAESProvider("key-1", bytes.Repeat([]byte{9}, 32))
	if err != nil {
		t.Fatal(err)
	}
	store := &httpCredentialStore{}
	credentials, err := platformops.NewCredentialService(store, provider)
	if err != nil {
		t.Fatal(err)
	}
	handler := &Handler{services: Services{Credentials: credentials, Mutations: newHTTPMutationLedger()}}
	auth, err := api.NewMasterKeyAuthenticator(httpTestMasterKey)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/ops/credentials", strings.NewReader(`{"name":"signer","purpose":"webhook_signing","secret":"never-return-this"}`))
	request.Header.Set("Authorization", "Bearer "+httpTestMasterKey)
	request.Header.Set("X-Fenix-Organization-Id", "organization_auth")
	request.Header.Set("Idempotency-Key", "credential-create-1")
	response := httptest.NewRecorder()
	auth.Middleware(handler.Routes()).ServeHTTP(response, request)
	if response.Code != http.StatusCreated || store.organizationID != "organization_auth" || strings.Contains(response.Body.String(), "never-return-this") || strings.Contains(response.Body.String(), "secret") {
		t.Fatalf("status=%d organization=%q response=%s", response.Code, store.organizationID, response.Body.String())
	}
}

// 验证 /ops 请求体里 jsonb / text 存不下的转义在解码边界返回 400 invalid_argument：
// 此前 Name 含 \u0000 会写进列时被 Postgres 以 22021 拒绝，respond 没有归类，调用方拿到 500。
func TestCredentialRouteRejectsUnstorableName(t *testing.T) {
	provider, err := platformops.NewLocalAESProvider("key-1", bytes.Repeat([]byte{9}, 32))
	if err != nil {
		t.Fatal(err)
	}
	store := &httpCredentialStore{}
	credentials, err := platformops.NewCredentialService(store, provider)
	if err != nil {
		t.Fatal(err)
	}
	handler := &Handler{services: Services{Credentials: credentials, Mutations: newHTTPMutationLedger()}}
	auth, err := api.NewMasterKeyAuthenticator(httpTestMasterKey)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/ops/credentials", strings.NewReader(`{"name":"nul\u0000name","purpose":"webhook_signing","secret":"never-return-this"}`))
	request.Header.Set("Authorization", "Bearer "+httpTestMasterKey)
	request.Header.Set("X-Fenix-Organization-Id", "organization_auth")
	request.Header.Set("Idempotency-Key", "credential-nul-name-1")
	response := httptest.NewRecorder()
	auth.Middleware(handler.Routes()).ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var body struct {
		Code string `json:"code"`
	}
	if err = json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Code != "invalid_argument" || store.creates != 0 {
		t.Fatalf("code=%q creates=%d", body.Code, store.creates)
	}
}

// 验证所有 ops mutation 共用持久幂等语义：同指纹重放，异指纹冲突且不再执行。
func TestCredentialMutationReplaysAndRejectsFingerprintConflict(t *testing.T) {
	provider, _ := platformops.NewLocalAESProvider("key-1", bytes.Repeat([]byte{7}, 32))
	store := &httpCredentialStore{}
	credentials, _ := platformops.NewCredentialService(store, provider)
	handler := &Handler{services: Services{Credentials: credentials, Mutations: newHTTPMutationLedger()}}
	auth, _ := api.NewMasterKeyAuthenticator(httpTestMasterKey)
	serve := func(body string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/ops/credentials", strings.NewReader(body))
		request.Header.Set("Authorization", "Bearer "+httpTestMasterKey)
		request.Header.Set("X-Fenix-Organization-Id", "organization")
		request.Header.Set("Idempotency-Key", "credential-stable-key")
		response := httptest.NewRecorder()
		auth.Middleware(handler.Routes()).ServeHTTP(response, request)
		return response
	}
	first := serve(`{"name":"one","purpose":"webhook_signing","secret":"value"}`)
	replay := serve(`{"name":"one","purpose":"webhook_signing","secret":"value"}`)
	conflict := serve(`{"name":"two","purpose":"webhook_signing","secret":"value"}`)
	if first.Code != http.StatusCreated || replay.Code != http.StatusCreated || replay.Header().Get("Idempotency-Replayed") != "true" || conflict.Code != http.StatusConflict || store.creates != 1 {
		t.Fatalf("first=%d replay=%d conflict=%d creates=%d", first.Code, replay.Code, conflict.Code, store.creates)
	}
}

// 验证 Supaflow 不解释 Fenix 用户角色；幂等作用域仅由已认证组织上下文决定。
func TestCredentialMutationReplayRechecksCurrentRole(t *testing.T) {
	provider, _ := platformops.NewLocalAESProvider("key-1", bytes.Repeat([]byte{7}, 32))
	store := &httpCredentialStore{}
	credentials, _ := platformops.NewCredentialService(store, provider)
	handler := &Handler{services: Services{Credentials: credentials, Mutations: newHTTPMutationLedger()}}
	auth, _ := api.NewMasterKeyAuthenticator(httpTestMasterKey)
	body := `{"name":"one","purpose":"webhook_signing","secret":"value"}`
	serve := func(key string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/ops/credentials", strings.NewReader(body))
		authenticatedTestRequest(request, key)
		request.Header.Set("Idempotency-Key", "credential-role-change")
		response := httptest.NewRecorder()
		auth.Middleware(handler.Routes()).ServeHTTP(response, request)
		return response
	}
	created := serve("admin-key-at-least-24-bytes")
	denied := serve("viewer-key-at-least-24-bytes")
	if created.Code != http.StatusCreated || denied.Code != http.StatusCreated ||
		denied.Header().Get("Idempotency-Replayed") != "" || store.creates != 2 {
		t.Fatalf("created=%d denied=%d replay=%q creates=%d body=%s", created.Code, denied.Code,
			denied.Header().Get("Idempotency-Replayed"), store.creates, denied.Body.String())
	}
}

// 验证 mutation 响应快照严格有界，超限响应转换成可重放的失败而不在内存和 ledger 中放大。
func TestMutationResponseSnapshotIsBoundedAndReplayable(t *testing.T) {
	ledger := newHTTPMutationLedger()
	handler := (&Handler{services: Services{Mutations: ledger}}).idempotentMutations(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(bytes.Repeat([]byte("x"), maximumMutationResponseBytes+4096))
	}))
	serve := func() *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/ops/triggers", strings.NewReader(`{}`))
		request.Header.Set("Idempotency-Key", "bounded-response-key")
		request = request.WithContext(organization.ContextWithPrincipal(request.Context(), api.Principal{
			OrganizationID: "organization", ActorID: "operator",
		}))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response
	}
	first, replay := serve(), serve()
	if first.Code != http.StatusInsufficientStorage || replay.Code != http.StatusInsufficientStorage ||
		replay.Header().Get("Idempotency-Replayed") != "true" || first.Body.Len() > 256 || replay.Body.Len() > 256 {
		t.Fatalf("first=%d/%d replay=%d/%d replayed=%q", first.Code, first.Body.Len(), replay.Code, replay.Body.Len(), replay.Header().Get("Idempotency-Replayed"))
	}
}

// 验证 quota 拒绝映射为 429 并携带秒级 Retry-After。
func TestRateLimitResponseIncludesRetryAfter(t *testing.T) {
	response := httptest.NewRecorder()
	respond(response, nil, &platformops.RateLimitError{RetryAfter: 3 * time.Second, Reason: "full"}, http.StatusOK)
	if response.Code != http.StatusTooManyRequests || response.Header().Get("Retry-After") != "3" {
		t.Fatalf("status=%d retry-after=%q", response.Code, response.Header().Get("Retry-After"))
	}
}

// 验证 /ops 的存储错误归类：调用方送来的字节 Postgres 存不下（22021 / 22P05 / json 的 22P02）
// 返回 400，而真正的存储故障仍是 500——把服务端故障说成 400 会掩盖运维需要看到的信号。
func TestRespondClassifiesUnstorableInputAsClientError(t *testing.T) {
	for _, code := range []string{"22021", "22P05"} {
		response := httptest.NewRecorder()
		respond(response, nil, &pgconn.PgError{Code: code, Message: "stored bytes rejected"}, http.StatusOK)
		var body struct {
			Code string `json:"code"`
		}
		if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if response.Code != http.StatusBadRequest || body.Code != "invalid_request" {
			t.Fatalf("sqlstate=%s status=%d code=%q", code, response.Code, body.Code)
		}
	}
	outage := httptest.NewRecorder()
	respond(outage, nil, &pgconn.PgError{Code: "57P03", Message: "the database system is starting up"}, http.StatusOK)
	if outage.Code != http.StatusInternalServerError {
		t.Fatalf("outage status=%d body=%s", outage.Code, outage.Body.String())
	}
}

// 验证 Runs admission 只能由 Runtime 生命周期推进，运维面不暴露可绕过原子计数的修改入口。
func TestRunAdmissionMutationRoutesAreNotExposed(t *testing.T) {
	handler := (&Handler{}).Routes()
	for _, method := range []string{http.MethodPost, http.MethodDelete} {
		request := httptest.NewRequest(method, "/ops/admission/runs/run-1", nil)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusNotFound {
			t.Fatalf("method=%s status=%d body=%s", method, response.Code, response.Body.String())
		}
	}
}

// 验证 Trigger 列表的 organization 只能来自认证上下文，且写操作强制幂等键。
func TestTriggerLifecycleUsesAuthenticatedOrganizationAndRequiresIdempotency(t *testing.T) {
	store := &httpTriggerStore{}
	service, err := platformops.NewTriggerService(store, nil, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	handler := &Handler{services: Services{Triggers: service}}
	auth, err := api.NewMasterKeyAuthenticator(httpTestMasterKey)
	if err != nil {
		t.Fatal(err)
	}
	list := httptest.NewRequest(http.MethodGet, "/ops/triggers?organizationId=attacker", nil)
	authenticatedTestRequest(list, "organization_auth")
	listResponse := httptest.NewRecorder()
	auth.Middleware(handler.Routes()).ServeHTTP(listResponse, list)
	create := httptest.NewRequest(http.MethodPost, "/ops/triggers", strings.NewReader(`{"workflowId":"flow","kind":"api","name":"api","config":{}}`))
	authenticatedTestRequest(create, "organization_auth")
	createResponse := httptest.NewRecorder()
	auth.Middleware(handler.Routes()).ServeHTTP(createResponse, create)
	if listResponse.Code != http.StatusOK || store.receivedOrganization != "organization_auth" || createResponse.Code != http.StatusBadRequest {
		t.Fatalf("list=%d organization=%q create=%d body=%s", listResponse.Code, store.receivedOrganization, createResponse.Code, createResponse.Body.String())
	}
}

// 验证 ops JSON 上限针对完整原始 body，不能把 limit 边界伪装成合法 EOF 后忽略尾部。
func TestOpsMutationRejectsBodyWithValidJSONPrefixBeyondLimit(t *testing.T) {
	store := &httpTriggerStore{}
	service, err := platformops.NewTriggerService(store, nil, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	handler := &Handler{services: Services{Triggers: service, Mutations: newHTTPMutationLedger()}}
	auth, err := api.NewMasterKeyAuthenticator(httpTestMasterKey)
	if err != nil {
		t.Fatal(err)
	}
	prefix := `{"workflowId":"flow","kind":"api","name":"api","config":{}}`
	body := prefix + strings.Repeat(" ", (1<<20)-len(prefix)) + `{"ignored":true}`
	request := httptest.NewRequest(http.MethodPost, "/ops/triggers", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+httpTestMasterKey)
	request.Header.Set("X-Fenix-Organization-Id", "organization")
	request.Header.Set("Idempotency-Key", "oversized-trigger-body")
	response := httptest.NewRecorder()
	auth.Middleware(handler.Routes()).ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || store.receivedOrganization != "" {
		t.Fatalf("status=%d organization=%q body=%s", response.Code, store.receivedOrganization, response.Body.String())
	}
}

// 验证事件 fanout 的可重试 429 不会写成永久完成结果，同一幂等键可在容量恢复后继续。
func TestEventCapacityResponseReleasesMutationForImmediateRetry(t *testing.T) {
	store := &httpTriggerStore{eventFailures: 1}
	service, err := platformops.NewTriggerService(store, nil, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	ledger := newHTTPMutationLedger()
	handler := &Handler{services: Services{Triggers: service, Mutations: ledger}}
	auth, err := api.NewMasterKeyAuthenticator(httpTestMasterKey)
	if err != nil {
		t.Fatal(err)
	}
	serve := func() *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/ops/events/document.ready", strings.NewReader(`{"eventId":"event-1","payload":{"id":1}}`))
		request.Header.Set("Authorization", "Bearer "+httpTestMasterKey)
		request.Header.Set("X-Fenix-Organization-Id", "organization")
		request.Header.Set("Idempotency-Key", "event-capacity-retry")
		response := httptest.NewRecorder()
		auth.Middleware(handler.Routes()).ServeHTTP(response, request)
		return response
	}
	first, second := serve(), serve()
	if first.Code != http.StatusTooManyRequests || second.Code != http.StatusAccepted || ledger.retries != 1 || store.eventCalls != 2 {
		t.Fatalf("first=%d second=%d retries=%d calls=%d", first.Code, second.Code, ledger.retries, store.eventCalls)
	}
}

// 验证 outbound webhook 管理面仅使用认证 organization 且不接收明文 secret。
func TestCreateOutboundWebhookSubscriptionUsesAuthenticatedOrganization(t *testing.T) {
	webhooks := &httpWebhookManager{}
	handler := &Handler{services: Services{Webhooks: webhooks, Mutations: newHTTPMutationLedger()}}
	auth, err := api.NewMasterKeyAuthenticator(httpTestMasterKey)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/ops/webhook-subscriptions", strings.NewReader(`{"name":"events","endpointUrl":"https://events.example.test/hook","eventTypes":["run.succeeded"],"signingCredentialId":"cred-1","timeoutMs":1000}`))
	request.Header.Set("Authorization", "Bearer "+httpTestMasterKey)
	request.Header.Set("X-Fenix-Organization-Id", "organization")
	request.Header.Set("Idempotency-Key", "subscription-create-1")
	response := httptest.NewRecorder()
	auth.Middleware(handler.Routes()).ServeHTTP(response, request)
	if response.Code != http.StatusCreated || webhooks.created.OrganizationID != "organization" || webhooks.created.ActorID != "fenix" {
		t.Fatalf("status=%d request=%+v body=%s", response.Code, webhooks.created, response.Body.String())
	}
}

type httpSecrets struct{ secret []byte }

type httpMutationLedger struct {
	records map[string]platformops.MutationRecord
	hashes  map[string]string
	retries int
}

func newHTTPMutationLedger() *httpMutationLedger {
	return &httpMutationLedger{records: make(map[string]platformops.MutationRecord), hashes: make(map[string]string)}
}

func (l *httpMutationLedger) Claim(_ context.Context, organization, operation, key string, hash []byte) (platformops.MutationRecord, error) {
	scope := organization + "/" + operation + "/" + key
	encoded := string(hash)
	if previous, exists := l.hashes[scope]; exists {
		if previous != encoded {
			return platformops.MutationRecord{}, platformops.ErrConflict
		}
		return l.records[scope], nil
	}
	l.hashes[scope] = encoded
	return platformops.MutationRecord{Owner: "test-owner", OperationID: "test-operation"}, nil
}

func (l *httpMutationLedger) Complete(_ context.Context, organization, operation, key string, completion platformops.MutationCompletion) error {
	l.records[organization+"/"+operation+"/"+key] = platformops.MutationRecord{Replay: true, StatusCode: completion.StatusCode, ContentType: completion.ContentType, Body: append([]byte(nil), completion.Body...)}
	return nil
}

func (l *httpMutationLedger) Renew(context.Context, string, string, string, string) error { return nil }
func (l *httpMutationLedger) Retry(_ context.Context, organization, operation, key, _ string) error {
	scope := organization + "/" + operation + "/" + key
	delete(l.records, scope)
	delete(l.hashes, scope)
	l.retries++
	return nil
}

func (s httpSecrets) ResolveTriggerSecret(context.Context, string, string) ([]byte, error) {
	return append([]byte(nil), s.secret...), nil
}

type httpTriggerStore struct {
	receivedOrganization string
	receivedBody         []byte
	resolveErr           error
	eventFailures        int
	eventCalls           int
}

func (s *httpTriggerStore) CreateTrigger(context.Context, platformops.CreateTriggerRequest) (platformops.Trigger, error) {
	return platformops.Trigger{}, nil
}
func (s *httpTriggerStore) GetTrigger(context.Context, string, string) (platformops.Trigger, error) {
	return platformops.Trigger{}, nil
}
func (s *httpTriggerStore) ResolvePublicWebhook(context.Context, string) (platformops.Trigger, error) {
	if s.resolveErr != nil {
		return platformops.Trigger{}, s.resolveErr
	}
	return platformops.Trigger{OrganizationID: "organization_from_store", ID: "trigger_from_store", Kind: platformops.TriggerWebhook}, nil
}
func (s *httpTriggerStore) SetTriggerEnabled(context.Context, string, string, bool, time.Time) (platformops.Trigger, error) {
	return platformops.Trigger{}, nil
}
func (s *httpTriggerStore) Fire(context.Context, platformops.FireRequest) (platformops.Firing, bool, error) {
	return platformops.Firing{}, false, nil
}
func (s *httpTriggerStore) FireWebhook(_ context.Context, request platformops.FireRequest, _ string, _ [sha256.Size]byte, _ time.Time) (platformops.Firing, bool, error) {
	s.receivedOrganization = request.OrganizationID
	s.receivedBody = append([]byte(nil), request.Payload...)
	return platformops.Firing{ID: "firing-1"}, true, nil
}
func (s *httpTriggerStore) FireDueSchedules(context.Context, platformops.ScheduleOptions) ([]platformops.Firing, error) {
	return nil, nil
}
func (s *httpTriggerStore) FireEvent(context.Context, string, string, string, json.RawMessage, time.Time) ([]platformops.Firing, error) {
	s.eventCalls++
	if s.eventCalls <= s.eventFailures {
		return nil, platformops.ErrRateLimited
	}
	return []platformops.Firing{{ID: "event-firing"}}, nil
}
func (s *httpTriggerStore) ListTriggers(_ context.Context, request platformops.ListTriggersRequest) ([]platformops.Trigger, error) {
	s.receivedOrganization = request.OrganizationID
	return []platformops.Trigger{{ID: "trigger-1", OrganizationID: request.OrganizationID}}, nil
}
func (s *httpTriggerStore) UpdateTrigger(_ context.Context, request platformops.UpdateTriggerRequest) (platformops.Trigger, error) {
	s.receivedOrganization = request.OrganizationID
	return platformops.Trigger{ID: request.TriggerID, OrganizationID: request.OrganizationID}, nil
}
func (s *httpTriggerStore) SetTriggerEnabledIdempotently(_ context.Context, request platformops.ChangeTriggerStateRequest) (platformops.Trigger, error) {
	s.receivedOrganization = request.OrganizationID
	return platformops.Trigger{ID: request.TriggerID, OrganizationID: request.OrganizationID, Enabled: request.Enabled}, nil
}
func (s *httpTriggerStore) DeleteTrigger(_ context.Context, request platformops.DeleteTriggerRequest) error {
	s.receivedOrganization = request.OrganizationID
	return nil
}

type httpCredentialStore struct {
	organizationID string
	creates        int
}

type httpWebhookManager struct {
	created platformops.CreateOutboundWebhookRequest
}

func (m *httpWebhookManager) Create(_ context.Context, request platformops.CreateOutboundWebhookRequest) (platformops.OutboundWebhookSubscription, error) {
	m.created = request
	return platformops.OutboundWebhookSubscription{ID: "sub-1", Name: request.Name}, nil
}
func (m *httpWebhookManager) List(context.Context, string, int) ([]platformops.OutboundWebhookSubscription, error) {
	return nil, nil
}
func (m *httpWebhookManager) SetEnabled(context.Context, string, string, string, bool, time.Time) (platformops.OutboundWebhookSubscription, error) {
	return platformops.OutboundWebhookSubscription{}, nil
}
func (m *httpWebhookManager) Delete(context.Context, string, string, string, time.Time) error {
	return nil
}

func (s *httpCredentialStore) CreateCredential(_ context.Context, encrypted platformops.EncryptedCredential, _ string) (platformops.Credential, error) {
	s.organizationID = encrypted.Credential.OrganizationID
	s.creates++
	return encrypted.Credential, nil
}
func (s *httpCredentialStore) RotateCredential(context.Context, platformops.EncryptedCredential, string) (platformops.Credential, error) {
	return platformops.Credential{}, nil
}
func (s *httpCredentialStore) RevokeCredential(context.Context, string, string, string, time.Time) (platformops.Credential, error) {
	return platformops.Credential{}, nil
}
func (s *httpCredentialStore) GrantCredential(context.Context, platformops.GrantCredentialRequest) error {
	return nil
}
func (s *httpCredentialStore) EncryptedForAttempt(context.Context, platformops.ResolveCredentialRequest) (platformops.EncryptedCredential, error) {
	return platformops.EncryptedCredential{}, nil
}
func (s *httpCredentialStore) EncryptedForTrigger(context.Context, string, string) (platformops.EncryptedCredential, error) {
	return platformops.EncryptedCredential{}, nil
}
