package httpapi

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pazhou/supaflow/internal/api"
	"github.com/pazhou/supaflow/internal/platformops"
)

// 验证业务提交后 ledger 完成前崩溃时，过期接管复用稳定资源 ID，不会创建第二份凭据。
func TestCredentialMutationTakeoverDoesNotDuplicateCommittedResource(t *testing.T) {
	provider, err := platformops.NewLocalAESProvider("crash-window", bytes.Repeat([]byte{5}, 32))
	if err != nil {
		t.Fatal(err)
	}
	store := &recoveringCredentialStore{resources: make(map[string]platformops.Credential)}
	credentials, err := platformops.NewCredentialService(store, provider)
	if err != nil {
		t.Fatal(err)
	}
	ledger := &crashWindowLedger{operationID: "operation_crash_window", failCompletion: true}
	handler := &Handler{services: Services{Credentials: credentials, Mutations: ledger}}
	auth, err := api.NewMasterKeyAuthenticator(httpTestMasterKey)
	if err != nil {
		t.Fatal(err)
	}
	serve := func() *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/ops/credentials", strings.NewReader(
			`{"name":"signer","purpose":"webhook_signing","secret":"value"}`))
		request.Header.Set("Authorization", "Bearer "+httpTestMasterKey)
		request.Header.Set("X-Fenix-Organization-Id", "tenant")
		request.Header.Set("Idempotency-Key", "credential-crash-window")
		response := httptest.NewRecorder()
		auth.Middleware(handler.Routes()).ServeHTTP(response, request)
		return response
	}

	first := serve()
	ledger.failCompletion = false // models takeover after the first owner's lease expired
	second := serve()
	if first.Code != http.StatusInternalServerError || second.Code != http.StatusCreated {
		t.Fatalf("first=%d second=%d bodies=%q/%q", first.Code, second.Code, first.Body.String(), second.Body.String())
	}
	if len(store.resources) != 1 || store.calls != 2 {
		t.Fatalf("resources=%d calls=%d", len(store.resources), store.calls)
	}
	if firstID, secondID := extractCredentialID(first.Body.String()), extractCredentialID(second.Body.String()); firstID != "" || secondID == "" {
		t.Fatalf("unexpected ids first=%q second=%q", firstID, secondID)
	}
}

type crashWindowLedger struct {
	operationID    string
	failCompletion bool
}

func (l *crashWindowLedger) Claim(context.Context, string, string, string, []byte) (platformops.MutationRecord, error) {
	return platformops.MutationRecord{Owner: "lease-owner", OperationID: l.operationID}, nil
}

func (l *crashWindowLedger) Complete(context.Context, string, string, string, platformops.MutationCompletion) error {
	if l.failCompletion {
		return errors.New("simulated crash before ledger completion")
	}
	return nil
}

func (l *crashWindowLedger) Renew(context.Context, string, string, string, string) error { return nil }
func (l *crashWindowLedger) Retry(context.Context, string, string, string, string) error { return nil }

type recoveringCredentialStore struct {
	resources map[string]platformops.Credential
	calls     int
}

func (s *recoveringCredentialStore) CreateCredential(_ context.Context, encrypted platformops.EncryptedCredential, _ string) (platformops.Credential, error) {
	s.calls++
	if existing, ok := s.resources[encrypted.Credential.ID]; ok {
		return existing, nil
	}
	s.resources[encrypted.Credential.ID] = encrypted.Credential
	return encrypted.Credential, nil
}

func (s *recoveringCredentialStore) RotateCredential(context.Context, platformops.EncryptedCredential, string) (platformops.Credential, error) {
	return platformops.Credential{}, nil
}
func (s *recoveringCredentialStore) RevokeCredential(context.Context, string, string, string, time.Time) (platformops.Credential, error) {
	return platformops.Credential{}, nil
}
func (s *recoveringCredentialStore) GrantCredential(context.Context, platformops.GrantCredentialRequest) error {
	return nil
}
func (s *recoveringCredentialStore) EncryptedForAttempt(context.Context, platformops.ResolveCredentialRequest) (platformops.EncryptedCredential, error) {
	return platformops.EncryptedCredential{}, nil
}
func (s *recoveringCredentialStore) EncryptedForTrigger(context.Context, string, string) (platformops.EncryptedCredential, error) {
	return platformops.EncryptedCredential{}, nil
}

func extractCredentialID(body string) string {
	marker := `"id":"`
	start := strings.Index(body, marker)
	if start < 0 {
		return ""
	}
	rest := body[start+len(marker):]
	end := strings.IndexByte(rest, '"')
	if end < 0 {
		return ""
	}
	return rest[:end]
}
