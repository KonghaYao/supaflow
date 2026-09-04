package platformops

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// EnvelopeProvider wraps per-version data keys with an external KMS key.
type EnvelopeProvider interface {
	KeyID() string
	WrapKey(context.Context, []byte, []byte) ([]byte, error)
	UnwrapKey(context.Context, string, []byte, []byte) ([]byte, error)
}

// LocalAESProvider is a deployment-injected AES-GCM key provider for local/self-hosted use.
// The master key must come from a secret manager or environment injection and is never persisted.
type LocalAESProvider struct {
	keyID string
	aead  cipher.AEAD
}

// NewLocalAESProvider creates the local envelope provider from a 256-bit master key.
func NewLocalAESProvider(keyID string, masterKey []byte) (*LocalAESProvider, error) {
	if keyID == "" || len(masterKey) != 32 {
		return nil, ErrInvalidArgument
	}
	block, err := aes.NewCipher(masterKey)
	if err != nil {
		return nil, fmt.Errorf("configure local key provider: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("configure local key provider: %w", err)
	}
	return &LocalAESProvider{keyID: keyID, aead: aead}, nil
}

// KeyID identifies the injected wrapping key without exposing it.
func (p *LocalAESProvider) KeyID() string { return p.keyID }

// WrapKey encrypts a data key with authenticated context.
func (p *LocalAESProvider) WrapKey(_ context.Context, dataKey, aad []byte) ([]byte, error) {
	nonce := make([]byte, p.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("generate wrapping nonce: %w", err)
	}
	return append(nonce, p.aead.Seal(nil, nonce, dataKey, aad)...), nil
}

// UnwrapKey decrypts a data key only with the configured key identity and context.
func (p *LocalAESProvider) UnwrapKey(_ context.Context, keyID string, wrapped, aad []byte) ([]byte, error) {
	if keyID != p.keyID || len(wrapped) < p.aead.NonceSize() {
		return nil, ErrCredentialForbidden
	}
	nonce := wrapped[:p.aead.NonceSize()]
	plain, err := p.aead.Open(nil, nonce, wrapped[p.aead.NonceSize():], aad)
	if err != nil {
		return nil, ErrCredentialForbidden
	}
	return plain, nil
}

// Credential is safe metadata returned to management APIs; it never includes secret bytes.
type Credential struct {
	TenantID       string     `json:"-"`
	ID             string     `json:"id"`
	Name           string     `json:"name"`
	Provider       string     `json:"provider"`
	Purpose        string     `json:"purpose"`
	Status         string     `json:"status"`
	CurrentVersion int        `json:"currentVersion"`
	CreatedBy      string     `json:"createdBy"`
	CreatedAt      time.Time  `json:"createdAt"`
	UpdatedAt      time.Time  `json:"updatedAt"`
	RevokedAt      *time.Time `json:"revokedAt,omitempty"`
}

// CreateCredentialRequest creates a credential and its first encrypted version.
type CreateCredentialRequest struct {
	TenantID string
	ActorID  string
	Name     string
	Purpose  string
	Secret   []byte
}

// ResolveCredentialRequest grants one running attempt least-privilege secret access.
type ResolveCredentialRequest struct {
	TenantID     string
	RunID        string
	AttemptID    string
	CredentialID string
	Purpose      string
	FencingToken uint64
	Now          time.Time
}

// GrantCredentialRequest binds one purpose-specific credential grant to the
// currently leased attempt. Stale workers cannot mint or consume a grant.
type GrantCredentialRequest struct {
	TenantID     string
	RunID        string
	AttemptID    string
	CredentialID string
	Purpose      string
	FencingToken uint64
	Now          time.Time
	ExpiresAt    *time.Time
}

// EncryptedCredential is the persistence boundary model.
type EncryptedCredential struct {
	Credential Credential
	Version    int
	KeyID      string
	WrappedKey []byte
	Nonce      []byte
	Ciphertext []byte
}

// CredentialStore atomically persists versions, grants, revocation, and audit evidence.
type CredentialStore interface {
	CreateCredential(context.Context, EncryptedCredential, string) (Credential, error)
	RotateCredential(context.Context, EncryptedCredential, string) (Credential, error)
	RevokeCredential(context.Context, string, string, string, time.Time) (Credential, error)
	GrantCredential(context.Context, GrantCredentialRequest) error
	EncryptedForAttempt(context.Context, ResolveCredentialRequest) (EncryptedCredential, error)
	EncryptedForTrigger(context.Context, string, string) (EncryptedCredential, error)
}

type credentialReferenceStore interface {
	EncryptedForReference(context.Context, string, string, string) (EncryptedCredential, error)
}

// CredentialService encrypts secret material before persistence and decrypts it only for authorized attempts.
type CredentialService struct {
	store    CredentialStore
	provider EnvelopeProvider
}

// NewCredentialService creates a credential manager backed by envelope encryption.
func NewCredentialService(store CredentialStore, provider EnvelopeProvider) (*CredentialService, error) {
	if store == nil || provider == nil {
		return nil, ErrInvalidArgument
	}
	return &CredentialService{store: store, provider: provider}, nil
}

// Create encrypts a new credential and clears the caller-owned secret buffer after use.
func (s *CredentialService) Create(ctx context.Context, request CreateCredentialRequest) (Credential, error) {
	if request.TenantID == "" || request.ActorID == "" || request.Name == "" || request.Purpose == "" || len(request.Secret) == 0 {
		return Credential{}, ErrInvalidArgument
	}
	id, err := mutationResourceIDOrRandom(ctx, "cred", "reference")
	if err != nil {
		return Credential{}, err
	}
	encrypted, err := s.encrypt(ctx, request.TenantID, id, 1, request.Secret)
	clearBytes(request.Secret)
	if err != nil {
		return Credential{}, err
	}
	encrypted.Credential = Credential{
		TenantID: request.TenantID, ID: id, Name: request.Name, Provider: "envelope",
		Purpose: request.Purpose, Status: "active", CurrentVersion: 1, CreatedBy: request.ActorID,
	}
	return s.store.CreateCredential(ctx, encrypted, request.ActorID)
}

// Rotate replaces the active version without changing the stable reference.
func (s *CredentialService) Rotate(ctx context.Context, tenantID, credentialID, actorID string, nextVersion int, secret []byte) (Credential, error) {
	if tenantID == "" || credentialID == "" || actorID == "" || nextVersion < 2 || len(secret) == 0 {
		return Credential{}, ErrInvalidArgument
	}
	encrypted, err := s.encrypt(ctx, tenantID, credentialID, nextVersion, secret)
	clearBytes(secret)
	if err != nil {
		return Credential{}, err
	}
	encrypted.Credential = Credential{TenantID: tenantID, ID: credentialID, CurrentVersion: nextVersion}
	return s.store.RotateCredential(ctx, encrypted, actorID)
}

// Revoke permanently prevents further resolution while retaining audit history.
func (s *CredentialService) Revoke(ctx context.Context, tenantID, credentialID, actorID string, now time.Time) (Credential, error) {
	if tenantID == "" || credentialID == "" || actorID == "" || now.IsZero() {
		return Credential{}, ErrInvalidArgument
	}
	return s.store.RevokeCredential(ctx, tenantID, credentialID, actorID, now)
}

// GrantForAttempt permits exactly the current fenced attempt to resolve one
// purpose-bound credential, optionally until an earlier explicit expiry.
func (s *CredentialService) GrantForAttempt(ctx context.Context, request GrantCredentialRequest) error {
	if request.TenantID == "" || request.RunID == "" || request.AttemptID == "" || request.CredentialID == "" ||
		request.Purpose == "" || request.FencingToken == 0 || request.Now.IsZero() {
		return ErrInvalidArgument
	}
	return s.store.GrantCredential(ctx, request)
}

// ResolveForAttempt returns secret bytes only after a tenant-scoped active-attempt grant check.
func (s *CredentialService) ResolveForAttempt(ctx context.Context, request ResolveCredentialRequest) ([]byte, error) {
	if request.TenantID == "" || request.RunID == "" || request.AttemptID == "" || request.CredentialID == "" || request.Purpose == "" || request.FencingToken == 0 || request.Now.IsZero() {
		return nil, ErrInvalidArgument
	}
	encrypted, err := s.store.EncryptedForAttempt(ctx, request)
	if err != nil {
		return nil, err
	}
	if encrypted.Credential.Status != "active" || encrypted.Credential.Purpose != request.Purpose {
		return nil, ErrCredentialForbidden
	}
	return s.decrypt(ctx, encrypted)
}

// ResolveTriggerSecret implements TriggerSecretResolver without exposing secret material to HTTP.
func (s *CredentialService) ResolveTriggerSecret(ctx context.Context, tenantID, triggerID string) ([]byte, error) {
	encrypted, err := s.store.EncryptedForTrigger(ctx, tenantID, triggerID)
	if err != nil {
		return nil, err
	}
	if encrypted.Credential.Status != "active" || encrypted.Credential.Purpose != "webhook_signing" {
		return nil, ErrCredentialForbidden
	}
	return s.decrypt(ctx, encrypted)
}

// ResolveOutboundWebhookSecret decrypts one active signing reference only for delivery.
func (s *CredentialService) ResolveOutboundWebhookSecret(ctx context.Context, tenantID, credentialID string) ([]byte, error) {
	store, ok := s.store.(credentialReferenceStore)
	if !ok || tenantID == "" || credentialID == "" {
		return nil, ErrCredentialForbidden
	}
	encrypted, err := store.EncryptedForReference(ctx, tenantID, credentialID, "outbound_webhook_signing")
	if err != nil {
		return nil, err
	}
	return s.decrypt(ctx, encrypted)
}

func (s *CredentialService) encrypt(ctx context.Context, tenantID, credentialID string, version int, secret []byte) (EncryptedCredential, error) {
	dataKey := make([]byte, 32)
	if _, err := rand.Read(dataKey); err != nil {
		return EncryptedCredential{}, fmt.Errorf("generate credential data key: %w", err)
	}
	defer clearBytes(dataKey)
	block, err := aes.NewCipher(dataKey)
	if err != nil {
		return EncryptedCredential{}, fmt.Errorf("configure credential cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return EncryptedCredential{}, fmt.Errorf("configure credential cipher: %w", err)
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return EncryptedCredential{}, fmt.Errorf("generate credential nonce: %w", err)
	}
	aad := credentialAAD(tenantID, credentialID, version)
	wrapped, err := s.provider.WrapKey(ctx, dataKey, aad)
	if err != nil {
		return EncryptedCredential{}, fmt.Errorf("wrap credential data key: %w", err)
	}
	return EncryptedCredential{Version: version, KeyID: s.provider.KeyID(), WrappedKey: wrapped, Nonce: nonce, Ciphertext: aead.Seal(nil, nonce, secret, aad)}, nil
}

func (s *CredentialService) decrypt(ctx context.Context, encrypted EncryptedCredential) ([]byte, error) {
	aad := credentialAAD(encrypted.Credential.TenantID, encrypted.Credential.ID, encrypted.Version)
	dataKey, err := s.provider.UnwrapKey(ctx, encrypted.KeyID, encrypted.WrappedKey, aad)
	if err != nil {
		return nil, fmt.Errorf("unwrap credential: %w", err)
	}
	defer clearBytes(dataKey)
	block, err := aes.NewCipher(dataKey)
	if err != nil {
		return nil, ErrCredentialForbidden
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, ErrCredentialForbidden
	}
	secret, err := aead.Open(nil, encrypted.Nonce, encrypted.Ciphertext, aad)
	if err != nil {
		return nil, ErrCredentialForbidden
	}
	return secret, nil
}

func credentialAAD(tenantID, credentialID string, version int) []byte {
	value, _ := json.Marshal([]any{tenantID, credentialID, version})
	return value
}

func credentialError(operation string, err error) error {
	if errors.Is(err, ErrCredentialForbidden) || errors.Is(err, ErrCredentialRevoked) || errors.Is(err, ErrNotFound) || errors.Is(err, ErrConflict) {
		return err
	}
	return fmt.Errorf("%s: %w", operation, err)
}
