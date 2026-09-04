package platformops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresCredentialStore persists envelope-encrypted versions and least-privilege grants.
type PostgresCredentialStore struct{ pool *pgxpool.Pool }

// NewPostgresCredentialStore creates a durable credential store.
func NewPostgresCredentialStore(pool *pgxpool.Pool) *PostgresCredentialStore {
	return &PostgresCredentialStore{pool: pool}
}

// CreateCredential atomically stores the first version and audit evidence.
func (s *PostgresCredentialStore) CreateCredential(ctx context.Context, encrypted EncryptedCredential, actorID string) (Credential, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Credential{}, credentialError("begin credential creation", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	credential := encrypted.Credential
	err = tx.QueryRow(ctx, `
		INSERT INTO credential_refs (tenant_id,id,name,provider,purpose,status,current_version,created_by)
		VALUES ($1,$2,$3,$4,$5,'active',1,$6)
		ON CONFLICT (tenant_id,id) DO NOTHING
		RETURNING status,current_version,created_at,updated_at,revoked_at`,
		credential.TenantID, credential.ID, credential.Name, credential.Provider,
		credential.Purpose, actorID).Scan(&credential.Status, &credential.CurrentVersion,
		&credential.CreatedAt, &credential.UpdatedAt, &credential.RevokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		if _, stable := MutationResourceID(ctx, "cred", "reference"); !stable {
			return Credential{}, credentialError("create credential reference", err)
		}
		credential, err = lockCredential(ctx, tx, credential.TenantID, credential.ID)
		if err != nil {
			return Credential{}, err
		}
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return Credential{}, credentialError("commit credential creation replay", commitErr)
		}
		return credential, nil
	}
	if err != nil {
		return Credential{}, credentialError("create credential reference", err)
	}
	if err = insertCredentialVersion(ctx, tx, encrypted, actorID); err != nil {
		return Credential{}, err
	}
	if _, err = insertAuditTx(ctx, tx, AuditRecord{
		TenantID: credential.TenantID, ActorID: actorID, Action: "credential.create",
		ResourceType: "credential", ResourceID: credential.ID, Outcome: AuditAllowed,
	}); err != nil {
		return Credential{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Credential{}, credentialError("commit credential creation", err)
	}
	return credential, nil
}

// RotateCredential atomically advances exactly one version and retires the prior ciphertext.
func (s *PostgresCredentialStore) RotateCredential(ctx context.Context, encrypted EncryptedCredential, actorID string) (Credential, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Credential{}, credentialError("begin credential rotation", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	credential, err := lockCredential(ctx, tx, encrypted.Credential.TenantID, encrypted.Credential.ID)
	if err != nil {
		return Credential{}, err
	}
	if credential.Status != "active" {
		return Credential{}, ErrCredentialRevoked
	}
	if encrypted.Version == credential.CurrentVersion {
		if _, stable := MutationResourceID(ctx, "credential-rotation", credential.ID); stable {
			if commitErr := tx.Commit(ctx); commitErr != nil {
				return Credential{}, credentialError("commit credential rotation replay", commitErr)
			}
			return credential, nil
		}
	}
	if encrypted.Version != credential.CurrentVersion+1 {
		return Credential{}, ErrConflict
	}
	if err = insertCredentialVersion(ctx, tx, encrypted, actorID); err != nil {
		return Credential{}, err
	}
	if _, err = tx.Exec(ctx, `
		UPDATE credential_versions SET retired_at=now()
		WHERE tenant_id=$1 AND credential_id=$2 AND version=$3 AND retired_at IS NULL`,
		credential.TenantID, credential.ID, credential.CurrentVersion); err != nil {
		return Credential{}, credentialError("retire credential version", err)
	}
	err = tx.QueryRow(ctx, `
		UPDATE credential_refs SET current_version=$3,updated_at=now()
		WHERE tenant_id=$1 AND id=$2
		RETURNING status,current_version,created_at,updated_at,revoked_at`,
		credential.TenantID, credential.ID, encrypted.Version).Scan(
		&credential.Status, &credential.CurrentVersion, &credential.CreatedAt, &credential.UpdatedAt, &credential.RevokedAt)
	if err != nil {
		return Credential{}, credentialError("advance credential version", err)
	}
	if _, err = insertAuditTx(ctx, tx, AuditRecord{
		TenantID: credential.TenantID, ActorID: actorID, Action: "credential.rotate",
		ResourceType: "credential", ResourceID: credential.ID, Outcome: AuditAllowed,
	}); err != nil {
		return Credential{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Credential{}, credentialError("commit credential rotation", err)
	}
	return credential, nil
}

// RevokeCredential atomically revokes a reference and records who did it.
func (s *PostgresCredentialStore) RevokeCredential(ctx context.Context, tenantID, credentialID, actorID string, now time.Time) (Credential, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Credential{}, credentialError("begin credential revocation", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	credential, err := lockCredential(ctx, tx, tenantID, credentialID)
	if err != nil {
		return Credential{}, err
	}
	if credential.Status != "revoked" {
		err = tx.QueryRow(ctx, `
			UPDATE credential_refs SET status='revoked',revoked_at=$3,updated_at=$3
			WHERE tenant_id=$1 AND id=$2
			RETURNING status,current_version,created_at,updated_at,revoked_at`, tenantID, credentialID, now).Scan(
			&credential.Status, &credential.CurrentVersion, &credential.CreatedAt, &credential.UpdatedAt, &credential.RevokedAt)
		if err != nil {
			return Credential{}, credentialError("revoke credential", err)
		}
		if _, err = insertAuditTx(ctx, tx, AuditRecord{
			TenantID: tenantID, ActorID: actorID, Action: "credential.revoke",
			ResourceType: "credential", ResourceID: credentialID, Outcome: AuditAllowed,
		}); err != nil {
			return Credential{}, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return Credential{}, credentialError("commit credential revocation", err)
	}
	return credential, nil
}

// GrantCredential grants a tenant-scoped attempt access without copying secret data.
func (s *PostgresCredentialStore) GrantCredential(ctx context.Context, request GrantCredentialRequest) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return credentialError("begin credential grant", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	tag, err := tx.Exec(ctx, `
		INSERT INTO attempt_credential_grants (tenant_id,attempt_id,credential_id,granted_by,expires_at)
		SELECT $1,$2,$3,$2,$8 FROM credential_refs c
		JOIN node_attempts a ON a.tenant_id=c.tenant_id AND a.id=$2 AND a.run_id=$4
		JOIN worker_leases l ON l.tenant_id=a.tenant_id AND l.run_id=a.run_id AND l.attempt_id=a.id
		WHERE c.tenant_id=$1 AND c.id=$3 AND c.status='active' AND c.purpose=$5
		  AND a.status='running' AND a.fencing_token=$6
		  AND l.fencing_token=$6 AND l.expires_at>$7
		ON CONFLICT (tenant_id,attempt_id,credential_id) DO UPDATE
		SET granted_by=EXCLUDED.granted_by,expires_at=EXCLUDED.expires_at,created_at=now()`,
		request.TenantID, request.AttemptID, request.CredentialID, request.RunID, request.Purpose,
		request.FencingToken, request.Now, request.ExpiresAt)
	if err != nil {
		return credentialError("grant credential", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrCredentialForbidden
	}
	if _, err = insertAuditTx(ctx, tx, AuditRecord{
		TenantID: request.TenantID, ActorID: request.AttemptID, Action: "credential.grant",
		ResourceType: "credential", ResourceID: request.CredentialID, Outcome: AuditAllowed,
		Metadata: json.RawMessage(fmt.Sprintf(`{"attemptId":%q,"runId":%q,"fencingToken":%d}`, request.AttemptID, request.RunID, request.FencingToken)),
	}); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return credentialError("commit credential grant", err)
	}
	return nil
}

// EncryptedForAttempt returns ciphertext only after the running-attempt grant predicate succeeds.
func (s *PostgresCredentialStore) EncryptedForAttempt(ctx context.Context, request ResolveCredentialRequest) (EncryptedCredential, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return EncryptedCredential{}, credentialError("begin credential resolution", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	encrypted, err := scanEncryptedCredential(tx.QueryRow(ctx, encryptedCredentialSelect+`
		JOIN attempt_credential_grants g ON g.tenant_id=c.tenant_id AND g.credential_id=c.id
		JOIN node_attempts a ON a.tenant_id=g.tenant_id AND a.id=g.attempt_id
		JOIN worker_leases l ON l.tenant_id=a.tenant_id AND l.run_id=a.run_id AND l.attempt_id=a.id
		WHERE c.tenant_id=$1 AND c.id=$2 AND g.attempt_id=$3 AND a.run_id=$4
		  AND c.status='active' AND c.purpose=$5 AND a.status='running'
		  AND a.fencing_token=$6 AND l.fencing_token=$6 AND l.expires_at>$7
		  AND (g.expires_at IS NULL OR g.expires_at > $7)`,
		request.TenantID, request.CredentialID, request.AttemptID, request.RunID,
		request.Purpose, request.FencingToken, request.Now))
	if errors.Is(err, pgx.ErrNoRows) {
		_, _ = insertAuditTx(ctx, tx, AuditRecord{
			TenantID: request.TenantID, ActorID: request.AttemptID, Action: "credential.resolve",
			ResourceType: "credential", ResourceID: request.CredentialID, Outcome: AuditDenied,
		})
		_ = tx.Commit(ctx)
		return EncryptedCredential{}, ErrCredentialForbidden
	}
	if err != nil {
		return EncryptedCredential{}, credentialError("resolve attempt credential", err)
	}
	if _, err = insertAuditTx(ctx, tx, AuditRecord{
		TenantID: request.TenantID, ActorID: request.AttemptID, Action: "credential.resolve",
		ResourceType: "credential", ResourceID: request.CredentialID, Outcome: AuditAllowed,
	}); err != nil {
		return EncryptedCredential{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return EncryptedCredential{}, credentialError("commit credential resolution", err)
	}
	return encrypted, nil
}

// EncryptedForTrigger returns webhook signing ciphertext scoped to the trigger's tenant.
func (s *PostgresCredentialStore) EncryptedForTrigger(ctx context.Context, tenantID, triggerID string) (EncryptedCredential, error) {
	encrypted, err := scanEncryptedCredential(s.pool.QueryRow(ctx, encryptedCredentialSelect+`
		JOIN triggers t ON t.tenant_id=c.tenant_id AND t.signing_credential_id=c.id
		WHERE c.tenant_id=$1 AND t.id=$2 AND c.status='active' AND c.purpose='webhook_signing'`, tenantID, triggerID))
	if errors.Is(err, pgx.ErrNoRows) {
		return EncryptedCredential{}, ErrCredentialForbidden
	}
	if err != nil {
		return EncryptedCredential{}, credentialError("resolve trigger credential", err)
	}
	return encrypted, nil
}

// EncryptedForReference loads one active purpose-bound credential without exposing plaintext.
func (s *PostgresCredentialStore) EncryptedForReference(ctx context.Context, tenantID, credentialID, purpose string) (EncryptedCredential, error) {
	encrypted, err := scanEncryptedCredential(s.pool.QueryRow(ctx, encryptedCredentialSelect+`
		WHERE c.tenant_id=$1 AND c.id=$2 AND c.status='active' AND c.purpose=$3`, tenantID, credentialID, purpose))
	if errors.Is(err, pgx.ErrNoRows) {
		return EncryptedCredential{}, ErrCredentialForbidden
	}
	if err != nil {
		return EncryptedCredential{}, credentialError("resolve credential reference", err)
	}
	return encrypted, nil
}

const encryptedCredentialSelect = `
	SELECT c.tenant_id,c.id,c.name,c.provider,c.purpose,c.status,c.current_version,
	       c.created_by,c.created_at,c.updated_at,c.revoked_at,
	       v.version,v.key_id,v.wrapped_data_key,v.nonce,v.ciphertext
	FROM credential_refs c
	JOIN credential_versions v ON v.tenant_id=c.tenant_id AND v.credential_id=c.id AND v.version=c.current_version `

func insertCredentialVersion(ctx context.Context, tx pgx.Tx, encrypted EncryptedCredential, actorID string) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO credential_versions (tenant_id,credential_id,version,key_id,wrapped_data_key,nonce,ciphertext,created_by)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, encrypted.Credential.TenantID,
		encrypted.Credential.ID, encrypted.Version, encrypted.KeyID, encrypted.WrappedKey,
		encrypted.Nonce, encrypted.Ciphertext, actorID)
	if err != nil {
		return credentialError("persist credential version", err)
	}
	return nil
}

func lockCredential(ctx context.Context, tx pgx.Tx, tenantID, credentialID string) (Credential, error) {
	var credential Credential
	err := tx.QueryRow(ctx, `
		SELECT tenant_id,id,name,provider,purpose,status,current_version,created_by,created_at,updated_at,revoked_at
		FROM credential_refs WHERE tenant_id=$1 AND id=$2 FOR UPDATE`, tenantID, credentialID).Scan(
		&credential.TenantID, &credential.ID, &credential.Name, &credential.Provider,
		&credential.Purpose, &credential.Status, &credential.CurrentVersion, &credential.CreatedBy,
		&credential.CreatedAt, &credential.UpdatedAt, &credential.RevokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Credential{}, ErrNotFound
	}
	if err != nil {
		return Credential{}, credentialError("lock credential", err)
	}
	return credential, nil
}

func scanEncryptedCredential(row pgx.Row) (EncryptedCredential, error) {
	var encrypted EncryptedCredential
	err := row.Scan(&encrypted.Credential.TenantID, &encrypted.Credential.ID,
		&encrypted.Credential.Name, &encrypted.Credential.Provider, &encrypted.Credential.Purpose,
		&encrypted.Credential.Status, &encrypted.Credential.CurrentVersion, &encrypted.Credential.CreatedBy,
		&encrypted.Credential.CreatedAt, &encrypted.Credential.UpdatedAt, &encrypted.Credential.RevokedAt,
		&encrypted.Version, &encrypted.KeyID, &encrypted.WrappedKey, &encrypted.Nonce, &encrypted.Ciphertext)
	return encrypted, err
}
