package identity

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const resolveAPIKeySQL = `
SELECT k.tenant_id, k.service_account_id, m.role
FROM api_keys k
JOIN tenants t ON t.id = k.tenant_id AND t.status = 'active'
JOIN service_accounts s ON s.tenant_id = k.tenant_id
    AND s.id = k.service_account_id AND s.status = 'active'
JOIN tenant_memberships m ON m.tenant_id = k.tenant_id
    AND m.actor_id = s.id AND m.actor_type = 'service_account' AND m.status = 'active'
WHERE k.key_hash = $1 AND k.revoked_at IS NULL
    AND (k.expires_at IS NULL OR k.expires_at > $2)`

const resolveSessionSQL = `
SELECT s.tenant_id, s.actor_id, m.role
FROM identity_sessions s
JOIN tenants t ON t.id = s.tenant_id AND t.status = 'active'
JOIN tenant_memberships m ON m.tenant_id = s.tenant_id
    AND m.actor_id = s.actor_id AND m.actor_type = 'user' AND m.status = 'active'
JOIN oidc_identities i ON i.id = s.actor_id AND i.status = 'active'
WHERE s.session_hash = $1 AND s.revoked_at IS NULL AND s.expires_at > $2`

// PostgresStore is the durable production identity adapter.
type PostgresStore struct{ pool *pgxpool.Pool }

// NewPostgresStore creates a PostgreSQL identity store.
func NewPostgresStore(pool *pgxpool.Pool) (*PostgresStore, error) {
	if pool == nil {
		return nil, errors.New("identity postgres store requires a pool")
	}
	return &PostgresStore{pool: pool}, nil
}

// BootstrapAPIKey inserts an initial service account, membership, and digest-only API key.
// Existing rows are never changed, so bootstrap configuration cannot silently mutate production RBAC.
func (s *PostgresStore) BootstrapAPIKey(ctx context.Context, key BootstrapAPIKey) error {
	if len(key.Value) < 24 || key.TenantID == "" || key.ActorID == "" || key.Name == "" || !ValidRole(key.Role) {
		return errors.New("invalid bootstrap API key")
	}
	digest := DigestSecret(key.Value)
	digestHex := hex.EncodeToString(digest[:])
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin API key bootstrap: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err = tx.Exec(ctx, `
INSERT INTO service_accounts (tenant_id,id,name) VALUES ($1,$2,$3)
ON CONFLICT (tenant_id,id) DO NOTHING`, key.TenantID, key.ActorID, key.Name); err != nil {
		return fmt.Errorf("bootstrap service account: %w", err)
	}
	if _, err = tx.Exec(ctx, `
INSERT INTO tenant_memberships (tenant_id,actor_id,actor_type,role)
VALUES ($1,$2,'service_account',$3) ON CONFLICT (tenant_id,actor_id) DO NOTHING`, key.TenantID, key.ActorID, key.Role); err != nil {
		return fmt.Errorf("bootstrap membership: %w", err)
	}
	if _, err = tx.Exec(ctx, `
INSERT INTO api_keys (id,tenant_id,name,key_hash,key_prefix,service_account_id)
VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (key_hash) DO NOTHING`,
		"key_"+digestHex[:24], key.TenantID, key.Name, digest[:], "sha256:"+digestHex[:8], key.ActorID); err != nil {
		return fmt.Errorf("bootstrap API key digest: %w", err)
	}
	var storedTenant, storedActor string
	var storedRole Role
	if err = tx.QueryRow(ctx, resolveAPIKeySQL, digest[:], time.Now().UTC()).Scan(&storedTenant, &storedActor, &storedRole); err != nil {
		return fmt.Errorf("verify bootstrapped API key: %w", err)
	}
	if storedTenant != key.TenantID || storedActor != key.ActorID || storedRole != key.Role {
		return errors.New("bootstrap API key conflicts with existing server-side identity")
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit API key bootstrap: %w", err)
	}
	return nil
}

// ResolveAPIKey resolves an active API key through its service account and membership.
func (s *PostgresStore) ResolveAPIKey(ctx context.Context, digest [sha256.Size]byte, now time.Time) (Principal, error) {
	return scanPrincipal(s.pool.QueryRow(ctx, resolveAPIKeySQL, digest[:], now))
}

// ResolveSession resolves an unexpired session and re-reads the current membership role.
func (s *PostgresStore) ResolveSession(ctx context.Context, digest [sha256.Size]byte, now time.Time) (Principal, error) {
	return scanPrincipal(s.pool.QueryRow(ctx, resolveSessionSQL, digest[:], now))
}

// CreateLoginAttempt persists one short-lived OIDC state transaction.
func (s *PostgresStore) CreateLoginAttempt(ctx context.Context, attempt LoginAttempt) error {
	if _, err := s.pool.Exec(ctx, `DELETE FROM oidc_login_attempts WHERE state_hash IN (
SELECT state_hash FROM oidc_login_attempts WHERE expires_at <= $1 ORDER BY expires_at LIMIT 1000)`, time.Now().UTC()); err != nil {
		return fmt.Errorf("purge expired OIDC login attempts: %w", err)
	}
	_, err := s.pool.Exec(ctx, `
INSERT INTO oidc_login_attempts
    (state_hash,browser_hash,nonce_hash,pkce_verifier,requested_tenant_id,expires_at)
VALUES ($1,$2,$3,$4,NULLIF($5,''),$6)`, attempt.StateHash[:], attempt.BrowserHash[:], attempt.NonceHash[:], attempt.PKCEVerifier, attempt.RequestedTenant, attempt.ExpiresAt)
	if err != nil {
		return fmt.Errorf("create OIDC login attempt: %w", err)
	}
	return nil
}

// ConsumeLoginAttempt atomically deletes and returns a live OIDC login transaction.
func (s *PostgresStore) ConsumeLoginAttempt(ctx context.Context, stateHash [sha256.Size]byte, now time.Time) (LoginAttempt, error) {
	var attempt LoginAttempt
	var browser, nonce []byte
	err := s.pool.QueryRow(ctx, `
DELETE FROM oidc_login_attempts
WHERE state_hash=$1 AND expires_at>$2
RETURNING browser_hash,nonce_hash,pkce_verifier,COALESCE(requested_tenant_id,''),expires_at`, stateHash[:], now).
		Scan(&browser, &nonce, &attempt.PKCEVerifier, &attempt.RequestedTenant, &attempt.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return LoginAttempt{}, ErrInvalidLogin
	}
	if err != nil {
		return LoginAttempt{}, fmt.Errorf("consume OIDC login attempt: %w", err)
	}
	if len(browser) != sha256.Size || len(nonce) != sha256.Size {
		return LoginAttempt{}, ErrInvalidLogin
	}
	copy(attempt.StateHash[:], stateHash[:])
	copy(attempt.BrowserHash[:], browser)
	copy(attempt.NonceHash[:], nonce)
	return attempt, nil
}

// ResolveOIDCPrincipal upserts provider profile data and loads role only from membership storage.
func (s *PostgresStore) ResolveOIDCPrincipal(ctx context.Context, claims OIDCClaims, requestedTenant string, now time.Time) (Principal, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Principal{}, fmt.Errorf("begin OIDC identity resolution: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	actorID := "usr_" + uuid.NewString()
	err = tx.QueryRow(ctx, `
INSERT INTO oidc_identities (id,issuer,subject,email,display_name,updated_at)
VALUES ($1,$2,$3,NULLIF($4,''),NULLIF($5,''),$6)
ON CONFLICT (issuer,subject) DO UPDATE SET
    email=EXCLUDED.email, display_name=EXCLUDED.display_name, updated_at=EXCLUDED.updated_at
RETURNING id`, actorID, claims.Issuer, claims.Subject, claims.Email, claims.DisplayName, now).Scan(&actorID)
	if err != nil {
		return Principal{}, fmt.Errorf("upsert OIDC identity: %w", err)
	}
	query := `SELECT m.tenant_id,m.actor_id,m.role FROM tenant_memberships m
JOIN tenants t ON t.id=m.tenant_id AND t.status='active'
JOIN oidc_identities i ON i.id=m.actor_id AND i.status='active'
WHERE m.actor_id=$1 AND m.actor_type='user' AND m.status='active'`
	args := []any{actorID}
	if requestedTenant != "" {
		query += ` AND m.tenant_id=$2`
		args = append(args, requestedTenant)
	}
	query += ` ORDER BY m.tenant_id LIMIT 2`
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return Principal{}, fmt.Errorf("resolve OIDC membership: %w", err)
	}
	principals, err := pgx.CollectRows(rows, pgx.RowToStructByPos[Principal])
	if err != nil {
		return Principal{}, fmt.Errorf("scan OIDC membership: %w", err)
	}
	if len(principals) != 1 {
		return Principal{}, ErrMembershipNotFound
	}
	if err = tx.Commit(ctx); err != nil {
		return Principal{}, fmt.Errorf("commit OIDC identity resolution: %w", err)
	}
	return principals[0], nil
}

// CreateSession stores only a session digest and returns its non-secret identifier.
func (s *PostgresStore) CreateSession(ctx context.Context, principal Principal, digest [sha256.Size]byte, expiresAt, now time.Time) (string, error) {
	id := "session_" + uuid.NewString()
	result, err := s.pool.Exec(ctx, `
INSERT INTO identity_sessions (id,session_hash,tenant_id,actor_id,expires_at,created_at)
SELECT $1,$2,m.tenant_id,m.actor_id,$5,$6
FROM tenant_memberships m JOIN tenants t ON t.id=m.tenant_id AND t.status='active'
WHERE m.tenant_id=$3 AND m.actor_id=$4 AND m.actor_type='user' AND m.status='active'`,
		id, digest[:], principal.TenantID, principal.ActorID, expiresAt, now)
	if err != nil {
		return "", fmt.Errorf("create identity session: %w", err)
	}
	if result.RowsAffected() != 1 {
		return "", ErrMembershipNotFound
	}
	return id, nil
}

// RevokeSession revokes the matching bearer session without exposing its identifier.
func (s *PostgresStore) RevokeSession(ctx context.Context, digest [sha256.Size]byte, now time.Time) error {
	_, err := s.pool.Exec(ctx, `UPDATE identity_sessions SET revoked_at=COALESCE(revoked_at,$2) WHERE session_hash=$1`, digest[:], now)
	if err != nil {
		return fmt.Errorf("revoke identity session: %w", err)
	}
	return nil
}

func scanPrincipal(row pgx.Row) (Principal, error) {
	var principal Principal
	if err := row.Scan(&principal.TenantID, &principal.ActorID, &principal.Role); errors.Is(err, pgx.ErrNoRows) {
		return Principal{}, ErrUnauthenticated
	} else if err != nil {
		return Principal{}, fmt.Errorf("resolve identity principal: %w", err)
	}
	return principal, nil
}
