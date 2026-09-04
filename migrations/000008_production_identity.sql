CREATE TABLE service_accounts (
    tenant_id text NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    id text NOT NULL,
    name text NOT NULL,
    status text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, name)
);

CREATE TABLE oidc_identities (
    id text PRIMARY KEY,
    issuer text NOT NULL,
    subject text NOT NULL,
    email text,
    display_name text,
    status text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (issuer, subject)
);

CREATE TABLE tenant_memberships (
    tenant_id text NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    actor_id text NOT NULL,
    actor_type text NOT NULL CHECK (actor_type IN ('user', 'service_account')),
    role text NOT NULL CHECK (role IN ('viewer', 'editor', 'publisher', 'operator', 'approver', 'admin')),
    status text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'revoked')),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, actor_id)
);
CREATE INDEX tenant_memberships_actor_active_idx
    ON tenant_memberships (actor_id, tenant_id) WHERE status = 'active';

ALTER TABLE api_keys ADD COLUMN service_account_id text;
ALTER TABLE api_keys ADD CONSTRAINT api_keys_service_account_fk
    FOREIGN KEY (tenant_id, service_account_id)
    REFERENCES service_accounts(tenant_id, id) ON DELETE CASCADE;
CREATE INDEX api_keys_service_account_active_idx
    ON api_keys (tenant_id, service_account_id) WHERE revoked_at IS NULL;

CREATE TABLE identity_sessions (
    id text PRIMARY KEY,
    session_hash bytea NOT NULL UNIQUE CHECK (octet_length(session_hash) = 32),
    tenant_id text NOT NULL,
    actor_id text NOT NULL,
    expires_at timestamptz NOT NULL,
    revoked_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz,
    FOREIGN KEY (tenant_id, actor_id)
        REFERENCES tenant_memberships(tenant_id, actor_id) ON DELETE CASCADE
);
CREATE INDEX identity_sessions_actor_active_idx
    ON identity_sessions (tenant_id, actor_id, expires_at) WHERE revoked_at IS NULL;

CREATE TABLE oidc_login_attempts (
    state_hash bytea PRIMARY KEY CHECK (octet_length(state_hash) = 32),
    browser_hash bytea NOT NULL CHECK (octet_length(browser_hash) = 32),
    nonce_hash bytea NOT NULL CHECK (octet_length(nonce_hash) = 32),
    pkce_verifier text NOT NULL CHECK (length(pkce_verifier) BETWEEN 43 AND 128),
    requested_tenant_id text REFERENCES tenants(id) ON DELETE CASCADE,
    expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX oidc_login_attempts_expiry_idx ON oidc_login_attempts (expires_at);
