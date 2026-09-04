CREATE TABLE tenants (
    id text PRIMARY KEY,
    name text NOT NULL,
    status text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'suspended', 'deleted')),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE api_keys (
    id text PRIMARY KEY,
    tenant_id text NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name text NOT NULL,
    key_hash bytea NOT NULL CHECK (octet_length(key_hash) = 32),
    key_prefix text NOT NULL,
    scopes text[] NOT NULL DEFAULT '{}',
    expires_at timestamptz,
    revoked_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    last_used_at timestamptz,
    UNIQUE (tenant_id, id),
    UNIQUE (key_hash),
    UNIQUE (tenant_id, name)
);
CREATE INDEX api_keys_tenant_active_idx ON api_keys (tenant_id, key_prefix)
    WHERE revoked_at IS NULL;

CREATE TABLE workflows (
    id text NOT NULL,
    tenant_id text NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    slug text NOT NULL,
    name text NOT NULL,
    description text NOT NULL DEFAULT '',
    status text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'archived')),
    created_by text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, slug)
);

CREATE TABLE workflow_drafts (
    id text PRIMARY KEY,
    tenant_id text NOT NULL,
    workflow_id text NOT NULL,
    revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    document jsonb NOT NULL,
    updated_by text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, workflow_id),
    FOREIGN KEY (tenant_id, workflow_id) REFERENCES workflows(tenant_id, id) ON DELETE CASCADE
);

CREATE TABLE workflow_versions (
    id text PRIMARY KEY,
    tenant_id text NOT NULL,
    workflow_id text NOT NULL,
    version integer NOT NULL CHECK (version > 0),
    publication_key text NOT NULL CHECK (length(publication_key) BETWEEN 1 AND 255),
    definition jsonb NOT NULL,
    definition_hash bytea NOT NULL CHECK (octet_length(definition_hash) = 32),
    published_by text NOT NULL,
    published_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, workflow_id, id),
    UNIQUE (tenant_id, workflow_id, version),
    UNIQUE (tenant_id, publication_key),
    FOREIGN KEY (tenant_id, workflow_id) REFERENCES workflows(tenant_id, id) ON DELETE RESTRICT
);
CREATE INDEX workflow_versions_definition_hash_idx
    ON workflow_versions (tenant_id, workflow_id, definition_hash);

CREATE TABLE execution_plans (
    id text PRIMARY KEY,
    tenant_id text NOT NULL,
    workflow_id text NOT NULL,
    workflow_version_id text NOT NULL,
    plan jsonb NOT NULL,
    plan_hash bytea NOT NULL CHECK (octet_length(plan_hash) = 32),
    compiler_version text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, workflow_id, id),
    UNIQUE (tenant_id, workflow_version_id),
    FOREIGN KEY (tenant_id, workflow_id) REFERENCES workflows(tenant_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, workflow_id, workflow_version_id)
        REFERENCES workflow_versions(tenant_id, workflow_id, id) ON DELETE RESTRICT
);
CREATE INDEX execution_plans_hash_idx
    ON execution_plans (tenant_id, workflow_id, plan_hash);

-- Published versions and plans are append-only. Mutability is rejected at the
-- database boundary so a future application bug cannot alter a running plan.
CREATE FUNCTION reject_immutable_workflow_record() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'published workflow records are immutable' USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER workflow_versions_immutable
BEFORE UPDATE OR DELETE ON workflow_versions
FOR EACH ROW EXECUTE FUNCTION reject_immutable_workflow_record();

CREATE TRIGGER execution_plans_immutable
BEFORE UPDATE OR DELETE ON execution_plans
FOR EACH ROW EXECUTE FUNCTION reject_immutable_workflow_record();
