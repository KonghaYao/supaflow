CREATE TABLE credential_refs (
    tenant_id text NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    id text NOT NULL,
    name text NOT NULL,
    provider text NOT NULL,
    purpose text NOT NULL,
    status text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'revoked')),
    current_version integer NOT NULL DEFAULT 1 CHECK (current_version > 0),
    created_by text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    revoked_at timestamptz,
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, name),
    CHECK ((status = 'revoked') = (revoked_at IS NOT NULL))
);

CREATE TABLE credential_versions (
    tenant_id text NOT NULL,
    credential_id text NOT NULL,
    version integer NOT NULL CHECK (version > 0),
    key_id text NOT NULL,
    wrapped_data_key bytea NOT NULL,
    nonce bytea NOT NULL CHECK (octet_length(nonce) = 12),
    ciphertext bytea NOT NULL,
    created_by text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    retired_at timestamptz,
    PRIMARY KEY (tenant_id, credential_id, version),
    FOREIGN KEY (tenant_id, credential_id) REFERENCES credential_refs(tenant_id, id) ON DELETE CASCADE
);

CREATE TABLE attempt_credential_grants (
    tenant_id text NOT NULL,
    attempt_id text NOT NULL,
    credential_id text NOT NULL,
    granted_by text NOT NULL,
    expires_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, attempt_id, credential_id),
    FOREIGN KEY (tenant_id, attempt_id) REFERENCES node_attempts(tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, credential_id) REFERENCES credential_refs(tenant_id, id) ON DELETE CASCADE
);

CREATE TABLE triggers (
    tenant_id text NOT NULL,
    id text NOT NULL,
    public_hash text NOT NULL CHECK (char_length(public_hash) BETWEEN 16 AND 128),
    workflow_id text NOT NULL,
    kind text NOT NULL CHECK (kind IN ('api', 'manual', 'schedule', 'webhook', 'event')),
    name text NOT NULL,
    status text NOT NULL DEFAULT 'enabled' CHECK (status IN ('enabled', 'disabled')),
    config jsonb NOT NULL DEFAULT '{}',
    schedule_interval_seconds integer CHECK (schedule_interval_seconds > 0),
    misfire_policy text CHECK (misfire_policy IN ('skip', 'fire_once', 'catch_up')),
    next_fire_at timestamptz,
    signing_credential_id text,
    event_type text,
    created_by text NOT NULL,
    source_system text,
    source_import_hash bytea CHECK (source_import_hash IS NULL OR octet_length(source_import_hash) = 32),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, id),
    UNIQUE (public_hash),
    UNIQUE (tenant_id, workflow_id, name),
    FOREIGN KEY (tenant_id, workflow_id) REFERENCES workflows(tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, signing_credential_id) REFERENCES credential_refs(tenant_id, id) ON DELETE RESTRICT,
    CHECK ((kind = 'schedule') = (schedule_interval_seconds IS NOT NULL AND misfire_policy IS NOT NULL AND next_fire_at IS NOT NULL)),
    CHECK (kind <> 'webhook' OR signing_credential_id IS NOT NULL),
    CHECK (kind <> 'event' OR event_type IS NOT NULL)
);
CREATE INDEX triggers_schedule_due_idx ON triggers (next_fire_at, tenant_id, id)
    WHERE status = 'enabled' AND kind = 'schedule';
CREATE INDEX triggers_event_idx ON triggers (tenant_id, event_type, id)
    WHERE status = 'enabled' AND kind = 'event';

CREATE TABLE trigger_firings (
    tenant_id text NOT NULL,
    id text NOT NULL,
    trigger_id text NOT NULL,
    deduplication_key text NOT NULL CHECK (char_length(deduplication_key) BETWEEN 1 AND 255),
    source_time timestamptz,
    payload jsonb NOT NULL DEFAULT '{}',
    status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'dispatched', 'failed')),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, trigger_id, deduplication_key),
    FOREIGN KEY (tenant_id, trigger_id) REFERENCES triggers(tenant_id, id) ON DELETE CASCADE
);

CREATE TABLE webhook_replays (
    tenant_id text NOT NULL,
    trigger_id text NOT NULL,
    request_id text NOT NULL,
    request_digest bytea NOT NULL CHECK (octet_length(request_digest) = 32),
    expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, trigger_id, request_id),
    FOREIGN KEY (tenant_id, trigger_id) REFERENCES triggers(tenant_id, id) ON DELETE CASCADE
);
CREATE INDEX webhook_replays_expiry_idx ON webhook_replays (expires_at);

CREATE TABLE tenant_quotas (
    tenant_id text PRIMARY KEY REFERENCES tenants(id) ON DELETE CASCADE,
    max_active_runs integer NOT NULL CHECK (max_active_runs > 0),
    max_active_workers integer NOT NULL CHECK (max_active_workers > 0),
    max_queue_depth integer NOT NULL CHECK (max_queue_depth >= 0),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE tenant_quota_counters (
    tenant_id text PRIMARY KEY REFERENCES tenant_quotas(tenant_id) ON DELETE CASCADE,
    active_runs integer NOT NULL DEFAULT 0 CHECK (active_runs >= 0),
    active_workers integer NOT NULL DEFAULT 0 CHECK (active_workers >= 0),
    queued_runs integer NOT NULL DEFAULT 0 CHECK (queued_runs >= 0),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE admission_entries (
    tenant_id text NOT NULL,
    id text NOT NULL,
    resource_type text NOT NULL CHECK (resource_type IN ('run', 'worker')),
    resource_id text NOT NULL,
    priority integer NOT NULL DEFAULT 0 CHECK (priority BETWEEN -1000 AND 1000),
    status text NOT NULL CHECK (status IN ('active', 'queued', 'released')),
    admitted_at timestamptz,
    released_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, resource_type, resource_id),
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
);
CREATE INDEX admission_queue_idx ON admission_entries (tenant_id, resource_type, priority DESC, created_at, id)
    WHERE status = 'queued';

CREATE TABLE outbox_dead_letters (
    tenant_id text NOT NULL,
    id text NOT NULL,
    original_message_id text NOT NULL,
    aggregate_type text NOT NULL,
    aggregate_id text NOT NULL,
    topic text NOT NULL,
    payload jsonb NOT NULL,
    attempts integer NOT NULL CHECK (attempts > 0),
    last_error text NOT NULL,
    failed_at timestamptz NOT NULL DEFAULT now(),
    requeued_at timestamptz,
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, original_message_id),
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
);

CREATE TABLE managed_objects (
    tenant_id text NOT NULL,
    id text NOT NULL,
    run_id text,
    kind text NOT NULL CHECK (kind IN ('output', 'archive', 'tenant_export', 'backup')),
    object_key text NOT NULL,
    content_type text NOT NULL,
    size_bytes bigint NOT NULL CHECK (size_bytes >= 0),
    content_hash bytea NOT NULL CHECK (octet_length(content_hash) = 32),
    retention_until timestamptz,
    status text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'archived', 'deleted')),
    created_at timestamptz NOT NULL DEFAULT now(),
    archived_at timestamptz,
    deleted_at timestamptz,
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, object_key),
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, run_id) REFERENCES runs(tenant_id, id) ON DELETE CASCADE
);
CREATE INDEX managed_objects_retention_idx ON managed_objects (retention_until, tenant_id, id)
    WHERE status = 'active' AND retention_until IS NOT NULL;

CREATE TABLE retention_policies (
    tenant_id text PRIMARY KEY REFERENCES tenants(id) ON DELETE CASCADE,
    event_days integer NOT NULL CHECK (event_days > 0),
    output_days integer NOT NULL CHECK (output_days > 0),
    archive_before_delete boolean NOT NULL DEFAULT true,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE tenant_exports (
    tenant_id text NOT NULL,
    id text NOT NULL,
    requested_by text NOT NULL,
    status text NOT NULL CHECK (status IN ('pending', 'ready', 'failed', 'expired')),
    object_id text,
    expires_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    PRIMARY KEY (tenant_id, id),
    FOREIGN KEY (tenant_id, object_id) REFERENCES managed_objects(tenant_id, id) ON DELETE RESTRICT
);

CREATE TABLE worker_registrations (
    tenant_id text NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    worker_id text NOT NULL,
    status text NOT NULL CHECK (status IN ('ready', 'draining', 'offline')),
    capacity integer NOT NULL CHECK (capacity > 0),
    active_attempts integer NOT NULL DEFAULT 0 CHECK (active_attempts >= 0),
    heartbeat_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, worker_id)
);

ALTER TABLE outbox_messages
    ADD COLUMN max_attempts integer NOT NULL DEFAULT 8 CHECK (max_attempts > 0);
