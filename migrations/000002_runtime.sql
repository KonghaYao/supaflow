CREATE TABLE runs (
    id text PRIMARY KEY,
    tenant_id text NOT NULL,
    workflow_id text NOT NULL,
    workflow_version_id text NOT NULL,
    execution_plan_id text NOT NULL,
    idempotency_key text NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 255),
    request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32),
    input jsonb NOT NULL,
    plan_hash text NOT NULL,
    plan_entry_node_id text NOT NULL DEFAULT '',
    plan_executor text NOT NULL DEFAULT '',
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'running', 'suspended', 'cancelling', 'success', 'failed', 'error', 'cancelled')),
    next_event_sequence bigint NOT NULL DEFAULT 0 CHECK (next_event_sequence >= 0),
    cancel_requested_at timestamptz,
    cancel_reason text,
    started_at timestamptz,
    finished_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, idempotency_key),
    FOREIGN KEY (tenant_id, workflow_id) REFERENCES workflows(tenant_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, workflow_id, workflow_version_id)
        REFERENCES workflow_versions(tenant_id, workflow_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, workflow_id, execution_plan_id)
        REFERENCES execution_plans(tenant_id, workflow_id, id) ON DELETE RESTRICT
);
CREATE INDEX runs_tenant_status_created_idx ON runs (tenant_id, status, created_at, id);

CREATE TABLE node_states (
    id text PRIMARY KEY,
    tenant_id text NOT NULL,
    run_id text NOT NULL,
    node_id text NOT NULL,
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'ready', 'running', 'suspended', 'success', 'failed', 'error', 'cancelled', 'skipped')),
    output_id text,
    last_error jsonb,
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    latest_attempt_id text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, run_id, id),
    UNIQUE (tenant_id, run_id, node_id),
    FOREIGN KEY (tenant_id, run_id) REFERENCES runs(tenant_id, id) ON DELETE CASCADE
);

CREATE TABLE node_attempts (
    id text PRIMARY KEY,
    tenant_id text NOT NULL,
    run_id text NOT NULL,
    node_state_id text NOT NULL,
    node_id text NOT NULL,
    executor text NOT NULL,
    input jsonb NOT NULL,
    resume_data jsonb,
    attempt_number integer NOT NULL CHECK (attempt_number > 0),
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'retry_ready', 'running', 'suspended', 'succeeded', 'failed', 'error', 'cancelled', 'timed_out')),
    available_at timestamptz NOT NULL DEFAULT now(),
    worker_id text,
    fencing_token bigint NOT NULL DEFAULT 0 CHECK (fencing_token >= 0),
    started_at timestamptz,
    finished_at timestamptz,
    error jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, run_id, id),
    UNIQUE (tenant_id, node_state_id, attempt_number),
    FOREIGN KEY (tenant_id, run_id) REFERENCES runs(tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, run_id, node_state_id)
        REFERENCES node_states(tenant_id, run_id, id) ON DELETE CASCADE
);
CREATE INDEX node_attempts_claim_idx ON node_attempts (available_at, created_at, id)
    WHERE status IN ('pending', 'retry_ready', 'running');

CREATE TABLE worker_leases (
    attempt_id text PRIMARY KEY,
    tenant_id text NOT NULL,
    run_id text NOT NULL,
    worker_id text NOT NULL,
    fencing_token bigint NOT NULL CHECK (fencing_token > 0),
    acquired_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL,
    FOREIGN KEY (tenant_id, run_id, attempt_id)
        REFERENCES node_attempts(tenant_id, run_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, run_id) REFERENCES runs(tenant_id, id) ON DELETE CASCADE,
    CHECK (expires_at > acquired_at)
);
CREATE INDEX worker_leases_expiry_idx ON worker_leases (expires_at, attempt_id);

CREATE TABLE run_events (
    id text PRIMARY KEY,
    tenant_id text NOT NULL,
    run_id text NOT NULL,
    sequence bigint NOT NULL CHECK (sequence > 0),
    event_type text NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, run_id, sequence),
    FOREIGN KEY (tenant_id, run_id) REFERENCES runs(tenant_id, id) ON DELETE CASCADE
);
CREATE INDEX run_events_cursor_idx ON run_events (tenant_id, run_id, sequence);

CREATE TABLE run_snapshots (
    id text PRIMARY KEY,
    tenant_id text NOT NULL,
    run_id text NOT NULL,
    attempt_id text NOT NULL,
    sequence bigint NOT NULL CHECK (sequence >= 0),
    state jsonb NOT NULL,
    state_hash bytea NOT NULL CHECK (octet_length(state_hash) = 32),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, run_id, sequence),
    FOREIGN KEY (tenant_id, run_id) REFERENCES runs(tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, run_id, attempt_id)
        REFERENCES node_attempts(tenant_id, run_id, id) ON DELETE CASCADE
);
CREATE INDEX run_snapshots_latest_idx ON run_snapshots (tenant_id, run_id, sequence DESC);

CREATE TABLE node_outputs (
    id text PRIMARY KEY,
    tenant_id text NOT NULL,
    run_id text NOT NULL,
    node_state_id text NOT NULL,
    attempt_id text NOT NULL,
    content_type text NOT NULL,
    inline_value jsonb,
    object_ref text,
    size_bytes bigint NOT NULL CHECK (size_bytes >= 0),
    content_hash bytea NOT NULL CHECK (octet_length(content_hash) = 32),
    sensitivity text NOT NULL DEFAULT 'internal'
        CHECK (sensitivity IN ('public', 'internal', 'sensitive')),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, attempt_id),
    FOREIGN KEY (tenant_id, run_id) REFERENCES runs(tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, run_id, node_state_id)
        REFERENCES node_states(tenant_id, run_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, run_id, attempt_id)
        REFERENCES node_attempts(tenant_id, run_id, id) ON DELETE CASCADE,
    CHECK ((inline_value IS NULL) <> (object_ref IS NULL))
);
