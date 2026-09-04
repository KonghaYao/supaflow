CREATE TABLE interrupts (
    id text PRIMARY KEY,
    tenant_id text NOT NULL,
    run_id text NOT NULL,
    node_state_id text NOT NULL,
    attempt_id text NOT NULL,
    kind text NOT NULL CHECK (kind IN ('approval', 'wait', 'webhook')),
    status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'resolved', 'expired', 'cancelled')),
    prompt jsonb NOT NULL,
    resume_token_hash bytea NOT NULL CHECK (octet_length(resume_token_hash) = 32),
    expires_at timestamptz,
    resolved_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, id),
    UNIQUE (resume_token_hash),
    FOREIGN KEY (tenant_id, run_id) REFERENCES runs(tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, run_id, node_state_id)
        REFERENCES node_states(tenant_id, run_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, run_id, attempt_id)
        REFERENCES node_attempts(tenant_id, run_id, id) ON DELETE CASCADE
);
CREATE INDEX interrupts_pending_idx ON interrupts (tenant_id, run_id, created_at)
    WHERE status = 'pending';

CREATE TABLE approval_decisions (
    id text PRIMARY KEY,
    tenant_id text NOT NULL,
    interrupt_id text NOT NULL,
    decision text NOT NULL CHECK (decision IN ('approve', 'reject')),
    decision_data jsonb NOT NULL DEFAULT '{}',
    decided_by text NOT NULL,
    comment text NOT NULL DEFAULT '',
    decided_at timestamptz NOT NULL DEFAULT now(),
    idempotency_key text NOT NULL CHECK (char_length(idempotency_key) BETWEEN 8 AND 128),
    request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32),
    response_snapshot jsonb NOT NULL CHECK (jsonb_typeof(response_snapshot) = 'object'),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, interrupt_id),
    UNIQUE (tenant_id, idempotency_key),
    FOREIGN KEY (tenant_id, interrupt_id) REFERENCES interrupts(tenant_id, id) ON DELETE CASCADE
);

CREATE TABLE audit_records (
    id text PRIMARY KEY,
    tenant_id text NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    actor_id text NOT NULL,
    action text NOT NULL,
    resource_type text NOT NULL,
    resource_id text NOT NULL,
    outcome text NOT NULL CHECK (outcome IN ('allowed', 'denied', 'failed')),
    metadata jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, id)
);
CREATE INDEX audit_records_resource_idx
    ON audit_records (tenant_id, resource_type, resource_id, created_at DESC);

CREATE TABLE outbox_messages (
    id text PRIMARY KEY,
    tenant_id text NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    aggregate_type text NOT NULL,
    aggregate_id text NOT NULL,
    topic text NOT NULL,
    payload jsonb NOT NULL,
    available_at timestamptz NOT NULL DEFAULT now(),
    claimed_by text,
    claim_expires_at timestamptz,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    delivered_at timestamptz,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, id),
    CHECK ((claimed_by IS NULL) = (claim_expires_at IS NULL))
);
CREATE INDEX outbox_claim_idx ON outbox_messages (available_at, created_at, id)
    WHERE delivered_at IS NULL;

-- Append-only audit records provide evidence even when application code is wrong.
CREATE FUNCTION reject_audit_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'audit records are append-only' USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER audit_records_append_only
BEFORE UPDATE OR DELETE ON audit_records
FOR EACH ROW EXECUTE FUNCTION reject_audit_mutation();
