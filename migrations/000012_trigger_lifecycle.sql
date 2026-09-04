-- Trigger lifecycle mutations are claimed before their domain write in the
-- same transaction. Replays return the persisted safe result while a reused
-- key with a different request hash is rejected.
CREATE TABLE trigger_mutations (
    tenant_id text NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    idempotency_key text NOT NULL CHECK (length(idempotency_key) BETWEEN 8 AND 128),
    operation text NOT NULL CHECK (operation IN ('create', 'update', 'state', 'delete')),
    request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32),
    trigger_id text NOT NULL DEFAULT '',
    result jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, idempotency_key)
);

CREATE INDEX trigger_mutations_created_idx ON trigger_mutations (tenant_id, created_at, idempotency_key);
