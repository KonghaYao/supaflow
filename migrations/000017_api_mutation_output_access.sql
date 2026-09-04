CREATE TABLE api_mutation_ledger (
    operation_id text PRIMARY KEY,
    tenant_id text NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    action text NOT NULL,
    idempotency_key text NOT NULL CHECK (char_length(idempotency_key) BETWEEN 8 AND 128),
    request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32),
    resource_type text NOT NULL,
    resource_id text NOT NULL,
    response_snapshot jsonb NOT NULL DEFAULT '{}',
    completed_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, action, idempotency_key),
    UNIQUE (tenant_id, operation_id)
);

ALTER TABLE audit_records ADD COLUMN operation_id text;
CREATE UNIQUE INDEX audit_records_operation_idx
    ON audit_records (tenant_id, operation_id)
    WHERE operation_id IS NOT NULL;
