CREATE TABLE ops_mutation_ledger (
    tenant_id text NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    operation text NOT NULL CHECK (char_length(operation) BETWEEN 1 AND 100),
    idempotency_key text NOT NULL CHECK (char_length(idempotency_key) BETWEEN 8 AND 128),
    request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32),
    status text NOT NULL CHECK (status IN ('pending', 'completed')),
    response_status integer CHECK (response_status BETWEEN 100 AND 599),
    response_content_type text,
    response_body bytea,
    created_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    PRIMARY KEY (tenant_id, operation, idempotency_key),
    CHECK ((status = 'completed') = (response_status IS NOT NULL AND response_body IS NOT NULL AND completed_at IS NOT NULL))
);

INSERT INTO retention_policies (tenant_id,event_days,output_days,archive_before_delete)
SELECT id,30,30,true FROM tenants
ON CONFLICT (tenant_id) DO NOTHING;

UPDATE managed_objects output
SET retention_until = output.created_at + policy.output_days * interval '1 day'
FROM retention_policies policy
WHERE output.tenant_id = policy.tenant_id
  AND output.kind = 'output'
  AND output.retention_until IS NULL;
