ALTER TABLE ops_mutation_ledger
	ADD COLUMN operation_id text,
    ADD COLUMN lease_owner text,
    ADD COLUMN lease_expires_at timestamptz,
    ADD COLUMN attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    ADD COLUMN updated_at timestamptz NOT NULL DEFAULT now();

UPDATE ops_mutation_ledger
SET operation_id = 'legacy-' || encode(request_hash, 'hex'),
	lease_owner = 'legacy-pending',
    lease_expires_at = CASE WHEN status = 'pending' THEN now() ELSE NULL END,
    attempt_count = CASE WHEN status = 'pending' THEN 1 ELSE 0 END;

ALTER TABLE ops_mutation_ledger
	ALTER COLUMN operation_id SET NOT NULL,
    ALTER COLUMN lease_owner SET NOT NULL,
    ADD CONSTRAINT ops_mutation_pending_lease CHECK (
        (status = 'pending' AND lease_expires_at IS NOT NULL)
        OR (status = 'completed' AND lease_expires_at IS NULL)
    );

CREATE INDEX ops_mutation_takeover_idx
    ON ops_mutation_ledger (lease_expires_at, tenant_id, operation, idempotency_key)
    WHERE status = 'pending';

ALTER TABLE node_outputs
    ADD COLUMN retention_until timestamptz,
    ADD COLUMN retention_status text NOT NULL DEFAULT 'active'
        CHECK (retention_status IN ('active', 'archived', 'deleted')),
    ADD COLUMN archived_at timestamptz,
    ADD COLUMN deleted_at timestamptz;

ALTER TABLE node_outputs DROP CONSTRAINT node_outputs_check;
ALTER TABLE node_outputs ADD CONSTRAINT node_outputs_payload_retention_check CHECK (
    (retention_status = 'deleted' AND inline_value IS NULL AND object_ref IS NULL)
    OR (retention_status <> 'deleted' AND ((inline_value IS NULL) <> (object_ref IS NULL)))
);

UPDATE node_outputs output
SET retention_until = output.created_at + policy.output_days * interval '1 day'
FROM retention_policies policy
WHERE output.tenant_id = policy.tenant_id
  AND output.retention_until IS NULL;

CREATE INDEX node_outputs_retention_idx
    ON node_outputs (retention_until, tenant_id, id)
    WHERE retention_status = 'active' AND retention_until IS NOT NULL;
