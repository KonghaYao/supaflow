CREATE TABLE object_upload_intents (
    tenant_id text NOT NULL,
    id text NOT NULL,
    run_id text,
    kind text NOT NULL CHECK (kind IN ('output', 'archive', 'tenant_export', 'backup')),
    object_key text NOT NULL,
    content_type text NOT NULL,
    retention_until timestamptz,
    owner_token text NOT NULL,
    lease_expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, object_key),
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, run_id) REFERENCES runs(tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX object_upload_intents_recovery_idx
    ON object_upload_intents (tenant_id, lease_expires_at, id);

ALTER TABLE managed_objects DROP CONSTRAINT managed_objects_status_check;
ALTER TABLE managed_objects ADD CONSTRAINT managed_objects_status_check
    CHECK (status IN ('active', 'archived', 'deleting', 'deleted'));

CREATE INDEX managed_objects_deleting_idx
    ON managed_objects (tenant_id, id) WHERE status = 'deleting';
