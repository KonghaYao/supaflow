-- Reruns are new runs with immutable tenant-scoped ancestry. A row exists only
-- for reruns; NULL from_node_id denotes a full rerun from the plan entry.
CREATE TABLE run_rerun_lineage (
    tenant_id text NOT NULL,
    run_id text NOT NULL,
    source_run_id text NOT NULL,
    from_node_id text,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, run_id),
    FOREIGN KEY (tenant_id, run_id) REFERENCES runs(tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, source_run_id) REFERENCES runs(tenant_id, id) ON DELETE RESTRICT,
    CHECK (run_id <> source_run_id),
    CHECK (from_node_id IS NULL OR length(from_node_id) > 0)
);
CREATE INDEX run_rerun_lineage_source_idx
    ON run_rerun_lineage (tenant_id, source_run_id, created_at, run_id);
