-- Durable fan-in barrier. The join row is the serialization point; edge
-- contributions are immutable and deduplicated across retry/takeover.
ALTER TABLE interrupts ADD COLUMN auto_resume boolean NOT NULL DEFAULT false;

CREATE TABLE dag_joins (
    tenant_id text NOT NULL,
    run_id text NOT NULL,
    node_id text NOT NULL,
    executor text NOT NULL,
    required_edges jsonb NOT NULL CHECK (jsonb_typeof(required_edges) = 'array'),
    scheduled boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, run_id, node_id),
    FOREIGN KEY (tenant_id, run_id) REFERENCES runs(tenant_id, id) ON DELETE CASCADE
);

CREATE TABLE dag_edge_inputs (
    tenant_id text NOT NULL,
    run_id text NOT NULL,
    node_id text NOT NULL,
    edge_id text NOT NULL,
    source_node_id text NOT NULL,
    source_port text NOT NULL,
    target_port text NOT NULL,
    value jsonb NOT NULL,
    available_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, run_id, node_id, edge_id),
    FOREIGN KEY (tenant_id, run_id, node_id)
        REFERENCES dag_joins(tenant_id, run_id, node_id) ON DELETE CASCADE
);

-- Parent lineage is immutable and tenant scoped. A unique parent attempt makes
-- child creation idempotent under worker takeover.
CREATE TABLE subworkflow_links (
    tenant_id text NOT NULL,
    parent_run_id text NOT NULL,
    parent_node_id text NOT NULL,
    parent_attempt_id text NOT NULL,
    child_run_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, parent_attempt_id),
    UNIQUE (tenant_id, child_run_id),
    FOREIGN KEY (tenant_id, parent_run_id) REFERENCES runs(tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, child_run_id) REFERENCES runs(tenant_id, id) ON DELETE RESTRICT
);
