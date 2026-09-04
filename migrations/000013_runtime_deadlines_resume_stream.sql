-- Run deadlines are absolute durable timestamps. NULL preserves explicitly
-- unbounded runs; API callers may only set bounded durations.
ALTER TABLE runs ADD COLUMN deadline_at timestamptz;
CREATE INDEX runs_deadline_due_idx
    ON runs (tenant_id, deadline_at, id)
    WHERE deadline_at IS NOT NULL AND status IN ('pending','running','suspended','cancelling');

-- Public webhook resumes use a separate idempotency ledger from human
-- approvals so arbitrary callback data can never be interpreted as a decision.
CREATE TABLE webhook_resumptions (
    id text NOT NULL,
    tenant_id text NOT NULL,
    interrupt_id text NOT NULL,
    idempotency_key text NOT NULL,
    request_hash bytea NOT NULL,
    resume_data jsonb NOT NULL,
    response_snapshot jsonb NOT NULL,
    created_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, idempotency_key),
    UNIQUE (tenant_id, interrupt_id),
    FOREIGN KEY (tenant_id, interrupt_id) REFERENCES interrupts(tenant_id, id) ON DELETE CASCADE,
    CHECK (octet_length(request_hash) = 32),
    CHECK (length(idempotency_key) BETWEEN 8 AND 128)
);

-- This row bounds streamed deltas without retaining a second output copy.
-- Individual durable chunks live in run_events and their matching outbox rows.
CREATE TABLE attempt_stream_state (
    tenant_id text NOT NULL,
    attempt_id text NOT NULL,
    run_id text NOT NULL,
    chunk_count integer NOT NULL DEFAULT 0,
    total_bytes bigint NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, attempt_id),
    FOREIGN KEY (tenant_id, attempt_id) REFERENCES node_attempts(tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, run_id) REFERENCES runs(tenant_id, id) ON DELETE CASCADE,
    CHECK (chunk_count BETWEEN 0 AND 4096),
    CHECK (total_bytes BETWEEN 0 AND 1048576)
);

-- The provider-visible sequence is the idempotency identity. Persisting its
-- digest lets a new fenced worker replay a stream from chunk one without
-- duplicating SSE/outbox records, while divergent replay fails closed.
CREATE TABLE attempt_stream_chunks (
    tenant_id text NOT NULL,
    attempt_id text NOT NULL,
    run_id text NOT NULL,
    chunk_sequence integer NOT NULL,
    content_hash bytea NOT NULL,
    event_sequence bigint NOT NULL,
    created_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, attempt_id, chunk_sequence),
    FOREIGN KEY (tenant_id, attempt_id) REFERENCES node_attempts(tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, run_id, event_sequence)
        REFERENCES run_events(tenant_id, run_id, sequence) ON DELETE CASCADE,
    CHECK (chunk_sequence BETWEEN 1 AND 4096),
    CHECK (octet_length(content_hash) = 32),
    CHECK (event_sequence > 0)
);
