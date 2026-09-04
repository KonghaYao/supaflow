ALTER TABLE runs
    ADD COLUMN priority integer NOT NULL DEFAULT 0
        CHECK (priority BETWEEN -1000 AND 1000);

-- Existing tenants get conservative bootstrap limits. Operator-configured
-- values win because the runtime only inserts missing rows.
INSERT INTO tenant_quotas (tenant_id, max_active_runs, max_active_workers, max_queue_depth)
SELECT id, 100, 50, 1000 FROM tenants
ON CONFLICT (tenant_id) DO NOTHING;

INSERT INTO tenant_quota_counters (tenant_id)
SELECT id FROM tenants
ON CONFLICT (tenant_id) DO NOTHING;

-- Reconcile manually-created entries before enforcing the runtime invariant.
UPDATE admission_entries a
SET status = 'released', released_at = COALESCE(a.released_at, now())
FROM runs r
WHERE a.tenant_id = r.tenant_id
  AND a.resource_type = 'run'
  AND a.resource_id = r.id
  AND a.status IN ('active', 'queued')
  AND r.status IN ('success', 'failed', 'error', 'cancelled');

-- Deploying admission must not strand work accepted by an older binary.
INSERT INTO admission_entries (
    tenant_id, id, resource_type, resource_id, priority, status, admitted_at, created_at
)
SELECT r.tenant_id,
       'adm_migrated_' || md5(length(r.tenant_id)::text || ':' || r.tenant_id || r.id),
       'run', r.id, r.priority, 'active', now(), r.created_at
FROM runs r
WHERE r.status NOT IN ('success', 'failed', 'error', 'cancelled')
ON CONFLICT (tenant_id, resource_type, resource_id) DO NOTHING;

-- Preserve all previously accepted work by raising an undersized configured
-- active limit during the one-time cutover. Operators may lower it after the
-- backlog drains; new creates still obey the configured queue bound.
WITH active_counts AS (
    SELECT tenant_id, count(*)::integer AS total
    FROM admission_entries
    WHERE resource_type = 'run' AND status = 'active'
    GROUP BY tenant_id
)
UPDATE tenant_quotas q
SET max_active_runs = GREATEST(q.max_active_runs, c.total), updated_at = now()
FROM active_counts c
WHERE q.tenant_id = c.tenant_id;

WITH counts AS (
    SELECT tenant_id,
           count(*) FILTER (WHERE resource_type = 'run' AND status = 'active')::integer AS active_runs,
           count(*) FILTER (WHERE resource_type = 'run' AND status = 'queued')::integer AS queued_runs
    FROM admission_entries
    GROUP BY tenant_id
)
UPDATE tenant_quota_counters c
SET active_runs = counts.active_runs,
    queued_runs = counts.queued_runs,
    updated_at = now()
FROM counts
WHERE c.tenant_id = counts.tenant_id;

CREATE FUNCTION enforce_terminal_run_admission_released() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.status IN ('success', 'failed', 'error', 'cancelled')
       AND EXISTS (
           SELECT 1 FROM admission_entries a
           WHERE a.tenant_id = NEW.tenant_id
             AND a.resource_type = 'run'
             AND a.resource_id = NEW.id
             AND a.status IN ('active', 'queued')
       ) THEN
        RAISE EXCEPTION 'terminal run must release admission capacity'
            USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER terminal_run_releases_admission
AFTER INSERT OR UPDATE OF status ON runs
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION enforce_terminal_run_admission_released();
