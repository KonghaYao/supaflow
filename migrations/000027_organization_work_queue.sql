-- Durable organization work queue. Rows are scheduling hints/cursors only;
-- node_attempts, runs, admission_entries, and worker_leases remain authoritative.
CREATE TABLE organization_work_queue (
    organization_id text PRIMARY KEY,
    ready_at timestamptz NOT NULL,
    last_claimed_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX organization_work_queue_ready_idx
    ON organization_work_queue (ready_at, last_claimed_at NULLS FIRST, organization_id);

CREATE FUNCTION refresh_organization_work_hint(target_organization_id text) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE next_ready_at timestamptz;
BEGIN
    SELECT min(CASE WHEN a.status = 'running' THEN l.expires_at ELSE a.available_at END)
      INTO next_ready_at
      FROM node_attempts a
      JOIN runs r ON r.organization_id = a.organization_id AND r.id = a.run_id
      JOIN admission_entries admission
        ON admission.organization_id = r.organization_id
       AND admission.resource_type = 'run'
       AND admission.resource_id = r.id
       AND admission.status = 'active'
      LEFT JOIN worker_leases l
        ON l.organization_id = a.organization_id AND l.attempt_id = a.id
     WHERE a.organization_id = target_organization_id
       AND (r.deadline_at IS NULL OR r.deadline_at > clock_timestamp())
       AND ((r.status IN ('pending','running') AND a.status IN ('pending','retry_ready'))
         OR (r.status IN ('running','cancelling') AND a.status = 'running' AND l.expires_at IS NOT NULL));

    IF next_ready_at IS NULL THEN
        DELETE FROM organization_work_queue WHERE organization_id = target_organization_id;
    ELSE
        INSERT INTO organization_work_queue(organization_id, ready_at, updated_at)
        VALUES (target_organization_id, next_ready_at, clock_timestamp())
        ON CONFLICT (organization_id) DO UPDATE
          SET ready_at = EXCLUDED.ready_at, updated_at = EXCLUDED.updated_at;
    END IF;
END;
$$;

CREATE FUNCTION refresh_organization_work_hint_trigger() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        PERFORM refresh_organization_work_hint(OLD.organization_id);
    ELSE
        PERFORM refresh_organization_work_hint(NEW.organization_id);
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER node_attempt_work_hint AFTER INSERT OR UPDATE OR DELETE ON node_attempts
FOR EACH ROW EXECUTE FUNCTION refresh_organization_work_hint_trigger();
CREATE TRIGGER run_work_hint AFTER UPDATE OF status, deadline_at ON runs
FOR EACH ROW EXECUTE FUNCTION refresh_organization_work_hint_trigger();
CREATE TRIGGER admission_work_hint AFTER INSERT OR UPDATE OR DELETE ON admission_entries
FOR EACH ROW EXECUTE FUNCTION refresh_organization_work_hint_trigger();
CREATE TRIGGER lease_work_hint AFTER INSERT OR UPDATE OR DELETE ON worker_leases
FOR EACH ROW EXECUTE FUNCTION refresh_organization_work_hint_trigger();

-- Forward-only backfill. This does not create an organization registry: only
-- organizations with authoritative runnable or leased work receive a hint.
INSERT INTO organization_work_queue(organization_id, ready_at)
SELECT a.organization_id,
       min(CASE WHEN a.status = 'running' THEN l.expires_at ELSE a.available_at END)
FROM node_attempts a
JOIN runs r ON r.organization_id = a.organization_id AND r.id = a.run_id
JOIN admission_entries admission
  ON admission.organization_id = r.organization_id
 AND admission.resource_type = 'run' AND admission.resource_id = r.id
 AND admission.status = 'active'
LEFT JOIN worker_leases l ON l.organization_id = a.organization_id AND l.attempt_id = a.id
WHERE (r.deadline_at IS NULL OR r.deadline_at > clock_timestamp())
  AND ((r.status IN ('pending','running') AND a.status IN ('pending','retry_ready'))
    OR (r.status IN ('running','cancelling') AND a.status = 'running' AND l.expires_at IS NOT NULL))
GROUP BY a.organization_id;
