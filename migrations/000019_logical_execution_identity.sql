-- Physical attempts are replaceable retry records. External side-effect
-- deduplication is instead scoped to one stable logical node execution.
ALTER TABLE node_attempts ADD COLUMN execution_id text;
UPDATE node_attempts SET execution_id = id WHERE execution_id IS NULL;
ALTER TABLE node_attempts ALTER COLUMN execution_id SET NOT NULL;
ALTER TABLE node_attempts ADD CONSTRAINT node_attempts_execution_id_not_empty
    CHECK (char_length(execution_id) BETWEEN 1 AND 255);
CREATE INDEX node_attempts_execution_idx
    ON node_attempts (tenant_id, run_id, execution_id, attempt_number);

CREATE FUNCTION default_node_attempt_execution_id() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.execution_id IS NULL OR NEW.execution_id = '' THEN
        NEW.execution_id := NEW.id;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER node_attempt_execution_id_default
BEFORE INSERT ON node_attempts
FOR EACH ROW EXECUTE FUNCTION default_node_attempt_execution_id();

-- Keep the first physical parent attempt for audit lineage, while using the
-- logical execution identity as the idempotency authority across retries.
ALTER TABLE subworkflow_links ADD COLUMN parent_execution_id text;
UPDATE subworkflow_links link
SET parent_execution_id = attempt.execution_id
FROM node_attempts attempt
WHERE attempt.tenant_id = link.tenant_id
  AND attempt.id = link.parent_attempt_id
  AND link.parent_execution_id IS NULL;
ALTER TABLE subworkflow_links ALTER COLUMN parent_execution_id SET NOT NULL;
ALTER TABLE subworkflow_links DROP CONSTRAINT subworkflow_links_pkey;
ALTER TABLE subworkflow_links ADD PRIMARY KEY (tenant_id, parent_execution_id);
ALTER TABLE subworkflow_links ADD CONSTRAINT subworkflow_links_parent_attempt_unique
    UNIQUE (tenant_id, parent_attempt_id);

CREATE FUNCTION default_subworkflow_parent_execution_id() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.parent_execution_id IS NULL OR NEW.parent_execution_id = '' THEN
        SELECT execution_id INTO NEW.parent_execution_id
        FROM node_attempts
        WHERE tenant_id = NEW.tenant_id AND id = NEW.parent_attempt_id;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER subworkflow_parent_execution_id_default
BEFORE INSERT ON subworkflow_links
FOR EACH ROW EXECUTE FUNCTION default_subworkflow_parent_execution_id();
