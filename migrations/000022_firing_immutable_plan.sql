-- A firing is an accepted command, so its execution target must survive both
-- trigger deletion and later workflow publication. Historical rows remain
-- nullable only when no publication existed before this invariant was added;
-- all new firings are populated atomically by the application.
ALTER TABLE trigger_firings
    DROP CONSTRAINT trigger_firings_tenant_id_trigger_id_fkey,
    ADD COLUMN workflow_id text,
    ADD COLUMN workflow_version_id text,
    ADD COLUMN execution_plan_id text,
    ADD COLUMN plan_hash text;

UPDATE trigger_firings firing
SET workflow_id = target.workflow_id,
    workflow_version_id = target.workflow_version_id,
    execution_plan_id = target.execution_plan_id,
    plan_hash = target.plan_hash
FROM (
    SELECT firing_row.tenant_id, firing_row.id, trigger.workflow_id,
           version.id AS workflow_version_id, plan.id AS execution_plan_id,
           encode(plan.plan_hash, 'hex') AS plan_hash
    FROM trigger_firings firing_row
    JOIN triggers trigger
      ON trigger.tenant_id = firing_row.tenant_id AND trigger.id = firing_row.trigger_id
    JOIN LATERAL (
        SELECT candidate.id
        FROM workflow_versions candidate
        WHERE candidate.tenant_id = trigger.tenant_id
          AND candidate.workflow_id = trigger.workflow_id
        ORDER BY candidate.version DESC
        LIMIT 1
    ) version ON true
    JOIN execution_plans plan
      ON plan.tenant_id = firing_row.tenant_id AND plan.workflow_version_id = version.id
) target
WHERE firing.tenant_id = target.tenant_id AND firing.id = target.id;

-- Existing pending delivery envelopes must carry the same immutable target;
-- otherwise upgraded dispatchers would be forced to consult mutable state.
UPDATE outbox_messages message
SET payload = message.payload || jsonb_build_object(
    'workflowId', firing.workflow_id,
    'workflowVersionId', firing.workflow_version_id,
    'executionPlanId', firing.execution_plan_id,
    'planHash', firing.plan_hash
)
FROM trigger_firings firing
WHERE message.tenant_id = firing.tenant_id
  AND message.topic = 'trigger.fired'
  AND message.payload->>'firingId' = firing.id
  AND firing.execution_plan_id IS NOT NULL;

ALTER TABLE trigger_firings
    ADD CONSTRAINT trigger_firings_workflow_version_fkey
        FOREIGN KEY (tenant_id, workflow_version_id)
        REFERENCES workflow_versions(tenant_id, id) ON DELETE RESTRICT,
    ADD CONSTRAINT trigger_firings_execution_plan_fkey
        FOREIGN KEY (tenant_id, execution_plan_id)
        REFERENCES execution_plans(tenant_id, id) ON DELETE RESTRICT,
    ADD CONSTRAINT trigger_firings_bound_plan_check CHECK (
        (workflow_id IS NULL AND workflow_version_id IS NULL AND execution_plan_id IS NULL AND plan_hash IS NULL)
        OR
        (workflow_id IS NOT NULL AND workflow_version_id IS NOT NULL AND execution_plan_id IS NOT NULL AND plan_hash IS NOT NULL)
    );

CREATE INDEX trigger_firings_plan_idx
    ON trigger_firings (tenant_id, execution_plan_id, id)
    WHERE execution_plan_id IS NOT NULL;
