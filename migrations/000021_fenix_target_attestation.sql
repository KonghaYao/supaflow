-- Every independently migrated Supaflow database receives a durable identity.
-- A Fenix cleanup report must be verified against this exact production target;
-- a valid report from a staging database is not interchangeable.
CREATE TABLE platform_installations (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    installation_id uuid NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    created_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO platform_installations(singleton) VALUES (true);

CREATE TRIGGER platform_installations_immutable
BEFORE UPDATE OR DELETE ON platform_installations
FOR EACH ROW EXECUTE FUNCTION reject_immutable_workflow_record();

-- Returns a read-only, canonical attestation of every row imported from Fenix.
-- Callers need SELECT on the referenced tables; the function never elevates
-- privileges and never returns workflow data, inputs, outputs, or secrets.
CREATE FUNCTION fenix_target_attestation_v1()
RETURNS TABLE (
    schema_version integer,
    installation_id text,
    content_hash text,
    definition_count bigint,
    draft_count bigint,
    trigger_count bigint,
    run_count bigint
)
LANGUAGE sql
STABLE
SECURITY INVOKER
SET search_path FROM CURRENT
AS $$
WITH imported_drafts AS (
    SELECT DISTINCT mutation.tenant_id, mutation.workflow_id
    FROM definition_mutations mutation
    WHERE mutation.action = 'create'
      AND mutation.idempotency_key LIKE 'fenix-draft-%'
), records(category, row_id, row_hash) AS (
    SELECT 'definition', version.tenant_id || E'\x1f' || version.workflow_id || E'\x1f' || version.version::text,
      encode(sha256(convert_to(jsonb_build_object(
        'tenantId', version.tenant_id,
        'workflowId', version.workflow_id,
        'version', version.version,
        'definition', version.definition,
        'definitionHash', encode(version.definition_hash, 'hex'),
        'planId', plan.id,
        'plan', plan.plan,
        'planHash', encode(plan.plan_hash, 'hex'),
        'compilerVersion', plan.compiler_version
      )::text, 'UTF8')), 'hex')
    FROM workflow_versions version
    JOIN execution_plans plan
      ON plan.tenant_id = version.tenant_id
     AND plan.workflow_version_id = version.id
    WHERE version.publication_key LIKE 'fenix-version-%'

    UNION ALL
    SELECT 'draft', draft.tenant_id || E'\x1f' || draft.workflow_id,
      encode(sha256(convert_to(jsonb_build_object(
        'tenantId', draft.tenant_id,
        'workflowId', draft.workflow_id,
        'revision', draft.revision,
        'document', draft.document
      )::text, 'UTF8')), 'hex')
    FROM workflow_drafts draft
    JOIN imported_drafts imported
      ON imported.tenant_id = draft.tenant_id
     AND imported.workflow_id = draft.workflow_id

    UNION ALL
    SELECT 'trigger', trigger.tenant_id || E'\x1f' || trigger.id,
      encode(sha256(convert_to(jsonb_build_object(
        'tenantId', trigger.tenant_id,
        'id', trigger.id,
        'publicHash', trigger.public_hash,
        'workflowId', trigger.workflow_id,
        'kind', trigger.kind,
        'status', trigger.status,
        'config', trigger.config,
        'scheduleIntervalSeconds', trigger.schedule_interval_seconds,
        'misfirePolicy', trigger.misfire_policy,
        'nextFireAt', trigger.next_fire_at,
        'signingCredentialId', trigger.signing_credential_id,
        'eventType', trigger.event_type,
        'sourceImportHash', encode(trigger.source_import_hash, 'hex')
      )::text, 'UTF8')), 'hex')
    FROM triggers trigger
    WHERE trigger.source_system = 'fenix'

    UNION ALL
    SELECT 'run', run.tenant_id || E'\x1f' || run.id,
      encode(sha256(convert_to(jsonb_build_object(
        'tenantId', run.tenant_id,
        'id', run.id,
        'workflowId', run.workflow_id,
        'workflowVersionId', run.workflow_version_id,
        'executionPlanId', run.execution_plan_id,
        'planHash', run.plan_hash,
        'status', run.status,
        'requestHash', encode(run.request_hash, 'hex'),
        'input', run.input,
        'outputId', run.output_id,
        'events', COALESCE((
          SELECT jsonb_agg(jsonb_build_object(
            'sequence', event.sequence,
            'type', event.event_type,
            'payload', event.payload
          ) ORDER BY event.sequence)
          FROM run_events event
          WHERE event.tenant_id = run.tenant_id AND event.run_id = run.id
        ), '[]'::jsonb),
        'outputs', COALESCE((
          SELECT jsonb_agg(jsonb_build_object(
            'id', output.id,
            'nodeId', output_state.node_id,
            'contentType', output.content_type,
            'inlineValue', output.inline_value,
            'objectRef', output.object_ref,
            'sizeBytes', output.size_bytes,
            'contentHash', encode(output.content_hash, 'hex'),
            'sensitivity', output.sensitivity
          ) ORDER BY output.id)
          FROM node_outputs output
          JOIN node_states output_state
            ON output_state.tenant_id = output.tenant_id
           AND output_state.run_id = output.run_id
           AND output_state.id = output.node_state_id
          WHERE output.tenant_id = run.tenant_id AND output.run_id = run.id
        ), '[]'::jsonb)
      )::text, 'UTF8')), 'hex')
    FROM runs run
    WHERE run.idempotency_key = 'import:fenix:' || run.id
), categories(category) AS (
    VALUES ('definition'), ('draft'), ('trigger'), ('run')
), category_digests AS (
    SELECT category.category,
      count(records.row_id)::bigint AS row_count,
      encode(sha256(convert_to(
        COALESCE(string_agg(records.row_hash, '' ORDER BY records.row_id), ''),
        'UTF8'
      )), 'hex') AS category_hash
    FROM categories category
    LEFT JOIN records USING (category)
    GROUP BY category.category
), summary AS (
    SELECT encode(sha256(convert_to(
      'supaflow-fenix-target-v1' || E'\n' ||
      string_agg(category || ':' || row_count || ':' || category_hash, E'\n' ORDER BY category),
      'UTF8'
    )), 'hex') AS content_hash
    FROM category_digests
)
SELECT 1,
  installation.installation_id::text,
  summary.content_hash,
  (SELECT row_count FROM category_digests WHERE category = 'definition'),
  (SELECT row_count FROM category_digests WHERE category = 'draft'),
  (SELECT row_count FROM category_digests WHERE category = 'trigger'),
  (SELECT row_count FROM category_digests WHERE category = 'run')
FROM platform_installations installation
CROSS JOIN summary
WHERE installation.singleton = true;
$$;
