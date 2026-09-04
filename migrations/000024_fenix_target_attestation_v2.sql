-- v2 records workflow identity explicitly and attests every row produced by
-- the Fenix importer. v1 remains frozen for compatibility with old evidence.
CREATE TABLE workflow_imports (
    tenant_id text NOT NULL,
    workflow_id text NOT NULL,
    source_system text NOT NULL CHECK (length(source_system) BETWEEN 1 AND 64),
    source_workflow_id text NOT NULL CHECK (length(source_workflow_id) BETWEEN 1 AND 255),
    source_import_hash bytea NOT NULL CHECK (octet_length(source_import_hash) = 32),
    imported_by text NOT NULL,
    imported_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, workflow_id),
    UNIQUE (tenant_id, source_system, source_workflow_id),
    FOREIGN KEY (tenant_id, workflow_id) REFERENCES workflows(tenant_id, id) ON DELETE CASCADE
);

-- The digest deliberately hashes complete row projections. It can therefore
-- detect changes to imported metadata and supporting rows, without returning
-- definitions, inputs, outputs, credentials, or other tenant content.
CREATE FUNCTION fenix_target_attestation_v2()
RETURNS TABLE (
    schema_version integer,
    installation_id text,
    content_hash text,
    workflow_count bigint,
    definition_count bigint,
    draft_count bigint,
    trigger_count bigint,
    run_count bigint
)
LANGUAGE sql
STABLE
SECURITY INVOKER
SET search_path = pg_catalog, public
SET timezone = 'UTC'
SET bytea_output = 'hex'
AS $$
WITH fenix_workflows AS (
    SELECT marker.tenant_id, marker.workflow_id
    FROM workflow_imports marker
    WHERE marker.source_system = 'fenix'
), imported_drafts AS (
    SELECT mutation.tenant_id, mutation.workflow_id,
      jsonb_agg(to_jsonb(mutation) ORDER BY mutation.id) AS mutations
    FROM definition_mutations mutation
    JOIN fenix_workflows imported
      ON imported.tenant_id = mutation.tenant_id
     AND imported.workflow_id = mutation.workflow_id
    WHERE mutation.action = 'create'
      AND mutation.idempotency_key LIKE 'fenix-draft-%'
    GROUP BY mutation.tenant_id, mutation.workflow_id
), records(category, row_id, row_hash) AS (
    SELECT 'workflow', workflow.tenant_id || E'\x1f' || workflow.id,
      encode(sha256(convert_to(jsonb_build_object(
        'workflow', to_jsonb(workflow),
        'import', to_jsonb(marker)
      )::text, 'UTF8')), 'hex')
    FROM workflows workflow
    JOIN workflow_imports marker
      ON marker.tenant_id = workflow.tenant_id
     AND marker.workflow_id = workflow.id
    WHERE marker.source_system = 'fenix'

    UNION ALL
    SELECT 'definition', version.tenant_id || E'\x1f' || version.workflow_id || E'\x1f' || version.version::text,
      encode(sha256(convert_to(jsonb_build_object(
        'version', to_jsonb(version),
        'plan', to_jsonb(plan)
      )::text, 'UTF8')), 'hex')
    FROM workflow_versions version
    JOIN execution_plans plan
      ON plan.tenant_id = version.tenant_id
     AND plan.workflow_version_id = version.id
    JOIN fenix_workflows imported
      ON imported.tenant_id = version.tenant_id
     AND imported.workflow_id = version.workflow_id
    WHERE version.publication_key LIKE 'fenix-version-%'

    UNION ALL
    SELECT 'draft', draft.tenant_id || E'\x1f' || draft.workflow_id,
      encode(sha256(convert_to(jsonb_build_object(
        'draft', to_jsonb(draft),
        'mutations', imported.mutations
      )::text, 'UTF8')), 'hex')
    FROM workflow_drafts draft
    JOIN imported_drafts imported
      ON imported.tenant_id = draft.tenant_id
     AND imported.workflow_id = draft.workflow_id

    UNION ALL
    SELECT 'trigger', trigger.tenant_id || E'\x1f' || trigger.id,
      encode(sha256(convert_to(to_jsonb(trigger)::text, 'UTF8')), 'hex')
    FROM triggers trigger
    JOIN fenix_workflows imported
      ON imported.tenant_id = trigger.tenant_id
     AND imported.workflow_id = trigger.workflow_id
    WHERE trigger.source_system = 'fenix'

    UNION ALL
    SELECT 'run', run.tenant_id || E'\x1f' || run.id,
      encode(sha256(convert_to(jsonb_build_object(
        'run', to_jsonb(run),
        'events', COALESCE((
          SELECT jsonb_agg(to_jsonb(event) ORDER BY event.sequence, event.id)
          FROM run_events event
          WHERE event.tenant_id = run.tenant_id AND event.run_id = run.id
        ), '[]'::jsonb),
        'nodeStates', COALESCE((
          SELECT jsonb_agg(to_jsonb(state) ORDER BY state.id)
          FROM node_states state
          WHERE state.tenant_id = run.tenant_id AND state.run_id = run.id
        ), '[]'::jsonb),
        'nodeAttempts', COALESCE((
          SELECT jsonb_agg(to_jsonb(attempt) ORDER BY attempt.id)
          FROM node_attempts attempt
          WHERE attempt.tenant_id = run.tenant_id AND attempt.run_id = run.id
        ), '[]'::jsonb),
        'outputs', COALESCE((
          SELECT jsonb_agg(to_jsonb(output) ORDER BY output.id)
          FROM node_outputs output
          WHERE output.tenant_id = run.tenant_id AND output.run_id = run.id
        ), '[]'::jsonb)
      )::text, 'UTF8')), 'hex')
    FROM runs run
    JOIN fenix_workflows imported
      ON imported.tenant_id = run.tenant_id
     AND imported.workflow_id = run.workflow_id
    WHERE run.idempotency_key = 'import:fenix:' || run.id
), categories(category) AS (
    VALUES ('workflow'), ('definition'), ('draft'), ('trigger'), ('run')
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
      'supaflow-fenix-target-v2' || E'\n' ||
      string_agg(category || ':' || row_count || ':' || category_hash, E'\n' ORDER BY category),
      'UTF8'
    )), 'hex') AS content_hash
    FROM category_digests
)
SELECT 2,
  installation.installation_id::text,
  summary.content_hash,
  (SELECT row_count FROM category_digests WHERE category = 'workflow'),
  (SELECT row_count FROM category_digests WHERE category = 'definition'),
  (SELECT row_count FROM category_digests WHERE category = 'draft'),
  (SELECT row_count FROM category_digests WHERE category = 'trigger'),
  (SELECT row_count FROM category_digests WHERE category = 'run')
FROM platform_installations installation
CROSS JOIN summary
WHERE installation.singleton = true;
$$;
