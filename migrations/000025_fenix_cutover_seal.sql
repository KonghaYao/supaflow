-- A successful Fenix target attestation must remain true while the source
-- database is irreversibly cleaned up. The singleton seal closes that
-- cross-database gap: acquisition locks every imported root, verifies the
-- caller's complete attestation, and activates write guards in one commit.
CREATE TABLE fenix_cutover_seals (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    schema_version integer NOT NULL CHECK (schema_version = 2),
    installation_id uuid NOT NULL,
    content_hash text NOT NULL CHECK (content_hash ~ '^[0-9a-f]{64}$'),
    workflow_count bigint NOT NULL CHECK (workflow_count >= 0),
    definition_count bigint NOT NULL CHECK (definition_count >= 0),
    draft_count bigint NOT NULL CHECK (draft_count >= 0),
    trigger_count bigint NOT NULL CHECK (trigger_count >= 0),
    run_count bigint NOT NULL CHECK (run_count >= 0),
    token_hash bytea NOT NULL CHECK (octet_length(token_hash) = 32),
    acquired_at timestamptz NOT NULL,
    released_at timestamptz,
    FOREIGN KEY (installation_id) REFERENCES platform_installations(installation_id) ON DELETE RESTRICT,
    CHECK (released_at IS NULL OR released_at >= acquired_at)
);

REVOKE ALL ON TABLE fenix_cutover_seals FROM PUBLIC;

CREATE FUNCTION acquire_fenix_cutover_seal_v2(
    expected_installation_id uuid,
    expected_content_hash text,
    expected_workflow_count bigint,
    expected_definition_count bigint,
    expected_draft_count bigint,
    expected_trigger_count bigint,
    expected_run_count bigint,
    seal_token text
)
RETURNS TABLE (
    schema_version integer,
    installation_id text,
    content_hash text,
    workflow_count bigint,
    definition_count bigint,
    draft_count bigint,
    trigger_count bigint,
    run_count bigint,
    acquired_at timestamptz
)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    attestation record;
    existing_seal record;
    requested_token_hash bytea;
    sealed_at timestamptz;
BEGIN
    IF expected_installation_id IS NULL
       OR expected_content_hash IS NULL
       OR expected_content_hash !~ '^[0-9a-fA-F]{64}$'
       OR expected_workflow_count IS NULL OR expected_workflow_count < 0
       OR expected_definition_count IS NULL OR expected_definition_count < 0
       OR expected_draft_count IS NULL OR expected_draft_count < 0
       OR expected_trigger_count IS NULL OR expected_trigger_count < 0
       OR expected_run_count IS NULL OR expected_run_count < 0
       OR seal_token IS NULL
       OR seal_token !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION 'invalid Fenix cutover seal request' USING ERRCODE = '22023';
    END IF;

    -- SHARE ROW EXCLUSIVE conflicts with ordinary INSERT/UPDATE/DELETE locks.
    -- Once these waits finish, the attestation and active guard become visible
    -- atomically when this function's transaction commits.
    LOCK TABLE public.workflows, public.workflow_imports, public.workflow_drafts, public.workflow_versions,
        public.execution_plans, public.definition_mutations, public.triggers,
        public.runs, public.run_events, public.node_states, public.node_attempts, public.node_outputs
        IN SHARE ROW EXCLUSIVE MODE;

    SELECT * INTO attestation FROM public.fenix_target_attestation_v2();
    IF NOT FOUND
       OR attestation.schema_version <> 2
       OR attestation.installation_id::uuid IS DISTINCT FROM expected_installation_id
       OR attestation.content_hash <> lower(expected_content_hash)
       OR attestation.workflow_count <> expected_workflow_count
       OR attestation.definition_count <> expected_definition_count
       OR attestation.draft_count <> expected_draft_count
       OR attestation.trigger_count <> expected_trigger_count
       OR attestation.run_count <> expected_run_count THEN
        RAISE EXCEPTION 'Fenix target attestation does not match expected content'
            USING ERRCODE = '55000';
    END IF;

    requested_token_hash := sha256(convert_to(seal_token, 'UTF8'));
    SELECT seal.* INTO existing_seal
    FROM public.fenix_cutover_seals seal
    WHERE seal.singleton = true
    FOR UPDATE;

    IF FOUND AND existing_seal.released_at IS NULL THEN
        IF existing_seal.installation_id IS DISTINCT FROM expected_installation_id
           OR existing_seal.content_hash <> lower(expected_content_hash)
           OR existing_seal.workflow_count <> expected_workflow_count
           OR existing_seal.definition_count <> expected_definition_count
           OR existing_seal.draft_count <> expected_draft_count
           OR existing_seal.trigger_count <> expected_trigger_count
           OR existing_seal.run_count <> expected_run_count
           OR existing_seal.token_hash <> requested_token_hash THEN
            RAISE EXCEPTION 'a different Fenix cutover seal is already active'
                USING ERRCODE = '55000';
        END IF;
        sealed_at := existing_seal.acquired_at;
    ELSE
        sealed_at := clock_timestamp();
        INSERT INTO public.fenix_cutover_seals (
            singleton, schema_version, installation_id, content_hash,
            workflow_count, definition_count, draft_count, trigger_count, run_count,
            token_hash, acquired_at, released_at
        ) VALUES (
            true, 2, expected_installation_id, lower(expected_content_hash),
            expected_workflow_count,
            expected_definition_count, expected_draft_count,
            expected_trigger_count, expected_run_count,
            requested_token_hash, sealed_at, NULL
        )
        ON CONFLICT (singleton) DO UPDATE SET
            schema_version = EXCLUDED.schema_version,
            installation_id = EXCLUDED.installation_id,
            content_hash = EXCLUDED.content_hash,
            workflow_count = EXCLUDED.workflow_count,
            definition_count = EXCLUDED.definition_count,
            draft_count = EXCLUDED.draft_count,
            trigger_count = EXCLUDED.trigger_count,
            run_count = EXCLUDED.run_count,
            token_hash = EXCLUDED.token_hash,
            acquired_at = EXCLUDED.acquired_at,
            released_at = NULL;
    END IF;

    RETURN QUERY SELECT
        attestation.schema_version,
        attestation.installation_id,
        attestation.content_hash,
        attestation.workflow_count,
        attestation.definition_count,
        attestation.draft_count,
        attestation.trigger_count,
        attestation.run_count,
        sealed_at;
END;
$$;

CREATE FUNCTION release_fenix_cutover_seal_v2(
    expected_installation_id uuid,
    seal_token text
)
RETURNS boolean
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    existing_seal record;
    requested_token_hash bytea;
BEGIN
    IF expected_installation_id IS NULL
       OR seal_token IS NULL
       OR seal_token !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION 'invalid Fenix cutover seal release' USING ERRCODE = '22023';
    END IF;
    requested_token_hash := sha256(convert_to(seal_token, 'UTF8'));

    SELECT seal.* INTO existing_seal
    FROM public.fenix_cutover_seals seal
    WHERE seal.singleton = true
    FOR UPDATE;
    IF NOT FOUND OR existing_seal.installation_id IS DISTINCT FROM expected_installation_id THEN
        RAISE EXCEPTION 'Fenix cutover seal does not exist for this installation'
            USING ERRCODE = '55000';
    END IF;
    IF existing_seal.token_hash <> requested_token_hash THEN
        RAISE EXCEPTION 'Fenix cutover seal token is invalid' USING ERRCODE = '28000';
    END IF;
    IF existing_seal.released_at IS NULL THEN
        UPDATE public.fenix_cutover_seals
        SET released_at = clock_timestamp()
        WHERE singleton = true;
    END IF;
    RETURN true;
END;
$$;

CREATE FUNCTION reject_mutation_while_fenix_cutover_sealed_v2()
RETURNS trigger
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM public.fenix_cutover_seals seal
        WHERE seal.singleton = true AND seal.released_at IS NULL
    ) THEN
        RAISE EXCEPTION 'Fenix cutover seal is active; target mutations are disabled'
            USING ERRCODE = '55000';
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

DO $$
DECLARE
    guarded_table text;
BEGIN
    FOREACH guarded_table IN ARRAY ARRAY[
        'workflows', 'workflow_imports', 'workflow_drafts', 'workflow_versions',
        'execution_plans', 'definition_mutations', 'triggers', 'runs',
        'run_events', 'node_states', 'node_attempts', 'node_outputs'
    ] LOOP
        EXECUTE format(
            'CREATE TRIGGER %I_fenix_cutover_seal_guard '
            'BEFORE INSERT OR UPDATE OR DELETE ON %I '
            'FOR EACH ROW EXECUTE FUNCTION reject_mutation_while_fenix_cutover_sealed_v2()',
            guarded_table, guarded_table
        );
        EXECUTE format(
            'CREATE TRIGGER %I_fenix_cutover_seal_truncate_guard '
            'BEFORE TRUNCATE ON %I '
            'FOR EACH STATEMENT EXECUTE FUNCTION reject_mutation_while_fenix_cutover_sealed_v2()',
            guarded_table, guarded_table
        );
    END LOOP;
END;
$$;

REVOKE ALL ON FUNCTION acquire_fenix_cutover_seal_v2(uuid, text, bigint, bigint, bigint, bigint, bigint, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION release_fenix_cutover_seal_v2(uuid, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION reject_mutation_while_fenix_cutover_sealed_v2() FROM PUBLIC;
