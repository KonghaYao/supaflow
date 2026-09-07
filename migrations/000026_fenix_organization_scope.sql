-- Applied atomically by the Supaflow migrator. This forward-only migration removes
-- Supaflow IAM while preserving opaque Fenix organization ownership on business data.

-- Functions compiled against tenant_id must be recreated after column renames. Drop
-- provisioning triggers first because organizations are not a local lifecycle registry.
DROP TRIGGER IF EXISTS tenants_default_retention_policy ON tenants;
DROP TRIGGER IF EXISTS tenants_default_quota ON tenants;
DROP FUNCTION IF EXISTS provision_default_retention_policy();
DROP FUNCTION IF EXISTS provision_default_tenant_quota();

-- Remove every local-registry FK. Composite business FKs do not reference tenants and
-- remain intact; PostgreSQL follows column/table renames for those relationships.
DO $$
DECLARE item record;
BEGIN
    FOR item IN
        SELECT conrelid::regclass AS relation_name, conname
        FROM pg_constraint
        WHERE contype = 'f' AND confrelid = 'tenants'::regclass
    LOOP
        EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', item.relation_name, item.conname);
    END LOOP;
END;
$$;

DROP TABLE identity_sessions;
DROP TABLE oidc_login_attempts;
DROP TABLE api_keys;
DROP TABLE tenant_memberships;
DROP TABLE service_accounts;
DROP TABLE oidc_identities;
DROP TABLE tenants;

-- Rename every persisted business scope column without enumerating future additions.
DO $$
DECLARE item record;
BEGIN
    FOR item IN
        SELECT table_schema, table_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND column_name = 'tenant_id'
        ORDER BY table_name
    LOOP
        EXECUTE format('ALTER TABLE %I.%I RENAME COLUMN tenant_id TO organization_id', item.table_schema, item.table_name);
    END LOOP;
END;
$$;

ALTER TABLE tenant_quotas RENAME TO organization_quotas;
ALTER TABLE tenant_quota_counters RENAME TO organization_quota_counters;
ALTER TABLE tenant_exports RENAME TO organization_exports;
ALTER TABLE trigger_schedule_tenant_state RENAME TO trigger_schedule_organization_state;

-- Operational defaults are scoped state, not an organization registry. Every trusted
-- business insert provisions them in the same transaction. ON CONFLICT preserves
-- explicit administrator quota and retention settings under concurrent first use.
CREATE FUNCTION provision_organization_operational_scope() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO organization_quotas (
        organization_id,max_active_runs,max_active_workers,max_queue_depth,
        max_triggers,max_pending_trigger_firings,max_pending_outbox_messages
    ) VALUES (NEW.organization_id,100,100,1000,1000,5000,20000)
    ON CONFLICT (organization_id) DO NOTHING;
    INSERT INTO organization_quota_counters (organization_id)
    VALUES (NEW.organization_id) ON CONFLICT (organization_id) DO NOTHING;
    INSERT INTO retention_policies (organization_id,event_days,output_days,archive_before_delete)
    VALUES (NEW.organization_id,30,30,true) ON CONFLICT (organization_id) DO NOTHING;
    INSERT INTO trigger_schedule_organization_state (organization_id)
    VALUES (NEW.organization_id) ON CONFLICT (organization_id) DO NOTHING;
    RETURN NEW;
END;
$$;

DO $$
DECLARE relation_name text;
BEGIN
    FOREACH relation_name IN ARRAY ARRAY['workflows','runs','triggers','outbox_messages','credential_refs','managed_objects','object_upload_intents','organization_exports','ops_mutation_ledger']
    LOOP
        EXECUTE format(
            'CREATE TRIGGER %I_provision_organization_scope BEFORE INSERT ON %I '
            'FOR EACH ROW EXECUTE FUNCTION provision_organization_operational_scope()',
            relation_name, relation_name
        );
    END LOOP;
END;
$$;

-- The domain rename changes the persisted object kind as well as Go/API vocabulary.
UPDATE managed_objects SET kind='organization_export' WHERE kind='tenant_export';
ALTER TABLE managed_objects DROP CONSTRAINT managed_objects_kind_check;
ALTER TABLE managed_objects ADD CONSTRAINT managed_objects_kind_check
    CHECK (kind IN ('output','archive','organization_export','backup'));
ALTER TABLE object_upload_intents DROP CONSTRAINT object_upload_intents_kind_check;
ALTER TABLE object_upload_intents ADD CONSTRAINT object_upload_intents_kind_check
    CHECK (kind IN ('output','archive','organization_export','backup'));

-- Keep persisted object names aligned with the current domain. PostgreSQL does not
-- automatically rename constraints or indexes when referenced columns are renamed.
DO $$
DECLARE item record;
DECLARE renamed text;
BEGIN
    FOR item IN
        SELECT c.oid, n.nspname, c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relkind IN ('i', 'I')
          AND c.relname LIKE '%tenant%'
    LOOP
        renamed := replace(item.relname, 'tenant', 'organization');
        EXECUTE format('ALTER INDEX %I.%I RENAME TO %I', item.nspname, item.relname, renamed);
    END LOOP;

    FOR item IN
        SELECT conrelid::regclass AS relation_name, conname
        FROM pg_constraint
        WHERE connamespace = 'public'::regnamespace AND conname LIKE '%tenant%'
    LOOP
        renamed := replace(item.conname, 'tenant', 'organization');
        EXECUTE format('ALTER TABLE %s RENAME CONSTRAINT %I TO %I', item.relation_name, item.conname, renamed);
    END LOOP;
END;
$$;

-- Recompile PL/pgSQL/SQL functions whose bodies refer to renamed scope objects.
-- Keep externally named Fenix attestation APIs stable while replacing only domain terms
-- inside their definitions.
DO $$
DECLARE item record;
DECLARE definition text;
BEGIN
    FOR item IN
        SELECT p.oid
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'public'
          AND p.prokind = 'f'
          AND pg_get_functiondef(p.oid) LIKE '%tenant%'
          AND p.proname NOT IN ('provision_default_tenant_quota')
    LOOP
        definition := pg_get_functiondef(item.oid);
        definition := replace(definition, 'tenant', 'organization');
        EXECUTE definition;
    END LOOP;
END;
$$;

CREATE FUNCTION enforce_organization_capacity() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    organization_limit integer;
    organization_usage bigint;
BEGIN
    SELECT max_triggers INTO organization_limit
    FROM organization_quotas WHERE organization_id = NEW.organization_id FOR UPDATE;
    IF organization_limit IS NULL THEN
        RAISE EXCEPTION 'organization quota is not configured' USING ERRCODE = '23514';
    END IF;
    SELECT count(*) INTO organization_usage FROM triggers WHERE organization_id = NEW.organization_id;
    IF organization_usage >= organization_limit THEN
        RAISE EXCEPTION 'organization trigger quota exceeded' USING ERRCODE = '53400';
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER triggers_capacity_guard ON triggers;
CREATE TRIGGER triggers_capacity_guard BEFORE INSERT ON triggers
FOR EACH ROW EXECUTE FUNCTION enforce_organization_capacity();
DROP FUNCTION enforce_trigger_capacity();
