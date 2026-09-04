-- Trigger ingress is tenant-serialized at the database boundary. This protects
-- every producer, including older binaries and runtime outbox writers that do
-- not pass through the platform operations service.
ALTER TABLE tenant_quotas
    ADD COLUMN max_triggers integer NOT NULL DEFAULT 1000
        CHECK (max_triggers BETWEEN 1 AND 10000),
    ADD COLUMN max_pending_trigger_firings integer NOT NULL DEFAULT 5000
        CHECK (max_pending_trigger_firings BETWEEN 1 AND 100000),
    ADD COLUMN max_pending_outbox_messages integer NOT NULL DEFAULT 20000
        CHECK (max_pending_outbox_messages BETWEEN 1 AND 500000);

CREATE TABLE trigger_schedule_tenant_state (
    tenant_id text PRIMARY KEY REFERENCES tenants(id) ON DELETE CASCADE,
    last_served_at timestamptz
);

INSERT INTO trigger_schedule_tenant_state (tenant_id)
SELECT id FROM tenants ON CONFLICT (tenant_id) DO NOTHING;

CREATE INDEX trigger_firings_pending_capacity_idx
    ON trigger_firings (tenant_id, id)
    WHERE status = 'pending';

CREATE INDEX trigger_firings_completed_retention_idx
    ON trigger_firings (tenant_id, created_at, id)
    WHERE status IN ('dispatched', 'failed');

CREATE INDEX outbox_messages_delivered_retention_idx
    ON outbox_messages (tenant_id, delivered_at, id)
    WHERE delivered_at IS NOT NULL;

CREATE OR REPLACE FUNCTION provision_default_tenant_quota() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO tenant_quotas (
        tenant_id, max_active_runs, max_active_workers, max_queue_depth,
        max_triggers, max_pending_trigger_firings, max_pending_outbox_messages
    ) VALUES (NEW.id, 100, 50, 1000, 1000, 5000, 20000)
    ON CONFLICT (tenant_id) DO NOTHING;
    INSERT INTO tenant_quota_counters (tenant_id)
    VALUES (NEW.id) ON CONFLICT (tenant_id) DO NOTHING;
    INSERT INTO trigger_schedule_tenant_state (tenant_id)
    VALUES (NEW.id) ON CONFLICT (tenant_id) DO NOTHING;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS tenants_default_quota ON tenants;
CREATE TRIGGER tenants_default_quota
AFTER INSERT ON tenants
FOR EACH ROW EXECUTE FUNCTION provision_default_tenant_quota();

CREATE OR REPLACE FUNCTION enforce_trigger_capacity() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    tenant_limit integer;
    tenant_usage bigint;
BEGIN
    SELECT max_triggers INTO tenant_limit
    FROM tenant_quotas WHERE tenant_id = NEW.tenant_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'tenant quota is not provisioned' USING ERRCODE = '23503';
    END IF;
    SELECT count(*) INTO tenant_usage FROM triggers WHERE tenant_id = NEW.tenant_id;
    IF tenant_usage >= tenant_limit THEN
        RAISE EXCEPTION 'tenant trigger capacity exceeded' USING ERRCODE = '53400';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER triggers_capacity_guard
BEFORE INSERT ON triggers
FOR EACH ROW EXECUTE FUNCTION enforce_trigger_capacity();

CREATE OR REPLACE FUNCTION enforce_pending_firing_capacity() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    tenant_limit integer;
    tenant_usage bigint;
BEGIN
    IF EXISTS (
        SELECT 1 FROM trigger_firings
        WHERE tenant_id=NEW.tenant_id AND trigger_id=NEW.trigger_id
          AND deduplication_key=NEW.deduplication_key
    ) THEN
        RETURN NEW;
    END IF;
    SELECT max_pending_trigger_firings INTO tenant_limit
    FROM tenant_quotas WHERE tenant_id = NEW.tenant_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'tenant quota is not provisioned' USING ERRCODE = '23503';
    END IF;
    SELECT count(*) INTO tenant_usage
    FROM trigger_firings WHERE tenant_id = NEW.tenant_id AND status = 'pending';
    IF tenant_usage >= tenant_limit THEN
        RAISE EXCEPTION 'tenant pending firing capacity exceeded' USING ERRCODE = '53400';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trigger_firings_capacity_guard
BEFORE INSERT ON trigger_firings
FOR EACH ROW WHEN (NEW.status = 'pending')
EXECUTE FUNCTION enforce_pending_firing_capacity();

CREATE OR REPLACE FUNCTION enforce_pending_outbox_capacity() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    tenant_limit integer;
    tenant_usage bigint;
BEGIN
    SELECT max_pending_outbox_messages INTO tenant_limit
    FROM tenant_quotas WHERE tenant_id = NEW.tenant_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'tenant quota is not provisioned' USING ERRCODE = '23503';
    END IF;
    SELECT count(*) INTO tenant_usage
    FROM outbox_messages WHERE tenant_id = NEW.tenant_id AND delivered_at IS NULL;
    IF tenant_usage >= tenant_limit THEN
        RAISE EXCEPTION 'tenant pending outbox capacity exceeded' USING ERRCODE = '53400';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER outbox_messages_capacity_guard
BEFORE INSERT ON outbox_messages
FOR EACH ROW WHEN (NEW.delivered_at IS NULL)
EXECUTE FUNCTION enforce_pending_outbox_capacity();

-- A terminal trigger delivery releases firing capacity even if the process
-- stops between acknowledging the outbox row and its application-level status
-- update. A diagnostic distinguishes exhausted delivery from normal dispatch.
CREATE OR REPLACE FUNCTION finalize_terminal_trigger_firing() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.delivered_at IS NULL AND NEW.delivered_at IS NOT NULL
       AND NEW.topic = 'trigger.fired' THEN
        UPDATE trigger_firings
        SET status = CASE WHEN NEW.last_error IS NULL THEN 'dispatched' ELSE 'failed' END
        WHERE tenant_id = NEW.tenant_id
          AND id = NEW.payload ->> 'firingId'
          AND status = 'pending';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER outbox_terminal_trigger_firing
AFTER UPDATE OF delivered_at ON outbox_messages
FOR EACH ROW EXECUTE FUNCTION finalize_terminal_trigger_firing();

UPDATE trigger_firings firing
SET status = CASE WHEN message.last_error IS NULL THEN 'dispatched' ELSE 'failed' END
FROM outbox_messages message
WHERE firing.tenant_id=message.tenant_id
  AND firing.id=message.payload->>'firingId'
  AND firing.status='pending' AND message.topic='trigger.fired'
  AND message.delivered_at IS NOT NULL;
