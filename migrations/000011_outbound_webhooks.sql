CREATE TABLE outbound_webhook_subscriptions (
    tenant_id text NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    id text NOT NULL,
    name text NOT NULL,
    endpoint_url text NOT NULL CHECK (char_length(endpoint_url) BETWEEN 9 AND 2048 AND endpoint_url ~ '^https://'),
    event_types text[] NOT NULL CHECK (cardinality(event_types) BETWEEN 1 AND 128),
    signing_credential_id text NOT NULL,
    status text NOT NULL DEFAULT 'enabled' CHECK (status IN ('enabled', 'disabled', 'deleted')),
    timeout_ms integer NOT NULL DEFAULT 5000 CHECK (timeout_ms BETWEEN 100 AND 10000),
    created_by text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    deleted_at timestamptz,
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, name),
    FOREIGN KEY (tenant_id, signing_credential_id)
        REFERENCES credential_refs(tenant_id, id) ON DELETE RESTRICT,
    CHECK ((status = 'deleted') = (deleted_at IS NOT NULL))
);
CREATE INDEX outbound_webhook_subscriptions_event_idx
    ON outbound_webhook_subscriptions USING gin (event_types)
    WHERE status = 'enabled';

CREATE TABLE outbound_webhook_receipts (
    tenant_id text NOT NULL,
    subscription_id text NOT NULL,
    message_id text NOT NULL,
    request_id text NOT NULL,
    status text NOT NULL CHECK (status IN ('pending', 'delivered')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    response_status integer,
    last_error text,
    first_attempt_at timestamptz NOT NULL,
    last_attempt_at timestamptz NOT NULL,
    delivered_at timestamptz,
    PRIMARY KEY (tenant_id, subscription_id, message_id),
    UNIQUE (tenant_id, request_id),
    FOREIGN KEY (tenant_id, subscription_id)
        REFERENCES outbound_webhook_subscriptions(tenant_id, id) ON DELETE RESTRICT,
    CHECK ((status = 'delivered') = (delivered_at IS NOT NULL))
);
CREATE INDEX outbound_webhook_receipts_pending_idx
    ON outbound_webhook_receipts (tenant_id, last_attempt_at, subscription_id, message_id)
    WHERE status = 'pending';

CREATE INDEX outbox_messages_delivery_topic_idx
    ON outbox_messages (tenant_id, topic, available_at, created_at, id)
    WHERE delivered_at IS NULL;
