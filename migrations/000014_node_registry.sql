-- Node definitions and their executable versions are global platform
-- authority. Published versions are append-only: upgrades create a new
-- semantic version and explicit migration metadata instead of mutating rows.
CREATE TABLE node_types (
    type text PRIMARY KEY,
    display_name text NOT NULL,
    description text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (type ~ '^[a-z][a-z0-9.-]{0,127}$'),
    CHECK (char_length(display_name) BETWEEN 1 AND 200)
);

CREATE TABLE node_type_versions (
    node_type text NOT NULL REFERENCES node_types(type) ON DELETE RESTRICT,
    version text NOT NULL,
    config_schema jsonb NOT NULL,
    input_ports jsonb NOT NULL,
    output_ports jsonb NOT NULL,
    capabilities jsonb NOT NULL,
    permissions jsonb NOT NULL,
    executor_binding jsonb NOT NULL,
    sensitive_fields jsonb NOT NULL DEFAULT '[]'::jsonb,
    default_timeout_ms bigint NOT NULL CHECK (default_timeout_ms > 0),
    maximum_output_bytes bigint NOT NULL CHECK (maximum_output_bytes > 0),
    migration_metadata jsonb NOT NULL DEFAULT '[]'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (node_type, version),
    CHECK (version ~ '^[0-9]+\.[0-9]+\.[0-9]+$'),
    CHECK (jsonb_typeof(config_schema) = 'object'),
    CHECK (jsonb_typeof(input_ports) = 'object'),
    CHECK (jsonb_typeof(output_ports) = 'object'),
    CHECK (jsonb_typeof(capabilities) = 'object'),
    CHECK (jsonb_typeof(permissions) = 'object'),
    CHECK (jsonb_typeof(executor_binding) = 'object'),
    CHECK (jsonb_typeof(sensitive_fields) = 'array'),
    CHECK (jsonb_typeof(migration_metadata) = 'array'),
    CHECK (executor_binding ? 'kind' AND executor_binding ? 'minimumRunnerVersion')
);

CREATE FUNCTION reject_node_registry_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'node registry rows are immutable; insert a new version'
        USING ERRCODE = '23514';
END;
$$;

CREATE TRIGGER node_types_immutable
BEFORE UPDATE OR DELETE ON node_types
FOR EACH ROW EXECUTE FUNCTION reject_node_registry_mutation();

CREATE TRIGGER node_type_versions_immutable
BEFORE UPDATE OR DELETE ON node_type_versions
FOR EACH ROW EXECUTE FUNCTION reject_node_registry_mutation();
