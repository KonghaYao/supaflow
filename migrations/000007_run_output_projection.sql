ALTER TABLE runs ADD COLUMN output_id text;

ALTER TABLE runs
    ADD CONSTRAINT runs_output_fk
    FOREIGN KEY (tenant_id, output_id)
    REFERENCES node_outputs(tenant_id, id)
    ON DELETE RESTRICT;
