CREATE OR REPLACE FUNCTION provision_default_retention_policy()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO retention_policies (tenant_id,event_days,output_days,archive_before_delete)
  VALUES (NEW.id,30,30,true)
  ON CONFLICT (tenant_id) DO NOTHING;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS tenants_default_retention_policy ON tenants;
CREATE TRIGGER tenants_default_retention_policy
AFTER INSERT ON tenants
FOR EACH ROW EXECUTE FUNCTION provision_default_retention_policy();

INSERT INTO retention_policies (tenant_id,event_days,output_days,archive_before_delete)
SELECT id,30,30,true FROM tenants
ON CONFLICT (tenant_id) DO NOTHING;
