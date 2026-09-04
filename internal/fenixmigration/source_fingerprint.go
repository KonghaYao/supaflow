package fenixmigration

// sourceFingerprintSQL is mirrored by Fenix's cleanup gate and migration 0027.
// PostgreSQL canonicalizes every complete row through jsonb, hashes it before
// aggregation, and orders row hashes by primary key. Sensitive values therefore
// influence the digest without entering the migration report or application log.
const sourceFingerprintSQL = `
WITH table_names(table_name) AS (VALUES
  ('workflow'),('workflow_board'),('workflow_event'),('workflow_job'),
  ('workflow_node_output'),('workflow_run'),('workflow_snapshot'),
  ('workflow_trigger'),('workflow_version')
), table_rows(table_name,row_id,row_hash) AS (
  SELECT 'workflow',id::text,encode(sha256(convert_to(to_jsonb(r)::text,'UTF8')),'hex') FROM workflow r
  UNION ALL SELECT 'workflow_board',id::text,encode(sha256(convert_to(to_jsonb(r)::text,'UTF8')),'hex') FROM workflow_board r
  UNION ALL SELECT 'workflow_event',id::text,encode(sha256(convert_to(to_jsonb(r)::text,'UTF8')),'hex') FROM workflow_event r
  UNION ALL SELECT 'workflow_job',id::text,encode(sha256(convert_to(to_jsonb(r)::text,'UTF8')),'hex') FROM workflow_job r
  UNION ALL SELECT 'workflow_node_output',id::text,encode(sha256(convert_to(to_jsonb(r)::text,'UTF8')),'hex') FROM workflow_node_output r
  UNION ALL SELECT 'workflow_run',id::text,encode(sha256(convert_to(to_jsonb(r)::text,'UTF8')),'hex') FROM workflow_run r
  UNION ALL SELECT 'workflow_snapshot',id::text,encode(sha256(convert_to(to_jsonb(r)::text,'UTF8')),'hex') FROM workflow_snapshot r
  UNION ALL SELECT 'workflow_trigger',id::text,encode(sha256(convert_to(to_jsonb(r)::text,'UTF8')),'hex') FROM workflow_trigger r
  UNION ALL SELECT 'workflow_version',id::text,encode(sha256(convert_to(to_jsonb(r)::text,'UTF8')),'hex') FROM workflow_version r
), table_digests AS (
  SELECT n.table_name,count(r.row_id) AS row_count,
    encode(sha256(convert_to(COALESCE(string_agg(r.row_hash,'' ORDER BY r.row_id),''),'UTF8')),'hex') AS content_hash
  FROM table_names n LEFT JOIN table_rows r USING(table_name) GROUP BY n.table_name
)
SELECT encode(sha256(convert_to('fenix-workflow-cleanup-v1' || E'\n' ||
  string_agg(table_name || ':' || row_count || ':' || content_hash,E'\n' ORDER BY table_name),'UTF8')),'hex') AS "sourceFingerprint"
FROM table_digests`
