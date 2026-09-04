# Fenix cutover runbook

The migration is a one-way, restartable import into Supaflow. It is not a dual-write mode. Freeze Fenix workflow create/update/publish/trigger changes before the first production import and keep the source database and definition root read-only until the final fingerprint is recorded.

## Mapping

Provide a strict JSON mapping file. Every source organization needs an explicit target tenant. HTTP credential, webhook signing credential, and subworkflow references must already exist in Supaflow; raw Fenix secrets are never imported.

```json
{
  "tenants": { "fenix-organization-id": "supaflow-tenant-id" },
  "httpNodeCredentials": {
    "workflow-id": { "node-id": "credential-reference-id" }
  },
  "subworkflows": {
    "child-workflow-id": { "planId": "immutable-plan-id", "planHash": "sha256-hex" }
  },
  "triggerCredentials": { "trigger-id": "signing-credential-reference-id" }
}
```

The converter supports static Fenix Shell, Python, Agent, API, approval/audit, subworkflow, transform and End semantics and adds canonical Start/End nodes when required. Dynamic host paths, environment injection, arbitrary expressions, implicit secret copying, custom tools, nested loops, unbounded retry, unsupported output storage or ambiguous dependencies are preflight blockers. Resolve blockers by publishing an explicit canonical design; never weaken the converter to guess behavior.

## Dry run and import

Run migrations for Supaflow, create target tenants/credentials, then execute:

```bash
fenix-migrate \
  -source-database-url "$FENIX_DATABASE_URL" \
  -target-database-url "$DATABASE_URL" \
  -source-workflow-root "$FENIX_WORKFLOW_ROOT" \
  -mapping ./cutover-mapping.json \
  -report ./migration-report.json \
  -actor migration-admin \
  -dry-run
```

Remove `-dry-run` only after every blocker is resolved. Published versions import in contiguous source order; drafts, triggers and terminal run projections use independent idempotency identities. Re-running the exact snapshot is safe and is reported as replay. The command reloads the source after import and fails when its watermark or fingerprint changed during the run. `sourceDatabaseFingerprint` is the versioned cleanup digest: PostgreSQL canonicalizes and hashes every complete row from all nine legacy Workflow tables, then aggregates row hashes in primary-key order. `sourceFingerprint` additionally commits to that database digest, the complete database watermark, and every exact Workflow YAML byte sequence, trigger and run in the loaded snapshot. Secrets and outputs affect the database digest, but their raw values never leave PostgreSQL or enter the report.

The final non-dry-run report contains the durable target installation UUID plus a versioned canonical digest and counts for every Fenix-imported definition, draft, trigger, and run. The migration fails rather than emitting a cleanup-capable report when these target counts differ from the source plan. Record the production UUID from `SELECT installation_id FROM platform_installations` and keep it in deployment configuration; do not substitute a staging UUID. Compare source and target counts and hashes, then shadow only workflows with no external side effects or with every side-effect executor replaced by a reviewed stub. Route Fenix execution traffic to Supaflow and observe the agreed stability window.

## Destructive Fenix cleanup gate

Fenix migration `0027_remove_legacy_workflow.sql` is deliberately blocked by the production migration wrapper when a legacy Workflow table exists and the cleanup migration is still pending. The evidence must assert identical initial/final source fingerprints and nine-table database watermarks, bind the production Supaflow installation UUID, canonical imported-content digest and counts, reference both a database backup and the matching final migration report by SHA-256, and carry a signed expiry no more than one hour after signing. Immediately before Drizzle runs, the wrapper re-hashes both artifacts, verifies the report fingerprints and HMAC, and checks that the observation window has ended and the short validity window remains open. It then uses the separately configured production UUID and a least-privilege PostgreSQL URL in a `REPEATABLE READ READ ONLY` transaction to query `fenix_target_attestation_v1()`. A signed staging report, a changed target record, or the wrong database therefore fails closed. Inside Drizzle's transaction, `0027` takes a transaction-scoped advisory lock and `ACCESS EXCLUSIVE` locks on all nine legacy tables, blocks every non-terminal run, and recomputes the full source fingerprint and watermarks before the first `DROP TABLE`. Run status/output edits and board, event, job, node-output, snapshot, trigger, version, or workflow changes after evidence generation therefore fail closed without exposing their contents. Generate and mount the evidence only after the source database, YAML root, and production target import are read-only. Fresh databases with no pre-existing legacy table do not require cutover evidence.

Create the signed, exclusive evidence file from the final non-dry-run report and matching backup. The generator revalidates report completeness, hashes both artifacts, binds the composite and database fingerprints separately, writes with mode `0600`, and refuses to overwrite an existing file:

```bash
export RCS_WORKFLOW_CUTOVER_EVIDENCE_HMAC_KEY='<hex-or-base64-secret-at-least-32-bytes>'
bun run workflow:cutover:evidence -- \
  --report /secure/migration-report.json \
  --backup /secure/fenix-workflow-backup.dump \
  --output /secure/cutover-evidence.json \
  --frozen-at '2026-09-04T00:00:00Z' \
  --stable-window-ended-at '2026-09-04T02:00:00Z' \
  --expires-at '2026-09-04T03:00:00Z'
```

Configure only for the dedicated cleanup run:

```bash
export RCS_WORKFLOW_CUTOVER_EVIDENCE_FILE=/secure/cutover-evidence.json
export RCS_WORKFLOW_CUTOVER_EVIDENCE_HMAC_KEY='<injected-secret>'
export RCS_SUPAFLOW_PRODUCTION_INSTALLATION_ID='<production-platform-installation-uuid>'
export RCS_SUPAFLOW_PRODUCTION_DATABASE_URL='postgres://read_only_attestor:secret@production-db:5432/supaflow'
bun run db:migrate
```

The target database role must be limited to connecting and selecting the attestation function's referenced tables; the wrapper additionally opens a read-only transaction. Do not keep the signing key or attestation URL in normal application configuration. On any mismatch the migration transaction stops before the first table is dropped. After cleanup, rollback means rolling back compatible binaries or restoring Supaflow/Fenix backups with traffic stopped and an explicit acknowledgement reconciliation; no local runtime fallback remains.
