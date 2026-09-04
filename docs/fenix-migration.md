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

Compare source and target counts and hashes, then shadow only workflows with no external side effects or with every side-effect executor replaced by a reviewed stub. Route Fenix execution traffic to Supaflow and observe the agreed stability window.

## Destructive Fenix cleanup gate

Fenix migration `0027_remove_legacy_workflow.sql` is deliberately blocked by the production migration wrapper when a legacy Workflow table exists and the cleanup migration is still pending. The evidence must assert identical initial/final source fingerprints and nine-table database watermarks, reference both a database backup and the matching final migration report by SHA-256, and carry a signed expiry no more than one hour after signing. Immediately before Drizzle runs, the wrapper re-hashes both artifacts, verifies the report fingerprints and HMAC, and checks that the observation window has ended and the short validity window remains open. Inside Drizzle's transaction, `0027` takes a transaction-scoped advisory lock and `ACCESS EXCLUSIVE` locks on all nine legacy tables, blocks every non-terminal run, and recomputes the full source fingerprint and watermarks before the first `DROP TABLE`. Run status/output edits and board, event, job, node-output, snapshot, trigger, version, or workflow changes after evidence generation therefore fail closed without exposing their contents. Generate and mount the evidence only after the source database and YAML root are read-only. Fresh databases with no pre-existing legacy table do not require cutover evidence.

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
bun run db:migrate
```

Do not keep the signing key in application configuration. On any mismatch the migration transaction stops before the first table is dropped. After cleanup, rollback means rolling back compatible binaries or restoring Supaflow/Fenix backups with traffic stopped and an explicit acknowledgement reconciliation; no local runtime fallback remains.
