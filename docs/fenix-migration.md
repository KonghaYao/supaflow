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

Remove `-dry-run` only after every blocker is resolved. Workflow identity, name, and description are imported first with an immutable source fingerprint; published versions then import in contiguous source order, while drafts, triggers and terminal run projections use independent idempotency identities. Re-running the exact snapshot is safe and is reported as replay; a changed name or description under the same source identity is a conflict rather than a silent overwrite. The command reloads the source after import and fails when its watermark or fingerprint changed during the run. `sourceDatabaseFingerprint` is the versioned cleanup digest: PostgreSQL canonicalizes and hashes every complete row from all nine legacy Workflow tables, then aggregates row hashes in primary-key order. `sourceFingerprint` additionally commits to that database digest, the complete database watermark, and every exact Workflow YAML byte sequence, trigger and run in the loaded snapshot. Secrets and outputs affect the database digest, but their raw values never leave PostgreSQL or enter the report.

The final non-dry-run report contains the durable target installation UUID plus the v2 canonical digest and counts for every Fenix-imported workflow, definition, draft, trigger, and run. The digest covers complete workflow/import-marker rows, immutable versions and plans, draft mutation evidence, triggers, and each imported run with its events, node states, node attempts, and outputs. The migration fails rather than emitting a cleanup-capable report when these target counts differ from the source plan. Record the production UUID from `SELECT installation_id FROM platform_installations` and keep it in deployment configuration; do not substitute a staging UUID. Compare source and target counts and hashes, then shadow only workflows with no external side effects or with every side-effect executor replaced by a reviewed stub. Route Fenix execution traffic to Supaflow and observe the agreed stability window.

## Destructive Fenix cleanup gate

Fenix migration `0027_remove_legacy_workflow.sql` is deliberately blocked by the production migration wrapper when a legacy Workflow table exists and the cleanup migration is still pending. The evidence must assert identical initial/final source fingerprints and nine-table database watermarks, bind the production Supaflow installation UUID, canonical imported-content digest and counts, reference both a database backup and the matching final migration report by SHA-256, and carry a signed expiry no more than one hour after signing. Immediately before Drizzle runs, the wrapper re-hashes both artifacts, verifies the report fingerprints and HMAC, and checks that the observation window has ended and the short validity window remains open.

The wrapper must then acquire the production target's durable cutover seal by passing every signed target field and an escrowed 32-byte random token to `acquire_fenix_cutover_seal_v2`. Acquisition takes write-conflicting locks on all attested roots and supporting records (`workflows`, workflow import markers, drafts, versions, plans, definition mutations, triggers, runs, events, node states, node attempts, and outputs), re-evaluates `fenix_target_attestation_v2()`, and commits the singleton token hash and write barrier together. A writer already in flight completes before the verification snapshot; every later mutation that could change the attestation fails closed while the seal is active. The function returns the attestation it actually sealed, which the wrapper must compare to the evidence before entering Drizzle. A signed staging report, changed target record, wrong count, or wrong installation therefore fails closed without a target TOCTOU window. `fenix_target_attestation_v1()` remains available only to verify older evidence and is not accepted for a new cutover.

Create a dedicated login/group for this one operation. Migration ownership remains separate; normal control-plane, worker, read-only, and application roles receive neither direct seal-table access nor function execution:

```sql
CREATE ROLE fenix_cutover_operator NOLOGIN;
GRANT EXECUTE ON FUNCTION acquire_fenix_cutover_seal_v2(uuid,text,bigint,bigint,bigint,bigint,bigint,text)
  TO fenix_cutover_operator;
GRANT EXECUTE ON FUNCTION release_fenix_cutover_seal_v2(uuid,text)
  TO fenix_cutover_operator;
```

Generate at least 32 random bytes (`openssl rand -hex 32`) and store the plaintext token only in the cutover secret escrow. PostgreSQL stores only its SHA-256 digest. Keep the seal active while `0027` takes its transaction-scoped advisory lock and `ACCESS EXCLUSIVE` locks on all nine legacy tables, blocks every non-terminal run, and recomputes the full source fingerprint and watermarks before the first `DROP TABLE`. Run status/output edits and board, event, job, node-output, snapshot, trigger, version, or workflow changes after evidence generation therefore fail closed without exposing their contents. Generate and mount the evidence only after the source database and YAML root are read-only.

After `0027` commits and the operator has verified that Fenix no longer serves Workflow traffic, call `release_fenix_cutover_seal_v2(expected_installation_id, seal_token)` and destroy the temporary operator login and token escrow. Release is idempotent with the same token; an installation or token mismatch is rejected. Fresh Fenix databases with no pre-existing legacy table do not require cutover evidence or a target seal.

If the cleanup process crashes, leave the target sealed. The source transaction rolls back, while the durable target barrier remains active; restart with the same evidence and escrowed token, and the acquire call safely replays. Do not release merely to restore writes. For manual recovery, first prove whether `0027` committed, stop both systems' Workflow traffic, verify the signed source evidence and the sealed row's installation/hash/counts, then either resume cleanup or use the same escrowed token to release. A lost token has no bypass through the operator API: keep the system fail-closed and escalate to the database-owner disaster-recovery procedure with backup reconciliation and audited change approval.

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
export RCS_SUPAFLOW_PRODUCTION_DATABASE_URL='postgres://cutover_operator:secret@production-db:5432/supaflow'
export RCS_SUPAFLOW_CUTOVER_SEAL_TOKEN='<at-least-32-random-bytes-from-secret-escrow>'
bun run db:migrate
```

The target database role must be limited to connecting and executing the two seal functions. It needs no `SELECT` or mutation privilege on Workflow tables. Do not keep the signing key, seal token, or operator URL in normal application configuration. On any mismatch the migration transaction stops before the first table is dropped and the target remains sealed if acquisition already committed. After cleanup, rollback means rolling back compatible binaries or restoring Supaflow/Fenix backups with traffic stopped and an explicit acknowledgement reconciliation; no local runtime fallback remains.
