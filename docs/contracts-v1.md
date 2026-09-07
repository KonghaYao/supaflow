# Version 1 contract freeze

The following artifacts are the compatibility boundary for Phase 0–4. Changes that alter accepted data, state meaning, event ordering, or authorization require a new version or an explicit forward migration.

- Canonical definition: `contracts/schemas/workflow-definition-v1.schema.json` and `internal/workflow` validation/canonicalization.
- Execution plan: immutable canonical node versions, executor bindings, topology and content hash produced by `workflow.Compile`.
- Node registry: `NodeType@Version`, schemas, declared capabilities/permissions, executor kind and explicit migration registry.
- Run state: `PENDING`, `RUNNING`, `SUSPENDED`, `CANCELLING`, and terminal `SUCCESS`, `FAILED`, `ERROR`, `CANCELLED`.
- Node/attempt state: logical node projection is distinct from retry/takeover attempts; the attempt fencing token is monotonic and mandatory on commits.
- Event protocol: cursor is positive, monotonic per run, replayed strictly after `afterCursor`/`Last-Event-ID`, and committed with the state it describes.
- Organization model: organization comes from an authenticated principal; all durable workflow, version, plan, run, event, output, credential, trigger and audit records carry the organization key.
- Stable transport: `contracts/openapi/openapi.yaml`; error codes are stable and never contain stack traces, SQL, internal addresses or secrets.
- Shadow execution: `/shadow-runs` requires the exact reviewed stub binding for every non-pure node; each durable attempt carries its `shadow-v1:` real-or-stub executor selection.

Published plans and historical events are immutable. Editor/FlowGram documents, database rows, OpenAPI DTOs and executor requests are adapter models and are converted at their boundaries rather than shared as domain state.
