# Supaflow

Supaflow is an independently deployable, multi-organization workflow platform. It owns visual drafts, immutable publication, triggers, durable runs, isolated execution, events, credentials, quota, audit, retention, and operations. Its canonical domain model is independent of FlowGram, HTTP, PostgreSQL, and executor implementations.

The repository builds separate control-plane, worker, migration, isolation-runner, migration-import, and operations commands. Fenix and other hosts integrate only through the versioned API; there is no in-process Fenix dependency or local-runtime fallback.

Start with `docs/architecture.md`, use `docs/operations.md` for deployment and verification, and use `docs/fenix-migration.md` for the one-way Phase 4 cutover.
