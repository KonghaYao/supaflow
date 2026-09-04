# Operations

Supaflow runs as two independently scalable processes: the HTTP control plane and the orchestration worker. PostgreSQL is the durable coordination authority for both processes. Run migrations before starting either process.

The automated E2E and load suites are API-level tests and never use a browser.
The default deterministic suite uses the real Go HTTP handler without opening
a listener or requiring PostgreSQL. The explicit
`TestPostgresControlPlanePressure` production profile intentionally opens a
loopback TCP listener and requires an isolated PostgreSQL database; it crosses
database API-key authentication, RBAC, catalog, runtime and admission instead
of substituting memory adapters.

## Configuration

The production processes use these environment variables:

| Variable | Process | Required | Purpose |
| --- | --- | --- | --- |
| `DATABASE_URL` | all | yes | PostgreSQL connection string. Use TLS and a least-privilege service identity outside local development. |
| `SUPAFLOW_IDENTITY_BOOTSTRAP_ENABLED` | control plane | no | Set to `true` only during an explicit initial API-key import; disable it after the digest has been persisted. |
| `SUPAFLOW_BOOTSTRAP_API_KEYS_JSON` | control plane | with bootstrap | One-time JSON array of initial service-account keys. Raw values are hashed before insertion and are never used by the production request path. |
| `SUPAFLOW_OIDC_ENABLED` | control plane | no | Enables OIDC Authorization Code + PKCE login. When `true`, every OIDC variable below is validated and startup fails closed. |
| `SUPAFLOW_OIDC_ISSUER_URL` | control plane | with OIDC | Exact HTTPS issuer used for discovery and ID-token issuer validation. |
| `SUPAFLOW_OIDC_CLIENT_ID` | control plane | with OIDC | Registered OIDC client ID and ID-token audience. |
| `SUPAFLOW_OIDC_CLIENT_SECRET` | control plane | provider-dependent | Client secret injected by a secret manager; may be empty for a public PKCE client if the provider allows it. |
| `SUPAFLOW_OIDC_REDIRECT_URL` | control plane | with OIDC | Registered HTTPS callback URL ending in `/auth/oidc/callback`. |
| `SUPAFLOW_OIDC_SUCCESS_REDIRECT` | control plane | no | Local absolute path after login; defaults to `/studio/`. |
| `SUPAFLOW_RESUME_TOKEN_SECRET` | all | yes | Shared HMAC secret of at least 32 bytes used to derive approval resume tokens. Every control-plane and worker replica must use the same value. |
| `SUPAFLOW_ADDR` | control plane | no | Listen address; defaults to `127.0.0.1:8080`. |
| `SUPAFLOW_WORKER_ID` | worker | no | Stable, unique identity for one worker process. A random ID is generated when absent; set it explicitly in production for diagnostics. Never share it between live replicas. |
| `SUPAFLOW_WORKER_TENANTS` | worker | yes | Comma-separated allowlist of tenant IDs this worker polls. |
| `SUPAFLOW_HTTP_ALLOWED_HOSTS` | worker | yes | Comma-separated exact hostname/IP allowlist for HTTP nodes. Empty configuration fails closed. |
| `SUPAFLOW_WEBHOOK_EGRESS_ALLOWLIST` | control plane | no | Comma-separated DNS suffix allowlist for outbound event webhooks. When empty, public HTTPS destinations are permitted; private, loopback, link-local, multicast and rebinding targets remain blocked. |
| `SUPAFLOW_CREDENTIAL_MASTER_KEY` | control plane, worker | yes | Exactly 32 bytes encoded as hex or base64. Deployment-injected local envelope key; use an external KMS provider in managed deployments. |
| `SUPAFLOW_CREDENTIAL_KEY_ID` | control plane, worker | yes | Non-secret identifier for the active envelope key version. |
| `SUPAFLOW_EINO_MODELS_JSON` | worker | no | Trusted model templates (`name`, `credentialRef`, `model`, optional `baseUrl`/`timeoutMs`). Any configured `baseUrl` must be HTTPS, including loopback; model clients reject every redirect. Provider keys are resolved from attempt grants, never this JSON. |
| `SUPAFLOW_EINO_TOOLS_JSON` | worker | no | Trusted built-in tool bindings (`name`, `kind`). Supported kinds are `json.project` and `json.merge`; workflow definitions cannot upload tool code. |
| `SUPAFLOW_EINO_AGENTS_JSON` | worker | no | Fixed bounded agents (`name`, `model`, `tools`, `system`, `maxSteps`); every reference is validated before the worker claims attempts. |
| `SUPAFLOW_ISOLATION_BINARY` | worker | yes | Absolute path to the separately built `cmd/isolation` helper; worker startup fails closed without it. |
| `SUPAFLOW_ISOLATION_CGROUP_ROOT` | isolation helper | yes | Absolute delegated cgroup-v2 subtree below `/sys/fs/cgroup`; it must expose enabled `pids` and `memory` controllers. One child cgroup is created per fenced attempt. |
| `SUPAFLOW_ISOLATION_WORKSPACE_ROOT` | isolation helper | yes | Absolute scratch root used only for the bounded source file; production code writes inside a private bwrap tmpfs. |
| `SUPAFLOW_ISOLATION_MAX_PIDS` | worker | no | Per-attempt `pids.max`; defaults to 32 and cannot exceed 32. This deliberately does not use UID-global `RLIMIT_NPROC`. |
| `SUPAFLOW_ISOLATION_MEMORY_BYTES` | worker | no | Per-attempt cgroup `memory.max` and address-space ceiling; defaults to and cannot exceed 256 MiB. |
| `SUPAFLOW_ISOLATION_WORKSPACE_BYTES` | worker | no | Hard bwrap tmpfs byte ceiling; defaults to and cannot exceed 16 MiB. |
| `SUPAFLOW_ISOLATION_WORKSPACE_INODES` | worker | no | Per-attempt inode ceiling monitored every 10 ms; defaults to and cannot exceed 1024. Exceeding it kills the whole cgroup. |
| `SUPAFLOW_OBJECT_STORE_DIR` | control plane, worker | no | Traversal-safe local object-store root; defaults to `./data/objects`. Every replica must share the same durable volume; replace this adapter for distributed deployments. |
| `SUPAFLOW_OUTPUT_MAX_BYTES` | worker | no | Hard bound for one externally stored node output; defaults to 64 MiB and cannot exceed 1 GiB. Outputs larger than a node's inline limit or marked `sensitiveOutput` are stored externally. |
| `SUPAFLOW_OTEL_ENDPOINT` | control plane, worker | no | OTLP gRPC endpoint. When set, real trace and metric exporters are enabled. |
| `SUPAFLOW_TENANT_HASH_KEY` | control plane | with OTLP | Exactly 32 bytes encoded as hex or base64, used for privacy-preserving tenant attribution. |
| `SUPAFLOW_OTEL_INSECURE` | control plane | no | Set to `true` only for a trusted plaintext OTLP development endpoint. |
| `SUPAFLOW_DELIVERY_BATCH` | control plane | no | Total outbox claims per delivery tick; defaults to 100 and is capped at 1000. Work is rotated across active tenants. |
| `SUPAFLOW_DELIVERY_CONCURRENCY` | control plane | no | Maximum simultaneous outbound deliveries; defaults to 4 and is capped at 64. |
| `SUPAFLOW_DELIVERY_TIMEOUT_MS` | control plane | no | Per-delivery deadline including DNS/connect/write/read; defaults to 20000 and is capped at 30000. |
| `SUPAFLOW_SSE_GLOBAL_LIMIT` | control plane | no | Process-wide concurrent event-stream cap; defaults to 512. Size ingress routing across replicas accordingly. |
| `SUPAFLOW_SSE_TENANT_LIMIT` | control plane | no | Per-process, per-tenant concurrent event-stream cap; defaults to 64 and cannot exceed the global cap. |
| `SUPAFLOW_SSE_POLL_MIN_MS` | control plane | no | Minimum empty-stream database polling delay; defaults to 250. |
| `SUPAFLOW_SSE_POLL_MAX_MS` | control plane | no | Maximum exponential empty-stream polling delay; defaults to 2000. |
| `SUPAFLOW_SSE_WRITE_TIMEOUT_MS` | control plane | no | Deadline for every event or heartbeat socket write; defaults to 5000 and is capped at 30000. |

Authenticated HTTP telemetry is installed inside the identity boundary: successful requests use an HMAC-derived tenant hash, rejected authentication remains anonymous, and HTTP status codes drive the span/metric outcome. Public webhook telemetry starts one request span before routing, attributes it only after `publicHash` resolves to a server-owned tenant, and leaves unresolved or early-invalid requests anonymous. Raw tenant IDs, API keys, cookies, and request bodies are never attached to telemetry.

The explicit bootstrap JSON has this exact shape:

```json
[
  {
    "key": "replace-with-at-least-24-random-bytes",
    "tenantId": "tenant-a",
    "actorId": "service-a",
    "role": "admin"
  }
]
```

Valid roles are `viewer`, `editor`, `publisher`, `operator`, `approver`, and `admin`. A bootstrap tenant must already exist. Enable bootstrap for one controlled start, verify the service account and API-key digest, then remove `SUPAFLOW_BOOTSTRAP_API_KEYS_JSON` and disable the switch. The old `SUPAFLOW_API_KEYS_JSON` is rejected so production cannot silently fall back to a static tenant/role map.

Normal requests authenticate through a database API-key digest or the `__Host-supaflow_session` HttpOnly, Secure, SameSite=Lax cookie. API-key principals are joined through an active service account and active tenant membership; sessions are joined through an active OIDC identity and membership on every request. Expiration, credential revocation, membership revocation, identity disablement, service-account disablement, and tenant suspension therefore take effect without restarting the process. Tenant and role are always read from these joins. `X-Tenant-ID`, request body fields, and OIDC profile claims cannot assign either value; the optional OIDC `tenant` query parameter only selects a membership that must already exist server-side.

OIDC login is `GET /auth/oidc/start?tenant=<membership-tenant>`, with callback at `GET /auth/oidc/callback`; logout is `POST /auth/logout`. The flow uses a one-time state digest, browser-bound HttpOnly cookie, nonce digest, and S256 PKCE. Provider discovery, token endpoints, redirect URLs, ID-token signatures, issuer, audience, expiry, issued-at, nonce, and allowed algorithms are validated. Configuration or discovery errors stop startup when OIDC is enabled.

Keep bootstrap keys, OIDC client secrets, and the resume-token secret in a secret manager. Do not commit them, put them in images, or log connection strings, bearer keys, cookies, authorization codes, PKCE verifiers, or raw resume tokens. Only API-key/session/state/browser/nonce digests are durable credentials; the PKCE verifier is short-lived and atomically deleted before exchange.

For local development only:

```bash
docker compose up -d postgres
export DATABASE_URL='postgres://supaflow:local-development-only@127.0.0.1:55432/supaflow?sslmode=disable'
export SUPAFLOW_IDENTITY_BOOTSTRAP_ENABLED='true'
export SUPAFLOW_BOOTSTRAP_API_KEYS_JSON='[{"key":"local-api-key-at-least-24-bytes","tenantId":"tenant-a","actorId":"local-admin","role":"admin"}]'
export SUPAFLOW_RESUME_TOKEN_SECRET='local-resume-token-secret-at-least-32-bytes'
export SUPAFLOW_CREDENTIAL_MASTER_KEY='0707070707070707070707070707070707070707070707070707070707070707'
export SUPAFLOW_CREDENTIAL_KEY_ID='local-key-1'
export SUPAFLOW_ADDR='127.0.0.1:8080'
export SUPAFLOW_WORKER_ID='local-worker-1'
export SUPAFLOW_WORKER_TENANTS='tenant-a'
export SUPAFLOW_HTTP_ALLOWED_HOSTS='example.com'
export SUPAFLOW_WEBHOOK_EGRESS_ALLOWLIST='events.example.com'
export SUPAFLOW_EINO_MODELS_JSON='[{"name":"chat","credentialRef":"cred_provider","model":"gpt-5"}]'
export SUPAFLOW_EINO_TOOLS_JSON='[{"name":"json-project","kind":"json.project"}]'
export SUPAFLOW_EINO_AGENTS_JSON='[{"name":"analyst","model":"chat","tools":["json-project"],"system":"Use verified structured values only.","maxSteps":4}]'
export SUPAFLOW_ISOLATION_BINARY='/absolute/path/to/supaflow-isolation'
export SUPAFLOW_ISOLATION_CGROUP_ROOT='/sys/fs/cgroup/supaflow'
export SUPAFLOW_ISOLATION_WORKSPACE_ROOT='/var/lib/supaflow/isolation'
export SUPAFLOW_OBJECT_STORE_DIR='./data/objects'
export SUPAFLOW_OUTPUT_MAX_BYTES='67108864'
```

## Migration and startup

The published Dockerfile has separate `control-plane`, `worker-runtime`, and `migration` targets. The control plane remains distroless and never contains bwrap, a shell, Python, or the isolation helper. `worker-runtime` contains Debian's `bubblewrap`, `/bin/sh`, Python 3, the worker, and the helper. Worker startup runs Shell and Python through the real boundary and exits before claiming work if bwrap, unprivileged namespaces, cgroup attachment, the private tmpfs, or an interpreter is unavailable.

Before starting `docker-compose.prod.yml`, create a dedicated cgroup-v2 subtree on the host and delegate only that subtree to UID 65532. The parent must have `memory` and `pids` in `cgroup.subtree_control`; do not bind the host cgroup root or run the worker privileged. Set `SUPAFLOW_CGROUP_HOST_PATH` to that exact directory. In a systemd deployment, use a delegated service/slice (`Delegate=memory pids`) and point the bind mount at the resulting empty child subtree. The compose file mounts only this subtree read-write at `/sys/fs/cgroup/supaflow`, drops every container capability, enables `no-new-privileges`, keeps the root filesystem read-only, and provides a bounded scratch tmpfs.

Each attempt is atomically born into its own cgroup using `CLONE_INTO_CGROUP`, so it cannot fork before `pids.max` applies. `memory.max`, `RLIMIT_CPU`, `RLIMIT_AS`, `RLIMIT_NOFILE`, and `RLIMIT_FSIZE` provide the remaining hard ceilings. bwrap creates a private PID/network/mount namespace, a read-only `/usr` and `/bin`, and a byte-bounded tmpfs workspace. Inode use is sampled every 10 ms through the sandbox mount namespace; this is a kill-on-violation guard rather than a filesystem project quota, so production must also alert on the worker scratch filesystem and keep its compose tmpfs bound. Cancellation sends `SIGTERM` to the helper and sandbox process groups, waits 1.5 seconds for cleanup, then sends `SIGKILL`; the attempt cgroup is killed and removed before the request returns.

Generate the checked-in node contract and build the Studio plus all production binaries from a clean checkout:

```bash
make build
```

`make build` uses `npm ci`, builds the Studio into the embedded `web/dist` directory, regenerates `contracts/schemas/builtin-node-types-v1.json`, and writes binaries under `bin/`. CI runs the generator and rejects contract drift. Apply every forward migration before rolling out application binaries:

```bash
go run ./cmd/migrate -dir migrations
```

The migrator serializes concurrent invocations with a PostgreSQL advisory lock, records checksums in `schema_migrations`, rejects an applied migration whose name or content changed, and idempotently seeds every built-in `NodeType@Version` into the append-only PostgreSQL registry. Control-plane and worker startup verify the same durable projection before accepting traffic or claiming attempts. Never edit an applied migration or registry version; add a new forward migration or node version for every correction.

Start the built processes in separate supervisors or terminals:

```bash
./bin/control-plane
./bin/worker
```

The control plane must be reachable only through the intended ingress and must not expose its local development bind address as a production security boundary. Check liveness and readiness independently:

```bash
curl --fail http://127.0.0.1:8080/health/live
curl --fail http://127.0.0.1:8080/health/ready
```

The readiness probe verifies that the process can reach PostgreSQL; it does not indicate whether workers are keeping up. Production monitoring must also alert on runnable-attempt age, expired leases, terminal failure rate, outbox backlog, database pool saturation, and SSE disconnect/error rate.

The control plane also mounts authenticated operations APIs at `/ops/*` and the signature-authenticated public webhook at `POST /hooks/{publicHash}`. The inbound webhook HMAC input is `v1.<request-id>.<unix-seconds>.<raw-body>` and uses `X-Supaflow-Request-ID`, `X-Supaflow-Timestamp`, and `X-Supaflow-Signature: v1=<hex-sha256>`. This is the initial public Supaflow contract; signatures generated without the request ID are rejected rather than accepted through a downgrade path. Outbound subscriptions are managed at `/ops/webhook-subscriptions`; they reference a credential with purpose `outbound_webhook_signing` and never accept or return plaintext signing material. Outbound signatures cover `<unix-seconds>.<stable-request-id>.<exact-body>`. Receivers must deduplicate `X-Supaflow-Request-ID`: delivery is at least once and a process can fail after the receiver commits but before the durable receipt is marked delivered.

Every `/ops` mutation requires `Idempotency-Key`. The durable ledger stores a stable
`operation_id`, lease owner/expiry, request hash, audit evidence, and the exact completed HTTP
response. An expired owner may be replaced, while an old owner is fenced from completion.
The crash-window recovery contract for each mutation family is:

| Mutation | Durable side-effect recovery |
| --- | --- |
| quota and retention policy | tenant-keyed upsert |
| trigger create/update/state/delete | `trigger_mutations` request hash and stored result |
| trigger fire and event receive | firing/event deduplication keys in the trigger transaction |
| credential create | resource ID derived from ledger `operation_id`; duplicate insert returns the existing reference |
| credential rotate/revoke | requested version and terminal revoked state are replay-safe |
| tenant export | export/object IDs derived from `operation_id`; an existing export is returned before rebuilding bytes |
| outbound webhook create/state/delete | stable subscription ID plus idempotent lifecycle transitions |
| DLQ requeue | stable outbox ID plus the durable `requeued_at` marker |
| worker drain | tenant/worker-keyed terminal draining state |

The offline Fenix importer is not an `/ops` route. It independently fingerprints source triggers
and runs and rejects a conflicting replay. No mutation may add a random resource ID without also
adding an operation-derived identity or a repository-local durable result record.

An expired attempt lease is not permission to repeat an ambiguous side effect. Workers only re-execute node types whose immutable plan capability declares `recoverySafe`. Built-in LLM, Agent, Tool, Shell and Python nodes fail closed with `unsafe_lease_takeover`; HTTP is recovery-safe only for GET, HEAD, PUT and DELETE, or when the author explicitly sets `retrySafe: true`. Operators must reconcile the external effect before starting a deliberate replacement run.

The control-plane supervisor runs schedule/replay, deadline, delivery, and retention as four independent ticker loops, so a slow webhook cannot block deadline enforcement. Schedule polling uses a durable database-clock tenant cursor, serves at most one due schedule per tenant in each round, and repeats round-robin rounds up to the global batch limit while processing each trigger at most once per tick. Tenants commit independently and a saturated tenant is removed from later rounds, so a large `catch_up` history or exhausted ingress capacity cannot roll back or starve another tenant. Event fan-out uses pages of at most 100 trigger rows and commits each page independently. PostgreSQL serializes trigger, pending firing, and pending outbox admission on the tenant quota row, so every producer observes the same hard limits. Delivery rotates its first tenant, caps the total tick batch and concurrency, applies a per-call deadline, and renews an owned outbox lease while the network call is in flight. `trigger.fired` is routed to the internal run starter; run, node, attempt, stream, approval and terminal topics are routed to matching outbound subscriptions. Successful per-subscription receipts prevent already-completed fan-out targets from being repeated when a sibling target retries. External calls use bounded exponential retry and DLQ, disable redirects, cap request/response bodies and timeouts, resolve DNS at connect time, and reject non-public addresses. Credential-bearing HTTP nodes additionally require HTTPS and reject redirects; the exact host allowlist cannot weaken this transport rule. Every loop waits on a ticker, observes context cancellation, and logs redacted failures. Workers register durable heartbeats, honor remote drain state before claiming, and release their worker quota counter during graceful exit.

The default trigger limits are 1,000 definitions, 5,000 pending firings, and 20,000 pending outbox messages per tenant; administrators can lower or raise them within the schema's absolute ceilings. Capacity rejection is retryable and never falls back to another tenant or an in-memory queue. Delivered outbox messages and dispatched/failed firings are removed in bounded tenant-fair retention batches after seven days. The same seven-day interval is the API/manual/event firing idempotency window, so cleanup never deletes a deduplication key that is still contractually reusable; pending delivery rows and pending firings are never purged by this maintenance path.

Event streams reject excess global or tenant connections with `429` and `Retry-After`. Empty streams exponentially back off database polling, every socket write has a deadline, and request cancellation releases the quota immediately. Operators should alert on `429` rate and disconnects, and size the process limits below the database connection and ingress capacities.

Run capacity/alert and backup/restore drills with the production `ops` command. Restore always requires a distinct scratch database URL and never accepts a connection string on the command line:

```bash
go run ./cmd/ops -action capacity -tenant tenant-a
go run ./cmd/ops -action alerts -tenant tenant-a
SUPAFLOW_BACKUP_ROOT=./data/backups go run ./cmd/ops -action backup -artifact nightly.dump
SUPAFLOW_RESTORE_DATABASE_URL='postgres://scratch-host/scratch' go run ./cmd/ops -action restore -artifact nightly.dump
```

## Verification

Run all commands from `packages/supaflow`:

```bash
go test ./...
go test -race ./...
go vet ./...
go test ./tests/e2e
go test ./tests/deployment
```

The E2E suite covers immutable publish, tenant-scoped idempotent start replay, `Start -> Human Approval -> End`, role-gated token delivery, lost-response approval replay and conflicting-key rejection, bounded worker advancement, success, cancellation, RBAC, two-tenant run/event/interrupt isolation, and SSE continuation through `Last-Event-ID` and `afterCursor`. It uses `httptest.ResponseRecorder` and a deadline-bound request context, so it is safe in sandboxes that prohibit binding network ports.

Run the in-process authenticated API benchmark without any browser or listening socket:

```bash
go test -run '^$' -bench=. -benchmem ./tests/load/...
go test -run '^$' -bench=BenchmarkStartRunHTTP -benchmem -benchtime=10s -count=5 -cpu=1,4,8 ./tests/load/...
```

The benchmark drives concurrent JSON requests through the actual auth, RBAC, plan resolution, idempotency, runtime service, and HTTP response path. Each start request gets a unique idempotency key. Record `ns/op`, `B/op`, and `allocs/op` with the commit, Go version, CPU count, and `-benchtime`; compare results only on equivalent machines. This benchmark intentionally uses the memory adapters to isolate API overhead. Database throughput, lock contention, connection-pool sizing, SSE connection capacity, and worker saturation require a separate staging test against PostgreSQL with production-equivalent limits.

Run the real-TCP PostgreSQL contention profile separately so its 1,000
simultaneous clients do not distort unit-test timing:

```bash
export SUPAFLOW_TEST_DATABASE_URL='postgres://least-privilege-user@127.0.0.1:55432/supaflow_pressure?sslmode=disable'
go test -timeout=120s -run '^TestPostgresControlPlanePressure$' -count=1 -v ./tests/load
```

The database must be disposable and isolated from development and production
data. The test applies the immutable migration chain, creates a unique tenant,
database API key, service account and membership, uses a 32-connection pool,
then releases 1,000 TCP starts simultaneously. It also performs exactly 2,000
bounded worker advances and requires all 1,000 Runs to be successful with both
admission counters returned to zero. A missing
`SUPAFLOW_TEST_DATABASE_URL` is an intentional skip; release evidence must set
it and retain the `profile=postgres-tcp` and `profile=postgres-worker-drain`
log lines.

Before a production release, also build every command to catch command-only dependency errors:

```bash
go build ./cmd/...
docker build --target control-plane -t supaflow-control-plane .
docker build --target worker-runtime -t supaflow-worker .
docker build --target migration -t supaflow-migration .
docker compose -f docker-compose.prod.yml config
```

Inspect the worker artifact rather than inferring its contents from the
Dockerfile. It must run as UID/GID 65532 and contain executable `bwrap`,
`/bin/sh`, `python3`, `/usr/local/bin/worker`, and
`/usr/local/bin/isolation`. Conversely, an exported control-plane rootfs must
not contain the shell, Python, bwrap or the isolation helper. The helper's
startup probe is expected to exit 125 when no delegated cgroup subtree is
mounted; this is the required fail-closed behavior, not a successful isolation
smoke test. A complete smoke test requires the production cgroup bind mount and
must make `--supaflow-capability-check` exit zero before the worker may claim
attempts.

## Recovery and rolling upgrades

PostgreSQL, not process memory, owns non-terminal run state, attempts, snapshots, interrupts, events, leases, and fencing-token high-water marks. After a process crash:

1. Restore database availability before restarting control-plane or worker replicas.
2. Restart the same known-good binary/configuration and wait for existing leases to expire naturally.
3. Let workers reclaim attempts through the normal claim path. Recovery-safe nodes resume automatically; `unsafe_lease_takeover` requires external-effect reconciliation and a deliberate replacement run. Never delete leases, rewrite run status, or lower fencing tokens manually.
4. Confirm event cursors remain monotonic, outbox backlog drains, and suspended approvals remain pending before restoring normal traffic.

For a rolling upgrade, apply only backward-compatible expand migrations first, then roll control-plane replicas, then workers. Keep old and new binaries compatible with the current schema for the entire rollout. Contracting migrations belong in a later release after every old binary has drained and retained data no longer needs the old shape.

Keep `SUPAFLOW_RESUME_TOKEN_SECRET` identical across control-plane and worker replicas for the full lifetime of pending approvals. Rotating it immediately invalidates their reconstructable tokens. Drain or resolve pending approvals before a coordinated rotation, or retain the old issuer through an explicitly designed multi-key rotation release.

On `SIGTERM`, stop routing new control-plane requests and allow its bounded graceful-shutdown window to finish in-flight requests. The worker stops its polling loop when its signal context is cancelled; any active executor receives the cancellation signal. Give the process enough termination grace to persist that outcome. During execution the orchestrator renews its 30-second lease every 10 seconds. If renewal fails, it cancels the executor and does not commit with the stale fencing token. If the process is forcibly terminated, the lease must expire before another worker takes over; fencing prevents the old worker from committing after takeover.

## Rollback

Application rollback means redeploying the previous binary while preserving PostgreSQL state. Verify the previous binary understands every already-applied schema and every immutable plan/node version that can still be in flight. Do not roll back by restoring an older database snapshot while accepting traffic: that can reuse idempotency keys, event cursors, lease tokens, or approval tokens.

SQL migrations are forward-only. If a migration causes an incident, stop affected writers, deploy a reviewed compensating forward migration, and then roll binaries as needed. Restore a database backup only as a disaster-recovery operation with traffic stopped and an explicit reconciliation plan for requests acknowledged after the backup point.
