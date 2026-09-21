# Operations

Supaflow runs as two independently scalable processes: the HTTP control plane and the orchestration worker. PostgreSQL is the durable coordination authority for both processes. Run migrations before starting either process.

The automated E2E and load suites are API-level tests and never use a browser.
The default deterministic suite uses the real Go HTTP handler without opening
a listener or requiring PostgreSQL. The explicit
`TestPostgresControlPlanePressure` production profile intentionally opens a
loopback TCP listener and requires an isolated PostgreSQL database; it crosses
master-key authentication, trusted Fenix organization scope, catalog, runtime and admission instead
of substituting memory adapters.

## Configuration

The production processes use these environment variables:

| Variable | Process | Required | Purpose |
| --- | --- | --- | --- |
| `DATABASE_URL` | all | yes | PostgreSQL connection string. Use TLS and a least-privilege service identity outside local development. |
| `SUPAFLOW_MASTER_KEY_FILE` | control plane | yes | Read-only file containing the Fenix platform master key (at least 32 bytes). Only its SHA-256 digest is retained by the authenticator. |
| `SUPAFLOW_PREVIOUS_MASTER_KEY_FILE` | control plane | no | Read-only previous-key file accepted only during a bounded rotation overlap. The production Compose defaults it to the current secret file when no previous key is configured. |
| `SUPAFLOW_RESUME_TOKEN_SECRET` | all | yes | Shared HMAC secret of at least 32 bytes used to derive approval resume tokens. Every control-plane and worker replica must use the same value. |
| `SUPAFLOW_ADDR` | control plane | no | Listen address; defaults to `127.0.0.1:8080`. |
| `SUPAFLOW_WORKER_ID` | worker | no | Stable, unique identity for one worker process. A random ID is generated when absent; set it explicitly in production for diagnostics. Never share it between live replicas. |
| `SUPAFLOW_WORKER_TENANTS` | worker | yes | Transitional env contract: comma-separated organization IDs this worker polls. PR 2 preserves this name and behavior; PR 3 removes static enumeration in favor of organization-aware discovery and fair claiming. |
| `SUPAFLOW_HTTP_ALLOWED_HOSTS` | worker | yes | Comma-separated exact hostname/IP allowlist for HTTP nodes. Empty configuration fails closed. |
| `SUPAFLOW_WEBHOOK_EGRESS_ALLOWLIST` | control plane | no | Comma-separated DNS suffix allowlist for outbound event webhooks. When empty, public HTTPS destinations are permitted; private, loopback, link-local, multicast and rebinding targets remain blocked. |
| `SUPAFLOW_CREDENTIAL_MASTER_KEY` | control plane, worker | yes | Exactly 32 bytes encoded as hex or base64. Deployment-injected local envelope key; use an external KMS provider in managed deployments. |
| `SUPAFLOW_CREDENTIAL_KEY_ID` | control plane, worker | yes | Non-secret identifier for the active envelope key version. |
| `SUPAFLOW_EINO_MODELS_JSON` | worker | no | Trusted model templates (`name`, `credentialRef`, `model`, optional `baseUrl`/`timeoutMs`). Any configured `baseUrl` must be HTTPS, including loopback; model clients reject every redirect. Provider keys are resolved from attempt grants, never this JSON. |
| `SUPAFLOW_EINO_TOOLS_JSON` | worker | no | Trusted built-in tool bindings (`name`, `kind`). Supported kinds are `json.project` and `json.merge`; workflow definitions cannot upload tool code. |
| `SUPAFLOW_EINO_AGENTS_JSON` | worker | no | Fixed bounded agents (`name`, `model`, `tools`, `system`, `maxSteps`); every reference is validated before the worker claims attempts. |
| `SUPAFLOW_ISOLATION_BINARY` | worker | yes | Absolute path to the separately built `cmd/isolation` helper; worker startup fails closed without it. |
| `SUPAFLOW_ISOLATION_ALLOW_UNSAFE_LOCAL_EXECUTION` | isolation helper | no | Set to `1` or `true` to run user code without the Linux sandbox on a host that cannot provide one. Matching is case-insensitive and ignores surrounding whitespace, consistent with every other boolean in this table; `0`, `false`, any other value, and an unset or empty variable all keep the fail-closed startup probe. Ignored on Linux, which always takes the bwrap plus cgroup path, and on hosts that are neither Linux nor macOS, which fail closed regardless. Only the helper reads it; the worker must export it so its child processes inherit it. Leave it unset on every shared or production host. See "Local execution without the Linux sandbox". |
| `SUPAFLOW_ISOLATION_CGROUP_ROOT` | isolation helper (Linux only) | yes on Linux | Absolute delegated cgroup-v2 subtree below `/sys/fs/cgroup`; it must expose enabled `pids` and `memory` controllers. One child cgroup is created per fenced attempt. Not read on macOS, which has no cgroup v2. |
| `SUPAFLOW_ISOLATION_WORKSPACE_ROOT` | isolation helper | yes on Linux | Absolute scratch root used only for the bounded source file; production code writes inside a private bwrap tmpfs. On Linux a relative path fails closed. On macOS an unset or empty value silently falls back to the operating system temp directory, so set it explicitly to keep attempt workspaces on a known volume. |
| `SUPAFLOW_ISOLATION_MAX_PIDS` | worker | no | Per-attempt `pids.max`; defaults to 32 and cannot exceed 32. This deliberately does not use UID-global `RLIMIT_NPROC`. |
| `SUPAFLOW_ISOLATION_MEMORY_BYTES` | worker | no | Per-attempt cgroup `memory.max` and address-space ceiling; defaults to and cannot exceed 256 MiB. |
| `SUPAFLOW_ISOLATION_WORKSPACE_BYTES` | worker | no | Hard bwrap tmpfs byte ceiling; defaults to and cannot exceed 16 MiB. |
| `SUPAFLOW_ISOLATION_WORKSPACE_INODES` | worker | no | Per-attempt inode ceiling monitored every 10 ms; defaults to and cannot exceed 1024. Exceeding it kills the whole cgroup. |
| `SUPAFLOW_OBJECT_STORE_DIR` | control plane, worker | no | Traversal-safe local object-store root; defaults to `./data/objects`. Every replica must share the same durable volume; replace this adapter for distributed deployments. |
| `SUPAFLOW_OUTPUT_MAX_BYTES` | worker | no | Hard bound for one externally stored node output; defaults to 64 MiB and cannot exceed 1 GiB. Outputs larger than a node's inline limit or marked `sensitiveOutput` are stored externally. |
| `SUPAFLOW_OTEL_ENDPOINT` | control plane, worker | no | OTLP gRPC endpoint. When set, real trace and metric exporters are enabled. |
| `SUPAFLOW_ORGANIZATION_HASH_KEY` | control plane | with OTLP | Exactly 32 bytes encoded as hex or base64, used for privacy-preserving organization attribution. |
| `SUPAFLOW_OTEL_INSECURE` | control plane | no | Set to `true` only for a trusted plaintext OTLP development endpoint. |
| `SUPAFLOW_DELIVERY_BATCH` | control plane | no | Total outbox claims per delivery tick; defaults to 100 and is capped at 1000. Work is rotated across active organizations. |
| `SUPAFLOW_DELIVERY_CONCURRENCY` | control plane | no | Maximum simultaneous outbound deliveries; defaults to 4 and is capped at 64. |
| `SUPAFLOW_DELIVERY_TIMEOUT_MS` | control plane | no | Per-delivery deadline including DNS/connect/write/read; defaults to 20000 and is capped at 30000. |
| `SUPAFLOW_SSE_GLOBAL_LIMIT` | control plane | no | Process-wide concurrent event-stream cap; defaults to 512. Size ingress routing across replicas accordingly. |
| `SUPAFLOW_SSE_ORGANIZATION_LIMIT` | control plane | no | Per-process, per-organization concurrent event-stream cap; defaults to 64 and cannot exceed the global cap. |
| `SUPAFLOW_SSE_POLL_MIN_MS` | control plane | no | Minimum empty-stream database polling delay; defaults to 250. |
| `SUPAFLOW_SSE_POLL_MAX_MS` | control plane | no | Maximum exponential empty-stream polling delay; defaults to 2000. |
| `SUPAFLOW_SSE_WRITE_TIMEOUT_MS` | control plane | no | Deadline for every event or heartbeat socket write; defaults to 5000 and is capped at 30000. |

Authenticated HTTP telemetry is installed inside the Fenix trust boundary: successful requests use an HMAC-derived organization hash, rejected authentication remains anonymous, and HTTP status codes drive the span/metric outcome. Public webhook telemetry starts one request span before routing, attributes it only after `publicHash` resolves to a server-owned organization, and leaves unresolved or early-invalid requests anonymous. Raw organization IDs, bearer master keys, and request bodies are never attached to telemetry.

Normal organization-scoped requests require `Authorization: Bearer <master-key>` and `X-Fenix-Organization-Id`. The master key authenticates the trusted Fenix backend; the organization header becomes `organization.Principal.OrganizationID` and is the only request organization scope. Fenix is the only organization registry, IAM, and business-RBAC authority. Supaflow has no local users, memberships, API keys, service accounts, sessions, OIDC endpoints, or role checks. Migration `000026` removes those tables and renames current persisted scope to `organization_id`; old `tenant` terms remain only in immutable migrations `000001`–`000025`.

Keep the current/previous master-key files, resume-token secret, credential key, and connection strings in a secret manager. Do not commit them, put them in images, or log their values or file paths. During rotation, deploy the new current file while mounting the old file as previous, update Fenix, then remove previous after the bounded overlap.

For local development only:

```bash
docker compose up -d postgres
export DATABASE_URL='postgres://supaflow:local-development-only@127.0.0.1:55432/supaflow?sslmode=disable'
umask 077
printf '%s\n' 'local-master-key-at-least-32-random-bytes' > /tmp/supaflow-master-key
export SUPAFLOW_MASTER_KEY_FILE='/tmp/supaflow-master-key'
export SUPAFLOW_RESUME_TOKEN_SECRET='local-resume-token-secret-at-least-32-bytes'
export SUPAFLOW_CREDENTIAL_MASTER_KEY='0707070707070707070707070707070707070707070707070707070707070707'
export SUPAFLOW_CREDENTIAL_KEY_ID='local-key-1'
export SUPAFLOW_ADDR='127.0.0.1:8080'
export SUPAFLOW_WORKER_ID='local-worker-1'
export SUPAFLOW_WORKER_TENANTS='organization-a'
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

## Local execution without the Linux sandbox

Production has a single supported user-code boundary: the `worker-runtime` image on Linux, where bwrap, cgroup v2, and unprivileged namespaces are all present. The helper refuses to start anywhere it cannot enforce that complete boundary, and it exits `125` from its startup capability probe rather than degrade silently.

macOS cannot provide that boundary at all: there is no bubblewrap equivalent, no cgroup v2, no mount/PID/IPC/UTS namespace, and no filesystem confinement. Setting `SUPAFLOW_ISOLATION_ALLOW_UNSAFE_LOCAL_EXECUTION=1` lets the helper run there anyway, for local development on a disposable machine. The helper is the only reader of that variable, so the worker must export it for its child processes to inherit; the Studio and control plane do not need it. Hosts that are neither Linux nor macOS cannot use this opt-in at all: they fail closed regardless of its value.

The degraded runner keeps every boundary the host can still enforce — request timeout, bounded output, per-attempt temporary workspace, the process group that cancellation kills, the environment allowlist it injects, `RLIMIT_CPU`, and `RLIMIT_NOFILE` — and abandons the rest. In particular, user code runs **as the worker's own user with the worker's full filesystem and network reach**: it can read and write every file that user can, including credentials and the repository, and it can open sockets. Treat an enabled variable as equivalent to running the script yourself.

The environment allowlist is enforced on what the runner injects, not on the interpreter's effective environment. On macOS the interpreter adds its own: under exactly the environment `isolatedEnvironment` builds, `/usr/bin/python3` also sees `CPATH`, `LIBRARY_PATH`, `MANPATH`, `SDKROOT`, and `__CF_USER_TEXT_ENCODING`, injected through `libxcselect`. A shell node adds nothing beyond the allowlist. The degraded runner therefore cannot promise a fully closed environment the way the Linux sandbox does.

Each abandoned boundary has a recorded reason in `internal/isolation`:

| Missing boundary | Why the host cannot provide it |
| ---------------- | ------------------------------ |
| Mount, PID, IPC, UTS namespaces and read-only system binds | macOS has no bubblewrap equivalent. |
| Network namespace | The same: `--unshare-all` is a Linux feature, so user code reaches the host network stack directly. |
| Workspace byte and inode quotas | Quotas are a Linux mount/cgroup feature. |
| Death of the attempt when its launcher dies | Linux sets `Pdeathsig` and passes `--die-with-parent`; Darwin's `SysProcAttr` has no equivalent, so a helper killed with `SIGKILL` leaves the attempt running and its workspace directory behind. |
| Address-space ceiling (`RLIMIT_AS`) | The Darwin kernel rejects it with `EINVAL`. |
| Per-invocation process ceiling (`RLIMIT_NPROC`) | It is a per-host-user counter on Darwin, not a per-invocation boundary. |
| File-size ceiling (`RLIMIT_FSIZE`) | macOS `/usr/bin/python3` links `libxcselect.dylib`, which rebuilds a ~1.4 MB `xcrun_db` cache whenever the interpreter's `HOME` changes. Every attempt gets a fresh `HOME`, so this happens on every attempt, and the write lands in the Darwin per-user temp directory rather than the attempt workspace because `libxcselect` ignores `TMPDIR`. Any lower ceiling kills the interpreter with `SIGXFSZ` before user code runs, and the interpreter's own `-B`, `-S`, `-E`, `-I` and `-s` flags do not prevent the write. |

The worker's own cancellation path does not walk into that last gap. It signals the helper, and it waits `isolation.TerminationGrace` plus a margin before it escalates to `SIGKILL`. The helper spends its own 1.5-second grace signalling the child group and escalates itself only afterwards, so the margin — `executor`'s default `CancelGrace` is `isolation.TerminationGrace + 1s` — leaves it about one second for that escalation and for `os.RemoveAll`, and a teardown that needs longer is cut short rather than waited for. Waiting no longer than the helper does — which the default used to — means the worker's `SIGKILL` lands first, the helper dies mid-teardown, and a script that ignores `SIGTERM` keeps running as an orphan with its workspace still on disk.
The margin is fixed while the teardown is not: the helper's own grace expires before the worker's, so by the time the worker escalates it has already sent `SIGKILL` to the child group and what an interrupted teardown costs is the workspace directory, which nothing reclaims, rather than a running user process. User code survives when the helper dies before it escalates at all, such as an OOM kill or an operator's `kill -9`, or when user code leaves the process group the helper started it in. A helper killed by something outside the worker leaks the workspace unconditionally, and with it any process that escaped the group. Cancellation is not the only path that ends in an interrupted teardown: the executor derives its process context from the attempt's own context with the node timeout added, so a node that reaches its timeout stops the helper the same way. Nor does the workspace need an interruption to survive: the degraded runner's cleanup is `_ = os.RemoveAll(workspace)`, which discards the error, so a removal that fails for any reason leaks the directory just as silently.

Two further consequences are worth stating explicitly, because both escape the per-attempt workspace: that interpreter cache is rebuilt outside it, and a rebuild killed by a file-size ceiling leaves its partially written temp file behind (the measured residue of one investigation was 79 `xcrun_db-*` orphans totalling ~20 MB). Disk exhaustion is therefore not defended against on a degraded host.

Neither the plan nor the run record states that a boundary was abandoned. The immutable plan keeps the node type's declared permissions — a shell node still reads `{"compute":true,"network":false,"filesystem":false}` — because those describe the production boundary the node type is entitled to, not what the host actually enforced. On a degraded host the effective authority is the worker's own user, so any decision that depends on "this run could not touch the filesystem or the network" must come from knowing where the worker ran and whether the opt-in was set, not from the run record.

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
response. An expired owner may be replaced, while an old owner is fenced from completion. The
request hash covers the method, the escaped path, and the exact body bytes, with no JSON
normalization, so a replay that re-serializes the same document with different whitespace or key
order is rejected as `idempotency_conflict` rather than accepted as the same request. Clients that
retry must resend the original bytes; `/v1` reports that conflict as RFC 7807 `application/problem+json`
while `/ops` reports it as `{"code":"conflict","status":409}`.
The crash-window recovery contract for each mutation family is:

| Mutation | Durable side-effect recovery |
| --- | --- |
| quota and retention policy | organization-keyed upsert |
| trigger create/update/state/delete | `trigger_mutations` request hash and stored result |
| trigger fire and event receive | firing/event deduplication keys in the trigger transaction |
| credential create | resource ID derived from ledger `operation_id`; duplicate insert returns the existing reference |
| credential rotate/revoke | requested version and terminal revoked state are replay-safe |
| organization export | export/object IDs derived from `operation_id`; an existing export is returned before rebuilding bytes |
| outbound webhook create/state/delete | stable subscription ID plus idempotent lifecycle transitions |
| DLQ requeue | stable outbox ID plus the durable `requeued_at` marker |
| worker drain | organization/worker-keyed terminal draining state |

The offline Fenix importer is not an `/ops` route. It independently fingerprints source triggers
and runs and rejects a conflicting replay. No mutation may add a random resource ID without also
adding an operation-derived identity or a repository-local durable result record.

An expired attempt lease is not permission to repeat an ambiguous side effect. Workers only re-execute node types whose immutable plan capability declares `recoverySafe`. Built-in LLM, Agent, Tool, Shell and Python nodes fail closed with `unsafe_lease_takeover`; HTTP is recovery-safe only for GET, HEAD, PUT and DELETE, or when the author explicitly sets `retrySafe: true`. Operators must reconcile the external effect before starting a deliberate replacement run.

The control-plane supervisor runs schedule/replay, deadline, delivery, and retention as four independent ticker loops, so a slow webhook cannot block deadline enforcement. Schedule polling uses a durable database-clock organization cursor, serves at most one due schedule per organization in each round, and repeats round-robin rounds up to the global batch limit while processing each trigger at most once per tick. Organizations commit independently and a saturated organization is removed from later rounds, so a large `catch_up` history or exhausted ingress capacity cannot roll back or starve another organization. Event fan-out uses pages of at most 100 trigger rows and commits each page independently. A capacity `429` relinquishes only the HTTP mutation lease, preserves its operation identity, and is never cached as a terminal idempotency response; retrying the same key replays existing firing IDs and resumes at the first missing target. PostgreSQL serializes trigger, pending firing, and pending outbox admission on the organization quota row, so every producer observes the same hard limits. Delivery rotates its first organization, caps the total tick batch and concurrency, applies a per-call deadline, and renews an owned outbox lease while the network call is in flight. `trigger.fired` is routed to the internal run starter; run, node, attempt, stream, approval and terminal topics are routed to matching outbound subscriptions. Successful per-subscription receipts prevent already-completed fan-out targets from being repeated when a sibling target retries. External calls use bounded exponential retry and DLQ, disable redirects, cap request/response bodies and timeouts, resolve DNS at connect time, and reject non-public addresses. Credential-bearing HTTP nodes additionally require HTTPS and reject redirects; the exact host allowlist cannot weaken this transport rule. Every loop waits on a ticker, observes context cancellation, and logs redacted failures. Workers register durable heartbeats, honor remote drain state before claiming, and release their worker quota counter during graceful exit.

The default trigger limits are 1,000 definitions, 5,000 pending firings, and 20,000 pending outbox messages per organization; administrators can lower or raise them within the schema's absolute ceilings. Capacity rejection is retryable and never falls back to another organization or an in-memory queue. Delivered outbox messages and dispatched/failed firings are removed in bounded organization-fair retention batches after seven days. The same seven-day interval is the API/manual/event firing idempotency window, so cleanup never deletes a deduplication key that is still contractually reusable; pending delivery rows and pending firings are never purged by this maintenance path.

Event streams reject excess global or organization connections with `429` and `Retry-After`. Empty streams exponentially back off database polling, every socket write has a deadline, and request cancellation releases the quota immediately. Operators should alert on `429` rate and disconnects, and size the process limits below the database connection and ingress capacities.

Run capacity/alert and backup/restore drills with the production `ops` command. Restore always requires a distinct scratch database URL and never accepts a connection string on the command line:

```bash
go run ./cmd/ops -action capacity -organization organization-a
go run ./cmd/ops -action alerts -organization organization-a
SUPAFLOW_BACKUP_ROOT=./data/backups go run ./cmd/ops -action backup -artifact nightly.dump
SUPAFLOW_RESTORE_DATABASE_URL='postgres://scratch-host/scratch' \
SUPAFLOW_RESTORE_ARCHIVE_SHA256='<trusted-64-character-lowercase-hex-digest>' \
  go run ./cmd/ops -action restore -artifact nightly.dump
```

Immediately after backup, calculate the archive SHA-256 and store it in signed release evidence or another access-controlled, immutable channel separate from the backup directory. Restore requires that trusted value through `SUPAFLOW_RESTORE_ARCHIVE_SHA256`; it accepts exactly 64 lowercase hex characters. Do not derive this environment variable from the artifact immediately before restore, because an attacker able to replace both inputs would defeat the trust boundary.

The restore drill does not hand the archive path directly to a third, independently resolved `pg_restore` connection. It first resolves the production and scratch PostgreSQL cluster system identifier, database OID, and database name. Supaflow opens the archive once, rejects symlinks or an inode changed during open, and in one streaming pass hashes its bytes while copying them into an already-unlinked mode-`0600` snapshot. Only after the snapshot digest matches the trusted value does Supaflow rewind that private descriptor and supply it to `pg_restore` through stdin; later path replacement or in-place mutation of the source inode cannot change the bytes restored. `pg_restore` renders the complete archive without connecting to a database into a separate already-unlinked mode-`0600` file descriptor. Before opening `psql`, Supaflow scans the rendered stream and rejects every psql meta-command except exact `\restrict`/`\unrestrict` guards emitted by supported pg_dump versions and the `\.` terminator while inside `COPY FROM stdin`. It also rejects top-level transaction-control statements such as `BEGIN`, `START TRANSACTION`, `COMMIT`, `END`, `ROLLBACK`, `ABORT`, `PREPARE TRANSACTION`, `SAVEPOINT`, and `RELEASE`. Transaction words inside SQL strings, quoted identifiers, comments, dollar-quoted bodies, and `COPY` data are not interpreted as commands. This prevents a replaced or damaged artifact from changing databases, running a shell, including a second script, or committing a prefix of the restore outside the enforced transaction.

Finally one `psql --single-transaction` connection executes an identity guard as its first statement and the validated archive after it. Supaflow requires URL-form PostgreSQL DSNs, validates their connection options, and passes the parsed host, port, user, password, database, and TLS settings only as explicit libpq environment variables; the URI and password never enter process arguments or diagnostics. Inherited `PG*` variables are removed so `PGHOSTADDR` or another ambient setting cannot redirect the subprocess. The guard must observe the exact verified scratch identity and must not observe the production identity. A DNS, service-discovery, or proxy change between preflight and that connection therefore aborts before archive SQL; neither a psql reconnect nor an explicit SQL commit can escape the guarded transaction. Keep backup hashes in release evidence and investigate any meta-command or transaction-control rejection as artifact corruption rather than bypassing the scanner.

Any guard rejection, `pg_restore` error, `psql` error, signal, or connection loss returns failure. Because all archive SQL is applied in one transaction, PostgreSQL rolls it back on error or process death. The expanded script has no directory entry and is reclaimed by the kernel even after a hard crash. Before retrying, confirm that the scratch database has no successful prior restore, resolve the routing incident, and rerun the same immutable backup. Never bypass the identity guard by invoking `pg_restore --dbname` manually, and never point `SUPAFLOW_RESTORE_DATABASE_URL` at a transaction router that can move one open transaction between database identities.

The real-tool restore drill is opt-in because it creates and drops three disposable databases. Run it only against an isolated PostgreSQL instance with a URL-form admin connection that can use `CREATE DATABASE`, `pg_control_system()`, and event triggers. `pg_dump`, `pg_restore`, and `psql` must already be in `PATH` and should match the server major version. A local Docker/CI invocation is:

```bash
docker run --rm --detach --name supaflow-restore-postgres \
  --publish 55433:5432 \
  --env POSTGRES_USER=supaflow_restore_admin \
  --env POSTGRES_PASSWORD=restore-test-only \
  postgres:17-alpine
trap 'docker rm --force supaflow-restore-postgres >/dev/null 2>&1 || true' EXIT
for attempt in $(seq 1 30); do
  docker exec supaflow-restore-postgres pg_isready -U supaflow_restore_admin && break
  test "$attempt" -lt 30 || exit 1
  sleep 1
done
export SUPAFLOW_RESTORE_TEST_ADMIN_DATABASE_URL='postgres://supaflow_restore_admin:restore-test-only@127.0.0.1:55433/postgres?sslmode=disable'
command -v pg_dump pg_restore psql
go test ./internal/platformops -run '^TestExecCommandRunnerRealPostgresRestoreDrill$' -count=1 -v
```

The test creates a real custom-format archive, restores it successfully, proves the first-statement identity guard rejects the source database as a target, and injects a target-side DDL failure to prove earlier restored schema/data is rolled back. CI should execute the same command in a job image that already includes matching PostgreSQL client tools; absence of the env var or any required client produces an explicit skip.

## Verification

Run all commands from `packages/supaflow`:

```bash
go test ./...
go test -race ./...
go vet ./...
go test ./tests/e2e
go test ./tests/deployment
```

Several packages hold PostgreSQL integration suites that migrate the target database
and then write their own fixtures. They are read-only with respect to the source tree,
but not with respect to the database: point `SUPAFLOW_TEST_DATABASE_URL` at a throwaway
database and recreate it between runs, never at a development or production one.

- The suites register synthetic node types such as `test.registry.<id>@1.2.3` in the
  append-only `node_type_versions` registry. The immutability trigger rejects every
  `UPDATE` and `DELETE`, so those rows cannot be removed through the normal path, and
  both the control plane and the worker refuse to start while the durable registry
  holds a node version that no adapter serves. Recovering a polluted database needs a
  transaction that disables `node_type_versions_immutable`, deletes the
  `test.registry.%` rows, and re-enables it.
- Some suites insert at hard-coded primary keys — `organization_work_queue` row `stale`,
  `outbox_messages` row `clock-message` — and do not clean up when they fail, so a
  second run against the same database fails on duplicate keys. A fresh database passes
  every suite; the same database does not pass twice.

The E2E suite covers immutable publish, organization-scoped idempotent start replay, `Start -> Human Approval -> End`, organization-authenticated token delivery, lost-response approval replay and conflicting-key rejection, bounded worker advancement, success, cancellation, Fenix authorization-boundary behavior, two-organization run/event/interrupt isolation, and SSE continuation through `Last-Event-ID` and `afterCursor`. It uses `httptest.ResponseRecorder` and a deadline-bound request context, so it is safe in sandboxes that prohibit binding network ports.

Run the in-process authenticated API benchmark without any browser or listening socket:

```bash
go test -run '^$' -bench=. -benchmem ./tests/load/...
go test -run '^$' -bench=BenchmarkStartRunHTTP -benchmem -benchtime=10s -count=5 -cpu=1,4,8 ./tests/load/...
```

The benchmark drives concurrent JSON requests through the actual master-key auth, organization scope, plan resolution, idempotency, runtime service, and HTTP response path. Each start request gets a unique idempotency key. Record `ns/op`, `B/op`, and `allocs/op` with the commit, Go version, CPU count, and `-benchtime`; compare results only on equivalent machines. This benchmark intentionally uses the memory adapters to isolate API overhead. Database throughput, lock contention, connection-pool sizing, SSE connection capacity, and worker saturation require a separate staging test against PostgreSQL with production-equivalent limits.

Run the real-TCP PostgreSQL contention profile separately so its 1,000
simultaneous clients do not distort unit-test timing:

```bash
set -euo pipefail
umask 077
export SUPAFLOW_TEST_DATABASE_URL='postgres://least-privilege-user@127.0.0.1:55432/supaflow_pressure?sslmode=disable'
test -z "$(git status --porcelain)" || { echo 'release profile requires a clean checkout' >&2; exit 1; }
export SUPAFLOW_RELEASE_COMMIT="$(git rev-parse HEAD)"
evidence_root="release-evidence/${SUPAFLOW_RELEASE_COMMIT}"
mkdir -p "$evidence_root"
chmod 700 "$evidence_root"
export SUPAFLOW_RELEASE_EVIDENCE_DIR="$evidence_root/$(date -u +%Y%m%dT%H%M%SZ)-$$-${RANDOM}${RANDOM}"
mkdir "$SUPAFLOW_RELEASE_EVIDENCE_DIR"
chmod 700 "$SUPAFLOW_RELEASE_EVIDENCE_DIR"
go test -timeout=120s -run '^TestPostgresControlPlanePressure$' -count=1 -json ./tests/load 2>&1 \
  | tee "$SUPAFLOW_RELEASE_EVIDENCE_DIR/go-test.jsonl"
find "$SUPAFLOW_RELEASE_EVIDENCE_DIR" -type f ! -name SHA256SUMS -print0 \
  | xargs -0 shasum -a 256 \
  > "$SUPAFLOW_RELEASE_EVIDENCE_DIR/SHA256SUMS"
```

The database must be disposable and isolated from development and production
data. The test applies the immutable migration chain, creates a unique organization,
trusted master key and organization scope, uses a 32-connection pool,
then releases 1,000 TCP starts simultaneously. It records p50/p95/p99 request
latency. Eight independently identified workers then issue concurrent claims while
performing exactly 2,000 successful advances under an 8,000-call bound. The
profile records advance latency, non-working calls, lease takeovers, durable
event and outbox enqueue throughput, oldest pre-drain queue age and maximum
queue admission delay. It requires all 1,000 Runs to be successful with both
admission counters returned to zero. A missing
`SUPAFLOW_TEST_DATABASE_URL` is an intentional skip; release evidence must set
it. The test atomically writes a private, commit-bound JSON object and the
`go test -json` stream retains the raw logs. The JSON also captures Go/host,
PostgreSQL-version and connection-pool context and refuses to overwrite an
existing object for that commit. Keep those files and their
`SHA256SUMS`; console excerpts alone are not release evidence.

There is no in-process substitute for ingress/TLS SSE capacity or a replicated
outbox dispatcher. On production-equivalent staging, first choose a non-terminal
Run that will remain open for the measurement and use the staging master key and target organization ID, then exercise bounded SSE fan-out:

```bash
set -euo pipefail
umask 077
: "${SUPAFLOW_RELEASE_COMMIT:?export the exact deployed 40-character commit SHA}"
printf '%s\n' "$SUPAFLOW_RELEASE_COMMIT" | grep -Eq '^[0-9a-f]{40}$'
export SUPAFLOW_RELEASE_COMMIT
export SUPAFLOW_RELEASE_EVIDENCE_DIR="${SUPAFLOW_RELEASE_EVIDENCE_DIR:-release-evidence/${SUPAFLOW_RELEASE_COMMIT}/staging-$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "$SUPAFLOW_RELEASE_EVIDENCE_DIR"
chmod 700 "$SUPAFLOW_RELEASE_EVIDENCE_DIR"
export SSE_PROFILE_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$-${RANDOM}${RANDOM}"
export SSE_EVIDENCE_DIR="$SUPAFLOW_RELEASE_EVIDENCE_DIR/sse-$SSE_PROFILE_ID"
mkdir "$SSE_EVIDENCE_DIR"
chmod 700 "$SSE_EVIDENCE_DIR"
printf '%s\n' "$SUPAFLOW_RELEASE_COMMIT" > "$SSE_EVIDENCE_DIR/deployed-commit.txt"
export SUPAFLOW_URL='https://supaflow.staging.example'
export SUPAFLOW_MASTER_KEY='staging-master-key-from-vault'
export SUPAFLOW_ORGANIZATION_ID='org-staging'
export SUPAFLOW_RUN_ID='run_kept_non_terminal_for_profile'
export SSE_CLIENTS=64
export SSE_DURATION_SECONDS=30
curl --fail --silent --show-error -H "Authorization: Bearer $SUPAFLOW_MASTER_KEY" -H "X-Fenix-Organization-Id: $SUPAFLOW_ORGANIZATION_ID" \
  "$SUPAFLOW_URL/ops/metrics" > "$SSE_EVIDENCE_DIR/metrics-before-sse.json"
seq "$SSE_CLIENTS" | xargs -I{} -P "$SSE_CLIENTS" sh -c '
  curl --fail --silent --show-error --no-buffer --max-time "$SSE_DURATION_SECONDS" \
    -H "Authorization: Bearer $SUPAFLOW_MASTER_KEY" -H "X-Fenix-Organization-Id: $SUPAFLOW_ORGANIZATION_ID" -H "Accept: text/event-stream" \
    "$SUPAFLOW_URL/v1/runs/$SUPAFLOW_RUN_ID/events?afterCursor=0" \
    > "$SSE_EVIDENCE_DIR/client-{}.log"
  status=$?
  [ "$status" -eq 0 ] || [ "$status" -eq 28 ]
'
curl --fail --silent --show-error -H "Authorization: Bearer $SUPAFLOW_MASTER_KEY" -H "X-Fenix-Organization-Id: $SUPAFLOW_ORGANIZATION_ID" \
  "$SUPAFLOW_URL/ops/metrics" > "$SSE_EVIDENCE_DIR/metrics-after-sse.json"
find "$SSE_EVIDENCE_DIR" -type f ! -name SHA256SUMS -print0 \
  | xargs -0 shasum -a 256 \
  > "$SSE_EVIDENCE_DIR/SHA256SUMS"
```

To measure outbox drain rather than only enqueue throughput, keep the deployed
dispatcher running, prepare a JSON Run request containing a valid immutable
`planId`/`planHash`, and capture the capacity series while creating bounded load:

```bash
set -euo pipefail
umask 077
: "${SUPAFLOW_URL:?run the SSE preamble or export the staging URL}"
: "${SUPAFLOW_MASTER_KEY:?export the staging master key}"
: "${SUPAFLOW_ORGANIZATION_ID:?export the Fenix organization ID}"
: "${SUPAFLOW_RELEASE_COMMIT:?export the exact deployed commit SHA}"
: "${SUPAFLOW_RELEASE_EVIDENCE_DIR:?run the SSE preamble or export an evidence directory}"
source_run_request="$SUPAFLOW_RELEASE_EVIDENCE_DIR/run-request.json"
test -f "$source_run_request"
export OUTBOX_OBSERVATION_SECONDS=120
export OUTBOX_PROFILE_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$-${RANDOM}${RANDOM}"
export OUTBOX_EVIDENCE_DIR="$SUPAFLOW_RELEASE_EVIDENCE_DIR/outbox-$OUTBOX_PROFILE_ID"
mkdir "$OUTBOX_EVIDENCE_DIR"
chmod 700 "$OUTBOX_EVIDENCE_DIR"
printf '%s\n' "$OUTBOX_PROFILE_ID" > "$OUTBOX_EVIDENCE_DIR/profile-id.txt"
printf '%s\n' "$SUPAFLOW_RELEASE_COMMIT" > "$OUTBOX_EVIDENCE_DIR/deployed-commit.txt"
cp "$source_run_request" "$OUTBOX_EVIDENCE_DIR/run-request.json"
export RUN_REQUEST_JSON="$OUTBOX_EVIDENCE_DIR/run-request.json"
curl --fail --silent --show-error -H "Authorization: Bearer $SUPAFLOW_MASTER_KEY" -H "X-Fenix-Organization-Id: $SUPAFLOW_ORGANIZATION_ID" \
  "$SUPAFLOW_URL/ops/metrics" > "$OUTBOX_EVIDENCE_DIR/metrics-before-outbox.json"
(
  for sample in $(seq 0 "$OUTBOX_OBSERVATION_SECONDS"); do
    curl --fail --silent --show-error -H "Authorization: Bearer $SUPAFLOW_MASTER_KEY" -H "X-Fenix-Organization-Id: $SUPAFLOW_ORGANIZATION_ID" \
      "$SUPAFLOW_URL/ops/metrics" \
      > "$OUTBOX_EVIDENCE_DIR/capacity-${sample}.json"
    sleep 1
  done
) &
metrics_sampler_pid=$!
export RUN_REQUEST_JSON OUTBOX_PROFILE_ID OUTBOX_EVIDENCE_DIR SUPAFLOW_URL SUPAFLOW_MASTER_KEY SUPAFLOW_ORGANIZATION_ID
seq 1000 | xargs -P 64 -n 1 sh -c '
  index=$1
  http_code=$(curl --fail --silent --show-error \
    -H "Authorization: Bearer $SUPAFLOW_MASTER_KEY" -H "X-Fenix-Organization-Id: $SUPAFLOW_ORGANIZATION_ID" \
    -H "Idempotency-Key: staging-outbox-$OUTBOX_PROFILE_ID-$index" \
    -H "Content-Type: application/json" --data-binary "@$RUN_REQUEST_JSON" \
    --dump-header "$OUTBOX_EVIDENCE_DIR/run-start-$index.headers" \
    --output "$OUTBOX_EVIDENCE_DIR/run-start-$index.json" \
    --write-out "%{http_code}" "$SUPAFLOW_URL/v1/runs")
  printf "%s\n" "$http_code" > "$OUTBOX_EVIDENCE_DIR/run-start-$index.status"
  [ "$http_code" -eq 202 ]
' sh
wait "$metrics_sampler_pid"
find "$OUTBOX_EVIDENCE_DIR" -type f ! -name SHA256SUMS -print0 \
  | xargs -0 shasum -a 256 \
  > "$OUTBOX_EVIDENCE_DIR/SHA256SUMS"
```

Treat these staging artifacts as observations, not a production SLO. Record the
replica counts, quota/pool configuration, ingress version and commit alongside
them. A release candidate is rejected if SSE requests fail for reasons other
than the intentional curl time limit, if outbox backlog does not return toward
its pre-load value, or if queue/outbox age remains unbounded after load stops.

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

`POST /runs/{runId}/cancel` is durable and acknowledged immediately with `202` and `CANCELLING`; it is not an immediate kill. A running attempt observes cancellation on its next renewal tick, so the transition to `CANCELLED` lands within one `LeaseDuration/3` interval (10 seconds by default) and the measured end-to-end latency is on that order. Alerting on cancel latency below one tick would page on the design rather than on a fault.

While an attempt is executing, the same ticker that renews its lease is what bounds cancellation latency, and it also means a long-running node is never taken over mid-flight: the attempt's window keeps being extended, so a node that outlives one lease ends through its own node timeout (`node_timeout`) rather than through a second worker claiming it.

## Rollback

Application rollback means redeploying the previous binary while preserving PostgreSQL state. Verify the previous binary understands every already-applied schema and every immutable plan/node version that can still be in flight. Do not roll back by restoring an older database snapshot while accepting traffic: that can reuse idempotency keys, event cursors, lease tokens, or approval tokens.

SQL migrations are forward-only. If a migration causes an incident, stop affected writers, deploy a reviewed compensating forward migration, and then roll binaries as needed. Restore a database backup only as a disaster-recovery operation with traffic stopped and an explicit reconciliation plan for requests acknowledged after the backup point.
