# Architecture

## Boundaries

Supaflow owns workflow drafts, immutable publications and execution plans, runs, attempts, leases, events, outputs, interrupts, audit records, and transactional outbox messages. Host products integrate only through authenticated APIs and event streams.

The dependency direction is:

```text
HTTP / worker commands -> application services -> domain ports
                                             <- PostgreSQL / executor adapters
```

Editor documents are untrusted adapter inputs. Compilation converts them into a canonical `WorkflowDefinition`; publication resolves node versions into an immutable `ExecutionPlan`. Runs bind a plan ID and content hash, so registry upgrades cannot change in-flight behavior.

## Platform modules

The Studio adapter persists FlowGram documents with optimistic revisions, compiles them into canonical definitions, publishes immutable plans, shows version diffs, and observes organization-scoped runs. PostgreSQL `node_types` and `node_type_versions` are the append-only registry authority for schemas, permissions, limits, executor bindings, and explicit node migrations. Control-plane and worker startup idempotently seed built-ins, reject immutable contract drift, and verify the process-local compiler/executor adapter cache against that durable projection before accepting work. Runtime nodes include Start/End, HTTP, LLM, Agent, Tool, Condition/Switch, Loop, Transform, Approval, Subworkflow, Delay/Wait, Webhook Resume, Shell, and Python.

Control-plane and worker processes scale independently. PostgreSQL is the coordination authority for idempotency, state transitions, event sequence allocation, suspension, cancellation, leases, fencing, outputs, snapshots, lineage, admission, audit, and transactional outbox writes. A Run is scheduled at least once; an expired worker is fenced from committing. External side effects use stable attempt identity and must provide deduplication or compensation rather than claiming exactly-once behavior.

Large or sensitive outputs cross an `OutputManager` seam and are represented in the runtime by bounded metadata. Code nodes execute through the separate isolation helper. The control plane never evaluates user code.

## Security model

Organization-scoped requests are accepted only from the trusted Fenix backend with `Authorization: Bearer <master-key>` and `X-Fenix-Organization-Id`. Supaflow retains only current/previous master-key digests and compares them in constant time; after authentication, the organization header is injected into the neutral `organization.Principal.OrganizationID` context. Fenix is the sole organization registry, IAM, and business-RBAC authority: it authenticates users and API keys, verifies organization membership, and authorizes Workflow operations before calling Supaflow. Supaflow has no local organization registry, users, memberships, service accounts, sessions, OIDC endpoints, or business roles. Request JSON, query parameters, legacy `X-API-Key`, cookies, and browser-provided headers cannot select or override organization scope. Every repository operation carries an `organization_id` predicate unless it is an explicit internal supervisor operation over bounded work. Legacy `tenant` terminology exists only in immutable migrations `000001`–`000025`; migration `000026` removes local IAM and renames current business schema objects. Secrets are encrypted references with per-attempt grants; neither API responses nor events contain raw credentials.

HTTP execution validates the exact host, every resolved address, redirects, timeouts, and response bounds. Nodes that resolve a credential require HTTPS and never follow redirects, preventing both cross-origin header copying and HTTPS-to-HTTP downgrade; credential-free nodes still revalidate every redirect through the egress policy. Eino model providers apply the same credential transport invariant to every configured endpoint, including loopback destinations, and use a dedicated client that rejects all redirects. The code runner defaults to no network, no host filesystem, a disposable workspace, and finite CPU, memory, process, descriptor, file, and output limits; unsupported isolation capabilities fail closed.

## Recovery and rollback

Workers claim runnable attempts using expiring leases and monotonically increasing fencing tokens. Completion checks the current token, preventing stale workers from committing. Non-terminal runs remain in PostgreSQL and are claimable after lease expiry. Deploy rollback therefore replaces binaries without rewriting durable state; migration rollback uses compensating forward migrations once production data exists.
