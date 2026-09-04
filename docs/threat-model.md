# Threat model

This review applies to WorkflowDefinition v1, the v1 HTTP API, PostgreSQL adapters, workers, trigger delivery, and the isolation runner. Revisit it whenever a trust boundary, executor capability, identity provider, or public endpoint changes.

## Assets and trust boundaries

Protected assets are tenant definitions and plans, API/session credentials, wrapped credential data, run inputs and outputs, approval tokens, webhook signing material, audit records, and the authority to cause external side effects. Tenant identity is established only by the authentication adapter and is never accepted from a URL body or node payload.

The public ingress terminates TLS before the control plane. The control plane validates untrusted HTTP, Studio documents, webhook bytes, and event payloads before calling application services. PostgreSQL is the durable coordination authority. Workers receive only tenant-scoped attempts and resolve credentials only for a specific attempt grant. User code crosses a separate process boundary into the isolation runner; it never executes in the control-plane process.

## Threats and controls

| Threat | Preventive control | Detection and recovery |
| --- | --- | --- |
| Cross-tenant object probing | Authentication-derived tenant; mandatory tenant predicates and composite foreign keys; not-found responses hide ownership | Isolation tests; audit actor/tenant hashes; revoke affected principal |
| Stolen API/session credential | Only digests persisted; constant-time comparison; expiration/revocation; least-privilege RBAC | Authentication failure metrics; credential rotation and session revocation |
| OIDC login CSRF/code interception | One-time state digest, browser-bound HttpOnly cookie, nonce, S256 PKCE, HTTPS-only discovery/endpoints, signed ID-token validation | Reject and consume invalid transactions; revoke any resulting session during provider incident response |
| Definition/schema confusion | Strict JSON decoding; canonical WorkflowDefinition; pinned node versions; immutable content hash | Compiler contract tests; republish rather than in-place mutation |
| Duplicate side effects | Tenant-scoped idempotency, trigger replay keys, attempt fencing, bounded retry and executor capability declarations | Duplicate-message and lease-takeover tests; compensation/DLQ workflow |
| Stale worker commit | Expiring lease plus monotonically increasing fencing token checked in the completion transaction | Lease-age and stale-token metrics; drain/restart worker |
| SSRF and DNS rebinding | Exact egress allowlist; validate every resolved address before dialing; re-resolve on redirect and block metadata/private ranges | Denial events without internal addresses; update egress policy |
| Webhook forgery/replay | HMAC over exact raw bytes and timestamp; constant-time verification; bounded time window; durable request-id replay record | Signature/replay metrics; rotate signing credential |
| Approval token theft/reuse | Expiring tenant/run/attempt-bound HMAC token; digest-only storage; single-use idempotent resolution | Approval audit; cancel run and rotate issuer after draining pending approvals |
| Secret disclosure | Opaque credential references in plans; envelope encryption; attempt-scoped grants; redacting logs/errors/events | Secret-leak tests and audit; revoke/rotate credential and encryption key version |
| User-code host escape | Dedicated worker image and runner process; bwrap PID/network/mount namespaces; delegated per-attempt cgroup v2; private bounded tmpfs; no host filesystem or undeclared network; finite wall-clock/output/CPU/memory/PID/FD/file limits | Real-launcher capability/fork/disk/cancel tests; disable Compute capability and drain workers |
| Queue or stream exhaustion | Tenant quotas, priority admission, bounded batches/body/output sizes, SSE cancellation and backpressure | Queue age, outbox backlog, pool saturation and worker-capacity alerts |
| Migration data loss | Read freeze, stable fingerprints, dry-run blockers, file hashes, signed evidence and verified backup gate before destructive migration | Retained evidence/report; restore only with traffic stopped and reconcile acknowledgements |

## Residual risks

Executors that call external systems cannot promise exactly-once effects when the remote system has no idempotency facility. Such nodes must declare retry safety truthfully and use compensation where necessary. The local AES provider is a production-capable envelope boundary only when its master key comes from an external secret manager; deployments requiring managed KMS rotation should replace that adapter without changing the credential domain.

Linux sandbox guarantees depend on the host providing bwrap with user namespaces and a narrowly delegated cgroup-v2 subtree with `memory` and `pids` enabled. A missing primitive is a hard worker-startup/execution failure, never a fallback to host execution. `RLIMIT_NPROC` is intentionally not treated as an attempt boundary because Linux counts it across the real UID. Workspace bytes are a hard private-tmpfs limit; inode use is sampled and kills the attempt on violation, so a malicious attempt may briefly exceed the configured inode count within one 10 ms sampling interval. The outer worker scratch area is separately bounded by the deployment tmpfs. Non-Linux hosts do not provide the production boundary and fail closed.

## Release review checklist

- Run tenant-isolation, webhook replay, SSRF/DNS rebinding, secret-redaction and sandbox escape suites.
- Confirm public endpoints have authentication or explicit raw-body signature verification and finite limits.
- Confirm every newly enabled node capability has a production adapter and worker policy.
- Confirm migrations preserve tenant keys, immutable plan hashes, fencing high-water marks and rollback evidence.
- Review dependencies and container base images for current security advisories before each release.
