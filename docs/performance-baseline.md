# Performance baseline and release evidence

Baseline date: 2026-09-03. Revision: historical developer run; its exact commit and raw log were not retained, so it is not eligible as release evidence. Go: 1.23 module toolchain. Host: Apple M5 Pro, darwin/arm64, 18 logical benchmark workers. These numbers are useful only as a local reference, not as a production SLO.

Command:

```bash
go test -run '^$' -bench='Benchmark(DraftAutosave|ImmutablePublish|StartRunHTTP|EventCommit)$' -benchmem -benchtime=100x ./tests/load
go test -v ./tests/load -run TestNodeScaleBaseline
```

| Path | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Authenticated HTTP Run start | 14,745 | 12,102 | 82 |
| Studio optimistic autosave | 3,325 | 2,182 | 16 |
| Canonical compile + immutable publish | 13,952 | 15,613 | 131 |
| Attempt state/output/event atomic commit | 17,288 | 9,888 | 121 |

| Linear DAG | Compile + publish | Full execution |
| --- | ---: | ---: |
| 100 nodes | 2.93 ms | 3.55 ms |
| 500 nodes | 7.95 ms | 36.74 ms |
| 1,000 nodes | 11.27 ms | 108.96 ms |

The microbenchmarks use memory adapters to isolate API/domain allocation and scheduler complexity. The node-scale test executes every node and asserts the exact number of durable advances with a 20-second ceiling; it is not a fake parser-only benchmark.

## PostgreSQL and real-TCP profile

The historical run used PostgreSQL 16 in Docker Desktop through
`127.0.0.1:55432`, using an isolated `supaflow_pressure` database. The test
configured the pgx pool with `MaxConns=32`, created a database-backed API key,
service account and organization membership, published through the PostgreSQL
catalog, then released 1,000 client goroutines at the same start gate. Every
request used a unique idempotency key and crossed a real TCP listener. It then
used the PostgreSQL runtime/admission store and a real orchestrator to drain
the two-node plans to terminal state.

| Profile | Work | Elapsed | Rate | Durable assertion |
| --- | ---: | ---: | ---: | --- |
| Authenticated Run start | 1,000 simultaneous requests | 5.305 s | 188 req/s | 1,000 accepted and 1,000 distinct durable Runs |
| Worker drain | 2,000 fenced advances | 36.392 s | 55 advances/s | 1,000 `success`; active and queued counters both zero |

The standalone historical test completed in 42.424 seconds. This measures sustained
throughput and correctness under connection-pool contention; it does not claim
request-latency percentiles or production capacity. Docker Desktop used 6
virtual CPUs and 16.7 GB assigned memory while the Go test process ran on the
Apple M5 Pro host. Network ingress, TLS, external object storage and multiple
worker replicas remain staging variables. Do not copy these numbers into a release
record: the original run predates commit-bound evidence and latency collection.

## Commit-bound PostgreSQL profile

Run this from the repository root against a fresh, disposable PostgreSQL database.
The test crosses a real TCP listener and database authentication path. It records
1,000-connection Run-start p50/p95/p99, durable event and outbox enqueue throughput,
oldest queue age before drain, maximum queue admission delay, and eight-worker claim
competition. The drain is bounded to 8,000 calls for exactly 2,000 successful
advances and fails if the queue or admission counters do not return to zero.

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

The test atomically writes mode-`0600`
`postgres-control-plane-<commit>.json` and also emits the compact object as
`release_evidence_json=` in the raw `go test -json` stream. Setting an evidence
directory without an exact lowercase 40-character `SUPAFLOW_RELEASE_COMMIT` fails
before the profile starts, and an existing commit JSON is never overwritten. The
JSON includes the Go/OS/architecture/CPU context,
PostgreSQL version and connection-pool bounds as well as the measured values.
Archive both files and `SHA256SUMS`; a table copied into this document is not a
substitute for those artifacts.

This profile measures the in-repository control plane with real PostgreSQL, not the
production network. Before release, run the staging SSE/outbox procedure in
`docs/operations.md` against production-equivalent TLS, ingress, object storage,
worker replicas, pool limits and quotas. Retain its raw responses beside this profile.
Compare commits only on equivalent hosts and append a link or artifact identifier to
the release record rather than rewriting the historical row above.
