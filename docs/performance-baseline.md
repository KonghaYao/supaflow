# Performance baseline

Baseline date: 2026-09-03. Revision: the initial implementation commit that introduces this document; subsequent releases must append a commit-addressed result instead of rewriting this row. Go: 1.23 module toolchain. Host: Apple M5 Pro, darwin/arm64, 18 logical benchmark workers. These numbers are a reproducible developer baseline, not a production SLO.

Command:

```bash
go test -run '^$' -bench='Benchmark(DraftAutosave|ImmutablePublish|StartRunHTTP|EventCommit)$' -benchmem -benchtime=100x ./tests/load
go test -v ./tests/load -run TestNodeScaleBaseline
SUPAFLOW_TEST_DATABASE_URL='postgres://.../supaflow_pressure?sslmode=disable' \
  go test -timeout=120s -run '^TestPostgresControlPlanePressure$' -count=1 -v ./tests/load
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

The same host ran PostgreSQL 16 in Docker Desktop through
`127.0.0.1:55432`, using an isolated `supaflow_pressure` database. The test
configured the pgx pool with `MaxConns=32`, created a database-backed API key,
service account and tenant membership, published through the PostgreSQL
catalog, then released 1,000 client goroutines at the same start gate. Every
request used a unique idempotency key and crossed a real TCP listener. It then
used the PostgreSQL runtime/admission store and a real orchestrator to drain
the two-node plans to terminal state.

| Profile | Work | Elapsed | Rate | Durable assertion |
| --- | ---: | ---: | ---: | --- |
| Authenticated Run start | 1,000 simultaneous requests | 5.305 s | 188 req/s | 1,000 accepted and 1,000 distinct durable Runs |
| Worker drain | 2,000 fenced advances | 36.392 s | 55 advances/s | 1,000 `success`; active and queued counters both zero |

The standalone test completed in 42.424 seconds. This measures sustained
throughput and correctness under connection-pool contention; it does not claim
request-latency percentiles or production capacity. Docker Desktop used 6
virtual CPUs and 16.7 GB assigned memory while the Go test process ran on the
Apple M5 Pro host. Network ingress, TLS, external object storage and multiple
worker replicas remain staging variables.

Before release, repeat the PostgreSQL profile against production-equivalent
PostgreSQL, object storage, network policy, worker count and connection-pool
limits. Record p50/p95/p99 for start latency, event commit throughput, queue
age, lease contention, SSE connection capacity, outbox drain rate and
100/500/1000-node execution. Compare commits only on equivalent hosts and
retain the raw command output with release evidence.
