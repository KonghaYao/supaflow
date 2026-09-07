# Supaflow engineering rules

Supaflow is an independently versioned Go service. It must not import Fenix packages or rely on the parent repository at runtime.

- Dependency direction: transport adapters -> application/domain ports <- infrastructure adapters.
- Organization scope comes only from a Fenix backend request authenticated with the platform master key; request bodies, query parameters, and browser-provided headers never select or override it. Fenix is the sole authority for users, organization membership, and business permissions; Supaflow retains organization ownership only for storage, execution, credentials, scheduling, and audit isolation.
- Published workflow versions and execution plans are immutable.
- Durable state transitions use PostgreSQL transactions, expected-state checks, leases, and fencing tokens.
- Keep protocol DTOs, domain models, and database rows separate and convert at boundaries.
- No user code runs in the control-plane process.
- Public functions and types need Go documentation. Keep files below 500 lines.
- Tests must cover isolation, idempotency, retries, cancellation, recovery, and stale lease rejection where applicable.
- Run `go test ./...`, `go vet ./...`, and the API E2E/load test before claiming completion.
