import { useCallback, useState } from "react";

import type { StudioClient } from "../api/client";
import type { AuditRecord, CapacitySnapshot, DeadLetter, Readiness } from "../domain/models";
import { useOperatorResource } from "../state/use-operator-resource";
import { errorMessage, type Feedback, FeedbackMessage, ResourceBoundary } from "./operator-shared";

export function OperatorOverview({ client }: { client: StudioClient }) {
  const loadReadiness = useCallback((signal: AbortSignal) => client.readiness(signal), [client]);
  const loadCapacity = useCallback((signal: AbortSignal) => client.capacity(signal), [client]);
  const readiness = useOperatorResource<Readiness>(loadReadiness);
  const capacity = useOperatorResource<CapacitySnapshot>(loadCapacity);
  const [workerId, setWorkerId] = useState("");
  const [feedback, setFeedback] = useState<Feedback>();
  const drain = async () => {
    if (!confirm(`Drain worker “${workerId}”? It will stop accepting new attempts.`)) return;
    setFeedback({ kind: "loading", message: "Requesting worker drain…" });
    try {
      await client.drainWorker(workerId.trim());
      setFeedback({ kind: "success", message: "Worker is draining." });
      setWorkerId("");
    } catch (error) {
      setFeedback({ kind: "error", message: errorMessage(error) });
    }
  };
  return (
    <div className="operator-grid">
      <section className="operator-card">
        <div className="section-heading">
          <h2>Readiness</h2>
          <button onClick={readiness.reload}>Refresh</button>
        </div>
        <ResourceBoundary state={readiness.state} retry={readiness.reload}>
          {(value) => (
            <div>
              <strong className={value.ready ? "healthy" : "unhealthy"}>{value.ready ? "Ready" : "Unavailable"}</strong>
              {value.failures && <pre>{JSON.stringify(value.failures, null, 2)}</pre>}
            </div>
          )}
        </ResourceBoundary>
      </section>
      <section className="operator-card">
        <div className="section-heading">
          <h2>Capacity</h2>
          <button onClick={capacity.reload}>Refresh</button>
        </div>
        <ResourceBoundary state={capacity.state} retry={capacity.reload}>
          {(value) => (
            <dl className="metric-grid">
              <Metric name="Active runs" value={value.activeRuns} />
              <Metric name="Queued runs" value={value.queuedRuns} />
              <Metric name="Active workers" value={value.activeWorkers} />
              <Metric name="Outbox" value={value.outboxBacklog} />
              <Metric name="DLQ" value={value.dlqDepth} />
              <Metric name="Oldest outbox" value={duration(value.oldestOutboxAge)} />
            </dl>
          )}
        </ResourceBoundary>
      </section>
      <section className="operator-card">
        <h2>Worker drain</h2>
        <p className="muted">Gracefully stop one registered worker from claiming new attempts.</p>
        <div className="inline-form">
          <label>
            Worker ID
            <input value={workerId} onChange={(event) => setWorkerId(event.target.value)} />
          </label>
          <button className="button-danger" disabled={!workerId.trim()} onClick={() => void drain()}>
            Drain
          </button>
        </div>
        <FeedbackMessage value={feedback} />
      </section>
    </div>
  );
}

function Metric({ name, value }: { name: string; value: string | number }) {
  return (
    <div>
      <dt>{name}</dt>
      <dd>{value}</dd>
    </div>
  );
}
function duration(nanoseconds: number): string {
  return nanoseconds <= 0 ? "0s" : `${Math.round(nanoseconds / 1_000_000_000)}s`;
}

export function DeliveryPanel({ client }: { client: StudioClient }) {
  const loader = useCallback((signal: AbortSignal) => client.listDeadLetters(signal), [client]);
  const resource = useOperatorResource<DeadLetter[]>(loader);
  const [feedback, setFeedback] = useState<Feedback>();
  const requeue = async (item: DeadLetter) => {
    if (!confirm(`Requeue dead letter “${item.id}”? Delivery can be attempted again.`)) return;
    setFeedback({ kind: "loading", message: "Requeueing delivery…" });
    try {
      await client.requeueDeadLetter(item.id);
      setFeedback({ kind: "success", message: "Dead letter requeued." });
      resource.reload();
    } catch (error) {
      setFeedback({ kind: "error", message: errorMessage(error) });
    }
  };
  return (
    <section className="operator-card">
      <div className="section-heading">
        <h2>Delivery dead letters</h2>
        <button onClick={resource.reload}>Refresh</button>
      </div>
      <FeedbackMessage value={feedback} />
      <ResourceBoundary state={resource.state} retry={resource.reload} empty={(value) => value.length === 0}>
        {(items) => (
          <div className="operator-list">
            {items.map((item) => (
              <article key={item.id}>
                <div>
                  <strong>{item.topic}</strong>
                  <p>
                    {item.id} · {item.attempts} attempts · {new Date(item.failedAt).toLocaleString()}
                  </p>
                  <p className="danger-text">{item.lastError}</p>
                </div>
                <button onClick={() => void requeue(item)}>Requeue</button>
              </article>
            ))}
          </div>
        )}
      </ResourceBoundary>
    </section>
  );
}

export function QuotaPanel({ client }: { client: StudioClient }) {
  const [values, setValues] = useState({ maxActiveRuns: 20, maxActiveWorkers: 10, maxQueueDepth: 100 });
  const [feedback, setFeedback] = useState<Feedback>();
  const save = async () => {
    setFeedback({ kind: "loading", message: "Saving tenant quota…" });
    try {
      await client.configureQuota(values);
      setFeedback({ kind: "success", message: "Tenant quota configured." });
    } catch (error) {
      setFeedback({ kind: "error", message: errorMessage(error) });
    }
  };
  return (
    <section className="operator-card">
      <h2>Tenant quota</h2>
      <div className="field-grid">
        {Object.entries(values).map(([key, value]) => (
          <label key={key}>
            {quotaLabel(key)}
            <input
              type="number"
              min="1"
              value={value}
              onChange={(event) => setValues((current) => ({ ...current, [key]: Number(event.target.value) }))}
            />
          </label>
        ))}
      </div>
      <button className="button-primary" onClick={() => void save()}>
        Save quota
      </button>
      <FeedbackMessage value={feedback} />
    </section>
  );
}
function quotaLabel(key: string) {
  return (
    { maxActiveRuns: "Active runs", maxActiveWorkers: "Active workers", maxQueueDepth: "Queue depth" } as Record<
      string,
      string
    >
  )[key];
}

export function AuditPanel({ client }: { client: StudioClient }) {
  const [filters, setFilters] = useState({ resourceType: "", resourceId: "" });
  const [applied, setApplied] = useState(filters);
  const loader = useCallback((signal: AbortSignal) => client.listAudit(applied, signal), [applied, client]);
  const resource = useOperatorResource<AuditRecord[]>(loader);
  return (
    <section className="operator-card operator-wide">
      <div className="section-heading">
        <h2>Audit evidence</h2>
        <button onClick={resource.reload}>Refresh</button>
      </div>
      <form
        className="inline-form"
        onSubmit={(event) => {
          event.preventDefault();
          setApplied(filters);
        }}
      >
        <label>
          Resource type
          <input
            value={filters.resourceType}
            onChange={(event) => setFilters((value) => ({ ...value, resourceType: event.target.value }))}
          />
        </label>
        <label>
          Resource ID
          <input
            value={filters.resourceId}
            onChange={(event) => setFilters((value) => ({ ...value, resourceId: event.target.value }))}
          />
        </label>
        <button type="submit">Filter</button>
      </form>
      <ResourceBoundary state={resource.state} retry={resource.reload} empty={(value) => value.length === 0}>
        {(items) => (
          <div className="operator-list">
            {items.map((item) => (
              <article key={item.id}>
                <div>
                  <strong>{item.action}</strong>
                  <p>
                    {item.actorId} · {item.resourceType}/{item.resourceId} · {new Date(item.createdAt).toLocaleString()}
                  </p>
                </div>
                <span className={`outcome outcome--${item.outcome}`}>{item.outcome}</span>
              </article>
            ))}
          </div>
        )}
      </ResourceBoundary>
    </section>
  );
}
