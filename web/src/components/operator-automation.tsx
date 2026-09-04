import { useCallback, useState } from "react";

import type { StudioClient } from "../api/client";
import type { Trigger, TriggerInput, TriggerKind, WebhookSubscription } from "../domain/models";
import { mutationKey, parseJSONObject, useOperatorResource } from "../state/use-operator-resource";
import { errorMessage, type Feedback, FeedbackMessage, ResourceBoundary } from "./operator-shared";

const defaultTrigger: TriggerInput = { workflowId: "", kind: "api", name: "", config: {} };

export function TriggerPanel({ client }: { client: StudioClient }) {
  const loader = useCallback((signal: AbortSignal) => client.listTriggers(signal), [client]);
  const resource = useOperatorResource<Trigger[]>(loader);
  const [input, setInput] = useState(defaultTrigger);
  const [config, setConfig] = useState("{}");
  const [editing, setEditing] = useState<Trigger>();
  const [editConfig, setEditConfig] = useState("{}");
  const [firePayload, setFirePayload] = useState("{}");
  const [feedback, setFeedback] = useState<Feedback>();

  const create = async () => {
    setFeedback({ kind: "loading", message: "Creating trigger…" });
    try {
      await client.createTrigger({ ...input, config: parseJSONObject(config) }, mutationKey("trigger-create"));
      setInput(defaultTrigger);
      setConfig("{}");
      setFeedback({ kind: "success", message: "Trigger created." });
      resource.reload();
    } catch (error) {
      setFeedback({ kind: "error", message: errorMessage(error) });
    }
  };
  const saveEdit = async () => {
    if (!editing) return;
    setFeedback({ kind: "loading", message: "Updating trigger…" });
    try {
      await client.updateTrigger(
        editing,
        {
          name: editing.name,
          config: parseJSONObject(editConfig),
          scheduleIntervalSeconds: durationSeconds(editing.scheduleInterval),
          misfirePolicy: editing.misfirePolicy,
          signingCredentialId: editing.signingCredentialId,
          eventType: editing.eventType,
        },
        mutationKey("trigger-update"),
      );
      setEditing(undefined);
      setFeedback({ kind: "success", message: "Trigger updated." });
      resource.reload();
    } catch (error) {
      setFeedback({ kind: "error", message: errorMessage(error) });
    }
  };
  const toggle = async (item: Trigger) => {
    if (item.enabled && !confirm(`Disable trigger “${item.name}”? New firings will stop.`)) return;
    setFeedback({ kind: "loading", message: `${item.enabled ? "Disabling" : "Enabling"} trigger…` });
    try {
      await client.setTriggerEnabled(item, !item.enabled, mutationKey("trigger-state"));
      setFeedback({ kind: "success", message: `Trigger ${item.enabled ? "disabled" : "enabled"}.` });
      resource.reload();
    } catch (error) {
      setFeedback({ kind: "error", message: errorMessage(error) });
    }
  };
  const remove = async (item: Trigger) => {
    if (!confirm(`Delete trigger “${item.name}”? Existing firing history is retained.`)) return;
    setFeedback({ kind: "loading", message: "Deleting trigger…" });
    try {
      await client.deleteTrigger(item, mutationKey("trigger-delete"));
      setFeedback({ kind: "success", message: "Trigger deleted." });
      resource.reload();
    } catch (error) {
      setFeedback({ kind: "error", message: errorMessage(error) });
    }
  };
  const fire = async (item: Trigger) => {
    setFeedback({ kind: "loading", message: "Submitting trigger firing…" });
    try {
      await client.fireTrigger(item, parseJSONObject(firePayload), mutationKey("trigger-fire"));
      setFeedback({ kind: "success", message: "Trigger firing accepted." });
    } catch (error) {
      setFeedback({ kind: "error", message: errorMessage(error) });
    }
  };
  return (
    <section className="operator-card operator-wide">
      <div className="section-heading">
        <h2>Triggers</h2>
        <button onClick={resource.reload}>Refresh</button>
      </div>
      <FeedbackMessage value={feedback} />
      <fieldset>
        <legend>Create trigger</legend>
        <div className="field-grid">
          <label>
            Workflow ID
            <input
              value={input.workflowId}
              onChange={(event) => setInput((value) => ({ ...value, workflowId: event.target.value }))}
            />
          </label>
          <label>
            Name
            <input
              value={input.name}
              onChange={(event) => setInput((value) => ({ ...value, name: event.target.value }))}
            />
          </label>
          <label>
            Kind
            <select
              value={input.kind}
              onChange={(event) => setInput((value) => ({ ...value, kind: event.target.value as TriggerKind }))}
            >
              {["api", "manual", "schedule", "webhook", "event"].map((kind) => (
                <option key={kind}>{kind}</option>
              ))}
            </select>
          </label>
          <label>
            Config JSON
            <textarea rows={2} value={config} onChange={(event) => setConfig(event.target.value)} />
          </label>
          {input.kind === "schedule" && (
            <>
              <label>
                Interval seconds
                <input
                  type="number"
                  min="1"
                  value={input.scheduleIntervalSeconds ?? 60}
                  onChange={(event) =>
                    setInput((value) => ({
                      ...value,
                      scheduleIntervalSeconds: Number(event.target.value),
                      misfirePolicy: value.misfirePolicy ?? "fire_once",
                    }))
                  }
                />
              </label>
              <label>
                Misfire policy
                <select
                  value={input.misfirePolicy ?? "fire_once"}
                  onChange={(event) =>
                    setInput((value) => ({
                      ...value,
                      misfirePolicy: event.target.value as TriggerInput["misfirePolicy"],
                    }))
                  }
                >
                  <option value="skip">skip</option>
                  <option value="fire_once">fire once</option>
                  <option value="catch_up">catch up</option>
                </select>
              </label>
            </>
          )}
          {input.kind === "webhook" && (
            <label>
              Signing credential ID
              <input
                value={input.signingCredentialId ?? ""}
                onChange={(event) => setInput((value) => ({ ...value, signingCredentialId: event.target.value }))}
              />
            </label>
          )}
          {input.kind === "event" && (
            <label>
              Event type
              <input
                value={input.eventType ?? ""}
                onChange={(event) => setInput((value) => ({ ...value, eventType: event.target.value }))}
              />
            </label>
          )}
        </div>
        <button
          className="button-primary"
          disabled={!input.workflowId.trim() || !input.name.trim()}
          onClick={() => void create()}
        >
          Create trigger
        </button>
      </fieldset>
      <label>
        Manual/API fire payload
        <textarea rows={2} value={firePayload} onChange={(event) => setFirePayload(event.target.value)} />
      </label>
      <ResourceBoundary state={resource.state} retry={resource.reload} empty={(value) => value.length === 0}>
        {(items) => (
          <div className="operator-list">
            {items.map((item) => (
              <article key={item.id}>
                <div className="operator-item-main">
                  <strong>{item.name}</strong>
                  <p>
                    {item.kind} · {item.workflowId} · {item.id}
                  </p>
                  {item.publicHash && <p>Public hook: /hooks/{item.publicHash}</p>}
                </div>
                <span className={item.enabled ? "healthy" : "muted"}>{item.enabled ? "enabled" : "disabled"}</span>
                <button
                  onClick={() => {
                    setEditing(item);
                    setEditConfig(JSON.stringify(item.config, null, 2));
                  }}
                >
                  Edit
                </button>
                <button onClick={() => void toggle(item)}>{item.enabled ? "Disable" : "Enable"}</button>
                <button disabled={item.kind !== "api" && item.kind !== "manual"} onClick={() => void fire(item)}>
                  Fire
                </button>
                <button className="button-danger" onClick={() => void remove(item)}>
                  Delete
                </button>
              </article>
            ))}
          </div>
        )}
      </ResourceBoundary>
      {editing && (
        <fieldset>
          <legend>Edit {editing.name}</legend>
          <div className="field-grid">
            <label>
              Name
              <input value={editing.name} onChange={(event) => setEditing({ ...editing, name: event.target.value })} />
            </label>
            <label>
              Config JSON
              <textarea rows={2} value={editConfig} onChange={(event) => setEditConfig(event.target.value)} />
            </label>
            {editing.kind === "schedule" && (
              <label>
                Interval seconds
                <input
                  type="number"
                  min="1"
                  value={durationSeconds(editing.scheduleInterval)}
                  onChange={(event) =>
                    setEditing({ ...editing, scheduleInterval: Number(event.target.value) * 1_000_000_000 })
                  }
                />
              </label>
            )}
          </div>
          <div className="button-row">
            <button className="button-primary" onClick={() => void saveEdit()}>
              Save update
            </button>
            <button onClick={() => setEditing(undefined)}>Cancel</button>
          </div>
        </fieldset>
      )}
    </section>
  );
}

function durationSeconds(value?: number) {
  return value ? Math.round(value / 1_000_000_000) : undefined;
}

export function WebhookSubscriptionPanel({ client }: { client: StudioClient }) {
  const loader = useCallback((signal: AbortSignal) => client.listWebhookSubscriptions(signal), [client]);
  const resource = useOperatorResource<WebhookSubscription[]>(loader);
  const [form, setForm] = useState({
    name: "",
    endpointUrl: "",
    eventTypes: "run.succeeded,run.failed",
    signingCredentialId: "",
    timeoutMs: 3000,
  });
  const [feedback, setFeedback] = useState<Feedback>();
  const create = async () => {
    setFeedback({ kind: "loading", message: "Creating outbound subscription…" });
    try {
      await client.createWebhookSubscription({
        ...form,
        eventTypes: form.eventTypes
          .split(",")
          .map((item) => item.trim())
          .filter(Boolean),
      });
      setFeedback({ kind: "success", message: "Outbound subscription created." });
      setForm({
        name: "",
        endpointUrl: "",
        eventTypes: "run.succeeded,run.failed",
        signingCredentialId: "",
        timeoutMs: 3000,
      });
      resource.reload();
    } catch (error) {
      setFeedback({ kind: "error", message: errorMessage(error) });
    }
  };
  const toggle = async (item: WebhookSubscription) => {
    if (
      item.status === "enabled" &&
      !confirm(`Disable outbound subscription “${item.name}”? Matching events will not be delivered.`)
    )
      return;
    setFeedback({ kind: "loading", message: "Updating subscription state…" });
    try {
      await client.setWebhookSubscriptionEnabled(item.id, item.status !== "enabled");
      setFeedback({ kind: "success", message: "Subscription state updated." });
      resource.reload();
    } catch (error) {
      setFeedback({ kind: "error", message: errorMessage(error) });
    }
  };
  const remove = async (item: WebhookSubscription) => {
    if (!confirm(`Delete outbound subscription “${item.name}”?`)) return;
    setFeedback({ kind: "loading", message: "Deleting outbound subscription…" });
    try {
      await client.deleteWebhookSubscription(item.id);
      setFeedback({ kind: "success", message: "Subscription deleted." });
      resource.reload();
    } catch (error) {
      setFeedback({ kind: "error", message: errorMessage(error) });
    }
  };
  return (
    <section className="operator-card operator-wide">
      <div className="section-heading">
        <h2>Outbound webhooks</h2>
        <button onClick={resource.reload}>Refresh</button>
      </div>
      <FeedbackMessage value={feedback} />
      <fieldset>
        <legend>Create subscription</legend>
        <div className="field-grid">
          <label>
            Name
            <input
              value={form.name}
              onChange={(event) => setForm((value) => ({ ...value, name: event.target.value }))}
            />
          </label>
          <label>
            HTTPS endpoint
            <input
              type="url"
              value={form.endpointUrl}
              onChange={(event) => setForm((value) => ({ ...value, endpointUrl: event.target.value }))}
            />
          </label>
          <label>
            Event types (comma separated)
            <input
              value={form.eventTypes}
              onChange={(event) => setForm((value) => ({ ...value, eventTypes: event.target.value }))}
            />
          </label>
          <label>
            Signing credential ID
            <input
              value={form.signingCredentialId}
              onChange={(event) => setForm((value) => ({ ...value, signingCredentialId: event.target.value }))}
            />
          </label>
          <label>
            Timeout ms
            <input
              type="number"
              min="100"
              max="10000"
              value={form.timeoutMs}
              onChange={(event) => setForm((value) => ({ ...value, timeoutMs: Number(event.target.value) }))}
            />
          </label>
        </div>
        <button
          className="button-primary"
          disabled={!form.name || !form.endpointUrl || !form.signingCredentialId}
          onClick={() => void create()}
        >
          Create subscription
        </button>
      </fieldset>
      <ResourceBoundary state={resource.state} retry={resource.reload} empty={(value) => value.length === 0}>
        {(items) => (
          <div className="operator-list">
            {items.map((item) => (
              <article key={item.id}>
                <div className="operator-item-main">
                  <strong>{item.name}</strong>
                  <p>{item.endpointUrl}</p>
                  <p>
                    {item.eventTypes.join(", ")} · signer {item.signingCredentialId}
                  </p>
                </div>
                <span>{item.status}</span>
                <button onClick={() => void toggle(item)}>{item.status === "enabled" ? "Disable" : "Enable"}</button>
                <button className="button-danger" onClick={() => void remove(item)}>
                  Delete
                </button>
              </article>
            ))}
          </div>
        )}
      </ResourceBoundary>
    </section>
  );
}
