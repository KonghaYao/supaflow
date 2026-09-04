import { useState } from "react";

import type { StudioClient } from "../api/client";
import type { CredentialMetadata, TenantExport } from "../domain/models";
import { consumeSecretForm } from "../state/use-operator-resource";
import { errorMessage, type Feedback, FeedbackMessage } from "./operator-shared";

export function CredentialPanel({ client }: { client: StudioClient }) {
  const [created, setCreated] = useState<CredentialMetadata[]>([]);
  const [createForm, setCreateForm] = useState({ name: "", purpose: "http_auth", secret: "" });
  const [rotateForm, setRotateForm] = useState({ id: "", version: 2, secret: "" });
  const [revokeId, setRevokeId] = useState("");
  const [feedback, setFeedback] = useState<Feedback>();
  const create = async () => {
    const { command, cleared } = consumeSecretForm(createForm);
    setCreateForm(cleared);
    setFeedback({ kind: "loading", message: "Encrypting and creating credential…" });
    try {
      const result = await client.createCredential(command);
      setCreated((items) => [result, ...items.filter((item) => item.id !== result.id)]);
      setFeedback({
        kind: "success",
        message: `Credential ${result.id} created. Its secret was cleared from this form.`,
      });
    } catch (error) {
      setFeedback({ kind: "error", message: errorMessage(error) });
    }
  };
  const rotate = async () => {
    const { command, cleared } = consumeSecretForm(rotateForm);
    setRotateForm(cleared);
    setFeedback({ kind: "loading", message: "Rotating credential…" });
    try {
      const result = await client.rotateCredential(command.id, command.version, command.secret);
      setCreated((items) => [result, ...items.filter((item) => item.id !== result.id)]);
      setFeedback({
        kind: "success",
        message: `Credential rotated to version ${result.currentVersion}. Secret cleared.`,
      });
    } catch (error) {
      setFeedback({ kind: "error", message: errorMessage(error) });
    }
  };
  const revoke = async () => {
    if (!confirm(`Permanently revoke credential “${revokeId}”? Running attempts can lose external access.`)) return;
    setFeedback({ kind: "loading", message: "Revoking credential…" });
    try {
      const result = await client.revokeCredential(revokeId);
      setCreated((items) => [result, ...items.filter((item) => item.id !== result.id)]);
      setRevokeId("");
      setFeedback({ kind: "success", message: "Credential revoked." });
    } catch (error) {
      setFeedback({ kind: "error", message: errorMessage(error) });
    }
  };
  return (
    <section className="operator-card operator-wide">
      <h2>Credentials</h2>
      <p className="muted">
        Secret values exist only inside the active form, are cleared before the request completes, and never appear in
        metadata.
      </p>
      <FeedbackMessage value={feedback} />
      <div className="operator-grid">
        <fieldset>
          <legend>Create</legend>
          <label>
            Name
            <input
              value={createForm.name}
              onChange={(event) => setCreateForm((value) => ({ ...value, name: event.target.value }))}
            />
          </label>
          <label>
            Purpose
            <input
              value={createForm.purpose}
              onChange={(event) => setCreateForm((value) => ({ ...value, purpose: event.target.value }))}
            />
          </label>
          <label>
            Secret
            <input
              type="password"
              autoComplete="new-password"
              value={createForm.secret}
              onChange={(event) => setCreateForm((value) => ({ ...value, secret: event.target.value }))}
            />
          </label>
          <button
            className="button-primary"
            disabled={!createForm.name || !createForm.purpose || !createForm.secret}
            onClick={() => void create()}
          >
            Create and clear
          </button>
        </fieldset>
        <fieldset>
          <legend>Rotate</legend>
          <label>
            Credential ID
            <input
              value={rotateForm.id}
              onChange={(event) => setRotateForm((value) => ({ ...value, id: event.target.value }))}
            />
          </label>
          <label>
            Next version
            <input
              type="number"
              min="2"
              value={rotateForm.version}
              onChange={(event) => setRotateForm((value) => ({ ...value, version: Number(event.target.value) }))}
            />
          </label>
          <label>
            New secret
            <input
              type="password"
              autoComplete="new-password"
              value={rotateForm.secret}
              onChange={(event) => setRotateForm((value) => ({ ...value, secret: event.target.value }))}
            />
          </label>
          <button
            className="button-primary"
            disabled={!rotateForm.id || !rotateForm.secret}
            onClick={() => void rotate()}
          >
            Rotate and clear
          </button>
        </fieldset>
        <fieldset>
          <legend>Revoke</legend>
          <label>
            Credential ID
            <input value={revokeId} onChange={(event) => setRevokeId(event.target.value)} />
          </label>
          <button className="button-danger" disabled={!revokeId} onClick={() => void revoke()}>
            Revoke permanently
          </button>
        </fieldset>
      </div>
      {created.length === 0 ? (
        <div className="operator-empty">
          <p>No credential metadata created in this session. Paste an existing ID to rotate or revoke it.</p>
        </div>
      ) : (
        <div className="operator-list">
          {created.map((item) => (
            <article key={`${item.id}-${item.currentVersion}`}>
              <div>
                <strong>{item.name || item.id}</strong>
                <p>
                  {item.id} · {item.purpose} · version {item.currentVersion}
                </p>
              </div>
              <span>{item.status}</span>
            </article>
          ))}
        </div>
      )}
    </section>
  );
}

export function RetentionPanel({ client }: { client: StudioClient }) {
  const [policy, setPolicy] = useState({ eventDays: 30, outputDays: 30, archiveBeforeDelete: true });
  const [exports, setExports] = useState<TenantExport[]>([]);
  const [feedback, setFeedback] = useState<Feedback>();
  const save = async () => {
    if (!confirm("Apply this retention policy to the authenticated tenant? Expired data can later be removed.")) return;
    setFeedback({ kind: "loading", message: "Applying retention policy…" });
    try {
      await client.configureRetention(policy);
      setFeedback({ kind: "success", message: "Retention policy configured." });
    } catch (error) {
      setFeedback({ kind: "error", message: errorMessage(error) });
    }
  };
  const createExport = async () => {
    setFeedback({ kind: "loading", message: "Starting tenant export…" });
    try {
      const result = await client.exportTenant();
      setExports((items) => [result, ...items]);
      setFeedback({ kind: "success", message: "Tenant export accepted." });
    } catch (error) {
      setFeedback({ kind: "error", message: errorMessage(error) });
    }
  };
  return (
    <section className="operator-card operator-wide">
      <h2>Retention and export</h2>
      <FeedbackMessage value={feedback} />
      <div className="field-grid">
        <label>
          Event days
          <input
            type="number"
            min="1"
            value={policy.eventDays}
            onChange={(event) => setPolicy((value) => ({ ...value, eventDays: Number(event.target.value) }))}
          />
        </label>
        <label>
          Output days
          <input
            type="number"
            min="1"
            value={policy.outputDays}
            onChange={(event) => setPolicy((value) => ({ ...value, outputDays: Number(event.target.value) }))}
          />
        </label>
        <label className="check-field">
          <input
            type="checkbox"
            checked={policy.archiveBeforeDelete}
            onChange={(event) => setPolicy((value) => ({ ...value, archiveBeforeDelete: event.target.checked }))}
          />
          Archive before delete
        </label>
      </div>
      <div className="button-row">
        <button className="button-danger" onClick={() => void save()}>
          Apply retention
        </button>
        <button onClick={() => void createExport()}>Create 24-hour export</button>
      </div>
      {exports.length === 0 ? (
        <div className="operator-empty">
          <p>No exports requested in this session.</p>
        </div>
      ) : (
        <div className="operator-list">
          {exports.map((item) => (
            <article key={item.id}>
              <div>
                <strong>{item.id}</strong>
                <p>
                  Created {new Date(item.createdAt).toLocaleString()}{" "}
                  {item.expiresAt && `· expires ${new Date(item.expiresAt).toLocaleString()}`}
                </p>
              </div>
              <span>{item.status}</span>
            </article>
          ))}
        </div>
      )}
    </section>
  );
}
