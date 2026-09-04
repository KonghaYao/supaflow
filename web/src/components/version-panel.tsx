import { useCallback, useEffect, useState } from "react";

import type { StudioClient } from "../api/client";
import type { VersionSummary } from "../domain/models";

interface Props {
  client: StudioClient;
  workflowId: string;
  refreshToken: number;
}

/** Immutable version history and two-version structural diff viewer. */
export function VersionPanel({ client, workflowId, refreshToken }: Props) {
  const [versions, setVersions] = useState<VersionSummary[]>([]);
  const [state, setState] = useState<"loading" | "ready" | "error">("loading");
  const [selection, setSelection] = useState<number[]>([]);
  const [diff, setDiff] = useState<unknown>();
  const load = useCallback(async () => {
    setState("loading");
    try {
      setVersions(await client.versions(workflowId));
      setState("ready");
    } catch {
      setState("error");
    }
  }, [client, workflowId]);
  useEffect(() => {
    // The parent increments this token after publication to request a fresh history snapshot.
    void refreshToken;
    void load();
  }, [load, refreshToken]);
  useEffect(() => {
    if (selection.length !== 2) {
      setDiff(undefined);
      return;
    }
    void client
      .diff(workflowId, selection[0], selection[1])
      .then(setDiff)
      .catch((error: unknown) => setDiff({ error: error instanceof Error ? error.message : "Diff failed." }));
  }, [client, selection, workflowId]);
  return (
    <section className="utility-panel" aria-labelledby="versions-title">
      <div className="section-heading">
        <h2 id="versions-title">Versions</h2>
        <button onClick={() => void load()}>Refresh</button>
      </div>
      {state === "loading" && <p role="status">Loading versions…</p>}
      {state === "error" && (
        <div role="alert" className="inline-error">
          Could not load history. <button onClick={() => void load()}>Retry</button>
        </div>
      )}
      {state === "ready" && versions.length === 0 && <p className="muted">No published versions yet.</p>}
      {versions.map((version) => (
        <label className="version-row" key={version.versionId}>
          <input
            type="checkbox"
            checked={selection.includes(version.version)}
            onChange={() =>
              setSelection((current) =>
                current.includes(version.version)
                  ? current.filter((item) => item !== version.version)
                  : [...current, version.version].slice(-2),
              )
            }
          />
          <span>
            <strong>v{version.version}</strong>
            <small>
              {version.planHash.slice(0, 10)} · {new Date(version.publishedAt).toLocaleString()}
            </small>
          </span>
        </label>
      ))}
      {diff !== undefined && (
        <pre className="result-output" role="region" aria-label="Version diff">
          {JSON.stringify(diff, null, 2)}
        </pre>
      )}
    </section>
  );
}
