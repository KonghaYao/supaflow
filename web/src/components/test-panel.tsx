import { useState } from "react";

import type { StudioClient } from "../api/client";

interface Props {
  client: StudioClient;
  workflowId: string;
  selectedNodeId?: string;
}

/** Side-effect-free workflow and node test controls with complete async states. */
export function TestPanel({ client, workflowId, selectedNodeId }: Props) {
  const [sample, setSample] = useState('{\n  "message": "hello"\n}');
  const [state, setState] = useState<"idle" | "loading" | "success" | "error">("idle");
  const [result, setResult] = useState<unknown>();
  const execute = async (kind: "dry-run" | "node-test") => {
    setState("loading");
    try {
      const input: unknown = JSON.parse(sample);
      const response =
        kind === "dry-run"
          ? await client.dryRun(workflowId, input)
          : await client.nodeTest(workflowId, selectedNodeId ?? "", input);
      setResult(response);
      setState("success");
    } catch (error) {
      setResult(error instanceof Error ? error.message : "Test failed.");
      setState("error");
    }
  };
  return (
    <section className="utility-panel" aria-labelledby="test-title">
      <div className="section-heading">
        <div>
          <span className="eyebrow">No side effects</span>
          <h2 id="test-title">Test bench</h2>
        </div>
      </div>
      <label>
        Sample input
        <textarea rows={5} value={sample} onChange={(event) => setSample(event.target.value)} />
      </label>
      <div className="button-row">
        <button disabled={state === "loading"} onClick={() => void execute("dry-run")}>
          Dry run
        </button>
        <button disabled={!selectedNodeId || state === "loading"} onClick={() => void execute("node-test")}>
          Test node
        </button>
      </div>
      {state === "idle" && (
        <p className="muted">Run a validation preview to inspect resolved order and executor binding.</p>
      )}
      {state === "loading" && <p role="status">Testing…</p>}
      {state === "error" && (
        <div className="inline-error" role="alert">
          <strong>Test failed</strong>
          <pre>{String(result)}</pre>
          <button onClick={() => void execute("dry-run")}>Retry</button>
        </div>
      )}
      {state === "success" && (
        <pre className="result-output" role="region" aria-label="Test output">
          {JSON.stringify(result, null, 2)}
        </pre>
      )}
    </section>
  );
}
