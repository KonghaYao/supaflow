import { useCallback, useEffect, useReducer, useRef, useState } from "react";

import type { StudioClient } from "../api/client";
import type { ApprovalDecisionInput, RunEvent, RunInterrupt, RerunInput } from "../domain/models";
import {
  canCancelRun,
  canRerunRun,
  initialRunMonitorState,
  isApprovalInterrupt,
  isInterruptExpired,
  reduceRunMonitor,
} from "./run-monitor-state";

interface Props {
  client: StudioClient;
  workflowId: string;
}

type InterruptLoadState = "idle" | "loading" | "ready" | "error";
type OperationState = "idle" | "loading" | "error";
type PendingOperation =
  | { kind: "cancel"; runId: string; key: string }
  | { kind: "rerun"; runId: string; key: string; input: RerunInput }
  | { kind: "approval"; runId: string; interruptId: string; key: string; input: ApprovalDecisionInput };

const terminalEvents = new Set(["run.succeeded", "run.failed", "run.error", "run.cancelled"]);

/** Tenant-scoped run discovery, diagnostics, control, and resumable SSE timeline. */
export function RunMonitor({ client, workflowId }: Props) {
  const [state, dispatch] = useReducer(reduceRunMonitor, initialRunMonitorState);
  const [events, setEvents] = useState<RunEvent[]>([]);
  const [streamState, setStreamState] = useState<"idle" | "connecting" | "streaming" | "complete" | "error">(
    "idle",
  );
  const [streamError, setStreamError] = useState<string>();
  const [interrupts, setInterrupts] = useState<RunInterrupt[]>([]);
  const [interruptState, setInterruptState] = useState<InterruptLoadState>("idle");
  const [interruptError, setInterruptError] = useState<string>();
  const [comments, setComments] = useState<Record<string, string>>({});
  const [operationState, setOperationState] = useState<OperationState>("idle");
  const [pendingOperation, setPendingOperation] = useState<PendingOperation>();
  const [operationError, setOperationError] = useState<string>();
  const [operationSuccess, setOperationSuccess] = useState<string>();
  const activeStream = useRef<AbortController | undefined>(undefined);
  const activeList = useRef<AbortController | undefined>(undefined);
  const activeDetails = useRef<AbortController | undefined>(undefined);
  const activeInterrupts = useRef<AbortController | undefined>(undefined);

  const loadRuns = useCallback(async () => {
    activeList.current?.abort();
    const controller = new AbortController();
    activeList.current = controller;
    dispatch({ type: "listLoading" });
    try {
      const loaded = await client.listRuns(workflowId, controller.signal);
      dispatch({ type: "listLoaded", runs: loaded });
    } catch (reason) {
      if (controller.signal.aborted) return;
      dispatch({ type: "listFailed", error: errorMessage(reason, "Could not load runs.") });
    }
  }, [client, workflowId]);

  const loadDetails = useCallback(
    (runId: string) => {
      if (!runId) return;
      activeDetails.current?.abort();
      const controller = new AbortController();
      activeDetails.current = controller;
      dispatch({ type: "detailsLoading" });
      void client
        .getRunDetails(runId, controller.signal)
        .then((details) => dispatch({ type: "detailsLoaded", details }))
        .catch((reason: unknown) => {
          if (!controller.signal.aborted)
            dispatch({ type: "detailsFailed", error: errorMessage(reason, "Could not load diagnostics.") });
        });
    },
    [client],
  );

  const loadInterrupts = useCallback(
    (runId: string) => {
      if (!runId) return;
      activeInterrupts.current?.abort();
      const controller = new AbortController();
      activeInterrupts.current = controller;
      setInterruptState("loading");
      setInterruptError(undefined);
      void client
        .listRunInterrupts(runId, controller.signal)
        .then((loaded) => {
          setInterrupts(loaded);
          setInterruptState("ready");
        })
        .catch((reason: unknown) => {
          if (controller.signal.aborted) return;
          setInterruptError(errorMessage(reason, "Could not load run interrupts."));
          setInterruptState("error");
        });
    },
    [client],
  );

  useEffect(() => {
    activeStream.current?.abort();
    activeInterrupts.current?.abort();
    setEvents([]);
    setInterrupts([]);
    setInterruptState("idle");
    dispatch({ type: "reset" });
    setStreamState("idle");
    clearOperationFeedback(setOperationState, setPendingOperation, setOperationError, setOperationSuccess);
    void loadRuns();
    return () => {
      activeList.current?.abort();
      activeDetails.current?.abort();
      activeInterrupts.current?.abort();
      activeStream.current?.abort();
    };
  }, [loadRuns]);

  useEffect(() => {
    if (!state.selectedId) return;
    loadDetails(state.selectedId);
    loadInterrupts(state.selectedId);
  }, [loadDetails, loadInterrupts, state.selectedId]);

  const executeOperation = async (operation: PendingOperation) => {
    setPendingOperation(operation);
    setOperationState("loading");
    setOperationError(undefined);
    setOperationSuccess(undefined);
    try {
      let result;
      if (operation.kind === "cancel") result = await client.cancelRun(operation.runId, operation.key);
      else if (operation.kind === "rerun")
        result = await client.rerunRun(operation.runId, operation.input, operation.key);
      else result = await client.resolveApproval(operation.interruptId, operation.input, operation.key);

      await loadRuns();
      if (operation.kind === "rerun") dispatch({ type: "selected", runId: result.id });
      else {
        loadDetails(operation.runId);
        loadInterrupts(operation.runId);
      }
      setOperationSuccess(successMessage(operation));
      setOperationState("idle");
      setPendingOperation(undefined);
    } catch (reason) {
      setOperationError(errorMessage(reason, "The run operation could not be completed."));
      setOperationState("error");
    }
  };

  const confirmCancel = () => {
    if (
      !state.selectedId ||
      !globalThis.confirm("Cancel this run? In-flight work will stop at a safe boundary.")
    )
      return;
    void executeOperation({ kind: "cancel", runId: state.selectedId, key: operationKey("run-cancel") });
  };

  const confirmRerun = () => {
    if (!state.selectedId || !globalThis.confirm("Create a new run from this run's immutable plan and input?")) return;
    void executeOperation({ kind: "rerun", runId: state.selectedId, input: {}, key: operationKey("run-rerun") });
  };

  const confirmApproval = (interrupt: RunInterrupt, decision: "approved" | "rejected") => {
    if (!interrupt.resumeToken) return;
    const verb = decision === "approved" ? "approve" : "reject";
    if (!globalThis.confirm(`${verb[0]?.toUpperCase()}${verb.slice(1)} this workflow request?`)) return;
    void executeOperation({
      kind: "approval",
      runId: interrupt.runId,
      interruptId: interrupt.id,
      key: operationKey(`approval-${decision}`),
      input: {
        resumeToken: interrupt.resumeToken,
        decision,
        comment: comments[interrupt.id]?.trim() || undefined,
      },
    });
  };

  const connect = async () => {
    activeStream.current?.abort();
    const controller = new AbortController();
    activeStream.current = controller;
    setEvents([]);
    setStreamState("connecting");
    setStreamError(undefined);
    try {
      setStreamState("streaming");
      await client.streamRunEvents(
        state.selectedId,
        0,
        (event) => {
          setEvents((current) =>
            current.some((item) => item.cursor === event.cursor) ? current : [...current, event],
          );
          if (terminalEvents.has(event.type)) setStreamState("complete");
        },
        controller.signal,
      );
      setStreamState((current) => (current === "complete" ? current : "complete"));
      await loadRuns();
    } catch (reason) {
      if (controller.signal.aborted) return;
      setStreamError(errorMessage(reason, "Stream disconnected."));
      setStreamState("error");
    }
  };

  const selectedRun = state.runs.find((run) => run.id === state.selectedId);
  const selectedStatus = state.details?.run.status ?? selectedRun?.status ?? "";
  const operationLoading = operationState === "loading";
  const approvals = interrupts.filter((interrupt) => isApprovalInterrupt(interrupt.kind) && !interrupt.resolvedAt);

  return (
    <section className="monitor" aria-labelledby="monitor-title">
      <div className="section-heading">
        <div>
          <span className="eyebrow">Durable execution</span>
          <h2 id="monitor-title">Run monitor</h2>
        </div>
        <button onClick={() => void loadRuns()}>Refresh</button>
      </div>
      {state.listState === "loading" && <p role="status">Loading recent runs…</p>}
      {state.listState === "empty" && (
        <p className="muted">No runs for this workflow yet. Publish and start a run to inspect it here.</p>
      )}
      {state.listState === "error" && (
        <div role="alert" className="inline-error">
          <p>{state.error}</p>
          <button onClick={() => void loadRuns()}>Retry</button>
        </div>
      )}
      {state.listState === "ready" && (
        <div className="monitor-connect">
          <label>
            Recent run
            <select
              aria-label="Recent run"
              disabled={operationLoading}
              value={state.selectedId}
              onChange={(event) => {
                dispatch({ type: "selected", runId: event.target.value });
                setEvents([]);
                setStreamState("idle");
                setComments({});
                clearOperationFeedback(setOperationState, setPendingOperation, setOperationError, setOperationSuccess);
              }}
            >
              {state.runs.map((run) => (
                <option value={run.id} key={run.id}>
                  {run.status} · {new Date(run.createdAt).toLocaleString()} · {run.id.slice(0, 14)}
                </option>
              ))}
            </select>
          </label>
          <button
            disabled={!state.selectedId || streamState === "connecting" || streamState === "streaming"}
            onClick={() => void connect()}
          >
            {streamState === "error" ? "Reconnect" : "Follow timeline"}
          </button>
        </div>
      )}

      {state.selectedId && (
        <div className="run-controls" aria-labelledby="run-controls-title" aria-busy={operationLoading}>
          <h3 id="run-controls-title">Run controls</h3>
          <div className="button-row">
            <button
              className="danger-button"
              disabled={operationLoading || !canCancelRun(selectedStatus)}
              onClick={confirmCancel}
              title={canCancelRun(selectedStatus) ? undefined : "Only active runs can be cancelled"}
            >
              {operationLoading && pendingOperation?.kind === "cancel" ? "Cancelling…" : "Cancel run"}
            </button>
            <button
              disabled={operationLoading || !canRerunRun(selectedStatus)}
              onClick={confirmRerun}
              title={canRerunRun(selectedStatus) ? undefined : "Only terminal runs can be rerun"}
            >
              {operationLoading && pendingOperation?.kind === "rerun" ? "Starting rerun…" : "Rerun"}
            </button>
          </div>
          {operationSuccess && (
            <p className="operation-success" role="status" aria-live="polite">
              {operationSuccess}
            </p>
          )}
          {operationError && (
            <div role="alert" className="inline-error">
              <p>{operationError}</p>
              {pendingOperation && (
                <button disabled={operationLoading} onClick={() => void executeOperation(pendingOperation)}>
                  Retry operation
                </button>
              )}
            </div>
          )}
        </div>
      )}

      {state.selectedId && (
        <section
          className="approval-panel"
          aria-labelledby="approvals-title"
          aria-busy={interruptState === "loading" || operationLoading}
        >
          <h3 id="approvals-title">Human approvals</h3>
          {interruptState === "loading" && <p role="status">Loading pending approvals…</p>}
          {interruptState === "error" && (
            <div role="alert" className="inline-error">
              <p>{interruptError}</p>
              <button onClick={() => loadInterrupts(state.selectedId)}>Retry approvals</button>
            </div>
          )}
          {interruptState === "ready" && approvals.length === 0 && (
            <p className="muted">No pending human approvals for this run.</p>
          )}
          {approvals.length > 0 && (
            <ul className="approval-list">
              {approvals.map((interrupt) => {
                const expired = isInterruptExpired(interrupt.expiresAt);
                const canResolve = Boolean(interrupt.resumeToken) && !expired;
                const loadingThisApproval =
                  operationLoading &&
                  pendingOperation?.kind === "approval" &&
                  pendingOperation.interruptId === interrupt.id;
                return (
                  <li key={interrupt.id}>
                    <p>
                      <strong>{interrupt.nodeId}</strong> · expires {new Date(interrupt.expiresAt).toLocaleString()}
                    </p>
                    {expired && <p className="muted">This approval request has expired and can no longer be resolved.</p>}
                    {!expired && !canResolve && (
                      <p className="muted">Approver access is required to resolve this request.</p>
                    )}
                    {canResolve && (
                      <>
                        <label htmlFor={`approval-comment-${interrupt.id}`}>Decision comment (optional)</label>
                        <textarea
                          id={`approval-comment-${interrupt.id}`}
                          value={comments[interrupt.id] ?? ""}
                          disabled={operationLoading}
                          onChange={(event) =>
                            setComments((current) => ({ ...current, [interrupt.id]: event.target.value }))
                          }
                        />
                        <div className="button-row">
                          <button disabled={operationLoading} onClick={() => confirmApproval(interrupt, "approved")}>
                            {loadingThisApproval && pendingOperation.input.decision === "approved"
                              ? "Approving…"
                              : "Approve"}
                          </button>
                          <button
                            className="danger-button"
                            disabled={operationLoading}
                            onClick={() => confirmApproval(interrupt, "rejected")}
                          >
                            {loadingThisApproval && pendingOperation.input.decision === "rejected"
                              ? "Rejecting…"
                              : "Reject"}
                          </button>
                        </div>
                      </>
                    )}
                  </li>
                );
              })}
            </ul>
          )}
        </section>
      )}

      {state.detailState === "loading" && <p role="status">Loading diagnostics…</p>}
      {state.detailState === "error" && (
        <div role="alert" className="inline-error">
          <p>{state.error}</p>
          <button onClick={() => loadDetails(state.selectedId)}>Retry diagnostics</button>
        </div>
      )}
      {streamError && (
        <p role="alert" className="inline-error">
          {streamError}
        </p>
      )}
      {state.details && (
        <div className="run-diagnostics">
          <p>
            <strong>{state.details.run.status}</strong> · {state.details.nodes.length} nodes · cursor{" "}
            {state.details.lastEventCursor}
          </p>
          <ol>
            {state.details.nodes.map((node) => (
              <li key={node.nodeId}>
                <strong>{node.nodeId}</strong> <span>{node.status}</span> <small>{node.attemptCount} attempt(s)</small>
                {node.attempts.some((attempt) => attempt.errorCode) && (
                  <p role="alert">{node.attempts.find((attempt) => attempt.errorCode)?.errorCode}</p>
                )}
                {node.outputMetadata?.sensitive && <p>Sensitive output hidden.</p>}
              </li>
            ))}
          </ol>
        </div>
      )}
      {streamState === "connecting" && <p role="status">Connecting…</p>}
      {streamState === "streaming" && events.length === 0 && <p role="status">Connected. Waiting for events…</p>}
      {events.length > 0 && (
        <ol className="timeline">
          {events.map((event) => (
            <li key={event.cursor} className={terminalEvents.has(event.type) ? "timeline__terminal" : ""}>
              <span>{event.cursor}</span>
              <strong>{event.type}</strong>
              <time>{new Date(event.at).toLocaleTimeString()}</time>
              {event.data !== undefined && <pre>{JSON.stringify(event.data, null, 2)}</pre>}
            </li>
          ))}
        </ol>
      )}
    </section>
  );
}

function errorMessage(reason: unknown, fallback: string): string {
  return reason instanceof Error ? reason.message : fallback;
}

function operationKey(prefix: string): string {
  return `${prefix}-${crypto.randomUUID()}`;
}

function successMessage(operation: PendingOperation): string {
  if (operation.kind === "cancel") return "Cancellation requested. Run status has been refreshed.";
  if (operation.kind === "rerun") return "Rerun created and selected.";
  return operation.input.decision === "approved"
    ? "Approval recorded. The run can resume."
    : "Rejection recorded. The run has been refreshed.";
}

function clearOperationFeedback(
  setState: (state: OperationState) => void,
  setPending: (operation: PendingOperation | undefined) => void,
  setError: (message: string | undefined) => void,
  setSuccess: (message: string | undefined) => void,
) {
  setState("idle");
  setPending(undefined);
  setError(undefined);
  setSuccess(undefined);
}
