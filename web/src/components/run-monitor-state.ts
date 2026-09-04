import type { RunDetails, RunSummary } from "../domain/models";

export type LoadState = "loading" | "ready" | "empty" | "error";

export interface RunMonitorState {
  runs: RunSummary[];
  selectedId: string;
  details?: RunDetails;
  listState: LoadState;
  detailState: "idle" | "loading" | "ready" | "error";
  error?: string;
}

export type RunMonitorAction =
  | { type: "reset" }
  | { type: "listLoading" }
  | { type: "listLoaded"; runs: RunSummary[] }
  | { type: "listFailed"; error: string }
  | { type: "selected"; runId: string }
  | { type: "detailsLoading" }
  | { type: "detailsLoaded"; details: RunDetails }
  | { type: "detailsFailed"; error: string };

export const initialRunMonitorState: RunMonitorState = {
  runs: [],
  selectedId: "",
  listState: "loading",
  detailState: "idle",
};

const CANCELLABLE_RUN_STATUSES = new Set(["PENDING", "RUNNING", "SUSPENDED"]);
const RERUNNABLE_RUN_STATUSES = new Set(["SUCCEEDED", "SUCCESS", "FAILED", "ERROR", "CANCELLED"]);

/** Only active durable states accept a cancellation request. */
export function canCancelRun(status: string): boolean {
  return CANCELLABLE_RUN_STATUSES.has(status.toUpperCase());
}

/** A rerun is valid only after the source run has reached a terminal state. */
export function canRerunRun(status: string): boolean {
  return RERUNNABLE_RUN_STATUSES.has(status.toUpperCase());
}

/** Keep compatibility with historical persisted approval kind values. */
export function isApprovalInterrupt(kind: string): boolean {
  return kind === "approval" || kind === "human_approval";
}

/** Invalid or elapsed expiries fail closed so Studio never offers a stale resolution. */
export function isInterruptExpired(expiresAt: string, now = Date.now()): boolean {
  const expiry = Date.parse(expiresAt);
  return !Number.isFinite(expiry) || expiry <= now;
}

/** Pure state transitions keep retries and stale selection handling deterministic. */
export function reduceRunMonitor(state: RunMonitorState, action: RunMonitorAction): RunMonitorState {
  switch (action.type) {
    case "reset":
      return initialRunMonitorState;
    case "listLoading":
      return { ...state, listState: "loading", error: undefined };
    case "listLoaded": {
      const selectedId = action.runs.some((run) => run.id === state.selectedId)
        ? state.selectedId
        : (action.runs[0]?.id ?? "");
      return {
        ...state,
        runs: action.runs,
        selectedId,
        details: selectedId === state.selectedId ? state.details : undefined,
        listState: action.runs.length === 0 ? "empty" : "ready",
        detailState: selectedId ? state.detailState : "idle",
        error: undefined,
      };
    }
    case "listFailed":
      return { ...state, listState: "error", error: action.error };
    case "selected":
      return { ...state, selectedId: action.runId, details: undefined, detailState: "loading", error: undefined };
    case "detailsLoading":
      return { ...state, detailState: "loading", error: undefined };
    case "detailsLoaded":
      return { ...state, details: action.details, detailState: "ready", error: undefined };
    case "detailsFailed":
      return { ...state, details: undefined, detailState: "error", error: action.error };
  }
}
