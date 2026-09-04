import type { Draft, StudioDocument } from "../domain/models";

export type LoadState = "loading" | "ready" | "empty" | "error";
export type SaveState = "idle" | "dirty" | "saving" | "saved" | "conflict" | "error";

export interface StudioState {
  load: LoadState;
  save: SaveState;
  draft?: Draft;
  document?: StudioDocument;
  selectedNodeId?: string;
  remoteConflict?: Draft;
  past: StudioDocument[];
  future: StudioDocument[];
  error?: string;
}

export type StudioAction =
  | { type: "loading" }
  | { type: "loaded"; draft: Draft }
  | { type: "empty" }
  | { type: "load-failed"; error: string }
  | { type: "edit"; document: StudioDocument }
  | { type: "select"; nodeId?: string }
  | { type: "undo" }
  | { type: "redo" }
  | { type: "saving" }
  | { type: "saved"; draft: Draft }
  | { type: "save-failed"; error: string }
  | { type: "conflict"; remote: Draft }
  | { type: "accept-remote" }
  | { type: "retry-local" };

export const initialStudioState: StudioState = { load: "loading", save: "idle", past: [], future: [] };

/** Deterministic Studio state machine for history and optimistic autosave. */
export function studioReducer(state: StudioState, action: StudioAction): StudioState {
  switch (action.type) {
    case "loading":
      return { ...initialStudioState };
    case "loaded":
      return { ...initialStudioState, load: "ready", draft: action.draft, document: action.draft.document };
    case "empty":
      return { ...initialStudioState, load: "empty" };
    case "load-failed":
      return { ...initialStudioState, load: "error", error: action.error };
    case "edit":
      if (!state.document || documentsEqual(state.document, action.document)) return state;
      return {
        ...state,
        document: action.document,
        save: "dirty",
        past: [...state.past, state.document].slice(-100),
        future: [],
        error: undefined,
      };
    case "select":
      return { ...state, selectedNodeId: action.nodeId };
    case "undo": {
      const previous = state.past.at(-1);
      if (!previous || !state.document) return state;
      return {
        ...state,
        document: previous,
        save: "dirty",
        past: state.past.slice(0, -1),
        future: [state.document, ...state.future],
      };
    }
    case "redo": {
      const next = state.future.at(0);
      if (!next || !state.document) return state;
      return {
        ...state,
        document: next,
        save: "dirty",
        past: [...state.past, state.document],
        future: state.future.slice(1),
      };
    }
    case "saving":
      return { ...state, save: "saving", error: undefined };
    case "saved":
      return {
        ...state,
        save: "saved",
        draft: action.draft,
        document: action.draft.document,
        remoteConflict: undefined,
      };
    case "save-failed":
      return { ...state, save: "error", error: action.error };
    case "conflict":
      return {
        ...state,
        save: "conflict",
        remoteConflict: action.remote,
        error: "A newer draft revision is available.",
      };
    case "accept-remote":
      if (!state.remoteConflict) return state;
      return {
        ...initialStudioState,
        load: "ready",
        save: "idle",
        draft: state.remoteConflict,
        document: state.remoteConflict.document,
      };
    case "retry-local":
      if (!state.draft || !state.remoteConflict) return state;
      return {
        ...state,
        save: "dirty",
        draft: { ...state.draft, revision: state.remoteConflict.revision },
        remoteConflict: undefined,
        error: undefined,
      };
  }
}

function documentsEqual(left: StudioDocument, right: StudioDocument): boolean {
  return JSON.stringify(left) === JSON.stringify(right);
}
