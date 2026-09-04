import { useEffect, useRef } from "react";

import { APIError, type StudioClient } from "../api/client";
import type { StudioAction, StudioState } from "./studio-reducer";

/** Saves the latest dirty revision with cancellation and explicit conflict recovery. */
export function useAutosave(
  client: StudioClient | undefined,
  state: StudioState,
  dispatch: (action: StudioAction) => void,
) {
  const generation = useRef(0);
  const { document, draft, save } = state;
  useEffect(() => {
    if (!client || save !== "dirty" || !document || !draft) return;
    const current = ++generation.current;
    const abort = new AbortController();
    const timer = window.setTimeout(async () => {
      dispatch({ type: "saving" });
      try {
        const key = `autosave-${draft.workflowId}-${draft.revision}-${current}`;
        const saved = await client.saveDraft(draft.workflowId, document, draft.revision, key, abort.signal);
        if (generation.current === current) dispatch({ type: "saved", draft: saved });
      } catch (error) {
        if (abort.signal.aborted || generation.current !== current) return;
        if (error instanceof APIError && error.status === 409) {
          try {
            const remote = await client.getDraft(draft.workflowId, abort.signal);
            dispatch({ type: "conflict", remote });
          } catch (readError) {
            dispatch({ type: "save-failed", error: message(readError) });
          }
          return;
        }
        dispatch({ type: "save-failed", error: message(error) });
      }
    }, 700);
    return () => {
      window.clearTimeout(timer);
      abort.abort();
    };
  }, [client, dispatch, document, draft, save]);
}

function message(error: unknown): string {
  return error instanceof Error ? error.message : "Autosave failed.";
}
