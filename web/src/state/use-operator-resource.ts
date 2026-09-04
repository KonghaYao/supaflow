import { useCallback, useEffect, useRef, useState } from "react";

export type ResourceState<T> =
  | { status: "loading" }
  | { status: "error"; message: string }
  | { status: "ready"; value: T };

/** Loads one independent operator projection and exposes an explicit retry seam. */
export function useOperatorResource<T>(loader: (signal: AbortSignal) => Promise<T>) {
  const [state, setState] = useState<ResourceState<T>>({ status: "loading" });
  const activeRequest = useRef<AbortController | undefined>(undefined);
  const reload = useCallback(() => {
    activeRequest.current?.abort();
    const controller = new AbortController();
    activeRequest.current = controller;
    setState({ status: "loading" });
    void loader(controller.signal).then(
      (value) => {
        if (!controller.signal.aborted) setState({ status: "ready", value });
      },
      (error: unknown) => {
        if (!controller.signal.aborted)
          setState({ status: "error", message: error instanceof Error ? error.message : "Operation failed." });
      },
    );
  }, [loader]);
  useEffect(() => {
    reload();
    return () => activeRequest.current?.abort();
  }, [reload]);
  return { state, reload };
}

export function parseJSONObject(value: string): Record<string, unknown> {
  const parsed: unknown = JSON.parse(value);
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed))
    throw new Error("Value must be a JSON object.");
  return parsed as Record<string, unknown>;
}

export function mutationKey(prefix: string): string {
  return `${prefix}-${crypto.randomUUID()}`;
}

/** Produces a one-shot command and the state that must replace its secret form immediately. */
export function consumeSecretForm<T extends { secret: string }>(value: T): { command: T; cleared: T } {
  return { command: { ...value }, cleared: { ...value, secret: "" } };
}
