import type { ReactNode } from "react";

import type { ResourceState } from "../state/use-operator-resource";

export type Feedback = { kind: "loading" | "success" | "error"; message: string } | undefined;

export function FeedbackMessage({ value }: { value: Feedback }) {
  if (!value) return null;
  return (
    <p
      className={`operator-feedback operator-feedback--${value.kind}`}
      role={value.kind === "error" ? "alert" : "status"}
      aria-live="polite"
    >
      {value.message}
    </p>
  );
}

export function ResourceBoundary<T>({
  state,
  retry,
  empty,
  children,
}: {
  state: ResourceState<T>;
  retry: () => void;
  empty?: (value: T) => boolean;
  children: (value: T) => ReactNode;
}) {
  if (state.status === "loading")
    return (
      <p className="muted" role="status" aria-busy="true">
        Loading…
      </p>
    );
  if (state.status === "error")
    return (
      <div className="inline-error" role="alert">
        <p>{state.message}</p>
        <button onClick={retry}>Retry</button>
      </div>
    );
  if (empty?.(state.value))
    return (
      <div className="operator-empty">
        <p>No records in this tenant.</p>
        <button onClick={retry}>Refresh</button>
      </div>
    );
  return children(state.value);
}

export function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : "Operation failed.";
}
