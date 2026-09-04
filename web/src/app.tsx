import { lazy, Suspense, useCallback, useReducer, useState } from "react";

import { StudioClient } from "./api/client";
import { EditorToolbar } from "./components/editor-toolbar";
import { NodeInspector } from "./components/node-inspector";
import { NodePalette } from "./components/node-palette";
import { OperatorConsole } from "./components/operator-console";
import { RunMonitor } from "./components/run-monitor";
import { TestPanel } from "./components/test-panel";
import { VersionPanel } from "./components/version-panel";
import { createDefaultDocument } from "./domain/default-document";
import type { Draft, NodeType, StudioNode } from "./domain/models";
import type { CanvasHandle } from "./flowgram/adapter";
import { initialStudioState, studioReducer } from "./state/studio-reducer";
import { useAutosave } from "./state/use-autosave";

type UtilityTab = "test" | "versions" | "monitor";
type WorkspaceTab = "designer" | "operator";

const StudioCanvas = lazy(async () => ({ default: (await import("./flowgram/adapter")).StudioCanvas }));

export function App() {
  const [apiKey, setAPIKey] = useState("");
  const [client, setClient] = useState<StudioClient>();
  const [drafts, setDrafts] = useState<Draft[]>([]);
  const [nodeTypes, setNodeTypes] = useState<NodeType[]>([]);
  const [state, dispatch] = useReducer(studioReducer, initialStudioState);
  const [canvas, setCanvas] = useState<CanvasHandle | null>(null);
  const [utilityTab, setUtilityTab] = useState<UtilityTab>("test");
  const [workspaceTab, setWorkspaceTab] = useState<WorkspaceTab>("designer");
  const [action, setAction] = useState<{ state: "idle" | "loading" | "success" | "error"; message?: string }>({
    state: "idle",
  });
  const [versionRefresh, setVersionRefresh] = useState(0);
  useAutosave(client, state, dispatch);

  const loadWorkspace = useCallback(async (nextClient: StudioClient) => {
    dispatch({ type: "loading" });
    try {
      const [loadedDrafts, loadedTypes] = await Promise.all([nextClient.listDrafts(), nextClient.nodeTypes()]);
      setDrafts(loadedDrafts);
      setNodeTypes(loadedTypes);
      if (loadedDrafts.length === 0) dispatch({ type: "empty" });
      else dispatch({ type: "loaded", draft: loadedDrafts[0] });
    } catch (error) {
      dispatch({ type: "load-failed", error: error instanceof Error ? error.message : "Could not load Studio." });
    }
  }, []);

  const connect = () => {
    if (apiKey.trim().length < 24) return;
    const nextClient = new StudioClient(apiKey.trim());
    setClient(nextClient);
    void loadWorkspace(nextClient);
  };

  const createWorkflow = async () => {
    if (!client) return;
    const workflowId = `workflow-${Date.now().toString(36)}`;
    setAction({ state: "loading", message: "Creating workflow…" });
    try {
      const draft = await client.createDraft(
        workflowId,
        createDefaultDocument(workflowId),
        `create-${crypto.randomUUID()}`,
      );
      setDrafts((current) => [...current, draft]);
      dispatch({ type: "loaded", draft });
      setAction({ state: "success", message: "Workflow created." });
    } catch (error) {
      setAction({ state: "error", message: error instanceof Error ? error.message : "Creation failed." });
    }
  };

  const selectDraft = async (workflowId: string) => {
    if (!client) return;
    dispatch({ type: "loading" });
    try {
      dispatch({ type: "loaded", draft: await client.getDraft(workflowId) });
    } catch (error) {
      dispatch({ type: "load-failed", error: error instanceof Error ? error.message : "Could not load workflow." });
    }
  };

  const runAction = async (kind: "compile" | "publish") => {
    if (!client || !state.draft) return;
    setAction({ state: "loading", message: kind === "compile" ? "Validating…" : "Publishing…" });
    try {
      const result =
        kind === "compile"
          ? await client.compile(state.draft.workflowId)
          : await client.publishDraft(state.draft.workflowId, `publish-${crypto.randomUUID()}`);
      const hash = typeof result.hash === "string" ? ` ${result.hash.slice(0, 10)}` : "";
      setAction({ state: "success", message: `${kind === "compile" ? "Valid" : "Published"}.${hash}` });
      if (kind === "publish") setVersionRefresh((value) => value + 1);
    } catch (error) {
      setAction({ state: "error", message: error instanceof Error ? error.message : `${kind} failed.` });
    }
  };

  const editNode = (replacement: StudioNode) => {
    if (!state.document) return;
    dispatch({
      type: "edit",
      document: {
        ...state.document,
        nodes: state.document.nodes.map((node) => (node.id === replacement.id ? replacement : node)),
      },
    });
  };

  const addNode = (node: StudioNode) => {
    if (!state.document) return;
    dispatch({ type: "edit", document: { ...state.document, nodes: [...state.document.nodes, node] } });
    dispatch({ type: "select", nodeId: node.id });
  };

  if (!client) {
    return (
      <main className="login-shell">
        <section className="login-card">
          <span className="brand-mark">S</span>
          <p className="eyebrow">Supaflow control plane</p>
          <h1>Design durable work.</h1>
          <p>Connect with a tenant API key. Credentials stay in memory and are never persisted by Studio.</p>
          <label>
            API key
            <input
              type="password"
              autoComplete="off"
              value={apiKey}
              onChange={(event) => setAPIKey(event.target.value)}
              onKeyDown={(event) => {
                if (event.key === "Enter") connect();
              }}
            />
          </label>
          <button className="button-primary" disabled={apiKey.trim().length < 24} onClick={connect}>
            Open Studio
          </button>
        </section>
      </main>
    );
  }

  return (
    <main className="studio-shell">
      <header className="topbar">
        <div className="brand">
          <span className="brand-mark">S</span>
          <span>
            Supaflow <strong>Studio</strong>
          </span>
        </div>
        <nav aria-label="Workspace">
          <div className="workspace-tabs" role="tablist" aria-label="Studio workspace">
            <button role="tab" aria-selected={workspaceTab === "designer"} onClick={() => setWorkspaceTab("designer")}>
              Designer
            </button>
            <button role="tab" aria-selected={workspaceTab === "operator"} onClick={() => setWorkspaceTab("operator")}>
              Operator
            </button>
          </div>
          {workspaceTab === "designer" && (
            <>
              <select
                aria-label="Current workflow"
                value={state.draft?.workflowId ?? ""}
                onChange={(event) => void selectDraft(event.target.value)}
              >
                <option value="" disabled>
                  Select workflow
                </option>
                {drafts.map((draft) => (
                  <option key={draft.id} value={draft.workflowId}>
                    {draft.workflowId}
                  </option>
                ))}
              </select>
              <button onClick={() => void createWorkflow()}>＋ New</button>
            </>
          )}
        </nav>
        <button
          className="quiet"
          onClick={() => {
            setClient(undefined);
            setAPIKey("");
          }}
        >
          Disconnect
        </button>
      </header>
      {action.state !== "idle" && (
        <div className={`toast toast--${action.state}`} role={action.state === "error" ? "alert" : "status"}>
          {action.message}
          <button aria-label="Dismiss message" onClick={() => setAction({ state: "idle" })}>
            ×
          </button>
        </div>
      )}
      {workspaceTab === "operator" && <OperatorConsole client={client} />}
      {workspaceTab === "designer" && (
        <>
          {state.load === "loading" && (
            <section className="center-state" aria-busy="true">
              <div className="spinner" />
              <h1>Loading Studio</h1>
              <p>Resolving drafts and node registry…</p>
            </section>
          )}
          {state.load === "error" && (
            <section className="center-state" role="alert">
              <h1>Studio could not load</h1>
              <p>{state.error}</p>
              <button onClick={() => void loadWorkspace(client)}>Retry</button>
            </section>
          )}
          {state.load === "empty" && (
            <section className="center-state">
              <span className="empty-graphic">◇</span>
              <h1>Your canvas is clear</h1>
              <p>Create a workflow with a valid Start → End path.</p>
              <button className="button-primary" onClick={() => void createWorkflow()}>
                Create first workflow
              </button>
            </section>
          )}
          {state.load === "ready" && state.document && state.draft && (
            <>
              <EditorToolbar
                canvas={canvas}
                canUndo={state.past.length > 0}
                canRedo={state.future.length > 0}
                saveState={state.save}
                onUndo={() => dispatch({ type: "undo" })}
                onRedo={() => dispatch({ type: "redo" })}
                onCompile={() => void runAction("compile")}
                onPublish={() => void runAction("publish")}
              />
              {state.save === "conflict" && (
                <section className="conflict-banner" role="alert">
                  <div>
                    <strong>Autosave conflict</strong>
                    <p>Your changes are intact. A newer server revision was detected.</p>
                  </div>
                  <button onClick={() => dispatch({ type: "accept-remote" })}>Use server copy</button>
                  <button className="button-primary" onClick={() => dispatch({ type: "retry-local" })}>
                    Save my copy over latest
                  </button>
                </section>
              )}
              {state.save === "error" && (
                <section className="conflict-banner" role="alert">
                  <div>
                    <strong>Autosave failed</strong>
                    <p>{state.error}</p>
                  </div>
                  <button onClick={() => dispatch({ type: "retry-local" })}>Retry</button>
                </section>
              )}
              <div className="editor-grid">
                <NodePalette nodeTypes={nodeTypes} onAdd={addNode} />
                <section className="canvas" aria-label="Free layout workflow canvas">
                  <Suspense
                    fallback={
                      <div className="canvas-loading" role="status">
                        Loading FlowGram canvas…
                      </div>
                    }
                  >
                    <StudioCanvas
                      ref={setCanvas}
                      document={state.document}
                      nodeTypes={nodeTypes}
                      onChange={(document) => dispatch({ type: "edit", document })}
                      onSelect={(nodeId) => dispatch({ type: "select", nodeId })}
                    />
                  </Suspense>
                </section>
                <NodeInspector
                  key={state.selectedNodeId}
                  document={state.document}
                  node={state.document.nodes.find((node) => node.id === state.selectedNodeId)}
                  nodeTypes={nodeTypes}
                  onChange={editNode}
                />
              </div>
              <div className="utility-tabs" role="tablist" aria-label="Workflow tools">
                {(["test", "versions", "monitor"] as const).map((tab) => (
                  <button role="tab" aria-selected={utilityTab === tab} key={tab} onClick={() => setUtilityTab(tab)}>
                    {tab}
                  </button>
                ))}
              </div>
              {utilityTab === "test" && (
                <TestPanel client={client} workflowId={state.draft.workflowId} selectedNodeId={state.selectedNodeId} />
              )}
              {utilityTab === "versions" && (
                <VersionPanel client={client} workflowId={state.draft.workflowId} refreshToken={versionRefresh} />
              )}
              {utilityTab === "monitor" && <RunMonitor client={client} workflowId={state.draft.workflowId} />}
            </>
          )}
        </>
      )}
    </main>
  );
}
