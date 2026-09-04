import type { CanvasHandle } from "../flowgram/adapter";

interface Props {
  canvas?: CanvasHandle | null;
  canUndo: boolean;
  canRedo: boolean;
  saveState: string;
  onUndo(): void;
  onRedo(): void;
  onCompile(): void;
  onPublish(): void;
}

/** Keyboard-accessible actions shared by canvas and Studio history. */
export function EditorToolbar({ canvas, canUndo, canRedo, saveState, onUndo, onRedo, onCompile, onPublish }: Props) {
  return (
    <div className="toolbar" role="toolbar" aria-label="Workflow editing actions">
      <button onClick={onUndo} disabled={!canUndo} aria-label="Undo">
        ↶ Undo
      </button>
      <button onClick={onRedo} disabled={!canRedo} aria-label="Redo">
        ↷ Redo
      </button>
      <button onClick={() => canvas?.copySelected()}>Copy</button>
      <button onClick={() => canvas?.paste()}>Paste</button>
      <button onClick={() => canvas?.groupSelected()}>Group</button>
      <button onClick={() => canvas?.autoLayout()}>Auto layout</button>
      <button onClick={() => canvas?.fitView()}>Fit</button>
      <span className={`save-state save-state--${saveState}`} role="status" aria-live="polite">
        {saveState}
      </span>
      <button className="button-secondary" onClick={onCompile}>
        Validate
      </button>
      <button className="button-primary" onClick={onPublish}>
        Publish
      </button>
    </div>
  );
}
