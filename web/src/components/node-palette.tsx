import type { NodeType, StudioNode } from "../domain/models";

interface Props {
  nodeTypes: NodeType[];
  onAdd(node: StudioNode): void;
}

/** Backend-driven node palette; every item maps to a registered executable type. */
export function NodePalette({ nodeTypes, onAdd }: Props) {
  return (
    <aside className="palette" aria-label="Node palette">
      <div>
        <span className="eyebrow">Registry</span>
        <h2>Node types</h2>
      </div>
      {nodeTypes.length === 0 ? (
        <p className="muted">No node types are available.</p>
      ) : (
        nodeTypes.map((entry) => (
          <button
            className="palette-item"
            key={`${entry.type}@${entry.version}`}
            onClick={() =>
              onAdd({
                id: `${entry.type}-${crypto.randomUUID().slice(0, 8)}`,
                type: entry.type,
                version: entry.version,
                label: entry.type.replaceAll("-", " "),
                config: {},
                inputs: {},
                position: { x: 280, y: 240 },
              })
            }
          >
            <span className="palette-icon">{entry.type.slice(0, 1).toUpperCase()}</span>
            <span>
              <strong>{entry.type}</strong>
              <small>{entry.version}</small>
            </span>
          </button>
        ))
      )}
    </aside>
  );
}
