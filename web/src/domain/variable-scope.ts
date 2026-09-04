import type { NodeType, StudioDocument, ValueType } from "./models";

export interface ScopedVariable {
  nodeId: string;
  port: string;
  type: ValueType;
  selector: string;
}

/** Returns typed outputs from control-flow ancestors visible to a target node. */
export function variablesInScope(
  document: StudioDocument,
  targetNodeId: string,
  nodeTypes: NodeType[],
): ScopedVariable[] {
  const ancestors = new Set<string>();
  const incoming = new Map<string, string[]>();
  for (const edge of document.edges)
    incoming.set(edge.targetNodeId, [...(incoming.get(edge.targetNodeId) ?? []), edge.sourceNodeId]);
  const pending = [...(incoming.get(targetNodeId) ?? [])];
  while (pending.length > 0) {
    const id = pending.shift();
    if (!id || ancestors.has(id)) continue;
    ancestors.add(id);
    pending.push(...(incoming.get(id) ?? []));
  }
  const registry = new Map(nodeTypes.map((entry) => [`${entry.type}@${entry.version}`, entry]));
  return document.nodes
    .filter((node) => ancestors.has(node.id))
    .flatMap((node) =>
      Object.entries(registry.get(`${node.type}@${node.version}`)?.outputs ?? {}).map(([port, output]) => ({
        nodeId: node.id,
        port,
        type: output.type,
        selector: `${node.id}.${port}`,
      })),
    )
    .sort((left, right) => left.selector.localeCompare(right.selector));
}
