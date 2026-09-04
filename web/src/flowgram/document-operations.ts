import type { StudioDocument, StudioEdge, StudioNode } from "../domain/models";

/** Immutable Studio selection snapshot used by the toolbar clipboard. */
export interface StudioClipboard {
  nodes: StudioNode[];
  edges: StudioEdge[];
  groups: NonNullable<StudioDocument["groups"]>;
}

/** Captures selected nodes/groups and only the edges fully contained by that selection. */
export function copySelection(document: StudioDocument, selectedIds: ReadonlySet<string>): StudioClipboard {
  const groups = document.groups ?? [];
  const nodeIds = new Set(selectedIds);
  for (const group of groups) {
    if (selectedIds.has(group.id)) group.nodeIds.forEach((id) => nodeIds.add(id));
  }
  const copiedGroups = groups.filter(
    (group) =>
      group.nodeIds.length > 0 &&
      group.nodeIds.every(
        (id) => nodeIds.has(id) && document.nodes.some((node) => node.id === id && node.groupId === group.id),
      ),
  );
  const copiedGroupIds = new Set(copiedGroups.map((group) => group.id));
  const nodes = document.nodes
    .filter((node) => nodeIds.has(node.id))
    .map((node) => (node.groupId && copiedGroupIds.has(node.groupId) ? { ...node } : withoutGroup(node)));
  const copiedNodeIds = new Set(nodes.map((node) => node.id));
  return {
    nodes,
    edges: document.edges.filter(
      (edge) => copiedNodeIds.has(edge.sourceNodeId) && copiedNodeIds.has(edge.targetNodeId),
    ),
    groups: copiedGroups.map((group) => ({ ...group, nodeIds: [...group.nodeIds] })),
  };
}

/** Pastes a clipboard snapshot with collision-free node, edge, and group identities. */
export function pasteSelection(
  document: StudioDocument,
  clipboard: StudioClipboard,
  suffix: string,
): StudioDocument {
  if (clipboard.nodes.length === 0) return document;
  const nodeIds = new Map(clipboard.nodes.map((node, index) => [node.id, `${node.id}-copy-${suffix}-${index}`]));
  const groupIds = new Map(clipboard.groups.map((group, index) => [group.id, `${group.id}-copy-${suffix}-${index}`]));
  const nodes = clipboard.nodes.map((node) => {
    const groupId = node.groupId ? groupIds.get(node.groupId) : undefined;
    const copy: StudioNode = {
      ...node,
      id: requiredMappedID(nodeIds, node.id),
      label: `${node.label} copy`,
      position: { x: node.position.x + 40, y: node.position.y + 40 },
    };
    return groupId ? { ...copy, groupId } : withoutGroup(copy);
  });
  const edges = clipboard.edges.map((edge, index) => ({
    ...edge,
    id: `${edge.id}-copy-${suffix}-${index}`,
    sourceNodeId: requiredMappedID(nodeIds, edge.sourceNodeId),
    targetNodeId: requiredMappedID(nodeIds, edge.targetNodeId),
  }));
  const groups = clipboard.groups.map((group) => ({
    ...group,
    id: requiredMappedID(groupIds, group.id),
    title: `${group.title} copy`,
    nodeIds: group.nodeIds.map((id) => requiredMappedID(nodeIds, id)),
  }));
  return {
    ...document,
    nodes: [...document.nodes, ...nodes],
    edges: [...document.edges, ...edges],
    groups: [...(document.groups ?? []), ...groups],
  };
}

/** Creates one group and removes its selected members from any former group. */
export function groupSelection(
  document: StudioDocument,
  selectedIds: ReadonlySet<string>,
  groupId: string,
  title: string,
): StudioDocument {
  const nodeIds = document.nodes.filter((node) => selectedIds.has(node.id)).map((node) => node.id);
  if (nodeIds.length === 0) return document;
  const groupedIds = new Set(nodeIds);
  const groups = (document.groups ?? []).flatMap((group) => {
    const remaining = group.nodeIds.filter((id) => !groupedIds.has(id));
    return remaining.length > 0 ? [{ ...group, nodeIds: remaining }] : [];
  });
  return {
    ...document,
    nodes: document.nodes.map((node) => (groupedIds.has(node.id) ? { ...node, groupId } : node)),
    groups: [...groups, { id: groupId, title, nodeIds }],
  };
}

/** Removes selected groups, including groups inferred from their selected members. */
export function ungroupSelection(document: StudioDocument, selectedIds: ReadonlySet<string>): StudioDocument {
  const groupIds = new Set(selectedIds);
  for (const node of document.nodes) {
    if (selectedIds.has(node.id) && node.groupId) groupIds.add(node.groupId);
  }
  const existingGroupIds = new Set((document.groups ?? []).filter((group) => groupIds.has(group.id)).map((group) => group.id));
  if (existingGroupIds.size === 0) return document;
  return {
    ...document,
    nodes: document.nodes.map((node) =>
      node.groupId && existingGroupIds.has(node.groupId) ? withoutGroup(node) : node,
    ),
    groups: (document.groups ?? []).filter((group) => !existingGroupIds.has(group.id)),
  };
}

function withoutGroup(node: StudioNode): StudioNode {
  const { groupId: _groupId, ...ungrouped } = node;
  return ungrouped;
}

function requiredMappedID(ids: ReadonlyMap<string, string>, id: string): string {
  const mapped = ids.get(id);
  if (!mapped) throw new Error(`clipboard contains an invalid reference: ${id}`);
  return mapped;
}
