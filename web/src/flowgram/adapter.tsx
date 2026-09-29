import "reflect-metadata";
import "@flowgram.ai/free-layout-editor/index.css";

import {
  FlowNodeBaseType,
  type FlowNodeType,
  FreeLayoutEditor,
  type FreeLayoutPluginContext,
  type WorkflowJSON,
  type WorkflowNodeJSON,
  type WorkflowNodeProps,
  type WorkflowNodeRegistry,
  WorkflowNodeRenderer,
} from "@flowgram.ai/free-layout-editor";
import { forwardRef, useEffect, useImperativeHandle, useRef } from "react";

import type { NodeType, StudioDocument, StudioEdge, StudioNode } from "../domain/models";
import {
  type StudioClipboard,
  copySelection,
  groupSelection,
  pasteSelection,
  ungroupSelection,
} from "./document-operations";
import "./groups.css";

export interface CanvasHandle {
  fitView(): void;
  autoLayout(): void;
  copySelected(): void;
  paste(): void;
  groupSelected(): void;
  ungroupSelected(): void;
}

interface StudioCanvasProps {
  document: StudioDocument;
  nodeTypes: NodeType[];
  readonly?: boolean;
  onChange(document: StudioDocument): void;
  onSelect(nodeId?: string): void;
}

interface FlowNodeData {
  definition: Omit<StudioNode, "position">;
}

interface FlowEdgeData {
  id: string;
}

interface FlowGroupData {
  title: string;
  blockIDs: string[];
  width: number;
  height: number;
}

const NODE_WIDTH = 210;
const NODE_HEIGHT = 76;
const GROUP_PADDING = { top: 52, right: 28, bottom: 28, left: 28 };

function toWorkflowJSON(document: StudioDocument): WorkflowJSON {
  const nodeByID = new Map(document.nodes.map((node) => [node.id, node]));
  const groups = (document.groups ?? []).flatMap((group) => {
    const members = group.nodeIds.flatMap((id) => {
      const node = nodeByID.get(id);
      return node?.groupId === group.id ? [node] : [];
    });
    return members.length > 0 ? [{ group, members, bounds: groupBounds(members) }] : [];
  });
  const groupBoundsByID = new Map(groups.map((entry) => [entry.group.id, entry.bounds]));
  return {
    nodes: [
      ...document.nodes.map((node) => {
        const bounds = node.groupId ? groupBoundsByID.get(node.groupId) : undefined;
        const position = bounds
          ? { x: node.position.x - bounds.x, y: node.position.y - bounds.y }
          : node.position;
        return {
          id: node.id,
          type: node.type,
          meta: { position },
          data: { definition: withoutPosition(node) } satisfies FlowNodeData,
        };
      }),
      ...groups.map(
        ({ group, members, bounds }): WorkflowNodeJSON => ({
          id: group.id,
          type: FlowNodeBaseType.GROUP,
          meta: { position: { x: bounds.x, y: bounds.y } },
          data: {
            title: group.title,
            blockIDs: members.map((node) => node.id),
            width: bounds.width,
            height: bounds.height,
          } satisfies FlowGroupData,
        }),
      ),
    ],
    edges: document.edges.map((edge) => ({
      sourceNodeID: edge.sourceNodeId,
      sourcePortID: edge.sourcePortId,
      targetNodeID: edge.targetNodeId,
      targetPortID: edge.targetPortId,
      data: { id: edge.id } satisfies FlowEdgeData,
    })),
  };
}

function fromWorkflowJSON(source: WorkflowJSON, previous: StudioDocument): StudioDocument {
  const rawNodes = flattenNodes(source.nodes);
  const groupEntries = rawNodes.flatMap((raw) => {
    if (raw.type !== FlowNodeBaseType.GROUP) return [];
    const data = readGroupData(raw.data);
    const position = raw.meta?.position;
    return data
      ? [
          {
            group: { id: raw.id, title: data.title, nodeIds: data.blockIDs },
            position: { x: position?.x ?? 0, y: position?.y ?? 0 },
          },
        ]
      : [];
  });
  const groups = groupEntries.map((entry) => entry.group);
  const groupByNode = new Map(
    groupEntries.flatMap((entry) =>
      entry.group.nodeIds.map((id) => [id, { id: entry.group.id, position: entry.position }] as const),
    ),
  );
  const seen = new Set<string>();
  const nodes = rawNodes.flatMap((raw): StudioNode[] => {
    if (raw.type === FlowNodeBaseType.GROUP || seen.has(raw.id)) return [];
    const data = readNodeData(raw.data);
    if (!data) return [];
    seen.add(raw.id);
    const local = raw.meta?.position;
    const membership = groupByNode.get(raw.id);
    const position = membership
      ? { x: (local?.x ?? 0) + membership.position.x, y: (local?.y ?? 0) + membership.position.y }
      : { x: local?.x ?? 0, y: local?.y ?? 0 };
    const node = { ...data.definition, position };
    return [membership ? { ...node, groupId: membership.id } : withoutGroup(node)];
  });
  const edges = source.edges.map(
    (raw, index): StudioEdge => ({
      id: readEdgeID(raw.data) ?? `edge-${index}`,
      sourceNodeId: raw.sourceNodeID,
      sourcePortId: String(raw.sourcePortID ?? "output"),
      targetNodeId: raw.targetNodeID,
      targetPortId: String(raw.targetPortID ?? "input"),
    }),
  );
  return { ...previous, nodes, edges, groups };
}

function withoutPosition(node: StudioNode): Omit<StudioNode, "position"> {
  const { position: _position, ...definition } = node;
  return definition;
}

function readNodeData(value: unknown): FlowNodeData | undefined {
  if (!isRecord(value) || !isRecord(value.definition)) return;
  const definition = value.definition;
  if (
    typeof definition.id !== "string" ||
    typeof definition.type !== "string" ||
    typeof definition.version !== "string"
  )
    return;
  return value as unknown as FlowNodeData;
}

function readEdgeID(value: unknown): string | undefined {
  return isRecord(value) && typeof value.id === "string" ? value.id : undefined;
}

function readGroupData(value: unknown): FlowGroupData | undefined {
  if (!isRecord(value) || typeof value.title !== "string" || !Array.isArray(value.blockIDs)) return;
  const blockIDs = value.blockIDs.filter((id): id is string => typeof id === "string");
  if (blockIDs.length !== value.blockIDs.length) return;
  return {
    title: value.title,
    blockIDs,
    width: typeof value.width === "number" ? value.width : NODE_WIDTH + GROUP_PADDING.left + GROUP_PADDING.right,
    height: typeof value.height === "number" ? value.height : NODE_HEIGHT + GROUP_PADDING.top + GROUP_PADDING.bottom,
  };
}

function flattenNodes(nodes: WorkflowNodeJSON[]): WorkflowNodeJSON[] {
  return nodes.flatMap((node) => [node, ...flattenNodes(node.blocks ?? [])]);
}

function withoutGroup(node: StudioNode): StudioNode {
  const { groupId: _groupId, ...ungrouped } = node;
  return ungrouped;
}

function groupBounds(nodes: StudioNode[]) {
  const left = Math.min(...nodes.map((node) => node.position.x));
  const top = Math.min(...nodes.map((node) => node.position.y));
  const right = Math.max(...nodes.map((node) => node.position.x + NODE_WIDTH));
  const bottom = Math.max(...nodes.map((node) => node.position.y + NODE_HEIGHT));
  return {
    x: left - GROUP_PADDING.left,
    y: top - GROUP_PADDING.top,
    width: right - left + GROUP_PADDING.left + GROUP_PADDING.right,
    height: bottom - top + GROUP_PADDING.top + GROUP_PADDING.bottom,
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function registryFor(type: FlowNodeType, nodeTypes: NodeType[]): WorkflowNodeRegistry {
  if (type === FlowNodeBaseType.GROUP) {
    return {
      type,
      meta: {
        isContainer: true,
        padding: GROUP_PADDING,
        size: { width: NODE_WIDTH + GROUP_PADDING.left + GROUP_PADDING.right, height: 156 },
        defaultPorts: [],
      },
    };
  }
  const descriptor = nodeTypes.find((entry) => entry.type === String(type));
  return {
    type,
    meta: {
      size: { width: 210, height: 76 },
      defaultPorts: descriptor ? portsForNodeType(descriptor) : [],
    },
  };
}

function GroupCard({ node }: WorkflowNodeProps) {
  const data = readGroupData(node.toJSON().data);
  return (
    <WorkflowNodeRenderer
      node={node}
      className="flow-group"
      style={{ width: data?.width, height: data?.height }}
    >
      <strong className="flow-group__title">{data?.title ?? "Group"}</strong>
    </WorkflowNodeRenderer>
  );
}

/** Maps the backend's typed port contract to FlowGram's directional port model. */
export function portsForNodeType(nodeType: NodeType) {
  return [
    ...Object.keys(nodeType.inputs).map((portID) => ({ type: "input" as const, portID })),
    ...Object.keys(nodeType.outputs).map((portID) => ({ type: "output" as const, portID })),
  ];
}

function NodeCard({ node }: WorkflowNodeProps) {
  const source = node.toJSON();
  const data = readNodeData(source.data);
  const label = data?.definition.label ?? String(source.type);
  return (
    <WorkflowNodeRenderer node={node} className="flow-node">
      <span className="flow-node__type">{String(source.type)}</span>
      <strong>{label}</strong>
    </WorkflowNodeRenderer>
  );
}

/** Official FlowGram free-layout integration; vendor types never escape this adapter. */
export const StudioCanvas = forwardRef<CanvasHandle, StudioCanvasProps>(function StudioCanvas(
  { document, nodeTypes, readonly = false, onChange, onSelect },
  ref,
) {
  const context = useRef<FreeLayoutPluginContext | null>(null);
  const clipboard = useRef<StudioClipboard>({ nodes: [], edges: [], groups: [] });
  const identity = useRef(0);
  const canvasSignature = JSON.stringify(toWorkflowJSON(document));
  const lastCanvasSignature = useRef(canvasSignature);
  // The imperative handle keeps a stable identity while its actions read the
  // latest props. Studio binds this canvas with a callback ref (a state setter),
  // so a handle recreated on every edit re-invokes that setter from a layout
  // effect until React aborts with "Maximum update depth exceeded".
  const latest = useRef({ document, onChange });
  useEffect(() => {
    latest.current = { document, onChange };
  });
  useEffect(() => {
    if (!context.current || canvasSignature === lastCanvasSignature.current) return;
    lastCanvasSignature.current = canvasSignature;
    context.current.operation.fromJSON(toWorkflowJSON(document));
  }, [canvasSignature, document]);
  useImperativeHandle(
    ref,
    () => ({
      fitView: () => void context.current?.tools.fitView(true),
      autoLayout: () => void context.current?.tools.autoLayout({}),
      copySelected: () => {
        const ids = new Set(context.current?.selection.selection.map((entity) => entity.id) ?? []);
        clipboard.current = copySelection(latest.current.document, ids);
      },
      paste: () => {
        if (clipboard.current.nodes.length === 0) return;
        identity.current += 1;
        const suffix = `${Date.now().toString(36)}-${identity.current}`;
        latest.current.onChange(pasteSelection(latest.current.document, clipboard.current, suffix));
      },
      groupSelected: () => {
        const ids = new Set(context.current?.selection.selection.map((entity) => entity.id) ?? []);
        identity.current += 1;
        const id = `group-${Date.now().toString(36)}-${identity.current}`;
        latest.current.onChange(groupSelection(latest.current.document, ids, id, "New group"));
      },
      ungroupSelected: () => {
        const ids = new Set(context.current?.selection.selection.map((entity) => entity.id) ?? []);
        latest.current.onChange(ungroupSelection(latest.current.document, ids));
      },
    }),
    [],
  );

  return (
    <FreeLayoutEditor
      ref={context}
      initialData={toWorkflowJSON(document)}
      readonly={readonly}
      getNodeDefaultRegistry={(type) => registryFor(type, nodeTypes)}
      nodeRegistries={[
        registryFor(FlowNodeBaseType.GROUP, nodeTypes),
        ...nodeTypes.map((entry) => registryFor(entry.type as FlowNodeType, nodeTypes)),
      ]}
      materials={{ renderDefaultNode: NodeCard, renderNodes: { [FlowNodeBaseType.GROUP]: GroupCard } }}
      history={{ enable: true }}
      variableEngine={{ enable: true }}
      onContentChange={(ctx) => {
        const source = ctx.document.toJSON();
        lastCanvasSignature.current = JSON.stringify(source);
        onChange(fromWorkflowJSON(source, document));
      }}
      onAllLayersRendered={(ctx) => {
        context.current = ctx;
        void ctx.tools.fitView(false);
        ctx.selection.onSelectionChanged(() => {
          const selected = ctx.selection.selection.at(0);
          onSelect(selected?.id);
        });
      }}
    />
  );
});

export const flowGramContract = { toWorkflowJSON, fromWorkflowJSON };
