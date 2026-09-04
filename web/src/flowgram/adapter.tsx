import "reflect-metadata";
import "@flowgram.ai/free-layout-editor/index.css";

import {
  type FlowNodeType,
  FreeLayoutEditor,
  type FreeLayoutPluginContext,
  type WorkflowJSON,
  type WorkflowNodeProps,
  type WorkflowNodeRegistry,
  WorkflowNodeRenderer,
} from "@flowgram.ai/free-layout-editor";
import { forwardRef, useEffect, useImperativeHandle, useRef } from "react";

import type { NodeType, StudioDocument, StudioEdge, StudioNode } from "../domain/models";

export interface CanvasHandle {
  fitView(): void;
  autoLayout(): void;
  copySelected(): void;
  paste(): void;
  groupSelected(): void;
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

function toWorkflowJSON(document: StudioDocument): WorkflowJSON {
  return {
    nodes: document.nodes.map((node) => ({
      id: node.id,
      type: node.type,
      meta: { position: node.position },
      data: { definition: withoutPosition(node) } satisfies FlowNodeData,
    })),
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
  const nodes = source.nodes.flatMap((raw): StudioNode[] => {
    const data = readNodeData(raw.data);
    if (!data) return [];
    const position = raw.meta?.position;
    return [{ ...data.definition, position: { x: position?.x ?? 0, y: position?.y ?? 0 } }];
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
  return { ...previous, nodes, edges };
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

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function registryFor(type: FlowNodeType, nodeTypes: NodeType[]): WorkflowNodeRegistry {
  const descriptor = nodeTypes.find((entry) => entry.type === String(type));
  return {
    type,
    meta: {
      size: { width: 210, height: 76 },
      defaultPorts: descriptor ? portsForNodeType(descriptor) : [],
    },
  };
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
  const clipboard = useRef<StudioNode[]>([]);
  const canvasSignature = JSON.stringify(toWorkflowJSON(document));
  const lastCanvasSignature = useRef(canvasSignature);
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
        clipboard.current = document.nodes.filter((node) => ids.has(node.id));
      },
      paste: () => {
        if (clipboard.current.length === 0) return;
        const stamp = Date.now().toString(36);
        const copies = clipboard.current.map((node, index) => ({
          ...node,
          id: `${node.id}-copy-${stamp}-${index}`,
          label: `${node.label} copy`,
          position: { x: node.position.x + 40, y: node.position.y + 40 },
        }));
        onChange({ ...document, nodes: [...document.nodes, ...copies] });
      },
      groupSelected: () => {
        const ids = new Set(context.current?.selection.selection.map((entity) => entity.id) ?? []);
        if (ids.size === 0) return;
        const id = `group-${Date.now().toString(36)}`;
        onChange({
          ...document,
          nodes: document.nodes.map((node) => (ids.has(node.id) ? { ...node, groupId: id } : node)),
          groups: [...(document.groups ?? []), { id, title: "New group", nodeIds: [...ids] }],
        });
      },
    }),
    [document, onChange],
  );

  return (
    <FreeLayoutEditor
      ref={context}
      initialData={toWorkflowJSON(document)}
      readonly={readonly}
      getNodeDefaultRegistry={(type) => registryFor(type, nodeTypes)}
      nodeRegistries={nodeTypes.map((entry) => registryFor(entry.type as FlowNodeType, nodeTypes))}
      materials={{ renderDefaultNode: NodeCard }}
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
