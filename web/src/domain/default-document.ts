import type { StudioDocument } from "./models";

/** Creates the smallest editable, compilable free-layout workflow. */
export function createDefaultDocument(workflowId: string): StudioDocument {
  return {
    schemaVersion: "flowgram-v1",
    workflowId,
    nodes: [
      {
        id: "start",
        type: "start",
        version: "1.0.0",
        label: "Start",
        config: {},
        inputs: {},
        position: { x: 80, y: 180 },
      },
      { id: "end", type: "end", version: "1.0.0", label: "End", config: {}, inputs: {}, position: { x: 520, y: 180 } },
    ],
    edges: [
      { id: "start-end", sourceNodeId: "start", sourcePortId: "input", targetNodeId: "end", targetPortId: "result" },
    ],
    groups: [],
    viewport: { x: 0, y: 0, zoom: 1 },
  };
}
