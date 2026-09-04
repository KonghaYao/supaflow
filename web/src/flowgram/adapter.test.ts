import { buildGroupJSON } from "@flowgram.ai/free-layout-editor";
import { describe, expect, it } from "vitest";

import { createDefaultDocument } from "../domain/default-document";
import { flowGramContract, portsForNodeType } from "./adapter";

describe("FlowGram adapter contract", () => {
  // 验证供应商 JSON 往返不会丢失可执行节点和自由布局坐标。
  it("round-trips the Studio document through official WorkflowJSON", () => {
    const document = createDefaultDocument("flow");
    const flowgram = flowGramContract.toWorkflowJSON(document);
    const restored = flowGramContract.fromWorkflowJSON(buildGroupJSON(flowgram), document);
    expect(restored.nodes).toEqual(document.nodes);
    expect(restored.edges).toEqual(document.edges);
  });

  // 验证领域分组会成为 FlowGram 原生 group 节点，并在画布往返后保持成员关系。
  it("round-trips visible FlowGram groups", () => {
    const document = createDefaultDocument("flow");
    document.nodes = document.nodes.map((node) => ({ ...node, groupId: "main" }));
    document.groups = [{ id: "main", title: "Main path", nodeIds: ["start", "end"] }];

    const flowgram = flowGramContract.toWorkflowJSON(document);
    expect(flowgram.nodes).toContainEqual(
      expect.objectContaining({
        id: "main",
        type: "group",
        data: expect.objectContaining({ title: "Main path", blockIDs: ["start", "end"] }),
      }),
    );
    expect(flowgram.nodes.find((node) => node.id === "start")?.meta?.position).toEqual({ x: 28, y: 52 });

    const restored = flowGramContract.fromWorkflowJSON(flowgram, document);
    expect(restored.groups).toEqual(document.groups);
    expect(restored.nodes).toEqual(document.nodes);
  });

  // 验证画布端口严格来自后端 registry，而不是伪造通用 input/output。
  it("uses the backend typed port contract", () => {
    expect(
      portsForNodeType({
        type: "human-approval",
        version: "1.0.0",
        inputs: { request: { type: "any", required: true } },
        outputs: { decision: { type: "object", required: true } },
        configSchema: {},
        sensitiveFields: [],
        capabilities: {
          streaming: false,
          cancellable: false,
          interruptible: true,
          idempotent: true,
          recoverySafe: true,
        },
        migrations: [],
      }),
    ).toEqual([
      { type: "input", portID: "request" },
      { type: "output", portID: "decision" },
    ]);
  });
});
