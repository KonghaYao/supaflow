import { describe, expect, it } from "vitest";

import { createDefaultDocument } from "../domain/default-document";
import { flowGramContract, portsForNodeType } from "./adapter";

describe("FlowGram adapter contract", () => {
  // 验证供应商 JSON 往返不会丢失可执行节点和自由布局坐标。
  it("round-trips the Studio document through official WorkflowJSON", () => {
    const document = createDefaultDocument("flow");
    const flowgram = flowGramContract.toWorkflowJSON(document);
    const restored = flowGramContract.fromWorkflowJSON(flowgram, document);
    expect(restored.nodes).toEqual(document.nodes);
    expect(restored.edges).toEqual(document.edges);
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
