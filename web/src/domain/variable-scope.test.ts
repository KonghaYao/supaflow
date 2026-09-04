import { describe, expect, it } from "vitest";

import { createDefaultDocument } from "./default-document";
import type { NodeType } from "./models";
import { variablesInScope } from "./variable-scope";

describe("typed variable scope", () => {
  // 验证选择器只展示控制流祖先的真实 typed outputs。
  it("excludes downstream node outputs", () => {
    const document = createDefaultDocument("flow");
    const nodeTypes: NodeType[] = [
      {
        type: "start",
        version: "1.0.0",
        inputs: {},
        outputs: { input: { type: "object", required: true } },
        configSchema: {},
        sensitiveFields: [],
        capabilities: {
          streaming: false,
          cancellable: false,
          interruptible: false,
          idempotent: true,
          recoverySafe: true,
        },
        migrations: [],
      },
      {
        type: "end",
        version: "1.0.0",
        inputs: {},
        outputs: { result: { type: "object", required: true } },
        configSchema: {},
        sensitiveFields: [],
        capabilities: {
          streaming: false,
          cancellable: false,
          interruptible: false,
          idempotent: true,
          recoverySafe: true,
        },
        migrations: [],
      },
    ];
    expect(variablesInScope(document, "end", nodeTypes)).toEqual([
      { nodeId: "start", port: "input", selector: "start.input", type: "object" },
    ]);
    expect(variablesInScope(document, "start", nodeTypes)).toEqual([]);
  });
});
