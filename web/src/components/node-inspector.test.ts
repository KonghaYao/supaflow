import { describe, expect, it } from "vitest";

import { configFields, parseLiteralValue } from "./node-inspector";

describe("schema driven node inspector", () => {
  // 验证后端 JSON Schema 会生成带必填、枚举与类型的表单字段。
  it("extracts typed configuration controls", () => {
    expect(
      configFields({
        type: "object",
        required: ["method"],
        properties: {
          method: { type: "string", enum: ["GET", "POST"] },
          timeoutMs: { type: "integer", description: "Bounded timeout" },
        },
      }),
    ).toEqual([
      { name: "method", title: "method", type: "string", required: true, choices: ["GET", "POST"] },
      { name: "timeoutMs", title: "timeoutMs", description: "Bounded timeout", type: "integer", required: false },
    ]);
  });

  // 验证 typed literal 在写入文档前执行真实类型解析和对象边界校验。
  it("parses and rejects typed literals", () => {
    expect(parseLiteralValue("42", "integer")).toBe(42);
    expect(parseLiteralValue('{"ok":true}', "object")).toEqual({ ok: true });
    expect(() => parseLiteralValue("1.5", "integer")).toThrow("valid integer");
    expect(() => parseLiteralValue("[]", "object")).toThrow("JSON object");
  });
});
