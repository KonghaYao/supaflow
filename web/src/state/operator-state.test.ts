import { describe, expect, it } from "vitest";

import { consumeSecretForm, parseJSONObject } from "./use-operator-resource";

describe("operator state boundary", () => {
  // 验证运维表单只接受对象配置，避免把数组或标量泄漏到 Trigger 领域模型。
  it("parses only JSON objects", () => {
    expect(parseJSONObject('{"region":"cn"}')).toEqual({ region: "cn" });
    expect(() => parseJSONObject("[]")).toThrow("JSON object");
  });

  // 验证 credential 提交命令生成时同步给出清空后的组件状态，避免成功或失败后回显。
  it("consumes a secret form into a cleared replacement state", () => {
    const result = consumeSecretForm({ id: "cred-1", secret: "one-shot" });
    expect(result.command.secret).toBe("one-shot");
    expect(result.cleared).toEqual({ id: "cred-1", secret: "" });
  });
});
