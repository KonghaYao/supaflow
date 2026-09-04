import { describe, expect, it } from "vitest";

import { createDefaultDocument } from "../domain/default-document";
import type { Draft } from "../domain/models";
import { initialStudioState, studioReducer } from "./studio-reducer";

function draft(revision: number, label = "Start"): Draft {
  const document = createDefaultDocument("flow");
  document.nodes[0] = { ...document.nodes[0], label };
  return {
    id: "draft",
    workflowId: "flow",
    document,
    revision,
    updatedBy: "actor",
    createdAt: "2026-01-01",
    updatedAt: "2026-01-01",
  };
}

describe("Studio reducer", () => {
  // 验证 undo/redo 恢复完整编辑文档且重新进入待保存状态。
  it("maintains bounded document history", () => {
    const loaded = studioReducer(initialStudioState, { type: "loaded", draft: draft(1) });
    const changed = draft(1, "Changed").document;
    const edited = studioReducer(loaded, { type: "edit", document: changed });
    const undone = studioReducer(edited, { type: "undo" });
    const redone = studioReducer(undone, { type: "redo" });
    expect(undone.document?.nodes[0].label).toBe("Start");
    expect(redone.document?.nodes[0].label).toBe("Changed");
    expect(redone.save).toBe("dirty");
  });

  // 验证 revision 冲突保留本地内容，并提供接受远端与强制重试两条显式路径。
  it("models autosave conflicts without silently overwriting", () => {
    const loaded = studioReducer(initialStudioState, { type: "loaded", draft: draft(2, "Local") });
    const conflicted = studioReducer(loaded, { type: "conflict", remote: draft(3, "Remote") });
    expect(conflicted.document?.nodes[0].label).toBe("Local");
    expect(studioReducer(conflicted, { type: "accept-remote" }).document?.nodes[0].label).toBe("Remote");
    const retry = studioReducer(conflicted, { type: "retry-local" });
    expect(retry.document?.nodes[0].label).toBe("Local");
    expect(retry.draft?.revision).toBe(3);
  });
});
