import { describe, expect, it } from "vitest";

import { createDefaultDocument } from "../domain/default-document";
import { copySelection, groupSelection, pasteSelection, ungroupSelection } from "./document-operations";

describe("Studio document operations", () => {
  // 验证复制完整分组会复制内部连线，并为节点、边和分组建立全新的身份。
  it("copies a selected group with its internal edges and remaps every identity", () => {
    const document = createDefaultDocument("flow");
    document.nodes = document.nodes.map((node) => ({ ...node, groupId: "main" }));
    document.groups = [{ id: "main", title: "Main path", nodeIds: ["start", "end"] }];

    const clipboard = copySelection(document, new Set(["main"]));
    const pasted = pasteSelection(document, clipboard, "paste-1");

    expect(pasted.nodes.slice(2).map(({ id, groupId }) => ({ id, groupId }))).toEqual([
      { id: "start-copy-paste-1-0", groupId: "main-copy-paste-1-0" },
      { id: "end-copy-paste-1-1", groupId: "main-copy-paste-1-0" },
    ]);
    expect(pasted.edges.at(-1)).toEqual({
      id: "start-end-copy-paste-1-0",
      sourceNodeId: "start-copy-paste-1-0",
      sourcePortId: "input",
      targetNodeId: "end-copy-paste-1-1",
      targetPortId: "result",
    });
    expect(pasted.groups?.at(-1)).toEqual({
      id: "main-copy-paste-1-0",
      title: "Main path copy",
      nodeIds: ["start-copy-paste-1-0", "end-copy-paste-1-1"],
    });
  });

  // 验证只复制分组的一部分时不会保留指向未复制分组的悬空 groupId。
  it("drops group ownership when only part of a group is copied", () => {
    const document = createDefaultDocument("flow");
    document.nodes = document.nodes.map((node) => ({ ...node, groupId: "main" }));
    document.groups = [{ id: "main", title: "Main path", nodeIds: ["start", "end"] }];

    const pasted = pasteSelection(document, copySelection(document, new Set(["start"])), "partial");

    expect(pasted.nodes.at(-1)?.groupId).toBeUndefined();
    expect(pasted.groups).toHaveLength(1);
    expect(pasted.edges).toHaveLength(1);
  });

  // 验证新分组会接管所选节点，并从旧分组中移除这些成员以保持单一归属。
  it("groups selected nodes without leaving stale memberships", () => {
    const document = createDefaultDocument("flow");
    document.nodes[0] = { ...document.nodes[0], groupId: "old" };
    document.groups = [{ id: "old", title: "Old", nodeIds: ["start"] }];

    const grouped = groupSelection(document, new Set(["start", "end"]), "new", "New group");

    expect(grouped.nodes.every((node) => node.groupId === "new")).toBe(true);
    expect(grouped.groups).toEqual([{ id: "new", title: "New group", nodeIds: ["start", "end"] }]);
  });

  // 验证用户可以取消所选分组，且成员节点不会残留无效 groupId。
  it("ungroups selected visible groups", () => {
    const document = createDefaultDocument("flow");
    document.nodes = document.nodes.map((node) => ({ ...node, groupId: "main" }));
    document.groups = [{ id: "main", title: "Main path", nodeIds: ["start", "end"] }];

    const ungrouped = ungroupSelection(document, new Set(["main"]));

    expect(ungrouped.groups).toEqual([]);
    expect(ungrouped.nodes.every((node) => node.groupId === undefined)).toBe(true);
  });
});
