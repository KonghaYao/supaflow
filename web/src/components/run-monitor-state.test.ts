import { describe, expect, it } from "vitest";

import type { RunSummary } from "../domain/models";
import {
  canCancelRun,
  canRerunRun,
  initialRunMonitorState,
  isApprovalInterrupt,
  isInterruptExpired,
  reduceRunMonitor,
} from "./run-monitor-state";

const run = (id: string): RunSummary => ({
  id,
  workflowId: "flow",
  planId: "plan",
  planHash: "hash",
  status: "PENDING",
  priority: 0,
  createdAt: "2026-09-03T00:00:00Z",
  updatedAt: "2026-09-03T00:00:00Z",
});

describe("run monitor state", () => {
  // 验证空列表和错误均可再次进入 loading，从而驱动可访问的重试流程。
  it("covers loading, empty, error, and retry transitions", () => {
    const empty = reduceRunMonitor(initialRunMonitorState, { type: "listLoaded", runs: [] });
    expect(empty.listState).toBe("empty");
    const failed = reduceRunMonitor(empty, { type: "listFailed", error: "offline" });
    expect(failed).toMatchObject({ listState: "error", error: "offline" });
    expect(reduceRunMonitor(failed, { type: "listLoading" })).toMatchObject({ listState: "loading", error: undefined });
  });

  // 验证列表会自动选择最近运行，用户切换时清除旧详情避免串 run 展示。
  it("auto-selects a run and clears stale details on selection", () => {
    const loaded = reduceRunMonitor(initialRunMonitorState, { type: "listLoaded", runs: [run("run-2"), run("run-1")] });
    expect(loaded.selectedId).toBe("run-2");
    const selected = reduceRunMonitor(
      { ...loaded, details: { run: run("run-2"), input: {}, nodes: [], lastEventCursor: 0 } },
      { type: "selected", runId: "run-1" },
    );
    expect(selected).toMatchObject({ selectedId: "run-1", details: undefined, detailState: "loading" });
  });

  // 验证取消与重跑操作只会在后端允许的运行生命周期中开放。
  it("gates cancel and rerun controls by durable run status", () => {
    expect(["PENDING", "RUNNING", "SUSPENDED"].every(canCancelRun)).toBe(true);
    expect(["CANCELLING", "SUCCEEDED", "FAILED", "CANCELLED"].some(canCancelRun)).toBe(false);
    expect(["SUCCEEDED", "FAILED", "ERROR", "CANCELLED"].every(canRerunRun)).toBe(true);
    expect(["PENDING", "RUNNING", "SUSPENDED", "CANCELLING"].some(canRerunRun)).toBe(false);
  });

  // 验证人工审批兼容新旧中断类型，同时不把 webhook 等中断误呈现为审批操作。
  it("recognizes only approval interrupts", () => {
    expect(isApprovalInterrupt("approval")).toBe(true);
    expect(isApprovalInterrupt("human_approval")).toBe(true);
    expect(isApprovalInterrupt("webhook")).toBe(false);
  });

  // 验证已过期或格式异常的审批会保守关闭操作，避免提交必然失败的恢复令牌。
  it("fails closed for expired approval tokens", () => {
    const now = Date.parse("2026-09-03T12:00:00Z");
    expect(isInterruptExpired("2026-09-03T11:59:59Z", now)).toBe(true);
    expect(isInterruptExpired("not-a-date", now)).toBe(true);
    expect(isInterruptExpired("2026-09-03T12:00:01Z", now)).toBe(false);
  });
});
