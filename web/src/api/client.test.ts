import { afterEach, describe, expect, it, vi } from "vitest";

import { parseEventFrame, StudioClient } from "./client";

afterEach(() => vi.unstubAllGlobals());

describe("SSE protocol", () => {
  // 验证 API 的 id/cursor/type 事件帧能被无浏览器流客户端恢复。
  it("parses a run event frame", () => {
    expect(
      parseEventFrame(
        'id: 7\nevent: node.succeeded\ndata: {"cursor":7,"type":"node.succeeded","at":"2026-01-01T00:00:00Z"}',
      ),
    ).toEqual({
      cursor: 7,
      type: "node.succeeded",
      at: "2026-01-01T00:00:00Z",
    });
  });

  // 验证 SSE heartbeat 不会生成虚假时间线事件。
  it("ignores heartbeat comments", () => {
    expect(parseEventFrame(": keepalive")).toBeUndefined();
  });
});

describe("Run monitor API", () => {
  // 验证监控从 workflow 过滤后的 Run 列表选择，不再要求用户输入 Run ID。
  it("lists workflow runs with authenticated tenant context only", async () => {
    const fetchMock = vi.fn((..._arguments: Parameters<typeof fetch>) =>
      Promise.resolve(new Response("[]", { status: 200, headers: { "Content-Type": "application/json" } })),
    );
    vi.stubGlobal("fetch", fetchMock);
    const client = new StudioClient("tenant-api-key");
    await client.listRuns("flow/a");
    const [path, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
    expect(path).toBe("/v1/runs?workflowId=flow%2Fa&limit=100");
    expect(init.headers).toMatchObject({ "X-API-Key": "tenant-api-key" });
    expect(path).not.toContain("tenantId");
  });

  // 验证选择 Run 后读取安全节点详情并正确转义标识符。
  it("loads selected run diagnostics", async () => {
    const payload = {
      run: {
        id: "run/1",
        workflowId: "flow",
        planId: "p",
        planHash: "h",
        status: "FAILED",
        createdAt: "",
        updatedAt: "",
      },
      input: {},
      nodes: [],
      lastEventCursor: 2,
    };
    const fetchMock = vi.fn((..._arguments: Parameters<typeof fetch>) =>
      Promise.resolve(
        new Response(JSON.stringify(payload), { status: 200, headers: { "Content-Type": "application/json" } }),
      ),
    );
    vi.stubGlobal("fetch", fetchMock);
    const result = await new StudioClient("tenant-api-key").getRunDetails("run/1");
    expect(fetchMock.mock.calls[0]?.[0]).toBe("/v1/runs/run%2F1/details");
    expect(result.lastEventCursor).toBe(2);
  });

  // 验证取消和重跑与稳定 API 的路径、请求体和调用方提供的幂等键完全一致。
  it("sends durable cancel and rerun commands", async () => {
    const responseRun = {
      id: "run-2",
      planId: "plan-1",
      planHash: "hash-1",
      status: "PENDING",
      priority: 4,
      createdAt: "2026-09-03T00:00:00Z",
      updatedAt: "2026-09-03T00:00:00Z",
    };
    const fetchMock = vi.fn((..._arguments: Parameters<typeof fetch>) =>
      Promise.resolve(
        new Response(JSON.stringify(responseRun), { status: 202, headers: { "Content-Type": "application/json" } }),
      ),
    );
    vi.stubGlobal("fetch", fetchMock);
    const client = new StudioClient("operator-key");

    await client.cancelRun("run/1", "cancel-key");
    await client.rerunRun("run/1", { fromNodeId: "node-2", priority: 4, timeoutMs: 30_000 }, "rerun-key");

    const [cancelPath, cancelInit] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
    expect(cancelPath).toBe("/v1/runs/run%2F1/cancel");
    expect(cancelInit).toMatchObject({ method: "POST", body: undefined });
    expect(cancelInit.headers).toMatchObject({ "X-API-Key": "operator-key", "Idempotency-Key": "cancel-key" });
    const [rerunPath, rerunInit] = fetchMock.mock.calls[1] as unknown as [string, RequestInit];
    expect(rerunPath).toBe("/v1/runs/run%2F1/rerun");
    expect(rerunInit).toMatchObject({ method: "POST" });
    expect(rerunInit.headers).toMatchObject({ "Idempotency-Key": "rerun-key" });
    expect(JSON.parse(String(rerunInit.body))).toEqual({ fromNodeId: "node-2", priority: 4, timeoutMs: 30_000 });
  });

  // 验证中断列表由已认证租户作用域读取，前端不能注入 tenant。
  it("lists run interrupts without a caller-selected tenant", async () => {
    const fetchMock = vi.fn((..._arguments: Parameters<typeof fetch>) =>
      Promise.resolve(new Response("[]", { status: 200, headers: { "Content-Type": "application/json" } })),
    );
    vi.stubGlobal("fetch", fetchMock);

    await new StudioClient("viewer-key").listRunInterrupts("run/1");

    const [path, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
    expect(path).toBe("/v1/runs/run%2F1/interrupts");
    expect(init.headers).toMatchObject({ "X-API-Key": "viewer-key" });
    expect(path).not.toContain("tenant");
  });

  // 验证批准和拒绝都携带服务端签发的恢复令牌及稳定幂等键。
  it("resolves an approval with the stable interrupt contract", async () => {
    const responseRun = {
      id: "run-1",
      planId: "plan-1",
      planHash: "hash-1",
      status: "RUNNING",
      priority: 0,
      createdAt: "2026-09-03T00:00:00Z",
      updatedAt: "2026-09-03T00:00:01Z",
    };
    const fetchMock = vi.fn((..._arguments: Parameters<typeof fetch>) =>
      Promise.resolve(
        new Response(JSON.stringify(responseRun), { status: 200, headers: { "Content-Type": "application/json" } }),
      ),
    );
    vi.stubGlobal("fetch", fetchMock);

    await new StudioClient("approver-key").resolveApproval(
      "interrupt/1",
      { resumeToken: "server-issued-token", decision: "rejected", comment: "Needs revision" },
      "approval-key",
    );

    const [path, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
    expect(path).toBe("/v1/interrupts/interrupt%2F1/resolve");
    expect(init).toMatchObject({ method: "POST" });
    expect(init.headers).toMatchObject({ "Idempotency-Key": "approval-key" });
    expect(JSON.parse(String(init.body))).toEqual({
      resumeToken: "server-issued-token",
      decision: "rejected",
      comment: "Needs revision",
    });
  });
});

describe("Operator API", () => {
  // 验证运维读取只携带认证 key，tenant 不会由前端 query 选择。
  it("loads tenant-scoped operational projections", async () => {
    const fetchMock = vi.fn((..._arguments: Parameters<typeof fetch>) =>
      Promise.resolve(new Response("[]", { status: 200, headers: { "Content-Type": "application/json" } })),
    );
    vi.stubGlobal("fetch", fetchMock);
    const client = new StudioClient("tenant-api-key");
    await Promise.all([
      client.listTriggers(),
      client.listDeadLetters(),
      client.listAudit({ resourceType: "run", resourceId: "run/1" }),
    ]);
    const paths = fetchMock.mock.calls.map((call) => call[0]);
    expect(paths).toContain("/ops/triggers?limit=100");
    expect(paths).toContain("/ops/dlq?limit=100");
    expect(paths).toContain("/ops/audit?limit=100&resourceType=run&resourceId=run%2F1");
    expect(paths.join("")).not.toContain("tenantId");
  });

  // 验证 Trigger 更新携带乐观锁版本与幂等键，避免控制台竞态覆盖。
  it("uses optimistic concurrency for trigger state changes", async () => {
    const trigger = {
      id: "trigger/1",
      publicHash: "",
      workflowId: "flow",
      kind: "api" as const,
      name: "manual",
      enabled: true,
      config: {},
      createdBy: "actor",
      createdAt: "2026-01-01T00:00:00Z",
      updatedAt: "2026-01-02T00:00:00Z",
    };
    const fetchMock = vi.fn((..._arguments: Parameters<typeof fetch>) =>
      Promise.resolve(
        new Response(JSON.stringify(trigger), { status: 200, headers: { "Content-Type": "application/json" } }),
      ),
    );
    vi.stubGlobal("fetch", fetchMock);
    await new StudioClient("tenant-api-key").setTriggerEnabled(trigger, false, "trigger-state-123");
    const [path, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
    expect(path).toBe("/ops/triggers/trigger%2F1/state");
    expect(init.headers).toMatchObject({ "Idempotency-Key": "trigger-state-123" });
    expect(JSON.parse(String(init.body))).toEqual({ enabled: false, expectedUpdatedAt: trigger.updatedAt });
  });

  // 验证后端无 title 的 RBAC problem 仍向操作员展示明确权限错误。
  it("maps a title-less forbidden problem to an actionable message", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(() =>
        Promise.resolve(
          new Response('{"status":403,"code":"forbidden"}', {
            status: 403,
            headers: { "Content-Type": "application/problem+json" },
          }),
        ),
      ),
    );
    await expect(
      new StudioClient("viewer-key").configureQuota({ maxActiveRuns: 1, maxActiveWorkers: 1, maxQueueDepth: 1 }),
    ).rejects.toMatchObject({
      status: 403,
      code: "forbidden",
      message: "Your role does not allow this operator action.",
    });
  });

  // 验证 credential 明文只出现在写请求，返回模型只消费安全元数据。
  it("sends credential secrets only to create and never synthesizes them in metadata", async () => {
    const metadata = {
      id: "cred-1",
      name: "signer",
      provider: "envelope",
      purpose: "webhook_signing",
      status: "active",
      currentVersion: 1,
      createdBy: "admin",
      createdAt: "",
      updatedAt: "",
    };
    const fetchMock = vi.fn((..._arguments: Parameters<typeof fetch>) =>
      Promise.resolve(
        new Response(JSON.stringify(metadata), { status: 201, headers: { "Content-Type": "application/json" } }),
      ),
    );
    vi.stubGlobal("fetch", fetchMock);
    const result = await new StudioClient("admin-key").createCredential({
      name: "signer",
      purpose: "webhook_signing",
      secret: "one-use-value",
    });
    expect(JSON.parse(String(fetchMock.mock.calls[0]?.[1]?.body))).toMatchObject({ secret: "one-use-value" });
    expect(fetchMock.mock.calls[0]?.[1]?.headers).toMatchObject({ "Idempotency-Key": expect.any(String) });
    expect(result).not.toHaveProperty("secret");
  });
});
