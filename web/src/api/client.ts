import type {
  AuditRecord,
  ApprovalDecisionInput,
  CapacitySnapshot,
  CredentialMetadata,
  DeadLetter,
  Draft,
  NodeType,
  Readiness,
  RetentionPolicy,
  RunDetails,
  RunEvent,
  RunInterrupt,
  RunRecord,
  RunSummary,
  RerunInput,
  StudioDocument,
  TenantExport,
  Trigger,
  TriggerInput,
  VersionSummary,
  WebhookSubscription,
  WebhookSubscriptionInput,
} from "../domain/models";

export class APIError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
  ) {
    super(message);
  }
}

interface Problem {
  status: number;
  code: string;
  title?: string;
}

/** Authenticated client for the stable organization-scoped Studio API. */
export class StudioClient {
  /**
   * Supaflow trusts only the Fenix platform master key: it authenticates the trusted backend,
   * and the organization header is the sole storage, execution, credential, and audit isolation
   * scope. The browser never selects its own identity and cannot override the organization
   * through a request body, a query parameter, or a self-declared header.
   */
  constructor(
    private readonly masterKey: string,
    private readonly organizationId: string,
    private readonly baseURL = "",
  ) {}

  /** Platform auth headers. Missing the key yields 401; missing the organization yields 400. */
  private authHeaders(): Record<string, string> {
    return { Authorization: `Bearer ${this.masterKey}`, "X-Fenix-Organization-Id": this.organizationId };
  }

  listDrafts(signal?: AbortSignal): Promise<Draft[]> {
    return this.request("/v1/workflows", { signal });
  }

  getDraft(workflowId: string, signal?: AbortSignal): Promise<Draft> {
    return this.request(`/v1/workflows/${encodeURIComponent(workflowId)}`, { signal });
  }

  createDraft(workflowId: string, document: StudioDocument, key: string): Promise<Draft> {
    return this.request("/v1/workflows", { method: "POST", idempotencyKey: key, body: { workflowId, document } });
  }

  saveDraft(
    workflowId: string,
    document: StudioDocument,
    expectedRevision: number,
    key: string,
    signal?: AbortSignal,
  ): Promise<Draft> {
    return this.request(`/v1/workflows/${encodeURIComponent(workflowId)}`, {
      method: "PUT",
      idempotencyKey: key,
      body: { document, expectedRevision },
      signal,
    });
  }

  deleteDraft(workflowId: string, expectedRevision: number, key: string): Promise<void> {
    return this.request(`/v1/workflows/${encodeURIComponent(workflowId)}`, {
      method: "DELETE",
      idempotencyKey: key,
      body: { expectedRevision },
    });
  }

  compile(workflowId: string): Promise<Record<string, unknown>> {
    return this.request(`/v1/workflows/${encodeURIComponent(workflowId)}/compile`, { method: "POST" });
  }

  publishDraft(workflowId: string, key: string): Promise<Record<string, unknown>> {
    return this.request(`/v1/workflows/${encodeURIComponent(workflowId)}/publish-draft`, {
      method: "POST",
      idempotencyKey: key,
    });
  }

  dryRun(workflowId: string, sampleInput: unknown): Promise<Record<string, unknown>> {
    return this.request(`/v1/workflows/${encodeURIComponent(workflowId)}/dry-run`, {
      method: "POST",
      body: { sampleInput },
    });
  }

  nodeTest(workflowId: string, nodeId: string, sampleInput: unknown): Promise<Record<string, unknown>> {
    return this.request(`/v1/workflows/${encodeURIComponent(workflowId)}/node-test`, {
      method: "POST",
      body: { nodeId, sampleInput },
    });
  }

  versions(workflowId: string): Promise<VersionSummary[]> {
    return this.request(`/v1/workflows/${encodeURIComponent(workflowId)}/versions`);
  }

  diff(workflowId: string, from: number, to: number): Promise<Record<string, unknown>> {
    return this.request(`/v1/workflows/${encodeURIComponent(workflowId)}/versions/diff?from=${from}&to=${to}`);
  }

  nodeTypes(): Promise<NodeType[]> {
    return this.request("/v1/node-types");
  }

  listRuns(workflowId: string, signal?: AbortSignal): Promise<RunSummary[]> {
    const query = new URLSearchParams({ workflowId, limit: "100" });
    return this.request(`/v1/runs?${query.toString()}`, { signal });
  }

  getRunDetails(runId: string, signal?: AbortSignal): Promise<RunDetails> {
    return this.request(`/v1/runs/${encodeURIComponent(runId)}/details`, { signal });
  }

  cancelRun(runId: string, key: string): Promise<RunRecord> {
    return this.request(`/v1/runs/${encodeURIComponent(runId)}/cancel`, {
      method: "POST",
      idempotencyKey: key,
    });
  }

  rerunRun(runId: string, input: RerunInput, key: string): Promise<RunRecord> {
    return this.request(`/v1/runs/${encodeURIComponent(runId)}/rerun`, {
      method: "POST",
      body: input,
      idempotencyKey: key,
    });
  }

  listRunInterrupts(runId: string, signal?: AbortSignal): Promise<RunInterrupt[]> {
    return this.request(`/v1/runs/${encodeURIComponent(runId)}/interrupts`, { signal });
  }

  resolveApproval(interruptId: string, input: ApprovalDecisionInput, key: string): Promise<RunRecord> {
    return this.request(`/v1/interrupts/${encodeURIComponent(interruptId)}/resolve`, {
      method: "POST",
      body: input,
      idempotencyKey: key,
    });
  }

  readiness(signal?: AbortSignal): Promise<Readiness> {
    return this.request("/ops/ready", { signal }, true);
  }

  capacity(signal?: AbortSignal): Promise<CapacitySnapshot> {
    return this.request("/ops/capacity", { signal });
  }

  drainWorker(workerId: string): Promise<void> {
    return this.request(`/ops/workers/${encodeURIComponent(workerId)}/drain`, {
      method: "POST",
      idempotencyKey: operationKey("worker-drain"),
    });
  }

  listDeadLetters(signal?: AbortSignal): Promise<DeadLetter[]> {
    return this.request("/ops/dlq?limit=100", { signal });
  }

  requeueDeadLetter(id: string): Promise<void> {
    return this.request(`/ops/dlq/${encodeURIComponent(id)}/requeue`, {
      method: "POST",
      idempotencyKey: operationKey("dlq-requeue"),
    });
  }

  listAudit(filters: { resourceType?: string; resourceId?: string }, signal?: AbortSignal): Promise<AuditRecord[]> {
    const query = new URLSearchParams({ limit: "100" });
    if (filters.resourceType) query.set("resourceType", filters.resourceType);
    if (filters.resourceId) query.set("resourceId", filters.resourceId);
    return this.request(`/ops/audit?${query.toString()}`, { signal });
  }

  configureQuota(input: { maxActiveRuns: number; maxActiveWorkers: number; maxQueueDepth: number }): Promise<void> {
    return this.request("/ops/quotas", { method: "POST", body: input, idempotencyKey: operationKey("quota") });
  }

  listTriggers(signal?: AbortSignal): Promise<Trigger[]> {
    return this.request("/ops/triggers?limit=100", { signal });
  }

  createTrigger(input: TriggerInput, key: string): Promise<Trigger> {
    return this.request("/ops/triggers", { method: "POST", body: input, idempotencyKey: key });
  }

  updateTrigger(trigger: Trigger, input: Omit<TriggerInput, "workflowId" | "kind">, key: string): Promise<Trigger> {
    return this.request(`/ops/triggers/${encodeURIComponent(trigger.id)}`, {
      method: "PUT",
      idempotencyKey: key,
      body: { ...input, expectedUpdatedAt: trigger.updatedAt },
    });
  }

  setTriggerEnabled(trigger: Trigger, enabled: boolean, key: string): Promise<Trigger> {
    return this.request(`/ops/triggers/${encodeURIComponent(trigger.id)}/state`, {
      method: "POST",
      idempotencyKey: key,
      body: { enabled, expectedUpdatedAt: trigger.updatedAt },
    });
  }

  deleteTrigger(trigger: Trigger, key: string): Promise<void> {
    const query = new URLSearchParams({ expectedUpdatedAt: trigger.updatedAt });
    return this.request(`/ops/triggers/${encodeURIComponent(trigger.id)}?${query.toString()}`, {
      method: "DELETE",
      idempotencyKey: key,
    });
  }

  fireTrigger(trigger: Trigger, payload: unknown, idempotencyKey: string): Promise<void> {
    return this.request(`/ops/triggers/${encodeURIComponent(trigger.id)}/fire`, {
      method: "POST",
      idempotencyKey,
      body: { kind: trigger.kind, idempotencyKey, payload },
    });
  }

  createCredential(input: { name: string; purpose: string; secret: string }): Promise<CredentialMetadata> {
    return this.request("/ops/credentials", { method: "POST", body: input, idempotencyKey: operationKey("credential-create") });
  }

  rotateCredential(id: string, version: number, secret: string): Promise<CredentialMetadata> {
    return this.request(`/ops/credentials/${encodeURIComponent(id)}/rotate`, {
      method: "POST",
      idempotencyKey: operationKey("credential-rotate"),
      body: { version, secret },
    });
  }

  revokeCredential(id: string): Promise<CredentialMetadata> {
    return this.request(`/ops/credentials/${encodeURIComponent(id)}/revoke`, {
      method: "POST",
      idempotencyKey: operationKey("credential-revoke"),
    });
  }

  configureRetention(policy: RetentionPolicy): Promise<void> {
    return this.request("/ops/retention", { method: "POST", body: policy, idempotencyKey: operationKey("retention") });
  }

  exportTenant(): Promise<TenantExport> {
    return this.request("/ops/exports", { method: "POST", idempotencyKey: operationKey("tenant-export") });
  }

  listWebhookSubscriptions(signal?: AbortSignal): Promise<WebhookSubscription[]> {
    return this.request("/ops/webhook-subscriptions?limit=100", { signal });
  }

  createWebhookSubscription(input: WebhookSubscriptionInput): Promise<WebhookSubscription> {
    return this.request("/ops/webhook-subscriptions", {
      method: "POST",
      body: input,
      idempotencyKey: operationKey("webhook-create"),
    });
  }

  setWebhookSubscriptionEnabled(id: string, enabled: boolean): Promise<WebhookSubscription> {
    return this.request(`/ops/webhook-subscriptions/${encodeURIComponent(id)}/${enabled ? "enable" : "disable"}`, {
      method: "POST",
      idempotencyKey: operationKey(enabled ? "webhook-enable" : "webhook-disable"),
    });
  }

  deleteWebhookSubscription(id: string): Promise<void> {
    return this.request(`/ops/webhook-subscriptions/${encodeURIComponent(id)}`, {
      method: "DELETE",
      idempotencyKey: operationKey("webhook-delete"),
    });
  }

  async streamRunEvents(
    runId: string,
    afterCursor: number,
    onEvent: (event: RunEvent) => void,
    signal: AbortSignal,
  ): Promise<void> {
    const response = await fetch(
      `${this.baseURL}/v1/runs/${encodeURIComponent(runId)}/events?afterCursor=${afterCursor}`,
      {
        headers: { Accept: "text/event-stream", ...this.authHeaders() },
        signal,
      },
    );
    if (!response.ok || !response.body) throw await responseError(response);
    const reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
    let pending = "";
    while (true) {
      const chunk = await reader.read();
      if (chunk.done) break;
      pending += chunk.value.replaceAll("\r\n", "\n");
      const frames = pending.split("\n\n");
      pending = frames.pop() ?? "";
      for (const frame of frames) {
        const event = parseEventFrame(frame);
        if (event) onEvent(event);
      }
    }
  }

  private async request<T>(
    path: string,
    options: {
      method?: string;
      body?: unknown;
      idempotencyKey?: string;
      signal?: AbortSignal;
    } = {},
    acceptUnavailable = false,
  ): Promise<T> {
    const headers: Record<string, string> = { Accept: "application/json", ...this.authHeaders() };
    if (options.body !== undefined) headers["Content-Type"] = "application/json";
    if (options.idempotencyKey) headers["Idempotency-Key"] = options.idempotencyKey;
    const response = await fetch(this.baseURL + path, {
      method: options.method ?? "GET",
      headers,
      body: options.body === undefined ? undefined : JSON.stringify(options.body),
      signal: options.signal,
    });
    if (!response.ok && !(acceptUnavailable && response.status === 503)) throw await responseError(response);
    if (response.status === 204) return undefined as T;
    return response.json() as Promise<T>;
  }
}

function operationKey(prefix: string): string {
  return `${prefix}-${crypto.randomUUID()}`;
}

export function parseEventFrame(frame: string): RunEvent | undefined {
  if (frame.startsWith(":")) return;
  const data = frame
    .split("\n")
    .find((line) => line.startsWith("data: "))
    ?.slice(6);
  if (!data) return;
  const parsed: unknown = JSON.parse(data);
  if (
    !isRecord(parsed) ||
    typeof parsed.cursor !== "number" ||
    typeof parsed.type !== "string" ||
    typeof parsed.at !== "string"
  )
    return;
  return parsed as unknown as RunEvent;
}

async function responseError(response: Response): Promise<APIError> {
  let problem: Problem = {
    status: response.status,
    code: "request_failed",
    title: `Request failed (${response.status})`,
  };
  try {
    const value: unknown = await response.json();
    if (isRecord(value) && typeof value.code === "string") {
      problem = {
        status: response.status,
        code: value.code,
        title: typeof value.title === "string" ? value.title : undefined,
      };
    }
  } catch (cause) {
    if (!(cause instanceof SyntaxError)) throw cause;
  }
  return new APIError(problem.status, problem.code, problem.title ?? problemMessage(problem.code, problem.status));
}

function problemMessage(code: string, status: number): string {
  const messages: Record<string, string> = {
    unauthorized: "Authentication expired or the API key is invalid.",
    forbidden: "Your role does not allow this operator action.",
    conflict: "The resource changed. Refresh it before retrying.",
    invalid_request: "The request is invalid. Check the fields and retry.",
    invalid_idempotency_key: "The mutation identifier is invalid.",
    not_found: "The resource was not found in this tenant.",
    rate_limited: "Tenant capacity is full. Retry after capacity is available.",
  };
  return messages[code] ?? `Request failed (${status}).`;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
