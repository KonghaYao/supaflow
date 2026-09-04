export type ValueType = "any" | "string" | "number" | "integer" | "boolean" | "object" | "array";

export interface Position {
  x: number;
  y: number;
}

export interface InputBinding {
  type: ValueType;
  literal?: unknown;
  source?: { nodeId: string; port: string };
}

export interface StudioNode {
  id: string;
  type: string;
  version: string;
  label: string;
  config: Record<string, unknown>;
  inputs: Record<string, InputBinding>;
  position: Position;
  groupId?: string;
  ui?: { collapsed?: boolean };
}

export interface StudioEdge {
  id: string;
  sourceNodeId: string;
  sourcePortId: string;
  targetNodeId: string;
  targetPortId: string;
}

export interface StudioDocument {
  schemaVersion: "flowgram-v1";
  workflowId: string;
  nodes: StudioNode[];
  edges: StudioEdge[];
  groups?: Array<{ id: string; title: string; nodeIds: string[] }>;
  viewport?: { x: number; y: number; zoom: number };
  metadata?: Record<string, unknown>;
}

export interface Draft {
  id: string;
  workflowId: string;
  document: StudioDocument;
  revision: number;
  updatedBy: string;
  createdAt: string;
  updatedAt: string;
}

export interface VersionSummary {
  workflowId: string;
  version: number;
  versionId: string;
  planId: string;
  planHash: string;
  publishedAt: string;
}

/** Execution and recovery guarantees published by the runtime registry. */
export interface NodeCapabilities {
  streaming: boolean;
  cancellable: boolean;
  interruptible: boolean;
  idempotent: boolean;
  recoverySafe: boolean;
  retrySafeErrors?: string[];
}

export interface NodeType {
  type: string;
  version: string;
  inputs: Record<string, { type: ValueType; required: boolean }>;
  outputs: Record<string, { type: ValueType; required: boolean }>;
  configSchema: Record<string, unknown>;
  sensitiveFields: string[];
  capabilities: NodeCapabilities;
  migrations: Array<{ fromVersion: string; toVersion: string; binding: string }>;
}

export interface RunEvent {
  cursor: number;
  type: string;
  data?: unknown;
  at: string;
}

export interface RunRecord {
  id: string;
  planId: string;
  planHash: string;
  status: string;
  priority: number;
  deadlineAt?: string;
  cancelRequestedAt?: string;
  output?: unknown;
  outputMetadata?: {
    contentType: string;
    sizeBytes: number;
    sha256: string;
    sensitive: boolean;
    storage: "inline" | "object";
    downloadUrl?: string;
    retentionStatus?: string;
  };
  rerun?: {
    sourceRunId: string;
    fromNodeId?: string;
    createdAt: string;
  };
  createdAt: string;
  updatedAt: string;
}

export interface RunSummary extends RunRecord {
  workflowId: string;
}

export interface RerunInput {
  fromNodeId?: string;
  priority?: number;
  timeoutMs?: number;
}

export type ApprovalDecision = "approved" | "rejected";

export interface ApprovalDecisionInput {
  resumeToken: string;
  decision: ApprovalDecision;
  comment?: string;
  data?: unknown;
}

export interface RunInterrupt {
  id: string;
  runId: string;
  nodeId: string;
  kind: string;
  expiresAt: string;
  createdAt: string;
  resolvedAt?: string;
  resumeToken?: string;
}

export interface AttemptDiagnostic {
  id: string;
  number: number;
  executor: string;
  status: string;
  input: unknown;
  errorCode?: string;
  startedAt?: string;
  finishedAt?: string;
}

export interface NodeDiagnostic {
  nodeId: string;
  status: string;
  attemptCount: number;
  latestAttemptId: string;
  updatedAt: string;
  attempts: AttemptDiagnostic[];
  output?: unknown;
  outputMetadata?: {
    contentType: string;
    sizeBytes: number;
    sha256: string;
    sensitive: boolean;
    storage: "inline" | "object";
  };
}

export interface RunDetails {
  run: RunRecord;
  input: unknown;
  nodes: NodeDiagnostic[];
  lastEventCursor: number;
}

export interface Readiness {
  ready: boolean;
  failures?: Record<string, string>;
}

export interface CapacitySnapshot {
  activeRuns: number;
  queuedRuns: number;
  activeWorkers: number;
  outboxBacklog: number;
  oldestOutboxAge: number;
  dlqDepth: number;
}

export interface DeadLetter {
  id: string;
  originalMessageId: string;
  aggregateType: string;
  aggregateId: string;
  topic: string;
  payload: unknown;
  attempts: number;
  lastError: string;
  failedAt: string;
  requeuedAt?: string;
}

export interface AuditRecord {
  id: string;
  actorId: string;
  action: string;
  resourceType: string;
  resourceId: string;
  outcome: "allowed" | "denied" | "failed";
  metadata: Record<string, unknown>;
  createdAt: string;
}

export type TriggerKind = "api" | "manual" | "schedule" | "webhook" | "event";

export interface Trigger {
  id: string;
  publicHash: string;
  workflowId: string;
  kind: TriggerKind;
  name: string;
  enabled: boolean;
  config: Record<string, unknown>;
  scheduleInterval?: number;
  misfirePolicy?: "skip" | "fire_once" | "catch_up";
  nextFireAt?: string;
  signingCredentialId?: string;
  eventType?: string;
  createdBy: string;
  createdAt: string;
  updatedAt: string;
}

export interface TriggerInput {
  workflowId: string;
  kind: TriggerKind;
  name: string;
  config: Record<string, unknown>;
  scheduleIntervalSeconds?: number;
  misfirePolicy?: "skip" | "fire_once" | "catch_up";
  firstFireAt?: string;
  signingCredentialId?: string;
  eventType?: string;
}

export interface CredentialMetadata {
  id: string;
  name: string;
  provider: string;
  purpose: string;
  status: string;
  currentVersion: number;
  createdBy: string;
  createdAt: string;
  updatedAt: string;
  revokedAt?: string;
}

export interface RetentionPolicy {
  eventDays: number;
  outputDays: number;
  archiveBeforeDelete: boolean;
}

export interface TenantExport {
  id: string;
  status: string;
  objectId?: string;
  expiresAt?: string;
  createdAt: string;
  completedAt?: string;
}

export interface WebhookSubscription {
  id: string;
  name: string;
  endpointUrl: string;
  eventTypes: string[];
  signingCredentialId: string;
  status: string;
  timeoutMs: number;
  createdBy: string;
  createdAt: string;
  updatedAt: string;
}

export interface WebhookSubscriptionInput {
  name: string;
  endpointUrl: string;
  eventTypes: string[];
  signingCredentialId: string;
  timeoutMs: number;
}
