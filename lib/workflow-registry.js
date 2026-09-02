// ── Durable bounded allowlisted workflow-run registry (Phase 4 foundation) ──
// One atomic JSON file per session. Records contain workflow/node lifecycle
// metadata only: never raw logs, paths, tokens, credentials, or parameters.
import { randomBytes } from "node:crypto";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { atomicWriteJson, withDirectoryLock } from "./atomic-store.js";

const SCHEMA = "genbio-workflow-registry/1";
const MAX_WORKFLOWS = 128;
const MAX_NODES = 32;
const MAX_FILE_BYTES = 1024 * 1024;
const SAFE_SESSION_RE = /^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/u;
const SAFE_ID_RE = /^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/u;
const SAFE_NAME_RE = /^[a-z0-9][a-z0-9-]*$/u;
const HEX64_RE = /^[a-f0-9]{64}$/u;

const RESTART_TO_RECONCILING = new Set(["submitting", "submitted", "pending", "running", "cancel-requested"]);
const NODE_TERMINAL = new Set(["completed", "failed", "cancelled", "skipped"]);
const NODE_NON_TERMINAL = new Set(["planned", "blocked", "ready", "submitting", "submitted", "pending", "running", "cancel-requested", "reconciling"]);
const NODE_STATUSES = new Set([...NODE_NON_TERMINAL, ...NODE_TERMINAL]);
const WORKFLOW_TERMINAL = new Set(["completed", "failed", "cancelled"]);
const WORKFLOW_NON_TERMINAL = new Set(["planned", "paused", "submitting", "submitted", "pending", "running", "cancel-requested", "reconciling"]);
const WORKFLOW_STATUSES = new Set([...WORKFLOW_NON_TERMINAL, ...WORKFLOW_TERMINAL]);

const NODE_TRANSITIONS = Object.freeze({
  planned: new Set(["blocked", "ready", "cancelled", "skipped"]),
  blocked: new Set(["ready", "cancelled", "skipped"]),
  ready: new Set(["submitting", "cancelled", "skipped"]),
  submitting: new Set(["submitted", "reconciling", "failed", "cancel-requested"]),
  submitted: new Set(["pending", "running", "reconciling", "completed", "failed", "cancel-requested"]),
  pending: new Set(["running", "reconciling", "completed", "failed", "cancel-requested"]),
  running: new Set(["reconciling", "completed", "failed", "cancel-requested"]),
  "cancel-requested": new Set(["reconciling", "cancelled", "completed", "failed"]),
  reconciling: new Set(["pending", "running", "completed", "failed", "cancelled"]),
});
const WORKFLOW_TRANSITIONS = Object.freeze({
  planned: new Set(["paused", "submitting", "running", "cancel-requested", "cancelled", "failed"]),
  paused: new Set(["running", "cancel-requested", "cancelled", "failed"]),
  submitting: new Set(["paused", "submitted", "reconciling", "cancel-requested", "failed"]),
  submitted: new Set(["paused", "pending", "running", "reconciling", "cancel-requested", "completed", "failed"]),
  pending: new Set(["paused", "running", "reconciling", "cancel-requested", "completed", "failed"]),
  running: new Set(["paused", "cancel-requested", "reconciling", "completed", "failed", "cancelled"]),
  "cancel-requested": new Set(["reconciling", "completed", "failed", "cancelled"]),
  reconciling: new Set(["paused", "pending", "running", "cancel-requested", "completed", "failed", "cancelled"]),
});

function plainObject(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}
function assertKeys(value, allowed, label) { for (const key of Object.keys(value)) if (!allowed.has(key)) throw new Error(`${label} has unknown field and is rejected: ${key}`); }
function assertInteger(value, label) { if (!Number.isSafeInteger(value) || value < 0) throw new Error(`${label} must be a non-negative safe integer`); }
function assertName(value, label) { if (typeof value !== "string" || !SAFE_NAME_RE.test(value)) throw new Error(`${label} is invalid`); }
function assertId(value, label) { if (typeof value !== "string" || !SAFE_ID_RE.test(value)) throw new Error(`${label} is invalid or unsafe`); }
function assertHash(value, label) { if (typeof value !== "string" || !HEX64_RE.test(value)) throw new Error(`${label} must be a full 64-hex SHA-256 digest`); }

function normalizeNode(node) {
  if (!plainObject(node)) throw new Error("workflow registry node must be a mapping");
  assertKeys(node, new Set(["nodeId", "project", "operation", "status", "operationPlanHash", "runId", "slurmJobId", "slurmState", "exitCode", "evidence", "createdAt", "updatedAt", "finishedAt"]), "workflow registry node");
  assertName(node.nodeId, "workflow registry nodeId");
  assertName(node.project, "workflow registry node project");
  assertName(node.operation, "workflow registry node operation");
  if (!NODE_STATUSES.has(node.status)) throw new Error(`workflow registry node status is invalid: ${node.status}`);
  assertHash(node.operationPlanHash, "workflow registry node operationPlanHash");
  if (node.runId !== undefined && node.runId !== null && !SAFE_ID_RE.test(node.runId)) throw new Error("workflow registry node runId is invalid");
  if (node.slurmJobId !== undefined && node.slurmJobId !== null && !/^[0-9]{1,10}$/u.test(node.slurmJobId)) throw new Error("workflow registry node slurmJobId must be an exact numeric job id");
  for (const field of ["slurmState", "exitCode", "evidence"]) if (node[field] !== undefined && node[field] !== null && (typeof node[field] !== "string" || node[field].length > 64)) throw new Error(`workflow registry node ${field} must be a bounded string`);
  assertInteger(node.createdAt, "workflow registry node createdAt");
  assertInteger(node.updatedAt, "workflow registry node updatedAt");
  if (NODE_TERMINAL.has(node.status)) assertInteger(node.finishedAt, "workflow registry terminal node finishedAt");
  else if (node.finishedAt !== null && node.finishedAt !== undefined) throw new Error("workflow registry non-terminal node cannot carry finishedAt");
  return { nodeId: node.nodeId, project: node.project, operation: node.operation, status: node.status, operationPlanHash: node.operationPlanHash, runId: node.runId ?? null, slurmJobId: node.slurmJobId ?? null, slurmState: node.slurmState ?? null, exitCode: node.exitCode ?? null, evidence: node.evidence ?? null, createdAt: node.createdAt, updatedAt: node.updatedAt, finishedAt: node.finishedAt ?? null };
}

function normalizeRecord(record) {
  if (!plainObject(record)) throw new Error("workflow registry record must be a mapping");
  assertKeys(record, new Set(["workflowRunId", "workflow", "workflowPlanHash", "workflowPlan", "workflowSha", "workflowPath", "workflowOrigin", "workspace", "nodeRecords", "status", "nodes", "createdAt", "updatedAt", "finishedAt"]), "workflow registry record");
  assertId(record.workflowRunId, "workflow registry workflowRunId");
  assertName(record.workflow, "workflow registry workflow");
  assertHash(record.workflowPlanHash, "workflow registry workflowPlanHash");
  if (record.workflowPlan !== undefined && record.workflowPlan !== null && (!plainObject(record.workflowPlan) || record.workflowPlan.workflow_plan_hash !== record.workflowPlanHash)) throw new Error("workflow registry durable workflowPlan is invalid");
  if (record.workflowSha !== undefined && record.workflowSha !== null) assertHash(record.workflowSha, "workflow registry workflowSha");
  for (const field of ["workflowPath", "workspace"]) if (record[field] !== undefined && record[field] !== null && (typeof record[field] !== "string" || !record[field].startsWith("/") || record[field].length > 1024)) throw new Error(`workflow registry ${field} is invalid`);
  if (record.workflowOrigin !== undefined && record.workflowOrigin !== null && !["workspace", "configured"].includes(record.workflowOrigin)) throw new Error("workflow registry workflowOrigin is invalid");
  if (record.nodeRecords !== undefined && record.nodeRecords !== null && !Array.isArray(record.nodeRecords)) throw new Error("workflow registry nodeRecords must be an array");
  if (!WORKFLOW_STATUSES.has(record.status)) throw new Error(`workflow registry workflow status is invalid: ${record.status}`);
  if (!Array.isArray(record.nodes) || record.nodes.length === 0 || record.nodes.length > MAX_NODES) throw new Error(`workflow registry record must contain 1..${MAX_NODES} nodes`);
  const nodes = record.nodes.map(normalizeNode);
  const ids = new Set();
  for (const node of nodes) {
    if (ids.has(node.nodeId)) throw new Error(`workflow registry duplicate node id: ${node.nodeId}`);
    ids.add(node.nodeId);
  }
  assertInteger(record.createdAt, "workflow registry record createdAt");
  assertInteger(record.updatedAt, "workflow registry record updatedAt");
  if (WORKFLOW_TERMINAL.has(record.status)) {
    assertInteger(record.finishedAt, "workflow registry terminal workflow finishedAt");
    if (nodes.some((node) => !NODE_TERMINAL.has(node.status))) throw new Error("terminal workflow requires every node to be terminal");
  } else if (record.finishedAt !== null && record.finishedAt !== undefined) throw new Error("non-terminal workflow cannot carry finishedAt");
  return { workflowRunId: record.workflowRunId, workflow: record.workflow, workflowPlanHash: record.workflowPlanHash, workflowPlan: record.workflowPlan == null ? null : structuredClone(record.workflowPlan), workflowSha: record.workflowSha ?? null, workflowPath: record.workflowPath ?? null, workflowOrigin: record.workflowOrigin ?? null, workspace: record.workspace ?? null, nodeRecords: record.nodeRecords == null ? [] : structuredClone(record.nodeRecords), status: record.status, nodes, createdAt: record.createdAt, updatedAt: record.updatedAt, finishedAt: record.finishedAt ?? null };
}

function evictOldestTerminal(records) {
  while (records.length > MAX_WORKFLOWS) {
    const terminal = records.filter((record) => WORKFLOW_TERMINAL.has(record.status)).sort((a, b) => (a.finishedAt ?? a.createdAt) - (b.finishedAt ?? b.createdAt));
    if (terminal.length === 0) throw new Error(`workflow registry is saturated with ${records.length} non-terminal workflows (cap ${MAX_WORKFLOWS}); refusing to drop reconciliation evidence`);
    records.splice(records.indexOf(terminal[0]), 1);
  }
}

export function canTransitionWorkflowStatus(from, to) {
  return WORKFLOW_TRANSITIONS[from]?.has(to) ?? false;
}
export function canTransitionWorkflowNodeStatus(from, to) {
  return NODE_TRANSITIONS[from]?.has(to) ?? false;
}
export function isTerminalWorkflowStatus(status) { return WORKFLOW_TERMINAL.has(status); }
export function isTerminalWorkflowNodeStatus(status) { return NODE_TERMINAL.has(status); }

export function createWorkflowRegistry(rootDir) {
  if (typeof rootDir !== "string" || !rootDir.startsWith("/") || rootDir.length > 512) throw new Error("workflow registry root must be an absolute local path");
  const sessions = new Map();

  function entryOf(sessionId) {
    if (typeof sessionId !== "string" || !SAFE_SESSION_RE.test(sessionId)) throw new Error(`unsafe workflow registry session id: ${sessionId}`);
    let entry = sessions.get(sessionId);
    if (!entry) {
      const dir = join(rootDir, sessionId);
      entry = { dir, file: join(dir, "workflows.json"), records: null, chain: Promise.resolve(), lockOwner: () => ({ pid: process.pid, nonce: randomBytes(8).toString("hex"), sessionId }) };
      sessions.set(sessionId, entry);
    }
    return entry;
  }
  function queue(entry, fn) {
    const next = entry.chain.then(fn, fn);
    entry.chain = next.catch(() => {});
    return next;
  }
  async function persist(entry) {
    const document = { schema: SCHEMA, updatedAt: Date.now(), records: entry.records };
    if (Buffer.byteLength(JSON.stringify(document), "utf8") > MAX_FILE_BYTES) throw new Error(`workflow registry exceeds ${MAX_FILE_BYTES} serialized bytes; refusing to write`);
    await atomicWriteJson(entry.file, document);
  }
  async function loadEntry(entry, fresh = false, recover = true) {
    if (!fresh && entry.records !== null) return entry.records;
    let text;
    try { text = await readFile(entry.file, "utf8"); }
    catch (error) {
      if (error?.code === "ENOENT") { entry.records = []; return entry.records; }
      throw new Error(`workflow registry load failed: ${String(error?.message ?? error)}`);
    }
    let parsed;
    try { parsed = JSON.parse(text); } catch { throw new Error("workflow registry file is corrupt (invalid JSON); refusing to load (fail closed)"); }
    if (!plainObject(parsed) || parsed.schema !== SCHEMA || !Array.isArray(parsed.records)) throw new Error("workflow registry file is corrupt (invalid shape); refusing to load (fail closed)");
    assertKeys(parsed, new Set(["schema", "updatedAt", "records"]), "workflow registry file");
    assertInteger(parsed.updatedAt, "workflow registry updatedAt");
    const records = parsed.records.map(normalizeRecord);
    const runIds = new Set();
    for (const record of records) {
      if (runIds.has(record.workflowRunId)) throw new Error(`workflow registry duplicate workflow run id: ${record.workflowRunId}`);
      runIds.add(record.workflowRunId);
    }
    entry.records = records;
    let changed = false;
    if (!recover) return records;
    const now = Date.now();
    for (const record of records) {
      let nodeChanged = false;
      for (const node of record.nodes) if (RESTART_TO_RECONCILING.has(node.status)) {
        node.status = "reconciling";
        node.updatedAt = now;
        nodeChanged = true;
      }
      if (nodeChanged || RESTART_TO_RECONCILING.has(record.status) || record.status === "running" || record.status === "cancel-requested") {
        record.status = "reconciling";
        record.updatedAt = now;
        changed = true;
      }
    }
    if (records.length > MAX_WORKFLOWS) { evictOldestTerminal(records); changed = true; }
    if (changed) await persist(entry);
    return records;
  }

  async function list(sessionId) {
    const entry = entryOf(sessionId);
    return queue(entry, async () => structuredClone(await loadEntry(entry)));
  }
  async function load(sessionId) {
    const records = await list(sessionId);
    return { records };
  }
  async function record(sessionId, input) {
    const entry = entryOf(sessionId);
    return queue(entry, async () => withDirectoryLock(entry.dir, entry.lockOwner(), async () => {
      const records = await loadEntry(entry, true, false);
      if (!plainObject(input)) throw new Error("workflow registry input must be a mapping");
      assertKeys(input, new Set(["workflowRunId", "workflow", "workflowPlanHash", "workflowPlan", "workflowSha", "workflowPath", "workflowOrigin", "workspace", "nodeRecords", "status", "nodes", "createdAt"]), "workflow registry input");
      if (input.status !== undefined && input.status !== "planned") throw new Error("workflow registry creation status must be planned");
      if (!Array.isArray(input.nodes) || input.nodes.length === 0 || input.nodes.length > MAX_NODES) throw new Error(`workflow registry input must contain 1..${MAX_NODES} nodes`);
      const now = input.createdAt ?? Date.now();
      const candidate = normalizeRecord({
        workflowRunId: input.workflowRunId ?? `wr-${randomBytes(8).toString("hex")}`,
        workflow: input.workflow,
        workflowPlanHash: input.workflowPlanHash,
        workflowPlan: input.workflowPlan,
        workflowSha: input.workflowSha,
        workflowPath: input.workflowPath,
        workflowOrigin: input.workflowOrigin,
        workspace: input.workspace,
        nodeRecords: input.nodeRecords,
        status: "planned",
        nodes: input.nodes.map((node) => {
          if (!plainObject(node)) throw new Error("workflow registry input node must be a mapping");
          assertKeys(node, new Set(["nodeId", "project", "operation", "operationPlanHash", "status", "runId", "slurmJobId", "slurmState", "exitCode", "evidence"]), "workflow registry input node");
          return { ...node, status: node.status ?? "planned", createdAt: now, updatedAt: now, finishedAt: null };
        }),
        createdAt: now,
        updatedAt: now,
        finishedAt: null,
      });
      if (candidate.nodes.some((node) => !["planned", "blocked", "ready"].includes(node.status))) throw new Error("workflow registry creation node status must be planned, blocked, or ready");
      if (records.some((record) => record.workflowRunId === candidate.workflowRunId)) throw new Error(`duplicate workflow registry id: ${candidate.workflowRunId}`);
      if (records.some((record) => record.workflowPlanHash === candidate.workflowPlanHash && !WORKFLOW_TERMINAL.has(record.status))) throw new Error(`workflow plan ${candidate.workflowPlanHash} already has a non-terminal workflow run`);
      records.push(candidate);
      evictOldestTerminal(records);
      await persist(entry);
      return structuredClone(candidate);
    }));
  }
  async function updateNode(sessionId, workflowRunId, nodeId, patch) {
    const entry = entryOf(sessionId);
    return queue(entry, async () => withDirectoryLock(entry.dir, entry.lockOwner(), async () => {
      const records = await loadEntry(entry, true, false);
      if (!plainObject(patch)) throw new Error("workflow registry node patch must be a mapping");
      assertKeys(patch, new Set(["status", "runId", "slurmJobId", "slurmState", "exitCode", "evidence"]), "workflow registry node patch");
      if (!NODE_STATUSES.has(patch.status)) throw new Error(`workflow registry node status is invalid: ${patch.status}`);
      if (patch.runId !== undefined && patch.runId !== null && !SAFE_ID_RE.test(patch.runId)) throw new Error("workflow registry node runId is invalid");
      if (patch.slurmJobId !== undefined && patch.slurmJobId !== null && !/^[0-9]{1,10}$/u.test(patch.slurmJobId)) throw new Error("workflow registry node slurmJobId must be an exact numeric job id");
      for (const field of ["slurmState", "exitCode", "evidence"]) if (patch[field] !== undefined && patch[field] !== null && (typeof patch[field] !== "string" || patch[field].length > 64)) throw new Error(`workflow registry node ${field} must be a bounded string`);
      const record = records.find((item) => item.workflowRunId === workflowRunId);
      if (!record) throw new Error(`unknown workflow registry id: ${workflowRunId}`);
      if (WORKFLOW_TERMINAL.has(record.status)) throw new Error(`workflow registry ${workflowRunId} is terminal (${record.status}); terminal workflows are immutable`);
      const node = record.nodes.find((item) => item.nodeId === nodeId);
      if (!node) throw new Error(`unknown workflow registry node: ${nodeId}`);
      if (NODE_TERMINAL.has(node.status)) throw new Error(`workflow registry node ${nodeId} is terminal (${node.status}); terminal nodes are immutable`);
      if (!canTransitionWorkflowNodeStatus(node.status, patch.status)) throw new Error(`invalid workflow node transition ${node.status} -> ${patch.status}`);
      const now = Date.now();
      node.status = patch.status;
      for (const field of ["runId", "slurmJobId", "slurmState", "exitCode", "evidence"]) if (patch[field] !== undefined) node[field] = patch[field];
      node.updatedAt = now;
      if (NODE_TERMINAL.has(node.status)) node.finishedAt = now;
      record.updatedAt = now;
      await persist(entry);
      return structuredClone(node);
    }));
  }
  async function updateWorkflow(sessionId, workflowRunId, patch) {
    const entry = entryOf(sessionId);
    return queue(entry, async () => withDirectoryLock(entry.dir, entry.lockOwner(), async () => {
      const records = await loadEntry(entry, true, false);
      if (!plainObject(patch)) throw new Error("workflow registry workflow patch must be a mapping");
      assertKeys(patch, new Set(["status"]), "workflow registry workflow patch");
      if (!WORKFLOW_STATUSES.has(patch.status)) throw new Error(`workflow registry workflow status is invalid: ${patch.status}`);
      const record = records.find((item) => item.workflowRunId === workflowRunId);
      if (!record) throw new Error(`unknown workflow registry id: ${workflowRunId}`);
      if (WORKFLOW_TERMINAL.has(record.status)) throw new Error(`workflow registry ${workflowRunId} is terminal (${record.status}); terminal workflows are immutable`);
      if (!canTransitionWorkflowStatus(record.status, patch.status)) throw new Error(`invalid workflow transition ${record.status} -> ${patch.status}`);
      if (WORKFLOW_TERMINAL.has(patch.status) && record.nodes.some((node) => !NODE_TERMINAL.has(node.status))) throw new Error("cannot make workflow terminal while any node is non-terminal");
      const now = Date.now();
      record.status = patch.status;
      record.updatedAt = now;
      if (WORKFLOW_TERMINAL.has(record.status)) record.finishedAt = now;
      await persist(entry);
      return structuredClone(record);
    }));
  }
  function drop(sessionId) { sessions.delete(sessionId); }

  return Object.freeze({ record, updateNode, updateWorkflow, list, load, drop, rootDir, limits: Object.freeze({ maxWorkflows: MAX_WORKFLOWS, maxNodes: MAX_NODES, maxFileBytes: MAX_FILE_BYTES }) });
}

export {
  SCHEMA as WORKFLOW_REGISTRY_SCHEMA,
  MAX_WORKFLOWS as MAX_WORKFLOW_RECORDS,
  MAX_NODES as MAX_WORKFLOW_REGISTRY_NODES,
  MAX_FILE_BYTES as MAX_WORKFLOW_REGISTRY_FILE_BYTES,
  WORKFLOW_TERMINAL as WORKFLOW_REGISTRY_TERMINAL_STATUSES,
  NODE_TERMINAL as WORKFLOW_NODE_TERMINAL_STATUSES,
};
