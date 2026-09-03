import { randomBytes } from "node:crypto";
import { join } from "node:path";
import { atomicWriteJson, plainObject, readBoundedJson, withDirectoryLock } from "./atomic-store.js";

const SCHEMA = "genbio-execution-registry/1";
const MAX_RECORDS = 256;
const MAX_FILE_BYTES = 4 * 1024 * 1024;
const SAFE_ID_RE = /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,191}$/u;
const SAFE_NAME_RE = /^[a-z0-9][a-z0-9-]*$/u;
const HEX64_RE = /^[a-f0-9]{64}$/u;
const ACTIVE_ALLOCATION = new Set(["submitting", "nonterminal", "ambiguous", "cancel-requested"]);
const TERMINAL_WORKLOAD = new Set(["completed", "failed", "cancelled", "not_applicable"]);

function assertKeys(value, allowed, label) { for (const key of Object.keys(value)) if (!allowed.has(key)) throw new Error(`${label} contains unknown field ${key}`); }
function assertId(value, label) { if (typeof value !== "string" || !SAFE_ID_RE.test(value)) throw new Error(`${label} is invalid`); }
function assertName(value, label) { if (typeof value !== "string" || !SAFE_NAME_RE.test(value)) throw new Error(`${label} is invalid`); }
function assertHash(value, label, nullable = false) { if (nullable && value === null) return; if (typeof value !== "string" || !HEX64_RE.test(value)) throw new Error(`${label} must be a full SHA-256 digest`); }
function assertInteger(value, label, min = 0) { if (!Number.isSafeInteger(value) || value < min) throw new Error(`${label} must be an integer >= ${min}`); }
function boundedString(value, label, max, nullable = true) { if (nullable && value === null) return null; if (typeof value !== "string" || value.length === 0 || value.length > max) throw new Error(`${label} must be a bounded string`); return value; }

function normalizeRecord(record) {
  if (!plainObject(record)) throw new Error("execution registry record must be a mapping");
  assertKeys(record, new Set(["runId", "attemptId", "sessionId", "workspace", "target", "project", "operation", "status", "helperStatus", "workloadStatus", "allocationStatus", "token", "uniqueJobName", "sbatchIssued", "slurmJobId", "planHash", "manifestSha", "packageSha", "wrapperSha", "policyHash", "remoteBase", "remoteRunDir", "cpus", "gpus", "concurrency", "node", "partition", "slurmState", "exitCode", "elapsed", "workloadEvidence", "createdAt", "updatedAt", "finishedAt", "cancelRequestedAt", "note"]), "execution registry record");
  assertId(record.runId, "execution registry runId"); assertId(record.attemptId, "execution registry attemptId"); assertId(record.sessionId, "execution registry sessionId");
  if (typeof record.workspace !== "string" || !record.workspace.startsWith("/") || record.workspace.length > 1024) throw new Error("execution registry workspace is invalid");
  if (record.target !== "HPC") throw new Error("execution registry target must be HPC");
  assertName(record.project, "execution registry project"); assertName(record.operation, "execution registry operation");
  const status = boundedString(record.status, "execution registry status", 32, false);
  const helperStatus = boundedString(record.helperStatus, "execution registry helperStatus", 32, false);
  const workloadStatus = boundedString(record.workloadStatus, "execution registry workloadStatus", 32, false);
  const allocationStatus = boundedString(record.allocationStatus, "execution registry allocationStatus", 32, false);
  if (!/^[a-f0-9]{32}$/u.test(record.token)) throw new Error("execution registry token must be 128-bit hex");
  boundedString(record.uniqueJobName, "execution registry uniqueJobName", 128, false);
  if (typeof record.sbatchIssued !== "boolean") throw new Error("execution registry sbatchIssued must be boolean");
  if (record.slurmJobId !== null && !/^[0-9]{1,10}$/u.test(record.slurmJobId)) throw new Error("execution registry slurmJobId must be numeric or null");
  for (const field of ["planHash", "manifestSha", "packageSha", "wrapperSha", "policyHash"]) assertHash(record[field], `execution registry ${field}`);
  for (const field of ["remoteBase", "remoteRunDir"]) if (typeof record[field] !== "string" || !record[field].startsWith("/") || record[field].length > 2048) throw new Error(`execution registry ${field} is invalid`);
  assertInteger(record.cpus, "execution registry cpus"); assertInteger(record.gpus, "execution registry gpus"); assertInteger(record.concurrency, "execution registry concurrency", 1);
  boundedString(record.node, "execution registry node", 64, false); boundedString(record.partition, "execution registry partition", 64, false);
  for (const field of ["slurmState", "exitCode", "elapsed", "workloadEvidence", "note"]) if (record[field] !== null) boundedString(record[field], `execution registry ${field}`, field === "note" ? 512 : 64, true);
  for (const field of ["createdAt", "updatedAt"]) assertInteger(record[field], `execution registry ${field}`);
  if (record.finishedAt !== null) assertInteger(record.finishedAt, "execution registry finishedAt");
  if (record.cancelRequestedAt !== null) assertInteger(record.cancelRequestedAt, "execution registry cancelRequestedAt");
  if (TERMINAL_WORKLOAD.has(workloadStatus) && record.finishedAt === null) throw new Error("terminal execution registry record requires finishedAt");
  return { ...record, status, helperStatus, workloadStatus, allocationStatus };
}

function documentOf(records) { return { schema: SCHEMA, updatedAt: Date.now(), records }; }
function evict(records) {
  while (records.length > MAX_RECORDS) {
    const candidates = records.filter((record) => !ACTIVE_ALLOCATION.has(record.allocationStatus) && TERMINAL_WORKLOAD.has(record.workloadStatus)).sort((a, b) => (a.finishedAt ?? a.createdAt) - (b.finishedAt ?? b.createdAt));
    if (candidates.length === 0) throw new Error(`execution registry is saturated with ${records.length} active/nonterminal records`);
    records.splice(records.indexOf(candidates[0]), 1);
  }
}

export function createExecutionRegistry(rootDir) {
  if (typeof rootDir !== "string" || !rootDir.startsWith("/")) throw new Error("execution registry root must be absolute");
  const file = join(rootDir, "executions.json");
  const owner = () => ({ pid: process.pid, nonce: randomBytes(8).toString("hex") });
  async function loadUnlocked() {
    const parsed = await readBoundedJson(file, { maxBytes: MAX_FILE_BYTES, missing: documentOf([]) });
    if (!plainObject(parsed) || parsed.schema !== SCHEMA || !Array.isArray(parsed.records)) throw new Error("execution registry is corrupt or has an unsupported schema");
    assertKeys(parsed, new Set(["schema", "updatedAt", "records"]), "execution registry file");
    const records = parsed.records.map(normalizeRecord);
    const ids = new Set();
    for (const record of records) { if (ids.has(record.runId)) throw new Error(`execution registry duplicate runId ${record.runId}`); ids.add(record.runId); }
    return records;
  }
  async function persistUnlocked(records) { evict(records); await atomicWriteJson(file, documentOf(records)); }
  async function list() { return structuredClone(await loadUnlocked()); }
  async function transact(fn) {
    return withDirectoryLock(rootDir, owner(), async () => {
      const records = await loadUnlocked();
      const result = await fn(records);
      records.splice(0, records.length, ...records.map(normalizeRecord));
      await persistUnlocked(records);
      return structuredClone(result);
    });
  }
  async function create(record) {
    return transact((records) => {
      const normalized = normalizeRecord(record);
      if (records.some((item) => item.runId === normalized.runId || item.attemptId === normalized.attemptId)) throw new Error("duplicate execution registry identity");
      records.push(normalized); return normalized;
    });
  }
  async function update(runId, mutate) {
    return transact((records) => {
      const index = records.findIndex((item) => item.runId === runId);
      if (index < 0) throw new Error(`unknown durable Genbio run ${runId}`);
      const before = records[index]; const candidate = normalizeRecord(mutate(structuredClone(before)));
      if (candidate.runId !== before.runId || candidate.attemptId !== before.attemptId || candidate.token !== before.token || candidate.planHash !== before.planHash || candidate.policyHash !== before.policyHash) throw new Error("execution registry immutable identity changed");
      if (before.slurmJobId !== null && candidate.slurmJobId !== before.slurmJobId) throw new Error("execution registry slurmJobId is write-once");
      candidate.updatedAt = Date.now(); records[index] = normalizeRecord(candidate); return records[index];
    });
  }
  async function find(runId) { return (await list()).find((record) => record.runId === runId) ?? null; }
  async function activeForPair(workspace, project, operation) { return (await list()).filter((record) => record.workspace === workspace && record.project === project && record.operation === operation && ACTIVE_ALLOCATION.has(record.allocationStatus)); }
  async function reserve({ sessionId, workspace, project, operation, planHash, manifestSha, packageSha, wrapperSha, policyHash, remoteBase, cpus, gpus, concurrency, node, partition, envelope }) {
    return transact((records) => {
      const existing = records.find((record) => record.workspace === workspace && record.project === project && record.operation === operation && ACTIVE_ALLOCATION.has(record.allocationStatus));
      if (existing) {
        const policyDetail = existing.policyHash !== policyHash ? " under a different policy hash" : "";
        throw new Error(`${project}: operation ${operation} has durable active attempt ${existing.runId} (${existing.allocationStatus})${policyDetail}; reconcile it or collect terminal evidence before another submission`);
      }
      const outstanding = records.filter((record) => record.workspace === workspace && ACTIVE_ALLOCATION.has(record.allocationStatus));
      if (envelope) {
        const usedCpus = outstanding.reduce((sum, item) => sum + item.cpus, 0), usedGpus = outstanding.reduce((sum, item) => sum + item.gpus, 0), usedConcurrency = outstanding.reduce((sum, item) => sum + item.concurrency, 0);
        if (usedCpus + cpus > envelope.maxCpus || usedGpus + gpus > envelope.maxGpus || usedConcurrency + concurrency > envelope.concurrency) throw new Error(`durable aggregate capacity exceeded: ${usedCpus}/${envelope.maxCpus} CPU, ${usedGpus}/${envelope.maxGpus} GPU, ${usedConcurrency}/${envelope.concurrency} concurrency already reserved`);
      }
      const now = Date.now(); const token = randomBytes(16).toString("hex"); const attemptId = `att-${randomBytes(12).toString("hex")}`; const runId = `run-${randomBytes(12).toString("hex")}`;
      const record = normalizeRecord({ runId, attemptId, sessionId, workspace, target: "HPC", project, operation, status: "running", helperStatus: "running", workloadStatus: "submitting", allocationStatus: "submitting", token, uniqueJobName: "pending", sbatchIssued: false, slurmJobId: null, planHash, manifestSha, packageSha, wrapperSha, policyHash, remoteBase, remoteRunDir: `${remoteBase}/runs/${attemptId}`, cpus, gpus, concurrency, node, partition, slurmState: null, exitCode: null, elapsed: null, workloadEvidence: "unobserved", createdAt: now, updatedAt: now, finishedAt: null, cancelRequestedAt: null, note: `outstanding:${outstanding.length}` });
      records.push(record); return { record, outstanding };
    });
  }
  return Object.freeze({ rootDir, file, list, find, create, update, reserve, activeForPair, transact, limits: { maxRecords: MAX_RECORDS, maxFileBytes: MAX_FILE_BYTES } });
}

export { ACTIVE_ALLOCATION, MAX_FILE_BYTES, MAX_RECORDS, SCHEMA as EXECUTION_REGISTRY_SCHEMA, TERMINAL_WORKLOAD, normalizeRecord };
