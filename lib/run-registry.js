// ── Durable bounded local run registry (Phase 2 foundation) ──────────────────
// One JSON file per session under a fixed local root:
//   <root>/<sessionId>/runs.json
//
// Properties:
//  * DURABLE + ATOMIC: every mutation is serialized per session (promise
//    chain), written to a temp file in the same directory, and published with
//    rename(2) — a crash never leaves a torn runs.json behind.
//  * RESTART LOAD + RECONCILIATION: on first access after a restart the file
//    is loaded and validated; any record still "in-flight" (its owning process
//    is gone, so it can no longer claim to be running) is moved to the
//    "reconciling" reconciliation state — its remote outcome is UNKNOWN and
//    the pair lock it holds stays closed until explicit terminal evidence.
//  * BOUNDED: at most MAX_RECORDS records per session (oldest TERMINAL
//    records are evicted first; a registry saturated with non-terminal
//    records fails closed instead of dropping reconciliation evidence),
//    notes capped at MAX_NOTE_CHARS, and a hard serialized-file size cap.
//  * NEVER STORES SECRETS OR RAW LOGS: the record schema is an ALLOWLIST.
//    stdout/stderr/tokens/credentials have no field to live in (unknown
//    fields throw), and every note is scanned for sensitive material and
//    rejected fail-closed when it looks secret-like.
//
// The exact-once pair lock identity remains project+operation: the registry
// DERIVES pairKey from the record's project and operation (callers cannot
// supply it), and at most one non-terminal record (in-flight or reconciling)
// may exist per pair.
//
// This module is a store: it performs no remote access, no submission, and no
// scheduler queries. Reconciliation of a "reconciling" record to a terminal
// state happens by an explicit update() call with terminal evidence.
//
// Limitation: per-session serialization is per-PROCESS (an in-memory promise
// chain). Two app instances sharing the same registry root would last-writer-
// wins on the record set; torn files remain impossible (tmp+rename), but an
// inter-process lock is out of scope for the single-app DSH model.
import { randomBytes } from "node:crypto";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { join } from "node:path";

const SCHEMA = "genbio-run-registry/1";
const MAX_RECORDS = 256;
const MAX_NOTE_CHARS = 512;
const MAX_FILE_BYTES = 1024 * 1024;
const SAFE_SESSION_RE = /^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/u;
const SAFE_RUN_ID_RE = /^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/u;
const SAFE_NAME_RE = /^[a-z0-9][a-z0-9-]*$/u;
const HEX64_RE = /^[a-f0-9]{64}$/u;
const SOURCES = new Set(["aizyme", "pinned", "project", "workflow", "manual"]);
const TERMINAL_STATUSES = new Set(["completed", "failed", "killed", "cancelled"]);
const NON_TERMINAL_STATUSES = new Set(["in-flight", "reconciling"]);
const ALL_STATUSES = new Set(["in-flight", "reconciling", "completed", "failed", "killed", "cancelled"]);
// Conservative sensitive-material scan applied to free-text notes. A note that
// MIGHT carry credential-like material is rejected rather than stored.
const SECRET_RE = /\b(api[_-]?keys?|access[_-]?keys?|auth[_-]?tokens?|client[_-]?secrets?|passwords?|passwds?|private[_-]?keys?|ssh-(?:rsa|ed25519|dss)|BEGIN [A-Z]+ PRIVATE KEY)\b|Bearer\s+[A-Za-z0-9._~+/=-]{8,}/iu;

function plainObject(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function assertSafeInteger(value, label, min = 0, max = Number.MAX_SAFE_INTEGER) {
  if (!Number.isSafeInteger(value) || value < min || value > max) throw new Error(`${label} must be a safe integer ${min}..${max}`);
}

function assertNote(note, label) {
  if (typeof note !== "string") throw new Error(`${label} must be a string`);
  if (note.length > MAX_NOTE_CHARS) throw new Error(`${label} exceeds ${MAX_NOTE_CHARS} characters`);
  if (SECRET_RE.test(note)) throw new Error(`${label} appears to contain sensitive material and is not stored`);
  return note;
}

function assertHex64(value, label) {
  if (value === null || value === undefined) return null;
  if (typeof value !== "string" || !HEX64_RE.test(value)) throw new Error(`${label} must be a full 64-hex SHA-256 digest`);
  return value;
}

// Allowlist normalization: every stored record is rebuilt from these fields
// only. Anything else (logs, tokens, paths of record, ...) is rejected.
function normalizeRecord(record) {
  if (!plainObject(record)) throw new Error("registry record must be a mapping");
  const allowed = new Set(["runId", "source", "project", "operation", "status", "startedAt", "finishedAt", "note", "packageSha", "planHash", "pairKey", "createdAt", "updatedAt"]);
  for (const key of Object.keys(record)) if (!allowed.has(key)) throw new Error(`registry record has an unknown field and is rejected: ${key}`);
  const runId = typeof record.runId === "string" ? record.runId : "";
  if (!SAFE_RUN_ID_RE.test(runId)) throw new Error("registry record runId is missing or unsafe");
  if (!SOURCES.has(record.source)) throw new Error(`registry record source is invalid: ${record.source}`);
  if (typeof record.project !== "string" || !SAFE_NAME_RE.test(record.project)) throw new Error(`registry record project is invalid: ${record.project}`);
  if (typeof record.operation !== "string" || !SAFE_NAME_RE.test(record.operation)) throw new Error(`registry record operation is invalid: ${record.operation}`);
  if (!ALL_STATUSES.has(record.status)) throw new Error(`registry record status is invalid: ${record.status}`);
  const expectedPairKey = `${record.project}/${record.operation}`;
  if (record.pairKey !== undefined && record.pairKey !== expectedPairKey) throw new Error("registry record pairKey does not match its project+operation identity");
  assertSafeInteger(record.startedAt, "registry record startedAt");
  if (record.finishedAt === null || record.finishedAt === undefined) {
    if (TERMINAL_STATUSES.has(record.status)) throw new Error(`registry terminal record must carry finishedAt (status ${record.status})`);
  } else {
    assertSafeInteger(record.finishedAt, "registry record finishedAt");
  }
  if (record.note !== undefined && record.note !== null) assertNote(record.note, "registry record note");
  if (record.packageSha !== undefined) assertHex64(record.packageSha, "registry record packageSha");
  if (record.planHash !== undefined) assertHex64(record.planHash, "registry record planHash");
  if (record.createdAt !== undefined) assertSafeInteger(record.createdAt, "registry record createdAt");
  if (record.updatedAt !== undefined) assertSafeInteger(record.updatedAt, "registry record updatedAt");
  return {
    runId,
    source: record.source,
    project: record.project,
    operation: record.operation,
    pairKey: expectedPairKey,
    status: record.status,
    startedAt: record.startedAt,
    finishedAt: record.finishedAt ?? null,
    note: record.note ?? null,
    packageSha: record.packageSha ?? null,
    planHash: record.planHash ?? null,
    createdAt: record.createdAt ?? record.startedAt,
    updatedAt: record.updatedAt ?? record.createdAt ?? record.startedAt,
  };
}

function evictOldestTerminal(records) {
  while (records.length > MAX_RECORDS) {
    const terminal = records
      .filter((record) => TERMINAL_STATUSES.has(record.status))
      .sort((a, b) => (a.finishedAt ?? a.startedAt) - (b.finishedAt ?? b.startedAt));
    if (terminal.length === 0) throw new Error(`run registry is saturated with ${records.length} non-terminal records (cap ${MAX_RECORDS}); collect terminal evidence before recording more`);
    records.splice(records.indexOf(terminal[0]), 1);
  }
}

export function createRunRegistry(rootDir) {
  if (typeof rootDir !== "string" || !rootDir.startsWith("/") || rootDir.length > 512) throw new Error("run registry root must be an absolute local path");
  // sessionId -> { dir, file, records, chain }
  const sessions = new Map();

  function sessionEntry(sessionId) {
    if (typeof sessionId !== "string" || !SAFE_SESSION_RE.test(sessionId)) throw new Error(`unsafe run registry session id: ${sessionId}`);
    let entry = sessions.get(sessionId);
    if (!entry) {
      const dir = join(rootDir, sessionId);
      entry = { dir, file: join(dir, "runs.json"), records: null, chain: Promise.resolve() };
      sessions.set(sessionId, entry);
    }
    return entry;
  }

  // Every per-session mutation and read funnels through the entry's promise
  // chain: operations are serialized, so concurrent record/update calls can
  // never interleave a stale snapshot over a fresh one.
  function queue(entry, fn) {
    const next = entry.chain.then(fn, fn);
    // Keep the chain alive even when an operation rejects (callers see the
    // error through `next`; the chain itself must not die).
    entry.chain = next.catch(() => {});
    return next;
  }

  async function persist(entry) {
    const payload = JSON.stringify({ schema: SCHEMA, updatedAt: Date.now(), records: entry.records }, null, 1);
    if (Buffer.byteLength(payload, "utf8") > MAX_FILE_BYTES) throw new Error(`run registry exceeds ${MAX_FILE_BYTES} serialized bytes; refusing to write`);
    await mkdir(entry.dir, { recursive: true });
    const tmp = `${entry.file}.tmp-${randomBytes(6).toString("hex")}`;
    await writeFile(tmp, payload, "utf8");
    await rename(tmp, entry.file);
  }

  async function loadEntry(entry) {
    if (entry.records !== null) return entry.records;
    let text;
    try {
      text = await readFile(entry.file, "utf8");
    } catch (error) {
      if (error?.code === "ENOENT") { entry.records = []; return entry.records; }
      throw new Error(`run registry load failed: ${String(error?.message ?? error)}`);
    }
    let parsed;
    try { parsed = JSON.parse(text); } catch { throw new Error("run registry file is corrupt (invalid JSON); refusing to load (fail closed)"); }
    if (!plainObject(parsed) || parsed.schema !== SCHEMA || !Array.isArray(parsed.records)) throw new Error("run registry file is corrupt (invalid shape); refusing to load (fail closed)");
    const records = parsed.records.map((record) => normalizeRecord(record));
    entry.records = records;
    // Restart reconciliation: an "in-flight" record whose owner is gone cannot
    // claim to be running. Its remote outcome is UNKNOWN: move it to the
    // "reconciling" state (the pair lock stays closed) and persist the
    // reconciled file so the transition is durable and observable.
    let reconciled = 0;
    for (const record of records) {
      if (record.status === "in-flight") {
        record.status = "reconciling";
        record.note = "reconciling: in-flight at last load; outcome unknown pending terminal evidence";
        record.updatedAt = Date.now();
        reconciled += 1;
      }
    }
    if (records.length > MAX_RECORDS) evictOldestTerminal(records);
    if (reconciled > 0) await persist(entry);
    entry.records = records;
    return records;
  }

  /** Load (idempotent) one session's persisted records, reconciling restart state. */
  async function load(sessionId) {
    const entry = sessionEntry(sessionId);
    return queue(entry, async () => {
      const records = await loadEntry(entry);
      return { records: structuredClone(records) };
    });
  }

  /** List one session's records (triggers load/reconciliation). Read-only. */
  async function list(sessionId) {
    const entry = sessionEntry(sessionId);
    return queue(entry, async () => structuredClone(await loadEntry(entry)));
  }

  /**
   * Record ONE newly admitted run. The creation status must be "in-flight":
   * a registry record exists because something was admitted. Enforces the
   * pair lock: a pair (project, operation) with an existing non-terminal
   * record cannot admit a second one. Returns a copy of the stored record.
   */
  async function record(sessionId, input) {
    const entry = sessionEntry(sessionId);
    return queue(entry, async () => {
      const records = await loadEntry(entry);
      if (!plainObject(input)) throw new Error("registry record input must be a mapping");
      const inputAllowed = new Set(["runId", "source", "project", "operation", "status", "startedAt", "note", "packageSha", "planHash"]);
      for (const key of Object.keys(input)) if (!inputAllowed.has(key)) throw new Error(`registry record input has an unknown field and is rejected: ${key}`);
      if (input.status !== "in-flight") throw new Error(`registry record creation status must be in-flight (got ${input.status})`);
      const now = Date.now();
      const candidate = normalizeRecord({
        runId: input.runId ?? `rr-${randomBytes(8).toString("hex")}`,
        source: input.source,
        project: input.project,
        operation: input.operation,
        status: input.status,
        startedAt: input.startedAt ?? now,
        finishedAt: null,
        note: input.note,
        packageSha: input.packageSha,
        planHash: input.planHash,
        createdAt: now,
        updatedAt: now,
      });
      if (candidate.status !== "in-flight") throw new Error(`registry record creation status must be in-flight (got ${candidate.status})`);
      const conflict = records.find((item) => item.pairKey === candidate.pairKey && NON_TERMINAL_STATUSES.has(item.status));
      if (conflict) throw new Error(`pair lock ${candidate.pairKey} is held by ${conflict.status} record ${conflict.runId}; the pair (project, operation) is the exact-once identity and cannot admit a second in-flight run`);
      records.push(candidate);
      evictOldestTerminal(records);
      await persist(entry);
      return structuredClone(candidate);
    });
  }

  /**
   * Transition one record. Only non-terminal records are updatable, and the
   * only allowed transition is to a terminal status (plus an optional bounded
   * note). Terminal records are immutable.
   */
  async function update(sessionId, runId, patch = {}) {
    const entry = sessionEntry(sessionId);
    return queue(entry, async () => {
      const records = await loadEntry(entry);
      if (!plainObject(patch)) throw new Error("registry update patch must be a mapping");
      const allowed = new Set(["status", "note"]);
      for (const key of Object.keys(patch)) if (!allowed.has(key)) throw new Error(`registry update has an unknown field and is rejected: ${key}`);
      const target = records.find((item) => item.runId === runId);
      if (!target) throw new Error(`unknown run registry id: ${runId}`);
      if (TERMINAL_STATUSES.has(target.status)) throw new Error(`run registry record ${runId} is terminal (${target.status}); terminal records are immutable`);
      const next = { ...target };
      if (patch.note !== undefined) next.note = assertNote(patch.note, "registry update note");
      if (patch.status !== undefined) {
        if (!TERMINAL_STATUSES.has(patch.status)) throw new Error(`registry update status must be terminal (got ${patch.status})`);
        next.status = patch.status;
        next.finishedAt = Date.now();
      }
      next.updatedAt = Date.now();
      records.splice(records.indexOf(target), 1, next);
      await persist(entry);
      return structuredClone(next);
    });
  }

  /** Clear the in-memory cache for one session (the file stays durable). */
  function drop(sessionId) {
    sessions.delete(sessionId);
  }

  return Object.freeze({ record, update, list, load, drop, rootDir, limits: Object.freeze({ maxRecords: MAX_RECORDS, maxNoteChars: MAX_NOTE_CHARS, maxFileBytes: MAX_FILE_BYTES }) });
}

export { SCHEMA as RUN_REGISTRY_SCHEMA, MAX_RECORDS, MAX_NOTE_CHARS, MAX_FILE_BYTES, SOURCES as RUN_REGISTRY_SOURCES, TERMINAL_STATUSES as RUN_REGISTRY_TERMINAL_STATUSES, NON_TERMINAL_STATUSES as RUN_REGISTRY_NON_TERMINAL_STATUSES };
