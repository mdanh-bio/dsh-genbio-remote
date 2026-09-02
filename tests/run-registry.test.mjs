// Phase 2: durable bounded local run registry — atomic writes, restart load
// with reconciliation, bounds, secrets/raw-log rejection, and the
// project+operation pair lock. Local fs only; no remote surface.
import assert from "node:assert/strict";
import { mkdtemp, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { createRunRegistry, MAX_RECORDS, MAX_NOTE_CHARS } from "../lib/run-registry.js";

const SESSION = "session-abc123";

async function root(t) {
  const dir = await mkdtemp(join(tmpdir(), "run-registry-"));
  t.after(() => rm(dir, { recursive: true, force: true }));
  return dir;
}

test("records persist durably and load back through a fresh registry instance", async (t) => {
  const dir = await root(t);
  const reg = createRunRegistry(dir);
  const created = await reg.record(SESSION, { source: "aizyme", project: "demo", operation: "run", status: "in-flight", note: "admitted" });
  assert.match(created.runId, /^rr-/u);
  assert.equal(created.pairKey, "demo/run", "pair lock identity is derived as project/operation");
  assert.equal(created.status, "in-flight");
  // The file exists and is valid JSON with the registry schema.
  const file = join(dir, SESSION, "runs.json");
  const onDisk = JSON.parse(await readFile(file, "utf8"));
  assert.equal(onDisk.schema, "genbio-run-registry/1");
  assert.equal(onDisk.records.length, 1);
  // A fresh instance (simulated restart) sees the durable record — still
  // in-flight records are reconciled to "reconciling" (unknown outcome).
  const restarted = createRunRegistry(dir);
  const { records } = await restarted.load(SESSION);
  assert.equal(records.length, 1);
  assert.equal(records[0].runId, created.runId);
  assert.equal(records[0].status, "reconciling", "in-flight at load reconciles to reconciling");
  assert.match(records[0].note, /reconciling/u);
  // The reconciled state is durable.
  const diskAgain = JSON.parse(await readFile(file, "utf8"));
  assert.equal(diskAgain.records[0].status, "reconciling");
});

test("writes are atomic: no temp files remain and the file is always valid", async (t) => {
  const dir = await root(t);
  const reg = createRunRegistry(dir);
  for (let index = 0; index < 5; index += 1) {
    await reg.record(SESSION, { source: "manual", project: "demo", operation: `op${index}`, status: "in-flight" });
    const entries = await readdir(join(dir, SESSION));
    assert.equal(entries.filter((entry) => entry.includes(".tmp-")).length, 0, "temp files are renamed away, never left behind");
    const parsed = JSON.parse(await readFile(join(dir, SESSION, "runs.json"), "utf8"));
    assert.equal(parsed.records.length, index + 1, "every published file is complete and valid");
  }
});

test("restart load of a healthy terminal registry changes nothing", async (t) => {
  const dir = await root(t);
  const reg = createRunRegistry(dir);
  const created = await reg.record(SESSION, { source: "pinned", project: "demo", operation: "run", status: "in-flight" });
  await reg.update(SESSION, created.runId, { status: "completed" });
  const before = await readFile(join(dir, SESSION, "runs.json"), "utf8");
  const restarted = createRunRegistry(dir);
  const { records } = await restarted.load(SESSION);
  assert.equal(records[0].status, "completed", "terminal records are not reconciled");
  const after = await readFile(join(dir, SESSION, "runs.json"), "utf8");
  assert.deepEqual(JSON.parse(before).records, JSON.parse(after).records, "no spurious rewrite for an already-consistent file");
});

test("raw log fields and unknown fields are never stored (allowlist schema)", async (t) => {
  const dir = await root(t);
  const reg = createRunRegistry(dir);
  await assert.rejects(reg.record(SESSION, { source: "aizyme", project: "demo", operation: "run", status: "in-flight", stdout: "raw stdout log" }), /unknown field/u);
  await assert.rejects(reg.record(SESSION, { source: "aizyme", project: "demo", operation: "run", status: "in-flight", stderr: "raw stderr log" }), /unknown field/u);
  await assert.rejects(reg.record(SESSION, { source: "aizyme", project: "demo", operation: "run", status: "in-flight", token: "opaque-token" }), /unknown field/u);
  await assert.rejects(reg.update(SESSION, "anything", { slurmJobId: "123" }), /unknown field/u);
});

test("notes are bounded and scanned for sensitive material", async (t) => {
  const dir = await root(t);
  const reg = createRunRegistry(dir);
  await assert.rejects(reg.record(SESSION, { source: "aizyme", project: "demo", operation: "run", status: "in-flight", note: "x".repeat(MAX_NOTE_CHARS + 1) }), /exceeds/u);
  for (const note of ["api_key=abcd1234", "Bearer abcdefghij", "password=hunter2", "BEGIN RSA PRIVATE KEY", "private_key leaked"]) {
    await assert.rejects(reg.record(SESSION, { source: "aizyme", project: "demo", operation: "run", status: "in-flight", note }), /sensitive material/u);
  }
  const created = await reg.record(SESSION, { source: "aizyme", project: "demo", operation: "run", status: "in-flight", note: "stage admitted" });
  await assert.rejects(reg.update(SESSION, created.runId, { status: "failed", note: "api_key stolen" }), /sensitive material/u, "a secret note on a terminal update is rejected");
  await assert.rejects(reg.update(SESSION, created.runId, { note: "password=xyz" }), /sensitive material/u);
});

test("creation status must be in-flight and terminal creation is impossible", async (t) => {
  const dir = await root(t);
  const reg = createRunRegistry(dir);
  await assert.rejects(reg.record(SESSION, { source: "aizyme", project: "demo", operation: "run", status: "completed" }), /must be in-flight/u);
  await assert.rejects(reg.record(SESSION, { source: "aizyme", project: "demo", operation: "run", status: "reconciling" }), /must be in-flight/u);
  await assert.rejects(reg.record(SESSION, { source: "evil", project: "demo", operation: "run", status: "in-flight" }), /source is invalid/u);
});

test("the pair lock keeps project+operation as the exact-once identity", async (t) => {
  const dir = await root(t);
  const reg = createRunRegistry(dir);
  const first = await reg.record(SESSION, { source: "aizyme", project: "demo", operation: "run", status: "in-flight" });
  // Same pair, second in-flight record: rejected.
  await assert.rejects(reg.record(SESSION, { source: "aizyme", project: "demo", operation: "run", status: "in-flight" }), /pair lock/u);
  // A different operation of the same project is a different pair: allowed.
  await reg.record(SESSION, { source: "aizyme", project: "demo", operation: "other", status: "in-flight" });
  // After the pair reaches terminal, it can admit again.
  await reg.update(SESSION, first.runId, { status: "completed" });
  const second = await reg.record(SESSION, { source: "aizyme", project: "demo", operation: "run", status: "in-flight" });
  assert.notEqual(second.runId, first.runId);
  // A "reconciling" record still holds the pair lock (outcome unknown).
  const restarted = createRunRegistry(dir);
  await restarted.load(SESSION);
  await assert.rejects(restarted.record(SESSION, { source: "aizyme", project: "demo", operation: "run", status: "in-flight" }), /pair lock/u);
});

test("terminal records are immutable; only terminal transitions are allowed", async (t) => {
  const dir = await root(t);
  const reg = createRunRegistry(dir);
  const created = await reg.record(SESSION, { source: "manual", project: "demo", operation: "run", status: "in-flight" });
  await assert.rejects(reg.update(SESSION, created.runId, { status: "in-flight" }), /must be terminal/u);
  await assert.rejects(reg.update(SESSION, created.runId, { status: "reconciling" }), /must be terminal/u);
  await reg.update(SESSION, created.runId, { status: "completed" });
  await assert.rejects(reg.update(SESSION, created.runId, { status: "failed" }), /immutable/u);
  await assert.rejects(reg.update(SESSION, "rr-missing", { status: "completed" }), /unknown run registry id/u);
});

test("the record cap evicts oldest terminal records and fails closed on saturation", async (t) => {
  const dir = await root(t);
  const reg = createRunRegistry(dir);
  for (let index = 0; index < MAX_RECORDS; index += 1) {
    const created = await reg.record(SESSION, { source: "manual", project: "demo", operation: `op${index}`, status: "in-flight" });
    await reg.update(SESSION, created.runId, { status: "completed" });
  }
  let { records } = await reg.load(SESSION);
  assert.equal(records.length, MAX_RECORDS);
  // One more terminal record evicts the oldest terminal.
  const extra = await reg.record(SESSION, { source: "manual", project: "demo", operation: "extra", status: "in-flight" });
  await reg.update(SESSION, extra.runId, { status: "completed" });
  ({ records } = await reg.load(SESSION));
  assert.equal(records.length, MAX_RECORDS, "the cap holds");
  assert.equal(records.some((record) => record.operation === "op0"), false, "the oldest terminal record was evicted");
  assert.equal(records.some((record) => record.operation === "extra"), true);
  // Saturation: a session full of NON-terminal records cannot evict
  // reconciliation evidence — recording the (cap+1)-th fails closed.
  const SAT = "session-sat";
  for (let index = 0; index < MAX_RECORDS; index += 1) {
    await reg.record(SAT, { source: "manual", project: "sat", operation: `sat${index}`, status: "in-flight" });
  }
  await assert.rejects(reg.record(SAT, { source: "manual", project: "sat", operation: "sat-x", status: "in-flight" }), /saturated/u);
});

test("corrupt registry files fail closed instead of resetting the store", async (t) => {
  const dir = await root(t);
  const sessionDir = join(dir, SESSION);
  await reg_writeCorrupt(t, sessionDir, "not json at all");
  const reg1 = createRunRegistry(dir);
  await assert.rejects(reg1.list(SESSION), /corrupt.*invalid JSON/u);
  await reg_writeCorrupt(t, sessionDir, JSON.stringify({ schema: "wrong", records: [] }));
  const reg2 = createRunRegistry(dir);
  await assert.rejects(reg2.list(SESSION), /corrupt.*invalid shape/u);
  // A record with a disallowed field in the durable file is also corrupt.
  const now = Date.now();
  await writeFile(join(sessionDir, "runs.json"), JSON.stringify({ schema: "genbio-run-registry/1", updatedAt: now, records: [{ runId: "rr-seeded", source: "manual", project: "demo", operation: "run", status: "in-flight", pairKey: "demo/run", startedAt: now, finishedAt: null, note: null, packageSha: null, planHash: null, createdAt: now, updatedAt: now }] }), "utf8");
  const file = join(sessionDir, "runs.json");
  const onDisk = JSON.parse(await readFile(file, "utf8"));
  onDisk.records[0].secret = "sneaky";
  await writeFile(file, JSON.stringify(onDisk), "utf8");
  const reg3 = createRunRegistry(dir);
  await assert.rejects(reg3.list(SESSION), /unknown field/u);
});

async function reg_writeCorrupt(t, sessionDir, text) {
  const { mkdir } = await import("node:fs/promises");
  await mkdir(sessionDir, { recursive: true });
  await writeFile(join(sessionDir, "runs.json"), text, "utf8");
}

test("session ids are filesystem-safe and unsafe values fail closed", async (t) => {
  const dir = await root(t);
  const reg = createRunRegistry(dir);
  await assert.rejects(reg.record("../evil", { source: "manual", project: "demo", operation: "run", status: "in-flight" }), /unsafe run registry session id/u);
  await assert.rejects(reg.record("a/b", { source: "manual", project: "demo", operation: "run", status: "in-flight" }), /unsafe run registry session id/u);
  assert.throws(() => createRunRegistry("relative/path"), /absolute/u);
});

test("concurrent record/update calls are serialized per session", async (t) => {
  const dir = await root(t);
  const reg = createRunRegistry(dir);
  await Promise.allSettled([
    reg.record(SESSION, { source: "manual", project: "demo", operation: "op-a", status: "in-flight" }),
    reg.record(SESSION, { source: "manual", project: "demo", operation: "op-b", status: "in-flight" }),
    reg.record(SESSION, { source: "manual", project: "demo", operation: "op-c", status: "in-flight" }),
  ]);
  const { records } = await reg.load(SESSION);
  assert.equal(records.length, 3, "serialized operations never lose a record to an interleaved snapshot");
  const first = records.find((record) => record.operation === "op-a");
  await reg.update(SESSION, first.runId, { status: "killed" });
  assert.equal((await reg.load(SESSION)).records.find((record) => record.operation === "op-a").status, "killed");
});
