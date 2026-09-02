// Phase 2: AI.zymes lifecycle records adapt into the durable run registry
// (same registry/status shape) WITHOUT changing existing stage behavior.
// The stage compute path is unchanged: the registry mirror is best-effort —
// a registry failure is surfaced on the run record and never aborts the stage.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { createAizymeTools, LOCAL_REMOTE_BUNDLE, STAGES } from "../lib/aizyme.js";
import { createRunRegistry } from "../lib/run-registry.js";

const SESSION = "aiz-registry-session";
const exec = { agent: { id: "aiz", session: { id: SESSION, header: { cwd: "/tmp" } } }, signal: new AbortController().signal };
const stage = STAGES["stage0-1"];
const sha = (data) => createHash("sha256").update(data).digest("hex");

function freshState() {
  return {
    policy: { hash: "hash-current" }, runs: [],
    envelope: { target: "HPC", node: "gpu04", partition: "gpus", maxCpus: 32, maxGpus: 4, concurrency: 1, policyHash: "hash-current" },
  };
}

function harness({ remoteImpl, runRegistry } = {}) {
  const state = freshState();
  const calls = { remote: [], shell: [] };
  let latest = null;
  const tools = createAizymeTools({
    makeTool: (name, description, parameters, execute) => ({ name, description, parameters, execute }),
    requirePolicy: () => ({ targets: { HPC: { test_gate: { real_submission: "gpu04" }, allowlist: { gpu04: { partition: "gpus" } } } } }),
    requireState: () => state,
    publicState: () => ({}),
    requireRemoteAccess: async () => {},
    runRemote: async (target, command) => { calls.remote.push(command); return remoteImpl?.(target, command) ?? { stdout: "", stderr: "", exitCode: 0 }; },
    shell: {
      resolve: (request) => request,
      run: async (request) => { calls.shell.push(request.command); return { stdout: { text: "" }, stderr: { text: "" }, exitCode: 0, signal: null, timedOut: false }; },
    },
    userQuestions: { ask: async ({ questions }) => ({ answers: questions.map((q) => ({ id: q.id, selected: ["Approve this transfer"] })) }) },
    jobs: { start(spec) { latest = spec.run(); return `job-${calls.remote.length}`; } },
    config: { logMaxBytes: 65536 },
    runRegistry,
  });
  return { tools, calls, state, latest: () => latest };
}

// The stage0-1 success path needs the REAL bundle file digests (the remote
// checksum gate compares against the in-memory local digests).
const bundleSha = Object.fromEntries(stage.files.map((name) => [name, sha(readFileSync(join(LOCAL_REMOTE_BUNDLE, name)))]));

function successRemoteImpl() {
  return (_target, command) => {
    if (command.includes("sbatch --parsable")) return { stdout: `JOB_ID=4242\nRUN_DIR=/remote/stage\n`, stderr: "", exitCode: 0 };
    if (command.includes("sha256sum")) return { stdout: stage.files.map((name) => `${bundleSha[name]}  ${name}`).join("\n"), stderr: "", exitCode: 0 };
    if (command.includes("sinfo")) return { stdout: "", stderr: "", exitCode: 0 }; // documented probe stub no-op
    return { stdout: "", stderr: "", exitCode: 0 };
  };
}

test("a successful stage mirrors its lifecycle into the registry (admission -> completed)", async (t) => {
  const registryDir = await mkdtemp(join(tmpdir(), "aiz-registry-"));
  t.after(() => rm(registryDir, { recursive: true, force: true }));
  const reg = createRunRegistry(registryDir);
  const h = harness({ remoteImpl: successRemoteImpl(), runRegistry: reg });
  const result = await h.tools.stageTool.execute({ operation: "stage0-1" }, exec);
  assert.equal(result.ok, true, "stage admission succeeds");
  await h.latest().done;
  const started = result.status.started;
  assert.equal(started.status, "completed", "existing stage behavior is unchanged (completed)");
  assert.match(started.runId, /^HPC-aizyme-stage0-1-/u);
  assert.equal(started.registryError, null);
  assert.match(started.registryRunId, /^rr-/u, "the run record carries its registry record id");
  const { records } = await reg.load(SESSION);
  assert.equal(records.length, 1, "exactly one registry record for the stage lifecycle");
  const record = records[0];
  assert.equal(record.source, "aizyme");
  assert.equal(record.project, "aizyme", "pair identity stays project+operation: project 'aizyme'");
  assert.equal(record.operation, "stage0-1");
  assert.equal(record.pairKey, "aizyme/stage0-1");
  assert.equal(record.status, "completed", "the stage terminal state reached the registry");
  assert.match(record.note, /terminal \(completed\)/u);
  assert.equal(record.finishedAt !== null, true);
  // No raw logs or secrets in the durable record.
  const text = JSON.stringify(records);
  assert.equal(text.includes("JOB_ID=4242"), false, "raw stdout never enters the registry");
  assert.equal(text.length < 4096, true, "the record is bounded metadata");
});

test("a failed stage mirrors its failure into the registry (admission -> failed)", async (t) => {
  const registryDir = await mkdtemp(join(tmpdir(), "aiz-registry-fail-"));
  t.after(() => rm(registryDir, { recursive: true, force: true }));
  const reg = createRunRegistry(registryDir);
  const h = harness({
    remoteImpl: (_target, command) => (command.includes("test ! -e") ? { stdout: "", stderr: "mkdir refused", exitCode: 1 } : successRemoteImpl()(_target, command)),
    runRegistry: reg,
  });
  const result = await h.tools.stageTool.execute({ operation: "stage0-1" }, exec);
  assert.equal(result.ok, true, "admission still succeeds; the remote failure happens in the run");
  const settled = await h.latest().done;
  assert.equal(settled.status, "failed", "the stage run ends failed exactly as before");
  assert.match(settled.detail, /failed to create fresh remote staging directory/u);
  assert.equal(h.state.runs[0].status, "failed", "the session run record state is unchanged behavior");
  const { records } = await reg.load(SESSION);
  assert.equal(records.length, 1);
  assert.equal(records[0].status, "failed", "the stage failure reached the registry");
  assert.match(records[0].note, /terminal \(failed\)/u);
  assert.equal(h.state.runs.length, 1, "the session run record still exists as before");
});

test("a registry write failure does NOT change stage behavior (best-effort mirror)", async (t) => {
  // Point the registry at a regular file: every persist fails (ENOTDIR).
  const blocker = await mkdtemp(join(tmpdir(), "aiz-registry-block-"));
  const blockerFile = join(blocker, "not-a-dir");
  await writeFile(blockerFile, "x");
  t.after(() => rm(blocker, { recursive: true, force: true }));
  const reg = createRunRegistry(blockerFile);
  const h = harness({ remoteImpl: successRemoteImpl(), runRegistry: reg });
  const result = await h.tools.stageTool.execute({ operation: "stage0-1" }, exec);
  assert.equal(result.ok, true, "the stage is unaffected by the registry failure");
  await h.latest().done;
  const started = result.status.started;
  assert.equal(started.status, "completed", "the stage still completes exactly as before");
  assert.equal(started.registryRunId, null, "no registry record id when the registry write failed");
  assert.match(started.registryError, /run registry/u, "the registry failure is surfaced on the run record");
});

test("a crashed in-flight stage reconciles after restart and keeps its pair lock", async (t) => {
  const registryDir = await mkdtemp(join(tmpdir(), "aiz-registry-crash-"));
  t.after(() => rm(registryDir, { recursive: true, force: true }));
  const reg = createRunRegistry(registryDir);
  const h = harness({ remoteImpl: successRemoteImpl(), runRegistry: reg });
  const result = await h.tools.stageTool.execute({ operation: "stage0-1" }, exec);
  assert.equal(result.ok, true);
  // Simulate a crash BEFORE the terminal mirror: the durable file still says
  // in-flight. A restarted registry must reconcile it.
  const file = join(registryDir, SESSION, "runs.json");
  const onDisk = JSON.parse(await readFile(file, "utf8"));
  assert.equal(onDisk.records[0].status, "in-flight", "the admission record was persisted before the crash");
  const restarted = createRunRegistry(registryDir);
  const { records } = await restarted.load(SESSION);
  assert.equal(records[0].status, "reconciling", "a crashed in-flight stage reconciles after restart");
  // The pair lock stays closed: a second stage admission for the same pair
  // cannot record a second in-flight registry record (surfaced, non-fatal).
  const h2 = harness({ remoteImpl: successRemoteImpl(), runRegistry: restarted });
  const second = await h2.tools.stageTool.execute({ operation: "stage0-1" }, exec);
  assert.equal(second.ok, true, "admission itself is still allowed (the in-memory guard is authoritative)");
  await h2.latest().done;
  const secondStarted = second.status.started;
  assert.equal(secondStarted.registryRunId, null, "the registry pair lock refused the duplicate in-flight record");
  assert.match(secondStarted.registryError, /pair lock/u, "the refusal is surfaced on the run record");
  const after = (await restarted.load(SESSION)).records;
  assert.equal(after.filter((record) => record.pairKey === "aizyme/stage0-1").length, 1, "the pair holds at most one non-terminal record across the crash");
});
