// Adversarial tests for genbio_project_execute: the declarative recipe
// executor wired into the schema-v2 internal execution core.
// Reuses the execution-core primitives (validateEnvelope, assertAggregateCapacity,
// allocationsOf, submissionsOf, startTrackedJob, submitJob, stageAndValidate,
// stageRecipeWrapper) — no parallel submission path.
//
// Covers (per task t1):
//  1. drift rejection (manifest changed → fail closed, ZERO side effects)
//  2. unknown/expired plan hash
//  3. no-envelope
//  4. pair-lock reentry rejection (concurrent same-(project, operation))
//  5. wrapper exact prefix staging (genbio-recipes/<shortHash>.run.sbatch)
//  6. ambiguous no-resubmit reusing execution-core semantics (harness stubs)
//  7. wrapper bytes re-resolution (fresh re-resolution, not stored bytes)
//
// No remote action during development: every runRemote/shell.run is a harness
// stub; the integrity gates (remote sha256, package self-check, node probe,
// reconciliation) are exercised against the stubs exactly as in production.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { createProjectTools } from "../lib/project-tools.js";
import { createProjectSource } from "../lib/project-source.js";
import { resolveRecipe } from "../lib/project.js";

const POLICY_HASH = "a".repeat(64);
const REMOTE_ROOT = "/data01/demo";
const RUN_SH = "#!/bin/bash\necho run\n";
const exec = { agent: { id: "exec", session: { id: "exec", header: { cwd: "/tmp" } } }, signal: new AbortController().signal };
const policy = { targets: { HPC: { test_gate: { real_submission: "gpu04" }, allowlist: { gpu04: { partition: "gpus" } }, environment: { recipe_envs: {} } } } };
const envelope = { target: "HPC", node: "gpu04", partition: "gpus", maxCpus: 8, maxGpus: 1, concurrency: 1, policyHash: POLICY_HASH };

function sha256(data) { return createHash("sha256").update(data).digest("hex"); }

// Build a remote-command stub that answers each integrity gate with the exact
// checksums the production path would emit, so the staging/verify/probe/reconcile
// gates stand as in the real run. `submit` controls the sbatch outcome.
function makeRemoteImpl({ runShSha, wrapperRel, wrapperSha, submit }) {
  return (_target, command) => {
    if (command.includes("sbatch --parsable")) return submit(command);
    // stageAndValidate package verify (ends in `cat PREPARED_SHA256.txt`).
    if (command.includes("cat PREPARED_SHA256.txt")) return { stdout: `${runShSha}  scripts/run.sh\nPINNED_STAGE_VALIDATION_OK\n`, stderr: "", exitCode: 0 };
    // submitJob verifyPackage (`test -s PREPARED_SHA256.txt` self-check).
    if (command.includes("test -s PREPARED_SHA256.txt")) return { stdout: `${runShSha}  scripts/run.sh\n`, stderr: "", exitCode: 0 };
    // stageRecipeWrapper remote sha256 gate on the wrapper.
    if (command.includes("sha256sum") && command.includes("genbio-recipes/")) return { stdout: `${wrapperSha}  ${wrapperRel}\n`, stderr: "", exitCode: 0 };
    // node probe / reconciliation / mkdir / inspect: empty, exit 0 → the
    // documented stub/no-op paths (probe: no marker; reconcile: no candidate).
    return { stdout: "", stderr: "", exitCode: 0 };
  };
}

function harness({ projectsDir, remoteImpl, shellImpl } = {}) {
  const calls = { remote: [], shell: [] };
  let latest = null;
  const state = { policy: { hash: POLICY_HASH }, envelope: { ...envelope }, runs: [], plans: [] };
  const tools = createProjectTools({
    makeTool: (name, description, parameters, execute) => ({ name, description, parameters, execute }),
    requirePolicy: () => policy,
    requireState: () => state,
    publicState: () => ({}),
    config: { projectsDir, logMaxBytes: 65536 },
    runRemote: async (target, command) => { calls.remote.push(command); return remoteImpl?.(target, command) ?? { stdout: "", stderr: "", exitCode: 0 }; },
    shell: { resolve: (request) => request, run: async (request) => { calls.shell.push(request.command); return shellImpl?.(request.command) ?? { stdout: { text: "" }, stderr: { text: "" }, exitCode: 0, signal: null, timedOut: false }; } },
    userQuestions: { ask: async ({ questions }) => ({ answers: questions.map((q) => ({ id: q.id, selected: ["Approve this transfer"] })) }) },
    jobs: { start(spec) { latest = spec.run(); return `job-${calls.remote.length}`; } },
    requireRemoteAccess: async () => {},
  });
  return { tools, calls, state, latest: () => latest };
}

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), "execution-"));
  const localRoot = join(root, "local");
  const projectsDir = join(root, "projects");
  await mkdir(join(localRoot, "scripts"), { recursive: true });
  await mkdir(projectsDir);
  t.after(() => rm(root, { recursive: true, force: true }));
  await writeFile(join(localRoot, "scripts", "run.sh"), RUN_SH);
  const manifest = `schema_version: 2
project: demo
local_root: ${localRoot}
remote_root: ${REMOTE_ROOT}
files:
  - scripts/run.sh
jobs:
  run:
    cpus: 4
    gpus: 1
    recipe:
      name: demo-run
      script: scripts/run.sh
      parameters:
        count: {type: integer, min: 1, max: 10}
      argv:
        - --count
        - {param: count}
`;
  await writeFile(join(projectsDir, "demo.yaml"), manifest);
  return { root, localRoot, projectsDir, manifest };
}

// Compute the FRESH re-resolution (what execution must use): the wrapper bytes,
// the wrapper SHA, and the content-addressed wrapper path.
async function freshWrapper(projectsDir, params = { count: 4 }) {
  const loaded = await createProjectSource({ projectsDir }).loadProject("demo", exec);
  const resolution = resolveRecipe({ manifest: loaded.manifest, operation: "run", parameters: params, policy, envelope });
  return { loaded, resolution, wrapperSha: resolution.bytesSha, shortHash: loaded.manifestSha.slice(0, 12), wrapperRel: `genbio-recipes/${loaded.manifestSha.slice(0, 12)}.run.sbatch` };
}

// ── 1. Drift rejection: manifest changed after planning → fail closed, ZERO side effects ──
test("drift rejection: manifest changed after planning fails closed with zero side effects", async (t) => {
  const fx = await fixture(t);
  const h = harness({ projectsDir: fx.projectsDir });
  const planned = await h.tools.planTool.execute({ project: "demo", operation: "run", parameters: { count: 4 } }, exec);
  const planHash = planned.status.planned.plan_hash;
  // Drift the manifest: change the recipe name → different manifestSha AND
  // different wrapper bytes (bytesSha) → recomputed plan hash differs.
  const manifestPath = join(fx.projectsDir, "demo.yaml");
  const existing = await readFile(manifestPath, "utf8");
  await writeFile(manifestPath, existing.replace("name: demo-run", "name: demo-run-drift"));
  await assert.rejects(h.tools.executeTool.execute({ plan_hash: planHash }, exec), /plan hash drift/u, "a drifted manifest must be rejected");
  assert.equal(h.calls.remote.length, 0, "drift rejection must not touch the remote");
  assert.equal(h.calls.shell.length, 0, "drift rejection must not run local commands");
  assert.equal(h.state.runs.length, 0, "drift rejection records no run");
  assert.equal((h.state.allocations ?? []).length, 0, "drift rejection reserves no allocation");
  assert.equal((h.state.submissions ?? []).length, 0, "drift rejection records no submission");
});

// ── 2. Unknown/expired plan hash ──
test("unknown or expired plan hash fails closed with zero side effects", async (t) => {
  const fx = await fixture(t);
  const h = harness({ projectsDir: fx.projectsDir });
  await assert.rejects(h.tools.executeTool.execute({ plan_hash: "f".repeat(64) }, exec), /unknown or expired/u);
  assert.equal(h.state.runs.length, 0, "no run recorded for an unknown plan");
  assert.equal(h.calls.remote.length, 0, "no remote action for an unknown plan");
  assert.equal((h.state.submissions ?? []).length, 0, "no submission recorded for an unknown plan");
});

// ── 3. No-envelope ──
test("no-envelope: execution is rejected before any side effect when the envelope is unset", async (t) => {
  const fx = await fixture(t);
  const h = harness({ projectsDir: fx.projectsDir });
  const planned = await h.tools.planTool.execute({ project: "demo", operation: "run", parameters: { count: 4 } }, exec);
  const planHash = planned.status.planned.plan_hash;
  h.state.envelope = null; // unset the session envelope after planning
  await assert.rejects(h.tools.executeTool.execute({ plan_hash: planHash }, exec), /envelope/u, "execution without an envelope must be rejected");
  assert.equal(h.calls.remote.length, 0, "no remote action without an envelope");
  assert.equal(h.state.runs.length, 0, "no run recorded without an envelope");
  assert.equal((h.state.allocations ?? []).length, 0, "no allocation reserved without an envelope");
});

// ── 4. Pair-lock reentry rejection ──
test("pair-lock reentry: concurrent same-(project,operation) admits exactly one sbatch", async (t) => {
  const fx = await fixture(t);
  const fw = await freshWrapper(fx.projectsDir);
  let submitCount = 0;
  let releaseSbatch;
  const sbatchInFlight = new Promise((resolve) => { releaseSbatch = resolve; });
  const h = harness({
    projectsDir: fx.projectsDir,
    remoteImpl: makeRemoteImpl({
      runShSha: sha256(RUN_SH), wrapperRel: fw.wrapperRel, wrapperSha: fw.wrapperSha,
      // Hold the first sbatch in flight so the pair is "submitting" when the
      // second concurrent execute reaches the atomic admission block.
      submit: () => { submitCount += 1; return sbatchInFlight.then(() => ({ stdout: `JOB_ID=7777\nRUN_DIR=${REMOTE_ROOT}\n`, stderr: "", exitCode: 0 })); },
    }),
  });
  const planned = await h.tools.planTool.execute({ project: "demo", operation: "run", parameters: { count: 4 } }, exec);
  const planHash = planned.status.planned.plan_hash;
  const settled = await Promise.allSettled([
    h.tools.executeTool.execute({ plan_hash: planHash }, exec),
    h.tools.executeTool.execute({ plan_hash: planHash }, exec),
  ]);
  const fulfilled = settled.filter((s) => s.status === "fulfilled");
  const rejected = settled.filter((s) => s.status === "rejected");
  assert.equal(fulfilled.length, 1, "exactly one concurrent admission succeeds");
  assert.equal(rejected.length, 1, "the other concurrent admission is rejected before sbatch");
  assert.match(rejected[0].reason.message, /in-flight submission/u, "the loser is rejected by the atomic pair-lock");
  // Wait until the admitted attempt's sbatch is actually dispatched (held in
  // flight); the rejected attempt never reached dispatch, so at most one sbatch.
  const deadline = Date.now() + 5000;
  while (!h.calls.remote.some((command) => command.includes("sbatch --parsable"))) {
    if (Date.now() > deadline) throw new Error("the admitted attempt did not reach sbatch dispatch");
    await new Promise((resolve) => setImmediate(resolve));
  }
  assert.equal(submitCount, 1, "exactly one sbatch across the concurrent pair");
  releaseSbatch();
  const outcome = await h.latest().done;
  assert.equal(outcome.status, "completed", "the admitted run completes with a confirmed job id");
  assert.match(h.latest().readOutput(), /JOB_ID=7777/u);
  assert.equal(submitCount, 1, "the rejected call never issues a second sbatch");
});

// ── 5. Wrapper exact prefix staging ──
test("wrapper exact prefix staging: wrapper lands at genbio-recipes/<shortHash>.run.sbatch", async (t) => {
  const fx = await fixture(t);
  const fw = await freshWrapper(fx.projectsDir);
  const h = harness({
    projectsDir: fx.projectsDir,
    remoteImpl: makeRemoteImpl({ runShSha: sha256(RUN_SH), wrapperRel: fw.wrapperRel, wrapperSha: fw.wrapperSha, submit: () => ({ stdout: `JOB_ID=1234\nRUN_DIR=${REMOTE_ROOT}\n`, stderr: "", exitCode: 0 }) }),
  });
  const planned = await h.tools.planTool.execute({ project: "demo", operation: "run", parameters: { count: 4 } }, exec);
  const result = await h.tools.executeTool.execute({ plan_hash: planned.status.planned.plan_hash }, exec);
  assert.equal(result.ok, true, "admission succeeds");
  const outcome = await h.latest().done;
  assert.equal(outcome.status, "completed", "the recipe submission completes");
  assert.match(h.latest().readOutput(), /JOB_ID=1234/u);
  // The rclone staging command targeted the exact content-addressed prefix.
  const wrapperRclone = h.calls.shell.find((c) => c.includes("rclone") && c.includes("genbio-recipes/"));
  assert.ok(wrapperRclone, "the wrapper was staged via rclone");
  assert.ok(wrapperRclone.includes(`hpc:${REMOTE_ROOT}/genbio-recipes/${fw.shortHash}.run.sbatch`), `wrapper staged at the exact prefix (got: ${wrapperRclone})`);
  // The remote SHA-256 gate and the clean-env bash -n both ran on that path.
  assert.ok(h.calls.remote.some((c) => c.includes(`sha256sum '${fw.wrapperRel}'`)), "remote sha256 gate ran on the wrapper");
  assert.ok(h.calls.remote.some((c) => c.includes(`bash --noprofile --norc -n -- '${fw.wrapperRel}'`)), "clean-env bash -n ran on the wrapper");
  // The sbatch submitted the wrapper path (relative to the run-dir root).
  const sbatch = h.calls.remote.find((c) => c.includes("sbatch --parsable"));
  assert.ok(sbatch.includes(`${fw.wrapperRel}`), `sbatch submitted the wrapper path (got: ${sbatch})`);
});

// ── 6. Ambiguous no-resubmit (reusing execution-core semantics via harness stubs) ──
test("ambiguous no-resubmit: a lost-transport submission is never resubmitted", async (t) => {
  const fx = await fixture(t);
  const fw = await freshWrapper(fx.projectsDir);
  let submitCount = 0;
  const h = harness({
    projectsDir: fx.projectsDir,
    remoteImpl: makeRemoteImpl({
      runShSha: sha256(RUN_SH), wrapperRel: fw.wrapperRel, wrapperSha: fw.wrapperSha,
      // Local shell timeout mid-dispatch (SIGTERM, timedOut, exit null): a lost
      // transport — the sbatch may already have been accepted.
      submit: () => { submitCount += 1; return { stdout: "", stderr: "", exitCode: null, signal: "SIGTERM", timedOut: true }; },
    }),
  });
  const planned = await h.tools.planTool.execute({ project: "demo", operation: "run", parameters: { count: 4 } }, exec);
  const planHash = planned.status.planned.plan_hash;
  // First execute: the dispatch times out → the attempt ends AMBIGUOUS.
  await h.tools.executeTool.execute({ plan_hash: planHash }, exec);
  const firstOutcome = await h.latest().done;
  assert.equal(firstOutcome.status, "failed", "a timedOut submission settles as a failed run");
  assert.match(firstOutcome.detail, /ambiguous|transport|timed out|reconcil/u, "the timedOut outcome must be classified ambiguous, not a definite failure");
  assert.equal(submitCount, 1, "exactly one sbatch was dispatched");
  // Second execute: reconcile read-only, NEVER resubmit (execution-core exact-once gate).
  await h.tools.executeTool.execute({ plan_hash: planHash }, exec);
  const secondOutcome = await h.latest().done;
  assert.equal(secondOutcome.status, "failed", "the gated pair stays ambiguous until resolved");
  assert.match(secondOutcome.detail, /still ambiguous|resubmitted|reconcil/u, "the second attempt reconciles and reports the unresolved ambiguity");
  assert.equal(submitCount, 1, "a timedOut submission must reconcile read-only, never resubmit");
});

// ── 7. Wrapper bytes re-resolution ──
test("wrapper bytes re-resolution: the submitted wrapper is the FRESH re-resolution, not the stored bytes", async (t) => {
  const fx = await fixture(t);
  const fw = await freshWrapper(fx.projectsDir);
  const h = harness({
    projectsDir: fx.projectsDir,
    // The stub answers the wrapper sha256 gate with the FRESH re-resolution's
    // SHA. If execution used the (tampered) stored bytes instead of re-resolving,
    // the gate would compute a different SHA and staging would fail.
    remoteImpl: makeRemoteImpl({ runShSha: sha256(RUN_SH), wrapperRel: fw.wrapperRel, wrapperSha: fw.wrapperSha, submit: () => ({ stdout: `JOB_ID=999\nRUN_DIR=${REMOTE_ROOT}\n`, stderr: "", exitCode: 0 }) }),
  });
  const planned = await h.tools.planTool.execute({ project: "demo", operation: "run", parameters: { count: 4 } }, exec);
  const planHash = planned.status.planned.plan_hash;
  // Tamper with the STORED sbatchText (same planHash). The execute must ignore
  // it and use the FRESH re-resolution.
  const idx = h.state.plans.findIndex((r) => r.planHash === planHash);
  const original = h.state.plans[idx];
  const tampered = Object.freeze({ ...original, sbatchText: "#!/bin/bash\n# TAMPERED (must be ignored by re-resolution)\n" });
  h.state.plans[idx] = tampered;
  assert.notEqual(fw.wrapperSha, sha256(tampered.sbatchText), "sanity: tampered stored bytes differ from the fresh re-resolution");
  const result = await h.tools.executeTool.execute({ plan_hash: planHash }, exec);
  assert.equal(result.ok, true, "admission succeeds");
  const outcome = await h.latest().done;
  assert.equal(outcome.status, "completed", "execution succeeds only if it used the FRESH re-resolution bytes (the gate passed)");
  assert.match(h.latest().readOutput(), /JOB_ID=999/u);
  // The wrapper bytes used hashed to the fresh re-resolution's SHA.
  assert.equal(sha256(Buffer.from(fw.resolution.sbatchText, "utf8")), fw.wrapperSha, "the fresh re-resolution bytes hash to the gated wrapper SHA");
});

// ── Bonus: a clean end-to-end success path (no ambiguity) ──
test("clean end-to-end: approved plan stages the package + wrapper and submits one job", async (t) => {
  const fx = await fixture(t);
  const fw = await freshWrapper(fx.projectsDir, { count: 7 }); // match the plan's parameters
  let submitCount = 0;
  const h = harness({
    projectsDir: fx.projectsDir,
    remoteImpl: makeRemoteImpl({ runShSha: sha256(RUN_SH), wrapperRel: fw.wrapperRel, wrapperSha: fw.wrapperSha, submit: () => { submitCount += 1; return { stdout: `JOB_ID=4242\nRUN_DIR=${REMOTE_ROOT}\n`, stderr: "", exitCode: 0 }; } }),
  });
  const planned = await h.tools.planTool.execute({ project: "demo", operation: "run", parameters: { count: 7 } }, exec);
  const result = await h.tools.executeTool.execute({ plan_hash: planned.status.planned.plan_hash }, exec);
  assert.equal(result.ok, true, "admission succeeds");
  assert.equal(result.status.started.runId, h.state.runs[0].runId, "a session run is recorded in the declared status output");
  const outcome = await h.latest().done;
  assert.equal(outcome.status, "completed", "the clean submission completes");
  assert.match(h.latest().readOutput(), /JOB_ID=4242/u);
  assert.equal(submitCount, 1, "exactly one sbatch");
  assert.equal(h.state.runs.length, 1, "one session run recorded");
  const allocation = (h.state.allocations ?? [])[0];
  assert.equal(allocation?.status, "nonterminal", "the accepted job reserves capacity until terminal evidence");
  assert.equal(allocation?.slurmJobId, "4242", "the allocation is bound to the confirmed job id");
});
