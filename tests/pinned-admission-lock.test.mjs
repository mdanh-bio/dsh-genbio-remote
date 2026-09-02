// t10 follow-up 1: the concurrent same-(project, operation) double-submit
// race. Admission must be atomic: the in-flight check and the intent/
// "submitting" allocation reservation happen in one contiguous synchronous
// block, so the second concurrent call is rejected BEFORE any sbatch. This
// regression test fires two concurrent executes for the same (project,
// operation) and asserts submitCount === 1.
//
// Interleaving note: with the node:test harness both executes reach their
// validateLocalJobTemplate await before either reserves; the first to resume
// reserves (check + push are atomic), the second then sees the "submitting"
// allocation and is rejected by the in-flight lock. The assertions hold for
// any interleaving: at most one reservation can succeed, so at most one
// sbatch can be issued.
import assert from "node:assert/strict";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { createPinnedTools } from "../lib/pinned.js";

const REMOTE_ROOT = "/data01/test/admission-lock-run";
const exec = { agent: { id: "lock", session: { id: "lock", header: { cwd: "/tmp" } } }, signal: new AbortController().signal };

function harness({ projectsDir, state, remoteImpl } = {}) {
  const calls = { remote: [], shell: [] };
  let latest = null;
  const tools = createPinnedTools({
    makeTool: (name, description, parameters, execute) => ({ name, description, parameters, execute }),
    requirePolicy: () => ({ targets: { HPC: { test_gate: { real_submission: "gpu04" }, allowlist: { gpu04: { partition: "gpus" } } } } }),
    requireState: () => state,
    publicState: () => ({}),
    requireRemoteAccess: async () => {},
    runRemote: async (target, command) => {
      calls.remote.push(command);
      return remoteImpl?.(target, command) ?? { stdout: "", stderr: "", exitCode: 0 };
    },
    shell: {
      resolve: (request) => request,
      run: async (request) => {
        calls.shell.push(request.command);
        return { stdout: { text: "" }, stderr: { text: "" }, exitCode: 0, signal: null, timedOut: false };
      },
    },
    userQuestions: { ask: async ({ questions }) => ({ answers: questions.map((q) => ({ id: q.id, selected: ["Approve this transfer"] })) }) },
    jobs: { start(spec) { latest = spec.run(); return `job-${calls.remote.length}`; } },
    config: { pinnedProjectsDir: projectsDir, logMaxBytes: 65536 },
  });
  return { tools, calls, latest: () => latest };
}

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), "pinned-admission-lock-"));
  const localRoot = join(root, "local");
  const projectsDir = join(root, "projects");
  await mkdir(localRoot, { recursive: true });
  await mkdir(projectsDir, { recursive: true });
  t.after(() => rm(root, { recursive: true, force: true }));
  return { localRoot, projectsDir };
}

const valid = "#!/bin/bash\n#SBATCH --job-name=lock\n#SBATCH --partition=gpus\n#SBATCH --nodelist=gpu04\n#SBATCH --nodes=1\n#SBATCH --ntasks=1\n#SBATCH --cpus-per-task=4\n#SBATCH --output=lock_%j.out\n#SBATCH --error=lock_%j.err\n#SBATCH --gres=gpu:1\nset -euo pipefail\ncd \"$SLURM_SUBMIT_DIR\"\necho ok\n";

async function writeProject({ localRoot, projectsDir }) {
  await writeFile(join(localRoot, "run.sbatch"), valid);
  await writeFile(join(projectsDir, "lock.yaml"), `schema_version: 1\nproject: lock\nlocal_root: ${localRoot}\nremote_root: ${REMOTE_ROOT}\nfiles:\n  - run.sbatch\njobs:\n  run:\n    template: run.sbatch\n    cpus: 4\n    gpus: 1\n`);
}

function state() {
  return {
    policy: { hash: "hash-current" }, runs: [],
    envelope: { target: "HPC", node: "gpu04", partition: "gpus", maxCpus: 8, maxGpus: 2, concurrency: 1, policyHash: "hash-current" },
  };
}

test("concurrent same-(project,operation) admits exactly one submission (submitCount=1)", async (t) => {
  const fx = await fixture(t);
  await writeProject(fx);
  let submitCount = 0;
  const h = harness({
    projectsDir: fx.projectsDir,
    state: state(),
    remoteImpl: (_target, command) => {
      if (command.includes("sbatch --parsable")) {
        submitCount += 1;
        return { stdout: `JOB_ID=7777\nRUN_DIR=${REMOTE_ROOT}\n`, stderr: "", exitCode: 0 };
      }
      // verify + gpu03 probe both answer with an empty, successful remote
      // output: verify treats empty output as a pass; the probe treats
      // marker-less output as the documented stub/no-op path.
      return { stdout: "", stderr: "", exitCode: 0 };
    },
  });

  const settled = await Promise.allSettled([
    h.tools.jobTool.execute({ project: "lock", operation: "run" }, exec),
    h.tools.jobTool.execute({ project: "lock", operation: "run" }, exec),
  ]);

  const outcome = await h.latest().done;

  assert.equal(submitCount, 1, "the concurrent double-submit race must issue exactly one sbatch");
  assert.equal(h.calls.remote.filter((command) => command.includes("sbatch --parsable")).length, 1, "exactly one remote command may contain sbatch");
  const fulfilled = settled.filter((entry) => entry.status === "fulfilled");
  const rejected = settled.filter((entry) => entry.status === "rejected");
  assert.equal(fulfilled.length, 1, "exactly one concurrent admission succeeds");
  assert.equal(rejected.length, 1, "the other concurrent admission is rejected before sbatch");
  assert.match(rejected[0].reason.message, /in-flight submission|exceeds envelope capacity/u, "the loser is rejected by the admission lock or capacity gate");
  assert.equal(fulfilled[0].value.ok, true);
  assert.equal(outcome.status, "completed", "the admitted run completes with a confirmed job id");
  assert.match(h.latest().readOutput(), /JOB_ID=7777/u);
});

test("a failed admission releases the in-flight reservation (pair is not locked)", async (t) => {
  const fx = await fixture(t);
  await writeProject(fx);
  let submitCount = 0;
  // The FIRST attempt dies on a definite pre-sbatch failure (sbatch refuses);
  // the second attempt must then be admitted normally and submit.
  let sbatchCalls = 0;
  const h = harness({
    projectsDir: fx.projectsDir,
    state: state(),
    remoteImpl: (_target, command) => {
      if (command.includes("sbatch --parsable")) {
        submitCount += 1;
        sbatchCalls += 1;
        if (sbatchCalls === 1) return { stdout: "", stderr: "Batch job submission refused", exitCode: 1 };
        return { stdout: `JOB_ID=8888\nRUN_DIR=${REMOTE_ROOT}\n`, stderr: "", exitCode: 0 };
      }
      return { stdout: "", stderr: "", exitCode: 0 };
    },
  });

  const first = await h.tools.jobTool.execute({ project: "lock", operation: "run" }, exec);
  const firstOutcome = await h.latest().done;
  assert.equal(first.ok, true, "admission itself succeeds; the run fails later on the definite sbatch refusal");
  assert.equal(firstOutcome.status, "failed");
  assert.equal(submitCount, 1);

  // The definite failure released the reservation: a fresh admission is
  // allowed and submits normally.
  const second = await h.tools.jobTool.execute({ project: "lock", operation: "run" }, exec);
  const secondOutcome = await h.latest().done;
  assert.equal(second.ok, true, "after a definite pre-sbatch failure the pair is not locked");
  assert.equal(secondOutcome.status, "completed");
  assert.match(h.latest().readOutput(), /JOB_ID=8888/u);
  assert.equal(submitCount, 2);
});

test("a different operation on the same project is not blocked by the in-flight lock", async (t) => {
  const fx = await fixture(t);
  await writeProject(fx);
  // Add a second, distinct operation to the same project.
  const manifestPath = join(fx.projectsDir, "lock.yaml");
  const fs = await import("node:fs/promises");
  const existing = await fs.readFile(manifestPath, "utf8");
  await fs.writeFile(manifestPath, `${existing}  run2:\n    template: run.sbatch\n    cpus: 4\n    gpus: 1\n`);
  let submitCount = 0;
  const h = harness({
    projectsDir: fx.projectsDir,
    state: state(),
    remoteImpl: (_target, command) => {
      if (command.includes("sbatch --parsable")) {
        submitCount += 1;
        return { stdout: `JOB_ID=9999\nRUN_DIR=${REMOTE_ROOT}\n`, stderr: "", exitCode: 0 };
      }
      return { stdout: "", stderr: "", exitCode: 0 };
    },
  });

  // Both operations admitted concurrently. The envelope (8 cpu / 2 gpu /
  // concurrency 1) is tight: the authoritative capacity gate sits inside the
  // atomic admission block, so the second admission sees the first's
  // in-flight "submitting" reservation and is rejected by CAPACITY (not by
  // the same-pair in-flight lock — different pair).
  const settled = await Promise.allSettled([
    h.tools.jobTool.execute({ project: "lock", operation: "run" }, exec),
    h.tools.jobTool.execute({ project: "lock", operation: "run2" }, exec),
  ]);
  await h.latest().done;
  const fulfilled = settled.filter((entry) => entry.status === "fulfilled");
  const rejected = settled.filter((entry) => entry.status === "rejected");
  assert.equal(fulfilled.length, 1, "exactly one concurrent admission succeeds");
  assert.equal(rejected.length, 1, "the other concurrent admission is rejected before sbatch");
  assert.match(rejected[0].reason.message, /exceeds envelope capacity/u, "the loser is rejected by the aggregate capacity gate");
  assert.doesNotMatch(rejected[0].reason.message, /in-flight submission/u, "a different operation must never be rejected by the same-pair in-flight lock");
  assert.equal(submitCount, 1, "exactly one sbatch is issued across the concurrent different operations");
});

test("a rejected concurrent call cannot re-enter while the first sbatch is still in flight", async (t) => {
  const fx = await fixture(t);
  await writeProject(fx);
  let submitCount = 0;
  // The first attempt's sbatch dispatch is HELD in flight (ssh call pending)
  // until this test releases it — the window where an outstanding allocation
  // is possible.
  let releaseSbatch;
  const sbatchInFlight = new Promise((resolve) => { releaseSbatch = resolve; });
  const h = harness({
    projectsDir: fx.projectsDir,
    state: state(),
    remoteImpl: (_target, command) => {
      if (command.includes("sbatch --parsable")) {
        submitCount += 1;
        return sbatchInFlight.then(() => ({ stdout: `JOB_ID=1111\nRUN_DIR=${REMOTE_ROOT}\n`, stderr: "", exitCode: 0 }));
      }
      return { stdout: "", stderr: "", exitCode: 0 };
    },
  });

  await h.tools.jobTool.execute({ project: "lock", operation: "run" }, exec);
  // Wait until the first attempt's sbatch command is actually dispatched
  // (in flight: the allocation is still "submitting", outcome unknown).
  const deadline = Date.now() + 5000;
  while (!h.calls.remote.some((command) => command.includes("sbatch --parsable"))) {
    if (Date.now() > deadline) throw new Error("the first attempt did not reach sbatch dispatch");
    await new Promise((resolve) => setImmediate(resolve));
  }

  // While the first sbatch is in flight, a concurrent call for the same pair
  // MUST be rejected — the in-flight state only clears on a definite end with
  // no possible outstanding allocation.
  await assert.rejects(
    h.tools.jobTool.execute({ project: "lock", operation: "run" }, exec),
    /in-flight submission/u,
    "re-entry while the first sbatch is in flight must be rejected before any sbatch",
  );
  assert.equal(submitCount, 1, "only the in-flight attempt's sbatch is issued");

  // Release the held dispatch: the attempt settles normally.
  releaseSbatch();
  const outcome = await h.latest().done;
  assert.equal(outcome.status, "completed", "the in-flight attempt completes with a confirmed job id");
  assert.match(h.latest().readOutput(), /JOB_ID=1111/u);
  assert.equal(submitCount, 1, "the rejected call never issues a second sbatch");
});

test("an exception after sbatch dispatch keeps the pair gated (ambiguous), never re-submits", async (t) => {
  const fx = await fixture(t);
  await writeProject(fx);
  let submitCount = 0;
  // The sbatch call THROWS (local shell.run failure — timeout/abort/spawn
  // error — once the ssh command is in flight): the job MAY have been
  // accepted, so the attempt must end AMBIGUOUS, not as a definite failure.
  const h = harness({
    projectsDir: fx.projectsDir,
    state: state(),
    remoteImpl: (_target, command) => {
      if (command.includes("sbatch --parsable")) {
        submitCount += 1;
        throw new Error("ssh: connect to host timed out (simulated mid-dispatch failure)");
      }
      return { stdout: "", stderr: "", exitCode: 0 };
    },
  });

  const first = await h.tools.jobTool.execute({ project: "lock", operation: "run" }, exec);
  const firstOutcome = await h.latest().done;
  assert.equal(first.ok, true, "admission succeeds; the dispatch exception surfaces in the run");
  assert.equal(firstOutcome.status, "failed");
  assert.match(firstOutcome.detail, /simulated mid-dispatch failure/u);
  assert.equal(submitCount, 1);

  // The pair is GATED (ambiguous), not released: the next attempt reconciles
  // read-only (sacct) and never issues a second sbatch.
  const second = await h.tools.jobTool.execute({ project: "lock", operation: "run" }, exec);
  const secondOutcome = await h.latest().done;
  assert.equal(second.ok, true, "admission is allowed (nothing 'submitting'); the exact-once gate governs the run");
  assert.equal(secondOutcome.status, "failed", "reconciliation found no single candidate: stays ambiguous, run fails");
  assert.match(secondOutcome.detail, /still ambiguous/u);
  assert.equal(submitCount, 1, "the gated pair never issues a second sbatch");
});

// t11 / t9 advisory A: a LOCAL timeout or signal kill of the sbatch ssh call
// (timedOut / signal, with no exitCode 255 and no stderr text) is a lost
// transport — the job may have been accepted — and must be classified
// transport-ambiguous, never a definite failure.
test("accepted-then-timedOut (local timeout) is ambiguous and never re-submits", async (t) => {
  const fx = await fixture(t);
  await writeProject(fx);
  let submitCount = 0;
  const h = harness({
    projectsDir: fx.projectsDir,
    state: state(),
    remoteImpl: (_t, command) => {
      if (command.includes("sbatch --parsable")) {
        submitCount += 1;
        // Local shell timeout mid-dispatch: ssh killed locally (SIGTERM),
        // exit code null, no stderr — but sbatch may already be accepted.
        return { stdout: "", stderr: "", exitCode: null, signal: "SIGTERM", timedOut: true };
      }
      return { stdout: "", stderr: "", exitCode: 0 };
    },
  });
  await h.tools.jobTool.execute({ project: "lock", operation: "run" }, exec);
  const firstOutcome = await h.latest().done;
  assert.equal(firstOutcome.status, "failed", "a timedOut submission settles as a failed run");
  assert.match(firstOutcome.detail, /ambiguous|transport|timed out|reconcil/u, "the timedOut outcome must be classified ambiguous, not a definite failure");
  assert.equal(submitCount, 1);
  // Second attempt: reconcile read-only only, never a second sbatch.
  await h.tools.jobTool.execute({ project: "lock", operation: "run" }, exec);
  const secondOutcome = await h.latest().done;
  assert.equal(secondOutcome.status, "failed", "the gated pair stays ambiguous until resolved");
  assert.equal(submitCount, 1, "a timedOut submission must reconcile read-only, never resubmit");
});
