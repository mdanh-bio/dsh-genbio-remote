import assert from "node:assert/strict";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { createPinnedTools } from "../lib/pinned.js";

// t6 blocker specification matrix (captain-owned, 2026-08-27).
//
// Each test below encodes one confirmed t5 review blocker as an executable
// specification. t7 (2026-08-27) implemented the fixes; the skip options were
// removed verbatim so every blocker spec now runs. No assertion was weakened.

const REMOTE_ROOT = "/data01/test/blockers-run";
const exec = { agent: { id: "blk", session: { id: "blk", header: { cwd: "/tmp" } } }, signal: new AbortController().signal };

function harness({ projectsDir, state, remoteImpl } = {}) {
  const calls = { access: 0, jobs: 0, remote: [], shell: [] };
  let latest = null;
  const tools = createPinnedTools({
    makeTool: (name, description, parameters, execute) => ({ name, description, parameters, execute }),
    requirePolicy: () => ({ targets: { HPC: { test_gate: { real_submission: "gpu04" }, allowlist: { gpu04: { partition: "gpus" } } } } }),
    requireState: () => state,
    publicState: () => ({}),
    requireRemoteAccess: async () => { calls.access += 1; },
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
    jobs: { start(spec) { calls.jobs += 1; latest = spec.run(); return `job-${calls.jobs}`; } },
    config: { pinnedProjectsDir: projectsDir, logMaxBytes: 65536 },
  });
  return { tools, calls, latest: () => latest };
}

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), "pinned-blockers-"));
  const localRoot = join(root, "local");
  const projectsDir = join(root, "projects");
  await mkdir(join(localRoot, "sbatch"), { recursive: true });
  await mkdir(projectsDir, { recursive: true });
  t.after(() => rm(root, { recursive: true, force: true }));
  return { localRoot, projectsDir };
}

const valid = "#!/bin/bash\n#SBATCH --job-name=blk\n#SBATCH --partition=gpus\n#SBATCH --nodelist=gpu04\n#SBATCH --nodes=1\n#SBATCH --ntasks=1\n#SBATCH --cpus-per-task=4\n#SBATCH --output=blk_%j.out\n#SBATCH --error=blk_%j.err\n#SBATCH --gres=gpu:1\nset -euo pipefail\ncd \"$SLURM_SUBMIT_DIR\"\necho ok\n";

async function writeProject({ localRoot, projectsDir }, { pythonBin = null, extra = "" } = {}) {
  await writeFile(join(localRoot, "run.sbatch"), valid);
  await writeFile(join(localRoot, "payload.py"), "print('innocent')\n");
  const pythonLine = pythonBin ? `python_bin: ${pythonBin}\n` : "";
  await writeFile(join(projectsDir, "blk.yaml"), `schema_version: 1\nproject: blk\nlocal_root: ${localRoot}\nremote_root: ${REMOTE_ROOT}\n${pythonLine}files:\n  - run.sbatch\n  - payload.py\njobs:\n  run:\n    template: run.sbatch\n    cpus: 4\n    gpus: 1\n${extra}`);
}

function state(overrides = {}) {
  return {
    policy: { hash: "hash-current" }, runs: [],
    envelope: { target: "HPC", node: "gpu04", partition: "gpus", maxCpus: 8, maxGpus: 2, concurrency: 1, policyHash: "hash-current" },
    ...overrides,
  };
}

// ── Blocker 1: no manifest-controlled interpreter may run on the login node ──

test("staging never executes a manifest-selected python interpreter remotely", async (t) => {
  const fx = await fixture(t);
  await writeProject(fx, { pythonBin: "/data01/evil/payload-interpreter" });
  const h = harness({ projectsDir: fx.projectsDir, state: state() });
  await h.tools.stageTool.execute({ project: "blk" }, exec);
  await h.latest().done;
  assert.equal(
    h.calls.remote.some((command) => command.includes("/data01/evil/payload-interpreter") || /py_compile/u.test(command)),
    false,
    "staging must not execute any manifest-controlled interpreter on the login node",
  );
});

test("staging without python_bin performs no remote python execution at all", async (t) => {
  const fx = await fixture(t);
  await writeProject(fx);
  const h = harness({ projectsDir: fx.projectsDir, state: state() });
  await h.tools.stageTool.execute({ project: "blk" }, exec);
  await h.latest().done;
  assert.equal(
    h.calls.remote.some((command) => /\bpython[0-9]*\b/u.test(command) || /py_compile/u.test(command)),
    false,
    "no python interpreter may be invoked remotely when no python_bin is declared",
  );
});

// ── Blocker: remote syntax validation must be bounded, fixed, clean-env bash ──

test("remote bash -n uses the exact fixed clean-environment form", async (t) => {
  const fx = await fixture(t);
  await writeProject(fx);
  const h = harness({ projectsDir: fx.projectsDir, state: state() });
  await h.tools.stageTool.execute({ project: "blk" }, exec);
  await h.latest().done;
  const syntaxCommands = h.calls.remote.filter((command) => /bash\b/u.test(command) && /-n\b/u.test(command));
  assert.ok(syntaxCommands.length > 0, "staged .sbatch files must receive one bounded syntax check");
  for (const command of syntaxCommands) {
    assert.match(command, /env -i PATH=\/usr\/bin:\/bin BASH_ENV=\/dev\/null \/bin\/bash --noprofile --norc -n --/u, "syntax validation must be the exact clean-env fixed-binary form");
  }
});

// ── Blocker 7: stale policy hash must invalidate the envelope before effects ──

test("stale envelope policy hash is rejected before any side effect", async (t) => {
  const fx = await fixture(t);
  await writeProject(fx);
  const h = harness({ projectsDir: fx.projectsDir, state: state({ policy: { hash: "hash-rotated" } }) });
  await assert.rejects(h.tools.jobTool.execute({ project: "blk", operation: "run" }, exec), /policy (hash|changed)|stale/u);
  assert.deepEqual({ access: h.calls.access, jobs: h.calls.jobs, remote: h.calls.remote.length, shell: h.calls.shell.length }, { access: 0, jobs: 0, remote: 0, shell: 0 });
});

// ── Blocker 3: nonterminal allocations must consume envelope capacity ──

test("a second submission while the first allocation is nonterminal is rejected", async (t) => {
  const fx = await fixture(t);
  await writeProject(fx);
  const shared = state();
  const h = harness({
    projectsDir: fx.projectsDir,
    state: shared,
    remoteImpl: (_t, command) => command.includes("sbatch --parsable")
      ? { stdout: "JOB_ID=5001\n", stderr: "", exitCode: 0 }
      : { stdout: "", stderr: "", exitCode: 0 },
  });
  await h.tools.jobTool.execute({ project: "blk", operation: "run" }, exec);
  await h.latest().done;
  // 4 CPUs + 1 GPU still outstanding against a fresh call with the same envelope.
  await assert.rejects(h.tools.jobTool.execute({ project: "blk", operation: "run" }, exec), /exceed|capacity|aggregate/u);
});

// ── Blocker 2: accepted-then-timeout is ambiguous and never resubmits ──

test("sbatch accepted then transport timeout never issues a second sbatch", async (t) => {
  const fx = await fixture(t);
  await writeProject(fx);
  let submitCount = 0;
  const h = harness({
    projectsDir: fx.projectsDir,
    state: state(),
    remoteImpl: (_t, command) => {
      if (command.includes("sbatch --parsable")) {
        submitCount += 1;
        return { stdout: "", stderr: "ssh: connection timed out", exitCode: 255 };
      }
      return { stdout: "", stderr: "", exitCode: 0 };
    },
  });
  await h.tools.jobTool.execute({ project: "blk", operation: "run" }, exec);
  await h.latest().done;
  await h.tools.jobTool.execute({ project: "blk", operation: "run" }, exec);
  await h.latest().done;
  assert.equal(submitCount, 1, "an ambiguous submission must reconcile read-only, never resubmit");
});

// ── Blocker 6: job-id parsing must be an exact single structured record ──

test("malformed or duplicate job-id output is treated as ambiguous, not success", async (t) => {
  const fx = await fixture(t);
  await writeProject(fx);
  const h = harness({
    projectsDir: fx.projectsDir,
    state: state(),
    remoteImpl: (_t, command) => command.includes("sbatch --parsable")
      ? { stdout: "JOB_ID=123evil\nJOB_ID=456\n", stderr: "", exitCode: 0 }
      : { stdout: "", stderr: "", exitCode: 0 },
  });
  await h.tools.jobTool.execute({ project: "blk", operation: "run" }, exec);
  const outcome = await h.latest().done;
  assert.notEqual(outcome.status, "completed", "ambiguous job-id output must never settle as a completed submission");
});

// ── Blocker 4: validated bytes must equal submitted bytes (TOCTOU) ──

test("template mutated between validation and background verification submits nothing", async (t) => {
  const { readFile, writeFile: wf } = await import("node:fs/promises");
  const fx = await fixture(t);
  await writeProject(fx);
  const path = join(fx.localRoot, "run.sbatch");
  const original = await readFile(path, "utf8");
  const h = harness({
    projectsDir: fx.projectsDir,
    state: state(),
    remoteImpl: (_t, command) => command.includes("sbatch --parsable")
      ? { stdout: "JOB_ID=7001\n", stderr: "", exitCode: 0 }
      : { stdout: `${Buffer.from(original).toString("base64").slice(0, 0)}  run.sbatch\n`, stderr: "", exitCode: 0 },
  });
  const pending = h.tools.jobTool.execute({ project: "blk", operation: "run" }, exec);
  await wf(path, original.replace("echo ok", "rm -rf $HOME"));
  await pending;
  await h.latest().done;
  assert.equal(h.calls.remote.some((command) => command.includes("sbatch --parsable")), false, "mutated template bytes must never be submitted");
});
