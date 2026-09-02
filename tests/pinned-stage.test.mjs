import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtemp, mkdir, readFile, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

const { createPinnedTools } = await import("../lib/pinned.js");

const REMOTE_ROOT = "/data01/test/pinnedtest-run";
const tmp = await mkdtemp(join(tmpdir(), "pinned-stage-test-"));

function makeDeps({ projectsDir, runRemoteImpl, state, latestRef, inspectStdout = "__NO_ROOT__\n" }) {
  const shellCommands = [];
  const remoteCommands = [];
  let counter = 0;
  return {
    shellCommands,
    remoteCommands,
    tools: createPinnedTools({
      makeTool: (name, description, parameters, execute) => ({ name, description, parameters, execute }),
      requirePolicy: () => ({ targets: { HPC: { test_gate: { real_submission: "gpu04" }, allowlist: { gpu04: { partition: "gpus" } } } } }),
      requireState: () => state,
      publicState: (s) => ({ policy: { valid: true, hash: "test" }, envelope: s.envelope, runs: s.runs, lastError: null, remoteGrants: [] }),
      runRemote: async (target, command) => {
        remoteCommands.push({ target, command });
        return runRemoteImpl(target, command, inspectStdout);
      },
      shell: {
        resolve: (request) => request,
        run: async (request) => {
          shellCommands.push(request.command);
          return { stdout: { text: "" }, stderr: { text: "" }, exitCode: 0, signal: null, timedOut: false };
        },
      },
      userQuestions: {
        ask: async ({ questions }) => ({ answers: questions.map((q) => ({ id: q.id, selected: ["Approve this transfer"] })) }),
      },
      jobs: {
        start(spec) {
          const hooks = spec.run();
          latestRef.run = { spec, hooks };
          return `job-${++counter}`;
        },
      },
      config: { pinnedProjectsDir: projectsDir, logMaxBytes: 65536 },
      requireRemoteAccess: async () => {},
    }),
  };
}

try {
  const localRoot = join(tmp, "local");
  const projectsDir = join(tmp, "projects");
  await mkdir(join(localRoot, "a/b"), { recursive: true });
  await mkdir(projectsDir, { recursive: true });
  await writeFile(join(localRoot, "a/b/hello.py"), "print(1)\n");
  await writeFile(join(localRoot, "a/b/run.sbatch"), "#!/bin/bash\n#SBATCH --job-name=cpu_job\n#SBATCH --partition=gpus\n#SBATCH --nodelist=gpu04\n#SBATCH --nodes=1\n#SBATCH --ntasks=1\n#SBATCH --cpus-per-task=1\n#SBATCH --output=cpu_%j.out\n#SBATCH --error=cpu_%j.err\nset -euo pipefail\ncd \"$SLURM_SUBMIT_DIR\"\necho hi\n");
  await writeFile(join(localRoot, "a/b/gpu.sbatch"), "#!/bin/bash\n#SBATCH --job-name=gpu_job\n#SBATCH --partition=gpus\n#SBATCH --nodelist=gpu04\n#SBATCH --nodes=1\n#SBATCH --ntasks=1\n#SBATCH --cpus-per-task=4\n#SBATCH --output=gpu_%j.out\n#SBATCH --error=gpu_%j.err\n#SBATCH --gres=gpu:3\nset -euo pipefail\ncd \"$SLURM_SUBMIT_DIR\"\necho gpu\n");
  await writeFile(join(localRoot, "top.txt"), "x\n");
  const fileSha = async (p) => createHash("sha256").update(await readFile(p)).digest("hex");
  const stageChecksums = [
    `${await fileSha(join(localRoot, "a/b/hello.py"))}  a/b/hello.py`,
    `${await fileSha(join(localRoot, "a/b/run.sbatch"))}  a/b/run.sbatch`,
    `${await fileSha(join(localRoot, "a/b/gpu.sbatch"))}  a/b/gpu.sbatch`,
    `${await fileSha(join(localRoot, "top.txt"))}  top.txt`,
  ].join("\n") + "\n";
  await writeFile(join(projectsDir, "pinnedtest.yaml"), `schema_version: 1
project: pinnedtest
description: unit test manifest
local_root: ${localRoot}
remote_root: ${REMOTE_ROOT}
python_bin: /usr/bin/python3
files:
  - a/b/hello.py
  - a/b/run.sbatch
  - a/b/gpu.sbatch
  - top.txt
extra_dirs:
  - out
jobs:
  t-job:
    template: a/b/run.sbatch
    cpus: 1
  t-gpu-job:
    template: a/b/gpu.sbatch
    cpus: 4
    gpus: 3
    concurrency: 1
`);

  const exec = { agent: { id: "t", session: { id: "t", header: { cwd: tmp } } }, signal: new AbortController().signal };
  const defaultRemoteImpl = (target, command, inspectStdout) => {
    if (command.includes("sbatch --parsable")) return Promise.resolve({ stdout: "JOB_ID=4242\nRUN_DIR=/data01/test/pinnedtest-run\n", stderr: "", exitCode: 0 });
    if (command.includes("sha256sum -c")) return Promise.resolve({ stdout: stageChecksums, stderr: "", exitCode: 0 });
    if (command.includes("> PREPARED_SHA256.txt")) return Promise.resolve({ stdout: stageChecksums, stderr: "", exitCode: 0 });
    if (command.includes("-printf '%P")) return Promise.resolve({ stdout: inspectStdout, stderr: "", exitCode: 0 });
    return Promise.resolve({ stdout: "", stderr: "", exitCode: 0 });
  };

  // ── Happy path: fresh run dir, 3 remote rclone transfers, validation ──
  const allocationEnvelope = { target: "HPC", node: "gpu04", partition: "gpus", workloadClass: "md-build-prep", maxCpus: 32, maxGpus: 1, memGb: 96, concurrency: 1, usedCpus: 0, usedGpus: 0, policyHash: "test" };
  const state = {
    policy: { hash: "test" },
    envelope: allocationEnvelope,
    runs: [],
  };
  const latestRef = { run: null };
  const deps = makeDeps({ projectsDir, state, latestRef, runRemoteImpl: defaultRemoteImpl });

  // Transfer-only staging must not depend on a compute envelope. Policy,
  // transfer approval, folder access, rclone, checksums, and syntax validation
  // remain active gates.
  state.envelope = null;
  await deps.tools.stageTool.execute({ project: "pinnedtest" }, exec);
  await latestRef.run.hooks.done;
  assert.equal(state.runs.at(-1).status, "completed", `stage run must complete without an envelope: ${state.runs.at(-1).error}`);
  // Stage is a non-allocation operation: its run record requests zeros/one.
  assert.deepEqual(state.runs.at(-1).resources, { cpus: 0, gpus: 0, memGb: null, concurrency: 1 });

  // Global transfer policy (2026-08-24): every staging transfer must use
  // `rclone copyto` — never scp — with a `<remote>:<abs remote path>`
  // destination (resolved via lib/transfer.js, fail-closed per target).
  assert.equal(deps.shellCommands.length, 4, `expected 4 rclone copyto commands, got ${deps.shellCommands.length}`);
  const expectedRels = new Set(["a/b/hello.py", "a/b/run.sbatch", "a/b/gpu.sbatch", "top.txt"]);
  for (const command of deps.shellCommands) {
    assert.match(command, /^rclone copyto --retries 2 --low-level-retries 2 --contimeout 20s /u);
    const destination = command.trim().split(/\s+/u).at(-1).replace(/^'/u, "").replace(/'$/u, "");
    assert.match(destination, /^hpc:\/data01\/test\/pinnedtest-run\/[a-z0-9.\/]+$/u, `rclone destination must be a remote <remote>:<abs> path, got: ${destination}`);
    const rel = destination.slice(`hpc:${REMOTE_ROOT}/`.length);
    assert.ok(expectedRels.has(rel), `unexpected rclone destination rel: ${rel}`);
  }

  // Inspect: read-only file listing (or __NO_ROOT__) before any transfer.
  const inspect = deps.remoteCommands.find((entry) => entry.command.includes("-printf '%P"));
  assert.ok(inspect, "staging must inspect the remote run dir before transferring");
  assert.ok(inspect.command.includes(`test -e '${REMOTE_ROOT}'`), "inspect must test the exact run root");
  assert.ok(inspect.command.includes("__NO_ROOT__"), "inspect must report an absent run dir distinctly");
  // mkdir must be idempotent and create file parent dirs + extra_dirs.
  const mkdirCmd = deps.remoteCommands.find((entry) => entry.command.includes("mkdir -p"));
  assert.ok(mkdirCmd, "staging must (idempotently) mkdir the dir structure");
  assert.ok(mkdirCmd.command.includes(`'${REMOTE_ROOT}/a/b'`), "mkdir must create file parent dirs");
  assert.ok(mkdirCmd.command.includes(`'${REMOTE_ROOT}/out'`), "mkdir must create extra_dirs");

  // Validation body: checksum build + fixed clean-environment bash -n on
  // .sh/.sbatch. A manifest-selected interpreter (python_bin) is NEVER
  // executed remotely (t7 / t5#1 login-node confinement).
  const validation = deps.remoteCommands.find((entry) => entry.command.includes("> PREPARED_SHA256.txt"));
  assert.ok(validation, "validation command must build PREPARED_SHA256.txt from transferred bytes");
  assert.ok(validation.command.includes("env -i PATH=/usr/bin:/bin BASH_ENV=/dev/null /bin/bash --noprofile --norc -n -- 'a/b/run.sbatch'"), "validation must bash -n shell files in the exact fixed clean-environment form");
  assert.equal(validation.command.includes("/usr/bin/python3") || /py_compile/u.test(validation.command), false, "validation must never execute a manifest-selected interpreter on the login node");
  assert.ok(validation.command.includes("PINNED_STAGE_VALIDATION_OK"));

  // ── Job submission path: allocation still requires a current envelope. ──
  state.envelope = allocationEnvelope;
  await deps.tools.jobTool.execute({ project: "pinnedtest", operation: "t-job" }, exec);
  await latestRef.run.hooks.done;
  const jobRun = state.runs.at(-1);
  assert.equal(jobRun.status, "completed", `job run must complete: ${jobRun.error}`);
  assert.equal(jobRun.slurmJobId, "4242");
  const submit = deps.remoteCommands.find((entry) => entry.command.includes("sbatch --parsable"));
  assert.ok(submit, "submission command must use sbatch --parsable");
  assert.ok(submit.command.includes("\\$(sbatch"), "remote $(sbatch ...) must be escaped against local shell expansion");
  assert.ok(submit.command.includes("'a/b/run.sbatch'"), "submission must use the manifest-pinned template");

  // ── Job resources regression (audit 2026-08-25 P2-F3): the run record must
  // carry the manifest job spec's FULL resources (gpus + concurrency), not
  // hardcoded zeros — a wider envelope admits the gpu job spec.
  const state5 = { policy: state.policy, envelope: { ...state.envelope, maxGpus: 4, concurrency: 4 }, runs: [] };
  const latestRef5 = { run: null };
  const deps5 = makeDeps({ projectsDir, state: state5, latestRef: latestRef5, runRemoteImpl: defaultRemoteImpl });
  await deps5.tools.jobTool.execute({ project: "pinnedtest", operation: "t-gpu-job" }, exec);
  await latestRef5.run.hooks.done;
  const gpuJobRun = state5.runs.at(-1);
  assert.equal(gpuJobRun.status, "completed", `gpu job run must complete: ${gpuJobRun.error}`);
  assert.deepEqual(gpuJobRun.resources, { cpus: 4, gpus: 3, memGb: null, concurrency: 1 });

  // ── Resume: existing files are a subset of the manifest → safe re-stage ──
  const state3 = { policy: state.policy, envelope: state.envelope, runs: [] };
  const latestRef3 = { run: null };
  const deps3 = makeDeps({ projectsDir, state: state3, latestRef: latestRef3, inspectStdout: "top.txt\n", runRemoteImpl: defaultRemoteImpl });
  await deps3.tools.stageTool.execute({ project: "pinnedtest" }, exec);
  await latestRef3.run.hooks.done;
  assert.equal(state3.runs.at(-1).status, "completed", `resume over own partial stage must complete: ${state3.runs.at(-1).error}`);
  assert.equal(deps3.shellCommands.length, 4, "resume must re-transfer ALL files (idempotent overwrite)");

  // ── Re-stage over OWN artifacts (audit 2026-08-25 P2-F1): a previously
  // successful stage leaves PREPARED_SHA256.txt behind and jobs write outputs
  // under extra_dirs; re-staging over those must be accepted (they are owned
  // by this project), re-transferring every manifest file idempotently. ──
  const state4 = { policy: state.policy, envelope: state.envelope, runs: [] };
  const latestRef4 = { run: null };
  const deps4 = makeDeps({
    projectsDir, state: state4, latestRef: latestRef4,
    inspectStdout: "top.txt\nPREPARED_SHA256.txt\nout/inventory.json\nout/reports/inventory.md\n",
    runRemoteImpl: defaultRemoteImpl,
  });
  await deps4.tools.stageTool.execute({ project: "pinnedtest" }, exec);
  await latestRef4.run.hooks.done;
  assert.equal(state4.runs.at(-1).status, "completed", `re-stage over own receipt + extra_dirs outputs must complete: ${state4.runs.at(-1).error}`);
  assert.equal(deps4.shellCommands.length, 4, "re-stage must re-transfer ALL manifest files (idempotent overwrite)");

  // ── Fail closed: foreign content → refuse, no transfer. Foreign includes
  // plain unknown files, prefix traps ("outer/…" is NOT under extra_dir
  // "out"), and a FILE squatting exactly on an extra_dir path. ──
  const state2 = { policy: state.policy, envelope: state.envelope, runs: [] };
  const latestRef2 = { run: null };
  const deps2 = makeDeps({ projectsDir, state: state2, latestRef: latestRef2, inspectStdout: "top.txt\ntroll.bin\nouter/troll.bin\nout\n", runRemoteImpl: defaultRemoteImpl });
  await deps2.tools.stageTool.execute({ project: "pinnedtest" }, exec);
  await latestRef2.run.hooks.done;
  assert.equal(state2.runs.at(-1).status, "failed", "unexpected foreign file must fail the stage");
  assert.match(state2.runs.at(-1).error, /refusing to stage over them/u);
  const refused = state2.runs.at(-1).error.split("refusing to stage over them: ")[1].split(", ");
  assert.deepEqual(refused, ["troll.bin", "outer/troll.bin", "out"], "error must name exactly the foreign files");
  assert.equal(deps2.shellCommands.length, 0, "no transfer may happen after the fail-closed decision");

  // Global no-scp invariant: every shell command emitted by staging across all
  // scenarios must be an rclone copyto — scp is never a legal transfer path.
  for (const command of [...deps.shellCommands, ...deps3.shellCommands, ...deps4.shellCommands, ...deps2.shellCommands]) {
    assert.match(command, /^rclone copyto /u, `all transfers must use rclone copyto, got: ${command.slice(0, 60)}`);
    assert.ok(!/\bscp\b/u.test(command), `scp must never appear in transfer commands: ${command.slice(0, 60)}`);
  }

  console.log("pinned-stage unit tests passed");
} finally {
  await rm(tmp, { recursive: true, force: true });
}
