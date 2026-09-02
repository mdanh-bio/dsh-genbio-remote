import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { createPinnedTools } from "../lib/pinned.js";

const REMOTE_ROOT = "/data01/test/safety-run";
const exec = { agent: { id: "safety", session: { id: "safety", header: { cwd: "/tmp" } } }, signal: new AbortController().signal };

function harness({ projectsDir, state, policyGate = "gpu04", remoteImpl } = {}) {
  const calls = { access: [], jobs: [], remote: [], shell: [] };
  let latest;
  const tools = createPinnedTools({
    makeTool: (name, description, parameters, execute) => ({ name, description, parameters, execute }),
    requirePolicy: () => ({ targets: { HPC: { test_gate: { real_submission: policyGate }, allowlist: { gpu04: { partition: "gpus" } } } } }),
    requireState: () => state,
    publicState: () => ({}),
    requireRemoteAccess: async (...args) => { calls.access.push(args); },
    runRemote: async (target, command) => {
      calls.remote.push({ target, command });
      return remoteImpl?.(target, command) ?? { stdout: "", stderr: "", exitCode: 0 };
    },
    shell: {
      resolve: (request) => request,
      run: async (request) => {
        calls.shell.push(request.command);
        return { stdout: { text: "" }, stderr: { text: "" }, exitCode: 0, signal: null, timedOut: false };
      },
    },
    userQuestions: { ask: async () => ({ answers: [] }) },
    jobs: {
      start(spec) {
        calls.jobs.push(spec);
        const hooks = spec.run();
        latest = hooks;
        return `job-${calls.jobs.length}`;
      },
    },
    config: { pinnedProjectsDir: projectsDir, logMaxBytes: 65536 },
  });
  return { tools, calls, latest: () => latest };
}

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), "pinned-safety-"));
  const localRoot = join(root, "local");
  const projectsDir = join(root, "projects");
  await mkdir(localRoot, { recursive: true });
  await mkdir(projectsDir, { recursive: true });
  t.after(() => rm(root, { recursive: true, force: true }));
  return { root, localRoot, projectsDir };
}

async function writeProject({ localRoot, projectsDir }, batch, { cpus = 4, gpus = 1, concurrency = 1, template = "run.sbatch", files = ["run.sbatch"], pythonBin = null } = {}) {
  for (const rel of new Set([...files, template])) await writeFile(join(localRoot, rel), batch);
  const pythonLine = pythonBin ? `python_bin: ${pythonBin}\n` : "";
  const fileLines = files.map((rel) => `  - ${rel}`).join("\n");
  await writeFile(join(projectsDir, "safety.yaml"), `schema_version: 1\nproject: safety\nlocal_root: ${localRoot}\nremote_root: ${REMOTE_ROOT}\n${pythonLine}files:\n${fileLines}\njobs:\n  run:\n    template: ${template}\n    cpus: ${cpus}\n    gpus: ${gpus}\n    concurrency: ${concurrency}\n`);
  return createHash("sha256").update(await readFile(join(localRoot, template))).digest("hex");
}

function assertNoSideEffects(calls, label) {
  assert.deepEqual({ access: calls.access.length, jobs: calls.jobs.length, remote: calls.remote.length, shell: calls.shell.length }, { access: 0, jobs: 0, remote: 0, shell: 0 }, label);
}

function state(overrides = {}) {
  return {
    policy: { hash: "test" }, runs: [],
    envelope: { target: "HPC", node: "gpu04", partition: "gpus", maxCpus: 8, maxGpus: 2, concurrency: 1 },
    ...overrides,
  };
}

const valid = "#!/bin/bash\n#SBATCH --job-name=safety\n#SBATCH --partition=gpus\n#SBATCH --nodelist=gpu04\n#SBATCH --nodes=1\n#SBATCH --ntasks=1\n#SBATCH --cpus-per-task=4\n#SBATCH --output=safety_%j.out\n#SBATCH --error=safety_%j.err\n#SBATCH --gres=gpu:1\nset -euo pipefail\ncd \"$SLURM_SUBMIT_DIR\"\necho ok\n";

test("every invalid Phase-0 template fails before access/jobs/shell/remote", async (t) => {
  const cases = [
    ["wrong node", valid.replace("gpu04", "gpu05"), {}, /not in the policy allowlist/u],
    ["indented SBATCH", valid.replace("#SBATCH --job-name", " #SBATCH --job-name"), {}, /column zero|after executable/u],
    ["missing shebang", valid.replace("#!/bin/bash\n", ""), {}, /first line/u],
    ["missing job name", valid.replace("#SBATCH --job-name=safety\n", ""), {}, /required --job-name/u],
    ["unknown directive", valid.replace("#SBATCH --job-name", "#SBATCH --mail-type=ALL\n#SBATCH --job-name"), {}, /not allowlisted/u],
    ["account", valid.replace("#SBATCH --job-name", "#SBATCH --account=x\n#SBATCH --job-name"), {}, /not allowlisted/u],
    ["time", valid.replace("#SBATCH --job-name", "#SBATCH --time=01:00\n#SBATCH --job-name"), {}, /not allowlisted/u],
    ["mem", valid.replace("#SBATCH --job-name", "#SBATCH --mem=4G\n#SBATCH --job-name"), {}, /not allowlisted/u],
    ["alternate GPU", valid.replace("#SBATCH --gres=gpu:1", "#SBATCH --gpus=1"), {}, /not allowlisted/u],
    ["missing strict shell", valid.replace("set -euo pipefail\n", ""), {}, /body must/u],
    ["missing submit cd", valid.replace('cd "$SLURM_SUBMIT_DIR"\n', ""), {}, /body must/u],
    ["nested sbatch", valid.replace("echo ok", "sbatch nested.sbatch"), {}, /nested/u],
    ["nested salloc", valid.replace("echo ok", "salloc bash"), {}, /nested/u],
    ["unsafe srun", valid.replace("echo ok", "srun --cpus-per-task=8 echo bad"), {}, /resource-changing/u],
    ["template not files", valid, { files: ["payload.txt"] }, /included in files/u],
    ["template not sbatch", valid, { template: "run.sh", files: ["run.sh"] }, /end in .sbatch/u],
    ["CPU mismatch", valid, { cpus: 8 }, /aggregate CPUs/u],
    ["GPU mismatch", valid, { gpus: 2 }, /GPUs/u],
    ["concurrency mismatch", valid, { concurrency: 2 }, /concurrency|exceeds the current HPC envelope/u],
  ];
  for (const [name, batch, projectOptions, expected] of cases) await t.test(name, async (st) => {
    const fx = await fixture(st);
    await writeProject(fx, batch, projectOptions);
    const h = harness({ projectsDir: fx.projectsDir, state: state() });
    await assert.rejects(h.tools.jobTool.execute({ project: "safety", operation: "run" }, exec), expected);
    assertNoSideEffects(h.calls, `${name} must fail before any side effect`);
  });
});

test("policy and envelope denial also occur before remote access or job creation", async (t) => {
  const fx = await fixture(t);
  await writeProject(fx, valid);

  const deniedPolicy = harness({ projectsDir: fx.projectsDir, state: state(), policyGate: "disabled" });
  await assert.rejects(deniedPolicy.tools.jobTool.execute({ project: "safety", operation: "run" }, exec), /not in the policy allowlist|does not authorize|does not match policy default/u);
  assertNoSideEffects(deniedPolicy.calls, "policy denial must precede all effects");

  const deniedEnvelope = harness({ projectsDir: fx.projectsDir, state: state({ envelope: { target: "HPC", node: "cpu01", partition: "cpus", maxCpus: 8, maxGpus: 0, concurrency: 1 } }) });
  await assert.rejects(deniedEnvelope.tools.jobTool.execute({ project: "safety", operation: "run" }, exec), /require an HPC\/gpus envelope|does not match template node/u);
  assertNoSideEffects(deniedEnvelope.calls, "envelope denial must precede all effects");
});

test("valid schema-v1 job remains compatible and submits exactly once", async (t) => {
  const fx = await fixture(t);
  const sha = await writeProject(fx, valid);
  const h = harness({
    projectsDir: fx.projectsDir,
    state: state(),
    remoteImpl: (_target, command) => command.includes("sbatch --parsable")
      ? { stdout: `JOB_ID=9876\nRUN_DIR=${REMOTE_ROOT}\n`, stderr: "", exitCode: 0 }
      : { stdout: `${sha}  run.sbatch\n`, stderr: "", exitCode: 0 },
  });

  await h.tools.jobTool.execute({ project: "safety", operation: "run" }, exec);
  await h.latest().done;
  assert.equal(h.calls.access.length, 1, "one fixed-root remote grant check is expected");
  assert.equal(h.calls.jobs.length, 1, "one tracked local job is expected");
  // The gpu03 pre-submit state/headroom probe (t10 follow-up) is its OWN
  // bounded read-only remote call immediately before the submit call, so
  // probe evidence and the submit act remain separately observable and
  // parseable: exactly three remote calls (checksum verification, probe, submit).
  assert.equal(h.calls.remote.length, 3, "checksum verification, the node pre-submit probe, and the submission are each separate remote calls");
  const submissionCommands = h.calls.remote.filter(({ command }) => command.includes("sbatch --parsable"));
  assert.equal(submissionCommands.length, 1, "exactly one remote command may contain sbatch");
  assert.equal((submissionCommands[0].command.match(/sbatch --parsable/gu) ?? []).length, 1, "the command itself contains exactly one sbatch invocation");
  assert.match(submissionCommands[0].command, /cd -- '\/data01\/test\/safety-run'/u);
  assert.match(submissionCommands[0].command, /'run\.sbatch'/u);
  assert.equal(h.calls.remote.some(({ command }) => /\bsrun\b/u.test(command)), false, "login-node command path never invokes srun");
  assert.equal(h.calls.remote.some(({ command }) => /(?:^|[;&|]\s*)python(?:3)?\b/u.test(command)), false, "login-node command path never executes workload Python");
  assert.equal(h.calls.remote.some(({ command }) => /(?:^|[;&|]\s*)(?:bash|sh)\s+'?run\.sbatch/u.test(command)), false, "login node never executes the batch template directly");
  assert.equal(h.calls.shell.length, 0, "job submission compatibility path does not transfer or execute locally");
  assert.equal(h.calls.jobs[0].kind, "genbio-HPC-pinned");
});

test("verification failure prevents submission", async (t) => {
  const fx = await fixture(t);
  await writeProject(fx, valid);
  const h = harness({
    projectsDir: fx.projectsDir,
    state: state(),
    remoteImpl: () => ({ stdout: "", stderr: "checksum drift", exitCode: 1 }),
  });
  await h.tools.jobTool.execute({ project: "safety", operation: "run" }, exec);
  await h.latest().done;
  assert.equal(h.calls.remote.length, 1);
  assert.equal(h.calls.remote.some(({ command }) => command.includes("sbatch --parsable")), false, "failed package verification submits zero jobs");
  assert.equal(h.calls.jobs.length, 1, "failure remains visible in a tracked run");
});
