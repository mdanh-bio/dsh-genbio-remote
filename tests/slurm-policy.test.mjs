import assert from "node:assert/strict";
import test from "node:test";
import { parseSbatchHeader, validatePinnedSbatch } from "../lib/slurm-policy.js";

const policy = { targets: { HPC: { test_gate: { real_submission: "gpu04" }, allowlist: { gpu04: { partition: "gpus" } } } } };
const envelope = { target: "HPC", node: "gpu04", partition: "gpus", maxCpus: 80, maxGpus: 4, concurrency: 1 };
const spec = { cpus: 4, gpus: 1, concurrency: 1, template: "run.sbatch" };
const valid = `#!/bin/bash
#SBATCH --job-name=safe_job
#SBATCH --partition=gpus
#SBATCH --nodelist=gpu04
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --output=safe_%j.out
#SBATCH --error=safe_%j.err
#SBATCH --gres=gpu:1
set -euo pipefail
cd "$SLURM_SUBMIT_DIR"
srun python run.py
`;

test("parser exposes strict header/body boundary contract", () => {
  const parsed = parseSbatchHeader(valid);
  assert.ok(parsed.options instanceof Map);
  assert.equal(parsed.options.get("job-name")?.value, "safe_job");
  assert.equal(parsed.bodyStartLine, 11);
  assert.deepEqual(parsed.bodyLines.slice(0, 3), ["set -euo pipefail", 'cd "$SLURM_SUBMIT_DIR"', "srun python run.py"]);
});

test("SBATCH-looking text after the real header is body data, not a directive", () => {
  const withFixture = valid.replace("srun python run.py", "python - <<'PY'\nSUBMIT_HEAD = \"\"\"\n#SBATCH --account=not-a-real-directive\n\"\"\"\nprint(SUBMIT_HEAD)\nPY");
  const parsed = parseSbatchHeader(withFixture);
  assert.equal(parsed.options.has("account"), false);
  assert.match(parsed.bodyLines.join("\n"), /#SBATCH --account=not-a-real-directive/u);
  assert.doesNotThrow(() => validatePinnedSbatch(withFixture, spec, { policy, envelope }));
});

test("exact policy matrix yields frozen aggregate plan", () => {
  const plan = validatePinnedSbatch(valid, spec, { policy, envelope });
  assert.equal(Object.isFrozen(plan), true);
  assert.deepEqual(plan, { jobName: "safe_job", partition: "gpus", node: "gpu04", nodes: 1, taskDirective: "ntasks", ntasks: 1, cpusPerTask: 4, cpus: 4, gpus: 1, concurrency: 1, output: "safe_%j.out", error: "safe_%j.err" });
});

test("ntasks-per-node is the only alternative task directive", () => {
  const text = valid.replace("--ntasks=1", "--ntasks-per-node=2").replace("--cpus-per-task=4", "--cpus-per-task=2");
  assert.equal(validatePinnedSbatch(text, spec, { policy, envelope }).taskDirective, "ntasks-per-node");
  assert.throws(() => validatePinnedSbatch(text.replace("#SBATCH --ntasks-per-node=2", "#SBATCH --ntasks=1\n#SBATCH --ntasks-per-node=1"), spec, { policy, envelope }), /exactly one/u);
});

test("requires shebang, column-zero directives, every required header/body field", () => {
  assert.throws(() => parseSbatchHeader(valid.replace("#!/bin/bash", "#!/usr/bin/env bash")), /first line/u);
  assert.throws(() => parseSbatchHeader(valid.replace("#SBATCH --job-name", " #SBATCH --job-name")), /after executable|column zero/u);
  const required = [
    "#SBATCH --job-name=safe_job\n", "#SBATCH --partition=gpus\n", "#SBATCH --nodelist=gpu04\n",
    "#SBATCH --nodes=1\n", "#SBATCH --ntasks=1\n", "#SBATCH --cpus-per-task=4\n",
    "#SBATCH --output=safe_%j.out\n", "#SBATCH --error=safe_%j.err\n",
    "set -euo pipefail\n", "cd \"$SLURM_SUBMIT_DIR\"\n",
  ];
  for (const token of required) assert.throws(() => validatePinnedSbatch(valid.replace(token, ""), spec, { policy, envelope }), /required|exactly one|body must/u, `missing ${token.trim()} must fail`);
});

test("unknown and explicitly prohibited directives fail closed", () => {
  for (const option of ["account=x", "time=1:00", "time-min=1", "mem=4G", "mem-per-cpu=1G", "exclusive=yes", "gpus=1", "array=1-2", "constraint=a100"]) {
    assert.throws(() => validatePinnedSbatch(valid.replace("#SBATCH --job-name", `#SBATCH --${option}\n#SBATCH --job-name`), spec, { policy, envelope }), /not allowlisted/u);
  }
  assert.throws(() => validatePinnedSbatch(valid.replace("--gres=gpu:1", "--gres=gpu:a100:1"), spec, { policy, envelope }), /exactly gpu:N/u);
});

test("duplicate directives and alternate GPU forms fail closed", () => {
  assert.throws(() => validatePinnedSbatch(valid.replace("#SBATCH --job-name=safe_job", "#SBATCH --job-name=safe_job\n#SBATCH --job-name=again"), spec, { policy, envelope }), /duplicate/u);
  for (const gpu of ["--gpus=1", "--gpus-per-node=1", "--gres=gpu:a100:1", "--gres=gpu:0", "--gres=cpu:1"]) {
    const text = gpu.startsWith("--gres") ? valid.replace("--gres=gpu:1", gpu) : valid.replace("#SBATCH --gres=gpu:1", `#SBATCH ${gpu}`);
    assert.throws(() => validatePinnedSbatch(text, spec, { policy, envelope }), /not allowlisted|exactly gpu:N/u);
  }
});

test("output/error, nested allocation, and resource-changing srun fail", () => {
  assert.throws(() => validatePinnedSbatch(valid.replace("safe_%j.err", "safe_%j.out"), spec, { policy, envelope }), /distinct/u);
  assert.throws(() => validatePinnedSbatch(valid.replace("safe_%j.out", "safe.out"), spec, { policy, envelope }), /contain %j/u);
  for (const body of ["sbatch other.sbatch", "salloc bash", "srun --cpus-per-task=8 python run.py", "srun -n 8 python run.py"]) assert.throws(() => validatePinnedSbatch(valid.replace("srun python run.py", body), spec, { policy, envelope }), /nested|resource-changing/u);
});

test("manifest parity, envelope fit, and policy mapping are authoritative", () => {
  assert.throws(() => validatePinnedSbatch(valid, { ...spec, cpus: 8 }, { policy, envelope }), /aggregate CPUs/u);
  assert.throws(() => validatePinnedSbatch(valid, { ...spec, gpus: 2 }, { policy, envelope }), /GPUs/u);
  assert.throws(() => validatePinnedSbatch(valid, { ...spec, concurrency: 2 }, { policy, envelope }), /concurrency/u);
  assert.throws(() => validatePinnedSbatch(valid, spec, { policy, envelope: { ...envelope, maxCpus: 3 } }), /exceed/u);
  assert.throws(() => validatePinnedSbatch(valid, spec, { policy: { targets: { HPC: { test_gate: { real_submission: "gpu04" }, allowlist: { gpu04: { partition: "wrong" } } } } }, envelope }), /not in the policy allowlist/u);
});
