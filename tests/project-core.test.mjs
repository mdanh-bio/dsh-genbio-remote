import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import test from "node:test";

import { buildOperationPlan, canonicalJson, parseProjectManifest, planHashOf, posixQuote, resolveRecipe } from "../lib/project.js";
import { validatePinnedSbatch } from "../lib/slurm-policy.js";

const policy = { targets: { HPC: { test_gate: { real_submission: "gpu04" }, allowlist: { gpu04: { partition: "gpus" } }, environment: { recipe_envs: { gmx: { source: "/data01/modules/gromacs/gmxrc.sh", unset_u: true } } } } } };
const envelope = { target: "HPC", node: "gpu04", partition: "gpus", maxCpus: 16, maxGpus: 2, concurrency: 1 };
const base = {
  schema_version: 2,
  project: "demo",
  local_root: "/tmp/demo",
  remote_root: "/data01/demo",
  files: ["scripts/run.sh", "inputs/base.dat"],
  extra_dirs: ["work"],
  jobs: {
    prepare: {
      cpus: 4,
      gpus: 1,
      recipe: {
        name: "demo-prepare",
        script: "scripts/run.sh",
        env: "gmx",
        parameters: {
          arm: { type: "enum", values: ["ext 100", "ext'1000"] },
          count: { type: "integer", min: 1, max: 20 },
          enabled: { type: "boolean", default: true },
          input: { type: "path" },
        },
        argv: ["--arm", { param: "arm" }, "--count", { param: "count" }, "--enabled", { param: "enabled" }, "--input", { param: "input" }],
      },
    },
  },
};

function manifest(overrides = {}) { return parseProjectManifest("demo", { ...base, ...overrides }); }

test("schema v2 parses a staged script recipe with four strict parameter types", () => {
  const parsed = manifest();
  assert.equal(parsed.schemaVersion, 2);
  assert.equal(parsed.jobs.prepare.recipe.script, "scripts/run.sh");
  assert.equal(Object.isFrozen(parsed), true);
  const nullProto = Object.assign(Object.create(null), base);
  assert.equal(parseProjectManifest("demo", nullProto).schemaVersion, 2, "safe null-prototype YAML mappings are accepted");
});

test("schema v2 is mandatory and raw templates are rejected", () => {
  assert.throws(() => manifest({ schema_version: 1 }), /schema_version 2 is required/u);
  assert.throws(() => manifest({ jobs: { prepare: { cpus: 4, template: "job.sbatch" } } }), /raw templates|unknown field template/u);
});

test("schema v2 rejects arbitrary shell surfaces and unsafe recipes", () => {
  assert.throws(() => manifest({ jobs: { prepare: { cpus: 4, recipe: { name: "x", script: "scripts/run.sh", run: "rm -rf /" } } } }), /unknown field run/u);
  assert.throws(() => manifest({ jobs: { prepare: { cpus: 4, recipe: { name: "x", script: "missing.sh", argv: [] } } } }), /included in files/u);
  assert.throws(() => manifest({ python_bin: "/evil/python" }), /unknown field python_bin/u);
  assert.throws(() => manifest({ remote_root: "/tmp/evil';touch-pwned;'" }), /shell-inert absolute path/u);
  assert.throws(() => manifest({ workflows: {} }), /unknown field workflows/u);
  assert.throws(() => manifest({ files: ["scripts/run.sh", "scripts/run.sh"] }), /duplicate/u);
});

test("typed parameters are not coerced and paths reject traversal", () => {
  const parsed = manifest();
  assert.throws(() => resolveRecipe({ manifest: parsed, operation: "prepare", parameters: { arm: "ext 100", count: "4", input: "inputs/base.dat" }, policy, envelope }), /must be an integer/u);
  assert.throws(() => resolveRecipe({ manifest: parsed, operation: "prepare", parameters: { arm: "ext 100", count: 4, enabled: "true", input: "inputs/base.dat" }, policy, envelope }), /must be boolean/u);
  assert.throws(() => resolveRecipe({ manifest: parsed, operation: "prepare", parameters: { arm: "wrong", count: 4, input: "inputs/base.dat" }, policy, envelope }), /must be one of/u);
  assert.throws(() => resolveRecipe({ manifest: parsed, operation: "prepare", parameters: { arm: "ext 100", count: 4, input: "../escape" }, policy, envelope }), /unsafe path segment/u);
});

test("POSIX serializer round-trips spaces, quotes, and metacharacters as one inert argv", () => {
  for (const value of ["plain", "space value", "a'b", "$(touch /tmp/pwn); | & < > * ? [x]"]) {
    const output = execFileSync("/bin/bash", ["--noprofile", "--norc", "-c", `printf '%s' ${posixQuote(value)}`], { encoding: "utf8", env: { PATH: "/usr/bin:/bin", BASH_ENV: "/dev/null" } });
    assert.equal(output, value);
  }
  assert.throws(() => posixQuote("line\nbreak"), /control characters/u);
  assert.throws(() => posixQuote("nul\0byte"), /control characters/u);
  assert.throws(() => posixQuote("tab\tvalue"), /control characters/u);
});

test("compiled recipe is deterministic, invokes exactly one staged script, and passes shared policy", () => {
  const parsed = manifest();
  const args = { arm: "ext'1000", count: 4, input: "inputs/base.dat" };
  const a = resolveRecipe({ manifest: parsed, operation: "prepare", parameters: args, policy, envelope });
  const b = resolveRecipe({ manifest: parsed, operation: "prepare", parameters: { input: "inputs/base.dat", count: 4, arm: "ext'1000" }, policy, envelope });
  assert.equal(a.sbatchText, b.sbatchText);
  assert.equal(a.bytesSha, b.bytesSha);
  assert.equal((a.sbatchText.match(/'scripts\/run\.sh'/gu) ?? []).length, 1);
  assert.doesNotMatch(a.sbatchText, /\beval\b|bash -c|sh -c|\$\(|`/u);
  assert.match(a.sbatchText, /'ext'"'"'1000'/u);
  const plan = validatePinnedSbatch(a.sbatchText, parsed.jobs.prepare, { policy, envelope });
  assert.equal(plan.cpus, 4);
  assert.equal(plan.gpus, 1);
});

test("policy environment is closed and shell-inert", () => {
  const parsed = manifest();
  const args = { arm: "ext 100", count: 4, input: "inputs/base.dat" };
  assert.throws(() => resolveRecipe({ manifest: parsed, operation: "prepare", parameters: args, policy: { targets: { HPC: { test_gate: { real_submission: "gpu04" }, allowlist: { gpu04: { partition: "gpus" } }, environment: { recipe_envs: {} } } } }, envelope }), /unknown policy recipe environment/u);
  const bad = structuredClone(policy); bad.targets.HPC.environment.recipe_envs.gmx.source = "/data01/gmx;touch-pwn";
  assert.throws(() => resolveRecipe({ manifest: parsed, operation: "prepare", parameters: args, policy: bad, envelope }), /unsafe source path/u);
});

test("schema strictness covers unsafe keys, numeric overflow, and invalid control data", () => {
  assert.throws(() => manifest({ jobs: { prepare: { cpus: Number.MAX_SAFE_INTEGER, recipe: base.jobs.prepare.recipe } } }), /cpus must be an integer/u);
  assert.throws(() => manifest({ jobs: { prepare: { cpus: 4, gpus: Infinity, recipe: base.jobs.prepare.recipe } } }), /gpus must be an integer/u);
  assert.throws(() => manifest({ jobs: { prepare: { cpus: 4, recipe: { ...base.jobs.prepare.recipe, argv: ["tab\tvalue"] } } } }), /literal is invalid/u);
  assert.throws(() => manifest({ jobs: { prepare: { cpus: 4, recipe: { ...base.jobs.prepare.recipe, parameters: { bad: { type: "string" } } } } } }), /type must be enum, integer, boolean, or path/u);
  assert.throws(() => manifest({ jobs: { prepare: { cpus: 4, recipe: { ...base.jobs.prepare.recipe, argv: [{ param: "arm", extra: true }] } } } }), /unknown field extra/u);
});

test("canonical plan hashes are key-order stable and material-change sensitive", () => {
  assert.equal(canonicalJson({ z: 1, a: { y: 2, x: 3 } }), canonicalJson({ a: { x: 3, y: 2 }, z: 1 }));
  assert.equal(planHashOf({ b: 2, a: 1 }), planHashOf({ a: 1, b: 2 }));
  assert.notEqual(planHashOf({ a: 1 }), planHashOf({ a: 2 }));
  const parsed = manifest();
  const resolution = resolveRecipe({ manifest: parsed, operation: "prepare", parameters: { arm: "ext 100", count: 4, input: "inputs/base.dat" }, policy, envelope });
  const built = buildOperationPlan({ project: "demo", operation: "prepare", policyHash: "a".repeat(64), manifestSha: "b".repeat(64), packageSha: "c".repeat(64), resolution });
  assert.equal(built.planHash, planHashOf(built.plan));
  assert.equal(built.plan.schema, "genbio-plan/2");
  assert.equal(built.plan.target, "HPC");
  assert.equal(built.plan.packageSha, "c".repeat(64));
  assert.equal(Object.isFrozen(built.plan), true);
});
