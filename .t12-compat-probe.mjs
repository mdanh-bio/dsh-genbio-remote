// t12 local-only schema-v2 compatibility probe (no remote/network operations).
// Resolves every declarative recipe through the same compiler and Slurm policy
// validator used by genbio_project_plan.
//
// Usage: node .t12-compat-probe.mjs <manifest.yaml> [overrideRoot] [policyPath]
//   overrideRoot replaces manifest.local_root when probing a staged copy.
//   policyPath defaults to the installed Genbio compute policy under $HOME.
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { load } from "js-yaml";
import { parseProjectManifest, resolveRecipe } from "./lib/project.js";
import { validatePinnedSbatch } from "./lib/slurm-policy.js";

const HOME = process.env.HOME || "/Users/mdanh";
const DEFAULT_POLICY_PATH = join(HOME, ".codex/skills/operate-genbio-hpc-remote/references/genbio-compute-policy.yaml");

const manifestPath = process.argv[2];
const overrideRoot = process.argv[3] ?? null;
const policyPath = process.argv[4] ?? DEFAULT_POLICY_PATH;
if (!manifestPath) {
  console.error("usage: node .t12-compat-probe.mjs <manifest.yaml> [overrideRoot] [policyPath]");
  process.exit(2);
}

const parsedManifest = await load(await readFile(manifestPath, "utf8"));
const parsedPolicy = await load(await readFile(policyPath, "utf8"));
if (overrideRoot !== null) parsedManifest.local_root = overrideRoot;
const manifest = parseProjectManifest(parsedManifest.project, parsedManifest);

function probeParameters(job) {
  const values = {};
  for (const [name, spec] of Object.entries(job.recipe.parameters)) {
    if (spec.default !== undefined) values[name] = spec.default;
    else if (spec.type === "enum") values[name] = spec.values[0];
    else if (spec.type === "integer") values[name] = spec.min;
    else if (spec.type === "boolean") values[name] = false;
    else if (spec.type === "path") values[name] = job.recipe.script;
  }
  return values;
}

let pass = 0;
const rows = [];
for (const [name, job] of Object.entries(manifest.jobs)) {
  const node = job.nodes?.[0] ?? parsedPolicy?.targets?.HPC?.test_gate?.real_submission;
  const partition = parsedPolicy?.targets?.HPC?.allowlist?.[node]?.partition;
  const envelope = { target: "HPC", node, partition, maxCpus: 128, maxGpus: 8, concurrency: 8 };
  try {
    const resolution = resolveRecipe({ manifest, operation: name, parameters: probeParameters(job), policy: parsedPolicy, envelope });
    validatePinnedSbatch(resolution.sbatchText, job, { policy: parsedPolicy, envelope });
    pass += 1;
    rows.push([name, "PASS", `${node}/${partition}`]);
  } catch (error) {
    rows.push([name, "FAIL", error.message]);
  }
}

for (const [name, verdict, detail] of rows) console.log(`${verdict}  ${name}${detail ? `  →  ${detail}` : ""}`);
console.log(`\n${pass}/${rows.length} pass`);
process.exit(pass === rows.length ? 0 : 1);
