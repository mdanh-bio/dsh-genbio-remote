import { createHash } from "node:crypto";

const SAFE_NAME_RE = /^[a-z0-9][a-z0-9-]*$/u;
const SAFE_JOB_NAME_RE = /^[A-Za-z0-9_.-]{1,30}$/u;
const SAFE_PATH_SEGMENT_RE = /^[A-Za-z0-9_.-]+$/u;
const SAFE_ENV_SOURCE_RE = /^\/[A-Za-z0-9_./:=+@,-]+$/u;
const SAFE_NODE_RE = /^[a-z0-9][a-z0-9-]*$/u;
const MAX_NODES = 8;
const PARAM_NAME_RE = /^[a-z][a-z0-9_]{0,31}$/u;
const MAX_FILES = 64;
const MAX_JOBS = 32;
const MAX_EXTRA_DIRS = 32;
const MAX_FETCH_FILES = 32;
const MAX_FETCH_TOTAL_BYTES = 50 * 1024 * 1024;
const MAX_ARGV = 64;
const MAX_LITERAL_CHARS = 512;
const MAX_COMPILED_BYTES = 16 * 1024;
const CONTROL_RE = /[\u0000-\u001f\u007f]/u;

function sha256(data) { return createHash("sha256").update(data).digest("hex"); }
function assertNoControls(value, label) { if (CONTROL_RE.test(value)) throw new Error(`${label} may not contain control characters`); }
function plainObject(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}
function assertKeys(value, allowed, label) {
  for (const key of Object.keys(value)) if (!allowed.has(key)) throw new Error(`${label}: unknown field ${key}`);
}
function assertAbsolutePath(value, label) {
  if (typeof value !== "string" || !/^\/[A-Za-z0-9_./:=+@,-]+$/u.test(value) || value.includes("..")) throw new Error(`${label} must be a shell-inert absolute path without traversal`);
}
export function assertProjectRelativePath(rel, label = "path") {
  if (typeof rel !== "string" || rel.length === 0 || rel.startsWith("/") || rel.includes("\0") || /[\r\n]/u.test(rel)) throw new Error(`${label} must be a non-empty relative path`);
  for (const segment of rel.split("/")) if (!SAFE_PATH_SEGMENT_RE.test(segment) || segment === "." || segment === "..") throw new Error(`${label} contains an unsafe path segment: ${rel}`);
  return rel;
}
function uniqueRelativePaths(values, label, max, { nonempty = false } = {}) {
  if (!Array.isArray(values) || (nonempty && values.length === 0) || values.length > max) throw new Error(`${label} must be ${nonempty ? "a non-empty " : "an "}array of at most ${max} relative paths`);
  const seen = new Set();
  for (const value of values) {
    assertProjectRelativePath(value, label);
    if (seen.has(value)) throw new Error(`${label} contains duplicate path ${value}`);
    seen.add(value);
  }
  return Object.freeze([...values]);
}
function integer(value, label, min, max) {
  if (!Number.isSafeInteger(value) || value < min || value > max) throw new Error(`${label} must be an integer ${min}..${max}`);
  return value;
}
function parseFetch(project, value) {
  if (value === undefined) return null;
  if (!plainObject(value)) throw new Error(`${project}: fetch must be a mapping`);
  assertKeys(value, new Set(["max_bytes", "dest", "files"]), `${project}: fetch`);
  const maxBytes = integer(value.max_bytes, `${project}: fetch.max_bytes`, 1, MAX_FETCH_TOTAL_BYTES);
  assertProjectRelativePath(value.dest, `${project}: fetch.dest`);
  const files = uniqueRelativePaths(value.files, `${project}: fetch.files`, MAX_FETCH_FILES, { nonempty: true });
  return Object.freeze({ maxBytes, dest: value.dest, files });
}
function parseResources(project, name, spec) {
  const nodes = parseNodes(project, name, spec.node);
  return {
    cpus: integer(spec.cpus, `${project}: job ${name} cpus`, 1, 128),
    gpus: integer(spec.gpus ?? 0, `${project}: job ${name} gpus`, 0, 8),
    concurrency: integer(spec.concurrency ?? 1, `${project}: job ${name} concurrency`, 1, 8),
    ...(nodes !== null ? { nodes } : {}),
  };
}
function parseNodes(project, name, value) {
  if (value === undefined) return null;
  if (typeof value === "string") {
    if (!SAFE_NODE_RE.test(value)) throw new Error(`${project}: job ${name} node must be a safe kebab-case identifier`);
    return Object.freeze([value]);
  }
  if (!Array.isArray(value) || value.length === 0 || value.length > MAX_NODES) throw new Error(`${project}: job ${name} node must be a string or array of 1..${MAX_NODES} strings`);
  const seen = new Set();
  for (const item of value) {
    if (typeof item !== "string" || !SAFE_NODE_RE.test(item)) throw new Error(`${project}: job ${name} node list contains an invalid entry`);
    if (seen.has(item)) throw new Error(`${project}: job ${name} node list contains duplicate: ${item}`);
    seen.add(item);
  }
  return Object.freeze([...value]);
}
function parseParameter(project, job, name, spec) {
  if (!PARAM_NAME_RE.test(name)) throw new Error(`${project}: job ${job} has invalid parameter name ${name}`);
  if (!plainObject(spec)) throw new Error(`${project}: job ${job} parameter ${name} must be a mapping`);
  const type = spec.type;
  if (type === "enum") {
    assertKeys(spec, new Set(["type", "values", "default"]), `${project}: job ${job} parameter ${name}`);
    if (!Array.isArray(spec.values) || spec.values.length === 0 || spec.values.length > 64 || spec.values.some((v) => typeof v !== "string" || v.length === 0 || v.length > MAX_LITERAL_CHARS || CONTROL_RE.test(v))) throw new Error(`${project}: job ${job} parameter ${name} enum values are invalid`);
    if (new Set(spec.values).size !== spec.values.length) throw new Error(`${project}: job ${job} parameter ${name} enum values must be unique`);
    if (spec.default !== undefined && !spec.values.includes(spec.default)) throw new Error(`${project}: job ${job} parameter ${name} default is not an allowed enum value`);
    return Object.freeze({ type, values: Object.freeze([...spec.values]), ...(spec.default !== undefined ? { default: spec.default } : {}) });
  }
  if (type === "integer") {
    assertKeys(spec, new Set(["type", "min", "max", "default"]), `${project}: job ${job} parameter ${name}`);
    const min = integer(spec.min, `${project}: job ${job} parameter ${name}.min`, -2147483648, 2147483647);
    const max = integer(spec.max, `${project}: job ${job} parameter ${name}.max`, -2147483648, 2147483647);
    if (min > max) throw new Error(`${project}: job ${job} parameter ${name} min exceeds max`);
    if (spec.default !== undefined && (!Number.isSafeInteger(spec.default) || spec.default < min || spec.default > max)) throw new Error(`${project}: job ${job} parameter ${name} default is out of range`);
    return Object.freeze({ type, min, max, ...(spec.default !== undefined ? { default: spec.default } : {}) });
  }
  if (type === "boolean") {
    assertKeys(spec, new Set(["type", "default"]), `${project}: job ${job} parameter ${name}`);
    if (spec.default !== undefined && typeof spec.default !== "boolean") throw new Error(`${project}: job ${job} parameter ${name} default must be boolean`);
    return Object.freeze({ type, ...(spec.default !== undefined ? { default: spec.default } : {}) });
  }
  if (type === "path") {
    assertKeys(spec, new Set(["type", "default"]), `${project}: job ${job} parameter ${name}`);
    if (spec.default !== undefined) assertProjectRelativePath(spec.default, `${project}: job ${job} parameter ${name} default`);
    return Object.freeze({ type, ...(spec.default !== undefined ? { default: spec.default } : {}) });
  }
  throw new Error(`${project}: job ${job} parameter ${name} type must be enum, integer, boolean, or path`);
}
function parseArgv(project, job, argv, parameters) {
  if (!Array.isArray(argv) || argv.length > MAX_ARGV) throw new Error(`${project}: job ${job} recipe.argv must be an array of at most ${MAX_ARGV} items`);
  return Object.freeze(argv.map((item, index) => {
    if (typeof item === "string") {
      if (item.length === 0 || item.length > MAX_LITERAL_CHARS || CONTROL_RE.test(item)) throw new Error(`${project}: job ${job} recipe.argv[${index}] literal is invalid`);
      return Object.freeze({ literal: item });
    }
    if (!plainObject(item)) throw new Error(`${project}: job ${job} recipe.argv[${index}] must be a literal string or {param}`);
    assertKeys(item, new Set(["param"]), `${project}: job ${job} recipe.argv[${index}]`);
    if (typeof item.param !== "string" || !Object.hasOwn(parameters, item.param)) throw new Error(`${project}: job ${job} recipe.argv[${index}] references an unknown parameter`);
    return Object.freeze({ param: item.param });
  }));
}
function parseRecipe(project, name, value, files) {
  if (!plainObject(value)) throw new Error(`${project}: job ${name} recipe must be a mapping`);
  assertKeys(value, new Set(["name", "script", "env", "parameters", "argv"]), `${project}: job ${name} recipe`);
  if (!SAFE_JOB_NAME_RE.test(value.name ?? "")) throw new Error(`${project}: job ${name} recipe.name must use 1..30 safe characters`);
  assertProjectRelativePath(value.script, `${project}: job ${name} recipe.script`);
  if (!files.includes(value.script)) throw new Error(`${project}: job ${name} recipe.script must be included in files`);
  if (value.env !== undefined && (typeof value.env !== "string" || !SAFE_NAME_RE.test(value.env))) throw new Error(`${project}: job ${name} recipe.env must be a safe policy profile name`);
  const rawParameters = value.parameters ?? {};
  if (!plainObject(rawParameters) || Object.keys(rawParameters).length > 32) throw new Error(`${project}: job ${name} recipe.parameters must be a mapping of at most 32 entries`);
  const parameters = {};
  for (const [paramName, paramSpec] of Object.entries(rawParameters)) parameters[paramName] = parseParameter(project, name, paramName, paramSpec);
  const argv = parseArgv(project, name, value.argv ?? [], parameters);
  return Object.freeze({ name: value.name, script: value.script, env: value.env ?? null, parameters: Object.freeze(parameters), argv });
}

export function parseProjectManifest(project, parsed) {
  if (!plainObject(parsed)) throw new Error(`${project}: manifest must be a plain mapping`);
  const version = parsed.schema_version;
  if (version !== 2) throw new Error(`${project}: schema_version 2 is required; schema_version 1 is no longer supported`);
  const topKeys = new Set(["schema_version", "project", "description", "local_root", "remote_root", "files", "extra_dirs", "jobs", "fetch"]);
  assertKeys(parsed, topKeys, `${project}: manifest`);
  if (typeof parsed.project !== "string" || !SAFE_NAME_RE.test(parsed.project) || parsed.project !== project) throw new Error(`${project}: project name must be kebab-case and match the manifest filename`);
  assertAbsolutePath(parsed.local_root, `${project}: local_root`);
  assertAbsolutePath(parsed.remote_root, `${project}: remote_root`);
  const files = uniqueRelativePaths(parsed.files, `${project}: files`, MAX_FILES, { nonempty: true });
  const extraDirs = uniqueRelativePaths(parsed.extra_dirs ?? [], `${project}: extra_dirs`, MAX_EXTRA_DIRS);
  for (const rel of extraDirs) if (files.includes(rel)) throw new Error(`${project}: a path cannot appear in both files and extra_dirs: ${rel}`);
  if (!plainObject(parsed.jobs)) throw new Error(`${project}: jobs must be a mapping`);
  const names = Object.keys(parsed.jobs);
  if (names.length === 0 || names.length > MAX_JOBS) throw new Error(`${project}: jobs must define 1..${MAX_JOBS} entries`);
  const jobs = {};
  for (const [name, spec] of Object.entries(parsed.jobs)) {
    if (!SAFE_NAME_RE.test(name) || !plainObject(spec)) throw new Error(`${project}: invalid job ${name}`);
    const resources = parseResources(project, name, spec);
    assertKeys(spec, new Set(["recipe", "cpus", "gpus", "concurrency", "node"]), `${project}: job ${name}`);
    if (spec.recipe === undefined) throw new Error(`${project}: job ${name} must define a declarative recipe; raw templates are no longer supported`);
    jobs[name] = Object.freeze({ recipe: parseRecipe(project, name, spec.recipe, files), ...resources });
  }
  return Object.freeze({ schemaVersion: 2, project: parsed.project, localRoot: parsed.local_root, remoteRoot: parsed.remote_root, files, extraDirs, jobs: Object.freeze(jobs), fetch: parseFetch(project, parsed.fetch), description: typeof parsed.description === "string" ? parsed.description : "" });
}

export function posixQuote(value) {
  const text = String(value);
  assertNoControls(text, "argv values");
  return `'${text.replace(/'/gu, `'"'"'`)}'`;
}
function resolveParameter(name, spec, supplied) {
  let value = Object.hasOwn(supplied, name) ? supplied[name] : spec.default;
  if (value === undefined) throw new Error(`missing required parameter: ${name}`);
  if (spec.type === "enum") {
    if (typeof value !== "string" || !spec.values.includes(value)) throw new Error(`parameter ${name} must be one of: ${spec.values.join(", ")}`);
    return value;
  }
  if (spec.type === "integer") {
    if (!Number.isSafeInteger(value) || value < spec.min || value > spec.max) throw new Error(`parameter ${name} must be an integer ${spec.min}..${spec.max}`);
    return String(value);
  }
  if (spec.type === "boolean") {
    if (typeof value !== "boolean") throw new Error(`parameter ${name} must be boolean`);
    return value ? "true" : "false";
  }
  assertProjectRelativePath(value, `parameter ${name}`);
  return value;
}
function policyEnvironment(recipe, policy) {
  if (!recipe.env) return [];
  const profile = policy?.targets?.HPC?.environment?.recipe_envs?.[recipe.env];
  if (!plainObject(profile)) throw new Error(`unknown policy recipe environment: ${recipe.env}`);
  assertKeys(profile, new Set(["source", "unset_u"]), `policy recipe environment ${recipe.env}`);
  if (!SAFE_ENV_SOURCE_RE.test(profile.source ?? "") || profile.source.includes("..")) throw new Error(`policy recipe environment ${recipe.env} has an unsafe source path`);
  if (profile.unset_u !== undefined && typeof profile.unset_u !== "boolean") throw new Error(`policy recipe environment ${recipe.env}.unset_u must be boolean`);
  return profile.unset_u ? ["set +u", `source ${posixQuote(profile.source)}`, "set -u"] : [`source ${posixQuote(profile.source)}`];
}

export function resolveRecipe({ manifest, operation, parameters = {}, policy, envelope }) {
  if (!manifest || manifest.schemaVersion !== 2) throw new Error("recipe resolution requires a schema_version 2 manifest");
  const jobSpec = manifest.jobs?.[operation];
  if (!jobSpec?.recipe) throw new Error(`operation ${operation} is not a declarative recipe`);
  if (!plainObject(parameters)) throw new Error("parameters must be a mapping");
  for (const key of Object.keys(parameters)) if (!Object.hasOwn(jobSpec.recipe.parameters, key)) throw new Error(`unknown parameter: ${key}`);
  const values = {};
  for (const [name, spec] of Object.entries(jobSpec.recipe.parameters)) values[name] = resolveParameter(name, spec, parameters);
  const args = jobSpec.recipe.argv.map((item) => Object.hasOwn(item, "literal") ? item.literal : values[item.param]);
  const invocation = [jobSpec.recipe.script, ...args].map(posixQuote).join(" ");
  const selectedNode = selectJobNode(jobSpec, policy, envelope);
  const partition = policy?.targets?.HPC?.allowlist?.[selectedNode]?.partition;
  if (!partition) throw new Error(`no policy allowlist entry for node ${selectedNode}`);
  const lines = [
    "#!/bin/bash",
    `#SBATCH --job-name=${jobSpec.recipe.name}`,
    `#SBATCH --partition=${partition}`,
    `#SBATCH --nodelist=${selectedNode}`,
    "#SBATCH --nodes=1",
    "#SBATCH --ntasks=1",
    `#SBATCH --cpus-per-task=${jobSpec.cpus}`,
    "#SBATCH --output=%x_%j.out",
    "#SBATCH --error=%x_%j.err",
    ...(jobSpec.gpus > 0 ? [`#SBATCH --gres=gpu:${jobSpec.gpus}`] : []),
    "set -euo pipefail",
    'cd "$SLURM_SUBMIT_DIR"',
    "printf 'DSH_SLURM_FRAME=START|%s|%s|%s|%s\\n' \"${SLURM_JOB_ID-}\" \"${SLURM_JOB_NAME-}\" \"${SLURM_JOB_NODELIST-}\" \"${CUDA_VISIBLE_DEVICES-}\"",
    ...policyEnvironment(jobSpec.recipe, policy),
    invocation,
    "printf 'DSH_SLURM_FRAME=DONE|%s|%s\\n' \"${SLURM_JOB_ID-}\" \"${SLURM_JOB_NAME-}\"",
    "",
  ];
  const sbatchText = lines.join("\n");
  if (Buffer.byteLength(sbatchText) > MAX_COMPILED_BYTES) throw new Error(`compiled recipe exceeds ${MAX_COMPILED_BYTES} bytes`);
  return Object.freeze({ sbatchText, bytesSha: sha256(sbatchText), jobName: jobSpec.recipe.name, cpus: jobSpec.cpus, gpus: jobSpec.gpus, concurrency: jobSpec.concurrency, node: selectedNode, partition, script: jobSpec.recipe.script, parameters: Object.freeze(values), envelope });
}

export function selectJobNode(jobSpec, policy, envelope) {
  const allowlist = policy?.targets?.HPC?.allowlist;
  if (!allowlist || !plainObject(allowlist)) throw new Error("policy missing HPC allowlist");
  const candidates = jobSpec.nodes ?? null;
  if (candidates !== null) {
    for (const node of candidates) {
      const entry = allowlist[node];
      if (entry && typeof entry.partition === "string") {
        if (envelope && envelope.node && envelope.node !== node) continue;
        return node;
      }
    }
    if (envelope && envelope.node) throw new Error(`none of the manifest nodes [${candidates.join(", ")}] match the session envelope node ${envelope.node} with a valid policy allowlist entry`);
    throw new Error(`none of the manifest nodes [${candidates.join(", ")}] have a valid policy allowlist entry`);
  }
  const fallback = policy?.targets?.HPC?.test_gate?.real_submission;
  if (typeof fallback !== "string" || !allowlist[fallback]) throw new Error("manifest omits node and policy test_gate.real_submission is not a valid allowlist entry");
  if (envelope && envelope.node && envelope.node !== fallback) throw new Error(`policy default node ${fallback} does not match session envelope node ${envelope.node}`);
  return fallback;
}

function canonicalValue(value) {
  if (value === null || typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number") {
    if (!Number.isSafeInteger(value)) throw new Error("canonical JSON accepts only safe integers");
    return value;
  }
  if (Array.isArray(value)) return value.map(canonicalValue);
  if (!plainObject(value)) throw new Error("canonical JSON accepts only plain JSON values");
  const out = {};
  for (const key of Object.keys(value).sort()) out[key] = canonicalValue(value[key]);
  return out;
}
export function canonicalJson(value) { return JSON.stringify(canonicalValue(value)); }
export function planHashOf(value) { return sha256(canonicalJson(value)); }
export function buildOperationPlan({ project, operation, policyHash, manifestSha, packageSha, resolution }) {
  if (!/^[a-f0-9]{64}$/u.test(policyHash ?? "") || !/^[a-f0-9]{64}$/u.test(manifestSha ?? "") || !/^[a-f0-9]{64}$/u.test(packageSha ?? "")) throw new Error("plan requires full policy, manifest, and package SHA-256 values");
  const plan = Object.freeze({ schema: "genbio-plan/2", target: "HPC", project, operation, policyHash, manifestSha, packageSha, bytesSha: resolution.bytesSha, jobName: resolution.jobName, cpus: resolution.cpus, gpus: resolution.gpus, concurrency: resolution.concurrency, node: resolution.node, partition: resolution.partition, script: resolution.script, parameters: resolution.parameters });
  return Object.freeze({ plan, planHash: planHashOf(plan) });
}

export { SAFE_ENV_SOURCE_RE, SAFE_JOB_NAME_RE };
