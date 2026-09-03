// Local-only, fail-closed validation for compiled schema-v2 Slurm wrappers.
// The accepted matrix intentionally mirrors the active Genbio policy validator,
// rather than attempting to accept Slurm's full command-line grammar.

import { spawnSync } from "node:child_process";

const SAFE_JOB_NAME_RE = /^[A-Za-z0-9_.-]{1,128}$/u;
const POSITIVE_INTEGER_RE = /^[1-9][0-9]*$/u;
const DIRECTIVE_NAMES = new Set([
  "job-name", "partition", "nodelist", "nodes", "ntasks",
  "ntasks-per-node", "cpus-per-task", "output", "error", "gres",
]);
const RESOURCE_SRUN_OPTIONS = /(?:^|\s)(?:-[cNn]|--(?:cpus-per-task|gpus|gpus-per-node|gres|mem|mem-per-cpu|nodes|ntasks|ntasks-per-node|partition|nodelist|node-list))(?:[=\s]|$)/u;
// Unresolved-template markers: no placeholder may survive into a submission
// (t5#5). Matched on the whole template text, including comments, because a
// leftover __TOKEN__/PREPARED_ONLY/TODO anywhere means the template is not
// finished, not merely imperfectly commented.
const PLACEHOLDER_RE = /__[A-Z0-9_]+__|\bPREPARED_ONLY\b|\bTODO\b/u;
// Log-path directives must be collision-safe relative filenames (no directory
// components, no traversal) so later status tails can never escape the run dir.
const SAFE_LOG_PATH_RE = /^[A-Za-z0-9_.%-]{1,128}$/u;

// Lexically blank out single/double-quoted spans and trailing comments so the
// token scan below only fires on executable text (quoted data and # comments
// may legitimately mention sbatch/srun, e.g. ".sbatch consumption" notes in
// real-world batch templates).
function stripQuotedAndCommented(line) {
  let out = "";
  let quote = null;
  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];
    if (quote) {
      if (char === "\\" && quote === '"') { index += 1; continue; }
      if (char === quote) quote = null;
      continue;
    }
    if (char === "'" || char === '"') { quote = char; out += " "; continue; }
    if (char === "\\") { index += 1; out += "  "; continue; }
    if (char === "#") break;
    out += char;
  }
  return out;
}

function positiveInteger(value, name, lineNumber) {
  if (!POSITIVE_INTEGER_RE.test(value)) throw new Error(`SBATCH policy: --${name} must be a positive integer (line ${lineNumber})`);
  const number = Number(value);
  if (!Number.isSafeInteger(number)) throw new Error(`SBATCH policy: --${name} is too large (line ${lineNumber})`);
  return number;
}

function parseDirective(line, lineNumber, options) {
  if (!line.startsWith("#SBATCH ")) throw new Error(`SBATCH policy: #SBATCH directives must start in column zero and use one space (line ${lineNumber})`);
  const body = line.slice("#SBATCH ".length);
  if (!body.startsWith("--")) throw new Error(`SBATCH policy: only long-form #SBATCH options are supported (line ${lineNumber})`);
  const token = body.slice(2);
  const equal = token.indexOf("=");
  let name;
  let value;
  if (equal >= 0) { name = token.slice(0, equal); value = token.slice(equal + 1); }
  else { const match = /^(\S+)\s+(\S+)$/u.exec(token); name = match?.[1] ?? ""; value = match?.[2] ?? ""; }
  if (!/^[a-z][a-z0-9-]*$/u.test(name) || !value || /[\s'"\0]/u.test(value)) throw new Error(`SBATCH policy: malformed or ambiguous directive on line ${lineNumber}`);
  if (!DIRECTIVE_NAMES.has(name)) throw new Error(`SBATCH policy: --${name} is not allowlisted for project jobs (line ${lineNumber})`);
  if (options.has(name)) throw new Error(`SBATCH policy: duplicate --${name} directive (line ${lineNumber})`);
  options.set(name, { value, lineNumber });
}

export function parseSbatchHeader(text) {
  if (typeof text !== "string" || text.length === 0) throw new Error("SBATCH policy: template must be non-empty text");
  if (text.includes("\0")) throw new Error("SBATCH policy: template contains a NUL byte");
  const lines = text.replace(/^\uFEFF/u, "").split(/\r?\n/u);
  if (lines[0] !== "#!/bin/bash") throw new Error("SBATCH policy: first line must be exactly #!/bin/bash");
  const options = new Map();
  let bodyStart = lines.length;
  for (let index = 1; index < lines.length; index += 1) {
    const line = lines[index];
    if (line.startsWith("#SBATCH")) { parseDirective(line, index + 1, options); continue; }
    if (line.trim() === "" || (line.startsWith("#") && !line.startsWith("#SBATCH"))) continue;
    bodyStart = index;
    break;
  }
  // Only the contiguous header block immediately after the shebang is parsed as
  // scheduler metadata. Slurm stops interpreting directives after the first
  // executable body line, so later column-zero "#SBATCH" text (including a
  // Python triple-quoted fixture) has no scheduler effect and is body data.
  // Indented directive-looking lines remain rejected because they commonly
  // indicate a malformed header whose intended constraint was lost.
  for (let index = bodyStart; index < lines.length; index += 1) if (/^\s+#SBATCH/u.test(lines[index])) throw new Error(`SBATCH policy: #SBATCH directives must start in column zero (line ${index + 1})`);
  return { options, bodyLines: lines.slice(bodyStart), bodyStartLine: bodyStart + 1 };
}

function required(options, name) {
  const entry = options.get(name);
  if (!entry) throw new Error(`SBATCH policy: required --${name} directive is missing`);
  return entry;
}

// Fixed local syntax gate (t5#5): the validated bytes must parse under the
// policy-owned bash binary with a cleared environment before any side effect.
// Never uses an interpreter path from the manifest; fails closed when the
// local bash is unavailable or the template does not parse.
function localBashSyntaxOk(text) {
  let result;
  try {
    result = spawnSync("/bin/bash", ["--noprofile", "--norc", "-n", "-"], { input: text, env: { PATH: "/usr/bin:/bin", BASH_ENV: "/dev/null" }, timeout: 5000, encoding: "utf8" });
  } catch (error) {
    throw new Error(`SBATCH policy: local bash syntax check failed closed: ${String(error?.message ?? error)}`);
  }
  if (result.error) throw new Error(`SBATCH policy: local bash syntax check unavailable (fail closed): ${String(result.error.code ?? result.error.message)}`);
  if (result.status !== 0) {
    const firstLine = String(result.stderr ?? "").trim().split(/\r?\n/u)[0] ?? "";
    throw new Error(`SBATCH policy: template fails local bash syntax check: ${firstLine || `exit ${result.status}`}`);
  }
}

function validateBody(bodyLines, bodyStartLine) {
  let strictMode = false;
  let submitDir = false;
  for (let index = 0; index < bodyLines.length; index += 1) {
    const line = bodyLines[index];
    const trimmed = line.trim();
    if (trimmed === "set -euo pipefail") strictMode = true;
    if (trimmed === 'cd "$SLURM_SUBMIT_DIR"') submitDir = true;
    // Token scan on executable text only (t5#5): rejects sbatch/salloc in any
    // position — including command substitutions, backticks, and command/env/if
    // wrappers the old anchored regex missed — while still tolerating the words
    // inside quoted data or comments.
    const executable = stripQuotedAndCommented(line);
    if (/\b(?:sbatch|salloc)\b/u.test(executable)) throw new Error(`SBATCH policy: nested sbatch/salloc is forbidden (line ${bodyStartLine + index})`);
    if (/\bsrun\b/u.test(executable)) {
      if (/\$\(|`/u.test(executable)) throw new Error(`SBATCH policy: srun may not appear inside a command substitution (line ${bodyStartLine + index})`);
      // srun is only allowed as a plain command token (statement start or
      // after ; & |) — `command srun`, `env srun`, `x=srun` and similar
      // wrappers/assignments are rejected outright.
      if (!/(?:^|[;&|]\s*)srun(?:\s|$)/u.test(executable.replace(/^\s+/u, ""))) throw new Error(`SBATCH policy: srun is only allowed as a plain command (line ${bodyStartLine + index})`);
      const srun = /(?:^|[;&|]\s*)srun(?:\s+([^;&|]*))?/u.exec(executable);
      if (srun && RESOURCE_SRUN_OPTIONS.test(srun[1] ?? "")) throw new Error(`SBATCH policy: resource-changing srun is forbidden (line ${bodyStartLine + index})`);
    }
  }
  if (!strictMode) throw new Error("SBATCH policy: body must contain exactly set -euo pipefail");
  if (!submitDir) throw new Error('SBATCH policy: body must contain exactly cd "$SLURM_SUBMIT_DIR"');
}

export function validatePinnedSbatch(text, jobSpec, context = {}) {
  if (!jobSpec || !Number.isInteger(jobSpec.cpus) || !Number.isInteger(jobSpec.gpus) || !Number.isInteger(jobSpec.concurrency)) throw new Error("SBATCH policy: invalid project job resource specification");
  const { options, bodyLines, bodyStartLine } = parseSbatchHeader(text);
  const jobName = required(options, "job-name").value;
  if (!SAFE_JOB_NAME_RE.test(jobName)) throw new Error("SBATCH policy: --job-name contains unsafe characters or is too long");
  const partition = required(options, "partition").value;
  const node = required(options, "nodelist").value;
  const allowlist = context.policy?.targets?.HPC?.allowlist;
  if (!allowlist || typeof allowlist !== "object") throw new Error("SBATCH policy: missing HPC allowlist in policy");
  const nodeEntry = allowlist[node];
  if (!nodeEntry || nodeEntry.partition !== partition) throw new Error(`SBATCH policy: node ${node} with partition ${partition} is not in the policy allowlist`);
  // Validate node against manifest-declared nodes (if present) or policy default
  const manifestNodes = jobSpec.nodes ?? null;
  if (manifestNodes !== null) {
    if (!manifestNodes.includes(node)) throw new Error(`SBATCH policy: template node ${node} is not in the manifest's declared nodes [${manifestNodes.join(", ")}]`);
  } else {
    const fallback = context.policy?.targets?.HPC?.test_gate?.real_submission;
    if (typeof fallback !== "string" || node !== fallback) throw new Error(`SBATCH policy: manifest omits node and template node ${node} does not match policy default ${fallback}`);
  }
  const nodesEntry = required(options, "nodes");
  const nodes = positiveInteger(nodesEntry.value, "nodes", nodesEntry.lineNumber);
  if (nodes !== 1) throw new Error("SBATCH policy: project submission requires exactly one node");
  const taskNames = ["ntasks", "ntasks-per-node"].filter((name) => options.has(name));
  if (taskNames.length !== 1) throw new Error("SBATCH policy: require exactly one of --ntasks or --ntasks-per-node");
  const taskEntry = options.get(taskNames[0]);
  const ntasks = positiveInteger(taskEntry.value, taskNames[0], taskEntry.lineNumber);
  const cpuEntry = required(options, "cpus-per-task");
  const cpusPerTask = positiveInteger(cpuEntry.value, "cpus-per-task", cpuEntry.lineNumber);
  const output = required(options, "output").value;
  const error = required(options, "error").value;
  if (output === error || !output.includes("%j") || !error.includes("%j")) throw new Error("SBATCH policy: distinct --output/--error paths must each contain %j");
  let gpus = 0;
  if (options.has("gres")) {
    const entry = options.get("gres");
    const match = /^gpu:([1-9][0-9]*)$/u.exec(entry.value);
    if (!match) throw new Error(`SBATCH policy: --gres must use exactly gpu:N (line ${entry.lineNumber})`);
    gpus = positiveInteger(match[1], "gres GPU count", entry.lineNumber);
  }
  validateBody(bodyLines, bodyStartLine);
  const cpus = nodes * ntasks * cpusPerTask;
  const concurrency = 1;
  if (cpus !== jobSpec.cpus) throw new Error(`SBATCH policy: template requests ${cpus} aggregate CPUs but manifest declares ${jobSpec.cpus}`);
  if (gpus !== jobSpec.gpus) throw new Error(`SBATCH policy: template requests ${gpus} GPUs but manifest declares ${jobSpec.gpus}`);
  if (concurrency !== jobSpec.concurrency) throw new Error(`SBATCH policy: template concurrency is ${concurrency} but manifest declares ${jobSpec.concurrency}`);
  const envelope = context.envelope;
  if (!envelope || envelope.target !== "HPC" || envelope.node !== node || envelope.partition !== partition) throw new Error(`SBATCH policy: session envelope does not match template node ${node}/${partition}`);
  if (cpus > envelope.maxCpus || gpus > envelope.maxGpus || concurrency > envelope.concurrency) throw new Error("SBATCH policy: parsed aggregate resources exceed the active session envelope");
  return Object.freeze({ jobName, partition, node, nodes, taskDirective: taskNames[0], ntasks, cpusPerTask, cpus, gpus, concurrency, output, error });
}

// Direct genbioh100 workloads use the CPU/GPU class concurrency limits from the
// target policy. Missing or malformed caps fail closed to one workload.
function positiveCap(value, fallback) {
  return Number.isInteger(value) && value >= 1 ? value : fallback;
}
export function genbioh100ConcurrencyCap(policy, gpus) {
  const limits = policy?.targets?.genbioh100?.limits;
  if (!limits || typeof limits !== "object") throw new Error("genbioh100 policy must define limits");
  const gpuCap = positiveCap(limits.concurrent_gpu_jobs, 1);
  const cpuCap = positiveCap(limits.concurrent_cpu_jobs, 1);
  return gpus > 0 ? gpuCap : cpuCap;
}
