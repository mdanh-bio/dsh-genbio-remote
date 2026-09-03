import { createHash, randomBytes } from "node:crypto";
import { lstat, mkdir, mkdtemp, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve, sep } from "node:path";
import { validatePinnedSbatch } from "./slurm-policy.js";
import { shellQuote } from "./shell.js";
import { resolveRcloneRemote, rcloneCopyfromCommand, rcloneCopytoCommand, runRemoteWithRetry } from "./transfer.js";
import { classifyWorkloadEvidence, parseJobOutputMarkers, parseOwnedSacctTable } from "./scheduler-evidence.js";

// Project-neutral internal execution primitives for schema-v2 project tools.
// Manifest discovery and validation live in project-source.js/project.js; this
// module accepts an already validated manifest and enforces remote safety,
// integrity, exact-once submission, tracking, scheduler evidence, cancellation,
// and bounded artifact transfers.
const SSH_OPTIONS = ["-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "StrictHostKeyChecking=yes"];

function sha256(data) { return createHash("sha256").update(data).digest("hex"); }
export function freshRunDirectoryCommand(remoteBase, remoteRunDir, target = "HPC") {
  const parent = `${remoteBase}/runs`;
  return strictRemote(target, `set -eu; mkdir -p -m 700 -- ${shellQuote(parent)}; mkdir -m 700 -- ${shellQuote(remoteRunDir)}`);
}

async function localFiles(manifest) {
  const files = []; let bytes = 0;
  for (const rel of manifest.files) {
    const path = join(manifest.localRoot, rel);
    const info = await stat(path);
    if (!info.isFile()) throw new Error(`${manifest.project}: staged path is not a regular file: ${rel}`);
    const content = await readFile(path);
    files.push({ rel, path, size: info.size, sha256: sha256(content) });
    bytes += info.size;
  }
  if (bytes === 0) throw new Error(`${manifest.project}: package has zero total bytes`);
  return { files, bytes };
}

// Policy shared_checks.global.approval-gates: material-transfer-in requires a
// separate explicit owner approval (uses the shared project staging prompt).
async function askTransfer(userQuestions, exec, project, files, bytes) {
  if (!userQuestions) throw new Error("material transfer requires the DSH user-question provider");
  const answer = await userQuestions.ask({ agent: exec.agent, signal: exec.signal, questions: [{ id: `genbio-project-transfer-${project}`, header: "Project HPC staging", question: `Project ${project} staging will transfer ${files.length} allowlisted file(s), ${bytes} logical bytes, to the fixed authoritative HPC run directory. Approve this material transfer?`, options: [{ label: "Approve this transfer", description: "Transfer only the listed allowlisted files into a fresh collision-checked run directory." }, { label: "Reject", description: "Do not transfer or allocate this stage." }] }] });
  const selected = answer.answers?.find((item) => item.id === `genbio-project-transfer-${project}`)?.selected ?? [];
  if (!selected.includes("Approve this transfer")) throw new Error(`${project}: material transfer was not explicitly approved`);
}

function validateEnvelope(state, requested) {
  const envelope = state.envelope;
  if (!envelope) throw new Error("set the HPC session envelope before running a project operation");
  if (envelope.target !== "HPC" || envelope.partition !== "gpus") throw new Error("project operations currently require an HPC/gpus envelope; other targets are not supported for project execution");
  // Policy-hash binding (t7 / t5#7): an envelope recorded under an older
  // policy generation is stale. Enforced before any side effect whenever the
  // envelope carries a hash (legacy envelopes without one predate the binding
  // and are accepted for compatibility).
  if (envelope.policyHash !== undefined && state.policy?.hash != null && envelope.policyHash !== state.policy.hash) throw new Error("envelope policy hash is stale (policy changed after the envelope was set); re-set the envelope");
  if (requested.cpus > envelope.maxCpus || requested.gpus > envelope.maxGpus || requested.concurrency > envelope.concurrency) throw new Error(`project operation (${requested.cpus} CPUs) exceeds the current HPC envelope`);
}

// Durable Slurm allocation accounting (t7 / t10, t5#3): every submission
// attempt RESERVES its FULL manifest resources the moment it starts
// (status "submitting", before any remote effect), stays reserved while the
// job is pending/running ("nonterminal"), and is released only on terminal
// scheduler evidence (genbio_project_status marks the allocation terminal
// from the sacct State) or on a definite pre-sbatch failure ("failed").
// Aggregate fit — outstanding reservations plus the requested resources must
// fit the envelope — is checked atomically at admission, before remote
// access, background jobs, or any scheduler action (the aggregate is
// recomputed from the record each time, so there is no drift). "ambiguous"
// submissions are NOT counted at admission: they are fully gated by the
// exact-once gate for that (project, operation) pair, which is strictly
// stronger than a capacity count (the pair cannot resubmit at all until the
// ambiguity is resolved). State is session-scoped, so reservations clear with
// the session; terminal evidence is the explicit release path within it.
function allocationsOf(state) {
  if (!Array.isArray(state.allocations)) state.allocations = [];
  return state.allocations;
}
function markAllocationTerminal(state, slurmJobId, stateText) {
  let found = false;
  for (const entry of allocationsOf(state)) if (entry.slurmJobId === slurmJobId && ["nonterminal", "ambiguous", "cancel-requested"].includes(entry.status)) { entry.status = "terminal"; entry.terminalState = stateText ?? null; entry.terminalAt = Date.now(); found = true; }
  return found;
}
function assertAggregateCapacity(state, requested) {
  // CPU, GPU, and concurrency are aggregate admission resources. Memory is
  // intentionally enforced as a per-job envelope maximum rather than summed
  // here because Slurm accounts each job's requested memory independently.
  const envelope = state.envelope;
  const outstanding = allocationsOf(state).filter((entry) => ["submitting", "nonterminal", "ambiguous", "cancel-requested"].includes(entry.status));
  const usedCpus = outstanding.reduce((sum, entry) => sum + entry.cpus, 0);
  const usedGpus = outstanding.reduce((sum, entry) => sum + entry.gpus, 0);
  const usedConcurrency = outstanding.reduce((sum, entry) => sum + entry.concurrency, 0);
  if (requested.cpus + usedCpus > envelope.maxCpus || requested.gpus + usedGpus > envelope.maxGpus || requested.concurrency + usedConcurrency > envelope.concurrency) {
    throw new Error(`project operation exceeds envelope capacity: aggregate of ${outstanding.length} outstanding allocation(s) (submitting/nonterminal) reserves ${usedCpus} CPU / ${usedGpus} GPU / ${usedConcurrency} concurrency, and requesting ${requested.cpus} CPU / ${requested.gpus} GPU / ${requested.concurrency} concurrency exceeds the remaining envelope (max ${envelope.maxCpus} CPU / ${envelope.maxGpus} GPU / ${envelope.concurrency} concurrency); collect terminal evidence with genbio_project_status before submitting again`);
  }
}

async function runLocal(shell, command, timeoutMs, signal) { const request = shell.resolve({ command, timeoutMs, signal }); const result = await shell.run(request); return { stdout: result.stdout?.text ?? "", stderr: result.stderr?.text ?? "", exitCode: result.exitCode ?? null, signal: result.signal ?? null, timedOut: result.timedOut === true }; }
function parseChecksums(stdout) { const values = new Map(); for (const line of String(stdout).split(/\r?\n/u)) { const match = /^([a-f0-9]{64})\s+\*?([^\s]+)$/u.exec(line.trim()); if (match) values.set(match[2], match[1]); } return values; }

// Same local-expansion guard as index.js preflight/launch assembly and
// strictRemote uses JSON.stringify to emit one double-quoted shell argument;
// so an unescaped $ would be expanded by the LOCAL shell before ssh transmits
// it ($(sbatch ...) would run locally, $job_id would vanish). "\$" becomes a
// literal $ in the transmitted body; expansion happens only on the target.
function strictRemote(target, body) { return `ssh ${SSH_OPTIONS.join(" ")} -- ${target} ${JSON.stringify(body).replace(/\$/g, "\\$")}`; }

// Stage the project's manifest files (owner-approved), build the remote
// PREPARED_SHA256.txt from the transferred bytes, then verify checksums and
// run a bounded, fixed, clean-environment bash -n on shell/sbatch files. No
// allocation. Login-node confinement (t5#1 / t7): a manifest-selected
// interpreter (python_bin) is NEVER invoked remotely — the manifest key is
// accepted for schema compatibility but has no execution effect.
// Idempotent and resumable (2026-08-24): a flaky login node can leave a
// PARTIAL stage behind (some files transferred, the rest failed to connect).
// Re-staging is safe iff every file already present in the run dir is an OWN
// artifact of this project: a manifest file (our earlier partial transfer),
// the PREPARED_SHA256.txt receipt this stage itself writes (rebuilt below), or
// a job output under one of the manifest's extra_dirs. rclone copyto
// overwrites, and the validation below re-hashes ALL files and rebuilds
// PREPARED_SHA256.txt. Any other file (foreign content) fails closed.
// extraOwnPrefixes: additional remote-relative directory prefixes treated as
// this project's OWN artifacts for the resume/foreign-content check. Purely
// additive and default-empty; the declarative recipe executor passes
// ["genbio-recipes"] so that
// re-staging a manifest package over a run dir that already holds this
// project's recipe wrapper (genbio-recipes/<hash>.<op>.sbatch) resumes instead
// of failing closed on a foreign file.
async function stageAndValidate({ manifest, exec, userQuestions, shell, runRemote, config, extraOwnPrefixes = [] }) {
  const { files, bytes } = await localFiles(manifest);
  await askTransfer(userQuestions, exec, manifest.project, files, bytes);
  const root = shellQuote(manifest.remoteRoot);
  const parents = [...new Set([...files.map((item) => item.rel.split("/").slice(0, -1).join("/")).filter(Boolean), ...manifest.extraDirs])];
  const dirs = parents.map((rel) => shellQuote(`${manifest.remoteRoot}/${rel}`)).join(" ");
  // Read-only inspection: which files already exist under the run dir (or the
  // dir is absent). Drives the resume-vs-fail-closed decision below.
  const inspect = await runRemoteWithRetry(runRemote, "HPC", strictRemote("HPC", `set -eu; if test -e ${root}; then cd -- ${root}; find . -type f -printf '%P\\n' | sort; else printf '__NO_ROOT__\\n'; fi`), exec, 30000);
  if (inspect.exitCode !== 0) throw new Error(`${manifest.project}: failed to inspect remote run directory: ${inspect.stderr || inspect.stdout || inspect.exitCode}`);
  const expected = new Set(files.map((item) => item.rel));
  const existing = inspect.stdout.trim() === "__NO_ROOT__" ? [] : inspect.stdout.split(/\r?\n/u).map((line) => line.trim()).filter(Boolean);
  // Own artifacts (safe to stage over): manifest files, the PREPARED_SHA256.txt
  // receipt from an earlier stage, and outputs under manifest extra_dirs
  // (this project's job area). extra_dir containment requires the explicit
  // trailing separator so "out" does not bless "outer/x"; a file AT an
  // extra_dir path itself is foreign (the slot must be a directory).
  // Everything else is foreign content and fails closed below.
  // extraOwnPrefixes adds caller-owned prefixes (e.g. "genbio-recipes") with
  // the same explicit trailing-separator containment so "genbio-recipes"
  // never blesses a sibling like "genbio-recipes-x/y".
  const ownArtifact = (rel) => expected.has(rel) || rel === "PREPARED_SHA256.txt" || manifest.extraDirs.some((dir) => rel.startsWith(`${dir}/`)) || extraOwnPrefixes.some((prefix) => rel.startsWith(`${prefix}/`));
  const unexpected = existing.filter((rel) => !ownArtifact(rel));
  if (unexpected.length > 0) throw new Error(`${manifest.project}: run dir contains files not owned by this project (not manifest files, PREPARED_SHA256.txt, or extra_dirs outputs); refusing to stage over them: ${unexpected.join(", ")}`);
  const remoteInit = await runRemoteWithRetry(runRemote, "HPC", strictRemote("HPC", `set -eu; mkdir -p ${dirs}`), exec, 30000);
  if (remoteInit.exitCode !== 0) throw new Error(`${manifest.project}: failed to create remote run directory structure: ${remoteInit.stderr || remoteInit.stdout || remoteInit.exitCode}`);
  const stagedErrors = [];
  // Global transfer policy (owner directive 2026-08-24): staging uses rclone
  // only — never scp. The destination is `<rclone-remote>:<abs remote path>`
  // resolved via lib/transfer.js (fail-closed when the target has no remote).
  // rclone copyto creates missing parents and overwrites files already present
  // from our own partial stage, preserving idempotent resume semantics; the
  // remote sha256 gate below remains the integrity authority.
  const rcloneRemote = resolveRcloneRemote(config, "HPC");
  for (const item of files) {
    const staged = await runLocal(shell, rcloneCopytoCommand(item.path, rcloneRemote, `${manifest.remoteRoot}/${item.rel}`), 120000, exec.signal);
    if (staged.exitCode !== 0) stagedErrors.push(`${item.rel}: ${staged.stderr || staged.stdout || staged.exitCode}`);
  }
  if (stagedErrors.length > 0) throw new Error(`${manifest.project}: staging transfer (rclone:${rcloneRemote}) failed: ${stagedErrors.join("; ")}`);
  const rels = files.map((item) => shellQuote(item.rel)).join(" ");
  const executableScripts = manifest.files.filter((rel) => rel.endsWith(".sh"));
  const segments = [`set -eu`, `cd -- ${root}`, ...(executableScripts.length > 0 ? [`chmod 700 -- ${executableScripts.map(shellQuote).join(" ")}`] : []), `sha256sum ${rels} > PREPARED_SHA256.txt`];
  // Checksum-pin before any syntax check (t10): the receipt must self-verify
  // under set -eu, so the files below are parsed only after their bytes are
  // proven identical to the transferred bytes.
  segments.push("sha256sum -c --quiet PREPARED_SHA256.txt", "cat PREPARED_SHA256.txt");
  // Fixed clean-environment syntax check (t7/t10): the exact fixed binary
  // /bin/bash, cleared environment, no profile/rc, explicit operand
  // separator — never a PATH-resolved `bash`, and never an interpreter
  // chosen by the manifest (python_bin has no execution effect here).
  for (const rel of manifest.files) if (rel.endsWith(".sbatch") || rel.endsWith(".sh")) segments.push(`env -i PATH=/usr/bin:/bin BASH_ENV=/dev/null /bin/bash --noprofile --norc -n -- ${shellQuote(rel)}`);
  segments.push("printf 'PROJECT_STAGE_VALIDATION_OK\\n'");
  const validation = await runRemoteWithRetry(runRemote, "HPC", strictRemote("HPC", segments.join("; ")), exec, 60000);
  if (validation.exitCode !== 0) throw new Error(`${manifest.project}: remote package validation failed: ${validation.stderr || validation.stdout || validation.exitCode}`);
  const remoteChecksums = parseChecksums(validation.stdout);
  for (const item of files) if (remoteChecksums.get(item.rel) !== item.sha256) throw new Error(`${manifest.project}: checksum mismatch after staging: ${item.rel}`);
  return { files, bytes, validation };
}

// Bounded, read-only, fail-closed live node state/headroom probe (t10).
//
// Runs as ITS OWN bounded runRemote call immediately before the submit call,
// so the probe evidence and the submit act remain separately observable and
// parseable. The remote command always emits a structured NODE_PROBE= record
// (printf under set -eu) containing one-line `scontrol show node` evidence.
// State, CPUAlloc/CPUTot, and CfgTRES/AllocTRES are parsed locally; only
// idle/alloc/mix are usable, and free CPU/GPU must cover the request. Any
// failure — nonzero exit, missing/wrong node, unusable state, unparseable
// resources, or short headroom —
// throws BEFORE the submit call, so no allocation is issued.
//
const SAFE_NODE_RE = /^[a-z0-9][a-z0-9-]*$/u;
const USABLE_NODE_STATES = new Set(["idle", "alloc", "mix"]);
export async function probeNodeHeadroom({ exec, runRemote, cpus, gpus, node }) {
  if (typeof node !== "string" || !SAFE_NODE_RE.test(node)) throw new Error("probeNodeHeadroom requires a safe target node");
  const cpusReq = Number(cpus), gpusReq = Number(gpus);
  if (!Number.isInteger(cpusReq) || !Number.isInteger(gpusReq) || cpusReq < 0 || gpusReq < 0) throw new Error(`invalid node probe resources: ${cpus}/${gpus}`);
  const body = `set -eu; line=$(scontrol show node ${node} -o 2>/dev/null || true); printf 'NODE_PROBE=%s\n' "$line"`;
  const result = await runRemote("HPC", strictRemote("HPC", body), exec, 30000);
  if (result.exitCode !== 0) throw new Error(`${node} pre-submit probe failed: ${result.stderr || result.stdout || result.exitCode}`);
  const lines = String(result.stdout).split(/\r?\n/u).map((l) => l.trim()).filter(Boolean);
  const probeLine = lines.find((l) => l.startsWith("NODE_PROBE="));
  if (probeLine === undefined) throw new Error(`${node} pre-submit probe: missing NODE_PROBE marker; failing closed before sbatch`);
  const value = probeLine.slice("NODE_PROBE=".length).trim();
  if (value === "") throw new Error(`${node} pre-submit probe: scontrol reported no such node; failing closed before sbatch`);
  const field = (name) => new RegExp(`(?:^|\\s)${name}=([^\\s]+)`, "u").exec(value)?.[1] ?? null;
  const nodeName = field("NodeName"), stateText = field("State"), cores = Number(field("CPUTot")), allocCores = Number(field("CPUAlloc"));
  if (nodeName !== node || !stateText) throw new Error(`${node} pre-submit probe: unparseable scontrol row "${value}"; failing closed before sbatch`);
  const rawState = stateText.split(/[+~*#]/u)[0].toLowerCase();
  const baseState = rawState === "allocated" ? "alloc" : rawState === "mixed" ? "mix" : rawState;
  if (!USABLE_NODE_STATES.has(baseState)) throw new Error(`${node} pre-submit probe: node state "${stateText}" is not usable (idle/alloc/mix); failing closed before sbatch`);
  const gpuCount = (text) => { const match = /(?:^|[,\s])(?:gres\/)?gpu(?::[^:=,\s()]+)?(?:=|:)(\d+)/u.exec(text ?? ""); return match ? Number(match[1]) : null; };
  const configuredGpus = gpuCount(field("CfgTRES")) ?? gpuCount(field("Gres"));
  const allocatedGpus = gpuCount(field("AllocTRES"));
  const freeCpus = cores - allocCores;
  if (gpusReq > 0 && configuredGpus === null) throw new Error(`${node} pre-submit probe: missing or non-numeric CPU/GPU resources "${value}"; failing closed before sbatch`);
  if (gpusReq > 0 && allocatedGpus === null && baseState !== "idle") throw new Error(`${node} pre-submit probe: incomplete allocated-GPU evidence for a non-idle node; failing closed before sbatch`);
  const freeGpus = allocatedGpus === null ? configuredGpus : configuredGpus - allocatedGpus;
  if (![cores, allocCores, freeCpus].every(Number.isFinite) || (gpusReq > 0 && !Number.isFinite(freeGpus))) throw new Error(`${node} pre-submit probe: missing or non-numeric CPU/GPU resources "${value}"; failing closed before sbatch`);
  if (freeCpus < cpusReq) throw new Error(`${node} pre-submit probe: insufficient CPU headroom (free ${freeCpus} < requested ${cpusReq}); failing closed before sbatch`);
  if (gpusReq > 0 && freeGpus < gpusReq) throw new Error(`${node} pre-submit probe: insufficient GPU headroom (free ${freeGpus} < requested ${gpusReq}); failing closed before sbatch`);
  return { ok: true, note: `${node} state=${stateText} free_cpus=${freeCpus} free_gpus=${gpusReq > 0 ? freeGpus : "not-requested"}` };
}

// Re-establish package trust before any submission: the remote receipt must
// exist and self-verify (sha256sum -c under set -eu), and any per-file hashes
// the remote reports must equal the freshly hashed local bytes (catches drift
// on either side since staging). Empty stdout with exit 0 is unreachable in
// the real world (the command ends in sha256sum, which always prints under
// set -eu) and means "receipt self-check passed, no per-file data to
// compare"; NON-empty output without a single parseable hash line is
// fail-closed garbage.
async function verifyPackage({ manifest, exec, runRemote }) {
  const { files } = await localFiles(manifest);
  const root = shellQuote(manifest.remoteRoot);
  const rels = files.map((item) => shellQuote(item.rel)).join(" ");
  const result = await runRemote("HPC", strictRemote("HPC", `set -eu; cd -- ${root}; test -s PREPARED_SHA256.txt; sha256sum -c --quiet PREPARED_SHA256.txt; sha256sum ${rels}`), exec, 60000);
  if (result.exitCode !== 0) throw new Error(`${manifest.project}: package verification failed (package missing or drifted): ${result.stderr || result.stdout || result.exitCode}`);
  const remoteChecksums = parseChecksums(result.stdout);
  const trimmed = String(result.stdout).trim();
  if (trimmed.length > 0 && remoteChecksums.size === 0) throw new Error(`${manifest.project}: package verification returned unparseable checksum output; failing closed before submission`);
  for (const item of files) if (remoteChecksums.has(item.rel) && remoteChecksums.get(item.rel) !== item.sha256) throw new Error(`${manifest.project}: package checksum mismatch: ${item.rel}`);
  if (trimmed.length > 0) for (const item of files) if (!remoteChecksums.has(item.rel)) throw new Error(`${manifest.project}: package verification reported no checksum for ${item.rel}; failing closed before submission`);
  return result;
}

// Stage a generated recipe wrapper (in-memory bytes from the plan resolution)
// into the fixed remote run dir's genbio-recipes/ subdirectory. rclone-only
// (lib/transfer.js, never scp). The destination is content-addressed by the
// manifest: <remoteRoot>/genbio-recipes/<shortHash>.<operation>.sbatch, where
// shortHash is a prefix of the manifest SHA-256 — so an identical manifest
// always lands at the SAME wrapper path (no collision across plans/retries)
// while a drifted manifest yields a different path. After the transfer the
// remote SHA-256 of the wrapper must equal the SHA-256 of wrapperBytes, and
// the wrapper must parse under a fixed clean-environment /bin/bash -n.
// userQuestions is accepted for API symmetry with the other staging helpers;
// the wrapper is a deterministic artifact of the approved plan (not a user
// file), so no separate material-transfer card is issued.
async function stageRecipeWrapper({ manifest, wrapperBytes, operation, manifestSha, exec, userQuestions, shell, runRemote, config }) {
  void userQuestions;
  const project = manifest.project;
  if (!/^[a-f0-9]{64}$/u.test(manifestSha ?? "")) throw new Error(`${project}: recipe wrapper staging requires a full 64-hex manifest SHA-256`);
  let bytes = wrapperBytes;
  if (typeof bytes === "string") bytes = Buffer.from(bytes, "utf8");
  if (!Buffer.isBuffer(bytes) || bytes.length === 0) throw new Error(`${project}: recipe wrapper bytes must be a non-empty Buffer`);
  const shortHash = manifestSha.slice(0, 12);
  const wrapperRel = `genbio-recipes/${shortHash}.${operation}.sbatch`;
  const wrapperRemotePath = `${manifest.remoteRoot}/${wrapperRel}`;
  const expectedSha = sha256(bytes);
  const root = shellQuote(manifest.remoteRoot);
  const rcloneRemote = resolveRcloneRemote(config, "HPC");
  const localTmp = join(tmpdir(), `genbio-recipe-${shortHash}-${operation}-${randomBytes(6).toString("hex")}.sbatch`);
  await writeFile(localTmp, bytes);
  try {
    const mkd = await runRemoteWithRetry(runRemote, "HPC", strictRemote("HPC", `set -eu; mkdir -p ${shellQuote(`${manifest.remoteRoot}/genbio-recipes`)}`), exec, 30000);
    if (mkd.exitCode !== 0) throw new Error(`${project}: failed to create remote genbio-recipes directory: ${mkd.stderr || mkd.stdout || mkd.exitCode}`);
    const staged = await runLocal(shell, rcloneCopytoCommand(localTmp, rcloneRemote, wrapperRemotePath), 120000, exec.signal);
    if (staged.exitCode !== 0) throw new Error(`${project}: recipe wrapper staging (rclone:${rcloneRemote}) failed: ${staged.stderr || staged.stdout || staged.exitCode}`);
    // Remote SHA-256 gate: the bytes that landed must equal wrapperBytes.
    const check = await runRemoteWithRetry(runRemote, "HPC", strictRemote("HPC", `set -eu; cd -- ${root}; sha256sum ${shellQuote(wrapperRel)}`), exec, 30000);
    if (check.exitCode !== 0) throw new Error(`${project}: recipe wrapper checksum read failed: ${check.stderr || check.stdout || check.exitCode}`);
    const remoteChecksums = parseChecksums(check.stdout);
    if (remoteChecksums.get(wrapperRel) !== expectedSha) throw new Error(`${project}: recipe wrapper checksum mismatch after staging (expected ${expectedSha}, got ${remoteChecksums.get(wrapperRel) ?? "none"})`);
    // Fixed clean-environment syntax check (t7/t10): exact /bin/bash, cleared
    // env, no profile/rc — the same fixed binary package staging uses.
    const syntax = await runRemoteWithRetry(runRemote, "HPC", strictRemote("HPC", `set -eu; cd -- ${root}; env -i PATH=/usr/bin:/bin BASH_ENV=/dev/null /bin/bash --noprofile --norc -n -- ${shellQuote(wrapperRel)}`), exec, 30000);
    if (syntax.exitCode !== 0) throw new Error(`${project}: recipe wrapper fails remote clean-env bash -n: ${syntax.stderr || syntax.stdout || syntax.exitCode}`);
    return { wrapperRel, wrapperRemotePath, wrapperSha: expectedSha };
  } finally {
    await rm(localTmp, { force: true });
  }
}

// Exactly-one structured submission record (t7 / t5#6): the submit command
// prints JOB_ID=<digits> and (optionally) RUN_DIR=<path>. Anything else — a
// malformed id, a duplicate record, extra output, or nothing at all — is
// ambiguous (the job may exist) and must never settle as a completed
// submission.
function parseSubmitOutput(stdout) {
  const lines = String(stdout).split(/\r?\n/u).map((line) => line.trim()).filter(Boolean);
  if (lines.length === 0 || lines.length > 2) throw new Error("ambiguous sbatch output: expected exactly one JOB_ID record (plus at most one RUN_DIR record)");
  let jobId = null;
  let sawRunDir = false;
  for (const line of lines) {
    const jobMatch = /^JOB_ID=([0-9]{1,10})$/u.exec(line);
    if (jobMatch) { if (jobId !== null) throw new Error(`ambiguous sbatch output: duplicate JOB_ID records (${jobId} and ${jobMatch[1]})`); jobId = jobMatch[1]; continue; }
    if (/^RUN_DIR=.+$/u.test(line)) { if (sawRunDir) throw new Error("ambiguous sbatch output: duplicate RUN_DIR records"); sawRunDir = true; continue; }
    throw new Error(`ambiguous sbatch output: unrecognized record "${line.slice(0, 80)}"`);
  }
  if (jobId === null) throw new Error("ambiguous sbatch output: no well-formed JOB_ID record");
  return jobId;
}

// A lost transport (ssh timeout/disconnect) after the sbatch command was
// issued is AMBIGUOUS: Slurm may already have accepted the job. Definite
// failures (sbatch itself refused, package verification failed) are not.
//
// (t11 / t9 advisory A) A LOCAL timeout (timedOut) or signal kill of the ssh
// command is also a lost transport even when no stderr text is present and
// the exit code is null/other-than-255: the remote sbatch may already have
// been accepted. These must classify as transport-ambiguous, NEVER as a
// definite failure.
function isTransportFailure(result) {
  if (result.timedOut === true) return true;
  if (result.signal != null) return true;
  if (result.exitCode === 255) return true;
  const stderr = String(result.stderr ?? "");
  return /connection timed out|connect: (Connection refused|Operation timed out)|ssh: connect|broken pipe|reset by peer|connection reset|network is unreachable/u.test(stderr);
}

// Exact-once submission intents (t7 / t5#2): every submission attempt for a
// (project, operation) is recorded durably in session state. An UNRESOLVED
// ambiguous intent gates all later attempts for that pair: they reconcile
// read-only and never issue a second sbatch.
function submissionsOf(state) {
  if (!Array.isArray(state.submissions)) state.submissions = [];
  return state.submissions;
}

// Read-only reconciliation for an ambiguous submission (t7 / t5#2): query
// BOTH live queue state and accounting for the exact unique job name. The
// tagged envelope is parsed locally; no awk/grep or fixed-row assumptions are
// used. Exactly one union candidate resolves the ambiguity; zero, malformed,
// or multiple candidates leave it unresolved (fail closed). Never invokes
// sbatch/salloc/srun.
function parseSchedulerRows(text, section, expectedJobName) {
  const rows = [];
  for (const raw of String(text).split(/\r?\n/u)) {
    const line = raw.trim();
    if (!line) continue;
    const fields = line.split("|");
    if (fields.length !== 3) return null;
    const [jobId, jobName, state] = fields;
    if (!/^[0-9]{1,10}$/u.test(jobId) || jobName !== expectedJobName || !/^[A-Za-z][A-Za-z0-9_+.-]*$/u.test(state)) return null;
    rows.push({ source: section, jobId, jobName, state });
  }
  return rows;
}

export function parseReconciliationEnvelope(stdout, expectedJobName) {
  if (typeof expectedJobName !== "string" || expectedJobName.length === 0) return null;
  const text = String(stdout);
  const match = /^SQUEUE_BEGIN\n([\s\S]*?)\nSQUEUE_END\nSACCT_BEGIN\n([\s\S]*?)\nSACCT_END\n?$/u.exec(text);
  if (!match) return null;
  const squeue = parseSchedulerRows(match[1], "squeue", expectedJobName);
  const sacct = parseSchedulerRows(match[2], "sacct", expectedJobName);
  if (squeue === null || sacct === null) return null;
  return { squeue, sacct };
}

export function resolveReconciliationCandidates(envelope) {
  if (!envelope || !Array.isArray(envelope.squeue) || !Array.isArray(envelope.sacct)) return null;
  const candidates = [...new Set([...envelope.squeue, ...envelope.sacct].map((row) => row.jobId))];
  return candidates.length === 1 ? candidates[0] : null;
}

export async function reconcileSubmission({ exec, runRemote, jobName }) {
  const body = `set -eu; squeue_table=$(squeue -h -u "$USER" -n '${jobName}' -o '%.18i|%.100j|%.20T' 2>/dev/null || true); sacct_table=$(sacct -X -n -u "$USER" --name='${jobName}' --starttime=-7days --format=JobIDRaw,JobName,State -P 2>/dev/null || true); printf 'SQUEUE_BEGIN\\n%s\\nSQUEUE_END\\nSACCT_BEGIN\\n%s\\nSACCT_END\\n' "$squeue_table" "$sacct_table"`;
  const result = await runRemote("HPC", strictRemote("HPC", body), exec, 30000);
  if (result.exitCode !== 0) return null;
  return resolveReconciliationCandidates(parseReconciliationEnvelope(result.stdout, jobName));
}

// Submit exactly one generated schema-v2 wrapper from the remote run dir and return
// immediately with the Slurm job id. Deliberate submit-and-return: long jobs
// survive laptop sleep/network drops while Slurm keeps them alive; terminal
// evidence is collected with genbio_project_status.
//
// Exact-once and TOCTOU-safe (t7):
//  1. the local template must still hash to the Phase-0 validated digest
//     (t5#4) and re-validate, so bytes the policy validator never saw can
//     never reach sbatch;
//  2. an unresolved ambiguous submission for this (project, operation) gates
//     the attempt: read-only reconciliation only, never a second sbatch
//     (t5#2);
//  3. the staged remote package re-verifies (receipt self-check + per-file
//     hashes against fresh local bytes);
//  4. ONE sbatch; its output must be exactly one well-formed JOB_ID record
//     (t5#6);
//  5. a transport loss or malformed output is AMBIGUOUS: recorded, reconciled
//     read-only, and never retried; an accepted job reserves its full
//     resources until terminal evidence (t5#3).
// Unique job name per submission intent (t10): the validated job name gets a
// short high-entropy suffix so read-only reconciliation matches the exact job
// and never confuses it with an earlier same-named run. Bounded to stay within
// Slurm job-name limits; if the base name is already long, uniqueness rides on
// the persisted intent token instead.
function uniqueJobNameFor(jobName, token) {
  const suffix = `.${token.slice(0, 8)}`;
  const maxBase = 100 - suffix.length;
  return `${jobName.slice(0, maxBase)}${suffix}`;
}

// The intent and allocation are RESERVED AT ADMISSION (jobTool.execute) in a
// contiguous synchronous block — the in-flight check, the intent push, and the
// "submitting" allocation push happen with no await between them, which is
// atomic under the single-threaded event loop: two concurrent executes for
// the same (project, operation) can never both reserve (t10 follow-up).
// submitJob only TRANSITIONS the reserved records:
//   "submitting" → "nonterminal" (job id confirmed / reconciliation resolved)
//   "submitting" → "ambiguous"   (sbatch outcome unknown; pair-gated)
//   "submitting" → "failed"      (definite pre-sbatch failure)
// A run that throws before any transition (abort, digest check) is released
// to "failed" by the runBody catch, so a pair can never stay in-flight forever.
async function submitJob({ manifest, operation, policy, state, exec, runRemote, intent, allocation, templateSha, templateRel, templateBytes, beforeDispatch = null }) {
  const project = manifest.project;
  const jobSpec = manifest.jobs[operation];
  const settleAlloc = (slurmJobId, status, source) => { allocation.slurmJobId = slurmJobId; allocation.status = status; allocation.source = source ?? allocation.source; };
  // 1. Bind and re-validate the generated schema-v2 wrapper bytes that are
  //    about to be submitted. The execution core never reads a manifest-owned
  //    Slurm template and therefore has no raw-template submission path.
  if (!Buffer.isBuffer(templateBytes) || templateBytes.length === 0) throw new Error(`${project}: schema-v2 submission requires non-empty generated wrapper bytes`);
  if (sha256(templateBytes) !== templateSha) throw new Error(`${project}: recipe wrapper bytes changed after validation (digest mismatch); nothing is submitted`);
  const revalidated = validatePinnedSbatch(templateBytes.toString("utf8"), jobSpec, { policy, envelope: state.envelope });
  intent.uniqueJobName = uniqueJobNameFor(revalidated.jobName, intent.token);
  // 2. Exact-once gate: an unresolved ambiguous intent from an EARLIER attempt
  // reconciles by its OWN unique job name, never resubmits. Duplicate
  // candidate matches stay ambiguous (hard fail: the run ends without a
  // confirmed job id). This call reconciles rather than submits, so its own
  // admission reservation is released and the prior reservation is transitioned.
  const pending = submissionsOf(state).find((entry) => entry.project === project && entry.operation === operation && entry.status === "ambiguous");
  if (pending) {
    const resolved = await reconcileSubmission({ exec, runRemote, jobName: pending.uniqueJobName });
    if (resolved !== null) {
      pending.status = "submitted"; pending.slurmJobId = resolved; pending.resolvedAt = Date.now();
      if (pending.allocation && pending.allocation.status === "ambiguous") { pending.allocation.slurmJobId = resolved; pending.allocation.status = "nonterminal"; pending.allocation.source = "reconciliation"; }
      intent.status = "submitted"; intent.slurmJobId = resolved; intent.resolvedAt = Date.now();
      allocation.status = "failed"; allocation.source = "reconciled prior submission (no new job)";
      return { slurmJobId: resolved, stdout: `RECONCILED_JOB_ID=${resolved}\n`, stderr: "" };
    }
    intent.status = "failed"; intent.note = "reconciliation found no single candidate";
    allocation.status = "failed"; allocation.source = "reconciliation found no single candidate";
    throw new Error(`${project}: submission ${operation} is still ambiguous (token ${pending.token}); read-only reconciliation for job name ${pending.uniqueJobName} found no single candidate. Nothing is resubmitted; inspect the cluster (sacct/squeue) and re-run after the ambiguity is resolved`);
  }
  // 3. Package re-verification (remote receipt self-check + per-file hashes).
  // A verification failure is DEFINITE (sbatch was never attempted): the
  // intent and reservation are released and a later attempt is allowed.
  try {
    await verifyPackage({ manifest, exec, runRemote });
  } catch (error) {
    intent.status = "failed"; intent.note = "package verification failed before sbatch";
    settleAlloc(null, "failed", "verify-failed");
    throw error;
  }
  // 4. Bounded fail-closed live node state/headroom probe — ITS OWN bounded
  // runRemote call, immediately before the submit call, so probe evidence and
  // the submit act remain separately observable (t10 follow-up). Any probe
  // failure throws before sbatch: no allocation is issued.
  try {
    await probeNodeHeadroom({ exec, runRemote, cpus: jobSpec.cpus, gpus: jobSpec.gpus, node: revalidated.node });
  } catch (error) {
    intent.status = "failed"; intent.note = `${revalidated.node} pre-submit probe failed before sbatch`;
    settleAlloc(null, "failed", "probe-failed");
    throw error;
  }
  // 5. Exactly one sbatch, under the per-intent unique job name. The dispatch
  // flag is set BEFORE the await: if the runRemote call itself throws (local
  // timeout/abort/spawn error) once the ssh command is in flight, the job MAY
  // have been accepted, and the runBody catch must keep the pair gated
  // ("ambiguous"), never release it to "failed" (captain t10 note: the
  // in-flight state only clears on a definite end with no possible
  // outstanding allocation).
  intent.sbatchIssued = true;
  if (typeof beforeDispatch === "function") await beforeDispatch({ uniqueJobName: intent.uniqueJobName, token: intent.token });
  const root = shellQuote(manifest.remoteRoot);
  // The submitted path is the wrapper (templateRel) for a generated recipe, or
  // wrapper path is relative to the run-dir root (the sbatch cwd), so the wrapper's
  // cd "$SLURM_SUBMIT_DIR" still lands in the manifest run dir.
  if (typeof templateRel !== "string" || templateRel.length === 0) throw new Error(`${project}: schema-v2 submission requires a staged wrapper path`);
  const submitTemplate = templateRel;
  const submit = await runRemote("HPC", strictRemote("HPC", `set -eu; cd -- ${root}; job_id=$(sbatch --parsable --job-name=${shellQuote(intent.uniqueJobName)} ${shellQuote(submitTemplate)}); test -n "$job_id"; printf 'JOB_ID=%s\\nRUN_DIR=%s\\n' "$job_id" ${root}`), exec, 60000);
  if (submit.exitCode !== 0) {
    if (isTransportFailure(submit)) {
      intent.status = "ambiguous"; intent.note = "transport failure after sbatch attempt (job may have been accepted)";
      settleAlloc(null, "ambiguous", null);
      const resolved = await reconcileSubmission({ exec, runRemote, jobName: intent.uniqueJobName });
      if (resolved !== null) {
        intent.status = "submitted"; intent.slurmJobId = resolved; intent.resolvedAt = Date.now();
        settleAlloc(resolved, "nonterminal", "reconciliation");
        return { slurmJobId: resolved, stdout: `RECONCILED_JOB_ID=${resolved}\n`, stderr: String(submit.stderr ?? "") };
      }
      throw new Error(`${project}: sbatch transport failure is ambiguous (token ${intent.token}, job name ${intent.uniqueJobName}); the job may have been accepted. Nothing is resubmitted; reconcile read-only (genbio_project_status / cluster inspection) before retrying`);
    }
    intent.status = "failed"; intent.note = "definite submission failure";
    settleAlloc(null, "failed", "sbatch-refused");
    throw new Error(`${project}: sbatch submission failed: ${submit.stderr || submit.stdout || submit.exitCode}`);
  }
  // 6. Exactly one well-formed JOB_ID record, or the outcome is ambiguous.
  let slurmJobId;
  try {
    slurmJobId = parseSubmitOutput(submit.stdout);
  } catch (error) {
    intent.status = "ambiguous"; intent.note = "malformed or duplicate job-id output";
    settleAlloc(null, "ambiguous", null);
    const resolved = await reconcileSubmission({ exec, runRemote, jobName: intent.uniqueJobName });
    if (resolved !== null) {
      intent.status = "submitted"; intent.slurmJobId = resolved; intent.resolvedAt = Date.now();
      settleAlloc(resolved, "nonterminal", "reconciliation");
      return { slurmJobId: resolved, stdout: `${submit.stdout}\nRECONCILED_JOB_ID=${resolved}\n`, stderr: String(submit.stderr ?? "") };
    }
    throw new Error(`${project}: ${error.message}; submission is ambiguous (token ${intent.token}, job name ${intent.uniqueJobName}). Nothing is resubmitted; reconcile via cluster inspection before retrying`);
  }
  intent.slurmJobId = slurmJobId;
  settleAlloc(slurmJobId, "nonterminal", "sbatch");
  return { slurmJobId, stdout: submit.stdout, stderr: String(submit.stderr ?? "") };
}

// Slurm states that free envelope capacity (t7 / t5#3): only terminal
// evidence releases a nonterminal allocation's reserved resources.
const TERMINAL_SCHEDULER_STATES = new Set(["COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL", "OUT_OF_MEMORY", "BOOT_FAIL", "DEADLINE", "PREEMPTED", "REVOKED"]);
const PENDING_SCHEDULER_STATES = new Set(["PENDING", "CONFIGURING", "RESIZING", "REQUEUED", "REQUEUE_FED"]);
const RUNNING_SCHEDULER_STATES = new Set(["RUNNING", "COMPLETING", "SUSPENDED", "STAGE_OUT"]);

function findWorkloadRun(state, project, jobId) {
  return [...(state.runs ?? [])].reverse().find((run) => run.target === "HPC" && run.slurmJobId === jobId && String(run.operation).startsWith(`project-${project}-`) && !String(run.operation).includes("status-")) ?? null;
}

function findOwnedOperationRun(state, project, operation, jobId) {
  return [...(state.runs ?? [])].reverse().find((run) => run.target === "HPC" && run.slurmJobId === jobId && run.operation === `project-${project}-${operation}`) ?? null;
}

function updateWorkloadFromScheduler(run, scheduler) {
  if (!run || !scheduler?.state) return false;
  run.slurmStatus = scheduler.state;
  run.slurmExitCode = scheduler.exitCode ?? null;
  run.slurmElapsed = scheduler.elapsed ?? null;
  run.workloadEvidence = scheduler.markers?.identity ? (scheduler.markers.complete ? "scheduler-and-job-output" : "scheduler-and-incomplete-job-output") : "scheduler-only";
  run.workloadStatus = classifyWorkloadEvidence(scheduler, scheduler.markers);
  if (["completed", "failed", "cancelled", "evidence-incomplete"].includes(run.workloadStatus)) run.workloadFinishedAt ??= Date.now();
  return true;
}

// Bounded read-only evidence for one Slurm job id: sacct accounting line plus
// the tail of the job's %x_%j.{out,err} files under the project run dir.
// No allocation, no writes. Terminal evidence marks the matching allocation
// terminal, releasing its reserved envelope capacity.
async function cancelOwnedJob({ state, project, operation, jobId, exec, userQuestions, runRemote, expectedJobName = null, beforeCancel = null }) {
  const exactJobId = String(jobId);
  if (!/^[0-9]{1,10}$/u.test(exactJobId)) throw new Error(`invalid Slurm job id: ${exactJobId}`);
  const owned = (state.allocations ?? []).find((entry) => entry.project === project && entry.operation === operation && entry.slurmJobId === exactJobId && ["submitting", "nonterminal", "ambiguous"].includes(entry.status));
  if (!owned || !findOwnedOperationRun(state, project, operation, exactJobId)) throw new Error(`Slurm job ${exactJobId} is not an active session-owned job for ${project}/${operation}`);
  if (expectedJobName) {
    const identity = await runRemote("HPC", strictRemote("HPC", `set -eu; job=${shellQuote(exactJobId)}; expected=${shellQuote(expectedJobName)}; table=$(sacct -X -n -u "$USER" -j "$job" --format=JobIDRaw,JobName -P 2>/dev/null || true); matches=$(printf '%s\\n' "$table" | awk -F'|' -v id="$job" -v name="$expected" '$1 == id && $2 == name { n++ } END { print n+0 }'); test "$matches" = 1`), exec, 30000);
    if (identity.exitCode !== 0) throw new Error(`Slurm cancellation identity check failed closed for ${exactJobId}/${expectedJobName}`);
  }
  if (!userQuestions) throw new Error("cancellation requires the DSH user-question provider");
  const questionId = `genbio-project-cancel-${exactJobId}`;
  const answer = await userQuestions.ask({ agent: exec.agent, signal: exec.signal, questions: [{ id: questionId, header: "Cancel Slurm job", question: `Cancel session-owned Slurm job ${exactJobId} for ${project}/${operation}?`, options: [{ label: "Cancel this job", description: "Issue exactly one cancellation request for this numeric job ID." }, { label: "Keep running", description: "Do not cancel the job." }] }] });
  if (!(answer.answers?.find((item) => item.id === questionId)?.selected ?? []).includes("Cancel this job")) throw new Error("cancellation was not explicitly approved");
  if (typeof beforeCancel === "function") await beforeCancel();
  owned.status = "cancel-requested";
  owned.source = "explicit scancel request pending";
  const result = await runRemote("HPC", strictRemote("HPC", `set -eu; scancel ${shellQuote(exactJobId)}`), exec, 30000);
  if (result.exitCode !== 0) throw new Error(`scancel outcome is unresolved: ${result.stderr || result.stdout || result.exitCode}`);
  owned.source = "explicit scancel request";
  return { jobId: exactJobId, requested: true, terminalEvidence: "pending status refresh" };
}

async function statusJob({ manifest, jobId, state, exec, runRemote }) {
  if (!/^[0-9]{1,10}$/u.test(jobId)) throw new Error(`invalid Slurm job id: ${jobId}`);
  const run = findWorkloadRun(state, manifest.project, jobId);
  if (!run) throw new Error(`no owned workload run for Slurm job ${jobId}`);
  const operation = String(run.operation).slice(`project-${manifest.project}-`.length);
  const intent = [...submissionsOf(state)].reverse().find((entry) => entry.project === manifest.project && entry.operation === operation && entry.slurmJobId === jobId);
  if (!intent?.uniqueJobName) throw new Error(`owned Slurm job ${jobId} has no persisted unique job name`);
  const root = shellQuote(manifest.remoteRoot);
  const name = shellQuote(intent.uniqueJobName);
  const body = `set -eu; run_dir=${root}; job=${shellQuote(jobId)}; expected_name=${name}; table=$(sacct -X -n -u "$USER" -j "$job" --format=JobIDRaw,JobName,State,ExitCode,Elapsed -P 2>/dev/null || true); printf 'SACCT_BEGIN\\n%s\\nSACCT_END\\n' "$table"; for suffix in out err; do file="$run_dir/\${expected_name}_\${job}.\${suffix}"; if test -f "$file"; then printf 'FILE_BEGIN=%s\\n' "$file"; head -c 4096 "$file"; printf '\\nFILE_TAIL\\n'; tail -c 30000 "$file"; printf '\\nFILE_END\\n'; fi; done`;
  const result = await runRemote("HPC", strictRemote("HPC", body), exec, 60000);
  if (result.exitCode !== 0) throw new Error(`${manifest.project}: scheduler evidence query failed closed: ${result.stderr || result.stdout || result.exitCode}`);
  const stdout = String(result.stdout ?? "");
  const table = /^SACCT_BEGIN\n([\s\S]*?)\nSACCT_END$/mu.exec(stdout)?.[1] ?? null;
  const parsed = table === null ? null : parseOwnedSacctTable(table, jobId, intent.uniqueJobName);
  if (!parsed) return { ...result, scheduler: { state: null, exitCode: null, elapsed: null, markers: { identity: false, complete: false }, evidence: "accounting-pending-or-unavailable" } };
  const markers = parseJobOutputMarkers(stdout, jobId, intent.uniqueJobName);
  const scheduler = { state: parsed.state, exitCode: parsed.exitCode, elapsed: parsed.elapsed, markers };
  updateWorkloadFromScheduler(run, scheduler);
  if (TERMINAL_SCHEDULER_STATES.has(parsed.state)) markAllocationTerminal(state, jobId, parsed.state);
  return { ...result, scheduler };
}

// Bounded read-only retrieval (owner-approved design 2026-08-24, completed
// 2026-08-25): pull manifest-allowlisted small report/artifact files from the
// fixed remote run dir into the manifest's fetch destination under local_root.
// rclone-only (lib/transfer.js, never scp), byte-capped by fetch.max_bytes,
// explicit owner transfer card, and every file's remote SHA-256 must equal the
// local SHA-256 after download (downloads land as .part files and are renamed
// only after both the size and hash gates pass). A dated FETCH_RECEIPT json is
// written next to the files. No allocation, no envelope.
async function askFetch(userQuestions, exec, project, rels, totalBytes, destDir) {
  if (!userQuestions) throw new Error("material retrieval requires the DSH user-question provider");
  const answer = await userQuestions.ask({ agent: exec.agent, signal: exec.signal, questions: [{ id: `genbio-project-fetch-${project}`, header: "Project HPC retrieval", question: `Project ${project} retrieval will download ${rels.length} allowlisted file(s), ${totalBytes} bytes total, from the fixed authoritative HPC run directory to the local destination ${destDir}. Approve this material retrieval?`, options: [{ label: "Approve this retrieval", description: "Download only the listed allowlisted files; each file must match its remote SHA-256 after transfer." }, { label: "Reject", description: "Do not retrieve anything." }] }] });
  const selected = answer.answers?.find((item) => item.id === `genbio-project-fetch-${project}`)?.selected ?? [];
  if (!selected.includes("Approve this retrieval")) throw new Error(`${project}: material retrieval was not explicitly approved`);
}

async function assertNoSymlinkAncestors(root, destination) {
  const relative = destination.slice(root.length).split(sep).filter(Boolean);
  let current = root;
  for (const part of relative) {
    current = join(current, part);
    try { if ((await lstat(current)).isSymbolicLink()) throw new Error(`fetch destination contains symbolic link: ${current}`); } catch (error) { if (error?.code !== "ENOENT") throw error; }
  }
}

async function fetchArtifacts({ manifest, requested, exec, userQuestions, shell, runRemote, config }) {
  const fetchSpec = manifest.fetch;
  if (!fetchSpec) throw new Error(`${manifest.project}: manifest has no fetch section; add fetch: { max_bytes, dest, files } to enable bounded retrieval`);
  const allowlist = new Set(fetchSpec.files);
  const rels = [...new Set(requested && requested.length > 0 ? requested : fetchSpec.files)];
  const unknown = rels.filter((rel) => !allowlist.has(rel));
  if (unknown.length > 0) throw new Error(`${manifest.project}: fetch files not in the manifest allowlist: ${unknown.join(", ")} (allowlisted: ${fetchSpec.files.join(", ")})`);
  const root = shellQuote(manifest.remoteRoot);
  // Read-only discovery: exact size + SHA-256 of each requested file (one
  // bounded ssh). Missing files are reported distinctly and fail closed below.
  const forList = rels.map((rel) => shellQuote(rel)).join(" ");
  const discovery = await runRemoteWithRetry(runRemote, "HPC", strictRemote("HPC", `set -eu; cd -- ${root}; for f in ${forList}; do if test -f "$f" && ! test -L "$f"; then printf 'OK|%s|%s|%s\\n' "$f" "$(stat -c %s "$f")" "$(sha256sum "$f" | cut -d' ' -f1)"; else printf 'MISSING|%s\\n' "$f"; fi; done`), exec, 30000);
  if (discovery.exitCode !== 0) throw new Error(`${manifest.project}: remote artifact discovery failed: ${discovery.stderr || discovery.stdout || discovery.exitCode}`);
  const discovered = new Map();
  const missing = [];
  for (const line of String(discovery.stdout).split(/\r?\n/u)) {
    const parts = line.split("|");
    if (parts.length === 2 && parts[0] === "MISSING") { missing.push(parts[1]); continue; }
    if (parts.length !== 4 || parts[0] !== "OK") continue;
    const [ , rel, sizeText, sha] = parts;
    if (Number.isInteger(Number(sizeText)) && Number(sizeText) >= 0 && /^[a-f0-9]{64}$/u.test(sha)) discovered.set(rel, { size: Number(sizeText), sha });
  }
  const notFound = rels.filter((rel) => !discovered.has(rel));
  if (notFound.length > 0) throw new Error(`${manifest.project}: remote artifacts missing or unreadable: ${notFound.join(", ")}`);
  const totalBytes = rels.reduce((sum, rel) => sum + discovered.get(rel).size, 0);
  if (totalBytes > fetchSpec.maxBytes) throw new Error(`${manifest.project}: fetch total ${totalBytes} bytes exceeds the manifest cap fetch.max_bytes=${fetchSpec.maxBytes}; raise the cap or select fewer files`);
  const destDir = join(manifest.localRoot, fetchSpec.dest);
  await askFetch(userQuestions, exec, manifest.project, rels, totalBytes, destDir);
  await assertNoSymlinkAncestors(manifest.localRoot, destDir);
  const rcloneRemote = resolveRcloneRemote(config, "HPC");
  const stageDir = await mkdtemp(join(dirname(destDir), `.genbio-fetch-${manifest.project}-`));
  const transferred = [];
  let receiptName = null;
  try {
    for (const rel of rels) {
      const stagedPath = join(stageDir, rel);
      await mkdir(dirname(stagedPath), { recursive: true, mode: 0o700 });
      const result = await runLocal(shell, rcloneCopyfromCommand(rcloneRemote, `${manifest.remoteRoot}/${rel}`, stagedPath), 120000, exec.signal);
      if (result.exitCode !== 0) throw new Error(`${rel}: ${result.stderr || result.stdout || result.exitCode}`);
      const remote = discovered.get(rel);
      const localInfo = await lstat(stagedPath);
      if (!localInfo.isFile() || localInfo.isSymbolicLink() || localInfo.size !== remote.size) throw new Error(`${rel}: local file type or size mismatch`);
      const localSha = sha256(await readFile(stagedPath));
      if (localSha !== remote.sha) throw new Error(`${rel}: sha256 mismatch (remote ${remote.sha}, local ${localSha})`);
      transferred.push({ rel, size: remote.size, sha256: remote.sha });
    }
    const verification = await runRemoteWithRetry(runRemote, "HPC", strictRemote("HPC", `set -eu; cd -- ${root}; for f in ${forList}; do test -f "$f" && ! test -L "$f"; printf 'OK|%s|%s|%s\\n' "$f" "$(stat -c %s "$f")" "$(sha256sum "$f" | cut -d' ' -f1)"; done`), exec, 30000);
    if (verification.exitCode !== 0 || String(verification.stdout).trim() !== String(discovery.stdout).trim()) throw new Error("remote artifacts changed during retrieval");
    const stamp = new Date().toISOString().replace(/[:.]/gu, "-");
    receiptName = `FETCH_RECEIPT_${stamp}.json`;
    const stagedReceiptPath = join(stageDir, receiptName);
    await writeFile(stagedReceiptPath, JSON.stringify({ receipt_schema: "genbio-project-fetch/1", project: manifest.project, fetched_at: new Date().toISOString(), source_root: manifest.remoteRoot, rclone_remote: rcloneRemote, transfer: "rclone-copyfrom", files: transferred }, null, 2) + "\n", { encoding: "utf8", mode: 0o600 });
    await mkdir(dirname(destDir), { recursive: true, mode: 0o700 });
    await assertNoSymlinkAncestors(manifest.localRoot, destDir);
    try { await lstat(destDir); throw new Error(`${manifest.project}: fetch destination already exists; refusing non-atomic overwrite`); } catch (error) { if (error?.code !== "ENOENT") throw error; }
    await rename(stageDir, destDir);
  } catch (error) {
    await rm(stageDir, { recursive: true, force: true });
    throw new Error(`${manifest.project}: retrieval (rclone:${rcloneRemote}) failed: ${String(error?.message ?? error)}`);
  }
  const receiptPath = join(destDir, receiptName);
  const summary = transferred.map((item) => `OK ${item.rel} ${item.size} ${item.sha256}`).join("\n");
  return { stdout: `${summary}\nPROJECT_FETCH_OK ${receiptPath}\n`, stderr: "", bytes: totalBytes, receiptPath };
}

// Run records carry the operation's full schema-v2 resources: project execution
// records the manifest job spec's cpus/gpus and
// concurrency so envelope-usage math and finalized evidence reflect reality;
// non-allocation operations (stage/status/fetch) request zeros/one.
function makeRun(project, operation, requested, state) {
  return { runId: `HPC-project-${project}-${operation}-${Date.now()}`, target: "HPC", operation: `project-${project}-${operation}`, status: "running", helperStatus: "running", workloadStatus: requested.cpus === 0 && requested.gpus === 0 ? "not_applicable" : "unsubmitted", startedAt: Date.now(), finishedAt: null, stdout: "", stderr: "", error: null, slurmJobId: null, slurmStatus: null, slurmExitCode: null, slurmElapsed: null, workloadEvidence: "unobserved", resources: { cpus: requested.cpus, gpus: requested.gpus, memGb: null, concurrency: requested.concurrency }, policyHash: state.policy.hash, node: state.envelope?.node ?? null, partition: state.envelope?.partition ?? null, envelope: state.envelope ? JSON.parse(JSON.stringify(state.envelope)) : null, memory: { status: "not-finalized", error: null, openVikingSessionId: null, traceId: null } };
}

function startTrackedJob({ jobs, exec, state, project, operation, resources, runBody, label, logMaxBytes }) {
  if (!jobs) throw new Error("background job registry is unavailable");
  const maxBytes = Number(logMaxBytes ?? 65536);
  const run = makeRun(project, operation, resources, state);
  run.jobId = jobs.start({ kind: "genbio-HPC-project", label, owner: exec.agent, run: () => {
    const controller = new AbortController();
    const done = (async () => {
      try {
        const outcome = await runBody({ exec: { ...exec, signal: controller.signal } });
        if (outcome.slurmJobId) {
          run.slurmJobId = outcome.slurmJobId;
          run.workloadStatus = "submitted";
          run.workloadEvidence = "scheduler-job-id-confirmed";
        }
        run.stdout = String(outcome.stdout ?? "").slice(-maxBytes);
        run.stderr = String(outcome.stderr ?? "").slice(-maxBytes);
        run.status = outcome.exitCode === 0 ? "completed" : "failed";
        run.helperStatus = run.status;
        run.error = run.status === "completed" ? null : run.stderr || `exit ${outcome.exitCode}`;
        return { status: run.status, detail: run.error ?? (outcome.slurmJobId ? `submitted Slurm job ${outcome.slurmJobId}` : `exit code: ${outcome.exitCode}`) };
      } catch (error) {
        run.status = controller.signal.aborted ? "killed" : "failed";
        run.helperStatus = run.status;
        if (run.workloadStatus === "unsubmitted" && /ambiguous|may have been accepted|reconcil/u.test(String(error?.message ?? error))) {
          run.workloadStatus = "unknown";
          run.workloadEvidence = "submission-outcome-ambiguous";
        }
        run.error = controller.signal.aborted ? null : String(error?.message ?? error);
        return { status: run.status, detail: run.error ?? "project operation failed" };
      } finally { run.finishedAt = Date.now(); }
    })();
    return { cancel: (reason) => controller.abort(reason ?? "project operation cancelled"), done, readOutput: () => { const text = [run.stdout, run.stderr && `[stderr]\n${run.stderr}`, run.error && `[error] ${run.error}`].filter(Boolean).join("\n"); run.stdout = ""; run.stderr = ""; return text; } };
  } });
  state.runs.push(run);
  if (state.runs.length > 50) state.runs.splice(0, state.runs.length - 50);
  return run;
}

export {
  shellQuote, strictRemote, validateEnvelope, assertAggregateCapacity, allocationsOf, submissionsOf,
  uniqueJobNameFor, verifyPackage, localFiles, runLocal, parseChecksums, parseSubmitOutput,
  isTransportFailure, startTrackedJob, askTransfer, submitJob,
  stageAndValidate, stageRecipeWrapper, statusJob, findOwnedOperationRun,
  updateWorkloadFromScheduler, TERMINAL_SCHEDULER_STATES, fetchArtifacts, cancelOwnedJob,
};
