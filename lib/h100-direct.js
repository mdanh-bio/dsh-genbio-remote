// ── W0: manifest-driven DIRECT genbioh100 execution (stage/job/status/fetch) ──
// genbioh100 has no scheduler (surface: direct, login_shell: false). Workloads
// run as a DETACHED bash process. This module follows two existing safety
// patterns and composes them:
//
//   (a) the pinned manifest-driven model (lib/pinned.js): one user-owned YAML
//       manifest per project; stage/job/status/fetch; rclone-only transfer
//       (never scp); remote SHA-256 gates; byte-capped fetch with a dated
//       FETCH_RECEIPT; fail-closed manifest parsing.
//   (b) the aizyme-h100 direct-execution pattern (lib/aizyme-h100.js, deployed
//       reference): 128-bit CSPRNG run identity (never Date.now()); local
//       clean-env bash -n BEFORE any remote/grant/job; requireRemoteAccess
//       AFTER all local validation and BEFORE mutation; explicit material
//       transfer approval; launch ambiguity -> "reconciling" (NEVER "failed",
//       NEVER retryable); cancellation is local-observer-only; status requires
//       run_id + run-bound token match; completed only on authoritative
//       terminal evidence; GPU 1 / gpu_util are never touched.
//
// W0 addition: the direct layer is CPU-ONLY (jobs must declare gpus: 0, never
// a GPU; CUDA is left unset). Admission is doubly bounded: the active
// CPU-only run count <= concurrent_cpu_jobs (default 1 until an explicit
// owner-approved policy raises it, see
// genbioh100ConcurrencyCap in slurm-policy.js) AND the sum of active CPU
// threads + this job <= the FRESH machine nproc (read live each admission,
// fail-closed). This is what lets analysis jobs run without the GPU single-
// slot while a raised concurrency cap can never oversubscribe the cores.
//
// The entry script is executed through a FIXED plugin-owned runner
// (RUNNER_BYTES, pinned by RUNNER_SHA). Dynamic inputs (run dir, token, entry
// script, argv) are injected as environment variables set as arguments to
// `env -i` (so they survive -i), not baked into the runner bytes. This keeps
// the runner deterministic and auditable and imposes no evidence-writing
// contract on the owner's scientific script.

import { createHash, randomBytes } from "node:crypto";
import { mkdtemp, mkdir, readFile, readdir, rename, rm, stat, writeFile } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { load as parseYaml } from "js-yaml";
import { genbioh100ConcurrencyCap } from "./slurm-policy.js";
import { RCLONE_TRANSFER_ARGS, resolveRcloneRemote, rcloneCopyfromCommand, runRemoteWithRetry } from "./transfer.js";

const HOME_DIR = typeof process.env.HOME === "string" && process.env.HOME.startsWith("/") ? process.env.HOME : "/Users/mdanh";
const DEFAULT_DIRECT_PROJECTS_DIR = join(HOME_DIR, ".dsh/profiles/desktop/genbio-h100-direct-projects");
const SSH_OPTIONS = ["-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "StrictHostKeyChecking=yes"];
const SAFE_NAME_RE = /^[a-z0-9][a-z0-9-]*$/u;
const SAFE_PATH_SEGMENT_RE = /^[A-Za-z0-9_.-]+$/u;
// argv literals: no whitespace, no newline, no shell metacharacters. Space-joined
// on the wire and re-split by the runner (unambiguous because no literal has a space).
const SAFE_ARGV_RE = /^[A-Za-z0-9_./:=+@,-]+$/u;
const MAX_FILES = 64;
const MAX_JOBS = 32;
const MAX_FETCH_FILES = 32;
const MAX_FETCH_TOTAL_BYTES = 50 * 1024 * 1024;
const MAX_ARGV = 32;
// Optional `provenance` block: pins the package/software version and any
// provenance facts (e.g. the radii set a workload was run against) so every
// direct run record is reproducible and auditable. Keys are safe identifiers;
// values are bounded printable ASCII (never shell-interpolated — recorded only
// in the manifest, the run record, and the public status).
const SAFE_PROVENANCE_KEY_RE = /^[A-Za-z0-9_.-]{1,64}$/u;
const SAFE_PROVENANCE_VALUE_RE = /^[\x20-\x7E]{1,256}$/u;
const MAX_PROVENANCE_ENTRIES = 32;
const RUNNER_NAME = "h100_direct_runner.sh";

// FIXED runner bytes (deterministic; pinned by RUNNER_SHA below). Records run
// identity (pid + token), execs the manifest entry script with the injected
// argv, and records its exit code. All dynamic inputs arrive via H100_* env vars.
const RUNNER_BYTES = `#!/bin/bash
set -uo pipefail
run_dir="\${H100_RUN_DIR:?H100_RUN_DIR missing}"
token="\${H100_RUN_TOKEN:?H100_RUN_TOKEN missing}"
entry="\${H100_ENTRY_SCRIPT:?H100_ENTRY_SCRIPT missing}"
if ! printf 'pid=%s\\ntoken=%s\\n' "$$" "$token" > "$run_dir/run_identity"; then
  printf 'RUNNER_INIT_FAIL\\n' > "$run_dir/exit_code" 2>/dev/null || true
  exit 78
fi
args=()
if [ -n "\${H100_ENTRY_ARGS:-}" ]; then
  read -r -a args <<< "\${H100_ENTRY_ARGS}"
fi
if ! cd -- "$run_dir"; then
  printf 'RUNNER_CDW_FAIL\\n' > "$run_dir/exit_code" 2>/dev/null || true
  exit 79
fi
/bin/bash --noprofile --norc -- "$entry" "\${args[@]}"
rc=$?
printf '%s\\n' "$rc" > "$run_dir/exit_code"
exit 0
`;
const RUNNER_SHA = createHash("sha256").update(RUNNER_BYTES).digest("hex");

function sha256(data) { return createHash("sha256").update(data).digest("hex"); }
function shellQuote(value) { const text = String(value); if (!/^[A-Za-z0-9_./:=+@,-]+$/u.test(text)) throw new Error(`unsafe fixed path/token: ${text}`); return `'${text}'`; }
function assertRelativePath(rel) {
  if (typeof rel !== "string" || rel.length === 0 || rel.startsWith("/")) throw new Error(`staged path must be relative: ${rel}`);
  for (const segment of rel.split("/")) if (!SAFE_PATH_SEGMENT_RE.test(segment) || segment === "..") throw new Error(`unsafe staged path segment: ${rel}`);
}
function strictRemoteH100(body) { return `ssh ${SSH_OPTIONS.join(" ")} -- genbioh100 ${JSON.stringify(body).replace(/\$/g, "\\$")}`; }
async function runLocal(shell, command, timeoutMs, signal) { const request = shell.resolve({ command, timeoutMs, signal }); const result = await shell.run(request); return { stdout: result.stdout?.text ?? "", stderr: result.stderr?.text ?? "", exitCode: result.exitCode ?? null, timedOut: result.timedOut === true }; }

function directEvidenceCommand(remoteRunDir, evidence, { includePid = false, includeTails = false } = {}) {
  const pidSection = includePid ? `; printf '== PID_ALIVE ==\\n'; pid=$(grep '^pid=' run_identity 2>/dev/null | cut -d= -f2 || echo 0); if test "$pid" != "0" && kill -0 "$pid" 2>/dev/null; then printf 'alive\\n'; else printf 'dead\\n'; fi` : "";
  const tails = includeTails ? `; printf '== STDOUT_TAIL ==\\n'; tail -c 4000 stdout.log 2>/dev/null || true; printf '== STDERR_TAIL ==\\n'; tail -c 2000 stderr.log 2>/dev/null || true` : "";
  return `set -u; cd -- ${shellQuote(remoteRunDir)} 2>/dev/null || { printf 'RUN_DIR_GONE=1\\n'; exit 0; }; printf '== IDENTITY ==\\n'; cat run_identity 2>/dev/null || printf 'missing\\n'; printf '== EXIT_CODE ==\\n'; cat exit_code 2>/dev/null || printf 'pending\\n'${pidSection}; printf '== CHECKSUM ==\\n'; if test -f ${shellQuote(evidence.checksumManifest)}; then if sha256sum -c -- ${shellQuote(evidence.checksumManifest)}; then printf 'CHECKSUM_OK\\n'; else printf 'CHECKSUM_FAIL\\n'; fi; else printf 'CHECKSUM_MISSING\\n'; fi; printf '== PASS_MARKER ==\\n'; if test -f ${shellQuote(evidence.passMarkerFile)}; then grep -Fxc ${shellQuote(evidence.marker)} ${shellQuote(evidence.passMarkerFile)} 2>/dev/null || true; else printf '0\\n'; fi${tails}`;
}

// Parse the status/recovery terminal-evidence response once. Both callers must
// apply the same token-bound evidence semantics; never trust a mismatched token.
function parseDirectEvidence(stdout) {
  const text = String(stdout);
  const section = (name) => text.split(`== ${name} ==`)[1]?.split(/\n== [A-Z0-9_]+ ==/u)[0]?.trim() ?? "";
  const identitySection = section("IDENTITY");
  const identityToken = identitySection.match(/token=([a-f0-9]{32})/u)?.[1] ?? null;
  const exitCodeRaw = section("EXIT_CODE") || "pending";
  let exitCode = null; let exitMarker = null;
  if (exitCodeRaw !== "pending") {
    const raw = exitCodeRaw.trim();
    if (/^[0-9]+$/u.test(raw)) exitCode = Number(raw);
    else if (/^RUNNER_[A-Z_]+$/u.test(raw)) exitMarker = raw;
    else exitMarker = "UNKNOWN_EXIT";
  }
  const checksumSection = section("CHECKSUM");
  const checksumOk = /(?:^|\n)CHECKSUM_OK(?:\n|$)/u.test(checksumSection) && !/(?:^|\n)CHECKSUM_(?:FAIL|MISSING)(?:\n|$)/u.test(checksumSection);
  const passMarkerCount = Number(section("PASS_MARKER").trim());
  const passMarkerOk = Number.isInteger(passMarkerCount) && passMarkerCount === 1;
  return { text, section, identityToken, exitCode, exitMarker, checksumOk, passMarkerCount, passMarkerOk, pidAlive: section("PID_ALIVE"), stdoutTail: section("STDOUT_TAIL").slice(-4000), stderrTail: section("STDERR_TAIL").slice(-2000), runDirGone: /^RUN_DIR_GONE=1$/mu.test(text) };
}

// Local clean-env bash -n: runs BEFORE any remote access, grant, or job.
function localCleanEnvBashSyntaxOk(text) {
  let result;
  try {
    result = spawnSync("/bin/bash", ["--noprofile", "--norc", "-n", "-"], { input: text, env: { PATH: "/usr/bin:/bin", BASH_ENV: "/dev/null" }, timeout: 5000, encoding: "utf8" });
  } catch (error) {
    throw new Error(`h100 direct: local bash syntax check failed closed: ${String(error?.message ?? error)}`);
  }
  if (result.error) throw new Error(`h100 direct: local bash syntax check unavailable (fail closed): ${String(result.error.code ?? result.error.message)}`);
  if (result.status !== 0) {
    const firstLine = String(result.stderr ?? "").trim().split(/\r?\n/u)[0] ?? "";
    throw new Error(`h100 direct: script fails local bash -n: ${firstLine || `exit ${result.status}`}`);
  }
}

/** Strict direct-manifest validation — anything malformed throws (fail closed). */
function parseDirectManifest(project, parsed) {
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error(`${project}: manifest must be a mapping`);
  const m = parsed;
  if (m.schema_version !== 1) throw new Error(`${project}: schema_version must be 1`);
  if (m.target !== "genbioh100") throw new Error(`${project}: target must be genbioh100`);
  if (typeof m.project !== "string" || !SAFE_NAME_RE.test(m.project) || m.project !== project) throw new Error(`${project}: project name must be kebab-case and match the manifest filename`);
  for (const key of ["local_root", "remote_root"]) {
    const value = m[key];
    if (typeof value !== "string" || !value.startsWith("/") || value.includes("..")) throw new Error(`${project}: ${key} must be an absolute path without ..`);
  }
  if (!Array.isArray(m.files) || m.files.length === 0 || m.files.length > MAX_FILES) throw new Error(`${project}: files must be a non-empty array of at most ${MAX_FILES} relative paths`);
  for (const rel of m.files) assertRelativePath(rel);
  const filesSet = new Set(m.files);
  if (!m.jobs || typeof m.jobs !== "object" || Array.isArray(m.jobs)) throw new Error(`${project}: jobs must be a mapping`);
  const jobSpecs = {}; let count = 0;
  for (const [name, spec] of Object.entries(m.jobs)) {
    if (!SAFE_NAME_RE.test(name)) throw new Error(`${project}: invalid job name: ${name}`);
    if (!spec || typeof spec !== "object" || Array.isArray(spec)) throw new Error(`${project}: job ${name} must be a mapping`);
    assertRelativePath(spec.script);
    if (!filesSet.has(spec.script)) throw new Error(`${project}: job ${name} script ${spec.script} must be listed in files`);
    const cpus = spec.cpus; const gpus = spec.gpus ?? 0; const memGb = spec.mem_gb;
    if (typeof cpus !== "number" || !Number.isInteger(cpus) || cpus < 1 || cpus > 16) throw new Error(`${project}: job ${name} cpus must be an integer 1..16`);
    if (gpus !== 0) throw new Error(`${project}: job ${name} must be CPU-only (gpus must be 0; the direct layer never assigns a GPU)`);
    if (typeof memGb !== "number" || !Number.isInteger(memGb) || memGb < 1 || memGb > 32) throw new Error(`${project}: job ${name} mem_gb must be an integer 1..32`);
    const argv = spec.argv ?? [];
    if (!Array.isArray(argv) || argv.length > MAX_ARGV) throw new Error(`${project}: job ${name} argv must be an array of at most ${MAX_ARGV} literals`);
    for (const a of argv) { if (typeof a !== "string" || a.length === 0 || !SAFE_ARGV_RE.test(a)) throw new Error(`${project}: job ${name} argv literal is unsafe: ${a}`); }
    const timeoutS = spec.timeout_s ?? 3600;
    if (typeof timeoutS !== "number" || !Number.isInteger(timeoutS) || timeoutS < 1 || timeoutS > 24 * 60 * 60) throw new Error(`${project}: job ${name} timeout_s must be an integer 1..86400`);
    jobSpecs[name] = Object.freeze({ script: spec.script, cpus, gpus, memGb, argv: Object.freeze(argv), timeoutS });
    count += 1;
  }
  if (count === 0 || count > MAX_JOBS) throw new Error(`${project}: jobs must define 1..${MAX_JOBS} entries`);
  // Direct completion is evidence-bound. The entry script must write these files
  // in its fresh run directory; status verifies them before declaring success.
  // A legacy-compatible manifest gets these safe, fixed defaults; it cannot
  // become completed unless both actual files exist and verify remotely.
  const evidenceSpec = m.completion_evidence ?? { checksum_manifest: "OUTPUT_SHA256.txt", pass_marker_file: "PYTIM_PASS" };
  if (!evidenceSpec || typeof evidenceSpec !== "object" || Array.isArray(evidenceSpec)) throw new Error(`${project}: completion_evidence must be a mapping`);
  const checksumManifest = evidenceSpec.checksum_manifest;
  const passMarkerFile = evidenceSpec.pass_marker_file;
  if (typeof checksumManifest !== "string" || typeof passMarkerFile !== "string") throw new Error(`${project}: completion_evidence requires checksum_manifest and pass_marker_file`);
  assertRelativePath(checksumManifest);
  assertRelativePath(passMarkerFile);
  if (checksumManifest === passMarkerFile) throw new Error(`${project}: completion evidence files must differ`);
  const completionEvidence = Object.freeze({ checksumManifest, passMarkerFile, marker: "PYTIM_PASS" });
  let fetchSpec = null;
  if (m.fetch !== undefined) {
    if (!m.fetch || typeof m.fetch !== "object" || Array.isArray(m.fetch)) throw new Error(`${project}: fetch must be a mapping`);
    const fetchMaxBytes = m.fetch.max_bytes;
    if (!Number.isInteger(fetchMaxBytes) || fetchMaxBytes < 1 || fetchMaxBytes > MAX_FETCH_TOTAL_BYTES) throw new Error(`${project}: fetch.max_bytes must be an integer 1..${MAX_FETCH_TOTAL_BYTES}`);
    if (typeof m.fetch.dest !== "string") throw new Error(`${project}: fetch.dest must be a relative path`);
    assertRelativePath(m.fetch.dest);
    if (!Array.isArray(m.fetch.files) || m.fetch.files.length === 0 || m.fetch.files.length > MAX_FETCH_FILES) throw new Error(`${project}: fetch.files must be a non-empty array of at most ${MAX_FETCH_FILES} relative paths`);
    for (const rel of m.fetch.files) assertRelativePath(rel);
    fetchSpec = Object.freeze({ maxBytes: fetchMaxBytes, dest: m.fetch.dest, files: Object.freeze(m.fetch.files) });
  }
  let provenance = null;
  if (m.provenance !== undefined) {
    if (!m.provenance || typeof m.provenance !== "object" || Array.isArray(m.provenance)) throw new Error(`${project}: provenance must be a mapping of safe key/value pins`);
    const entries = Object.entries(m.provenance);
    if (entries.length === 0 || entries.length > MAX_PROVENANCE_ENTRIES) throw new Error(`${project}: provenance must contain 1..${MAX_PROVENANCE_ENTRIES} pins`);
    const provenanceMap = {};
    for (const [key, value] of entries) {
      if (!SAFE_PROVENANCE_KEY_RE.test(key)) throw new Error(`${project}: provenance key is unsafe: ${key}`);
      if (typeof value !== "string" || !SAFE_PROVENANCE_VALUE_RE.test(value)) throw new Error(`${project}: provenance value for ${key} must be 1..256 printable characters`);
      provenanceMap[key] = value;
    }
    provenance = Object.freeze(provenanceMap);
  }
  return Object.freeze({ project: m.project, target: "genbioh100", localRoot: m.local_root, remoteRoot: m.remote_root, files: Object.freeze(m.files), jobs: Object.freeze(jobSpecs), completionEvidence, fetch: fetchSpec, provenance, description: typeof m.description === "string" ? m.description : "" });
}

async function loadDirectManifest(projectsDir, project) {
  if (typeof project !== "string" || !SAFE_NAME_RE.test(project)) throw new Error(`invalid project name: ${project}`);
  const path = join(projectsDir, `${project}.yaml`);
  let text;
  try { text = await readFile(path, "utf8"); } catch {
    let available = [];
    try { available = (await readdir(projectsDir)).filter((name) => name.endsWith(".yaml")).map((name) => name.replace(/\.yaml$/u, "")).sort(); } catch { /* dir unreadable */ }
    throw new Error(`unknown h100 direct project: ${project} (available: ${available.join(", ") || "none"})`);
  }
  return parseDirectManifest(project, parseYaml(text));
}

async function localFiles(manifest) {
  const files = []; let bytes = 0;
  for (const rel of manifest.files) {
    const path = join(manifest.localRoot, rel);
    const info = await stat(path);
    if (!info.isFile()) throw new Error(`${manifest.project}: staged path is not a regular file: ${rel}`);
    const content = await readFile(path);
    files.push({ rel, path, size: info.size, sha256: sha256(content), content });
    bytes += info.size;
  }
  if (bytes === 0) throw new Error(`${manifest.project}: package has zero total bytes`);
  return { files, bytes };
}

function parseChecksums(stdout) { const values = new Map(); for (const line of String(stdout).split(/\r?\n/u)) { const match = /^([a-f0-9]{64})\s+\*?([^\s]+)$/u.exec(line.trim()); if (match) values.set(match[2], match[1]); } return values; }

export function createH100DirectTools({ makeTool, requirePolicy, requireState, publicState, runRemote, shell, userQuestions, jobs, config, requireRemoteAccess, runRegistry }) {
  const projectsDir = typeof config?.h100DirectProjectsDir === "string" && config.h100DirectProjectsDir.length > 0 ? config.h100DirectProjectsDir : DEFAULT_DIRECT_PROJECTS_DIR;
  const logMaxBytes = Number(config?.logMaxBytes ?? 65536);

  // ── genbio_h100_direct_stage: integrity pre-check (non-allocation) ──────────
  const stageTool = makeTool("genbio_h100_direct_stage", "Stage one genbioh100 direct project's file set (owner-approved, rclone-only) into a fixed genbioh100 staging directory, verify every SHA-256, and run a bounded clean-environment bash -n on each entry script plus the fixed runner. No envelope and no launch; a package pre-check before genbio_h100_direct_job. Unknown/malformed projects fail closed.", { project: { type: "string", required: true } }, async (args, exec) => {
    const state = requireState(exec); requirePolicy();
    const manifest = await loadDirectManifest(projectsDir, String(args.project));
    const { files } = await localFiles(manifest);
    for (const item of files) if (item.rel.endsWith(".sh")) localCleanEnvBashSyntaxOk(item.content.toString("utf8"));
    localCleanEnvBashSyntaxOk(RUNNER_BYTES);
    await requireRemoteAccess("genbioh100", [{ root: manifest.remoteRoot, write: true }], exec, state);
    if (!userQuestions) throw new Error("material transfer requires the DSH user-question provider");
    const stagingRoot = `${manifest.remoteRoot}/staged`;
    const answer = await userQuestions.ask({ agent: exec.agent, signal: exec.signal, questions: [{ id: `genbio-h100-direct-${manifest.project}-stage`, header: "genbioh100 direct staging", question: `Stage ${manifest.project} will transfer ${files.length} file(s), ${files.reduce((s, f) => s + f.size, 0)} bytes, to genbioh100:${stagingRoot}. Approve this material transfer?`, options: [{ label: "Approve this transfer", description: "Stage the package into the fixed staging directory (idempotent)." }, { label: "Reject", description: "Do not transfer." }] }] });
    const selected = answer.answers?.find((item) => item.id === `genbio-h100-direct-${manifest.project}-stage`)?.selected ?? [];
    if (!selected.includes("Approve this transfer")) throw new Error("genbioh100 direct staging transfer was not approved");
    if (!jobs) throw new Error("background job registry is unavailable");
    const run = { runId: `genbioh100-h100-direct-${manifest.project}-stage-${Date.now()}`, target: "genbioh100", operation: `h100-direct-${manifest.project}-stage`, status: "running", startedAt: Date.now(), finishedAt: null, stdout: "", stderr: "", error: null, pid: null, jobId: null, resources: { cpus: 0, gpus: 0, memGb: null, concurrency: 1 }, policyHash: state.policy.hash, node: "genbioh100", partition: null, envelope: null, finalization: null, memory: { status: "not-finalized", error: null, openVikingSessionId: null, traceId: null } };
    run.jobId = jobs.start({ kind: "genbio-h100-direct", label: `genbioh100 direct ${manifest.project} stage`, owner: exec.agent, run: () => {
      const controller = new AbortController();
      const done = (async () => {
        try {
          const rcloneRemote = resolveRcloneRemote(config, "genbioh100");
          const init = await runRemoteWithRetry(runRemote, "genbioh100", strictRemoteH100(`set -eu; root=${shellQuote(stagingRoot)}; mkdir -p ${shellQuote(stagingRoot)}`), { ...exec, signal: controller.signal }, 30000);
          if (init.exitCode !== 0) throw new Error(`failed to create genbioh100 staging dir: ${init.stderr || init.stdout || init.exitCode}`);
          for (const item of files) {
            const rc = await runLocal(shell, `rclone copyto ${RCLONE_TRANSFER_ARGS.join(" ")} ${shellQuote(item.path)} ${shellQuote(`${rcloneRemote}:${stagingRoot}/${item.rel}`)}`, 120000, controller.signal);
            if (rc.exitCode !== 0) throw new Error(`staging rclone failed for ${item.rel}: ${rc.stderr || rc.stdout || rc.exitCode}`);
          }
          const remoteNames = files.map((item) => shellQuote(item.rel)).join(" ");
          const checksumResult = await runRemote("genbioh100", strictRemoteH100(`set -eu; cd -- ${shellQuote(stagingRoot)}; sha256sum ${remoteNames}`), { ...exec, signal: controller.signal }, 30000);
          if (checksumResult.exitCode !== 0) throw new Error(`remote SHA-256 verification failed: ${checksumResult.stderr || checksumResult.stdout}`);
          const remoteHashes = parseChecksums(checksumResult.stdout);
          for (const item of files) if (remoteHashes.get(item.rel) !== item.sha256) throw new Error(`checksum mismatch after staging: ${item.rel}`);
          run.stdout = `H100_DIRECT_STAGE_OK ${files.length} file(s)\n`.slice(-logMaxBytes);
          run.status = "completed";
          return { status: "completed", detail: "staged and verified" };
        } catch (error) {
          run.status = controller.signal.aborted ? "killed" : "failed";
          run.error = controller.signal.aborted ? null : String(error?.message ?? error);
          return { status: run.status, detail: run.error ?? "cancelled" };
        } finally { run.finishedAt = Date.now(); }
      })();
      return { cancel: (reason) => controller.abort(reason ?? "stage cancelled"), done, readOutput: () => { const text = [run.stdout, run.stderr && `[stderr]\n${run.stderr}`, run.error && `[error] ${run.error}`].filter(Boolean).join("\n"); run.stdout = ""; run.stderr = ""; return text; } };
    } });
    state.runs.push(run);
    if (state.runs.length > 50) state.runs.splice(0, state.runs.length - 50);
    return { ok: true, status: { ...publicState(state), started: run } };
  });

  // ── genbio_h100_direct_job: exact-once detached launch ──────────────────────
  const jobTool = makeTool("genbio_h100_direct_job", "Launch one genbioh100 direct project operation as a detached bash process after re-verifying the staged package. CPU-only only (gpus:0, never a GPU; CUDA left unset). Exact-once: 128-bit CSPRNG run identity, rclone+SHA-256 staging into a fresh per-run directory, local+remote clean-env bash -n, setsid detached launch. No retry on any side-effecting operation. Launch ambiguity is reconciling, never failed. Cancel is local-observer-only. Admission: active CPU-only run count <= concurrent_cpu_jobs (default 1 unless policy explicitly raises it) AND active CPU threads + this job <= FRESH machine nproc (read each admission; fail-closed).", { project: { type: "string", required: true }, operation: { type: "string", required: true } }, async (args, exec) => {
    const state = requireState(exec); const policy = requirePolicy();
    const manifest = await loadDirectManifest(projectsDir, String(args.project));
    const operation = String(args.operation);
    const jobSpec = manifest.jobs[operation];
    if (!jobSpec) throw new Error(`${manifest.project}: unknown operation ${operation} (available: ${Object.keys(manifest.jobs).join(", ")})`);
    // Phase 0a: policy validation (genbioh100 direct / no-login / GPU0-only).
    const h100 = policy.targets.genbioh100;
    if (!h100 || h100.surface !== "direct" || h100.login_shell !== false || h100.ssh_target !== "genbioh100") throw new Error("genbioh100 policy must be direct/no-login");
    if (JSON.stringify(h100.limits?.gpus_allowed) !== JSON.stringify([0])) throw new Error("genbioh100 must allow GPU 0 only");
    if (h100.hardware?.reserved_gpu !== 1 || h100.hardware?.protected_process !== "gpu_util") throw new Error("genbioh100 GPU 1 protection is required");
    // Phase 0b: envelope validation (target/node/partition-null/policy-hash,
    // per-job fit, and the CLASS concurrency cap + aggregate capacity).
    const envelope = state.envelope;
    if (!envelope) throw new Error("set the genbioh100 session envelope before launching a direct job");
    if (envelope.target !== "genbioh100" || envelope.node !== "genbioh100") throw new Error("genbio_h100_direct_job requires the genbioh100 session envelope (node genbioh100)");
    if (envelope.partition !== null) throw new Error("genbioh100 envelope partition must be null (direct, no scheduler)");
    if (envelope.policyHash !== undefined && state.policy?.hash != null && envelope.policyHash !== state.policy.hash) throw new Error("envelope policy hash is stale; re-set the envelope");
    if (jobSpec.cpus > envelope.maxCpus) throw new Error(`job ${operation} cpus ${jobSpec.cpus} exceed envelope maxCpus ${envelope.maxCpus}`);
    if (envelope.memGb === null || jobSpec.memGb > envelope.memGb) throw new Error(`job ${operation} mem_gb ${jobSpec.memGb} exceeds envelope memGb ${envelope.memGb} (envelope memGb must be non-null and sufficient)`);
    // CPU-only class concurrency cap (concurrent_cpu_jobs, default 1 until
    // explicitly raised by policy). Every direct job is CPU-only; the GPU class
    // (concurrent_gpu_jobs=1) is not used by this layer.
    const classCap = genbioh100ConcurrencyCap(policy, 0);
    if (envelope.concurrency > classCap) throw new Error(`envelope concurrency ${envelope.concurrency} exceeds the CPU-only class cap ${classCap}`);
    const effCap = Math.min(envelope.concurrency, classCap);
    const activeCpuRuns = state.runs.filter((run) => run.target === "genbioh100" && typeof run.operation === "string" && run.operation.startsWith("h100-direct-") && !run.operation.endsWith("-stage") && ["running", "reconciling"].includes(run.status));
    if (activeCpuRuns.length >= effCap) throw new Error(`aggregate capacity: ${activeCpuRuns.length} active CPU-only direct run(s) already at the class concurrency limit ${effCap}`);
    // Phase 0c: load local files + entry script; compute SHAs.
    const { files, bytes } = await localFiles(manifest);
    const entry = files.find((item) => item.rel === jobSpec.script);
    if (!entry) throw new Error(`${manifest.project}: entry script ${jobSpec.script} not found in files`);
    const entrySha = entry.sha256;
    // Phase 0d: local clean-env bash -n on entry + runner (BEFORE any remote).
    localCleanEnvBashSyntaxOk(entry.content.toString("utf8"));
    localCleanEnvBashSyntaxOk(RUNNER_BYTES);
    // Phase 0e: in-flight pair-lock (one concurrent launch per project:op).
    const inFlight = state.h100DirectInFlight ?? (state.h100DirectInFlight = new Map());
    const pairKey = `${manifest.project}:${operation}`;
    if (inFlight.has(pairKey)) throw new Error(`${pairKey} already has an in-flight launch; the pair-lock is held until it reconciles`);
    // Phase 0f: registry record (FAIL-CLOSED).
    const sessionId = exec.agent.session.id;
    let registryRunId = null;
    if (runRegistry) {
      try { const created = await runRegistry.record(sessionId, { source: "h100-direct", project: manifest.project, operation, status: "in-flight", note: "genbioh100 direct job admitted" }); registryRunId = created.runId; }
      catch (error) { throw new Error(`h100 direct registry record failed (fail-closed): ${String(error?.message ?? error)}`); }
    }
    // Phase 0g: remote access grant (AFTER local validation, BEFORE mutation).
    const runsRoot = `${manifest.remoteRoot}/runs`;
    await requireRemoteAccess("genbioh100", [{ root: manifest.remoteRoot, write: true }], exec, state);
    // Phase 0g2: FRESH machine nproc + aggregate total-CPU gate (read-only
    // remote). The sum of active CPU threads across ALL active CPU-only direct
    // runs plus this job must fit the machine's online core count, read FRESH
    // on each admission (never a stale or policy-assumed value). Fails closed
    // if nproc is unreadable. This is what stops a raised concurrency cap from
    // oversubscribing the cores — the run-count cap alone is NOT sufficient.
    const nprocResult = await runRemote("genbioh100", strictRemoteH100("set -eu; nproc"), exec, 15000);
    const nprocText = String(nprocResult.stdout).trim();
    const nproc = Number(nprocText);
    if (nprocResult.exitCode !== 0 || !Number.isInteger(nproc) || nproc < 1) throw new Error(`aggregate capacity: could not read a valid fresh nproc from genbioh100 (fail closed): exit ${nprocResult.exitCode}, stdout=${nprocText.slice(0, 80) || "(empty)"}`);
    const activeCpuCpus = activeCpuRuns.reduce((sum, run) => sum + (Number(run.resources?.cpus) || 0), 0);
    if (activeCpuCpus + jobSpec.cpus > nproc) throw new Error(`aggregate capacity: ${activeCpuCpus} active CPU thread(s) + this job's ${jobSpec.cpus} exceed the fresh machine nproc ${nproc}; a raised concurrency cap cannot oversubscribe the cores`);
    // Phase 0h: material-transfer approval (explicit, before rclone).
    if (!userQuestions) throw new Error("material transfer requires the DSH user-question provider");
    const transferId = `genbio-h100-direct-${manifest.project}-${operation}-transfer`;
    const answer = await userQuestions.ask({ agent: exec.agent, signal: exec.signal, questions: [{ id: transferId, header: "genbioh100 direct launch", question: `Direct job ${manifest.project}/${operation} will stage ${files.length} file(s) + the fixed runner (${bytes} bytes) to genbioh100:${runsRoot}/<run> and launch it detached. Approve this material transfer + launch?`, options: [{ label: "Approve this transfer", description: "Stage the package into a fresh verified per-run directory and launch it detached." }, { label: "Reject", description: "Do not stage or launch." }] }] });
    const selected = answer.answers?.find((item) => item.id === transferId)?.selected ?? [];
    if (!selected.includes("Approve this transfer")) {
      if (runRegistry && registryRunId) void runRegistry.update(sessionId, registryRunId, { status: "cancelled", note: "transfer rejected" }).catch(() => {});
      throw new Error("genbioh100 direct material transfer was not approved");
    }
    if (!jobs) throw new Error("background job registry is unavailable");
    // Phase 0i: 128-bit CSPRNG token + fresh run dir identity.
    const runToken = randomBytes(16).toString("hex");
    const runName = `${operation}-${runToken}`;
    const remoteRunDir = `${runsRoot}/${runName}`;
    const runTmp = await mkdtemp(join(tmpdir(), "dsh-h100-direct-"));
    const runnerLocal = join(runTmp, RUNNER_NAME);
    await writeFile(runnerLocal, RUNNER_BYTES, "utf8");
    const run = { runId: `genbioh100-h100-direct-${manifest.project}-${operation}-${runToken}`, target: "genbioh100", operation: `h100-direct-${manifest.project}-${operation}`, status: "running", startedAt: Date.now(), finishedAt: null, stdout: "", stderr: "", error: null, pid: null, jobId: null, resources: { cpus: jobSpec.cpus, gpus: jobSpec.gpus, memGb: jobSpec.memGb, concurrency: 1 }, policyHash: state.policy.hash, node: "genbioh100", partition: null, envelope: JSON.parse(JSON.stringify(envelope)), remoteRunDir, runToken, entryScript: jobSpec.script, entrySha, completionEvidence: { ...manifest.completionEvidence }, provenance: manifest.provenance ? { ...manifest.provenance } : null, pairKey, remoteGrants: JSON.parse(JSON.stringify(state.remoteGrants)), finalization: null, memory: { status: "not-finalized", error: null, openVikingSessionId: null, traceId: null }, registryRunId, registryError: null };
    inFlight.set(pairKey, run.runId);
    run.jobId = jobs.start({ kind: "genbio-h100-direct", label: `genbioh100 direct ${manifest.project}/${operation}`, owner: exec.agent, run: () => {
      const controller = new AbortController();
      const done = (async () => {
        let dispatched = false;
        try {
          // Step 1: fresh per-run directory (fail closed if it exists).
          dispatched = true;
          const initResult = await runRemote("genbioh100", strictRemoteH100(`set -u; runs_root=${shellQuote(runsRoot)}; run_dir=${shellQuote(remoteRunDir)}; if ! mkdir -p ${shellQuote(runsRoot)} 2>/dev/null; then printf 'RUNS_ROOT_ERROR=1\\n'; exit 2; fi; if test -e "$run_dir"; then printf 'RUN_DIR_EXISTS=1\\n'; exit 0; fi; if ! mkdir "$run_dir" 2>/dev/null; then printf 'RUN_DIR_MK_ERROR=1\\n'; exit 3; fi; printf 'RUN_DIR_READY=1\\n'`), { ...exec, signal: controller.signal }, 30000);
          const initOut = String(initResult.stdout);
          if (initResult.exitCode === 0 && initOut.includes("RUN_DIR_EXISTS=1")) {
            inFlight.delete(pairKey); if (runRegistry && registryRunId) void runRegistry.update(sessionId, registryRunId, { status: "cancelled", note: "run dir already exists (token collision); no launch" }).catch(() => {});
            run.status = "failed"; run.finishedAt = Date.now(); run.error = "run directory already exists (token collision); no launch occurred";
            return { status: "failed", detail: run.error };
          }
          if (initResult.exitCode !== 0) {
            const marker = ["RUNS_ROOT_ERROR=1", "RUN_DIR_MK_ERROR=1"].find((m) => initOut.includes(m));
            if (marker) {
              inFlight.delete(pairKey); if (runRegistry && registryRunId) void runRegistry.update(sessionId, registryRunId, { status: "failed", note: marker }).catch(() => {});
              run.status = "failed"; run.finishedAt = Date.now(); run.error = `run dir init failed (${marker})`;
              return { status: "failed", detail: run.error };
            }
            throw new Error(`run dir init transport failure: ${initResult.stderr || initOut || initResult.exitCode}`);
          }
          if (!initOut.includes("RUN_DIR_READY=1")) throw new Error(`run dir init: unexpected output: ${initOut.slice(0, 200)}`);
          // Step 2: stage files + runner via rclone (user-approved; NO RETRY).
          const rcloneRemote = resolveRcloneRemote(config, "genbioh100");
          for (const item of files) {
            const rc = await runLocal(shell, `rclone copyto ${RCLONE_TRANSFER_ARGS.join(" ")} ${shellQuote(item.path)} ${shellQuote(`${rcloneRemote}:${remoteRunDir}/${item.rel}`)}`, 120000, controller.signal);
            if (rc.exitCode !== 0) throw new Error(`staging rclone failed for ${item.rel}: ${rc.stderr || rc.stdout || rc.exitCode}`);
          }
          const rcRunner = await runLocal(shell, `rclone copyto ${RCLONE_TRANSFER_ARGS.join(" ")} ${shellQuote(runnerLocal)} ${shellQuote(`${rcloneRemote}:${remoteRunDir}/${RUNNER_NAME}`)}`, 120000, controller.signal);
          if (rcRunner.exitCode !== 0) throw new Error(`staging rclone failed for the fixed runner: ${rcRunner.stderr || rcRunner.stdout || rcRunner.exitCode}`);
          // Step 3: remote SHA-256 verification (every file + the runner).
          const allRels = [...files.map((item) => item.rel), RUNNER_NAME];
          const checksumResult = await runRemote("genbioh100", strictRemoteH100(`set -eu; cd -- ${shellQuote(remoteRunDir)}; sha256sum ${allRels.map(shellQuote).join(" ")}`), { ...exec, signal: controller.signal }, 30000);
          if (checksumResult.exitCode !== 0) throw new Error(`remote SHA-256 verification failed: ${checksumResult.stderr || checksumResult.stdout}`);
          const remoteHashes = parseChecksums(checksumResult.stdout);
          for (const item of files) if (remoteHashes.get(item.rel) !== item.sha256) throw new Error(`SHA-256 mismatch after staging: ${item.rel}`);
          if (remoteHashes.get(RUNNER_NAME) !== RUNNER_SHA) throw new Error(`fixed runner SHA-256 mismatch after staging: ${remoteHashes.get(RUNNER_NAME)} != ${RUNNER_SHA}`);
          // Step 4: remote clean-env bash -n on the entry script + runner.
          const syntaxResult = await runRemote("genbioh100", strictRemoteH100(`set -eu; cd -- ${shellQuote(remoteRunDir)}; env -i PATH=/usr/bin:/bin BASH_ENV=/dev/null /bin/bash --noprofile --norc -n -- ${shellQuote(jobSpec.script)}; env -i PATH=/usr/bin:/bin BASH_ENV=/dev/null /bin/bash --noprofile --norc -n -- ${shellQuote(RUNNER_NAME)}`), { ...exec, signal: controller.signal }, 30000);
          if (syntaxResult.exitCode !== 0) throw new Error(`remote clean-env bash -n failed: ${syntaxResult.stderr || syntaxResult.stdout}`);
          // Step 5: launch the runner detached (EXACT-ONCE, NO RETRY). Dynamic
          // inputs are env -i arguments so they survive -i. CPU-only:
          // CUDA_VISIBLE_DEVICES is NEVER set (CUDA left unset; no GPU
          // assigned). An empty argv injects no H100_ENTRY_ARGS at all (the
          // runner treats it as none).
          const argvEnv = jobSpec.argv.length > 0 ? ` H100_ENTRY_ARGS=${shellQuote(jobSpec.argv.join(" "))}` : "";
          const launchCmd = `set -eu; cd -- ${shellQuote(remoteRunDir)}; test "$(sha256sum ${shellQuote(jobSpec.script)} | cut -d' ' -f1)" = '${entrySha}'; test "$(sha256sum ${shellQuote(RUNNER_NAME)} | cut -d' ' -f1)" = '${RUNNER_SHA}'; setsid env -i PATH=/usr/bin:/bin:/usr/sbin:/sbin H100_RUN_DIR=${shellQuote(remoteRunDir)} H100_RUN_TOKEN=${shellQuote(runToken)} H100_ENTRY_SCRIPT=${shellQuote(jobSpec.script)}${argvEnv} OMP_NUM_THREADS=${shellQuote(String(jobSpec.cpus))} /bin/bash --noprofile --norc -- ${shellQuote(RUNNER_NAME)} < /dev/null > ${shellQuote(remoteRunDir + "/stdout.log")} 2> ${shellQuote(remoteRunDir + "/stderr.log")} & printf 'LAUNCH_RC=0\\n'; sleep 1; if test -f ${shellQuote(remoteRunDir + "/run_identity")}; then cat ${shellQuote(remoteRunDir + "/run_identity")}; else printf 'LAUNCH_AMBIGUITY=1\\n'; fi`;
          const launchResult = await runRemote("genbioh100", strictRemoteH100(launchCmd), { ...exec, signal: controller.signal }, 30000);
          if (launchResult.exitCode !== 0) {
            run.status = "reconciling"; run.error = `launch ssh returned exit ${launchResult.exitCode}: ${launchResult.stderr || launchResult.stdout}`; run.stdout = String(launchResult.stdout).slice(-4000);
            return { status: "reconciling", detail: "launch result ambiguous (ssh failure); run stays reconciling" };
          }
          const output = String(launchResult.stdout);
          if (output.includes("LAUNCH_AMBIGUITY=1")) {
            run.status = "reconciling"; run.error = "launch ambiguity: run_identity not found after 1s; process state unknown"; run.stdout = output.slice(-4000);
            return { status: "reconciling", detail: "launch ambiguous; run stays reconciling" };
          }
          const pidMatch = output.match(/pid=(\d+)/u);
          if (!pidMatch) {
            run.status = "reconciling"; run.error = "launch succeeded but PID/run_identity is missing or unparseable; treating as ambiguity"; run.stdout = output.slice(-4000);
            return { status: "reconciling", detail: "PID unparseable; run stays reconciling" };
          }
          run.pid = Number(pidMatch[1]);
          run.status = "running"; run.stdout = output.slice(-4000);
          return { status: "running", detail: `genbioh100 direct launched (pid=${run.pid}, run_dir=${remoteRunDir})` };
        } catch (error) {
          if (controller.signal.aborted) {
            run.status = "reconciling"; run.error = "local cancellation observed; remote state unknown; run stays reconciling";
            return { status: "reconciling", detail: "local cancel; remote state unknown" };
          }
          if (dispatched) {
            run.status = "reconciling"; run.error = String(error?.message ?? error);
            return { status: "reconciling", detail: run.error };
          }
          inFlight.delete(pairKey); if (runRegistry && registryRunId) void runRegistry.update(sessionId, registryRunId, { status: "cancelled", note: "pre-dispatch error" }).catch(() => {});
          run.status = "failed"; run.error = String(error?.message ?? error); run.finishedAt = Date.now();
          return { status: "failed", detail: run.error };
        } finally { await rm(runTmp, { recursive: true, force: true }).catch(() => {}); }
      })();
      // NOTE: the in-flight pair-lock is NOT cleared on post-dispatch paths. It
      // is held until genbio_h100_direct_status reconciles the run to terminal.
      return { cancel: (reason) => controller.abort(reason ?? "local cancel (remote state unknown)"), done, readOutput: () => { const text = [run.stdout, run.stderr && `[stderr]\n${run.stderr}`, run.error && `[error] ${run.error}`].filter(Boolean).join("\n"); run.stdout = ""; run.stderr = ""; return text; } };
    } });
    state.runs.push(run);
    if (state.runs.length > 50) state.runs.splice(0, state.runs.length - 50);
    return { ok: true, status: { ...publicState(state), started: run } };
  });

  // ── genbio_h100_direct_status: reconciliation ───────────────────────────────
  const statusTool = makeTool("genbio_h100_direct_status", "Check the status of a genbioh100 direct run. Requires run_id. Reads the run-bound identity (token match), exit_code, PID liveness, evidence checksum manifest, exactly-one PYTIM_PASS marker, and bounded stdout/stderr tails. Completed only on token match + exit 0 + sha256sum -c success + exactly one marker; failed on definitive non-zero exit or bad terminal evidence; remote outages and live processes stay reconciling. On a terminal verdict it releases the pair-lock and terminal-updates the registry.", { run_id: { type: "string", required: true } }, async (args, exec) => {
    const state = requireState(exec);
    const sessionId = exec.agent.session.id;
    const runId = String(args.run_id ?? "");
    if (!runId) throw new Error("genbio_h100_direct_status requires run_id");
    let run = state.runs.find((item) => item.runId === runId && item.target === "genbioh100" && typeof item.operation === "string" && item.operation.startsWith("h100-direct-") && !item.operation.endsWith("-stage"));
    if (!run) {
      const idMatch = /^genbioh100-h100-direct-([a-z0-9][a-z0-9-]*)-([a-z0-9][a-z0-9-]*)-([a-f0-9]{32})$/u.exec(runId);
      if (!idMatch) return { ok: true, status: { ...publicState(state), h100DirectStatus: { found: false, runId } } };
      const [, project, operation, token] = idMatch;
      let manifest;
      try { manifest = await loadDirectManifest(projectsDir, project); } catch { return { ok: true, status: { ...publicState(state), h100DirectStatus: { found: false, runId } } }; }
      const jobSpec = manifest.jobs[operation];
      const durable = runRegistry ? (await runRegistry.list(sessionId)).find((item) => item.source === "h100-direct" && item.project === project && item.operation === operation && item.status === "reconciling") : null;
      if (!jobSpec || !durable) return { ok: true, status: { ...publicState(state), h100DirectStatus: { found: false, runId } } };
      const remoteRunDir = `${manifest.remoteRoot}/runs/${operation}-${token}`;
      run = { runId, target: "genbioh100", operation: `h100-direct-${project}-${operation}`, status: "reconciling", startedAt: durable.startedAt, finishedAt: null, stdout: "", stderr: "", error: null, resources: { cpus: jobSpec.cpus, gpus: jobSpec.gpus, memGb: jobSpec.memGb, concurrency: 1 }, policyHash: state.policy.hash, node: "genbioh100", partition: null, envelope: null, remoteRunDir, runToken: token, completionEvidence: { ...manifest.completionEvidence }, pairKey: `${project}/${operation}`, registryRunId: durable.runId, registryError: null, recovered: true };
      state.runs.push(run);
      const inFlight = state.h100DirectInFlight ?? (state.h100DirectInFlight = new Set());
      inFlight.add(run.pairKey);
    }
    const remoteRunDir = run.remoteRunDir;
    if (!remoteRunDir) return { ok: true, status: { ...publicState(state), h100DirectStatus: { found: true, runId, error: "no remote run directory recorded" } } };
    await requireRemoteAccess("genbioh100", [{ root: dirname(remoteRunDir), write: false }], exec, state);
    const evidence = run.completionEvidence;
    if (!evidence?.checksumManifest || !evidence?.passMarkerFile) return { ok: true, status: { ...publicState(state), h100DirectStatus: { found: true, runId, reconciled: "reconciling", error: "run lacks the required completion-evidence contract" } } };
    const statusCmd = directEvidenceCommand(remoteRunDir, evidence, { includePid: true, includeTails: true });
    let result;
    try { result = await runRemote("genbioh100", strictRemoteH100(statusCmd), exec, Number(config?.commandTimeoutMs ?? 30000)); }
    catch (error) { return { ok: true, status: { ...publicState(state), h100DirectStatus: { found: true, runId, reconciled: "reconciling", error: `remote status check failed (outage): ${String(error?.message ?? error)}` } } }; }
    if (result.exitCode !== 0) return { ok: true, status: { ...publicState(state), h100DirectStatus: { found: true, runId, reconciled: "reconciling", error: `remote status check exit ${result.exitCode}: ${result.stderr || result.stdout}` } } };
    const evidenceResult = parseDirectEvidence(result.stdout);
    const { identityToken, exitCode, exitMarker, checksumOk, passMarkerOk, pidAlive, stdoutTail, stderrTail, runDirGone } = evidenceResult;
    const tokenMatch = identityToken === run.runToken;

    // Reconciliation (token-bound; never trust evidence that does not match):
    //   completed   : token + exit 0 + checksum verification + exactly one PYTIM_PASS
    //   failed      : token + non-zero/runner exit; or terminal exit 0 with bad evidence
    //   running     : token + live pid + exit still pending
    //   reconciling : token mismatch, incomplete success evidence, or lost exit code
    let reconciled;
    if (!tokenMatch) reconciled = "reconciling";
    else if (exitCode === 0 && checksumOk && passMarkerOk) reconciled = "completed";
    else if (exitCode === 0) reconciled = "failed";
    else if (exitCode !== null || exitMarker !== null) reconciled = "failed";
    else if (pidAlive === "alive") reconciled = "running";
    else reconciled = "reconciling";

    if (reconciled === "completed" || reconciled === "failed") {
      run.computeTerminalStatus = reconciled;
      run.finishedAt = run.finishedAt || Date.now();
      run.stdout = stdoutTail;
      run.stderr = stderrTail;
      run.error = reconciled === "failed" ? (stderrTail || `exit ${exitCode ?? exitMarker}`) : null;
      // Release the in-flight pair-lock and terminal-update the registry.
      if (run.pairKey) { const inFlight = state.h100DirectInFlight; if (inFlight) inFlight.delete(run.pairKey); }
      if (runRegistry && run.registryRunId) {
        try { await runRegistry.update(sessionId, run.registryRunId, { status: reconciled, note: `genbioh100 direct reconciled ${reconciled}` }); }
        catch (regErr) { run.registryError = `terminal registry update failed: ${String(regErr?.message ?? regErr)}`; }
      }
      run.status = reconciled;
    } else if (reconciled === "reconciling" && tokenMatch && run.status === "running") {
      // Token match but the process ended without an exit code: genuinely
      // ambiguous. A token MISMATCH must NOT demote the recorded run state —
      // the evidence is from a different run and is simply not trusted.
      run.status = "reconciling";
    }

    return { ok: true, status: { ...publicState(state), h100DirectStatus: { found: true, runId, tokenMatch, reconciled, exitCode, exitMarker, pidAlive, runDirGone, stdoutTail, stderrTail, runDir: remoteRunDir } } };
  });

  // ── genbio_h100_direct_fetch: bounded retrieval (rclone-only) ───────────────
  const fetchTool = makeTool("genbio_h100_direct_fetch", "Retrieve one genbioh100 direct project's allowlisted small report/artifact files (manifest fetch.files, byte-capped by fetch.max_bytes) from a verified completed run directory into the manifest's local fetch destination. Optional run_id permits fail-closed recovery after a DSH restart erased session state: its exact project/operation/token, identity, exit 0, checksum, and one marker are re-verified before retrieval. rclone-only download; every local SHA-256 must equal remote. Read-only on the remote; no allocation and no envelope is required. Omit files to retrieve every allowlisted file.", { project: { type: "string", required: true }, files: { type: "array", items: { type: "string" } }, run_id: { type: "string" } }, async (args, exec) => {
    const state = requireState(exec); requirePolicy();
    const manifest = await loadDirectManifest(projectsDir, String(args.project));
    const fetchSpec = manifest.fetch;
    if (!fetchSpec) throw new Error(`${manifest.project}: manifest has no fetch section; add fetch: { max_bytes, dest, files } to enable bounded retrieval`);
    const allowlist = new Set(fetchSpec.files);
    const requested = Array.isArray(args.files) ? args.files.map(String) : [];
    const rels = [...new Set(requested.length > 0 ? requested : fetchSpec.files)];
    const unknown = rels.filter((rel) => !allowlist.has(rel));
    if (unknown.length > 0) throw new Error(`${manifest.project}: fetch files not in the manifest allowlist: ${unknown.join(", ")} (allowlisted: ${fetchSpec.files.join(", ")})`);
    // Direct jobs write artifacts in fresh per-run directories, never in
    // manifest.remoteRoot itself. Normal fetch chooses the latest session-owned
    // completed run. A DSH restart clears this volatile state, so an explicit
    // run_id can recover ONLY one manifest-derived path after re-proving all
    // terminal evidence; recovery never enumerates/globs remote paths.
    const suppliedRunId = args.run_id;
    const operationPrefix = `h100-direct-${manifest.project}-`;
    const runPrefix = `${manifest.remoteRoot}/runs/`;
    let completedRun;
    let sourceRoot;
    let recovered = false;
    if (suppliedRunId !== undefined) {
      if (typeof suppliedRunId !== "string") throw new Error(`${manifest.project}: fetch run_id must be a string`);
      const runId = suppliedRunId;
      const escapedProject = manifest.project.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
      const recoveryMatch = new RegExp(`^genbioh100-h100-direct-${escapedProject}-(.+)-([a-f0-9]{32})$`, "u").exec(runId);
      const recoveryOperation = recoveryMatch?.[1] ?? null;
      const recoveryToken = recoveryMatch?.[2] ?? null;
      if (!recoveryOperation || !recoveryToken || !Object.hasOwn(manifest.jobs, recoveryOperation)) throw new Error(`${manifest.project}: fetch run_id is not an exact declared direct-run identity`);
      sourceRoot = `${manifest.remoteRoot}/runs/${recoveryOperation}-${recoveryToken}`;
      await requireRemoteAccess("genbioh100", [{ root: sourceRoot, write: false }], exec, state);
      const evidence = manifest.completionEvidence;
      const proof = await runRemote("genbioh100", strictRemoteH100(directEvidenceCommand(sourceRoot, evidence)), exec, 30000);
      if (proof.exitCode !== 0) throw new Error(`${manifest.project}: recovery terminal-evidence probe unavailable; no retrieval was performed: ${proof.stderr || proof.stdout || proof.exitCode}`);
      const evidenceResult = parseDirectEvidence(proof.stdout);
      if (evidenceResult.runDirGone) throw new Error(`${manifest.project}: supplied run_id run directory is absent; no retrieval was performed`);
      if (evidenceResult.identityToken !== recoveryToken) throw new Error(`${manifest.project}: supplied run_id identity token mismatch; no retrieval was performed`);
      if (evidenceResult.exitCode !== 0 || !evidenceResult.checksumOk || !evidenceResult.passMarkerOk) throw new Error(`${manifest.project}: supplied run_id has incomplete successful terminal evidence; no retrieval was performed`);
      completedRun = { runId, remoteRunDir: sourceRoot, recovered: true };
      recovered = true;
    } else {
      const completedRuns = state.runs.filter((run) =>
        run.target === "genbioh100" && typeof run.operation === "string" &&
        run.operation.startsWith(operationPrefix) && !run.operation.endsWith("-stage") &&
        run.status === "completed" && typeof run.remoteRunDir === "string" &&
        run.remoteRunDir.startsWith(runPrefix),
      );
      completedRuns.sort((a, b) => (Number(b.finishedAt) || 0) - (Number(a.finishedAt) || 0));
      completedRun = completedRuns[0];
      if (!completedRun) throw new Error(`${manifest.project}: no completed direct run is available for bounded fetch; supply its exact run_id to perform terminal-evidence-verified recovery after restart`);
      sourceRoot = completedRun.remoteRunDir;
      await requireRemoteAccess("genbioh100", [{ root: sourceRoot, write: false }], exec, state);
    }
    const forList = rels.map((rel) => shellQuote(rel)).join(" ");
    const discovery = await runRemoteWithRetry(runRemote, "genbioh100", strictRemoteH100(`set -eu; cd -- ${shellQuote(sourceRoot)}; for f in ${forList}; do if test -f "$f"; then printf 'OK|%s|%s|%s\\n' "$f" "$(stat -c %s "$f")" "$(sha256sum "$f" | cut -d' ' -f1)"; else printf 'MISSING|%s\\n' "$f"; fi; done`), exec, 30000);
    if (discovery.exitCode !== 0) throw new Error(`${manifest.project}: remote artifact discovery failed: ${discovery.stderr || discovery.stdout || discovery.exitCode}`);
    const discovered = new Map();
    for (const line of String(discovery.stdout).split(/\r?\n/u)) {
      const parts = line.split("|");
      if (parts.length === 2 && parts[0] === "MISSING") continue;
      if (parts.length !== 4 || parts[0] !== "OK") continue;
      const [, rel, sizeText, sha] = parts;
      if (Number.isInteger(Number(sizeText)) && Number(sizeText) >= 0 && /^[a-f0-9]{64}$/u.test(sha)) discovered.set(rel, { size: Number(sizeText), sha });
    }
    const notFound = rels.filter((rel) => !discovered.has(rel));
    if (notFound.length > 0) throw new Error(`${manifest.project}: remote artifacts missing or unreadable: ${notFound.join(", ")}`);
    const totalBytes = rels.reduce((sum, rel) => sum + discovered.get(rel).size, 0);
    if (totalBytes > fetchSpec.maxBytes) throw new Error(`${manifest.project}: fetch total ${totalBytes} bytes exceeds the manifest cap fetch.max_bytes=${fetchSpec.maxBytes}; raise the cap or select fewer files`);
    const destDir = join(manifest.localRoot, fetchSpec.dest);
    if (!userQuestions) throw new Error("material retrieval requires the DSH user-question provider");
    const answer = await userQuestions.ask({ agent: exec.agent, signal: exec.signal, questions: [{ id: `genbio-h100-direct-${manifest.project}-fetch`, header: "genbioh100 direct retrieval", question: `${manifest.project} retrieval will download ${rels.length} allowlisted file(s), ${totalBytes} bytes total, from completed run ${completedRun.runId} at genbioh100:${sourceRoot}${recovered ? " (terminal evidence re-verified after restart)" : ""} to the local destination ${destDir}. Approve this material retrieval?`, options: [{ label: "Approve this retrieval", description: "Download only the listed allowlisted files; each file must match its remote SHA-256 after transfer." }, { label: "Reject", description: "Do not retrieve anything." }] }] });
    const selected = answer.answers?.find((item) => item.id === `genbio-h100-direct-${manifest.project}-fetch`)?.selected ?? [];
    if (!selected.includes("Approve this retrieval")) throw new Error(`${manifest.project}: material retrieval was not explicitly approved`);
    const rcloneRemote = resolveRcloneRemote(config, "genbioh100");
    const errors = []; const transferred = [];
    for (const rel of rels) {
      const finalPath = join(destDir, rel);
      const tmpPath = `${finalPath}.part`;
      try {
        await mkdir(dirname(finalPath), { recursive: true });
        const result = await runLocal(shell, rcloneCopyfromCommand(rcloneRemote, `${sourceRoot}/${rel}`, tmpPath), 120000, exec.signal);
        if (result.exitCode !== 0) { errors.push(`${rel}: ${result.stderr || result.stdout || result.exitCode}`); continue; }
        const remote = discovered.get(rel);
        const localInfo = await stat(tmpPath);
        if (localInfo.size !== remote.size) { await rm(tmpPath, { force: true }); errors.push(`${rel}: size mismatch (remote ${remote.size}, local ${localInfo.size})`); continue; }
        const localSha = sha256(await readFile(tmpPath));
        if (localSha !== remote.sha) { await rm(tmpPath, { force: true }); errors.push(`${rel}: sha256 mismatch (remote ${remote.sha}, local ${localSha})`); continue; }
        await rename(tmpPath, finalPath);
        transferred.push({ rel, size: remote.size, sha256: remote.sha, localPath: finalPath });
      } catch (error) { try { await rm(tmpPath, { force: true }); } catch { /* never created or already removed */ } errors.push(`${rel}: ${String(error?.message ?? error)}`); }
    }
    if (errors.length > 0) throw new Error(`${manifest.project}: retrieval (rclone:${rcloneRemote}) failed: ${errors.join("; ")}`);
    const stamp = new Date().toISOString().replace(/[:.]/gu, "-");
    const receiptPath = join(destDir, `FETCH_RECEIPT_${stamp}.json`);
    await writeFile(receiptPath, JSON.stringify({ schema_version: 1, project: manifest.project, target: "genbioh100", fetched_at: new Date().toISOString(), source_root: sourceRoot, rclone_remote: rcloneRemote, transfer: "rclone-copyto", recovery: recovered ? { run_id: completedRun.runId, run_token: completedRun.runId.slice(-32), terminal_evidence_reverified: true } : null, files: transferred }, null, 2) + "\n", "utf8");
    return { ok: true, status: { ...publicState(state), h100DirectFetch: { project: manifest.project, bytes: totalBytes, receiptPath, files: transferred } } };
  });

  return { stageTool, jobTool, statusTool, fetchTool };
}

export { DEFAULT_DIRECT_PROJECTS_DIR, RUNNER_NAME, RUNNER_BYTES, RUNNER_SHA, parseDirectManifest };