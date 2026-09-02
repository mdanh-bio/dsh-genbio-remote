import { createHash } from "node:crypto";
import { readFile, stat } from "node:fs/promises";
import { join } from "node:path";
import { RCLONE_TRANSFER_ARGS, resolveRcloneRemote, runRemoteWithRetry } from "./transfer.js";
import { probeNodeHeadroom } from "./pinned.js";
import { validatePinnedSbatch } from "./slurm-policy.js";

const LOCAL_PROJECT_ROOT = "/Users/mdanh/Library/CloudStorage/OneDrive-Personal/Documents/research/DAE_enzyme";
const LOCAL_REMOTE_BUNDLE = join(LOCAL_PROJECT_ROOT, "workflow", "aizyme_v1", "remote");
const DEFAULT_LOCAL_REMOTE_BUNDLE = LOCAL_REMOTE_BUNDLE;
const REMOTE_PROJECT_ROOT = "/data01/genbiolab/mdanh/data/simulation/daes_enzyme";
const REMOTE_STAGE_ROOT = `${REMOTE_PROJECT_ROOT}/workflow/aizyme_v1_remote_runs`;
const SSH_OPTIONS = ["-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "StrictHostKeyChecking=yes"];
const SAFE_OPERATION_RE = /^[a-z0-9-]+$/u;

const STAGES = Object.freeze({
  "stage0-1": Object.freeze({ template: "stage0_1.sbatch", files: ["stage0_1.sbatch", "stage0_execution_contract.yaml"], cpus: 2, gpus: 0, concurrency: 1, timeoutMs: 30 * 60 * 1000, description: "authoritative WT input bootstrap and checksum manifest" }),
  stage2: Object.freeze({ template: "stage2_environment.sbatch", files: ["stage2_environment.sbatch", "AIzymes-52176ff.tar.gz"], cpus: 24, gpus: 1, concurrency: 1, timeoutMs: 60 * 60 * 1000, description: "HPC environment and tool validation" }),
  stage3: Object.freeze({ template: "stage3_chemistry.sbatch", files: ["stage3_chemistry.sbatch"], cpus: 8, gpus: 0, concurrency: 1, timeoutMs: 60 * 60 * 1000, description: "WT FUD/Co chemistry and constraint validation" }),
  stage4: Object.freeze({ template: "stage4_design_space.sbatch", files: ["stage4_design_space.sbatch"], cpus: 4, gpus: 0, concurrency: 1, timeoutMs: 30 * 60 * 1000, description: "branch-specific design-space derivation" }),
  stage5: Object.freeze({ template: "stage5_patch_tests.sbatch", files: ["stage5_patch_tests.sbatch", "AIzymes-patched.tar.gz"], cpus: 8, gpus: 0, concurrency: 1, timeoutMs: 60 * 60 * 1000, description: "patched AI.zymes tests and throwaway setup" }),
});

function sha256(data) { return createHash("sha256").update(data).digest("hex"); }
function shellQuote(value) { const text = String(value); if (!/^[A-Za-z0-9_./:=+@,-]+$/u.test(text)) throw new Error(`unsafe fixed path/token: ${text}`); return `'${text}'`; }
function assertOperation(operation) { if (!SAFE_OPERATION_RE.test(operation) || !Object.hasOwn(STAGES, operation)) throw new Error(`unsupported AI.zymes stage: ${operation}`); }

async function localFiles(operation, bundleRoot = DEFAULT_LOCAL_REMOTE_BUNDLE) {
  const stage = STAGES[operation]; const files = []; let bytes = 0;
  for (const name of stage.files) {
    if (!/^[A-Za-z0-9_.-]+$/u.test(name)) throw new Error(`invalid staged filename: ${name}`);
    const path = join(bundleRoot, name); const info = await stat(path);
    if (!info.isFile()) throw new Error(`staged path is not a regular file: ${path}`);
    const content = await readFile(path); files.push({ name, path, size: info.size, sha256: sha256(content), content }); bytes += info.size;
  }
  return { stage, files, bytes };
}

async function askTransfer(userQuestions, exec, operation, files, bytes) {
  if (!userQuestions) throw new Error("material AI.zymes transfer requires the DSH user-question provider");
  const answer = await userQuestions.ask({ agent: exec.agent, signal: exec.signal, questions: [{ id: `genbio-aizyme-transfer-${operation}`, header: "AI.zymes HPC staging", question: `Stage ${operation} will transfer ${files.length} allowlisted file(s), ${bytes} logical bytes, to the fixed authoritative HPC workflow path. Approve this material transfer?`, options: [{ label: "Approve this transfer", description: "Transfer only the listed allowlisted files into a fresh collision-checked run directory." }, { label: "Reject", description: "Do not transfer or allocate this stage." }] }] });
  const selected = answer.answers?.find((item) => item.id === `genbio-aizyme-transfer-${operation}`)?.selected ?? [];
  if (!selected.includes("Approve this transfer")) throw new Error("AI.zymes material transfer was not explicitly approved");
}

function validateEnvelope(state, stage) {
  const envelope = state.envelope;
  if (!envelope) throw new Error("set the HPC session envelope before running an AI.zymes stage");
  if (envelope.target !== "HPC" || envelope.partition !== "gpus") throw new Error("AI.zymes stages require an HPC/gpus envelope");
  // Policy-hash binding (t11 / t9 advisory B): the same staleness rejection
  // as the pinned path (pinned.js validateEnvelope) — an envelope recorded
  // under an older policy generation is stale and is rejected BEFORE any side
  // effect (transfer approval, remote access, background job, shell, SSH).
  if (envelope.policyHash !== undefined && state.policy?.hash != null && envelope.policyHash !== state.policy.hash) throw new Error("envelope policy hash is stale (policy changed after the envelope was set); re-set the envelope");
  if (stage.cpus > envelope.maxCpus || stage.gpus > envelope.maxGpus || stage.concurrency > envelope.concurrency) throw new Error(`AI.zymes ${stage.description} exceeds the current HPC envelope`);
}

async function runLocal(shell, command, timeoutMs, signal) { const request = shell.resolve({ command, timeoutMs, signal }); const result = await shell.run(request); return { stdout: result.stdout?.text ?? "", stderr: result.stderr?.text ?? "", exitCode: result.exitCode ?? null, signal: result.signal ?? null, timedOut: result.timedOut === true }; }
function parseChecksums(stdout) { const values = new Map(); for (const line of String(stdout).split(/\r?\n/u)) { const match = /^([a-f0-9]{64})\s+\*?([^\s]+)$/u.exec(line.trim()); if (match) values.set(match[2], match[1]); } return values; }

// rclone transfer (user-approved 2026-08-24 after repeated scp/ssh connection
// timeouts on the HPC route): one persistent SFTP session per file with bounded
// internal retries. The remote sha256 gate that follows remains the integrity
// authority and fails closed if rclone no-ops or truncates.
// Since 2026-08-24 this is the GLOBAL plugin transfer policy: the shared
// implementation (resolveRcloneRemote / runRemoteWithRetry) lives in
// lib/transfer.js and is used by every staging module — scp is never used.

function inspectionCommand() {
  return `set -eu; root=${shellQuote(REMOTE_PROJECT_ROOT)}; test -d "$root"; printf 'ROOT=%s\\n' "$root"; hostname -f; printf 'DATE=%s\\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"; df -h "$root"; df -i "$root"; printf '\\nAIzymes_COMMIT\\n'; git -C "$root/aizyme_v1/AIzymes" rev-parse HEAD 2>&1 || true; printf '\\nREQUIRED_ARTIFACTS\\n'; for path in data/01c_g1_pilot/qc/moxdae_postrelax/moxdae_postrelax_qc.json data/01_pipeline_pilot/ligand_params/FUD.params reports/stage0/stage1b/stage_1b_substrate_block.md; do if test -f "$root/$path"; then sha256sum "$root/$path"; else printf 'MISSING %s\\n' "$root/$path"; fi; done; printf '\\nWORKFLOW_TARGET\\n'; if test -e "$root/workflow/aizyme_v1"; then printf 'EXISTS %s\\n' "$root/workflow/aizyme_v1"; else printf 'ABSENT %s\\n' "$root/workflow/aizyme_v1"; fi`;
}

// Same local-expansion guard as index.js's preflight/launch assembly: JSON.stringify
// emits a double-quoted shell argument, so an unescaped $ would be expanded by the
// LOCAL shell before ssh transmits it ($(sbatch ...) would run locally, $job_id
// would vanish). "\$" becomes a literal $ in the transmitted body; remote-side
// expansion then happens only on the target.
function strictRemote(target, body) { return `ssh ${SSH_OPTIONS.join(" ")} -- ${target} ${JSON.stringify(body).replace(/\$/g, "\\$")}`; }

// Digest-binding chain (t10): the admission gate validates the EXACT
// in-memory template bytes with the shared fail-closed Phase-0 matrix and
// returns their SHA-256 (templateSha). Every later step re-proves the bytes:
// the remote validation command requires the staged template to hash to
// templateSha before any syntax check, the local checksum gate requires every
// remote hash to equal the in-memory digest, and the submit command requires
// the template to still hash to templateSha immediately before sbatch. Any
// mutation between validation and submission therefore fails closed.
async function stageAndRun({ operation, files, bytes, templateSha, exec, state, userQuestions, shell, runRemote, config }) {
  assertOperation(operation); const stage = STAGES[operation]; await askTransfer(userQuestions, exec, operation, files, bytes);
  const runName = `${operation}-${Date.now()}`; const remoteRunDir = `${REMOTE_STAGE_ROOT}/${runName}`;
  const remoteInit = await runRemoteWithRetry(runRemote, "HPC", strictRemote("HPC", `set -eu; test ! -e ${shellQuote(remoteRunDir)}; mkdir -p ${shellQuote(remoteRunDir)}`), exec, 30000); if (remoteInit.exitCode !== 0) throw new Error(`failed to create fresh remote staging directory: ${remoteInit.stderr || remoteInit.stdout || remoteInit.exitCode}`);
  const rcloneRemote = resolveRcloneRemote(config, "HPC");
  const transferLog = [];
  for (const item of files) {
    const rc = await runLocal(shell, `rclone copyto ${RCLONE_TRANSFER_ARGS.join(" ")} ${shellQuote(item.path)} ${shellQuote(`${rcloneRemote}:${remoteRunDir}/${item.name}`)}`, 120000, exec.signal);
    if (rc.exitCode !== 0) throw new Error(`AI.zymes staging transfer (rclone) failed for ${item.name}: ${rc.stderr || rc.stdout || rc.exitCode}`);
    transferLog.push(`${item.name}: transfer=ok (integrity: remote sha256 gate below)`);
  }
  const staged = { exitCode: 0, stdout: `transfer=rclone:${rcloneRemote}\n${transferLog.join("\n")}`, stderr: "" };
  // Remote validation: per-file hashes, then the staged template must hash to
  // the validated digest BEFORE the fixed clean-environment syntax check
  // (t10: same exact form as the pinned path — fixed /bin/bash, cleared
  // environment, no profile/rc, explicit operand separator).
  const remoteNames = files.map((item) => shellQuote(item.name)).join(" ");
  const checksumResult = await runRemoteWithRetry(runRemote, "HPC", strictRemote("HPC", `set -eu; cd -- ${shellQuote(remoteRunDir)}; sha256sum ${remoteNames}; test "$(sha256sum ${shellQuote(stage.template)} | cut -d' ' -f1)" = '${templateSha}'; env -i PATH=/usr/bin:/bin BASH_ENV=/dev/null /bin/bash --noprofile --norc -n -- ${shellQuote(stage.template)}`), exec, 30000); if (checksumResult.exitCode !== 0) throw new Error(`remote staged-file validation failed: ${checksumResult.stderr || checksumResult.stdout || checksumResult.exitCode}`);
  const remoteChecksums = parseChecksums(checksumResult.stdout); for (const item of files) if (remoteChecksums.get(item.name) !== item.sha256) throw new Error(`checksum mismatch after staging: ${item.name}`);
  // Submit: the node state/headroom probe (t10) runs as ITS OWN bounded
  // read-only remote call immediately before the submit call — probe evidence
  // and the submit act stay separately observable and parseable, and any probe
  // failure throws before sbatch, so no allocation is issued. The template
  // must still hash to the validated digest immediately before sbatch
  // (remote-side TOCTOU closure), then one fresh HPC job is submitted.
  const targetNode = state.envelope?.node;
  if (!targetNode) throw new Error("AI.zymes staging requires a session envelope with a node set");
  const probe = await probeNodeHeadroom({ exec, runRemote, cpus: stage.cpus, gpus: stage.gpus, node: targetNode });
  const submitCommand = `set -eu; cd -- ${shellQuote(remoteRunDir)}; test "$(sha256sum ${shellQuote(stage.template)} | cut -d' ' -f1)" = '${templateSha}'; job_id=$(sbatch --parsable ${shellQuote(stage.template)}); printf 'JOB_ID=%s\\nRUN_DIR=%s\\n' "$job_id" ${shellQuote(remoteRunDir)}; i=0; while test "$i" -lt 1800 && squeue -h -j "$job_id" | grep -q .; do sleep 2; i=$((i+1)); done; sacct -X -j "$job_id" --format=JobIDRaw,State,ExitCode -P; state=$(sacct -X -n -j "$job_id" --format=State -P | head -1 | cut -d+ -f1); exit_code=$(sacct -X -n -j "$job_id" --format=ExitCode -P | head -1); test "$state" = COMPLETED; test "$exit_code" = 0:0`;
  const result = await runRemote("HPC", strictRemote("HPC", submitCommand), exec, stage.timeoutMs); return { stage, files, bytes, remoteRunDir, checksumResult, staged, probe, result };
}

export function createAizymeTools({ makeTool, requirePolicy, requireState, publicState, runRemote, shell, userQuestions, jobs, config, requireRemoteAccess, runRegistry }) {
  const inspect = makeTool("genbio_aizyme_inspect", "Run a fixed read-only inspection of the authoritative DAE project tree for AI.zymes Stages 0–5. No files are changed and no allocation is submitted.", {}, async (_args, exec) => { const state = requireState(exec); requirePolicy(); await requireRemoteAccess("HPC", [{ root: REMOTE_PROJECT_ROOT, write: false }], exec, state); const result = await runRemote("HPC", strictRemote("HPC", inspectionCommand()), exec, Number(config.commandTimeoutMs ?? 30000)); state.lastError = result.exitCode === 0 ? null : result.stderr || `inspection exit ${result.exitCode}`; return { ok: result.exitCode === 0, status: { ...publicState(state), inspection: result } }; });
  const stageTool = makeTool("genbio_aizyme_stage", "Run one allowlisted, policy-checked AI.zymes preparation stage on authoritative HPC. Transfers only fixed local bundle files, validates checksums and bash syntax, submits one fresh HPC Slurm job, and waits for terminal evidence.", { operation: { type: "string", required: true, enum: Object.keys(STAGES) } }, async (args, exec) => {
    const state = requireState(exec); const policy = requirePolicy(); if (!policy.targets.HPC?.allowlist) throw new Error("active policy missing HPC allowlist"); const operation = String(args.operation); assertOperation(operation); const stage = STAGES[operation]; validateEnvelope(state, stage);
    // In-flight stage guard (t10 follow-up 1, same class as the pinned
    // admission lock): at most one concurrent run per stage. Set
    // synchronously at admission (before the first await), released when the
    // background run settles (finally) or when admission fails (catch).
    const inFlightStages = state.aizymeInFlight ?? (state.aizymeInFlight = new Set()); if (inFlightStages.has(operation)) throw new Error(`AI.zymes stage ${operation} already has an in-flight run; wait for it to settle before starting another`); inFlightStages.add(operation);
    try {
    // Phase 2 durable-registry mirror (best-effort; the compute path is
    // UNCHANGED): the stage lifecycle is recorded in the same registry/status
    // shape as every other Genbio run. A registry failure is surfaced on the
    // run record (run.registryError) but never aborts or alters the stage.
    // Pair lock identity remains project+operation: project "aizyme",
    // operation = the stage name.
    const sessionId = exec.agent.session.id;
    let registryRunId = null;
    let registryError = null;
    if (runRegistry) {
      try {
        const created = await runRegistry.record(sessionId, { source: "aizyme", project: "aizyme", operation, status: "in-flight", note: `AI.zymes stage ${operation} admitted` });
        registryRunId = created.runId;
      } catch (error) { registryError = String(error?.message ?? error).slice(0, 300); }
    }
    // Shared pre-side-effect validation (t10): before transfer approval, the
    // remote access grant, or any background job, the EXACT in-memory
    // template bytes are validated with the same fail-closed Phase-0 matrix
    // as the pinned path; their SHA-256 anchors the stage's digest binding.
    const { files, bytes } = await localFiles(operation, typeof config.aizymeBundleDir === "string" && config.aizymeBundleDir.length > 0 ? config.aizymeBundleDir : DEFAULT_LOCAL_REMOTE_BUNDLE);
    const templateContent = files.find((item) => item.name === stage.template).content;
    const templateSha = sha256(templateContent);
    validatePinnedSbatch(templateContent.toString("utf8"), { template: stage.template, cpus: stage.cpus, gpus: stage.gpus, concurrency: stage.concurrency }, { policy, envelope: state.envelope });
    await requireRemoteAccess("HPC", [{ root: REMOTE_PROJECT_ROOT, write: true }], exec, state); if (!jobs) throw new Error("background job registry is unavailable");
    const run = { runId: `HPC-aizyme-${operation}-${Date.now()}`, target: "HPC", operation: `aizyme-${operation}`, status: "running", startedAt: Date.now(), finishedAt: null, stdout: "", stderr: "", error: null, pid: null, jobId: null, resources: { cpus: stage.cpus, gpus: stage.gpus, memGb: null, concurrency: stage.concurrency }, policyHash: state.policy.hash, node: state.envelope.node, partition: state.envelope.partition, envelope: JSON.parse(JSON.stringify(state.envelope)), finalization: null, memory: { status: "not-finalized", error: null, openVikingSessionId: null, traceId: null }, registryRunId, registryError };
    run.jobId = jobs.start({ kind: "genbio-HPC-aizyme", label: `HPC AI.zymes ${operation}`, owner: exec.agent, run: () => { const controller = new AbortController(); const done = (async () => { try { const outcome = await stageAndRun({ operation, files, bytes, templateSha, exec: { ...exec, signal: controller.signal }, state, userQuestions, shell, runRemote, config }); run.stdout = [outcome.result.stdout, outcome.checksumResult.stdout].filter(Boolean).join("\n").slice(-Number(config.logMaxBytes ?? 65536)); run.stderr = [outcome.result.stderr, outcome.checksumResult.stderr, outcome.staged.stderr].filter(Boolean).join("\n").slice(-Number(config.logMaxBytes ?? 65536)); run.remoteRunDir = outcome.remoteRunDir; run.status = outcome.result.exitCode === 0 ? "completed" : "failed"; run.error = run.status === "completed" ? null : run.stderr || `exit ${outcome.result.exitCode}`; return { status: run.status, detail: run.error ?? `exit code: ${outcome.result.exitCode}` }; } catch (error) { run.status = controller.signal.aborted ? "killed" : "failed"; run.error = controller.signal.aborted ? null : String(error?.message ?? error); return { status: run.status, detail: run.error ?? "cancelled" }; } finally { run.finishedAt = Date.now(); inFlightStages.delete(operation); if (runRegistry && registryRunId) { void runRegistry.update(sessionId, registryRunId, { status: run.status, note: `AI.zymes stage ${operation} terminal (${run.status})` }).catch((error) => { run.registryError = String(error?.message ?? error).slice(0, 300); }); } } })(); return { cancel: (reason) => controller.abort(reason ?? "AI.zymes stage cancelled"), done, readOutput: () => { const text = [run.stdout, run.stderr && `[stderr]\n${run.stderr}`, run.error && `[error] ${run.error}`].filter(Boolean).join("\n"); run.stdout = ""; run.stderr = ""; return text; } }; } });
    state.runs.push(run); if (state.runs.length > 50) state.runs.splice(0, state.runs.length - 50); return { ok: true, status: { ...publicState(state), started: run } };
    } catch (error) { inFlightStages.delete(operation); throw error; }
  });
  return { inspect, stageTool };
}

export { LOCAL_PROJECT_ROOT, LOCAL_REMOTE_BUNDLE, REMOTE_PROJECT_ROOT, STAGES };
