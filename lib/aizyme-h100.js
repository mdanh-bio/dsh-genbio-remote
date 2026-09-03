// ── AI.zymes H100 Stage-2: bash-native direct execution on genbioh100 ──────
// Unlike the HPC path (Slurm batch jobs), genbioh100 runs a detached bash
// process directly. This module implements:
//
// SAFEGUARDS (all blocking findings addressed):
//   - 128-bit CSPRNG token (randomBytes(16)) for run identity — never Date.now()
//   - Remote pair/launch lock: one launch only; no retry for side-effecting ops
//   - Registry/pair-lock is FAIL-CLOSED: conflict/failure aborts the launch
//   - Lock/in-flight is held while the remote run is running or ambiguous;
//     it is NOT cleared in the launch finally block
//   - Explicit material-transfer user approval before any rclone transfer
//   - Local clean-env bash -n BEFORE any remote access, grant, or job
//   - requireRemoteAccess AFTER all local validation, BEFORE mutation
//   - Exact policy caps: direct surface, no-login, GPU0 only, partition null,
//     memory non-null and exactly sufficient, aggregate active-run capacity
//   - Launch ambiguity (transport loss, unparseable output) → "reconciling",
//     NEVER "failed" and NEVER retryable
//   - Cancellation is local-observer-only: it does NOT claim remote
//     cancellation; the run stays "reconciling" and the lock is held
//   - Status tool requires run_id; remote access coverage checked;
//     completed only on exit 0 + missing=0 + checksum -c + run-bound G2 +
//     exactly one STAGE2_PASS. Remote outages → "reconciling", not "failed".

import { createHash, randomBytes } from "node:crypto";
import { readFile, stat } from "node:fs/promises";
import { join } from "node:path";
import { spawnSync } from "node:child_process";
import { RCLONE_TRANSFER_ARGS, resolveRcloneRemote } from "./transfer.js";

const HOME_DIR = typeof process.env.HOME === "string" && process.env.HOME.startsWith("/") ? process.env.HOME : "/Users/mdanh";
const DEFAULT_LOCAL_REMOTE_BUNDLE = join(HOME_DIR, "Library", "CloudStorage", "OneDrive-Personal", "Documents", "research", "DAE_enzyme", "workflow", "aizyme_v1", "remote");
const SSH_OPTIONS = ["-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "StrictHostKeyChecking=yes"];

// genbioh100 fixed paths (root MUST be /home/work/GenbioLAB/shared/daes_enzyme)
const H100_PROJECT_ROOT = "/home/work/GenbioLAB/shared/daes_enzyme";
const H100_STAGE_ROOT = `${H100_PROJECT_ROOT}/workflow/aizyme_v1/runs/stage2`;
const H100_SCRIPT_NAME = "stage2_environment_genbioh100.sh";
const H100_ARCHIVE_NAME = "AIzymes-52176ff.tar.gz";
const H100_ARCHIVE_SHA = "f408113ab7c2fbbfa2771312d0c0eaae308565da521f0d01f713ddc04441a70a";
const H100_FILES = Object.freeze([H100_SCRIPT_NAME, H100_ARCHIVE_NAME]);

// Resource caps: 16 CPU, 1 GPU (GPU 0), 32 GB, 1 concurrency
const H100_STAGE2_RESOURCES = Object.freeze({ cpus: 16, gpus: 1, memGb: 32, concurrency: 1 });

function sha256(data) { return createHash("sha256").update(data).digest("hex"); }
function shellQuote(value) { const text = String(value); if (!/^[A-Za-z0-9_./:=+@,-]+$/u.test(text)) throw new Error(`unsafe fixed path/token: ${text}`); return `'${text}'`; }

function strictRemoteH100(body) {
  return `ssh ${SSH_OPTIONS.join(" ")} -- genbioh100 ${JSON.stringify(body).replace(/\$/g, "\\$")}`;
}

async function loadLocalFiles(bundleRoot = DEFAULT_LOCAL_REMOTE_BUNDLE) {
  const files = [];
  let bytes = 0;
  for (const name of H100_FILES) {
    if (!/^[A-Za-z0-9_.-]+$/u.test(name)) throw new Error(`invalid staged filename: ${name}`);
    const path = join(bundleRoot, name);
    const info = await stat(path);
    if (!info.isFile()) throw new Error(`staged path is not a regular file: ${path}`);
    const content = await readFile(path);
    files.push({ name, path, size: info.size, sha256: sha256(content), content });
    bytes += info.size;
  }
  return { files, bytes };
}

// Local clean-env bash -n: BEFORE any remote access, grant, or job.
function localBashSyntaxOk(text) {
  let result;
  try {
    result = spawnSync("/bin/bash", ["--noprofile", "--norc", "-n", "-"], { input: text, env: { PATH: "/usr/bin:/bin", BASH_ENV: "/dev/null" }, timeout: 5000, encoding: "utf8" });
  } catch (error) {
    throw new Error(`H100 stage2: local bash syntax check failed closed: ${String(error?.message ?? error)}`);
  }
  if (result.error) throw new Error(`H100 stage2: local bash syntax check unavailable (fail closed): ${String(result.error.code ?? result.error.message)}`);
  if (result.status !== 0) {
    const firstLine = String(result.stderr ?? "").trim().split(/\r?\n/u)[0] ?? "";
    throw new Error(`H100 stage2: script fails local bash -n: ${firstLine || `exit ${result.status}`}`);
  }
}

// Exact policy validation for genbioh100 stage2.
function validateH100Policy(policy) {
  const h100 = policy.targets.genbioh100;
  if (!h100) throw new Error("genbioh100 target missing from policy");
  if (h100.surface !== "direct") throw new Error("genbioh100 policy must use surface=direct");
  if (h100.login_shell !== false) throw new Error("genbioh100 policy must set login_shell=false");
  if (h100.ssh_target !== "genbioh100") throw new Error("genbioh100 ssh_target must be 'genbioh100'");
  const limits = h100.limits;
  if (!limits) throw new Error("genbioh100 policy must define limits");
  if (JSON.stringify(limits.gpus_allowed) !== JSON.stringify([0])) throw new Error("genbioh100 must allow GPU 0 only");
  if (limits.cpu_threads_per_job !== H100_STAGE2_RESOURCES.cpus) throw new Error(`genbioh100 cpu_threads_per_job must be ${H100_STAGE2_RESOURCES.cpus}`);
  if (limits.mem_gb_per_job !== H100_STAGE2_RESOURCES.memGb) throw new Error(`genbioh100 mem_gb_per_job must be ${H100_STAGE2_RESOURCES.memGb}`);
  if (limits.concurrent_gpu_jobs !== H100_STAGE2_RESOURCES.concurrency) throw new Error(`genbioh100 concurrent_gpu_jobs must be ${H100_STAGE2_RESOURCES.concurrency}`);
}

// Envelope validation: exact fit, partition null, memory non-null.
function validateH100Envelope(state, policy) {
  const envelope = state.envelope;
  if (!envelope) throw new Error("set the genbioh100 session envelope before running AI.zymes H100 Stage-2");
  if (envelope.target !== "genbioh100") throw new Error("AI.zymes H100 Stage-2 requires the genbioh100 session envelope");
  if (envelope.node !== "genbioh100") throw new Error("genbioh100 envelope node must be 'genbioh100'");
  if (envelope.partition !== null) throw new Error("genbioh100 envelope partition must be null (direct, no scheduler)");
  // Policy-hash binding (staleness rejection).
  if (envelope.policyHash !== undefined && state.policy?.hash != null && envelope.policyHash !== state.policy.hash) throw new Error("envelope policy hash is stale; re-set the envelope");
  // Resource caps: exactly sufficient.
  const r = H100_STAGE2_RESOURCES;
  if (envelope.maxCpus < r.cpus) throw new Error(`envelope maxCpus ${envelope.maxCpus} < required ${r.cpus}`);
  if (envelope.maxGpus < r.gpus || envelope.maxGpus > 1) throw new Error("genbioh100 envelope maxGpus must be exactly 1 (GPU 0 only)");
  if (envelope.memGb === null) throw new Error("genbioh100 envelope memGb must be non-null");
  if (envelope.memGb < r.memGb) throw new Error(`envelope memGb ${envelope.memGb} < required ${r.memGb}`);
  if (envelope.concurrency < r.concurrency) throw new Error(`envelope concurrency ${envelope.concurrency} < required ${r.concurrency}`);
  // Aggregate active-run capacity: count running/reconciling H100 stage2 runs.
  const activeRuns = state.runs.filter((run) => run.operation === "aizyme-h100-stage2" && ["running", "reconciling"].includes(run.status));
  if (activeRuns.length >= envelope.concurrency) throw new Error(`aggregate capacity: ${activeRuns.length} active H100 stage2 run(s) already at concurrency limit ${envelope.concurrency}`);
}

async function runLocal(shell, command, timeoutMs, signal) {
  const request = shell.resolve({ command, timeoutMs, signal });
  const result = await shell.run(request);
  return { stdout: result.stdout?.text ?? "", stderr: result.stderr?.text ?? "", exitCode: result.exitCode ?? null, timedOut: result.timedOut === true };
}

export function createAizymeH100Tools({ makeTool, requirePolicy, requireState, publicState, runRemote, shell, userQuestions, jobs, config, requireRemoteAccess, runRegistry }) {
  const localRemoteBundle = typeof config?.aizymeLocalRemoteBundle === "string" && config.aizymeLocalRemoteBundle.startsWith("/") ? config.aizymeLocalRemoteBundle : DEFAULT_LOCAL_REMOTE_BUNDLE;
  // ── genbio_aizyme_h100_stage2: exact-once launch ──────────────────────────
  const stageTool = makeTool(
    "genbio_aizyme_h100_stage2",
    "Run the AI.zymes Stage-2 verification on genbioh100 (bash-native, detached). Exact-once: 128-bit CSPRNG run identity, rclone+SHA-256 staging, local+remote bash -n, setsid detached launch. No retry on any side-effecting operation. Launch ambiguity is reconciling, never failed. Cancel is local-observer-only.",
    {},
    async (_args, exec) => {
      const state = requireState(exec);
      const policy = requirePolicy();

      // Phase 0: local validation (BEFORE any remote, grant, or job).
      // 0a: Policy validation.
      validateH100Policy(policy);
      // 0b: Envelope validation (exact fit, partition null, mem non-null, aggregate).
      validateH100Envelope(state, policy);
      // 0c: Load local files and compute SHA-256.
      const { files, bytes } = await loadLocalFiles(localRemoteBundle);
      const scriptFile = files.find((f) => f.name === H100_SCRIPT_NAME);
      const scriptSha = scriptFile.sha256;
      const archiveFile = files.find((f) => f.name === H100_ARCHIVE_NAME);
      if (archiveFile.sha256 !== H100_ARCHIVE_SHA) throw new Error(`archive SHA-256 does not match the pinned digest: ${archiveFile.sha256.slice(0, 12)}... != ${H100_ARCHIVE_SHA.slice(0, 12)}...`);
      // 0d: Local clean-env bash -n (BEFORE any remote access, grant, or job).
      localBashSyntaxOk(scriptFile.content.toString("utf8"));

      // Phase 1: registry/pair-lock (FAIL-CLOSED).
      const sessionId = exec.agent.session.id;
      let registryRunId = null;
      if (runRegistry) {
        try {
          const created = await runRegistry.record(sessionId, { source: "aizyme", project: "aizyme", operation: "stage2", status: "in-flight", note: "AI.zymes H100 Stage-2 admitted" });
          registryRunId = created.runId;
        } catch (error) {
          // FAIL-CLOSED: a registry failure aborts the launch.
          throw new Error(`H100 stage2 registry record failed (fail-closed): ${String(error?.message ?? error)}`);
        }
      }

      // Phase 2: in-flight guard (pair-lock).
      const inFlightH100 = state.aizymeH100InFlight ?? (state.aizymeH100InFlight = new Set());
      if (inFlightH100.has("stage2")) throw new Error("AI.zymes H100 Stage-2 already has an in-flight run; the pair-lock is held");
      inFlightH100.add("stage2");

      // Phase 3: remote access grant (AFTER all local validation, BEFORE mutation).
      const effectiveGrants = await requireRemoteAccess("genbioh100", [{ root: H100_PROJECT_ROOT, write: true }], exec, state);

      // Phase 4: material-transfer user approval (explicit, before rclone).
      if (!userQuestions) throw new Error("material transfer requires the DSH user-question provider");
      const transferAnswer = await userQuestions.ask({
        agent: exec.agent,
        signal: exec.signal,
        questions: [{
          id: "genbio-h100-stage2-transfer",
          header: "AI.zymes H100 Stage-2 staging",
          question: `Stage 2 (genbioh100) will transfer ${files.length} file(s), ${bytes} bytes, to ${H100_STAGE_ROOT}/<run>. Approve this material transfer?`,
          options: [
            { label: "Approve this transfer", description: "Transfer the script and archive to a fresh run directory on genbioh100." },
            { label: "Reject", description: "Do not transfer or launch this stage." },
          ],
        }],
      });
      const selected = transferAnswer.answers?.find((item) => item.id === "genbio-h100-stage2-transfer")?.selected ?? [];
      if (!selected.includes("Approve this transfer")) {
        // Clean up: release the in-flight lock on rejection (no side effects yet).
        inFlightH100.delete("stage2");
        if (runRegistry && registryRunId) void runRegistry.update(sessionId, registryRunId, { status: "cancelled", note: "transfer rejected" }).catch(() => {});
        throw new Error("AI.zymes H100 Stage-2 material transfer was not approved");
      }

      if (!jobs) throw new Error("background job registry is unavailable");

      // Phase 5: create the run record with a 128-bit CSPRNG token.
      const runToken = randomBytes(16).toString("hex");
      const runName = `stage2-h100-${runToken}`;
      const remoteRunDir = `${H100_STAGE_ROOT}/${runName}`;
      const run = {
        runId: `genbioh100-aizyme-stage2-${runToken}`,
        target: "genbioh100",
        operation: "aizyme-h100-stage2",
        status: "running",
        startedAt: Date.now(),
        finishedAt: null,
        stdout: "",
        stderr: "",
        error: null,
        pid: null,
        jobId: null,
        resources: { ...H100_STAGE2_RESOURCES },
        policyHash: state.policy.hash,
        node: "genbioh100",
        partition: null,
        envelope: JSON.parse(JSON.stringify(state.envelope)),
        remoteRunDir,
        runToken,
        scriptSha,
        remoteGrants: JSON.parse(JSON.stringify(effectiveGrants)),
        finalization: null,
        memory: { status: "not-finalized", error: null, openVikingSessionId: null, traceId: null },
        registryRunId,
        registryError: null,
      };

      run.jobId = jobs.start({
        kind: "genbio-h100-aizyme",
        label: "genbioh100 AI.zymes Stage-2",
        owner: exec.agent,
        run: () => {
          const controller = new AbortController();
          const done = (async () => {
            let dispatched = false;
            let lockAcquired = false;
            try {
              // Step 1: Atomic GLOBAL pair lock + run directory creation (NO RETRY).
              // The global lock is at ${H100_STAGE_ROOT}/.stage2.lock (fixed path).
              // mkdir is atomic: if the directory already exists, it fails (cross-session mutex).
              // Token is written inside the lock dir for identity verification on release.
              // Then the unique run dir is created under the stage root.
              // From this point, any ssh failure means remote state is UNKNOWN.
              dispatched = true;
              const lockDir = `${H100_STAGE_ROOT}/.stage2.lock`;
              // The remote command outputs LOCK_CONFLICT=1 if the lock already exists,
              // or LOCK_ACQUIRED=1 on success. This distinguishes authoritative
              // no-launch (conflict) from transport ambiguity (ssh failure).
               const initCmd = `set -u; stage_root=${shellQuote(H100_STAGE_ROOT)}; lock=${shellQuote(lockDir)}; if ! mkdir -p "$stage_root" 2>/dev/null; then printf 'STAGE_ROOT_ERROR=1\n'; exit 2; fi; if test -d "$lock"; then printf 'LOCK_CONFLICT=1\n'; exit 0; fi; if ! mkdir "$lock" 2>/dev/null; then if test -d "$lock"; then printf 'LOCK_CONFLICT=1\n'; exit 0; fi; printf 'LOCK_ERROR_NO_LOCK=1\n'; exit 3; fi; if ! printf 'token=%s\nlocked_utc=%s\n' '${runToken}' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$lock/token.txt"; then rmdir "$lock" 2>/dev/null || true; printf 'LOCK_TOKEN_ERROR=1\n'; exit 4; fi; if ! mkdir -p ${shellQuote(remoteRunDir)} 2>/dev/null; then rm -f "$lock/token.txt"; rmdir "$lock" 2>/dev/null || true; printf 'RUN_DIR_ERROR_NO_LOCK=1\n'; exit 5; fi; printf 'LOCK_ACQUIRED=1\n'`;
              const remoteInit = await runRemote("genbioh100", strictRemoteH100(initCmd), exec, 30000);
              // Authoritative NO-LAUNCH: LOCK_CONFLICT means the lock exists, we did NOT launch.
              if (remoteInit.exitCode === 0 && String(remoteInit.stdout).includes("LOCK_CONFLICT=1")) {
                // Do NOT mark reconciling. This is a definitive no-launch.
                // Release local in-flight and terminal-cancel the registry.
                inFlightH100.delete("stage2");
                if (runRegistry && registryRunId) void runRegistry.update(sessionId, registryRunId, { status: "cancelled", note: "lock conflict: another session holds the stage2 global lock" }).catch(() => {});
                run.status = "failed";
                run.error = "genbioh100 stage2 global lock is held by another session (authoritative no-launch, exact-once mutex)";
                return { status: "failed", detail: "lock conflict: another session holds the stage2 lock; no launch occurred" };
              }
               if (remoteInit.exitCode !== 0) {
                 const initStdout = String(remoteInit.stdout);
                 const provenNoLaunch = ["STAGE_ROOT_ERROR=1", "LOCK_ERROR_NO_LOCK=1", "LOCK_TOKEN_ERROR=1", "RUN_DIR_ERROR_NO_LOCK=1"].find((marker) => initStdout.includes(marker));
                 if (provenNoLaunch) {
                   inFlightH100.delete("stage2");
                   run.status = "failed";
                   run.finishedAt = Date.now();
                   run.error = `genbioh100 stage2 pre-launch initialization failed (${provenNoLaunch})`;
                   if (runRegistry && registryRunId) {
                     try {
                       await runRegistry.update(sessionId, registryRunId, { status: "failed", note: run.error });
                     } catch (regError) {
                       run.registryError = `terminal registry update failed: ${String(regError?.message ?? regError)}`;
                     }
                   }
                   return { status: "failed", detail: run.error };
                 }
                 // Unmarked SSH/transport failure: remote state is UNKNOWN → reconciling.
                 throw new Error(`failed to acquire genbioh100 stage2 global lock + run dir (transport): ${remoteInit.stderr || remoteInit.stdout || remoteInit.exitCode}`);
               }
              if (!String(remoteInit.stdout).includes("LOCK_ACQUIRED=1")) {
                // Unexpected output: treat as ambiguity.
                throw new Error(`genbioh100 stage2 lock init: unexpected output (no LOCK_ACQUIRED marker): ${remoteInit.stdout.slice(0, 200)}`);
              }
              lockAcquired = true;
              // Store the lock dir on the run for terminal release.
              run.remoteLockDir = lockDir;

              // Step 2: Stage files via rclone (user-approved; NO RETRY on failure).
              const rcloneRemote = resolveRcloneRemote(config, "genbioh100");
              for (const item of files) {
                const rc = await runLocal(shell, `rclone copyto ${RCLONE_TRANSFER_ARGS.join(" ")} ${shellQuote(item.path)} ${shellQuote(`${rcloneRemote}:${remoteRunDir}/${item.name}`)}`, 120000, exec.signal);
                if (rc.exitCode !== 0) throw new Error(`genbioh100 staging rclone failed for ${item.name}: ${rc.stderr || rc.stdout || rc.exitCode}`);
              }

              // Step 3: Remote SHA-256 verification.
              const remoteNames = files.map((f) => shellQuote(f.name)).join(" ");
              const checksumCmd = `set -eu; cd -- ${shellQuote(remoteRunDir)}; sha256sum ${remoteNames}`;
              const checksumResult = await runRemote("genbioh100", strictRemoteH100(checksumCmd), exec, 30000);
              if (checksumResult.exitCode !== 0) throw new Error(`genbioh100 remote SHA-256 verification failed: ${checksumResult.stderr || checksumResult.stdout}`);
              const remoteHashes = new Map();
              for (const line of String(checksumResult.stdout).split(/\r?\n/u)) {
                const m = /^([a-f0-9]{64})\s+\*?([^\s]+)$/u.exec(line.trim());
                if (m) remoteHashes.set(m[2], m[1]);
              }
              for (const item of files) {
                if (remoteHashes.get(item.name) !== item.sha256) throw new Error(`SHA-256 mismatch after staging: ${item.name}`);
              }

              // Step 4: Remote bash -n (clean environment).
              const syntaxCmd = `set -eu; cd -- ${shellQuote(remoteRunDir)}; env -i PATH=/usr/bin:/bin BASH_ENV=/dev/null /bin/bash --noprofile --norc -n -- ${shellQuote(H100_SCRIPT_NAME)}`;
              const syntaxResult = await runRemote("genbioh100", strictRemoteH100(syntaxCmd), exec, 30000);
              if (syntaxResult.exitCode !== 0) throw new Error(`genbioh100 remote bash -n failed: ${syntaxResult.stderr || syntaxResult.stdout}`);

              // Step 5: Launch the detached process (EXACT-ONCE, NO RETRY).
              // setsid + /bin/bash --noprofile --norc + stdin /dev/null + env fixed.
              const launchCmd = `set -eu; cd -- ${shellQuote(remoteRunDir)}; test "$(sha256sum ${shellQuote(H100_SCRIPT_NAME)} | cut -d' ' -f1)" = '${scriptSha}'; setsid env -i PATH=/usr/bin:/bin:/usr/sbin:/sbin CUDA_VISIBLE_DEVICES=0 OMP_NUM_THREADS=16 AIZH100_RUN_DIR=${shellQuote(remoteRunDir)} AIZH100_RUN_TOKEN='${runToken}' /bin/bash --noprofile --norc -- ${shellQuote(H100_SCRIPT_NAME)} < /dev/null > ${shellQuote(remoteRunDir + "/stdout.log")} 2> ${shellQuote(remoteRunDir + "/stderr.log")} & printf 'LAUNCH_RC=0\\n'; sleep 1; if test -f ${shellQuote(remoteRunDir + "/run_identity")}; then cat ${shellQuote(remoteRunDir + "/run_identity")}; else printf 'LAUNCH_AMBIGUITY=1\\n'; fi`;
              const launchResult = await runRemote("genbioh100", strictRemoteH100(launchCmd), exec, 30000);

              // Parse launch result. AMBIGUITY → reconciling, NEVER failed.
              if (launchResult.exitCode !== 0) {
                // Transport failure or ssh error: the remote state is UNKNOWN.
                run.status = "reconciling";
                run.error = `launch ssh returned exit ${launchResult.exitCode}: ${launchResult.stderr || launchResult.stdout}`;
                run.stdout = launchResult.stdout.slice(-4000);
                return { status: "reconciling", detail: "launch result ambiguous (ssh failure); run stays reconciling, lock held" };
              }
              const output = String(launchResult.stdout);
              if (output.includes("LAUNCH_AMBIGUITY=1")) {
                // The launch command succeeded but the run_identity file was not
                // found after 1s. The process MAY or MAY NOT have started.
                run.status = "reconciling";
                run.error = "launch ambiguity: run_identity not found after 1s; process state unknown";
                return { status: "reconciling", detail: "launch ambiguous; run stays reconciling, lock held" };
              }
              // Success: parse the PID from the run_identity output.
              // Reject missing/unparseable PID as ambiguity (not success).
              const pidMatch = output.match(/pid=(\d+)/);
              if (!pidMatch) {
                run.status = "reconciling";
                run.error = "launch succeeded but PID/run_identity is missing or unparseable; treating as ambiguity";
                run.stdout = output.slice(-4000);
                return { status: "reconciling", detail: "PID unparseable; run stays reconciling, lock held" };
              }
              run.pid = Number(pidMatch[1]);
              run.status = "running";
              run.stdout = output.slice(-4000);
              return { status: "running", detail: `genbioh100 stage2 launched (pid=${run.pid}, run_dir=${run.remoteRunDir})` };
            } catch (error) {
              if (controller.signal.aborted) {
                // Cancellation is LOCAL-OBSERVER-ONLY. We do NOT claim remote
                // cancellation. The run stays "reconciling" and the lock is held.
                run.status = "reconciling";
                run.error = "local cancellation observed; remote state unknown; lock held, run stays reconciling";
                return { status: "reconciling", detail: "local cancel; remote state unknown" };
              }
              if (dispatched) {
                // Post-dispatch: the remote state is unknown. Reconciling, lock held.
                run.status = "reconciling";
                run.error = String(error?.message ?? error);
                return { status: "reconciling", detail: run.error };
              } else {
                // Pre-dispatch: no side effects occurred. Clean up lock + registry.
                inFlightH100.delete("stage2");
                if (runRegistry && registryRunId) void runRegistry.update(sessionId, registryRunId, { status: "cancelled", note: "pre-dispatch error" }).catch(() => {});
                run.status = "failed";
                run.error = String(error?.message ?? error);
                return { status: "failed", detail: run.error };
              }
            }
            // NOTE: the in-flight lock is NOT cleared on post-dispatch paths.
            // It is held until the status tool reconciles the run to terminal.
          })();
          return {
            // Cancel is LOCAL-OBSERVER-ONLY: it does not send a remote signal.
            // The run stays "reconciling" and the pair-lock is held.
            cancel: (reason) => controller.abort(reason ?? "local cancel (remote state unknown)"),
            done,
            readOutput: () => {
              const text = [run.stdout, run.stderr && `[stderr]\n${run.stderr}`, run.error && `[error] ${run.error}`].filter(Boolean).join("\n");
              run.stdout = ""; run.stderr = "";
              return text;
            },
          };
        },
      });

      state.runs.push(run);
      if (state.runs.length > 50) state.runs.splice(0, state.runs.length - 50);
      return { ok: true, status: { ...publicState(state), started: run } };
    },
  );

  // ── genbio_aizyme_h100_status: status reconciliation ─────────────────────
  const statusTool = makeTool(
    "genbio_aizyme_h100_status",
    "Check the status of a genbioh100 AI.zymes Stage-2 run. Requires run_id. Reads remote status, PID liveness, exit code, checksum -c, and run-bound G2/STAGE2_PASS evidence. Completed only on exit 0 + missing=0 + checksum -c + G2_PASS + STAGE2_PASS. Remote outages remain reconciling, never failed. RSS/threads/GPU evidence included.",
    { run_id: { type: "string", required: true } },
    async (args, exec) => {
      const state = requireState(exec);
      const sessionId = exec.agent.session.id;
      const runId = String(args.run_id ?? "");
      if (!runId) throw new Error("genbio_aizyme_h100_status requires run_id");
      let run = state.runs.find((item) => item.runId === runId && item.operation === "aizyme-h100-stage2");
      if (!run) {
        const tokenMatch = /^genbioh100-aizyme-stage2-([a-f0-9]{32})$/u.exec(runId);
        const durable = runRegistry ? (await runRegistry.list(sessionId)).find((item) => item.source === "aizyme" && item.project === "dae-enzyme" && item.operation === "stage2" && item.status === "reconciling") : null;
        if (!tokenMatch || !durable) return { ok: true, status: { ...publicState(state), h100Status: { found: false, runId } } };
        run = { runId, target: "genbioh100", operation: "aizyme-h100-stage2", status: "reconciling", startedAt: durable.startedAt, finishedAt: null, stdout: "", stderr: "", error: null, resources: { ...H100_STAGE2_RESOURCES }, policyHash: state.policy.hash, node: "genbioh100", partition: null, envelope: null, remoteRunDir: `${H100_STAGE_ROOT}/stage2-h100-${tokenMatch[1]}`, runToken: tokenMatch[1], remoteLockDir: `${H100_STAGE_ROOT}/.stage2.lock`, registryRunId: durable.runId, registryError: null, recovered: true };
        state.runs.push(run);
        const inFlight = state.aizymeH100InFlight ?? (state.aizymeH100InFlight = new Set());
        inFlight.add("stage2");
      }

      const remoteRunDir = run.remoteRunDir;
      if (!remoteRunDir) return { ok: true, status: { ...publicState(state), h100Status: { found: true, runId, error: "no remote run directory recorded" } } };

      // Remote access coverage: use requireRemoteAccess (the same gate as launch).
      await requireRemoteAccess("genbioh100", [{ root: H100_PROJECT_ROOT, write: false }], exec, state);

      // Read the status + evidence from the remote.
      // No GPU1 query. nvidia-smi -i 0 only for GPU evidence.
      const statusCmd = `set -eu; cd -- ${shellQuote(remoteRunDir)}; printf '== IDENTITY ==\\n'; cat run_identity 2>/dev/null || printf 'missing\\n'; printf '== STATUS ==\\n'; cat status 2>/dev/null || printf 'state=unknown\\n'; printf '== EXIT_CODE ==\\n'; cat exit_code 2>/dev/null || printf 'pending\\n'; printf '== PID_ALIVE ==\\n'; pid=$(cat run_identity 2>/dev/null | grep '^pid=' | cut -d= -f2 || echo 0); if test "$pid" != "0" && kill -0 "$pid" 2>/dev/null; then printf 'alive\\n'; else printf 'dead\\n'; fi; printf '== PROCESS_EVIDENCE ==\\n'; if test -f run_identity; then pid=$(grep '^pid=' run_identity | cut -d= -f2); if kill -0 "$pid" 2>/dev/null; then printf 'rss_kb='; grep VmRSS /proc/$pid/status 2>/dev/null | awk '{print $2}' || printf 'unavailable\\n'; printf 'threads='; grep Threads /proc/$pid/status 2>/dev/null | awk '{print $2}' || printf 'unavailable\\n'; fi; fi; printf '== GPU_EVIDENCE ==\\n'; if command -v nvidia-smi >/dev/null 2>&1; then nvidia-smi -i 0 --query-compute-apps=pid,used_memory --format=csv,noheader 2>/dev/null || true; fi; printf '== CHECKSUM_VERIFY ==\\n'; if test -f manifests/stage2_checksums.sha256; then if (cd manifests && sha256sum -c stage2_checksums.sha256 >/dev/null 2>&1); then printf 'CHECKSUM_OK=1\\n'; printf 'EVIDENCE_MANIFEST_SHA=%s\\n' "$(sha256sum manifests/stage2_checksums.sha256 | awk '{print $1}')"; else printf 'CHECKSUM_OK=0\\n'; fi; else printf 'CHECKSUM_OK=0\\n'; fi; printf '== G2_EVIDENCE ==\\n'; if test -f manifests/G2_PASS; then cat manifests/G2_PASS; else printf 'G2_PASS=absent\\n'; fi; if test -f manifests/STAGE2_PASS; then cat manifests/STAGE2_PASS; else printf 'STAGE2_PASS=absent\\n'; fi; printf 'STAGE2_PASS_COUNT=%s\\n' "$(grep -xc '^STAGE2_PASS$' stdout.log 2>/dev/null || echo 0)"; if test -f manifests/stage2_environment_runtime.txt; then grep -E '^(missing_required_checks|target|cuda_visible_devices|omp_num_threads|memory_ulimit_kb)=' manifests/stage2_environment_runtime.txt 2>/dev/null || true; fi; printf '== STDOUT_TAIL ==\\n'; tail -20 stdout.log 2>/dev/null || true; printf '== STDERR_TAIL ==\\n'; tail -10 stderr.log 2>/dev/null || true`;

      let result;
      try {
        result = await runRemote("genbioh100", strictRemoteH100(statusCmd), exec, Number(config.commandTimeoutMs ?? 30000));
      } catch (error) {
        // Remote outage: the run stays "reconciling", NOT "failed".
        return { ok: true, status: { ...publicState(state), h100Status: { found: true, runId, state: "reconciling", error: `remote status check failed (outage): ${String(error?.message ?? error)}` } } };
      }

      if (result.exitCode !== 0) {
        // Remote command failed (e.g., run dir deleted): stay reconciling.
        return { ok: true, status: { ...publicState(state), h100Status: { found: true, runId, state: "reconciling", error: `remote status check exit ${result.exitCode}: ${result.stderr || result.stdout}` } } };
      }

      // Parse the evidence sections.
      const stdout = result.stdout;
      const section = (name) => stdout.split(`== ${name} ==`)[1]?.split(/\n== [A-Z_]+ ==/)[0]?.trim() ?? "";
      const statusFields = {};
      for (const line of section("STATUS").split(/\r?\n/u)) {
        const m = /^([a-z_]+)=(.*)$/u.exec(line.trim());
        if (m) statusFields[m[1]] = m[2];
      }
      const exitCodeRaw = section("EXIT_CODE");
      const exitCode = exitCodeRaw === "pending" ? null : Number(exitCodeRaw);
      const pidAlive = section("PID_ALIVE");
      const processEvidence = section("PROCESS_EVIDENCE");
      const gpuEvidence = section("GPU_EVIDENCE");
      const checksumVerify = section("CHECKSUM_VERIFY");
      const g2Section = section("G2_EVIDENCE");
      const stdoutTail = section("STDOUT_TAIL").slice(-4000);
      const stderrTail = section("STDERR_TAIL").slice(-2000);

      const remoteState = statusFields.state ?? "unknown";
      const pidDead = pidAlive === "dead";
      const g2Pass = g2Section.includes("G2_PASS") && !g2Section.includes("G2_PASS=absent");
      const g2PassRunBound = g2Pass && g2Section.includes(`run_dir=${remoteRunDir}`);
      // Validate evidence_cksum: must equal sha256 of the checksums manifest.
      // Validate evidence_cksum: must exactly equal the remotely computed
      // EVIDENCE_MANIFEST_SHA (sha256 of manifests/stage2_checksums.sha256).
      const g2EvidenceCksum = g2Section.match(/evidence_cksum=([a-f0-9]{64})/)?.[1] ?? null;
      const evidenceManifestSha = checksumVerify.match(/EVIDENCE_MANIFEST_SHA=([a-f0-9]{64})/)?.[1] ?? null;
      const g2EvidenceCksumValid = g2EvidenceCksum !== null && evidenceManifestSha !== null && g2EvidenceCksum === evidenceManifestSha;
      // STAGE2_PASS: use the explicit remote count (grep -xc '^STAGE2_PASS$' stdout.log).
      const stage2PassCountRaw = g2Section.match(/STAGE2_PASS_COUNT=(\d+)/)?.[1] ?? "0";
      const stage2PassCount = Number(stage2PassCountRaw);
      const stage2Pass = stage2PassCount === 1;
      const missingChecks = g2Section.match(/missing_required_checks=(\d+)/)?.[1] ?? null;
      const checksumOk = checksumVerify.includes("CHECKSUM_OK=1");

      // Verify run_identity token matches the run token (authoritative terminal proof).
      const identitySection = stdout.split("== IDENTITY ==")[1]?.split("\n== [A-Z_]+ ==")[0]?.trim() ?? "";
      const identityToken = identitySection.match(/token=([a-f0-9]{32})/)?.[1] ?? null;
      const tokenMatch = identityToken === run.runToken;

      // Reconciliation logic:
      // COMPLETED: exit 0 + missing=0 + CHECKSUM_OK=1 + G2_PASS (token-bound) + exactly one STAGE2_PASS + token match
      // FAILED: exit != 0 (definitive terminal failure from the script)
      // RECONCILING: anything else (pid alive, exit pending, remote partial)
      let reconciled;
      if (exitCode === 0 && missingChecks === "0" && checksumOk && g2PassRunBound && g2EvidenceCksumValid && stage2PassCount === 1 && tokenMatch) {
        reconciled = "completed";
      } else if (exitCode !== null && exitCode !== 0) {
        reconciled = "failed";
      } else if (exitCode === null && identityToken === null && stderrTail.trim().length > 0 && Date.now() - run.startedAt > 60000) {
        // Pre-instrumentation termination: the detached payload exited before
        // writing run_identity/exit_code, and its own captured stderr names the
        // cause (e.g. a launcher-environment assertion). No terminal identity
        // evidence can ever appear, so this is a definitive failure with the
        // stderr as evidence — not an ambiguity to reconcile forever. The
        // 60s elapsed bound keeps a slow-starting healthy payload (empty
        // stderr, identity pending) firmly in the reconciling branch.
        reconciled = "failed";
      } else if (pidAlive === "alive" || exitCode === null) {
        reconciled = "reconciling";
      } else if (pidDead && exitCode === 0 && !(missingChecks === "0" && checksumOk && g2PassRunBound && stage2Pass && stage2PassCount === 1)) {
        // Exit 0 but evidence incomplete: the script exited 0 without writing
        // all the required markers. This is a failure of the gate, not the script.
        reconciled = "failed";
      } else {
        reconciled = "reconciling";
      }

      // Update the run record.
      // Compute terminal status is stored but run.status stays nonterminal
      // until the remote lock is successfully released (or no lock exists).
      if (reconciled === "completed" || reconciled === "failed") {
        // Store the compute terminal verdict and evidence.
        run.computeTerminalStatus = reconciled;
        run.finishedAt = run.finishedAt || Date.now();
        run.stdout = stdoutTail;
        run.stderr = stderrTail;
        run.error = reconciled === "failed" ? stderrTail || `exit ${exitCode}` : null;

        // Attempt remote lock release (token-verified). Retry on every status call
        // until LOCK_RELEASED=1. Only then do we set run.status terminal.
        let lockReleased = false;
        if (run.remoteLockDir) {
          try {
            const releaseCmd = `set -eu; lock=${shellQuote(run.remoteLockDir)}; lock_token=$(cat "$lock/token.txt" 2>/dev/null || echo ""); if test "$lock_token" = '${run.runToken}'; then rm -f "$lock/token.txt"; rmdir "$lock"; printf 'LOCK_RELEASED=1\n'; else printf 'LOCK_RELEASED=0\n'; fi; if test -d "$lock"; then printf 'LOCK_STILL_PRESENT=1\n'; exit 1; fi`;
            const releaseResult = await runRemote("genbioh100", strictRemoteH100(releaseCmd), exec, 15000);
            if (releaseResult.exitCode === 0 && String(releaseResult.stdout).includes("LOCK_RELEASED=1")) {
              lockReleased = true;
              run.remoteLockDir = null;
              run.lockReleasePending = false;
            } else {
              run.lockReleasePending = true;
              run.error = (run.error ? run.error + "; " : "") + "remote lock release failed (will retry on next status call)";
            }
          } catch (releaseErr) {
            run.lockReleasePending = true;
            run.error = (run.error ? run.error + "; " : "") + `remote lock release error: ${String(releaseErr?.message ?? releaseErr)}`;
          }
        } else {
          lockReleased = true; // no lock to release
        }

        if (lockReleased) {
          // Authoritative terminal: lock released (or none). Set status terminal.
          run.status = reconciled;
          run.lockReleased = true;
          const inFlightH100 = state.aizymeH100InFlight;
          if (inFlightH100) inFlightH100.delete("stage2");
          if (runRegistry && run.registryRunId) {
            try {
              await runRegistry.update(sessionId, run.registryRunId, { status: reconciled, note: `H100 stage2 reconciled ${reconciled}` });
            } catch (regErr) {
              // Terminal registry update failure: surface in run.registryError.
              // The compute verdict is authoritative; the registry is a secondary record.
              run.registryError = `terminal registry update failed: ${String(regErr?.message ?? regErr)}`;
            }
          }
        } else {
          // Lock release pending: keep run.status NONTERMINAL (reconciling).
          // The compute verdict is preserved in run.computeTerminalStatus.
          // Local in-flight lock and registry remain nonterminal.
          // Next status call will retry the release.
          run.status = "reconciling";
          run.lockReleased = false;
        }
      } else if (reconciled === "reconciling" && run.status === "running") {
        run.status = "reconciling";
      }
      // If run was already in lock-release-pending state (from a prior call)
      // and the compute is still terminal, the retry above handles it.
      // If the compute is now nonterminal (e.g., process came back?), reset.
      if (run.computeTerminalStatus && reconciled === "reconciling" && !run.lockReleasePending) {
        // Compute is no longer terminal; clear the pending terminal.
        delete run.computeTerminalStatus;
      }

      return {
        ok: true,
        status: {
          ...publicState(state),
          h100Status: {
            found: true,
            runId,
            remoteState,
            reconciled,
            exitCode,
            pidAlive,
            missingChecks,
            checksumOk,
            g2PassRunBound,
            stage2PassCount,
            processEvidence,
            gpuEvidence: gpuEvidence.slice(-1000),
            stdoutTail,
            stderrTail,
            runDir: remoteRunDir,
            lockReleased: run.lockReleased ?? null,
            lockReleasePending: run.lockReleasePending ?? false,
          },
        },
      };
    },
  );

  return { stageTool, statusTool };
}

export { H100_PROJECT_ROOT, H100_STAGE_ROOT, H100_SCRIPT_NAME, H100_ARCHIVE_NAME, H100_ARCHIVE_SHA, H100_STAGE2_RESOURCES };
