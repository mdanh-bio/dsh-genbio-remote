// Adversarial tests for the AI.zymes H100 Stage-2 genbioh100 path.
// Covers all 15 blocking findings from the captain's review.
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { test } from "node:test";
import { createAizymeH100Tools, H100_PROJECT_ROOT, H100_STAGE_ROOT, H100_SCRIPT_NAME, H100_ARCHIVE_NAME, H100_ARCHIVE_SHA, H100_STAGE2_RESOURCES } from "../lib/aizyme-h100.js";

const __dirname = fileURLToPath(new URL(".", import.meta.url));
const SCRIPT_PATH = join(__dirname, "..", "fixtures", "aizyme-h100", H100_SCRIPT_NAME);

// ═══════════════════════════════════════════════════════════════════════════════
// Finding 1: Root MUST be /home/work/GenbioLAB/shared/daes_enzyme
// ═══════════════════════════════════════════════════════════════════════════════

test("F1: H100_PROJECT_ROOT is /home/work/GenbioLAB/shared/daes_enzyme", () => {
  assert.equal(H100_PROJECT_ROOT, "/home/work/GenbioLAB/shared/daes_enzyme");
});

test("F1: bash script uses the correct shared root", async () => {
  const content = await readFile(SCRIPT_PATH, "utf8");
  assert.ok(content.includes("/home/work/GenbioLAB/shared/daes_enzyme"), "script must use shared root");
  assert.ok(!content.includes("/home/work/GenbioLAB/data/simulation"), "script must NOT use data/simulation path");
});

// ═══════════════════════════════════════════════════════════════════════════════
// Finding 2: Registry/pair-lock fail-closed; lock held while running/reconciling
// ═══════════════════════════════════════════════════════════════════════════════

test("F2: registry failure is fail-closed (aborts launch)", async () => {
  const h = makeH100Harness({ state: h100State(), registryImpl: { record: async () => { throw new Error("registry disk full"); } } });
  await assert.rejects(h.tools.stageTool.execute({}, makeExec()), /registry record failed.*fail-closed/u);
  assert.equal(h.calls.remote.length, 0, "no remote calls after registry failure");
});

test("F2: in-flight lock is held and rejects second launch", async () => {
  const state = h100State();
  state.aizymeH100InFlight = new Set(["stage2"]);
  const h = makeH100Harness({ state });
  await assert.rejects(h.tools.stageTool.execute({}, makeExec()), /pair-lock|in-flight/u);
  assert.equal(h.calls.remote.length, 0);
  assert.equal(h.calls.jobs, 0);
});

test("F2: lock is NOT cleared in launch finally (stays held for reconciling)", async () => {
  const source = await readFile(join(__dirname, "..", "lib", "aizyme-h100.js"), "utf8");
  const doneBody = source.slice(source.indexOf("const done = (async () =>"), source.indexOf("return { cancel:", source.indexOf("const done = (async () =>")));
  assert.doesNotMatch(doneBody, /finally\s*\{[\s\S]*inFlightH100\.delete\("stage2"\)/u);
  assert.match(source, /inFlightH100\.delete\("stage2"\)/u);
});

test("F2b: stage root matches binding contract and is created before atomic lock", async () => {
  assert.equal(H100_STAGE_ROOT, "/home/work/GenbioLAB/shared/daes_enzyme/workflow/aizyme_v1/runs/stage2");
  const h = makeH100Harness({ state: h100State() });
  await h.tools.stageTool.execute({}, makeExec());
  if (h.latest()) await h.latest().done;
  const init = h.calls.remote.find((command) => command.includes("LOCK_ACQUIRED=1"));
  assert.ok(init, "lock initialization command transmitted");
  assert.match(init, /stage_root=.*mkdir -p .*stage_root[\s\S]*mkdir .*lock/u, "fixed stage parent is created before atomic lock");
});

test("F2c: explicit stage-root failure is definitive no-launch and releases local guard", async () => {
  const state = h100State();
  const updates = [];
  const h = makeH100Harness({
    state,
    remoteImpl: async (_target, command) => command.includes("LOCK_ACQUIRED=1")
      ? { stdout: "STAGE_ROOT_ERROR=1\n", stderr: "", exitCode: 2, signal: null, timedOut: false }
      : { stdout: "", stderr: "", exitCode: 0, signal: null, timedOut: false },
    registryImpl: { record: async () => ({ runId: "reg-root-fail" }), update: async (_sid, _rid, patch) => { updates.push(patch); } },
  });
  await h.tools.stageTool.execute({}, makeExec());
  const result = await h.latest().done;
  assert.equal(result.status, "failed");
  assert.equal(state.runs[0].status, "failed");
  assert.equal(state.aizymeH100InFlight.has("stage2"), false);
  assert.equal(updates.at(-1)?.status, "failed");
  assert.equal(h.calls.shell.length, 0, "no rclone or launch after proven pre-launch failure");
});

// ═══════════════════════════════════════════════════════════════════════════════
// Finding 3: 128-bit CSPRNG token, no Date.now for run ID, no retry
// ═══════════════════════════════════════════════════════════════════════════════

test("F3: run ID uses randomBytes(16) not Date.now", async () => {
  const source = await readFile(join(__dirname, "..", "lib", "aizyme-h100.js"), "utf8");
  assert.ok(source.includes("randomBytes(16)"), "must use randomBytes(16) for run token");
  assert.ok(!source.includes(`runId: \`genbioh100-aizyme-stage2-${Date.now()}\``), "must NOT use Date.now() for run ID");
});

test("F3: no retry for side-effecting operations (no runRemoteWithRetry)", async () => {
  const source = await readFile(join(__dirname, "..", "lib", "aizyme-h100.js"), "utf8");
  assert.ok(!source.includes("runRemoteWithRetry"), "must NOT use runRemoteWithRetry (no retry on side effects)");
});

// ═══════════════════════════════════════════════════════════════════════════════
// Finding 4: Explicit material-transfer user approval
// ═══════════════════════════════════════════════════════════════════════════════

test("F4: material transfer requires explicit user approval", async () => {
  const state = h100State();
  const calls = { remote: [], shell: [], jobs: 0, questions: 0 };
  const tools = createAizymeH100Tools({
    makeTool: (name, description, parameters, execute) => ({ name, description, parameters, execute }),
    requirePolicy: () => ({ targets: { genbioh100: { surface: "direct", login_shell: false, ssh_target: "genbioh100", limits: { cpu_threads_per_job: 16, mem_gb_per_job: 32, concurrent_gpu_jobs: 1, gpus_allowed: [0] } } } }),
    requireState: () => state,
    publicState: () => ({}),
    requireRemoteAccess: async (target, needs) => needs.map((n) => ({ target, root: n.root, mode: "rw" })),
    runRemote: async (target, command) => { calls.remote.push(command); return { stdout: "", stderr: "", exitCode: 0, signal: null, timedOut: false }; },
    shell: { resolve: (r) => r, run: async () => { return { stdout: { text: "" }, stderr: { text: "" }, exitCode: 0, signal: null, timedOut: false }; } },
    userQuestions: { ask: async () => { calls.questions += 1; return { answers: [{ id: "genbio-h100-stage2-transfer", selected: ["Reject"] }] }; } },
    jobs: { start: (spec) => { calls.jobs += 1; return "job-1"; } },
    config: { logMaxBytes: 65536, aizymeLocalRemoteBundle: join(__dirname, "..", "fixtures", "aizyme-h100") },
    runRegistry: { record: async () => ({ runId: "reg-1" }), update: async () => {} },
  });
  await assert.rejects(tools.stageTool.execute({}, makeExec()), /transfer was not approved/u);
  assert.equal(calls.remote.length, 0, "no remote calls when transfer rejected");
  assert.equal(calls.jobs, 0, "no jobs when transfer rejected");
});

test("F4: transfer approval is requested BEFORE any rclone transfer", async () => {
  const source = await readFile(join(__dirname, "..", "lib", "aizyme-h100.js"), "utf8");
  // The approval must come before the rclone staging in the code.
  const approvalIdx = source.indexOf("Approve this transfer");
  const rcloneIdx = source.indexOf("rclone copyto");
  assert.ok(approvalIdx < rcloneIdx, "user approval must precede rclone in the code");
});

// ═══════════════════════════════════════════════════════════════════════════════
// Finding 5: Exact policy caps validation
// ═══════════════════════════════════════════════════════════════════════════════

test("F5: genbioh100 policy must have surface=direct", async () => {
  const h = makeH100Harness({ state: h100State(), policy: { targets: { genbioh100: { surface: "slurm" } } } });
  await assert.rejects(h.tools.stageTool.execute({}, makeExec()), /surface=direct/u);
});

test("F5: genbioh100 policy must have login_shell=false", async () => {
  const h = makeH100Harness({ state: h100State(), policy: { targets: { genbioh100: { surface: "direct", login_shell: true } } } });
  await assert.rejects(h.tools.stageTool.execute({}, makeExec()), /login_shell=false/u);
});

test("F5: envelope partition must be null for genbioh100", async () => {
  const state = h100State({ envelope: { ...h100State().envelope, partition: "gpus" } });
  const h = makeH100Harness({ state });
  await assert.rejects(h.tools.stageTool.execute({}, makeExec()), /partition must be null/u);
});

test("F5: envelope memGb must be non-null", async () => {
  const state = h100State({ envelope: { ...h100State().envelope, memGb: null } });
  const h = makeH100Harness({ state });
  await assert.rejects(h.tools.stageTool.execute({}, makeExec()), /memGb must be non-null/u);
});

test("F5: aggregate active-run capacity is enforced", async () => {
  const state = h100State();
  state.runs.push({ operation: "aizyme-h100-stage2", status: "running", runId: "existing" });
  const h = makeH100Harness({ state });
  await assert.rejects(h.tools.stageTool.execute({}, makeExec()), /aggregate capacity/u);
});

// ═══════════════════════════════════════════════════════════════════════════════
// Finding 6: Local clean-env bash -n BEFORE any remote/grant/job
// ═══════════════════════════════════════════════════════════════════════════════

test("F6: local bash -n runs before any remote call", async () => {
  const source = await readFile(join(__dirname, "..", "lib", "aizyme-h100.js"), "utf8");
  const syntaxIdx = source.indexOf("localBashSyntaxOk(scriptFile.content.toString(\"utf8\"))");
  const grantIdx = source.indexOf("await requireRemoteAccess", syntaxIdx);
  assert.ok(syntaxIdx >= 0, "production stage must perform local bash -n");
  assert.ok(grantIdx >= 0 && syntaxIdx < grantIdx, "local syntax validation must precede remote access");
});

test("F6: script passes local bash -n", () => {
  const result = spawnSync("/bin/bash", ["--noprofile", "--norc", "-n", SCRIPT_PATH], {
    env: { PATH: "/usr/bin:/bin", BASH_ENV: "/dev/null" }, timeout: 10000, encoding: "utf8",
  });
  assert.equal(result.status, 0, `bash -n failed: ${result.stderr}`);
});

// ═══════════════════════════════════════════════════════════════════════════════
// Finding 7: requireRemoteAccess after local validation, before mutation
// ═══════════════════════════════════════════════════════════════════════════════

test("F7: requireRemoteAccess is called after local validation", async () => {
  const source = await readFile(join(__dirname, "..", "lib", "aizyme-h100.js"), "utf8");
  const localBashIdx = source.indexOf("localBashSyntaxOk(");
  const remoteAccessCallIdx = source.indexOf("await requireRemoteAccess(");
  assert.ok(localBashIdx < remoteAccessCallIdx, "local bash -n call must precede requireRemoteAccess call");
});

test("F7: effective grants are captured on the run record", async () => {
  const source = await readFile(join(__dirname, "..", "lib", "aizyme-h100.js"), "utf8");
  assert.ok(source.includes("remoteGrants: JSON.parse(JSON.stringify(effectiveGrants))"), "run must capture effective grants");
});

// ═══════════════════════════════════════════════════════════════════════════════
// Finding 8: Payload verification-only
// ═══════════════════════════════════════════════════════════════════════════════

test("F8: script has no git clone", async () => {
  const content = await readFile(SCRIPT_PATH, "utf8");
  assert.ok(!content.includes("git clone"), "no clone in verification-only script");
});

test("F8: script has no download (huggingface_hub snapshot_download)", async () => {
  const content = await readFile(SCRIPT_PATH, "utf8");
  assert.ok(!content.includes("snapshot_download"), "no download in verification-only script");
});

test("F8: script has no conda install", async () => {
  const content = await readFile(SCRIPT_PATH, "utf8");
  assert.ok(!/conda install/u.test(content), "no conda install in verification-only script");
});

test("F8: script has no broad $HOME find", async () => {
  const content = await readFile(SCRIPT_PATH, "utf8");
  assert.ok(!content.includes('find "$HOME"'), "no broad $HOME find");
  assert.ok(!content.includes("find $HOME"), "no broad $HOME find (no quotes)");
});

test("F8: script has no nvidia-smi -L (full listing)", async () => {
  const content = await readFile(SCRIPT_PATH, "utf8");
  // Check only non-comment lines for nvidia-smi -L (executable usage, not documentation).
  const execLines = content.split("\n").filter((l) => !l.trim().startsWith("#"));
  assert.ok(!execLines.some((l) => l.includes("nvidia-smi -L")), "no nvidia-smi -L in executable lines");
});

test("F8: script writes only under $RUN_DIR (no shared manifest writes)", async () => {
  const content = await readFile(SCRIPT_PATH, "utf8");
  // All manifest writes go to $MANIFESTS which is $RUN_DIR/manifests
  assert.ok(content.includes('MANIFESTS="$RUN_DIR/manifests"'), "manifests must be under RUN_DIR");
  // No writes to shared WF/manifests
  assert.ok(!content.includes('$WF/manifests'), "no shared manifest writes");
});

test("F8: script has root/run env assertions", async () => {
  const content = await readFile(SCRIPT_PATH, "utf8");
  assert.ok(content.includes("export CUDA_VISIBLE_DEVICES=0"), "must assert CUDA_VISIBLE_DEVICES=0");
  assert.ok(content.includes("export OMP_NUM_THREADS=16"), "must assert OMP_NUM_THREADS=16");
});

// ═══════════════════════════════════════════════════════════════════════════════
// Finding 9: Archive SHA exact, tar traversal-safe, immutable code
// ═══════════════════════════════════════════════════════════════════════════════

test("F9: archive SHA is pinned to the exact digest", () => {
  assert.equal(H100_ARCHIVE_SHA, "f408113ab7c2fbbfa2771312d0c0eaae308565da521f0d01f713ddc04441a70a");
});

test("F9: script verifies archive SHA-256 exactly", async () => {
  const content = await readFile(SCRIPT_PATH, "utf8");
  assert.ok(content.includes("f408113ab7c2fbbfa2771312d0c0eaae308565da521f0d01f713ddc04441a70a"), "script must pin the exact archive SHA");
});

test("F9: script checks tar traversal safety", async () => {
  const content = await readFile(SCRIPT_PATH, "utf8");
  assert.ok(content.includes("traversal"), "script must check for traversal-unsafe paths");
  assert.ok(content.includes("\\.\\./"), "script must detect ../ in tar entries");
});

test("F9: script never rm-rf shared code", async () => {
  const content = await readFile(SCRIPT_PATH, "utf8");
  // It may rm-rf the per-run CODE_TMP, but never the shared $CODE
  const lines = content.split("\n");
  for (const line of lines) {
    if (line.includes("rm -rf") && line.includes("$CODE") && !line.includes("$CODE_TMP")) {
      throw new Error(`script must never rm-rf shared code: ${line.trim()}`);
    }
  }
});

test("F9: script fails on code divergence (never overwrites)", async () => {
  const content = await readFile(SCRIPT_PATH, "utf8");
  assert.ok(content.includes("diverges"), "script must detect divergence");
  assert.ok(content.includes("FATAL"), "divergence must be a fatal error");
});

// ═══════════════════════════════════════════════════════════════════════════════
// Finding 10: ulimit hard-fail, wrapper /bin/bash --noprofile --norc, atomic identity
// ═══════════════════════════════════════════════════════════════════════════════

test("F10: ulimit -v failure is a hard fail (no || true)", async () => {
  const content = await readFile(SCRIPT_PATH, "utf8");
  const ulimitLine = content.split("\n").find((l) => l.includes("ulimit -v"));
  assert.ok(ulimitLine, "ulimit -v must be present");
  assert.ok(!ulimitLine.includes("|| true"), "ulimit must NOT have || true (hard fail)");
});

test("F10: plugin launch uses /bin/bash --noprofile --norc", async () => {
  const source = await readFile(join(__dirname, "..", "lib", "aizyme-h100.js"), "utf8");
  assert.ok(source.includes("/bin/bash --noprofile --norc"), "launch must use /bin/bash --noprofile --norc");
});

test("F10: plugin launch uses stdin /dev/null", async () => {
  const source = await readFile(join(__dirname, "..", "lib", "aizyme-h100.js"), "utf8");
  assert.ok(source.includes("< /dev/null"), "launch must redirect stdin from /dev/null");
});

test("F10b: clean env receives required run directory and token", async () => {
  const source = await readFile(join(__dirname, "..", "lib", "aizyme-h100.js"), "utf8");
  assert.match(source, /setsid env -i[^`]*AIZH100_RUN_DIR=\$\{shellQuote\(remoteRunDir\)\}[^`]*AIZH100_RUN_TOKEN='\$\{runToken\}'/u);
  assert.doesNotMatch(source, /AIZH100_RUN_DIR=.*AIZH100_RUN_TOKEN=.*setsid env -i/u, "required variables must not be erased by env -i");
});

test("F10: script records PID+PGID+start identity atomically", async () => {
  const content = await readFile(SCRIPT_PATH, "utf8");
  assert.ok(content.includes("run_identity"), "script must write run_identity file");
  assert.ok(content.includes("pid=") && content.includes("pgroup=") && content.includes("started_utc="), "run_identity must contain pid, pgroup, started_utc");
});

// ═══════════════════════════════════════════════════════════════════════════════
// Finding 11: Verification gates (import, MPNN probe, Amber, Python, freeze, Rosetta, ESMFold)
// ═══════════════════════════════════════════════════════════════════════════════

test("F11: script gates actual legacy import (not just version)", async () => {
  const content = await readFile(SCRIPT_PATH, "utf8");
  assert.ok(content.includes("import aizymes"), "must test actual import");
  assert.ok(content.includes("legacy_mode_rc="), "must record the import return code");
});

test("F11: script has MPNN functional probe", async () => {
  const content = await readFile(SCRIPT_PATH, "utf8");
  assert.ok(content.includes("functional_import"), "must probe MPNN functionally");
  assert.ok(!content.includes("git clone"), "no clone for MPNN");
});

test("F11: script checks all four Amber executables", async () => {
  const content = await readFile(SCRIPT_PATH, "utf8");
  for (const exe of ["tleap", "sander", "cpptraj", "pdb4amber"]) {
    assert.ok(content.includes(exe), `must check ${exe}`);
  }
});

test("F11: script tests required Python imports (actual import)", async () => {
  const content = await readFile(SCRIPT_PATH, "utf8");
  assert.ok(content.includes("importlib.import_module"), "must use importlib for actual import test");
  assert.ok(content.includes("numpy"), "must test numpy");
  assert.ok(content.includes("torch"), "must test torch");
});

test("F11: script has nonempty environment freeze", async () => {
  const content = await readFile(SCRIPT_PATH, "utf8");
  assert.ok(content.includes("environment_freeze"), "must write environment freeze");
  assert.ok(content.includes("test ! -s"), "must verify freeze is nonempty");
});

test("F11: script has Rosetta hash + help", async () => {
  const content = await readFile(SCRIPT_PATH, "utf8");
  assert.ok(content.includes("rosetta_sha256"), "must record Rosetta hash");
  assert.ok(content.includes("-help"), "must probe Rosetta -help");
});

test("F11: ESMFold is offline only (no download)", async () => {
  const content = await readFile(SCRIPT_PATH, "utf8");
  assert.ok(content.includes("HF_HUB_OFFLINE=1"), "must set HF_HUB_OFFLINE=1");
  assert.ok(content.includes("TRANSFORMERS_OFFLINE=1"), "must set TRANSFORMERS_OFFLINE=1");
  assert.ok(!content.includes("snapshot_download"), "no download");
  assert.ok(!content.includes("local_files_only=False"), "no online mode");
});

// ═══════════════════════════════════════════════════════════════════════════════
// Finding 12: GPU proof only nvidia-smi -i 0, torch count=1, RSS/threads/GPU in status
// ═══════════════════════════════════════════════════════════════════════════════

test("F12: script uses nvidia-smi -i 0 only (no full listing, no GPU1)", async () => {
  const content = await readFile(SCRIPT_PATH, "utf8");
  const execLines = content.split("\n").filter((l) => !l.trim().startsWith("#"));
  assert.ok(content.includes("nvidia-smi -i 0"), "must use nvidia-smi -i 0");
  assert.ok(!execLines.some((l) => l.includes("nvidia-smi -L")), "must NOT use nvidia-smi -L in executable lines");
  assert.ok(!execLines.some((l) => l.includes("gpu1")), "must NOT query GPU1");
  assert.ok(!execLines.some((l) => l.includes("gpu_util")), "must NOT reference gpu_util");
});

test("F12: torch device_count must be exactly 1", async () => {
  const content = await readFile(SCRIPT_PATH, "utf8");
  assert.ok(content.includes("device_count() == 1"), "torch must assert exactly 1 visible device");
});

test("F12: status tool includes RSS/threads/GPU evidence", async () => {
  const source = await readFile(join(__dirname, "..", "lib", "aizyme-h100.js"), "utf8");
  assert.ok(source.includes("VmRSS"), "status must include RSS evidence");
  assert.ok(source.includes("Threads"), "status must include thread count");
  assert.ok(source.includes("nvidia-smi -i 0"), "status GPU evidence must use -i 0 only");
});

// ═══════════════════════════════════════════════════════════════════════════════
// Finding 13: Status requires run_id, exit 0 + missing=0 + checksum -c + G2 + STAGE2_PASS
// ═══════════════════════════════════════════════════════════════════════════════

test("F13: status tool requires run_id (throws if missing)", async () => {
  const h = makeH100Harness({ state: h100State() });
  await assert.rejects(h.tools.statusTool.execute({}, makeExec()), /requires run_id/u);
});

test("F13: status marks completed only on full evidence (exit 0 + missing=0 + checksum + G2 + STAGE2_PASS)", async () => {
  const state = h100State();
  const run = makeRun(state);
  const fullEvidence = fullCompletedEvidence();
  const h = makeH100Harness({ state, remoteImpl: () => ({ stdout: fullEvidence, stderr: "", exitCode: 0, signal: null, timedOut: false }) });
  const result = await h.tools.statusTool.execute({ run_id: run.runId }, makeExec());
  assert.equal(result.status.h100Status.reconciled, "completed");
  assert.equal(state.runs[0].status, "completed");
});

test("F13: status stays reconciling when exit=0 but G2_PASS missing", async () => {
  const state = h100State();
  const run = makeRun(state);
  const evidence = fullCompletedEvidence().replace("G2_PASS run_dir=", "G2_PASS=absent\nG2_PASS run_dir=").replace("G2_PASS\n", "");
  // Simulate: exit 0 but G2 marker absent
  const partialEvidence = `== IDENTITY ==
pid=12345
pgroup=12345
started_utc=2026-08-27T12:00:00Z
hostname=genbioh100
uid=1000
cwd=/home/work
== STATUS ==
state=completed
updated=2026-08-27T12:00:00Z
pid=12345
detail=exit 0
== EXIT_CODE ==
0
== PID_ALIVE ==
dead
== PROCESS_EVIDENCE ==
== GPU_EVIDENCE ==
== CHECKSUM_VERIFY ==
stage2_environment_runtime.txt: OK
== G2_EVIDENCE ==
G2_PASS=absent
STAGE2_PASS=absent
missing_required_checks=0
== STDOUT_TAIL ==
== STDERR_TAIL ==
`;
  const h = makeH100Harness({ state, remoteImpl: () => ({ stdout: partialEvidence, stderr: "", exitCode: 0, signal: null, timedOut: false }) });
  const result = await h.tools.statusTool.execute({ run_id: run.runId }, makeExec());
  assert.equal(result.status.h100Status.reconciled, "failed", "exit 0 without G2_PASS is a gate failure");
});

test("F13: remote outage stays reconciling, NOT failed", async () => {
  const state = h100State();
  const run = makeRun(state);
  const h = makeH100Harness({ state, remoteImpl: () => { throw new Error("connection timeout"); } });
  const result = await h.tools.statusTool.execute({ run_id: run.runId }, makeExec());
  assert.equal(result.status.h100Status.state, "reconciling");
  assert.equal(state.runs[0].status, "running", "run must NOT be marked failed on remote outage");
});

test("F13d: pre-instrumentation termination (no identity, no exit_code, fatal stderr) is definitive failed and releases the lock", async () => {
  const state = h100State();
  const run = makeRun(state);
  run.startedAt = Date.now() - 300000;
  run.remoteLockDir = `${H100_STAGE_ROOT}/.stage2.lock`;
  const staleEvidence = `== IDENTITY ==
pid=
pgroup=
started_utc=
hostname=genbioh100
uid=1000
cwd=/home/work
== STATUS ==
state=unknown
== EXIT_CODE ==
pending
== PID_ALIVE ==
dead
== PROCESS_EVIDENCE ==
== GPU_EVIDENCE ==
== CHECKSUM_VERIFY ==
== G2_EVIDENCE ==
missing_required_checks=1
== STDOUT_TAIL ==
== STDERR_TAIL ==
stage2_environment_genbioh100.sh: line 39: AIZH100_RUN_DIR: AIZH100_RUN_DIR must be set by the plugin launcher
`;
  const h = makeH100Harness({ state, remoteImpl: (target, cmd) => {
    if (cmd.includes("LOCK_RELEASED")) return { stdout: "LOCK_RELEASED=1\n", stderr: "", exitCode: 0, signal: null, timedOut: false };
    return { stdout: staleEvidence, stderr: "", exitCode: 0, signal: null, timedOut: false };
  }});
  const result = await h.tools.statusTool.execute({ run_id: run.runId }, makeExec());
  assert.equal(result.status.h100Status.reconciled, "failed", "dead payload with fatal stderr and no identity/exit evidence must terminally fail");
  assert.match(String(result.status.h100Status.stderrTail), /AIZH100_RUN_DIR must be set by the plugin launcher/u);
  assert.equal(state.runs[0].status, "failed", "terminal verdict applies after token-verified lock release");
  assert.equal(state.runs[0].remoteLockDir, null, "remote lock is cleared after successful release");
});

test("F13d: recent launch with no identity yet stays reconciling (elapsed bound)", async () => {
  const state = h100State();
  const run = makeRun(state); // startedAt = now - 60000
  run.startedAt = Date.now() - 5000;
  const freshEvidence = `== IDENTITY ==
pid=
pgroup=
started_utc=
hostname=genbioh100
uid=1000
cwd=/home/work
== STATUS ==
state=unknown
== EXIT_CODE ==
pending
== PID_ALIVE ==
dead
== PROCESS_EVIDENCE ==
== GPU_EVIDENCE ==
== CHECKSUM_VERIFY ==
== G2_EVIDENCE ==
missing_required_checks=1
== STDOUT_TAIL ==
== STDERR_TAIL ==
some transient warning
`;
  const h = makeH100Harness({ state, remoteImpl: () => ({ stdout: freshEvidence, stderr: "", exitCode: 0, signal: null, timedOut: false }) });
  const result = await h.tools.statusTool.execute({ run_id: run.runId }, makeExec());
  assert.equal(result.status.h100Status.reconciled, "reconciling", "young run without identity must not be terminally failed");
});

// ═══════════════════════════════════════════════════════════════════════════════
// Finding 14: Launch ambiguity → reconciling, never failed/retryable
// ═══════════════════════════════════════════════════════════════════════════════

test("F14: ssh failure during launch → reconciling (not failed)", async () => {
  const state = h100State();
  const h = makeH100Harness({ state, remoteImpl: (target, cmd) => {
    if (cmd.includes("setsid")) return { stdout: "", stderr: "Connection reset by peer", exitCode: 255, signal: null, timedOut: false };
    return { stdout: "", stderr: "", exitCode: 0, signal: null, timedOut: false };
  }});
  await h.tools.stageTool.execute({}, makeExec());
  // Wait for the job to settle.
  await new Promise((r) => setTimeout(r, 50));
  const run = state.runs.find((r) => r.operation === "aizyme-h100-stage2");
  assert.equal(run.status, "reconciling", "ssh failure must be reconciling, not failed");
});

test("F14: launch ambiguity (run_identity not found) → reconciling", async () => {
  const state = h100State();
  const h = makeH100Harness({ state, remoteImpl: (target, cmd) => {
    if (cmd.includes("setsid")) return { stdout: "LAUNCH_RC=0\nLAUNCH_AMBIGUITY=1\n", stderr: "", exitCode: 0, signal: null, timedOut: false };
    return { stdout: "", stderr: "", exitCode: 0, signal: null, timedOut: false };
  }});
  await h.tools.stageTool.execute({}, makeExec());
  await new Promise((r) => setTimeout(r, 50));
  const run = state.runs.find((r) => r.operation === "aizyme-h100-stage2");
  assert.equal(run.status, "reconciling", "launch ambiguity must be reconciling");
});

// ═══════════════════════════════════════════════════════════════════════════════
// Finding 15: Cancel is local-observer-only, no remote cancellation claim
// ═══════════════════════════════════════════════════════════════════════════════

test("F15: cancel does NOT claim remote cancellation", async () => {
  const source = await readFile(join(__dirname, "..", "lib", "aizyme-h100.js"), "utf8");
  assert.ok(source.includes("local-observer-only"), "cancel must be documented as local-observer-only");
  assert.ok(source.includes("remote state unknown"), "cancel must note remote state is unknown");
  // kill -0 is a liveness probe (status tool), not a cancellation signal.
  // Reject actual termination signals: kill -9, kill -TERM, kill -KILL, kill -SIGTERM
  assert.ok(!source.includes("kill -9"), "must NOT send kill -9");
  assert.ok(!source.includes("kill -TERM"), "must NOT send kill -TERM");
  assert.ok(!source.includes("kill -KILL"), "must NOT send kill -KILL");
  assert.ok(!source.includes("kill -SIGTERM"), "must NOT send kill -SIGTERM");
});

test("F15: after cancel, run stays reconciling and lock is held", async () => {
  const source = await readFile(join(__dirname, "..", "lib", "aizyme-h100.js"), "utf8");
  assert.match(source, /controller\.signal\.aborted[\s\S]*run\.status = "reconciling"/u);
  assert.match(source, /local cancellation observed; remote state unknown; lock held/u);
});

// ═══════════════════════════════════════════════════════════════════════════════
// Bash script structural checks
// ═══════════════════════════════════════════════════════════════════════════════

test("script passes bash -n syntax check", () => {
  const result = spawnSync("/bin/bash", ["--noprofile", "--norc", "-n", SCRIPT_PATH], {
    env: { PATH: "/usr/bin:/bin", BASH_ENV: "/dev/null" }, timeout: 10000, encoding: "utf8",
  });
  assert.equal(result.status, 0, `bash -n failed: ${result.stderr}`);
});

test("script has set -euo pipefail", async () => {
  const content = await readFile(SCRIPT_PATH, "utf8");
  assert.ok(content.includes("set -euo pipefail"));
});

// ═══════════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════════

function makeExec() {
  return { agent: { id: "h100", session: { id: "h100", header: { cwd: "/tmp" } } }, signal: new AbortController().signal };
}

function h100State(overrides = {}) {
  return {
    policy: { hash: "hash-current" },
    remoteGrants: [{ target: "genbioh100", root: H100_PROJECT_ROOT, mode: "rw" }],
    runs: [],
    envelope: { target: "genbioh100", node: "genbioh100", partition: null, maxCpus: 16, maxGpus: 1, memGb: 32, concurrency: 1, policyHash: "hash-current" },
    ...overrides,
  };
}

function makeRun(state) {
  const run = {
    runId: "genbioh100-aizyme-stage2-testtoken123",
    target: "genbioh100",
    operation: "aizyme-h100-stage2",
    status: "running",
    startedAt: Date.now() - 60000,
    finishedAt: null,
    stdout: "", stderr: "", error: null, pid: 12345, jobId: "job-1",
    resources: { cpus: 16, gpus: 1, memGb: 32, concurrency: 1 },
    policyHash: "hash-current", node: "genbioh100", partition: null,
    envelope: {}, remoteRunDir: `${H100_STAGE_ROOT}/stage2-h100-testtoken123`,
    runToken: "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6", scriptSha: "abc", remoteGrants: [],
    finalization: null, memory: { status: "not-finalized", error: null, openVikingSessionId: null, traceId: null },
    registryRunId: null, registryError: null,
  };
  state.runs.push(run);
  return run;
}

function fullCompletedEvidence() {
  const runDir = `${H100_STAGE_ROOT}/stage2-h100-testtoken123`;
  const runToken = "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6"; // 32 hex chars
  return `== IDENTITY ==
pid=12345
pgroup=12345
started_utc=2026-08-27T12:00:00Z
hostname=genbioh100
uid=1000
cwd=/home/work
token=${runToken}
== STATUS ==
state=completed
updated=2026-08-27T12:05:00Z
pid=12345
detail=exit 0
== EXIT_CODE ==
0
== PID_ALIVE ==
dead
== PROCESS_EVIDENCE ==
== GPU_EVIDENCE ==
== CHECKSUM_VERIFY ==
CHECKSUM_OK=1
EVIDENCE_MANIFEST_SHA=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
== G2_EVIDENCE ==
G2_PASS token=${runToken} evidence_cksum=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa run_dir=${runDir}
STAGE2_PASS token=${runToken} evidence_cksum=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa run_dir=${runDir}
STAGE2_PASS_COUNT=1
target=genbioh100
cuda_visible_devices=0
omp_num_threads=16
memory_ulimit_kb=33554432
missing_required_checks=0
== STDOUT_TAIL ==
Stage 2 genbioh100 verification complete: G2_PASS (run-scoped, token-bound)
STAGE2_PASS
== STDERR_TAIL ==
`;
}

function makeH100Harness({ state = h100State(), remoteImpl, registryImpl, policy } = {}) {
  const calls = { remote: [], shell: [], jobs: 0, questions: 0 };
  let latest = null;
  const defaultPolicy = {
    targets: { genbioh100: {
      surface: "direct", login_shell: false, ssh_target: "genbioh100",
      limits: { cpu_threads_per_job: 16, mem_gb_per_job: 32, concurrent_gpu_jobs: 1, gpus_allowed: [0] },
    } },
  };
  const tools = createAizymeH100Tools({
    makeTool: (name, description, parameters, execute) => ({ name, description, parameters, execute }),
    requirePolicy: () => policy ?? defaultPolicy,
    requireState: () => state,
    publicState: () => ({}),
    requireRemoteAccess: async (target, needs) => needs.map((n) => ({ target, root: n.root, mode: "rw" })),
    runRemote: async (target, command) => { calls.remote.push(command); return remoteImpl?.(target, command) ?? { stdout: "", stderr: "", exitCode: 0, signal: null, timedOut: false }; },
    shell: {
      resolve: (request) => request,
      run: async (request) => { calls.shell.push(request.command); return { stdout: { text: "" }, stderr: { text: "" }, exitCode: 0, signal: null, timedOut: false }; },
    },
    userQuestions: {
      ask: async () => {
        calls.questions += 1;
        // Default: approve the transfer (for tests that get past the transfer gate).
        return { answers: [{ id: "genbio-h100-stage2-transfer", selected: ["Approve this transfer"] }] };
      },
    },
    jobs: { start(spec) { calls.jobs += 1; latest = spec.run(); return `job-${calls.jobs}`; } },
    config: { logMaxBytes: 65536, aizymeLocalRemoteBundle: join(__dirname, "..", "fixtures", "aizyme-h100") },
    runRegistry: registryImpl ?? { record: async () => ({ runId: "reg-1" }), update: async () => {} },
  });
  return { tools, calls, latest: () => latest };
}
