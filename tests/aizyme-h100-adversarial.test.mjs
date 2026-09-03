// Independent adversarial tests for the genbioh100 bash-native AI.zymes Stage-2
// path (aizyme-h100.js). Authored by test-engineer (team aizyme-h100-stage2-execution).
//
// Prepared in the testplan dir; promoted into the isolated copy's tests/ after
// the engineer (t4) completes. Independent of the engineer's aizyme-h100-stage2.test.mjs;
// targets: transmitted-command validity (shellQuote), exact-once/no-retry,
// required run_id, outage=reconciling, full completion predicate (exit0 +
// missing=0 + checksum -c + run-bound G2 + exactly-one stdout STAGE2_PASS),
// GPU0-only in the transmitted status command, durable non-draining logs, and
// payload static invariants.
//
// NOTE: tests that encode the captain's required contract are marked "(CONTRACT)"
// and expected to FAIL if the implementation regresses; they are reported, not
// silently dropped.
import assert from "node:assert/strict";
import { test } from "node:test";
import { spawnSync } from "node:child_process";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  createAizymeH100Tools,
  H100_PROJECT_ROOT,
  H100_STAGE_ROOT,
  H100_SCRIPT_NAME,
  H100_ARCHIVE_SHA,
} from "../lib/aizyme-h100.js";

const TEST_DIR = fileURLToPath(new URL(".", import.meta.url));
const PAYLOAD_PATH = join(TEST_DIR, "..", "fixtures", "aizyme-h100", "stage2_environment_genbioh100.sh");

const exec = { agent: { id: "te", session: { id: "te", header: { cwd: "/tmp" } } }, signal: new AbortController().signal };
const APPROVE = { answers: [{ id: "genbio-h100-stage2-transfer", selected: ["Approve this transfer"] }] };

function basePolicy() {
  return {
    hash: "hash-current",
    targets: {
      genbioh100: { surface: "direct", login_shell: false, ssh_target: "genbioh100", limits: { gpus_allowed: [0], cpu_threads_per_job: 16, mem_gb_per_job: 32, concurrent_gpu_jobs: 1 } },
    },
  };
}
function baseEnvelope() {
  return { target: "genbioh100", node: "genbioh100", partition: null, maxCpus: 16, maxGpus: 1, memGb: 32, concurrency: 1, policyHash: "hash-current" };
}

function harness({ remoteImpl, stateOverrides = {}, runRegistry = null, requireAccess = null, shellImpl = null, answer = APPROVE } = {}) {
  const calls = { remote: [], shell: [], jobs: 0, questions: 0, grants: [] };
  let latest = null;
  const state = {
    policy: basePolicy(),
    envelope: baseEnvelope(),
    remoteGrants: [{ target: "genbioh100", root: "/home/work/GenbioLAB/shared/daes_enzyme", write: true }],
    runs: [],
    ...stateOverrides,
  };
  const tools = createAizymeH100Tools({
    makeTool: (name, description, parameters, execute) => ({ name, description, parameters, execute }),
    requirePolicy: () => state.policy,
    requireState: () => state,
    publicState: () => ({}),
    runRemote: async (target, command) => { calls.remote.push(command); if (remoteImpl) return remoteImpl(target, command); return { stdout: "", stderr: "", exitCode: 0, signal: null, timedOut: false }; },
    shell: shellImpl ?? {
      resolve: (request) => request,
      run: async (request) => { calls.shell.push(request.command); return { stdout: { text: "" }, stderr: { text: "" }, exitCode: 0, signal: null, timedOut: false }; },
    },
    userQuestions: { ask: async () => { calls.questions += 1; return answer; } },
    jobs: { start(spec) { calls.jobs += 1; latest = spec.run(); return `job-${calls.jobs}`; } },
    config: { logMaxBytes: 65536, commandTimeoutMs: 30000, aizymeLocalRemoteBundle: join(TEST_DIR, "..", "fixtures", "aizyme-h100") },
    requireRemoteAccess: requireAccess ?? (async (target, reqs) => { calls.grants.push({ target, reqs }); return reqs; }),
    runRegistry,
  });
  return { tools, calls, state, latest: () => latest };
}

function runStatus(resp) { return async () => ({ stdout: resp, stderr: "", exitCode: 0, signal: null, timedOut: false }); }

// Default remote stub for a successful launch. Branch order matters:
// setsid-specific checks must come BEFORE the generic hash/lock branches because
// the launch command itself re-verifies the script hash.
const SCRIPT_SHA = "23c4394b27a6d49a54b240c92cb8784278146b30b2499a819d4892d73f445d46";
const ARCHIVE_SHA = H100_ARCHIVE_SHA;
function launchRemote(overrides = {}) {
  const { launchExit = 0, launchOut = "pid=4242\npgroup=4242\nstarted_utc=2026-08-31T00:00:00Z\nhostname=h100\n" } = overrides;
  return async (_target, command) => {
    if (command.includes("setsid")) return { stdout: launchOut, stderr: "", exitCode: launchExit, signal: null, timedOut: false };
    if (command.includes("== STATUS ==")) return runStatus("")(_target, command);
    if (command.includes("LOCK_RELEASED")) return { stdout: "LOCK_RELEASED=1\n", stderr: "", exitCode: 0, signal: null, timedOut: false };
    if (command.includes("LOCK_ACQUIRED") || command.includes("lock=")) return { stdout: "LOCK_ACQUIRED=1\n", stderr: "", exitCode: 0, signal: null, timedOut: false };
    if (command.includes("sha256sum")) return { stdout: `${SCRIPT_SHA}  stage2_environment_genbioh100.sh\n${ARCHIVE_SHA}  AIzymes-52176ff.tar.gz\n`, stderr: "", exitCode: 0, signal: null, timedOut: false };
    if (command.includes("bash --noprofile --norc -n")) return { stdout: "", stderr: "", exitCode: 0, signal: null, timedOut: false };
    return { stdout: "", stderr: "", exitCode: 0, signal: null, timedOut: false };
  };
}

function bashSyntaxOk(text) {
  const r = spawnSync("/bin/bash", ["--noprofile", "--norc", "-n", "-"], { input: text, env: { PATH: "/usr/bin:/bin", BASH_ENV: "/dev/null" }, timeout: 5000, encoding: "utf8" });
  return { ok: r.status === 0, stderr: String(r.stderr ?? "") };
}

// Reconstruct the remote body from a strictRemote ssh command as the local shell
// would pass it through double quotes: strip the wrapper and unescape \$ \" \\.
function remoteBody(command) {
  const marker = " -- genbioh100 ";
  const idx = command.indexOf(marker);
  assert.ok(idx >= 0, "strict ssh contract marker present");
  const arg = command.slice(idx + marker.length).trim();
  if (!arg.startsWith('"') || !arg.endsWith('"')) throw new Error(`remote arg is not a double-quoted shell string: ${arg.slice(0, 80)}`);
  return arg.slice(1, -1).replace(/\\([$"\\])/gu, (_, p) => (p === "$" ? "$" : p === '"' ? '"' : "\\"));
}

// Build the status remote response, run-bound to the given run dir, using the
// final evidence protocol: identity token, EVIDENCE_MANIFEST_SHA / CHECKSUM_OK
// in CHECKSUM_VERIFY, evidence_cksum + STAGE2_PASS_COUNT in G2_EVIDENCE.
function statusOutput(runDir, { exitCode = "0", missing = "0", passCount = "1", runToken = "36c8169d6764939c296b9320a545cb3a", g2Dir = null, stage2Dir = null, checksumOk = "1", cksumManifestSha = "9b0f6d0f848f54731fc69f6312b625c4d21e9b0e4d38f9a2b0b8b9d1b56f1c7a3", evidenceCksum = null, state = "completed", stdoutTail = ["all_required_imports=ok"] } = {}) {
  const g2 = g2Dir === null ? `G2_PASS run_dir=${runDir}` : `G2_PASS run_dir=${g2Dir}`;
  const s2 = stage2Dir === null ? `STAGE2_PASS run_dir=${runDir}` : `STAGE2_PASS run_dir=${stage2Dir}`;
  const evCksum = evidenceCksum === null ? cksumManifestSha : evidenceCksum;
  return [
    "== IDENTITY ==", "pid=4242", `token=${runToken}`, "pgroup=4242",
    "== STATUS ==", `state=${state}`, "updated=2026-08-31T00:00:00Z", "pid=4242",
    "== EXIT_CODE ==", exitCode,
    "== PID_ALIVE ==", "dead",
    "== PROCESS_EVIDENCE ==", "rss_kb=123456", "threads=16",
    "== GPU_EVIDENCE ==", "4242, 1024 MiB",
    "== CHECKSUM_VERIFY ==", `EVIDENCE_MANIFEST_SHA=${cksumManifestSha}`, `CHECKSUM_OK=${checksumOk}`,
    "== G2_EVIDENCE ==", g2, s2, `evidence_cksum=${evCksum}`, `missing_required_checks=${missing}`, `STAGE2_PASS_COUNT=${passCount}`,
    "== STDOUT_TAIL ==", ...stdoutTail,
    "== STDERR_TAIL ==", "",
  ].join("\n");
}

async function launchRun(h) {
  const r = await h.tools.stageTool.execute({}, exec);
  // The tracked job is fire-and-forget; settle it so all transmitted remote
  // commands have flushed before we assert on them.
  await h.latest().done;
  return r.status.started;
}

function statusHarness(run, remoteStdout, opts = {}) {
  const h = harness({
    remoteImpl: async (_t, cmd) => {
      if (cmd.includes("== STATUS ==")) return runStatus(remoteStdout)(_t, cmd);
      if (cmd.includes("LOCK_RELEASED")) return { stdout: "LOCK_RELEASED=1\n", stderr: "", exitCode: 0, signal: null, timedOut: false };
      return launchRemote()(_t, cmd);
    },
    stateOverrides: { runs: [run] },
    ...opts,
  });
  return h;
}

// ─────────────────────────────────────────────────────────────────────────────
// A. Transmitted remote-command validity (catches unterminated shellQuote).
// ─────────────────────────────────────────────────────────────────────────────
test("A1: every transmitted remote command is syntactically valid bash and quote-balanced (shellQuote)", async () => {
  const h = harness({ remoteImpl: launchRemote() });
  await h.tools.stageTool.execute({}, exec);
  if (h.latest()) await h.latest().done;
  assert.ok(h.calls.remote.length >= 4, `expected init/checksum/syntax/launch, got ${h.calls.remote.length}`);
  for (const command of h.calls.remote) {
    assert.ok(command.startsWith("ssh -T -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=yes -- genbioh100 "), "strict ssh contract");
    const body = remoteBody(command);
    const check = bashSyntaxOk(body);
    assert.equal(check.ok, true, `transmitted body must parse under bash -n: ${check.stderr}\nbody: ${body}`);
  }
});

test("A2: init lock+run-dir command quotes each path with a balanced pair and a 32-hex token (regression: unterminated shellQuote)", async () => {
  const h = harness({ remoteImpl: launchRemote() });
  await h.tools.stageTool.execute({}, exec);
  if (h.latest()) await h.latest().done;
  const init = h.calls.remote.find((c) => c.includes("lock="));
  assert.ok(init, "init lock transmission present");
  const body = remoteBody(init);
  const quotedPairs = (body.match(/'[^']*'/gu) ?? []).length;
  assert.ok(quotedPairs >= 2, `expected >=2 balanced single-quoted tokens, got ${quotedPairs}: ${body}`);
  const mk = /mkdir -p '([^']+)'/.exec(body);
  assert.ok(mk, `must create a quoted fresh run dir: ${body}`);
  assert.ok(mk[1].startsWith(`${H100_STAGE_ROOT}/stage2-h100-`), "fresh collision-safe run dir under stage root");
  assert.match(body, /\.stage2\.lock/u, "cross-session global lock path used");
  const tok = /token=%s\\nlocked_utc=%s\\n' '([0-9a-f]{32})'/.exec(body);
  assert.ok(tok, `init writes the CSPRNG token into the lock: ${body}`);
});

// ─────────────────────────────────────────────────────────────────────────────
// B. Exact-once / no retry on side-effecting ops.
// ─────────────────────────────────────────────────────────────────────────────
test("B1: launch is dispatched exactly once even on transport ambiguity; no retry", async () => {
  let launchCount = 0;
  const remoteImpl = async (_t, command) => {
    if (command.includes("setsid")) { launchCount += 1; return { stdout: "", stderr: "Operation timed out", exitCode: 255, signal: null, timedOut: true }; }
    if (command.includes("lock=")) return { stdout: "LOCK_ACQUIRED=1\n", stderr: "", exitCode: 0, signal: null, timedOut: false };
    if (command.includes("sha256sum")) return { stdout: `${SCRIPT_SHA}  stage2_environment_genbioh100.sh\n${ARCHIVE_SHA}  AIzymes-52176ff.tar.gz\n`, stderr: "", exitCode: 0, signal: null, timedOut: false };
    if (command.includes("bash --noprofile --norc -n")) return { stdout: "", stderr: "", exitCode: 0, signal: null, timedOut: false };
    return { stdout: "", stderr: "", exitCode: 0, signal: null, timedOut: false };
  };
  const h = harness({ remoteImpl });
  const r = await h.tools.stageTool.execute({}, exec);
  if (h.latest()) await h.latest().done;
  const run = r.status.started;
  assert.equal(launchCount, 1, "launch ssh issued exactly once");
  assert.equal(run.status, "reconciling", "transport ambiguity → reconciling, never failed");
  assert.match(run.error, /255/u, "records the transport failure");
  await assert.rejects(h.tools.stageTool.execute({}, exec), /pair-lock|in-flight|aggregate/u);
  assert.equal(launchCount, 1, "no second launch after ambiguity");
});

test("B2: LAUNCH_AMBIGUITY (run_identity absent after 1s) → reconciling, exactly one dispatch", async () => {
  let launchCount = 0;
  const remoteImpl = async (_t, command) => {
    if (command.includes("setsid")) { launchCount += 1; return { stdout: "LAUNCH_RC=0\nLAUNCH_AMBIGUITY=1\n", stderr: "", exitCode: 0, signal: null, timedOut: false }; }
    if (command.includes("lock=")) return { stdout: "LOCK_ACQUIRED=1\n", stderr: "", exitCode: 0, signal: null, timedOut: false };
    if (command.includes("sha256sum")) return { stdout: `${SCRIPT_SHA}  stage2_environment_genbioh100.sh\n${ARCHIVE_SHA}  AIzymes-52176ff.tar.gz\n`, stderr: "", exitCode: 0, signal: null, timedOut: false };
    if (command.includes("bash --noprofile --norc -n")) return { stdout: "", stderr: "", exitCode: 0, signal: null, timedOut: false };
    return { stdout: "", stderr: "", exitCode: 0, signal: null, timedOut: false };
  };
  const h = harness({ remoteImpl });
  const r = await h.tools.stageTool.execute({}, exec);
  if (h.latest()) await h.latest().done;
  assert.equal(launchCount, 1);
  assert.equal(r.status.started.status, "reconciling");
  await assert.rejects(h.tools.stageTool.execute({}, exec), /pair-lock|in-flight|aggregate/u);
  assert.equal(launchCount, 1, "never re-dispatches");
});

test("B2b: launch succeeds but PID/run_identity unparseable → reconciling (not silent success)", async () => {
  const h = harness({ remoteImpl: launchRemote({ launchOut: "LAUNCH_RC=0\nnoop=1\n" }) });
  const r = await h.tools.stageTool.execute({}, exec);
  if (h.latest()) await h.latest().done;
  assert.equal(r.status.started.status, "reconciling");
  assert.match(r.status.started.error, /PID\/run_identity is missing or unparseable/u);
});

test("B3: staging rclone failure never dispatches (no setsid), run stays locked-reconciling", async () => {
  const h = harness({
    remoteImpl: launchRemote(),
    shellImpl: { resolve: (r) => r, run: async () => ({ stdout: { text: "" }, stderr: { text: "rclone: connect failed" }, exitCode: 3, signal: null, timedOut: false }) },
  });
  await h.tools.stageTool.execute({}, exec);
  if (h.latest()) await h.latest().done;
  assert.equal(h.calls.remote.filter((c) => c.includes("setsid")).length, 0, "no launch dispatched");
  await assert.rejects(h.tools.stageTool.execute({}, exec), /pair-lock|in-flight|aggregate/u, "post-dispatch lock must stay held (no retry)");
});

test("B4: successful launch is exactly one setsid dispatch, one job, running + CSPRNG token", async () => {
  const h = harness({ remoteImpl: launchRemote() });
  const r = await h.tools.stageTool.execute({}, exec);
  if (h.latest()) await h.latest().done;
  assert.equal(h.calls.remote.filter((c) => c.includes("setsid")).length, 1);
  assert.equal(h.calls.jobs, 1);
  assert.equal(r.status.started.status, "running");
  assert.equal(r.status.started.pid, 4242);
  assert.equal(r.status.started.runToken.length, 32, "128-bit CSPRNG token (32 hex chars)");
  assert.match(r.status.started.runId, /^genbioh100-aizyme-stage2-[0-9a-f]{32}$/u);
});

// ─────────────────────────────────────────────────────────────────────────────
// C. Required run_id + outage handling.
// ─────────────────────────────────────────────────────────────────────────────
test("C1: status requires run_id (missing/empty throws)", async () => {
  const h = harness({ remoteImpl: launchRemote() });
  await h.tools.stageTool.execute({}, exec);
  if (h.latest()) await h.latest().done;
  await assert.rejects(h.tools.statusTool.execute({}, exec), /requires run_id/u);
  await assert.rejects(h.tools.statusTool.execute({ run_id: "" }, exec), /requires run_id/u);
});

test("C2: status with unknown run_id returns found:false (no crash)", async () => {
  const h = harness({ remoteImpl: launchRemote() });
  const r = await h.tools.statusTool.execute({ run_id: "genbioh100-aizyme-stage2-doesnotexist" }, exec);
  assert.equal(r.status.h100Status.found, false);
});

test("C3: remote status outage → reconciling (never failed, never fabricated)", async () => {
  const run = await launchRun(harness({ remoteImpl: launchRemote() }));
  const h = harness({
    remoteImpl: async () => { throw new Error("ssh: connect to host ... Operation timed out"); },
    stateOverrides: { runs: [run] },
  });
  const s = await h.tools.statusTool.execute({ run_id: run.runId }, exec);
  assert.equal(s.status.h100Status.state, "reconciling");
  assert.match(s.status.h100Status.error, /outage/u);
  assert.equal(run.status, "running", "run record not mutated to failed");
});

test("C4: status remote exit!=0 (e.g. run dir gone) → reconciling, not failed/fabricated", async () => {
  const run = await launchRun(harness({ remoteImpl: launchRemote() }));
  const h = harness({
    remoteImpl: async () => ({ stdout: "", stderr: "test: /...: No such file or directory", exitCode: 2, signal: null, timedOut: false }),
    stateOverrides: { runs: [run] },
  });
  const s = await h.tools.statusTool.execute({ run_id: run.runId }, exec);
  assert.equal(s.status.h100Status.state, "reconciling");
  assert.ok(s.status.h100Status.error.includes("exit 2") || s.status.h100Status.error.includes("No such file"), "reports failure cause without fabricating a verdict");
  assert.equal(run.status, "running");
});

test("C5: status grant gate uses requireRemoteAccess write:false on the exact shared root", async () => {
  const run = await launchRun(harness({ remoteImpl: launchRemote() }));
  const h = harness({ remoteImpl: launchRemote(), stateOverrides: { runs: [run] } });
  await h.tools.statusTool.execute({ run_id: run.runId }, exec);
  const grant = h.calls.grants.find((g) => g.target === "genbioh100");
  assert.ok(grant, "requireRemoteAccess called for genbioh100");
  assert.equal(grant.reqs[0].write, false);
  assert.equal(grant.reqs[0].root, H100_PROJECT_ROOT);
});

// ─────────────────────────────────────────────────────────────────────────────
// D. Completion predicate: exit0 + missing=0 + checksum -c OK + run-bound G2 +
//    STAGE2_PASS + EXACTLY-ONE stdout marker (captain's contract).
// ─────────────────────────────────────────────────────────────────────────────
test("D1: full evidence completes (happy path validates the harness)", async () => {
  const run = await launchRun(harness({ remoteImpl: launchRemote() }));
  const h = statusHarness(run, statusOutput(run.remoteRunDir, { runToken: run.runToken }));
  const s = await h.tools.statusTool.execute({ run_id: run.runId }, exec);
  assert.equal(s.status.h100Status.reconciled, "completed");
  assert.equal(s.status.h100Status.stage2PassCount, 1);
  assert.equal(s.status.h100Status.checksumOk, true);
  assert.equal(s.status.h100Status.g2PassRunBound, true);
  assert.equal(run.status, "completed");
});

test("D2 (CONTRACT): stdout WITHOUT the STAGE2_PASS marker must NOT complete (exit 0 → gate failure)", async () => {
  const run = await launchRun(harness({ remoteImpl: launchRemote() }));
  const h = statusHarness(run, statusOutput(run.remoteRunDir, { runToken: run.runToken, passCount: "0" }));
  const s = await h.tools.statusTool.execute({ run_id: run.runId }, exec);
  assert.equal(s.status.h100Status.stage2PassCount, 0);
  assert.notEqual(s.status.h100Status.reconciled, "completed", "no stdout marker → never completed");
});

test("D3 (CONTRACT): stdout with TWO STAGE2_PASS markers must NOT complete", async () => {
  const run = await launchRun(harness({ remoteImpl: launchRemote() }));
  const h = statusHarness(run, statusOutput(run.remoteRunDir, { runToken: run.runToken, passCount: "2" }));
  const s = await h.tools.statusTool.execute({ run_id: run.runId }, exec);
  assert.equal(s.status.h100Status.stage2PassCount, 2);
  assert.notEqual(s.status.h100Status.reconciled, "completed");
});

test("D4: missing_required_checks != 0 → never completed (gate failure)", async () => {
  const run = await launchRun(harness({ remoteImpl: launchRemote() }));
  const h = statusHarness(run, statusOutput(run.remoteRunDir, { runToken: run.runToken, missing: "2" }));
  const s = await h.tools.statusTool.execute({ run_id: run.runId }, exec);
  assert.notEqual(s.status.h100Status.reconciled, "completed");
  assert.equal(s.status.h100Status.missingChecks, "2");
});

test("D5: checksum FAILED → never completed", async () => {
  const run = await launchRun(harness({ remoteImpl: launchRemote() }));
  const h = statusHarness(run, statusOutput(run.remoteRunDir, { runToken: run.runToken, checksumOk: "0" }));
  const s = await h.tools.statusTool.execute({ run_id: run.runId }, exec);
  assert.notEqual(s.status.h100Status.reconciled, "completed");
  assert.equal(s.status.h100Status.checksumOk, false);
});

test("D6: G2_PASS not run-bound (different run_dir) → never completed", async () => {
  const run = await launchRun(harness({ remoteImpl: launchRemote() }));
  const foreign = `${H100_STAGE_ROOT}/stage2-h100-OTHER`;
  const h = statusHarness(run, statusOutput(run.remoteRunDir, { runToken: run.runToken, g2Dir: foreign, stage2Dir: foreign }));
  const s = await h.tools.statusTool.execute({ run_id: run.runId }, exec);
  assert.equal(s.status.h100Status.g2PassRunBound, false);
  assert.notEqual(s.status.h100Status.reconciled, "completed");
});

test("D8 (CONTRACT): evidence_cksum must equal EVIDENCE_MANIFEST_SHA (tamper cross-check) → not completed on mismatch", async () => {
  const run = await launchRun(harness({ remoteImpl: launchRemote() }));
  const h = statusHarness(run, statusOutput(run.remoteRunDir, { runToken: run.runToken, evidenceCksum: "0".repeat(64) }));
  const s = await h.tools.statusTool.execute({ run_id: run.runId }, exec);
  assert.notEqual(s.status.h100Status.reconciled, "completed", "evidence checksum mismatch must never complete");
});

test("D9 (CONTRACT): run_identity token must match the run token → not completed on mismatch", async () => {
  const run = await launchRun(harness({ remoteImpl: launchRemote() }));
  const h = statusHarness(run, statusOutput(run.remoteRunDir, { runToken: "f".repeat(32) }));
  const s = await h.tools.statusTool.execute({ run_id: run.runId }, exec);
  assert.notEqual(s.status.h100Status.reconciled, "completed", "identity token mismatch must never complete");
});

test("D7: definitive nonzero exit → failed (terminal) and lock released", async () => {
  const run = await launchRun(harness({ remoteImpl: launchRemote() }));
  const h = statusHarness(run, statusOutput(run.remoteRunDir, { runToken: run.runToken, exitCode: "1", passCount: "0", state: "failed" }));
  const s = await h.tools.statusTool.execute({ run_id: run.runId }, exec);
  assert.equal(s.status.h100Status.reconciled, "failed");
  assert.equal(s.status.h100Status.exitCode, 1);
  assert.equal(run.status, "failed");
  // Terminal reconciliation releases the pair-lock → a fresh launch is allowed.
  const h2 = harness({ remoteImpl: launchRemote() });
  const r = await h2.tools.stageTool.execute({}, exec);
  assert.equal(r.status.started.status, "running");
});

// ─────────────────────────────────────────────────────────────────────────────
// E. GPU0-only in the transmitted status command.
// ─────────────────────────────────────────────────────────────────────────────
test("E1: transmitted status command is GPU0-only (no -L, no GPU1, no gpu_util)", async () => {
  const run = await launchRun(harness({ remoteImpl: launchRemote() }));
  const h = statusHarness(run, statusOutput(run.remoteRunDir, {}));
  await h.tools.statusTool.execute({ run_id: run.runId }, exec);
  const statusCmd = h.calls.remote.find((c) => c.includes("== STATUS =="));
  assert.ok(statusCmd);
  assert.doesNotMatch(statusCmd, /nvidia-smi -L/u, "no full-GPU listing");
  assert.doesNotMatch(statusCmd, /\bgpu1\b/u, "no GPU 1");
  assert.doesNotMatch(statusCmd, /gpu_util/u, "protected gpu_util untouched");
  assert.match(statusCmd, /nvidia-smi -i 0/u, "GPU 0 queried exclusively");
});

// ─────────────────────────────────────────────────────────────────────────────
// V. Vertical producer/consumer contract: the transmitted status command and
//    the payload must actually EMIT every marker the completion consumer
//    requires (CHECKSUM_OK, EVIDENCE_MANIFEST_SHA, STAGE2_PASS_COUNT, token).
// ─────────────────────────────────────────────────────────────────────────────
test("V1 (CONTRACT): emitted statusCmd must produce CHECKSUM_OK, EVIDENCE_MANIFEST_SHA and STAGE2_PASS_COUNT", async () => {
  const run = await launchRun(harness({ remoteImpl: launchRemote() }));
  const h = statusHarness(run, statusOutput(run.remoteRunDir, { runToken: run.runToken }));
  await h.tools.statusTool.execute({ run_id: run.runId }, exec);
  const statusCmd = h.calls.remote.find((c) => c.includes("== STATUS =="));
  assert.ok(statusCmd, "status command transmitted");
  assert.match(statusCmd, /CHECKSUM_OK=1|grep -xc '^STAGE2_PASS\$'|STAGE2_PASS_COUNT=/u, "statusCmd must compute/emit the explicit stdout marker count");
  assert.match(statusCmd, /EVIDENCE_MANIFEST_SHA/u, "statusCmd must emit the checksums-manifest SHA for the evidence cross-check");
  assert.match(statusCmd, /sha256sum -c/u, "statusCmd must verify the checksums manifest");
});

test("V2 (CONTRACT): payload must emit/cross-check what the consumer needs (token-bound G2 markers, one stdout STAGE2_PASS)", async () => {
  const payload = await readFile(PAYLOAD_PATH, "utf8");
  // Payload emits evidence_cksum + token in G2 markers and run_identity token.
  assert.match(payload, /printf 'G2_PASS token=%s evidence_cksum=%s run_dir=%s\\n'/u);
  assert.match(payload, /printf 'token=%s\\n' "\$RUN_TOKEN"/u);
  assert.match(payload, /RUN_TOKEN="\$\{AIZH100_RUN_TOKEN:/u);
  assert.equal((payload.match(/printf 'STAGE2_PASS\\n'/gu) ?? []).length, 1, "exactly one STAGE2_PASS to stdout");
  // The plugin passes AIZH100_RUN_TOKEN; the remote status must still surface
  // the stdout STAGE2_PASS count explicitly (or the payload must record it).
});

// ─────────────────────────────────────────────────────────────────────────────
// F. Durable non-draining logs + immutable evidence.
// ─────────────────────────────────────────────────────────────────────────────
test("F1: status never deletes/truncates remote logs (durable non-draining), reads bounded tails", async () => {
  const run = await launchRun(harness({ remoteImpl: launchRemote() }));
  const h = statusHarness(run, statusOutput(run.remoteRunDir, { runToken: run.runToken }));
  await h.tools.statusTool.execute({ run_id: run.runId }, exec);
  for (const cmd of h.calls.remote) {
    assert.doesNotMatch(cmd, /\b(?:rm|truncate)\b[^;]*stdout\.log/u, "never deletes/truncates stdout.log");
    assert.doesNotMatch(cmd, /\b(?:rm|truncate)\b[^;]*stderr\.log/u, "never deletes/truncates stderr.log");
    assert.doesNotMatch(cmd, /\b(?:rm|truncate)\b[^;]*manifests\//u, "never deletes/truncates manifests");
    assert.doesNotMatch(cmd, /\btruncate\b/u, "never truncates");
  }
  const statusCmd = h.calls.remote.find((c) => c.includes("== STATUS =="));
  assert.match(statusCmd, /tail -20 stdout\.log/u);
  assert.match(statusCmd, /tail -10 stderr\.log/u);
});

// ─────────────────────────────────────────────────────────────────────────────
// G. Payload static invariants.
// ─────────────────────────────────────────────────────────────────────────────
function executablePayload(text) {
  return text.split(/\r?\n/u).filter((l) => !/^\s*#/u.test(l.trim()) && l.trim() !== "").join("\n");
}

test("G1: payload forbids network/install/clone/HOME scan/GPU1 in EXECUTABLE text", async () => {
  const payload = await readFile(PAYLOAD_PATH, "utf8");
  const exec_text = executablePayload(payload);
  const forbidden = [/\bwget\b/u, /\bcurl\b/u, /\bpip\s+install\b/u, /\bconda\s+install\b/u, /\bgit\s+clone\b/u, /snapshot_download/u, /find\s+"\$HOME"/u, /\bnvidia-smi\s+-L\b/u, /\bgpu1\b/u, /\bsbatch\b/u, /\bsrun\b/u, /\bsalloc\b/u, /\bmodule\s+load\b/u, /\bhuggingface_hub\.snapshot_download/u];
  for (const re of forbidden) assert.doesNotMatch(exec_text, re, `forbidden executable pattern ${re}`);
  assert.match(exec_text, /nvidia-smi -i 0/u);
});

test("G2: payload hard-ulimit (32 GiB) unconditional; writes run-scoped; set -euo pipefail", async () => {
  const payload = await readFile(PAYLOAD_PATH, "utf8");
  assert.match(payload, /ulimit -v 33554432/u, "32 GiB virtual-memory cap present");
  assert.doesNotMatch(payload, /ulimit -v 33554432\s*\|\|\s*true/u, "hard fail, never silenced");
  assert.match(payload, /set -euo pipefail/u);
  assert.ok(payload.includes('RUN_DIR="${AIZH100_RUN_DIR:?'), "run dir required");
  assert.ok(payload.includes("FATAL: refusing rm -rf outside RUN_DIR"), "rm -rf containment guard present");
});

test("G3: payload reads only fixed roots, prints exactly one stdout STAGE2_PASS", async () => {
  const payload = await readFile(PAYLOAD_PATH, "utf8");
  assert.match(payload, /ROOT="\/home\/work\/GenbioLAB\/shared\/daes_enzyme"/u);
  assert.match(payload, /CUDA_VISIBLE_DEVICES=0/u);
  assert.match(payload, /OMP_NUM_THREADS=16/u);
  const stdoutMarkers = (payload.match(/printf 'STAGE2_PASS\\n'/gu) ?? []).length;
  assert.equal(stdoutMarkers, 1, "payload prints STAGE2_PASS to stdout exactly once");
});

test("G4: launch transmits GPU0/OMP pinning at the plugin boundary (defense in depth)", async () => {
  const h = harness({ remoteImpl: launchRemote() });
  await h.tools.stageTool.execute({}, exec);
  if (h.latest()) await h.latest().done;
  const launch = h.calls.remote.find((c) => c.includes("setsid"));
  assert.ok(launch, "launch command transmitted");
  assert.match(launch, /CUDA_VISIBLE_DEVICES=0/u);
  assert.match(launch, /OMP_NUM_THREADS=16/u);
  assert.match(launch, /< \/dev\/null/u);
  assert.match(launch, /env -i PATH=\/usr\/bin:\/bin:\/usr\/sbin:\/sbin/u);
  assert.doesNotMatch(launch, /CUDA_VISIBLE_DEVICES=1/u);
});

test("G5: payload immutable archive contract matches the plugin pin", async () => {
  const payload = await readFile(PAYLOAD_PATH, "utf8");
  assert.match(payload, new RegExp(`EXPECTED_ARCHIVE_SHA=${H100_ARCHIVE_SHA}`));
  assert.match(payload, /EXPECTED_COMMIT=52176ffab5d00b54f76141de8721949a28fe674c/u);
  assert.doesNotMatch(executablePayload(payload), /rm -rf "\$(?:ROOT|WF)"/u, "never removes shared root/workflow");
});

// ─────────────────────────────────────────────────────────────────────────────
// H. Envelope/exact-fit + capacity + approval order.
// ─────────────────────────────────────────────────────────────────────────────
test("H1: stale envelope policy hash rejected before any side effect", async () => {
  const h = harness({ stateOverrides: { policy: { ...basePolicy(), hash: "hash-rotated" } } });
  await assert.rejects(h.tools.stageTool.execute({}, exec), /stale/u);
  assert.deepEqual({ q: h.calls.questions, j: h.calls.jobs, r: h.calls.remote.length, s: h.calls.shell.length }, { q: 0, j: 0, r: 0, s: 0 });
});

test("H2: envelope memGb non-null and >=32; maxGpus exactly 1", async () => {
  await assert.rejects(harness({ stateOverrides: { envelope: { ...baseEnvelope(), memGb: null } } }).tools.stageTool.execute({}, exec), /memGb must be non-null/u);
  await assert.rejects(harness({ stateOverrides: { envelope: { ...baseEnvelope(), memGb: 16 } } }).tools.stageTool.execute({}, exec), /memGb 16 < required 32/u);
  await assert.rejects(harness({ stateOverrides: { envelope: { ...baseEnvelope(), maxGpus: 2 } } }).tools.stageTool.execute({}, exec), /exactly 1/u);
});

test("H3: a reconciling run still counts against aggregate capacity (blocks a new launch)", async () => {
  const run = await launchRun(harness({ remoteImpl: launchRemote() }));
  const second = harness({
    remoteImpl: launchRemote(),
    stateOverrides: { runs: [run, { ...run, status: "reconciling", runId: "genbioh100-aizyme-stage2-second" }] },
  });
  await assert.rejects(second.tools.stageTool.execute({}, exec), /aggregate capacity/u);
});

test("H4: transfer rejection is a clean abort with zero remote side effects (lock released)", async () => {
  const reject = { answers: [{ id: "genbio-h100-stage2-transfer", selected: [] }] };
  const h = harness({ remoteImpl: launchRemote(), answer: reject });
  await assert.rejects(h.tools.stageTool.execute({}, exec), /material transfer was not approved/u);
  assert.equal(h.calls.remote.filter((c) => c.includes("setsid") || c.includes("mkdir") || c.includes("test ! -e")).length, 0, "no remote mutation on rejection");
  const h2 = harness({ remoteImpl: launchRemote() });
  const r = await h2.tools.stageTool.execute({}, exec);
  assert.equal(r.status.started.status, "running", "lock released → a later approved launch works");
});

test("H5: registry failure is fail-closed (aborts the launch)", async () => {
  const h = harness({ runRegistry: { record: async () => { throw new Error("registry down"); } } });
  await assert.rejects(h.tools.stageTool.execute({}, exec), /registry record failed \(fail-closed\)/u);
  assert.equal(h.calls.remote.length, 0, "no remote when registry fails");
  assert.equal(h.calls.jobs, 0, "no job when registry fails");
});

test("H6: policy surface must be direct and matching limits", async () => {
  await assert.rejects(harness({ stateOverrides: { policy: { ...basePolicy(), targets: { genbioh100: { ...basePolicy().targets.genbioh100, surface: "shell" } } } } }).tools.stageTool.execute({}, exec), /surface=direct/u);
  await assert.rejects(harness({ stateOverrides: { policy: { ...basePolicy(), targets: { genbioh100: { ...basePolicy().targets.genbioh100, limits: { gpus_allowed: [0, 1], cpu_threads_per_job: 16, mem_gb_per_job: 32, concurrent_gpu_jobs: 1 } } } } } }).tools.stageTool.execute({}, exec), /GPU 0 only/u);
});

// ─────────────────────────────────────────────────────────────────────────────
// T. Captain t7 code-review findings coverage (14 blockers → adversarial checks
//    that would regress if any fix were reverted).
// ─────────────────────────────────────────────────────────────────────────────
test("T1 (t7#2): registry record uses source 'aizyme' (matches run-registry allowlist)", async () => {
  const records = [];
  const h = harness({ runRegistry: { record: async (_sid, payload) => { records.push(payload); return { runId: "r1" }; }, update: async () => {} } });
  const run = await launchRun(h);
  assert.ok(records.length >= 1, "registry.record called");
  assert.equal(records[0].source, "aizyme", `registry source must be 'aizyme', got '${records[0].source}'`);
  assert.equal(records[0].project, "aizyme");
  assert.equal(records[0].operation, "stage2");
  assert.ok(run.status === "running" || run.status === "reconciling", "launch proceeds past registry");
});

test("T2 (t7#6): payload checksum manifest excludes self + pass markers, nonempty, self-verifies, no || true", async () => {
  const payload = await readFile(PAYLOAD_PATH, "utf8");
  assert.match(payload, /! -name "stage2_checksums\.sha256" ! -name "G2_PASS" ! -name "STAGE2_PASS" -exec sha256sum/u, "checksum file must exclude itself and the pass markers");
  assert.match(payload, /test -s "\$CKSUM_FILE"/u, "checksum manifest must be asserted nonempty (no || true)");
  assert.match(payload, /sha256sum -c stage2_checksums\.sha256/u, "self-verifies with sha256sum -c");
});

test("T3 (t7#8): AI.zymes import asserts resolved __file__ realpath under ACTIVE_CODE/src", async () => {
  const payload = await readFile(PAYLOAD_PATH, "utf8");
  assert.match(payload, /realpath_verified=under_active_code/u, "payload must print the realpath-verification marker");
  assert.match(payload, /real\.startswith\(code_src \+ "\/"\)/u, "resolved file must be under code/src");
});

test("T4 (t7#9): MPNN present_no_probe must NOT set mpnn_ok (no functional probe = insufficient)", async () => {
  const payload = await readFile(PAYLOAD_PATH, "utf8");
  const block = payload.split("# 8. ProteinMPNN / LaSerMPNN")[1]?.split("# 9. AI.zymes import")[0] ?? "";
  assert.match(block, /present_no_probe/u);
  // mpnn_ok=1 may appear ONLY in the functional-import probe branch.
  const funcBlock = block.split("if (cd \"$cand\" && \"$PY\" -c")[1] ?? "";
  const noProbeBlock = block.split("present_no_probe")[1] ?? "";
  assert.match(funcBlock, /mpnn_ok=1/u, "functional probe still sets mpnn_ok");
  assert.doesNotMatch(noProbeBlock, /mpnn_ok=1/u, "present_no_probe must not set mpnn_ok (would pass a dir without a backend)");
});

test("T5 (t7#10): AmberTools records all four paths + coherent-prefix check fails mixed env", async () => {
  const payload = await readFile(PAYLOAD_PATH, "utf8");
  assert.match(payload, /amber_paths\.txt/u, "records all four executable paths");
  assert.match(payload, /INCOHERENT_MIXED_PREFIXES/u, "mixed-prefix env must be flagged");
  assert.match(payload, /coherent=0/u, "coherence violation tracked");
  const pointer = payload.split("INCOHERENT_MIXED_PREFIXES")[0] ?? "";
  assert.match(pointer, /missing=\$\(\(missing\+1\)\)/u, "incoherent prefix must increment missing (fail the gate)");
});

test("T6 (t7#11): Rosetta sha256 mandatory when selected (no || true)", async () => {
  const payload = await readFile(PAYLOAD_PATH, "utf8");
  const ros = payload.split("# 7. Rosetta")[1]?.split("# 8. ProteinMPNN")[0] ?? "";
  assert.match(ros, /sha256sum "\$rosetta_path" > "\$MANIFESTS\/rosetta_sha256\.txt"/u, "rosetta sha recorded");
  assert.doesNotMatch(ros, /sha256sum "\$rosetta_path"[^\n]*\|\|\s*true/u, "rosetta sha must not be silenced with || true when selected");
});

test("T7 (t7#12): ESMFold records exact snapshot dir/name/checksum/revision (run-scoped, no /tmp)", async () => {
  const payload = await readFile(PAYLOAD_PATH, "utf8");
  const block = payload.split("# 10. HF cache / ESMFold")[1] ?? "";
  assert.match(block, /esmfold_cache_dir=/u);
  assert.match(block, /esmfold_snapshot=/u);
  assert.match(block, /esmfold_snapshot_checksum=/u);
  assert.match(block, /esmfold_revision=/u);
  const executable = executablePayload(block);
  assert.doesNotMatch(executable, /(^|[=\s"'])\/tmp(?:\/|[\s"']|$)/mu, "snapshot checksum path must be run-scoped, not under /tmp");
});

test("T8 (t7#13): rm -rf is guarded by a RUN_DIR containment assertion", async () => {
  const payload = await readFile(PAYLOAD_PATH, "utf8");
  assert.match(payload, /FATAL: refusing rm -rf outside RUN_DIR/u);
  assert.doesNotMatch(executablePayload(payload), /rm -rf "\$CODE"/u, "never removes shared code");
});

test("T9 (t7#14): launch lock token (launch_lock) is bound to the CSPRNG run token", async () => {
  const h = harness({ remoteImpl: launchRemote() });
  const run = await launchRun(h);
  const init = h.calls.remote.find((c) => c.includes("lock="));
  assert.ok(init, "init lock command transmitted");
  const body = remoteBody(init);
  // The global .stage2.lock carries the run's CSPRNG token for identity-checked release.
  assert.match(body, new RegExp(`'${run.runToken}'`, "u"), "lock write must embed the run token");
  assert.ok(run.remoteLockDir && /\.stage2\.lock$/.test(run.remoteLockDir), "run records the lock dir");
});