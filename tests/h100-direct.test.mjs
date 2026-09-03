import assert from "node:assert/strict";
import { after, test } from "node:test";
import { createHash } from "node:crypto";
import { mkdtemp, mkdir, readFile, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, relative } from "node:path";
import { parseDirectManifest, RUNNER_SHA, RUNNER_BYTES, RUNNER_NAME } from "../lib/h100-direct.js";

const { createH100DirectTools } = await import("../lib/h100-direct.js");

const REMOTE_ROOT = "/home/work/w0test-remote";
const tmp = await mkdtemp(join(tmpdir(), "h100-direct-test-"));

// ── controllable fake remote ─────────────────────────────────────────────────
const remoteState = {
  files: new Map(),        // rel -> { sha, size, content }
  runDirExists: false,
  launchAmbiguity: false,
  outage: false,
  identityToken: null,
  exitCode: null,          // null => "pending"
  exitMarker: null,
  checksumOk: true,
  checksumMissing: false,
  passMarkerCount: 1,
  pidAlive: false,
  nprocValue: 48,          // fresh machine core count returned by `nproc`
  nprocFail: false,        // simulate an unreadable nproc (fail-closed test)
};
const remoteCommands = [];
const shellCommands = [];

function makePolicy({ cpuCap = 4 } = {}) {
  const limits = { gpus_allowed: [0], cpu_threads_per_job: 16, mem_gb_per_job: 32, concurrent_gpu_jobs: 1, concurrent_cpu_jobs: cpuCap };
  return { hash: "testpolicyhash", targets: { HPC: { test_gate: { real_submission: "gpu04" }, allowlist: { gpu04: { partition: "gpus" } } }, genbioh100: { ssh_target: "genbioh100", surface: "direct", login_shell: false, hardware: { reserved_gpu: 1, protected_process: "gpu_util", gpus: [0, 1] }, limits, environment: { tool_roots: ["/home/work/GenbioLAB/miniconda3"] } } } };
}

function makeState(envelope, policyOpts) {
  return { policy: makePolicy(policyOpts), envelope, runs: [], remoteGrants: [], lastError: null, h100DirectInFlight: undefined };
}

function makeExec() {
  return { agent: { id: "w0", session: { id: "w0-session", header: { cwd: "/tmp" } } }, signal: new AbortController().signal };
}

// Reset the mutable fake-remote flags to a clean "fresh, healthy" baseline so
// one test's scenario (outage/ambiguity/existing-dir) can never leak into the
// next. Tests that need a specific scenario set the flag AFTER this.
function resetRemote() {
  remoteState.runDirExists = false;
  remoteState.launchAmbiguity = false;
  remoteState.outage = false;
  remoteState.identityToken = null;
  remoteState.exitCode = null;
  remoteState.exitMarker = null;
  remoteState.checksumOk = true;
  remoteState.checksumMissing = false;
  remoteState.passMarkerCount = 1;
  remoteState.pidAlive = false;
  remoteState.nprocValue = 48;
  remoteState.nprocFail = false;
}

function fakeRunRemote(target, command) {
  const cmd = String(command);
  remoteCommands.push({ target, command: cmd });
  if (remoteState.outage) throw new Error("simulated remote outage");
  if (target !== "genbioh100") return { stdout: "", stderr: "wrong target", exitCode: 1 };
  // fresh machine nproc (aggregate total-CPU gate)
  if (cmd.includes("nproc")) {
    if (remoteState.nprocFail) return { stdout: "", stderr: "nproc: command not found", exitCode: 127 };
    return { stdout: `${remoteState.nprocValue}\n`, stderr: "", exitCode: 0 };
  }
  if (cmd.includes("RUN_DIR_READY") && cmd.includes("mkdir")) {
    if (remoteState.runDirExists) return { stdout: "RUN_DIR_EXISTS=1\n", stderr: "", exitCode: 0 };
    return { stdout: "RUN_DIR_READY=1\n", stderr: "", exitCode: 0 };
  }
  // stage pre-check init: `set -eu; root=...; mkdir -p ...`
  if (cmd.includes("mkdir -p") && !cmd.includes("RUN_DIR_READY")) return { stdout: "", stderr: "", exitCode: 0 };
  // fetch discovery: `for f in 'a' 'b'; do ... printf 'OK|%s|...' ...` (contains sha256sum, so match it FIRST)
  if (cmd.includes("OK|%s") && cmd.includes("for f in")) {
    const forMatch = cmd.match(/for f in (.+); do/u);
    const requested = forMatch ? forMatch[1].match(/'([^']+)'/gu).map((s) => s.slice(1, -1)) : [];
    const lines = requested.map((rel) => (remoteState.files.has(rel) ? `OK|${rel}|${remoteState.files.get(rel).size}|${remoteState.files.get(rel).sha}` : `MISSING|${rel}`));
    return { stdout: lines.join("\n") + "\n", stderr: "", exitCode: 0 };
  }
  if (cmd.includes("== IDENTITY ==")) {
    const ident = remoteState.identityToken ? `pid=4242\ntoken=${remoteState.identityToken}\n` : "missing\n";
    const exitSection = remoteState.exitMarker ? remoteState.exitMarker : (remoteState.exitCode === null ? "pending" : String(remoteState.exitCode));
    const pidSection = remoteState.pidAlive ? "alive" : "dead";
    const checksumSection = remoteState.checksumMissing ? "CHECKSUM_MISSING" : (remoteState.checksumOk ? "CHECKSUM_OK" : "CHECKSUM_FAIL");
    return { stdout: `== IDENTITY ==\n${ident}== EXIT_CODE ==\n${exitSection}\n== PID_ALIVE ==\n${pidSection}\n== CHECKSUM ==\n${checksumSection}\n== PASS_MARKER ==\n${remoteState.passMarkerCount}\n== STDOUT_TAIL ==\nstdout-line\n== STDERR_TAIL ==\nstderr-line\n`, stderr: "", exitCode: 0 };
  }
  if (cmd.includes("sha256sum") && !cmd.includes("setsid")) {
    const lines = [...remoteState.files.entries()].map(([rel, f]) => `${f.sha}  ${rel}`);
    lines.push(`${RUNNER_SHA}  ${RUNNER_NAME}`);
    return { stdout: lines.join("\n") + "\n", stderr: "", exitCode: 0 };
  }
  if (cmd.includes("--noprofile --norc -n")) return { stdout: "", stderr: "", exitCode: 0 };
  if (cmd.includes("setsid env -i") && cmd.includes("LAUNCH_RC")) {
    if (remoteState.launchAmbiguity) return { stdout: "LAUNCH_RC=0\nLAUNCH_AMBIGUITY=1\n", stderr: "", exitCode: 0 };
    const tokenMatch = cmd.match(/H100_RUN_TOKEN='([a-f0-9]{32})'/u);
    const token = tokenMatch ? tokenMatch[1] : "deadbeef".repeat(4);
    return { stdout: `LAUNCH_RC=0\npid=4242\ntoken=${token}\n`, stderr: "", exitCode: 0 };
  }
  if (cmd.includes("== IDENTITY ==")) {
    const ident = remoteState.identityToken ? `pid=4242\ntoken=${remoteState.identityToken}\n` : "missing\n";
    const exitSection = remoteState.exitMarker ? remoteState.exitMarker : (remoteState.exitCode === null ? "pending" : String(remoteState.exitCode));
    const pidSection = remoteState.pidAlive ? "alive" : "dead";
    return { stdout: `== IDENTITY ==\n${ident}== EXIT_CODE ==\n${exitSection}\n== PID_ALIVE ==\n${pidSection}\n== STDOUT_TAIL ==\nstdout-line\n== STDERR_TAIL ==\nstderr-line\n`, stderr: "", exitCode: 0 };
  }
  return { stdout: "", stderr: `unhandled remote command: ${cmd.slice(0, 120)}`, exitCode: 1 };
}

function makeHarness({ projectsDir, state, approve = true, latestRef }) {
  return createH100DirectTools({
    makeTool: (name, description, parameters, execute) => ({ name, description, parameters, execute }),
    requirePolicy: () => state.policy,
    requireState: () => state,
    publicState: (s) => JSON.parse(JSON.stringify({ policy: { valid: true, hash: s.policy.hash }, envelope: s.envelope, runs: s.runs, lastError: null, remoteGrants: s.remoteGrants })),
    runRemote: async (target, command) => fakeRunRemote(target, command),
    shell: {
      resolve: (request) => request,
      run: async (request) => {
        const cmd = String(request.command);
        shellCommands.push(cmd);
        if (cmd.startsWith("rclone copyto ")) {
          const m = cmd.match(/rclone copyto .*? '([^']+)' '([^']+)'\s*$/u);
          const src = m?.[1]; const dst = m?.[2];
          if (dst && !dst.includes(":") && dst.endsWith(".part") && src && src.includes(REMOTE_ROOT)) {
            // Fetch sources may be direct-run artifacts under REMOTE_ROOT/runs/<token>/.
            // The fake file catalogue is deliberately relative to that artifact root.
            const rel = src.split("/").at(-1);
            const f = remoteState.files.get(rel);
            if (f) { const fs = await import("node:fs/promises"); await fs.mkdir(dirname(dst), { recursive: true }); await fs.writeFile(dst, f.content, "utf8"); }
          }
        }
        return { stdout: { text: "" }, stderr: { text: "" }, exitCode: 0, signal: null, timedOut: false };
      },
    },
    userQuestions: {
      ask: async ({ questions }) => ({ answers: questions.map((q) => ({ id: q.id, selected: approve ? ["Approve this transfer", "Approve this retrieval"] : ["Reject"] })) }),
    },
    jobs: {
      start(spec) { const hooks = spec.run(); latestRef.run = { spec, hooks }; return `job-${spec.label}`; },
    },
    config: { h100DirectProjectsDir: projectsDir, logMaxBytes: 65536 },
    requireRemoteAccess: async () => {},
  });
}

const fileSha = async (p) => createHash("sha256").update(await readFile(p)).digest("hex");

// ── fixtures ──────────────────────────────────────────────────────────────────
const localRoot = join(tmp, "local");
const projectsDir = join(tmp, "projects");
await mkdir(localRoot, { recursive: true });
await mkdir(projectsDir, { recursive: true });
const entryPath = join(localRoot, "entry.sh");
await writeFile(entryPath, "#!/bin/bash\nset -euo pipefail\necho \"hello from entry\"\nexit 0\n");
const dataPath = join(localRoot, "data.txt");
await writeFile(dataPath, "payload\n");
const entrySha = await fileSha(entryPath);
const dataSha = await fileSha(dataPath);
remoteState.files.set("entry.sh", { sha: entrySha, size: (await readFile(entryPath)).length, content: await readFile(entryPath, "utf8") });
remoteState.files.set("data.txt", { sha: dataSha, size: (await readFile(dataPath)).length, content: await readFile(dataPath, "utf8") });

const { dump: dumpYaml } = await import("js-yaml");
function writeManifest(name, doc) {
  return writeFile(join(projectsDir, `${name}.yaml`), dumpYaml(doc));
}

function addCompletedDirectRun(state, project, token = "verified-run") {
  state.runs.push({
    target: "genbioh100",
    operation: `h100-direct-${project}-analyze`,
    status: "completed",
    finishedAt: 123456789,
    remoteRunDir: `${REMOTE_ROOT}/runs/${token}`,
  });
}

// ── RUNNER invariant ──────────────────────────────────────────────────────────
  assert.match(RUNNER_SHA, /^[a-f0-9]{64}$/u, "runner SHA must be a 64-hex digest");
  assert.ok(RUNNER_BYTES.includes("H100_RUN_TOKEN"), "runner must read the injected token");
  assert.ok(RUNNER_BYTES.includes("run_identity"), "runner must write run_identity");
  assert.ok(RUNNER_BYTES.includes("exit_code"), "runner must write exit_code");

  // ── manifest parsing ────────────────────────────────────────────────────────
  test("valid CPU-only direct manifest parses", () => {
    const m = parseDirectManifest("w0test", {
      schema_version: 1, target: "genbioh100", project: "w0test",
      local_root: "/abs/local", remote_root: "/abs/remote",
      files: ["entry.sh", "data.txt"],
      jobs: { analyze: { script: "entry.sh", cpus: 8, gpus: 0, mem_gb: 16 } },
    });
    assert.equal(m.jobs.analyze.cpus, 8);
    assert.equal(m.jobs.analyze.gpus, 0);
    assert.equal(m.jobs.analyze.memGb, 16);
  });

  test("GPU manifest (gpus:1) is rejected — the direct layer is CPU-only", () => {
    assert.throws(() => parseDirectManifest("w0test", {
      schema_version: 1, target: "genbioh100", project: "w0test", local_root: "/a", remote_root: "/b",
      files: ["e.sh"], jobs: { g: { script: "e.sh", cpus: 16, gpus: 1, mem_gb: 32 } },
    }), /must be CPU-only/u);
  });

  test("non-genbioh100 target is rejected", () => {
    assert.throws(() => parseDirectManifest("w0test", { schema_version: 1, target: "HPC", project: "w0test", local_root: "/a", remote_root: "/b", files: ["e.sh"], jobs: { g: { script: "e.sh", cpus: 1, mem_gb: 1 } } }), /target must be genbioh100/u);
  });

  test("entry script not listed in files is rejected", () => {
    assert.throws(() => parseDirectManifest("w0test", { schema_version: 1, target: "genbioh100", project: "w0test", local_root: "/a", remote_root: "/b", files: ["other.sh"], jobs: { g: { script: "e.sh", cpus: 1, mem_gb: 1 } } }), /must be listed in files/u);
  });

  test("cpus out of 1..16 is rejected", () => {
    assert.throws(() => parseDirectManifest("w0test", { schema_version: 1, target: "genbioh100", project: "w0test", local_root: "/a", remote_root: "/b", files: ["e.sh"], jobs: { g: { script: "e.sh", cpus: 17, mem_gb: 1 } } }), /cpus must be an integer 1\.\.16/u);
  });

  test("any non-zero gpus is rejected (CPU-only only)", () => {
    assert.throws(() => parseDirectManifest("w0test", { schema_version: 1, target: "genbioh100", project: "w0test", local_root: "/a", remote_root: "/b", files: ["e.sh"], jobs: { g: { script: "e.sh", cpus: 1, gpus: 1, mem_gb: 1 } } }), /must be CPU-only/u);
    assert.throws(() => parseDirectManifest("w0test", { schema_version: 1, target: "genbioh100", project: "w0test", local_root: "/a", remote_root: "/b", files: ["e.sh"], jobs: { g: { script: "e.sh", cpus: 1, gpus: 2, mem_gb: 1 } } }), /must be CPU-only/u);
  });

  test("missing mem_gb is rejected (envelope must be exactly sufficient)", () => {
    assert.throws(() => parseDirectManifest("w0test", { schema_version: 1, target: "genbioh100", project: "w0test", local_root: "/a", remote_root: "/b", files: ["e.sh"], jobs: { g: { script: "e.sh", cpus: 1 } } }), /mem_gb must be an integer/u);
  });

  test("unsafe argv literal is rejected", () => {
    assert.throws(() => parseDirectManifest("w0test", { schema_version: 1, target: "genbioh100", project: "w0test", local_root: "/a", remote_root: "/b", files: ["e.sh"], jobs: { g: { script: "e.sh", cpus: 1, mem_gb: 1, argv: ["x;rm -rf /"] } } }), /argv literal is unsafe/u);
  });

  test("safe argv literal is accepted and frozen", () => {
    const m = parseDirectManifest("w0test", { schema_version: 1, target: "genbioh100", project: "w0test", local_root: "/a", remote_root: "/b", files: ["e.sh"], jobs: { g: { script: "e.sh", cpus: 1, mem_gb: 1, argv: ["--mode", "fast", "in/a.dat"] } } });
    assert.deepEqual([...m.jobs.g.argv], ["--mode", "fast", "in/a.dat"]);
    assert.ok(Object.isFrozen(m.jobs.g.argv), "argv must be frozen");
    assert.ok(Object.isFrozen(m.jobs.g), "job spec must be frozen");
  });

  // ── provenance pinning (version / radii) ────────────────────────────────────
  test("provenance pins version and radii (parse)", () => {
    const m = parseDirectManifest("w0prov", { schema_version: 1, target: "genbioh100", project: "w0prov", local_root: "/a", remote_root: "/b", files: ["e.sh"], jobs: { g: { script: "e.sh", cpus: 1, mem_gb: 1 } }, provenance: { version: "pytim 0.9.1", radii: "cg20_radii_v3", radii_sha256: "abc123" } });
    assert.deepEqual(m.provenance, { version: "pytim 0.9.1", radii: "cg20_radii_v3", radii_sha256: "abc123" });
    assert.ok(Object.isFrozen(m.provenance), "provenance must be frozen");
  });

  test("provenance is optional (absent -> null)", () => {
    const m = parseDirectManifest("w0prov", { schema_version: 1, target: "genbioh100", project: "w0prov", local_root: "/a", remote_root: "/b", files: ["e.sh"], jobs: { g: { script: "e.sh", cpus: 1, mem_gb: 1 } } });
    assert.equal(m.provenance, null);
  });

  test("unsafe provenance key or value is rejected (fail closed)", () => {
    const base = { schema_version: 1, target: "genbioh100", project: "w0prov", local_root: "/a", remote_root: "/b", files: ["e.sh"], jobs: { g: { script: "e.sh", cpus: 1, mem_gb: 1 } } };
    assert.throws(() => parseDirectManifest("w0prov", { ...base, provenance: { "bad key;rm": "x" } }), /provenance key is unsafe/u);
    assert.throws(() => parseDirectManifest("w0prov", { ...base, provenance: { version: "pytim\n0.9.1" } }), /provenance value/u);
    assert.throws(() => parseDirectManifest("w0prov", { ...base, provenance: { version: "" } }), /provenance value/u);
    assert.throws(() => parseDirectManifest("w0prov", { ...base, provenance: [] }), /provenance must be a mapping/u);
  });

  // ── stage ───────────────────────────────────────────────────────────────────
  test("stage transfers via rclone (never scp) and verifies SHA-256", async () => {
    const manifestDoc = { schema_version: 1, target: "genbioh100", project: "w0stage", local_root: localRoot, remote_root: REMOTE_ROOT, files: ["entry.sh", "data.txt"], jobs: { analyze: { script: "entry.sh", cpus: 4, gpus: 0, mem_gb: 8 } } };
    await writeManifest("w0stage", manifestDoc);
    const state = makeState(null);
    const latestRef = {};
    const tools = makeHarness({ projectsDir, state, latestRef });
    const result = await tools.stageTool.execute({ project: "w0stage" }, makeExec());
    assert.equal(result.ok, true);
    await latestRef.run.hooks.done;
    const run = state.runs.at(-1);
    assert.equal(run.status, "completed", `stage should complete, got ${run.status}: ${run.error}`);
    // no-scp invariant
    for (const c of shellCommands) { assert.match(c, /^rclone copyto /u); assert.ok(!/\bscp\b/u.test(c), "scp must never appear"); }
  });

  test("stage without transfer approval fails closed (no transfer)", async () => {
    const state = makeState(null);
    const latestRef = {};
    const before = shellCommands.length;
    const tools = makeHarness({ projectsDir, state, approve: false, latestRef });
    await assert.rejects(tools.stageTool.execute({ project: "w0stage" }, makeExec()), /not approved/u);
    assert.equal(shellCommands.length, before, "no transfer command may run after a rejected approval");
  });

  // ── job: envelope + class-cap + aggregate + exact-once ─────────────────────
  const cpuEnvelope = { target: "genbioh100", node: "genbioh100", partition: null, workloadClass: "cpu-only-analysis", maxCpus: 16, maxGpus: 0, memGb: 32, concurrency: 4, usedCpus: 0, usedGpus: 0, policyHash: "testpolicyhash" };

  test("job without an envelope is rejected", async () => {
    const manifestDoc = { schema_version: 1, target: "genbioh100", project: "w0job", local_root: localRoot, remote_root: REMOTE_ROOT, files: ["entry.sh", "data.txt"], jobs: { analyze: { script: "entry.sh", cpus: 4, gpus: 0, mem_gb: 8 } } };
    await writeManifest("w0job", manifestDoc);
    const state = makeState(null);
    const latestRef = {};
    const tools = makeHarness({ projectsDir, state, latestRef });
    await assert.rejects(tools.jobTool.execute({ project: "w0job", operation: "analyze" }, makeExec()), /envelope/iu);
  });

  test("job with a non-genbioh100 envelope is rejected", async () => {
    const state = makeState({ target: "HPC", node: "gpu04", partition: "gpus", workloadClass: "gpu", maxCpus: 24, maxGpus: 1, memGb: null, concurrency: 1, policyHash: "testpolicyhash" });
    const latestRef = {};
    const tools = makeHarness({ projectsDir, state, latestRef });
    await assert.rejects(tools.jobTool.execute({ project: "w0job", operation: "analyze" }, makeExec()), /genbioh100 session envelope/u);
  });

  test("CPU-only envelope concurrency above the class cap is rejected", async () => {
    const state = makeState({ ...cpuEnvelope, concurrency: 99 });
    const latestRef = {};
    const tools = makeHarness({ projectsDir, state, latestRef });
    await assert.rejects(tools.jobTool.execute({ project: "w0job", operation: "analyze" }, makeExec()), /class cap|exceeds the .*class/u);
  });

  test("a gpus:1 job manifest is rejected at load (CPU-only only)", async () => {
    const manifestDoc = { schema_version: 1, target: "genbioh100", project: "w0gpujob", local_root: localRoot, remote_root: REMOTE_ROOT, files: ["entry.sh", "data.txt"], jobs: { run: { script: "entry.sh", cpus: 16, gpus: 1, mem_gb: 32 } } };
    await writeManifest("w0gpujob", manifestDoc);
    const state = makeState(cpuEnvelope);
    const latestRef = {};
    const tools = makeHarness({ projectsDir, state, latestRef });
    await assert.rejects(tools.jobTool.execute({ project: "w0gpujob", operation: "run" }, makeExec()), /must be CPU-only/u);
  });

  test("aggregate CPU-only capacity is enforced at the class cap", async () => {
    const state = makeState(cpuEnvelope); // concurrency 4, cpuCap 4
    const latestRef = {};
    const tools = makeHarness({ projectsDir, state, latestRef });
    // seed 4 active CPU-only direct runs (same class)
    for (let i = 0; i < 4; i++) state.runs.push({ target: "genbioh100", operation: `h100-direct-w0job-analyze`, status: "running", resources: { cpus: 4, gpus: 0, memGb: 8, concurrency: 1 } });
    await assert.rejects(tools.jobTool.execute({ project: "w0job", operation: "analyze" }, makeExec()), /aggregate capacity/u);
  });

  // ── CPU-only AGGREGATE total-CPU gate (independent of the concurrency cap) ──
  // w0job "analyze" is a 4-cpu job. These prove that on a 48-core machine the
  // TOTAL active CPU threads bound admission, so a raised concurrency cap (10)
  // can never oversubscribe the cores — concurrency alone is NOT sufficient.
  const cpuEnvelope10 = { target: "genbioh100", node: "genbioh100", partition: null, workloadClass: "cpu-only-analysis", maxCpus: 16, maxGpus: 0, memGb: 32, concurrency: 10, usedCpus: 0, usedGpus: 0, policyHash: "testpolicyhash" };

  test("a fully-subscribed 48-core machine blocks any further CPU-only job even at concurrency 10", async () => {
    const state = makeState(cpuEnvelope10, { cpuCap: 10 });
    const latestRef = {};
    const tools = makeHarness({ projectsDir, state, latestRef });
    // 3 active 16-cpu runs = 48 (machine fully subscribed)
    for (let i = 0; i < 3; i++) state.runs.push({ target: "genbioh100", operation: "h100-direct-w0job-analyze", status: "running", resources: { cpus: 16, gpus: 0, memGb: 32, concurrency: 1 } });
    // concurrency 10 would admit a 4th run, but 48 + 4 = 52 > fresh nproc 48 must reject
    await assert.rejects(tools.jobTool.execute({ project: "w0job", operation: "analyze" }, makeExec()), /exceed the fresh machine nproc 48/u);
  });

  test("a job that fits the remaining 48-core budget is admitted", async () => {
    const state = makeState(cpuEnvelope10, { cpuCap: 10 });
    const latestRef = {};
    const tools = makeHarness({ projectsDir, state, latestRef });
    // 2 active 16-cpu runs = 32; the 4-cpu job fits (32 + 4 = 36 <= 48)
    for (let i = 0; i < 2; i++) state.runs.push({ target: "genbioh100", operation: "h100-direct-w0job-analyze", status: "running", resources: { cpus: 16, gpus: 0, memGb: 32, concurrency: 1 } });
    resetRemote();
    const result = await tools.jobTool.execute({ project: "w0job", operation: "analyze" }, makeExec());
    assert.equal(result.ok, true, "a job within the remaining core budget must be admitted");
    await latestRef.run.hooks.done;
    const run = state.runs.at(-1);
    assert.equal(run.status, "running", `expected running, got ${run.status}: ${run.error}`);
  });

  test("mixed job sizes pack to the 48-core budget and reject past it", async () => {
    const state = makeState(cpuEnvelope10, { cpuCap: 10 });
    const latestRef = {};
    const tools = makeHarness({ projectsDir, state, latestRef });
    // active 16 + 16 + 14 = 46; the 4-cpu job would be 50 > fresh nproc 48
    state.runs.push({ target: "genbioh100", operation: "h100-direct-a", status: "running", resources: { cpus: 16, gpus: 0, memGb: 32, concurrency: 1 } });
    state.runs.push({ target: "genbioh100", operation: "h100-direct-b", status: "running", resources: { cpus: 16, gpus: 0, memGb: 32, concurrency: 1 } });
    state.runs.push({ target: "genbioh100", operation: "h100-direct-c", status: "running", resources: { cpus: 14, gpus: 0, memGb: 32, concurrency: 1 } });
    await assert.rejects(tools.jobTool.execute({ project: "w0job", operation: "analyze" }, makeExec()), /exceed the fresh machine nproc 48/u);
  });

  test("an unreadable fresh nproc fails closed (no admission)", async () => {
    // If the machine's nproc cannot be read, the aggregate total-CPU gate cannot
    // be verified -> admission is rejected (fail closed), never guessed.
    remoteState.nprocFail = true;
    const state = makeState(cpuEnvelope10, { cpuCap: 10 });
    const latestRef = {};
    const tools = makeHarness({ projectsDir, state, latestRef });
    await assert.rejects(tools.jobTool.execute({ project: "w0job", operation: "analyze" }, makeExec()), /could not read a valid fresh nproc/u);
    remoteState.nprocFail = false;
  });

  test("job happy path: exact-once detached launch, run is running with pid", async () => {
    const state = makeState(cpuEnvelope);
    const latestRef = {};
    const tools = makeHarness({ projectsDir, state, latestRef });
    resetRemote();
    const result = await tools.jobTool.execute({ project: "w0job", operation: "analyze" }, makeExec());
    assert.equal(result.ok, true);
    await latestRef.run.hooks.done;
    const run = state.runs.at(-1);
    assert.equal(run.status, "running", `expected running, got ${run.status}: ${run.error}`);
    assert.equal(run.pid, 4242, "launch must record the detached PID");
    assert.match(run.runToken, /^[a-f0-9]{32}$/u, "run identity must be a 128-bit hex token");
    assert.ok(run.remoteRunDir.includes("/runs/analyze-"), "run must use a fresh per-run directory");
    // the launch command must pin OMP_NUM_THREADS and (for GPU) CUDA_VISIBLE_DEVICES=0
    const launch = remoteCommands.at(-1).command;
    assert.match(launch, /OMP_NUM_THREADS='4'/u);
    assert.ok(!launch.includes("CUDA_VISIBLE_DEVICES=1"), "must never target GPU 1");
    assert.ok(!launch.includes("CUDA_VISIBLE_DEVICES"), "a CPU-only (gpus:0) job must set no CUDA_VISIBLE_DEVICES at all");
  });

  test("job run record pins the manifest version/radii provenance", async () => {
    const manifestDoc = { schema_version: 1, target: "genbioh100", project: "w0provjob", local_root: localRoot, remote_root: REMOTE_ROOT, files: ["entry.sh", "data.txt"], jobs: { analyze: { script: "entry.sh", cpus: 4, gpus: 0, mem_gb: 8 } }, provenance: { version: "pytim 0.9.1", radii: "cg20_radii_v3", radii_sha256: "abc123" } };
    await writeManifest("w0provjob", manifestDoc);
    const state = makeState(cpuEnvelope);
    const latestRef = {};
    const tools = makeHarness({ projectsDir, state, latestRef });
    resetRemote();
    await tools.jobTool.execute({ project: "w0provjob", operation: "analyze" }, makeExec());
    await latestRef.run.hooks.done;
    const run = state.runs.at(-1);
    assert.equal(run.status, "running", `expected running, got ${run.status}: ${run.error}`);
    assert.deepEqual(run.provenance, { version: "pytim 0.9.1", radii: "cg20_radii_v3", radii_sha256: "abc123" }, "run record must pin the package version/radii provenance");
  });

  test("launch ambiguity (run_identity missing) -> reconciling, never failed", async () => {
    const state = makeState(cpuEnvelope);
    const latestRef = {};
    const tools = makeHarness({ projectsDir, state, latestRef });
    remoteState.launchAmbiguity = true;
    await tools.jobTool.execute({ project: "w0job", operation: "analyze" }, makeExec());
    await latestRef.run.hooks.done;
    const run = state.runs.at(-1);
    assert.equal(run.status, "reconciling", "ambiguity must be reconciling, never failed");
    // pair-lock must still be held (not released on ambiguity)
    assert.ok(state.h100DirectInFlight.has("w0job:analyze"), "pair-lock held while ambiguous");
    remoteState.launchAmbiguity = false;
  });

  test("run-dir token collision (exists) -> failed, no launch", async () => {
    const state = makeState(cpuEnvelope);
    const latestRef = {};
    const tools = makeHarness({ projectsDir, state, latestRef });
    remoteState.runDirExists = true;
    await tools.jobTool.execute({ project: "w0job", operation: "analyze" }, makeExec());
    await latestRef.run.hooks.done;
    const run = state.runs.at(-1);
    assert.equal(run.status, "failed", "a pre-existing run dir must fail closed");
    assert.ok(!state.h100DirectInFlight.has("w0job:analyze"), "pair-lock released on definitive no-launch");
    remoteState.runDirExists = false;
  });

  test("material transfer rejection -> no launch, pair-lock released", async () => {
    const state = makeState(cpuEnvelope);
    const latestRef = {};
    const tools = makeHarness({ projectsDir, state, approve: false, latestRef });
    await assert.rejects(tools.jobTool.execute({ project: "w0job", operation: "analyze" }, makeExec()), /not approved/u);
    assert.ok(!state.h100DirectInFlight?.has("w0job:analyze"), "pair-lock released when transfer is rejected");
    assert.equal(state.runs.length, 0, "no run record on rejected transfer");
  });

  // ── status reconciliation ───────────────────────────────────────────────────
  async function launchForStatus(state, tools, latestRef) {
    resetRemote();
    await tools.jobTool.execute({ project: "w0job", operation: "analyze" }, makeExec());
    await latestRef.run.hooks.done;
    return state.runs.at(-1);
  }

  test("status: token + exit 0 + verified checksum + one marker -> completed, pair-lock released", async () => {
    const state = makeState(cpuEnvelope);
    const latestRef = {};
    const tools = makeHarness({ projectsDir, state, latestRef });
    const run = await launchForStatus(state, tools, latestRef);
    remoteState.identityToken = run.runToken; remoteState.exitCode = 0; remoteState.pidAlive = false;
    const res = await tools.statusTool.execute({ run_id: run.runId }, makeExec());
    assert.equal(res.status.h100DirectStatus.reconciled, "completed");
    assert.equal(run.status, "completed");
    assert.ok(!state.h100DirectInFlight.has("w0job:analyze"), "pair-lock released on terminal");
  });

  test("status: exit 0 with invalid checksum evidence -> failed", async () => {
    const state = makeState(cpuEnvelope);
    const latestRef = {};
    const tools = makeHarness({ projectsDir, state, latestRef });
    const run = await launchForStatus(state, tools, latestRef);
    remoteState.identityToken = run.runToken; remoteState.exitCode = 0; remoteState.checksumOk = false; remoteState.pidAlive = false;
    const res = await tools.statusTool.execute({ run_id: run.runId }, makeExec());
    assert.equal(res.status.h100DirectStatus.reconciled, "failed");
    assert.equal(run.status, "failed");
  });

  test("status: exit 0 with zero or duplicate PYTIM_PASS -> failed", async () => {
    const state = makeState(cpuEnvelope);
    const latestRef = {};
    const tools = makeHarness({ projectsDir, state, latestRef });
    const run = await launchForStatus(state, tools, latestRef);
    remoteState.identityToken = run.runToken; remoteState.exitCode = 0; remoteState.passMarkerCount = 2; remoteState.pidAlive = false;
    const res = await tools.statusTool.execute({ run_id: run.runId }, makeExec());
    assert.equal(res.status.h100DirectStatus.reconciled, "failed");
    assert.equal(run.status, "failed");
  });

  test("status: token match + non-zero exit -> failed", async () => {
    const state = makeState(cpuEnvelope);
    const latestRef = {};
    const tools = makeHarness({ projectsDir, state, latestRef });
    const run = await launchForStatus(state, tools, latestRef);
    remoteState.identityToken = run.runToken; remoteState.exitCode = 3; remoteState.pidAlive = false;
    const res = await tools.statusTool.execute({ run_id: run.runId }, makeExec());
    assert.equal(res.status.h100DirectStatus.reconciled, "failed");
    assert.equal(run.status, "failed");
  });

  test("status: token mismatch -> reconciling (evidence not trusted)", async () => {
    const state = makeState(cpuEnvelope);
    const latestRef = {};
    const tools = makeHarness({ projectsDir, state, latestRef });
    const run = await launchForStatus(state, tools, latestRef);
    remoteState.identityToken = "f".repeat(32); remoteState.exitCode = 0; remoteState.pidAlive = false;
    const res = await tools.statusTool.execute({ run_id: run.runId }, makeExec());
    assert.equal(res.status.h100DirectStatus.reconciled, "reconciling");
    assert.equal(run.status, "running", "status must not claim terminal on a token mismatch");
  });

  test("status: exit pending + live pid -> running (in progress, not terminal)", async () => {
    const state = makeState(cpuEnvelope);
    const latestRef = {};
    const tools = makeHarness({ projectsDir, state, latestRef });
    const run = await launchForStatus(state, tools, latestRef);
    remoteState.identityToken = run.runToken; remoteState.exitCode = null; remoteState.pidAlive = true;
    const res = await tools.statusTool.execute({ run_id: run.runId }, makeExec());
    assert.equal(res.status.h100DirectStatus.reconciled, "running");
    assert.equal(run.status, "running", "a live process with a pending exit code is still running");
  });

  test("status: remote outage -> reconciling (never failed)", async () => {
    const state = makeState(cpuEnvelope);
    const latestRef = {};
    const tools = makeHarness({ projectsDir, state, latestRef });
    const run = await launchForStatus(state, tools, latestRef);
    remoteState.outage = true;
    const res = await tools.statusTool.execute({ run_id: run.runId }, makeExec());
    assert.equal(res.status.h100DirectStatus.reconciled, "reconciling");
    assert.equal(run.status, "running", "an outage must not mark the run failed");
    remoteState.outage = false;
  });

  test("status requires run_id", async () => {
    const state = makeState(cpuEnvelope);
    const latestRef = {};
    const tools = makeHarness({ projectsDir, state, latestRef });
    await assert.rejects(tools.statusTool.execute({}, makeExec()), /run_id/u);
  });

  // ── fetch ───────────────────────────────────────────────────────────────────
  test("fetch: bounded rclone retrieval with SHA-256 match + receipt", async () => {
    const manifestDoc = { schema_version: 1, target: "genbioh100", project: "w0fetch", local_root: localRoot, remote_root: REMOTE_ROOT, files: ["entry.sh", "data.txt"], jobs: { analyze: { script: "entry.sh", cpus: 4, gpus: 0, mem_gb: 8 } }, fetch: { max_bytes: 1000000, dest: "results", files: ["data.txt"] } };
    await writeManifest("w0fetch", manifestDoc);
    const state = makeState(cpuEnvelope);
    const latestRef = {};
    const tools = makeHarness({ projectsDir, state, latestRef });
    resetRemote();
    addCompletedDirectRun(state, "w0fetch");
    const res = await tools.fetchTool.execute({ project: "w0fetch" }, makeExec());
    assert.equal(res.ok, true);
    assert.equal(res.status.h100DirectFetch.bytes, (await readFile(dataPath)).length);
    const destDir = join(localRoot, "results");
    const fetched = await readFile(join(destDir, "data.txt"), "utf8");
    assert.equal(fetched, "payload\n", "fetched bytes must match the remote content");
    const receipts = await import("node:fs/promises").then((m) => m.readdir(destDir));
    const receiptName = receipts.find((n) => n.startsWith("FETCH_RECEIPT_"));
    assert.ok(receiptName, "a dated FETCH_RECEIPT must be written");
    const receipt = JSON.parse(await readFile(join(destDir, receiptName), "utf8"));
    assert.equal(receipt.source_root, `${REMOTE_ROOT}/runs/verified-run`, "receipt must pin the completed run directory");
    assert.ok(shellCommands.some((cmd) => cmd.includes(`${REMOTE_ROOT}/runs/verified-run/data.txt`)), "rclone must retrieve from the completed run directory");
  });

  test("fetch: chooses the latest completed run directory, never manifest remote_root", async () => {
    const manifestDoc = { schema_version: 1, target: "genbioh100", project: "w0fetchlatest", local_root: localRoot, remote_root: REMOTE_ROOT, files: ["entry.sh", "data.txt"], jobs: { analyze: { script: "entry.sh", cpus: 4, gpus: 0, mem_gb: 8 } }, fetch: { max_bytes: 1000000, dest: "results-latest", files: ["data.txt"] } };
    await writeManifest("w0fetchlatest", manifestDoc);
    const state = makeState(cpuEnvelope);
    state.runs.push(
      { target: "genbioh100", operation: "h100-direct-w0fetchlatest-analyze", status: "completed", finishedAt: 1000, remoteRunDir: `${REMOTE_ROOT}/runs/old-run` },
      { target: "genbioh100", operation: "h100-direct-w0fetchlatest-analyze", status: "completed", finishedAt: 2000, remoteRunDir: `${REMOTE_ROOT}/runs/new-run` },
    );
    const latestRef = {};
    const tools = makeHarness({ projectsDir, state, latestRef });
    resetRemote();
    const commandStart = shellCommands.length;
    await tools.fetchTool.execute({ project: "w0fetchlatest" }, makeExec());
    const commands = shellCommands.slice(commandStart);
    assert.ok(commands.some((cmd) => cmd.includes(`${REMOTE_ROOT}/runs/new-run/data.txt`)), "rclone must select the latest completed run directory");
    assert.ok(!commands.some((cmd) => cmd.includes(`${REMOTE_ROOT}/data.txt`)), "rclone must never source artifacts from bare manifest remote_root");
    const receiptNames = await import("node:fs/promises").then((m) => m.readdir(join(localRoot, "results-latest")));
    const receipt = JSON.parse(await readFile(join(localRoot, "results-latest", receiptNames.find((name) => name.startsWith("FETCH_RECEIPT_"))), "utf8"));
    assert.equal(receipt.source_root, `${REMOTE_ROOT}/runs/new-run`);
  });

  test("fetch: explicit run_id recovers a completed terminal run after restart", async () => {
    const manifestDoc = { schema_version: 1, target: "genbioh100", project: "w0recover", local_root: localRoot, remote_root: REMOTE_ROOT, files: ["entry.sh", "data.txt"], jobs: { analyze: { script: "entry.sh", cpus: 4, gpus: 0, mem_gb: 8 } }, fetch: { max_bytes: 1000000, dest: "results-recover", files: ["data.txt"] } };
    await writeManifest("w0recover", manifestDoc);
    const token = "0123456789abcdef0123456789abcdef";
    const runId = `genbioh100-h100-direct-w0recover-analyze-${token}`;
    const state = makeState(cpuEnvelope);
    const latestRef = {};
    const tools = makeHarness({ projectsDir, state, latestRef });
    resetRemote();
    remoteState.identityToken = token;
    remoteState.exitCode = 0;
    const remoteStart = remoteCommands.length;
    const shellStart = shellCommands.length;
    const res = await tools.fetchTool.execute({ project: "w0recover", run_id: runId }, makeExec());
    assert.equal(res.ok, true);
    assert.equal(state.runs.length, 0, "recovery must not synthesize a session run");
    assert.ok(remoteCommands.slice(remoteStart).some(({ command }) => command.includes(`cd -- '${REMOTE_ROOT}/runs/analyze-${token}'`)), "terminal probe must address only the derived run directory");
    assert.ok(shellCommands.slice(shellStart).some((command) => command.includes(`${REMOTE_ROOT}/runs/analyze-${token}/data.txt`)), "rclone must read only the derived artifact path");
    const receiptNames = await import("node:fs/promises").then((m) => m.readdir(join(localRoot, "results-recover")));
    const receipt = JSON.parse(await readFile(join(localRoot, "results-recover", receiptNames.find((name) => name.startsWith("FETCH_RECEIPT_"))), "utf8"));
    assert.deepEqual(receipt.recovery, { run_id: runId, run_token: token, terminal_evidence_reverified: true });
  });

  test("fetch: recovery refuses mismatched identity or incomplete terminal evidence before discovery", async () => {
    const token = "0123456789abcdef0123456789abcdef";
    const runId = `genbioh100-h100-direct-w0recover-analyze-${token}`;
    for (const scenario of ["identity", "marker-zero", "marker-duplicate", "checksum-fail", "checksum-missing"]) {
      const state = makeState(cpuEnvelope);
      const latestRef = {};
      const tools = makeHarness({ projectsDir, state, latestRef });
      resetRemote();
      remoteState.identityToken = scenario === "identity" ? "fedcba9876543210fedcba9876543210" : token;
      remoteState.exitCode = 0;
      if (scenario === "marker-zero") remoteState.passMarkerCount = 0;
      if (scenario === "marker-duplicate") remoteState.passMarkerCount = 2;
      if (scenario === "checksum-fail") remoteState.checksumOk = false;
      if (scenario === "checksum-missing") remoteState.checksumMissing = true;
      const remoteStart = remoteCommands.length;
      const shellStart = shellCommands.length;
      await assert.rejects(tools.fetchTool.execute({ project: "w0recover", run_id: runId }, makeExec()), /identity token mismatch|incomplete successful terminal evidence/u, scenario);
      assert.equal(state.runs.length, 0, `${scenario}: no synthetic state`);
      assert.ok(!remoteCommands.slice(remoteStart).some(({ command }) => command.includes("for f in")), `${scenario}: no artifact discovery`);
      assert.equal(shellCommands.slice(shellStart).length, 0, `${scenario}: no local retrieval`);
    }
  });

  test("fetch: malformed recovery run_id is rejected before remote access", async () => {
    const state = makeState(cpuEnvelope);
    const latestRef = {};
    const tools = makeHarness({ projectsDir, state, latestRef });
    const badIds = ["", "genbioh100-h100-direct-w0recover-analyze-ABCDEF0123456789ABCDEF0123456789", "genbioh100-h100-direct-other-analyze-0123456789abcdef0123456789abcdef", "genbioh100-h100-direct-w0recover-unknown-0123456789abcdef0123456789abcdef", "genbioh100-h100-direct-w0recover-analyze-0123456789abcdef0123456789abcdef-extra"];
    for (const run_id of badIds) {
      const remoteStart = remoteCommands.length;
      await assert.rejects(tools.fetchTool.execute({ project: "w0recover", run_id }, makeExec()), /not an exact declared direct-run identity/u);
      assert.equal(remoteCommands.length, remoteStart, `bad run_id ${run_id}: no remote call`);
    }
  });

  test("fetch: refuses when only non-matching runs exist", async () => {
    const state = makeState(cpuEnvelope);
    state.runs.push(
      { target: "genbioh100", operation: "h100-direct-other-analyze", status: "completed", finishedAt: 900, remoteRunDir: `${REMOTE_ROOT}/runs/other-project` },
      { target: "genbioh100", operation: "h100-direct-w0fetch-analyze", status: "running", finishedAt: 800, remoteRunDir: `${REMOTE_ROOT}/runs/running` },
      { target: "genbioh100", operation: "h100-direct-w0fetch-analyze", status: "failed", finishedAt: 700, remoteRunDir: `${REMOTE_ROOT}/runs/failed` },
      { target: "genbioh100", operation: "h100-direct-w0fetch-analyze", status: "completed", finishedAt: 600, remoteRunDir: REMOTE_ROOT },
      { target: "genbioh100", operation: "h100-direct-w0fetch-stage", status: "completed", finishedAt: 500, remoteRunDir: `${REMOTE_ROOT}/runs/stage` },
      { target: "HPC", operation: "h100-direct-w0fetch-analyze", status: "completed", finishedAt: 400, remoteRunDir: `${REMOTE_ROOT}/runs/wrong-target` },
    );
    const latestRef = {};
    const tools = makeHarness({ projectsDir, state, latestRef });
    await assert.rejects(tools.fetchTool.execute({ project: "w0fetch" }, makeExec()), /no completed direct run/u);
  });

  test("fetch: file not in the allowlist is rejected", async () => {
    const state = makeState(cpuEnvelope);
    const latestRef = {};
    const tools = makeHarness({ projectsDir, state, latestRef });
    await assert.rejects(tools.fetchTool.execute({ project: "w0fetch", files: ["evil.txt"] }, makeExec()), /not in the manifest allowlist/u);
  });

  test("fetch: total above the byte cap is rejected", async () => {
    const manifestDoc = { schema_version: 1, target: "genbioh100", project: "w0fetchcap", local_root: localRoot, remote_root: REMOTE_ROOT, files: ["entry.sh", "data.txt"], jobs: { analyze: { script: "entry.sh", cpus: 4, gpus: 0, mem_gb: 8 } }, fetch: { max_bytes: 2, dest: "results", files: ["data.txt"] } };
    await writeManifest("w0fetchcap", manifestDoc);
    const state = makeState(cpuEnvelope);
    const latestRef = {};
    const tools = makeHarness({ projectsDir, state, latestRef });
    resetRemote();
    addCompletedDirectRun(state, "w0fetchcap");
    await assert.rejects(tools.fetchTool.execute({ project: "w0fetchcap" }, makeExec()), /exceeds the manifest cap/u);
  });

after(async () => { await rm(tmp, { recursive: true, force: true }); });
