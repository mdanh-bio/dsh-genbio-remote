import assert from "node:assert/strict";
import { after, test } from "node:test";
import { createHash } from "node:crypto";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createH100MirrorTools, loadMirrorManifest } from "../lib/h100-mirror.js";
import { dump } from "js-yaml";

const tmp = await mkdtemp(join(tmpdir(), "h100-mirror-test-"));
const SOURCE_ROOT = "/data01/src";
const DEST_ROOT = "/home/work/dest";

const keyContent = "key\n";
const otherContent = "other\n";
const keySha = createHash("sha256").update(keyContent).digest("hex");
const otherSha = createHash("sha256").update(otherContent).digest("hex");

// Inventory lines: FILE<TAB>size<TAB>mtime<TAB>sha<TAB>rel
const inventoryLines = [
  `FILE\t${keyContent.length}\t1700000000\t${keySha}\ta/key.dat`,
  `FILE\t${otherContent.length}\t1700000000\t${otherSha}\tb/other.dat`,
];

function makeManifestDoc() {
  return {
    schema_version: 1, project: "w0mirror", mode: "curated",
    source: { target: "HPC", root: SOURCE_ROOT },
    destination: { target: "genbioh100", root: DEST_ROOT },
    include_roots: ["a", "b"],
    exclude_globs: ["*.tmp"],
    critical_anchors: [{ path: "a/key.dat", sha256: keySha }],
  };
}

// controllable fake remote
const mirrorState = {
  inventory: inventoryLines.slice(),   // current HPC inventory lines
  availKb: 200000000,
  destExists: false,
  outage: false,
  verifiedCount: 2,
  rcloneFail: false,
};
const remoteCmds = [];
const shellCmds = [];

function fakeRunRemote(target, command) {
  const cmd = String(command);
  remoteCmds.push({ target, command: cmd });
  if (mirrorState.outage) throw new Error("simulated outage");
  if (target === "HPC") {
    if (cmd.includes("find -P") || cmd.includes("openssl dgst")) {
      return { stdout: mirrorState.inventory.join("\n") + "\n", stderr: "", exitCode: 0 };
    }
    return { stdout: "", stderr: "unhandled HPC command", exitCode: 1 };
  }
  if (target === "genbioh100") {
    if (cmd.includes("RECEIPT_MISSING")) {
      return { stdout: `DEST_EXISTS=1\n${createHash("sha256").update("receipt").digest("hex")}  ${DEST_ROOT}/manifests/mirror/TRANSFER_RECEIPT.json\n{"schema_version":1,"status":"verified","project":"w0mirror"}\n`, stderr: "", exitCode: 0 };
    }
    if (cmd.includes("AVAIL_KB")) {
      return { stdout: `HOST=genbioh100\nAVAIL_KB=${mirrorState.availKb}\nDEST_EXISTS=${mirrorState.destExists ? 1 : 0}\nDEST_NONEMPTY=0\n`, stderr: "", exitCode: 0 };
    }
    if (cmd.includes("source_inventory.sha256") && cmd.includes("while read")) {
      return { stdout: `VERIFIED_COUNT=${mirrorState.verifiedCount}\n`, stderr: "", exitCode: 0 };
    }
    if (cmd.includes("H100_MIRROR_PROMOTED")) {
      return { stdout: `H100_MIRROR_PROMOTED=${DEST_ROOT}\n`, stderr: "", exitCode: 0 };
    }
    return { stdout: "", stderr: "unhandled genbioh100 command", exitCode: 1 };
  }
  return { stdout: "", stderr: "wrong target", exitCode: 1 };
}

function makeExec(session) {
  return { agent: { id: session, session: { id: session, header: { cwd: "/tmp" } } }, signal: new AbortController().signal };
}

async function makeHarness({ session, manifestPath, approve = true }) {
  const tools = createH100MirrorTools({
    makeTool: (name, description, parameters, execute) => ({ name, description, parameters, execute }),
    requirePolicy: () => ({ hash: "testpolicyhash", targets: {} }),
    requireState: () => ({ policy: { hash: "testpolicyhash", targets: {} }, runs: [], remoteGrants: [] }),
    publicState: (s) => JSON.parse(JSON.stringify({ policy: { valid: true, hash: s.policy.hash }, runs: s.runs, lastError: null })),
    runRemote: async (target, command) => fakeRunRemote(target, command),
    shell: {
      resolve: (r) => r,
      run: async (request) => {
        const cmd = String(request.command);
        shellCmds.push(cmd);
        if (mirrorState.rcloneFail) return { stdout: { text: "" }, stderr: { text: "rclone failed" }, exitCode: 1, signal: null, timedOut: false };
        return { stdout: { text: "" }, stderr: { text: "" }, exitCode: 0, signal: null, timedOut: false };
      },
    },
    userQuestions: {
      ask: async ({ questions }) => ({ answers: questions.map((q) => ({ id: q.id, selected: approve ? ["Approve this transfer"] : ["Reject"] })) }),
    },
    config: { h100MirrorManifestPath: manifestPath, commandTimeoutMs: 30000 },
    requireRemoteAccess: async () => {},
  });
  return tools;
}

let manifestPath;
async function writeManifest(doc, name = "mirror-manifest.yaml") {
  manifestPath = join(tmp, name);
  await writeFile(manifestPath, dump(doc), "utf8");
  return manifestPath;
}

function resetMirror() {
  mirrorState.inventory = inventoryLines.slice();
  mirrorState.availKb = 200000000;
  mirrorState.destExists = false;
  mirrorState.outage = false;
  mirrorState.verifiedCount = 2;
  mirrorState.rcloneFail = false;
  remoteCmds.length = 0;
  shellCmds.length = 0;
}

// ── manifest validation ───────────────────────────────────────────────────────
test("mirror manifest loads and validates anchors", async () => {
  const p = await writeManifest(makeManifestDoc(), "valid.yaml");
  const m = await loadMirrorManifest(p);
  assert.equal(m.project, "w0mirror");
  assert.equal(m.sourceRoot, SOURCE_ROOT);
  assert.equal(m.destinationRoot, DEST_ROOT);
  assert.deepEqual(m.includeRoots, ["a", "b"]);
  assert.equal(m.anchors.length, 1);
  assert.equal(m.anchors[0].sha256, keySha);
});

test("non-curated or wrong-source-target manifest is rejected", async () => {
  const bad1 = makeManifestDoc(); bad1.mode = "sync";
  await assert.rejects(loadMirrorManifest(await writeManifest(bad1, "b1.yaml")), /curated/u);
  const bad2 = makeManifestDoc(); bad2.source.target = "genbioh100";
  await assert.rejects(loadMirrorManifest(await writeManifest(bad2, "b2.yaml")), /HPC/u);
});

// ── plan ──────────────────────────────────────────────────────────────────────
test("plan: read-only fresh inventory -> immutable session plan hash", async () => {
  resetMirror();
  const p = await writeManifest(makeManifestDoc(), "plan1.yaml");
  const tools = await makeHarness({ session: "s-plan1", manifestPath: p });
  const result = await tools.planTool.execute({}, makeExec("s-plan1"));
  assert.equal(result.ok, true);
  assert.match(result.status.mirrorPlan.plan_hash, /^[a-f0-9]{64}$/u);
  assert.equal(result.status.mirrorPlan.file_count, 2);
  // plan must not transfer (no rclone on the local shell side)
  assert.equal(shellCmds.length, 0, "plan must perform no local rclone transfer");
});

test("plan: destination already exists -> rejected (no overwrite)", async () => {
  resetMirror();
  mirrorState.destExists = true;
  const p = await writeManifest(makeManifestDoc(), "plan2.yaml");
  const tools = await makeHarness({ session: "s-plan2", manifestPath: p });
  await assert.rejects(tools.planTool.execute({}, makeExec("s-plan2")), /already exists/u);
});

test("plan: inventory blocker (symlink) -> rejected", async () => {
  resetMirror();
  mirrorState.inventory = ["SYMLINK\tbadlink", ...inventoryLines];
  const p = await writeManifest(makeManifestDoc(), "plan3.yaml");
  const tools = await makeHarness({ session: "s-plan3", manifestPath: p });
  await assert.rejects(tools.planTool.execute({}, makeExec("s-plan3")), /inventory blockers/u);
});

test("plan: critical anchor checksum mismatch on HPC -> rejected", async () => {
  resetMirror();
  // anchor expects keySha but HPC returns a different digest
  mirrorState.inventory = [
    `FILE\t${keyContent.length}\t1700000000\t${"f".repeat(64)}\ta/key.dat`,
    ...inventoryLines.slice(1),
  ];
  const p = await writeManifest(makeManifestDoc(), "plan4.yaml");
  const tools = await makeHarness({ session: "s-plan4", manifestPath: p });
  await assert.rejects(tools.planTool.execute({}, makeExec("s-plan4")), /critical anchor checksum mismatch/u);
});

test("plan: insufficient genbioh100 free space -> rejected", async () => {
  resetMirror();
  mirrorState.availKb = 1024; // far below the selected bytes
  const p = await writeManifest(makeManifestDoc(), "plan5.yaml");
  const tools = await makeHarness({ session: "s-plan5", manifestPath: p });
  await assert.rejects(tools.planTool.execute({}, makeExec("s-plan5")), /insufficient genbioh100 free space/u);
});

// ── execute ───────────────────────────────────────────────────────────────────
async function planThenExecute({ session, approve = true, mutate } = {}) {
  const p = await writeManifest(makeManifestDoc(), `exec-${session}.yaml`);
  const tools = await makeHarness({ session, manifestPath: p, approve });
  const plan = await tools.planTool.execute({}, makeExec(session));
  if (mutate) await mutate(plan);
  return { tools, plan: plan.status.mirrorPlan.plan_hash };
}

test("execute: happy path -> rclone-only copy + atomic promote (no destructive verbs)", async () => {
  resetMirror();
  const { tools, plan } = await planThenExecute({ session: "s-exec1" });
  const result = await tools.executeTool.execute({ plan_hash: plan }, makeExec("s-exec1"));
  assert.equal(result.ok, true);
  // every local transfer command is rclone, and none is destructive
  assert.ok(shellCmds.length >= 3, "execute must run rclone transfers");
  for (const c of shellCmds) {
    assert.match(c, /^rclone (?:copy|copyto) /u, `transfer must be rclone copy/copyto: ${c.slice(0, 40)}`);
    assert.ok(!/\b(?:sync|delete|purge|moveto|move)\b/u.test(c), `destructive rclone verb rejected: ${c.slice(0, 60)}`);
  }
  assert.ok(!shellCmds.some((c) => /\bscp\b/u.test(c)), "scp must never be used");
  // an atomic promote (mv of the verified temp) was issued on genbioh100
  assert.ok(remoteCmds.some((r) => r.target === "genbioh100" && r.command.includes("H100_MIRROR_PROMOTED")), "atomic promote must run");
});

test("execute: source drift after planning -> rejected (fresh plan required)", async () => {
  resetMirror();
  const { tools, plan } = await planThenExecute({
    session: "s-exec2",
    mutate: () => { mirrorState.inventory = [inventoryLines[0], `FILE\t999\t1700000000\t${"a".repeat(64)}\tb/changed.dat`]; },
  });
  await assert.rejects(tools.executeTool.execute({ plan_hash: plan }, makeExec("s-exec2")), /source drifted|manifest/u);
});

test("execute: manifest drift after planning -> rejected", async () => {
  resetMirror();
  const p = await writeManifest(makeManifestDoc(), "exec-3.yaml");
  const tools = await makeHarness({ session: "s-exec3", manifestPath: p });
  const plan = (await tools.planTool.execute({}, makeExec("s-exec3"))).status.mirrorPlan.plan_hash;
  // mutate the manifest on disk after planning
  await writeFile(p, dump({ ...makeManifestDoc(), exclude_globs: ["*.tmp", "*.tmp2"] }), "utf8");
  await assert.rejects(tools.executeTool.execute({ plan_hash: plan }, makeExec("s-exec3")), /manifest drifted/u);
});

test("execute: material transfer rejection -> no transfer, no promote", async () => {
  resetMirror();
  const { tools, plan } = await planThenExecute({ session: "s-exec4", approve: false });
  await assert.rejects(tools.executeTool.execute({ plan_hash: plan }, makeExec("s-exec4")), /not explicitly approved/u);
  assert.equal(shellCmds.length, 0, "no rclone transfer may run after a rejected approval");
  assert.ok(!remoteCmds.some((r) => r.command.includes("H100_MIRROR_PROMOTED")), "no promotion after a rejected approval");
});

test("execute: unknown plan hash -> rejected", async () => {
  resetMirror();
  const { tools } = await planThenExecute({ session: "s-exec5" });
  await assert.rejects(tools.executeTool.execute({ plan_hash: "f".repeat(64) }, makeExec("s-exec5")), /unknown or non-session-owned/u);
});

// ── status ────────────────────────────────────────────────────────────────────
test("status: read-only receipt + destination check (no transfer)", async () => {
  resetMirror();
  const p = await writeManifest(makeManifestDoc(), "status1.yaml");
  const tools = await makeHarness({ session: "s-status1", manifestPath: p });
  const result = await tools.statusTool.execute({}, makeExec("s-status1"));
  assert.equal(result.ok, true);
  assert.equal(result.status.mirrorStatus.project, "w0mirror");
  assert.equal(shellCmds.length, 0, "status must perform no local transfer");
});

after(async () => { await rm(tmp, { recursive: true, force: true }); });
