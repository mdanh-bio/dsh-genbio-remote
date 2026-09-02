import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { mkdtemp, mkdir, readdir, readFile, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

const { createPinnedTools, parseManifest } = await import("../lib/pinned.js");

const REMOTE_ROOT = "/data01/test/pinnedfetch-run";
const tmp = await mkdtemp(join(tmpdir(), "pinned-fetch-test-"));

// Deterministic "remote" bytes per allowlisted rel.
const REMOTE_CONTENT = {
  "inventory.json": "remote inventory bytes\n",
  "report.md": "remote report bytes\n",
  "env.txt": "remote env bytes\n",
};
const sha = (text) => createHash("sha256").update(text).digest("hex");
const ALLOWLIST = Object.keys(REMOTE_CONTENT);

function makeDeps({ projectsDir, state, latestRef, discoveryRows, shellContent = REMOTE_CONTENT, questionAnswers, questionCalls }) {
  const shellCommands = [];
  const remoteCommands = [];
  let counter = 0;
  return {
    shellCommands,
    remoteCommands,
    tools: createPinnedTools({
      makeTool: (name, description, parameters, execute) => ({ name, description, parameters, execute }),
      requirePolicy: () => ({ targets: { HPC: { test_gate: { real_submission: "gpu04" } } } }),
      requireState: () => state,
      publicState: (s) => ({ policy: { valid: true, hash: "test" }, envelope: s.envelope, runs: s.runs, lastError: null, remoteGrants: [] }),
      runRemote: async (target, command) => {
        remoteCommands.push({ target, command });
        if (command.includes("stat -c %s")) return { stdout: `${discoveryRows.join("\n")}\n`, stderr: "", exitCode: 0 };
        return { stdout: "", stderr: "", exitCode: 0 };
      },
      shell: {
        resolve: (request) => request,
        run: async (request) => {
          shellCommands.push(request.command);
          const tokens = request.command.trim().split(/\s+/u);
          const source = tokens.at(-2).replace(/^'/u, "").replace(/'$/u, "");
          const dest = tokens.at(-1).replace(/^'/u, "").replace(/'$/u, "");
          const rel = source.slice(`hpc:${REMOTE_ROOT}/`.length);
          const content = shellContent[rel];
          if (typeof content !== "string") return { stdout: { text: "" }, stderr: { text: `no remote content for ${rel}` }, exitCode: 1, signal: null, timedOut: false };
          await mkdir(dirname(dest), { recursive: true });
          await writeFile(dest, content);
          return { stdout: { text: "" }, stderr: { text: "" }, exitCode: 0, signal: null, timedOut: false };
        },
      },
      userQuestions: {
        ask: async ({ questions }) => {
          for (const q of questions) questionCalls.push(q.id);
          return { answers: questions.map((q) => ({ id: q.id, selected: questionAnswers(q) })) };
        },
      },
      jobs: {
        start(spec) {
          const hooks = spec.run();
          latestRef.run = { spec, hooks };
          return `job-${++counter}`;
        },
      },
      config: { pinnedProjectsDir: projectsDir, logMaxBytes: 65536 },
      requireRemoteAccess: async () => {},
    }),
  };
}

try {
  const localRoot = join(tmp, "local");
  const projectsDir = join(tmp, "projects");
  const retrievalDir = join(localRoot, "retrieval");
  await mkdir(join(localRoot, "a/b"), { recursive: true });
  await mkdir(projectsDir, { recursive: true });
  await writeFile(join(localRoot, "a/b/hello.py"), "print(1)\n");

  const manifestYaml = (project, fetchYaml) => `schema_version: 1
project: ${project}
description: fetch unit test manifest
local_root: ${localRoot}
remote_root: ${REMOTE_ROOT}
files:
  - a/b/hello.py
jobs:
  t-job:
    template: a/b/hello.py
    cpus: 1
${fetchYaml}`;
  const fetchYaml = `fetch:
  max_bytes: 1048576
  dest: retrieval
  files:
    - inventory.json
    - report.md
    - env.txt
`;
  await writeFile(join(projectsDir, "pinnedfetch.yaml"), manifestYaml("pinnedfetch", fetchYaml));
  await writeFile(join(projectsDir, "pinnedfetchcap.yaml"), manifestYaml("pinnedfetchcap", fetchYaml.replace("1048576", "10")));
  await writeFile(join(projectsDir, "pinnedfetch-nofetch.yaml"), manifestYaml("pinnedfetch-nofetch", ""));

  const exec = { agent: { id: "t", session: { id: "t", header: { cwd: tmp } } }, signal: new AbortController().signal };
  const defaultRows = ALLOWLIST.map((rel) => `OK|${rel}|${REMOTE_CONTENT[rel].length}|${sha(REMOTE_CONTENT[rel])}`);
  const approveAll = () => ["Approve this retrieval"];

  // ── Happy path: all allowlisted files, dual hash gate, receipt ──
  const state = { policy: { hash: "test" }, envelope: null, runs: [] };
  const latestRef = { run: null };
  const questionCalls = [];
  const deps = makeDeps({ projectsDir, state, latestRef, discoveryRows: defaultRows, questionAnswers: approveAll, questionCalls });

  await deps.tools.fetchTool.execute({ project: "pinnedfetch" }, exec);
  await latestRef.run.hooks.done;
  assert.equal(state.runs.at(-1).status, "completed", `fetch run must complete: ${state.runs.at(-1).error}`);
  assert.match(state.runs.at(-1).stdout, /PINNED_FETCH_OK/u);

  assert.equal(deps.shellCommands.length, 3, `expected 3 rclone copyto commands, got ${deps.shellCommands.length}`);
  const fetchedRels = new Set();
  for (const command of deps.shellCommands) {
    assert.match(command, /^rclone copyto --retries 2 --low-level-retries 2 --contimeout 20s /u);
    const tokens = command.trim().split(/\s+/u);
    const source = tokens.at(-2).replace(/^'/u, "").replace(/'$/u, "");
    const dest = tokens.at(-1).replace(/^'/u, "").replace(/'$/u, "");
    assert.match(source, /^hpc:\/data01\/test\/pinnedfetch-run\/[a-z0-9.]+$/u, `rclone source must be remote <remote>:<abs>, got: ${source}`);
    assert.match(dest, new RegExp(`^${localRoot.replace(/[\\^$.*+?()[\]{}|]/gu, "\\$&")}/retrieval/[a-z0-9.]+\\.part$`), `rclone destination must be a local .part file, got: ${dest}`);
    fetchedRels.add(source.slice(`hpc:${REMOTE_ROOT}/`.length));
  }
  assert.deepEqual([...fetchedRels].sort(), [...ALLOWLIST].sort(), "fetch must download exactly the allowlisted set");

  // Files must be final (renamed from .part) with the exact remote bytes.
  for (const rel of ALLOWLIST) {
    assert.equal(existsSync(`${retrievalDir}/${rel}.part`), false, "no .part file may remain after success");
    assert.equal(await readFile(join(retrievalDir, rel), "utf8"), REMOTE_CONTENT[rel]);
  }
  // Exactly one dated receipt, parseable, with matching hashes.
  const entries = await readdir(retrievalDir);
  const receipts = entries.filter((name) => /^FETCH_RECEIPT_.*\.json$/u.test(name));
  assert.equal(receipts.length, 1, `expected exactly one receipt, got ${receipts.join(", ")}`);
  const receipt = JSON.parse(await readFile(join(retrievalDir, receipts[0]), "utf8"));
  assert.equal(receipt.project, "pinnedfetch");
  assert.equal(receipt.source_root, REMOTE_ROOT);
  assert.equal(receipt.transfer, "rclone-copyto");
  assert.equal(receipt.files.length, 3);
  for (const item of receipt.files) assert.equal(item.sha256, sha(REMOTE_CONTENT[item.rel]), `receipt hash must equal recomputed remote-content hash for ${item.rel}`);

  // Discovery must be the single read-only ssh, strict-contract, listing all rels.
  const discovery = deps.remoteCommands.filter((entry) => entry.command.includes("stat -c %s"));
  assert.equal(discovery.length, 1, "fetch must issue exactly one read-only discovery ssh");
  assert.match(discovery[0].command, /^ssh -T -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=yes -- HPC /u);
  for (const rel of ALLOWLIST) assert.ok(discovery[0].command.includes(`'${rel}'`), `discovery must list ${rel}`);
  assert.equal(questionCalls.length, 1, "happy path must show exactly one transfer card");
  assert.match(questionCalls[0], /^genbio-pinned-fetch-pinnedfetch$/u);

  // ── Selective fetch: only the requested allowlisted rel ──
  const state2 = { policy: state.policy, envelope: null, runs: [] };
  const latestRef2 = { run: null };
  const deps2 = makeDeps({ projectsDir, state: state2, latestRef: latestRef2, discoveryRows: defaultRows, questionAnswers: approveAll, questionCalls: [] });
  await deps2.tools.fetchTool.execute({ project: "pinnedfetch", files: ["env.txt"] }, exec);
  await latestRef2.run.hooks.done;
  assert.equal(state2.runs.at(-1).status, "completed", `selective fetch must complete: ${state2.runs.at(-1).error}`);
  assert.equal(deps2.shellCommands.length, 1, "selective fetch must download only the requested file");
  assert.match(deps2.shellCommands[0], /env\.txt\.part/u);
  const discovery2 = deps2.remoteCommands.find((entry) => entry.command.includes("stat -c %s"));
  assert.ok(!discovery2.command.includes("'inventory.json'"), "selective discovery must not hash unrequested files");

  // ── Fail closed: owner rejects the transfer card ──
  const state3 = { policy: state.policy, envelope: null, runs: [] };
  const latestRef3 = { run: null };
  const deps3 = makeDeps({ projectsDir, state: state3, latestRef: latestRef3, discoveryRows: defaultRows, questionAnswers: () => ["Reject"], questionCalls: [] });
  await deps3.tools.fetchTool.execute({ project: "pinnedfetch" }, exec);
  await latestRef3.run.hooks.done;
  assert.equal(state3.runs.at(-1).status, "failed");
  assert.match(state3.runs.at(-1).error, /not explicitly approved/u);
  assert.equal(deps3.shellCommands.length, 0, "no transfer may happen after rejection");

  // ── Fail closed: requested rel outside the allowlist ──
  const state4 = { policy: state.policy, envelope: null, runs: [] };
  const latestRef4 = { run: null };
  const deps4 = makeDeps({ projectsDir, state: state4, latestRef: latestRef4, discoveryRows: defaultRows, questionAnswers: approveAll, questionCalls: [] });
  await deps4.tools.fetchTool.execute({ project: "pinnedfetch", files: ["troll.bin"] }, exec);
  await latestRef4.run.hooks.done;
  assert.equal(state4.runs.at(-1).status, "failed");
  assert.match(state4.runs.at(-1).error, /troll\.bin/u);
  assert.match(state4.runs.at(-1).error, /allowlisted/u);
  assert.equal(deps4.remoteCommands.length, 0, "no ssh may be issued for a non-allowlisted request");
  assert.equal(deps4.shellCommands.length, 0, "no transfer may happen for a non-allowlisted request");

  // ── Fail closed: artifact missing on the remote ──
  const state5 = { policy: state.policy, envelope: null, runs: [] };
  const latestRef5 = { run: null };
  const rows5 = [`OK|${ALLOWLIST[0]}|${REMOTE_CONTENT[ALLOWLIST[0]].length}|${sha(REMOTE_CONTENT[ALLOWLIST[0]])}`, "MISSING|report.md", `OK|${ALLOWLIST[2]}|${REMOTE_CONTENT[ALLOWLIST[2]].length}|${sha(REMOTE_CONTENT[ALLOWLIST[2]])}`];
  const deps5 = makeDeps({ projectsDir, state: state5, latestRef: latestRef5, discoveryRows: rows5, questionAnswers: approveAll, questionCalls: [] });
  await deps5.tools.fetchTool.execute({ project: "pinnedfetch" }, exec);
  await latestRef5.run.hooks.done;
  assert.equal(state5.runs.at(-1).status, "failed");
  assert.match(state5.runs.at(-1).error, /missing/u);
  assert.match(state5.runs.at(-1).error, /report\.md/u);
  assert.equal(deps5.shellCommands.length, 0, "no transfer may happen when a remote artifact is missing");

  // ── Fail closed: total bytes above the manifest cap (before the card) ──
  const state6 = { policy: state.policy, envelope: null, runs: [] };
  const latestRef6 = { run: null };
  const questionCalls6 = [];
  const deps6 = makeDeps({ projectsDir, state: state6, latestRef: latestRef6, discoveryRows: defaultRows, questionAnswers: approveAll, questionCalls: questionCalls6 });
  await deps6.tools.fetchTool.execute({ project: "pinnedfetchcap" }, exec);
  await latestRef6.run.hooks.done;
  assert.equal(state6.runs.at(-1).status, "failed");
  assert.match(state6.runs.at(-1).error, /exceeds the manifest cap/u);
  assert.equal(deps6.shellCommands.length, 0, "no transfer may happen above the byte cap");
  assert.equal(questionCalls6.length, 0, "the cap must fail before any transfer card is shown");

  // ── Fail closed: local bytes do not match the remote SHA-256 ──
  // (The happy path above already wrote a valid inventory.json + one receipt
  // into the shared destination; assert the failed fetch changes nothing.)
  await rm(`${retrievalDir}/inventory.json`, { force: true });
  const receiptsBefore7 = (await readdir(retrievalDir)).filter((name) => /^FETCH_RECEIPT_.*\.json$/u.test(name)).length;
  const state7 = { policy: state.policy, envelope: null, runs: [] };
  const latestRef7 = { run: null };
  const shellContent7 = { ...REMOTE_CONTENT, "inventory.json": "remote inventory bytsX\n" };
  const deps7 = makeDeps({ projectsDir, state: state7, latestRef: latestRef7, discoveryRows: defaultRows, shellContent: shellContent7, questionAnswers: approveAll, questionCalls: [] });
  await deps7.tools.fetchTool.execute({ project: "pinnedfetch" }, exec);
  await latestRef7.run.hooks.done;
  assert.equal(state7.runs.at(-1).status, "failed");
  assert.match(state7.runs.at(-1).error, /sha256 mismatch/u);
  assert.match(state7.runs.at(-1).error, /inventory\.json/u);
  assert.equal(existsSync(`${retrievalDir}/inventory.json.part`), false, "mismatched .part file must be removed");
  assert.equal(existsSync(`${retrievalDir}/inventory.json`), false, "mismatched file must never be promoted to the final name");
  assert.equal((await readdir(retrievalDir)).filter((name) => /^FETCH_RECEIPT_.*\.json$/u.test(name)).length, receiptsBefore7, "no new receipt may be written on failure");

  // ── Fail closed: manifest without a fetch section ──
  const deps8 = makeDeps({ projectsDir, state: { policy: state.policy, envelope: null, runs: [] }, latestRef: { run: null }, discoveryRows: defaultRows, questionAnswers: approveAll, questionCalls: [] });
  await assert.rejects(deps8.tools.fetchTool.execute({ project: "pinnedfetch-nofetch" }, exec), /no fetch section/u);

  // ── parseManifest: malformed fetch sections fail closed ──
  const base = parseManifest("pinnedfetch", {
    schema_version: 1, project: "pinnedfetch", local_root: localRoot, remote_root: REMOTE_ROOT,
    files: ["a/b/hello.py"], jobs: { "t-job": { template: "a/b/hello.py", cpus: 1 } },
    fetch: { max_bytes: 1048576, dest: "retrieval", files: ["inventory.json"] },
  });
  assert.equal(base.fetch.maxBytes, 1048576);
  assert.equal(base.fetch.dest, "retrieval");
  const noFetch = parseManifest("pinnedfetch", {
    schema_version: 1, project: "pinnedfetch", local_root: localRoot, remote_root: REMOTE_ROOT,
    files: ["a/b/hello.py"], jobs: { "t-job": { template: "a/b/hello.py", cpus: 1 } },
  });
  assert.equal(noFetch.fetch, null, "absent fetch section must parse as null");
  const badFetch = (fetch, pattern) => assert.throws(
    () => parseManifest("pinnedfetch", {
      schema_version: 1, project: "pinnedfetch", local_root: localRoot, remote_root: REMOTE_ROOT,
      files: ["a/b/hello.py"], jobs: { "t-job": { template: "a/b/hello.py", cpus: 1 } }, fetch,
    }), pattern,
  );
  badFetch("nope", /fetch must be a mapping/u);
  badFetch({ max_bytes: 0, dest: "retrieval", files: ["a"] }, /fetch\.max_bytes/u);
  badFetch({ max_bytes: 100, dest: "/abs", files: ["a"] }, /fetch\.dest|staged path must be relative/u);
  badFetch({ max_bytes: 100, dest: "retrieval", files: [] }, /fetch\.files/u);
  badFetch({ dest: "retrieval", files: ["a"] }, /fetch\.max_bytes/u);

  // Global no-scp invariant across every scenario.
  for (const command of [...deps.shellCommands, ...deps2.shellCommands, ...deps3.shellCommands, ...deps4.shellCommands, ...deps5.shellCommands, ...deps6.shellCommands, ...deps7.shellCommands]) {
    assert.match(command, /^rclone copyto /u, `all transfers must use rclone copyto, got: ${command.slice(0, 60)}`);
    assert.ok(!/\bscp\b/u.test(command), `scp must never appear in transfer commands: ${command.slice(0, 60)}`);
  }

  console.log("pinned-fetch unit tests passed");
} finally {
  await rm(tmp, { recursive: true, force: true });
}
