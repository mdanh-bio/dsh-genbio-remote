import assert from "node:assert/strict";
import path from "node:path";
import test from "node:test";
import * as bundle from "../lib/index.js";

const policyPath = path.resolve(import.meta.dirname, "../fixtures/genbio-compute-policy.test.yaml");

async function harness({ memoryMode = "manual", publish } = {}) {
  const tools = [];
  const provided = new Map();
  let latestJob;
  const ctx = {
    shell: {
      resolve(value) { return value; },
      async run() { return { exitCode: 0, signal: null, timedOut: false, stdout: { text: "safe raw stdout\n" }, stderr: { text: "" } }; },
    },
    timer: { interval() { return () => {}; } },
    systemPrompt: { section() { return () => {}; } },
    tools: { register(tool) { tools.push(tool); return () => {}; } },
    provide(name, value) { provided.set(name, value); return () => provided.delete(name); },
    get(name) {
      if (name === "jobs") return { start(spec) { latestJob = spec.run(); return "job-1"; } };
      if (name === "userQuestions") return { async ask() { return { answers: [] }; } };
    },
    effect(setup) { return setup(); },
  };
  await bundle.apply(ctx, { policyPath, policyPollMs: 999999, memoryMode });
  if (publish) provided.get("genbioRemote").registerMemoryPublisher({ publish });
  return { tools: Object.fromEntries(tools.map(tool => [tool.name, tool])), provided, get latestJob() { return latestJob; } };
}

const exec = id => ({ agent: { id, session: { id } }, signal: new AbortController().signal });

async function terminalRun(h, id = "session") {
  const owner = exec(id);
  await h.tools.genbio_set_envelope.execute({ target: "genbio_mdanh", node: "genbio_mdanh", workload_class: "cpu", max_cpus: 1, max_gpus: 0, concurrency: 1, acknowledge_restrictions: true }, owner);
  const launched = await h.tools.genbio_launch.execute({ target: "genbio_mdanh", operation: "preflight-smoke", cpus: 1, gpus: 0, concurrency: 1 }, owner);
  await h.latestJob.done;
  return { owner, runId: launched.status.started.runId };
}

test("memoryMode off freezes without calling a publisher", async () => {
  let calls = 0;
  const h = await harness({ memoryMode: "off", publish: async () => { calls++; return { ok: true, status: "published" }; } });
  const { owner, runId } = await terminalRun(h, "off");
  const result = await h.tools.genbio_finalize_run.execute({ run_id: runId, project: "p", summary: "durable summary" }, owner);
  assert.equal(calls, 0);
  assert.equal(result.status.runs[0].memory.status, "disabled");
});

test("unavailable finalization can publish later from the frozen record", async () => {
  const h = await harness();
  const { owner, runId } = await terminalRun(h, "late");
  const first = await h.tools.genbio_finalize_run.execute({ run_id: runId, project: "p", summary: "durable summary" }, owner);
  assert.equal(first.ok, false);
  assert.equal(first.status.runs[0].memory.status, "unavailable");
  let text;
  h.provided.get("genbioRemote").registerMemoryPublisher({ async publish(_session, record) { text = record.text; return { ok: true, status: "published" }; } });
  const retried = await h.tools.genbio_publish_run.execute({ run_id: runId }, owner);
  assert.equal(retried.ok, true);
  assert.equal(retried.status.runs[0].memory.status, "published");
  assert.match(text, /durable summary/);
  assert.doesNotMatch(text, /safe raw stdout/);
});

test("rejects oversized and sensitive curated fields", async () => {
  const h = await harness({ publish: async () => ({ ok: true, status: "published" }) });
  const { owner, runId } = await terminalRun(h, "bounds");
  await assert.rejects(h.tools.genbio_finalize_run.execute({ run_id: runId, project: "p", summary: "x".repeat(12001) }, owner), /summary exceeds/);
  await assert.rejects(h.tools.genbio_finalize_run.execute({ run_id: runId, project: "p", summary: "ok", artifacts: [{ kind: "log", location: "authorization=secret" }] }, owner), /sensitive material/);
  await assert.rejects(h.tools.genbio_finalize_run.execute({ run_id: runId, project: "p", summary: "ok", artifacts: [{ kind: "log", location: "/x", sha256: "bad" }] }, owner), /full SHA-256/);
});

test("coalesces concurrent frozen-record publication", async () => {
  let calls = 0;
  let release;
  const gate = new Promise(resolve => { release = resolve; });
  const h = await harness({ publish: async () => { calls++; await gate; return { ok: true, status: "published" }; } });
  const { owner, runId } = await terminalRun(h, "race");
  const first = h.tools.genbio_finalize_run.execute({ run_id: runId, project: "p", summary: "durable summary" }, owner);
  await new Promise(resolve => setImmediate(resolve));
  const second = h.tools.genbio_publish_run.execute({ run_id: runId }, owner);
  release();
  await Promise.all([first, second]);
  assert.equal(calls, 1);
});
