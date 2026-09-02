import assert from "node:assert/strict";
import path from "node:path";

const bundle = await import("../lib/index.js");
const policyPath = path.resolve(import.meta.dirname, "../fixtures/genbio-compute-policy.test.yaml");
const registered = [];
const provided = new Map();
const published = [];
const shellRuns = [];
let latestJob = null;
let jobCounter = 0;
const WORKSPACE_CWD = "/tmp/dsh-smoke-workspace";
// Persistent per-workspace remote grants (the dsh-workspace-folder-access store
// stand-in). Start empty so the policy smoke root must go through the ask flow.
const remoteGrantStore = [];
const jobs = {
  start(spec) {
    latestJob = { spec, hooks: spec.run() };
    return `${spec.kind}-${++jobCounter}`;
  },
};
const ctx = {
  shell: {
    resolve(request) { return request; },
    async run(request) {
      shellRuns.push(request);
      return { exitCode: 0, signal: null, timedOut: false, aborted: false, stdout: { text: "ok\n" }, stderr: { text: "" } };
    },
  },
  timer: { interval() { return () => {}; } },
  systemPrompt: { section() { return () => {}; } },
  tools: { register(tool) { registered.push(tool); return () => {}; } },
  provide(name, value) { provided.set(name, value); return () => provided.delete(name); },
  get(name) {
    if (name === "jobs") return jobs;
    if (name === "workspaceAccess") return {
      remoteRootsFor: (workspacePath) => workspacePath === WORKSPACE_CWD ? remoteGrantStore : [],
      grantRemote: async (workspacePath, target, root, mode) => { remoteGrantStore.push({ target, root, mode }); },
    };
    if (name === "userQuestions") return {
      async ask({ questions }) {
        return { answers: questions.map((question) => ({ id: question.id, selected: question.id === "genbio-resource-expansion" ? ["Expand to requested resources"] : ["Grant for this session only"] })) };
      },
    };
  },
  effect(setup) { return setup(); },
};
await bundle.apply(ctx, {
  policyPath,
  policyPollMs: 3600000,
  commandTimeoutMs: 30000,
  smokeTimeoutMs: 180000,
  logMaxBytes: 65536,
});
const tools = Object.fromEntries(registered.map((tool) => [tool.name, tool]));
for (const required of ["genbio_project_plan", "genbio_project_execute", "genbio_project_status", "genbio_project_cancel", "genbio_project_fetch"]) assert.ok(tools[required], `missing required schema-v2 tool ${required}`);
provided.get("genbioRemote").registerMemoryPublisher({ async publish(session, record) { published.push({ session, record }); return { ok: true, status: "published", sessionId: `dsh-${session.id}` }; } });
const exec = (id) => ({ agent: { id, session: { id, header: { cwd: WORKSPACE_CWD } } }, signal: new AbortController().signal });

await assert.rejects(tools.genbio_set_envelope.execute({ target: "HPC", node: "gpu04", partition: "gpus", workload_class: "gpu", max_cpus: 64, max_gpus: 4, concurrency: 8, acknowledge_restrictions: true }, exec("gpu04")), /concurrency exceeds policy cap 4/);

const hpc = exec("hpc");
await tools.genbio_set_envelope.execute({ target: "HPC", node: "gpu04", partition: "gpus", workload_class: "gpu", max_cpus: 1, max_gpus: 0, concurrency: 1, acknowledge_restrictions: true }, hpc);
const smokeLaunched = await tools.genbio_launch.execute({ target: "HPC", operation: "gpu04-smoke", cpus: 1, gpus: 0, concurrency: 1 }, hpc);
await latestJob.hooks.done;
assert.equal(shellRuns.at(-1).timeoutMs, 180000);
assert.match(shellRuns.at(-1).command, /#SBATCH --nodelist=gpu04/);
assert.equal(["--account", "--time=", "--mem", "--exclusive"].some((token) => shellRuns.at(-1).command.includes(token)), false);
// The smoke root was not pre-granted, so the launch had to pause on the
// remote-folder question; the mock answered session-only with rw.
assert.deepEqual(smokeLaunched.status.remoteGrants, [{ target: "HPC", root: bundle.HPC_SMOKE_ROOT, mode: "rw" }]);
assert.equal(smokeLaunched.status.started.remoteGrants.some((entry) => entry.root === bundle.HPC_SMOKE_ROOT), true);

const direct = exec("direct");
await tools.genbio_set_envelope.execute({ target: "genbioh100", node: "genbioh100", workload_class: "gpu-render", max_cpus: 16, max_gpus: 1, mem_gb: 32, concurrency: 1, acknowledge_restrictions: true }, direct);
const launched = await tools.genbio_launch.execute({ target: "genbioh100", operation: "preflight-smoke", cpus: 16, gpus: 1, mem_gb: 32, concurrency: 1 }, direct);
await latestJob.hooks.done;
launched.status.started.stdout = "line1";
launched.status.started.stderr = "err1";
assert.equal(latestJob.hooks.readOutput(), "line1\n[stderr]\nerr1");
const finalized = await tools.genbio_finalize_run.execute({ run_id: launched.status.started.runId, project: "policy smoke", summary: "The direct policy smoke completed successfully.", significance: "The target respected the GPU 0 and CPU limits.", artifacts: [{ kind: "log", location: "/remote/run/log.txt", sha256: "a".repeat(64) }] }, direct);
assert.equal(finalized.ok, true);
assert.equal(published.length, 1);
assert.equal(published[0].record.text.includes("line1"), false);
assert.match(published[0].record.text, /direct policy smoke completed successfully/);
assert.equal(finalized.status.runs.at(-1).memory.status, "published");
await tools.genbio_finalize_run.execute({ run_id: launched.status.started.runId, project: "changed", summary: "ignored because frozen" }, direct);
assert.equal(published.length, 1);
await assert.rejects(tools.genbio_finalize_run.execute({ run_id: "missing", project: "x", summary: "x" }, direct), /unknown session-owned/);
console.log("runtime smoke passed: policy guards, job tracking, remote folder grants (ask flow, session-only, read ro), frozen finalization, and OpenViking handoff");
