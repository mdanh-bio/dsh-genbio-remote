// t11 / t9 advisory B: the AI.zymes envelope must enforce the SAME
// policy-hash staleness rejection as the pinned path (pinned.js
// validateEnvelope), BEFORE any side effect (transfer-approval question,
// remote access, background job, shell, or SSH). A stale envelope — recorded
// under an older policy generation — must be rejected at admission with zero
// observable effects.
import assert from "node:assert/strict";
import { test } from "node:test";
import { createAizymeTools } from "../lib/aizyme.js";

const exec = { agent: { id: "aiz", session: { id: "aiz", header: { cwd: "/tmp" } } }, signal: new AbortController().signal };

function harness({ state, remoteImpl } = {}) {
  const calls = { remote: [], shell: [], jobs: 0, questions: 0 };
  let latest = null;
  const tools = createAizymeTools({
    makeTool: (name, description, parameters, execute) => ({ name, description, parameters, execute }),
    requirePolicy: () => ({ targets: { HPC: { test_gate: { real_submission: "gpu04" }, allowlist: { gpu04: { partition: "gpus" } } } } }),
    requireState: () => state,
    publicState: () => ({}),
    requireRemoteAccess: async () => {},
    runRemote: async (target, command) => { calls.remote.push(command); return remoteImpl?.(target, command) ?? { stdout: "", stderr: "", exitCode: 0, signal: null, timedOut: false }; },
    shell: {
      resolve: (request) => request,
      run: async (request) => { calls.shell.push(request.command); return { stdout: { text: "" }, stderr: { text: "" }, exitCode: 0, signal: null, timedOut: false }; },
    },
    userQuestions: { ask: async () => { calls.questions += 1; return { answers: [] }; } },
    jobs: { start(spec) { calls.jobs += 1; latest = spec.run(); return `job-${calls.jobs}`; } },
    config: { logMaxBytes: 65536 },
  });
  return { tools, calls, latest: () => latest };
}

function state(overrides = {}) {
  return {
    policy: { hash: "hash-current" }, runs: [],
    // envelope.policyHash matches state.policy.hash by default; the stale
    // test overrides state.policy.hash to simulate a policy rotation.
    envelope: { target: "HPC", node: "gpu04", partition: "gpus", maxCpus: 32, maxGpus: 4, concurrency: 1, policyHash: "hash-current" },
    ...overrides,
  };
}

test("a stale AI.zymes envelope policy hash is rejected before any side effect", async (t) => {
  const h = harness({ state: state({ policy: { hash: "hash-rotated" } }) });
  await assert.rejects(h.tools.stageTool.execute({ operation: "stage0-1" }, exec), /stale|policy (hash|changed)/u);
  assert.deepEqual(
    { questions: h.calls.questions, jobs: h.calls.jobs, remote: h.calls.remote.length, shell: h.calls.shell.length },
    { questions: 0, jobs: 0, remote: 0, shell: 0 },
    "a stale envelope must produce zero transfer questions, jobs, remote calls, and shell calls",
  );
});

test("a matching AI.zymes envelope policy hash is accepted (not rejected as stale)", async (t) => {
  const h = harness({ state: state() });
  let error = null;
  try {
    const result = await h.tools.stageTool.execute({ operation: "stage0-1" }, exec);
    assert.equal(result.ok, true, "admission succeeds past the envelope check");
  } catch (caught) {
    error = caught;
  }
  if (error) {
    assert.doesNotMatch(String(error.message), /stale|policy (hash|changed)/u, "a matching hash must never be rejected as a stale envelope");
  }
});
