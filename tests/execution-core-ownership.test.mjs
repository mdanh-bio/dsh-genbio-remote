import assert from "node:assert/strict";
import test from "node:test";

import { cancelOwnedJob, findOwnedOperationRun } from "../lib/execution-core.js";

const exec = { agent: { id: "owner" }, signal: new AbortController().signal };

function ownedState() {
  return {
    allocations: [{ project: "demo", operation: "run", slurmJobId: "1234", status: "nonterminal" }],
    runs: [{ target: "HPC", operation: "project-demo-run", slurmJobId: "1234" }],
  };
}

test("ownership lookup binds exact project, operation, and numeric job id", () => {
  const state = ownedState();
  assert.ok(findOwnedOperationRun(state, "demo", "run", "1234"));
  assert.equal(findOwnedOperationRun(state, "demo", "other", "1234"), null);
  assert.equal(findOwnedOperationRun(state, "other", "run", "1234"), null);
  assert.equal(findOwnedOperationRun(state, "demo", "run", "9999"), null);
});

test("cancelOwnedJob issues exactly one exact numeric scancel for an approved owned job", async () => {
  const state = ownedState();
  const calls = [];
  const result = await cancelOwnedJob({
    state, project: "demo", operation: "run", jobId: "1234", exec,
    userQuestions: { ask: async ({ questions }) => ({ answers: [{ id: questions[0].id, selected: ["Cancel this job"] }] }) },
    runRemote: async (_target, command) => { calls.push(command); return { stdout: "", stderr: "", exitCode: 0 }; },
  });
  assert.deepEqual(result, { jobId: "1234", requested: true, terminalEvidence: "pending status refresh" });
  assert.equal(calls.length, 1);
  assert.match(calls[0], /scancel '1234'/u);
  assert.equal(calls[0].includes("--user"), false);
  assert.equal(state.allocations[0].status, "cancel-requested");
});

test("cancelOwnedJob rejects nonnumeric or unowned identifiers before remote action", async () => {
  const state = ownedState();
  let remoteCalls = 0;
  const args = {
    state, project: "demo", operation: "run", exec,
    userQuestions: { ask: async () => { throw new Error("question must not be reached"); } },
    runRemote: async () => { remoteCalls += 1; return { exitCode: 0 }; },
  };
  await assert.rejects(cancelOwnedJob({ ...args, jobId: "1234,5678" }), /invalid Slurm job id/u);
  await assert.rejects(cancelOwnedJob({ ...args, jobId: "9999" }), /not an active session-owned job/u);
  assert.equal(remoteCalls, 0);
});
