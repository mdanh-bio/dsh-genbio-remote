import assert from "node:assert/strict";
import test from "node:test";

import { aggregateProjects, runBelongsToProject, MAX_AGGREGATE_PLANS, MAX_AGGREGATE_RUNS } from "../lib/aggregate.js";

const bundle = await import("../lib/index.js");
const { projectionInit, projectionApply, projectionView } = bundle;

const resultEvent = (callId, status, seq) => ({
  type: "tool/result", seq, time: 0,
  data: {
    message: {
      source: { kind: "tool", callId },
      content: [{ type: "tool-result", toolCallId: callId, content: [{ type: "text", text: JSON.stringify({ ok: true, status }) }] }],
    },
  },
});
const callEvent = (callId, name, seq) => ({ type: "tool/call", seq, time: 0, data: { callId, name, arguments: "{}" } });

function summaries() {
  return [
    {
      project: "alpha",
      schema_version: 2,
      valid: true,
      description: "equilibration pipeline",
      operations: [
        { name: "build", form: "recipe", cpus: 4, gpus: 1, concurrency: 1 },
        { name: "equil", form: "recipe", cpus: 4, gpus: 1, concurrency: 1 },
        { name: "post", form: "recipe", cpus: 2, gpus: 0, concurrency: 1 },
      ],
    },
    { project: "beta", schema_version: 2, valid: true, operations: [{ name: "run", form: "recipe", cpus: 1, gpus: 0, concurrency: 1 }] },
    { project: "broken", valid: false, error: "bad yaml at line 3" },
  ];
}

function runs() {
  return [
    { runId: "r1", operation: "project-alpha-build", status: "completed", target: "HPC", startedAt: 1000, finishedAt: 2000, stdout: "LOG SECRET" },
    { runId: "r2", operation: "project-alpha-equil", status: "failed", target: "HPC", startedAt: 4000, finishedAt: 5000, error: "boom" },
    { runId: "r3", operation: "project-alpha-equil", status: "running", target: "HPC", startedAt: 6000, finishedAt: null },
    { runId: "r4", operation: "project-beta-run", status: "running", target: "HPC", startedAt: 7000, finishedAt: null, stderr: "NOT YET" },
    { runId: "r5", operation: "genbio-policy-step", status: "completed", target: "HPC", startedAt: 100, finishedAt: 200 },
  ];
}

test("runBelongsToProject correlates generic schema-v2 operation runs", () => {
  assert.equal(runBelongsToProject({ operation: "project-alpha-build" }, "alpha"), true);
  assert.equal(runBelongsToProject({ operation: "project-alpha-post" }, "alpha"), true);
  assert.equal(runBelongsToProject({ operation: "project-alpha-build" }, "beta"), false);
  assert.equal(runBelongsToProject(null, "alpha"), false);
  assert.equal(runBelongsToProject({ operation: 7 }, "alpha"), false);
});

test("aggregateProjects groups plans + runs per project and never leaks wrapper bytes or logs", () => {
  const plans = [
    { planHash: "a".repeat(64), plan: { project: "alpha", operation: "post", bytesSha: "b".repeat(16) }, createdAt: 3000, status: "planned", sbatchText: "#!/bin/bash\n#SBATCH --job-name=alpha-post\nSECRET BODY" },
    { planHash: "c".repeat(64), plan: { project: "other", operation: "x", bytesSha: "d".repeat(16) }, createdAt: 4000, status: "planned" },
  ];
  const { projects_status, unattributed } = aggregateProjects({ projectSummaries: summaries(), plans, runs: runs() });
  // r5 (genbio-policy-step) belongs to no project -> surfaced as a count.
  assert.equal(unattributed, 1);
  const alpha = projects_status.find((entry) => entry.project === "alpha");
  assert.equal(alpha.valid, true);
  assert.equal(alpha.schema_version, 2);
  assert.deepEqual(alpha.plans.map((p) => p.operation), ["post"]);
  assert.deepEqual(Object.keys(alpha.plans[0]).sort(), ["bytes_sha256", "created_at", "operation", "plan_hash", "status"]);
  assert.equal(alpha.plans[0].plan_hash, "a".repeat(64));
  // No wrapper bytes, no raw plan sbatchText, no run logs/errors in the fold.
  assert.equal(JSON.stringify(alpha).includes("SECRET BODY"), false);
  assert.equal(JSON.stringify(alpha).includes("LOG SECRET"), false);
  assert.equal(JSON.stringify(alpha).includes("boom"), false);
  // Classification per operation (latest run wins):
  assert.deepEqual(alpha.suggested, ["post"], "never-run operation is the suggested next");
  assert.deepEqual(alpha.needs_review, ["equil"], "failed operation needs review, never re-suggested");
  assert.deepEqual(alpha.completed, ["build"], "completed operation is informational");
  assert.deepEqual(alpha.active, [], "project-alpha lane is not a declared operation");
  assert.deepEqual(alpha.run_counts, { active: 1, completed: 1, failed: 1, killed: 0, cancelled: 0, other: 0 });
  assert.equal(alpha.last_activity_at, 6000);
  const beta = projects_status.find((entry) => entry.project === "beta");
  assert.deepEqual(beta.active, ["run"]);
  assert.deepEqual(beta.suggested, []);
  const broken = projects_status.find((entry) => entry.project === "broken");
  assert.deepEqual(broken, { project: "broken", valid: false, error: "bad yaml at line 3" });
});

test("aggregateProjects bounds plans/runs and rejects malformed inputs", () => {
  const many = Array.from({ length: MAX_AGGREGATE_PLANS + 5 }, (_, index) => ({
    planHash: `${index}`.padStart(64, "f"),
    plan: { project: "alpha", operation: `op${index}`, bytesSha: "a".repeat(16) },
    createdAt: index,
    status: "planned",
  }));
  const manyRuns = Array.from({ length: MAX_AGGREGATE_RUNS + 5 }, (_, index) => ({
    runId: `r${index}`, operation: "project-alpha-build", status: "completed", target: "HPC", startedAt: index, finishedAt: index + 1,
  }));
  const { projects_status } = aggregateProjects({ projectSummaries: summaries(), plans: many, runs: manyRuns });
  const alpha = projects_status.find((entry) => entry.project === "alpha");
  assert.ok(alpha.plans.length <= MAX_AGGREGATE_PLANS);
  assert.ok(alpha.runs.length <= MAX_AGGREGATE_RUNS);
  assert.throws(() => aggregateProjects({ projectSummaries: "nope", plans: [], runs: [] }), /requires arrays/u);
  assert.throws(() => aggregateProjects({ projectSummaries: [], plans: [], runs: {} }), /requires arrays/u);
});

test("projection init/view expose the aggregate surface and the fold narrows it", () => {
  let state = projectionInit();
  assert.deepEqual(state, { calls: {}, envelope: null, runs: [], projects: [], projects_status: [], workflows: [] });
  // A genbio_projects_status result carries rich projects + projects_status.
  const status = {
    envelope: null,
    runs: [],
    projects: [
      { project: "alpha", schema_version: 2, valid: true, operations: [{ name: "build", form: "recipe", cpus: 4, gpus: 1, concurrency: 1 }] },
      { project: "broken", valid: false, error: "bad" },
    ],
    projects_status: [
      {
        project: "alpha", valid: true, schema_version: 2,
        operations: [{ name: "build", form: "recipe", cpus: 4, gpus: 1, concurrency: 1 }],
        plans: [{ plan_hash: "a".repeat(64), operation: "build", status: "planned", created_at: 1, bytes_sha256: "b".repeat(16) }],
        run_counts: { active: 1, completed: 0, failed: 0, killed: 0, cancelled: 0, other: 0 },
        active: ["build"], suggested: [], needs_review: [], completed: [],
        last_activity_at: 5,
        // Stray fields must be stripped by the projection allowlist.
        sbatchText: "#!/bin/bash SECRET-BODY", runs_full: [{ stdout: "LOG" }],
      },
      { project: "broken", valid: false, error: "bad" },
    ],
    unattributed_runs: 3,
  };
  state = projectionApply(state, callEvent("c1", "genbio_projects_status", 1));
  state = projectionApply(state, resultEvent("c1", status, 2));
  assert.equal(state.projects[0].operations[0].name, "build", "rich operation object survives the tolerant project mirror");
  assert.equal(state.projects[0].operations[0].form, "recipe");
  const alpha = state.projects_status.find((entry) => entry.project === "alpha");
  assert.equal(alpha.suggested.length, 0);
  // Allowlist: unknown aggregate fields never cross the channel.
  assert.equal(Object.hasOwn(alpha, "sbatchText"), false);
  assert.equal(Object.hasOwn(alpha, "runs_full"), false);
  assert.equal(Object.hasOwn(alpha, "attributed"), false);
  assert.equal(Object.hasOwn(alpha, "last_activity_at"), true);
  assert.equal(JSON.stringify(state).includes("SECRET-BODY"), false);
  const view = projectionView(state);
  assert.deepEqual(Object.keys(view).sort(), ["envelope", "projects", "projects_status", "runs", "workflows"]);
  // An empty aggregate update clears stale projects/persists the empty mirror.
  state = projectionApply(state, callEvent("c2", "genbio_projects_status", 3));
  state = projectionApply(state, resultEvent("c2", { ...status, projects: [], projects_status: [] }, 4));
  const cleared = projectionView(state);
  assert.deepEqual(cleared.projects, []);
  assert.deepEqual(cleared.projects_status, []);
});

test("projection retains projects_status across unrelated genbio results", () => {
  let state = projectionInit();
  const status = { envelope: null, runs: [], projects: [], projects_status: [{ project: "alpha", valid: true, operations: [] }] };
  state = projectionApply(state, callEvent("c1", "genbio_projects_status", 1));
  state = projectionApply(state, resultEvent("c1", status, 2));
  assert.equal(state.projects_status.length, 1);
  // A later genbio_runs result (no projects_status) leaves it untouched.
  state = projectionApply(state, callEvent("c2", "genbio_runs", 3));
  state = projectionApply(state, resultEvent("c2", { envelope: null, runs: [{ runId: "x", status: "completed" }] }, 4));
  assert.equal(state.projects_status.length, 1);
});