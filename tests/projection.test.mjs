import assert from "node:assert/strict";

const bundle = await import("../lib/index.js");
const { projectionKey, projectionApply, projectionInit, projectionView } = bundle;

assert.equal(projectionKey, "genbio/remote");

let state = projectionInit();
assert.deepEqual(state, { calls: {}, envelope: null, runs: [], projects: [], projects_status: [], workflows: [] });

// A genbio tool call is paired by callId.
state = projectionApply(state, { type: "tool/call", seq: 1, time: 0, data: { callId: "c1", name: "genbio_set_envelope", arguments: "{}" } });
assert.deepEqual(state.calls, { c1: "genbio_set_envelope" });

// Unrelated events are no-ops (same state reference).
assert.equal(projectionApply(state, { type: "user/message", seq: 2, time: 0, data: {} }), state);

// A non-genbio tool call is ignored.
assert.equal(projectionApply(state, { type: "tool/call", seq: 3, time: 0, data: { callId: "c2", name: "bash", arguments: "{}" } }), state);

// Fixtures shaped like REAL plugin output: the envelope carries internal
// bookkeeping (usedCpus/usedGpus/policyHash) and the run record carries full
// logs, an envelope clone, a finalization payload, and memory state. The fold
// must narrow both to the client-needed fields (audit 2026-08-25 P2-F2): the
// projection is broadcast to every client and seeded into session history.
const envelope = { target: "genbioh100", node: "genbioh100", partition: null, workloadClass: "gpu-render", maxCpus: 16, maxGpus: 1, memGb: 32, concurrency: 1, usedCpus: 0, usedGpus: 0, policyHash: "h" };
const run = {
  runId: "genbioh100-1700000000000", jobId: "genbio-genbioh100-1", slurmJobId: null, target: "genbioh100", operation: "preflight-smoke", status: "running", startedAt: 1, finishedAt: null,
  node: "genbioh100", partition: null, pid: null, elapsedMs: 4, lastObservedAt: 4,
  stdout: "LOG LINE ".repeat(4000), stderr: "ERR LINE ".repeat(4000), error: "boom",
  resources: { cpus: 16, gpus: 1, memGb: 32, concurrency: 1 },
  policyHash: "h", envelope: { ...envelope }, remoteGrants: [{ target: "HPC", root: "/data01/x", mode: "rw" }],
  finalization: { recordId: "genbio-x", hash: "d", text: "# full curated memory record", finalizedAt: 5 },
  memory: { status: "ready", error: null, openVikingSessionId: "s", traceId: "t" },
};
const resultEvent = (callId, status, seq) => ({
  type: "tool/result", seq, time: 0,
  data: { message: { source: { kind: "tool", callId }, content: [{ type: "tool-result", toolCallId: callId, content: [{ type: "text", text: JSON.stringify({ ok: true, status }) }] }] } },
});

// The paired genbio result mirrors the envelope and runs (narrowed), and drops the call pairing.
state = projectionApply(state, resultEvent("c1", { envelope, runs: [run] }, 4));
assert.equal(Object.hasOwn(state.calls, "c1"), false);
// Regression (P2-F2): envelope internals are omitted from the mirror.
assert.deepEqual(state.envelope, { target: "genbioh100", node: "genbioh100", partition: null, workloadClass: "gpu-render", maxCpus: 16, maxGpus: 1, memGb: 32, concurrency: 1 });
// Regression (P2-F2): run records carry ONLY the client-needed fields — no
// logs, no envelope clone, no finalization/memory/policy internals.
assert.deepEqual(Object.keys(state.runs[0]).sort(), ["finishedAt", "jobId", "node", "operation", "partition", "resources", "runId", "slurmJobId", "startedAt", "status", "target"]);
for (const omitted of ["stdout", "stderr", "error", "envelope", "finalization", "memory", "policyHash", "remoteGrants", "pid", "elapsedMs", "lastObservedAt"]) {
  assert.equal(Object.hasOwn(state.runs[0], omitted), false, `projected run must omit ${omitted}`);
}
// The client-needed fields survive: jobId (live-job matching) + resource counts (usage math).
assert.equal(state.runs[0].jobId, "genbio-genbioh100-1");
assert.equal(state.runs[0].resources.cpus, 16);
assert.equal(state.runs[0].resources.gpus, 1);
assert.equal(JSON.stringify(state).includes("LOG LINE"), false, "mirrored state must not contain run logs");

// A later result updates the mirror (status change flows through).
const updatedRun = { ...run, status: "completed", finishedAt: 9 };
state = projectionApply(state, { type: "tool/call", seq: 5, time: 0, data: { callId: "c9", name: "genbio_monitor", arguments: "{}" } });
state = projectionApply(state, resultEvent("c9", { envelope, runs: [updatedRun] }, 7));
assert.equal(state.runs[0].status, "completed");

// Regression (P2-F2): a committed state update with an EMPTY runs array is
// preserved — it is the plugin's own session-state mirror, so it must clear
// the mirrored runs instead of being skipped (stale-run trap).
state = projectionApply(state, { type: "tool/call", seq: 8, time: 0, data: { callId: "c11", name: "genbio_runs", arguments: "{}" } });
state = projectionApply(state, resultEvent("c11", { envelope, runs: [] }, 9));
assert.deepEqual(state.runs, []);
assert.deepEqual(state.envelope, { target: "genbioh100", node: "genbioh100", partition: null, workloadClass: "gpu-render", maxCpus: 16, maxGpus: 1, memGb: 32, concurrency: 1 });

// Results for unknown callIds and non-JSON results are no-ops.
assert.equal(projectionApply(state, { type: "tool/result", seq: 10, time: 0, data: { message: { source: { kind: "tool", callId: "zzz" }, content: [] } } }), state);
state = projectionApply(state, { type: "tool/call", seq: 11, time: 0, data: { callId: "c10", name: "genbio_policy_status", arguments: "{}" } });
state = projectionApply(state, {
  type: "tool/result", seq: 12, time: 0,
  data: { message: { source: { kind: "tool", callId: "c10" }, content: [{ type: "tool-result", toolCallId: "c10", content: [{ type: "text", text: "not json" }] }] } },
});
assert.equal(Object.hasOwn(state.calls, "c10"), false);

// Project discovery is projected narrowly: never manifest paths, errors, wrapper
// bytes, parameters, full plan hashes, or execution tokens.
state = projectionApply(state, { type: "tool/call", seq: 13, time: 0, data: { callId: "p1", name: "genbio_projects", arguments: "{}" } });
state = projectionApply(state, resultEvent("p1", { projects: [{ project: "demo", valid: true, schema_version: 2, operations: ["run"], error: "secret", remote_root: "/secret" }] }, 14));
assert.deepEqual(state.projects, [{ project: "demo", valid: true, schemaVersion: 2, operations: ["run"] }]);
state = projectionApply(state, { type: "tool/call", seq: 15, time: 0, data: { callId: "p2", name: "genbio_project_status", arguments: "{}" } });
state = projectionApply(state, resultEvent("p2", { project: "demo", plans: [{ plan_hash: "a".repeat(64), operation: "run", status: "planned", bytes_sha256: "b".repeat(64), wrapper: "secret" }] }, 16));
assert.equal(state.projects[0].plans[0].planHash, "a".repeat(12));
assert.equal(JSON.stringify(state.projects).includes("secret"), false);
assert.equal(JSON.stringify(state.projects).includes("b".repeat(64)), false);

// The view exposes only narrowed client state.
const view = projectionView(state);
assert.deepEqual(Object.keys(view).sort(), ["envelope", "projects", "projects_status", "runs", "workflows"]);
assert.equal(view.envelope.maxCpus, 16);
assert.deepEqual(view.runs, []);
assert.deepEqual(view.projects_status, []);

// Aggregate project status (genbio_projects_status) mirrors NESTED array
// elements through field-level narrowing too: a future aggregate that adds
// wrapper bytes, parameters, full hashes, or log text to a plan/run/operation
// record must never widen the broadcast surface.
state = projectionApply(state, { type: "tool/call", seq: 17, time: 0, data: { callId: "p3", name: "genbio_projects_status", arguments: "{}" } });
state = projectionApply(state, resultEvent("p3", { projects_status: [{
  project: "demo", valid: true, schema_version: 2,
  operations: [{ name: "run", form: "recipe", cpus: 4, gpus: 1, concurrency: 1, binary: "/bin/sh", args: ["--danger"] }],
  plans: [{ plan_hash: "a".repeat(64), operation: "run", status: "planned", created_at: 1, wrapper: "secret-wrapper", parameters: { count: 7 }, note: "password=hunter2" }],
  runs: [{ run_id: "r1", operation: "run", status: "completed", target: "HPC", started_at: 1, finished_at: 2, stdout: "SECRET-LOG", error: "boom" }],
  run_counts: { active: 1, completed: 2, nested: { secret: "x" }, bad: -1, failed: 0 },
} ] }, 18));
const n = JSON.stringify(state.projects_status);
assert.equal(n.includes("secret-wrapper"), false, "nested plan wrapper is never projected");
assert.equal(n.includes("hunter2"), false, "nested secret-like plan fields are never projected");
assert.equal(n.includes("--danger"), false, "nested operation args/binary are never projected");
assert.equal(n.includes("SECRET-LOG"), false, "nested run logs are never projected");
assert.equal(n.includes("a".repeat(64)), false, "full plan hashes are never projected");
assert.equal(n.includes('"secret":"x"'), false, "run_counts nested objects are never projected");
assert.deepEqual(state.projects_status[0].run_counts, { active: 1, completed: 2, failed: 0 }, "run_counts mirrors only safe non-negative integer members");
assert.deepEqual(state.projects_status[0].plans[0], { operation: "run", status: "planned", plan_hash: "a".repeat(12), created_at: 1 });
assert.deepEqual(state.projects_status[0].operations[0], { name: "run", form: "recipe", cpus: 4, gpus: 1, concurrency: 1 });
assert.deepEqual(state.projects_status[0].runs[0], { run_id: "r1", operation: "run", status: "completed", target: "HPC", started_at: 1, finished_at: 2 });

state = projectionApply(state, { type: "tool/call", seq: 19, time: 0, data: { callId: "w1", name: "genbio_workflow_status", arguments: "{}" } });
state = projectionApply(state, resultEvent("w1", { workflow_run: { workflow_run_id: "wr-1", workflow: "pipeline", plan_hash: "f".repeat(64), status: "running", counts: { ready: 0, active: 1, completed: 1, secret: { raw: true } }, nodes: [{ node_id: "run", project: "demo", operation: "run", status: "running", job_id: "1234", slurm_state: "RUNNING", depends_on: ["prepare"], parameters: { secret: true }, wrapper: "raw" }] } }, 20));
assert.deepEqual(state.workflows[0], { workflow_run_id: "wr-1", workflow: "pipeline", status: "running", nodes: [{ node_id: "run", project: "demo", operation: "run", status: "running", job_id: "1234", slurm_state: "RUNNING", depends_on: ["prepare"] }], counts: { ready: 0, active: 1, completed: 1 } });
assert.equal(JSON.stringify(state.workflows).includes("secret"), false);
assert.equal(JSON.stringify(state.workflows).includes("raw"), false);

console.log("projection fold passed: narrowed envelope/runs/projects/projects_status (incl. nested fields), privacy, empty updates, and no-op discipline");
