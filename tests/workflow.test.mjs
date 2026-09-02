// Phase 2: conservative workflow/DAG schema + planner. Parse fails closed on
// cycles, unknown dependencies, duplicate pairs, and unsafe input. The
// planner only MARKS ready nodes; it has no execution/submission surface.
import assert from "node:assert/strict";
import test from "node:test";

import { parseWorkflow, planWorkflow, MAX_WORKFLOW_NODES } from "../lib/workflow.js";

function definition(overrides = {}) {
  return {
    schema_version: 1,
    workflow: "pipeline",
    nodes: [
      { id: "prepare", project: "demo", operation: "prepare" },
      { id: "run", project: "demo", operation: "run", depends_on: ["prepare"] },
      { id: "fetch", project: "demo", operation: "fetch", depends_on: ["run"] },
    ],
    ...overrides,
  };
}

const manifests = { demo: { jobs: { prepare: {}, run: {}, fetch: {} } } };
const resolveProject = async (project) => {
  const manifest = manifests[project];
  if (!manifest) throw new Error(`unknown Genbio project: ${project}`);
  return manifest;
};

test("a valid chain parses with derived pairKeys and frozen nodes", () => {
  const workflow = parseWorkflow(definition());
  assert.equal(workflow.name, "pipeline");
  assert.equal(Object.isFrozen(workflow), true);
  assert.equal(Object.isFrozen(workflow.nodes[0]), true);
  assert.equal(workflow.nodes[0].pairKey, "demo/prepare", "pairKey is derived as project/operation");
  assert.equal(workflow.nodes[1].pairKey, "demo/run");
  assert.deepEqual(workflow.pairs, { "demo/prepare": "prepare", "demo/run": "run", "demo/fetch": "fetch" });
});

test("the planner marks only dependency-ready nodes and never advances anything", async () => {
  const workflow = parseWorkflow(definition());
  const fresh = await planWorkflow(workflow, { resolveProject });
  assert.deepEqual(fresh.ready, ["prepare"], "only the node with no dependencies is ready");
  assert.deepEqual(fresh.blocked, ["run", "fetch"]);
  assert.deepEqual(fresh.nodes.find((node) => node.id === "run").waitingOn, ["prepare"]);
  assert.deepEqual(fresh.nodes.find((node) => node.id === "fetch").waitingOn, ["run"]);
  assert.equal(Object.isFrozen(fresh), true);

  const mid = await planWorkflow(workflow, { resolveProject, completed: ["prepare"] });
  assert.deepEqual(mid.completed, ["prepare"]);
  assert.deepEqual(mid.ready, ["run"], "completing the dependency unlocks exactly one node");
  assert.deepEqual(mid.blocked, ["fetch"]);

  const all = await planWorkflow(workflow, { resolveProject, completed: ["prepare", "run", "fetch"] });
  assert.deepEqual(all.ready, [], "nothing is ready when the whole workflow is complete");
  assert.deepEqual(all.completed, ["prepare", "run", "fetch"]);

  // Diamond: two independent branches, one join — the join is ready only
  // when BOTH branches are complete.
  const diamond = parseWorkflow({
    schema_version: 1,
    workflow: "diamond",
    nodes: [
      { id: "a", project: "alpha", operation: "run" },
      { id: "b", project: "beta", operation: "run" },
      { id: "join", project: "gamma", operation: "run", depends_on: ["a", "b"] },
    ],
  });
  const diamondManifests = { alpha: { jobs: { run: {} } }, beta: { jobs: { run: {} } }, gamma: { jobs: { run: {} } } };
  const diamondResolve = async (project) => {
    const manifest = diamondManifests[project];
    if (!manifest) throw new Error(`unknown Genbio project: ${project}`);
    return manifest;
  };
  const d1 = await planWorkflow(diamond, { resolveProject: diamondResolve, completed: ["a"] });
  assert.deepEqual(d1.ready, ["b"], "completing one branch does not release the join");
  assert.deepEqual(d1.blocked, ["join"]);
  assert.deepEqual(d1.nodes.find((node) => node.id === "join").waitingOn, ["b"]);
  const d2 = await planWorkflow(diamond, { resolveProject: diamondResolve, completed: ["a", "b"] });
  assert.deepEqual(d2.ready, ["join"], "the join is ready only when both branches are complete");
  assert.deepEqual(d2.blocked, []);
});

test("duplicate (project, operation) pairs are rejected (pair lock identity)", () => {
  assert.throws(() => parseWorkflow({
    schema_version: 1,
    workflow: "dup",
    nodes: [
      { id: "one", project: "demo", operation: "run" },
      { id: "two", project: "demo", operation: "run" },
    ],
  }), /reuses the \(project, operation\) pair/u);
  // The same operation in a DIFFERENT project is a different pair: allowed.
  const cross = parseWorkflow({
    schema_version: 1,
    workflow: "cross",
    nodes: [
      { id: "one", project: "alpha", operation: "run" },
      { id: "two", project: "beta", operation: "run" },
    ],
  });
  assert.equal(cross.nodes.length, 2);
});

test("cycles, self-dependencies, and unknown dependencies are rejected", () => {
  assert.throws(() => parseWorkflow({
    schema_version: 1,
    workflow: "cycle",
    nodes: [
      { id: "a", project: "demo", operation: "prepare", depends_on: ["b"] },
      { id: "b", project: "demo", operation: "run", depends_on: ["a"] },
      { id: "c", project: "demo", operation: "fetch" },
    ],
  }), /dependency cycle involving: a, b/u);
  assert.throws(() => parseWorkflow({
    schema_version: 1,
    workflow: "self",
    nodes: [{ id: "a", project: "demo", operation: "run", depends_on: ["a"] }],
  }), /depends on itself/u);
  assert.throws(() => parseWorkflow({
    schema_version: 1,
    workflow: "unknown-dep",
    nodes: [{ id: "a", project: "demo", operation: "run", depends_on: ["ghost"] }],
  }), /depends on unknown node: ghost/u);
  // A three-node cycle with distinct pairs (plus an acyclic side node) is
  // still caught; the side node is not blamed.
  assert.throws(() => parseWorkflow({
    schema_version: 1,
    workflow: "big-cycle",
    nodes: [
      { id: "a", project: "demo", operation: "prepare", depends_on: ["c"] },
      { id: "b", project: "demo", operation: "run", depends_on: ["a"] },
      { id: "c", project: "alpha", operation: "run", depends_on: ["b"] },
      { id: "d", project: "beta", operation: "run" },
    ],
  }), /dependency cycle involving: a, b, c/u);
});

test("schema strictness: unknown fields, unsafe names, duplicates, bounds", () => {
  assert.throws(() => parseWorkflow(definition({ extra: 1 })), /unknown field extra/u);
  assert.throws(() => parseWorkflow(definition({ schema_version: 2 })), /schema_version must be 1/u);
  assert.throws(() => parseWorkflow(definition({ name: "pipeline" })), /unknown field name/u);
  assert.throws(() => parseWorkflow({ schema_version: 1, workflow: "pipeline", nodes: [] }), /1..32 nodes/u);
  const many = { schema_version: 1, workflow: "big", nodes: Array.from({ length: MAX_WORKFLOW_NODES + 1 }, (_, index) => ({ id: `n${index}`, project: "demo", operation: `op${index}` })) };
  assert.throws(() => parseWorkflow(many), /1..32 nodes/u);
  assert.throws(() => parseWorkflow({
    schema_version: 1,
    workflow: "dup-id",
    nodes: [
      { id: "a", project: "alpha", operation: "run" },
      { id: "a", project: "beta", operation: "run" },
    ],
  }), /duplicate workflow node id/u);
  assert.throws(() => parseWorkflow({
    schema_version: 1,
    workflow: "bad-name",
    nodes: [{ id: "a", project: "Bad_Project", operation: "run" }],
  }), /kebab-case/u);
  assert.throws(() => parseWorkflow({
    schema_version: 1,
    workflow: "dup-dep",
    nodes: [
      { id: "a", project: "alpha", operation: "run" },
      { id: "b", project: "beta", operation: "run", depends_on: ["a", "a"] },
    ],
  }), /twice/u);
  assert.throws(() => parseWorkflow(null), /must be a mapping/u);
});

test("the planner verifies every node's operation exists in its manifest", async () => {
  const workflow = parseWorkflow({
    schema_version: 1,
    workflow: "missing-op",
    nodes: [
      { id: "ok", project: "demo", operation: "run" },
      { id: "bad", project: "demo", operation: "does-not-exist" },
    ],
  });
  await assert.rejects(planWorkflow(workflow, { resolveProject }), /has no operation does-not-exist/u);
  const unknownProject = parseWorkflow({
    schema_version: 1,
    workflow: "missing-project",
    nodes: [{ id: "a", project: "ghost", operation: "run" }],
  });
  await assert.rejects(planWorkflow(unknownProject, { resolveProject }), /project ghost unavailable/u);
  await assert.rejects(planWorkflow(parseWorkflow(definition()), { resolveProject, completed: ["ghost"] }), /unknown workflow node: ghost/u);
  await assert.rejects(planWorkflow(parseWorkflow(definition()), {}), /requires a manifest reader/u);
});

test("planning is pure: no mutation of the workflow, no side effects", async () => {
  const workflow = parseWorkflow(definition());
  const before = JSON.stringify(workflow);
  await planWorkflow(workflow, { resolveProject });
  await planWorkflow(workflow, { resolveProject, completed: ["prepare"] });
  assert.equal(JSON.stringify(workflow), before, "the parsed workflow is never mutated by planning");
});
