// Schema-v2-only workflow/DAG parsing and immutable local planning.
import assert from "node:assert/strict";
import test from "node:test";

import { parseWorkflow, planWorkflow, workflowPlanHashOf, MAX_WORKFLOW_NODES, MAX_WORKFLOW_PARAMETERS_BYTES } from "../lib/workflow.js";

function definition(overrides = {}) {
  return {
    schema_version: 2,
    workflow: "pipeline",
    nodes: [
      { id: "prepare", project: "fixture-project", operation: "prepare", parameters: { count: 2 } },
      { id: "run", project: "fixture-project", operation: "run", depends_on: ["prepare"], parameters: { mode: "safe" } },
      { id: "collect", project: "fixture-project", operation: "collect", depends_on: ["run"] },
    ],
    ...overrides,
  };
}

const operationHashes = Object.freeze({
  prepare: "a".repeat(64),
  run: "b".repeat(64),
  collect: "c".repeat(64),
});

function operationResolver(calls = []) {
  return async (input) => {
    calls.push(input);
    return {
      planHash: operationHashes[input.operation] ?? "d".repeat(64),
      resources: { cpus: input.operation === "run" ? 4 : 1, gpus: input.operation === "run" ? 1 : 0, concurrency: 1 },
      origin: `fixture:${input.project}`,
    };
  };
}

test("schema v2 parses a frozen DAG with parameters and derived pair identities", () => {
  const workflow = parseWorkflow(definition());
  assert.equal(workflow.schema, "genbio-workflow/2");
  assert.equal(workflow.name, "pipeline");
  assert.equal(Object.isFrozen(workflow), true);
  assert.equal(Object.isFrozen(workflow.nodes[0]), true);
  assert.equal(Object.isFrozen(workflow.nodes[0].parameters), true);
  assert.deepEqual(workflow.nodes[0].parameters, { count: 2 });
  assert.deepEqual(workflow.nodes[2].parameters, {});
  assert.equal(workflow.nodes[1].pairKey, "fixture-project/run");
  assert.deepEqual(workflow.pairs, {
    "fixture-project/prepare": "prepare",
    "fixture-project/run": "run",
    "fixture-project/collect": "collect",
  });
});

test("schema v1 workflows are rejected explicitly", () => {
  assert.throws(() => parseWorkflow({
    schema_version: 1,
    workflow: "legacy",
    nodes: [{ id: "run", project: "fixture-project", operation: "run" }],
  }), /workflow schema_version 2 is required/u);
});

test("planning resolves every operation and marks only root nodes ready", async () => {
  const workflow = parseWorkflow(definition());
  const calls = [];
  const plan = await planWorkflow(workflow, { resolveOperation: operationResolver(calls) });

  assert.equal(plan.schema, "genbio-workflow-plan/2");
  assert.deepEqual(plan.ready, ["prepare"]);
  assert.deepEqual(plan.blocked, ["run", "collect"]);
  assert.deepEqual(plan.completed, []);
  assert.deepEqual(plan.nodes.find((node) => node.id === "run").waitingOn, ["prepare"]);
  assert.deepEqual(plan.nodes.find((node) => node.id === "collect").waitingOn, ["run"]);
  assert.equal(plan.nodes[0].operation_plan_hash, operationHashes.prepare);
  assert.deepEqual(plan.nodes[1].resources, { cpus: 4, gpus: 1, concurrency: 1 });
  assert.equal(plan.nodes[1].origin, "fixture:fixture-project");
  assert.deepEqual(calls, [
    { project: "fixture-project", operation: "prepare", parameters: { count: 2 }, nodeId: "prepare" },
    { project: "fixture-project", operation: "run", parameters: { mode: "safe" }, nodeId: "run" },
    { project: "fixture-project", operation: "collect", parameters: {}, nodeId: "collect" },
  ]);
  assert.equal(Object.isFrozen(plan), true);
});

test("schema-v2 operation resolution and workflow hashing are canonical", async () => {
  const workflow = parseWorkflow(definition());
  const first = await planWorkflow(workflow, { resolveOperation: operationResolver() });
  const reordered = await planWorkflow(workflow, {
    resolveOperation: async (input) => ({
      origin: `fixture:${input.project}`,
      resources: { concurrency: 1, gpus: input.operation === "run" ? 1 : 0, cpus: input.operation === "run" ? 4 : 1 },
      plan_hash: operationHashes[input.operation],
    }),
  });
  assert.match(first.workflow_plan_hash, /^[a-f0-9]{64}$/u);
  assert.equal(reordered.workflow_plan_hash, first.workflow_plan_hash, "mapping insertion order must not affect the plan hash");

  const changedParameters = parseWorkflow(definition({
    nodes: [
      { id: "prepare", project: "fixture-project", operation: "prepare", parameters: { count: 3 } },
      { id: "run", project: "fixture-project", operation: "run", depends_on: ["prepare"], parameters: { mode: "safe" } },
      { id: "collect", project: "fixture-project", operation: "collect", depends_on: ["run"] },
    ],
  }));
  const changed = await planWorkflow(changedParameters, { resolveOperation: operationResolver() });
  assert.notEqual(changed.workflow_plan_hash, first.workflow_plan_hash, "node parameters are part of immutable workflow identity");
  assert.notEqual(workflowPlanHashOf({ value: 1 }), workflowPlanHashOf({ value: 2 }));
});

test("duplicate pairs, cycles, self-dependencies, and unknown dependencies fail closed", () => {
  assert.throws(() => parseWorkflow({
    schema_version: 2,
    workflow: "duplicate-pair",
    nodes: [
      { id: "one", project: "fixture-project", operation: "run" },
      { id: "two", project: "fixture-project", operation: "run" },
    ],
  }), /reuses the \(project, operation\) pair/u);
  assert.throws(() => parseWorkflow({
    schema_version: 2,
    workflow: "cycle",
    nodes: [
      { id: "a", project: "project-a", operation: "run", depends_on: ["b"] },
      { id: "b", project: "project-b", operation: "run", depends_on: ["a"] },
    ],
  }), /dependency cycle involving: a, b/u);
  assert.throws(() => parseWorkflow({
    schema_version: 2,
    workflow: "self",
    nodes: [{ id: "a", project: "fixture-project", operation: "run", depends_on: ["a"] }],
  }), /depends on itself/u);
  assert.throws(() => parseWorkflow({
    schema_version: 2,
    workflow: "unknown-dependency",
    nodes: [{ id: "a", project: "fixture-project", operation: "run", depends_on: ["missing"] }],
  }), /depends on unknown node: missing/u);
});

test("schema-v2 strictness rejects unknown fields, unsafe names, duplicates, and bounds", () => {
  assert.throws(() => parseWorkflow(definition({ extra: true })), /unknown field extra/u);
  assert.throws(() => parseWorkflow(definition({ schema_version: 3 })), /workflow schema_version 2 is required/u);
  assert.throws(() => parseWorkflow({ schema_version: 2, workflow: "empty", nodes: [] }), /1..32 nodes/u);
  const many = { schema_version: 2, workflow: "large", nodes: Array.from({ length: MAX_WORKFLOW_NODES + 1 }, (_, index) => ({ id: `n${index}`, project: "fixture-project", operation: `op${index}` })) };
  assert.throws(() => parseWorkflow(many), /1..32 nodes/u);
  assert.throws(() => parseWorkflow({
    schema_version: 2,
    workflow: "duplicate-id",
    nodes: [
      { id: "a", project: "project-a", operation: "run" },
      { id: "a", project: "project-b", operation: "run" },
    ],
  }), /duplicate workflow node id/u);
  assert.throws(() => parseWorkflow({ schema_version: 2, workflow: "bad-name", nodes: [{ id: "a", project: "Bad_Project", operation: "run" }] }), /kebab-case/u);
  assert.throws(() => parseWorkflow({
    schema_version: 2,
    workflow: "duplicate-dependency",
    nodes: [
      { id: "a", project: "project-a", operation: "run" },
      { id: "b", project: "project-b", operation: "run", depends_on: ["a", "a"] },
    ],
  }), /twice/u);
  assert.throws(() => parseWorkflow(null), /must be a mapping/u);
});

test("schema-v2 parameter values are bounded plain JSON", () => {
  const workflow = parseWorkflow({
    schema_version: 2,
    workflow: "parameterized",
    nodes: [{ id: "run", project: "fixture-project", operation: "run", parameters: { seed: 7, flags: ["fast", true], nested: { mode: "safe" } } }],
  });
  assert.deepEqual(workflow.nodes[0].parameters, { seed: 7, flags: ["fast", true], nested: { mode: "safe" } });
  assert.equal(Object.isFrozen(workflow.nodes[0].parameters.nested), true);
  assert.throws(() => parseWorkflow({ schema_version: 2, workflow: "bad", nodes: [{ id: "run", project: "fixture-project", operation: "run", parameters: { float: 1.5 } }] }), /safe integers/u);
  assert.throws(() => parseWorkflow({ schema_version: 2, workflow: "bad", nodes: [{ id: "run", project: "fixture-project", operation: "run", parameters: { Bad_Key: 1 } }] }), /unsafe key/u);
  assert.throws(() => parseWorkflow({ schema_version: 2, workflow: "large", nodes: [{ id: "run", project: "fixture-project", operation: "run", parameters: { text: "x".repeat(MAX_WORKFLOW_PARAMETERS_BYTES + 1) } }] }), /exceeds/u);
});

test("schema-v2 resolution fails closed on missing or malformed identity metadata", async () => {
  const workflow = parseWorkflow({ schema_version: 2, workflow: "resolved", nodes: [{ id: "run", project: "fixture-project", operation: "run" }] });
  await assert.rejects(planWorkflow(workflow, {}), /requires.*operation resolver|requires resolveOperation/u);
  await assert.rejects(planWorkflow(workflow, { resolveOperation: async () => ({ planHash: "short", resources: {}, origin: "fixture" }) }), /64-hex/u);
  await assert.rejects(planWorkflow(workflow, { resolveOperation: async () => ({ planHash: "a".repeat(64), resources: { cpus: -1 }, origin: "fixture" }) }), /non-negative/u);
  await assert.rejects(planWorkflow(workflow, { resolveOperation: async () => ({ planHash: "a".repeat(64), resources: {}, origin: "fixture", raw: "forbidden" }) }), /unknown field raw/u);
});

test("planning is pure and never mutates the parsed workflow", async () => {
  const workflow = parseWorkflow(definition());
  const before = JSON.stringify(workflow);
  await planWorkflow(workflow, { resolveOperation: operationResolver() });
  assert.equal(JSON.stringify(workflow), before);
});
