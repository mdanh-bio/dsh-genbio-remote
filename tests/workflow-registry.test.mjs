import assert from "node:assert/strict";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  createWorkflowRegistry,
  canTransitionWorkflowNodeStatus,
  canTransitionWorkflowStatus,
  MAX_WORKFLOW_RECORDS,
} from "../lib/workflow-registry.js";

const SESSION = "workflow-session";
const H1 = "a".repeat(64);
const H2 = "b".repeat(64);

async function fixture(t) {
  const dir = await mkdtemp(join(tmpdir(), "workflow-registry-"));
  t.after(() => rm(dir, { recursive: true, force: true }));
  return { dir, reg: createWorkflowRegistry(dir) };
}
function input(overrides = {}) {
  return {
    workflowRunId: "wr-one",
    workflow: "pipeline",
    workflowPlanHash: H1,
    nodes: [
      { nodeId: "prepare", project: "demo", operation: "prepare", operationPlanHash: H1, status: "ready" },
      { nodeId: "run", project: "demo", operation: "run", operationPlanHash: H2, status: "blocked" },
    ],
    ...overrides,
  };
}

test("records persist atomically and expose only the allowlisted lifecycle shape", async (t) => {
  const { dir, reg } = await fixture(t);
  const stored = await reg.record(SESSION, input());
  assert.equal(stored.status, "planned");
  assert.equal(stored.nodes[0].status, "ready");
  const disk = JSON.parse(await readFile(join(dir, SESSION, "workflows.json"), "utf8"));
  assert.equal(disk.schema, "genbio-workflow-registry/1");
  assert.deepEqual(Object.keys(disk.records[0]).sort(), ["createdAt", "finishedAt", "nodeRecords", "nodes", "status", "updatedAt", "workflow", "workflowOrigin", "workflowPath", "workflowPlan", "workflowPlanHash", "workflowRunId", "workflowSha", "workspace"].sort());
  const text = JSON.stringify(disk);
  for (const forbidden of ["stdout", "stderr", "token", "parameters", "/remote/path"]) assert.equal(text.includes(forbidden), false);
  assert.deepEqual((await createWorkflowRegistry(dir).list(SESSION))[0], stored);
});

test("scheduler identity and bounded evidence fields persist without widening to logs", async (t) => {
  const { reg } = await fixture(t);
  await reg.record(SESSION, input());
  await reg.updateWorkflow(SESSION, "wr-one", { status: "running" });
  await reg.updateNode(SESSION, "wr-one", "prepare", { status: "submitting" });
  const submitted = await reg.updateNode(SESSION, "wr-one", "prepare", { status: "submitted", slurmJobId: "12345", slurmState: "PENDING", evidence: "scheduler" });
  assert.equal(submitted.slurmJobId, "12345");
  assert.equal(submitted.slurmState, "PENDING");
  await assert.rejects(reg.updateNode(SESSION, "wr-one", "prepare", { status: "running", slurmJobId: "not-a-job" }), /exact numeric/u);
  await assert.rejects(reg.updateNode(SESSION, "wr-one", "prepare", { status: "running", evidence: "x".repeat(65) }), /bounded string/u);
});

test("unknown fields including logs paths tokens and params fail closed", async (t) => {
  const { reg } = await fixture(t);
  for (const field of ["stdout", "path", "token", "parameters"]) await assert.rejects(reg.record(SESSION, { ...input({ workflowRunId: `wr-${field}` }), [field]: "secret" }), new RegExp(`unknown field.*${field}`, "u"));
  await assert.rejects(reg.record(SESSION, input({ nodes: [{ ...input().nodes[0], parameters: { seed: 1 } }] })), /unknown field.*parameters/u);
  await assert.rejects(reg.record("../evil", input()), /unsafe workflow registry session id/u);
  assert.throws(() => createWorkflowRegistry("relative/path"), /absolute/u);
});

test("strict node and workflow transitions enforce terminal immutability", async (t) => {
  const { reg } = await fixture(t);
  await reg.record(SESSION, input());
  assert.equal(canTransitionWorkflowStatus("planned", "running"), true);
  assert.equal(canTransitionWorkflowStatus("completed", "running"), false);
  assert.equal(canTransitionWorkflowNodeStatus("ready", "submitting"), true);
  assert.equal(canTransitionWorkflowNodeStatus("completed", "running"), false);

  await reg.updateWorkflow(SESSION, "wr-one", { status: "running" });
  await reg.updateNode(SESSION, "wr-one", "prepare", { status: "submitting" });
  await reg.updateNode(SESSION, "wr-one", "prepare", { status: "submitted" });
  await reg.updateNode(SESSION, "wr-one", "prepare", { status: "running" });
  await reg.updateNode(SESSION, "wr-one", "prepare", { status: "completed" });
  await reg.updateNode(SESSION, "wr-one", "run", { status: "ready" });
  await reg.updateNode(SESSION, "wr-one", "run", { status: "cancelled" });
  const terminal = await reg.updateWorkflow(SESSION, "wr-one", { status: "completed" });
  assert.equal(terminal.status, "completed");
  assert.ok(terminal.finishedAt);
  await assert.rejects(reg.updateWorkflow(SESSION, "wr-one", { status: "failed" }), /terminal.*immutable/u);
  await assert.rejects(reg.updateNode(SESSION, "wr-one", "prepare", { status: "failed" }), /terminal.*immutable/u);
});

test("invalid transitions and premature terminal workflow fail closed", async (t) => {
  const { reg } = await fixture(t);
  await reg.record(SESSION, input());
  await assert.rejects(reg.updateNode(SESSION, "wr-one", "prepare", { status: "completed" }), /invalid workflow node transition ready -> completed/u);
  await reg.updateWorkflow(SESSION, "wr-one", { status: "running" });
  await assert.rejects(reg.updateWorkflow(SESSION, "wr-one", { status: "completed" }), /non-terminal/u);
  await assert.rejects(reg.updateWorkflow(SESSION, "wr-one", { raw: "log" }), /unknown field.*raw/u);
  await assert.rejects(reg.updateNode(SESSION, "wr-one", "prepare", { token: "x" }), /unknown field.*token/u);
});

test("restart converts ambiguous workflow and node states to reconciling", async (t) => {
  const { dir, reg } = await fixture(t);
  await reg.record(SESSION, input());
  await reg.updateWorkflow(SESSION, "wr-one", { status: "running" });
  await reg.updateNode(SESSION, "wr-one", "prepare", { status: "submitting" });
  const restarted = createWorkflowRegistry(dir);
  const [record] = await restarted.list(SESSION);
  assert.equal(record.status, "reconciling");
  assert.equal(record.nodes.find((node) => node.nodeId === "prepare").status, "reconciling");
  assert.equal(record.nodes.find((node) => node.nodeId === "run").status, "blocked", "safe local blocked state remains unchanged");
  const persisted = JSON.parse(await readFile(join(dir, SESSION, "workflows.json"), "utf8"));
  assert.equal(persisted.records[0].status, "reconciling", "restart conversion is durable");
});

test("all ambiguous scheduler-like node states reconcile after restart", async (t) => {
  const { dir } = await fixture(t);
  const now = Date.now();
  const statuses = ["submitting", "submitted", "pending", "running", "cancel-requested"];
  await mkdir(join(dir, SESSION), { recursive: true });
  await writeFile(join(dir, SESSION, "workflows.json"), JSON.stringify({
    schema: "genbio-workflow-registry/1",
    updatedAt: now,
    records: statuses.map((workflowStatus, recordIndex) => ({ workflowRunId: `wr-seeded-${recordIndex}`, workflow: `pipeline-${recordIndex}`, workflowPlanHash: recordIndex.toString(16).padStart(64, "0"), status: workflowStatus, createdAt: now, updatedAt: now, finishedAt: null, nodes: statuses.map((status, index) => ({ nodeId: `n${index}`, project: `p${index}`, operation: "run", operationPlanHash: H2, status, createdAt: now, updatedAt: now, finishedAt: null })) })),
  }), "utf8");
  const records = await createWorkflowRegistry(dir).list(SESSION);
  for (const record of records) {
    assert.deepEqual(record.nodes.map((node) => node.status), statuses.map(() => "reconciling"));
    assert.equal(record.status, "reconciling");
  }
});

test("corrupt registry files fail closed and are never silently reset", async (t) => {
  const { dir } = await fixture(t);
  await mkdir(join(dir, SESSION), { recursive: true });
  await writeFile(join(dir, SESSION, "workflows.json"), "{not-json", "utf8");
  await assert.rejects(createWorkflowRegistry(dir).list(SESSION), /corrupt.*fail closed/u);
  await writeFile(join(dir, SESSION, "workflows.json"), JSON.stringify({ schema: "wrong", updatedAt: 1, records: [] }), "utf8");
  await assert.rejects(createWorkflowRegistry(dir).list(SESSION), /corrupt.*fail closed/u);
});

test("bounded registry evicts oldest terminal workflows but saturated non-terminal state fails closed", async (t) => {
  const { reg } = await fixture(t);
  for (let index = 0; index < MAX_WORKFLOW_RECORDS; index += 1) {
    const id = `wr-${index}`;
    const hash = index.toString(16).padStart(64, "0");
    await reg.record(SESSION, input({ workflowRunId: id, workflowPlanHash: hash, nodes: [{ nodeId: "node", project: `p${index}`, operation: "run", operationPlanHash: H1, status: "ready" }] }));
  }
  await assert.rejects(reg.record(SESSION, input({ workflowRunId: "wr-over", workflowPlanHash: "f".repeat(64) })), /saturated.*non-terminal/u);
});

test("duplicate active workflow plan and duplicate node ids are rejected", async (t) => {
  const { reg } = await fixture(t);
  await reg.record(SESSION, input());
  await assert.rejects(reg.record(SESSION, input({ workflowRunId: "wr-two" })), /already has a non-terminal/u);
  await assert.rejects(reg.record(SESSION, input({ workflowRunId: "wr-dup", workflowPlanHash: H2, nodes: [input().nodes[0], { ...input().nodes[0], operationPlanHash: H2 }] })), /duplicate node id/u);
});
