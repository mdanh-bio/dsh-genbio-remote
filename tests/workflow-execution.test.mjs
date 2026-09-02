import assert from "node:assert/strict";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { createProjectSource } from "../lib/project-source.js";
import { createWorkflowRegistry } from "../lib/workflow-registry.js";
import { createWorkflowTools } from "../lib/workflow-tools.js";

const H = "a".repeat(64);
const policy = { targets: { HPC: { test_gate: { real_submission: "gpu04" }, allowlist: { gpu04: { partition: "gpus" } }, environment: { recipe_envs: {} } } } };
const envelope = { target: "HPC", node: "gpu04", partition: "gpus", maxCpus: 8, maxGpus: 1, concurrency: 1 };

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), "workflow-exec-"));
  const workspace = join(root, "workspace");
  const configured = join(root, "configured");
  const registryDir = join(root, "registry");
  await mkdir(join(workspace, "scripts"), { recursive: true });
  await mkdir(join(workspace, "genbio-workflows"));
  await mkdir(configured);
  await writeFile(join(workspace, "scripts/run.sh"), "#!/bin/bash\ntrue\n");
  await writeFile(join(workspace, "genbio-project.yml"), `schema_version: 2\nproject: demo\nlocal_root: ${workspace}\nremote_root: /data01/demo\nfiles: [scripts/run.sh]\njobs:\n  prepare:\n    cpus: 1\n    recipe: {name: demo-prepare, script: scripts/run.sh, argv: []}\n  run:\n    cpus: 1\n    recipe: {name: demo-run, script: scripts/run.sh, argv: []}\n`);
  await writeFile(join(workspace, "genbio-workflows/pipeline.yml"), `schema_version: 2\nworkflow: pipeline\nnodes:\n  - {id: prepare, project: demo, operation: prepare, parameters: {}}\n  - id: run\n    project: demo\n    operation: run\n    parameters: {}\n    depends_on: [prepare]\n`);
  t.after(() => rm(root, { recursive: true, force: true }));
  return { root, workspace, configured, registryDir };
}

function harness(fx) {
  const state = { policy: { hash: H }, envelope, runs: [], plans: [], workflowPlans: [] };
  const exec = { agent: { id: "wf", session: { id: "wf", header: { id: "wf", cwd: fx.workspace } } } };
  let submitCalls = 0;
  const projectPlanTool = { async execute({ project, operation, parameters }) {
    const wf = state.workflowPlans.at(-1);
    const node = wf.nodeRecords.find((item) => item.project === project && item.operation === operation);
    state.plans.push({ planHash: node.operationPlanHash, plan: { project, operation }, parameters });
    return { status: { planned: { plan_hash: node.operationPlanHash } } };
  }};
  const projectExecuteTool = { async execute({ plan_hash }) {
    submitCalls += 1;
    const wf = state.workflowPlans.at(-1);
    const node = wf.nodeRecords.find((item) => item.operationPlanHash === plan_hash);
    const run = { target: "HPC", operation: `project-${node.project}-${node.operation}`, slurmJobId: String(1000 + submitCalls), workloadStatus: "submitted" };
    state.runs.push(run);
    return { status: { started: run } };
  }};
  const projectStatusTool = { async execute({ project, operation, job_id }) {
    const run = state.runs.find((item) => item.slurmJobId === job_id && item.operation === `project-${project}-${operation}`);
    run.workloadStatus = "completed"; run.slurmStatus = "COMPLETED"; run.slurmExitCode = "0:0"; run.workloadEvidence = "scheduler-and-job-output";
    return { ok: true };
  }};
  const tools = createWorkflowTools({
    makeTool: (name, description, parameters, execute) => ({ name, description, parameters, execute }),
    requirePolicy: () => policy,
    requireState: () => state,
    publicState: () => ({}),
    config: { projectsDir: fx.configured },
    projectSource: createProjectSource({ projectsDir: fx.configured }),
    projectPlanTool, projectExecuteTool, projectStatusTool,
    projectCancelTool: { execute: async () => ({ status: { cancellation: { requested: true } } }) },
    workflowRegistry: createWorkflowRegistry(fx.registryDir),
  });
  return { state, exec, tools, submitCalls: () => submitCalls };
}

test("schema-v2 workflow submits one node per explicit execute or advance", async (t) => {
  const fx = await fixture(t); const h = harness(fx);
  const planned = await h.tools.planTool.execute({ workflow: "pipeline" }, h.exec);
  const hash = planned.status.workflow_plan.workflow_plan_hash;
  const first = await h.tools.executeTool.execute({ plan_hash: hash }, h.exec);
  assert.equal(h.submitCalls(), 1);
  assert.equal(first.status.workflow_run.nodes.find((node) => node.node_id === "prepare").status, "submitted");
  await h.tools.statusTool.execute({ workflow_run_id: first.status.workflow_run.workflow_run_id }, h.exec);
  assert.equal(h.submitCalls(), 1, "status never submits the next node");
  const advanced = await h.tools.advanceTool.execute({ workflow_run_id: first.status.workflow_run.workflow_run_id }, h.exec);
  assert.equal(h.submitCalls(), 2);
  assert.equal(advanced.status.workflow_run.nodes.find((node) => node.node_id === "run").status, "submitted");
  const done = await h.tools.statusTool.execute({ workflow_run_id: first.status.workflow_run.workflow_run_id }, h.exec);
  assert.equal(done.status.workflow_run.status, "completed");
});

test("durable workflow plan restores after session plan cache loss", async (t) => {
  const fx = await fixture(t); const h = harness(fx);
  const planned = await h.tools.planTool.execute({ workflow: "pipeline" }, h.exec);
  const started = await h.tools.executeTool.execute({ plan_hash: planned.status.workflow_plan.workflow_plan_hash }, h.exec);
  h.state.workflowPlans = [];
  const status = await h.tools.statusTool.execute({ workflow_run_id: started.status.workflow_run.workflow_run_id }, h.exec);
  assert.equal(status.status.workflow_run.nodes.find((node) => node.node_id === "prepare").status, "completed");
  assert.equal(h.state.workflowPlans.length, 1);
});

test("unknown workflow plan and drift fail before submission", async (t) => {
  const fx = await fixture(t); const h = harness(fx);
  await assert.rejects(h.tools.executeTool.execute({ plan_hash: "f".repeat(64) }, h.exec), /unknown or expired/u);
  const planned = await h.tools.planTool.execute({ workflow: "pipeline" }, h.exec);
  await writeFile(join(fx.workspace, "genbio-workflows/pipeline.yml"), `schema_version: 2\nworkflow: pipeline\nnodes:\n  - {id: run, project: demo, operation: run, parameters: {}}\n`);
  await assert.rejects(h.tools.executeTool.execute({ plan_hash: planned.status.workflow_plan.workflow_plan_hash }, h.exec), /workflow plan drift/u);
  assert.equal(h.submitCalls(), 0);
});
