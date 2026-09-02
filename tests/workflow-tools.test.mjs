// genbio_workflow_plan is a schema-v2-only, local, immutable planning surface.
import assert from "node:assert/strict";
import { mkdtemp, mkdir, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { createWorkflowTools } from "../lib/workflow-tools.js";
import { createWorkflowRegistry } from "../lib/workflow-registry.js";

const policyHash = "a".repeat(64);
const policy = { targets: { HPC: { test_gate: { real_submission: "gpu04" }, allowlist: { gpu04: { partition: "gpus" } }, environment: { recipe_envs: {} } } } };
const envelope = { target: "HPC", node: "gpu04", partition: "gpus", maxCpus: 8, maxGpus: 1, concurrency: 1 };

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), "workflow-tools-"));
  const workspace = join(root, "workspace");
  const projectsDir = join(root, "projects");
  const workflowsDir = join(projectsDir, "workflows");
  await mkdir(join(root, "project-files", "scripts"), { recursive: true });
  await mkdir(join(workspace, "genbio-workflows"), { recursive: true });
  await mkdir(workflowsDir, { recursive: true });
  await writeFile(join(root, "project-files", "scripts", "run.sh"), "#!/bin/bash\ntrue\n");
  t.after(() => rm(root, { recursive: true, force: true }));
  const manifest = `schema_version: 2
project: fixture-project
local_root: ${root}/project-files
remote_root: /data01/fixture-project
files:
  - scripts/run.sh
jobs:
  prepare:
    cpus: 1
    recipe:
      name: fixture-prepare
      script: scripts/run.sh
      parameters:
        count: {type: integer, min: 1, max: 9}
      argv: [--count, {param: count}]
  run:
    cpus: 2
    gpus: 1
    recipe:
      name: fixture-run
      script: scripts/run.sh
      parameters:
        mode: {type: enum, values: [safe, thorough]}
      argv: [--mode, {param: mode}]
`;
  await writeFile(join(projectsDir, "fixture-project.yaml"), manifest);
  return { root, workspace, projectsDir, workflowsDir, workspaceWorkflowsDir: join(workspace, "genbio-workflows") };
}

function harness(projectsDir, workspace) {
  const state = { policy: { hash: policyHash }, envelope, runs: [], plans: [], workflowPlans: [] };
  const exec = { agent: { id: "wf", session: { id: "wf", header: { id: "wf", cwd: workspace } } } };
  const tools = createWorkflowTools({
    makeTool: (name, description, parameters, execute) => ({ name, description, parameters, execute }),
    requirePolicy: () => policy,
    requireState: () => state,
    publicState: (current) => ({ policy: current.policy, runs: current.runs }),
    config: { projectsDir },
    workflowRegistry: createWorkflowRegistry(join(workspace, ".test-workflow-registry")),
  });
  return { tools, state, exec };
}

const workflowText = (name = "pipeline") => `schema_version: 2
workflow: ${name}
nodes:
  - id: prepare
    project: fixture-project
    operation: prepare
    parameters: {count: 3}
  - id: run
    project: fixture-project
    operation: run
    parameters: {mode: safe}
    depends_on: [prepare]
`;

test("genbio_workflow_plan exposes no completed input and resolves immutable v2 operations locally", async (t) => {
  const fx = await fixture(t);
  await writeFile(join(fx.workflowsDir, "pipeline.yaml"), workflowText());
  const h = harness(fx.projectsDir, fx.workspace);
  assert.deepEqual(Object.keys(h.tools.planTool.parameters).sort(), ["workflow"]);

  const first = await h.tools.planTool.execute({ workflow: "pipeline" }, h.exec);
  const second = await h.tools.planTool.execute({ workflow: "pipeline" }, h.exec);
  assert.equal(first.ok, true);
  assert.equal(first.status.workflow_origin, "configured");
  assert.equal(first.status.workflow_plan.schema, "genbio-workflow-plan/2");
  assert.match(first.status.workflow_plan.workflow_plan_hash, /^[a-f0-9]{64}$/u);
  assert.equal(second.status.workflow_plan.workflow_plan_hash, first.status.workflow_plan.workflow_plan_hash);
  assert.deepEqual(first.status.workflow_plan.ready, ["prepare"]);
  assert.deepEqual(first.status.workflow_plan.blocked, ["run"]);
  assert.deepEqual(first.status.workflow_plan.completed, []);
  assert.deepEqual(first.status.workflow_plan.nodes[0].parameters, { count: 3 });
  assert.equal(first.status.workflow_plan.nodes[1].resources.gpus, 1);
  assert.equal(h.state.workflowPlans.length, 1, "identical immutable plans are coalesced");
  assert.equal(h.state.runs.length, 0, "planning never creates runs");
});

test("workspace workflows are discovered before configured workflows", async (t) => {
  const fx = await fixture(t);
  await writeFile(join(fx.workflowsDir, "pipeline.yaml"), workflowText());
  await writeFile(join(fx.workspaceWorkflowsDir, "pipeline.yml"), workflowText());
  const h = harness(fx.projectsDir, fx.workspace);
  const result = await h.tools.planTool.execute({ workflow: "pipeline" }, h.exec);
  assert.equal(result.status.workflow_origin, "workspace");
  assert.equal(h.state.workflowPlans[0].workflowOrigin, "workspace");
  assert.equal(h.state.workflowPlans[0].workspace, await realpath(fx.workspace));
});

test("workflow schema v1 is rejected explicitly and stores no plan", async (t) => {
  const fx = await fixture(t);
  await writeFile(join(fx.workflowsDir, "legacy.yaml"), `schema_version: 1\nworkflow: legacy\nnodes:\n  - id: run\n    project: fixture-project\n    operation: run\n`);
  const h = harness(fx.projectsDir, fx.workspace);
  await assert.rejects(h.tools.planTool.execute({ workflow: "legacy" }, h.exec), /workflow schema_version 2 is required/u);
  assert.equal(h.state.workflowPlans.length, 0);
  assert.equal(h.state.runs.length, 0);
});

test("cycles, unknown dependencies, operations, projects, and invalid parameters fail closed", async (t) => {
  const fx = await fixture(t);
  const h = harness(fx.projectsDir, fx.workspace);
  await writeFile(join(fx.workflowsDir, "cycle.yaml"), `schema_version: 2\nworkflow: cycle\nnodes:\n  - {id: a, project: fixture-project, operation: prepare, parameters: {count: 1}, depends_on: [b]}\n  - {id: b, project: fixture-project, operation: run, parameters: {mode: safe}, depends_on: [a]}\n`);
  await assert.rejects(h.tools.planTool.execute({ workflow: "cycle" }, h.exec), /dependency cycle/u);
  await writeFile(join(fx.workflowsDir, "unknown-dependency.yaml"), `schema_version: 2\nworkflow: unknown-dependency\nnodes:\n  - {id: a, project: fixture-project, operation: prepare, parameters: {count: 1}, depends_on: [missing]}\n`);
  await assert.rejects(h.tools.planTool.execute({ workflow: "unknown-dependency" }, h.exec), /unknown node: missing/u);
  await writeFile(join(fx.workflowsDir, "unknown-operation.yaml"), `schema_version: 2\nworkflow: unknown-operation\nnodes:\n  - {id: a, project: fixture-project, operation: missing}\n`);
  await assert.rejects(h.tools.planTool.execute({ workflow: "unknown-operation" }, h.exec), /workflow operation missing must be a declarative recipe|operation resolution failed/u);
  await writeFile(join(fx.workflowsDir, "unknown-project.yaml"), `schema_version: 2\nworkflow: unknown-project\nnodes:\n  - {id: a, project: absent-project, operation: run}\n`);
  await assert.rejects(h.tools.planTool.execute({ workflow: "unknown-project" }, h.exec), /unknown Genbio project: absent-project|operation resolution failed/u);
  await writeFile(join(fx.workflowsDir, "invalid-parameters.yaml"), `schema_version: 2\nworkflow: invalid-parameters\nnodes:\n  - {id: a, project: fixture-project, operation: prepare, parameters: {count: 99}}\n`);
  await assert.rejects(h.tools.planTool.execute({ workflow: "invalid-parameters" }, h.exec), /at most 9|operation resolution failed/u);
  assert.equal(h.state.workflowPlans.length, 0);
  assert.equal(h.state.runs.length, 0);
});

test("workflow files fail closed on name mismatch, bad YAML, duplicate pairs, and unknown names", async (t) => {
  const fx = await fixture(t);
  const h = harness(fx.projectsDir, fx.workspace);
  await writeFile(join(fx.workflowsDir, "renamed.yaml"), workflowText("other-name"));
  await assert.rejects(h.tools.planTool.execute({ workflow: "renamed" }, h.exec), /workflow name must match the filename/u);
  await writeFile(join(fx.workflowsDir, "broken.yaml"), "schema_version: 2\nworkflow: broken\nnodes: [unclosed");
  await assert.rejects(h.tools.planTool.execute({ workflow: "broken" }, h.exec), /invalid workflow YAML/u);
  await writeFile(join(fx.workflowsDir, "duplicate.yaml"), `schema_version: 2\nworkflow: duplicate\nnodes:\n  - {id: one, project: fixture-project, operation: run, parameters: {mode: safe}}\n  - {id: two, project: fixture-project, operation: run, parameters: {mode: thorough}}\n`);
  await assert.rejects(h.tools.planTool.execute({ workflow: "duplicate" }, h.exec), /reuses the \(project, operation\) pair/u);
  await assert.rejects(h.tools.planTool.execute({ workflow: "missing" }, h.exec), /unknown workflow: missing \(available: /u);
  await assert.rejects(h.tools.planTool.execute({ workflow: "../evil" }, h.exec), /invalid workflow name/u);
  assert.equal(h.state.workflowPlans.length, 0);
});
