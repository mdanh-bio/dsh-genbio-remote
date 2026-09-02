// Phase 2: genbio_workflow_plan tool — local manifest reads only; the plan
// marks ready nodes and nothing else. No remote, transfer, allocation, or
// submission surface exists on this tool.
import assert from "node:assert/strict";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { createWorkflowTools } from "../lib/workflow-tools.js";

const policy = { targets: { HPC: { test_gate: { real_submission: "gpu04" }, allowlist: { gpu04: { partition: "gpus" } } } } };
const exec = { agent: { id: "wf", session: { id: "wf", header: { cwd: "/tmp" } } } };

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), "workflow-tools-"));
  const projectsDir = join(root, "projects");
  const workflowsDir = join(projectsDir, "workflows");
  await mkdir(projectsDir, { recursive: true });
  await mkdir(workflowsDir, { recursive: true });
  t.after(() => rm(root, { recursive: true, force: true }));
  const manifest = `schema_version: 2\nproject: demo\nlocal_root: ${root}/local\nremote_root: /data01/demo\nfiles:\n  - scripts/run.sh\njobs:\n  prepare:\n    cpus: 1\n    recipe:\n      name: demo-prepare\n      script: scripts/run.sh\n      argv: []\n  run:\n    cpus: 2\n    recipe:\n      name: demo-run\n      script: scripts/run.sh\n      argv: []\n`;
  await writeFile(join(projectsDir, "demo.yaml"), manifest);
  return { root, projectsDir, workflowsDir };
}

function harness(projectsDir, state = { policy: { hash: "h" }, runs: [] }) {
  const tools = createWorkflowTools({
    makeTool: (name, description, parameters, execute) => ({ name, description, parameters, execute }),
    requirePolicy: () => policy,
    requireState: () => state,
    publicState: (s) => ({ policy: s.policy, runs: s.runs }),
    config: { pinnedProjectsDir: projectsDir },
  });
  // NOTE: no runRemote/shell/userQuestions/jobs in the harness — any attempt
  // at a remote or side-effecting call would throw immediately.
  return { tools, state };
}

test("genbio_workflow_plan marks ready nodes from local manifests only", async (t) => {
  const fx = await fixture(t);
  await writeFile(join(fx.workflowsDir, "pipeline.yaml"), `schema_version: 1\nworkflow: pipeline\nnodes:\n  - id: prepare\n    project: demo\n    operation: prepare\n  - id: run\n    project: demo\n    operation: run\n    depends_on: [prepare]\n`);
  const h = harness(fx.projectsDir);
  const result = await h.tools.planTool.execute({ workflow: "pipeline" }, exec);
  assert.equal(result.ok, true);
  const plan = result.status.workflow_plan;
  assert.equal(plan.workflow, "pipeline");
  assert.deepEqual(plan.ready, ["prepare"]);
  assert.deepEqual(plan.blocked, ["run"]);
  assert.equal(plan.nodes.find((node) => node.id === "run").waitingOn[0], "prepare");
  assert.equal(plan.nodes.find((node) => node.id === "run").pairKey, "demo/run");
  assert.equal(h.state.runs.length, 0, "planning never creates runs");

  const advanced = await h.tools.planTool.execute({ workflow: "pipeline", completed: ["prepare"] }, exec);
  assert.deepEqual(advanced.status.workflow_plan.ready, ["run"]);
  assert.deepEqual(advanced.status.workflow_plan.completed, ["prepare"]);
  assert.equal(h.state.runs.length, 0);
});

test("workflows with cycles, unknown deps, or unknown operations fail closed", async (t) => {
  const fx = await fixture(t);
  const h = harness(fx.projectsDir);
  await writeFile(join(fx.workflowsDir, "cyc.yaml"), `schema_version: 1\nworkflow: cyc\nnodes:\n  - id: a\n    project: demo\n    operation: prepare\n    depends_on: [b]\n  - id: b\n    project: demo\n    operation: run\n    depends_on: [a]\n`);
  await assert.rejects(h.tools.planTool.execute({ workflow: "cyc" }, exec), /dependency cycle/u);
  await writeFile(join(fx.workflowsDir, "ghost.yaml"), `schema_version: 1\nworkflow: ghost\nnodes:\n  - id: a\n    project: demo\n    operation: prepare\n    depends_on: [nope]\n`);
  await assert.rejects(h.tools.planTool.execute({ workflow: "ghost" }, exec), /unknown node: nope/u);
  await writeFile(join(fx.workflowsDir, "badop.yaml"), `schema_version: 1\nworkflow: badop\nnodes:\n  - id: a\n    project: demo\n    operation: missing-op\n`);
  await assert.rejects(h.tools.planTool.execute({ workflow: "badop" }, exec), /has no operation missing-op/u);
  await writeFile(join(fx.workflowsDir, "badproj.yaml"), `schema_version: 1\nworkflow: badproj\nnodes:\n  - id: a\n    project: no-such-project\n    operation: run\n`);
  await assert.rejects(h.tools.planTool.execute({ workflow: "badproj" }, exec), /no-such-project unavailable/u);
  assert.equal(h.state.runs.length, 0, "failed planning leaves no state");
});

test("workflow files fail closed on name mismatch, bad YAML, and unknown names", async (t) => {
  const fx = await fixture(t);
  const h = harness(fx.projectsDir);
  await writeFile(join(fx.workflowsDir, "renamed.yaml"), `schema_version: 1\nworkflow: other-name\nnodes:\n  - id: a\n    project: demo\n    operation: prepare\n`);
  await assert.rejects(h.tools.planTool.execute({ workflow: "renamed" }, exec), /workflow name must match the filename/u);
  await writeFile(join(fx.workflowsDir, "broken.yaml"), `schema_version: 1\nworkflow: broken\nnodes: [unclosed`);
  await assert.rejects(h.tools.planTool.execute({ workflow: "broken" }, exec), /invalid workflow YAML/u);
  await assert.rejects(h.tools.planTool.execute({ workflow: "missing" }, exec), /unknown workflow: missing \(available: /u);
  await assert.rejects(h.tools.planTool.execute({ workflow: "../evil" }, exec), /invalid workflow name/u);
});

test("duplicate pairs inside a workflow file are rejected at plan time", async (t) => {
  const fx = await fixture(t);
  const h = harness(fx.projectsDir);
  await writeFile(join(fx.workflowsDir, "dup.yaml"), `schema_version: 1\nworkflow: dup\nnodes:\n  - id: one\n    project: demo\n    operation: run\n  - id: two\n    project: demo\n    operation: run\n`);
  await assert.rejects(h.tools.planTool.execute({ workflow: "dup" }, exec), /reuses the \(project, operation\) pair/u);
  assert.equal(h.state.runs.length, 0);
});
