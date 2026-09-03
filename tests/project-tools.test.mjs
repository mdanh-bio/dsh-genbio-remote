import assert from "node:assert/strict";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { createProjectTools, MAX_SESSION_PLANS } from "../lib/project-tools.js";

const policyHash = "a".repeat(64);
const policy = { targets: { HPC: { test_gate: { real_submission: "gpu04" }, allowlist: { gpu04: { partition: "gpus" } }, environment: { recipe_envs: {} } } } };
const envelope = { target: "HPC", node: "gpu04", partition: "gpus", maxCpus: 8, maxGpus: 1, concurrency: 1 };
const execFor = (cwd) => ({ agent: { id: "p", session: { id: "p", header: { cwd } } } });
const exec = execFor(tmpdir());

function harness(projectsDir) {
  const hooks = { policy: 0, state: 0 };
  const state = { policy: { hash: policyHash }, envelope, runs: [], plans: [] };
  const tools = createProjectTools({
    makeTool: (name, description, parameters, execute) => ({ name, description, parameters, execute }),
    requirePolicy: () => { hooks.policy += 1; return policy; },
    requireState: () => { hooks.state += 1; return state; },
    publicState: () => ({ policy: state.policy, envelope: state.envelope, runs: state.runs }),
    config: { projectsDir },
  });
  return { tools, state, hooks };
}

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), "project-tools-"));
  const projectsDir = join(root, "projects");
  await mkdir(projectsDir);
  await mkdir(join(root, "local", "scripts"), { recursive: true });
  await writeFile(join(root, "local", "scripts", "run.sh"), "#!/bin/bash\ntrue\n");
  t.after(() => rm(root, { recursive: true, force: true }));
  const manifest = `schema_version: 2
project: demo
local_root: ${root}/local
remote_root: /data01/demo
files:
  - scripts/run.sh
jobs:
  run:
    cpus: 4
    gpus: 1
    recipe:
      name: demo-run
      script: scripts/run.sh
      parameters:
        count: {type: integer, min: 1, max: 10}
      argv:
        - --count
        - {param: count}
`;
  await writeFile(join(projectsDir, "demo.yaml"), manifest);
  await writeFile(join(projectsDir, "bad.yaml"), "schema_version: 1\nproject: bad\n");
  return { root, projectsDir, manifest };
}

test("project discovery and describe are local read-only manifest views", async (t) => {
  const fx = await fixture(t);
  const h = harness(fx.projectsDir);
  const listed = await h.tools.projectsTool.execute({}, exec);
  assert.deepEqual(listed.status.projects.map(({ project, valid }) => ({ project, valid })), [{ project: "bad", valid: false }, { project: "demo", valid: true }]);
  assert.match(listed.status.projects[0].error, /schema_version 2 is required/u);
  const described = await h.tools.describeTool.execute({ project: "demo" }, exec);
  assert.equal(described.status.project.schema_version, 2);
  assert.equal(described.status.project.operations[0].form, "recipe");
  assert.equal(described.status.project.operations[0].parameters.count.type, "integer");
});

test("public inventory excludes local inode and canonical-root metadata", async (t) => {
  const fx = await fixture(t);
  const h = harness(fx.projectsDir);
  const result = await h.tools.inventoryTool.execute({ project: "demo" }, exec);
  assert.deepEqual(Object.keys(result.status.inventory.files[0]).sort(), ["rel", "sha256", "size"]);
  assert.equal("canonicalRoot" in result.status.inventory, false);
});

test("planning is deterministic, side-effect-free, and stores session-owned immutable records", async (t) => {
  const fx = await fixture(t);
  const h = harness(fx.projectsDir);
  const first = await h.tools.planTool.execute({ project: "demo", operation: "run", parameters: { count: 4 } }, exec);
  const second = await h.tools.planTool.execute({ project: "demo", operation: "run", parameters: { count: 4 } }, exec);
  assert.equal(first.status.planned.plan_hash, second.status.planned.plan_hash);
  assert.equal(h.state.plans.length, 1, "same plan hash is coalesced");
  assert.equal(Object.isFrozen(h.state.plans[0]), true);
  assert.match(first.status.planned.wrapper, /'scripts\/run\.sh' '--count' '4'/u);
  assert.equal(h.state.runs.length, 0);
});

test("planning rejects missing envelopes and invalid parameters before storing a plan", async (t) => {
  const fx = await fixture(t);
  const h = harness(fx.projectsDir);
  h.state.envelope = null;
  await assert.rejects(h.tools.planTool.execute({ project: "demo", operation: "run", parameters: { count: 4 } }, exec), /session envelope/u);
  h.state.envelope = envelope;
  await assert.rejects(h.tools.planTool.execute({ project: "demo", operation: "run", parameters: { count: "4" } }, exec), /must be an integer/u);
  assert.equal(h.state.plans.length, 0);
});

test("session plan store is capped and execution fails closed with zero remote surface", async (t) => {
  const fx = await fixture(t);
  const h = harness(fx.projectsDir);
  for (let count = 1; count <= MAX_SESSION_PLANS + 3; count += 1) {
    const current = (count % 10) || 10;
    await h.tools.planTool.execute({ project: "demo", operation: "run", parameters: { count: current } }, exec);
  }
  assert.ok(h.state.plans.length <= MAX_SESSION_PLANS);
  await assert.rejects(h.tools.executeTool.execute({ plan_hash: "f".repeat(64) }, exec), /unknown or expired/u);
  const known = h.state.plans.at(-1).planHash;
  await assert.rejects(h.tools.executeTool.execute({ plan_hash: known }, exec), /fail-closed.*integration is complete/u);
  assert.equal(h.state.runs.length, 0);
});

test("status reports session plans without scheduler or remote activity", async (t) => {
  const fx = await fixture(t);
  const h = harness(fx.projectsDir);
  await h.tools.planTool.execute({ project: "demo", operation: "run", parameters: { count: 2 } }, exec);
  const status = await h.tools.statusTool.execute({ project: "demo" }, exec);
  assert.equal(status.status.plans.length, 1);
  assert.deepEqual(status.status.project_runs, []);
});

test("status reconcile=false is local-only and rejects exact scheduler arguments", async (t) => {
  const fx = await fixture(t);
  const h = harness(fx.projectsDir);
  const status = await h.tools.statusTool.execute({ project: "demo", reconcile: false }, exec);
  assert.equal(status.status.scheduler_evidence, "skipped");
  assert.deepEqual(status.status.project_runs, []);
  await assert.rejects(h.tools.statusTool.execute({ project: "demo", operation: "run", job_id: "123", reconcile: false }, exec), /reconcile=false/u);
});

test("unsafe and unknown project names fail closed", async (t) => {
  const fx = await fixture(t);
  const h = harness(fx.projectsDir);
  await assert.rejects(h.tools.describeTool.execute({ project: "../demo" }, exec), /invalid project name/u);
  await assert.rejects(h.tools.describeTool.execute({ project: "missing" }, exec), /unknown Genbio project/u);
  await assert.rejects(h.tools.statusTool.execute({ project: "Bad_Name" }, exec), /invalid project name/u);
  assert.equal(h.state.plans.length, 0);
  assert.equal(h.state.runs.length, 0);
});
