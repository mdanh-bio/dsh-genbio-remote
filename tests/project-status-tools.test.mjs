import assert from "node:assert/strict";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { createProjectStatusTools } from "../lib/project-status-tools.js";

const policy = { targets: { HPC: { allowlist: {} } } };
const exec = { agent: { id: "p", session: { id: "p", header: { cwd: "/tmp" } } } };

function harness(projectsDir, state) {
  const tools = createProjectStatusTools({
    makeTool: (name, description, parameters, execute) => ({ name, description, parameters, execute }),
    requirePolicy: () => policy,
    requireState: () => state,
    publicState: () => ({ policy: state.policy, envelope: state.envelope, runs: state.runs }),
    config: { projectsDir: projectsDir },
  });
  return { tools, state };
}

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), "project-status-"));
  const projectsDir = join(root, "projects");
  await mkdir(projectsDir);
  t.after(() => rm(root, { recursive: true, force: true }));
  await writeFile(join(projectsDir, "demo.yaml"), `schema_version: 2
project: demo
description: demo equilibration
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
`);
  await writeFile(join(projectsDir, "bad.yaml"), "schema_version: 99\nproject: bad\n");
  return { root, projectsDir };
}

test("genbio_projects_status aggregates all projects read-only (zero session mutation)", async (t) => {
  const fx = await fixture(t);
  const state = {
    policy: { hash: "a".repeat(64) },
    envelope: null,
    runs: [
      { runId: "r1", operation: "project-demo-run", status: "running", target: "HPC", startedAt: 1, finishedAt: null },
      { runId: "r2", operation: "genbio-policy-step", status: "completed", target: "HPC", startedAt: 0, finishedAt: 2 },
    ],
    plans: [],
  };
  const h = harness(fx.projectsDir, state);
  const result = await h.tools.statusAllTool.execute({}, exec);
  assert.equal(result.ok, true);
  const status = result.status;
  assert.equal(status.unattributed_runs, 1, "run r2 belongs to no project and is surfaced, not dropped");
  const demo = status.projects_status.find((entry) => entry.project === "demo");
  assert.equal(demo.valid, true);
  assert.equal(demo.schema_version, 2);
  assert.equal(demo.description, "demo equilibration");
  assert.deepEqual(demo.suggested, []);
  assert.deepEqual(demo.active, ["run"]);
  assert.equal(demo.operations[0].form, "recipe");
  assert.equal(demo.operations[0].cpus, 4);
  const bad = status.projects_status.find((entry) => entry.project === "bad");
  assert.deepEqual(bad, { project: "bad", valid: false, error: "bad: schema_version 2 is required; schema_version 1 is no longer supported" });
  // Rich discovery list (with operation objects) is exposed for the GUI.
  const demoProject = status.projects.find((entry) => entry.project === "demo");
  assert.equal(demoProject.operations[0].cpus, 4);
  // Read-only: session state is never initialized, mutated, or truncated.
  assert.equal(state.runs.length, 2);
  assert.deepEqual(state.plans, []);
  assert.equal(state.runs[0].stdout, undefined);
});

test("session plans are folded into the aggregate and never leak wrapper bytes", async (t) => {
  const fx = await fixture(t);
  const plan = {
    planHash: "c".repeat(64),
    plan: { project: "demo", operation: "run", bytesSha: "d".repeat(16) },
    createdAt: 10,
    status: "planned",
    sbatchText: "#!/bin/bash\nSECRET-HEADER",
  };
  const state = { policy: { hash: "a".repeat(64) }, envelope: null, runs: [], plans: [plan] };
  const h = harness(fx.projectsDir, state);
  const result = await h.tools.statusAllTool.execute({}, exec);
  const demo = result.status.projects_status.find((entry) => entry.project === "demo");
  assert.equal(demo.plan_count, 1);
  assert.equal(demo.plans[0].plan_hash, "c".repeat(64));
  assert.deepEqual(Object.keys(demo.plans[0]).sort(), ["bytes_sha256", "created_at", "operation", "plan_hash", "status"]);
  assert.equal(JSON.stringify(result).includes("SECRET-HEADER"), false, "wrapper bytes never leave the aggregate tool result");
  // Planned-but-never-run: still 'suggested' (the operation has no run).
  assert.deepEqual(demo.suggested, ["run"]);
});

test("a state without a plans array is folded as empty without mutation", async (t) => {
  const fx = await fixture(t);
  const state = { policy: { hash: "a".repeat(64) }, envelope: null, runs: [] };
  const h = harness(fx.projectsDir, state);
  const result = await h.tools.statusAllTool.execute({}, exec);
  const demo = result.status.projects_status.find((entry) => entry.project === "demo");
  assert.equal(demo.plan_count, 0);
  assert.equal(Object.hasOwn(state, "plans"), false, "the read-only status tool must not initialize session plan storage");
});