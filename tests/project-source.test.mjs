import assert from "node:assert/strict";
import { mkdtemp, mkdir, realpath, rm, symlink, unlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { createProjectSource } from "../lib/project-source.js";

const execAt = (cwd) => ({ agent: { session: { header: { cwd } } } });
const manifest = (project, root) => `schema_version: 2\nproject: ${project}\nlocal_root: ${root}\nremote_root: /data01/${project}\nfiles: [scripts/run.sh]\njobs:\n  run:\n    cpus: 1\n    recipe: {name: ${project}-run, script: scripts/run.sh, argv: []}\n`;

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), "project-source-"));
  const workspace = join(root, "workspace");
  const configured = join(root, "configured");
  await mkdir(join(workspace, "scripts"), { recursive: true });
  await mkdir(configured);
  await writeFile(join(workspace, "scripts/run.sh"), "#!/bin/bash\ntrue\n");
  t.after(() => rm(root, { recursive: true, force: true }));
  return { root, workspace, configured, source: createProjectSource({ projectsDir: configured }) };
}

test("workspace project is discovered immediately and overrides configured project", async (t) => {
  const fx = await fixture(t);
  await writeFile(join(fx.configured, "demo.yaml"), manifest("demo", fx.workspace));
  let listed = await fx.source.listProjects(execAt(fx.workspace));
  assert.deepEqual(listed, [{ project: "demo", origin: "configured" }]);
  await writeFile(join(fx.workspace, "genbio-project.yml"), manifest("demo", fx.workspace));
  listed = await fx.source.listProjects(execAt(fx.workspace));
  assert.deepEqual(listed, [{ project: "demo", origin: "workspace" }]);
  const loaded = await fx.source.loadProject("demo", execAt(fx.workspace));
  assert.equal(loaded.origin.kind, "workspace");
  await unlink(join(fx.workspace, "genbio-project.yml"));
  assert.equal((await fx.source.loadProject("demo", execAt(fx.workspace))).origin.kind, "configured");
});

test("both workspace manifest extensions are individually supported but together ambiguous", async (t) => {
  const fx = await fixture(t);
  await writeFile(join(fx.workspace, "genbio-project.yaml"), manifest("demo", fx.workspace));
  assert.equal((await fx.source.loadProject("demo", execAt(fx.workspace))).origin.kind, "workspace");
  await writeFile(join(fx.workspace, "genbio-project.yml"), manifest("demo", fx.workspace));
  await assert.rejects(fx.source.listProjects(execAt(fx.workspace)), /both genbio-project\.yml and genbio-project\.yaml/u);
});

test("workspace boundary rejects missing cwd, manifest symlink, local_root escape and file symlink", async (t) => {
  const fx = await fixture(t);
  await assert.rejects(fx.source.listProjects({ agent: { session: { header: {} } } }), /cwd must be an absolute/u);
  const outside = join(fx.root, "outside");
  await mkdir(join(outside, "scripts"), { recursive: true });
  await writeFile(join(outside, "scripts/run.sh"), "true\n");
  await writeFile(join(outside, "manifest.yml"), manifest("demo", outside));
  await symlink(join(outside, "manifest.yml"), join(fx.workspace, "genbio-project.yml"));
  await assert.rejects(fx.source.listProjects(execAt(fx.workspace)), /manifest must not be a symlink/u);
  await unlink(join(fx.workspace, "genbio-project.yml"));
  await writeFile(join(fx.workspace, "genbio-project.yml"), manifest("demo", outside));
  await assert.rejects(fx.source.loadProject("demo", execAt(fx.workspace)), /local_root must resolve exactly/u);
  await writeFile(join(fx.workspace, "genbio-project.yml"), manifest("demo", fx.workspace));
  await unlink(join(fx.workspace, "scripts/run.sh"));
  await symlink(join(outside, "scripts/run.sh"), join(fx.workspace, "scripts/run.sh"));
  await assert.rejects(fx.source.loadProject("demo", execAt(fx.workspace)), /project files must not be symlinks/u);
});

test("workspace project lookup does not impersonate another requested name", async (t) => {
  const fx = await fixture(t);
  await writeFile(join(fx.workspace, "genbio-project.yml"), manifest("demo", fx.workspace));
  await assert.rejects(fx.source.loadProject("other", execAt(fx.workspace)), /unknown Genbio project: other/u);
});

test("workspace workflow discovery prefers genbio-workflows and preserves schema-v2 text", async (t) => {
  const fx = await fixture(t);
  const configuredWorkflows = join(fx.configured, "workflows");
  const workspaceWorkflows = join(fx.workspace, "genbio-workflows");
  await mkdir(configuredWorkflows);
  await mkdir(workspaceWorkflows);
  const configured = "schema_version: 2\nworkflow: pipeline\nnodes:\n  - {id: configured, project: demo, operation: run, parameters: {}}\n";
  const workspace = "schema_version: 2\nworkflow: pipeline\nnodes:\n  - {id: workspace, project: demo, operation: run, parameters: {mode: safe}}\n";
  await writeFile(join(configuredWorkflows, "pipeline.yaml"), configured);
  await writeFile(join(workspaceWorkflows, "pipeline.yml"), workspace);

  const loaded = await fx.source.loadWorkflow("pipeline", execAt(fx.workspace));
  assert.equal(loaded.origin.kind, "workspace");
  assert.equal(loaded.origin.workspace, await realpath(fx.workspace));
  assert.equal(loaded.text, workspace);
  await unlink(join(workspaceWorkflows, "pipeline.yml"));
  const fallback = await fx.source.loadWorkflow("pipeline", execAt(fx.workspace));
  assert.equal(fallback.origin.kind, "configured");
  assert.equal(fallback.text, configured);
});

test("workspace workflow discovery is bounded and rejects extension ambiguity", async (t) => {
  const fx = await fixture(t);
  const workflows = join(fx.workspace, "genbio-workflows");
  await mkdir(workflows);
  const text = "schema_version: 2\nworkflow: pipeline\nnodes:\n  - {id: run, project: demo, operation: run, parameters: {}}\n";
  await writeFile(join(workflows, "pipeline.yml"), text);
  await writeFile(join(workflows, "pipeline.yaml"), text);
  await assert.rejects(fx.source.loadWorkflow("pipeline", execAt(fx.workspace)), /keep exactly one workflow file/u);
  await assert.rejects(fx.source.loadWorkflow("../pipeline", execAt(fx.workspace)), /invalid workflow name/u);
});
