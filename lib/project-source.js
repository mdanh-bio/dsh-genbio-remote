// Workspace-first project/workflow discovery with a strict local containment boundary.
// This module performs local filesystem reads only. It never transfers, submits,
// allocates, asks questions, or contacts a scheduler.
import { createHash } from "node:crypto";
import { lstat, readFile, readdir, realpath, stat } from "node:fs/promises";
import { isAbsolute, join, relative, resolve, sep } from "node:path";
import { load as parseYaml } from "js-yaml";
import { parseProjectManifest } from "./project.js";

const SAFE_NAME_RE = /^[a-z0-9][a-z0-9-]*$/u;
const WORKSPACE_MANIFEST_NAMES = Object.freeze(["genbio-project.yml", "genbio-project.yaml"]);
const WORKSPACE_WORKFLOW_DIR = "genbio-workflows";

function sha256(data) { return createHash("sha256").update(data).digest("hex"); }
function configuredProjectsDir(config) { return config?.projectsDir ?? "/Users/mdanh/.dsh/profiles/desktop/genbio-projects"; }
function inside(root, candidate) {
  const rel = relative(root, candidate);
  return rel === "" || (rel !== ".." && !rel.startsWith(`..${sep}`) && !isAbsolute(rel));
}
function workspacePathOf(exec) {
  const cwd = exec?.agent?.session?.header?.cwd;
  return typeof cwd === "string" && cwd.length > 0 ? cwd : null;
}
async function canonicalWorkspace(exec) {
  const cwd = workspacePathOf(exec);
  if (!cwd || !isAbsolute(cwd)) throw new Error("the active Genbio workspace cwd must be an absolute existing directory");
  let root;
  try { root = await realpath(cwd); } catch { throw new Error(`the active Genbio workspace cwd does not resolve: ${cwd}`); }
  let info;
  try { info = await stat(root); } catch { throw new Error(`the active Genbio workspace cwd cannot be inspected: ${cwd}`); }
  if (!info.isDirectory()) throw new Error(`the active Genbio workspace cwd is not a directory: ${cwd}`);
  return root;
}
async function existingWorkspaceManifest(root) {
  const found = [];
  for (const name of WORKSPACE_MANIFEST_NAMES) {
    const path = join(root, name);
    try {
      const info = await lstat(path);
      if (info.isSymbolicLink()) throw new Error(`workspace project manifest must not be a symlink: ${name}`);
      if (!info.isFile()) throw new Error(`workspace project manifest must be a regular file: ${name}`);
      found.push(path);
    } catch (error) {
      if (error?.code !== "ENOENT") throw error;
    }
  }
  if (found.length > 1) throw new Error(`workspace contains both ${WORKSPACE_MANIFEST_NAMES.join(" and ")}; keep exactly one project manifest`);
  return found[0] ?? null;
}
function parseProjectText(expectedProject, path, text) {
  let parsed;
  try { parsed = parseYaml(text, { json: false }); } catch (error) { throw new Error(`${expectedProject}: invalid YAML: ${String(error?.reason ?? error?.message ?? error)}`); }
  const projectName = expectedProject ?? parsed?.project;
  if (typeof projectName !== "string" || !SAFE_NAME_RE.test(projectName)) throw new Error("workspace project manifest must declare a safe kebab-case project name");
  return { manifest: parseProjectManifest(projectName, parsed), project: projectName, path, text, manifestSha: sha256(text) };
}
async function assertWorkspaceContainment(root, loaded) {
  let localRoot;
  try { localRoot = await realpath(loaded.manifest.localRoot); } catch { throw new Error(`${loaded.project}: local_root does not resolve inside the active workspace`); }
  if (localRoot !== root) throw new Error(`${loaded.project}: workspace manifest local_root must resolve exactly to the active workspace root`);
  for (const rel of loaded.manifest.files) {
    const declared = join(localRoot, rel);
    let declaredInfo;
    try { declaredInfo = await lstat(declared); } catch { throw new Error(`${loaded.project}: declared project file is missing: ${rel}`); }
    if (declaredInfo.isSymbolicLink()) throw new Error(`${loaded.project}: declared project files must not be symlinks: ${rel}`);
    const actual = await realpath(declared);
    if (!inside(root, actual)) throw new Error(`${loaded.project}: declared project file escapes the active workspace: ${rel}`);
    const actualInfo = await stat(actual);
    if (!actualInfo.isFile()) throw new Error(`${loaded.project}: declared project path is not a regular file: ${rel}`);
  }
  return loaded;
}
async function loadWorkspaceProject(exec, expectedProject = null) {
  const root = await canonicalWorkspace(exec);
  const path = await existingWorkspaceManifest(root);
  if (!path) return null;
  const text = await readFile(path, "utf8");
  const loaded = parseProjectText(expectedProject, path, text);
  if (expectedProject !== null && loaded.project !== expectedProject) return null;
  await assertWorkspaceContainment(root, loaded);
  return Object.freeze({ ...loaded, origin: Object.freeze({ kind: "workspace", workspace: root, manifestPath: path }) });
}
async function loadConfiguredProject(projectsDir, project) {
  if (typeof project !== "string" || !SAFE_NAME_RE.test(project)) throw new Error(`invalid project name: ${project}`);
  const path = join(projectsDir, `${project}.yaml`);
  let text;
  try { text = await readFile(path, "utf8"); } catch { throw new Error(`unknown Genbio project: ${project}`); }
  const loaded = parseProjectText(project, path, text);
  return Object.freeze({ ...loaded, origin: Object.freeze({ kind: "configured", workspace: null, manifestPath: path }) });
}
export function createProjectSource(config = {}) {
  const projectsDir = configuredProjectsDir(config);
  async function loadProject(project, exec) {
    if (typeof project !== "string" || !SAFE_NAME_RE.test(project)) throw new Error(`invalid project name: ${project}`);
    const workspace = await loadWorkspaceProject(exec, null);
    if (workspace?.project === project) return workspace;
    return loadConfiguredProject(projectsDir, project);
  }
  async function listProjects(exec) {
    const byName = new Map();
    let entries = [];
    try { entries = await readdir(projectsDir); } catch { entries = []; }
    for (const name of entries.filter((entry) => entry.endsWith(".yaml")).map((entry) => entry.slice(0, -5)).filter((entry) => SAFE_NAME_RE.test(entry)).sort()) {
      byName.set(name, { project: name, origin: "configured" });
    }
    const workspace = await loadWorkspaceProject(exec, null);
    if (workspace) byName.set(workspace.project, { project: workspace.project, origin: "workspace" });
    return [...byName.values()].sort((a, b) => a.project.localeCompare(b.project));
  }
  async function loadWorkflow(name, exec) {
    if (typeof name !== "string" || !SAFE_NAME_RE.test(name)) throw new Error(`invalid workflow name: ${name}`);
    const root = await canonicalWorkspace(exec);
    const candidates = [join(root, WORKSPACE_WORKFLOW_DIR, `${name}.yml`), join(root, WORKSPACE_WORKFLOW_DIR, `${name}.yaml`)];
    const found = [];
    for (const path of candidates) {
      try {
        const info = await lstat(path);
        if (info.isSymbolicLink() || !info.isFile()) throw new Error(`workspace workflow must be a regular non-symlink file: ${path}`);
        const actual = await realpath(path);
        if (!inside(root, actual)) throw new Error(`workspace workflow escapes the active workspace: ${path}`);
        found.push(path);
      } catch (error) { if (error?.code !== "ENOENT") throw error; }
    }
    if (found.length > 1) throw new Error(`workspace contains both ${name}.yml and ${name}.yaml; keep exactly one workflow file`);
    if (found.length === 1) return { path: found[0], text: await readFile(found[0], "utf8"), origin: { kind: "workspace", workspace: root, manifestPath: found[0] } };
    const configuredDir = join(projectsDir, "workflows");
    const path = join(configuredDir, `${name}.yaml`);
    try { return { path, text: await readFile(path, "utf8"), origin: { kind: "configured", workspace: null, manifestPath: path } }; }
    catch {
      let available = [];
      try { available = (await readdir(configuredDir)).filter((entry) => entry.endsWith(".yaml")).map((entry) => entry.slice(0, -5)).sort(); } catch { /* report none */ }
      throw new Error(`unknown workflow: ${name} (available: ${available.join(", ") || "none"})`);
    }
  }
  return Object.freeze({ projectsDir, loadProject, listProjects, loadWorkflow, canonicalWorkspace });
}

export { SAFE_NAME_RE as PROJECT_SOURCE_SAFE_NAME_RE, WORKSPACE_MANIFEST_NAMES, WORKSPACE_WORKFLOW_DIR, canonicalWorkspace, configuredProjectsDir, inside, loadConfiguredProject, loadWorkspaceProject };
