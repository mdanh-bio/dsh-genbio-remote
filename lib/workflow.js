// ── Conservative workflow/DAG schema + planner (Phase 2 foundation) ──────────
// A workflow is a DAG of (project, operation) nodes. Nodes are declared in a
// user-owned YAML file under the projects directory's workflows/ subdirectory
// (mirroring the one-manifest-per-project convention):
//
//   schema_version: 1
//   workflow: demo-pipeline
//   nodes:
//     - id: prepare
//       project: demo
//       operation: prepare
//     - id: run
//       project: demo
//       operation: run
//       depends_on: [prepare]
//
// Conservative planner semantics (local-only, ZERO execution surface):
//  * parseWorkflow FAILS CLOSED on: unknown fields, unsafe names, duplicate
//    node ids, duplicate (project, operation) pairs, unknown dependencies,
//    self-dependencies, and dependency CYCLES (Kahn topological detection).
//  * planWorkflow only MARKS nodes: a node is "ready" when every one of its
//    dependencies is in the caller-supplied completed set; otherwise it stays
//    "blocked" with its unsatisfied dependencies listed. The planner never
//    executes, submits, schedules, mutates state, or touches the network —
//    it takes a manifest reader (local file reads) only to prove each node's
//    operation exists, and returns a frozen plan.
//  * The exact-once pair lock identity REMAINS project+operation: pairKey is
//    derived as "<project>/<operation>" and each pair may appear at most once
//    per workflow, so two nodes can never race for the same pair lock.
import { load as parseYaml } from "js-yaml";
import { readFile, readdir } from "node:fs/promises";
import { join } from "node:path";

const SAFE_NAME_RE = /^[a-z0-9][a-z0-9-]*$/u;
const MAX_NODES = 32;
const MAX_DEPS_PER_NODE = 32;

function plainObject(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function assertKeys(value, allowed, label) {
  for (const key of Object.keys(value)) if (!allowed.has(key)) throw new Error(`${label}: unknown field ${key}`);
}

function assertSafeName(value, label) {
  if (typeof value !== "string" || !SAFE_NAME_RE.test(value)) throw new Error(`${label} must be kebab-case [a-z0-9-]`);
}

/** Detect a dependency cycle (including self-dependencies) via Kahn's algorithm. */
function assertAcyclic(nodes) {
  const indegree = new Map(nodes.map((node) => [node.id, node.dependsOn.length]));
  const dependents = new Map(nodes.map((node) => [node.id, []]));
  for (const node of nodes) for (const dep of node.dependsOn) dependents.get(dep).push(node.id);
  const queue = nodes.filter((node) => indegree.get(node.id) === 0).map((node) => node.id);
  let visited = 0;
  while (queue.length > 0) {
    const id = queue.shift();
    visited += 1;
    for (const next of dependents.get(id)) {
      const remaining = indegree.get(next) - 1;
      indegree.set(next, remaining);
      if (remaining === 0) queue.push(next);
    }
  }
  if (visited !== nodes.length) {
    const stuck = nodes.filter((node) => indegree.get(node.id) > 0).map((node) => node.id);
    throw new Error(`workflow contains a dependency cycle involving: ${stuck.join(", ")}`);
  }
}

/** Strict fail-closed parse of one workflow definition mapping. */
export function parseWorkflow(definition) {
  if (!plainObject(definition)) throw new Error("workflow must be a mapping");
  assertKeys(definition, new Set(["schema_version", "workflow", "nodes"]), "workflow");
  if (definition.schema_version !== 1) throw new Error("workflow schema_version must be 1");
  assertSafeName(definition.workflow, "workflow name");
  if (!Array.isArray(definition.nodes) || definition.nodes.length === 0 || definition.nodes.length > MAX_NODES) throw new Error(`workflow must define 1..${MAX_NODES} nodes`);
  const ids = new Set();
  const pairs = new Map();
  const nodes = [];
  for (const [index, node] of definition.nodes.entries()) {
    if (!plainObject(node)) throw new Error(`workflow node ${index} must be a mapping`);
    assertKeys(node, new Set(["id", "project", "operation", "depends_on"]), `workflow node ${index}`);
    assertSafeName(node.id, `workflow node ${index} id`);
    assertSafeName(node.project, `workflow node ${index} project`);
    assertSafeName(node.operation, `workflow node ${index} operation`);
    if (ids.has(node.id)) throw new Error(`duplicate workflow node id: ${node.id}`);
    ids.add(node.id);
    const pairKey = `${node.project}/${node.operation}`;
    if (pairs.has(pairKey)) throw new Error(`workflow node ${node.id} reuses the (project, operation) pair ${pairKey}; the pair lock identity remains project+operation and each pair may appear at most once`);
    pairs.set(pairKey, node.id);
    let dependsOn = [];
    if (node.depends_on !== undefined) {
      if (!Array.isArray(node.depends_on) || node.depends_on.length > MAX_DEPS_PER_NODE) throw new Error(`workflow node ${node.id} depends_on must be an array of at most ${MAX_DEPS_PER_NODE} node ids`);
      const seen = new Set();
      for (const dep of node.depends_on) {
        if (typeof dep !== "string" || !SAFE_NAME_RE.test(dep)) throw new Error(`workflow node ${node.id} has an unsafe dependency id: ${dep}`);
        if (seen.has(dep)) throw new Error(`workflow node ${node.id} lists dependency ${dep} twice`);
        seen.add(dep);
      }
      dependsOn = node.depends_on;
    }
    nodes.push(Object.freeze({ id: node.id, project: node.project, operation: node.operation, pairKey, dependsOn: Object.freeze([...dependsOn]) }));
  }
  // Unknown dependencies (including self-dependencies, caught as cycles
  // below): a node may only depend on nodes declared in the same workflow.
  for (const node of nodes) for (const dep of node.dependsOn) {
    if (dep === node.id) throw new Error(`workflow node ${node.id} depends on itself (cycle)`);
    if (!ids.has(dep)) throw new Error(`workflow node ${node.id} depends on unknown node: ${dep}`);
  }
  assertAcyclic(nodes);
  return Object.freeze({ schema: "genbio-workflow/1", name: definition.workflow, nodes: Object.freeze(nodes), pairs: Object.freeze(Object.fromEntries(pairs)) });
}

/**
 * Plan one parsed workflow. Pure and side-effect-free:
 *  * every node's (project, operation) must exist in its project manifest
 *    (via the local manifest reader `resolveProject`);
 *  * a node is "ready" iff all its dependencies are in `completed`;
 *  * completed nodes are reported as "completed" (informational);
 *  * everything else is "blocked" with its unsatisfied dependencies.
 * Returns a frozen plan. Nothing is submitted, executed, or stored.
 */
export async function planWorkflow(workflow, { resolveProject, completed = [] } = {}) {
  if (!workflow || workflow.schema !== "genbio-workflow/1" || !Array.isArray(workflow.nodes)) throw new Error("planWorkflow requires a parsed workflow");
  if (typeof resolveProject !== "function") throw new Error("planWorkflow requires a manifest reader (resolveProject)");
  const knownIds = new Set(workflow.nodes.map((node) => node.id));
  if (!Array.isArray(completed)) throw new Error("completed must be an array of node ids");
  const completedSet = new Set(completed);
  for (const id of completedSet) if (!knownIds.has(id)) throw new Error(`completed set references unknown workflow node: ${id}`);
  const planned = [];
  for (const node of workflow.nodes) {
    let manifest;
    try {
      manifest = await resolveProject(node.project);
    } catch (error) {
      throw new Error(`workflow node ${node.id}: project ${node.project} unavailable: ${String(error?.message ?? error)}`);
    }
    if (!manifest || !manifest.jobs || !Object.hasOwn(manifest.jobs, node.operation)) throw new Error(`workflow node ${node.id}: project ${node.project} has no operation ${node.operation}`);
    const waitingOn = node.dependsOn.filter((dep) => !completedSet.has(dep));
    const status = completedSet.has(node.id) ? "completed" : waitingOn.length === 0 ? "ready" : "blocked";
    planned.push(Object.freeze({ id: node.id, project: node.project, operation: node.operation, pairKey: node.pairKey, dependsOn: node.dependsOn, status, waitingOn: Object.freeze(waitingOn) }));
  }
  const byStatus = (status) => Object.freeze(planned.filter((node) => node.status === status).map((node) => node.id));
  return Object.freeze({
    schema: "genbio-workflow-plan/1",
    workflow: workflow.name,
    nodes: Object.freeze(planned),
    ready: byStatus("ready"),
    blocked: byStatus("blocked"),
    completed: byStatus("completed"),
  });
}

/** Load + parse one workflow file from the workflows/ subdirectory. The
 *  workflow name must match the filename (same convention as manifests). */
export async function loadWorkflowFile(workflowsDir, name) {
  if (typeof name !== "string" || !SAFE_NAME_RE.test(name)) throw new Error(`invalid workflow name: ${name}`);
  const path = join(workflowsDir, `${name}.yaml`);
  let text;
  try {
    text = await readFile(path, "utf8");
  } catch {
    let available = [];
    try {
      available = (await readdir(workflowsDir)).filter((entry) => entry.endsWith(".yaml")).map((entry) => entry.replace(/\.yaml$/u, "")).sort();
    } catch { /* directory unreadable: report below */ }
    throw new Error(`unknown workflow: ${name} (available: ${available.join(", ") || "none"})`);
  }
  let parsed;
  try {
    parsed = parseYaml(text);
  } catch (error) {
    throw new Error(`${name}: invalid workflow YAML: ${String(error?.reason ?? error?.message ?? error)}`);
  }
  const workflow = parseWorkflow(parsed);
  if (workflow.name !== name) throw new Error(`${name}: workflow name must match the filename`);
  return workflow;
}

export { MAX_NODES as MAX_WORKFLOW_NODES, MAX_DEPS_PER_NODE as MAX_WORKFLOW_DEPS };
