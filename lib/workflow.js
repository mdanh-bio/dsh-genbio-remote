// ── Conservative workflow/DAG schema + immutable planner foundation ──────────
// Schema v2 provides bounded plain JSON parameters and content-addressed
// operation resolutions. Planning is
// local and pure: it never submits, persists, schedules, or touches the network.
import { load as parseYaml } from "js-yaml";
import { readFile, readdir } from "node:fs/promises";
import { join } from "node:path";
import { canonicalJson, planHashOf } from "./project.js";

const SAFE_NAME_RE = /^[a-z0-9][a-z0-9-]*$/u;
const HEX64_RE = /^[a-f0-9]{64}$/u;
const MAX_NODES = 32;
const MAX_DEPS_PER_NODE = 32;
const MAX_PARAMETER_KEYS = 32;
const MAX_PARAMETER_DEPTH = 4;
const MAX_PARAMETER_ARRAY = 32;
const MAX_PARAMETER_STRING_CHARS = 1024;
const MAX_PARAMETERS_BYTES = 8192;

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

function freezeJson(value) {
  if (Array.isArray(value)) return Object.freeze(value.map(freezeJson));
  if (plainObject(value)) return Object.freeze(Object.fromEntries(Object.entries(value).map(([key, item]) => [key, freezeJson(item)])));
  return value;
}

function normalizeParameterValue(value, label, depth = 0) {
  if (depth > MAX_PARAMETER_DEPTH) throw new Error(`${label} exceeds maximum nesting depth ${MAX_PARAMETER_DEPTH}`);
  if (value === null || typeof value === "boolean") return value;
  if (typeof value === "number") {
    if (!Number.isSafeInteger(value)) throw new Error(`${label} numbers must be safe integers`);
    return value;
  }
  if (typeof value === "string") {
    if (value.length > MAX_PARAMETER_STRING_CHARS) throw new Error(`${label} string exceeds ${MAX_PARAMETER_STRING_CHARS} characters`);
    return value;
  }
  if (Array.isArray(value)) {
    if (value.length > MAX_PARAMETER_ARRAY) throw new Error(`${label} array exceeds ${MAX_PARAMETER_ARRAY} items`);
    return value.map((item, index) => normalizeParameterValue(item, `${label}[${index}]`, depth + 1));
  }
  if (plainObject(value)) {
    const keys = Object.keys(value);
    if (keys.length > MAX_PARAMETER_KEYS) throw new Error(`${label} exceeds ${MAX_PARAMETER_KEYS} keys`);
    const out = {};
    for (const key of keys) {
      if (!SAFE_NAME_RE.test(key)) throw new Error(`${label} has unsafe key ${key}`);
      out[key] = normalizeParameterValue(value[key], `${label}.${key}`, depth + 1);
    }
    return out;
  }
  throw new Error(`${label} must contain only plain JSON values`);
}

function normalizeParameters(value, label) {
  if (!plainObject(value)) throw new Error(`${label} must be a plain mapping`);
  const normalized = normalizeParameterValue(value, label);
  if (Buffer.byteLength(canonicalJson(normalized), "utf8") > MAX_PARAMETERS_BYTES) throw new Error(`${label} exceeds ${MAX_PARAMETERS_BYTES} canonical bytes`);
  return freezeJson(normalized);
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

/** Strict fail-closed parse of a schema-v2 workflow definition. */
export function parseWorkflow(definition) {
  if (!plainObject(definition)) throw new Error("workflow must be a mapping");
  assertKeys(definition, new Set(["schema_version", "workflow", "nodes"]), "workflow");
  const version = definition.schema_version;
  if (version !== 2) throw new Error("workflow schema_version 2 is required; schema_version 1 is no longer supported");
  assertSafeName(definition.workflow, "workflow name");
  if (!Array.isArray(definition.nodes) || definition.nodes.length === 0 || definition.nodes.length > MAX_NODES) throw new Error(`workflow must define 1..${MAX_NODES} nodes`);
  const ids = new Set();
  const pairs = new Map();
  const nodes = [];
  for (const [index, node] of definition.nodes.entries()) {
    if (!plainObject(node)) throw new Error(`workflow node ${index} must be a mapping`);
    assertKeys(node, new Set(["id", "project", "operation", "depends_on", "parameters"]), `workflow node ${index}`);
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
    const parsed = { id: node.id, project: node.project, operation: node.operation, pairKey, dependsOn: Object.freeze([...dependsOn]) };
    parsed.parameters = node.parameters === undefined ? Object.freeze({}) : normalizeParameters(node.parameters, `workflow node ${node.id} parameters`);
    nodes.push(Object.freeze(parsed));
  }
  for (const node of nodes) for (const dep of node.dependsOn) {
    if (dep === node.id) throw new Error(`workflow node ${node.id} depends on itself (cycle)`);
    if (!ids.has(dep)) throw new Error(`workflow node ${node.id} depends on unknown node: ${dep}`);
  }
  assertAcyclic(nodes);
  return Object.freeze({ schema: "genbio-workflow/2", name: definition.workflow, nodes: Object.freeze(nodes), pairs: Object.freeze(Object.fromEntries(pairs)) });
}

function normalizeResources(resources, label) {
  if (!plainObject(resources)) throw new Error(`${label} resources must be a mapping`);
  assertKeys(resources, new Set(["cpus", "gpus", "mem_gb", "concurrency"]), `${label} resources`);
  const out = {};
  for (const key of ["cpus", "gpus", "mem_gb", "concurrency"]) if (resources[key] !== undefined) {
    if (!Number.isSafeInteger(resources[key]) || resources[key] < 0) throw new Error(`${label} resources.${key} must be a non-negative safe integer`);
    out[key] = resources[key];
  }
  return Object.freeze(out);
}

function normalizeResolution(value, node) {
  if (!plainObject(value)) throw new Error(`workflow node ${node.id}: resolveOperation must return a mapping`);
  assertKeys(value, new Set(["planHash", "plan_hash", "resources", "origin"]), `workflow node ${node.id} operation resolution`);
  const planHash = value.planHash ?? value.plan_hash;
  if (typeof planHash !== "string" || !HEX64_RE.test(planHash)) throw new Error(`workflow node ${node.id}: operation plan hash must be a full 64-hex SHA-256 digest`);
  if (value.planHash !== undefined && value.plan_hash !== undefined && value.planHash !== value.plan_hash) throw new Error(`workflow node ${node.id}: conflicting operation plan hashes`);
  if (typeof value.origin !== "string" || value.origin.length === 0 || value.origin.length > 256) throw new Error(`workflow node ${node.id}: operation origin must be a non-empty string of at most 256 characters`);
  return Object.freeze({ planHash, resources: normalizeResources(value.resources, `workflow node ${node.id}`), origin: value.origin });
}

/** Canonical hash payload for an immutable schema-v2 workflow plan. */
export function canonicalWorkflowPlan(value) {
  return canonicalJson(value);
}

export function workflowPlanHashOf(value) {
  return planHashOf(value);
}

/** Plan one parsed schema-v2 workflow. */
export async function planWorkflow(workflow, { resolveOperation } = {}) {
  if (!workflow || workflow.schema !== "genbio-workflow/2" || !Array.isArray(workflow.nodes)) throw new Error("planWorkflow requires a parsed schema-v2 workflow");
  if (typeof resolveOperation !== "function") throw new Error("schema-v2 workflow planning requires resolveOperation");
  const planned = [];
  for (const node of workflow.nodes) {
    let resolution;
    try {
      resolution = normalizeResolution(await resolveOperation({ project: node.project, operation: node.operation, parameters: node.parameters, nodeId: node.id }), node);
    } catch (error) {
      throw new Error(`workflow node ${node.id}: operation resolution failed: ${String(error?.message ?? error)}`);
    }
    const status = node.dependsOn.length === 0 ? "ready" : "blocked";
    planned.push(Object.freeze({ id: node.id, project: node.project, operation: node.operation, pairKey: node.pairKey, dependsOn: node.dependsOn, status, waitingOn: node.dependsOn, parameters: node.parameters, operation_plan_hash: resolution.planHash, resources: resolution.resources, origin: resolution.origin }));
  }
  const byStatus = (status) => Object.freeze(planned.filter((node) => node.status === status).map((node) => node.id));
  const immutable = { schema: "genbio-workflow-plan-hash/1", workflow: workflow.name, nodes: planned.map((node) => ({ id: node.id, project: node.project, operation: node.operation, pairKey: node.pairKey, dependsOn: node.dependsOn, parameters: node.parameters, operation_plan_hash: node.operation_plan_hash, resources: node.resources, origin: node.origin })) };
  return Object.freeze({ schema: "genbio-workflow-plan/2", workflow: workflow.name, nodes: Object.freeze(planned), ready: byStatus("ready"), blocked: byStatus("blocked"), completed: Object.freeze([]), workflow_plan_hash: workflowPlanHashOf(immutable) });
}

/** Load + parse one workflow file; workflow name must match filename. */
export async function loadWorkflowFile(workflowsDir, name) {
  if (typeof name !== "string" || !SAFE_NAME_RE.test(name)) throw new Error(`invalid workflow name: ${name}`);
  const path = join(workflowsDir, `${name}.yaml`);
  let text;
  try { text = await readFile(path, "utf8"); }
  catch {
    let available = [];
    try { available = (await readdir(workflowsDir)).filter((entry) => entry.endsWith(".yaml")).map((entry) => entry.replace(/\.yaml$/u, "")).sort(); } catch { /* report below */ }
    throw new Error(`unknown workflow: ${name} (available: ${available.join(", ") || "none"})`);
  }
  let parsed;
  try { parsed = parseYaml(text); }
  catch (error) { throw new Error(`${name}: invalid workflow YAML: ${String(error?.reason ?? error?.message ?? error)}`); }
  const workflow = parseWorkflow(parsed);
  if (workflow.name !== name) throw new Error(`${name}: workflow name must match the filename`);
  return workflow;
}

export {
  MAX_NODES as MAX_WORKFLOW_NODES,
  MAX_DEPS_PER_NODE as MAX_WORKFLOW_DEPS,
  MAX_PARAMETER_KEYS as MAX_WORKFLOW_PARAMETER_KEYS,
  MAX_PARAMETER_DEPTH as MAX_WORKFLOW_PARAMETER_DEPTH,
  MAX_PARAMETERS_BYTES as MAX_WORKFLOW_PARAMETERS_BYTES,
};
