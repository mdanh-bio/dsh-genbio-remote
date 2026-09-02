// Controlled workflow planning/execution. Schema v1 remains read-only. Schema
// v2 resolves immutable operation plans and advances at most one node per
// explicit execute/advance call through the existing project execution tool.
import { createHash } from "node:crypto";
import { load as parseYaml } from "js-yaml";
import { buildOperationPlan, resolveRecipe } from "./project.js";
import { createProjectSource } from "./project-source.js";
import { parseWorkflow, planWorkflow } from "./workflow.js";
import { validatePinnedSbatch } from "./slurm-policy.js";
import { securePackageInventory } from "./secure-package.js";

const MAX_SESSION_WORKFLOW_PLANS = 16;
const TERMINAL_NODE = new Set(["completed", "failed", "cancelled", "skipped"]);
const ACTIVE_NODE = new Set(["submitting", "submitted", "pending", "running", "reconciling", "cancel-requested"]);
function sha256(value) { return createHash("sha256").update(value).digest("hex"); }
function clone(value) { return JSON.parse(JSON.stringify(value)); }
function sessionIdOf(exec) { return exec?.agent?.session?.header?.id ?? exec?.agent?.session?.id ?? exec?.agent?.id; }
function plansOf(state) { if (!Array.isArray(state.workflowPlans)) state.workflowPlans = []; return state.workflowPlans; }
function storePlan(state, record) {
  const plans = plansOf(state);
  const duplicate = plans.findIndex((item) => item.planHash === record.planHash);
  if (duplicate >= 0) plans.splice(duplicate, 1);
  plans.push(record);
  while (plans.length > MAX_SESSION_WORKFLOW_PLANS) plans.shift();
}
export function workflowPublic(record, planRecord = null) {
  const nodes = record.nodes.map((node) => ({ node_id: node.nodeId, project: node.project, operation: node.operation, status: node.status, ...(node.runId ? { run_id: node.runId } : {}), ...(node.slurmJobId ? { job_id: node.slurmJobId } : {}), ...(node.slurmState ? { slurm_state: node.slurmState } : {}), ...(node.exitCode ? { exit_code: node.exitCode } : {}), ...(node.evidence ? { evidence: node.evidence } : {}), depends_on: planRecord?.plan.nodes.find((item) => item.id === node.nodeId)?.dependsOn ?? [] }));
  const count = (statuses) => nodes.filter((node) => statuses.has(node.status)).length;
  return { workflow_run_id: record.workflowRunId, workflow: record.workflow, plan_hash: record.workflowPlanHash, status: record.status, nodes, counts: { ready: count(new Set(["ready"])), active: count(ACTIVE_NODE), completed: count(new Set(["completed"])), failed: count(new Set(["failed", "cancelled"])), blocked: count(new Set(["blocked", "reconciling"])) } };
}
function parseWorkflowText(name, loaded) {
  let parsed;
  try { parsed = parseYaml(loaded.text); } catch (error) { throw new Error(`${name}: invalid workflow YAML: ${String(error?.reason ?? error?.message ?? error)}`); }
  const workflow = parseWorkflow(parsed);
  if (workflow.name !== name) throw new Error(`${name}: workflow name must match the filename`);
  return workflow;
}
function refreshReadiness(record, planRecord) {
  const nodeById = new Map(record.nodes.map((node) => [node.nodeId, node]));
  for (const planned of planRecord.plan.nodes) {
    const node = nodeById.get(planned.id);
    if (!node || TERMINAL_NODE.has(node.status) || ACTIVE_NODE.has(node.status)) continue;
    const deps = planned.dependsOn.map((id) => nodeById.get(id));
    const failed = deps.some((dep) => dep && ["failed", "cancelled", "skipped"].includes(dep.status));
    const ready = deps.every((dep) => dep?.status === "completed");
    node.status = failed ? "blocked" : ready ? "ready" : "blocked";
  }
}

export function createWorkflowTools({ makeTool, requirePolicy, requireState, publicState, config, projectSource: suppliedProjectSource, projectPlanTool, projectExecuteTool, projectStatusTool, projectCancelTool, workflowRegistry }) {
  const projectSource = suppliedProjectSource ?? createProjectSource(config);

  async function resolveWorkflowPlan(name, exec, state) {
    const policy = requirePolicy();
    if (!state.policy?.hash) throw new Error("current policy hash is unavailable");
    const loadedWorkflow = await projectSource.loadWorkflow(name, exec);
    const workflow = parseWorkflowText(name, loadedWorkflow);
    const nodeRecords = [];
    const plan = await planWorkflow(workflow, { resolveOperation: async ({ project, operation, parameters, nodeId }) => {
      const loaded = await projectSource.loadProject(project, exec);
      if (loaded.manifest.schemaVersion !== 2) throw new Error(`${project}: workflow execution requires schema_version 2`);
      const jobSpec = loaded.manifest.jobs[operation];
      if (!jobSpec?.recipe) throw new Error(`${project}: workflow operation ${operation} must be a declarative recipe`);
      const resolution = resolveRecipe({ manifest: loaded.manifest, operation, parameters, policy, envelope: state.envelope });
      validatePinnedSbatch(resolution.sbatchText, jobSpec, { policy, envelope: state.envelope });
      const inventory = await securePackageInventory(loaded.manifest, loaded.manifestSha);
      const built = buildOperationPlan({ project, operation, policyHash: state.policy.hash, manifestSha: loaded.manifestSha, packageSha: inventory.packageSha, resolution });
      nodeRecords.push({ nodeId, project, operation, parameters: clone(parameters), operationPlanHash: built.planHash, manifestPath: loaded.path, origin: loaded.origin.kind, workspace: loaded.origin.workspace });
      return { planHash: built.planHash, resources: { cpus: jobSpec.cpus, gpus: jobSpec.gpus, concurrency: jobSpec.concurrency }, origin: loaded.origin.kind };
    }});
    return { workflow, plan, loadedWorkflow, nodeRecords };
  }

  const planTool = makeTool("genbio_workflow_plan", "Plan a schema-v2 workflow by resolving every node into an immutable policy-validated operation plan and storing one session-owned workflow plan. No transfer, allocation, or submission occurs.", { workflow: { type: "string", required: true } }, async (args, exec) => {
    const state = requireState(exec);
    const resolved = await resolveWorkflowPlan(String(args.workflow), exec, state);
    const record = Object.freeze({ planHash: resolved.plan.workflow_plan_hash, plan: resolved.plan, workflowSha: sha256(resolved.loadedWorkflow.text), workflowPath: resolved.loadedWorkflow.path, workflowOrigin: resolved.loadedWorkflow.origin.kind, workspace: resolved.loadedWorkflow.origin.workspace, nodeRecords: Object.freeze(resolved.nodeRecords.map((item) => Object.freeze(item))), createdAt: Date.now(), status: "planned" });
    storePlan(state, record);
    if (!workflowRegistry) throw new Error("durable workflow registry is unavailable; refusing volatile-only planning");
    const sessionId = sessionIdOf(exec);
    let durable = (await workflowRegistry.list(sessionId)).find((item) => item.workflowPlanHash === record.planHash && !["completed", "failed", "cancelled"].includes(item.status));
    if (!durable) durable = await workflowRegistry.record(sessionId, { workflow: record.plan.workflow, workflowPlanHash: record.planHash, workflowPlan: record.plan, workflowSha: record.workflowSha, workflowPath: record.workflowPath, workflowOrigin: record.workflowOrigin, workspace: record.workspace, nodeRecords: record.nodeRecords, nodes: record.plan.nodes.map((node) => ({ nodeId: node.id, project: node.project, operation: node.operation, operationPlanHash: node.operation_plan_hash, status: node.status === "ready" ? "ready" : "blocked" })) });
    return { ok: true, status: { ...publicState(state), workflow_plan: clone(resolved.plan), workflow_origin: resolved.loadedWorkflow.origin.kind, workflow_run_id: durable.workflowRunId } };
  });

  async function startOrAdvance(planRecord, exec, state, existing = null) {
    if (!workflowRegistry || !projectPlanTool || !projectExecuteTool) throw new Error("workflow execution integration is unavailable; no operation was submitted");
    const fresh = await resolveWorkflowPlan(planRecord.plan.workflow, exec, state);
    if (fresh.plan.workflow_plan_hash !== planRecord.planHash || sha256(fresh.loadedWorkflow.text) !== planRecord.workflowSha || fresh.loadedWorkflow.path !== planRecord.workflowPath || fresh.loadedWorkflow.origin.kind !== planRecord.workflowOrigin || fresh.loadedWorkflow.origin.workspace !== planRecord.workspace) throw new Error("workflow plan drift: workflow, project, policy, parameters, or source changed; nothing was submitted");
    const sessionId = sessionIdOf(exec);
    let record = existing ?? (await workflowRegistry.list(sessionId)).find((item) => item.workflowPlanHash === planRecord.planHash && !["completed", "failed", "cancelled"].includes(item.status));
    if (!record) record = await workflowRegistry.record(sessionId, { workflow: planRecord.plan.workflow, workflowPlanHash: planRecord.planHash, workflowPlan: planRecord.plan, workflowSha: planRecord.workflowSha, workflowPath: planRecord.workflowPath, workflowOrigin: planRecord.workflowOrigin, workspace: planRecord.workspace, nodeRecords: planRecord.nodeRecords, nodes: planRecord.plan.nodes.map((node) => ({ nodeId: node.id, project: node.project, operation: node.operation, operationPlanHash: node.operation_plan_hash, status: node.status === "ready" ? "ready" : "blocked" })) });
    if (record.status === "paused") throw new Error("workflow is paused; resume it before advancing");
    refreshReadiness(record, planRecord);
    if (record.nodes.some((node) => ACTIVE_NODE.has(node.status))) return record;
    const ready = record.nodes.find((node) => node.status === "ready");
    if (!ready) return record;
    const persistedReady = (await workflowRegistry.list(sessionId)).find((item) => item.workflowRunId === record.workflowRunId)?.nodes.find((item) => item.nodeId === ready.nodeId);
    if (persistedReady?.status === "blocked") await workflowRegistry.updateNode(sessionId, record.workflowRunId, ready.nodeId, { status: "ready" });
    const nodeRecord = planRecord.nodeRecords.find((item) => item.nodeId === ready.nodeId);
    await workflowRegistry.updateNode(sessionId, record.workflowRunId, ready.nodeId, { status: "submitting" });
    try {
      const planned = await projectPlanTool.execute({ project: ready.project, operation: ready.operation, parameters: nodeRecord.parameters }, exec);
      const operationPlanHash = planned.status.planned.plan_hash;
      if (operationPlanHash !== ready.operationPlanHash) throw new Error("workflow node operation plan drift; nothing was submitted");
      const started = await projectExecuteTool.execute({ plan_hash: operationPlanHash }, exec);
      const run = started.status.started;
      const durableRunId = started.status.durable_run?.run_id ?? run.runId ?? null;
      await workflowRegistry.updateNode(sessionId, record.workflowRunId, ready.nodeId, { status: run.workloadStatus === "unknown" ? "reconciling" : "submitted", runId: durableRunId, slurmJobId: run.slurmJobId ?? null });
    } catch (error) {
      await workflowRegistry.updateNode(sessionId, record.workflowRunId, ready.nodeId, { status: "failed", evidence: "definite-pre-submission-failure" });
      throw error;
    }
    if (record.status === "planned") await workflowRegistry.updateWorkflow(sessionId, record.workflowRunId, { status: "running" });
    return (await workflowRegistry.list(sessionId)).find((item) => item.workflowRunId === record.workflowRunId);
  }

  const executeTool = makeTool("genbio_workflow_execute", "Execute one immutable session-owned schema-v2 workflow plan. Fresh drift checks run first, then at most one dependency-ready node is submitted through the existing project exact-once core.", { plan_hash: { type: "string", required: true } }, async (args, exec) => {
    const state = requireState(exec); requirePolicy();
    const planRecord = plansOf(state).find((item) => item.planHash === String(args.plan_hash));
    if (!planRecord) throw new Error("unknown or expired session-owned workflow plan hash");
    const record = await startOrAdvance(planRecord, exec, state);
    return { ok: true, status: { ...publicState(state), workflow_run: workflowPublic(record, planRecord) } };
  });

  const statusTool = makeTool("genbio_workflow_status", "Show and reconcile one durable workflow run. It may collect bounded scheduler evidence for its one active owned node, but never submits the next node.", { workflow_run_id: { type: "string", required: true } }, async (args, exec) => {
    const state = requireState(exec); requirePolicy();
    const sessionId = sessionIdOf(exec);
    let record = (await workflowRegistry.list(sessionId)).find((item) => item.workflowRunId === String(args.workflow_run_id));
    if (!record) throw new Error(`unknown session-owned workflow run ${args.workflow_run_id}`);
    let planRecord = plansOf(state).find((item) => item.planHash === record.workflowPlanHash);
    if (!planRecord && record.workflowPlan) {
      planRecord = Object.freeze({ planHash: record.workflowPlanHash, plan: record.workflowPlan, workflowSha: record.workflowSha, workflowPath: record.workflowPath, workflowOrigin: record.workflowOrigin, workspace: record.workspace, nodeRecords: Object.freeze(record.nodeRecords.map(Object.freeze)), createdAt: record.createdAt, status: "durable" });
      storePlan(state, planRecord);
    }
    if (!planRecord) throw new Error("workflow plan is unavailable from both session and durable registry; refusing to infer dependencies");
    for (const node of record.nodes.filter((item) => ACTIVE_NODE.has(item.status) && (item.runId || item.slurmJobId))) {
      if (!projectStatusTool) break;
      await projectStatusTool.execute(node.runId ? { project: node.project, run_id: node.runId } : { project: node.project, operation: node.operation, job_id: node.slurmJobId }, exec);
      const run = [...(state.runs ?? [])].reverse().find((item) => (node.runId && item.runId === node.runId) || (item.slurmJobId === node.slurmJobId && String(item.operation) === `project-${node.project}-${node.operation}`));
      if (!run) continue;
      const next = run.workloadStatus === "completed" ? "completed" : run.workloadStatus === "cancelled" ? "cancelled" : ["failed", "evidence-incomplete"].includes(run.workloadStatus) ? "failed" : ["pending", "running"].includes(run.workloadStatus) ? run.workloadStatus : "reconciling";
      if (next !== node.status) await workflowRegistry.updateNode(sessionId, record.workflowRunId, node.nodeId, { status: next, runId: node.runId, slurmJobId: run.slurmJobId ?? node.slurmJobId, slurmState: run.slurmStatus ?? null, exitCode: run.slurmExitCode ?? null, evidence: run.workloadEvidence ?? null });
    }
    record = (await workflowRegistry.list(sessionId)).find((item) => item.workflowRunId === record.workflowRunId);
    refreshReadiness(record, planRecord);
    for (const node of record.nodes) {
      const persisted = (await workflowRegistry.list(sessionId)).find((item) => item.workflowRunId === record.workflowRunId)?.nodes.find((item) => item.nodeId === node.nodeId);
      if (node.status === "ready" && persisted?.status === "blocked") await workflowRegistry.updateNode(sessionId, record.workflowRunId, node.nodeId, { status: "ready" });
    }
    record = (await workflowRegistry.list(sessionId)).find((item) => item.workflowRunId === record.workflowRunId);
    if (record.nodes.every((node) => node.status === "completed") && !["completed", "failed", "cancelled"].includes(record.status)) record = await workflowRegistry.updateWorkflow(sessionId, record.workflowRunId, { status: "completed" });
    return { ok: true, status: { ...publicState(state), workflow_run: workflowPublic(record, planRecord) } };
  });

  const advanceTool = makeTool("genbio_workflow_advance", "Advance one durable workflow by submitting at most one currently ready node. Status reconciliation and completion evidence must make the node ready first.", { workflow_run_id: { type: "string", required: true } }, async (args, exec) => {
    const state = requireState(exec); requirePolicy();
    const sessionId = sessionIdOf(exec);
    const existing = (await workflowRegistry.list(sessionId)).find((item) => item.workflowRunId === String(args.workflow_run_id));
    if (!existing) throw new Error(`unknown session-owned workflow run ${args.workflow_run_id}`);
    let planRecord = plansOf(state).find((item) => item.planHash === existing.workflowPlanHash);
    if (!planRecord && existing.workflowPlan) {
      planRecord = Object.freeze({ planHash: existing.workflowPlanHash, plan: existing.workflowPlan, workflowSha: existing.workflowSha, workflowPath: existing.workflowPath, workflowOrigin: existing.workflowOrigin, workspace: existing.workspace, nodeRecords: Object.freeze(existing.nodeRecords.map(Object.freeze)), createdAt: existing.createdAt, status: "durable" });
      storePlan(state, planRecord);
    }
    if (!planRecord) throw new Error("workflow plan is unavailable from both session and durable registry");
    const record = await startOrAdvance(planRecord, exec, state, existing);
    return { ok: true, status: { ...publicState(state), workflow_run: workflowPublic(record, planRecord) } };
  });

  const pauseTool = makeTool("genbio_workflow_pause", "Pause local workflow advancement. This does not cancel an already submitted Slurm job.", { workflow_run_id: { type: "string", required: true } }, async (args, exec) => {
    const state = requireState(exec); const record = await workflowRegistry.updateWorkflow(sessionIdOf(exec), String(args.workflow_run_id), { status: "paused" });
    return { ok: true, status: { ...publicState(state), workflow_run: workflowPublic(record, plansOf(state).find((item) => item.planHash === record.workflowPlanHash)) } };
  });
  const resumeTool = makeTool("genbio_workflow_resume", "Resume local workflow advancement. Resuming does not submit; call advance separately.", { workflow_run_id: { type: "string", required: true } }, async (args, exec) => {
    const state = requireState(exec); const record = await workflowRegistry.updateWorkflow(sessionIdOf(exec), String(args.workflow_run_id), { status: "running" });
    return { ok: true, status: { ...publicState(state), workflow_run: workflowPublic(record, plansOf(state).find((item) => item.planHash === record.workflowPlanHash)) } };
  });
  const cancelTool = makeTool("genbio_workflow_cancel", "Cancel exactly one active session-owned workflow node through the existing ownership-checked cancellation tool.", { workflow_run_id: { type: "string", required: true }, node_id: { type: "string", required: true } }, async (args, exec) => {
    const state = requireState(exec); const sessionId = sessionIdOf(exec);
    const record = (await workflowRegistry.list(sessionId)).find((item) => item.workflowRunId === String(args.workflow_run_id));
    const node = record?.nodes.find((item) => item.nodeId === String(args.node_id));
    if (!node?.slurmJobId || !ACTIVE_NODE.has(node.status)) throw new Error("workflow node is not an active owned Slurm job");
    if (!projectCancelTool) throw new Error("workflow cancellation integration is unavailable");
    const result = await projectCancelTool.execute({ project: node.project, operation: node.operation, job_id: node.slurmJobId }, exec);
    await workflowRegistry.updateNode(sessionId, record.workflowRunId, node.nodeId, { status: "cancel-requested" });
    return { ok: true, status: { ...publicState(state), workflow_run: workflowPublic((await workflowRegistry.list(sessionId)).find((item) => item.workflowRunId === record.workflowRunId), plansOf(state).find((item) => item.planHash === record.workflowPlanHash)), cancellation: result.status.cancellation } };
  });

  return { planTool, executeTool, statusTool, advanceTool, pauseTool, resumeTool, cancelTool, projectSource };
}

export { MAX_SESSION_WORKFLOW_PLANS };
