import { createHash } from "node:crypto";
import { readFile, stat } from "node:fs/promises";
import { join, resolve } from "node:path";
import { load as parseYaml } from "js-yaml";
import { defineTool } from "@deepseek-ai/dsh-tools";
import { z } from "zod";
import { createProjectSource } from "./project-source.js";
import { createProjectTools } from "./project-tools.js";
import { createProjectStatusTools } from "./project-status-tools.js";
import { parseSlurmDiscovery } from "./slurm-discovery.js";
import { createWorkflowRegistry } from "./workflow-registry.js";
import { createWorkflowTools } from "./workflow-tools.js";

const name = "dsh-genbio-remote";
const inject = ["tools", "shell", "timer", "systemPrompt"];
const TARGETS = ["HPC", "NHPC", "genbio_mdanh", "genbioh100"];
/** Fixed roots the policy smoke operations create their run dirs under. */
const HPC_SMOKE_ROOT = "/data01/genbiolab/mdanh/data/projects/dsh_policy_smoke";
const NHPC_SMOKE_ROOT = "/home/mdanh/.dsh/genbio-policy-smoke";
const DEFAULT_WORKFLOW_REGISTRY_DIR = "/Users/mdanh/.dsh/profiles/desktop/genbio-workflow-registry";
const TERMINAL_RUN_STATES = new Set(["completed", "failed", "killed"]);
const TERMINAL_WORKLOAD_STATES = new Set(["not_applicable", "completed", "failed", "cancelled"]);
const MEMORY_MODES = new Set(["manual", "off"]);
const ARTIFACT_KINDS = ["manifest", "log", "input", "output", "analysis", "figure", "other"];
const MAX_ARTIFACTS = 15;
const MAX_MEMORY_RECORD_CHARS = 22000;
const SSH_OPTIONS = ["-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "StrictHostKeyChecking=yes"];
const TOOL_OUTPUT = {
  type: "object",
  additionalProperties: false,
  properties: {
    ok: { type: "boolean", required: true },
    status: { type: "object", additionalProperties: true, required: true },
    error: { type: "string" },
  },
};

function jsonClone(value) {
  return JSON.parse(JSON.stringify(value));
}

function digest(text) {
  return createHash("sha256").update(text).digest("hex");
}

function bounded(value, label, max, required = false) {
  const text = String(value ?? "").replace(/\u0000/g, "").trim();
  if (required && !text) throw new Error(`${label} is required`);
  if (text.length > max) throw new Error(`${label} exceeds ${max} characters`);
  return text;
}

function boundedList(values, label, maxItems = 20, maxChars = 1000) {
  if (values === undefined) return [];
  if (!Array.isArray(values) || values.length > maxItems) throw new Error(`${label} must contain at most ${maxItems} items`);
  return values.map((value, index) => bounded(value, `${label}[${index}]`, maxChars, true));
}

function sanitizeArtifacts(values) {
  if (values === undefined) return [];
  if (!Array.isArray(values) || values.length > MAX_ARTIFACTS) throw new Error(`artifacts must contain at most ${MAX_ARTIFACTS} items`);
  return values.map((item, index) => {
    if (!item || typeof item !== "object") throw new Error(`artifacts[${index}] must be an object`);
    const kind = String(item.kind || "other");
    if (!ARTIFACT_KINDS.includes(kind)) throw new Error(`artifacts[${index}].kind is invalid`);
    const location = bounded(item.location, `artifacts[${index}].location`, 2048, true);
    if (/\b(?:authorization|bearer|api[_-]?key|password|private[_-]?key)\b/i.test(location)) throw new Error(`artifacts[${index}].location appears to contain sensitive material`);
    const sha256 = item.sha256 === undefined ? null : bounded(item.sha256, `artifacts[${index}].sha256`, 64, true).toLowerCase();
    if (sha256 !== null && !/^[a-f0-9]{64}$/.test(sha256)) throw new Error(`artifacts[${index}].sha256 must be a full SHA-256 digest`);
    return { kind, location, sha256, description: bounded(item.description, `artifacts[${index}].description`, 1000) || null };
  });
}

export function parseRunEvidence(run) {
  const stdout = String(run.stdout || "");
  const schedulerJobId = run.slurmJobId ?? stdout.match(/^JOB_ID=([^\s]+)$/m)?.[1] ?? null;
  const remoteRunDir = run.remoteRunDir ?? stdout.match(/^RUN_DIR=(.+)$/m)?.[1]?.trim() ?? null;
  const schedulerRow = stdout.split(/\r?\n/).find(line => schedulerJobId && line.startsWith(`${schedulerJobId}|`));
  const fields = schedulerRow?.split("|") ?? [];
  const terminalEvidence = run.slurmStatus ? { state: run.slurmStatus, exitCode: run.slurmExitCode ?? null, elapsed: run.slurmElapsed ?? null, workloadEvidence: run.workloadEvidence ?? null } : schedulerRow ? { state: fields[1] || null, exitCode: fields[2] || null } : null;
  return {
    schedulerJobId,
    remoteRunDir,
    terminalEvidence,
    errorSynopsis: run.error ? bounded(run.error, "error synopsis", 2000) : null,
  };
}

function markdownRecord(record) {
  const evidence = record.evidence;
  const lines = [
    `# Genbio run record: ${record.project}`,
    "",
    `Record ID: ${record.recordId}`,
    `Run ID: ${evidence.runId}`,
    `Status: ${evidence.status}`,
    "",
    "## Verified execution evidence",
    "",
    `- Target: ${evidence.target}`,
    `- Operation: ${evidence.operation}`,
    `- Node: ${evidence.node ?? "not recorded"}`,
    `- Partition: ${evidence.partition ?? "not applicable"}`,
    `- Started: ${new Date(evidence.startedAt).toISOString()}`,
    `- Finished: ${new Date(evidence.finishedAt).toISOString()}`,
    `- Elapsed milliseconds: ${evidence.elapsedMs}`,
    `- Policy SHA-256: ${evidence.policyHash ?? "unavailable"}`,
    `- Scheduler job ID: ${evidence.schedulerJobId ?? "not applicable"}`,
    `- Remote run directory: ${evidence.remoteRunDir ?? "not recorded"}`,
    ...(Array.isArray(evidence.remoteGrants) && evidence.remoteGrants.length > 0 ? [`- Remote folder grants in effect: ${evidence.remoteGrants.map((grant) => `${grant.target}:${grant.root} (${grant.mode})`).join("; ")}`] : []),
    "",
    "## Result summary",
    "",
    record.summary,
  ];
  if (record.significance) lines.push("", "## Interpretation and significance", "", record.significance);
  if (record.limitations.length) lines.push("", "## Limitations", "", ...record.limitations.map(item => `- ${item}`));
  if (record.nextSteps.length) lines.push("", "## Next steps", "", ...record.nextSteps.map(item => `- ${item}`));
  if (record.artifacts.length) {
    lines.push("", "## Artifact references", "");
    for (const artifact of record.artifacts) lines.push(`- ${artifact.kind}: ${artifact.location}${artifact.sha256 ? ` (sha256: ${artifact.sha256})` : ""}${artifact.description ? ` — ${artifact.description}` : ""}`);
  }
  if (evidence.errorSynopsis) lines.push("", "## Error synopsis", "", evidence.errorSynopsis);
  lines.push("", "## Machine-readable evidence", "", "```json", JSON.stringify({ schemaVersion: 1, ...evidence }, null, 2), "```", "");
  return lines.join("\n");
}

function normalizeTimestamp(value) {
  return value instanceof Date ? value.toISOString() : value ?? null;
}

function integer(value, label, min = 0) {
  if (!Number.isInteger(value) || value < min) throw new Error(`${label} must be an integer >= ${min}`);
  return value;
}

function sessionIdOf(exec) {
  const id = exec?.agent?.session?.id;
  if (typeof id !== "string" || id.length === 0) throw new Error("Genbio operation requires an owning session");
  return id;
}

function publicState(state) {
  return jsonClone({
    policy: state.policy,
    envelope: state.envelope,
    runs: state.runs,
    lastError: state.lastError,
    remoteGrants: state.remoteGrants,
  });
}

function commandResult(result) {
  return {
    stdout: result.stdout?.text ?? "",
    stderr: result.stderr?.text ?? "",
    exitCode: result.exitCode ?? null,
    signal: result.signal ?? null,
    timedOut: result.timedOut === true,
  };
}

// ── Session projection: client view of the session envelope + runs ──────────
// The client dock reads `genbio/remote` through the framework's session-projection
// channel (session/projection frames + history baselines). The fold mirrors the
// envelope and runs from the committed genbio_* tool results, so the client view
// is durable: a cold session refolds from its log, and a live session gets push
// frames on every genbio tool result.
const PROJECTION_KEY = "genbio/remote";
const MAX_MIRRORED_RUNS = 12;
const MAX_MIRRORED_PROJECTS = 64;
const MAX_PENDING_CALLS = 64;
// The fold output is broadcast to every client and seeded into session
// history, so mirrored records carry ONLY the fields the dock needs: bounded
// identifiers, lifecycle timestamps, and resource counts (audit 2026-08-25
// P2-F2). Logs (stdout/stderr/error), per-run envelope clones, finalization
// payloads, memory state, and other run internals are dropped at the fold —
// they stay available through genbio_monitor/genbio_runs/genbio_finalize_run.
const RUN_PROJECTION_FIELDS = ["runId", "jobId", "slurmJobId", "target", "operation", "status", "helperStatus", "workloadStatus", "slurmStatus", "startedAt", "finishedAt", "node", "partition", "resources"];
const ENVELOPE_PROJECTION_FIELDS = ["target", "node", "partition", "workloadClass", "maxCpus", "maxGpus", "memGb", "concurrency"];

function projectRun(run) {
  if (!run || typeof run !== "object") return null;
  const projected = {};
  for (const field of RUN_PROJECTION_FIELDS) if (run[field] !== undefined) projected[field] = run[field];
  return Object.keys(projected).length > 0 ? projected : null;
}

function projectEnvelope(envelope) {
  const projected = {};
  for (const field of ENVELOPE_PROJECTION_FIELDS) if (envelope[field] !== undefined) projected[field] = envelope[field];
  return projected;
}

// Project entries mirrored from genbio_projects / genbio_projects_status. The
// fold output is broadcast to every client and seeded into session history, so
// entries carry ONLY bounded identifiers and resource metadata: no wrapper
// bytes, paths, parameter values, tokens, full hashes, or logs cross the
// channel. Operation entries arrive either as plain names (genbio_projects)
// or as { name, form, cpus, gpus, concurrency } objects (aggregate), so the
// mirror is tolerant of both shapes.
function projectProjectEntry(item) {
  if (!item || typeof item !== "object") return null;
  const project = String(item.project ?? "").slice(0, 64);
  if (project.length === 0) return null;
  const operations = Array.isArray(item.operations)
    ? item.operations.slice(0, MAX_MIRRORED_PROJECTS).map((op) => {
        if (op && typeof op === "object") {
          return {
            name: String(op.name ?? "").slice(0, 64),
            ...(typeof op.form === "string" ? { form: op.form.slice(0, 16) } : {}),
            ...(Number.isInteger(op.cpus) ? { cpus: op.cpus } : {}),
            ...(Number.isInteger(op.gpus) ? { gpus: op.gpus } : {}),
            ...(Number.isInteger(op.concurrency) ? { concurrency: op.concurrency } : {}),
          };
        }
        // Legacy/discovery shape: a plain operation name stays a plain name.
        return String(op ?? "").slice(0, 64);
      }).filter((op) => (typeof op === "string" ? op.length > 0 : op.name.length > 0))
    : [];
  return {
    project,
    valid: item.valid === true,
    ...(Number.isInteger(item.schema_version) ? { schemaVersion: item.schema_version } : {}),
    ...(["workspace", "configured"].includes(item.origin) ? { origin: item.origin } : {}),
    operations,
    ...(Array.isArray(item.plans) ? { plans: item.plans.slice(0, 32) } : {}),
  };
}

// Per-project aggregate entries are mirrored through a strict allowlist so a
// future enrichment of the aggregate can never widen the broadcast surface.
const PROJECT_STATUS_PROJECTION_FIELDS = [
  "project", "origin", "valid", "schema_version", "error", "operations", "plans", "plan_count",
  "runs", "run_counts", "active", "suggested", "needs_review", "completed", "last_activity_at",
];
// Nested array elements are ALSO narrowed field-by-field (never passed through
// verbatim): a future aggregate that adds wrapper bytes, log text, or paths to
// a plan/run/operation record cannot silently widen the broadcast.
const PROJECT_STATUS_ARRAY_FIELDS = new Set(["operations", "plans", "runs"]);

function projectStatusOperation(op) {
  if (!op || typeof op !== "object") return null;
  const name = String(op.name ?? "").slice(0, 64);
  if (name.length === 0) return null;
  return {
    name,
    ...(typeof op.form === "string" ? { form: op.form.slice(0, 16) } : {}),
    ...(Number.isInteger(op.cpus) ? { cpus: op.cpus } : {}),
    ...(Number.isInteger(op.gpus) ? { gpus: op.gpus } : {}),
    ...(Number.isInteger(op.concurrency) ? { concurrency: op.concurrency } : {}),
  };
}

function projectStatusPlan(plan) {
  if (!plan || typeof plan !== "object") return null;
  const operation = String(plan.operation ?? "").slice(0, 64);
  if (operation.length === 0) return null;
  return {
    operation,
    status: String(plan.status ?? "planned").slice(0, 32),
    plan_hash: String(plan.plan_hash ?? "").slice(0, 12),
    ...(Number.isInteger(plan.created_at) ? { created_at: plan.created_at } : {}),
    ...(Number.isInteger(plan.bytes_sha256) || (typeof plan.bytes_sha256 === "string" && String(plan.bytes_sha256).length > 0) ? { bytes_sha256: String(plan.bytes_sha256).slice(0, 16) } : {}),
  };
}

function projectStatusRun(run) {
  if (!run || typeof run !== "object") return null;
  const runId = String(run.run_id ?? "").slice(0, 96);
  if (runId.length === 0) return null;
  return {
    run_id: runId,
    operation: String(run.operation ?? "").slice(0, 96),
    status: String(run.status ?? "unknown").slice(0, 32),
    ...(typeof run.target === "string" ? { target: run.target.slice(0, 32) } : {}),
    ...(Number.isInteger(run.started_at) ? { started_at: run.started_at } : {}),
    ...(Number.isInteger(run.finished_at) ? { finished_at: run.finished_at } : {}),
  };
}

function projectStatusEntry(entry) {
  if (!entry || typeof entry !== "object") return null;
  const project = String(entry.project ?? "").slice(0, 64);
  if (project.length === 0) return null;
  const projected = { project };
  for (const field of PROJECT_STATUS_PROJECTION_FIELDS) {
    if (field === "project" || entry[field] === undefined) continue;
    if (field === "operations" && Array.isArray(entry[field])) projected[field] = entry[field].slice(0, MAX_MIRRORED_PROJECTS).map(projectStatusOperation).filter(Boolean);
    else if (field === "plans" && Array.isArray(entry[field])) projected[field] = entry[field].slice(0, MAX_MIRRORED_PROJECTS).map(projectStatusPlan).filter(Boolean);
    else if (field === "runs" && Array.isArray(entry[field])) projected[field] = entry[field].slice(0, MAX_MIRRORED_PROJECTS).map(projectStatusRun).filter(Boolean);
    else if (field === "origin" && ["workspace", "configured"].includes(entry[field])) projected[field] = entry[field];
    else if (field === "error" && typeof entry[field] === "string") projected[field] = entry[field].slice(0, 300);
    else if (field === "run_counts" && entry[field] && typeof entry[field] === "object") {
      // run_counts is a flat integer map today; a future nested enrichment must
      // not cross the projection, so only safe integer members are mirrored.
      const counts = {};
      for (const [key, value] of Object.entries(entry[field])) {
        if (Number.isInteger(value) && value >= 0) counts[String(key).slice(0, 64)] = value;
      }
      projected[field] = counts;
    }
    else if (field === "plan_count" && Number.isInteger(entry[field])) projected[field] = entry[field];
    else if (Array.isArray(entry[field])) projected[field] = entry[field].slice(0, MAX_MIRRORED_PROJECTS).map((value) => typeof value === "string" ? value.slice(0, 64) : value);
    else if (typeof entry[field] === "string") projected[field] = entry[field].slice(0, 64);
    else if (typeof entry[field] === "boolean") projected[field] = entry[field];
    else if (Number.isInteger(entry[field])) projected[field] = entry[field];
  }
  return projected;
}

function projectionInit() {
  return { calls: {}, envelope: null, runs: [], projects: [], projects_status: [], workflows: [] };
}
const projectionStateSchema = z.object({
  calls: z.record(z.string(), z.string()),
  envelope: z.record(z.string(), z.unknown()).nullable(),
  runs: z.array(z.record(z.string(), z.unknown())),
  projects: z.array(z.record(z.string(), z.unknown())),
  projects_status: z.array(z.record(z.string(), z.unknown())),
  workflows: z.array(z.record(z.string(), z.unknown())),
});
const projectionViewSchema = z.object({
  envelope: z.record(z.string(), z.unknown()).nullable(),
  runs: z.array(z.record(z.string(), z.unknown())),
  projects: z.array(z.record(z.string(), z.unknown())),
  projects_status: z.array(z.record(z.string(), z.unknown())),
  workflows: z.array(z.record(z.string(), z.unknown())),
});
function projectionView(state) {
  return { envelope: state.envelope, runs: state.runs, projects: state.projects, projects_status: state.projects_status, workflows: state.workflows };
}

/** Parse the first JSON text block of a tool-result message into its `status` object. */
function parseToolResultStatus(message) {
  const result = Array.isArray(message?.content) ? message.content[0] : null;
  if (!result || result.type !== "tool-result" || !Array.isArray(result.content)) return null;
  for (const block of result.content) {
    if (block?.type !== "text" || typeof block.text !== "string") continue;
    try {
      const parsed = JSON.parse(block.text);
      if (parsed && typeof parsed === "object" && typeof parsed.status === "object" && parsed.status !== null) return parsed.status;
    } catch {
      // Not a JSON tool result; nothing to mirror.
    }
  }
  return null;
}

function projectWorkflowEntry(item) {
  if (!item || typeof item !== "object") return null;
  const runId = String(item.workflow_run_id ?? "").slice(0, 96);
  const workflow = String(item.workflow ?? "").slice(0, 64);
  if (!runId || !workflow) return null;
  const nodes = Array.isArray(item.nodes) ? item.nodes.slice(0, 32).map((node) => ({ node_id: String(node?.node_id ?? "").slice(0, 64), project: String(node?.project ?? "").slice(0, 64), operation: String(node?.operation ?? "").slice(0, 64), status: String(node?.status ?? "unknown").slice(0, 32), ...(typeof node?.job_id === "string" ? { job_id: node.job_id.slice(0, 10) } : {}), ...(typeof node?.slurm_state === "string" ? { slurm_state: node.slurm_state.slice(0, 32) } : {}), depends_on: Array.isArray(node?.depends_on) ? node.depends_on.slice(0, 32).map((value) => String(value).slice(0, 64)) : [] })).filter((node) => node.node_id && node.project && node.operation) : [];
  return { workflow_run_id: runId, workflow, status: String(item.status ?? "unknown").slice(0, 32), nodes, counts: item.counts && typeof item.counts === "object" ? Object.fromEntries(Object.entries(item.counts).filter(([, value]) => Number.isInteger(value) && value >= 0).map(([key, value]) => [String(key).slice(0, 32), value])) : {} };
}

/** Pure fold over committed session events (the session-projection unit contract). */
function projectionApply(state, event) {
  if (event.type === "tool/call") {
    const name = event.data?.name;
    const callId = event.data?.callId;
    if (typeof name !== "string" || !name.startsWith("genbio_") || typeof callId !== "string" || callId.length === 0) return state;
    const calls = { ...state.calls, [callId]: name };
    const keys = Object.keys(calls);
    if (keys.length > MAX_PENDING_CALLS) for (const key of keys.slice(0, keys.length - MAX_PENDING_CALLS)) delete calls[key];
    return { ...state, calls };
  }
  if (event.type !== "tool/result") return state;
  const callId = event.data?.message?.source?.callId;
  if (typeof callId !== "string" || !Object.hasOwn(state.calls, callId)) return state;
  const calls = { ...state.calls };
  delete calls[callId];
  const next = { ...state, calls };
  const status = parseToolResultStatus(event.data?.message);
  // Mirror every committed state update verbatim: a runs array in a genbio
  // status (even an empty one) IS the plugin's session-state mirror, so it is
  // projected as-is — an empty update clears stale runs instead of being
  // skipped (P2-F2). Envelope and run records are narrowed to the
  // client-needed projection fields defined above.
  if (status && typeof status.envelope === "object" && status.envelope !== null) next.envelope = projectEnvelope(status.envelope);
  if (status && Array.isArray(status.runs)) next.runs = status.runs.slice(-MAX_MIRRORED_RUNS).map(projectRun).filter(Boolean);
  // Project views are intentionally narrow: no wrapper bytes, paths, parameter
  // values, tokens, full hashes, or logs cross the session projection. The
  // aggregate mirror below (projects_status) is filtered through the strict
  // PROJECT_STATUS_PROJECTION_FIELDS allowlist defined above.
  if (status && Array.isArray(status.projects)) next.projects = status.projects.slice(0, MAX_MIRRORED_PROJECTS).map(projectProjectEntry).filter(Boolean);
  if (status && Array.isArray(status.projects_status)) next.projects_status = status.projects_status.slice(0, MAX_MIRRORED_PROJECTS).map(projectStatusEntry).filter(Boolean);
  if (status && status.workflow_run && typeof status.workflow_run === "object") {
    const workflow = projectWorkflowEntry(status.workflow_run);
    if (workflow) next.workflows = [...next.workflows.filter((item) => item.workflow_run_id !== workflow.workflow_run_id), workflow].slice(-16);
  }
  if (status && typeof status.project === "string" && Array.isArray(status.plans)) {
    const project = String(status.project).slice(0, 64);
    const existing = next.projects.filter((item) => item.project !== project);
    next.projects = [...existing, {
      project,
      valid: true,
      operations: [...new Set(status.plans.map((item) => String(item?.operation ?? "").slice(0, 64)).filter(Boolean))],
      plans: status.plans.slice(-32).map((item) => ({ operation: String(item?.operation ?? "").slice(0, 64), status: String(item?.status ?? "unknown").slice(0, 32), planHash: String(item?.plan_hash ?? "").slice(0, 12) })),
    }].slice(-64);
  }
  return next;
}

// ── Remote folder grants: per-workspace confinement of remote data paths ────
// Each operation template declares its remote DATA needs ({ root, write });
// before any command ships, every need must sit under a remote root granted
// to the session's workspace (rw covers reads+writes, ro reads only) or under
// a policy-level read-only tool root (conda & co. need no grant). Anything
// else pauses on a user question (grant persistent / session-only / reject).
function remotePathInside(path, root) {
  const p = String(path).replace(/\/{2,}/g, "/").replace(/\/+$/, "");
  const r = String(root).replace(/\/{2,}/g, "/").replace(/\/+$/, "");
  return p === r || p.startsWith(`${r}/`);
}

/** The target's read-only tool roots from the policy `environment.tool_roots`. */
function toolRootsFor(policy, target) {
  const roots = policy?.targets?.[target]?.environment?.tool_roots;
  return Array.isArray(roots) ? roots.filter((root) => typeof root === "string" && root.startsWith("/") && !root.includes("..")) : [];
}

/** One need is covered when an rw grant (or any grant for reads) contains it,
 *  or a read-only need sits under a policy tool root. */
function remoteNeedCovered(need, granted, toolRoots) {
  if (!need.write && toolRoots.some((root) => remotePathInside(need.root, root))) return true;
  return granted.some((entry) => remotePathInside(need.root, entry.root) && (entry.mode === "rw" || !need.write));
}

function validatePolicy(policy) {
  if (!policy || policy.schema_version !== 1 || policy.policy !== "genbio-remote-compute") throw new Error("invalid policy identity");
  if (!policy.ssh || policy.ssh.client !== "openssh-native" || policy.ssh.noninteractive !== true) throw new Error("policy must require native noninteractive OpenSSH");
  const options = policy.ssh.options ?? {};
  if (options.tty !== false || options.batch_mode !== true || options.connect_timeout_s !== 10 || options.strict_host_key_checking !== "yes") throw new Error("policy SSH contract is not strict");
  if (options.agent_forwarding !== false || options.x11_forwarding !== false || options.port_forwarding !== false) throw new Error("policy forwarding contract is not strict");
  if (!policy.targets || Object.keys(policy.targets).sort().join(",") !== TARGETS.slice().sort().join(",")) throw new Error(`policy targets must be exactly ${TARGETS.join(", ")}`);
  const hpc = policy.targets.HPC;
  if (hpc.ssh_target !== "HPC" || hpc.surface !== "slurm") throw new Error("HPC policy must target HPC with slurm surface");
  if (!hpc.allowlist || typeof hpc.allowlist !== "object") throw new Error("HPC policy must have an allowlist");
  for (const [node, partition] of [["gpu04", "gpus"], ["cpu01", "cpus"]]) if (hpc.allowlist[node]?.partition !== partition) throw new Error(`HPC ${node}/${partition} mapping is invalid`);
  const nhpc = policy.targets.NHPC;
  if (nhpc.ssh_target !== "NHPC" || nhpc.surface !== "slurm") throw new Error("NHPC policy must target NHPC with slurm surface");
  if (nhpc.allowlist?.gpu01?.partition !== "gpu") throw new Error("NHPC gpu01/gpu mapping is invalid");
  if (nhpc.test_gate?.real_submission !== "gpu01") throw new Error("NHPC real submission test must be pinned to gpu01");
  const h100 = policy.targets.genbioh100;
  if (h100.ssh_target !== "genbioh100" || h100.surface !== "direct" || h100.login_shell !== false) throw new Error("genbioh100 direct/no-login policy is invalid");
  if (JSON.stringify(h100.limits?.gpus_allowed) !== JSON.stringify([0])) throw new Error("genbioh100 must allow GPU 0 only");
  if (h100.hardware?.reserved_gpu !== 1 || h100.hardware?.protected_process !== "gpu_util") throw new Error("genbioh100 GPU 1 protection is required");
  integer(h100.limits?.cpu_threads_per_job, "genbioh100 CPU limit", 1);
  integer(h100.limits?.mem_gb_per_job, "genbioh100 memory limit", 1);
  if (h100.limits?.concurrent_gpu_jobs !== 1) throw new Error("genbioh100 concurrency must be one");
  for (const target of Object.keys(policy.targets)) {
    const toolRoots = policy.targets[target]?.environment?.tool_roots;
    if (toolRoots !== undefined && (!Array.isArray(toolRoots) || toolRoots.some((root) => typeof root !== "string" || !root.startsWith("/") || root.includes("..")))) throw new Error(`${target} environment.tool_roots must be an array of absolute POSIX paths`);
  }
}

function makeTool(name, description, parameters, execute) {
  return defineTool({
    name,
    description,
    parameters,
    output: { schema: TOOL_OUTPUT, render: (_args, value) => [{ type: "text", text: JSON.stringify(value) }] },
    execute,
  });
}

async function apply(ctx, config = {}) {
  const policyPath = config.policyPath ?? "/Users/mdanh/.codex/skills/operate-genbio-hpc-remote/references/genbio-compute-policy.yaml";
  const shell = ctx.shell;
  const timer = ctx.timer;
  const userQuestions = ctx.get("userQuestions");
  const jobs = ctx.get("jobs");
  const states = new Map();
  const workflowRegistry = createWorkflowRegistry(typeof config.workflowRegistryDir === "string" && config.workflowRegistryDir.length > 0 ? config.workflowRegistryDir : DEFAULT_WORKFLOW_REGISTRY_DIR);
  const memoryMode = MEMORY_MODES.has(config.memoryMode) ? config.memoryMode : "manual";
  let memoryPublisher = null;
  let activePolicy = null;
  let activePolicyText = null;
  let policyError = null;

  const readPolicy = async () => {
    try {
      const text = await readFile(policyPath, "utf8");
      if (text === activePolicyText && activePolicy !== null) return;
      const parsed = parseYaml(text);
      validatePolicy(parsed);
      activePolicyText = text;
      activePolicy = parsed;
      policyError = null;
      for (const state of states.values()) state.policy = { valid: true, hash: digest(text), updated: normalizeTimestamp(parsed.updated) };
    } catch (error) {
      activePolicy = null;
      policyError = String(error?.message ?? error);
      for (const state of states.values()) state.policy = { valid: false, hash: null, updated: null, error: policyError };
    }
  };
  await readPolicy();
  const disposePoll = timer.interval(() => { void readPolicy(); }, Number(config.policyPollMs ?? 2000));

  const stateFor = (id) => {
    let state = states.get(id);
    if (!state) {
      state = {
        policy: activePolicy ? { valid: true, hash: digest(activePolicyText), updated: normalizeTimestamp(activePolicy.updated) } : { valid: false, hash: null, updated: null, error: policyError },
        envelope: null,
        runs: [],
        lastError: null,
        remoteGrants: [],
      };
      states.set(id, state);
    }
    return state;
  };
  const requirePolicy = () => {
    if (!activePolicy) throw new Error(`Genbio policy unavailable: ${policyError ?? "unknown policy error"}`);
    return activePolicy;
  };
  const requireState = (exec) => stateFor(sessionIdOf(exec));
  const setError = (state, error) => { state.lastError = String(error?.message ?? error); };
  const genbioRemote = {
    registerMemoryPublisher(publisher) {
      if (!publisher || typeof publisher.publish !== "function") throw new Error("Genbio memory publisher must expose publish(session, record)");
      if (memoryPublisher && memoryPublisher !== publisher) throw new Error("Genbio memory publisher is already registered");
      memoryPublisher = publisher;
      return () => { if (memoryPublisher === publisher) memoryPublisher = null; };
    },
    async publish(session, record) {
      if (!memoryPublisher) return { ok: false, status: "unavailable", error: "OpenViking publisher is unavailable" };
      return memoryPublisher.publish(session, record);
    },
  };
  ctx.provide("genbioRemote", genbioRemote);

  const publishFinalized = async (run, session) => {
    if (memoryMode === "off") {
      run.memory.status = "disabled";
      return { ok: true, status: "disabled" };
    }
    if (["published", "queued"].includes(run.memory.status)) return { ok: true, status: run.memory.status };
    if (run.memory.inFlight) return run.memory.inFlight;
    run.memory.status = "publishing";
    run.memory.inFlight = (async () => {
      let result;
      try {
        result = await genbioRemote.publish(session, {
          recordId: run.finalization.recordId,
          text: run.finalization.text,
          createdAt: run.finishedAt,
        });
      } catch (error) {
        result = { ok: false, status: "failed", error: String(error?.message ?? error) };
      }
      run.memory.status = result.status === "duplicate" ? "published" : result.status;
      run.memory.error = result.error ?? null;
      run.memory.openVikingSessionId = result.sessionId ?? null;
      run.memory.traceId = result.traceId ?? null;
      return result;
    })().finally(() => { run.memory.inFlight = null; });
    return run.memory.inFlight;
  };

  const runRemote = async (target, command, exec, timeoutMs = 30000) => {
    const policy = requirePolicy();
    const expected = policy.targets[target]?.ssh_target;
    if (!expected) throw new Error(`target ${target} is not supported by the active policy`);
    if (!/^ssh -T -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=yes -- (HPC|NHPC|genbio_mdanh|genbioh100) /.test(command)) throw new Error("remote command does not match the strict native OpenSSH contract");
    // Exact alias binding (t10): the command's ssh target token must be the
    // policy-registered ssh alias for this target — a command assembled for
    // one target can never be executed against another (alias-swap defense;
    // the policy lists alias-swap under ssh.prohibited).
    if (!command.startsWith(`ssh ${SSH_OPTIONS.join(" ")} -- ${expected} `)) throw new Error(`remote command does not target the policy-registered ssh alias for ${target} (expected "${expected}")`);
    if (target === "genbioh100" && (command.includes("gpu1") || command.includes("gpu_util"))) throw new Error("genbioh100 protected GPU 1/gpu_util operation rejected");
    const request = shell.resolve({ command, timeoutMs, signal: exec.signal });
    const result = await shell.run(request);
    return { target, ...commandResult(result) };
  };

  const workspaceAccessFor = () => {
    try {
      return ctx.get("workspaceAccess") ?? null;
    } catch {
      return null;
    }
  };
  const workspacePathOf = (exec) => {
    const cwd = exec?.agent?.session?.header?.cwd;
    return typeof cwd === "string" && cwd.length > 0 ? cwd : null;
  };
  /** Effective remote roots for one target: workspace grants (persistent)
   *  plus this session's on-the-fly grants; unknown modes fail closed to ro. */
  const grantedRemoteRootsFor = (exec, state, target) => {
    const roots = [];
    const seen = new Set();
    const push = (entry) => {
      if (!entry || entry.target !== target || typeof entry.root !== "string") return;
      const mode = entry.mode === "rw" ? "rw" : "ro";
      const key = `${target}\u0000${entry.root}\u0000${mode}`;
      if (seen.has(key)) return;
      seen.add(key);
      roots.push({ target, root: entry.root, mode });
    };
    const workspaceAccess = workspaceAccessFor();
    const workspacePath = workspacePathOf(exec);
    if (workspaceAccess?.remoteRootsFor && workspacePath !== null) {
      try {
        for (const entry of workspaceAccess.remoteRootsFor(workspacePath) ?? []) push(entry);
      } catch {
        // store lookup failed closed: session grants and tool roots still apply
      }
    }
    for (const entry of state.remoteGrants) push(entry);
    return roots;
  };
  const askRemoteGrant = async (target, needs, exec, state) => {
    if (!userQuestions) throw new Error("remote folder access outside the granted roots requires the DSH user-question provider");
    const workspaceAccess = workspaceAccessFor();
    const workspacePath = workspacePathOf(exec);
    const canPersist = Boolean(workspaceAccess?.grantRemote && workspacePath !== null);
    const roots = [...new Set(needs.map((need) => need.root))];
    const writes = needs.some((need) => need.write);
    const mode = writes ? "rw" : "ro";
    const questionId = `genbio-remote-grant-${target}`;
    const options = [
      ...(canPersist ? [{ label: "Grant to this workspace (persistent)", description: "Store the grant for the session workspace; later sessions keep it." }] : []),
      { label: "Grant for this session only", description: "Cover just this session; nothing is stored." },
      { label: "Reject", description: "Do not access these remote folders." },
    ];
    const answer = await userQuestions.ask({
      agent: exec.agent,
      signal: exec.signal,
      questions: [{
        id: questionId,
        header: "Remote folder access",
        question: `This ${target} operation needs ${writes ? "read/write" : "read"} access to remote folder(s) outside the granted remote roots: ${roots.map((root) => `\`${root}\``).join(", ")}. Choose how to proceed.`,
        options,
      }],
    });
    const selected = answer.answers?.find((item) => item.id === questionId)?.selected ?? [];
    if (canPersist && selected.includes("Grant to this workspace (persistent)")) {
      for (const root of roots) await workspaceAccess.grantRemote(workspacePath, target, root, mode);
      return;
    }
    if (selected.includes("Grant for this session only")) {
      for (const root of roots) if (!state.remoteGrants.some((entry) => entry.target === target && entry.root === root)) state.remoteGrants.push({ target, root, mode });
      return;
    }
    throw new Error(`remote folder access to ${roots.join(", ")} on ${target} was not granted`);
  };
  /** Gate for one operation: every declared remote data need must be covered
   *  by a granted root or a policy tool root, or the user grants it now. */
  const requireRemoteAccess = async (target, needs, exec, state) => {
    if (!Array.isArray(needs) || needs.length === 0) return grantedRemoteRootsFor(exec, state, target);
    const policy = requirePolicy();
    const toolRoots = toolRootsFor(policy, target);
    const uncovered = () => needs.filter((need) => !remoteNeedCovered(need, grantedRemoteRootsFor(exec, state, target), toolRoots));
    if (uncovered().length > 0) {
      await askRemoteGrant(target, uncovered(), exec, state);
      const still = uncovered();
      if (still.length > 0) throw new Error(`remote access still not covered after grant for: ${still.map((need) => need.root).join(", ")}`);
    }
    return grantedRemoteRootsFor(exec, state, target);
  };

  const policyStatus = makeTool(
    "genbio_policy_status",
    "Return the active target-specific Genbio policy validity and hash. Remote operations fail closed when invalid.",
    {},
    async (_args, exec) => ({ ok: activePolicy !== null, status: jsonClone(requireState(exec).policy), ...(activePolicy ? {} : { error: policyError ?? "invalid policy" }) }),
  );

  const setEnvelope = makeTool(
    "genbio_set_envelope",
    "Set the explicit session-scoped Genbio resource envelope. The agent may operate within it without asking again; expansion requires an interactive user selection.",
    {
      target: { type: "string", required: true, enum: TARGETS },
      node: { type: "string", required: true },
      partition: { type: "string" },
      workload_class: { type: "string", required: true },
      max_cpus: { type: "integer", required: true },
      max_gpus: { type: "integer", required: true },
      mem_gb: { type: "integer" },
      concurrency: { type: "integer", required: true },
      acknowledge_restrictions: { type: "boolean", required: true },
    },
    async (args, exec) => {
      const policy = requirePolicy();
      const state = requireState(exec);
      integer(args.max_cpus, "max_cpus", 1); integer(args.max_gpus, "max_gpus", 0); integer(args.concurrency, "concurrency", 1);
      if (!args.acknowledge_restrictions) throw new Error("explicit acknowledgement of target restrictions is required");
      const targetPolicy = policy.targets[args.target];
      if (!targetPolicy || targetPolicy.ssh_target !== args.target) throw new Error("target does not match the active policy");
      if (args.target === "HPC") {
        if (!["gpu04", "cpu01"].includes(args.node)) throw new Error("HPC test and operation node must be gpu04 or cpu01");
        if (targetPolicy.allowlist[args.node]?.partition !== args.partition) throw new Error("node and partition do not match policy");
        const caps = targetPolicy.allowlist[args.node]?.caps ?? {};
        if (caps.max_aggregate_cpus !== undefined && args.max_cpus > caps.max_aggregate_cpus) throw new Error(`${args.node} CPU envelope exceeds policy cap ${caps.max_aggregate_cpus}`);
        if (caps.max_concurrent_gpu_jobs !== undefined && args.concurrency > caps.max_concurrent_gpu_jobs) throw new Error(`${args.node} concurrency exceeds policy cap ${caps.max_concurrent_gpu_jobs}`);
        if (args.node === "cpu01" && args.max_gpus !== 0) throw new Error("cpu01 cannot use GPUs");
      } else if (args.node !== args.target) throw new Error(`${args.target} direct envelope node must equal its exact target`);
      if (args.target === "genbioh100") {
        const limits = targetPolicy.limits;
        if (args.max_gpus > 1 || args.max_cpus > limits.cpu_threads_per_job || args.mem_gb !== undefined && args.mem_gb > limits.mem_gb_per_job || args.concurrency > limits.concurrent_gpu_jobs || args.node !== "genbioh100") throw new Error("genbioh100 envelope exceeds GPU0-only, CPU, memory, or concurrency policy");
      }
      state.envelope = { target: args.target, node: args.node, partition: args.partition ?? null, workloadClass: args.workload_class, maxCpus: args.max_cpus, maxGpus: args.max_gpus, memGb: args.mem_gb ?? null, concurrency: args.concurrency, usedCpus: 0, usedGpus: 0, policyHash: state.policy.hash };
      state.lastError = null;
      return { ok: true, status: publicState(state) };
    },
  );

  const preflight = makeTool(
    "genbio_preflight",
    "Run a bounded, read-only target-specific preflight using strict native OpenSSH and return identity, resource, and scheduler diagnostics.",
    { target: { type: "string", required: true, enum: TARGETS } },
    async (args, exec) => {
      const state = requireState(exec);
      try {
        requirePolicy();
        const target = args.target;
        const command = target === "HPC"
          ? "set -eu; hostname -f; command -v sbatch squeue sacct scontrol sinfo scancel; sinfo -N -p gpus; sinfo -N -p cpus; squeue -u \\\"$USER\\\""
          : target === "NHPC"
            ? "set -eu; hostname -f; command -v sbatch squeue sacct scontrol sinfo scancel; sinfo -N -p gpu; squeue -u \\\"$USER\\\" || true"
          : target === "genbioh100"
            ? "set -eu; hostname -f; command -v bash python3 nvidia-smi; nproc; free -h; nvidia-smi -L; nvidia-smi --query-compute-apps=pid,gpu_bus_id --format=csv,noheader"
            : "set -eu; hostname -f; command -v bash python3; nproc; free -h; nvidia-smi -L || true";
        // Escape $ for the local double-quoted shell context: JSON.stringify emits a
        // double-quoted string, so an unescaped $ would be expanded by the LOCAL shell
        // before ssh transmits (e.g. $(sbatch ...) runs locally, $RUN_DIR -> empty).
        // "\\$" becomes a literal $ in the transmitted body; remote-side expansion then
        // happens only on the target.
        const result = await runRemote(target, `ssh ${SSH_OPTIONS.join(" ")} -- ${target} ${JSON.stringify(command).replace(/\$/g, "\\$")}`, exec, Number(config.commandTimeoutMs ?? 30000));
        state.lastError = result.exitCode === 0 ? null : result.stderr || `preflight exit ${result.exitCode}`;
        return { ok: result.exitCode === 0, status: { ...publicState(state), preflight: result } };
      } catch (error) {
        setError(state, error);
        return { ok: false, status: publicState(state), error: state.lastError };
      }
    },
  );

  const slurmDiscovery = makeTool(
    "genbio_slurm_discovery",
    "Collect bounded read-only Slurm version, partition, policy-node/GRES, and current-user queue evidence for HPC or NHPC. No allocation, writes, or arbitrary command input.",
    { target: { type: "string", required: true, enum: ["HPC", "NHPC"] } },
    async (args, exec) => {
      const state = requireState(exec);
      try {
        const policy = requirePolicy();
        const target = args.target;
        const nodes = Object.entries(policy.targets[target].allowlist ?? {}).filter(([, value]) => value && typeof value === "object" && typeof value.partition === "string");
        if (nodes.length === 0) throw new Error(`${target} policy has no discoverable Slurm nodes`);
        const partitions = [...new Set(nodes.map(([, value]) => value.partition))];
        const nodeNames = nodes.map(([node]) => node);
        const body = `set -eu; printf 'VERSION_BEGIN\\n'; scontrol version 2>&1 || true; printf '\\nVERSION_END\\nPARTITIONS_BEGIN\\n'; sinfo -h -p ${partitions.join(",")} -o '%P|%a|%l|%D|%G' 2>&1 || true; printf '\\nPARTITIONS_END\\nNODES_BEGIN\\n'; sinfo -h -N -n ${nodeNames.join(",")} -o '%N|%P|%T|%c|%m|%G' 2>&1 || true; printf '\\nNODES_END\\nQUEUE_BEGIN\\n'; squeue -h -u "$USER" -o '%i|%P|%j|%T|%M|%R' 2>&1 || true; printf '\\nQUEUE_END\\n'`;
        const command = `ssh ${SSH_OPTIONS.join(" ")} -- ${target} ${JSON.stringify(body).replace(/\$/g, "\\$")}`;
        const result = await runRemote(target, command, exec, Number(config.commandTimeoutMs ?? 30000));
        return { ok: result.exitCode === 0, status: { ...publicState(state), discovery: { target, observedAt: Date.now(), sections: parseSlurmDiscovery(result.stdout), stderr: String(result.stderr ?? "").slice(-4096) } } };
      } catch (error) {
        setError(state, error);
        return { ok: false, status: publicState(state), error: state.lastError };
      }
    },
  );

  const validateOperation = (target, operation, requested, state) => {
    if (!state.envelope) throw new Error("set a session resource envelope before launching work");
    if (state.envelope.target !== target) throw new Error("operation target must match the active session envelope");
    integer(requested.cpus, "cpus", 1); integer(requested.gpus, "gpus", 0); integer(requested.concurrency, "concurrency", 1);
    if (requested.cpus > state.envelope.maxCpus || requested.gpus > state.envelope.maxGpus || requested.concurrency > state.envelope.concurrency || (requested.memGb !== null && state.envelope.memGb !== null && requested.memGb > state.envelope.memGb)) return false;
    if (target === "HPC" && operation !== "gpu04-smoke") throw new Error("persistent bundle currently permits only the policy-pinned gpu04 smoke operation on HPC");
    if (target === "HPC" && state.envelope.node !== "gpu04") throw new Error("HPC smoke operation is pinned to gpu04");
    if (target === "genbioh100" && requested.gpus > 1) throw new Error("genbioh100 permits GPU 0 only");
    return true;
  };

  const launch = makeTool(
    "genbio_launch",
    "Launch one named policy-checked operation as a session-owned tracked background job. Operations are constrained templates, not arbitrary remote commands.",
    { target: { type: "string", required: true, enum: TARGETS }, operation: { type: "string", required: true, enum: ["preflight-smoke", "gpu04-smoke"] }, cpus: { type: "integer", required: true }, gpus: { type: "integer", required: true }, mem_gb: { type: "integer" }, concurrency: { type: "integer", required: true } },
    async (args, exec) => {
      const state = requireState(exec); const policy = requirePolicy();
      const requested = { cpus: args.cpus, gpus: args.gpus, memGb: args.mem_gb ?? null, concurrency: args.concurrency };
      if (!validateOperation(args.target, args.operation, requested, state)) {
        await expand(state, { cpus: requested.cpus, gpus: requested.gpus, mem_gb: requested.memGb, concurrency: requested.concurrency }, exec);
        throw new Error("expanded resources must be committed with genbio_set_envelope before launch");
      }
      const target = args.target;
      if (args.operation === "gpu04-smoke" && target !== "HPC") throw new Error("gpu04-smoke is an HPC-only operation");
      if (args.operation === "preflight-smoke" && target === "HPC") throw new Error("HPC allocation smoke uses the separately gated gpu04-smoke operation");
      // Remote folder confinement: the gpu04 smoke creates and writes its run
      // dir under HPC_SMOKE_ROOT. The genbioh100 preflight smoke only touches
      // policy tool roots (conda python), which need no grant.
      if (args.operation === "gpu04-smoke") await requireRemoteAccess(target, [{ root: HPC_SMOKE_ROOT, write: true }], exec, state);
      const remoteBody = target === "genbioh100"
        ? "set -eu; CUDA_VISIBLE_DEVICES=0 OMP_NUM_THREADS=16 /home/work/GenbioLAB/miniconda3/bin/python3 -c 'import os; assert os.environ[\"CUDA_VISIBLE_DEVICES\"]==\"0\"; assert os.environ[\"OMP_NUM_THREADS\"]==\"16\"; print(\"GENBIOH100_POLICY_SMOKE_OK\")'"
        : target === "HPC"
          ? "set -eu; RUN_DIR=" + HPC_SMOKE_ROOT + "/runs/dsh-plugin-$(date -u +%Y%m%dT%H%M%SZ)-$$; test ! -e \"$RUN_DIR\"; mkdir -p \"$RUN_DIR/slurm\"; printf '%s\\n' '#!/bin/bash' '#SBATCH --job-name=dsh_policy_smoke' '#SBATCH --partition=gpus' '#SBATCH --nodes=1' '#SBATCH --nodelist=gpu04' '#SBATCH --ntasks=1' '#SBATCH --cpus-per-task=1' '#SBATCH --output=%x_%j.out' '#SBATCH --error=%x_%j.err' 'set -euo pipefail' 'cd \"$SLURM_SUBMIT_DIR\"' 'printf '\"'\"'%s\\n'\"'\"' DSH_GPU04_POLICY_SMOKE_OK' 'hostname -f' 'date -u +%Y-%m-%dT%H:%M:%SZ' > \"$RUN_DIR/slurm/gpu04_policy_smoke.sbatch\"; cd \"$RUN_DIR\"; JOB_ID=$(sbatch --parsable slurm/gpu04_policy_smoke.sbatch); printf 'JOB_ID=%s\\nRUN_DIR=%s\\n' \"$JOB_ID\" \"$RUN_DIR\"; i=0; while test $i -lt 60 && squeue -h -j \"$JOB_ID\" | grep -q .; do sleep 2; i=$((i+1)); done; sacct -X -j \"$JOB_ID\" --format=JobIDRaw,State,ExitCode -P; STATE=$(sacct -X -n -j \"$JOB_ID\" --format=State -P | head -1 | cut -d+ -f1); EXIT_CODE=$(sacct -X -n -j \"$JOB_ID\" --format=ExitCode -P | head -1); cat \"$RUN_DIR/dsh_policy_smoke_${JOB_ID}.out\"; test \"$STATE\" = COMPLETED; test \"$EXIT_CODE\" = 0:0"
          : "set -eu; hostname -f; nproc; free -h";
      // Same local-expansion guard as the preflight assembly: without the \$ escape,
      // the local shell expands $(sbatch ...)/$(sacct ...)/$RUN_DIR/$$ in this
      // double-quoted string before ssh transmits it (2026-08-24 HPC-1787537125755).
      const command = `ssh ${SSH_OPTIONS.join(" ")} -- ${target} ${JSON.stringify(remoteBody).replace(/\$/g, "\\$")}`;
      if (target === "HPC" && !policy.targets.HPC.allowlist?.gpu04) throw new Error("active policy does not have gpu04 in the HPC allowlist");
      const run = {
        runId: `${target}-${Date.now()}`,
        target,
        operation: args.operation,
        status: "running",
        startedAt: Date.now(),
        finishedAt: null,
        stdout: "",
        stderr: "",
        error: null,
        pid: null,
        jobId: null,
        resources: requested,
        policyHash: state.policy.hash,
        node: state.envelope.node,
        partition: state.envelope.partition,
        envelope: jsonClone(state.envelope),
        remoteGrants: jsonClone(state.remoteGrants),
        finalization: null,
        memory: { status: "not-finalized", error: null, openVikingSessionId: null, traceId: null },
      };
      if (!jobs) throw new Error("background job registry is unavailable");
      run.jobId = jobs.start({ kind: `genbio-${target}`, label: `${target} ${args.operation}`, owner: exec.agent, run: () => {
        const controller = new AbortController();
        const done = (async () => {
          try {
            const timeoutMs = target === "HPC" ? Number(config.smokeTimeoutMs ?? 180000) : Number(config.commandTimeoutMs ?? 30000);
            const result = await runRemote(target, command, { ...exec, signal: controller.signal }, timeoutMs);
            run.stdout = String(result.stdout).slice(-Number(config.logMaxBytes ?? 65536)); run.stderr = String(result.stderr).slice(-Number(config.logMaxBytes ?? 65536));
            if (controller.signal.aborted) { run.status = "killed"; run.error = null; return { status: "killed", detail: "cancelled" }; }
            run.status = result.exitCode === 0 ? "completed" : "failed"; run.error = result.exitCode === 0 ? null : run.stderr || `exit ${result.exitCode}`;
            return { status: run.status, detail: run.error ?? `exit code: ${result.exitCode}` };
          } catch (error) { if (controller.signal.aborted) { run.status = "killed"; run.error = null; return { status: "killed", detail: "cancelled" }; } run.status = "failed"; run.error = String(error?.message ?? error); return { status: "failed", detail: run.error }; }
          finally { run.finishedAt = Date.now(); }
        })();
        return { cancel: (reason) => controller.abort(reason ?? "Genbio job cancelled"), done, readOutput: () => { const text = [run.stdout, run.stderr && `[stderr]\n${run.stderr}`, run.error && `[error] ${run.error}`].filter(Boolean).join("\n"); run.stdout = ""; run.stderr = ""; return text; } };
      }});
      state.runs.push(run);
      if (state.runs.length > 50) state.runs.splice(0, state.runs.length - 50);
      return { ok: true, status: { ...publicState(state), started: run } };
    },
  );

  const monitor = makeTool(
    "genbio_monitor",
    "Refresh one session-owned Genbio target and return bounded stdout, stderr, status, and elapsed-time evidence.",
    { target: { type: "string", required: true, enum: TARGETS }, run_id: { type: "string" } },
    async (args, exec) => {
      const state = requireState(exec);
      const run = args.run_id ? state.runs.find((item) => item.runId === args.run_id) : state.runs.at(-1);
      if (!run) return { ok: true, status: publicState(state) };
      run.lastObservedAt = Date.now();
      run.elapsedMs = (run.finishedAt ?? Date.now()) - run.startedAt;
      return { ok: true, status: publicState(state) };
    },
  );

  const expand = async (state, requested, exec) => {
    if (!userQuestions) throw new Error("resource expansion requires the DSH user-question provider");
    const envelope = state.envelope;
    const answer = await userQuestions.ask({
      agent: exec.agent,
      signal: exec.signal,
      questions: [{
        id: "genbio-resource-expansion",
        header: "Genbio resources",
        question: `Requested resources exceed the session envelope. Current CPU ${envelope.maxCpus}, GPU ${envelope.maxGpus}; requested CPU ${requested.cpus}, GPU ${requested.gpus}. Choose a policy-valid action.`,
        options: [
          { label: "Reduce or serialize the job", description: "Keep the current envelope and reduce concurrency or per-job resources." },
          { label: "Expand to requested resources", description: "Revalidate the requested envelope against the active target policy and live state." },
          { label: "Wait", description: "Keep the envelope unchanged and wait for current jobs/capacity." },
          { label: "Reject", description: "Do not allocate this workload." },
        ],
      }],
    });
    const selected = answer.answers?.find((item) => item.id === "genbio-resource-expansion")?.selected ?? [];
    if (!selected.includes("Expand to requested resources")) throw new Error("resource expansion was not explicitly approved");
    return true;
  };

  const validateResources = makeTool(
    "genbio_validate_resources",
    "Check proposed resources against the session envelope and open an interactive expansion selector when they do not fit.",
    { cpus: { type: "integer", required: true }, gpus: { type: "integer", required: true }, mem_gb: { type: "integer" }, concurrency: { type: "integer", required: true } },
    async (args, exec) => {
      const state = requireState(exec);
      requirePolicy();
      if (!state.envelope) throw new Error("set a session resource envelope before proposing work");
      integer(args.cpus, "cpus", 1); integer(args.gpus, "gpus", 0); integer(args.concurrency, "concurrency", 1);
      const fits = args.cpus <= state.envelope.maxCpus && args.gpus <= state.envelope.maxGpus && args.concurrency <= state.envelope.concurrency && (args.mem_gb === undefined || state.envelope.memGb === null || args.mem_gb <= state.envelope.memGb);
      if (!fits) {
        await expand(state, args, exec);
        return { ok: true, status: { ...publicState(state), expansion: "approved-for-revalidation", requested: args } };
      }
      return { ok: true, status: { ...publicState(state), resourceCheck: { fits: true, requested: args } } };
    },
  );

  const finalizeRun = makeTool(
    "genbio_finalize_run",
    "Freeze a curated terminal Genbio run record and publish it to local OpenViking when memoryMode is manual. Authoritative execution metadata comes from plugin state.",
    {
      run_id: { type: "string", required: true },
      project: { type: "string", required: true },
      summary: { type: "string", required: true },
      significance: { type: "string" },
      limitations: { type: "array", items: { type: "string" } },
      next_steps: { type: "array", items: { type: "string" } },
      artifacts: {
        type: "array",
        items: {
          type: "object",
          additionalProperties: false,
          properties: {
            kind: { type: "string", required: true, enum: ARTIFACT_KINDS },
            location: { type: "string", required: true },
            sha256: { type: "string" },
            description: { type: "string" },
          },
        },
      },
    },
    async (args, exec) => {
      const state = requireState(exec);
      const run = state.runs.find(item => item.runId === args.run_id);
      if (!run) throw new Error(`unknown session-owned Genbio run ${args.run_id}`);
      if (!TERMINAL_RUN_STATES.has(run.helperStatus ?? run.status)) throw new Error("Genbio helper must reach a terminal state before finalization");
      if (run.workloadStatus !== undefined && !TERMINAL_WORKLOAD_STATES.has(run.workloadStatus)) throw new Error(`Genbio Slurm workload must reach terminal evidence before finalization (current: ${run.workloadStatus})`);
      if (run.finalization) {
        const result = await publishFinalized(run, exec.agent.session);
        return { ok: result.ok, status: publicState(state), ...(result.error ? { error: result.error } : {}) };
      }
      const project = bounded(args.project, "project", 200, true);
      const summary = bounded(args.summary, "summary", 12000, true);
      const significance = bounded(args.significance, "significance", 6000);
      const limitations = boundedList(args.limitations, "limitations");
      const nextSteps = boundedList(args.next_steps, "next_steps");
      const artifacts = sanitizeArtifacts(args.artifacts);
      const parsed = parseRunEvidence(run);
      const evidence = {
        runId: run.runId,
        backgroundJobId: run.jobId,
        target: run.target,
        operation: run.operation,
        status: run.status,
        startedAt: run.startedAt,
        finishedAt: run.finishedAt,
        elapsedMs: run.finishedAt - run.startedAt,
        policyHash: run.policyHash,
        node: run.node,
        partition: run.partition,
        envelope: run.envelope,
        remoteGrants: run.remoteGrants ?? null,
        requestedResources: run.resources,
        schedulerJobId: parsed.schedulerJobId,
        remoteRunDir: parsed.remoteRunDir,
        terminalEvidence: parsed.terminalEvidence,
        errorSynopsis: parsed.errorSynopsis,
      };
      const semantic = { project, summary, significance, limitations, nextSteps, artifacts };
      const recordId = `genbio-${run.runId}-${digest(JSON.stringify({ evidence, semantic })).slice(0, 16)}`;
      const record = { recordId, ...semantic, evidence };
      const text = markdownRecord(record);
      if (text.length > MAX_MEMORY_RECORD_CHARS) throw new Error(`curated memory record exceeds ${MAX_MEMORY_RECORD_CHARS} characters; reduce summary, lists, or artifact references`);
      run.finalization = { recordId, hash: digest(text), text, finalizedAt: Date.now() };
      run.memory.status = memoryMode === "off" ? "disabled" : "ready";
      const result = await publishFinalized(run, exec.agent.session);
      return { ok: result.ok, status: publicState(state), ...(result.error ? { error: result.error } : {}) };
    },
  );

  const publishRun = makeTool(
    "genbio_publish_run",
    "Retry local OpenViking publication for an already frozen Genbio run record. The curated record cannot be changed by this tool.",
    { run_id: { type: "string", required: true } },
    async (args, exec) => {
      const state = requireState(exec);
      const run = state.runs.find(item => item.runId === args.run_id);
      if (!run) throw new Error(`unknown session-owned Genbio run ${args.run_id}`);
      if (!run.finalization) throw new Error("finalize the Genbio run before publishing it");
      const result = await publishFinalized(run, exec.agent.session);
      return { ok: result.ok, status: publicState(state), ...(result.error ? { error: result.error } : {}) };
    },
  );

  const runs = makeTool(
    "genbio_runs",
    "List session-owned Genbio run state, resource envelopes, remote folder grants, policy hash, status, elapsed time, and bounded diagnostics.",
    {},
    async (_args, exec) => ({ ok: true, status: publicState(requireState(exec)) }),
  );

  const projectSource = createProjectSource(config);
  const { projectsTool, describeTool: projectDescribe, inventoryTool, planTool: projectPlan, executeTool: projectExecute, statusTool: projectStatus, cancelTool: projectCancel, fetchTool: projectFetch } = createProjectTools({ makeTool, requirePolicy, requireState, publicState, config, runRemote, shell, userQuestions, jobs, requireRemoteAccess, projectSource });
  const { planTool: workflowPlanTool, executeTool: workflowExecute, statusTool: workflowStatus, advanceTool: workflowAdvance, pauseTool: workflowPause, resumeTool: workflowResume, cancelTool: workflowCancel } = createWorkflowTools({ makeTool, requirePolicy, requireState, publicState, config, projectSource, projectPlanTool: projectPlan, projectExecuteTool: projectExecute, projectStatusTool: projectStatus, projectCancelTool: projectCancel, workflowRegistry });
  const { statusAllTool: projectsStatus } = createProjectStatusTools({ makeTool, requirePolicy, requireState, publicState, config, projectSource });

  const disposePrompt = ctx.systemPrompt.section({
    name: "genbio-remote:policy",
    order: 104,
    text: "For Genbio remote work, use only the genbio_* tools; never use generic bash, raw SSH/SCP, or ad hoc scheduler commands. Project and workflow manifests must use schema_version 2, and every project operation must be a typed declarative recipe; legacy project schemas and raw SBATCH template projects are unsupported. Prefer a workspace-local genbio-project.yml and genbio-workflows/*.yml; configured projects are a fallback. Before allocation call genbio_policy_status, genbio_preflight, genbio_set_envelope, and genbio_validate_resources. Use genbio_project_plan for immutable local resolution, genbio_project_execute for exact-once staging and submission, genbio_project_status for bounded owned scheduler and job-output evidence, genbio_project_cancel only for an exact active owned numeric job after confirmation, and genbio_project_fetch only for manifest-allowlisted bounded artifacts. Workflow execute or advance submits at most one ready node; workflow status never submits. All transfers use rclone over SFTP with SHA-256 verification, ambiguous submissions are never automatically retried, and remote folder grants remain workspace-scoped. After terminal evidence is verified, separate observation from interpretation and use genbio_finalize_run; report OpenViking publication separately from compute success."
  });
  ctx.tools.register(policyStatus);
  ctx.tools.register(setEnvelope);
  ctx.tools.register(preflight);
  ctx.tools.register(slurmDiscovery);
  ctx.tools.register(launch);
  ctx.tools.register(monitor);
  ctx.tools.register(validateResources);
  ctx.tools.register(finalizeRun);
  ctx.tools.register(publishRun);
  ctx.tools.register(runs);
  ctx.tools.register(projectsTool);
  ctx.tools.register(projectDescribe);
  ctx.tools.register(inventoryTool);
  ctx.tools.register(projectPlan);
  ctx.tools.register(projectExecute);
  ctx.tools.register(projectStatus);
  ctx.tools.register(projectCancel);
  ctx.tools.register(projectFetch);
  ctx.tools.register(projectsStatus);
  ctx.tools.register(workflowPlanTool);
  ctx.tools.register(workflowExecute);
  ctx.tools.register(workflowStatus);
  ctx.tools.register(workflowAdvance);
  ctx.tools.register(workflowPause);
  ctx.tools.register(workflowResume);
  ctx.tools.register(workflowCancel);
  // Optional client-view registration: stays inactive when the deployment does
  // not mount the session-projection registry (headless assemblies, test ctx).
  if (typeof ctx.inject === "function") {
    ctx.inject(["sessionProjections"], (projectionCtx) => {
      const disposeRegistration = projectionCtx.sessionProjections.register({
        key: PROJECTION_KEY,
        stateSchema: projectionStateSchema,
        init: projectionInit,
        apply: projectionApply,
        wire: { viewSchema: projectionViewSchema, view: projectionView },
        stateVersion: 3,
      });
      projectionCtx.effect(() => disposeRegistration, "genbio-remote projection registration");
    });
  }
  ctx.effect(() => () => { disposePrompt(); disposePoll(); }, "genbio-remote lifecycle");
}

export { apply, inject, name, PROJECTION_KEY as projectionKey, projectionApply, projectionInit, projectionView, remoteNeedCovered, remotePathInside, toolRootsFor, HPC_SMOKE_ROOT };
