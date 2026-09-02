// ── Aggregate project status (Phase 3): local-only rollup over session state ──
// Pure module: folds already-loaded project manifest summaries, session plan
// records, and session run records into a bounded per-project aggregate view.
// It performs NO filesystem, session, shell, remote, question, transfer,
// background-job, or scheduler activity — callers do the I/O and pass the
// already-bounded inputs in.
//
// Fail-closed honesty scope: scheduler aggregation (querying multiple known
// Slurm IDs in one bounded sacct call, parsing scheduler states into
// structured fields, tailing logs only for running/failed jobs) arrives with
// the execution integration. Until then this aggregate reflects only what the
// plugin can prove LOCALLY (manifest metadata + session plans + session runs)
// and never reports a remote scheduler state it has not observed.

const MAX_PROJECT_STATUS = 64;
const MAX_PLANS = 32;
const MAX_RUNS = 64;

// Status vocabulary is shared with generic session and project run records.
// A non-terminal run means the operation lane is active.
const NON_TERMINAL_RUN_STATUSES = new Set(["running", "in-flight", "reconciling", "submitting", "publishing"]);
const TERMINAL_RUN_STATUSES = new Set(["completed", "failed", "killed", "cancelled"]);
const REVIEW_STATUSES = new Set(["failed", "killed", "cancelled"]);

function plainObject(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

/** Does a generic schema-v2 operation run belong to `project`? */
export function runBelongsToProject(run, project) {
  if (!plainObject(run) || typeof project !== "string" || project.length === 0) return false;
  const operation = run.operation;
  return typeof operation === "string" && operation.startsWith(`project-${project}-`);
}

/** Map a generic run identity `project-<project>-<operation>` to its operation. */
export function projectOperationOfRun(run, project) {
  if (!plainObject(run) || typeof project !== "string" || project.length === 0) return null;
  const operation = run.operation;
  if (typeof operation !== "string") return null;
  const prefix = `project-${project}-`;
  return operation.startsWith(prefix) ? operation.slice(prefix.length) || null : null;
}

/** Latest run by startedAt; session runs are pushed in order, but durable
 *  reconciliation may reorder, so never rely on array order. */
function latestByStarted(runs) {
  return runs.reduce(
    (latest, run) => (latest === null || (run.startedAt ?? 0) > (latest.startedAt ?? 0) ? run : latest),
    null,
  );
}

function aggregateOne(summary, plans, runs) {
  const project = String(summary.project);
  const projectPlans = plans
    .filter((plan) => plainObject(plan) && plainObject(plan.plan) && plan.plan.project === project)
    .slice(-MAX_PLANS)
    .map((plan) => ({
      plan_hash: String(plan.planHash ?? "").slice(0, 64),
      operation: String(plan.plan.operation ?? "").slice(0, 64),
      status: String(plan.status ?? "planned").slice(0, 32),
      created_at: plan.createdAt ?? null,
      bytes_sha256: String(plan.plan.bytesSha ?? "").slice(0, 16),
    }));
  const projectRuns = runs
    .filter((run) => runBelongsToProject(run, project))
    .slice(-MAX_RUNS)
    .map((run) => ({
      run_id: String(run.runId ?? "").slice(0, 96),
      operation: String(run.operation ?? "").slice(0, 96),
      status: String(run.status ?? "unknown").slice(0, 32),
      ...(run.helperStatus !== undefined ? { helper_status: String(run.helperStatus).slice(0, 32) } : {}),
      ...(run.workloadStatus !== undefined ? { workload_status: String(run.workloadStatus).slice(0, 32) } : {}),
      ...(run.slurmStatus !== undefined && run.slurmStatus !== null ? { slurm_state: String(run.slurmStatus).slice(0, 32) } : {}),
      target: String(run.target ?? "").slice(0, 32),
      started_at: run.startedAt ?? null,
      finished_at: run.finishedAt ?? null,
    }));
  const run_counts = { active: 0, completed: 0, failed: 0, killed: 0, cancelled: 0, other: 0 };
  for (const run of projectRuns) {
    const status = run.workload_status && run.workload_status !== "not_applicable" ? run.workload_status : run.status;
    if (status === "completed") run_counts.completed += 1;
    else if (status === "failed") run_counts.failed += 1;
    else if (status === "killed") run_counts.killed += 1;
    else if (status === "cancelled") run_counts.cancelled += 1;
    else if (NON_TERMINAL_RUN_STATUSES.has(status)) run_counts.active += 1;
    else run_counts.other += 1;
  }
  const operations = (Array.isArray(summary.operations) ? summary.operations : [])
    .slice(0, MAX_PROJECT_STATUS)
    .map((op) => ({
      name: String(op?.name ?? "").slice(0, 64),
      form: String(op?.form ?? "template").slice(0, 16),
      cpus: Number.isInteger(op?.cpus) ? op.cpus : null,
      gpus: Number.isInteger(op?.gpus) ? op.gpus : null,
      concurrency: Number.isInteger(op?.concurrency) ? op.concurrency : null,
    }))
    .filter((op) => op.name.length > 0);
  const byOperation = new Map();
  for (const run of projectRuns) {
    const op = projectOperationOfRun(run, project);
    if (op === null) continue; // e.g. the one-shot `project-<name>` lane
    if (!byOperation.has(op)) byOperation.set(op, []);
    byOperation.get(op).push(run);
  }
  // Per-operation classification (latest run wins), following the priority-6
  // guidance: offer suggested next operations but never automatically rerun
  // failures. "suggested" is reserved for operations with NO run yet (the
  // natural next candidates); completed operations are reported separately in
  // the informational `completed` list rather than being re-suggested.
  const active = [];
  const suggested = [];
  const needsReview = [];
  const completed = [];
  for (const op of operations) {
    const latest = latestByStarted(byOperation.get(op.name) ?? []);
    if (!latest) { suggested.push(op.name); continue; }
    const effective = latest.workload_status && latest.workload_status !== "not_applicable" ? latest.workload_status : latest.status;
    if (NON_TERMINAL_RUN_STATUSES.has(effective) || ["submitted", "pending", "unknown"].includes(effective)) { active.push(op.name); continue; }
    if (REVIEW_STATUSES.has(effective)) { needsReview.push(op.name); continue; }
    if (effective === "completed") completed.push(op.name);
  }
  let lastActivityAt = null;
  for (const plan of projectPlans) if (plan.created_at != null) lastActivityAt = Math.max(lastActivityAt ?? 0, plan.created_at);
  for (const run of projectRuns) {
    for (const at of [run.started_at, run.finished_at]) if (at != null) lastActivityAt = Math.max(lastActivityAt ?? 0, at);
  }
  return {
    project,
    valid: true,
    ...(summary.origin === "workspace" || summary.origin === "configured" ? { origin: summary.origin } : {}),
    schema_version: Number.isInteger(summary.schema_version) ? summary.schema_version : null,
    ...(typeof summary.description === "string" && summary.description.length > 0 ? { description: summary.description.slice(0, 200) } : {}),
    ...(typeof summary.local_root === "string" ? { local_root: summary.local_root } : {}),
    ...(typeof summary.remote_root === "string" ? { remote_root: summary.remote_root } : {}),
    operations,
    plans: projectPlans,
    plan_count: projectPlans.length,
    runs: projectRuns,
    run_counts,
    active,
    suggested,
    needs_review: needsReview,
    completed,
    last_activity_at: lastActivityAt,
  };
}

/**
 * Fold manifest summaries + session plans + session runs into a bounded
 * per-project aggregate. `projectSummaries` entries:
 *   { project, schema_version, valid, description?, local_root?, remote_root?,
 *     operations: [{ name, form, cpus, gpus, concurrency }], error? }
 * Plans are session plan records ({ planHash, plan: { project, operation,
 * bytesSha }, createdAt, status }); runs are session run records.
 * Returns { projects_status, unattributed, max } — all bounded and
 * JSON-safe. Entries include manifest metadata (description and the
 * agent-facing local_root/remote_root paths) and never contain wrapper bytes
 * or logs; the GUI projection mirror strips the paths via its strict allowlist
 * (PROJECT_STATUS_PROJECTION_FIELDS in lib/index.js).
 */
export function aggregateProjects({ projectSummaries = [], plans = [], runs = [] } = {}) {
  if (!Array.isArray(projectSummaries) || !Array.isArray(plans) || !Array.isArray(runs)) {
    throw new Error("aggregateProjects requires arrays for projectSummaries, plans, and runs");
  }
  const projects_status = [];
  for (const summary of projectSummaries) {
    if (!plainObject(summary) || typeof summary.project !== "string" || summary.project.length === 0) continue;
    if (summary.valid === false) {
      projects_status.push({ project: summary.project, valid: false, error: String(summary.error ?? "invalid manifest").slice(0, 300) });
    } else {
      projects_status.push(aggregateOne(summary, plans, runs));
    }
    if (projects_status.length >= MAX_PROJECT_STATUS) break;
  }
  // Runs that belong to no discovered project are surfaced as a count (they
  // are still visible through genbio_runs); they are never silently dropped.
  let unattributed = 0;
  for (const run of runs) {
    if (!plainObject(run)) { unattributed += 1; continue; }
    const belongs = projects_status.some(
      (entry) => entry && entry.valid !== false && runBelongsToProject(run, entry.project),
    );
    if (!belongs) unattributed += 1;
  }
  return Object.freeze({
    projects_status: Object.freeze(projects_status),
    unattributed,
    max: Object.freeze({ projects: MAX_PROJECT_STATUS, plans: MAX_PLANS, runs: MAX_RUNS }),
  });
}

export { MAX_PROJECT_STATUS, MAX_PLANS as MAX_AGGREGATE_PLANS, MAX_RUNS as MAX_AGGREGATE_RUNS, TERMINAL_RUN_STATUSES as AGGREGATE_TERMINAL_STATUSES };