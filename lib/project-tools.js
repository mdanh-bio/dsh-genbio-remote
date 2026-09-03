import { randomBytes } from "node:crypto";
import { buildOperationPlan, resolveRecipe } from "./project.js";
import { createProjectSource } from "./project-source.js";
import { createPackageSnapshot, removePackageSnapshot, securePackageInventory } from "./secure-package.js";
import { buildPackageInventory } from "./inventory.js";
import { validatePinnedSbatch } from "./slurm-policy.js";
import {
  allocationsOf, assertAggregateCapacity, cancelOwnedJob, fetchArtifacts, findOwnedOperationRun, stageAndValidate, stageRecipeWrapper,
  freshRunDirectoryCommand, reconcileSubmission, startTrackedJob, statusJob, submissionsOf, submitJob, validateEnvelope,
} from "./execution-core.js";

const SAFE_NAME_RE = /^[a-z0-9][a-z0-9-]*$/u;
const MAX_SESSION_PLANS = 32;

function clone(value) { return JSON.parse(JSON.stringify(value)); }

function describe(manifest) {
  return {
    schema_version: manifest.schemaVersion,
    project: manifest.project,
    description: manifest.description,
    local_root: manifest.localRoot,
    remote_root: manifest.remoteRoot,
    files: [...manifest.files],
    extra_dirs: [...manifest.extraDirs],
    operations: Object.entries(manifest.jobs).map(([name, job]) => ({
      name,
      form: "recipe",
      cpus: job.cpus,
      gpus: job.gpus,
      concurrency: job.concurrency,
      script: job.recipe.script,
      env: job.recipe.env,
      parameters: clone(job.recipe.parameters),
    })),
  };
}

function plansOf(state) { if (!Array.isArray(state.plans)) state.plans = []; return state.plans; }
function storePlan(state, record, logger = null) {
  const plans = plansOf(state);
  const duplicate = plans.findIndex((item) => item.planHash === record.planHash);
  if (duplicate >= 0) plans.splice(duplicate, 1);
  plans.push(record);
  while (plans.length > MAX_SESSION_PLANS) {
    const evicted = plans.shift();
    logger?.warn?.(`genbio session plan cache evicted oldest plan ${evicted?.planHash ?? "unknown"}; recreate it before execution if still needed`);
  }
}

export function createProjectTools({ makeTool, requirePolicy, requireState, publicState, config, runRemote, shell, userQuestions, jobs, requireRemoteAccess, projectSource: suppliedProjectSource, executionRegistry = null, logger = null, hydrateState = null }) {
  const projectSource = suppliedProjectSource ?? createProjectSource(config);
  const projectsDir = projectSource.projectsDir;
  const loadFor = (project, exec) => projectSource.loadProject(project, exec);
  const stateForExec = async (exec) => typeof hydrateState === "function" ? hydrateState(exec) : requireState(exec);

  const projectsTool = makeTool(
    "genbio_projects",
    "List locally configured Genbio projects and their manifest schema versions. Read-only: no remote access, transfer, allocation, or scheduler query.",
    {},
    async (_args, exec) => {
      requirePolicy();
      const state = await stateForExec(exec);
      const projects = [];
      for (const entry of await projectSource.listProjects(exec)) {
        try {
          const loaded = await loadFor(entry.project, exec);
          projects.push({ project: entry.project, origin: loaded.origin.kind, schema_version: loaded.manifest.schemaVersion, valid: true, operations: Object.keys(loaded.manifest.jobs).sort() });
        } catch (error) {
          projects.push({ project: entry.project, origin: entry.origin, valid: false, error: String(error?.message ?? error) });
        }
      }
      return { ok: true, status: { ...publicState(state), projects } };
    },
  );

  const describeTool = makeTool(
    "genbio_project_describe",
    "Describe one local Genbio project, its resources, operations, and typed parameters. Read-only and side-effect-free.",
    { project: { type: "string", required: true } },
    async (args, exec) => {
      requirePolicy();
      const state = await stateForExec(exec);
      const loaded = await loadFor(String(args.project), exec);
      return { ok: true, status: { ...publicState(state), project: { ...describe(loaded.manifest), origin: loaded.origin.kind }, manifest_sha256: loaded.manifestSha } };
    },
  );

  const inventoryTool = makeTool(
    "genbio_project_inventory",
    "Compute the content-addressed package inventory of one local project's manifest file set: per-file SHA-256 digests, sizes, and one deterministic package digest over the canonical table. Read-only local file reads: no remote access, transfer, allocation, or submission.",
    { project: { type: "string", required: true } },
    async (args, exec) => {
      requirePolicy();
      const state = await stateForExec(exec);
      const loaded = await loadFor(String(args.project), exec);
      const inventory = await buildPackageInventory(loaded.manifest, loaded.manifestSha);
      return { ok: true, status: { ...publicState(state), inventory: { ...inventory, origin: loaded.origin.kind } } };
    },
  );

  const planTool = makeTool(
    "genbio_project_plan",
    "Resolve one schema-v2 declarative operation locally into immutable policy-validated SBATCH bytes and a deterministic session-owned plan hash. Planning performs no remote access, question, transfer, background job, or scheduler action.",
    { project: { type: "string", required: true }, operation: { type: "string", required: true }, parameters: { type: "object", additionalProperties: true } },
    async (args, exec) => {
      const policy = requirePolicy();
      const state = await stateForExec(exec);
      if (!state.policy?.hash) throw new Error("current policy hash is unavailable");
      const loaded = await loadFor(String(args.project), exec);
      if (loaded.manifest.schemaVersion !== 2) throw new Error(`${loaded.manifest.project}: declarative planning requires schema_version 2`);
      const resolution = resolveRecipe({ manifest: loaded.manifest, operation: String(args.operation), parameters: args.parameters ?? {}, policy, envelope: state.envelope });
      const jobSpec = loaded.manifest.jobs[String(args.operation)];
      const validation = validatePinnedSbatch(resolution.sbatchText, jobSpec, { policy, envelope: state.envelope });
      const inventory = await securePackageInventory(loaded.manifest, loaded.manifestSha);
      const built = buildOperationPlan({ project: loaded.manifest.project, operation: String(args.operation), policyHash: state.policy.hash, manifestSha: loaded.manifestSha, packageSha: inventory.packageSha, resolution });
      const record = Object.freeze({
        planHash: built.planHash,
        plan: built.plan,
        // ORIGINAL supplied parameters (not the resolved values), so execution
        // can FRESH re-resolve the recipe exactly as it did at plan time. The
        // resolved values (plan.parameters) are stringified for int/bool and
        // cannot be re-fed to resolveRecipe; the plan HASH is computed from
        // plan.parameters, so storing these does not change determinism.
        parameters: clone(args.parameters ?? {}),
        manifestPath: loaded.path,
        origin: loaded.origin.kind,
        workspace: loaded.origin.workspace,
        sbatchText: resolution.sbatchText,
        createdAt: Date.now(),
        status: "planned",
      });
      storePlan(state, record, logger);
      return { ok: true, status: { ...publicState(state), planned: { plan_hash: record.planHash, ...clone(record.plan), wrapper: resolution.sbatchText, validation: clone(validation) } } };
    },
  );

  const executeTool = makeTool(
    "genbio_project_execute",
    "Execute one known session-owned immutable Genbio plan. FRESH re-reads the manifest and active policy, rejects plan drift before any side effect, stages the manifest package and content-addressed recipe wrapper with rclone plus SHA-256 and clean-env bash validation, then submits through the project-neutral atomic exact-once execution core. An ambiguous submission is never resubmitted; an accepted job reserves capacity until terminal evidence. Collect status with genbio_project_status.",
    { plan_hash: { type: "string", required: true } },
    async (args, exec) => {
      const policy = requirePolicy();
      const state = await stateForExec(exec);
      const hash = String(args.plan_hash);
      const record = plansOf(state).find((item) => item.planHash === hash);
      if (!record) throw new Error("unknown or expired session-owned Genbio plan hash");
      // The execution integration must be fully wired (the real deployment in
      // lib/index.js passes every dependency). Fail closed — no remote action —
      // when it is not, so a partial assembly can never submit.
      if (typeof runRemote !== "function" || typeof shell !== "object" || !jobs || typeof requireRemoteAccess !== "function") {
        throw new Error("genbio_project_execute is fail-closed until the shared execution-core integration is complete and wired (runRemote, shell, jobs, requireRemoteAccess); no remote action was taken");
      }
      const { project, operation } = record.plan;
      // ── FRESH re-read + drift rejection (before any side effect) ──
      // Re-read the manifest bytes and the active policy, re-resolve the recipe
      // with the plan's ORIGINAL stored parameters, re-validate the wrapper
      // bytes, and recompute the plan hash. A changed manifest (manifestSha),
      // policy generation (policyHash), or recipe resolution (bytesSha) changes
      // the hash → fail closed before any staging or submission.
      const loaded = await loadFor(project, exec);
      if (loaded.origin.kind !== record.origin || loaded.origin.workspace !== record.workspace || loaded.path !== record.manifestPath) throw new Error("project manifest source drift: the approved workspace/configured source changed; nothing is staged or submitted");
      if (loaded.manifest.schemaVersion !== 2) throw new Error(`${project}: execution requires a schema_version 2 recipe manifest`);
      const jobSpec = loaded.manifest.jobs[operation];
      if (!jobSpec?.recipe) throw new Error(`${project}: operation ${operation} is not a declarative recipe; nothing is staged or submitted`);
      const resolution = resolveRecipe({ manifest: loaded.manifest, operation, parameters: record.parameters ?? {}, policy, envelope: state.envelope });
      validatePinnedSbatch(resolution.sbatchText, jobSpec, { policy, envelope: state.envelope });
      const inventory = await securePackageInventory(loaded.manifest, loaded.manifestSha);
      const built = buildOperationPlan({ project: loaded.manifest.project, operation, policyHash: state.policy.hash, manifestSha: loaded.manifestSha, packageSha: inventory.packageSha, resolution });
      if (built.planHash !== record.planHash) throw new Error(`plan hash drift: recomputed ${built.planHash} != approved ${record.planHash}; the manifest or policy changed after the plan was approved — nothing is staged or submitted`);
      // ── Require the HPC/gpus envelope ──
      validateEnvelope(state, { cpus: jobSpec.cpus, gpus: jobSpec.gpus, concurrency: jobSpec.concurrency });
      const wrapperBytes = Buffer.from(resolution.sbatchText, "utf8");
      const wrapperSha = resolution.bytesSha; // === sha256(wrapperBytes)
      const shortHash = loaded.manifestSha.slice(0, 12);
      const wrapperRel = `genbio-recipes/${shortHash}.${operation}.sbatch`;
      // ── Atomic pair-lock admission (contiguous, project-neutral core) ──
      // No await between the in-flight check and the two pushes: atomic under
      // the single-threaded event loop, so two CONCURRENT executes for the same
      // (project, operation) can never both reserve — the second is rejected
      // before any sbatch. The reservation reserves full resources as
      // "submitting" immediately, keeping the aggregate capacity gate accurate.
      const unresolved = submissionsOf(state).some((entry) => entry.project === project && entry.operation === operation && entry.status === "ambiguous");
      const inFlight = allocationsOf(state).some((entry) => entry.project === project && entry.operation === operation && entry.status === "submitting");
      if (inFlight) throw new Error(`${project}: operation ${operation} already has an in-flight submission; wait for it to settle (or collect terminal evidence) before submitting again`);
      if (!unresolved) assertAggregateCapacity(state, { cpus: jobSpec.cpus, gpus: jobSpec.gpus, concurrency: jobSpec.concurrency });
      let durable = null;
      if (executionRegistry && !unresolved) durable = (await executionRegistry.reserve({ sessionId: exec.agent.session.id, workspace: record.workspace ?? exec?.agent?.session?.header?.cwd, project, operation, planHash: record.planHash, manifestSha: loaded.manifestSha, packageSha: inventory.packageSha, wrapperSha, policyHash: state.policy.hash, remoteBase: loaded.manifest.remoteRoot, cpus: jobSpec.cpus, gpus: jobSpec.gpus, concurrency: jobSpec.concurrency, node: resolution.node, partition: resolution.partition, envelope: state.envelope })).record;
      const token = durable?.token ?? randomBytes(16).toString("hex");
      const intent = { project, operation, templateSha: wrapperSha, token, uniqueJobName: null, status: "attempted", slurmJobId: null, submittedAt: durable?.createdAt ?? Date.now(), durableRunId: durable?.runId ?? null };
      const allocation = { slurmJobId: null, project, operation, cpus: jobSpec.cpus, gpus: jobSpec.gpus, concurrency: jobSpec.concurrency, status: "submitting", submittedAt: durable?.createdAt ?? Date.now(), source: durable ? "durable-admission" : "admission", durableRunId: durable?.runId ?? null };
      intent.allocation = allocation;
      submissionsOf(state).push(intent);
      allocationsOf(state).push(allocation);
      // ── end atomic admission ──
      try {
        await requireRemoteAccess("HPC", [{ root: loaded.manifest.remoteRoot, write: true }], exec, state);
      } catch (error) {
        allocation.status = "failed"; allocation.source = "remote access not granted";
        intent.status = "failed"; intent.note = "remote access not granted";
        if (executionRegistry && durable) await executionRegistry.update(durable.runId, (item) => ({ ...item, status: "failed", helperStatus: "failed", workloadStatus: "failed", allocationStatus: "failed", finishedAt: Date.now(), workloadEvidence: "remote-access-denied", note: String(error?.message ?? error).slice(0, 512) }));
        throw error;
      }
      const run = startTrackedJob({
        jobs, exec, state, project, operation,
        resources: { cpus: jobSpec.cpus, gpus: jobSpec.gpus, concurrency: jobSpec.concurrency },
        label: `HPC project ${project} ${operation}`,
        logMaxBytes: config.logMaxBytes,
        runBody: async ({ exec: runExec }) => {
          let snapshot = null;
          try {
            snapshot = await createPackageSnapshot(loaded.manifest, loaded.manifestSha, record.plan.packageSha);
            const executionManifest = durable ? Object.freeze({ ...snapshot.manifest, remoteRoot: durable.remoteRunDir }) : snapshot.manifest;
            if (durable) {
              const created = await runRemote("HPC", freshRunDirectoryCommand(durable.remoteBase, durable.remoteRunDir, "HPC"), runExec, 30000);
              if (created.exitCode !== 0) throw new Error(`${project}: fresh remote attempt directory creation failed: ${created.stderr || created.stdout || created.exitCode}`);
            }
            await stageAndValidate({ manifest: executionManifest, exec: runExec, userQuestions, shell, runRemote, config, extraOwnPrefixes: ["genbio-recipes"] });
            const staged = await stageRecipeWrapper({ manifest: executionManifest, wrapperBytes, operation, manifestSha: loaded.manifestSha, exec: runExec, userQuestions, shell, runRemote, config });
            const outcome = await submitJob({ manifest: executionManifest, operation, policy, state, exec: runExec, runRemote, intent, allocation, templateSha: wrapperSha, templateRel: staged.wrapperRel, templateBytes: wrapperBytes, beforeDispatch: executionRegistry && durable ? async ({ uniqueJobName }) => executionRegistry.update(durable.runId, (item) => ({ ...item, uniqueJobName, sbatchIssued: true, workloadStatus: "submitting", allocationStatus: "ambiguous", workloadEvidence: "sbatch-dispatch-started" })) : null });
            if (executionRegistry && durable) await executionRegistry.update(durable.runId, (item) => ({ ...item, uniqueJobName: intent.uniqueJobName, sbatchIssued: true, slurmJobId: outcome.slurmJobId, status: "completed", helperStatus: "completed", workloadStatus: "submitted", allocationStatus: "nonterminal", workloadEvidence: "scheduler-job-id-confirmed", note: null }));
            return { stdout: `${outcome.stdout}\n`, stderr: outcome.stderr, exitCode: 0, slurmJobId: outcome.slurmJobId };
          } catch (error) {
            // Settle the in-flight reservation only if still "submitting"
            // (submitJob threw before any of its own transitions). sbatchIssued
            // distinguishes the two exact-once safety outcomes:
            //   set   → sbatch was in flight (job MAY be accepted): stay ambiguous,
            //           the exact-once gate keeps the pair from re-entering.
            //   unset → no sbatch dispatched: a DEFINITE failure, safe to release.
            if (allocation.status === "submitting") {
              if (intent.sbatchIssued) {
                intent.status = "ambiguous"; intent.note = "exception after sbatch dispatch (job may have been accepted)";
                allocation.status = "ambiguous"; allocation.source = "exception after sbatch dispatch";
              } else {
                allocation.status = "failed"; allocation.source = "pre-sbatch failure";
                if (intent.status === "attempted") { intent.status = "failed"; intent.note = "pre-sbatch failure"; }
              }
            }
            if (executionRegistry && durable) await executionRegistry.update(durable.runId, (item) => ({ ...item, uniqueJobName: intent.uniqueJobName ?? item.uniqueJobName, sbatchIssued: intent.sbatchIssued === true, slurmJobId: intent.slurmJobId ?? item.slurmJobId, status: "failed", helperStatus: "failed", workloadStatus: intent.sbatchIssued ? "reconciling" : "failed", allocationStatus: intent.sbatchIssued ? "ambiguous" : "failed", finishedAt: intent.sbatchIssued ? null : Date.now(), note: String(error?.message ?? error).slice(0, 512) }));
            throw error;
          } finally {
            await removePackageSnapshot(snapshot);
          }
        },
      });
      if (durable) { run.runId = durable.runId; run.attemptId = durable.attemptId; run.remoteRunDir = durable.remoteRunDir; }
      return { ok: true, status: { ...publicState(state), started: run, durable_run: durable ? { run_id: durable.runId, attempt_id: durable.attemptId, submission_state: durable.workloadStatus, remote_run_dir: durable.remoteRunDir } : null } };
    },
  );

  const statusTool = makeTool(
    "genbio_project_status",
    "Show session-owned project plans and related runs. By default, a run_id or exact operation/job_id refreshes bounded scheduler evidence and may persist reconciliation metadata. Set reconcile=false for a local-only durable snapshot.",
    { project: { type: "string", required: true }, run_id: { type: "string" }, operation: { type: "string" }, job_id: { type: "string" }, reconcile: { type: "boolean" } },
    async (args, exec) => {
      requirePolicy();
      const state = await stateForExec(exec);
      const project = String(args.project);
      const reconcile = args.reconcile !== false;
      if (!SAFE_NAME_RE.test(project)) throw new Error(`invalid project name: ${project}`);
      const loaded = await loadFor(project, exec);
      if (!reconcile && (args.job_id !== undefined || args.operation !== undefined)) throw new Error("reconcile=false supports durable run_id or project-local status only; exact job_id status always requires scheduler reconciliation");
      let reconciliationPending = false;
      if (args.run_id !== undefined) {
        if (!executionRegistry) throw new Error("durable execution registry is unavailable");
        const durable = await executionRegistry.find(String(args.run_id));
        if (!durable || durable.project !== project || durable.sessionId !== exec.agent.session.id) throw new Error(`run ${args.run_id} is not an owned durable run for ${project}`);
        let ownedDurable = durable;
        if (!ownedDurable.slurmJobId && ownedDurable.sbatchIssued && ownedDurable.uniqueJobName !== "pending") {
          reconciliationPending = true;
          if (reconcile) {
            await requireRemoteAccess("HPC", [{ root: ownedDurable.remoteRunDir, write: false }], exec, state);
            const reconciledJobId = await reconcileSubmission({ exec, runRemote, jobName: ownedDurable.uniqueJobName });
            if (reconciledJobId) {
              ownedDurable = await executionRegistry.update(ownedDurable.runId, (item) => ({ ...item, slurmJobId: reconciledJobId, workloadStatus: "submitted", allocationStatus: "nonterminal", workloadEvidence: "scheduler-reconciled-by-unique-name", note: null }));
              reconciliationPending = false;
            }
          }
        }
        if (ownedDurable.slurmJobId && reconcile) {
          const run = state.runs.find((item) => item.runId === ownedDurable.runId) ?? { runId: ownedDurable.runId, target: "HPC", operation: `project-${project}-${ownedDurable.operation}`, slurmJobId: ownedDurable.slurmJobId, workloadStatus: ownedDurable.workloadStatus };
          if (!state.runs.includes(run)) state.runs.push(run);
          if (!submissionsOf(state).some((item) => item.slurmJobId === ownedDurable.slurmJobId)) submissionsOf(state).push({ project, operation: ownedDurable.operation, uniqueJobName: ownedDurable.uniqueJobName, slurmJobId: ownedDurable.slurmJobId, status: "submitted", token: ownedDurable.token });
          if (!allocationsOf(state).some((item) => item.slurmJobId === ownedDurable.slurmJobId)) allocationsOf(state).push({ project, operation: ownedDurable.operation, slurmJobId: ownedDurable.slurmJobId, status: ownedDurable.allocationStatus, cpus: ownedDurable.cpus, gpus: ownedDurable.gpus, concurrency: ownedDurable.concurrency });
          await requireRemoteAccess("HPC", [{ root: ownedDurable.remoteRunDir, write: false }], exec, state);
          const result = await statusJob({ manifest: Object.freeze({ ...loaded.manifest, remoteRoot: ownedDurable.remoteRunDir }), jobId: ownedDurable.slurmJobId, state, exec, runRemote });
          if (result.scheduler.state === null) run.workloadEvidence = result.scheduler.evidence;
          await executionRegistry.update(ownedDurable.runId, (item) => ({ ...item, slurmState: result.scheduler.state, exitCode: result.scheduler.exitCode, elapsed: result.scheduler.elapsed, workloadStatus: run.workloadStatus, allocationStatus: ["completed", "failed", "cancelled"].includes(run.workloadStatus) ? "terminal" : item.allocationStatus, workloadEvidence: run.workloadEvidence, finishedAt: ["completed", "failed", "cancelled"].includes(run.workloadStatus) ? Date.now() : item.finishedAt }));
        }
      } else if (args.job_id !== undefined || args.operation !== undefined) {
        const jobId = String(args.job_id ?? "");
        const operation = String(args.operation ?? "");
        if (!SAFE_NAME_RE.test(operation) || !/^[0-9]{1,10}$/u.test(jobId)) throw new Error("project scheduler status requires a safe operation and exact numeric job_id");
        if (!findOwnedOperationRun(state, project, operation, jobId)) throw new Error(`Slurm job ${jobId} is not a session-owned run for ${project}/${operation}`);
        await requireRemoteAccess("HPC", [{ root: loaded.manifest.remoteRoot, write: false }], exec, state);
        await statusJob({ manifest: loaded.manifest, jobId, state, exec, runRemote });
      }
      const plans = plansOf(state).filter((item) => item.plan.project === project).map((item) => ({ plan_hash: item.planHash, operation: item.plan.operation, status: item.status, created_at: item.createdAt, bytes_sha256: item.plan.bytesSha, origin: item.origin }));
      const runs = state.runs.filter((run) => String(run.operation).startsWith(`project-${project}-`));
      const durableRuns = executionRegistry ? (await executionRegistry.list()).filter((run) => run.project === project && run.sessionId === exec.agent.session.id) : [];
      return { ok: true, status: { ...publicState(state), project, origin: loaded.origin.kind, plans, project_runs: clone(runs), durable_runs: durableRuns, scheduler_evidence: reconcile ? "refreshed-when-requested" : "skipped", ...(reconciliationPending ? { reconciliation_pending: true } : {}) } };
    },
  );


  const cancelTool = makeTool(
    "genbio_project_cancel",
    "Cancel one exact active session-owned schema-v2 project Slurm job after explicit confirmation. Broad cancellation is unsupported.",
    { project: { type: "string", required: true }, operation: { type: "string", required: true }, job_id: { type: "string", required: true } },
    async (args, exec) => {
      requirePolicy();
      const state = await stateForExec(exec);
      const project = String(args.project); const operation = String(args.operation); const jobId = String(args.job_id);
      if (!SAFE_NAME_RE.test(project) || !SAFE_NAME_RE.test(operation)) throw new Error("project cancellation requires safe project and operation names");
      await loadFor(project, exec);
      if (!executionRegistry) throw new Error("durable execution registry is unavailable");
      const durable = (await executionRegistry.list()).find((item) => item.project === project && item.operation === operation && item.slurmJobId === jobId && item.sessionId === exec.agent.session.id);
      if (!durable?.uniqueJobName) throw new Error(`Slurm job ${jobId} has no owned durable identity for ${project}/${operation}`);
      const cancellation = await cancelOwnedJob({ state, project, operation, jobId, exec, userQuestions, runRemote, expectedJobName: durable.uniqueJobName, beforeCancel: async () => executionRegistry.update(durable.runId, (item) => ({ ...item, allocationStatus: "cancel-requested", cancelRequestedAt: Date.now(), workloadEvidence: "scancel-dispatch-started" })) });
      return { ok: true, status: { ...publicState(state), cancellation } };
    },
  );

  const fetchTool = makeTool(
    "genbio_project_fetch",
    "Retrieve manifest-allowlisted small artifacts for one schema-v2 project with a byte cap and remote/local SHA-256 equality. Read-only remotely and requires explicit retrieval approval.",
    { project: { type: "string", required: true }, run_id: { type: "string", required: true }, files: { type: "array", items: { type: "string" } } },
    async (args, exec) => {
      requirePolicy();
      const state = await stateForExec(exec);
      const loaded = await loadFor(String(args.project), exec);
      if (!executionRegistry) throw new Error("durable execution registry is unavailable");
      const durable = await executionRegistry.find(String(args.run_id));
      if (!durable || durable.project !== loaded.manifest.project || durable.sessionId !== exec.agent.session.id) throw new Error(`run ${args.run_id} is not an owned durable run for ${loaded.manifest.project}`);
      await requireRemoteAccess("HPC", [{ root: durable.remoteRunDir, write: false }], exec, state);
      const fetchManifest = Object.freeze({ ...loaded.manifest, remoteRoot: durable.remoteRunDir, fetch: loaded.manifest.fetch ? Object.freeze({ ...loaded.manifest.fetch, dest: `${loaded.manifest.fetch.dest}/${durable.runId}` }) : null });
      const requested = Array.isArray(args.files) ? args.files.map(String) : [];
      const run = startTrackedJob({ jobs, exec, state, project: loaded.manifest.project, operation: "fetch", resources: { cpus: 0, gpus: 0, concurrency: 1 }, label: `HPC project ${loaded.manifest.project} fetch`, logMaxBytes: config.logMaxBytes, runBody: async ({ exec: runExec }) => {
        const outcome = await fetchArtifacts({ manifest: fetchManifest, requested, exec: runExec, userQuestions, shell, runRemote, config });
        return { stdout: outcome.stdout, stderr: outcome.stderr, exitCode: 0 };
      } });
      return { ok: true, status: { ...publicState(state), started: run } };
    },
  );

  return { projectsTool, describeTool, inventoryTool, planTool, executeTool, statusTool, cancelTool, fetchTool, projectsDir, projectSource };
}

export { MAX_SESSION_PLANS };
