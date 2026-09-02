import { createHash, randomBytes } from "node:crypto";
import { readFile, readdir } from "node:fs/promises";
import { join } from "node:path";
import { load as parseYaml } from "js-yaml";
import { buildOperationPlan, parseProjectManifest, resolveRecipe } from "./project.js";
import { buildPackageInventory } from "./inventory.js";
import { validatePinnedSbatch } from "./slurm-policy.js";
import {
  allocationsOf, assertAggregateCapacity, stageAndValidate, stageRecipeWrapper,
  startTrackedJob, submissionsOf, submitJob, validateEnvelope,
} from "./pinned.js";

const SAFE_NAME_RE = /^[a-z0-9][a-z0-9-]*$/u;
const MAX_SESSION_PLANS = 32;

function sha256(data) { return createHash("sha256").update(data).digest("hex"); }
function clone(value) { return JSON.parse(JSON.stringify(value)); }
function projectsDirOf(config) { return config.pinnedProjectsDir ?? "/Users/mdanh/.dsh/profiles/desktop/genbio-pinned-projects"; }

async function manifestNames(projectsDir) {
  let entries;
  try { entries = await readdir(projectsDir); } catch (error) { throw new Error(`cannot list Genbio projects: ${String(error?.message ?? error)}`); }
  return entries.filter((name) => name.endsWith(".yaml")).map((name) => name.slice(0, -5)).filter((name) => SAFE_NAME_RE.test(name)).sort();
}

async function loadProject(projectsDir, project) {
  if (typeof project !== "string" || !SAFE_NAME_RE.test(project)) throw new Error(`invalid project name: ${project}`);
  const path = join(projectsDir, `${project}.yaml`);
  let text;
  try { text = await readFile(path, "utf8"); } catch { throw new Error(`unknown Genbio project: ${project}`); }
  let parsed;
  try { parsed = parseYaml(text, { json: false }); } catch (error) { throw new Error(`${project}: invalid YAML: ${String(error?.reason ?? error?.message ?? error)}`); }
  return { path, text, manifestSha: sha256(text), manifest: parseProjectManifest(project, parsed) };
}

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
      form: job.recipe ? "recipe" : "template",
      cpus: job.cpus,
      gpus: job.gpus,
      concurrency: job.concurrency,
      ...(job.recipe ? {
        script: job.recipe.script,
        env: job.recipe.env,
        parameters: clone(job.recipe.parameters),
      } : { template: job.template }),
    })),
  };
}

function plansOf(state) { if (!Array.isArray(state.plans)) state.plans = []; return state.plans; }
function storePlan(state, record) {
  const plans = plansOf(state);
  const duplicate = plans.findIndex((item) => item.planHash === record.planHash);
  if (duplicate >= 0) plans.splice(duplicate, 1);
  plans.push(record);
  while (plans.length > MAX_SESSION_PLANS) plans.shift();
}

export function createProjectTools({ makeTool, requirePolicy, requireState, publicState, config, runRemote, shell, userQuestions, jobs, requireRemoteAccess }) {
  const projectsDir = projectsDirOf(config);

  const projectsTool = makeTool(
    "genbio_projects",
    "List locally configured Genbio projects and their manifest schema versions. Read-only: no remote access, transfer, allocation, or scheduler query.",
    {},
    async (_args, exec) => {
      requirePolicy();
      const state = requireState(exec);
      const projects = [];
      for (const project of await manifestNames(projectsDir)) {
        try {
          const loaded = await loadProject(projectsDir, project);
          projects.push({ project, schema_version: loaded.manifest.schemaVersion, valid: true, operations: Object.keys(loaded.manifest.jobs).sort() });
        } catch (error) {
          projects.push({ project, valid: false, error: String(error?.message ?? error) });
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
      const state = requireState(exec);
      const loaded = await loadProject(projectsDir, String(args.project));
      return { ok: true, status: { ...publicState(state), project: describe(loaded.manifest), manifest_sha256: loaded.manifestSha } };
    },
  );

  const inventoryTool = makeTool(
    "genbio_project_inventory",
    "Compute the content-addressed package inventory of one local project's manifest file set: per-file SHA-256 digests, sizes, and one deterministic package digest over the canonical table. Read-only local file reads: no remote access, transfer, allocation, or submission.",
    { project: { type: "string", required: true } },
    async (args, exec) => {
      requirePolicy();
      const state = requireState(exec);
      const loaded = await loadProject(projectsDir, String(args.project));
      const inventory = await buildPackageInventory(loaded.manifest, loaded.manifestSha);
      return { ok: true, status: { ...publicState(state), inventory } };
    },
  );

  const planTool = makeTool(
    "genbio_project_plan",
    "Resolve one schema-v2 declarative operation locally into immutable policy-validated SBATCH bytes and a deterministic session-owned plan hash. Planning performs no remote access, question, transfer, background job, or scheduler action.",
    { project: { type: "string", required: true }, operation: { type: "string", required: true }, parameters: { type: "object", additionalProperties: true } },
    async (args, exec) => {
      const policy = requirePolicy();
      const state = requireState(exec);
      if (!state.policy?.hash) throw new Error("current policy hash is unavailable");
      const loaded = await loadProject(projectsDir, String(args.project));
      if (loaded.manifest.schemaVersion !== 2) throw new Error(`${loaded.manifest.project}: declarative planning requires schema_version 2`);
      const resolution = resolveRecipe({ manifest: loaded.manifest, operation: String(args.operation), parameters: args.parameters ?? {}, policy, envelope: state.envelope });
      const jobSpec = loaded.manifest.jobs[String(args.operation)];
      const validation = validatePinnedSbatch(resolution.sbatchText, jobSpec, { policy, envelope: state.envelope });
      const built = buildOperationPlan({ project: loaded.manifest.project, operation: String(args.operation), policyHash: state.policy.hash, manifestSha: loaded.manifestSha, resolution });
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
        sbatchText: resolution.sbatchText,
        createdAt: Date.now(),
        status: "planned",
      });
      storePlan(state, record);
      return { ok: true, status: { ...publicState(state), planned: { plan_hash: record.planHash, ...clone(record.plan), wrapper: resolution.sbatchText, validation: clone(validation) } } };
    },
  );

  const executeTool = makeTool(
    "genbio_project_execute",
    "Execute one known session-owned immutable Genbio plan. FRESH re-reads the manifest and the active policy, re-resolves the recipe with the plan's stored parameters, and requires the recomputed plan hash to EQUAL the approved plan_hash (drift rejection before any side effect). Stages the manifest package and the content-addressed recipe wrapper (rclone-only, SHA-256 verified, clean-env bash -n), then submits through the shared atomic exact-once pinned core: envelope fit, aggregate capacity, an atomic pair-lock admission with a 128-bit CSPRNG token, one sbatch, and probe / read-only reconciliation / output parsing exactly as genbio_pinned_job. An ambiguous submission is never resubmitted; an accepted job reserves envelope capacity until terminal evidence. Collect status with genbio_project_status.",
    { plan_hash: { type: "string", required: true } },
    async (args, exec) => {
      const policy = requirePolicy();
      const state = requireState(exec);
      const hash = String(args.plan_hash);
      const record = plansOf(state).find((item) => item.planHash === hash);
      if (!record) throw new Error("unknown or expired session-owned Genbio plan hash");
      // The execution integration must be fully wired (the real deployment in
      // lib/index.js passes every dependency). Fail closed — no remote action —
      // when it is not, so a partial assembly can never submit.
      if (typeof runRemote !== "function" || typeof shell !== "object" || !jobs || typeof requireRemoteAccess !== "function") {
        throw new Error("genbio_project_execute is fail-closed until the shared pinned submission integration is complete and wired (runRemote, shell, jobs, requireRemoteAccess); no remote action was taken");
      }
      const { project, operation } = record.plan;
      // ── FRESH re-read + drift rejection (before any side effect) ──
      // Re-read the manifest bytes and the active policy, re-resolve the recipe
      // with the plan's ORIGINAL stored parameters, re-validate the wrapper
      // bytes, and recompute the plan hash. A changed manifest (manifestSha),
      // policy generation (policyHash), or recipe resolution (bytesSha) changes
      // the hash → fail closed before any staging or submission.
      const loaded = await loadProject(projectsDir, project);
      if (loaded.manifest.schemaVersion !== 2) throw new Error(`${project}: execution requires a schema_version 2 recipe manifest`);
      const jobSpec = loaded.manifest.jobs[operation];
      if (!jobSpec?.recipe) throw new Error(`${project}: operation ${operation} is not a declarative recipe; nothing is staged or submitted`);
      const resolution = resolveRecipe({ manifest: loaded.manifest, operation, parameters: record.parameters ?? {}, policy, envelope: state.envelope });
      validatePinnedSbatch(resolution.sbatchText, jobSpec, { policy, envelope: state.envelope });
      const built = buildOperationPlan({ project: loaded.manifest.project, operation, policyHash: state.policy.hash, manifestSha: loaded.manifestSha, resolution });
      if (built.planHash !== record.planHash) throw new Error(`plan hash drift: recomputed ${built.planHash} != approved ${record.planHash}; the manifest or policy changed after the plan was approved — nothing is staged or submitted`);
      // ── Require the HPC/gpus envelope ──
      validateEnvelope(state, { cpus: jobSpec.cpus, gpus: jobSpec.gpus, concurrency: jobSpec.concurrency });
      const wrapperBytes = Buffer.from(resolution.sbatchText, "utf8");
      const wrapperSha = resolution.bytesSha; // === sha256(wrapperBytes)
      const shortHash = loaded.manifestSha.slice(0, 12);
      const wrapperRel = `genbio-recipes/${shortHash}.${operation}.sbatch`;
      // ── Atomic pair-lock admission (contiguous, like genbio_pinned_job) ──
      // No await between the in-flight check and the two pushes: atomic under
      // the single-threaded event loop, so two CONCURRENT executes for the same
      // (project, operation) can never both reserve — the second is rejected
      // before any sbatch. The reservation reserves full resources as
      // "submitting" immediately, keeping the aggregate capacity gate accurate.
      const inFlight = allocationsOf(state).some((entry) => entry.project === project && entry.operation === operation && entry.status === "submitting");
      if (inFlight) throw new Error(`${project}: operation ${operation} already has an in-flight submission; wait for it to settle (or collect terminal evidence) before submitting again`);
      assertAggregateCapacity(state, { cpus: jobSpec.cpus, gpus: jobSpec.gpus, concurrency: jobSpec.concurrency });
      const token = randomBytes(16).toString("hex"); // 128-bit CSPRNG exact-once token
      const intent = { project, operation, templateSha: wrapperSha, token, uniqueJobName: null, status: "attempted", slurmJobId: null, submittedAt: Date.now() };
      const allocation = { slurmJobId: null, project, operation, cpus: jobSpec.cpus, gpus: jobSpec.gpus, concurrency: jobSpec.concurrency, status: "submitting", submittedAt: Date.now(), source: "admission" };
      intent.allocation = allocation;
      submissionsOf(state).push(intent);
      allocationsOf(state).push(allocation);
      // ── end atomic admission ──
      try {
        await requireRemoteAccess("HPC", [{ root: loaded.manifest.remoteRoot, write: true }], exec, state);
      } catch (error) {
        allocation.status = "failed"; allocation.source = "remote access not granted";
        intent.status = "failed"; intent.note = "remote access not granted";
        throw error;
      }
      const run = startTrackedJob({
        jobs, exec, state, project, operation,
        resources: { cpus: jobSpec.cpus, gpus: jobSpec.gpus, concurrency: jobSpec.concurrency },
        label: `HPC project ${project} ${operation}`,
        logMaxBytes: config.logMaxBytes,
        runBody: async ({ exec: runExec }) => {
          try {
            // Stage the manifest package (rclone-only, SHA-256 verified) treating
            // genbio-recipes/ as this project's own area (idempotent over a
            // previous wrapper), then stage the content-addressed recipe wrapper.
            await stageAndValidate({ manifest: loaded.manifest, exec: runExec, userQuestions, shell, runRemote, config, extraOwnPrefixes: ["genbio-recipes"] });
            const staged = await stageRecipeWrapper({ manifest: loaded.manifest, wrapperBytes, operation, manifestSha: loaded.manifestSha, exec: runExec, userQuestions, shell, runRemote, config });
            // Submit through the SHARED exact-once core (generalized submitJob):
            // TOCTOU digest on the in-memory wrapper bytes, exact-once gate,
            // package verify, node probe, one sbatch, read-only reconciliation.
            const outcome = await submitJob({ manifest: loaded.manifest, operation, policy, state, exec: runExec, runRemote, intent, allocation, templateSha: wrapperSha, templateRel: staged.wrapperRel, templateBytes: wrapperBytes });
            return { stdout: `${outcome.stdout}\n`, stderr: outcome.stderr, exitCode: 0, slurmJobId: outcome.slurmJobId };
          } catch (error) {
            // Settle the in-flight reservation only if still "submitting"
            // (submitJob threw before any of its own transitions). sbatchIssued
            // distinguishes the two safety outcomes, exactly as genbio_pinned_job:
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
            throw error;
          }
        },
      });
      return { ok: true, started: true, status: { ...publicState(state), started: run } };
    },
  );

  const statusTool = makeTool(
    "genbio_project_status",
    "Show session-owned immutable project plans and related run records without remote access. Scheduler aggregation is added with the execution integration.",
    { project: { type: "string", required: true } },
    async (args, exec) => {
      requirePolicy();
      const state = requireState(exec);
      const project = String(args.project);
      if (!SAFE_NAME_RE.test(project)) throw new Error(`invalid project name: ${project}`);
      const plans = plansOf(state).filter((item) => item.plan.project === project).map((item) => ({ plan_hash: item.planHash, operation: item.plan.operation, status: item.status, created_at: item.createdAt, bytes_sha256: item.plan.bytesSha }));
      const runs = state.runs.filter((run) => run.operation === `project-${project}` || String(run.operation).startsWith(`pinned-${project}-`));
      return { ok: true, status: { ...publicState(state), project, plans, project_runs: clone(runs) } };
    },
  );

  return { projectsTool, describeTool, inventoryTool, planTool, executeTool, statusTool, projectsDir };
}

export { loadProject, manifestNames, MAX_SESSION_PLANS };
