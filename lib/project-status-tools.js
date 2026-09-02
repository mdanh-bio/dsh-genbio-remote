// ── Aggregate project status tool (Phase 3): read-only local rollup ──────────
// Local manifest reads + session-state folding only: no remote access, no
// transfer, no allocation, no scheduler query, no background job, and no
// mutation of session state. The aggregate is explicitly scoped to what the
// plugin can prove locally; scheduler aggregation arrives with the execution
// integration and is not claimed here.
//
// This module imports only the read-only discovery helpers from
// project-tools.js (which themselves perform local file reads) plus the pure
// aggregate fold. It carries no runRemote, shell, jobs, userQuestions,
// requireRemoteAccess, rclone, or scp surface.
import { aggregateProjects } from "./aggregate.js";
import { createProjectSource } from "./project-source.js";

function projectsDirOf(config) { return createProjectSource(config).projectsDir; }

export function createProjectStatusTools({ makeTool, requirePolicy, requireState, publicState, config, projectSource: suppliedProjectSource }) {
  const projectSource = suppliedProjectSource ?? createProjectSource(config);
  const projectsDir = projectSource.projectsDir;

  const statusAllTool = makeTool(
    "genbio_projects_status",
    "Aggregate local Genbio project status across every discovered project: per-project operations with resources, session plan records, run rollups, and which operations are active, suggested next, or need review. Read-only local summary of manifest metadata and session state — no remote access, transfer, allocation, background job, or scheduler query is ever performed; scheduler aggregation is added with the execution integration.",
    {},
    async (_args, exec) => {
      requirePolicy();
      const state = requireState(exec);
      const projectSummaries = [];
      for (const entry of await projectSource.listProjects(exec)) {
        try {
          const loaded = await projectSource.loadProject(entry.project, exec);
          projectSummaries.push({
            project: entry.project,
            origin: loaded.origin.kind,
            schema_version: loaded.manifest.schemaVersion,
            valid: true,
            description: loaded.manifest.description.slice(0, 200),
            local_root: loaded.manifest.localRoot,
            remote_root: loaded.manifest.remoteRoot,
            operations: Object.entries(loaded.manifest.jobs).map(([name, job]) => ({
              name,
              form: job.recipe ? "recipe" : "template",
              cpus: job.cpus,
              gpus: job.gpus,
              concurrency: job.concurrency,
            })),
          });
        } catch (error) {
          projectSummaries.push({ project: entry.project, origin: entry.origin, valid: false, error: String(error?.message ?? error).slice(0, 300) });
        }
      }
      // Read-only: unlike the planning path, this tool never initializes or
      // mutates session plan/run storage — it only folds what already exists.
      const plans = Array.isArray(state.plans) ? state.plans : [];
      const runs = Array.isArray(state.runs) ? state.runs : [];
      const aggregate = aggregateProjects({ projectSummaries, plans, runs });
      return {
        ok: true,
        status: { ...publicState(state), projects: projectSummaries, projects_status: aggregate.projects_status, unattributed_runs: aggregate.unattributed },
      };
    },
  );

  return { statusAllTool, projectsDir };
}

export { projectsDirOf as PROJECT_STATUS_PROJECTS_DIR_OF };