// ── Read-only workflow/DAG planning tool (Phase 2 foundation) ────────────────
// Local manifest reads only: no remote access, no transfer, no allocation, no
// scheduler query, and — by construction of planWorkflow — no execution or
// submission surface. The tool marks which workflow nodes are ready given the
// caller-supplied completed set; it never advances anything.
import { join } from "node:path";
import { loadProject } from "./project-tools.js";
import { loadWorkflowFile, planWorkflow } from "./workflow.js";

const DEFAULT_PROJECTS_DIR = "/Users/mdanh/.dsh/profiles/desktop/genbio-pinned-projects";

export function createWorkflowTools({ makeTool, requirePolicy, requireState, publicState, config }) {
  const projectsDir = typeof config?.pinnedProjectsDir === "string" && config.pinnedProjectsDir.length > 0 ? config.pinnedProjectsDir : DEFAULT_PROJECTS_DIR;
  const workflowsDir = join(projectsDir, "workflows");

  const planTool = makeTool(
    "genbio_workflow_plan",
    "Plan one local workflow DAG from the projects directory's workflows/ subdirectory against its project manifests. Conservative and read-only: verifies every node's (project, operation) exists, rejects cycles, unknown dependencies, and duplicate (project, operation) pairs, and marks which nodes are ready given the completed set. No remote access, transfer, allocation, or submission is ever performed.",
    { workflow: { type: "string", required: true }, completed: { type: "array", items: { type: "string" } } },
    async (args, exec) => {
      requirePolicy();
      const state = requireState(exec);
      const workflow = await loadWorkflowFile(workflowsDir, String(args.workflow));
      const completed = Array.isArray(args.completed) ? args.completed.map((value) => String(value)) : [];
      const plan = await planWorkflow(workflow, { resolveProject: (project) => loadProject(projectsDir, project).then((loaded) => loaded.manifest), completed });
      return { ok: true, status: { ...publicState(state), workflow_plan: JSON.parse(JSON.stringify(plan)) } };
    },
  );

  return { planTool, workflowsDir };
}

export { DEFAULT_PROJECTS_DIR as WORKFLOW_DEFAULT_PROJECTS_DIR };
