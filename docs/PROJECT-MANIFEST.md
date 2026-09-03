# Workspace project manifest

Put exactly one of these files in the active DSH workspace root:

- `genbio-project.yml`
- `genbio-project.yaml`

The plugin discovers it lazily on each project tool call. No plugin edit, bundle rebuild, or restart is needed.

```yaml
schema_version: 2
project: example
local_root: /absolute/path/to/the/current/workspace
remote_root: /approved/remote/project/root
files:
  - scripts/run.sh
  - inputs/system.dat
extra_dirs:
  - results
jobs:
  simulate:
    node: gpu04
    cpus: 8
    gpus: 1
    concurrency: 1
    recipe:
      name: example-sim
      script: scripts/run.sh
      parameters:
        input: {type: path}
        steps: {type: integer, min: 1, max: 1000000}
      argv:
        - --input
        - {param: input}
        - --steps
        - {param: steps}
fetch:
  max_bytes: 10485760
  dest: fetched
  files:
    - results/summary.json
```

## Workspace boundary

For a workspace manifest, `local_root` must resolve exactly to the active session workspace. The manifest and every declared file must be regular, non-symlink files inside that workspace. Both manifest extensions at once are rejected as ambiguous.

A central `<projectsDir>/<project>.yaml` is also supported. If the active workspace manifest declares the same project, it takes precedence for that workspace and results report `origin: workspace`. All workspace and configured manifests must use schema version 2 and declarative recipes; raw template jobs are rejected.

## Planning and execution

```text
genbio_projects
genbio_project_describe
genbio_project_inventory
genbio_project_plan
genbio_project_execute
genbio_project_status
genbio_project_cancel
genbio_project_fetch
```

Planning securely reads every declared package file and includes the canonical `packageSha` in `genbio-plan/2`. Execution rebuilds that inventory, rejects byte drift, snapshots approved files through no-follow handles, persists a durable attempt, creates `<remote_root>/runs/<attempt-id>` fresh, and stages only the private snapshot. `genbio_project_execute` returns a durable `run_id`; use it for restart-safe status and allowlisted fetch. Fetch requires `project`, `run_id`, and optional `files`.

`genbio_project_status` reconciles scheduler evidence by default when a `run_id` or exact operation/job ID is supplied. Pass `reconcile: false` with a `run_id` for a local-only durable snapshot; the result reports `scheduler_evidence: skipped` and `reconciliation_pending: true` when fresh scheduler evidence is still required. A resource envelope remains session-scoped and must be set again after restarting DSH Desktop.
