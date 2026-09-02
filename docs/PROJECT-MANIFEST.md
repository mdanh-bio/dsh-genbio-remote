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

A central `<pinnedProjectsDir>/<project>.yaml` remains supported. If the active workspace manifest declares the same project, it takes precedence for that workspace and results report `origin: workspace`.

## Planning and execution

```text
genbio_projects
genbio_project_describe
genbio_project_inventory
genbio_project_plan
genbio_project_execute
genbio_project_status
```

Execution fresh-reads the same source and rejects workspace changes, source changes, manifest drift, policy drift, wrapper drift, or plan-hash drift before staging or submission.
