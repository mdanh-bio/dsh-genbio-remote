# Controlled workflows

Workspace workflows live under `genbio-workflows/` and use `.yml` or `.yaml`. Configured workflows live under `<projectsDir>/workflows/`. Every workflow must declare `schema_version: 2`; legacy schema-v1 workflows are rejected.

```yaml
schema_version: 2
workflow: production
nodes:
  - id: prepare
    project: example
    operation: prepare
    parameters: {}
  - id: simulate
    project: example
    operation: simulate
    parameters:
      input: inputs/system.dat
      steps: 100000
    depends_on: [prepare]
  - id: analyze
    project: example
    operation: analyze
    parameters: {}
    depends_on: [simulate]
```

Each node resolves to an immutable schema-v2 operation plan whose hash also binds the package bytes. Submitted nodes persist the durable project `run_id`, allowing workflow status to reconcile the project attempt before or after a Slurm job ID becomes available.

## Deliberately controlled progression

1. `genbio_workflow_plan` validates and records the immutable workflow plan.
2. `genbio_workflow_execute` fresh-resolves it and submits at most one ready node.
3. `genbio_workflow_status` reconciles scheduler and job-owned output evidence; it never submits.
4. `genbio_workflow_advance` submits at most one newly ready node.
5. Pause/resume change local orchestration state; resume does not submit.
6. Cancellation targets one exact session-owned node job ID and requires confirmation.

A dependency is complete only after Slurm reports `COMPLETED`, exit `0:0`, and job-owned output/provenance evidence is present. Ambiguous submission is `unknown/reconciling` and is never automatically retried.

Schema-v1 workflows are rejected. Only schema-v2 workflows can be planned or executed.
