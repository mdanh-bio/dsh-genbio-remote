# dsh-genbio-remote

Private DeepSeek Harness plugin for policy-controlled remote scientific computing through Slurm.

## Capabilities

- Mandatory schema-v2 project and workflow manifests with typed recipe operations
- Policy and session resource-envelope validation
- Package-content-bound plans and no-follow private staging snapshots
- Fresh run-scoped remote directories with SHA-256-bound rclone/SFTP staging
- Durable exact-once Slurm attempts with ambiguity reconciliation across restart
- Independent helper and workload lifecycle tracking
- Dual-source `squeue` and `sacct` evidence
- Bounded Slurm discovery and pending-job diagnosis
- Session-ownership-checked cancellation
- Job-owned provenance and output verification
- Allowlisted result fetching and durable run finalization

The plugin deliberately does not expose a generic remote-command interface. Transfers use rclone rather than SCP, and uncertain submissions are reconciled instead of automatically resubmitted.

## Project model

New scientific projects use a workspace-root `genbio-project.yml` (or `.yaml`) plus project-owned scripts, or a schema-v2 manifest in the configured `projectsDir`. The plugin discovers manifests lazily. Schema version 1, raw SBATCH template projects, and project-specific built-in adapters are intentionally unsupported; plugin source changes are reserved for reusable execution, policy, scheduler, or security capabilities.

See `docs/PROJECT-MANIFEST.md`, `docs/WORKFLOWS.md`, and `docs/INSTALL.md`.

The preferred lifecycle is:

1. Describe and securely inventory a declared project.
2. Resolve a plan whose hash binds policy, manifest, wrapper, parameters, and package bytes.
3. Validate target policy, cluster readiness, and the resource envelope.
4. Persist a durable attempt and fresh remote run directory before dispatch.
5. Stage an immutable private snapshot and submit at most one `sbatch` for the attempt.
6. Reconcile identity-bound scheduler accounting with job-owned output evidence.
7. Fetch allowlisted artifacts by durable `run_id` and finalize the run record.

## Local configuration

Use `cordis.patch.example.yml` for installation-specific paths, workflow/execution registry roots, and simple rclone aliases. The active `cordis.patch.yml` is intentionally excluded from package contents. Cluster policy, manifests, credentials, and rclone configuration remain external.

## Validation

```bash
npm run check
```

The check performs JavaScript syntax validation, the complete Node test suite, and the runtime smoke test.

## Safety boundary

This repository contains plugin code only. Installing it does not itself authorize a cluster operation. Remote work remains constrained by the active target policy, folder grants, resource envelope, immutable plan approval, and session-owned run tracking.
