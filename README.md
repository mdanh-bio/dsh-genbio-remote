# dsh-genbio-remote

Private DeepSeek Harness plugin for policy-controlled remote scientific computing through Slurm.

## Capabilities

- Mandatory schema-v2 project and workflow manifests with typed recipe operations
- Policy and session resource-envelope validation
- SHA-256-bound staging with rclone over SFTP
- Exact-once Slurm submission with ambiguity reconciliation
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

1. Describe and inventory a declared project.
2. Resolve an immutable execution plan.
3. Validate target policy, cluster readiness, and the resource envelope.
4. Stage and checksum-verify the package.
5. Submit once through `sbatch`.
6. Track the Slurm workload separately from the submission helper.
7. Reconcile scheduler accounting with job-owned output evidence.
8. Fetch allowlisted artifacts and finalize the run record.

## Local configuration

`cordis.patch.yml` contains installation-specific paths and rclone remote names. Review it before installing the plugin on another workstation. Cluster policy and per-project manifests remain external configuration and are not embedded as credentials in this repository.

## Validation

```bash
npm run check
```

The check performs JavaScript syntax validation, the complete Node test suite, and the runtime smoke test.

## Safety boundary

This repository contains plugin code only. Installing it does not itself authorize a cluster operation. Remote work remains constrained by the active target policy, folder grants, resource envelope, immutable plan approval, and session-owned run tracking.
