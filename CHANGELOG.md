# Changelog

## 0.3.0 — Unreleased

### Added

- Crash-consistent execution registry with fail-closed cross-process admission locks, durable attempt IDs, run-scoped remote directories, and write-once Slurm job IDs.
- No-follow secure package inventory and private snapshots; `genbio-plan/2` binds the canonical package digest.
- Strict scheduler evidence parsing that binds parent job ID, unique job name, user-filtered accounting, and job-owned provenance markers.
- Durable `run_id` project status/fetch surfaces and workflow-node run-ID handoff.
- Adversarial tests for atomic storage, symlink rejection, package drift, remote path injection, fresh run directories, job-name uniqueness, and scheduler-marker forgery.

### Changed

- Each durable attempt uses a fresh `<remote_root>/runs/<attempt-id>` directory.
- Node headroom probes fail closed on missing structured markers or incomplete GPU evidence.
- Ambiguous and cancellation-requested allocations remain reserved until terminal evidence.
- Rclone remotes are restricted to simple aliases and may be target-policy bound.
- Workflow persistence now uses file and directory fsync through the shared atomic store.
- Project fetch requires the owning `run_id` and writes beneath a run-specific destination.
- Active machine-specific `cordis.patch.yml` is excluded from npm package contents.

### Security

- Absolute manifest paths are restricted to shell-inert syntax.
- Staging reads only a verified private snapshot, preventing workspace symlink/TOCTOU disclosure.
- Reconciliation is filtered to the current remote user and exact unique job name.
- Fresh-directory command construction shares the strict remote quoting boundary.
- Remote artifact discovery rejects symbolic links.

### Known limitations before deployment

- Durable ownership remains bound to the original DSH session ID; cross-session takeover is unsupported.
- Stale execution-registry locks require explicit manual recovery and are never automatically broken.
- External policy/config migration, installation, DSH restart validation, and remote canaries remain separate approval gates.

## 0.2.0 — 2026-09-02

- Mandatory schema-v2 workspace projects and workflows.
- Declarative typed recipes, single-process exact-once submission, controlled workflows, and bounded project status/fetch surfaces.
- Removed schema-v1 template projects and project-specific adapters.
