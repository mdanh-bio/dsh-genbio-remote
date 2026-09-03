# Changelog

## 0.4.0 — 2026-09-03

### Added

- Constrained `genbioh100` direct-project stage, launch, status, and fetch tools with CPU-only admission, detached clean-environment execution, token-bound lifecycle evidence, and durable run-registry locking.
- Fixed-surface AI.zymes H100 Stage 2 verification tools with GPU 0 protection and bounded terminal evidence.
- Curated H100 mirror planning, drift checks, SHA-256 verification, receipts, and atomic destination promotion.
- Durable project-run rehydration for `genbio_runs` and project tools after runtime reload.
- Optional `reconcile: false` local-only mode for `genbio_project_status`.
- Shared strict shell-token quoting helper and schema-v2 compatibility probe.

### Changed

- Policy polling defaults to 15 seconds instead of 2 seconds.
- Default local registry paths are derived from `$HOME` rather than a fixed account path.
- Fresh remote run-directory construction is target-aware.
- Session plan-cache eviction is logged, and status/envelope/finalization diagnostics are more explicit.
- Package validation now includes the H100 modules, shared shell helper, and repaired compatibility probe.

### Security

- Durable execution-registry policy identity is immutable after reservation.
- Rehydrated records expose policy-hash match state without blocking read-only status or reconciliation after a legitimate policy update.
- Local-only status mode performs no remote folder grant, SSH reconciliation, or registry reconciliation update.

### Operational notes

- Durable ownership remains bound to the original DSH session ID; cross-session takeover is unsupported.
- Resource envelopes remain session-scoped and must be set again after a DSH Desktop restart.
- Stale execution-registry locks require explicit manual recovery and are never automatically broken.

## 0.3.0 — 2026-09-02

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
- Workflow persistence uses file and directory fsync through the shared atomic store.
- Project fetch requires the owning `run_id` and writes beneath a run-specific destination.
- Active machine-specific `cordis.patch.yml` is excluded from npm package contents.

### Security

- Absolute manifest paths are restricted to shell-inert syntax.
- Staging reads only a verified private snapshot, preventing workspace symlink/TOCTOU disclosure.
- Reconciliation is filtered to the current remote user and exact unique job name.
- Fresh-directory command construction shares the strict remote quoting boundary.
- Remote artifact discovery rejects symbolic links.

## 0.2.0 — 2026-09-02

- Mandatory schema-v2 workspace projects and workflows.
- Declarative typed recipes, single-process exact-once submission, controlled workflows, and bounded project status/fetch surfaces.
- Removed schema-v1 template projects and project-specific adapters from the generic Slurm project surface.
