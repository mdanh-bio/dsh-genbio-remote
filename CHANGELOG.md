# Changelog

## 0.2.0 — Unreleased

### Added

- Self-contained repository policy fixture and GitHub Actions validation.
- Workspace-local `genbio-project.yml` / `genbio-project.yaml` discovery.
- Strict workspace realpath, symlink, regular-file, and source-drift guards.
- Workspace-local `genbio-workflows/*.yml` discovery.
- Immutable schema-v2 workflow plans and durable workflow-run registry.
- Controlled one-node-at-a-time workflow execution, status, advance, pause, resume, and owned cancellation tools.
- Bounded workflow status projection and read-only GUI views.

### Changed

- Project discovery now prefers the active workspace and falls back to the configured `projectsDir`.
- Project and workflow manifests now require schema version 2; raw template jobs and schema-v1 compatibility paths were removed.
- Removed the legacy project-specific scientific adapter, its fixed paths/stages, and the unused adapter-only run registry.
- Replaced public legacy template-project tools with project-neutral schema-v2 status, cancellation, and bounded fetch surfaces.
- Repository validation no longer requires a policy file under a user-specific home directory.

### Security

- Workspace manifests cannot nominate another local tree or escape through symlinks.
- Workflow execution reuses the existing checksum-bound, exact-once operation submission core.
- Status reconciliation never authorizes automatic resubmission of an ambiguous Slurm dispatch.
