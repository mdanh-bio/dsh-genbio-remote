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

- Project discovery now prefers the active workspace and retains the configured project directory as a compatibility fallback.
- Repository validation no longer requires a policy file under a user-specific home directory.

### Security

- Workspace manifests cannot nominate another local tree or escape through symlinks.
- Workflow execution reuses the existing checksum-bound, exact-once operation submission core.
- Status reconciliation never authorizes automatic resubmission of an ambiguous Slurm dispatch.
