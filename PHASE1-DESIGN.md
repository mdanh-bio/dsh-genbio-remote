# Phase 1 authoritative design: typed staged-script recipes

This document supersedes the earlier Phase 1 draft. Implementation is confined to the isolated `plugin-fix-work/dsh-genbio-remote` copy. Deployment, policy-file modification, remote access, transfer, envelopes, and Slurm submission are separately gated and are not part of local implementation.

## Safety objective

Remove the handwritten-SBATCH burden without creating a generic remote-command surface. All scientific execution remains Slurm-mediated, policy-bound, envelope-checked, checksum-bound, and exact-once. Nothing in the new recipe format may execute a workload on an HPC login node.

## Phase 1 scope

1. Backward-compatible manifest `schema_version: 2`.
2. A v2 recipe names exactly one staged relative script already present in `manifest.files`.
3. The script receives a closed typed argv list:
   - enum from an explicit allowlist;
   - bounded safe integer;
   - strict boolean;
   - safe relative path.
4. Strict typing: no string-to-number or string-to-boolean coercion.
5. Each argv entry is a fixed literal or exactly one parameter reference. No mixed interpolation.
6. The plugin generates a policy-exact SBATCH wrapper and validates it with the existing shared `validatePinnedSbatch` gate.
7. Pure canonical plan construction and deterministic SHA-256 plan hashing.
8. Transfer-only pinned staging no longer requires a compute envelope. Policy validity, explicit transfer approval, remote-folder grants, rclone-only transfer, checksums, and bounded clean-environment syntax checks remain mandatory.
9. SBATCH parsing reads only the actual header before executable content, so later `#SBATCH`-looking data in a Python fixture is not misclassified as a scheduler directive.

## Explicitly prohibited in v2

- Generic `remote_command`, `command` strings, or owner-authored shell blocks.
- `run`, `test`, `check`, `block`, arbitrary `env`, or workflow/DAG sections.
- `eval`, `bash -c`, `sh -c`, pipelines, redirection, command substitution, or nested scheduler commands generated from manifest data.
- Manifest-controlled interpreter paths (`python_bin` is rejected in schema v2; it remains parse-only compatibility metadata in schema v1).
- User-supplied SBATCH directives or parameter-controlled CPU/GPU/concurrency.
- Persistent approvals or an audit ledger in Phase 1.

## Schema v2 job shape

```yaml
schema_version: 2
project: example
local_root: /absolute/local/project
remote_root: /absolute/remote/run
files:
  - scripts/run_analysis.sh
  - inputs/base.dat
jobs:
  analyze:
    cpus: 8
    gpus: 1
    concurrency: 1
    recipe:
      name: example-analyze
      script: scripts/run_analysis.sh
      env: gromacs              # optional policy-owned profile key
      parameters:
        arm: {type: enum, values: [ext100, ext1000]}
        count: {type: integer, min: 1, max: 20}
        resume: {type: boolean, default: false}
        input: {type: path}
      argv:
        - --arm
        - {param: arm}
        - --count
        - {param: count}
        - --resume
        - {param: resume}
        - --input
        - {param: input}
```

A schema-v2 manifest may mix legacy `template:` jobs and declarative `recipe:` jobs. Each job defines exactly one form. Schema v1 retains its existing behavior and rejects v2-only fields.

## Wrapper generation

The pure resolver produces exact deterministic bytes:

```text
#!/bin/bash
#SBATCH --job-name=<safe recipe name>
#SBATCH --partition=gpus
#SBATCH --nodelist=gpu03
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=<manifest cpus>
#SBATCH --output=%x_%j.out
#SBATCH --error=%x_%j.err
[SBATCH --gres=gpu:N]
set -euo pipefail
cd "$SLURM_SUBMIT_DIR"
[fixed policy-owned environment source]
'<staged/script>' '<arg1>' '<arg2>' ...
```

The invocation contains exactly one staged script. Every word is serialized with a POSIX single-quote encoder; embedded single quotes use the standard `'"'"'` sequence. NUL and newline values are rejected. Spaces and metacharacters in enum literals are inert data, not syntax. Safe-path parameters additionally enforce relative path segments and reject absolute paths, `.` and `..`.

An optional environment identifier resolves only through `policy.targets.HPC.environment.recipe_envs`. Its source path must be absolute, contain no `..`, use a shell-inert charset, and is quoted before emission. The environment setup executes inside the Slurm allocation, not as a login-node workload.

## Pure module boundary

`lib/project.js` contains no filesystem, session, shell, remote, question, job-registry, or scheduler capability. It provides:

- `parseProjectManifest`
- `resolveRecipe`
- `posixQuote`
- `canonicalJson`
- `planHashOf`
- `buildOperationPlan`

Planning is deterministic and side-effect-free. Canonical JSON recursively sorts object keys, preserves array order, and accepts only plain JSON values and safe integers. Plan material includes the current policy hash, manifest hash, exact compiled bytes hash, operation, resources, staged script, and resolved parameters.

## Later Phase 1 work

The project tools will add session-memory plan records capped at 32. Planning/inspection remain local and side-effect-free. Execute accepts only a known approved plan hash, freshly re-reads/re-parses the manifest and policy, re-resolves the operation, verifies all hashes, then reuses the single pinned admission and exact-once submission core. Content-addressed generated wrappers will be staged under the exact `genbio-recipes/` namespace through rclone and a remote SHA-256 gate.

Phase 1 execution handles one operation per call and returns the Slurm job ID immediately. Workflow/DAG submission, durable run registry, persistent audit records, automatic reconciliation across restarts, aggregate workflow progression, AI.zymes unification, and GUI controls are Phase 2/3 work.

## Acceptance gates

- Existing schema-v1 tests pass without weakened assertions.
- Generated bytes pass the same `validatePinnedSbatch` validator as v1 templates.
- Invalid schema/parameters/environment fail before any remote, question, background job, shell, transfer, or allocation effect.
- POSIX quoting is verified by an actual local Bash round-trip corpus.
- Same semantic inputs produce byte-identical wrappers and identical plan hashes; any behavior-relevant change changes the hash.
- No v2 code path introduces login-node workload execution.
- No remote/network action or installed-plugin modification occurs during implementation and local review.
