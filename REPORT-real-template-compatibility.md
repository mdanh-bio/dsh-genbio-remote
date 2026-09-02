# Real-template compatibility report (t10)

Generated 2026-08-27 by running every real production template through the
shared Phase-0 validator `validatePinnedSbatch` (lib/slurm-policy.js) against
the live policy
(`/Users/mdanh/.codex/skills/operate-genbio-hpc-remote/references/genbio-compute-policy.yaml`)
with a permissive envelope (`gpu03/gpus`, maxCpus 64, maxGpus 2,
concurrency 2). Validation was NOT weakened to make templates pass; failures
below are reported as required.

## Result: 4 pass / 15 fail (19 real templates probed)

### liposome-w5 (manifest `/Users/mdanh/.dsh/profiles/desktop/genbio-pinned-projects/liposome-w5.yaml`) — 0/14 pass

All 14 job templates (`analysis/b80_ext_resv_adsorption_20260823/sbatch/*.sbatch`):
`w5-inventory`, `w5-pinlocate`, `w5-buildprep-ext100`, `w5-buildprep-ext1000`,
`w5-buildmd-ext100`, `w5-buildmd-ext1000`, `w5-nvtprep-ext100`,
`w5-nvtprep-ext1000`, `w5-nvtmd-ext100`, `w5-nvtmd-ext1000`, `w5-nptprep-ext100`,
`w5-nptprep-ext1000`, `w5-nptmd-ext100`, `w5-nptmd-ext1000`.

First failure (identical for all 14): `SBATCH policy: body must contain exactly set -euo pipefail`

Root causes (uniform across the 14 templates):
1. Body opens with `set -Eeuo pipefail` (the `E` flag for ERR trap propagation)
   instead of the policy-required exact form `set -euo pipefail`.
2. Body uses `cd "$run_dir"` with a hardcoded absolute login-node path
   (`run_dir=/data01/genbiolab/mdanh/data/simulation/liposomes/runs/20260823_b80-ext-resv-adsorption-prep`)
   instead of the policy-required exact form `cd "$SLURM_SUBMIT_DIR"`.
   (A secondary `set +u`/`set -u` toggle also appears in several templates
   around a `source` of a GROMACS rc file; the `E`/`cd` items above are the
   first two violations the validator reports.)

Everything else PASSES for all 14: shebang, column-zero directives,
allowlisted directives only, `--job-name`/`--nodelist=gpu03`/`--nodes=1`/
single-task directives, `--cpus-per-task` matching the manifest job spec
(parity), `--gres=gpu:1` matching `gpus: 1` on the 8 GPU jobs, and
`%j`-carrying distinct `--output`/`--error` paths.

Remediation (template-owner decision, out of t10 scope): drop the `E` flag and
switch to `cd "$SLURM_SUBMIT_DIR"`, staging each template so it is submitted
from its run directory.

### AI.zymes (bundle `…/DAE_enzyme/workflow/aizyme_v1/remote/`) — 4/5 pass

- `stage0_1.sbatch` — PASS
- `stage2_environment.sbatch` — PASS
- `stage3_chemistry.sbatch` — PASS
- `stage4_design_space.sbatch` — PASS
- `stage5_patch_tests.sbatch` — FAIL:
  `SBATCH policy: #SBATCH directive appears after executable content (line 126)`

  This is a fail-closed false positive of the line-based header parser: line
  126 is a Python string literal (a `SUBMIT_HEAD` template the stage writes
  out) containing `"#!/bin/bash\n#SBATCH --partition=gpus\n…"`. The parser
  sees `#SBATCH`-prefixed text after the body has started and refuses. The
  template's actual header (lines 1–12) is fully compliant.

Remediation (template-owner decision, out of t10 scope): restructure that
string so no body line begins with `#SBATCH` (e.g. build it as
`"#" + "SBATCH --partition=gpus\n…"`), which the line-based parser cannot
misread.

## Notes

- The AI.zymes path now runs these same validations at admission (before
  transfer approval / remote access / job creation), with the validated
  in-memory bytes digest-bound (SHA-256) through staging, the remote
  validation gate, and immediately before sbatch.
- The liposome-w5 path (pinned projects) already enforces the same validator
  at admission and re-validates the digest-bound bytes before submission.
- `python_bin` in the liposome manifest is parsed for schema compatibility
  only; it has zero execution effect (t7 / t5#1).
