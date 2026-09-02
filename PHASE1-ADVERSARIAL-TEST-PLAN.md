# Phase 1 Adversarial Test Plan (dsh-genbio-remote) — REVISED to final architecture

Author: test-reviewer-2 · Team: genbio-safe-declarative-execution · 2026-08-27
Task: t14 — Design Phase 1 adversarial tests (READ-ONLY plan; no tests implemented yet)
Scope: isolated copy `plugin-fix-work/dsh-genbio-remote` only; no remote/network/deployment.

> **Architecture notice (captain ruling 2026-08-27):** the earlier t13 proposal
> of arbitrary recipe shell (`run`/`test`/`env`/`block` inline command grammar)
> is **REJECTED**. The final v2-job contract is:
>
> - **v2 job = a staged script path from `manifest.files`** (a real, checksum-pinned
>   file) **plus typed argv placeholders only** — `enum`, bounded integer,
>   `boolean`, safe relative `path`.
> - **Optional fixed policy-owned env profile** (the env `source` lines come only
>   from `policy.targets.HPC.environment.recipe_envs`, never from the manifest).
> - **The generated SBATCH calls that script through a safe single-quote
>   serializer** — no `eval`, no `bash -c`, no `sh -c`, no inline workload body.
>
> This plan is rewritten against that contract. Every requirement is an
> executable invariant; names still under implementation are marked `TBD:` and
> pinned by the t16/t17 binding pass (Phase 0 names are final and reused).

> **t14-acceptance corrections (captain, applied 2026-08-27) — all encoded below:**
> (1) typed params are strictly `enum`/`integer`/`boolean`/`path` — no `float`,
> no generic `string`, **no coercion** (§1.8/§1.10, §2); (2) pair lock remains
> `(project, operation)` and plan hash is metadata only (§6.10) — same-operation
> different-params is BLOCKED while submitting/nonterminal unless operation
> identity is deliberately parameterized (§6.2/§6.11); (3) generated wrapper is
> staged content-addressed from the **exact in-memory bytes** (§3.12);
> (4) planning is a tool backed by pure functions with zero
> remote/question/job/shell writes (§5/§9a.8); (5) the shared
> `validatePinnedSbatch` is mandatory for both forms (§3.2/§7.5); (6) the live
> gpu03 probe is a **standalone call, not embedded** (§3.14/§7.6); (7) **no
> hardcoded `remote_root` in the wrapper body** — the script path is relative to
> `$SLURM_SUBMIT_DIR` (§3 emission/3.8/3.13).

---

## 0. Working invariants inherited from Phase 0 (must not regress in Phase 1)

V1 already guarantees (tests `pinned-safety`, `pinned-blockers`, `pinned-stage`,
`pinned-admission-lock`, `slurm-policy`, `gpu03-probe`, `aizyme-envelope`):
- **Zero side effects on any invalid path**: for every invalid
  manifest/template/param/policy/envelope/live-state input, counters
  `questions`, `requireRemoteAccess`, `jobs.start`, `shell.run`, `runRemote`,
  `sbatch` stay exactly 0, and the failure is **synchronous** before the first
  remote-access/knowledge question.
- **Exact-once submission**: one unique job name + CSPRNG token per intent;
  ambiguous transport/malformed-ID states reconcile via bounded read-only
  `sacct`; never a second `sbatch` for the same pair; concurrent same-pair
  admission admits at most one (atomic admission block).
- **Login-node confinement**: no manifest-controlled interpreter runs remotely;
  remote validation = checksum + exact clean-env `/bin/bash -n`; no workload
  execution in any `ssh <control>` path.
- **Byte binding**: validated template bytes digest-bound to pre-submit bytes
  (TOCTOU closure); validated-log-path allowlist (`%j`, `SAFE_LOG_PATH_RE`).
- **Resource truth**: `state.allocations` reserve full manifest resources until
  terminal `sacct` evidence; aggregate admission before any side effect.

Phase 1 tests assert these still hold on every new v2 path (§1–§9a), because
each new tool/resolver/wrapper is a new attack surface.

---

## 1. Schema v2 — the v2-job contract (script + typed argv + env profile)

`TBD:parse-manifest-v2` in `lib/pinned.js` (v1 path byte-identical; v2 =
v1 + v2-form jobs). Final v2 job shape:

```yaml
jobs:
  w5-nvtprep-ext100:                    # v2 form
    script: analysis/b80.../nvtprep.sh  # MUST be in manifest.files (staged)
    params:                             # OPTIONAL typed argv placeholders
      system: { type: enum,    values: [ext100, ext1000] }
      nsteps: { type: int,     min: 1, max: 1000000 }
      verbose:{ type: bool }
      outdir: { type: path }            # safe RELATIVE path (see §2)
    env_profile: gmxrc-cpu01            # OPTIONAL; name must exist in policy recipe_envs
    cpus: 2                             # resources stay at job level (v1 parity)
    gpus: 0
```

| # | Case | Adversarial expectation |
|---|------|------------------------|
| 1.1 | Unknown top-level key / unknown nested key | Reject (strict), error names the key; zero side effects |
| 1.2 | Job entry has BOTH `script:` and a v1 `template:` (or neither) | Parse error (exactly one of template | script+params), never ambiguous |
| 1.3 | Job entry has a `recipe:`/inline-block/`steps:`/`run:`/`block:` key (rejected grammar) | **Hard reject** — the arbitrary-shell recipe grammar is forbidden; error says so |
| 1.4 | `script:` path not present in `manifest.files` | Reject (staged-script membership is mandatory) — mirrors v1 template∈files |
| 1.5 | `script:` not ending in `.sh`/`.py`/executable extension | No extension gate required, but must be a regular file in `files` and pass the staged-file rules; reject directories/FIFOs/sockets/symlink-escapes (§9a.3) |
| 1.6 | Duplicate keys (top-level, job-level, inside `params:`), `params:` with duplicate placeholder names | Reject (pin js-yaml behavior; §9a.1) |
| 1.7 | `params:` placeholder name colliding with reserved tokens (`run_dir`, `SLURM_*`, script name, `cpus`/`gpus`/`concurrency`) | Reject (can't shadow control names) |
| 1.8 | `params:` entry with no `type` or unknown type (string/float/number/list/map) | **Reject — the typed-param system is CLOSED to `enum`, `integer`, `boolean`, `path` only. No `float`, no generic `string`, no coercion** (a `float` or `string` type declaration is a compile error) |
| 1.9 | `params:` `enum` with `values` empty / non-list / containing duplicates | Reject |
| 1.10 | `params:` entry with a coercion hint (`coerce`, `as`, `cast`, `default_float`) | Reject — coercion is prohibited; values are validated strictly, never transformed |
| 1.11 | `params:` `integer` with bad bounds (`min>max`, non-integer, missing) | Reject; bounds mandatory for `integer` |
| 1.12 | `params:` `boolean` with extra subkeys (`values`,`min`) | Reject (schema strictness) |
| 1.13 | `params:` `path` with extra subkeys (no `values`/`min`) | Reject; `path` is relative-only (no `relative: false`) |
| 1.14 | `env_profile:` set but the policy has no `recipe_envs` section / no such entry | Reject at compile time (policy-owned allowlist; §9a.5) |
| 1.15 | `env_profile:` value not a bare policy key (contains `/`, `..`, shell chars) | Reject — it is an allowlist key, never a path/string |
| 1.16 | `cpus`/`gpus`/`concurrency` caps (0, >128, >8, non-integer, float, string) and **params attempting to set cpus/gpus** | Reject; resources are ONLY the job-level fields (params can't escalate) |
| 1.17 | v1 manifest containing any v2 key (`script:` job / `workflows:`) | Hard error (v2 key in v1 file is never silently ignored) |
| 1.18 | v1 jobs and v2 jobs mixed in one v2 manifest | Allowed (additive); each battery valid independently; byte-hash covers both |
| 1.19 | `files` duplicates / `extra_dirs` overlap / `files`=parent of another file | Reject (path-hierarchy ambiguity; §9a.3) |
| 1.20 | `local_root`/`remote_root` traversal (`..`, non-absolute) | Reject (v1 rule re-asserted) |
| 1.21 | `script:` file is a symlink / directory / non-regular at parse and at staging-read time | Reject with `stat().isFile()` + TOCTOU (§9a.3 C* rows) |

Test file: `tests/manifest-schema-v2.test.mjs`.

---

## 2. Typed argv placeholders — the ONLY value-carrying inputs

`TBD:resolve-params` / `TBD:validate-param-value` (pure). The typed-param
**system is CLOSED to `enum` / `integer` / `boolean` / `path` only** — no
`float`, no generic `string`, and **no coercion of any kind** (a `float`/
`string` type declaration or any coercion hint is a compile error):

| Type | Allowed value | Adversarial rejection rows |
|---|---|---|
| `enum` | one of `values[]` (exact match) | wrong value; value with case-mismatch (pin case-sensitivity); value in `values` but containing `'`/NUL/newline/control (rejected globally, §3); `values[i]` with `=` or leading `-` (documented; see §2.1) |
| `integer` | `[min,max]` inclusive, safe integer | out-of-bounds; `NaN`, `Infinity`, float (`4.0`), numeric string `"4"` (reject — no coercion), hex/octal/exponent (`0xFF`, `1e3`), `2^53+1`, leading-zero strings |
| `boolean` | `true` or `false` (exact literals only, no presence-only magic) | `"true"` string (reject — no coercion), `1`, `yes`, `on`; all rejected |
| `path` | safe RELATIVE path: segments `[A-Za-z0-9_.-]+`, no leading `/`, no `..`, no `\`, no control chars, no `'`, length-bounded, resolves inside `$SLURM_SUBMIT_DIR` | absolute path; `..`; `../..`; `a/../../b`; trailing `/`; `//`; NUL/newline/CR/tab; `'`; `$`; leading `-` (see §2.1); very long; a path that would escape `$SLURM_SUBMIT_DIR` worst-case resolution |

| # | Case | Adversarial expectation |
|---|------|------------------------|
| 2.1 | Value starting with `-` / `--` (option-injection into the **script's** argv) | The wrapper passes it VERBATIM as a positional argv token — never as a wrapper option (the wrapper has no option surface). Assert the emitted line preserves the exact token; document that option-parsing is the **trusted script's** contract, not a wrapper concern. Recommend: `enum`/`boolean`/`integer` reject a `-`-leading value at type-validation; `path` passes verbatim-positional. Pin one rule per type |
| 2.2 | Params with `'`, `\`, `"`, space, `$`, backtick, `;`, `|`, `&`, `(`, `)`, `{`, `}`, `*`, `?`, `#` | Every value either fails the type allowlist (§7 charset) or round-trips through the serializer to the EXACT single argv token (§3) — no interpolation, no expansion, no injection |
| 2.3 | Param count / total argv bytes (argv length limit, wrapper size) | Bounded: params ≤ N, serialized line ≤ cap (16 KiB total compiled); over → compile error |
| 2.4 | Missing a param that the manifest declares required (no default) | Compile-time "parameter required" error; a declared default is applied deterministically |
| 2.5 | Param present that the manifest does not declare | Reject (unknown placeholder), not silently dropped |
| 2.6 | Duplicate param keys / YAML key-order in `params:` | Pinned deterministic behavior (reject or canonical-last-wins) — same as §1.6 |
| 2.7 | `enum` value identical to another `enum` value by alias (`&`/`*` YAML aliasing) | Type-values deduped/resolved canonically; no shared-node mutation leak (§9a.1 A.4) |
| 2.8 | `path` value resolving (lexically) outside the run dir | Reject — safest rule: no `..` segment, no absolute, no `/`-start, no `\`; the script may further interpret it, but the planner guarantees containment shape |
| 2.9 | Same params → identical serialized argv and identical wrapper hash | Determinism (§4) |
| 2.10 | Param never referenced by the script (declared-unused) vs referenced-but-undeclared | Declared-unused → allowed (script is opaque); the wrapper still serializes it. No undeclared placeholder can exist (§2.5) |

Test file: `tests/typed-params.test.mjs`.

---

## 3. Generated SBATCH — safe single-quote serialization, no eval/bash -c

`TBD:compile` in `lib/recipe-resolver.js` (pure): `(scriptEntry, params, envProfile, jobSpec, policy, manifestMeta) -> { sbatchText, bytesSha, argv }`.

**Emission (fixed, ordered):**
```
#!/bin/bash
#SBATCH --job-name=<name>
#SBATCH --partition=gpus
#SBATCH --nodelist=gpu03
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=<jobSpec.cpus>
[#SBATCH --gres=gpu:<jobSpec.gpus>]
#SBATCH --output=%x_%j.out
#SBATCH --error=%x_%j.err

set -euo pipefail
cd "$SLURM_SUBMIT_DIR"
[set +u ; source <pol.recipe_envs[env_profile].source> ; set -u]    # ONLY from policy
exec '<script path relative to $SLURM_SUBMIT_DIR>' '<argv1>' '<argv2>' …   # safe single-quote serializer
```
- **No inline workload**: the body contains exactly the fixed header lines, the
  strict-shell + cd lines, the optional policy env source, and ONE `exec` line
  calling the staged script with serialized argv.
- **No hardcoded `remote_root`/`run_dir=` absolute path in the wrapper body**
  (captain ruling): the script is invoked relative to `$SLURM_SUBMIT_DIR` (the
  job directory the staged files already live under); `remote_root` never
  appears inside the emitted SBATCH text.
- **No `eval`, no `bash -c`, no `sh -c`, no here-doc feeding the workload, no
  command substitution** of manifest/params text.

| # | Case | Adversarial expectation |
|---|------|------------------------|
| 3.1 | Compiled bytes contain `eval`, `bash -c`, `sh -c`, `$(`, backtick, `source <manifest path>` (only the exact policy-env source may appear), or any `#SBATCH` after body | Compile-time rejection (assert by scanning compiled text; PLUS the authoritative `validatePinnedSbatch` gate) |
| 3.2 | Compiled text passes `validatePinnedSbatch` unmodified (single validator, both forms) | Always true on any accepted job; assert with the v1 validator (parity, no per-form branch) |
| 3.3 | Exactly ONE invocation of the script (single `exec` line, no double-run) | Assert the exec line count === 1 and argv token count === params+1 |
| 3.4 | Single-quote serializer round-trip: for every accepted value, shell-parsing the emitted `'<v>'` yields exactly `<v>` — spanning `'` (→ `'\''`), `\`, space, `$`, `"`, `#`, `;`, `|`, `&`, glob chars | Deterministic unit test against the POSIX single-quote rules; no value can break out of the quoted token |
| 3.5 | A value containing a literal `'` | Serialized as `'\''` (the only escape) or rejected; assert the parsed argv equals the original exactly and no quote-injection |
| 3.6 | A value containing NUL / newline / CR / control chars | Rejected at type validation (cannot exist in a single argv token); compile error |
| 3.7 | A value containing `$HOME`, `$var`, `$(...)`, backtick | Stays LITERAL (single quotes prevent expansion); assert the emitted text has the `$` inside single quotes and grep-finds no expansion construct outside quotes |
| 3.8 | The script path token is the staged path (relative, under `$SLURM_SUBMIT_DIR`), quoted, and NOT derivable from params | Params fill argv only — never the script path; assert the exec line's first token equals the manifest `script:` relative path and contains no `remote_root`/absolute prefix |
| 3.9 | Wrapper size / argv line length | Bounded (16 KiB total); over → reject |
| 3.10 | Wrapper digest binding | `bytesSha` = sha256 of the exact compiled string; pre-submit re-compile must equal it (TOCTOU: a param/manifest/policy change after compile → digest mismatch → zero submit) |
| 3.11 | Wrapper must not execute on the login node | No `ssh <control>` command ever runs the script directly; only via `sbatch` from the run dir (assert remote command shape) |
| 3.12 | Content-addressed staging of the compiled wrapper from the EXACT in-memory bytes | `<remoteRoot>/genbio-recipes/<bytesSha12>.<operation>.sbatch` via rclone from the plugin-written local temp copy of the compiled string; remote `sha256sum` must equal `bytesSha`; idempotent skip; `genbio-recipes/` is the sole `stageAndValidate` ownership relaxation |
| 3.13 | No hardcoded `remote_root`/`run_dir=` in the emitted body (captain ruling) | Assert compiled text contains no `remote_root` value / no `run_dir=` assignment; the exec line uses the relative script path only |
| 3.14 | Live gpu03 probe is a **standalone remote call**, NOT embedded in the wrapper or the sbatch command (captain ruling) | Assert the probe runs as a separate bounded read-only `runRemote` before the submit command and never appears inside the compiled SBATCH text; a probe failure aborts before sbatch with zero allocation |

Test file: `tests/wrapper-gen.test.mjs`.

---

## 4. Deterministic plan hashes

`TBD:plan-hash` / `canonicalJson` in `lib/plan-store.js` (sorted keys, no
whitespace, field allowlist).

Plan shape (unchanged from the design that survives the captain ruling):
`{ schema:"genbio-plan/1", project, planId, policyHash, manifestSha, on_failure:"stop",
steps:[{ seq, kind:"submit", operation, form:"script", bytesSha, jobName, cpus, gpus, needs:[] }] }`.

| # | Case | Adversarial expectation |
|---|------|------------------------|
| 4.1 | Same (manifest bytes, resolved params, policy hash, envelope) → same hash | Two planning calls equal |
| 4.2 | Any single behavior bit change — a char in the script bytes, one param value, `cpus`, `gpus`, `concurrency`, `job-name`, chosen `env_profile` | Different hash (mutation matrix) |
| 4.3 | Cosmetic-only edits (description, comments, key order, trailing ws/BOM) | Same hash (canonical serialization; §9a.2) |
| 4.4 | No `Date.now()`/random/timestamp in the hash input | Grep + determinism test |
| 4.5 | `bytesSha` covers the compiled wrapper (a script edit that changes emission → different hash) | Assert per-step bytesSha dependency |
| 4.6 | Plan hash bound to execution: re-derive before dispatch; stale cached hash rejected with zero effects | TOCTOU analog |
| 4.7 | Different argv sets that produce different wrappers → different hashes | No serialization-collision |
| 4.8 | `sha256(canonicalJson(plan))` stable across runs/workers | Determinism |
| 4.9 | Recorded hash === derived hash at submission | Greedy evidence check |
| 4.10 | Plan under policy hash A replayed after policy rotate B | Reject ("policy changed") before side effects |

Test file: `tests/plan-hash.test.mjs`.

---

## 5. Side-effect-free planning

The v2 compile path (`TBD:compile` + plan build) must be a pure function.

| # | Case | Adversarial expectation |
|---|------|------------------------|
| 5.1 | Compiling/planning a valid v2 job performs zero remote calls, questions, `jobs.start`, `shell.run`, fs writes, envelope/state mutation | Counter tuple all zero |
| 5.2 | Same for an invalid job (any §1/§2/§3 rejection) | Zero tuple; failure before any hook |
| 5.3 | Repeated compile (3×) identical plan/hash, state untouched | Determinism + purity |
| 5.4 | No `userQuestions.ask` during planning | Approval belongs to execution (`genbio_project_plan`), not planning |
| 5.5 | No partial wrapper file written during planning (fs spy) | Zero fs writes |
| 5.6 | No remote read (`squeue`/`sacct`/`sinfo`) during planning | Live probe is execution-time only |
| 5.7 | Planner/executor separation at the API boundary | Planner has no `runRemote`; executor re-verifies before dispatch |
| 5.8 | `state.runs`/`allocations`/`submissions`/`plans` lengths unchanged after plan+hash | Purity |

Test file: `tests/planner-sideeffects.test.mjs`.

---

## 6. Concurrent exact-once execution (v2 planner + executor)

Reuses the Phase-0 atomic `admitSubmission`/`submitAdmitted` block identically.

| # | Case | Adversarial expectation |
|---|------|------------------------|
| 6.1 | Two concurrent same-(project, operation, argv) | Exactly ONE plan and ONE `sbatch` (submitCount=1) |
| 6.2 | Same (project, operation) with DIFFERENT argv | **BLOCKED while the first submission is `submitting`/`nonterminal`** (captain ruling: pair lock is on (project, operation) — operation identity is NOT parameterized in Phase 1; safety over parallel convenience). The second call is rejected with a pair-in-flight error until the first reaches terminal evidence. If operation identity is DELIBERATELY parameterized (distinct operation names in the manifest per param set), those are different pairs and may run concurrently — that is the ONLY path to parallel-variant execution |
| 6.3 | Different operations same project / different projects | Not blocked by in-flight lock; independent |
| 6.4 | Rejected concurrent call can't re-enter while first sbatch in flight | Gate until dispatch settles |
| 6.5 | Exception after sbatch dispatch keeps the pair gated (ambiguous), never re-submits | Matches v1 post-dispatch gate |
| 6.6 | Unique job name per intent includes CSPRNG token, ≤100 chars (≤63 after current name scheme) | Assert uniqueness + bound |
| 6.7 | Reconciliation matches the exact unique job name, never an earlier run | sacct --name=<unique> |
| 6.8 | Concurrent + timeout → ambiguous; second call reconciles, no second sbatch | submitCount 1 |
| 6.9 | Envelope aggregate with multiple planned intents in flight | Nonterminal allocations sum; oversubscription rejected at admission |
| 6.10 | PlanHash is metadata (captain ruling: pair lock stays (project,operation)); different plan-hash is NOT a separate lock domain | Assert per-pair behavior; planHash recorded on intent only |
| 6.11 | Same (project, operation), different argv, attempted while first is terminal (sacct evidence collected) | Allowed — a new submission for the same pair may start once the previous one is terminal |

Test file: `tests/concurrent-exact-once-v2.test.mjs`.

---

## 7. Schema-v1 compatibility (v2 must not break v1)

| # | Case | Adversarial expectation |
|---|------|------------------------|
| 7.1 | Every existing v1 fixture/project loads with identical semantics | liposome-w5 (and v1 fixtures) load; job specs identical |
| 7.2 | v1 stage/status/fetch surface unchanged | Existing `pinned-stage`/`pinned-fetch`/`pinned-blockers`/`pinned-safety` suites pass unchanged |
| 7.3 | v1 exact-once/allocation/ambiguity semantics unchanged | `pinned-admission-lock`, `pinned-blockers` pass unchanged |
| 7.4 | v1 `python_bin` still parse-only (no execution) | Grep/no interpreter in any SSH path |
| 7.5 | The SAME `validatePinnedSbatch` accepts the compiled v2 wrapper — no per-form validator drift | Parity runner on real+v2-compiled templates |
| 7.6 | v2 additive: enabling it doesn't change v1 remote-call count | v1 happy path keeps its existing remote-call count contract. NOTE (captain ruling): the live gpu03 probe is a STANDALONE call, not embedded — the v1 pinned path currently embeds the probe in the submit command (t10 `gpu03ProbeSnippet`); if the Phase-1 refactor moves v1 to a standalone probe, `pinned-safety`'s "exactly 2 remote calls" assertion must be updated to 3 (verify + probe + submit) IN COORDINATION with t16 — never silently. The v2 path asserts probe + verify + submit as separate remote calls (§3.14) |
| 7.7 | No v1 regression under concurrent execution with v2 code loaded | v1 admission lock holds |
| 7.8 | Backward load: v2 library loads `schema_version: 1`, rejects `schema_version: 3` | Version gate explicit |

Test file: `tests/v1-compat.test.mjs`.

---

## 8. Shared helpers & likely files (unchanged from prior plan)

- `tests/helpers/harness.mjs`: extract `createPinnedTools` harness (counters
  access/jobs/shell/remote, runRemote mock, userQuestions, jobs.start capture,
  `assertNoSideEffects`) + `assertExactlyOneSbatch`.
- `tests/helpers/argv-serializer-fixtures.mjs`: the exhaustive single-quote
  round-trip corpus (§3.4–3.7) reused by wrapper-gen + typed-params + plan-hash.
- `tests/helpers/plan-fixtures.mjs`: canonical v2 manifests (each mutation
  variant), script files, param cases.
- New files: `tests/recipe-resolver.test.mjs` (→ compile/§3), `tests/typed-params.test.mjs`,
  `tests/wrapper-gen.test.mjs`, `tests/plan-hash.test.mjs`, `tests/planner-sideeffects.test.mjs`,
  `tests/plan-session-isolation.test.mjs`, `tests/concurrent-exact-once-v2.test.mjs`,
  `tests/manifest-schema-v2.test.mjs`, `tests/v1-compat.test.mjs`.
- `package.json` test glob already covers `tests/*.test.mjs`.

---

## 9. Verification protocol

1. Bind tests to the t16/t17-approved names; no invented v2 names.
2. `npm run check` green; existing 72+ tests unchanged (v1-compat gate).
3. Determinism: run plan-hash + planner-sideeffects ≥5× (no Date.now/ordering leak).
4. Concurrency: concurrent-exact-once-v2 ≥10× (admission-lock class pressure).
5. Serializer: run the argv round-trip corpus against a real local
   `exec /bin/echo '<argv...>'` harness (or a shell `set -- '<v>'` parse) to
   prove byte-exact single-arg transmission for every corpus item.
6. Parity: v1-compat runner executes BOTH the compiled-v2 wrapper and the v1
   template through `validatePinnedSbatch`; expect 14/14 liposome-w5 v1 passes
   and every compiled v2 wrapper passes (after the t12 template fixes).
7. No remote/network operations; all mocks local.

---

## 9a. Expanded adversarial coverage (captain directive + final-architecture retarget)

Each cluster inherits the zero-side-effect / determinism / purity invariants.

### 9a.1 YAML duplicate-key and alias/merge surprises (`TBD:parse-manifest-v2`)

| # | Case | Adversarial expectation |
|---|------|------------------------|
| A.1 | Duplicate top-level key (`project:` twice) | Reject (duplicate, not last-wins); js-yaml behavior asserted |
| A.2 | Duplicate nested key under a job / `params:` | Reject |
| A.3 | Duplicate key inside `fetch`/`extra_dirs` | Reject |
| A.4 | Anchor/alias reuse (`&x`/`*x`) + `<<:` merge that could couple `params`/`script` | Deep-clone each node (no shared mutation); merged-in forbidden keys rejected; assert no field-coupling leak |
| A.5 | Merge key pulling a base with a forbidden key (`script`, `cpus`, `type`) | Reject |
| A.6 | Alias bomb / deep nesting / huge alias expansion | Byte + node/depth caps fail closed; bounded memory |
| A.7 | Unused anchor tolerated; undefined alias → reject | Pinned js-yaml behavior |
| A.8 | Duplicate+alias combined to bypass a uniqueness rule (two `files` alias the same path object) | Uniqueness on resolved canonical values |

### 9a.2 Canonical JSON key ordering & plan hash (`canonicalJson`)

| # | Case | Adversarial expectation |
|---|------|------------------------|
| B.1–B.3 | Reorder top-level / job / `params` keys | Identical hash |
| B.4 | Change any single value | Different hash |
| B.5 | Whitespace/blank/comment lines | Identical hash (canonical AST) |
| B.6 | Numeric-key-vs-string-key, `1` vs `1.0` | Canonical coercion pinned; no surprise collision |
| B.7 | Equivalent spellings with different semantics (different `cpus`) | Distinct hashes |
| B.8 | Deterministic across calls/workers | Equal hashes |

### 9a.3 Symlink / non-regular input rejection

| # | Case | Adversarial expectation |
|---|------|------------------------|
| C.1 | `script:`/`files` entry that is a symlink pointing outside `local_root` | Reject (or realpath containment inside local_root enforced) |
| C.2 | Directory / FIFO / device node / socket as `script:`/`files` | Reject: `stat().isFile()` exact |
| C.3 | Symlink whose target is inside `local_root` | Rule pinned (allowed-with-realpath vs rejected); no escape |
| C.4 | Broken symlink | Reject (stat fails closed) |
| C.5 | TOCTOU: `script` swapped for a symlink between `stat` and read/digest | Digest binding catches → zero submit |
| C.6 | Symlink created after planning, before submit | Pre-submit digest rejects |
| C.7 | Symlink loop `a->a` | Reject without recursion blowup |

### 9a.4 Plan eviction & session isolation (`state.plans[]`, `_plan_ledger.jsonl`)

| # | Case | Adversarial expectation |
|---|------|------------------------|
| D.1 | Plan under policy hash A rejected after policy rotate B | Plan carries policyHash; stale rejected |
| D.2 | Plan invalidated when the manifest/script file changes after planning | Re-derive-or-reject; never execute stale |
| D.3 | Envelope set/reset between plan and execute | Bound to envelope identity; mismatch → reject |
| D.4 | Plans from session A not visible to session B | Per-session index; cross-session rejected |
| D.5 | Plan cache eviction (bounded); eviction cannot free an in-flight allocation | Eviction only touches unstarted plans; nonterminal allocations survive |
| D.6 | Concurrent agents/sessions can't cross-contaminate intents | Per-session state; no shared mutable store |
| D.7 | Run-finalization/gc doesn't release a still-nonterminal allocation | Envelope accounting until terminal evidence |
| D.8 | Session close → allocations counted until terminal or explicit release | No silent leak that over- or under-counts |
| D.9 | Ledger append-only; truncation/deletion doesn't re-enable a revoked plan | Session index is the authority; ledger is audit |

### 9a.5 Environment-source injection (policy-owned env profile)

| # | Case | Adversarial expectation |
|---|------|------------------------|
| E.1 | A manifest `env_profile:` name must be an exact key in `policy.targets.HPC.environment.recipe_envs` — never a path/string | Compile error otherwise; resolver emits ONLY the policy's `source` (+ `set +u`/`set -u` if `unset_u`) |
| E.2 | Recipe cannot introduce a new remote `source` line (no `source /data01/...` from the manifest) | Only the policy-owned line may appear; grep the compiled text |
| E.3 | `set +u`/`set -u` toggle comes from the policy `unset_u` flag, not the manifest | Assert emitted toggle matches policy; manifest can't control it |
| E.4 | Ambient `BASH_ENV`/`PATH`/`LD_PRELOAD`/`PYTHONPATH` doesn't change wrapper bytes/hash | Wrapper is a pure function of (manifest, params, policy, envelope, env_profile) |
| E.5 | `env_profile` absent / policy section absent → no env line emitted, job still valid | Default: no source line; works like a plain v1 job minus template |
| E.6 | Hostile `env_profile` value (shell chars, `/`, `..`) | Rejected at compile (allowlist key) |

### 9a.6 argv newline / NUL / option injection (typed placeholders → single-quote argv)

| # | Case | Adversarial expectation |
|---|------|------------------------|
| F.1 | Param with `\n` / CRLF / `\r` | Reject (cannot be a single argv token) |
| F.2 | Param with NUL (`\0`) | Reject with clear error |
| F.3 | Param starting `-`/`--` | Rejected at type validation for `enum`/`boolean`/`integer`; passed verbatim as positional argv for `path` — pinned per-type rule (§2.1); never a wrapper option |
| F.4 | Param `--job-name=...` etc. embedding scheduler options | Cannot reach `#SBATCH` (params never splice directives); single-quoted positional token only |
| F.5 | Param with `& ; | $( backtick { }` | Rejected by allowlist or stays literal inside single quotes; compiled wrapper still passes `validatePinnedSbatch` |
| F.6 | Extremely long param | Bounded; reject over cap |
| F.7 | Round-trip: emitted `'<v>'` shell-parses to exactly `<v>` | Serializer corpus (§3.4/3.5) |
| F.8 | NUL in the compiled wrapper (generation bug) | `validatePinnedSbatch` rejects NUL |

### 9a.7 Resource overflow (`integer` params + job-level caps)

| # | Case | Adversarial expectation |
|---|------|------------------------|
| G.1 | `cpus/gpus/concurrency` = NaN/Infinity/-Infinity | Reject |
| G.2 | `MAX_SAFE_INTEGER`, `2^53+1`, exponent/hex (`1e6`, `0xFF`), `Number.isSafeInteger` after multiply | Reject or pinned positive-integer path; no Infinity coercion |
| G.3 | Leading-zero numeric strings (`"04"`, `"0"`) | Reject or pinned; `0` always rejected |
| G.4 | `cpus` so large `nodes*ntasks*cpusPerTask` overflows | Reject after multiplication |
| G.5 | Aggregate overflow of summed nonterminal allocations | Safe arithmetic; reject overflow (no wrap) |
| G.6 | Param-type `integer` min/max themselves huge/unbounded | Bounds mandatory; reject absent/`min>max` |
| G.7 | Scientific notation / `"true"` string for integer | Rejected by typed validation (no coercion) |
| G.8 | Negative cpus/gpus/concurrency | Reject |

### 9a.8 plan() / compile() invokes zero hooks (purity — first-class test)

| # | Case | Adversarial expectation |
|---|------|------------------------|
| H.1 | `compile()`/`plan()` on a VALID v2 manifest + params | access/jobs/remote/shell/userQuestions.ask/fs-writes/state lengths all 0 |
| H.2 | Same on an INVALID manifest/params (mix of §1/§2/§3 rejections) | Same zero tuple after the throwing/rejecting call |
| H.3 | Twice with identical input | Identical plan + hash; every counter zero both times |
| H.4 | Under hostile ambient (BASH_ENV, LD_PRELOAD, metachar param, oversized manifest) | Same zero tuple; no hook fires |
| H.5 | Structural: `compile`/planner receives only (scriptEntry, params, policy, envelope, manifestMeta) — no remote/shell/jobs/question seams at all | API-shape assertion (strongest form of H.1) |
| H.6 | Plan-hash locked before first use; mutation → different hash; stale rejected | §4.6/9a.4 |

---

## 9b. Reconciliation with the final architecture (supersedes the rejected recipe grammar)

| Earlier concept (rejected t13) | Final contract (captain ruling) |
|---|---|
| `recipe:` with inline `run`/`test`/`env`/`block` shell steps | `script:` = a staged file path in `manifest.files`; ONLY typed argv placeholders (`enum`/`integer`/`boolean`/`path` — no float/string, no coercion) |
| Resolver emits arbitrary command lines | Resolver emits the fixed SBATCH header + a single `exec '<script relative to $SLURM_SUBMIT_DIR>' '<argv>…'` line through the safe single-quote serializer |
| `env:` steps carry arbitrary `NAME=VALUE` | `env_profile:` = policy-owned allowlist key; the only env lines are the policy's `source` + `unset_u` toggle |
| `run_dir`/`python_bin` auto-emitted from manifest | **NO hardcoded `remote_root`/`run_dir=` in the wrapper body** (captain ruling); script path is relative to `$SLURM_SUBMIT_DIR`; `python_bin` stays parse-only and is never used by the resolver |
| Arbitrary shell trust (R1 in t13) | Closed: no `eval`/`bash -c`/`sh -c`; workload = trusted staged script; wrapper = fixed invocation + serialized argv |
| Live gpu03 probe embedded in the sbatch command | **Standalone bounded read-only probe call before the submit command** (captain ruling; §3.14) |
| Pair lock / plan-hash (t13 "planHash as separate domain") | Pair lock stays `(project, operation)`; `planHash` is metadata only (captain ruling; §6.2/6.10) |
| plan store / plan hash / workflows / genbio_project_* | UNCHANGED from this plan's perspective — §4/§5/§6/§9a.4 still apply verbatim |

**Map of final seams** → `TBD:parse-manifest-v2` in `lib/pinned.js`;
`TBD:compile`/`TBD:resolve-params` in `lib/recipe-resolver.js` (pure;
`{ sbatchText, bytesSha, argv }`); `TBD:plan-hash`/`TBD:plan-store` in
`lib/plan-store.js` (`canonicalJson` + `sha256`, `state.plans[]` +
`_plan_ledger.jsonl`); `admitSubmission`/`submitAdmitted` extracted in
`lib/pinned.js` (reused unchanged); `genbio_project_list/_inspect/_plan/_run/_status`
in `lib/index.js`.

---

## 10. Out of scope / explicit non-goals for Phase-1 tests

- Any inline-command / arbitrary-shell / `run|test|block` recipe grammar: the
  captain rejected it — tests assert it is a compile error, and no such path is
  tested as valid.
- **Manifest v3+** and future schemas.
- Live HPC behavior (no real sbatch/squeue/sacct): everything mocked; this plan
  guards determinism + confinement.
- AI.zymes bundle correctness (science team).
- Script-owner behavior inside the script itself (the script is trusted; the
  planner's job ends at safe argv serialization — options parsed by a trusted
  script are out of wrapper scope, documented in §2.1/§3).

---

### Outstanding bind-readiness notes for t16/t17

- Pin the closed type set exactly as §2: `enum`/`integer`/`boolean`/`path`
  ONLY — no `float`, no generic `string`, **no coercion of any kind** (a
  `"4"` string is NOT an integer; `"true"` is NOT a boolean; all are rejected).
  Case-sensitivity of enums pinned.
- Pin the path rule (relative-only, `..`/absolute/`\`/NUL/control/`'`/`$`
  rejected; resolves under `$SLURM_SUBMIT_DIR`).
- Decide F.3 (leading `-`) per type; default recommendation: reject leading `-`
  in `enum`/`boolean`/`integer`, verbatim-positional for `path` (§2.1).
- Pair lock = `(project, operation)`, planHash metadata only; same-operation
  different-params is BLOCKED while submitting/nonterminal (§6.2/6.11).
- Probe is a STANDALONE remote call before submit, never embedded (§3.14);
  coordinate v1 `pinned-safety` remote-call-count if the refactor un-embeds it (§7.6).
- No hardcoded `remote_root`/`run_dir=` in any emitted wrapper; script path is
  relative to `$SLURM_SUBMIT_DIR` (§3/§3.13).
- Decouple Q6 (js-yaml duplicate/alias policy) and Q8 (symlink-inside-root) —
  both still need an explicit ruling to pin; recommend sec-verify in t16/S2.
- The safe single-quote serializer is the **most security-critical new
  primitive**: prove round-trip with a real local `exec` harness (§9.6) before
  any submission path depends on it.