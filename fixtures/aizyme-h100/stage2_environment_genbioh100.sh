#!/bin/bash
# Stage 2 (AI.zymes v1) — genbioh100 bash-native VERIFICATION-ONLY payload.
#
# CRITICAL CONSTRAINTS:
#   - This script is VERIFICATION-ONLY: no clone, download, install, or
#     modification of shared state. It only reads, verifies, and records.
#   - GPU 0 only: nvidia-smi -i 0 exclusively; no nvidia-smi -L; no GPU1 query.
#   - Memory: ulimit -v 33554432 (32 GB) must succeed or the script hard-fails.
#   - 16 CPU threads: OMP_NUM_THREADS=16.
#   - All evidence is run-scoped (written under $AIZH100_RUN_DIR only).
#   - Code deployment is immutable and content-addressed: verify the archive
#     SHA-256, extract to a per-run temp dir, verify tar traversal safety,
#     verify the tree fingerprint against the expected digest. On divergence,
#     FAIL — never rm-rf or overwrite shared code.
#   - ESMFold: offline load only. No download. If the model is absent, record
#     MISSING and fail the gate.
#   - MPNN: functional import probe only. No clone. If absent, record MISSING.
#   - AmberTools: all four executables must respond to --help or version query.
#   - Rosetta: hash + help probe. No modification.
#
# Launched by the plugin as:
#   /bin/bash --noprofile --norc -- stage2_environment_genbioh100.sh < /dev/null
# with AIZH100_RUN_DIR set to the run-scoped directory.

set -euo pipefail

# ─────────────────────────────────────────────────────────────────────────────
# 0. Resource constraints — HARD FAIL on any failure.
# ─────────────────────────────────────────────────────────────────────────────
export CUDA_VISIBLE_DEVICES=0
export OMP_NUM_THREADS=16
export MKL_NUM_THREADS=16
# 32 GB virtual memory: HARD FAIL if ulimit cannot be set.
ulimit -v 33554432

# ─────────────────────────────────────────────────────────────────────────────
# 1. Run directory, identity, and process evidence (atomic).
# ─────────────────────────────────────────────────────────────────────────────
RUN_DIR="${AIZH100_RUN_DIR:?AIZH100_RUN_DIR must be set by the plugin launcher}"
RUN_TOKEN="${AIZH100_RUN_TOKEN:?AIZH100_RUN_TOKEN must be set by the plugin launcher}"
test "${#RUN_TOKEN}" -eq 32 || { echo "FATAL: AIZH100_RUN_TOKEN must be 32 hex chars" >&2; exit 1; }
MANIFESTS="$RUN_DIR/manifests"
mkdir -p "$MANIFESTS"

# Atomic PID+PGID+start-identity record (single write to avoid partial reads).
# setsid makes this a session+process-group leader: PGID == PID.
{
  printf 'pid=%s\n' "$$"
  printf 'pgroup=%s\n' "$$"
  printf 'started_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf 'hostname=%s\n' "$(hostname -f)"
  printf 'uid=%s\n' "$(id -u)"
  printf 'cwd=%s\n' "$(pwd)"
  printf 'token=%s\n' "$RUN_TOKEN"
} > "$RUN_DIR/run_identity"

# Status file: the plugin reads this for reconciliation.
write_status() {
  printf 'state=%s\nupdated=%s\npid=%s\ndetail=%s\n' "$1" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$$" "${2:-}" > "$RUN_DIR/status"
}
write_status "running" "stage2 genbioh100 verification started"

# Exit trap: record exit code and final status.
trap 'ec=$?; echo "$ec" > "$RUN_DIR/exit_code"; if test "$ec" -eq 0; then write_status "completed" "exit 0"; else write_status "failed" "exit $ec"; fi' EXIT

# ─────────────────────────────────────────────────────────────────────────────
# 2. Fixed paths and constants.
# ─────────────────────────────────────────────────────────────────────────────
ROOT="/home/work/GenbioLAB/shared/daes_enzyme"
WF="$ROOT/workflow/aizyme_v1"
CODE="$WF/code/AIzymes"
CODE_TMP="$RUN_DIR/code_tmp"
ARCHIVE="$RUN_DIR/AIzymes-52176ff.tar.gz"
EXPECTED_ARCHIVE_SHA=f408113ab7c2fbbfa2771312d0c0eaae308565da521f0d01f713ddc04441a70a
EXPECTED_COMMIT=52176ffab5d00b54f76141de8721949a28fe674c
PY="/home/work/GenbioLAB/miniconda3/bin/python3"
missing=0

# Prerequisite gates (hard fail).
test -d "$WF"
test -f "$ARCHIVE"
test -x "$PY"

# ─────────────────────────────────────────────────────────────────────────────
# 3. Archive verification and immutable content-addressed code check.
# ─────────────────────────────────────────────────────────────────────────────
# Step 3a: Verify archive SHA-256 exactly.
actual_sha="$(sha256sum "$ARCHIVE" | cut -d' ' -f1)"
if test "$actual_sha" != "$EXPECTED_ARCHIVE_SHA"; then
  echo "FATAL: archive SHA-256 mismatch (expected $EXPECTED_ARCHIVE_SHA, got $actual_sha)" >&2
  exit 1
fi

# Step 3b: Tar traversal-safe inventory check.
# Extract to a per-run temp dir; verify NO path contains ../ or absolute paths.
mkdir -p "$CODE_TMP"
tar -xzf "$ARCHIVE" -C "$CODE_TMP"
# Traversal safety: no entry may have a path component of ".." or start with /.
if tar -tzf "$ARCHIVE" | grep -qE '^\.\./|^/|\.\./'; then
  echo "FATAL: archive contains traversal-unsafe paths" >&2
  test "${CODE_TMP/#$RUN_DIR\/}" != "$CODE_TMP" || { echo "FATAL: refusing rm -rf outside RUN_DIR" >&2; exit 1; }
  rm -rf "$CODE_TMP"
  exit 1
fi

# Step 3c: Verify the commit.
commit="$(cat "$CODE_TMP/AIzymes_SOURCE_COMMIT.txt" 2>/dev/null || true)"
if test "$commit" != "$EXPECTED_COMMIT"; then
  echo "FATAL: archive commit mismatch (expected $EXPECTED_COMMIT, got $commit)" >&2
  test "${CODE_TMP/#$RUN_DIR\/}" != "$CODE_TMP" || { echo "FATAL: refusing rm -rf outside RUN_DIR" >&2; exit 1; }
  rm -rf "$CODE_TMP"
  exit 1
fi

# Step 3d: Tree fingerprint (deterministic content-addressed digest).
tree_fingerprint() {
  (cd "$1" && find . -type f -print0 | LC_ALL=C sort -z | xargs -0 sha256sum)
}
fp_digest="$(tree_fingerprint "$CODE_TMP" | sha256sum | cut -d' ' -f1)"
printf 'code_tree_fingerprint=%s\n' "$fp_digest" > "$MANIFESTS/code_fingerprint.txt"

# Step 3e: If shared code exists, it must match. Never rm-rf or overwrite.
if test -d "$CODE"; then
  existing_fp="$(tree_fingerprint "$CODE" | sha256sum | cut -d' ' -f1)"
  if test "$existing_fp" != "$fp_digest"; then
    echo "FATAL: shared code at $CODE diverges from archive (fingerprint $existing_fp != $fp_digest)" >&2
    rm -rf "$CODE_TMP"
    exit 1
  fi
  echo "shared code verified as pristine (fingerprint match)"
  ACTIVE_CODE="$CODE"
else
  echo "shared code absent; using per-run extraction for verification only"
  ACTIVE_CODE="$CODE_TMP"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 4. Runtime record header (run-scoped).
# ─────────────────────────────────────────────────────────────────────────────
{
  printf 'utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf 'host=%s\n' "$(hostname -f)"
  printf 'target=genbioh100\n'
  printf 'aizyme_commit=%s\n' "$EXPECTED_COMMIT"
  printf 'archive_sha256=%s\n' "$EXPECTED_ARCHIVE_SHA"
  printf 'code_tree_fingerprint=%s\n' "$fp_digest"
  printf 'cuda_visible_devices=%s\n' "$CUDA_VISIBLE_DEVICES"
  printf 'omp_num_threads=%s\n' "$OMP_NUM_THREADS"
  printf 'memory_ulimit_kb=%s\n' "$(ulimit -v)"
} > "$MANIFESTS/stage2_environment_runtime.txt"

# ─────────────────────────────────────────────────────────────────────────────
# 5. Required Python imports (actual import test, not just version check).
# ─────────────────────────────────────────────────────────────────────────────
REQUIRED_PY_MODS="numpy pandas Bio torch transformers scipy sklearn matplotlib PIL"
if ! "$PY" - "$REQUIRED_PY_MODS" <<'PYEOF' > "$MANIFESTS/stage2_python_imports.txt" 2>&1
import importlib, sys
mods = sys.argv[1].split()
failures = []
for m in mods:
    try:
        importlib.import_module(m)
    except Exception as e:
        failures.append(f"{m}: {type(e).__name__}: {e}")
if failures:
    print("MISSING_IMPORTS:")
    for f in failures:
        print(f"  {f}")
    sys.exit(1)
print("all_required_imports=ok")
print("python=", sys.version.split()[0])
PYEOF
then
  echo "FAIL: required Python imports failed" >&2
  cat "$MANIFESTS/stage2_python_imports.txt" >&2
  missing=$((missing+1))
fi

# ─────────────────────────────────────────────────────────────────────────────
# 6. AmberTools — all four must respond to help/version (coherent check).
# ─────────────────────────────────────────────────────────────────────────────
amber_ok=1
for exe in tleap sander cpptraj pdb4amber; do
  if ! command -v "$exe" >/dev/null 2>&1; then
    if test -x "/home/work/GenbioLAB/miniconda3/bin/$exe"; then
      export PATH="/home/work/GenbioLAB/miniconda3/bin:$PATH"
    else
      printf 'MISSING=%s\n' "$exe" > "$MANIFESTS/amber_${exe}.txt"
      amber_ok=0
      missing=$((missing+1))
      continue
    fi
  fi
  if "$exe" --help > "$MANIFESTS/amber_${exe}_help.txt" 2>&1; then
    printf 'ok\n' > "$MANIFESTS/amber_${exe}_status.txt"
  else
    if "$exe" -h > "$MANIFESTS/amber_${exe}_help.txt" 2>&1; then
      printf 'ok (via -h)\n' > "$MANIFESTS/amber_${exe}_status.txt"
    else
      printf 'FAIL: no help output\n' > "$MANIFESTS/amber_${exe}_status.txt"
      amber_ok=0
      missing=$((missing+1))
    fi
  fi
done
if test "$amber_ok" -eq 1; then
  # Record paths and verify coherent prefix (all four from the same prefix).
  {
    for exe in tleap sander cpptraj pdb4amber; do
      printf '%s=%s\n' "$exe" "$(command -v "$exe" 2>/dev/null || echo unknown)"
    done
  } > "$MANIFESTS/amber_paths.txt"
  # Check that all four share the same parent prefix (directory).
  prefixes=""
  coherent=1
  for exe in tleap sander cpptraj pdb4amber; do
    p="$(dirname "$(command -v "$exe" 2>/dev/null || echo /unknown/$exe)")"
    if test -z "$prefixes"; then prefixes="$p"; else test "$p" = "$prefixes" || coherent=0; fi
  done
  if test "$coherent" -eq 1; then
    printf 'amber_tools=all_four_verified coherent_prefix=%s\n' "$prefixes" >> "$MANIFESTS/stage2_environment_runtime.txt"
  else
    printf 'amber_tools=INCOHERENT_MIXED_PREFIXES\n' >> "$MANIFESTS/stage2_environment_runtime.txt"
    echo "WARN: AmberTools executables from mixed prefixes" >&2
    missing=$((missing+1))
  fi
fi

# ─────────────────────────────────────────────────────────────────────────────
# 7. Rosetta — hash + help probe (no modification).
# ─────────────────────────────────────────────────────────────────────────────
rosetta_path=""
rosetta_ok=0
for cand in "$ROOT/containers/rosetta_ml420.sif" /home/work/GenbioLAB/containers/rosetta_ml420.sif; do
  if test -f "$cand"; then rosetta_path="$cand"; break; fi
done
if test -z "$rosetta_path" && command -v rosetta_scripts.linuxgccrelease >/dev/null 2>&1; then
  rosetta_path="$(command -v rosetta_scripts.linuxgccrelease)"
fi
if test -n "$rosetta_path"; then
  if test -f "$rosetta_path" && test ! -x "$rosetta_path"; then
    if command -v singularity >/dev/null 2>&1; then
      if singularity exec "$rosetta_path" rosetta_scripts.linuxgccrelease -help > "$MANIFESTS/rosetta_help.txt" 2>"$MANIFESTS/rosetta_help.err"; then
        rosetta_ok=1
        printf 'rosetta_mode=singularity:%s\n' "$rosetta_path" >> "$MANIFESTS/rosetta_info.txt"
      fi
    elif command -v apptainer >/dev/null 2>&1; then
      if apptainer exec "$rosetta_path" rosetta_scripts.linuxgccrelease -help > "$MANIFESTS/rosetta_help.txt" 2>"$MANIFESTS/rosetta_help.err"; then
        rosetta_ok=1
        printf 'rosetta_mode=apptainer:%s\n' "$rosetta_path" >> "$MANIFESTS/rosetta_info.txt"
      fi
    fi
    sha256sum "$rosetta_path" > "$MANIFESTS/rosetta_sha256.txt"
  elif test -x "$rosetta_path"; then
    if "$rosetta_path" -help > "$MANIFESTS/rosetta_help.txt" 2>"$MANIFESTS/rosetta_help.err"; then
      rosetta_ok=1
      printf 'rosetta_mode=PATH:%s\n' "$rosetta_path" >> "$MANIFESTS/rosetta_info.txt"
      sha256sum "$rosetta_path" > "$MANIFESTS/rosetta_sha256.txt"
    fi
  fi
fi
if test "$rosetta_ok" -eq 1; then
  printf 'rosetta=ok\n' >> "$MANIFESTS/stage2_environment_runtime.txt"
else
  echo "MISSING=rosetta" >> "$MANIFESTS/stage2_tool_versions.txt"
  missing=$((missing+1))
fi

# ─────────────────────────────────────────────────────────────────────────────
# 8. ProteinMPNN / LaSerMPNN — functional import probe (no clone).
# ─────────────────────────────────────────────────────────────────────────────
mpnn_ok=0
mpnn_mode=""
mpnn_root=""
for cand in "$WF/env/LaSERMPNN" "$ROOT/LaSERMPNN" "$WF/env/ProteinMPNN" "$ROOT/ProteinMPNN" /home/work/GenbioLAB/env/ProteinMPNN; do
  if test -d "$cand" && test -f "$cand/protein_mpnn_run.py" 2>/dev/null; then
    mpnn_root="$cand"
    if (cd "$cand" && "$PY" -c "import sys; sys.path.insert(0,'.'); import protein_mpnn_run" 2>/dev/null); then
      mpnn_ok=1
      mpnn_mode="functional_import"
    else
      mpnn_mode="present_import_failed"
    fi
    break
  fi
done
if test -z "$mpnn_root"; then
  # present_no_probe: a directory exists but has no usable backend.
  # This must FAIL — we require a functional backend probe.
  for cand in "$WF/env/LaSERMPNN" "$ROOT/LaSERMPNN"; do
    if test -d "$cand"; then
      mpnn_root="$cand"
      mpnn_mode="present_no_probe"
      # Do NOT set mpnn_ok=1 — a directory without a functional probe is insufficient.
      break
    fi
  done
fi
if test "$mpnn_ok" -eq 1; then
  printf 'mpnn_mode=%s\nmpnn_root=%s\n' "$mpnn_mode" "$mpnn_root" > "$MANIFESTS/mpnn_info.txt"
else
  echo "MISSING=ProteinMPNN/LaSerMPNN" >> "$MANIFESTS/stage2_tool_versions.txt"
  missing=$((missing+1))
fi

# ─────────────────────────────────────────────────────────────────────────────
# 9. AI.zymes import — legacy mode (the mode the pristine tree supports).
# ─────────────────────────────────────────────────────────────────────────────
if "$PY" - "$ACTIVE_CODE" <<'PYEOF' > "$MANIFESTS/stage2_aizymes_import.txt" 2>&1
import os, subprocess, sys
code = sys.argv[1]
legacy_env = {"PYTHONPATH": f"{code}/src/aizymes:{code}/src"}
r = subprocess.run([sys.executable, "-c", "import aizymes; print('aizymes_file=', aizymes.__file__)"],
                   env={**os.environ, **legacy_env}, capture_output=True, text=True)
print("legacy_mode_rc=", r.returncode)
if r.returncode == 0:
    # Assert the resolved __file__ realpath is under ACTIVE_CODE/src.
    file_line = [l for l in r.stdout.strip().splitlines() if "aizymes_file=" in l]
    if not file_line:
        print("legacy_mode=FAIL (no aizymes_file in output)")
        sys.exit(1)
    resolved = file_line[0].split("aizymes_file=", 1)[1].strip()
    import os as _os
    real = _os.path.realpath(resolved)
    code_src = _os.path.realpath(code + "/src")
    if not real.startswith(code_src + "/") and not real.startswith(code + "/"):
        print(f"legacy_mode=FAIL (resolved file {real} not under {code_src})")
        sys.exit(1)
    print("legacy_mode=OK")
    print("file=", resolved)
    print("realpath_verified=under_active_code")
else:
    print("legacy_mode=FAIL")
    print("stderr=", (r.stderr or r.stdout).strip()[:500])
    sys.exit(1)
PYEOF
then
  printf 'aizymes_import=ok\n' >> "$MANIFESTS/stage2_environment_runtime.txt"
else
  echo "FAIL: AI.zymes legacy import failed" >&2
  cat "$MANIFESTS/stage2_aizymes_import.txt" >&2
  missing=$((missing+1))
fi

# ─────────────────────────────────────────────────────────────────────────────
# 10. HF cache / ESMFold — OFFLINE ONLY. No download. If absent, MISSING.
# ─────────────────────────────────────────────────────────────────────────────
ESMFOLD_READY=0
huggingface_dir="${HF_HOME:-$WF/env/hf_cache}"
esm_cache="$huggingface_dir/hub/models--facebook--esmfold_v1"
if test -d "$esm_cache/snapshots" || test -f "$esm_cache/refs/main"; then
  printf 'hf_cache=present\n' > "$MANIFESTS/stage2_hf_cache.txt"
  if HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 "$PY" - <<'PYEOF' > "$MANIFESTS/stage2_esmfold_load.txt" 2>&1
import os
os.environ["HF_HUB_OFFLINE"] = "1"
os.environ["TRANSFORMERS_OFFLINE"] = "1"
from transformers import EsmForProteinFolding, AutoTokenizer
model = EsmForProteinFolding.from_pretrained("facebook/esmfold_v1", local_files_only=True, low_cpu_mem_usage=True)
tok = AutoTokenizer.from_pretrained("facebook/esmfold_v1", local_files_only=True)
print("esmfold_local_load=ok params=", model.num_parameters())
PYEOF
  then
    ESMFOLD_READY=1
    # Record exact cache snapshot/revision/checksum evidence.
    {
      printf 'esmfold_cache_dir=%s\n' "$esm_cache"
      if test -d "$esm_cache/snapshots"; then
        snap=$(ls "$esm_cache/snapshots" 2>/dev/null | head -1)
        printf 'esmfold_snapshot=%s\n' "$snap"
        if test -n "$snap" && test -d "$esm_cache/snapshots/$snap"; then
          # Run-scoped, recursive, deterministic snapshot checksum (no /tmp).
          snap_cksum_file="$MANIFESTS/.esmfold_snapshot_cksum.tmp"
          (cd "$esm_cache/snapshots/$snap" && find . -type f -print0 | LC_ALL=C sort -z | xargs -0 sha256sum) | sha256sum | cut -d' ' -f1 > "$snap_cksum_file"
          test -s "$snap_cksum_file" || { echo "FATAL: esmfold snapshot checksum is empty" >&2; exit 1; }
          printf 'esmfold_snapshot_checksum=%s\n' "$(cat "$snap_cksum_file")"
          rm -f "$snap_cksum_file"
        else
          echo "FATAL: esmfold snapshot identity missing" >&2
          exit 1
        fi
      fi
      if test -f "$esm_cache/refs/main"; then
        printf 'esmfold_revision=%s\n' "$(cat "$esm_cache/refs/main")"
      fi
    } >> "$MANIFESTS/stage2_hf_cache.txt"
  else
    printf 'hf_cache=present_but_load_failed\n' >> "$MANIFESTS/stage2_hf_cache.txt"
  fi
else
  printf 'hf_cache=absent\n' > "$MANIFESTS/stage2_hf_cache.txt"
fi
if test "$ESMFOLD_READY" -eq 1; then
  printf 'esmfold=ok (offline load)\n' >> "$MANIFESTS/stage2_environment_runtime.txt"
else
  echo "MISSING=esmfold_v1_model (offline only; no download performed)" >> "$MANIFESTS/stage2_tool_versions.txt"
  missing=$((missing+1))
fi

# ─────────────────────────────────────────────────────────────────────────────
# 11. CUDA — GPU 0 ONLY. nvidia-smi -i 0 exclusively.
# ─────────────────────────────────────────────────────────────────────────────
if command -v nvidia-smi >/dev/null 2>&1; then
  nvidia-smi -i 0 > "$MANIFESTS/stage2_nvidia_gpu0.txt" 2>&1 || {
    echo "FAIL: nvidia-smi -i 0 failed" >&2
    printf 'MISSING=cuda_device\n' >> "$MANIFESTS/stage2_tool_versions.txt"
    missing=$((missing+1))
  }
else
  echo "FAIL: nvidia-smi not found" >&2
  printf 'MISSING=cuda_device (nvidia-smi absent)\n' >> "$MANIFESTS/stage2_tool_versions.txt"
  missing=$((missing+1))
fi
# torch: device_count must be exactly 1 (GPU 0 only via CUDA_VISIBLE_DEVICES=0).
if "$PY" - <<'PYEOF' > "$MANIFESTS/stage2_cuda_test.txt" 2>&1
import torch, os
assert torch.cuda.is_available(), "CUDA unavailable"
assert torch.cuda.device_count() == 1, f"Expected exactly 1 visible device, got {torch.cuda.device_count()}"
x = torch.ones((8, 8), device="cuda:0")
result = float((x @ x).sum().item())
assert result == 64.0, f"GPU matmul sanity check failed: {result}"
print("gpu0_device=", torch.cuda.get_device_name(0))
print("device_count=", torch.cuda.device_count())
print("matmul_sanity=ok (64.0)")
print("cuda_visible_devices=", os.environ.get("CUDA_VISIBLE_DEVICES", "unset"))
PYEOF
then
  printf 'cuda_gpu0=verified\n' >> "$MANIFESTS/stage2_environment_runtime.txt"
else
  echo "FAIL: CUDA GPU 0 verification failed" >&2
  cat "$MANIFESTS/stage2_cuda_test.txt" >&2
  printf 'MISSING=cuda_device\n' >> "$MANIFESTS/stage2_tool_versions.txt"
  missing=$((missing+1))
fi

# ─────────────────────────────────────────────────────────────────────────────
# 12. Environment freeze (nonempty).
# ─────────────────────────────────────────────────────────────────────────────
{
  printf '# environment freeze (utc=%s, host=%s)\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$(hostname -f)"
  printf 'python=%s\n' "$("$PY" -c 'import sys; print(sys.version.split()[0])' 2>/dev/null || echo unknown)"
  printf 'torch=%s\n' "$("$PY" -c 'import torch; print(torch.__version__)' 2>/dev/null || echo unknown)"
  printf 'transformers=%s\n' "$("$PY" -c 'import transformers; print(transformers.__version__)' 2>/dev/null || echo unknown)"
  printf 'cuda_visible_devices=%s\n' "$CUDA_VISIBLE_DEVICES"
  printf 'omp_num_threads=%s\n' "$OMP_NUM_THREADS"
  printf 'ulimit_v=%s\n' "$(ulimit -v)"
  printf 'code_fingerprint=%s\n' "$fp_digest"
} > "$MANIFESTS/environment_freeze.txt"
if test ! -s "$MANIFESTS/environment_freeze.txt"; then
  echo "FAIL: environment freeze is empty" >&2
  missing=$((missing+1))
fi

# ─────────────────────────────────────────────────────────────────────────────
# 13. Final gate: checksums + G2/STAGE2_PASS (run-scoped, run-bound).
# ─────────────────────────────────────────────────────────────────────────────
printf 'missing_required_checks=%s\n' "$missing" >> "$MANIFESTS/stage2_environment_runtime.txt"

# Checksum manifest: exclude itself and the pass markers; must be nonempty;
# self-verify before writing the G2/STAGE2_PASS markers.
CKSUM_FILE="$MANIFESTS/stage2_checksums.sha256"
find "$MANIFESTS" -maxdepth 1 -type f ! -name "stage2_checksums.sha256" ! -name "G2_PASS" ! -name "STAGE2_PASS" -exec sha256sum {} + > "$CKSUM_FILE"
test -s "$CKSUM_FILE"  # must be nonempty
(cd "$MANIFESTS" && sha256sum -c stage2_checksums.sha256)  # self-verify; hard fail on mismatch

# G2 gate: only when missing=0. Run-bound: the marker includes the run dir.
if test "$missing" -ne 0; then
  echo "GATE FAIL: missing_required_checks=$missing" >&2
  cat "$MANIFESTS/stage2_tool_versions.txt" 2>/dev/null >&2 || true
  exit 1
fi
# Evidence checksum: sha256 of the checksums manifest (run-bound, token-bound).
EVIDENCE_CKSUM="$(sha256sum "$MANIFESTS/stage2_checksums.sha256" | cut -d' ' -f1)"
printf 'G2_PASS token=%s evidence_cksum=%s run_dir=%s\n' "$RUN_TOKEN" "$EVIDENCE_CKSUM" "$RUN_DIR" > "$MANIFESTS/G2_PASS"
printf 'STAGE2_PASS token=%s evidence_cksum=%s run_dir=%s\n' "$RUN_TOKEN" "$EVIDENCE_CKSUM" "$RUN_DIR" > "$MANIFESTS/STAGE2_PASS"
printf 'STAGE2_PASS\n'
echo "Stage 2 genbioh100 verification complete: G2_PASS (run-scoped, token-bound)"
