#!/bin/bash
#SBATCH --job-name=aizv01
#SBATCH --partition=gpus
#SBATCH --nodes=1
#SBATCH --nodelist=gpu04
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --output=%x_%j.out
#SBATCH --error=%x_%j.err

set -euo pipefail
cd "$SLURM_SUBMIT_DIR"

ROOT=/data01/genbiolab/mdanh/data/simulation/daes_enzyme
FINAL_WF="$ROOT/workflow/aizyme_v1"
WF_TMP="$ROOT/workflow/.aizyme_v1.stage0-1.$SLURM_JOB_ID"
QC="$ROOT/data/01c_g1_pilot/qc/moxdae_postrelax/moxdae_postrelax_qc.json"
CONTRACT="$SLURM_SUBMIT_DIR/stage0_execution_contract.yaml"

test -d "$ROOT"
test -f "$QC"
test ! -e "$FINAL_WF"
test ! -e "$WF_TMP"
mkdir -p "$WF_TMP"/{Input,config,manifests,patches,scripts,tests,code}
cp -- "$CONTRACT" "$WF_TMP/manifests/stage0_execution_contract.yaml"
WF="$WF_TMP"

# The AI.zymes source arrives in Stage 2 from the allowlisted tarball; no
# checkout can exist under $FINAL_WF (above it is required to be absent).
AIZYMES_COMMIT=52176ffab5d00b54f76141de8721949a28fe674c
AIZYMES_SOURCE_PRESENT=false
printf 'aizymes_commit: %s\n' "$AIZYMES_COMMIT" > "$WF/manifests/stage0_remote_facts.yaml"
printf 'aizymes_source_present: %s\n' "$AIZYMES_SOURCE_PRESENT" >> "$WF/manifests/stage0_remote_facts.yaml"
printf 'hostname: %s\n' "$(hostname -f)" >> "$WF/manifests/stage0_remote_facts.yaml"
printf 'utc: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$WF/manifests/stage0_remote_facts.yaml"
printf 'root: %s\n' "$ROOT" >> "$WF/manifests/stage0_remote_facts.yaml"
df -h "$ROOT" > "$WF/manifests/stage0_disk_df.txt"
df -i "$ROOT" > "$WF/manifests/stage0_inode_df.txt"

python3 - "$ROOT" "$QC" "$WF" <<'PY'
import hashlib, json, math, os, shutil, sys
from pathlib import Path

root, qc_path, wf = map(Path, sys.argv[1:])
input_dir = wf / "Input"
manifest_dir = wf / "manifests"
config_dir = wf / "config"

def sha(path):
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()

def existing_candidates(value):
    if not isinstance(value, str) or not value.lower().endswith(".pdb"):
        return []
    raw = value.replace("file://", "")
    candidates = []
    for candidate in (Path(raw), qc_path.parent / raw, root / raw):
        if candidate.is_file() and candidate not in candidates:
            candidates.append(candidate)
    return candidates

def walk_strings(obj):
    if isinstance(obj, dict):
        for key, value in obj.items():
            yield key, value
            yield from walk_strings(value)
    elif isinstance(obj, list):
        for value in obj:
            yield from walk_strings(value)

qc = json.loads(qc_path.read_text())
candidates = []
for key, value in walk_strings(qc):
    for path in existing_candidates(value):
        candidates.append((str(key), path))
unique = []
for key, path in candidates:
    if path not in [item[1] for item in unique]:
        unique.append((key, path))
resolution = manifest_dir / "qc_pdb_resolution.yaml"
if len(unique) == 0:
    raise SystemExit("QC JSON referenced no existing PDB: " + repr([(k, str(p)) for k, p in candidates]))
if len(unique) == 1:
    source_pdb, source_key = unique[0][1], unique[0][0]
else:
    monomer_cands = []
    for k, p in unique:
        key_norm = k.lower()
        if "monomer" in key_norm or "ems" in key_norm:
            monomer_cands.append((k, p))
    if len(monomer_cands) != 1:
        raise SystemExit("QC JSON did not resolve to a unique E·M·S monomer reference: " + repr([(k, str(p)) for k, p in unique]) + " monomer_candidates=" + repr([(k, str(p)) for k, p in monomer_cands]))
    source_pdb, source_key = monomer_cands[0][1], monomer_cands[0][0]
with resolution.open("w") as fh:
    fh.write("schema_version: aizyme_v1_qc_pdb_resolution\n")
    fh.write("qc_json: %s\n" % qc_path)
    fh.write("candidates:\n")
    for k, p in unique:
        fh.write("  - key: %s\n    path: %s\n" % (k, p))
    fh.write("selected_key: %s\n" % source_key)
    fh.write("selection_basis: Stage-1 design source is a single MoxDAE monomer with FUD and Co bound; the E·M·S monomer reference is selected and the tetramer reference is rejected for design-source use (retained only as provenance).\n")
print("QC_PDB_RESOLVED key=%s path=%s" % (source_key, source_pdb))

fud_params = root / "data/01_pipeline_pilot/ligand_params/FUD.params"
if not fud_params.is_file():
    raise SystemExit(f"missing validated FUD.params: {fud_params}")

fud_o1 = list(root.glob("**/moxdae_ems_protomerA_o1.pdb"))
if len(fud_o1) != 1:
    raise SystemExit(f"expected exactly one O1-completed FUD structure, found {len(fud_o1)}")
fud_o1 = fud_o1[0]
complete_script = root / "workflow/scripts/stage1b_complete_fud.py"
if not complete_script.is_file():
    raise SystemExit(f"missing O1-completion provenance script: {complete_script}")

tetramer = root / "workflow/sym_prod/pool_7M/moxdae_input_tetramer_0001.pdb"
tetramer_provenance = "canonical"
if not tetramer.is_file():
    matches = list(root.glob("**/moxdae_input_tetramer_0001.pdb"))
    if len(matches) == 1:
        tetramer = matches[0]
        tetramer_provenance = "unique_moxdae_input_tetramer_0001: %s" % tetramer
    else:
        qc_tetra = [(k, p) for k, p in unique if "tetramer" in k.lower()]
        if len(qc_tetra) != 1:
            raise SystemExit("missing canonical tetramer (found %d moxdae_input_tetramer_0001.pdb anywhere) and no unique QC tetramer reference: qc_tetramer=%s" % (len(matches), repr([(k, str(p)) for k, p in qc_tetra])))
        tetramer = qc_tetra[0][1]
        tetramer_provenance = "qc_%s: %s" % (qc_tetra[0][0], tetramer)
print("TETRAMER_RESOLVED provenance=%s path=%s" % (tetramer_provenance, tetramer))

shutil.copy2(source_pdb, input_dir / "source_postrelax.pdb")
shutil.copy2(fud_params, input_dir / "FUD.params")
shutil.copy2(fud_o1, input_dir / "moxdae_ems_protomerA_o1.pdb")
shutil.copy2(complete_script, wf / "scripts/stage1b_complete_fud.py")
shutil.copy2(tetramer, input_dir / "moxdae_input_tetramer_0001.pdb")
(input_dir / "moxdae_postrelax_qc.json").write_text(json.dumps(qc, indent=2, sort_keys=True) + "\n")

AA = {"ALA":"A","ARG":"R","ASN":"N","ASP":"D","CYS":"C","GLN":"Q","GLU":"E","GLY":"G","HIS":"H","ILE":"I","LEU":"L","LYS":"K","MET":"M","PHE":"F","PRO":"P","SER":"S","THR":"T","TRP":"W","TYR":"Y","VAL":"V"}
def atom_name(line): return line[12:16].strip()
def resname(line): return line[17:20].strip()
def chain(line): return line[21]
def resseq(line): return int(line[22:26])
def element(line):
    e = line[76:78].strip().upper()
    return e or (atom_name(line)[0].upper() if atom_name(line) else "")
def xyz(line): return tuple(float(line[i:i+8]) for i in (30, 38, 46))
def dist(a, b): return math.sqrt(sum((x-y)**2 for x,y in zip(a,b)))
def rewrite(line, new_chain, new_resseq, new_resname, record="HETATM"):
    return (f"{record:<6}{line[6:11]}{line[11:16]}{line[16]}"
            f"{new_resname:>3} {new_chain}{new_resseq:4d}{line[26]}{line[27:]}")

lines = source_pdb.read_text().splitlines(True)
protein, fud, co, remarks = [], [], [], []
for line in lines:
    if line.startswith("REMARK"):
        remarks.append(line.rstrip("\n"))
    if not line.startswith(("ATOM  ", "HETATM")):
        continue
    rn = resname(line)
    if line.startswith("ATOM  ") and chain(line) == "A":
        protein.append(line.rstrip("\n"))
    elif rn == "FUD":
        fud.append(line.rstrip("\n"))
    elif rn in {"CO", "COB", "COBALT"} or element(line) == "CO":
        co.append(line.rstrip("\n"))
if not protein or not fud or not co:
    raise SystemExit(f"input composition failure: protein={len(protein)} FUD={len(fud)} Co={len(co)}")
if len(co) != 1:
    raise SystemExit(f"expected exactly one Co atom, found {len(co)}")
protein_res = []
for line in protein:
    key = (resseq(line), line[26])
    if key not in protein_res:
        protein_res.append(key)
if [n for n, ins in protein_res] != list(range(protein_res[0][0], protein_res[0][0] + len(protein_res))):
    raise SystemExit("protein numbering is not contiguous and insertion-free")
ca_lines = [line for line in protein if atom_name(line) == "CA"]
if len(ca_lines) != len(protein_res) or any(resname(line) not in AA for line in ca_lines):
    raise SystemExit("protein sequence extraction failed")
expected_tetrad = {146: "GLU", 179: "ASP", 205: "HIS", 240: "GLU"}
resnames_by_position = {resseq(line): resname(line) for line in ca_lines}
if any(resnames_by_position.get(position) != residue for position, residue in expected_tetrad.items()):
    raise SystemExit(f"catalytic tetrad identity mismatch: {expected_tetrad} vs {resnames_by_position}")
if any(line[16] not in {" ", "A"} for line in protein + fud + co):
    raise SystemExit("alternate-location atoms are not allowed in staged input")

fud_heavy = [line for line in fud if element(line) not in {"H", "D"}]
if len(fud_heavy) != 12:
    raise SystemExit(f"FUD heavy-atom count is {len(fud_heavy)}, expected 12")

out = ["REMARK 900 AI.zymes v1 staged from authoritative post-relax QC PDB", f"REMARK 900 SOURCE {source_pdb}"]
out.extend(remarks)
out.extend(rewrite(line, "A", resseq(line), resname(line), "ATOM") for line in protein)
out.append("TER")
out.extend(rewrite(line, "X", 1, "FUD", "HETATM") for line in fud)
out.extend(rewrite(line, "X", 2, "CO", "HETATM") for line in co)
out.append("END")
(input_dir / "moxdae.pdb").write_text("".join(out))

seq = "".join(AA[resname(line)] for line in protein if atom_name(line) == "CA")
(input_dir / "moxdae.fasta").write_text(f">moxdae_WT\n{seq}\n")

coords = {(resseq(line), atom_name(line)): xyz(line) for line in protein}
coords.update({(1, atom_name(line)): xyz(line) for line in fud})
co_xyz = xyz(co[0])
donors = {"E146_OE2": coords[(146, "OE2")], "D179_OD2": coords[(179, "OD2")], "H205_ND1": coords[(205, "ND1")], "E240_OE2": coords[(240, "OE2")]}
geometry = {
    "source_pdb": str(source_pdb), "source_sha256": sha(source_pdb), "qc_json_sha256": sha(qc_path),
    "protein_chain": "A", "ligand_chain": "X", "protein_length": len(seq), "fud_heavy_atoms": len(fud_heavy),
    "catalytic_tetrad": {"146": "E", "179": "D", "205": "H", "240": "E"},
    "co_donor_distances_angstrom": {name: dist(co_xyz, point) for name, point in donors.items()},
    "co_fud_distances_angstrom": {atom: dist(co_xyz, coords[(1, atom)]) for atom in ("O2", "O3")},
    "fud_coordinates": {atom_name(line): xyz(line) for line in fud},
    "remarks_preserved": len(remarks),
}
(manifest_dir / "wt_reference_geometry.json").write_text(json.dumps(geometry, indent=2, sort_keys=True) + "\n")

# Interface positions: prefer an authoritative record already present in the
# project tree (e.g. from the 7M tetramer work). If none resolves uniquely,
# derive chain-A positions within 5 A of chains B/C/D as a comparison set
# pending G4 human review. 5.0 A is a comparison distance, not an acceptance
# threshold, and the stage never blocks on a position count.
def parse_position_list(path):
    values = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        first = line.split()[0].split(",")[0]
        if first.isdigit():
            values.append(int(first))
    return sorted(set(values))

authoritative = []
for pattern in ("reports/**/*interface*", "workflow/**/*interface*", "data/**/*interface*", "memory/**/*interface*"):
    for path in root.glob(pattern):
        if not path.is_file() or path.suffix not in {".txt", ".csv", ".tsv"}:
            continue
        if str(path).startswith(str(wf)) or path == tetramer:
            continue
        try:
            values = parse_position_list(path)
        except OSError:
            continue
        if 10 <= len(values) <= 600 and all(1 <= v <= 400 for v in values):
            authoritative.append((str(path), values))
if len(authoritative) > 1:
    raise SystemExit("multiple candidate authoritative interface lists found; disambiguate and rerun: " + repr([p for p, _ in authoritative]))
if authoritative:
    interface = authoritative[0][1]
    interface_provenance = f"authoritative: {authoritative[0][0]} ({len(interface)} positions)"
else:
    tet_lines = [line for line in tetramer.read_text().splitlines() if line.startswith(("ATOM  ", "HETATM")) and element(line) not in {"H", "D"}]
    chain_a = [line for line in tet_lines if chain(line) == "A"]
    other = [line for line in tet_lines if chain(line) in {"B", "C", "D"}]
    interface = sorted({resseq(a) for a in chain_a if any(dist(xyz(a), xyz(b)) <= 5.0 for b in other)})
    interface_provenance = f"derived: 5.0A_vs_tetramer_chains_BCD ({len(interface)} positions), pending G4 review"
(config_dir / "interface_positions.txt").write_text("\n".join(map(str, interface)) + "\n")
(config_dir / "interface_provenance.txt").write_text(interface_provenance + "\n")

# Machine-readable source/staged manifest.
items = [
    ("qc_json", qc_path, input_dir / "moxdae_postrelax_qc.json", "copied without semantic editing"),
    ("postrelax_pdb", source_pdb, input_dir / "source_postrelax.pdb", "copied without semantic editing"),
    ("staged_moxdae_pdb", source_pdb, input_dir / "moxdae.pdb", "chain A/FUD/Co normalization"),
    ("fud_params", fud_params, input_dir / "FUD.params", "copied validated Stage-1b artifact"),
    ("o1_completed_structure", fud_o1, input_dir / "moxdae_ems_protomerA_o1.pdb", "copied provenance artifact"),
    ("o1_completion_script", complete_script, wf / "scripts/stage1b_complete_fud.py", "copied provenance script"),
    ("canonical_tetramer", tetramer, input_dir / "moxdae_input_tetramer_0001.pdb", "copied interface source"),
]
with (manifest_dir / "input_manifest.yaml").open("w") as fh:
    fh.write("schema_version: aizyme_v1_input_manifest\n")
    fh.write("source_authority: ssh:HPC:/data01/genbiolab/mdanh/data/simulation/daes_enzyme\n")
    fh.write("items:\n")
    for role, source, staged, transform in items:
        fh.write(f"  - role: {role}\n    source: {source}\n    staged: {staged}\n    source_sha256: {sha(source)}\n    staged_sha256: {sha(staged)}\n    transformation: {transform}\n")
    fh.write("gates:\n  G1A_provenance: PASS\n  G1B_geometry: PASS_PENDING_QC_FIELD_COMPARISON\n")

with (manifest_dir / "input_checksums.sha256").open("w") as fh:
    for path in sorted(wf.rglob("*")):
        if path.is_file() and path.name != "input_checksums.sha256":
            fh.write(f"{sha(path)}  {path.relative_to(wf)}\n")
PY

printf 'G0_PASS\n' > "$WF/manifests/G0_PASS"
printf 'G1_INPUT_BUNDLE_CREATED\n' > "$WF/manifests/G1_INPUT_BUNDLE_CREATED"
sha256sum $(find "$WF/manifests" -maxdepth 1 -type f ! -name 'stage0_stage1_manifest_checksums.sha256' -print) > "$WF/manifests/stage0_stage1_manifest_checksums.sha256"
mv -- "$WF_TMP" "$FINAL_WF"
printf 'STAGE0_1_COMPLETE\n'
