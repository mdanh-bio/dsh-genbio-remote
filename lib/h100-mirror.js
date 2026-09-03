// ── W0: manifest-driven HPC → genbioh100 CURATED MIRROR (plan/execute/status) ─
// Generalizes the deployed dae-mirror safety pattern (lib/dae-mirror.js, DAE-
// specific) to a user-owned manifest that names the authoritative HPC source
// root and the fixed genbioh100 destination. The safety invariants are
// unchanged:
//   - plan: read-only; fresh HPC checksum inventory + genbioh100 dest
//     capacity check -> one IMMUTABLE session-owned plan hash (policy-hash
//     bound). No transfer or allocation.
//   - execute: fresh re-inventory + manifest-drift + source-drift checks
//     (fail closed if anything moved since plan), explicit material-transfer
//     approval, rclone-only copy (never scp, never sync/delete/move) into a
//     FRESH temporary destination, full per-file SHA-256 verification on the
//     destination, provenance receipt, then an ATOMIC promotion (mv) of the
//     verified temp to the destination. The destination must not pre-exist.
//   - status: read-only receipt + destination check.
// The source (HPC) remains the scientific evidence authority; the destination
// is a curated execution/analysis mirror, not a bidirectional sync target.

import { createHash, randomBytes } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { load as parseYaml } from "js-yaml";
import { RCLONE_TRANSFER_ARGS, resolveRcloneRemote } from "./transfer.js";

const HOME_DIR = typeof process.env.HOME === "string" && process.env.HOME.startsWith("/") ? process.env.HOME : "/Users/mdanh";
const DEFAULT_MIRROR_MANIFEST = join(HOME_DIR, ".dsh/profiles/desktop/genbio-h100-mirror/mirror-manifest.yaml");
const SSH_OPTIONS = ["-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "StrictHostKeyChecking=yes"];
const SAFE_REL_RE = /^(?!\/)(?!.*(?:^|\/)\.\.(?:\/|$))[A-Za-z0-9_.+@,=\/-]+$/u;
const SENSITIVE_RE = /(?:^|\/)(?:\.ssh|\.aws|\.gnupg|id_rsa|id_ed25519|authorized_keys|rclone\.conf|.*(?:password|passwd|credential|secret|token|private[_-]?key).*)(?:\/|$)/iu;
const plans = new Map();

function sha256(data) { return createHash("sha256").update(data).digest("hex"); }
function shellQuote(value) { const text = String(value); if (!/^[A-Za-z0-9_./:=+@,-]+$/u.test(text)) throw new Error(`unsafe fixed path/token: ${text}`); return `'${text}'`; }
function strictRemote(target, body) { return `ssh ${SSH_OPTIONS.join(" ")} -- ${target} ${JSON.stringify(body).replace(/\$/g, "\\$")}`; }
function sessionId(exec) { const value = exec?.agent?.session?.id; if (!value) throw new Error("h100 mirror operation requires an owning session"); return value; }
function runKey(exec, hash) { return `${sessionId(exec)}:${hash}`; }
async function runLocal(shell, command, timeoutMs, signal) { const result = await shell.run(shell.resolve({ command, timeoutMs, signal })); return { stdout: result.stdout?.text ?? result.stdout ?? "", stderr: result.stderr?.text ?? result.stderr ?? "", exitCode: result.exitCode ?? null, signal: result.signal ?? null, timedOut: result.timedOut === true }; }

function validateRel(value, label) {
  const rel = String(value ?? "");
  if (!SAFE_REL_RE.test(rel) || rel.includes("//") || SENSITIVE_RE.test(rel)) throw new Error(`${label} is unsafe or sensitive: ${rel}`);
  return rel;
}

async function loadMirrorManifest(manifestPath) {
  const raw = await readFile(manifestPath, "utf8");
  const doc = parseYaml(raw);
  if (!doc || doc.schema_version !== 1 || doc.mode !== "curated") throw new Error("h100 mirror manifest must be curated schema_version 1");
  if (typeof doc.project !== "string" || !/^[a-z0-9][a-z0-9-]*$/u.test(doc.project)) throw new Error("h100 mirror manifest project must be kebab-case");
  if (doc.source?.target !== "HPC" || typeof doc.source?.root !== "string" || !doc.source.root.startsWith("/") || doc.source.root.includes("..")) throw new Error("h100 mirror source must be an absolute HPC root without ..");
  if (doc.destination?.target !== "genbioh100" || typeof doc.destination?.root !== "string" || !doc.destination.root.startsWith("/") || doc.destination.root.includes("..")) throw new Error("h100 mirror destination must be an absolute genbioh100 root without ..");
  const includeRoots = (doc.include_roots ?? []).map((value, i) => validateRel(value, `include_roots[${i}]`));
  if (includeRoots.length === 0 || includeRoots.length > 64) throw new Error("h100 mirror include_roots must contain 1..64 paths");
  const excludeGlobs = (doc.exclude_globs ?? []).map((value, i) => {
    const text = String(value ?? "");
    if (!text || text.includes("\0") || text.includes("..") || /[;'"`$(){}\[\]<>|&!]/u.test(text)) throw new Error(`exclude_globs[${i}] is unsafe`);
    return text;
  });
  const anchors = (doc.critical_anchors ?? []).map((item, i) => {
    const path = validateRel(item?.path, `critical_anchors[${i}].path`);
    const digest = String(item?.sha256 ?? "").toLowerCase();
    if (!/^[a-f0-9]{64}$/u.test(digest)) throw new Error(`critical_anchors[${i}].sha256 is invalid`);
    return { path, sha256: digest };
  });
  return { raw, digest: sha256(raw), project: doc.project, sourceRoot: doc.source.root, destinationRoot: doc.destination.root, includeRoots, excludeGlobs, anchors, doc };
}

function inventoryBody(manifest) {
  const roots = manifest.includeRoots.map(shellQuote).join(" ");
  const excludeCases = manifest.excludeGlobs.map((pattern) => `${pattern}) continue ;;`).join(" ");
  return `set -euo pipefail; export PATH=/usr/bin:/bin; root=${shellQuote(manifest.sourceRoot)}; test -d "$root"; cd -- "$root"; for item in ${roots}; do test -e "$item" || { printf 'MISSING\\t%s\\n' "$item"; continue; }; test ! -L "$item" || { printf 'SYMLINK\\t%s\\n' "$item"; continue; }; if test -f "$item"; then printf '%s\\n' "$item"; else find -P "$item" -type l -printf 'SYMLINK\\t%p\\n'; find -P "$item" -type f -print; fi; done | LC_ALL=C sort -u | while IFS= read -r rel; do case "$rel" in SYMLINK*|MISSING*) printf '%s\\n' "$rel"; continue ;; ${excludeCases} esac; size=$(stat -c %s -- "$rel"); mtime=$(stat -c %Y -- "$rel"); sum=$(/usr/bin/openssl dgst -sha256 -- "$rel" | awk '{print $NF}'); printf 'FILE\\t%s\\t%s\\t%s\\t%s\\n' "$size" "$mtime" "$sum" "$rel"; done`;
}

function parseInventory(stdout, manifest) {
  const files = []; const blockers = [];
  for (const line of String(stdout).split(/\r?\n/u)) {
    if (!line) continue;
    const fields = line.split("\t");
    if (/^[a-f0-9]{32,64}(?:\t|\s)/u.test(line)) continue;
    if (fields[0] !== "FILE") { blockers.push(line); continue; }
    if (fields.length !== 5) { blockers.push(`MALFORMED:${line}`); continue; }
    const size = Number(fields[1]); const mtime = Number(fields[2]); const digest = fields[3]; const rel = validateRel(fields[4], "inventory path");
    if (!Number.isSafeInteger(size) || size < 0 || !Number.isSafeInteger(mtime) || !/^[a-f0-9]{64}$/u.test(digest)) blockers.push(`MALFORMED:${line}`); else files.push({ rel, size, mtime, sha256: digest });
  }
  files.sort((a, b) => a.rel.localeCompare(b.rel));
  if (blockers.length) throw new Error(`h100 mirror inventory blockers: ${blockers.slice(0, 20).join("; ")}`);
  if (files.length === 0) throw new Error("h100 mirror selection resolved to zero files");
  for (const anchor of manifest.anchors) { const found = files.find((item) => item.rel === anchor.path); if (!found) throw new Error(`critical anchor missing from selection: ${anchor.path}`); if (found.sha256 !== anchor.sha256) throw new Error(`critical anchor checksum mismatch on HPC: ${anchor.path}`); }
  return files;
}

function destinationInspectBody(destinationRoot) { const parent = dirname(destinationRoot); return `set -euo pipefail; parent=${shellQuote(parent)}; test -d "$parent"; printf 'HOST=%s\\n' "$(hostname -f)"; df -Pk "$parent" | tail -1 | awk '{printf "AVAIL_KB=%s\\n", $4}'; if test -e ${shellQuote(destinationRoot)}; then printf 'DEST_EXISTS=1\\n'; find ${shellQuote(destinationRoot)} -mindepth 1 -maxdepth 1 -print -quit | grep -q . && printf 'DEST_NONEMPTY=1\\n' || printf 'DEST_NONEMPTY=0\\n'; else printf 'DEST_EXISTS=0\\nDEST_NONEMPTY=0\\n'; fi`; }
function parseDestination(stdout) { const availKb = Number(String(stdout).match(/^AVAIL_KB=(\d+)$/mu)?.[1]); return { availBytes: Number.isSafeInteger(availKb) ? availKb * 1024 : null, exists: /^DEST_EXISTS=1$/mu.test(stdout), nonempty: /^DEST_NONEMPTY=1$/mu.test(stdout) }; }

async function askTransfer(userQuestions, exec, plan) {
  if (!userQuestions) throw new Error("h100 mirror transfer requires the DSH user-question provider");
  const id = `genbio-h100-mirror-${plan.hash.slice(0, 12)}`;
  const answer = await userQuestions.ask({ agent: exec.agent, signal: exec.signal, questions: [{ id, header: "h100 curated mirror", question: `Transfer ${plan.files.length} curated file(s), ${plan.totalBytes} bytes, from HPC:${plan.sourceRoot} to genbioh100:${plan.destinationRoot}?`, options: [{ label: "Approve this transfer", description: "Use the frozen plan and rclone-only copy into a fresh verified temporary directory." }, { label: "Reject", description: "Do not create or transfer the mirror." }] }] });
  const selected = answer.answers?.find((item) => item.id === id)?.selected ?? [];
  if (!selected.includes("Approve this transfer")) throw new Error("h100 mirror material transfer was not explicitly approved");
}

function mirrorReadme(plan) { return `# ${plan.project} curated genbioh100 mirror\n\n- Authoritative source: \`HPC:${plan.sourceRoot}\`\n- Working mirror: \`genbioh100:${plan.destinationRoot}\`\n- Plan SHA-256: \`${plan.hash}\`\n- Manifest SHA-256: \`${plan.manifestDigest}\`\n- Selected files: ${plan.files.length}\n- Selected bytes: ${plan.totalBytes}\n\nHPC remains the scientific evidence authority. This directory is a curated execution and analysis mirror, not an automatic bidirectional synchronization target. Large regenerable pools, environments, caches, trajectories, and checkpoints are excluded by the checked-in selection manifest. Refresh only through a new manifest-driven, checksum-verified plan.\n`; }

export function createH100MirrorTools({ makeTool, requirePolicy, requireState, publicState, runRemote, shell, userQuestions, config, requireRemoteAccess }) {
  const manifestPath = typeof config?.h100MirrorManifestPath === "string" && config.h100MirrorManifestPath.length > 0 ? config.h100MirrorManifestPath : DEFAULT_MIRROR_MANIFEST;

  const planTool = makeTool("genbio_h100_mirror_plan", "Resolve the user-owned curated h100 mirror manifest against fresh HPC checksums and return an immutable session-owned plan hash. Read-only; no transfer or allocation.", {}, async (_args, exec) => {
    const state = requireState(exec); requirePolicy();
    await requireRemoteAccess("HPC", [{ root: (await loadMirrorManifest(manifestPath)).sourceRoot, write: false }], exec, state);
    const manifest = await loadMirrorManifest(manifestPath);
    await requireRemoteAccess("genbioh100", [{ root: dirname(manifest.destinationRoot), write: false }], exec, state);
    const source = await runRemote("HPC", strictRemote("HPC", inventoryBody(manifest)), exec, 30 * 60 * 1000);
    if (source.exitCode !== 0) throw new Error(`h100 mirror source inventory failed: ${source.stderr || source.stdout || source.exitCode}`);
    const files = parseInventory(source.stdout, manifest);
    const destination = await runRemote("genbioh100", strictRemote("genbioh100", destinationInspectBody(manifest.destinationRoot)), exec, Number(config.commandTimeoutMs ?? 30000));
    if (destination.exitCode !== 0) throw new Error(`h100 mirror destination inspection failed: ${destination.stderr || destination.stdout}`);
    const dest = parseDestination(destination.stdout);
    if (dest.exists) throw new Error(`h100 mirror destination already exists: ${manifest.destinationRoot}`);
    const totalBytes = files.reduce((sum, item) => sum + item.size, 0);
    if (dest.availBytes === null || dest.availBytes < Math.ceil(totalBytes * 1.25) + 64 * 1024 * 1024) throw new Error(`insufficient genbioh100 free space: selected=${totalBytes}, available=${dest.availBytes}`);
    const canonical = JSON.stringify({ schemaVersion: 1, policyHash: state.policy?.hash ?? null, manifestDigest: manifest.digest, sourceRoot: manifest.sourceRoot, destinationRoot: manifest.destinationRoot, files });
    const hash = sha256(canonical);
    const plan = { hash, policyHash: state.policy?.hash ?? null, manifestDigest: manifest.digest, project: manifest.project, sourceRoot: manifest.sourceRoot, destinationRoot: manifest.destinationRoot, files, totalBytes, createdAt: new Date().toISOString() };
    plans.set(runKey(exec, hash), plan);
    return { ok: true, status: { ...publicState(state), mirrorPlan: { plan_hash: hash, manifest_sha256: manifest.digest, project: manifest.project, source: manifest.sourceRoot, destination: manifest.destinationRoot, file_count: files.length, total_bytes: totalBytes, available_bytes: dest.availBytes, largest: [...files].sort((a, b) => b.size - a.size).slice(0, 20) } } };
  });

  const executeTool = makeTool("genbio_h100_mirror_execute", "Execute one immutable curated h100 HPC-to-genbioh100 mirror plan using rclone only, verify every SHA-256, write provenance records, and atomically promote the fresh destination.", { plan_hash: { type: "string", required: true } }, async (args, exec) => {
    const state = requireState(exec); requirePolicy();
    const hash = String(args.plan_hash).toLowerCase();
    if (!/^[a-f0-9]{64}$/u.test(hash)) throw new Error("invalid h100 mirror plan hash");
    const plan = plans.get(runKey(exec, hash));
    if (!plan) throw new Error("unknown or non-session-owned h100 mirror plan hash");
    if (plan.policyHash !== (state.policy?.hash ?? null)) throw new Error("h100 mirror plan policy hash is stale; create a fresh plan");
    await requireRemoteAccess("HPC", [{ root: plan.sourceRoot, write: false }], exec, state);
    await requireRemoteAccess("genbioh100", [{ root: dirname(plan.destinationRoot), write: true }], exec, state);
    const manifest = await loadMirrorManifest(manifestPath);
    if (manifest.digest !== plan.manifestDigest) throw new Error("h100 mirror manifest drifted after planning");
    if (manifest.sourceRoot !== plan.sourceRoot || manifest.destinationRoot !== plan.destinationRoot) throw new Error("h100 mirror manifest roots drifted after planning");
    const freshSource = await runRemote("HPC", strictRemote("HPC", inventoryBody(manifest)), exec, 30 * 60 * 1000);
    if (freshSource.exitCode !== 0) throw new Error(`fresh h100 source inventory failed: ${freshSource.stderr || freshSource.stdout || freshSource.exitCode}`);
    const freshFiles = parseInventory(freshSource.stdout, manifest);
    if (sha256(JSON.stringify(freshFiles)) !== sha256(JSON.stringify(plan.files))) throw new Error("h100 mirror source drifted after planning; create a fresh plan");
    const destInspect = await runRemote("genbioh100", strictRemote("genbioh100", destinationInspectBody(plan.destinationRoot)), exec, Number(config.commandTimeoutMs ?? 30000));
    if (destInspect.exitCode !== 0) throw new Error("failed to re-inspect genbioh100 destination");
    const dest = parseDestination(destInspect.stdout);
    if (dest.exists) throw new Error(`h100 mirror destination already exists: ${plan.destinationRoot}`);
    if (dest.availBytes === null || dest.availBytes < Math.ceil(plan.totalBytes * 1.25) + 64 * 1024 * 1024) throw new Error("genbioh100 free space no longer satisfies the mirror plan");
    await askTransfer(userQuestions, exec, plan);
    const token = randomBytes(16).toString("hex");
    const tempRoot = `${plan.destinationRoot}.incoming-${plan.hash.slice(0, 12)}-${token}`;
    const localTmp = await mkdtemp(join(tmpdir(), "dsh-h100-mirror-"));
    try {
      const listPath = join(localTmp, "files-from.txt"); const checksumPath = join(localTmp, "source_inventory.sha256"); const readmePath = join(localTmp, "MIRROR_README.md"); const receiptPath = join(localTmp, "TRANSFER_RECEIPT.json");
      await writeFile(listPath, plan.files.map((item) => item.rel).join("\n") + "\n", "utf8");
      await writeFile(checksumPath, plan.files.map((item) => `${item.sha256}  ${item.rel}`).join("\n") + "\n", "utf8");
      await writeFile(readmePath, mirrorReadme(plan), "utf8");
      const hpcRemote = resolveRcloneRemote(config, "HPC"); const h100Remote = resolveRcloneRemote(config, "genbioh100");
      const copyCommand = `rclone copy ${RCLONE_TRANSFER_ARGS.join(" ")} --checkers 8 --transfers 4 --files-from ${shellQuote(listPath)} ${shellQuote(`${hpcRemote}:${plan.sourceRoot}`)} ${shellQuote(`${h100Remote}:${tempRoot}`)}`;
      if (/\b(?:sync|delete|purge|moveto|move)\b/u.test(copyCommand)) throw new Error("unsafe destructive rclone operation rejected");
      const copied = await runLocal(shell, copyCommand, 24 * 60 * 60 * 1000, exec.signal);
      if (copied.exitCode !== 0) throw new Error(`h100 mirror rclone copy failed; resumable temp retained at ${tempRoot}: ${copied.stderr || copied.stdout || copied.exitCode}`);
      for (const [local, remote] of [[checksumPath, `${tempRoot}/manifests/mirror/source_inventory.sha256`], [manifestPath, `${tempRoot}/manifests/mirror/selection_manifest.yaml`], [readmePath, `${tempRoot}/MIRROR_README.md`]]) { const rc = await runLocal(shell, `rclone copyto ${RCLONE_TRANSFER_ARGS.join(" ")} ${shellQuote(local)} ${shellQuote(`${h100Remote}:${remote}`)}`, 120000, exec.signal); if (rc.exitCode !== 0) throw new Error(`h100 mirror metadata transfer failed; temp retained at ${tempRoot}: ${rc.stderr || rc.stdout || rc.exitCode}`); }
      const verified = await runRemote("genbioh100", strictRemote("genbioh100", `set -euo pipefail; root=${shellQuote(tempRoot)}; test -d "$root"; cd -- "$root"; while read -r expected rel; do rel="$(printf '%s' "$rel" | sed 's/^  //')"; actual=$(/usr/bin/openssl dgst -sha256 -- "$rel" | awk '{print $NF}'); test "$actual" = "$expected"; done < manifests/mirror/source_inventory.sha256; count=$(wc -l < manifests/mirror/source_inventory.sha256); printf 'VERIFIED_COUNT=%s\\n' "$count"`), exec, 30 * 60 * 1000);
      if (verified.exitCode !== 0) throw new Error(`h100 mirror checksum verification failed; temp retained at ${tempRoot}: ${verified.stderr || verified.stdout || verified.exitCode}`);
      const receipt = { schema_version: 1, project: plan.project, status: "verified", created_at: new Date().toISOString(), policy_sha256: plan.policyHash, plan_sha256: plan.hash, manifest_sha256: plan.manifestDigest, source: `HPC:${plan.sourceRoot}`, destination: `genbioh100:${plan.destinationRoot}`, temporary_destination: tempRoot, transfer: "rclone-copy", file_count: plan.files.length, total_bytes: plan.totalBytes, verification: "sha256sum-all-files", exclusions: manifest.doc.exclude_globs };
      await writeFile(receiptPath, JSON.stringify(receipt, null, 2) + "\n", "utf8");
      const receiptRc = await runLocal(shell, `rclone copyto ${RCLONE_TRANSFER_ARGS.join(" ")} ${shellQuote(receiptPath)} ${shellQuote(`${h100Remote}:${tempRoot}/manifests/mirror/TRANSFER_RECEIPT.json`)}`, 120000, exec.signal);
      if (receiptRc.exitCode !== 0) throw new Error(`h100 mirror receipt transfer failed; temp retained at ${tempRoot}`);
      const promoted = await runRemote("genbioh100", strictRemote("genbioh100", `set -euo pipefail; test -d ${shellQuote(tempRoot)}; test ! -e ${shellQuote(plan.destinationRoot)}; mv -- ${shellQuote(tempRoot)} ${shellQuote(plan.destinationRoot)}; test -f ${shellQuote(`${plan.destinationRoot}/manifests/mirror/TRANSFER_RECEIPT.json`)}; printf 'H100_MIRROR_PROMOTED=%s\\n' ${shellQuote(plan.destinationRoot)}`), exec, Number(config.commandTimeoutMs ?? 30000));
      if (promoted.exitCode !== 0) throw new Error(`h100 mirror atomic promotion failed; verified temp retained at ${tempRoot}: ${promoted.stderr || promoted.stdout || promoted.exitCode}`);
      plans.delete(runKey(exec, hash));
      return { ok: true, status: { ...publicState(state), mirrorExecution: { plan_hash: hash, project: plan.project, destination: plan.destinationRoot, file_count: plan.files.length, total_bytes: plan.totalBytes, verification: verified, promotion: promoted, receipt: `${plan.destinationRoot}/manifests/mirror/TRANSFER_RECEIPT.json` } } };
    } finally { await rm(localTmp, { recursive: true, force: true }).catch(() => {}); }
  });

  const statusTool = makeTool("genbio_h100_mirror_status", "Read-only status and receipt check for the configured curated h100 mirror on genbioh100.", {}, async (_args, exec) => {
    const state = requireState(exec); requirePolicy();
    const manifest = await loadMirrorManifest(manifestPath);
    await requireRemoteAccess("genbioh100", [{ root: dirname(manifest.destinationRoot), write: false }], exec, state);
    const destRoot = manifest.destinationRoot;
    const body = `set -euo pipefail; dest=${shellQuote(destRoot)}; if test -d "$dest"; then printf 'DEST_EXISTS=1\\n'; test -f "$dest/manifests/mirror/TRANSFER_RECEIPT.json" && { sha256sum "$dest/manifests/mirror/TRANSFER_RECEIPT.json"; sed -n '1,120p' "$dest/manifests/mirror/TRANSFER_RECEIPT.json"; } || printf 'RECEIPT_MISSING=1\\n'; else printf 'DEST_EXISTS=0\\n'; fi; parent=${shellQuote(dirname(destRoot))}; find "$parent" -maxdepth 1 -type d -name '*.incoming-*' -printf 'TEMP=%p\\n' 2>/dev/null || true`;
    const result = await runRemote("genbioh100", strictRemote("genbioh100", body), exec, Number(config.commandTimeoutMs ?? 30000));
    return { ok: result.exitCode === 0, status: { ...publicState(state), mirrorStatus: { project: manifest.project, destination: destRoot, result } } };
  });

  return { planTool, executeTool, statusTool };
}

export { DEFAULT_MIRROR_MANIFEST, loadMirrorManifest, parseInventory, inventoryBody };
