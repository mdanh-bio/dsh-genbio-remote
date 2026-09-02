// ── Content-addressed package inventory (Phase 2 foundation) ─────────────────
// Local-only, read-only: hashes every file of one parsed manifest under its
// local_root and digests the canonical per-file table into a single package
// digest. The digest is content-addressed and deterministic — it depends ONLY
// on (manifestSha, relative paths, sizes, SHA-256): file creation order,
// modification times, and the absolute local_root never change it. This is the
// building block for later content-addressed staging: a future stage can prove
// "these bytes are the validated package" by comparing package digests.
//
// Strict and fail-closed: any missing file, non-regular file, relative-path
// violation, or malformed manifest input throws. The returned record is frozen
// and JSON-safe, and it NEVER contains file contents, mtimes, or absolute
// local paths.
import { createHash } from "node:crypto";
import { readFile, stat } from "node:fs/promises";
import { join } from "node:path";
import { canonicalJson } from "./project.js";

// Matches the manifest file cap (project.js manifest file cap): a package
// can never inventory more files than its manifest may declare.
const MAX_INVENTORY_FILES = 64;

function sha256(data) { return createHash("sha256").update(data).digest("hex"); }
function plainObject(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

/**
 * Build the content-addressed inventory of one parsed manifest's file set.
 *
 * @param {object} manifest parsed manifest (parseProjectManifest / parseManifest
 *   shape): needs { project, localRoot, files }.
 * @param {string} manifestSha full SHA-256 hex of the exact manifest text.
 * @returns {Promise<object>} frozen inventory:
 *   { schema, project, manifestSha, files: [{ rel, size, sha256 }],
 *     totalBytes, packageSha }.
 */
export async function buildPackageInventory(manifest, manifestSha) {
  if (!plainObject(manifest) || typeof manifest.localRoot !== "string" || manifest.localRoot.length === 0) throw new Error("package inventory requires a parsed manifest with localRoot");
  if (typeof manifest.project !== "string" || manifest.project.length === 0) throw new Error("package inventory requires a parsed manifest with project");
  if (!/^[a-f0-9]{64}$/u.test(manifestSha ?? "")) throw new Error("package inventory requires the full 64-hex manifest SHA-256");
  const files = manifest.files;
  if (!Array.isArray(files) || files.length === 0 || files.length > MAX_INVENTORY_FILES) throw new Error(`package inventory requires 1..${MAX_INVENTORY_FILES} manifest files`);
  const entries = [];
  let totalBytes = 0;
  for (const rel of files) {
    if (typeof rel !== "string" || rel.length === 0 || rel.startsWith("/") || rel.includes("\0")) throw new Error(`package inventory file path must be a non-empty relative path: ${rel}`);
    // Same segment policy as the manifest parser: no ".", "..", or unsafe
    // characters — a public caller cannot inventory files outside localRoot.
    for (const segment of rel.split("/")) if (!/^[A-Za-z0-9_.-]+$/u.test(segment) || segment === "." || segment === "..") throw new Error(`package inventory contains an unsafe path segment: ${rel}`);
    const path = join(manifest.localRoot, rel);
    let info;
    try { info = await stat(path); } catch { throw new Error(`package file is missing: ${rel}`); }
    if (!info.isFile()) throw new Error(`package file is not a regular file: ${rel}`);
    const bytes = await readFile(path);
    entries.push({ rel, size: bytes.length, sha256: sha256(bytes) });
    totalBytes += bytes.length;
  }
  // Canonical order: sorted by relative path, so the digest is independent of
  // manifest file ordering.
  entries.sort((a, b) => (a.rel < b.rel ? -1 : a.rel > b.rel ? 1 : 0));
  const packageSha = sha256(canonicalJson({ manifestSha, files: entries }));
  return Object.freeze({
    schema: "genbio-package-inventory/1",
    project: manifest.project,
    manifestSha,
    files: Object.freeze(entries.map((entry) => Object.freeze(entry))),
    totalBytes,
    packageSha,
  });
}

export { MAX_INVENTORY_FILES };
