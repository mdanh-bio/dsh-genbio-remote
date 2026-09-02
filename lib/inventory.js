// Backward-compatible public inventory surface. The authoritative implementation
// uses no-follow file handles and canonical-root containment in secure-package.js.
import { securePackageInventory } from "./secure-package.js";

const MAX_INVENTORY_FILES = 64;

export async function buildPackageInventory(manifest, manifestSha) {
  if (!manifest || typeof manifest !== "object" || Array.isArray(manifest) || typeof manifest.localRoot !== "string" || manifest.localRoot.length === 0) throw new Error("package inventory requires a parsed manifest with localRoot");
  if (typeof manifest.project !== "string" || manifest.project.length === 0) throw new Error("package inventory requires a parsed manifest with project");
  if (!/^[a-f0-9]{64}$/u.test(manifestSha ?? "")) throw new Error("package inventory requires the full 64-hex manifest SHA-256");
  if (!Array.isArray(manifest.files) || manifest.files.length === 0 || manifest.files.length > MAX_INVENTORY_FILES) throw new Error(`package inventory requires 1..${MAX_INVENTORY_FILES} manifest files`);
  for (const rel of manifest.files) {
    if (typeof rel !== "string" || rel.length === 0 || rel.startsWith("/") || rel.includes("\0")) throw new Error(`package inventory file path must be a non-empty relative path: ${rel}`);
    for (const segment of rel.split("/")) if (!/^[A-Za-z0-9_.-]+$/u.test(segment) || segment === "." || segment === "..") throw new Error(`package inventory contains an unsafe path segment: ${rel}`);
  }
  try {
    const inventory = await securePackageInventory(manifest, manifestSha);
    return Object.freeze({ schema: inventory.schema, project: inventory.project, manifestSha: inventory.manifestSha, files: Object.freeze(inventory.files.map(({ rel, size, sha256 }) => Object.freeze({ rel, size, sha256 }))), totalBytes: inventory.totalBytes, packageSha: inventory.packageSha });
  } catch (error) {
    const message = String(error?.message ?? error);
    if (/ENOENT|no such file/u.test(message)) throw new Error(`package file is missing: ${message}`);
    throw error;
  }
}

export { MAX_INVENTORY_FILES };
