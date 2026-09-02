import { createHash, randomBytes } from "node:crypto";
import { constants } from "node:fs";
import { mkdir, open, realpath, rm } from "node:fs/promises";
import { dirname, join, relative, sep } from "node:path";
import { tmpdir } from "node:os";
import { canonicalJson } from "./project.js";

const MAX_PACKAGE_BYTES = 1024 * 1024 * 1024;

function sha256(data) { return createHash("sha256").update(data).digest("hex"); }
function inside(root, candidate) {
  const rel = relative(root, candidate);
  return rel === "" || (rel !== ".." && !rel.startsWith(`..${sep}`));
}

async function readNoFollow(path, expectedRoot, label) {
  let handle;
  try {
    handle = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW);
    const info = await handle.stat();
    if (!info.isFile()) throw new Error(`${label} is not a regular file`);
    if (info.size > MAX_PACKAGE_BYTES) throw new Error(`${label} exceeds ${MAX_PACKAGE_BYTES} bytes`);
    const actual = await realpath(path);
    if (!inside(expectedRoot, actual)) throw new Error(`${label} escapes its approved local root`);
    const bytes = await handle.readFile();
    const after = await handle.stat();
    if (after.dev !== info.dev || after.ino !== info.ino || after.size !== info.size || after.mtimeMs !== info.mtimeMs) throw new Error(`${label} changed while it was being read`);
    return { bytes, size: bytes.length, sha256: sha256(bytes), dev: info.dev, ino: info.ino, mode: info.mode & 0o777 };
  } catch (error) {
    if (error?.code === "ELOOP") throw new Error(`${label} must not be a symbolic link`);
    throw error;
  } finally {
    await handle?.close();
  }
}

export async function securePackageInventory(manifest, manifestSha) {
  const root = await realpath(manifest.localRoot);
  const entries = [];
  let totalBytes = 0;
  for (const rel of manifest.files) {
    const item = await readNoFollow(join(root, rel), root, `${manifest.project}: ${rel}`);
    entries.push({ rel, size: item.size, sha256: item.sha256, dev: item.dev, ino: item.ino, mode: item.mode });
    totalBytes += item.size;
    if (totalBytes > MAX_PACKAGE_BYTES) throw new Error(`${manifest.project}: package exceeds ${MAX_PACKAGE_BYTES} bytes`);
  }
  entries.sort((a, b) => (a.rel < b.rel ? -1 : a.rel > b.rel ? 1 : 0));
  const packageSha = sha256(canonicalJson({ manifestSha, files: entries.map(({ rel, size, sha256: hash }) => ({ rel, size, sha256: hash })) }));
  return Object.freeze({ schema: "genbio-package-inventory/2", project: manifest.project, manifestSha, files: Object.freeze(entries.map(Object.freeze)), totalBytes, packageSha, canonicalRoot: root });
}

export async function createPackageSnapshot(manifest, manifestSha, approvedPackageSha = null) {
  const inventory = await securePackageInventory(manifest, manifestSha);
  if (approvedPackageSha !== null && inventory.packageSha !== approvedPackageSha) throw new Error(`${manifest.project}: package digest drift: recomputed ${inventory.packageSha} != approved ${approvedPackageSha}`);
  const snapshotRoot = join(tmpdir(), `genbio-package-${manifest.project}-${randomBytes(12).toString("hex")}`);
  await mkdir(snapshotRoot, { recursive: false, mode: 0o700 });
  try {
    for (const entry of inventory.files) {
      const source = await readNoFollow(join(inventory.canonicalRoot, entry.rel), inventory.canonicalRoot, `${manifest.project}: ${entry.rel}`);
      if (source.sha256 !== entry.sha256 || source.size !== entry.size) throw new Error(`${manifest.project}: ${entry.rel} changed after package inventory`);
      const destination = join(snapshotRoot, entry.rel);
      await mkdir(dirname(destination), { recursive: true, mode: 0o700 });
      let handle;
      try {
        handle = await open(destination, "wx", entry.mode & 0o111 ? 0o700 : 0o600);
        await handle.writeFile(source.bytes);
        await handle.sync();
      } finally {
        await handle?.close();
      }
    }
    const snapshotManifest = Object.freeze({ ...manifest, localRoot: snapshotRoot });
    const verified = await securePackageInventory(snapshotManifest, manifestSha);
    if (verified.packageSha !== inventory.packageSha) throw new Error(`${manifest.project}: private package snapshot failed digest verification`);
    return Object.freeze({ root: snapshotRoot, manifest: snapshotManifest, inventory });
  } catch (error) {
    await rm(snapshotRoot, { recursive: true, force: true });
    throw error;
  }
}

export async function removePackageSnapshot(snapshot) {
  if (snapshot?.root) await rm(snapshot.root, { recursive: true, force: true });
}

export { MAX_PACKAGE_BYTES, readNoFollow };
