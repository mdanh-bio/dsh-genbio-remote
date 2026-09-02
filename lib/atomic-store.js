import { randomBytes } from "node:crypto";
import { mkdir, open, readFile, rename, rm, stat } from "node:fs/promises";
import { dirname, join } from "node:path";

const DEFAULT_MAX_BYTES = 1024 * 1024;

function plainObject(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

async function syncDirectory(path) {
  let handle;
  try {
    handle = await open(path, "r");
    await handle.sync();
  } finally {
    await handle?.close();
  }
}

export async function atomicWriteJson(file, value, { mode = 0o600 } = {}) {
  const dir = dirname(file);
  await mkdir(dir, { recursive: true, mode: 0o700 });
  const tmp = join(dir, `.${randomBytes(12).toString("hex")}.tmp`);
  let handle;
  try {
    handle = await open(tmp, "wx", mode);
    await handle.writeFile(`${JSON.stringify(value, null, 2)}\n`, "utf8");
    await handle.sync();
    await handle.close();
    handle = null;
    await rename(tmp, file);
    await syncDirectory(dir);
  } finally {
    await handle?.close();
    await rm(tmp, { force: true });
  }
}

export async function readBoundedJson(file, { maxBytes = DEFAULT_MAX_BYTES, missing = undefined } = {}) {
  let info;
  try {
    info = await stat(file);
  } catch (error) {
    if (error?.code === "ENOENT" && missing !== undefined) return structuredClone(missing);
    throw error;
  }
  if (!info.isFile()) throw new Error(`atomic store entry is not a regular file: ${file}`);
  if (info.size > maxBytes) throw new Error(`atomic store entry exceeds ${maxBytes} bytes: ${file}`);
  let parsed;
  try {
    parsed = JSON.parse(await readFile(file, "utf8"));
  } catch (error) {
    throw new Error(`atomic store entry is corrupt: ${file}: ${String(error?.message ?? error)}`);
  }
  return parsed;
}

export async function withDirectoryLock(root, owner, fn) {
  if (typeof root !== "string" || !root.startsWith("/")) throw new Error("atomic store lock root must be absolute");
  if (!plainObject(owner)) throw new Error("atomic store lock owner must be a mapping");
  await mkdir(root, { recursive: true, mode: 0o700 });
  const lockDir = join(root, ".lock");
  try {
    await mkdir(lockDir, { mode: 0o700 });
  } catch (error) {
    if (error?.code !== "EEXIST") throw error;
    let detail = "unknown owner";
    try {
      const record = await readBoundedJson(join(lockDir, "owner.json"), { maxBytes: 16384 });
      detail = JSON.stringify(record);
    } catch {
      detail = "owner record unavailable";
    }
    throw new Error(`atomic store is locked (${detail}); refusing concurrent or stale-lock recovery automatically`);
  }
  try {
    await atomicWriteJson(join(lockDir, "owner.json"), { ...owner, acquiredAt: Date.now() });
    return await fn();
  } finally {
    await rm(lockDir, { recursive: true, force: true });
    await syncDirectory(root);
  }
}

export { DEFAULT_MAX_BYTES, plainObject, syncDirectory };
