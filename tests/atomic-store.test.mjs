import assert from "node:assert/strict";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { atomicWriteJson, readBoundedJson, withDirectoryLock } from "../lib/atomic-store.js";

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), "atomic-store-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  return root;
}

test("atomic JSON writes round-trip and reject corrupt data", async (t) => {
  const root = await fixture(t); const file = join(root, "state.json");
  await atomicWriteJson(file, { schema: "test/1", value: 7 });
  assert.deepEqual(await readBoundedJson(file), { schema: "test/1", value: 7 });
  await writeFile(file, "{bad");
  await assert.rejects(readBoundedJson(file), /corrupt/u);
});

test("directory lock serializes ownership and never breaks stale locks", async (t) => {
  const root = await fixture(t);
  let release;
  const hold = new Promise((resolve) => { release = resolve; });
  const first = withDirectoryLock(root, { pid: 1 }, async () => hold);
  while (true) {
    try { await mkdir(join(root, ".probe")); await rm(join(root, ".probe"), { recursive: true }); } catch { /* wait */ }
    try { await withDirectoryLock(root, { pid: 2 }, async () => {}); assert.fail("second lock must fail"); } catch (error) { if (/locked/u.test(error.message)) break; }
  }
  release(); await first;
  await mkdir(join(root, ".lock"));
  await assert.rejects(withDirectoryLock(root, { pid: 3 }, async () => {}), /locked/u);
});
