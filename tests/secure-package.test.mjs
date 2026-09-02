import assert from "node:assert/strict";
import { mkdtemp, mkdir, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { createPackageSnapshot, removePackageSnapshot, securePackageInventory } from "../lib/secure-package.js";

const H = "a".repeat(64);
async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), "secure-package-"));
  await mkdir(join(root, "scripts"));
  await writeFile(join(root, "scripts/run.sh"), "#!/bin/bash\nprintf ok\\n\n");
  await writeFile(join(root, "input.dat"), "input\n");
  t.after(() => rm(root, { recursive: true, force: true }));
  return { root, manifest: { project: "demo", localRoot: root, files: ["scripts/run.sh", "input.dat"] } };
}

test("secure package inventory is deterministic and content-bound", async (t) => {
  const fx = await fixture(t);
  const first = await securePackageInventory(fx.manifest, H);
  const second = await securePackageInventory(fx.manifest, H);
  assert.equal(first.packageSha, second.packageSha);
  assert.equal(first.files.length, 2);
  await writeFile(join(fx.root, "input.dat"), "changed\n");
  const changed = await securePackageInventory(fx.manifest, H);
  assert.notEqual(changed.packageSha, first.packageSha);
});

test("secure package inventory rejects symbolic-link files", async (t) => {
  const fx = await fixture(t);
  await rm(join(fx.root, "input.dat"));
  await symlink("/etc/hosts", join(fx.root, "input.dat"));
  await assert.rejects(securePackageInventory(fx.manifest, H), /symbolic link|escapes/u);
});

test("private snapshot preserves approved bytes and rejects digest drift", async (t) => {
  const fx = await fixture(t);
  const inventory = await securePackageInventory(fx.manifest, H);
  const snapshot = await createPackageSnapshot(fx.manifest, H, inventory.packageSha);
  try {
    assert.notEqual(snapshot.manifest.localRoot, fx.root);
    const verified = await securePackageInventory(snapshot.manifest, H);
    assert.equal(verified.packageSha, inventory.packageSha);
  } finally {
    await removePackageSnapshot(snapshot);
  }
  await writeFile(join(fx.root, "input.dat"), "drift\n");
  await assert.rejects(createPackageSnapshot(fx.manifest, H, inventory.packageSha), /package digest drift/u);
});
