import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { createRunRegistry } from "../lib/run-registry.js";

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), "genbio-run-registry-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  return root;
}

test("restart load moves in-flight records to reconciling and preserves the pair lock", async (t) => {
  const root = await fixture(t);
  const first = createRunRegistry(root);
  await first.record("session-one", { runId: "run-one", source: "h100-direct", project: "demo", operation: "run", status: "in-flight", startedAt: 1 });

  const restarted = createRunRegistry(root);
  const records = await restarted.list("session-one");
  assert.equal(records[0].status, "reconciling");
  assert.match(records[0].note, /outcome unknown/u);
  await assert.rejects(restarted.record("session-one", { runId: "run-two", source: "h100-direct", project: "demo", operation: "run", status: "in-flight", startedAt: 2 }), /pair lock/u);
});

test("terminal evidence releases the durable pair lock", async (t) => {
  const root = await fixture(t);
  const registry = createRunRegistry(root);
  await registry.record("session-one", { runId: "run-one", source: "aizyme", project: "dae-enzyme", operation: "stage2", status: "in-flight", startedAt: 1 });
  await registry.update("session-one", "run-one", { status: "completed", note: "verified terminal evidence" });
  const next = await registry.record("session-one", { runId: "run-two", source: "aizyme", project: "dae-enzyme", operation: "stage2", status: "in-flight", startedAt: 2 });
  assert.equal(next.runId, "run-two");
});
