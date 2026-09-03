import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { createExecutionRegistry } from "../lib/execution-registry.js";

const H = "a".repeat(64);
async function fixture(t) { const root = await mkdtemp(join(tmpdir(), "execution-registry-")); t.after(() => rm(root, { recursive: true, force: true })); return root; }
function reservation(overrides = {}) { return { sessionId: "session-one", workspace: "/workspace", project: "demo", operation: "run", planHash: H, manifestSha: H, packageSha: H, wrapperSha: H, policyHash: H, remoteBase: "/remote/demo", cpus: 2, gpus: 1, concurrency: 1, node: "gpu04", partition: "gpus", ...overrides }; }

test("durable reservation survives registry recreation and pair-gates", async (t) => {
  const root = await fixture(t); const a = createExecutionRegistry(root);
  const { record } = await a.reserve(reservation());
  const b = createExecutionRegistry(root);
  assert.equal((await b.find(record.runId)).allocationStatus, "submitting");
  await assert.rejects(b.reserve(reservation()), /durable active attempt/u);
  await assert.rejects(b.reserve(reservation({ policyHash: "b".repeat(64) })), /different policy hash/u);
});

test("job id and policy identity are immutable, and unknown fields fail closed", async (t) => {
  const root = await fixture(t); const registry = createExecutionRegistry(root); const { record } = await registry.reserve(reservation());
  const submitted = await registry.update(record.runId, (item) => ({ ...item, uniqueJobName: "demo.12345678", sbatchIssued: true, slurmJobId: "1234", workloadStatus: "submitted", allocationStatus: "nonterminal" }));
  assert.equal(submitted.slurmJobId, "1234");
  await assert.rejects(registry.update(record.runId, (item) => ({ ...item, slurmJobId: "9999" })), /write-once/u);
  await assert.rejects(registry.update(record.runId, (item) => ({ ...item, policyHash: "b".repeat(64) })), /immutable identity/u);
  const parsed = JSON.parse(await (await import("node:fs/promises")).readFile(registry.file, "utf8")); parsed.records[0].stdout = "secret"; await writeFile(registry.file, JSON.stringify(parsed));
  await assert.rejects(registry.list(), /unknown field stdout/u);
});

test("active allocations are visible across projects in one workspace", async (t) => {
  const root = await fixture(t); const registry = createExecutionRegistry(root);
  await registry.reserve(reservation());
  const second = await registry.reserve(reservation({ project: "other", operation: "analyze", gpus: 0 }));
  assert.equal(second.outstanding.length, 1);
});
