import assert from "node:assert/strict";
import test from "node:test";
import { classifyPendingReason, parseOwnedJobStatusRow, parseSlurmDiscovery } from "../lib/slurm-discovery.js";

const sample = `VERSION_BEGIN
slurm 25.05
VERSION_END
PARTITIONS_BEGIN
gpus*|up|infinite|1|gpu:4
PARTITIONS_END
NODES_BEGIN
gpu04|gpus|mixed|80|500000|gpu:4
NODES_END
QUEUE_BEGIN
123|gpus|analysis|PENDING|0:00|Resources
QUEUE_END
`;

test("Slurm discovery parser returns bounded verified sections", () => {
  const parsed = parseSlurmDiscovery(sample);
  assert.equal(parsed.version.status, "verified");
  assert.match(parsed.partitions.text, /gpus/u);
  assert.match(parsed.nodes.text, /gpu04/u);
  assert.match(parsed.queue.text, /Resources/u);
});

test("missing discovery sections are unavailable rather than invented", () => {
  const parsed = parseSlurmDiscovery("VERSION_BEGIN\nslurm\nVERSION_END\n");
  assert.equal(parsed.version.status, "verified");
  assert.equal(parsed.nodes.status, "unavailable");
});

test("pending reasons are tentative categories, never verified conclusions", () => {
  assert.deepEqual(classifyPendingReason("Resources"), { category: "resources", verified: false });
  assert.deepEqual(classifyPendingReason("Dependency"), { category: "dependency", verified: false });
  assert.deepEqual(classifyPendingReason("SiteSpecificThing"), { category: "unknown", verified: false });
});

test("owned job accounting rows require exact parent job id and fields", () => {
  assert.deepEqual(parseOwnedJobStatusRow("4242|analysis|COMPLETED|0:0|00:01:00", "4242"), { jobId: "4242", jobName: "analysis", state: "COMPLETED", exitCode: "0:0", elapsed: "00:01:00" });
  assert.equal(parseOwnedJobStatusRow("4242.batch|batch|COMPLETED|0:0|00:01:00", "4242"), null);
  assert.equal(parseOwnedJobStatusRow("4242|analysis|COMPLETED|0:0", "4242"), null);
});
