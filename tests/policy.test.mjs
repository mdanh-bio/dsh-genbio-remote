import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import { load as parseYaml } from "js-yaml";

const root = path.resolve(import.meta.dirname, "..");
const policyPath = path.join(root, "fixtures/genbio-compute-policy.test.yaml");
const host = fs.readFileSync(path.join(root, "lib/index.js"), "utf8");
const client = fs.readFileSync(path.join(root, "lib/client.js"), "utf8");
const adapter = fs.readFileSync(path.join(root, "lib/openviking.js"), "utf8");
const patch = fs.readFileSync(path.join(root, "cordis.patch.yml"), "utf8");
const policy = parseYaml(fs.readFileSync(policyPath, "utf8"));

test("policy target and ordered live-test invariants", () => {
  assert.deepEqual(Object.keys(policy.targets).sort(), ["HPC", "NHPC", "genbio_mdanh", "genbioh100"].sort());
  assert.equal(policy.targets.HPC.test_gate.real_submission, "gpu04");
  assert.equal(policy.targets.NHPC.test_gate.real_submission, "gpu01");
  assert.equal(policy.targets.NHPC.allowlist.gpu01.partition, "gpu");
  assert.deepEqual(policy.targets.genbioh100.limits.gpus_allowed, [0]);
  assert.equal(policy.targets.genbioh100.limits.cpu_threads_per_job, 16);
  assert.equal(policy.targets.genbioh100.hardware.reserved_gpu, 1);
});

test("host code pins strict native ssh options", () => {
  for (const text of ["-T", "BatchMode=yes", "ConnectTimeout=10", "StrictHostKeyChecking=yes"]) assert.match(host, new RegExp(text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")));
  for (const forbidden of ["StrictHostKeyChecking=no", "accept-new", "UserKnownHostsFile=/dev/null", "Paramiko"]) assert.equal(host.includes(forbidden), false);
});

test("resource expansion requires explicit selection", () => {
  assert.match(host, /resource expansion was not explicitly approved/);
  assert.match(host, /userQuestions\.ask/);
});

test("launch surface is template constrained and job tracked", () => {
  assert.match(host, /enum: \["preflight-smoke", "gpu04-smoke"\]/);
  assert.match(host, /jobs\.start/);
  assert.match(host, /readOutput/);
  assert.match(host, /gpu04-smoke/);
  assert.equal(/command: \{ type: "string", required: true \}/.test(host), false);
});

test("audit regressions stay fixed", () => {
  assert.match(host, /config\.smokeTimeoutMs \?\? 180000/);
  assert.match(patch, /smokeTimeoutMs: 180000/);
  assert.match(host, /max_concurrent_gpu_jobs/);
  assert.match(host, /status: "killed", detail: "cancelled"/);
  assert.match(host, /command\.includes\("gpu_util"\)\)\) throw/);
  assert.equal(host.includes("globalThis.harness?.handle"), false);
  assert.equal(host.includes('ctx.get("sessionProjections")'), false);
  assert.equal(host.includes('join("\\\\n")'), false);
  assert.match(host, /state\.runs\.length > 50/);
});

test("curated memory handoff stays optional and isolated", () => {
  assert.match(host, /ctx\.provide\("genbioRemote"/);
  assert.match(host, /genbio_finalize_run/);
  assert.match(host, /genbio_publish_run/);
  assert.match(host, /memoryMode/);
  assert.match(adapter, /openvikingMemory\.publishExternalRecord/);
  assert.match(patch, /id: openviking-memory/);
  assert.match(patch, /dsh-genbio-remote\/openviking/);
});

test("client UI advertises manifest-driven node and genbioh100 restrictions", () => {
  assert.match(client, /manifest-driven node/);
  assert.match(client, /GPU 0 only/);
  assert.match(client, /GPU 1 reserved/);
});
