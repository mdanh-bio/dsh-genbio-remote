import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import { load as parseYaml } from "js-yaml";
import { genbioh100ConcurrencyCap } from "../lib/slurm-policy.js";

const root = path.resolve(import.meta.dirname, "..");
const host = fs.readFileSync(path.join(root, "lib/index.js"), "utf8");

// ── genbioh100ConcurrencyCap: the CPU/GPU class concurrency core ─────────────
// gpus>0 -> concurrent_gpu_jobs (pinned 1); gpus===0 -> concurrent_cpu_jobs
// (optional, default 1, fail-closed). The machine-total-CPU bound is enforced
// separately in h100-direct.js via a FRESH nproc read (not a policy field).
function policyWith(gpuCap, cpuCap) {
  const limits = { gpus_allowed: [0], cpu_threads_per_job: 16, mem_gb_per_job: 32 };
  if (gpuCap !== undefined) limits.concurrent_gpu_jobs = gpuCap;
  if (cpuCap !== undefined) limits.concurrent_cpu_jobs = cpuCap;
  return { targets: { genbioh100: { limits } } };
}

test("GPU-class workloads (gpus>0) are bounded by concurrent_gpu_jobs", () => {
  assert.equal(genbioh100ConcurrencyCap(policyWith(1, 8), 1), 1);
  assert.equal(genbioh100ConcurrencyCap(policyWith(1, 8), 4), 1);
});

test("CPU-only workloads (gpus===0) are bounded by concurrent_cpu_jobs", () => {
  assert.equal(genbioh100ConcurrencyCap(policyWith(1, 8), 0), 8);
  assert.equal(genbioh100ConcurrencyCap(policyWith(1, 1), 0), 1);
  assert.equal(genbioh100ConcurrencyCap(policyWith(1, 3), 0), 3);
});

test("missing concurrent_cpu_jobs fails closed to the default (1)", () => {
  assert.equal(genbioh100ConcurrencyCap(policyWith(1, undefined), 0), 1);
  assert.equal(genbioh100ConcurrencyCap(policyWith(1, undefined), 1), 1, "GPU class still 1");
});

test("malformed concurrent_cpu_jobs fails closed to the default (1)", () => {
  assert.equal(genbioh100ConcurrencyCap(policyWith(1, 0), 0), 1);
  assert.equal(genbioh100ConcurrencyCap(policyWith(1, -4), 0), 1);
  assert.equal(genbioh100ConcurrencyCap(policyWith(1, "8"), 0), 1);
  assert.equal(genbioh100ConcurrencyCap(policyWith(1, 2.5), 0), 1);
  assert.equal(genbioh100ConcurrencyCap(policyWith(1, null), 0), 1);
});

test("non-numeric gpus is treated as CPU-only (fail-closed class)", () => {
  assert.equal(genbioh100ConcurrencyCap(policyWith(1, 5), undefined), 5);
  assert.equal(genbioh100ConcurrencyCap(policyWith(1, 5), null), 5);
});

test("the live policy tracks the owner-approved concurrent_cpu_jobs state", () => {
  const policyPath = "/Users/mdanh/.codex/skills/operate-genbio-hpc-remote/references/genbio-compute-policy.yaml";
  if (fs.existsSync(policyPath)) {
    const live = parseYaml(fs.readFileSync(policyPath, "utf8"));
    const ccj = live.targets.genbioh100.limits.concurrent_cpu_jobs;
    if (ccj === undefined) {
      // pre-approval state: absent field fails closed to 1
      assert.equal(genbioh100ConcurrencyCap(live, 0), 1, "absent CPU-only cap defaults fail-closed to 1");
    } else {
      // only the owner-approved value 10 may be installed (2026-09-01 sign-off)
      assert.equal(ccj, 10, "only the owner-approved concurrent_cpu_jobs value 10 may be installed");
      assert.equal(genbioh100ConcurrencyCap(live, 0), 10, "present owner-approved cap is applied");
    }
    assert.equal(genbioh100ConcurrencyCap(live, 1), 1, "GPU cap stays 1");
  }
});

// ── setEnvelope + validatePolicy wiring (static, fail-closed) ─────────────────
test("setEnvelope applies the CLASS-specific cap to genbioh100 envelopes", () => {
  assert.match(host, /genbioh100ConcurrencyCap\(policy, args\.max_gpus\)/u, "setEnvelope must compute the class cap from max_gpus");
  assert.match(host, /concurrency > classCap/u, "setEnvelope must bound concurrency by the class cap");
});

test("validatePolicy validates concurrent_cpu_jobs only when present", () => {
  assert.match(host, /concurrent_cpu_jobs !== undefined && \(!Number\.isInteger\(h100\.limits\.concurrent_cpu_jobs\)/u, "optional field must be validated when present");
  assert.match(host, /concurrent_cpu_jobs must be a positive integer/u, "malformed concurrent_cpu_jobs must be rejected");
});
