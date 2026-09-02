import assert from "node:assert/strict";
import test from "node:test";
import { parseRunEvidence } from "../lib/index.js";

test("finalization evidence uses structured project Slurm fields when helper logs are empty", () => {
  const parsed = parseRunEvidence({
    stdout: "",
    slurmJobId: "872160",
    slurmStatus: "COMPLETED",
    slurmExitCode: "0:0",
    slurmElapsed: "00:00:01",
    workloadEvidence: "scheduler-and-job-output",
    error: null,
  });
  assert.equal(parsed.schedulerJobId, "872160");
  assert.deepEqual(parsed.terminalEvidence, { state: "COMPLETED", exitCode: "0:0", elapsed: "00:00:01", workloadEvidence: "scheduler-and-job-output" });
});
