import assert from "node:assert/strict";
import test from "node:test";
import { classifyWorkloadEvidence, parseJobOutputMarkers, parseOwnedSacctTable } from "../lib/scheduler-evidence.js";

const ID = "1234"; const NAME = "demo-run.abcdef12";
test("owned sacct evidence requires exactly one matching parent id and name", () => {
  assert.deepEqual(parseOwnedSacctTable(`${ID}|${NAME}|COMPLETED|0:0|00:01\n${ID}.batch|batch|COMPLETED|0:0|00:01\n`, ID, NAME), { jobId: ID, jobName: NAME, state: "COMPLETED", exitCode: "0:0", elapsed: "00:01" });
  assert.equal(parseOwnedSacctTable(`${ID}|other|COMPLETED|0:0|00:01\n`, ID, NAME), null);
  assert.equal(parseOwnedSacctTable(`9999|${NAME}|COMPLETED|0:0|00:01\n`, ID, NAME), null);
  assert.equal(parseOwnedSacctTable(`${ID}|${NAME}|RUNNING||00:01\n${ID}|${NAME}|RUNNING||00:01\n`, ID, NAME), null);
});

test("job-owned markers bind job id/name and distinguish incomplete evidence", () => {
  const complete = parseJobOutputMarkers(`DSH_SLURM_FRAME=START|${ID}|${NAME}|gpu04|0\nDSH_SLURM_FRAME=DONE|${ID}|${NAME}\n`, ID, NAME);
  assert.deepEqual(complete, { identity: true, complete: true });
  assert.equal(classifyWorkloadEvidence({ state: "COMPLETED", exitCode: "0:0" }, complete), "completed");
  assert.equal(classifyWorkloadEvidence({ state: "COMPLETED", exitCode: "0:0" }, { identity: true, complete: false }), "evidence-incomplete");
  assert.equal(classifyWorkloadEvidence({ state: "CANCELLED", exitCode: "0:15" }, complete), "cancelled");
});
