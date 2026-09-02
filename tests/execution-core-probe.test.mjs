// The node pre-submit state/headroom probe is its own bounded read-only
// runRemote call immediately before submission. These tests cover Slurm's
// scontrol one-line node record, including long/short usable states and TRES.
import assert from "node:assert/strict";
import { test } from "node:test";
import { probeNodeHeadroom } from "../lib/execution-core.js";

const TEST_NODE = "gpu04";
const record = ({ state = "MIXED", cpuAlloc = 64, cpuTot = 192, cfgGpu = 8, allocGpu = 4, node = TEST_NODE } = {}) =>
  `NodeName=${node} CPUAlloc=${cpuAlloc} CPUTot=${cpuTot} State=${state} CfgTRES=cpu=${cpuTot},mem=515115M,billing=${cpuTot},gres/gpu=${cfgGpu} AllocTRES=cpu=${cpuAlloc},gres/gpu=${allocGpu}`;
const probe = (stdout, { exitCode = 0, stderr = "", cpus = 4, gpus = 1, commands = [], node = TEST_NODE } = {}) =>
  probeNodeHeadroom({ exec: {}, runRemote: async (_target, command) => { commands.push(command); return { stdout, stderr, exitCode }; }, cpus, gpus, node });

test("probe passes on usable short and long node states with sufficient headroom", async () => {
  for (const state of ["idle", "alloc", "allocated", "mix", "mixed", "MIXED"]) {
    const result = await probe(`NODE_PROBE=${record({ state })}\n`);
    assert.equal(result.ok, true, `${state} must pass`);
    assert.match(result.note, new RegExp(`${TEST_NODE} state=`));
  }
});

test("probe fails closed on unusable node states", async () => {
  for (const state of ["drain", "down", "inact", "maint", "comp", "fail", "unknown", "DRAIN+NOT_RESPONDING"]) {
    await assert.rejects(probe(`NODE_PROBE=${record({ state })}\n`), /not usable/u, `${state} must abort before sbatch`);
  }
});

test("probe fails closed on insufficient headroom", async () => {
  await assert.rejects(probe(`NODE_PROBE=${record({ cpuAlloc: 190 })}\n`, { cpus: 4 }), /CPU headroom/u);
  await assert.rejects(probe(`NODE_PROBE=${record({ allocGpu: 8 })}\n`, { gpus: 1 }), /GPU headroom/u);
});

test("mixed nodes without GPU allocation TRES fail closed", async () => {
  const liveStyle = record().replace("CfgTRES=cpu=192,mem=515115M,billing=192,gres/gpu=8", "Gres=gpu:rtx5000:8 CfgTRES=cpu=192,mem=515115M,billing=192").replace("AllocTRES=cpu=64,gres/gpu=4", "AllocTRES=cpu=64");
  await assert.rejects(probe(`NODE_PROBE=${liveStyle}\n`), /incomplete allocated-GPU evidence/u);
});

test("probe fails closed on missing, wrong, or malformed node evidence", async () => {
  await assert.rejects(probe("NODE_PROBE=\n"), /no such node/u);
  await assert.rejects(probe(`NODE_PROBE=${record({ node: "gpu03" })}\n`), /unparseable/u);
  await assert.rejects(probe("NODE_PROBE=garbage\n"), /unparseable/u);
  await assert.rejects(probe(`NODE_PROBE=${record().replace("CPUTot=192", "CPUTot=xx")}\n`), /non-numeric/u);
  await assert.rejects(probe(`NODE_PROBE=${record().replace("CfgTRES=cpu=192,mem=515115M,billing=192,gres/gpu=8", "CfgTRES=cpu=192").replace("AllocTRES=cpu=64,gres/gpu=4", "AllocTRES=cpu=64").replace(" Gres=gpu:rtx5000:8", "")}\n`), /CPU\/GPU resources/u);
});

test("probe fails closed on nonzero exit", async () => {
  await assert.rejects(probe("", { exitCode: 1, stderr: "scontrol: command not found" }), /probe failed/u);
});

test("probe fails closed when the structured marker is missing", async () => {
  for (const stdout of ["64a1b2 run.sbatch\n", ""]) await assert.rejects(probe(stdout), /missing NODE_PROBE marker/u);
});

test("probe rejects invalid requested resources", async () => {
  for (const args of [{ cpus: "abc" }, { cpus: 4.5 }, { cpus: 4, gpus: -1 }, { cpus: NaN }]) {
    await assert.rejects(probe(`NODE_PROBE=${record()}\n`, args), /invalid node probe resources/u);
  }
});

test("probe requires a target node", async () => {
  await assert.rejects(probeNodeHeadroom({ exec: {}, runRemote: async () => ({ stdout: "", exitCode: 0 }), cpus: 4, gpus: 1, node: "" }), /requires a safe target node/u);
});

test("probe issues exactly one bounded read-only remote call and never invokes sbatch", async () => {
  const commands = [];
  await probe(`NODE_PROBE=${record()}\n`, { commands });
  assert.equal(commands.length, 1);
  assert.match(commands[0], new RegExp(`scontrol show node ${TEST_NODE} -o`));
  assert.equal(commands[0].includes("sbatch"), false);
});
