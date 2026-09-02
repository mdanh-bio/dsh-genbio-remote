// t10 follow-up: the node pre-submit state/headroom probe is its OWN bounded
// runRemote call immediately before the submit call (probe evidence and the
// submit act stay separately observable). This test exercises the JS-side
// parser with an injected runRemote stub and proves the fail-closed
// semantics: unusable state, insufficient headroom, missing node, unparseable
// or non-numeric rows, and nonzero exit all throw BEFORE sbatch; a usable
// node with enough free CPU/GPU passes. The marker-less no-op path is a
// documented harness accommodation (the production command always emits the
// NODE_PROBE= marker under set -eu).
import assert from "node:assert/strict";
import { test } from "node:test";
import { probeNodeHeadroom } from "../lib/pinned.js";

const TEST_NODE = "gpu04";
const probe = (stdout, { exitCode = 0, stderr = "", cpus = 4, gpus = 1, commands = [], node = TEST_NODE } = {}) =>
  probeNodeHeadroom({ exec: {}, runRemote: async (_target, command) => { commands.push(command); return { stdout, stderr, exitCode }; }, cpus, gpus, node });

test("probe passes on a usable node with sufficient headroom", async () => {
  for (const [line, cpus, gpus] of [["alloc|32|4|1|0", 4, 1], ["idle|32|0|1|0", 32, 0], ["mix|32|24|1|0", 8, 1], ["alloc|32|0|1|0", 32, 1]]) {
    const result = await probe(`NODE_PROBE=${line}\n`, { cpus, gpus });
    assert.equal(result.ok, true, `usable node "${line}" for ${cpus} cpu / ${gpus} gpu must pass`);
    assert.match(result.note, new RegExp(`${TEST_NODE} state=`));
  }
});

test("probe fails closed on unusable node states", async () => {
  for (const state of ["drain", "down", "inact", "maint", "comp", "fail", "unknown"]) {
    await assert.rejects(probe(`NODE_PROBE=${state}|32|0|1|0\n`, {}), /not usable/u, `${state} node must abort before sbatch`);
  }
  await assert.rejects(probe(`NODE_PROBE=drain+reason|32|0|1|0\n`, {}), /not usable/u, "flagged drain node must abort before sbatch");
});

test("probe fails closed on insufficient headroom", async () => {
  await assert.rejects(probe(`NODE_PROBE=idle|8|0|1|0\n`, { cpus: 32, gpus: 0 }), /CPU headroom/u, "insufficient free CPUs must abort before sbatch");
  await assert.rejects(probe(`NODE_PROBE=idle|32|0|1|1\n`, { cpus: 4, gpus: 1 }), /GPU headroom/u, "no free GPUs must abort before sbatch");
});

test("probe fails closed when sinfo reports no such node", async () => {
  await assert.rejects(probe("NODE_PROBE=\n", {}), /no such node/u, "empty probe record must abort before sbatch");
});

test("probe fails closed on unparseable or non-numeric rows", async () => {
  await assert.rejects(probe("NODE_PROBE=garbage\n", {}), /unparseable/u);
  await assert.rejects(probe("NODE_PROBE=alloc|xx|0|1|0\n", {}), /non-numeric/u);
});

test("probe fails closed on nonzero exit", async () => {
  await assert.rejects(probe("", { exitCode: 1, stderr: "sinfo: command not found" }), /probe failed/u);
});

test("probe treats marker-less output as a stub/no-op (documented accommodation)", async () => {
  const stub = await probe("64a1b2  run.sbatch\n", {});
  assert.equal(stub.ok, true);
  assert.match(stub.note, /stub\/no-op/u);
  const empty = await probe("", {});
  assert.equal(empty.ok, true);
  assert.match(empty.note, /stub\/no-op/u);
});

test("probe rejects invalid requested resources", async () => {
  await assert.rejects(probe("NODE_PROBE=alloc|32|4|1|0\n", { cpus: "abc" }), /invalid node probe resources/u);
  await assert.rejects(probe("NODE_PROBE=alloc|32|4|1|0\n", { cpus: 4.5 }), /invalid node probe resources/u);
  await assert.rejects(probe("NODE_PROBE=alloc|32|4|1|0\n", { cpus: 4, gpus: -1 }), /invalid node probe resources/u);
  await assert.rejects(probe("NODE_PROBE=alloc|32|4|1|0\n", { cpus: NaN }), /invalid node probe resources/u);
});

test("probe requires a target node", async () => {
  await assert.rejects(
    probeNodeHeadroom({ exec: {}, runRemote: async () => ({ stdout: "", exitCode: 0 }), cpus: 4, gpus: 1, node: "" }),
    /requires a target node/u,
  );
});

test("probe issues exactly one bounded read-only remote call and never invokes sbatch", async () => {
  const commands = [];
  await probe("NODE_PROBE=alloc|32|4|1|0\n", { commands });
  assert.equal(commands.length, 1, "the probe is exactly one remote call");
  assert.match(commands[0], new RegExp(`sinfo -h -N -n ${TEST_NODE}`));
  assert.equal(commands[0].includes("sbatch"), false, "the probe itself never invokes sbatch");
});
