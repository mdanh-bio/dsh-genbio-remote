import assert from "node:assert/strict";

const bundle = await import("../lib/index.js");
const { remoteNeedCovered, remotePathInside, toolRootsFor, HPC_SMOKE_ROOT } = bundle;

// remotePathInside: containment with the prefix-trap and trailing-slash edges.
assert.equal(remotePathInside("/a/b/c", "/a/b"), true);
assert.equal(remotePathInside("/a/b", "/a/b"), true);
assert.equal(remotePathInside("/a/bc", "/a/b"), false);
assert.equal(remotePathInside("/a/b/", "/a/b"), true);
assert.equal(remotePathInside("/a//b/c", "/a/b"), true);
assert.equal(remotePathInside("/", "/a"), false);
assert.equal(remotePathInside("/a/b", "/a/b/c"), false);

// toolRootsFor: only absolute, traversal-free strings from the policy survive.
const policy = {
  targets: {
    genbioh100: { environment: { tool_roots: ["/home/work/GenbioLAB/miniconda3", "/home/work/GenbioLAB/common", "relative", "/bad/../x", 42] } },
    HPC: {},
  },
};
assert.deepEqual(toolRootsFor(policy, "genbioh100"), ["/home/work/GenbioLAB/miniconda3", "/home/work/GenbioLAB/common"]);
assert.deepEqual(toolRootsFor(policy, "HPC"), []);
assert.deepEqual(toolRootsFor(null, "HPC"), []);
assert.deepEqual(toolRootsFor({ targets: {} }, "HPC"), []);

// remoteNeedCovered: rw covers writes+reads, ro covers reads only, tool roots
// cover reads only, containment is by path prefix.
const granted = [
  { target: "HPC", root: "/data01/proj", mode: "rw" },
  { target: "HPC", root: "/data01/other", mode: "ro" },
];
const toolRoots = ["/home/work/GenbioLAB/miniconda3"];
assert.equal(remoteNeedCovered({ root: "/data01/proj/runs/x", write: true }, granted, toolRoots), true);
assert.equal(remoteNeedCovered({ root: "/data01/proj", write: true }, granted, toolRoots), true);
assert.equal(remoteNeedCovered({ root: "/data01/other/data", write: true }, granted, toolRoots), false);
assert.equal(remoteNeedCovered({ root: "/data01/other/data", write: false }, granted, toolRoots), true);
assert.equal(remoteNeedCovered({ root: "/home/work/GenbioLAB/miniconda3/bin/python3", write: false }, granted, toolRoots), true);
assert.equal(remoteNeedCovered({ root: "/home/work/GenbioLAB/miniconda3/bin/python3", write: true }, granted, toolRoots), false);
assert.equal(remoteNeedCovered({ root: "/data01/elsewhere", write: true }, granted, toolRoots), false);
assert.equal(remoteNeedCovered({ root: "/data01/other", write: false }, [], []), false);

// The smoke root constant stays the policy-pinned HPC path.
assert.equal(HPC_SMOKE_ROOT, "/data01/genbiolab/mdanh/data/projects/dsh_policy_smoke");
assert.equal(remotePathInside(`${HPC_SMOKE_ROOT}/runs/x`, HPC_SMOKE_ROOT), true);

console.log("remote-access unit tests passed");
