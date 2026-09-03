import assert from "node:assert/strict";
import test from "node:test";
import { parseProjectManifest } from "../lib/project.js";
import { freshRunDirectoryCommand, uniqueJobNameFor } from "../lib/execution-core.js";
import { parseJobOutputMarkers } from "../lib/scheduler-evidence.js";

const base = { schema_version: 2, project: "demo", local_root: "/tmp/demo", remote_root: "/data01/demo", files: ["run.sh"], jobs: { run: { cpus: 1, recipe: { name: "demo-run", script: "run.sh", argv: [] } } } };

test("remote roots are shell-inert and reject quote injection", () => {
  assert.throws(() => parseProjectManifest("demo", { ...base, remote_root: "/tmp/evil';touch-pwned;'" }), /shell-inert absolute path/u);
  assert.throws(() => parseProjectManifest("demo", { ...base, remote_root: "/tmp/has space" }), /shell-inert absolute path/u);
});

test("fresh attempt command creates parent but rejects attempt reuse", () => {
  const command = freshRunDirectoryCommand("/data01/demo", "/data01/demo/runs/att-abc");
  assert.match(command, /^ssh -T .* -- HPC /u);
  assert.match(command, /mkdir -p -m 700 -- '\/data01\/demo\/runs'/u);
  assert.match(command, /mkdir -m 700 -- '\/data01\/demo\/runs\/att-abc'/u);
  assert.equal(command.includes("mkdir -p -m 700 -- '/data01/demo/runs/att-abc'"), false);
  assert.match(freshRunDirectoryCommand("/srv/demo", "/srv/demo/runs/att-xyz", "NHPC"), /^ssh -T .* -- NHPC /u);
});

test("unique Slurm names always retain the exact-once suffix", () => {
  const name = uniqueJobNameFor("x".repeat(128), "abcdef1234567890");
  assert.equal(name.length, 100);
  assert.match(name, /\.abcdef12$/u);
});

test("framed identity markers are exact and forged frames fail closed", () => {
  const good = "DSH_SLURM_FRAME=START|123|demo.abc|gpu04|0\nDSH_SLURM_FRAME=DONE|123|demo.abc\n";
  assert.deepEqual(parseJobOutputMarkers(good, "123", "demo.abc"), { identity: true, complete: true });
  assert.deepEqual(parseJobOutputMarkers(`${good}DSH_SLURM_FRAME=DONE|999|demo.abc\n`, "123", "demo.abc"), { identity: false, complete: false });
});
