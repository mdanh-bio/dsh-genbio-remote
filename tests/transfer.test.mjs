import assert from "node:assert/strict";

const { DEFAULT_RCLONE_REMOTE, RCLONE_TRANSFER_ARGS, SSH_TRANSPORT_FAIL_RE, resolveRcloneRemote, rcloneCopytoCommand, runRemoteWithRetry } = await import("../lib/transfer.js");

const noSleep = () => Promise.resolve();

function makeRunRemote(script) {
  const calls = [];
  const impl = async (target, command, exec, timeoutMs) => {
    calls.push({ target, command, timeoutMs });
    return script[calls.length - 1] ?? { stdout: "", stderr: "", exitCode: 0 };
  };
  return { calls, impl };
}

try {
  // ── Target → remote resolution ──
  assert.deepEqual(DEFAULT_RCLONE_REMOTE, { HPC: "hpc", genbioh100: "genbioh100", genbio_mdanh: "genbio01" });
  assert.equal(resolveRcloneRemote({}, "HPC"), "hpc");
  assert.equal(resolveRcloneRemote(undefined, "HPC"), "hpc");
  assert.equal(resolveRcloneRemote({}, "genbioh100"), "genbioh100");
  assert.equal(resolveRcloneRemote({}, "genbio_mdanh"), "genbio01", "genbio01 was retargeted by the owner to user mdanh (2026-08-24 evening)");
  assert.equal(resolveRcloneRemote({ rcloneRemote: { HPC: "hpc-alt" } }, "HPC"), "hpc-alt", "config override must win over the default map");
  assert.throws(() => resolveRcloneRemote({}, "unknown_target"), /no rclone remote configured for target unknown_target/u, "unmapped targets must fail closed");
  assert.throws(() => resolveRcloneRemote({ rcloneRemote: { unknown_target: "" } }, "unknown_target"), /no rclone remote configured/u, "empty override must fall through to fail-closed");

  // ── copyto command construction ──
  assert.deepEqual(RCLONE_TRANSFER_ARGS, ["--retries", "2", "--low-level-retries", "2", "--contimeout", "20s"]);
  assert.equal(
    rcloneCopytoCommand("/tmp/pkg/x.tar.gz", "hpc", "/data01/run/x.tar.gz"),
    "rclone copyto --retries 2 --low-level-retries 2 --contimeout 20s '/tmp/pkg/x.tar.gz' 'hpc:/data01/run/x.tar.gz'",
  );
  assert.throws(() => rcloneCopytoCommand("relative/local.txt", "hpc", "/data01/x"), /absolute local path/u);
  assert.throws(() => rcloneCopytoCommand("/tmp/a b.txt", "hpc", "/data01/x"), /unsafe fixed path/u);
  assert.throws(() => rcloneCopytoCommand("/tmp/x", "hpc", "data/relative"), /absolute remote path/u);

  // ── Bounded transport retry semantics ──
  assert.match("connect to host x", SSH_TRANSPORT_FAIL_RE);
  // Success on the first attempt: exactly one call.
  {
    const { calls, impl } = makeRunRemote([]);
    const out = await runRemoteWithRetry(impl, "HPC", "cmd", {}, 1000, undefined, noSleep);
    assert.equal(calls.length, 1);
    assert.equal(out.exitCode, 0);
  }
  // Exit 0 is never retried, even with a scary stderr string.
  {
    const { calls, impl } = makeRunRemote([{ stdout: "", stderr: "ssh: connect to host 10.x port 22", exitCode: 0 }]);
    const out = await runRemoteWithRetry(impl, "HPC", "cmd", {}, 1000, undefined, noSleep);
    assert.equal(calls.length, 1);
    assert.equal(out.exitCode, 0);
  }
  // One transient transport failure (exit 255), then success.
  {
    const { calls, impl } = makeRunRemote([{ stdout: "", stderr: "kex_exchange_identification: Connection closed by remote host", exitCode: 255 }, { stdout: "ok", stderr: "", exitCode: 0 }]);
    const out = await runRemoteWithRetry(impl, "HPC", "cmd", {}, 1000, undefined, noSleep);
    assert.equal(calls.length, 2);
    assert.equal(out.stdout, "ok");
  }
  // Connection-class stderr with a non-255 exit code is still transport-level.
  {
    const { calls, impl } = makeRunRemote([{ stdout: "", stderr: "ssh: connect to host h port 22: Operation timed out", exitCode: 1 }, { stdout: "ok", stderr: "", exitCode: 0 }]);
    const out = await runRemoteWithRetry(impl, "HPC", "cmd", {}, 1000, undefined, noSleep);
    assert.equal(calls.length, 2);
    assert.equal(out.stdout, "ok");
  }
  // Persistent transport failure: bounded at `attempts`, last result returned.
  {
    const fail = { stdout: "", stderr: "ssh: connect to host h: No route to host", exitCode: 255 };
    const { calls, impl } = makeRunRemote([fail, fail, fail]);
    const out = await runRemoteWithRetry(impl, "HPC", "cmd", {}, 1000, 3, noSleep);
    assert.equal(calls.length, 3);
    assert.equal(out.exitCode, 255);
  }
  // A real non-transport failure stands immediately — never retried.
  {
    const { calls, impl } = makeRunRemote([{ stdout: "", stderr: "mkdir: permission denied", exitCode: 1 }]);
    const out = await runRemoteWithRetry(impl, "HPC", "cmd", {}, 1000, undefined, noSleep);
    assert.equal(calls.length, 1);
    assert.equal(out.exitCode, 1);
  }

  console.log("transfer unit tests passed");
} catch (error) {
  console.error(error);
  process.exitCode = 1;
}
