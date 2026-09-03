import { shellQuote } from "./shell.js";

// ── Global rclone transfer layer ───────────────────────────────────────────
// Owner directive (2026-08-24): ALL plugin data transfers must use rclone
// (SFTP) — never scp. Rationale: repeated scp/ssh connection timeouts on the
// HPC login-node route; rclone keeps one persistent SFTP session per file
// with bounded internal retries. The remote sha256 gates in each staging
// module remain the integrity authority and fail closed if a transfer no-ops
// or truncates.
//
// Target → rclone remote map (defaults; config.rcloneRemote.<target> wins):
//   HPC         → hpc         (sftp 10.201.134.217, user mdanh)
//   genbioh100  → genbioh100  (sftp proxy3.ainexus.ktcloud.com:10504, user work)
//   genbio_mdanh→ genbio01    (sftp 192.168.0.56) — the owner retargeted the
//     legacy `genbio01` remote from user `genbio` to user `mdanh`
//     (2026-08-24 evening), making it the correct account for this target;
//     before that change this mapping was deliberately absent and transfers
//     failed closed.

const DEFAULT_RCLONE_REMOTE = Object.freeze({ HPC: "hpc", NHPC: "nhpc", genbioh100: "genbioh100", genbio_mdanh: "genbio01" });
const SAFE_RCLONE_REMOTE_RE = /^[A-Za-z0-9_-]+$/u;
const RCLONE_TRANSFER_ARGS = Object.freeze(["--retries", "2", "--low-level-retries", "2", "--contimeout", "20s"]);
const SSH_TRANSPORT_FAIL_RE = /connect to host|Operation timed out|Connection reset|Connection closed by|No route to host|Network is unreachable/u;

// Resolve the rclone remote name for one plugin target. Fails closed when a
// target has no configured remote — callers must never fall back to scp.
function resolveRcloneRemote(config, target, policy = null) {
  const override = config?.rcloneRemote?.[target];
  const fallback = DEFAULT_RCLONE_REMOTE[target];
  const selected = typeof override === "string" && override.length > 0 ? override : fallback;
  if (typeof selected !== "string" || !SAFE_RCLONE_REMOTE_RE.test(selected)) throw new Error(`rclone remote for ${target} must be a simple configured alias [A-Za-z0-9_-]`);
  const policyRemote = policy?.targets?.[target]?.transfer?.rclone_remote;
  if (policyRemote !== undefined) {
    if (typeof policyRemote !== "string" || !SAFE_RCLONE_REMOTE_RE.test(policyRemote)) throw new Error(`policy rclone remote for ${target} is invalid`);
    if (selected !== policyRemote) throw new Error(`configured rclone remote ${selected} does not match policy-bound ${target} remote ${policyRemote}`);
  }
  return selected;
}

// One file: local path → <remote>:<absolute remote path>. Both endpoints must
// be absolute (a relative endpoint would silently resolve against the plugin
// process cwd). rclone copyto creates missing destination parents and
// overwrites existing files by default, which preserves the idempotent resume
// semantics of staging.
function rcloneCopytoCommand(localPath, rcloneRemoteName, destRemotePath) {
  if (typeof localPath !== "string" || !localPath.startsWith("/")) throw new Error(`rclone source must be an absolute local path: ${localPath}`);
  if (typeof destRemotePath !== "string" || !destRemotePath.startsWith("/")) throw new Error(`rclone destination must be an absolute remote path: ${destRemotePath}`);
  const dest = `${String(rcloneRemoteName)}:${destRemotePath}`;
  return `rclone copyto ${RCLONE_TRANSFER_ARGS.join(" ")} ${shellQuote(localPath)} ${shellQuote(dest)}`;
}

// One bounded artifact: `<remote>:<absolute remote path>` -> absolute local
// destination. This is deliberately separate from upload construction so a
// caller cannot accidentally reverse the endpoints. Callers must discover and
// enforce the remote size/hash before transfer, then recompute the local hash.
function rcloneCopyfromCommand(rcloneRemoteName, sourceRemotePath, localPath) {
  if (typeof sourceRemotePath !== "string" || !sourceRemotePath.startsWith("/")) throw new Error(`rclone source must be an absolute remote path: ${sourceRemotePath}`);
  if (typeof localPath !== "string" || !localPath.startsWith("/")) throw new Error(`rclone destination must be an absolute local path: ${localPath}`);
  const source = `${String(rcloneRemoteName)}:${sourceRemotePath}`;
  return `rclone copyto ${RCLONE_TRANSFER_ARGS.join(" ")} ${shellQuote(source)} ${shellQuote(localPath)}`;
}

function sleepMs(ms) { return new Promise((resolve) => setTimeout(resolve, ms)); }

// Bounded retry for transport-level SSH failures only (exit 255 or a
// connection-class error). A real remote result (any other exit code) stands.
// Use only for idempotent steps; never wrap the submit step (double-sbatch risk).
async function runRemoteWithRetry(runRemote, target, command, exec, timeoutMs, attempts = 3, sleep = sleepMs) {
  let last;
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    last = await runRemote(target, command, exec, timeoutMs);
    const transportFail = last.exitCode === 255 || SSH_TRANSPORT_FAIL_RE.test(`${last.stderr ?? ""}\n${last.stdout ?? ""}`);
    if (last.exitCode === 0 || !transportFail || attempt === attempts) return last;
    await sleep(attempt * 10000);
  }
  return last;
}

export { DEFAULT_RCLONE_REMOTE, SAFE_RCLONE_REMOTE_RE, RCLONE_TRANSFER_ARGS, SSH_TRANSPORT_FAIL_RE, resolveRcloneRemote, rcloneCopytoCommand, rcloneCopyfromCommand, sleepMs, runRemoteWithRetry };
