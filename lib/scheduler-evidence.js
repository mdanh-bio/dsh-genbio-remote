const SAFE_STATE_RE = /^[A-Za-z][A-Za-z0-9_+.-]*$/u;
const SAFE_JOB_NAME_RE = /^[A-Za-z0-9_.-]{1,128}$/u;

export function parseOwnedSacctTable(text, expectedJobId, expectedJobName) {
  if (!/^[0-9]{1,10}$/u.test(expectedJobId ?? "") || !SAFE_JOB_NAME_RE.test(expectedJobName ?? "")) return null;
  const rows = String(text ?? "").split(/\r?\n/u).map((line) => line.trim()).filter(Boolean);
  const parents = [];
  for (const line of rows) {
    const fields = line.split("|");
    if (fields.length !== 5) return null;
    const [jobId, jobName, state, exitCode, elapsed] = fields;
    if (jobId.includes(".")) continue;
    if (!/^[0-9]{1,10}$/u.test(jobId) || !SAFE_JOB_NAME_RE.test(jobName) || !SAFE_STATE_RE.test(state)) return null;
    if (jobId === expectedJobId && jobName === expectedJobName) parents.push({ jobId, jobName, state: state.replace(/[+~*#]$/u, ""), exitCode: exitCode || null, elapsed: elapsed || null });
  }
  return parents.length === 1 ? Object.freeze(parents[0]) : null;
}

export function parseJobOutputMarkers(text, expectedJobId, expectedJobName) {
  const value = String(text ?? "");
  const starts = [...value.matchAll(/^DSH_SLURM_FRAME=START\|([^|\s]+)\|([^|\s]+)\|([^|\r\n]*)\|([^|\r\n]*)$/gmu)];
  const dones = [...value.matchAll(/^DSH_SLURM_FRAME=DONE\|([^|\s]+)\|([^|\s]+)$/gmu)];
  const identity = starts.length === 1 && starts[0][1] === expectedJobId && starts[0][2] === expectedJobName && dones.every((match) => match[1] === expectedJobId && match[2] === expectedJobName);
  if (!identity) return Object.freeze({ identity: false, complete: false });
  return Object.freeze({ identity: true, complete: dones.length === 1 });
}

export function classifyWorkloadEvidence(scheduler, markers) {
  if (!scheduler) return "unknown";
  if (scheduler.state === "CANCELLED") return "cancelled";
  const terminal = new Set(["COMPLETED", "FAILED", "TIMEOUT", "NODE_FAIL", "OUT_OF_MEMORY", "BOOT_FAIL", "DEADLINE", "PREEMPTED", "REVOKED"]);
  if (!terminal.has(scheduler.state)) return scheduler.state === "PENDING" ? "pending" : "running";
  if (scheduler.state !== "COMPLETED" || scheduler.exitCode !== "0:0") return "failed";
  return markers?.identity && markers?.complete ? "completed" : "evidence-incomplete";
}

export { SAFE_JOB_NAME_RE, SAFE_STATE_RE };
