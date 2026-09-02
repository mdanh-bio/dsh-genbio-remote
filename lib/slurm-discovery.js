const MAX_SECTION_CHARS = 16000;
const SAFE_STATE_RE = /^[A-Za-z][A-Za-z0-9_+.-]*$/u;

function bounded(value) {
  return String(value ?? "").slice(0, MAX_SECTION_CHARS);
}

export function classifyPendingReason(reason) {
  const value = String(reason ?? "").trim();
  if (!value) return { category: "unknown", verified: false };
  if (/^(Resources|ReqNodeNotAvail)/u.test(value)) return { category: "resources", verified: false };
  if (/^Priority/u.test(value)) return { category: "priority", verified: false };
  if (/^(Partition|QOS|Assoc|InvalidAccount|InvalidQOS)/u.test(value)) return { category: "policy-or-association", verified: false };
  if (/^Dependency/u.test(value)) return { category: "dependency", verified: false };
  if (/^(JobHeld|BeginTime)/u.test(value)) return { category: "hold-or-begin-time", verified: false };
  return { category: "unknown", verified: false };
}

export function parseSlurmDiscovery(stdout) {
  const text = String(stdout ?? "");
  const sections = {};
  for (const name of ["VERSION", "PARTITIONS", "NODES", "QUEUE"]) {
    const match = new RegExp(`^${name}_BEGIN\\n([\\s\\S]*?)\\n${name}_END$`, "mu").exec(text);
    sections[name.toLowerCase()] = match ? { status: "verified", text: bounded(match[1]) } : { status: "unavailable", text: "" };
  }
  return Object.freeze(sections);
}

export function parseOwnedJobStatusRow(row, expectedJobId) {
  const fields = String(row ?? "").trim().split("|");
  if (fields.length !== 5 || fields[0] !== expectedJobId || !SAFE_STATE_RE.test(fields[2] ?? "")) return null;
  const [jobId, jobName, state, exitCode, elapsed] = fields;
  return Object.freeze({ jobId, jobName, state: state.replace(/\+$/u, ""), exitCode: exitCode || null, elapsed: elapsed || null });
}

export { MAX_SECTION_CHARS };
