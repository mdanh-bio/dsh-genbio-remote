import assert from "node:assert/strict";
import test from "node:test";
import { parseReconciliationEnvelope, resolveReconciliationCandidates } from "../lib/execution-core.js";

const name = "analysis.abcdef12";
const envelope = (squeue = "", sacct = "") => `SQUEUE_BEGIN\n${squeue}\nSQUEUE_END\nSACCT_BEGIN\n${sacct}\nSACCT_END\n`;

for (const [label, stdout] of [
  ["squeue-only pending", envelope(`1234|${name}|PENDING`)],
  ["sacct-only completed", envelope("", `1234|${name}|COMPLETED`)],
  ["same id in both sources", envelope(`1234|${name}|RUNNING`, `1234|${name}|RUNNING`)],
]) {
  test(`reconciliation resolves ${label}`, () => {
    const parsed = parseReconciliationEnvelope(stdout, name);
    assert.ok(parsed);
    assert.equal(resolveReconciliationCandidates(parsed), "1234");
  });
}

test("reconciliation remains ambiguous when scheduler sources disagree", () => {
  const parsed = parseReconciliationEnvelope(envelope(`1234|${name}|RUNNING`, `5678|${name}|COMPLETED`), name);
  assert.ok(parsed);
  assert.equal(resolveReconciliationCandidates(parsed), null);
});

test("reconciliation fails closed on malformed rows, wrong names, or markers", () => {
  for (const stdout of [
    envelope(`1234|${name}`),
    envelope(`1234|${name}|RUNNING|EXTRA`),
    envelope(`not-id|${name}|RUNNING`),
    envelope("1234|unrelated|RUNNING"),
    "SQUEUE_BEGIN\n\nSQUEUE_END\nSACCT_BEGIN\n",
    `banner\n${envelope()}`,
  ]) assert.equal(parseReconciliationEnvelope(stdout, name), null);
});

test("empty evidence remains unresolved and cannot authorize retry", () => {
  const parsed = parseReconciliationEnvelope(envelope(), name);
  assert.deepEqual(parsed, { squeue: [], sacct: [] });
  assert.equal(resolveReconciliationCandidates(parsed), null);
});
