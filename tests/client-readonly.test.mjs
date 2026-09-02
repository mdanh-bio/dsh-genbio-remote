import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

// The Genbio GUI dock is a READ-ONLY project control panel: it consumes the
// session-projection mirror (envelope/runs/projects/projects_status) and
// exposes only a project <select> plus the existing "Details" toggle. The
// "no execute button / no arbitrary remote actions" acceptance is encoded
// here as a structural regression guard against a future contributor adding
// an execution surface to the client bundle. The client has no tool-call
// channel, and it must also never even NAME a remote-executing tool.
const clientSource = await readFile(new URL("../lib/client.js", import.meta.url), "utf8");

const REMOTE_ACTION_TOOLS = [
  "genbio_launch",
  "genbio_project_execute",
  "genbio_project_cancel",
  "genbio_project_fetch",
  "genbio_workflow_execute",
  "genbio_workflow_advance",
  "genbio_workflow_cancel",
  "genbio_finalize_run",
  "genbio_publish_run",
  "scp ",
  "rclone ",
  "ssh ",
];

test("client panel consumes the aggregate projection and is present as a bounded overlay", () => {
  // The project panel is a COLLAPSIBLE overlay anchored above the one-row dock
  // (never inline in document flow), so many jobs/projects can never push the
  // chat viewport away. Assert the new structural markers for that design.
  assert.match(clientSource, /dgr-panel-wide/u, "the project panel overlay must exist (wide panel)");
  assert.match(clientSource, /bottom:calc\(100% \+ 8px\)/u, "overlay must open upward above the dock bar");
  assert.match(clientSource, /max-height:min\(66vh,480px\)/u, "overlay must be height-bounded and scrollable");
  assert.match(clientSource, /"Projects"/u, "a Projects button must toggle the overlay");
  assert.match(clientSource, /projects_status/u, "the panel must read the aggregate mirror");
  assert.match(clientSource, /aria-label": "Genbio project selector"/u, "the project selector must exist");
  assert.match(clientSource, /no execute, submit, or cancel control is exposed here/u, "the panel must state it exposes no consequential controls");
  assert.match(clientSource, /Controlled workflows/u, "the panel must render bounded workflow state");
  assert.match(clientSource, /HPC · NHPC/u, "the supported target copy includes NHPC");
  // Job rows inside the Details overlay are capped so a long job list stays bounded.
  assert.match(clientSource, /visibleJobs = relevant\.slice\(0, 10\)/u, "Details overlay must cap the rendered job rows");
});

test("client bundle has NO execute / submit / remote-action surface", () => {
  for (const needle of REMOTE_ACTION_TOOLS) {
    assert.equal(clientSource.includes(needle), false, `client bundle must not reference the remote/tool surface: ${JSON.stringify(needle.trim())}`);
  }
});