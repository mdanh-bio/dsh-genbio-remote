window.__ModuleLoader__.load({
  id: "dsh-genbio-remote",
  factory: (require) => {
    const React = require("react");
    const h = React.createElement;
    const PROJECTION_KEY = "genbio/remote";
    const css = `
      .dgr-dock{box-sizing:border-box;width:calc(100% - 40px);max-width:820px;margin:0 auto -10px;padding:0 12px;position:relative;z-index:2}
      .dgr-card{border:1px solid var(--dsw-alias-border-l1);background:var(--dsw-specific-tip);border-radius:12px 12px 0 0;padding:8px 12px;display:flex;align-items:center;gap:10px;color:var(--dsw-alias-label-secondary);font-size:12px}
      .dgr-dot{width:8px;height:8px;border-radius:50%;background:#d99a18;box-shadow:0 0 0 3px color-mix(in srgb,#d99a18 18%,transparent)}
      .dgr-dot[data-live=true]{background:#2caa65;box-shadow:0 0 0 3px color-mix(in srgb,#2caa65 18%,transparent)}
      .dgr-title{font-weight:650;color:var(--dsw-alias-label-primary)}
      .dgr-meta{min-width:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;flex:1}
      .dgr-chip{border-radius:999px;padding:2px 7px;background:var(--dsw-alias-fill-l2);font-variant-numeric:tabular-nums}
      .dgr-menu{position:relative}
      .dgr-button{border:0;background:transparent;color:var(--dsw-alias-label-secondary);cursor:pointer;padding:4px 7px;border-radius:6px}
      .dgr-button:hover{background:var(--dsw-alias-fill-l2)}
      /* Overlay panels open UPWARD above the one-row dock so a large number of
         jobs/projects can never push the chat viewport away; max-height + scroll
         bounds them regardless of content volume. */
      .dgr-panel{position:absolute;bottom:calc(100% + 8px);right:0;width:390px;max-width:calc(100vw - 32px);max-height:min(66vh,480px);overflow-y:auto;overscroll-behavior:contain;z-index:100;display:none;border:1px solid var(--dsw-alias-border-l2);background:var(--dsw-specific-menu);box-shadow:var(--dsw-shadow-lv3);border-radius:12px;padding:12px;color:var(--dsw-alias-label-primary)}
      .dgr-panel[data-open=true]{display:block}
      .dgr-panel.dgr-panel-wide{width:480px}
      .dgr-overflow-note{font-size:11px;color:var(--dsw-alias-label-secondary);padding-top:6px;text-align:center}
      .dgr-row{display:flex;justify-content:space-between;gap:16px;padding:6px 0;border-bottom:1px solid var(--dsw-alias-border-l1);font-size:12px}
      .dgr-row:last-child{border-bottom:0}.dgr-value{text-align:right;color:var(--dsw-alias-label-secondary);word-break:break-word}
      .dgr-error{margin-top:8px;padding:8px;border-radius:8px;background:color-mix(in srgb,#d83b3b 12%,transparent);color:#c94545;white-space:pre-wrap;font-family:var(--dsw-font-mono);font-size:11px}
      .dgr-section{margin-top:10px;padding-top:8px;border-top:1px solid var(--dsw-alias-border-l2)}
      .dgr-safe{padding:7px;border-radius:8px;background:color-mix(in srgb,#d99a18 10%,transparent);color:var(--dsw-alias-label-secondary);font-size:11px}
      .dgr-project-card{margin-top:6px;border-radius:12px;align-items:stretch;flex-direction:column;gap:8px;color:var(--dsw-alias-label-primary)}
      .dgr-project-head{display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap}
      .dgr-select{background:var(--dsw-alias-fill-l2);color:var(--dsw-alias-label-primary);border:1px solid var(--dsw-alias-border-l1);border-radius:8px;padding:3px 8px;font-size:12px;max-width:240px}
      .dgr-sub{font-size:11px;color:var(--dsw-alias-label-secondary);text-transform:uppercase;letter-spacing:.04em;margin:2px 0}
      .dgr-chips{display:flex;flex-wrap:wrap;gap:6px}
      .dgr-chip[data-tone=ok]{background:color-mix(in srgb,#2caa65 14%,transparent);color:#2caa65}
      .dgr-chip[data-tone=warn]{background:color-mix(in srgb,#d99a18 14%,transparent);color:#c79014}
      .dgr-chip[data-tone=active]{background:color-mix(in srgb,#2f6fe0 14%,transparent);color:#3b76e8}
      .dgr-chip[data-tone=idle]{background:var(--dsw-alias-fill-l2);color:var(--dsw-alias-label-secondary)}
      .dgr-proj-note{margin-top:4px;font-size:11px;color:var(--dsw-alias-label-secondary)}
    `;

    function installStyle() {
      const existing = document.querySelector('style[data-plugin-css="dsh-genbio-remote"]');
      if (existing) return () => {};
      const tag = document.createElement("style");
      tag.dataset.plugin = "dsh-genbio-remote";
      tag.dataset.pluginCss = "dsh-genbio-remote";
      tag.textContent = css;
      document.head.appendChild(tag);
      return () => tag.remove();
    }

    function duration(ms) {
      const total = Math.max(0, Math.floor(ms / 1000));
      const h1 = Math.floor(total / 3600);
      const m = Math.floor(total % 3600 / 60);
      const s = total % 60;
      return h1 ? `${h1}h ${m}m` : m ? `${m}m ${s}s` : `${s}s`;
    }

    function GenbioDock({ sessionId, useSessions, useProjection }) {
      const jobs = useSessions((state) => state.jobsBySession[sessionId]) || [];
      // Host-side mirror of the session envelope + runs (session-projection channel);
      // undefined until this session has committed a genbio_* tool result.
      const genbio = useProjection ? useProjection(PROJECTION_KEY) : undefined;
      const envelope = genbio?.envelope ?? null;
      const runs = genbio?.runs ?? [];
      const projects = genbio?.projects ?? [];
      const projectsStatus = genbio?.projects_status ?? [];
      const workflows = genbio?.workflows ?? [];
      const relevant = jobs.filter((job) => String(job.kind).startsWith("genbio") || /genbio|HPC|gpu03|genbioh100/i.test(job.label));
      const live = relevant.filter((job) => job.status === "running" || job.status === "stopping");
      const [panel, setPanel] = React.useState(null); // "details" | "projects" | null
      const [now, setNow] = React.useState(() => Date.now());
      const [selectedProject, setSelectedProject] = React.useState(null);
      React.useEffect(() => {
        if (!live.length) return;
        const dispose = setInterval(() => setNow(Date.now()), 1000);
        return () => clearInterval(dispose);
      }, [live.length]);
      // Keep the project selector in sync with the mirrored lists (aggregate
      // when available, else the lean discovery list); never dangle on a
      // project that disappeared.
      React.useEffect(() => {
        setSelectedProject((current) => {
          const names = new Set([...projectsStatus, ...projects].map((entry) => entry && entry.project).filter(Boolean));
          if (current && names.has(current)) return current;
          return projectsStatus[0]?.project ?? projects[0]?.project ?? null;
        });
      }, [projectsStatus, projects]);
      // Only occupy the input dock while this session actually uses remote
      // compute: at least one genbio job, a committed session envelope, a
      // mirrored run, or a mirrored project list. Idle sessions render nothing.
      if (relevant.length === 0 && !envelope && runs.length === 0 && projects.length === 0 && projectsStatus.length === 0 && workflows.length === 0) return null;
      const runByJob = new Map();
      for (const run of runs) if (run && run.jobId) runByJob.set(run.jobId, run);
      let usedCpus = 0;
      let usedGpus = 0;
      for (const job of live) {
        const run = runByJob.get(job.id);
        usedCpus += Number(run?.resources?.cpus ?? 0);
        usedGpus += Number(run?.resources?.gpus ?? 0);
      }
      const latest = live[0] || relevant[0];
      const resource = latest ? latest.label : envelope ? `envelope ${envelope.target} · ${envelope.node}` : "remote compute session";
      const envelopeLine = envelope
        ? [envelope.target, envelope.node, envelope.partition, envelope.workloadClass].filter(Boolean).join(" · ")
        : "not set";
      const limitsLine = envelope
        ? `${envelope.maxCpus} CPU · ${envelope.maxGpus} GPU${envelope.memGb != null ? ` · ${envelope.memGb} GB` : ""} · ${envelope.concurrency} job${envelope.concurrency === 1 ? "" : "s"}`
        : "—";
      const usageLine = `${usedCpus}${envelope ? `/${envelope.maxCpus}` : ""} CPU · ${usedGpus}${envelope ? `/${envelope.maxGpus}` : ""} GPU · ${live.length} active`;
      const projectNames = projectsStatus.length
        ? projectsStatus.map((entry) => entry && entry.project).filter(Boolean)
        : projects.map((entry) => entry && entry.project).filter(Boolean);
      const selectedName = selectedProject && projectNames.includes(selectedProject) ? selectedProject : projectNames[0] ?? null;
      const selectedAgg = projectsStatus.find((entry) => entry.project === selectedName) ?? null;
      const selectedLean = selectedAgg ? null : projects.find((entry) => entry.project === selectedName) ?? null;
      const visibleJobs = relevant.slice(0, 10);
      const hiddenJobs = relevant.length - visibleJobs.length;
      return h("div", { className: "dgr-dock" },
        h("div", { className: "dgr-card" },
          h("span", { className: "dgr-dot", "data-live": live.length > 0 }),
          h("span", { className: "dgr-title" }, "Genbio Remote"),
          h("span", { className: "dgr-meta", title: latest?.label || resource }, latest ? latest.label : resource),
          latest ? h("span", { className: "dgr-chip" }, latest.status) : null,
          latest ? h("span", { className: "dgr-chip" }, duration((latest.finishedAt || now) - latest.startedAt)) : null,
          h("div", { className: "dgr-menu" },
            h("button", { className: "dgr-button", type: "button", onClick: () => setPanel(panel === "details" ? null : "details"), "aria-expanded": panel === "details" }, "Details"),
            projectNames.length ? h("button", { className: "dgr-button", type: "button", onClick: () => setPanel(panel === "projects" ? null : "projects"), "aria-expanded": panel === "projects" }, "Projects") : null,
            h("div", { className: "dgr-panel", "data-open": panel === "details" ? "true" : "false" },
              h("div", { className: "dgr-row" }, h("strong", null, "Session envelope"), h("span", { className: "dgr-value" }, envelopeLine)),
              h("div", { className: "dgr-row" }, h("strong", null, "Envelope limits"), h("span", { className: "dgr-value" }, limitsLine)),
              h("div", { className: "dgr-row" }, h("strong", null, "Current usage"), h("span", { className: "dgr-value" }, usageLine)),
              h("div", { className: "dgr-row" }, h("strong", null, "Active jobs"), h("span", { className: "dgr-value" }, String(live.length))),
              h("div", { className: "dgr-row" }, h("strong", null, "Supported targets"), h("span", { className: "dgr-value" }, "HPC · NHPC · genbio_mdanh · genbioh100")),
              h("div", { className: "dgr-row" }, h("strong", null, "HPC live test"), h("span", { className: "dgr-value" }, "manifest-driven node")),
              h("div", { className: "dgr-row" }, h("strong", null, "genbioh100"), h("span", { className: "dgr-value" }, "GPU 0 only · GPU 1 reserved · 16 CPU · 32 GB · 1 job")),
              projects.length ? h("div", { className: "dgr-section" },
                h("div", { className: "dgr-safe" }, "Project and workflow views are read-only. Plan, advance, status, and cancellation remain explicit agent tools with policy and approval gates."),
                projects.slice(0, 8).map((project) => h("div", { className: "dgr-row", key: `project-${project.project}` },
                  h("strong", null, project.project),
                  h("span", { className: "dgr-value" }, `${project.valid ? `schema v${project.schemaVersion || "?"}` : "invalid"} · ${(project.operations || []).slice(0, 6).map((op) => op && typeof op === "object" ? op.name : op).join(", ")}${(project.operations || []).length > 6 ? "…" : ""}${project.plans?.length ? ` · ${project.plans.length} plan(s)` : ""}`)
                ))
              ) : null,
              ...(relevant.length === 0 ? [] : [h("div", { className: "dgr-section" }, "Jobs")]),
              visibleJobs.map((job) => h("div", { className: "dgr-row", key: job.id },
                h("strong", null, `${job.kind} · ${job.id}`),
                h("span", { className: "dgr-value" }, `${job.status} · ${duration((job.finishedAt || now) - job.startedAt)}${job.detail ? ` · ${job.detail}` : ""}`)
              )),
              hiddenJobs > 0 ? h("div", { className: "dgr-overflow-note" }, `${hiddenJobs} more job(s) — use genbio_runs / genbio_monitor for full tracking`) : null,
              workflows.length ? h("div", { className: "dgr-section" },
                h("div", { className: "dgr-sub" }, "Controlled workflows"),
                workflows.slice(-8).map((workflow) => h("div", { key: workflow.workflow_run_id },
                  h("div", { className: "dgr-row" }, h("strong", null, workflow.workflow), h("span", { className: "dgr-value" }, `${workflow.status} · ${workflow.counts?.completed ?? 0}/${workflow.nodes?.length ?? 0} complete`)),
                  (workflow.nodes || []).slice(0, 32).map((node) => h("div", { className: "dgr-row", key: `${workflow.workflow_run_id}-${node.node_id}` }, h("strong", null, `↳ ${node.node_id}`), h("span", { className: "dgr-value" }, `${node.status}${node.job_id ? ` · job ${node.job_id}` : ""}${node.slurm_state ? ` · ${node.slurm_state}` : ""}`)))
                ))
              ) : null,
              latest?.status === "failed" ? h("div", { className: "dgr-error" }, latest.detail || "Remote job failed") : null
            ),
            projectNames.length ? h("div", { className: "dgr-panel dgr-panel-wide", "data-open": panel === "projects" ? "true" : "false" },
              h("div", { className: "dgr-project-head" },
                h("span", { className: "dgr-title" }, "Genbio Projects"),
                h("select", { className: "dgr-select", value: selectedName ?? "", onChange: (event) => setSelectedProject(event.target.value), "aria-label": "Genbio project selector" },
                  projectNames.map((name) => h("option", { key: name, value: name }, name)))
              ),
              selectedAgg
                ? h("div", { className: "dgr-project-body" },
                    h("div", { className: "dgr-proj-note" }, "Read-only project/workflow aggregate. Consequential actions remain explicit agent tools; no execute, submit, or cancel control is exposed here."),
                    h("div", { className: "dgr-sub" }, "Health"),
                    h("div", { className: "dgr-chips" },
                      h("span", { className: "dgr-chip", "data-tone": selectedAgg.valid ? "ok" : "warn" }, selectedAgg.valid ? `schema v${selectedAgg.schema_version ?? "?"}` : "invalid"),
                      ...(selectedAgg.origin ? [h("span", { className: "dgr-chip", "data-tone": "idle" }, selectedAgg.origin)] : []),
                      h("span", { className: "dgr-chip", "data-tone": "idle" }, `${selectedAgg.run_counts?.active ?? 0} active`),
                      h("span", { className: "dgr-chip", "data-tone": "ok" }, `${selectedAgg.run_counts?.completed ?? 0} completed`),
                      ...(selectedAgg.run_counts?.failed ? [h("span", { className: "dgr-chip", "data-tone": "warn" }, `${selectedAgg.run_counts.failed} failed`)] : []),
                      h("span", { className: "dgr-chip", "data-tone": "idle" }, `${selectedAgg.plans?.length ?? 0} plan(s)`)
                    ),
                    ...(selectedAgg.description ? [h("div", { className: "dgr-proj-note" }, selectedAgg.description)] : []),
                    ...(selectedAgg.suggested || []).length ? [
                      h("div", { className: "dgr-sub" }, "Suggested next (read-only)"),
                      h("div", { className: "dgr-chips" }, selectedAgg.suggested.slice(0, 24).map((name) => h("span", { className: "dgr-chip", key: `sug-${name}`, "data-tone": "idle" }, name)))
                    ] : [],
                    ...(selectedAgg.active || []).length ? [
                      h("div", { className: "dgr-sub" }, "Active"),
                      h("div", { className: "dgr-chips" }, selectedAgg.active.map((name) => h("span", { className: "dgr-chip", key: `act-${name}`, "data-tone": "active" }, name)))
                    ] : [],
                    ...(selectedAgg.completed || []).length ? [
                      h("div", { className: "dgr-sub" }, "Completed"),
                      h("div", { className: "dgr-chips" }, selectedAgg.completed.slice(0, 24).map((name) => h("span", { className: "dgr-chip", key: `done-${name}`, "data-tone": "ok" }, name)))
                    ] : [],
                    ...(selectedAgg.needs_review || []).length ? [
                      h("div", { className: "dgr-sub" }, "Needs review"),
                      h("div", { className: "dgr-chips" }, selectedAgg.needs_review.slice(0, 24).map((name) => h("span", { className: "dgr-chip", key: `rev-${name}`, "data-tone": "warn" }, name)))
                    ] : [],
                    ...(selectedAgg.operations || []).length ? [
                      h("div", { className: "dgr-sub" }, "Operations"),
                      h("div", null, selectedAgg.operations.slice(0, 40).map((op) => h("div", { className: "dgr-row", key: `op-${op.name}` },
                        h("strong", null, op.name),
                        h("span", { className: "dgr-value" }, `${op.form ?? "template"} · ${op.cpus ?? "?"} CPU · ${op.gpus ?? 0} GPU · conc ${op.concurrency ?? 1}`)
                      )))
                    ] : [],
                    ...(selectedAgg.plans || []).length ? [
                      h("div", { className: "dgr-sub" }, "Session plans"),
                      h("div", null, selectedAgg.plans.slice(0, 24).map((plan, index) => h("div", { className: "dgr-row", key: `plan-${plan.plan_hash}-${index}` },
                        h("strong", null, plan.operation),
                        h("span", { className: "dgr-value" }, `${plan.status} · ${String(plan.plan_hash || "").slice(0, 8)}`)
                      )))
                    ] : [],
                    ...(selectedAgg.runs || []).length ? [
                      h("div", { className: "dgr-sub" }, "Recent runs"),
                      h("div", null, selectedAgg.runs.slice(0, 8).map((run, index) => h("div", { className: "dgr-row", key: `run-${run.run_id}-${index}` },
                        h("strong", null, run.operation),
                        h("span", { className: "dgr-value" }, `${run.status} · ${run.target || "-"} · ${run.finished_at ? duration(run.finished_at - (run.started_at ?? run.finished_at)) : "active"}`)
                      )))
                    ] : []
                  )
                : selectedLean
                  ? h("div", { className: "dgr-project-body" },
                      h("div", { className: "dgr-proj-note" }, "Run genbio_projects_status for the aggregate per-project view (read-only)."),
                      h("div", { className: "dgr-row" }, h("strong", null, "Operations"), h("span", { className: "dgr-value" }, (selectedLean.operations || []).slice(0, 30).map((op) => op && typeof op === "object" ? op.name : op).join(", ") || "none")),
                      ...(selectedLean.plans || []).slice(0, 24).map((plan, index) => h("div", { className: "dgr-row", key: `splan-${index}` },
                        h("strong", null, plan.operation || "?"),
                        h("span", { className: "dgr-value" }, `${plan.status ?? "planned"} · ${String(plan.planHash || "").slice(0, 8)}`)
                      ))
                    )
                  : projectNames.length
                    ? h("div", { className: "dgr-project-body" }, h("div", { className: "dgr-proj-note" }, "No project selected."))
                    : null
            ) : null
          )
        )
      );
    }

    const inject = ["slots"];
    function apply(ctx) {
      ctx.effect(installStyle, "genbio-remote styles");
      ctx.slots.inject("conversation.input.dock", () => ctx.slots.register({
        name: "conversation.input.dock",
        id: "genbio-remote",
        order: 30,
        label: "Genbio Remote"
      }, GenbioDock));
    }
    return { inject, apply };
  }
});
