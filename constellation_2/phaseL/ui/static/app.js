"use strict";

const el = (id) => document.getElementById(id);

const state = {
  view: "operations",
  refreshSec: 60,
  timer: null,
  days: [],
  day: null,
  attempts: [],
  attempt_id: null,
  statusV2: null,
};

async function api(path) {
  const r = await fetch(path, { cache: "no-store" });
  return await r.json();
}

function setView(v) {
  state.view = v;
  el("viewOperations").classList.toggle("hidden", v !== "operations");
  el("viewEngines").classList.toggle("hidden", v !== "engines");
  el("viewPortfolio").classList.toggle("hidden", v !== "portfolio");
  el("viewPositions").classList.toggle("hidden", v !== "positions");
  el("viewHistory").classList.toggle("hidden", v !== "history");
  el("viewTechnical").classList.toggle("hidden", v !== "technical");

  const tabs = [
    ["tabOperations", "operations"],
    ["tabEngines", "engines"],
    ["tabPortfolio", "portfolio"],
    ["tabPositions", "positions"],
    ["tabHistory", "history"],
    ["tabTechnical", "technical"],
  ];
  tabs.forEach(([id, vv]) => el(id).classList.toggle("active", vv === v));
}

function fmt(v) {
  if (v === null || v === undefined) return "n/a";
  if (typeof v === "number") {
    // compact
    if (Math.abs(v) >= 1000000) return v.toFixed(0);
    if (Math.abs(v) >= 1000) return v.toFixed(2);
    return v.toFixed(2);
  }
  return String(v);
}

function stateClass(st) {
  const u = String(st || "UNKNOWN").toUpperCase();
  if (u === "PASS") return "state-pass";
  if (u === "DEGRADED") return "state-degraded";
  if (u === "FAIL") return "state-fail";
  if (u === "ABORTED") return "state-aborted";
  if (u === "MISSING") return "state-missing";
  return "state-unknown";
}

function modeClass(mode, flattenOnly) {
  const m = String(mode || "UNKNOWN").toUpperCase();
  if (flattenOnly === true) return "mode-flatten";
  if (m === "PAPER") return "mode-paper";
  if (m === "LIVE") return "mode-live";
  if (m === "DISABLED") return "mode-disabled";
  return "mode-disabled";
}

function renderTiles(payload) {
  const tiles = (payload?.ops_health?.tiles || []);
  const grid = el("tileGrid");
  grid.innerHTML = "";

  const titleMap = {
    "orchestrator_run_verdict_v2": "Orchestrator Run Verdict (V2)",
    "safety_breach": "Safety Breach / Hard Gate",
    "broker_connection_observer": "Broker Connection / Observer",
    "feed_attestation": "Feed Attestation",
    "liquidity_gate": "Liquidity Gate",
    "correlation_gate": "Correlation Gate",
    "convex_gate": "Convex Gate",
    "replay_certification": "Replay Certification",
    "gate_stack_verdict_v1": "Gate Stack Verdict",
  };

  tiles.forEach(t => {
    const st = String(t.state || "UNKNOWN").toUpperCase();
    const stop = (st === "ABORTED");
    const last = t.last_updated_utc || "n/a";
    const rc = (t.reason_codes || []).slice(0,2).join(", ") || "n/a";
    const path = t.artifact_ref?.path;

    const human = (st === "PASS") ? "COMPLETED — PASS"
      : (st === "DEGRADED") ? "COMPLETED — DEGRADED"
      : (st === "FAIL") ? "RUN COMPLETED — FAIL"
      : (st === "ABORTED") ? "SAFETY BREACH — STOP"
      : st;

    const open = path ? `<button class="btn btn-mini" data-open="${encodeURIComponent(path)}" data-title="${t.tile_id}">Evidence</button>` : "";

    const div = document.createElement("div");
    div.className = `tile ${stop ? "stop" : ""}`;
    div.innerHTML = `
      <div class="tile-head">
        <div class="tile-title">${titleMap[t.tile_id] || t.tile_id}</div>
        <div>${open}</div>
      </div>
      <div class="tile-state ${stateClass(st)}">${human}</div>
      <div class="tile-meta">
        <div class="mono tiny muted">updated=${last}</div>
        <div class="mono tiny muted">reason=${rc}</div>
      </div>
    `;
    grid.appendChild(div);
  });

  document.querySelectorAll("[data-open]").forEach(b => {
    b.onclick = async () => {
      const p = decodeURIComponent(b.getAttribute("data-open") || "");
      const title = b.getAttribute("data-title") || "evidence";
      await openEvidence(title, p);
    };
  });
}

function renderScopeHealth(payload) {
  const sh = payload?.scope_health || {};
  const exec = sh?.sleeve_execution_health || {};
  const mon = sh?.system_monitoring_health || {};
  const overall = sh?.overall || {};
  const src = sh?.source || {};
  const liveReady = payload?.sleeve_live_readiness || {};

  const execState = String(exec.status || "UNKNOWN").toUpperCase();
  const monState = String(mon.status || "UNKNOWN").toUpperCase();
  const overallState = String(overall.status || "UNKNOWN").toUpperCase();
  const rows = [
    ["Sleeve Execution Health", execState],
    ["System Monitoring Health", monState],
    ["Overall Status", overallState],
  ];

  el("scopeHealthMeta").textContent = `runtime_state=${src?.present ? "present" : "missing"} • source=${src?.path || "n/a"}`;
  el("scopeHealthRow").innerHTML = rows.map(([k, st]) => `
    <div class="metric">
      <div class="k">${k}</div>
      <div class="v ${stateClass(st)}">${st}</div>
    </div>
  `).join("");

  const grade = liveReady?.readiness_grade ?? liveReady?.grade_band ?? "n/a";
  const score = (liveReady?.readiness_score ?? "n/a");
  const threshold = (liveReady?.score_threshold ?? "n/a");
  const state = String(liveReady?.state || "UNKNOWN").toUpperCase();
  const readinessSummary = liveReady?.readiness_summary || "n/a";
  const decisionBasis = liveReady?.promotion_decision_basis || "n/a";
  const blockersList = (liveReady?.top_blockers_ordered || liveReady?.promotion_blockers || []).slice(0, 3);
  const blockers = blockersList.join(", ")
    || (liveReady?.reason_codes || []).slice(0, 3).join(", ")
    || "n/a";
  const readyPath = liveReady?.path || "n/a";
  const candidate = (liveReady?.promotion_candidate === true) ? "YES"
    : (liveReady?.promotion_candidate === false) ? "NO"
    : "UNKNOWN";
  const remaining = (liveReady?.pass_conditions_remaining || []).slice(0, 3).join(" | ") || "n/a";
  const minimumSummary = (liveReady?.minimum_conditions_summary || []).slice(0, 3).join(" | ") || "n/a";
  const nextActions = (liveReady?.recommended_next_actions || []).slice(0, 3).join(" | ") || "n/a";
  const rootBlockers = (liveReady?.root_blockers || []).slice(0, 3).join(", ") || "n/a";
  const derivedBlockers = (liveReady?.derived_blockers || []).slice(0, 3).join(", ") || "n/a";
  const smallestClearanceSet = (liveReady?.smallest_clearance_set || []).slice(0, 3).join(" | ") || "n/a";
  const blockerOrder = (liveReady?.blocker_dependency_order || []).join(" -> ") || "n/a";
  const gateSequence = (liveReady?.estimated_promotion_gate_sequence || []).join(" -> ") || "n/a";
  const agg = liveReady?.aggregate_blocker_summary || {};
  const calib = liveReady?.calibration_support || {};
  const failedChecks = (calib?.failed_checks || []).map(x => `${x.check_id}:${x.score_awarded}/${x.weight}`).slice(0, 4).join(", ") || "n/a";
  const checklist = liveReady?.promotion_checklist || {};
  const checklistFalse = (checklist?.currently_false || []).slice(0, 3).join(" | ") || "none";
  const cv = liveReady?.current_vs_required || {};
  const histCv = cv?.pass_history || {};
  const freshCv = cv?.freshness || {};
  const lifeCv = cv?.lifecycle_monitor || {};
  const scoreCv = cv?.score || {};
  const cvSummary = `pass_history=${histCv.current ?? "n/a"}/${histCv.required ?? "n/a"} freshness_ok=${freshCv.ok ?? "n/a"} lifecycle_status=${lifeCv.current_status ?? "n/a"} score=${scoreCv.current ?? "n/a"}/${scoreCv.required ?? "n/a"}`;

  const lifecycleSurface = (((mon?.freshness || {}).surface_results || []).find(s => (s?.surface_id || "") === "lifecycle_monitor")) || {};
  const freshness = mon?.freshness || {};
  const monitoringReasonCodes = (freshness?.reason_codes || []).join(", ") || "none";
  const lifecycleReasons = (lifecycleSurface?.source_reason_codes || []).slice(0, 3).join(", ") || "n/a";
  const lifecycleFails = (lifecycleSurface?.source_check_failures || []).slice(0, 3).join(", ") || "n/a";
  const lifecycleFailClass = (lifecycleSurface?.fail_reasons || []).join(", ") || "n/a";
  const lifecycleCauseClass = lifecycleSurface?.lifecycle_cause_class || "n/a";
  const lifecycleGov = lifecycleSurface?.lifecycle_governance_summary || {};
  const lifecycleGovSummary = `exec_required_blocked=${lifecycleGov?.blocked_by_classification?.REQUIRED_FOR_EXECUTION ?? "n/a"} readiness_required_blocked=${lifecycleGov?.blocked_by_classification?.REQUIRED_FOR_READINESS ?? "n/a"} optional_blocked=${lifecycleGov?.blocked_by_classification?.OPTIONAL_MONITORING ?? "n/a"}`;
  const liveEvidencePaths = (liveReady?.evidence_paths || []).join(" | ") || "none";
  const freshnessSurfaces = (freshness?.surface_results || []).map(s => `${s?.surface_id || "unknown"}:${s?.status || "UNKNOWN"}`).join(", ") || "none";
  const overallSummary = `execution=${execState} • monitoring=${monState} • overall=${overallState}`;
  const sleeveSummary = `Sleeve Live Readiness: ${state} • score=${score}/${threshold} • grade=${grade} • promotion_candidate=${candidate}`;

  el("sleeveReadinessRow").innerHTML = `
    <div class="mono tiny">${overallSummary}</div>
    <div class="mono tiny">${sleeveSummary}</div>
    <details class="evidence-details" style="margin-top:8px;">
      <summary class="mono tiny">Evidence</summary>
      <div class="mono tiny" style="margin-top:6px;">readiness_summary=${readinessSummary}</div>
      <div class="mono tiny">promotion_decision_basis=${decisionBasis}</div>
      <div class="mono tiny">top_blockers=${blockers}</div>
      <div class="mono tiny">root_blockers=${rootBlockers}</div>
      <div class="mono tiny">derived_blockers=${derivedBlockers}</div>
      <div class="mono tiny">aggregate_blocker_summary=root=${agg.root_blocker_count ?? "n/a"} derived=${agg.derived_blocker_count ?? "n/a"} total=${agg.total_blocker_count ?? "n/a"}</div>
      <div class="mono tiny">minimum_conditions_summary=${minimumSummary}</div>
      <div class="mono tiny">pass_conditions_remaining=${remaining}</div>
      <div class="mono tiny">smallest_clearance_set=${smallestClearanceSet}</div>
      <div class="mono tiny">blocker_dependency_order=${blockerOrder}</div>
      <div class="mono tiny">estimated_promotion_gate_sequence=${gateSequence}</div>
      <div class="mono tiny">current_vs_required=${cvSummary}</div>
      <div class="mono tiny">calibration_failed_checks=${failedChecks}</div>
      <div class="mono tiny">promotion_checklist.currently_false=${checklistFalse}</div>
      <div class="mono tiny">recommended_next_actions=${nextActions}</div>
      <div class="mono tiny">freshness_status=${freshness?.status ?? "UNKNOWN"} reason_codes=${monitoringReasonCodes} surfaces=${freshnessSurfaces}</div>
      <div class="mono tiny">lifecycle_monitor: cause_class=${lifecycleCauseClass} fail_class=${lifecycleFailClass} reason_codes=${lifecycleReasons} failing_checks=${lifecycleFails}</div>
      <div class="mono tiny">lifecycle_governance=${lifecycleGovSummary}</div>
      <div class="mono tiny">evidence_paths=${liveEvidencePaths}</div>
      <div class="mono tiny muted">promotion_note=PASS_today_is_execution_only_not_live_readiness • path=${readyPath}</div>
    </details>
  `;
}

function renderPlatformReadiness(payload) {
  const platform = payload?.platform_readiness || {};
  const bug = payload?.platform_bug_metrics || {};
  const pm = platform?.metric_views || {};
  const bm = bug?.metric_views || {};
  const scope = payload?.scope_health || {};
  const mon = scope?.system_monitoring_health || {};
  const freshness = mon?.freshness || {};
  const lifecycleSurface = ((freshness?.surface_results || []).find(s => (s?.surface_id || "") === "lifecycle_monitor")) || {};

  const state = String(platform?.platform_readiness_state || "UNKNOWN").toUpperCase();
  const grade = platform?.platform_readiness_grade ?? "n/a";
  const score = platform?.platform_readiness_score ?? "n/a";
  const threshold = platform?.score_threshold_ready ?? "n/a";
  const candidate = (platform?.platform_promotion_candidate === true) ? "YES"
    : (platform?.platform_promotion_candidate === false) ? "NO"
    : "UNKNOWN";
  const blockers = (platform?.top_blockers_ordered || []).slice(0, 4).join(", ") || "none";
  const summary = platform?.readiness_summary || "n/a";
  const decision = platform?.promotion_decision_basis || "n/a";
  const velocityDisplay = pm?.bug_velocity_7d_avg?.display_value
    || bm?.bug_velocity_7d_avg?.display_value
    || "UNKNOWN";
  const recurrenceDisplay = pm?.recurrence_rate?.display_value
    || bm?.recurrence_rate?.display_value
    || "UNKNOWN";
  const stabilityDisplay = pm?.diagnostic_stability_rate?.display_value
    || bm?.diagnostic_stability_rate?.display_value
    || "UNKNOWN";
  const bugSummary = platform?.bug_stability_summary
    || `events_today=${bug?.new_bug_events_today ?? "n/a"} velocity_7d=${velocityDisplay} recurrence=${recurrenceDisplay} diagnostics_stability=${stabilityDisplay} trend=${bug?.bug_velocity_trend ?? "UNKNOWN"}`;
  const checklist = platform?.promotion_checklist || {};
  const checklistFalse = (checklist?.currently_false || []).slice(0, 4).join(" | ") || "none";
  const rootBlockers = (platform?.root_blockers || []).join(", ") || "none";
  const derivedBlockers = (platform?.derived_blockers || []).join(", ") || "none";
  const minimumSummary = (platform?.minimum_conditions_summary || []).join(" | ") || "none";
  const smallestClearance = (platform?.smallest_clearance_set || []).join(" | ") || "none";
  const blockerOrder = (platform?.blocker_dependency_order || []).join(" -> ") || "none";
  const currentVsRequired = platform?.current_vs_required || {};
  const aggregate = platform?.aggregate_blocker_summary || {};
  const bugCalc = bug?.calculation_summary || {};
  const bugUnknown = (bug?.unknown_fields || []).join(", ") || "none";
  const evidencePaths = [
    ...(Array.isArray(platform?.evidence_paths) ? platform.evidence_paths : []),
    ...(Array.isArray(bug?.evidence_paths) ? bug.evidence_paths : []),
  ].filter(Boolean);
  const path = platform?.path || "n/a";

  el("platformReadinessMeta").textContent = `artifact=${platform?.present ? "present" : "missing"} • path=${path}`;
  el("platformHeroRow").innerHTML = `
    <div class="metric">
      <div class="k">Platform Grade</div>
      <div class="v platform-grade">${grade}</div>
    </div>
    <div class="metric">
      <div class="k">Platform Score</div>
      <div class="v">${pm?.platform_readiness_score?.display_value || score} / ${pm?.score_threshold_ready?.display_value || threshold}</div>
    </div>
    <div class="metric">
      <div class="k">Platform State</div>
      <div class="v ${stateClass(state)}">${state}</div>
    </div>
    <div class="metric">
      <div class="k">Promotion Candidate</div>
      <div class="v">${candidate}</div>
    </div>
  `;
  el("platformSummaryRow").innerHTML = `
    <div class="mono tiny">readiness_summary=${summary}</div>
    <div class="mono tiny">bug_stability_summary=${bugSummary}</div>
    <details class="evidence-details" style="margin-top:8px;">
      <summary class="mono tiny">Evidence</summary>
      <div class="mono tiny" style="margin-top:6px;">promotion_decision_basis=${decision}</div>
      <div class="mono tiny">top_blockers=${blockers}</div>
      <div class="mono tiny">root_blockers=${rootBlockers}</div>
      <div class="mono tiny">derived_blockers=${derivedBlockers}</div>
      <div class="mono tiny">aggregate_blocker_summary=root=${aggregate?.root_blocker_count ?? "n/a"} derived=${aggregate?.derived_blocker_count ?? "n/a"} total=${aggregate?.total_blocker_count ?? "n/a"}</div>
      <div class="mono tiny">minimum_conditions_summary=${minimumSummary}</div>
      <div class="mono tiny">current_vs_required=${JSON.stringify(currentVsRequired)}</div>
      <div class="mono tiny">promotion_checklist.currently_false=${checklistFalse}</div>
      <div class="mono tiny">smallest_clearance_set=${smallestClearance}</div>
      <div class="mono tiny">blocker_dependency_order=${blockerOrder}</div>
      <div class="mono tiny">bug_metrics_display: velocity_7d=${velocityDisplay} recurrence=${recurrenceDisplay} diagnostics_stability=${stabilityDisplay}</div>
      <div class="mono tiny">bug_metrics_calculation_summary=${JSON.stringify(bugCalc)}</div>
      <div class="mono tiny">bug_metrics_unknown_fields=${bugUnknown}</div>
      <div class="mono tiny">monitoring_freshness_status=${freshness?.status ?? "UNKNOWN"} reason_codes=${(freshness?.reason_codes || []).join(", ") || "none"}</div>
      <div class="mono tiny">lifecycle_monitor_surface_status=${lifecycleSurface?.status ?? "UNKNOWN"} fail_reasons=${(lifecycleSurface?.fail_reasons || []).join(", ") || "none"} checks_failed=${(lifecycleSurface?.source_check_failures || []).join(", ") || "none"}</div>
      <div class="mono tiny">evidence_paths=${evidencePaths.join(" | ") || "none"}</div>
    </details>
  `;
}

function escapeHtml(v) {
  return String(v ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll("\"", "&quot;")
    .replaceAll("'", "&#39;");
}

function svgPlatformReadinessHistory(points) {
  const history = Array.isArray(points) ? points : [];
  if (!history.length) {
    return `<div class="mono small muted">No canonical platform readiness history found.</div>`;
  }

  const W = 920;
  const H = 260;
  const padL = 44;
  const padR = 18;
  const padT = 16;
  const padB = 34;
  const xSpan = Math.max(1, history.length - 1);
  const yMin = 0;
  const yMax = 100;
  const ySpan = yMax - yMin;
  const thresholds = history.map((p) => Number(p.threshold)).filter((v) => Number.isFinite(v));
  const threshold = thresholds.length ? thresholds[thresholds.length - 1] : null;
  const scoreToY = (score) => {
    const v = Number.isFinite(Number(score)) ? Number(score) : 0;
    return H - padB - ((v - yMin) / ySpan) * (H - padT - padB);
  };
  const indexToX = (idx) => padL + (idx * (W - padL - padR) / xSpan);
  const yTicks = [0, 25, 50, 75, 100];

  let path = "";
  const circles = [];
  const labels = [];
  history.forEach((point, idx) => {
    const score = Number(point.score);
    if (!Number.isFinite(score)) return;
    const x = indexToX(idx);
    const y = scoreToY(score);
    path += `${path ? " L " : "M "}${x} ${y}`;
    circles.push(`<circle class="history-point" cx="${x}" cy="${y}" r="4"></circle>`);
    labels.push(`<text class="history-axis-label" x="${x}" y="${H - 12}" text-anchor="middle">${escapeHtml(point.day.slice(5))}</text>`);
  });

  const grid = yTicks.map((tick) => {
    const y = scoreToY(tick);
    return `
      <line class="history-grid-line" x1="${padL}" y1="${y}" x2="${W - padR}" y2="${y}"></line>
      <text class="history-axis-label" x="${padL - 8}" y="${y + 4}" text-anchor="end">${tick}</text>
    `;
  }).join("");

  const thresholdSvg = Number.isFinite(threshold)
    ? `<line class="history-threshold-line" x1="${padL}" y1="${scoreToY(threshold)}" x2="${W - padR}" y2="${scoreToY(threshold)}"></line>
       <text class="history-axis-label" x="${W - padR}" y="${scoreToY(threshold) - 6}" text-anchor="end">threshold ${threshold}</text>`
    : "";

  return `
    <svg class="history-chart" viewBox="0 0 ${W} ${H}" role="img" aria-label="Platform readiness score history">
      ${grid}
      ${thresholdSvg}
      <path class="history-score-line" d="${path}"></path>
      ${circles.join("")}
      ${labels.join("")}
    </svg>
  `;
}

function renderPlatformReadinessHistory(payload) {
  const host = el("platformReadinessHistory");
  if (!host) return;
  const historyPayload = payload?.platform_readiness_history || {};
  const history = Array.isArray(historyPayload?.history) ? historyPayload.history : [];
  const dateRange = historyPayload?.date_range || null;
  const meta = historyPayload?.present
    ? `canonical_days=${history.length} • range=${dateRange?.start || "n/a"}..${dateRange?.end || "n/a"} • root=${historyPayload?.root || "n/a"}`
    : `canonical_days=0 • root=${historyPayload?.root || "n/a"}`;

  const rows = history.length
    ? history.map((row) => `
        <tr>
          <td>${escapeHtml(row.day)}</td>
          <td>${escapeHtml(row.score)}</td>
          <td>${escapeHtml(row.grade || "n/a")}</td>
          <td class="${stateClass(row.state)}">${escapeHtml(row.state || "UNKNOWN")}</td>
          <td>${escapeHtml(row.threshold)}</td>
        </tr>
      `).join("")
    : `<tr><td colspan="5" class="muted">No canonical platform readiness history artifacts found.</td></tr>`;

  host.innerHTML = `
    <div class="history-card">
      <div class="card-head">
        <div class="card-title">Platform Readiness History</div>
        <div class="mono tiny muted">${meta}</div>
      </div>
      <div class="history-chart-wrap">
        ${svgPlatformReadinessHistory(history)}
      </div>
      <table class="history-table">
        <thead>
          <tr>
            <th>Day</th>
            <th>Score</th>
            <th>Grade</th>
            <th>Status</th>
            <th>Threshold</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
  `;
}

function renderSleeveStrip(payload) {
  const sleeves = (payload?.sleeves || []);
  const container = el("sleeveStrip");
  const withAcct = sleeves.filter(s => s && s.ib_account_id).length;
  const byAcct = {};
  sleeves.forEach(s => {
    const acct = s?.ib_account_id || "n/a";
    byAcct[acct] = (byAcct[acct] || 0) + 1;
  });
  const split = Object.entries(byAcct).map(([k,v]) => `${k}:${v}`).join(" | ");
  container.innerHTML = `
    <div class="card-head">
      <div class="card-title">Sleeve Mode Strip (PAPER/LIVE per sleeve)</div>
      <div class="mono tiny muted">rows=${sleeves.length} • rendered=${sleeves.length} • with_account=${withAcct} • ${split || "no_accounts"}</div>
    </div>
    <div class="strip-row" id="stripRow"></div>
  `;
  const row = document.getElementById("stripRow");
  sleeves.forEach(s => {
    const mode = s.mode || "UNKNOWN";
    const acct = s.ib_account_id || "n/a";
    const ea = (s.entries_allowed === true) ? "ENTRIES: YES"
      : (s.entries_allowed === false) ? "ENTRIES: NO"
      : "ENTRIES: UNKNOWN";
    const fl = (s.flatten_only === true) ? "FLATTEN_ONLY" : "";
    const pill = document.createElement("div");
    pill.className = "sleeve-pill";
    pill.innerHTML = `
      <span class="mono sleeve-name">${s.sleeve_id}</span>
      <span class="acct-chip mono">IB ${acct}</span>
      <span class="mode-chip ${modeClass(mode, s.flatten_only)} mono">${String(mode).toUpperCase()}</span>
      <span class="entry-chip mono tiny muted">${ea}</span>
      ${fl ? `<span class="mono tiny muted">${fl}</span>` : ""}
    `;
    row.appendChild(pill);
  });
}

function toCount(v) {
  if (v === null || v === undefined) return null;
  if (typeof v === "number") return v;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function stageTone(stageKey, count) {
  const c = toCount(count);
  if (c === null) return "stage-fail";
  if ((stageKey === "intents")) return c > 0 ? "stage-info" : "stage-idle";
  if ((stageKey === "rejected_or_vetoed") || (stageKey === "vetoed")) return c > 0 ? "stage-warn" : "stage-idle";
  if (["authorized", "submitted", "filled", "reconciled"].includes(stageKey)) return c > 0 ? "stage-pass" : "stage-idle";
  return c > 0 ? "stage-info" : "stage-idle";
}

function renderFunnel(payload) {
  const c = payload?.trade_flow_today?.counts || {};
  const b = payload?.trade_flow_today?.blocked_by_gate || {};
  const d = payload?.trade_flow_today?.drilldown || {};

  const steps = [
    ["Intents", c.intents, "intents"],
    ["Rejected/Vetoed", (c.rejected ?? c.vetoed), "rejected_or_vetoed"],
    ["Authorized", c.authorized, "authorized"],
    ["Submitted", c.submitted, "submitted"],
    ["Filled", c.filled, "filled"],
    ["Reconciled", c.reconciled, "reconciled"],
  ];

  el("funnelRow").innerHTML = steps.map(([k,v,key]) => `
    <div class="funnel-step ${stageTone(key, v)}" data-funnel-stage="${key}">
      <div class="step-head">
        <div class="k">${k}</div>
        <span class="stage-chip mono">${stageTone(key, v).replace("stage-", "").toUpperCase()}</span>
      </div>
      <div class="v">${v === null || v === undefined ? "n/a" : v}</div>
    </div>
  `).join("");

  const blocked = [
    ["Liquidity", b.liquidity],
    ["Correlation", b.correlation],
    ["Attestation", b.attestation],
    ["Convex", b.convex],
    ["Capital", b.capital],
  ];
  el("blockedRow").innerHTML = blocked.map(([k,v]) =>
    `<div class="blocked-pill ${toCount(v) > 0 ? "blocked-active" : "blocked-none"}">${k}: ${v === null || v === undefined ? "n/a" : v}</div>`
  ).join("");

  el("funnelExplain").textContent =
    `Color workflow: blue=informational candidates, amber=blocked or vetoed, green=completed progress, gray=zero/not yet, red=error or missing proof.`;

  const drill = el("funnelDrilldown");
  const humanStage = {
    intents: "Intents",
    rejected_or_vetoed: "Rejected or Vetoed",
    authorized: "Authorized",
    submitted: "Submitted",
    filled: "Filled",
    reconciled: "Reconciled",
    vetoed: "Vetoed",
  };
  const stageHelp = {
    intents: "Candidate trading intents generated for this day.",
    rejected_or_vetoed: "Records blocked before broker submission by authorization rules or submit veto.",
    authorized: "Records that passed risk/governance and were allowed to submit.",
    submitted: "Orders sent to broker routing.",
    filled: "Submitted orders that reached filled state.",
    reconciled: "Filled records that completed reconciliation checks.",
    vetoed: "PhaseC veto records found for this day.",
  };
  const renderDrill = (key) => {
    const z = d?.[key] || {};
    const paths = z.evidence_paths || [];
    const rows = paths.length
      ? paths.map((p, i) => {
        const leaf = (String(p).split("/").pop() || p);
        return `
          <tr>
            <td class="mono tiny">${i + 1}</td>
            <td class="mono tiny">${leaf}</td>
            <td><button class="btn btn-mini" data-funnel-evidence="${encodeURIComponent(p)}" data-funnel-stage-open="${key}">Open evidence</button></td>
          </tr>
        `;
      }).join("")
      : `<tr><td colspan="3" class="mono tiny muted">No evidence paths recorded for this stage.</td></tr>`;
    drill.innerHTML = `
      <div class="drill-panel">
        <div class="drill-head">
          <div class="drill-title">${humanStage[key] || key}</div>
          <div class="mono tiny">count=${z.count ?? "n/a"}</div>
        </div>
        <div class="drill-summary mono tiny muted">${z.summary || stageHelp[key] || "No detail available."}</div>
        ${key === "submitted" ? `<div class="mono tiny muted" style="margin-top:4px;">pending_unfilled=${z.pending_count ?? "n/a"} (same evidence family as Positions → Pending Orders)</div>` : ""}
        <table class="drill-table">
          <thead><tr><th>#</th><th>Evidence record</th><th>Action</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
    `;
    document.querySelectorAll("[data-funnel-evidence]").forEach(btn => {
      btn.onclick = async (ev) => {
        ev.preventDefault();
        const p = decodeURIComponent(btn.getAttribute("data-funnel-evidence") || "");
        await openEvidence(`funnel:${key}`, p);
      };
    });
    document.querySelectorAll("[data-funnel-stage]").forEach(x => {
      x.classList.toggle("is-selected", x.getAttribute("data-funnel-stage") === key);
    });
  };

  renderDrill("intents");
  document.querySelectorAll("[data-funnel-stage]").forEach(x => {
    x.onclick = () => renderDrill(x.getAttribute("data-funnel-stage") || "intents");
  });
}

function renderWhatChanged(payload) {
  const diffs = payload?.meta?.what_changed?.diff_from_prev_poll || [];
  const hash = payload?.meta?.what_changed?.key_fields_sha256 || "n/a";

  el("whatChangedHash").textContent = `key_fields_sha256=${hash}`;
  el("whatChangedList").innerHTML = diffs.map(d =>
    `<div class="changed-item"><span class="code">${d.code}</span> — ${d.summary}</div>`
  ).join("");

  // badge count (excluding NO_CHANGE)
  const actionable = diffs.filter(d => d.code !== "NO_CHANGE" && d.code !== "FIRST_LOAD").length;
  const badge = el("whatChangedBadge");
  if (actionable > 0) {
    badge.classList.remove("hidden");
    badge.textContent = `Δ ${actionable}`;
  } else {
    badge.classList.add("hidden");
    badge.textContent = "";
  }
}

function renderEngines(payload) {
  const engines = (payload?.engines || []);
  const grid = el("engineGrid");
  grid.innerHTML = "";

  engines.forEach(e => {
    const mode = e.mode || "UNKNOWN";
    const acct = e.ib_account_id || "n/a";
    const ea = (e.entries_allowed === true) ? "YES" : (e.entries_allowed === false) ? "NO" : "UNKNOWN";
    const fl = (e.flatten_only === true) ? "FLATTEN_ONLY" : "";
    const st = e.status || "UNKNOWN";

    const card = document.createElement("div");
    card.className = "engine-card";
    card.innerHTML = `
      <div class="engine-head">
        <div>
          <div class="engine-title">${e.engine_name} <span class="mono tiny muted">${e.engine_id}</span></div>
          <div class="engine-sub">
            <span class="${modeClass(mode, e.flatten_only)} mono">${String(mode).toUpperCase()}</span>
            <span class="mono tiny muted">acct=${acct}</span>
            <span class="mono tiny muted">entries=${ea}</span>
            ${fl ? `<span class="mono tiny muted">${fl}</span>` : ""}
          </div>
        </div>
        <div class="engine-status ${stateClass(st)}">${st}</div>
      </div>

      <div class="engine-body">
        <div class="kv"><div class="k">Intents today</div><div class="v">${e.today?.intents ?? "n/a"}</div></div>
        <div class="kv"><div class="k">Authorized today</div><div class="v">${e.today?.authorized ?? "n/a"}</div></div>
        <div class="kv"><div class="k">Submitted today</div><div class="v">${e.today?.submitted ?? "n/a"}</div></div>
        <div class="kv"><div class="k">Filled today</div><div class="v">${e.today?.filled ?? "n/a"}</div></div>

        <div class="kv"><div class="k">Open positions</div><div class="v">${e.positions?.open_count ?? "n/a"}</div></div>
        <div class="kv"><div class="k">Exposure net/gross</div><div class="v">${e.exposure?.net_pct ?? "n/a"} / ${e.exposure?.gross_pct ?? "n/a"}</div></div>

        <div class="kv"><div class="k">PnL today</div><div class="v">${e.pnl?.today ?? "n/a"}</div></div>
        <div class="kv"><div class="k">PnL cumulative</div><div class="v">${e.pnl?.cumulative ?? "n/a"}</div></div>

        <div class="kv"><div class="k">Applied Risk base%</div><div class="v">${e.applied_risk?.base_risk_pct ?? "n/a"}</div></div>
        <div class="kv"><div class="k">Vol-adjusted weight</div><div class="v">${e.applied_risk?.vol_adjusted_weight ?? "n/a"}</div></div>
        <div class="kv"><div class="k">Liquidity scalar</div><div class="v">${e.applied_risk?.liquidity_scalar ?? "n/a"}</div></div>
        <div class="kv"><div class="k">Correlation scalar</div><div class="v">${e.applied_risk?.correlation_scalar ?? "n/a"}</div></div>
        <div class="kv"><div class="k">Convex scalar</div><div class="v">${e.applied_risk?.convex_scalar ?? "n/a"}</div></div>
        <div class="kv"><div class="k">Final authorized weight/cap</div><div class="v">${e.applied_risk?.final_authorized_weight ?? "n/a"}</div></div>
        <div class="kv"><div class="k">Cash authority cap</div><div class="v">${e.applied_risk?.cash_authority_cap ?? "n/a"}</div></div>

        <details class="accordion">
          <summary>Details</summary>
          <pre>${JSON.stringify(e.details_collapsed || {}, null, 2)}</pre>
        </details>
      </div>
    `;
    grid.appendChild(card);
  });
}

function renderPortfolio(payload) {
  const p = payload?.portfolio || {};
  el("portfolioAsOf").textContent = p.asof_utc ? `asof=${p.asof_utc}` : "";
  const note = p.note_if_missing || "";
  el("portfolioNote").textContent = note;

  const items = [
    ["NAV total", p.nav_total],
    ["PnL today", p.pnl_today],
    ["PnL cumulative", p.pnl_cumulative],
    ["Drawdown %", p.drawdown_pct],
    ["Cash %", p.cash_pct],
    ["Net exposure %", p.net_exposure_pct],
    ["Gross exposure %", p.gross_exposure_pct],
    ["NAV artifact", p.nav_path || "n/a"],
  ];

  el("portfolioMetrics").innerHTML = items.map(([k,v]) => `
    <div class="metric">
      <div class="k">${k}</div>
      <div class="v">${fmt(v)}</div>
    </div>
  `).join("");
}

function renderHistory(payload) {
  const day = payload?.meta?.selected_day || "n/a";
  const attempts = payload?.meta?.attempts || [];
  const sel = payload?.meta?.selected_attempt_id || "n/a";
  const rows = payload?.meta?.attempt_summaries || [];
  el("historyExplain").textContent =
    "Each row is one orchestrator attempt for the selected UTC day, with final status and top reason codes.";
  const hdr = `
    <table style="width:100%; border-collapse:collapse;">
      <thead>
        <tr>
          <th style="text-align:left;">Attempt</th>
          <th style="text-align:left;">Seq</th>
          <th style="text-align:left;">Status</th>
          <th style="text-align:left;">Produced UTC</th>
          <th style="text-align:left;">Top Reasons</th>
        </tr>
      </thead>
      <tbody>
  `;
  const body = rows.length ? rows.map(r => `
    <tr>
      <td class="mono tiny">${r.attempt_id || "n/a"}${r.attempt_id === sel ? " (selected)" : ""}</td>
      <td class="mono tiny">${r.attempt_seq ?? "n/a"}</td>
      <td class="mono tiny">${r.status || "UNKNOWN"}</td>
      <td class="mono tiny">${r.produced_utc || "n/a"}</td>
      <td class="mono tiny">${(r.reason_codes || []).join(", ") || "n/a"}</td>
    </tr>
  `).join("") : `<tr><td colspan="5" class="mono tiny">ATTEMPTS_NOT_FOUND</td></tr>`;
  const ftr = `</tbody></table>`;
  el("historyAttempts").innerHTML = `
    <div class="mono small">day=${day}</div>
    <div class="mono small">selected_attempt=${sel}</div>
    <div class="mono small">rows=${rows.length || attempts.length || 0}</div>
    <div style="margin-top:8px;">${hdr}${body}${ftr}</div>
  `;
}

function renderTechnical(payload) {
  const prov = payload?.provenance || {};
  const warn = (prov.warnings || []).slice(0, 20);
  const miss = (prov.missing_paths || []).slice(0, 30);
  const src = (prov.source_paths || []).slice(0, 30);

  el("techSummary").innerHTML = `
    warnings=${warn.length}<br/>
    missing_paths=${(prov.missing_paths || []).length}<br/>
    source_paths=${(prov.source_paths || []).length}<br/>
  `;

  const mk = (arr, label) => {
    const rows = (arr || []).map(p => {
      const ep = encodeURIComponent(p);
      return `- <a href="#" class="mono" data-artifact="${ep}" data-title="${label}">${p}</a>`;
    }).join("<br/>");
    return rows || "n/a";
  };

  el("techPaths").innerHTML = `
    <div class="mono tiny muted">warnings:</div>
    <div class="mono tiny">${(warn || []).map(x => `- ${x}`).join("<br/>") || "n/a"}</div>
    <div class="mono tiny muted" style="margin-top:10px;">missing_paths (top):</div>
    <div class="mono tiny">${mk(miss, "missing_path")}</div>
    <div class="mono tiny muted" style="margin-top:10px;">source_paths (top):</div>
    <div class="mono tiny">${mk(src, "source_path")}</div>
  `;

  document.querySelectorAll("[data-artifact]").forEach(a => {
    a.onclick = async (ev) => {
      ev.preventDefault();
      const p = decodeURIComponent(a.getAttribute("data-artifact") || "");
      const title = a.getAttribute("data-title") || "artifact";
      await openEvidence(title, p);
    };
  });
}

function renderPositions(payload) {
  const pe = payload?.positions_exposure || {};
  const sum = pe?.summary || {};
  const positions = pe?.positions || [];
  const exp = pe?.exposure_by_engine || [];
  const src = pe?.sources || {};
  const of = pe?.order_flow || {};
  const ofSummary = of?.summary || {};
  const pendingOrders = of?.pending_orders || [];
  const orderRecords = of?.records || [];

  el("positionsAsOf").textContent = pe?.asof_utc ? `asof=${pe.asof_utc}` : "";
  el("positionsExplain").textContent = "Read-only positions and exposure for the selected day from canonical truth artifacts.";

  const pendingCnt = pendingOrders.length;
  const filledCnt = ofSummary.filled_orders ?? 0;
  const rejectedCnt = ofSummary.rejected_orders ?? 0;
  const notExecutedCnt = ofSummary.not_executed_orders ?? 0;
  if (pendingCnt > 0) {
    el("positionsLifecycleBanner").innerHTML = `<span class="status-chip status-amber">PENDING</span> ${pendingCnt} submitted order(s) are not filled yet.`;
  } else if (filledCnt > 0) {
    el("positionsLifecycleBanner").innerHTML = `<span class="status-chip status-green">FILLED</span> ${filledCnt} order(s) have fill evidence.`;
  } else if (rejectedCnt > 0) {
    el("positionsLifecycleBanner").innerHTML = `<span class="status-chip status-amber">REJECTED</span> ${rejectedCnt} order(s) were rejected.`;
  } else if (notExecutedCnt > 0) {
    el("positionsLifecycleBanner").innerHTML = `<span class="status-chip status-gray">NOT_EXECUTED</span> ${notExecutedCnt} submitted record(s) were not executed.`;
  } else {
    el("positionsLifecycleBanner").innerHTML = `<span class="status-chip status-blue">INFO</span> No submitted order records for selected day.`;
  }

  const metrics = [
    ["Positions total", sum.positions_total],
    ["Open positions", sum.open_positions],
    ["Pending orders", ofSummary.pending_orders],
    ["Filled orders", ofSummary.filled_orders],
    ["Rejected orders", ofSummary.rejected_orders],
    ["Not executed", ofSummary.not_executed_orders],
    ["Net notional USD", sum.portfolio_net_notional_usd],
    ["Gross notional USD", sum.portfolio_gross_notional_usd],
    ["Capital at risk (cents)", sum.capital_at_risk_cents],
    ["Symbols", sum.symbol_count],
  ];
  el("positionsSummary").innerHTML = metrics.map(([k,v]) => `
    <div class="metric"><div class="k">${k}</div><div class="v">${v ?? "n/a"}</div></div>
  `).join("");

  const posRows = positions.length ? positions.map(p => {
    const qty = p.qty ?? "n/a";
    const pstate = (p.status || "UNKNOWN").toUpperCase();
    const hint = (pstate === "OPEN" && Number(p.qty || 0) === 0)
      ? `<span class="status-chip status-amber">PENDING_OR_UNFILLED</span>`
      : (pstate === "OPEN" ? `<span class="status-chip status-green">ACTIVE</span>` : `<span class="status-chip status-gray">${pstate}</span>`);
    return `
    <tr>
      <td class="mono tiny">${p.position_id || "n/a"}</td>
      <td class="mono tiny">${p.engine_id || "n/a"}</td>
      <td class="mono tiny">${qty}</td>
      <td class="mono tiny">${p.status || "n/a"}</td>
      <td class="mono tiny">${p.market_exposure_type || "n/a"}</td>
      <td class="mono tiny">${hint}</td>
    </tr>
  `;
  }).join("") : `<tr><td colspan="6" class="mono tiny">No position rows for selected day.</td></tr>`;
  el("positionsTable").innerHTML = `
    <table style="width:100%; border-collapse:collapse;">
      <thead><tr><th style="text-align:left;">Position ID</th><th style="text-align:left;">Engine</th><th style="text-align:left;">Qty</th><th style="text-align:left;">Status</th><th style="text-align:left;">Exposure Type</th><th style="text-align:left;">Operator State</th></tr></thead>
      <tbody>${posRows}</tbody>
    </table>
    <div class="mono tiny muted" style="margin-top:6px;">positions_source=${src.positions_path || "n/a"}</div>
  `;

  el("pendingOrdersMeta").textContent = `pending=${pendingCnt} • submitted_total=${ofSummary.submitted_records ?? 0}`;
  el("pendingOrdersExplain").textContent = "Subset of funnel Submitted stage: records with submission evidence but no fill evidence yet.";
  const pendingRows = pendingOrders.length ? pendingOrders.map((r, i) => `
    <tr>
      <td class="mono tiny">${i + 1}</td>
      <td class="mono tiny">${r.engine_id || "n/a"}</td>
      <td class="mono tiny">${r.symbol || "n/a"}</td>
      <td class="mono tiny">${r.side || "n/a"}</td>
      <td class="mono tiny">${r.qty ?? "n/a"}</td>
      <td class="mono tiny">${r.submitted_status || "n/a"}</td>
      <td class="mono tiny">${r.submitted_at_utc || "n/a"}</td>
      <td class="mono tiny"><span class="status-chip status-amber">PENDING</span></td>
    </tr>
  `).join("") : `<tr><td colspan="8" class="mono tiny muted">No submitted-unfilled records for selected day.</td></tr>`;
  el("pendingOrdersTable").innerHTML = `
    <table style="width:100%; border-collapse:collapse;">
      <thead><tr><th style="text-align:left;">#</th><th style="text-align:left;">Engine</th><th style="text-align:left;">Symbol</th><th style="text-align:left;">Side</th><th style="text-align:left;">Planned Qty</th><th style="text-align:left;">Submit Status</th><th style="text-align:left;">Submitted UTC</th><th style="text-align:left;">State</th></tr></thead>
      <tbody>${pendingRows}</tbody>
    </table>
    <div class="mono tiny muted" style="margin-top:6px;">submissions_source=${src.submissions_root || "n/a"} • records=${orderRecords.length}</div>
  `;

  el("exposureSummary").innerHTML = `
    <div>portfolio net=${sum.portfolio_net_notional_usd ?? "n/a"} gross=${sum.portfolio_gross_notional_usd ?? "n/a"} capital_at_risk_cents=${sum.capital_at_risk_cents ?? "n/a"}</div>
    <div class="mono tiny muted">exposure_source=${src.exposure_path || "n/a"}</div>
  `;
  el("exposureBars").innerHTML = exp.length ? exp.map(r => `
    <div style="display:flex;justify-content:space-between;gap:8px;">
      <span class="mono tiny">${r.engine_id || "n/a"}</span>
      <span class="mono tiny">net=${r.net_notional_usd ?? "n/a"} gross=${r.gross_notional_usd ?? "n/a"} risk=${r.capital_at_risk_cents ?? "n/a"}</span>
    </div>
  `).join("") : `<div class="mono tiny muted">No per-engine exposure rows for selected day.</div>`;
}

async function openEvidence(title, path) {
  // Raw JSON only via modal (explicit click).
  const q = encodeURIComponent(path);
  const d = encodeURIComponent(state.day || "");
  const r = await api(`/api/artifact?path=${q}&day=${d}`);
  el("evidenceTitle").textContent = title || "Evidence";
  el("evidencePath").textContent = r.path || path || "n/a";
  el("evidenceErrors").innerHTML = (r.errors || []).length
    ? (r.errors || []).map(e => `ERROR: ${e}`).join("<br/>")
    : "";
  el("evidenceBody").textContent = r.content || "";
  el("evidenceModal").classList.remove("hidden");
}

function closeEvidence() {
  el("evidenceModal").classList.add("hidden");
}

function resetTimer() {
  if (state.timer) clearInterval(state.timer);
  state.timer = setInterval(async () => {
    await loadAndRender();
  }, state.refreshSec * 1000);
}

async function loadDays() {
  const d = await api("/api/days");
  state.days = d.days || [];
  state.day = d.default_day_utc || (state.days.length ? state.days[state.days.length - 1] : null);

  const sel = el("daySelect");
  sel.innerHTML = "";
  state.days.forEach(day => {
    const o = document.createElement("option");
    o.value = day;
    o.textContent = day;
    if (day === state.day) o.selected = true;
    sel.appendChild(o);
  });
}

async function loadAttemptsForDay(day) {
  if (!day) return;
  const a = await api(`/api/attempts?day=${encodeURIComponent(day)}`);
  state.attempts = a.attempts || [];
  state.attempt_id = a.recommended_attempt_id || (state.attempts.length ? state.attempts[state.attempts.length - 1] : null);

  const sel = el("attemptSelect");
  sel.innerHTML = "";
  // Allow empty attempt (latest on server)
  const o0 = document.createElement("option");
  o0.value = "";
  o0.textContent = "latest (auto)";
  sel.appendChild(o0);

  state.attempts.forEach(id => {
    const o = document.createElement("option");
    o.value = id;
    o.textContent = id;
    if (id === state.attempt_id) o.selected = true;
    sel.appendChild(o);
  });
}

async function loadAndRender() {
  if (!state.day) return;

  const attemptParam = (state.attempt_id && el("attemptSelect").value) ? `&attempt_id=${encodeURIComponent(el("attemptSelect").value)}` : "";
  const payload = await api(`/api/status_v2?day=${encodeURIComponent(state.day)}${attemptParam}`);
  state.statusV2 = payload;

  el("lastRefresh").textContent = `refreshed=${payload?.meta?.server_time_utc || "n/a"}`;

  renderPlatformReadiness(payload);
  renderPlatformReadinessHistory(payload);
  renderScopeHealth(payload);
  renderTiles(payload);
  renderSleeveStrip(payload);
  renderFunnel(payload);
  renderWhatChanged(payload);

  renderEngines(payload);
  renderPortfolio(payload);
  renderPositions(payload);
  renderHistory(payload);
  renderTechnical(payload);
}

function svgLineChart(points) {
  const W = 900, H = 260, pad = 28;
  const vals = (points || []).map(p => Number(p.nav_end)).filter(v => !isNaN(v));
  if (vals.length < 2) return `<div class="mono small muted">Insufficient NAV data</div>`;
  const min = Math.min(...vals);
  const max = Math.max(...vals);
  const span = (max - min) || 1;
  const xs = (points || []).map((_, i) => pad + (i * (W - 2*pad) / Math.max(1, (points || []).length-1)));
  const ys = (points || []).map(p => {
    const v = Number(p.nav_end);
    if (isNaN(v)) return null;
    return (H-pad) - ((v-min)/span)*(H-2*pad);
  });
  let d="";
  for (let i=0;i<(points || []).length;i++){
    if (ys[i]==null) continue;
    d += (d ? " L " : "M ") + xs[i] + " " + ys[i];
  }
  return `<svg viewBox="0 0 ${W} ${H}">
    <path d="${d}" fill="none" stroke="#7ee787" stroke-width="2"></path>
  </svg>`;
}

async function loadCharts() {
  const n = Number(el("navDays").value) || 60;
  const nav = await api(`/api/series/nav?days=${n}`);
  el("chartDailyNav").innerHTML = svgLineChart(nav.points || []);
}

function wire() {
  el("refreshSelect").addEventListener("change", () => {
    state.refreshSec = Number(el("refreshSelect").value) || 60;
    resetTimer();
  });

  el("daySelect").addEventListener("change", async () => {
    state.day = el("daySelect").value;
    await loadAttemptsForDay(state.day);
    await loadAndRender();
  });

  el("attemptSelect").addEventListener("change", async () => {
    state.attempt_id = el("attemptSelect").value || null;
    await loadAndRender();
  });

  el("btnLatest").addEventListener("click", async () => {
    if ((state.days || []).length) {
      state.day = state.days[state.days.length - 1];
      el("daySelect").value = state.day;
    }
    await loadAttemptsForDay(state.day);
    el("attemptSelect").value = "";
    state.attempt_id = null;
    await loadAndRender();
  });

  el("tabOperations").addEventListener("click", () => setView("operations"));
  el("tabEngines").addEventListener("click", () => setView("engines"));
  el("tabPortfolio").addEventListener("click", () => setView("portfolio"));
  el("tabPositions").addEventListener("click", () => setView("positions"));
  el("tabHistory").addEventListener("click", () => setView("history"));
  el("tabTechnical").addEventListener("click", () => setView("technical"));

  el("btnEvidenceClose").addEventListener("click", closeEvidence);
  el("evidenceBackdrop").addEventListener("click", closeEvidence);

  el("btnReloadCharts").addEventListener("click", loadCharts);
}

(async function boot() {
  wire();
  await loadDays();
  await loadAttemptsForDay(state.day);
  setView("operations");
  await loadAndRender();
  await loadCharts();
  resetTimer();
})();
