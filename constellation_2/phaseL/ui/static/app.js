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

const STANDARD_TRADING_SLEEVE_IDS = [
  "C2_CROSS_ASSET_TREND",
  "C2_DEFENSIVE_TAIL",
  "C2_EVENT_DISLOCATION",
  "C2_MARKET_NEUTRAL_SPREAD",
  "C2_MEAN_REVERSION_EQ",
  "C2_TREND_EQ_PRIMARY",
  "C2_VOL_INCOME_DEFINED_RISK",
];

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
  if (u === "PASS" || u === "READY" || u === "OK") return "state-pass";
  if (u === "DEGRADED") return "state-degraded";
  if (u === "FAIL" || u === "BLOCKED") return "state-fail";
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

function toneClass(tone) {
  const t = String(tone || "neutral").toLowerCase();
  if (t === "positive" || t === "pass" || t === "ready") return "tone-positive";
  if (t === "warning" || t === "warn" || t === "partial") return "tone-warning";
  if (t === "negative" || t === "fail" || t === "missing") return "tone-negative";
  if (t === "info") return "tone-info";
  return "tone-neutral";
}

function renderTiles(payload) {
  const tiles = (payload?.ops_health?.tiles || []);
  const scope = payload?.scope_health || {};
  const execState = String(scope?.sleeve_execution_health?.status || "UNKNOWN").toUpperCase();
  const monState = String(scope?.system_monitoring_health?.status || "UNKNOWN").toUpperCase();
  const overallState = String(scope?.overall?.status || "UNKNOWN").toUpperCase();
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
    const rawState = String(t.state || "UNKNOWN").toUpperCase();
    let st = rawState;
    let stop = (rawState === "ABORTED");
    const last = t.last_updated_utc || "n/a";
    const rc = (t.reason_codes || []).slice(0,2).join(", ") || "n/a";
    const path = t.artifact_ref?.path;
    const normalizedAbort = rawState === "ABORTED" && execState === "PASS" &&
      (t.tile_id === "orchestrator_run_verdict_v2" || t.tile_id === "safety_breach");

    let human;
    let note = "";
    if (normalizedAbort && t.tile_id === "orchestrator_run_verdict_v2") {
      st = "DEGRADED";
      stop = false;
      human = "GOVERNED ABORT — EXECUTION PASS";
      note = `raw_verdict=ABORTED - execution=${execState} - monitoring=${monState} - overall=${overallState}`;
    } else if (normalizedAbort && t.tile_id === "safety_breach") {
      st = "DEGRADED";
      stop = false;
      human = "NO EXECUTION HARD STOP";
      note = `raw_abort preserved as evidence - normalized execution=${execState}`;
    } else {
      human = (st === "PASS") ? "COMPLETED — PASS"
        : (st === "DEGRADED") ? "COMPLETED — DEGRADED"
        : (st === "FAIL") ? "RUN COMPLETED — FAIL"
        : (st === "ABORTED") ? "SAFETY BREACH — STOP"
        : st;
    }


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
        ${note ? `<div class="mono tiny muted">normalized=${note}</div>` : ""}
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

function numOrNull(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function formatPointsOutOf100(v) {
  const n = numOrNull(v);
  return n === null ? "n/a" : `${n} / 100`;
}

function formatMetricDisplay(view, fallback, unitHint) {
  if (view && typeof view.display_value === "string" && view.display_value.trim()) return view.display_value;
  const n = numOrNull(fallback);
  if (n === null) return "n/a";
  if (unitHint === "percent_bp") return `${(n / 100).toFixed(2)}%`;
  if (unitHint === "rate_x100") return `${(n / 100).toFixed(2)}/day`;
  return String(n);
}

function titleizeCheckId(v) {
  return String(v || "")
    .split("_")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function renderPlatformReadiness(payload) {
  const platform = payload?.platform_readiness && typeof payload.platform_readiness === "object" ? payload.platform_readiness : {};
  const bug = payload?.platform_bug_metrics && typeof payload.platform_bug_metrics === "object" ? payload.platform_bug_metrics : {};
  const policy = payload?.platform_readiness_policy && typeof payload.platform_readiness_policy === "object" ? payload.platform_readiness_policy : {};
  const pm = platform?.metric_views || {};
  const bm = bug?.metric_views || {};
  const hasReadinessPayload = platform?.present === true;
  const hasBugPayload = bug?.present === true;
  const hasPolicyPayload = policy?.present === true;

  const state = String(platform?.platform_readiness_state || "UNKNOWN").toUpperCase();
  const grade = platform?.platform_readiness_grade ?? "n/a";
  const score = numOrNull(platform?.platform_readiness_score);
  const threshold = numOrNull(platform?.score_threshold_ready);
  const candidate = platform?.platform_promotion_candidate === true ? "YES"
    : platform?.platform_promotion_candidate === false ? "NO"
    : "UNKNOWN";
  const summary = platform?.readiness_summary || "n/a";
  const decision = platform?.promotion_decision_basis || "n/a";
  const rootBlockers = Array.isArray(platform?.root_blockers) ? platform.root_blockers : [];
  const derivedBlockers = Array.isArray(platform?.derived_blockers) ? platform.derived_blockers : [];
  const contributionRows = Array.isArray(platform?.score_contribution) ? platform.score_contribution : [];
  const gradeBands = Array.isArray(policy?.grade_bands) ? policy.grade_bands : [];
  const thresholdPolicy = platform?.policy_values || {};
  const hardBlockers = thresholdPolicy?.hard_blockers || {};
  const maxVelocity = numOrNull(hardBlockers?.max_bug_velocity_7d_avg_for_candidate);
  const maxRecurrence = numOrNull(hardBlockers?.max_recurrence_rate_for_candidate);
  const producedUtc = platform?.produced_utc || "n/a";
  const bugProducedUtc = bug?.produced_utc || "n/a";
  const readinessPath = platform?.path || "n/a";
  const bugPath = bug?.path || "n/a";
  const policyPath = policy?.path || thresholdPolicy?.policy_path || "n/a";
  const scoreDisplay = formatPointsOutOf100(score);
  const thresholdDisplay = threshold === null ? "n/a" : String(threshold);
  const velocityValue = numOrNull(bug?.bug_velocity_7d_avg);
  const recurrenceValue = numOrNull(bug?.recurrence_rate);
  const stabilityValue = numOrNull(bug?.diagnostic_stability_rate);
  const newBugsValue = numOrNull(bug?.new_bug_events_today);
  const velocityDisplay = formatMetricDisplay(pm?.bug_velocity_7d_avg || bm?.bug_velocity_7d_avg, bug?.bug_velocity_7d_avg, "rate_x100");
  const recurrenceDisplay = formatMetricDisplay(pm?.recurrence_rate || bm?.recurrence_rate, bug?.recurrence_rate, "percent_bp");
  const stabilityDisplay = formatMetricDisplay(pm?.diagnostic_stability_rate || bm?.diagnostic_stability_rate, bug?.diagnostic_stability_rate, "percent_bp");
  const newBugsDisplay = formatMetricDisplay(bm?.new_bug_events_today, bug?.new_bug_events_today, null);
  const bugTrend = String(bug?.bug_velocity_trend || "UNKNOWN");
  const recurringKeys = Array.isArray(bug?.recurring_bug_events) ? bug.recurring_bug_events : [];
  const evidencePaths = [...new Set([
    ...(Array.isArray(platform?.evidence_paths) ? platform.evidence_paths : []),
    ...(Array.isArray(bug?.evidence_paths) ? bug.evidence_paths : []),
  ].filter(Boolean))];
  const readinessSourceNote = hasReadinessPayload
    ? (platform?.requested_day_present === true
      ? `readiness_day=${platform?.resolved_day || payload?.meta?.selected_day || "n/a"}`
      : platform?.resolved_via_latest_pointer
        ? `readiness_fallback=${platform?.resolved_day || "latest"}`
        : "readiness_day=missing")
    : "readiness=missing";
  const bugSourceNote = hasBugPayload
    ? (bug?.requested_day_present === true
      ? `bug_metrics_day=${bug?.resolved_day || payload?.meta?.selected_day || "n/a"}`
      : bug?.resolved_via_latest_pointer
        ? `bug_metrics_fallback=${bug?.resolved_day || "latest"}`
        : "bug_metrics_day=missing")
    : "bug_metrics=missing";
  const note = threshold !== null ? "READY when score >= threshold" : "Threshold rule unavailable";
  const velocityBand = velocityValue === null
    ? "Unavailable"
    : velocityValue < 100
      ? "Stable"
      : velocityValue <= 300
        ? "Watch"
        : "Unstable";
  const velocityBandClass = velocityBand === "Stable"
    ? "tone-positive"
    : velocityBand === "Watch"
      ? "tone-warning"
      : velocityBand === "Unstable"
        ? "tone-negative"
        : "tone-neutral";
  const recentDirection = bugTrend === "WORSENING"
    ? (velocityBand === "Stable" ? "Slight uptick" : "Higher vs prior window")
    : bugTrend === "IMPROVING"
      ? "Lower vs prior window"
      : bugTrend === "STABLE"
        ? "Flat vs prior window"
        : "Current window only";
  const metricToneClass = (awarded, weight) => {
    if (awarded === null || weight === null) return "tone-neutral";
    if (awarded <= 0) return "tone-negative";
    if (awarded >= weight) return "tone-positive";
    return "tone-warning";
  };
  const formatThresholdMetric = (value, kind) => {
    if (value === null) return "Missing from payload";
    if (kind === "rate") return `${(value / 100).toFixed(2)} bugs/day`;
    if (kind === "percent") return `${(value / 100).toFixed(2)}%`;
    return String(value);
  };
  const gradeClass = `platform-grade-tone-${String(grade || "unknown").toLowerCase().replace(/[^a-z0-9_-]+/g, "-")}`;
  const candidateClass = candidate === "YES" ? "state-pass" : candidate === "NO" ? "state-fail" : "state-unknown";

  const gradeLegend = gradeBands.length
    ? gradeBands.map((band, idx) => {
        const currentMin = numOrNull(band?.min_score);
        const priorMin = idx > 0 ? numOrNull(gradeBands[idx - 1]?.min_score) : null;
        let range = "n/a";
        if (currentMin !== null && priorMin !== null) range = `${currentMin}-${priorMin - 1}`;
        else if (currentMin !== null) range = `${currentMin}-100`;
        const isCurrentBand = String(band?.grade || "").toUpperCase() === String(grade || "").toUpperCase();
        return `
          <div class="legend-row ${isCurrentBand ? "is-active" : ""}">
            <span class="legend-grade">${escapeHtml(band?.grade || "n/a")}</span>
            <span class="legend-range">${escapeHtml(range)}</span>
          </div>
        `;
      }).join("")
    : `<div class="mono tiny muted">Grade is sourced from the platform readiness model.</div>`;

  const breakdown = contributionRows.length
    ? contributionRows.map((row) => {
        const checkId = String(row?.check_id || "unknown");
        const weight = numOrNull(row?.weight);
        const awarded = numOrNull(row?.score_awarded);
        const checkState = String(row?.status || "UNKNOWN").toUpperCase();
        return `
          <div class="breakdown-row">
            <div class="breakdown-main">
              <div class="breakdown-title">${escapeHtml(titleizeCheckId(checkId))}</div>
              <div class="mono tiny muted">${escapeHtml(checkId)}</div>
              <div class="breakdown-bar-track">
                <div class="breakdown-bar-fill ${metricToneClass(awarded, weight)}" style="width:${escapeHtml(weight && awarded !== null ? `${Math.max(0, Math.min(100, (awarded / weight) * 100)).toFixed(0)}` : "0")}%"></div>
              </div>
            </div>
            <div class="breakdown-score">
              <div class="mono breakdown-points ${metricToneClass(awarded, weight)}">${escapeHtml(awarded === null || weight === null ? "n/a" : `${awarded} / ${weight}`)}</div>
              <div class="tiny breakdown-status ${stateClass(checkState)}">${escapeHtml(checkState)}</div>
            </div>
          </div>
        `;
      }).join("")
    : `<div class="mono tiny muted">No score contribution rows found.</div>`;

  const derivedHtml = derivedBlockers.length
    ? derivedBlockers.map((item) => `<div class="info-pill info-pill-warn">${escapeHtml(item.replaceAll("_", " "))}</div>`).join("")
    : `<div class="mono tiny muted">No derived warnings.</div>`;

  const recurringHtml = recurringKeys.length
    ? recurringKeys.map((row) => `
        <div class="info-row">
          <span class="mono tiny">${escapeHtml(row?.recurrence_key || "UNKNOWN")}</span>
          <span class="mono tiny muted">count=${escapeHtml(row?.count ?? "n/a")}</span>
        </div>
      `).join("")
    : `<div class="mono tiny muted">No recurring bug keys in current artifact.</div>`;

  el("platformReadinessMeta").textContent = hasReadinessPayload && hasBugPayload
    ? `readiness=present • bug_metrics=present • policy=${hasPolicyPayload ? "present" : "partial"} • ${readinessSourceNote} • ${bugSourceNote} • produced=${producedUtc}`
    : `partial readiness payload • ${readinessSourceNote} • ${bugSourceNote} • produced=${producedUtc}`;
  el("platformHeroRow").className = "platform-kpi-grid";
  el("platformHeroRow").innerHTML = `
    <div class="metric platform-kpi-card platform-kpi-status">
      <div class="k">Status</div>
      <div class="v platform-kpi-value ${stateClass(state)}">${state}</div>
      <div class="platform-kpi-note">Platform readiness state</div>
    </div>
    <div class="metric metric-hero platform-kpi-card">
      <div class="k">Platform Score</div>
      <div class="v platform-kpi-value platform-score">${scoreDisplay}</div>
      <div class="platform-kpi-note">${note}</div>
    </div>
    <div class="metric platform-kpi-card platform-kpi-threshold">
      <div class="k">Readiness Threshold</div>
      <div class="v platform-kpi-value">${thresholdDisplay}</div>
      <div class="platform-kpi-note">Governed policy threshold</div>
    </div>
    <div class="metric platform-kpi-card">
      <div class="k">Platform Grade</div>
      <div class="v platform-kpi-value platform-grade ${gradeClass}">${grade}</div>
      <div class="platform-kpi-note">Governed score band</div>
    </div>
    <div class="metric platform-kpi-card platform-kpi-candidate">
      <div class="k">Promotion Candidate</div>
      <div class="v platform-kpi-value ${candidateClass}">${candidate}</div>
      <div class="platform-kpi-note">Current operator recommendation</div>
    </div>
  `;
  el("platformSummaryRow").className = "platform-summary-stack";
  el("platformSummaryRow").innerHTML = `
    <div class="platform-section-grid">
      <div class="platform-section section-positive">
        <div class="section-title">Why Ready</div>
        <div class="section-body">${escapeHtml(summary)}</div>
        <div class="section-subnote">${escapeHtml(decision)}</div>
      </div>
      <div class="platform-section section-info">
        <div class="section-title">Grade Scale</div>
        <div class="legend-grid">${gradeLegend}</div>
        <div class="section-subnote">source=${escapeHtml(policyPath)}</div>
      </div>
    </div>
    <div class="platform-section-grid">
      <div class="platform-section section-accent">
        <div class="section-title">Score Breakdown</div>
        <div class="breakdown-list">${breakdown}</div>
        <div class="breakdown-total mono">Total: ${escapeHtml(scoreDisplay)}</div>
      </div>
      <div class="platform-section section-info">
        <div class="section-title">Bug Stability Metrics</div>
        <div class="bug-metrics-grid">
          <div class="bug-metric-card">
            <div class="bug-metric-label">7-day bug velocity</div>
            <div class="bug-metric-value ${velocityBandClass}">${escapeHtml(velocityDisplay)}</div>
            <div class="bug-metric-unit">bugs/day</div>
          </div>
          <div class="bug-metric-card">
            <div class="bug-metric-label">Recurrence rate</div>
            <div class="bug-metric-value ${recurrenceValue !== null && recurrenceValue <= (maxRecurrence ?? recurrenceValue) ? "tone-positive" : "tone-warning"}">${escapeHtml(recurrenceDisplay)}</div>
            <div class="bug-metric-unit">%</div>
          </div>
          <div class="bug-metric-card">
            <div class="bug-metric-label">Diagnostic stability rate</div>
            <div class="bug-metric-value ${stabilityValue !== null && stabilityValue >= 5000 ? "tone-warning" : "tone-neutral"}">${escapeHtml(stabilityDisplay)}</div>
            <div class="bug-metric-unit">%</div>
          </div>
          <div class="bug-metric-card">
            <div class="bug-metric-label">New bugs today</div>
            <div class="bug-metric-value ${newBugsValue !== null && newBugsValue > 0 ? "tone-warning" : "tone-positive"}">${escapeHtml(newBugsDisplay)}</div>
            <div class="bug-metric-unit">events</div>
          </div>
        </div>
        <div class="info-row info-row-emphasis">
          <span>Velocity band</span>
          <span class="mono ${velocityBandClass}">${escapeHtml(velocityBand)}</span>
        </div>
        <div class="info-row">
          <span>Recent direction</span>
          <span class="mono">${escapeHtml(recentDirection)}</span>
        </div>
      </div>
    </div>
    <div class="platform-section-grid">
      <div class="platform-section section-info">
        <div class="section-title">Policy Thresholds</div>
        <div class="info-row"><span>Readiness score threshold</span><span class="mono tone-info">${escapeHtml(thresholdDisplay === "n/a" ? "Missing from payload" : thresholdDisplay)}</span></div>
        <div class="info-row"><span>Max 7-day bug velocity</span><span class="mono tone-info">${escapeHtml(formatThresholdMetric(maxVelocity, "rate"))}</span></div>
        <div class="info-row"><span>Max recurrence rate</span><span class="mono tone-info">${escapeHtml(formatThresholdMetric(maxRecurrence, "percent"))}</span></div>
      </div>
      <div class="platform-section section-warning">
        <div class="section-title">Derived Warnings</div>
        <div class="info-pills">${derivedHtml}</div>
        <div class="section-subnote">READY can coexist with derived warnings when root blockers are clear.</div>
      </div>
    </div>
    <details class="evidence-details">
      <summary>Evidence</summary>
      <div class="evidence-grid">
        <div class="evidence-item">
          <div class="evidence-label">Readiness artifact</div>
          <div class="evidence-value mono">${escapeHtml(readinessPath)}</div>
        </div>
        <div class="evidence-item">
          <div class="evidence-label">Readiness produced UTC</div>
          <div class="evidence-value mono muted">${escapeHtml(producedUtc)}</div>
        </div>
        <div class="evidence-item">
          <div class="evidence-label">Bug metrics artifact</div>
          <div class="evidence-value mono">${escapeHtml(bugPath)}</div>
        </div>
        <div class="evidence-item">
          <div class="evidence-label">Bug metrics produced UTC</div>
          <div class="evidence-value mono muted">${escapeHtml(bugProducedUtc)}</div>
        </div>
        <div class="evidence-item">
          <div class="evidence-label">Artifact Resolution</div>
          <div class="evidence-value mono">${escapeHtml(readinessSourceNote)}<br>${escapeHtml(bugSourceNote)}</div>
        </div>
        <div class="evidence-item">
          <div class="evidence-label">Root blockers</div>
          <div class="evidence-value mono">${escapeHtml(rootBlockers.length ? rootBlockers.join(", ") : "none")}</div>
        </div>
        <div class="evidence-item">
          <div class="evidence-label">Derived blockers</div>
          <div class="evidence-value mono">${escapeHtml(derivedBlockers.length ? derivedBlockers.join(", ") : "none")}</div>
        </div>
        <div class="evidence-item">
          <div class="evidence-label">Policy path</div>
          <div class="evidence-value mono">${escapeHtml(policyPath)}</div>
        </div>
      </div>
      <div class="evidence-path-list mono tiny">${evidencePaths.map((p) => `<div>${escapeHtml(p)}</div>`).join("") || "<div>none</div>"}</div>
      <div class="section-title" style="margin-top:8px;">Recurring Bug Keys</div>
      <div>${recurringHtml}</div>
    </details>
  `;
}

function renderSignalActivity(payload) {
  const host = el("signalActivityCard");
  if (!host) return;
  const signal = payload?.signal_activity && typeof payload.signal_activity === "object" ? payload.signal_activity : {};
  const hb = signal?.engine_heartbeats || {};
  const intents = signal?.intents || {};
  const phasec = signal?.phasec_outcomes || {};
  const submit = signal?.governed_submit || {};
  const upstream = signal?.upstream_data_status || {};
  const expected = numOrNull(hb?.expected_count) ?? 0;
  const present = numOrNull(hb?.present_count) ?? 0;
  const missingEngines = Array.isArray(hb?.missing_engine_ids) ? hb.missing_engine_ids : [];
  const intentCount = numOrNull(intents?.count) ?? 0;
  const vetoCount = numOrNull(phasec?.veto_count) ?? 0;
  const releasedCount = numOrNull(phasec?.released_identity_dir_count) ?? 0;
  const upstreamSymbols = Array.isArray(upstream?.symbols) ? upstream.symbols : [];

  host.innerHTML = `
    <div class="card-head">
      <div class="card-title">Signal Activity</div>
      <div class="mono tiny muted">day=${escapeHtml(signal?.day_utc || payload?.meta?.selected_day || "n/a")}</div>
    </div>
    <div class="signal-grid">
      <div class="signal-card">
        <div class="signal-label">Engine Heartbeats</div>
        <div class="signal-value ${present >= expected && expected > 0 ? "tone-positive" : present > 0 ? "tone-warning" : "tone-negative"}">${escapeHtml(`${present} / ${expected}`)}</div>
        <div class="signal-subnote">${missingEngines.length ? `missing=${escapeHtml(missingEngines.join(", "))}` : "all expected engines present"}</div>
      </div>
      <div class="signal-card">
        <div class="signal-label">Intents</div>
        <div class="signal-value ${intentCount > 0 ? "tone-positive" : "tone-warning"}">${escapeHtml(String(intentCount))}</div>
        <div class="signal-subnote">${escapeHtml(intents?.label || "No real intents produced")}</div>
      </div>
      <div class="signal-card">
        <div class="signal-label">Phase C Outcomes</div>
        <div class="signal-value ${toneClass(phasec?.tone)}">${escapeHtml(phasec?.label || "no phaseC outputs")}</div>
        <div class="signal-subnote">vetoes=${escapeHtml(String(vetoCount))} • released_identities=${escapeHtml(String(releasedCount))}</div>
      </div>
      <div class="signal-card">
        <div class="signal-label">Governed Submit</div>
        <div class="signal-value ${toneClass(submit?.tone)}">${escapeHtml(submit?.stage_status || "NOT_REACHED")}</div>
        <div class="signal-subnote">${escapeHtml(submit?.label || "governed submit not reached")}</div>
      </div>
      <div class="signal-card signal-card-wide">
        <div class="signal-label">Upstream Data Status</div>
        <div class="signal-value ${toneClass(upstream?.tone)}">${escapeHtml(upstream?.label || "upstream data incomplete")}</div>
        <div class="signal-symbol-grid">
          ${upstreamSymbols.map((row) => `
            <div class="signal-symbol-row">
              <span class="mono">${escapeHtml(row?.symbol || "n/a")}</span>
              <span class="${toneClass(row?.tone)}">${escapeHtml(row?.same_day_present ? "same-day row present" : "same-day row missing")}</span>
            </div>
          `).join("") || `<div class="mono tiny muted">No symbol readiness rows found.</div>`}
        </div>
      </div>
    </div>
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
  const areaPath = path
    ? `${path} L ${indexToX(history.length - 1)} ${H - padB} L ${indexToX(0)} ${H - padB} Z`
    : "";

  return `
    <svg class="history-chart" viewBox="0 0 ${W} ${H}" role="img" aria-label="Platform readiness score history">
      ${grid}
      ${thresholdSvg}
      <path class="history-score-area" d="${areaPath}"></path>
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
  const comparison = historyPayload?.comparison || {};
  const meta = historyPayload?.present
    ? `canonical_days=${history.length} • range=${dateRange?.start || "n/a"}..${dateRange?.end || "n/a"} • root=${historyPayload?.root || "n/a"}`
    : `canonical_days=0 • root=${historyPayload?.root || "n/a"}`;
  const scoreChange = Number(comparison?.score_change);
  const scoreChangeDisplay = Number.isFinite(scoreChange)
    ? `${scoreChange > 0 ? "+" : ""}${scoreChange}`
    : "n/a";
  const scoreChangeClass = Number.isFinite(scoreChange)
    ? (scoreChange > 0 ? "history-change-positive" : scoreChange < 0 ? "history-change-negative" : "history-change-neutral")
    : "history-change-neutral";
  const comparisonHtml = comparison?.present
    ? `
      <div class="history-summary-grid">
        <div class="history-summary-card">
          <div class="history-summary-label">Canonical Days</div>
          <div class="history-summary-value">${escapeHtml(String(history.length))}</div>
          <div class="mono tiny muted">${escapeHtml(dateRange?.start || "n/a")} to ${escapeHtml(dateRange?.end || "n/a")}</div>
        </div>
        <div class="history-summary-card">
          <div class="history-summary-label">Score Change</div>
          <div class="history-summary-value ${scoreChangeClass}">${escapeHtml(scoreChangeDisplay)}</div>
          <div class="mono tiny muted">latest vs prior canonical day</div>
        </div>
        <div class="history-summary-card">
          <div class="history-summary-label">Grade Change</div>
          <div class="history-summary-value">${escapeHtml(comparison?.grade_change?.from || "n/a")} → ${escapeHtml(comparison?.grade_change?.to || "n/a")}</div>
          <div class="mono tiny muted">governed score bands</div>
        </div>
        <div class="history-summary-card">
          <div class="history-summary-label">State Change</div>
          <div class="history-summary-value">${escapeHtml(comparison?.state_change?.from || "UNKNOWN")} → ${escapeHtml(comparison?.state_change?.to || "UNKNOWN")}</div>
          <div class="mono tiny muted">${escapeHtml(comparison?.previous_day || "n/a")} to ${escapeHtml(comparison?.latest_day || "n/a")}</div>
        </div>
      </div>
      <div class="history-comparison-grid">
        <div class="metric platform-kpi-card">
          <div class="k">Score Change</div>
          <div class="v ${scoreChangeClass}">${escapeHtml(scoreChangeDisplay)}</div>
          <div class="mono tiny muted">vs ${escapeHtml(comparison?.previous_day || "n/a")} → ${escapeHtml(comparison?.latest_day || "n/a")}</div>
        </div>
        <div class="metric platform-kpi-card">
          <div class="k">Grade Change</div>
          <div class="v">${escapeHtml(comparison?.grade_change?.from || "n/a")} → ${escapeHtml(comparison?.grade_change?.to || "n/a")}</div>
          <div class="mono tiny muted">adjacent canonical days</div>
        </div>
        <div class="metric platform-kpi-card">
          <div class="k">State Change</div>
          <div class="v">${escapeHtml(comparison?.state_change?.from || "UNKNOWN")} → ${escapeHtml(comparison?.state_change?.to || "UNKNOWN")}</div>
          <div class="mono tiny muted">${escapeHtml(comparison?.previous_day || "n/a")} to ${escapeHtml(comparison?.latest_day || "n/a")}</div>
        </div>
      </div>
    `
    : `
      <div class="history-comparison-empty mono tiny muted">
        Improvement Trend: waiting for additional canonical days
      </div>
    `;

  const rows = history.length
    ? history.map((row) => `
        <tr>
          <td>${escapeHtml(row.day)}</td>
          <td>${escapeHtml(row.score)}</td>
          <td>${escapeHtml(row.grade || "n/a")}</td>
          <td class="${stateClass(row.state)}">${escapeHtml(row.state || "UNKNOWN")}</td>
          <td>${escapeHtml(row.threshold)}</td>
          <td>${escapeHtml(row.produced_utc || "n/a")}</td>
        </tr>
      `).join("")
    : `<tr><td colspan="6" class="muted">No canonical platform readiness history artifacts found.</td></tr>`;

  host.innerHTML = `
    <div class="history-card">
      <div class="card-head">
        <div class="card-title">Platform Readiness History</div>
        <div class="mono tiny muted">${meta}</div>
      </div>
      ${comparisonHtml}
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
            <th>Produced UTC</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
  `;
}

function renderSleeveStrip(payload) {
  const sleeves = [...(payload?.sleeves || [])];
  const liveReady = payload?.sleeve_live_readiness || {};
  const container = el("sleeveStrip");
  const standardSleeveIds = new Set(STANDARD_TRADING_SLEEVE_IDS);
  const standardSleeves = STANDARD_TRADING_SLEEVE_IDS
    .map((sleeveId) => sleeves.find((s) => String(s?.sleeve_id || "").toUpperCase() === sleeveId))
    .filter(Boolean);
  const blockerCount = liveReady?.aggregate_blocker_summary?.total_blocker_count;
  const withAcct = standardSleeves.filter(s => s && s.ib_account_id).length;
  const unexpectedSleeves = sleeves
    .map(s => String(s?.sleeve_id || "").toUpperCase())
    .filter(sleeveId => sleeveId && !standardSleeveIds.has(sleeveId));
  const byAcct = {};
  standardSleeves.forEach(s => {
    const acct = s?.ib_account_id || "n/a";
    byAcct[acct] = (byAcct[acct] || 0) + 1;
  });
  const split = Object.entries(byAcct).map(([k,v]) => `${k}:${v}`).join(" | ");

  container.innerHTML = `
    <div class="card-head">
      <div class="card-title">Sleeve Inventory</div>
      <div class="mono tiny muted">rows=${standardSleeves.length} • rendered=${standardSleeves.length} • with_account=${withAcct} • ${split || "no_accounts"}</div>
    </div>
    <div class="mono tiny muted" style="margin-bottom:6px;">Standard sleeve roster only (7 sleeves). Bond sleeve is shown in its own first-class section.</div>
    ${unexpectedSleeves.length ? `<div class="mono tiny muted" style="margin-bottom:6px;">Excluded non-trading sleeves: ${unexpectedSleeves.join(", ")}</div>` : ""}
    <div class="strip-row" id="stripRowStandard"></div>
  `;
  const rowStandard = document.getElementById("stripRowStandard");
  const gradeChipClass = (g) => {
    const u = String(g || "").toUpperCase();
    if (u === "A") return "grade-chip-a";
    if (u === "B") return "grade-chip-b";
    if (u === "C") return "grade-chip-c";
    if (u === "D") return "grade-chip-d";
    if (u === "F") return "grade-chip-f";
    return "grade-chip-unknown";
  };
  const statusChipClass = (readyVal) => {
    if (readyVal === "READY") return "ready-chip-yes";
    if (readyVal === "NOT READY") return "ready-chip-no";
    return "ready-chip-unknown";
  };
  const scoreToneClass = (g) => {
    const u = String(g || "").toUpperCase();
    if (u === "A") return "score-tone-a";
    if (u === "B") return "score-tone-b";
    if (u === "C") return "score-tone-c";
    if (u === "D") return "score-tone-d";
    if (u === "F") return "score-tone-f";
    return "score-tone-unknown";
  };
  standardSleeves.forEach(s => {
    const mode = s.mode || "UNKNOWN";
    const acct = s.ib_account_id || "n/a";
    const sleeveReady = (s?.sleeve_live_readiness && typeof s.sleeve_live_readiness === "object")
      ? s.sleeve_live_readiness
      : liveReady;
    const grade = sleeveReady?.readiness_grade ?? sleeveReady?.grade_band ?? "n/a";
    const score = sleeveReady?.readiness_score ?? "n/a";
    const threshold = sleeveReady?.score_threshold ?? "n/a";
    const ready = (sleeveReady?.promotion_candidate === true) ? "READY"
      : (sleeveReady?.promotion_candidate === false) ? "NOT READY"
      : "UNKNOWN";
    const gradeCls = gradeChipClass(grade);
    const statusCls = statusChipClass(ready);
    const scoreCls = scoreToneClass(grade);
    const ea = (s.entries_allowed === true) ? "ENTRIES: YES"
      : (s.entries_allowed === false) ? "ENTRIES: NO"
      : "ENTRIES: UNKNOWN";
    const fl = (s.flatten_only === true) ? "FLATTEN_ONLY" : "";
    const healthStatus = "UNKNOWN";
    const healthChip = "status-gray";
    const blockerText = Number.isFinite(Number(blockerCount)) ? String(blockerCount) : "n/a";
    const pill = document.createElement("div");
    pill.className = "sleeve-shell";
    pill.innerHTML = `
      <div class="sleeve-shell-head">
        <span class="mono sleeve-name">${s.sleeve_id}</span>
        <span class="status-chip ${healthChip}">Health ${healthStatus}</span>
      </div>
      <div class="sleeve-readiness-row">
        <span class="mode-chip ${modeClass(mode, s.flatten_only)} mono">${String(mode).toUpperCase()}</span>
        <span class="acct-chip mono">IB ${acct}</span>
        <span class="readiness-chip grade-chip ${gradeCls} mono">Grade ${grade}</span>
        <span class="mono tiny score-tone ${scoreCls}">Score ${score}/${threshold}</span>
        <span class="readiness-chip ready-chip ${statusCls} mono">${ready}</span>
        <span class="status-chip status-blue">Blockers ${blockerText}</span>
      </div>
      <div class="sleeve-meta-row">
        <span class="entry-chip mono tiny muted">${ea}</span>
        ${fl ? `<span class="mono tiny muted">${fl}</span>` : ""}
      </div>
      <details class="sleeve-drill">
        <summary class="mono tiny">Open drill-down</summary>
        <div class="mono tiny muted" style="margin-top:6px;">Operational health: ${healthStatus}</div>
        <div class="mono tiny muted">Readiness / promotion: grade ${grade}, score ${score}/${threshold}, status ${ready}</div>
        <div class="mono tiny muted">Recent changes: mode=${String(mode).toUpperCase()} entries=${ea}</div>
      </details>
    `;
    rowStandard.appendChild(pill);
  });

  if (!standardSleeves.length) {
    rowStandard.innerHTML = `<div class="mono tiny muted">No standard sleeves available for selected day.</div>`;
  }

  const autoOpenBond = new URLSearchParams(window.location.search).get("open_bond") === "1";
  if (autoOpenBond) {
    const bondDrill = document.querySelector("#bondSleeveCard details.bond-drill");
    if (bondDrill) bondDrill.open = true;
  }
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

function renderBondSleeve(payload) {
  const card = el("bondSleeveCard");
  if (!card) return;
  card.classList.remove("hidden");
  const b = payload?.bond_sleeve || {};
  const sum = b?.summary || {};
  const holdings = b?.operator_holdings || {};
  const recommendationState = String(b?.recommendation_state_label || "NO_CURRENT_ARTIFACT");
  const authorityState = String(b?.authority_state_label || "DISPLAY_HEAD_AVAILABLE_ONLY");
  const holdingsState = String(b?.holdings_state_label || "OPERATOR_HOLDINGS_MISSING");
  const currentDayReason = String(b?.current_day_unavailable_reason || "CURRENT_DAY_AVAILABLE");
  const runState = String(b?.run_metadata?.status || "NO_BOND_RUN_METADATA").toUpperCase();
  const runReasons = (b?.run_metadata?.reason_codes || []).join(", ") || "No reason codes";
  const selectedDay = b?.selected_day || "n/a";
  const usedDay = b?.used_day || "n/a";
  const hasRecommendation = recommendationState === "CURRENT_DAY_RECOMMENDATION_AVAILABLE"
    || recommendationState === "USING_LAST_AVAILABLE_RECOMMENDATION";
  const ladder = b?.families?.bond_ladder_recommendation_v1?.artifact || {};
  const purchase = b?.families?.bond_purchase_recommendation_v1?.artifact || {};
  const purchaseAction = String(purchase?.action_required || sum?.purchase_action_required || "NO_ACTION").toUpperCase();
  const purchaseState = String(purchase?.recommendation_state || sum?.purchase_recommendation_state || "NO_RECOMMENDATION");
  const trade = purchase?.recommended_trade || sum?.recommended_trade || null;
  const alternatives = Array.isArray(purchase?.candidate_rankings)
    ? purchase.candidate_rankings
    : (Array.isArray(sum?.candidate_rankings) ? sum.candidate_rankings : []);
  const purchaseReasonCodes = Array.isArray(purchase?.reason_codes)
    ? purchase.reason_codes
    : (Array.isArray(sum?.purchase_reason_codes) ? sum.purchase_reason_codes : []);
  const ladderBuckets = (ladder?.maturity_buckets || []).slice(0, 6);
  const ladderSummary = ladderBuckets.length
    ? `Ladder buckets loaded: ${ladderBuckets.length} buckets from ${usedDay}.`
    : "No ladder recommendation available yet.";
  const ladderRecs = (sum?.top_policy_recommendations || []).slice(0, 3);
  const statusHeadline = hasRecommendation
    ? (recommendationState === "USING_LAST_AVAILABLE_RECOMMENDATION"
      ? "Ladder recommendation available (fallback day)"
      : "Current-day ladder recommendation available")
    : "No bond ladder recommendation available";
  const reasonText = {
    CURRENT_DAY_AVAILABLE: "Current-day ladder recommendation is available.",
    NO_CURRENT_ARTIFACT: "No current-day ladder recommendation artifact is available.",
    BOND_RUN_SKIPPED: "Bond sleeve has not run for the current day.",
    BOND_RUN_FAILED: "Bond sleeve run failed for the current day.",
  }[currentDayReason] || `Current-day recommendation unavailable (${currentDayReason}).`;
  const nextStep = hasRecommendation
    ? (holdingsState === "OPERATOR_HOLDINGS_MISSING"
      ? "Record current bond positions in constellation_2/operator_inputs/bond_sleeve/bond_positions_v1.json."
      : purchaseAction === "BUY"
        ? "Review the next recommended purchase, execute manually in IB if approved, then record the filled bond in operator holdings."
        : "Compare holdings maturity mix against the recommended ladder buckets.")
    : (runState === "SKIP" || runState === "NO_BOND_RUN_METADATA")
      ? "Run bond sleeve once NAV and allocation artifacts exist for the selected day."
      : runState === "FAIL"
        ? "Review bond sleeve failure reason codes, resolve the issue, and rerun for the selected day."
        : "Run bond sleeve for the selected day.";
  const reviewNext = [];
  if (recommendationState === "USING_LAST_AVAILABLE_RECOMMENDATION") {
    reviewNext.push("Review fallback ladder recommendation and rerun the daily bond sleeve process for the selected day.");
  }
  if (holdingsState === "OPERATOR_HOLDINGS_MISSING") {
    reviewNext.push("Populate governed operator holdings input at constellation_2/operator_inputs/bond_sleeve/bond_positions_v1.json.");
  }
  if (hasRecommendation && holdingsState === "OPERATOR_HOLDINGS_LOADED") {
    if (purchaseAction === "BUY" && trade) {
      reviewNext.push(`Validate recommended bond ${trade.instrument_id || "N/A"} for rung ${trade.target_rung || "N/A"} and execute manually if approved.`);
      reviewNext.push("After execution, update bond_positions_v1.json so recommendation status can move to HOLD or next rung.");
    } else {
      reviewNext.push("Compare current holdings maturity distribution to the recommended ladder buckets and resolve concentration gaps.");
    }
  }
  if (!reviewNext.length) reviewNext.push("No additional review required right now.");
  const rows = holdings?.positions || [];
  const holdingsRows = rows.length
    ? rows.map((r, idx) => `
      <tr>
        <td class="mono tiny">${idx + 1}</td>
        <td class="mono tiny">${r.instrument_id || "NOT_PROVIDED"}</td>
        <td class="mono tiny">${r.maturity_date || "NOT_PROVIDED"}</td>
        <td class="mono tiny">${r.market_value || "NOT_PROVIDED"}</td>
      </tr>
    `).join("")
    : `<tr><td colspan="4" class="mono tiny muted">No bond positions have been recorded yet.</td></tr>`;
  const traceStates = (b?.state_labels || []).join(" • ") || "none";
  const ladderStatusText = {
    CURRENT_DAY_RECOMMENDATION_AVAILABLE: "Current-day ladder available",
    USING_LAST_AVAILABLE_RECOMMENDATION: "Showing last available ladder",
    NO_CURRENT_ARTIFACT: "No recommendation available",
  }[recommendationState] || "No recommendation available";
  const actionText = {
    BUY: "BUY",
    HOLD: "HOLD",
    NO_ACTION: "NO_ACTION",
  }[purchaseAction] || "NO_ACTION";
  const actionChip = purchaseAction === "BUY" ? "status-amber" : "status-blue";
  const whySelected = Array.isArray(trade?.why_selected) ? trade.why_selected : [];
  const whyNow = Array.isArray(trade?.why_now) ? trade.why_now : [];
  const altRows = alternatives.slice(0, 3).map((row) => `
    <tr>
      <td class="mono tiny">${row.rank ?? "n/a"}</td>
      <td class="mono tiny">${row.instrument_id || "NOT_PROVIDED"}</td>
      <td class="mono tiny">${row.maturity_date || "NOT_PROVIDED"}</td>
      <td class="mono tiny">${row.yield_to_maturity || "NOT_PROVIDED"}</td>
      <td class="mono tiny">${row.score_total || "NOT_PROVIDED"}</td>
    </tr>
  `).join("");
  const recommendationBlock = (hasRecommendation && trade && purchaseAction === "BUY")
    ? `
      <div class="mono tiny sleeve-drill-section"><strong>Next Recommended Purchase:</strong></div>
      <div class="mono tiny">Action: ${actionText}</div>
      <div class="mono tiny">Description: Buy ${trade.instrument_id || "N/A"} for target rung ${trade.target_rung || sum?.purchase_target_rung || "N/A"}.</div>
      <div class="mono tiny">Target rung: ${trade.target_rung || sum?.purchase_target_rung || "N/A"}</div>
      <div class="mono tiny">Maturity: ${trade.maturity_date || "N/A"} (${trade.years_to_maturity || "N/A"} years)</div>
      <div class="mono tiny">Yield: ${trade.yield_to_maturity || "N/A"} • Duration: ${trade.duration || "N/A"}</div>
      <div class="mono tiny">Face value target: ${trade.face_value_target || "N/A"} • Estimated cost: ${trade.estimated_cost || "N/A"}</div>
      <div class="mono tiny" style="margin-top:4px;"><strong>Why selected:</strong><br/>${whySelected.length ? whySelected.map(x => `• ${x}`).join("<br/>") : "• Deterministic score leader for the target rung."}</div>
      <div class="mono tiny" style="margin-top:4px;"><strong>Why now:</strong><br/>${whyNow.length ? whyNow.map(x => `• ${x}`).join("<br/>") : "• Target rung underweight and purchase budget available."}</div>
      <div class="mono tiny" style="margin-top:6px;"><strong>Top alternatives:</strong></div>
      <table class="bond-table" style="margin-top:4px;">
        <thead><tr><th>Rank</th><th>Instrument</th><th>Maturity</th><th>Yield</th><th>Score</th></tr></thead>
        <tbody>${altRows || `<tr><td colspan="5" class="mono tiny muted">No alternatives available.</td></tr>`}</tbody>
      </table>
    `
    : `
      <div class="mono tiny sleeve-drill-section"><strong>Next Recommended Purchase:</strong></div>
      <div class="mono tiny">${purchaseAction === "HOLD" ? "No Action Needed: target rung is currently filled from recorded holdings." : "No Action Needed: no policy-eligible buy candidate is available right now."}</div>
      <div class="mono tiny muted" style="margin-top:4px;">action=${actionText} • recommendation_state=${purchaseState}</div>
      <div class="mono tiny muted">reason_codes=${purchaseReasonCodes.join(", ") || "none"}</div>
    `;

  card.innerHTML = `
    <div class="card-head">
      <div class="card-title">Bond Sleeve (Fixed Income)</div>
      <span class="status-chip ${String(b?.status || "").toUpperCase() === "PASS" ? "status-green" : "status-amber"}">${statusHeadline}</span>
    </div>
    <div class="sleeve-readiness-row">
      <span class="mode-chip mode-paper mono">${String((b?.run_metadata?.truth_root_used || "").includes("/LIVE/") ? "LIVE" : "PAPER")}</span>
      <span class="status-chip status-blue">Allocation ${sum?.bond_sleeve_allocation_pct ?? "n/a"}%</span>
      <span class="status-chip status-blue">Duration ${sum?.weighted_duration ?? "n/a"}</span>
      <span class="status-chip status-blue">Yield ${sum?.weighted_yield ?? "n/a"}</span>
      <span class="status-chip ${actionChip}">Action ${actionText}</span>
      <span class="status-chip ${authorityState === "AUTHORITY_HEAD_AVAILABLE" ? "status-green" : "status-amber"}">${authorityState === "AUTHORITY_HEAD_AVAILABLE" ? "Authoritative ladder available" : "Preliminary ladder available"}</span>
      <span class="status-chip status-blue">Readiness grade n/a</span>
    </div>
    <div class="mono tiny muted" style="margin-top:6px;">Ladder status: ${ladderStatusText} • selected day ${selectedDay} • used day ${usedDay}</div>
    <details class="bond-drill sleeve-drill" style="margin-top:8px;">
      <summary class="mono tiny">Open bond advisory details</summary>
      <div class="mono tiny sleeve-drill-section" style="margin-top:6px;"><strong>Status:</strong> ${statusHeadline}</div>
      <div class="mono tiny sleeve-drill-section"><strong>Reason:</strong> ${reasonText}</div>
      <div class="mono tiny sleeve-drill-section"><strong>Next Step:</strong> ${nextStep}</div>
      ${recommendationBlock}
      <div class="mono tiny sleeve-drill-section"><strong>Ladder summary:</strong> ${ladderSummary}</div>
      <div class="mono tiny">${ladderRecs.length ? ladderRecs.map(x => `• ${x}`).join("<br/>") : "Review will populate once a ladder recommendation is produced."}</div>
      <div class="mono tiny sleeve-drill-section"><strong>Current holdings:</strong> ${holdingsState === "OPERATOR_HOLDINGS_MISSING" ? "No bond positions have been recorded yet." : `Rows loaded: ${holdings?.positions_count ?? 0} • market value total: ${holdings?.market_value_total ?? "n/a"}`}</div>
      <table class="bond-table" style="margin-top:4px;">
        <thead><tr><th>#</th><th>Instrument</th><th>Maturity</th><th>Market Value</th></tr></thead>
        <tbody>${holdingsRows}</tbody>
      </table>
      <div class="mono tiny sleeve-drill-section"><strong>What to review next:</strong><br/>${reviewNext.map(x => `• ${x}`).join("<br/>")}</div>
      <details class="evidence-details" style="margin-top:6px;">
        <summary class="mono tiny">Traceability labels</summary>
        <div class="mono tiny muted" style="margin-top:6px;">recommendation_state_label=${recommendationState}</div>
        <div class="mono tiny muted">authority_state_label=${authorityState}</div>
        <div class="mono tiny muted">holdings_state_label=${holdingsState}</div>
        <div class="mono tiny muted">current_day_unavailable_reason=${currentDayReason}</div>
        <div class="mono tiny muted">bond_run_status=${runState}</div>
        <div class="mono tiny muted">bond_run_reason_codes=${runReasons}</div>
        <div class="mono tiny muted">state_labels=${traceStates}</div>
      </details>
    </details>
  `;
  const autoOpenBond = new URLSearchParams(window.location.search).get("open_bond") === "1";
  if (autoOpenBond) {
    const bondDrill = card.querySelector("details.bond-drill");
    if (bondDrill) bondDrill.open = true;
  }
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
  renderSignalActivity(payload);
  renderScopeHealth(payload);
  renderTiles(payload);
  renderSleeveStrip(payload);
  renderFunnel(payload);
  renderBondSleeve(payload);
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
