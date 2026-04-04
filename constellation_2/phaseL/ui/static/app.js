"use strict";

const el = (id) => document.getElementById(id);

const state = {
  view: "operations",
  operationsSubView: "home",
  refreshSec: 60,
  timer: null,
  days: [],
  day: null,
  attempts: [],
  attempt_id: null,
  statusV2: null,
  operatorHome: null,
  operatorQuery: null,
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

function setOperationsSubView(v) {
  state.operationsSubView = v;
  document.querySelectorAll("[data-cockpit-view]").forEach((node) => {
    const matches = node.getAttribute("data-cockpit-view") === v;
    node.classList.toggle("hidden", !matches);
  });
  const nav = [
    ["cockpitViewHome", "home"],
    ["cockpitViewCockpit", "cockpit"],
    ["cockpitViewDiagnostics", "diagnostics"],
    ["cockpitViewRepair", "repair"],
    ["cockpitViewGovernance", "governance"],
  ];
  nav.forEach(([id, viewName]) => {
    const node = el(id);
    if (node) node.classList.toggle("active", viewName === v);
  });
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

function semanticTone(value, fallback = "info") {
  const normalized = String(value || "").toUpperCase();
  if (["PASS", "READY", "HEALTHY", "OK"].includes(normalized)) return "positive";
  if (["FAIL", "BLOCKED", "ABORTED", "MISSING"].includes(normalized)) return "negative";
  if (["DEGRADED", "WARNING", "WARN"].includes(normalized)) return "warning";
  if (["SKIP", "SKIPPED", "INFO", "NOT REQUIRED"].includes(normalized)) return "info";
  return fallback;
}

function isHealthyNoSignalReasonCodes(reasonCodes) {
  const codes = Array.isArray(reasonCodes) ? reasonCodes.filter(Boolean) : [];
  return codes.length === 1 && String(codes[0]).toUpperCase() === "NO_ACTIVITY_DAY";
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
    const noSignalVerdict = rawState === "DEGRADED" && t.tile_id === "orchestrator_run_verdict_v2" && isHealthyNoSignalReasonCodes(t.reason_codes);
    const normalizedAbort = rawState === "ABORTED" && execState === "PASS" &&
      (t.tile_id === "orchestrator_run_verdict_v2" || t.tile_id === "safety_breach");

    let human;
    let note = "";
    if (noSignalVerdict) {
      human = "COMPLETED — NO SIGNAL DAY";
      note = `raw_verdict=${rawState} - reason=NO_ACTIVITY_DAY - execution=${execState}`;
    } else if (normalizedAbort && t.tile_id === "orchestrator_run_verdict_v2") {
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
  const readiness = payload?.platform_readiness || {};
  const selfDiags = payload?.self_diagnostics || sh?.self_diagnostics || {};
  const execHost = el("executionHealthBody");
  const monHost = el("systemMonitoringBody");
  const repairHost = el("repairSignalsBody");

  const execState = String(exec.status || "UNKNOWN").toUpperCase();
  const monState = String(mon.status || "UNKNOWN").toUpperCase();
  const overallState = String(overall.status || "UNKNOWN").toUpperCase();
  el("executionHealthMeta").textContent = `runtime_state=${src?.present ? "present" : "missing"} • overall=${overallState}`;
  el("systemMonitoringMeta").textContent = `monitoring=${monState} • source=${src?.path || "n/a"}`;
  const lifecycleSurface = (((mon?.freshness || {}).surface_results || []).find(s => (s?.surface_id || "") === "lifecycle_monitor")) || {};
  const freshness = mon?.freshness || {};
  const lifecycleGov = lifecycleSurface?.lifecycle_governance_summary || {};
  const diagList = Array.isArray(selfDiags?.diagnostics) ? selfDiags.diagnostics : [];
  const selfHealRows = diagList.filter((d) => String(d?.code || "").startsWith("MONITORING_SELF_HEAL"));
  const repairRows = [
    ...(Array.isArray(readiness?.derived_blockers) ? readiness.derived_blockers.map((x) => ({code: x, summary: "Derived warning still present."})) : []),
    ...selfHealRows.map((x) => ({code: x.code, summary: x.summary || "Self-heal signal observed."}))
  ].slice(0, 5);

  const indicator = (label, value, tone, detail) => `
    <div class="health-line">
      <div class="health-line-main">
        <span class="health-indicator ${tone}"></span>
        <span class="health-label">${escapeHtml(label)}</span>
      </div>
      <div class="health-value ${toneClass(tone)}">${escapeHtml(value)}</div>
      ${detail ? `<div class="health-detail">${escapeHtml(detail)}</div>` : ""}
    </div>
  `;

  if (execHost) {
    const sleeves = Array.isArray(exec?.sleeves) ? exec.sleeves : [];
    const latest = sleeves[0] || {};
    execHost.innerHTML = `
      <div class="cockpit-kv-grid">
        ${indicator("Execution status", execState, execState === "PASS" ? "positive" : execState === "DEGRADED" ? "warning" : "negative", "Derived execution authority for the active sleeve")}
        ${indicator("Raw orchestrator verdict", latest?.verdict_status_raw || "UNKNOWN", latest?.verdict_status_raw === "DEGRADED" ? "warning" : latest?.verdict_status_raw === "PASS" ? "positive" : "negative", "Raw verdict remains preserved as evidence")}
        ${indicator("Latest execution day", latest?.latest_day || "n/a", "info", latest?.attempt_id ? `attempt=${latest.attempt_id}` : "")}
      </div>
    `;
  }

  if (monHost) {
    const surfaceResults = Array.isArray(freshness?.surface_results) ? freshness.surface_results : [];
    const keySurfaces = surfaceResults.filter((row) => ["paper_readiness", "lifecycle_monitor", "capital_authority_allocation"].includes(String(row?.surface_id || "")));
    monHost.innerHTML = `
      <div class="cockpit-kv-grid">
        ${indicator("Monitoring status", monState, monState === "PASS" ? "positive" : monState === "DEGRADED" ? "warning" : "negative", `reference_day=${freshness?.reference_day || "n/a"}`)}
        ${indicator("Paper readiness", String((keySurfaces.find((x) => x.surface_id === "paper_readiness") || {}).status || "UNKNOWN").toUpperCase(), String((keySurfaces.find((x) => x.surface_id === "paper_readiness") || {}).status || "").toUpperCase() === "PASS" ? "positive" : "warning", "Global readiness surface")}
        ${indicator("Lifecycle monitor", String((keySurfaces.find((x) => x.surface_id === "lifecycle_monitor") || {}).status || "UNKNOWN").toUpperCase(), String((keySurfaces.find((x) => x.surface_id === "lifecycle_monitor") || {}).status || "").toUpperCase() === "PASS" ? "positive" : "warning", `readiness_required=${lifecycleGov?.counts_by_classification?.REQUIRED_FOR_READINESS ?? "n/a"}`)}
        ${indicator("Capital authority", String((keySurfaces.find((x) => x.surface_id === "capital_authority_allocation") || {}).status || "UNKNOWN").toUpperCase(), String((keySurfaces.find((x) => x.surface_id === "capital_authority_allocation") || {}).status || "").toUpperCase() === "PASS" ? "positive" : "warning", "Attested global monitoring surface")}
      </div>
    `;
  }

  if (repairHost) {
    el("repairSignalsMeta").textContent = `readiness=${String(readiness?.platform_readiness_state || "UNKNOWN").toUpperCase()} • diagnostics=${diagList.length}`;
    repairHost.innerHTML = repairRows.length ? `
      <div class="repair-list">
        ${repairRows.map((row) => `
          <div class="repair-item">
            <div class="repair-code">${escapeHtml(row.code || "UNKNOWN")}</div>
            <div class="repair-summary">${escapeHtml(row.summary || "No detail provided.")}</div>
          </div>
        `).join("")}
      </div>
    ` : `<div class="cockpit-alert cockpit-success">No active repair signals. Monitoring and readiness are current.</div>`;
  }
}

function renderGovernedRiskSurfaces(payload) {
  const host = el("governedRiskBody");
  const meta = el("governedRiskMeta");
  if (!host || !meta) return;
  const groups = payload?.governed_risk_surfaces || {};
  const entries = [
    ["Authority & Data Integrity", Array.isArray(groups?.authority_and_data) ? groups.authority_and_data : []],
    ["Broker & Execution Readiness", Array.isArray(groups?.broker_and_execution) ? groups.broker_and_execution : []],
    ["Risk Envelope", Array.isArray(groups?.risk_envelope) ? groups.risk_envelope : []],
  ];
  const total = entries.reduce((sum, [, rows]) => sum + rows.length, 0);
  const attention = entries.reduce((sum, [, rows]) => sum + rows.filter((row) => ["negative", "warning"].includes(String(row?.tone || ""))).length, 0);
  meta.textContent = `surfaces=${total} • attention=${attention}`;
  host.innerHTML = `
    <div class="governed-risk-grid">
      ${entries.map(([title, rows]) => `
        <div class="governed-risk-group">
          <div class="governed-risk-group-title">${escapeHtml(title)}</div>
          <div class="governed-risk-list">
            ${rows.map((row) => `
              <div class="governed-risk-item">
                <div class="governed-risk-main">
                  <div class="governed-risk-label-row">
                    <span class="governed-risk-label">${escapeHtml(row?.label || "Unknown surface")}</span>
                    <span class="status-chip-compact ${toneClass(row?.tone)}">${escapeHtml(String(row?.state || "UNKNOWN").toUpperCase())}</span>
                  </div>
                  <div class="governed-risk-detail">${escapeHtml(row?.detail || "No governed detail available.")}</div>
                </div>
                <div class="governed-risk-actions">
                  <div class="mono tiny muted">${row?.authoritative === false ? "derived" : "authoritative"}</div>
                  ${row?.artifact_path ? `<button class="btn btn-mini" data-open="${encodeURIComponent(row.artifact_path)}" data-title="${escapeHtml(row?.label || "governed_surface")}">Evidence</button>` : ""}
                </div>
              </div>
            `).join("")}
          </div>
        </div>
      `).join("")}
    </div>
  `;
  host.querySelectorAll("[data-open]").forEach((button) => {
    button.onclick = async () => {
      const path = decodeURIComponent(button.getAttribute("data-open") || "");
      const title = button.getAttribute("data-title") || "evidence";
      await openEvidence(title, path);
    };
  });
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
  const platformPayload = payload?.platform_readiness && typeof payload.platform_readiness === "object" ? payload.platform_readiness : {};
  const operationalPayload = payload?.operational_readiness && typeof payload.operational_readiness === "object" ? payload.operational_readiness : {};
  const bugPayload = payload?.platform_bug_metrics && typeof payload.platform_bug_metrics === "object" ? payload.platform_bug_metrics : {};
  const policyPayload = payload?.platform_readiness_policy && typeof payload.platform_readiness_policy === "object" ? payload.platform_readiness_policy : {};
  const hasReadinessPayload = platformPayload?.present === true;
  const hasBugPayload = bugPayload?.present === true;
  const hasPolicyPayload = policyPayload?.present === true;
  const readiness = hasReadinessPayload ? platformPayload : {};
  const metrics = hasBugPayload ? bugPayload : {};
  const policy = hasPolicyPayload ? policyPayload : {};
  const pm = readiness?.metric_views || {};
  const bm = metrics?.metric_views || {};

  const state = hasReadinessPayload ? String(readiness?.platform_readiness_state || "UNKNOWN").toUpperCase() : "UNKNOWN";
  const grade = hasReadinessPayload ? (readiness?.platform_readiness_grade ?? "n/a") : "n/a";
  const score = hasReadinessPayload ? numOrNull(readiness?.platform_readiness_score) : null;
  const threshold = hasReadinessPayload ? numOrNull(readiness?.score_threshold_ready) : null;
  const candidate = hasReadinessPayload && readiness?.platform_promotion_candidate === true ? "YES"
    : hasReadinessPayload && readiness?.platform_promotion_candidate === false ? "NO"
    : "UNKNOWN";
  const summary = readiness?.readiness_summary || "n/a";
  const decision = readiness?.promotion_decision_basis || "n/a";
  const rootBlockers = Array.isArray(readiness?.root_blockers) ? readiness.root_blockers : [];
  const derivedBlockers = Array.isArray(readiness?.derived_blockers) ? readiness.derived_blockers : [];
  const contributionRows = Array.isArray(readiness?.score_contribution) ? readiness.score_contribution : [];
  const gradeBands = Array.isArray(policy?.grade_bands) ? policy.grade_bands : [];
  const thresholdPolicy = readiness?.policy_values || {};
  const hardBlockers = thresholdPolicy?.hard_blockers || {};
  const maxVelocity = numOrNull(hardBlockers?.max_bug_velocity_7d_avg_for_candidate);
  const maxRecurrence = numOrNull(hardBlockers?.max_recurrence_rate_for_candidate);
  const producedUtc = readiness?.produced_utc || "n/a";
  const bugProducedUtc = metrics?.produced_utc || "n/a";
  const readinessPath = readiness?.path || "n/a";
  const bugPath = metrics?.path || "n/a";
  const policyPath = policy?.path || thresholdPolicy?.policy_path || "n/a";
  const scoreDisplay = formatPointsOutOf100(score);
  const thresholdDisplay = threshold === null ? "n/a" : String(threshold);
  const velocityValue = numOrNull(metrics?.bug_velocity_7d_avg);
  const recurrenceValue = numOrNull(metrics?.recurrence_rate);
  const stabilityValue = numOrNull(metrics?.diagnostic_stability_rate);
  const newBugsValue = numOrNull(metrics?.new_bug_events_today);
  const velocityDisplay = formatMetricDisplay(pm?.bug_velocity_7d_avg || bm?.bug_velocity_7d_avg, metrics?.bug_velocity_7d_avg, "rate_x100");
  const recurrenceDisplay = formatMetricDisplay(pm?.recurrence_rate || bm?.recurrence_rate, metrics?.recurrence_rate, "percent_bp");
  const stabilityDisplay = formatMetricDisplay(pm?.diagnostic_stability_rate || bm?.diagnostic_stability_rate, metrics?.diagnostic_stability_rate, "percent_bp");
  const newBugsDisplay = formatMetricDisplay(bm?.new_bug_events_today, metrics?.new_bug_events_today, null);
  const bugTrend = String(metrics?.bug_velocity_trend || "UNKNOWN");
  const recurringKeys = Array.isArray(metrics?.recurring_bug_events) ? metrics.recurring_bug_events : [];
  const evidencePaths = [...new Set([
    ...(Array.isArray(readiness?.evidence_paths) ? readiness.evidence_paths : []),
    ...(Array.isArray(metrics?.evidence_paths) ? metrics.evidence_paths : []),
  ].filter(Boolean))];
  const readinessSourceNote = hasReadinessPayload
    ? (readiness?.requested_day_present === true
      ? `readiness_day=${readiness?.resolved_day || payload?.meta?.selected_day || "n/a"}`
      : readiness?.resolved_via_latest_pointer
        ? `readiness_fallback=${readiness?.resolved_day || "latest"}`
        : "readiness_day=missing")
    : "readiness=missing";
  const bugSourceNote = hasBugPayload
    ? (metrics?.requested_day_present === true
      ? `bug_metrics_day=${metrics?.resolved_day || payload?.meta?.selected_day || "n/a"}`
      : metrics?.resolved_via_latest_pointer
        ? `bug_metrics_fallback=${metrics?.resolved_day || "latest"}`
        : "bug_metrics_day=missing")
    : "bug_metrics=missing";
  const fallbackBanner = hasReadinessPayload && readiness?.resolved_via_latest_pointer
    ? `<div class="platform-fallback-banner">Structural readiness is using the latest canonical artifact (${escapeHtml(readiness?.resolved_day || "latest")}), not the selected day.</div>`
    : "";
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
    ${fallbackBanner}
    <div class="metric platform-kpi-card platform-kpi-status">
      <div class="k">Structural Status</div>
      <div class="v platform-kpi-value ${stateClass(state)}">${state}</div>
      <div class="platform-kpi-note">Structural readiness artifact state</div>
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
        <div class="section-title">Structural Readiness</div>
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
        <div class="section-title">Operational Guardrail</div>
        <div class="info-pills"><div class="info-pill ${stateClass(operationalPayload?.state)}">${escapeHtml(String(operationalPayload?.state || "UNKNOWN").replaceAll("_", " "))}</div></div>
        <div class="section-subnote">${escapeHtml(operationalPayload?.summary || "Selected-day operational readiness unavailable.")}</div>
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

function renderPortfolioSummary(payload) {
  const host = el("opsPortfolioSummaryBody");
  const meta = el("opsPortfolioSummaryMeta");
  if (!host || !meta) return;
  const p = payload?.portfolio || {};
  meta.textContent = p.asof_utc ? `asof=${p.asof_utc}` : "selected day portfolio view";
  const items = [
    ["NAV total", p.nav_total],
    ["PnL today", p.pnl_today],
    ["PnL cumulative", p.pnl_cumulative],
    ["Drawdown %", p.drawdown_pct],
    ["Cash %", p.cash_pct],
    ["Net exposure %", p.net_exposure_pct],
    ["Gross exposure %", p.gross_exposure_pct],
  ];
  host.innerHTML = `
    <div class="portfolio-summary-grid">
      ${items.map(([label, value]) => `
        <div class="metric">
          <div class="k">${escapeHtml(label)}</div>
          <div class="v">${escapeHtml(fmt(value))}</div>
        </div>
      `).join("")}
    </div>
    <div class="mono tiny muted" style="margin-top:10px;">${escapeHtml(p.note_if_missing || "Canonical portfolio summary loaded from current status payload.")}</div>
  `;
}

function renderPlatformReadinessSummary(payload) {
  const host = el("platformReadinessSummaryBody");
  const meta = el("platformReadinessSummaryMeta");
  if (!host || !meta) return;
  const readiness = payload?.platform_readiness || {};
  const rootBlockers = Array.isArray(readiness?.root_blockers) ? readiness.root_blockers : [];
  const derivedBlockers = Array.isArray(readiness?.derived_blockers) ? readiness.derived_blockers : [];
  const score = readiness?.platform_readiness_score ?? "n/a";
  const threshold = readiness?.score_threshold_ready ?? "n/a";
  meta.textContent = `score=${score}/${threshold} • state=${String(readiness?.platform_readiness_state || "UNKNOWN").toUpperCase()}`;
  host.innerHTML = `
    <div class="platform-summary-grid">
      <div class="metric">
        <div class="k">Structural state</div>
        <div class="v ${stateClass(readiness?.platform_readiness_state)}">${escapeHtml(String(readiness?.platform_readiness_state || "UNKNOWN").toUpperCase())}</div>
      </div>
      <div class="metric">
        <div class="k">Structural score</div>
        <div class="v">${escapeHtml(String(score))}</div>
      </div>
      <div class="metric">
        <div class="k">Threshold</div>
        <div class="v">${escapeHtml(String(threshold))}</div>
      </div>
      <div class="metric">
        <div class="k">Structural grade</div>
        <div class="v">${escapeHtml(String(readiness?.platform_readiness_grade || "n/a"))}</div>
      </div>
    </div>
    <div class="cockpit-alert cockpit-info" style="margin-top:12px;">${escapeHtml(readiness?.readiness_summary || "No structural readiness summary available.")}</div>
    <div class="platform-summary-operator-grid">
      <div class="platform-summary-operator-card">
        <div class="platform-summary-operator-title">Root blockers</div>
        <div class="platform-summary-operator-value">${escapeHtml(rootBlockers.length ? rootBlockers.join(", ") : "none")}</div>
      </div>
      <div class="platform-summary-operator-card">
        <div class="platform-summary-operator-title">Derived warnings</div>
        <div class="platform-summary-operator-value">${escapeHtml(derivedBlockers.length ? derivedBlockers.join(", ") : "none")}</div>
      </div>
    </div>
  `;
}

function renderDiagnosticsOverview(payload) {
  const host = el("diagnosticsOverviewBody");
  const meta = el("diagnosticsOverviewMeta");
  if (!host || !meta) return;
  const readiness = payload?.platform_readiness || {};
  const prov = payload?.provenance || {};
  const activity = payload?.activity_flow_diagnostics || {};
  const blockedDay = payload?.day_start_blocked || {};
  const tradingDay = payload?.trading_day_state || {};
  const dispositions = payload?.intent_terminal_dispositions || {};
  const rootBlockers = Array.isArray(readiness?.root_blockers) ? readiness.root_blockers : [];
  const derivedBlockers = Array.isArray(readiness?.derived_blockers) ? readiness.derived_blockers : [];
  const warnings = Array.isArray(prov?.warnings) ? prov.warnings : [];
  const missingPaths = Array.isArray(prov?.missing_paths) ? prov.missing_paths : [];
  const flags = Array.isArray(activity?.suspicion_flags) ? activity.suspicion_flags : [];
  const breakdown = Array.isArray(activity?.rejection_breakdown) ? activity.rejection_breakdown.slice(0, 3) : [];
  const terminalCounts = Array.isArray(dispositions?.terminal_state_counts) ? dispositions.terminal_state_counts.slice(0, 4) : [];
  const stageCounts = Array.isArray(dispositions?.stage_counts) ? dispositions.stage_counts.slice(0, 4) : [];
  const reasonCounts = Array.isArray(dispositions?.dominant_reason_codes) ? dispositions.dominant_reason_codes.slice(0, 4) : [];
  const byEngine = Array.isArray(dispositions?.by_engine) ? dispositions.by_engine.slice(0, 3) : [];
  const counts = activity?.counts || {};
  const dayDeps = Array.isArray(tradingDay?.dependency_statuses) ? tradingDay.dependency_statuses : [];
  const freshness = Array.isArray(tradingDay?.freshness_checks) ? tradingDay.freshness_checks : [];
  meta.textContent = `day_state=${String(tradingDay?.state || "UNKNOWN")} • heartbeat=${String(tradingDay?.heartbeat_status || "UNKNOWN")} • terminal=${String(activity?.terminal_state || "UNKNOWN")} • intent_dispositions=${String(dispositions?.intent_count || 0)} • blocked=${blockedDay?.blocked === true ? "yes" : "no"} • warnings=${warnings.length} • flags=${flags.length} • missing_paths=${missingPaths.length}`;
  const blockedBanner = blockedDay?.blocked === true ? `
    <div class="cockpit-alert cockpit-danger">
      <strong>Day Start Blocked</strong><br>
      ${escapeHtml(String(blockedDay?.blocked_stage || "UNKNOWN"))} • ${escapeHtml(String(blockedDay?.first_failing_prerequisite || "UNKNOWN"))}<br>
      ${escapeHtml(String(blockedDay?.first_failing_reason_code || "UNKNOWN"))}<br>
      ${escapeHtml(String(blockedDay?.first_failing_path || "no failing path recorded"))}
    </div>
  ` : "";
  const tradingDayBanner = `
    <div class="cockpit-alert ${String(tradingDay?.heartbeat_status || "UNKNOWN") === "PASS" ? "cockpit-info" : "cockpit-danger"}">
      <strong>Trading Day State</strong><br>
      ${escapeHtml(String(tradingDay?.state || "UNKNOWN"))} • heartbeat=${escapeHtml(String(tradingDay?.heartbeat_status || "UNKNOWN"))}<br>
      ${escapeHtml(String(tradingDay?.first_failing_prerequisite || "no failing prerequisite recorded"))}
    </div>
  `;
  host.innerHTML = `
    ${tradingDayBanner}
    ${blockedBanner}
    <div class="diagnostics-overview-grid">
      <div class="cockpit-alert ${String(tradingDay?.heartbeat_status || "UNKNOWN") === "PASS" ? "cockpit-success" : "cockpit-danger"}">
        <strong>09:35 heartbeat</strong><br>
        ${escapeHtml(String(tradingDay?.heartbeat_status || "UNKNOWN"))}
      </div>
      <div class="cockpit-alert cockpit-info">
        <strong>Dependency stages</strong><br>
        ${escapeHtml(dayDeps.length ? dayDeps.map((row) => `${row.stage}:${row.status}`).join(", ") : "none")}
      </div>
      <div class="cockpit-alert cockpit-warning">
        <strong>Expected outputs</strong><br>
        ${escapeHtml(freshness.length ? freshness.map((row) => `${row.artifact_id}:${row.status}`).join(", ") : "none")}
      </div>
      <div class="cockpit-alert cockpit-info">
        <strong>Activity terminal state</strong><br>
        ${escapeHtml(activity?.terminal_state || "UNKNOWN")}
      </div>
      <div class="cockpit-alert ${blockedDay?.blocked === true ? "cockpit-danger" : "cockpit-info"}">
        <strong>Blocked-day state</strong><br>
        ${escapeHtml(blockedDay?.blocked === true ? `BLOCKED • ${String(blockedDay?.blocked_stage || "UNKNOWN")}` : "not blocked")}
      </div>
      <div class="cockpit-alert cockpit-info">
        <strong>Funnel counts</strong><br>
        ${escapeHtml(`universe=${counts?.universe_size ?? 0} • signals=${counts?.signal_candidate_count ?? 0} • intents=${counts?.intent_count ?? 0} • plans=${counts?.order_plan_count ?? 0}`)}
      </div>
      <div class="cockpit-alert cockpit-info">
        <strong>Intent terminal states</strong><br>
        ${escapeHtml(terminalCounts.length ? terminalCounts.map((row) => `${row.terminal_disposition}:${row.count}`).join(", ") : "none")}
      </div>
      <div class="cockpit-alert cockpit-warning">
        <strong>Intent loss stages</strong><br>
        ${escapeHtml(stageCounts.length ? stageCounts.map((row) => `${row.terminal_stage}:${row.count}`).join(", ") : "none")}
      </div>
      <div class="cockpit-alert cockpit-warning">
        <strong>Top rejection reasons</strong><br>
        ${escapeHtml(breakdown.length ? breakdown.map((row) => `${row.reason_code}:${row.count}`).join(", ") : "none")}
      </div>
      <div class="cockpit-alert cockpit-info">
        <strong>Disposition reasons</strong><br>
        ${escapeHtml(reasonCounts.length ? reasonCounts.map((row) => `${row.reason_code}:${row.count}`).join(", ") : "none")}
      </div>
      <div class="cockpit-alert ${flags.length ? "cockpit-warning" : "cockpit-success"}">
        <strong>Suspicion flags</strong><br>
        ${escapeHtml(flags.length ? flags.join(", ") : "none")}
      </div>
      <div class="cockpit-alert cockpit-info">
        <strong>Readiness details</strong><br>
        ${escapeHtml(readiness?.promotion_decision_basis || readiness?.readiness_summary || "No readiness detail available.")}
      </div>
      <div class="cockpit-alert cockpit-warning">
        <strong>Missing paths</strong><br>
        ${escapeHtml(missingPaths.length ? String(missingPaths.length) : "0")}
      </div>
      <div class="cockpit-alert cockpit-warning">
        <strong>Warnings</strong><br>
        ${escapeHtml(warnings.length ? String(warnings.length) : "0")}
      </div>
      <div class="cockpit-alert ${blockedDay?.blocked === true ? "cockpit-warning" : "cockpit-info"}">
        <strong>Dependency chain</strong><br>
        ${escapeHtml(Array.isArray(blockedDay?.dependency_chain) && blockedDay.dependency_chain.length ? blockedDay.dependency_chain.join(" -> ") : "none")}
      </div>
      <div class="cockpit-alert cockpit-success">
        <strong>Root blockers</strong><br>
        ${escapeHtml(rootBlockers.length ? rootBlockers.join(", ") : "none")}
      </div>
      <div class="cockpit-alert cockpit-info">
        <strong>Derived blockers</strong><br>
        ${escapeHtml(derivedBlockers.length ? derivedBlockers.join(", ") : "none")}
      </div>
      <div class="cockpit-alert cockpit-info">
        <strong>Disposition by engine</strong><br>
        ${escapeHtml(byEngine.length ? byEngine.map((row) => {
          const states = Array.isArray(row?.terminal_state_counts) ? row.terminal_state_counts.map((stateRow) => `${stateRow.terminal_disposition}:${stateRow.count}`).join("/") : "none";
          return `${row.engine_id}:${states}`;
        }).join(", ") : "none")}
      </div>
    </div>
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
  const submitDisplay = submit?.stage_status === "SKIP" ? "NOT REQUIRED" : (submit?.stage_status || "NOT REACHED");
  const missingSummary = missingEngines.length
    ? `
      <details class="signal-inline-details">
        <summary>Missing engines (${missingEngines.length})</summary>
        <div class="signal-missing-engines">${missingEngines.map((engineId) => `<span class="signal-missing-engine mono">${escapeHtml(engineId)}</span>`).join("")}</div>
      </details>
    `
    : `<div class="signal-subnote signal-subnote-compact">Missing engines: none</div>`;

  host.innerHTML = `
    <div class="card-head">
      <div class="card-title">Signal Activity</div>
      <div class="mono tiny muted">day=${escapeHtml(signal?.day_utc || payload?.meta?.selected_day || "n/a")}</div>
    </div>
    <div class="signal-activity-layout">
      <div class="signal-activity-column">
        <div class="signal-row">
          <div class="signal-label">Trade intents</div>
          <div class="signal-value ${intentCount > 0 ? "tone-positive" : "tone-warning"}">${escapeHtml(String(intentCount))}</div>
          <div class="signal-subnote">${escapeHtml(intents?.label || "No real intents produced")}</div>
        </div>
        <div class="signal-row">
          <div class="signal-label">Engine heartbeats</div>
          <div class="signal-value ${present >= expected && expected > 0 ? "tone-positive" : present > 0 ? "tone-warning" : "tone-negative"}">${escapeHtml(`${present} / ${expected}`)}</div>
          ${missingSummary}
        </div>
      </div>
      <div class="signal-activity-column">
        <div class="signal-row">
          <div class="signal-label">Orders released</div>
          <div class="signal-value ${toneClass(phasec?.tone)}">${escapeHtml(String(releasedCount))}</div>
          <div class="signal-subnote">vetoes=${escapeHtml(String(vetoCount))} • ${escapeHtml(phasec?.label || "no phaseC outputs")}</div>
        </div>
        <div class="signal-row">
          <div class="signal-label">Submit stage</div>
          <div class="signal-value ${toneClass(submit?.tone)}">${escapeHtml(submitDisplay)}</div>
          <div class="signal-subnote">${escapeHtml(submit?.label || "governed submit not reached")}</div>
        </div>
      </div>
      <div class="signal-row signal-row-wide">
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

function renderTradingDayOutcome(payload) {
  const host = el("tradingDayOutcomeCard");
  if (!host) return;
  const signal = payload?.signal_activity && typeof payload.signal_activity === "object" ? payload.signal_activity : {};
  const outcome = signal?.trading_day_outcome && typeof signal.trading_day_outcome === "object" ? signal.trading_day_outcome : {};
  const frequency = signal?.signal_frequency_30d && typeof signal.signal_frequency_30d === "object" ? signal.signal_frequency_30d : {};
  const hb = signal?.engine_heartbeats || {};
  const readiness = payload?.platform_readiness || {};
  const operationalReadiness = payload?.operational_readiness || {};
  const submit = signal?.governed_submit || {};
  const upstream = signal?.upstream_data_status || {};
  const upstreamRows = Array.isArray(upstream?.symbols) ? upstream.symbols : [];
  const expected = numOrNull(hb?.expected_count) ?? 0;
  const present = numOrNull(hb?.present_count) ?? 0;
  const readinessState = String(readiness?.platform_readiness_state || "UNKNOWN").toUpperCase();
  const operationalReadinessState = String(operationalReadiness?.state || "UNKNOWN").toUpperCase();
  const upstreamReady = upstreamRows.length > 0 && upstreamRows.every((row) => row?.same_day_present === true);
  const executionPath = String(outcome?.execution_path || "UNKNOWN").toUpperCase();
  const submitLabel = submit?.stage_status === "SKIP" ? "SKIPPED" : String(submit?.stage_status || "NOT REACHED").toUpperCase();
  const signalBand = frequency?.label || "Signal frequency unavailable";
  const dailyCounts = Array.isArray(frequency?.daily_counts) ? frequency.daily_counts : [];
  const facts = Array.isArray(outcome?.facts) ? outcome.facts.slice(0, 6) : [];
  const tone = String(outcome?.tone || "neutral").toLowerCase();
  const chipGate = outcome?.gate_stack || {};
  const chipKill = outcome?.kill_switch || {};
  const statusChips = [
    ["Gates", chipGate?.status || "UNKNOWN"],
    ["Data", upstreamReady ? "READY" : "MISSING"],
    ["Engines", `${present} / ${expected}`],
    ["Submit", submitLabel],
    ["Execution", executionPath],
    ["Readiness", operationalReadinessState],
  ];
  const flowSteps = [
    ["Market Data", upstreamReady ? "READY" : "MISSING"],
    ["Engines", `${present} / ${expected}`],
    ["Phase C", `${signal?.phasec_outcomes?.released_identity_dir_count ?? 0} identities`],
    ["Submit", submitLabel],
  ];
  const sparkline = dailyCounts.length
    ? `
      <div class="signal-cadence-sparkline" aria-label="Recent 30 day signal cadence">
        ${dailyCounts.map((value, idx) => {
          const numeric = Math.max(0, Number(value) || 0);
          const height = Math.max(10, Math.min(100, numeric === 0 ? 10 : numeric * 18));
          return `<span class="signal-cadence-bar" style="height:${height}%;" title="day_${idx + 1}: ${numeric}"></span>`;
        }).join("")}
      </div>
    `
    : `
      <div class="signal-cadence-summary-row">
        <span class="signal-cadence-summary-chip">Avg Signals / Day <strong>${escapeHtml(
          frequency?.avg_signals_per_day === null || frequency?.avg_signals_per_day === undefined
            ? "n/a"
            : Number(frequency.avg_signals_per_day).toFixed(2)
        )}</strong></span>
        <span class="signal-cadence-summary-chip">Last Signal <strong>${escapeHtml(frequency?.last_signal_date || "n/a")}</strong></span>
        <span class="signal-cadence-summary-chip">Last Submit <strong>${escapeHtml(frequency?.last_submit_date || "n/a")}</strong></span>
        <span class="signal-cadence-summary-chip">Signal Band <strong>${escapeHtml(signalBand)}</strong></span>
      </div>
    `;
  host.className = `card trading-day-outcome-card outcome-${tone}`;
  host.innerHTML = `
    <div class="card-head">
      <div class="card-title">Trading Day Outcome</div>
      <div class="mono tiny muted">day=${escapeHtml(outcome?.day_utc || signal?.day_utc || payload?.meta?.selected_day || "n/a")}</div>
    </div>
    <div class="outcome-layout">
      <div class="outcome-hero">
        <div class="outcome-label ${toneClass(tone)}">${escapeHtml(outcome?.label || "UNKNOWN DAY")}</div>
        <div class="outcome-subtitle">${escapeHtml(outcome?.subtitle || "No derived operator summary available.")}</div>
        <div class="outcome-strip">
          <div class="outcome-chip">Classification: <strong>${escapeHtml(outcome?.classification || "UNKNOWN")}</strong></div>
          <div class="outcome-chip">Execution Path: <strong>${escapeHtml(outcome?.execution_path || "UNKNOWN")}</strong></div>
          <div class="outcome-chip">Gate stack: <strong>${escapeHtml(chipGate?.status || "UNKNOWN")}</strong></div>
          <div class="outcome-chip">Kill switch: <strong>${escapeHtml(chipKill?.state || "UNKNOWN")}</strong></div>
          <div class="outcome-chip">Structural readiness: <strong>${escapeHtml(readinessState)}</strong></div>
        </div>
        <div class="outcome-status-chip-row">
          ${statusChips.map(([label, value]) => `
            <span class="status-chip status-chip-compact status-chip-${semanticTone(label === "Engines" ? (present >= expected && expected > 0 ? "HEALTHY" : present > 0 ? "DEGRADED" : "FAIL") : value, "info")}">
              ${escapeHtml(label)}: ${escapeHtml(String(value))}
            </span>
          `).join("")}
        </div>
      </div>
      <div class="outcome-facts">
        ${facts.slice(0, 6).map((fact) => `
          <div class="outcome-fact">
            <div class="outcome-fact-label">${escapeHtml(fact?.label || "Fact")}</div>
            <div class="outcome-fact-value ${toneClass(
              String(fact?.value || "").toUpperCase() === "PASS" || String(fact?.value || "").toUpperCase() === "READY" || String(fact?.value || "").toUpperCase() === "HEALTHY"
                ? "positive"
                : String(fact?.value || "").toUpperCase().includes("SKIP") || String(fact?.value || "").toUpperCase().includes("NOT REACHED")
                  ? "warning"
                  : String(fact?.value || "").toUpperCase() === "FAIL" || String(fact?.value || "").toUpperCase() === "BLOCKED" || String(fact?.value || "").toUpperCase() === "MISSING"
                    ? "negative"
                    : "info"
            )}">${escapeHtml(fact?.value || "n/a")}</div>
          </div>
        `).join("")}
      </div>
    </div>
    <div class="execution-flow-strip">
      ${flowSteps.map(([label, value], idx) => `
        <div class="execution-flow-step">
          <div class="execution-flow-label">${escapeHtml(label)}</div>
          <div class="execution-flow-value status-chip status-chip-compact status-chip-${semanticTone(label === "Engines" ? (present >= expected && expected > 0 ? "HEALTHY" : present > 0 ? "DEGRADED" : "FAIL") : value, "info")}">${escapeHtml(String(value))}</div>
        </div>
        ${idx < flowSteps.length - 1 ? `<div class="execution-flow-divider" aria-hidden="true"></div>` : ""}
      `).join("")}
    </div>
    <div class="signal-frequency-band cockpit-${frequency?.status === "warning" ? "warning" : frequency?.status === "negative" ? "danger" : "success"}">
      <div class="signal-frequency-head">
        <div class="signal-frequency-title">Signal Frequency (30d)</div>
        <div class="signal-frequency-label ${toneClass(frequency?.status || "neutral")}">${escapeHtml(frequency?.label || "Signal frequency unavailable")}</div>
      </div>
      <div class="signal-frequency-grid">
        <div class="signal-frequency-item">
          <div class="signal-frequency-k">Avg Signals / Day</div>
          <div class="signal-frequency-v">${escapeHtml(
            frequency?.avg_signals_per_day === null || frequency?.avg_signals_per_day === undefined
              ? "n/a"
              : Number(frequency.avg_signals_per_day).toFixed(2)
          )}</div>
        </div>
        <div class="signal-frequency-item">
          <div class="signal-frequency-k">Last Signal</div>
          <div class="signal-frequency-v">${escapeHtml(frequency?.last_signal_date || "n/a")}</div>
        </div>
        <div class="signal-frequency-item">
          <div class="signal-frequency-k">Last Submit</div>
          <div class="signal-frequency-v">${escapeHtml(frequency?.last_submit_date || "n/a")}</div>
        </div>
      </div>
      ${sparkline}
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

function renderOperatorHome(payload) {
  state.operatorHome = payload;
  const home = payload?.home_view || {};
  const trust = payload?.trust_panel || {};
  const manifest = payload?.retrieval_manifest || {};
  const headline = el("operatorHomeHeadline");
  const meta = el("operatorHomeMeta");
  const trustMeta = el("operatorTrustMeta");
  const trustBody = el("operatorTrustBody");
  const panelsMeta = el("operatorPanelsMeta");
  const panelsBody = el("operatorHomePanels");
  const navBody = el("operatorNavigationBody");

  if (meta) meta.textContent = `scope=${home?.run_scope?.day_utc || "n/a"} • partiality=${home?.partiality_status || "n/a"}`;
  if (headline) {
    headline.innerHTML = `
      <div class="tile-state ${stateClass(home?.home_status_classification || trust?.exactness_classification)}">${escapeHtml(home?.home_status_classification || "UNAVAILABLE")}</div>
      <div class="mono tiny muted">readiness_ref=${escapeHtml(home?.readiness_ref || "n/a")}</div>
      <div class="mono tiny muted">daily_summary_ref=${escapeHtml(home?.daily_summary_ref || "n/a")}</div>
    `;
  }
  if (trustMeta) trustMeta.textContent = `finalization=${trust?.finalization_state || "n/a"} • exactness=${trust?.exactness_classification || "n/a"}`;
  if (trustBody) {
    const limitations = Array.isArray(trust?.bounded_limitations) ? trust.bounded_limitations : [];
    trustBody.innerHTML = `
      <div class="mono tiny">trust_classification=${escapeHtml(trust?.trust_classification || "UNAVAILABLE")}</div>
      <div class="mono tiny">integrity_state=${escapeHtml(trust?.integrity_state || "UNAVAILABLE")}</div>
      <div class="mono tiny">source_count=${escapeHtml(trust?.source_count ?? "n/a")}</div>
      <div class="mono tiny" style="margin-top:6px;">${limitations.length ? limitations.map((x) => `• ${escapeHtml(x)}`).join("<br/>") : "• No bounded limitations reported."}</div>
    `;
  }
  if (panelsMeta) panelsMeta.textContent = `rendered=${(home?.rendered_panels || []).length} • retrieval=${manifest?.retrieval_status || "n/a"}`;
  if (panelsBody) {
    const panels = Array.isArray(home?.rendered_panels) ? home.rendered_panels : [];
    panelsBody.innerHTML = panels.map((panel) => `
      <div class="cockpit-subtle-panel" style="margin-bottom:10px;">
        <div class="card-head">
          <div class="card-title">${escapeHtml(panel?.title || panel?.panel_id || "panel")}</div>
          <div class="mono tiny muted">${escapeHtml(panel?.state || "UNKNOWN")}${panel?.suppressed ? " • suppressed" : ""}</div>
        </div>
        <div class="mono tiny">${Array.isArray(panel?.content_lines) ? panel.content_lines.map((x) => `• ${escapeHtml(x)}`).join("<br/>") : "• No content."}</div>
      </div>
    `).join("");
  }
  if (navBody) {
    const items = Array.isArray(home?.navigation_options) ? home.navigation_options : [];
    navBody.innerHTML = items.map((item) => `
      <div class="info-row">
        <span>${escapeHtml(item?.label || item?.view_id || "view")}</span>
        <span class="mono ${item?.enabled ? "tone-positive" : "tone-warning"}">${escapeHtml(item?.enabled ? "ENABLED" : item?.reason || "DISABLED")}</span>
      </div>
    `).join("");
  }
}

function renderOperatorQuery(payload) {
  state.operatorQuery = payload;
  const response = payload?.query_response || {};
  const trust = payload?.trust_panel || {};
  const status = el("operatorQueryStatus");
  const body = el("operatorQueryBody");
  if (status) {
    status.textContent = `class=${response?.query_class_id || "n/a"} • status=${response?.response_status || "n/a"} • exactness=${trust?.exactness_classification || "n/a"}`;
  }
  if (body) {
    const blocks = Array.isArray(response?.answer_blocks) ? response.answer_blocks : [];
    body.innerHTML = blocks.map((block) => `
      <div class="cockpit-subtle-panel" style="margin-bottom:10px;">
        <div class="card-head">
          <div class="card-title">${escapeHtml(block?.title || block?.block_id || "block")}</div>
          <div class="mono tiny muted">${escapeHtml(response?.response_template_id || "template")}</div>
        </div>
        <div class="mono tiny">${Array.isArray(block?.lines) ? block.lines.map((x) => `• ${escapeHtml(x)}`).join("<br/>") : "• No detail."}</div>
      </div>
    `).join("");
  }
}

async function runOperatorQuery() {
  if (!state.day) return;
  const input = el("operatorQueryInput");
  const text = String(input?.value || "").trim();
  if (!text) {
    el("operatorQueryStatus").textContent = "query_text_required";
    el("operatorQueryBody").innerHTML = "";
    return;
  }
  const payload = await api(`/api/operator/query?day=${encodeURIComponent(state.day)}&q=${encodeURIComponent(text)}`);
  renderOperatorQuery(payload);
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
  const overlayAction = String(sum?.action_state || "HOLD").toUpperCase();
  const macroFallbackUsed = Boolean(sum?.macro_fallback_used);
  const macroFallbackMessage = String(sum?.macro_fallback_message || "Macro overlay active.");
  const macroRegimeLabel = String(sum?.macro_regime_label || "BASELINE_POLICY_FALLBACK");
  const strategyLabel = String(sum?.bond_strategy_label || "Baseline Ladder Reserve");
  const portfolioRole = String(sum?.portfolio_role || "Balanced Reserve");
  const durationTargetMin = sum?.duration_target_years_min ?? "n/a";
  const durationTargetMax = sum?.duration_target_years_max ?? "n/a";
  const treasuryTarget = sum?.treasury_target_weight ?? "n/a";
  const igTarget = sum?.ig_target_weight ?? "n/a";
  const liquidityHorizon = sum?.liquidity_reserve_horizon_years ?? "n/a";
  const fallbackChip = macroFallbackUsed ? "status-amber" : "status-green";
  const overlayActionChip = overlayAction === "BUILD" ? "status-amber" : overlayAction === "REBALANCE" ? "status-blue" : "status-green";
  const ladderShapeLabel = String(sum?.ladder_shape_label || "Baseline ladder");
  const explanationHeadline = String(sum?.explanation_headline || `${overlayAction}: ${strategyLabel}`);
  const explanationSummary = String(sum?.explanation_summary || macroFallbackMessage);
  const operatorNextStep = String(sum?.operator_next_step || nextStep);
  const whyNowList = Array.isArray(sum?.why_now) ? sum.why_now : [];
  const mismatchSummary = Array.isArray(sum?.mismatch_summary) ? sum.mismatch_summary : [];
  const topMismatchReasons = Array.isArray(sum?.top_mismatch_reasons) ? sum.top_mismatch_reasons : [];
  const mismatchSeverity = String(sum?.mismatch_severity || "NONE");
  const durationMismatch = String(sum?.duration_mismatch || "duration_within_band");
  const creditMixMismatch = Array.isArray(sum?.credit_mix_mismatch) ? sum.credit_mix_mismatch : [];
  const liquidityMismatch = String(sum?.liquidity_mismatch || "reserve_coverage_within_tolerance");
  const rolloverMismatch = String(sum?.rollover_concentration_mismatch || "bucket_weight_within_tolerance");
  const ladderShapeMismatch = Array.isArray(sum?.ladder_shape_mismatch) ? sum.ladder_shape_mismatch : [];
  const proxyMetrics = (sum?.proxy_metrics && typeof sum.proxy_metrics === "object") ? sum.proxy_metrics : {};
  const currentVsTarget = (sum?.current_vs_target && typeof sum.current_vs_target === "object") ? sum.current_vs_target : {};
  const currentLadder = Array.isArray(sum?.current_ladder) ? sum.current_ladder : [];
  const recommendedLadder = Array.isArray(sum?.recommended_ladder) ? sum.recommended_ladder : [];
  const ladderDeltas = Array.isArray(sum?.ladder_bucket_deltas) ? sum.ladder_bucket_deltas : [];
  const inputQuality = (sum?.input_quality && typeof sum.input_quality === "object") ? sum.input_quality : {};
  const candidateCoverageQuality = inputQuality?.candidate_coverage || {};
  const yieldCurveQuality = inputQuality?.yield_curve_coverage || {};
  const creditProxyQuality = inputQuality?.credit_proxy || {};
  const classificationQuality = inputQuality?.classification_coverage || {};
  const fallbackQuality = inputQuality?.fallback || {};
  const fmtWeightPct = (value) => {
    const n = Number(value);
    return Number.isFinite(n) ? `${(n * 100).toFixed(0)}%` : "n/a";
  };
  const formatBucketLabel = (bucketId) => String(bucketId || "n/a").replace("_", "-");
  const ladderRow = (label, rows, valueKey) => `
    <div class="bond-ladder-block">
      <div class="bond-ladder-title">${escapeHtml(label)}</div>
      <div class="bond-ladder-grid">
        ${(rows || []).map((row) => `
          <div class="bond-ladder-item">
            <span class="mono tiny muted">${escapeHtml(formatBucketLabel(row?.bucket_id))}</span>
            <strong>${escapeHtml(fmtWeightPct(row?.[valueKey]))}</strong>
          </div>
        `).join("") || `<div class="mono tiny muted">No ladder data available.</div>`}
      </div>
    </div>
  `;
  const mismatchBadgeClass = mismatchSeverity === "HIGH"
    ? "status-red"
    : mismatchSeverity === "MEDIUM"
      ? "status-amber"
      : mismatchSeverity === "LOW"
        ? "status-blue"
        : "status-green";
  const mismatchLine = (label, value) => `
    <div class="bond-overlay-metric">
      <span class="muted">${escapeHtml(label)}</span>
      <strong>${escapeHtml(value)}</strong>
    </div>
  `;
  const formatMismatchLabel = (value) => String(value || "n/a").replaceAll("_", " ");
  const ladderDeltaRows = ladderDeltas.length
    ? ladderDeltas.map((row) => `
      <div class="bond-ladder-item">
        <span class="mono tiny muted">${escapeHtml(formatBucketLabel(row?.bucket_id))}</span>
        <strong>${escapeHtml(fmtWeightPct(row?.gap_weight))}</strong>
      </div>
    `).join("")
    : `<div class="mono tiny muted">No ladder delta diagnostics available.</div>`;
  const qualityChipClass = (label) => {
    const normalized = String(label || "").toUpperCase();
    if (["GOOD", "AVAILABLE", "NONE"].includes(normalized)) return "status-green";
    if (["PARTIAL"].includes(normalized)) return "status-amber";
    if (["NOT AVAILABLE", "BASELINE POLICY ACTIVE"].includes(normalized)) return "status-amber";
    return "status-blue";
  };
  const qualityMissingText = (values, prefix) => {
    if (!Array.isArray(values) || !values.length) return "";
    return `${prefix}: ${values.join(", ")}`;
  };
  const inputQualityBlock = `
    <div class="bond-input-quality">
      <div class="bond-ladder-title">Bond Input Quality</div>
      <div class="bond-input-quality-grid">
        <div class="bond-input-quality-item">
          <span class="muted">Candidate coverage</span>
          <span class="status-chip ${qualityChipClass(candidateCoverageQuality?.label)}">${escapeHtml(candidateCoverageQuality?.label || "NOT AVAILABLE")}</span>
          <div class="mono tiny muted">${escapeHtml(qualityMissingText(candidateCoverageQuality?.missing_buckets, "Missing buckets") || `Eligible candidates: ${candidateCoverageQuality?.eligible_count ?? "n/a"}`)}</div>
        </div>
        <div class="bond-input-quality-item">
          <span class="muted">Curve coverage</span>
          <span class="status-chip ${qualityChipClass(yieldCurveQuality?.label)}">${escapeHtml(yieldCurveQuality?.label || "NOT AVAILABLE")}</span>
          <div class="mono tiny muted">${escapeHtml(qualityMissingText(yieldCurveQuality?.missing_buckets, "Missing buckets") || `Coverage: ${yieldCurveQuality?.coverage_status || "n/a"}`)}</div>
        </div>
        <div class="bond-input-quality-item">
          <span class="muted">Credit proxy</span>
          <span class="status-chip ${qualityChipClass(creditProxyQuality?.label)}">${escapeHtml(creditProxyQuality?.label || "NOT AVAILABLE")}</span>
          <div class="mono tiny muted">${escapeHtml(`Spread regime: ${creditProxyQuality?.spread_regime || "n/a"}`)}</div>
        </div>
        <div class="bond-input-quality-item">
          <span class="muted">Classification coverage</span>
          <span class="status-chip ${qualityChipClass(classificationQuality?.label)}">${escapeHtml(classificationQuality?.label || "NOT AVAILABLE")}</span>
          <div class="mono tiny muted">${escapeHtml(qualityMissingText(classificationQuality?.missing_dimensions, "Missing") || "All supported dimensions available.")}</div>
        </div>
        <div class="bond-input-quality-item">
          <span class="muted">Fallback</span>
          <span class="status-chip ${qualityChipClass(fallbackQuality?.label)}">${escapeHtml(fallbackQuality?.label || "NONE")}</span>
          <div class="mono tiny muted">${escapeHtml(fallbackQuality?.message || "Macro overlay active.")}</div>
        </div>
      </div>
    </div>
  `;
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
      <span class="status-chip status-blue">Duration ${sum?.weighted_duration ?? "n/a"}y</span>
      <span class="status-chip status-blue">Yield ${sum?.weighted_yield ?? "n/a"}</span>
      <span class="status-chip ${overlayActionChip}">Action ${overlayAction}</span>
      <span class="status-chip ${mismatchBadgeClass}">Mismatch ${escapeHtml(mismatchSeverity)}</span>
      <span class="status-chip ${authorityState === "AUTHORITY_HEAD_AVAILABLE" ? "status-green" : "status-amber"}">${authorityState === "AUTHORITY_HEAD_AVAILABLE" ? "Authoritative ladder available" : "Preliminary ladder available"}</span>
      <span class="status-chip ${fallbackChip}">${macroFallbackUsed ? "Baseline fallback active" : "Macro overlay active"}</span>
    </div>
    <div class="bond-overlay-summary">
      <div class="bond-overlay-hero">
        <div class="bond-overlay-label-row">
          <span class="status-chip status-blue">Bond Strategy ${escapeHtml(strategyLabel)}</span>
          <span class="status-chip status-blue">Portfolio Role ${escapeHtml(portfolioRole)}</span>
          <span class="status-chip ${fallbackChip}">${escapeHtml(macroRegimeLabel)}</span>
        </div>
        <div class="bond-overlay-headline">${escapeHtml(explanationHeadline)}</div>
        <div class="bond-overlay-summary-text">${escapeHtml(explanationSummary)}</div>
        <div class="bond-ladder-title">Targets</div>
        <div class="bond-overlay-grid">
          <div class="bond-overlay-metric"><span class="muted">Duration Target</span><strong>${escapeHtml(String(durationTargetMin))} to ${escapeHtml(String(durationTargetMax))}y</strong></div>
          <div class="bond-overlay-metric"><span class="muted">Treasury vs IG Mix</span><strong>${escapeHtml(fmtWeightPct(treasuryTarget))} / ${escapeHtml(fmtWeightPct(igTarget))}</strong></div>
          <div class="bond-overlay-metric"><span class="muted">Liquidity Horizon</span><strong>${escapeHtml(String(liquidityHorizon))} years</strong></div>
          <div class="bond-overlay-metric"><span class="muted">Ladder Shape</span><strong>${escapeHtml(ladderShapeLabel)}</strong></div>
        </div>
        <div class="bond-ladder-title">Action</div>
        <div class="bond-overlay-grid">
          <div class="bond-overlay-metric"><span class="muted">Action</span><strong>${escapeHtml(overlayAction)}</strong></div>
          <div class="bond-overlay-metric"><span class="muted">Mismatch Severity</span><strong>${escapeHtml(mismatchSeverity)}</strong></div>
          <div class="bond-overlay-metric"><span class="muted">Bond Strategy / Regime</span><strong>${escapeHtml(strategyLabel)}</strong></div>
          <div class="bond-overlay-metric"><span class="muted">Portfolio Role</span><strong>${escapeHtml(portfolioRole)}</strong></div>
        </div>
        <div class="bond-fallback-note ${macroFallbackUsed ? "bond-fallback-active" : "bond-fallback-clear"}">${escapeHtml(macroFallbackMessage)}</div>
        ${inputQualityBlock}
      </div>
      <div class="bond-ladder-compare">
        <div class="bond-ladder-title">Ladder Comparison</div>
        ${ladderRow("Current Ladder", currentLadder, "current_weight")}
        ${ladderRow("Recommended Ladder", recommendedLadder, "target_weight")}
        <div class="bond-ladder-block">
          <div class="bond-ladder-title">Bucket Deltas</div>
          <div class="bond-ladder-grid">${ladderDeltaRows}</div>
        </div>
      </div>
      <div class="bond-why-grid">
        <div class="bond-why-block">
          <div class="bond-ladder-title">Why Action</div>
          <div class="mono tiny">${whyNowList.length ? whyNowList.map(x => `• ${escapeHtml(x)}`).join("<br/>") : "• No overlay explanation available."}</div>
        </div>
        <div class="bond-why-block">
          <div class="bond-ladder-title">Mismatch Summary</div>
          <div class="mono tiny">${mismatchSummary.length ? mismatchSummary.map(x => `• ${escapeHtml(x)}`).join("<br/>") : "• Current holdings are close to target."}</div>
        </div>
      </div>
      <div class="bond-why-grid">
        <div class="bond-why-block">
          <div class="bond-ladder-title">Top Mismatch Reasons</div>
          <div class="mono tiny">${topMismatchReasons.length ? topMismatchReasons.map(x => `• ${escapeHtml(x)}`).join("<br/>") : "• No major mismatch reasons."}</div>
        </div>
        <div class="bond-why-block">
          <div class="bond-ladder-title">Deterministic Mismatch Classes</div>
          <div class="bond-overlay-grid">
            ${mismatchLine("Duration", formatMismatchLabel(durationMismatch))}
            ${mismatchLine("Credit Mix", creditMixMismatch.join(", ") || "credit mix within tolerance")}
            ${mismatchLine("Liquidity", formatMismatchLabel(liquidityMismatch))}
            ${mismatchLine("Rollover", formatMismatchLabel(rolloverMismatch))}
            ${mismatchLine("Ladder Shape", ladderShapeMismatch.join(", ") || "ladder shape within tolerance")}
          </div>
        </div>
      </div>
    </div>
    <div class="mono tiny muted" style="margin-top:6px;">Ladder status: ${ladderStatusText} • selected day ${selectedDay} • used day ${usedDay}</div>
    <details class="bond-drill sleeve-drill" style="margin-top:8px;">
      <summary class="mono tiny">Open bond advisory details</summary>
      <div class="mono tiny sleeve-drill-section" style="margin-top:6px;"><strong>Status:</strong> ${statusHeadline}</div>
      <div class="mono tiny sleeve-drill-section"><strong>Reason:</strong> ${reasonText}</div>
      <div class="mono tiny sleeve-drill-section"><strong>Next Step:</strong> ${escapeHtml(operatorNextStep)}</div>
      ${recommendationBlock}
      <div class="mono tiny sleeve-drill-section"><strong>Ladder summary:</strong> ${ladderSummary}</div>
      <div class="mono tiny">${ladderRecs.length ? ladderRecs.map(x => `• ${x}`).join("<br/>") : "Review will populate once a ladder recommendation is produced."}</div>
      <div class="mono tiny sleeve-drill-section"><strong>Current holdings:</strong> ${holdingsState === "OPERATOR_HOLDINGS_MISSING" ? "No bond positions have been recorded yet." : `Rows loaded: ${holdings?.positions_count ?? 0} • market value total: ${holdings?.market_value_total ?? "n/a"}`}</div>
      <table class="bond-table" style="margin-top:4px;">
        <thead><tr><th>#</th><th>Instrument</th><th>Maturity</th><th>Market Value</th></tr></thead>
        <tbody>${holdingsRows}</tbody>
      </table>
      <div class="mono tiny sleeve-drill-section"><strong>What to review next:</strong><br/>${reviewNext.map(x => `• ${x}`).join("<br/>")}</div>
      <div class="mono tiny sleeve-drill-section"><strong>Overlay trace:</strong><br/>
        regime=${escapeHtml(macroRegimeLabel)} • fallback_used=${macroFallbackUsed ? "true" : "false"}<br/>
        treasury_curve_slope_proxy=${escapeHtml(String(proxyMetrics?.treasury_curve_slope_proxy ?? "n/a"))} •
        ig_minus_treasury_candidate_spread_proxy=${escapeHtml(String(proxyMetrics?.ig_minus_treasury_candidate_spread_proxy ?? "n/a"))}
      </div>
      <details class="evidence-details" style="margin-top:6px;">
        <summary class="mono tiny">Overlay current vs target</summary>
        <div class="mono tiny muted" style="margin-top:6px;">${escapeHtml(JSON.stringify(currentVsTarget, null, 2))}</div>
      </details>
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
  const summaryHtml = `
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

  const pathsHtml = `
    <div class="mono tiny muted">warnings:</div>
    <div class="mono tiny">${(warn || []).map(x => `- ${x}`).join("<br/>") || "n/a"}</div>
    <div class="mono tiny muted" style="margin-top:10px;">missing_paths (top):</div>
    <div class="mono tiny">${mk(miss, "missing_path")}</div>
    <div class="mono tiny muted" style="margin-top:10px;">source_paths (top):</div>
    <div class="mono tiny">${mk(src, "source_path")}</div>
  `;
  const summaryTargets = ["techSummary", "techSummaryStandalone"];
  const pathsTargets = ["techPaths", "techPathsStandalone"];
  summaryTargets.forEach((id) => {
    const node = el(id);
    if (node) node.innerHTML = summaryHtml;
  });
  pathsTargets.forEach((id) => {
    const node = el(id);
    if (node) node.innerHTML = pathsHtml;
  });

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
  setRefreshState(true);
  try {
    const attemptParam = (state.attempt_id && el("attemptSelect").value) ? `&attempt_id=${encodeURIComponent(el("attemptSelect").value)}` : "";
    const payload = await api(`/api/status_v2?day=${encodeURIComponent(state.day)}${attemptParam}`);
    const operatorHome = await api(`/api/operator/home?day=${encodeURIComponent(state.day)}`);
    state.statusV2 = payload;
    state.operatorHome = operatorHome;

    el("lastRefresh").textContent = `refreshed=${payload?.meta?.server_time_utc || "n/a"}`;

    renderOperatorHome(operatorHome);
    renderPlatformReadiness(payload);
    renderPlatformReadinessHistory(payload);
    renderPlatformReadinessSummary(payload);
    renderDiagnosticsOverview(payload);
    renderTradingDayOutcome(payload);
    renderSignalActivity(payload);
    renderScopeHealth(payload);
    renderGovernedRiskSurfaces(payload);
    renderPortfolioSummary(payload);
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
    pulseUpdatedPanels();
  } finally {
    setRefreshState(false);
  }
}

function setRefreshState(isRefreshing) {
  document.body.classList.toggle("is-refreshing", isRefreshing === true);
}

function pulseUpdatedPanels() {
  document.querySelectorAll(".cockpit-panel, .cockpit-hero, .cockpit-subtle-panel").forEach((node) => {
    node.classList.remove("was-updated");
    void node.offsetWidth;
    node.classList.add("was-updated");
  });
  window.setTimeout(() => {
    document.querySelectorAll(".was-updated").forEach((node) => node.classList.remove("was-updated"));
  }, 180);
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
  el("cockpitViewHome").addEventListener("click", () => setOperationsSubView("home"));
  el("cockpitViewCockpit").addEventListener("click", () => setOperationsSubView("cockpit"));
  el("cockpitViewDiagnostics").addEventListener("click", () => setOperationsSubView("diagnostics"));
  el("cockpitViewRepair").addEventListener("click", () => setOperationsSubView("repair"));
  el("cockpitViewGovernance").addEventListener("click", () => setOperationsSubView("governance"));
  el("btnOperatorQuery").addEventListener("click", runOperatorQuery);
  el("operatorQueryInput").addEventListener("keydown", async (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      await runOperatorQuery();
    }
  });

  el("btnEvidenceClose").addEventListener("click", closeEvidence);
  el("evidenceBackdrop").addEventListener("click", closeEvidence);

  el("btnReloadCharts").addEventListener("click", loadCharts);
}

(async function boot() {
  wire();
  await loadDays();
  await loadAttemptsForDay(state.day);
  setView("operations");
  setOperationsSubView("home");
  await loadAndRender();
  await loadCharts();
  resetTimer();
})();
