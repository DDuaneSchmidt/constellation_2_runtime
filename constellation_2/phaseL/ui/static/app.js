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
  el("viewHistory").classList.toggle("hidden", v !== "history");
  el("viewTechnical").classList.toggle("hidden", v !== "technical");

  const tabs = [
    ["tabOperations", "operations"],
    ["tabEngines", "engines"],
    ["tabPortfolio", "portfolio"],
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

function renderSleeveStrip(payload) {
  const sleeves = (payload?.sleeves || []);
  const container = el("sleeveStrip");
  container.innerHTML = `
    <div class="card-head">
      <div class="card-title">Sleeve Mode Strip (PAPER/LIVE per sleeve)</div>
      <div class="mono tiny muted">IB account shown per sleeve</div>
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
      <span class="mono">${s.sleeve_id}</span>
      <span class="${modeClass(mode, s.flatten_only)} mono">${String(mode).toUpperCase()}</span>
      <span class="mono tiny muted">${acct}</span>
      <span class="mono tiny muted">${ea}</span>
      ${fl ? `<span class="mono tiny muted">${fl}</span>` : ""}
    `;
    row.appendChild(pill);
  });
}

function renderFunnel(payload) {
  const c = payload?.trade_flow_today?.counts || {};
  const b = payload?.trade_flow_today?.blocked_by_gate || {};

  const steps = [
    ["Intents", c.intents],
    ["Authorized", c.authorized],
    ["Submitted", c.submitted],
    ["Filled", c.filled],
    ["Reconciled", c.reconciled],
  ];

  el("funnelRow").innerHTML = steps.map(([k,v]) => `
    <div class="funnel-step">
      <div class="k">${k}</div>
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
    `<div class="blocked-pill">${k}: ${v === null || v === undefined ? "n/a" : v}</div>`
  ).join("");
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
  el("historyAttempts").innerHTML = `
    <div class="mono small">day=${day}</div>
    <div class="mono small">selected_attempt=${sel}</div>
    <div class="mono small" style="margin-top:8px;">attempts:</div>
    <div class="mono small">${attempts.length ? attempts.map(a => `- ${a}`).join("<br/>") : "ATTEMPTS_NOT_FOUND"}</div>
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

async function openEvidence(title, path) {
  // Raw JSON only via modal (explicit click).
  const q = encodeURIComponent(path);
  const r = await api(`/api/artifact?path=${q}`);
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
  state.attempt_id = state.attempts.length ? state.attempts[state.attempts.length - 1] : null;

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

  renderTiles(payload);
  renderSleeveStrip(payload);
  renderFunnel(payload);
  renderWhatChanged(payload);

  renderEngines(payload);
  renderPortfolio(payload);
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
