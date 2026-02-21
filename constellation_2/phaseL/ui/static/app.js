"use strict";

const el = (id) => document.getElementById(id);

const state = {
  view: "today",
  refreshSec: 60,
  timer: null,
  days: [],
  day: null,
  c3Status: null,
  missingExpanded: false,
  evidence: { title: "", path: "", content: "", ok: false, errors: [] },
};

function fmtMtimeSecToIso(mtimeSec) {
  if (mtimeSec == null) return "n/a";
  const ms = Math.floor(mtimeSec * 1000);
  const d = new Date(ms);
  return d.toISOString();
}

function uniq(arr) {
  return Array.from(new Set(arr || []));
}

async function api(path) {
  const r = await fetch(path, { cache: "no-store" });
  return await r.json();
}

function setView(v) {
  state.view = v;
  el("viewToday").classList.toggle("hidden", v !== "today");
  el("viewHistory").classList.toggle("hidden", v !== "history");
  el("viewCharts").classList.toggle("hidden", v !== "charts");
}

function table(headers, rows) {
  const h = headers.map(x => `<th>${x}</th>`).join("");
  const b = rows.map(r => `<tr>${r.map(x => `<td>${x}</td>`).join("")}</tr>`).join("");
  return `<table class="table"><thead><tr>${h}</tr></thead><tbody>${b}</tbody></table>`;
}

function badge(text, kind) {
  const k = kind || "warn";
  return `<span class="badge ${k}">${text}</span>`;
}

function badgeForState(st) {
  if (st === "PASS" || st === "OK" || st === "PRESENT") return badge(st, "ok");
  if (st === "FAIL" || st === "MISSING") return badge(st, "err");
  if (st === "DEGRADED" || st === "UNKNOWN") return badge(st, "warn");
  return badge(st ?? "n/a", "warn");
}

function openEvidence(title, path, content, ok, errors) {
  state.evidence = { title, path, content, ok, errors: errors || [] };
  el("evidenceTitle").textContent = title || "Evidence";
  el("evidencePath").textContent = path || "n/a";
  el("evidenceErrors").innerHTML = (errors || []).length
    ? `<div class="mono small">${errors.map(e => `ERROR: ${e}`).join("<br/>")}</div>`
    : "";
  el("evidenceBody").textContent = content || "";
  el("evidenceModal").classList.remove("hidden");
}

function closeEvidence() {
  el("evidenceModal").classList.add("hidden");
}

async function fetchArtifact(path, title) {
  if (!path) {
    openEvidence(title || "Artifact", "n/a", "", false, ["NO_PATH"]);
    return;
  }
  const q = encodeURIComponent(path);
  const r = await api(`/api/artifact?path=${q}`);
  openEvidence(title || "Artifact", r.path || path, r.content || "", !!r.ok, r.errors || []);
}

function renderBanners(objs) {
  const banners = el("banners");
  banners.innerHTML = "";

  const deny = (objs || []).some(o => o && o.overall_state === "FAIL");
  if (deny) {
    banners.innerHTML += `<div class="banner err"><div class="code">ENTRIES DENIED (FAIL-CLOSED)</div></div>`;
  }

  const errors = uniq((objs || []).flatMap(o => o.errors || []));
  const warnings = uniq((objs || []).flatMap(o => o.warnings || []));
  const missing = uniq((objs || []).flatMap(o => o.missing_paths || []));

  errors.forEach(code => {
    banners.innerHTML += `<div class="banner err"><div class="code">ERROR: ${code}</div></div>`;
  });
  warnings.forEach(code => {
    banners.innerHTML += `<div class="banner warn"><div class="code">WARN: ${code}</div></div>`;
  });

  if (missing.length) {
    const top = missing.slice(0, 6);
    const rest = missing.slice(6);
    const btn = `<button class="btn btn-mini" id="btnToggleMissing">${state.missingExpanded ? "Hide" : "Show"} missing paths</button>`;
    const body = state.missingExpanded
      ? missing.join("<br/>")
      : top.join("<br/>") + (rest.length ? `<br/><span class="muted mono small">(+${rest.length} more)</span>` : "");
    banners.innerHTML += `<div class="banner warn">
      <div class="code">MISSING PATHS ${btn}</div>
      <div class="small mono muted" style="margin-top:6px;">${body}</div>
    </div>`;
    const b = document.getElementById("btnToggleMissing");
    if (b) {
      b.onclick = () => { state.missingExpanded = !state.missingExpanded; renderBanners(objs); };
    }
  }

  if (!deny && !errors.length && !warnings.length && !missing.length) {
    banners.innerHTML = `<div class="banner ok"><div class="code">OK</div></div>`;
  }
}

function renderC3Status(st) {
  if (!st) return;

  el("c3StatusMeta").innerHTML =
    `schema_version=${st.schema_version || "n/a"}<br/>` +
    `generated_at_utc=${st.generated_at_utc || st.generated_utc || "n/a"}<br/>` +
    `day=${st.verdict?.day || "n/a"} source=${st.verdict?.source || "n/a"}`;

  const v = st.verdict || {};
  const rc = (v.reason_codes_top || []).join(", ") || "n/a";
  const rf = (v.required_failures_top || []).map(x => `${x.gate_id}:${x.status}`).join(", ") || "n/a";
  const verdictLink = v.artifact_path
    ? `<a href="#" class="link mono" data-artifact="${encodeURIComponent(v.artifact_path)}" data-title="gate_stack_verdict">open verdict</a>`
    : "";

  el("c3Verdict").innerHTML =
    `${badgeForState(v.state)} <span class="mono small muted">${v.source || ""}</span> ${verdictLink}<br/>` +
    `<span class="mono small muted">blocking_class=${v.blocking_class || "n/a"}</span><br/>` +
    `<span class="mono small muted">reason_codes_top=${rc}</span><br/>` +
    `<span class="mono small muted">required_failures_top=${rf}</span>`;

  // Gates expansion table
  const gates = v.gates_top || [];
  if (!gates.length) {
    el("c3Gates").innerHTML = `<div class="mono small muted">No gate rows.</div>`;
  } else {
    const rows = gates.map(g => {
      const link = g.artifact_path
        ? `<a href="#" class="link mono" data-artifact="${encodeURIComponent(g.artifact_path)}" data-title="${g.gate_id}">open</a>`
        : "";
      const rcg = (g.reason_codes_top || []).join(", ");
      return [
        g.gate_id,
        g.required ? badge("required", "warn") : `<span class="mono small muted">optional</span>`,
        g.blocking ? badge("blocking", "err") : `<span class="mono small muted">non-blocking</span>`,
        `<span class="mono small muted">${g.gate_class || "n/a"}</span>`,
        badgeForState(g.status),
        `<span class="mono small muted">${rcg || ""}</span>`,
        link,
      ];
    });
    el("c3Gates").innerHTML = table(
      ["gate_id", "required", "blocking", "class", "status", "reason_codes_top", "artifact"],
      rows
    );
  }

  const br = st.broker_reconciliation || {};
  const brEvidence =
    `cash_diff=${br.cash_diff ?? "n/a"} ` +
    `mismatches=${br.position_mismatches_count ?? "n/a"} ` +
    `notes=${br.notes_count ?? "n/a"}`;
  const brLink = br.artifact_path
    ? `<a href="#" class="link mono" data-artifact="${encodeURIComponent(br.artifact_path)}" data-title="broker_reconciliation">open reconciliation</a>`
    : "";

  el("c3Broker").innerHTML =
    `${badgeForState(br.state)} <span class="mono small muted">day=${br.day || "n/a"} acct=${br.account || "n/a"}</span> ${brLink}<br/>` +
    `<span class="mono small muted">${brEvidence}</span>`;

  // Broker mismatches expansion table
  const mm = br.mismatches_top || [];
  if (!mm.length) {
    el("c3Mismatches").innerHTML = `<div class="mono small muted">No mismatches.</div>`;
  } else {
    const rows = mm.map(m => ([
      m.symbol,
      `<span class="mono small muted">${m.sec_type}</span>`,
      m.broker_qty,
      m.internal_qty,
      m.qty_diff,
    ]));
    el("c3Mismatches").innerHTML = table(["symbol", "sec_type", "broker_qty", "internal_qty", "qty_diff"], rows);
  }

  el("c3Market").innerHTML =
    `${badgeForState(st.market_data?.state)} ` +
    `<span class="mono small muted">latest=${st.market_data?.latest_snapshot_day || "n/a"}</span>`;

  el("c3Overall").innerHTML =
    `${badgeForState(st.overall_state)} <span class="mono small muted">fail-closed</span>`;

  const comps = st.components || [];
  if (!comps.length) {
    el("c3Components").innerHTML = `<div class="mono small muted">No derived components.</div>`;
  } else {
    const rows = comps.map(c => ([
      c.name,
      badgeForState(c.state),
      `<span class="mono small muted">${c.reason_code || ""}</span>`,
    ]));
    el("c3Components").innerHTML = table(["name", "state", "reason_code"], rows);
  }

  // Provenance: clickable sources
  const src = (st.source_paths || []).slice(0, 10);
  const srcLines = src.map(p => {
    const ep = encodeURIComponent(p);
    return `- <a href="#" class="link mono" data-artifact="${ep}" data-title="source">${p}</a>`;
  }).join("<br/>");

  const mt = st.source_mtimes || {};
  const mtLines = Object.keys(mt).slice(0, 10)
    .map(k => `- ${k} mtime=${fmtMtimeSecToIso(mt[k])}`)
    .join("<br/>");

  el("footerProvenance").innerHTML =
    `<div>status_endpoint=/api/status</div>` +
    `<div style="margin-top:6px;">sources:<br/><span class="mono small">${srcLines || "n/a"}</span></div>` +
    `<div style="margin-top:6px;">mtimes:<br/><span class="mono small">${mtLines || "n/a"}</span></div>`;

  // Wire evidence links inside the rendered HTML
  document.querySelectorAll("[data-artifact]").forEach(a => {
    a.onclick = (ev) => {
      ev.preventDefault();
      const path = decodeURIComponent(a.getAttribute("data-artifact") || "");
      const title = a.getAttribute("data-title") || "artifact";
      fetchArtifact(path, title);
    };
  });
}

function renderSummary(sum) {
  el("summaryMeta").innerHTML =
    `generated_utc=${sum.generated_utc || "n/a"}<br/>` +
    `freshness=${fmtMtimeSecToIso(sum.data_freshness_max_mtime)}<br/>` +
    `day=${sum.day_utc || "n/a"}`;

  const c = sum.counts || {};
  const rows = Object.entries(c).map(([k, v]) =>
    `<div class="mono small"><span class="muted">${k}</span> = ${v ?? "n/a"}</div>`
  ).join("");

  const nav = sum.nav ? `nav_end=${sum.nav.nav_end ?? "n/a"}` : "nav=n/a";
  el("summaryCounts").innerHTML = rows + `<div class="mono small muted">${nav}</div>`;
}

function renderByEngine(sum) {
  const rows = (sum.by_engine || []).map(e => ([
    e.engine,
    e.submissions,
    e.fills,
    e.rejects,
    e.errors
  ]));
  el("byEngine").innerHTML = rows.length
    ? table(["engine","subs","fills","rejects","errors"], rows)
    : `<div class="mono small muted">No data.</div>`;

  if ((sum.warnings || []).includes("ENGINE_JOIN_NOT_POSSIBLE_WITHOUT_ENGINE_LINKAGE")) {
    el("engineJoinNote").innerText = "ENGINE_JOIN_NOT_POSSIBLE_WITHOUT_ENGINE_LINKAGE";
  } else {
    el("engineJoinNote").innerText = "";
  }
}

function renderPlan(planResp) {
  if (!(planResp.plans || []).length) {
    el("planTable").innerHTML = `<div class="mono small muted">NO_ORDER_PLAN_PRESENT</div>`;
    return;
  }
  const rows = planResp.plans.map(p => {
    const keys = Object.keys(p.order_plan || {}).slice(0,10).join(",");
    return [p.submission_id, keys];
  });
  el("planTable").innerHTML = table(["submission_id","order_plan_top_keys"], rows);
}

function renderSubmissions(subResp) {
  const subs = subResp.submissions || [];
  if (!subs.length) {
    el("submissionsTable").innerHTML = `<div class="mono small muted">NO_SUBMISSIONS_FOUND</div>`;
    return;
  }
  const rows = subs.map(s => {
    const b = s.broker_submission_record || {};
    const e = s.execution_event_record || {};
    return [s.submission_id, s.engine, b.status ?? "n/a", e.status ?? "NONE"];
  });
  el("submissionsTable").innerHTML = table(["submission_id","engine","broker_status","event_status"], rows);
}

function svgLineChart(points) {
  const W = 900, H = 260, pad = 28;
  const vals = points.map(p => Number(p.nav_end)).filter(v => !isNaN(v));
  if (vals.length < 2) return `<div class="mono small muted">Insufficient data</div>`;
  const min = Math.min(...vals);
  const max = Math.max(...vals);
  const span = (max - min) || 1;
  const xs = points.map((_, i) => pad + (i * (W - 2*pad) / Math.max(1, points.length-1)));
  const ys = points.map(p => {
    const v = Number(p.nav_end);
    if (isNaN(v)) return null;
    return (H-pad) - ((v-min)/span)*(H-2*pad);
  });
  let d="";
  for (let i=0;i<points.length;i++){
    if (ys[i]==null) continue;
    d += (d?" L ":"M ") + xs[i] + " " + ys[i];
  }
  return `<svg viewBox="0 0 ${W} ${H}">
    <path d="${d}" fill="none" stroke="#7ee787" stroke-width="2"></path>
  </svg>`;
}

async function loadDays() {
  const d = await api("/api/days");
  renderBanners([d]);
  state.days = d.days || [];
  state.day = d.default_day_utc;

  // Prefer canonical latest.json day when available
  try {
    const ld = await api("/api/latest_day");
    const latestDay = ld.day_utc;
    if (latestDay && state.days.includes(latestDay)) {
      state.day = latestDay;
    }
  } catch (_) {
    // fail closed: keep default
  }

  const sel = el("daySelect");
  sel.innerHTML="";
  state.days.forEach(day=>{
    const o=document.createElement("option");
    o.value=day; o.textContent=day;
    if(day===state.day) o.selected=true;
    sel.appendChild(o);
  });
}

async function loadStatus() {
  const st = await api("/api/status");
  state.c3Status = st;
  renderC3Status(st);
  return st;
}

async function loadToday() {
  if(!state.day) return;
  const [status,sum,plan,subs] = await Promise.all([
    loadStatus(),
    api(`/api/day/${state.day}/summary`),
    api(`/api/day/${state.day}/plan`),
    api(`/api/day/${state.day}/submissions`)
  ]);
  renderBanners([status,sum,plan,subs]);
  renderSummary(sum);
  renderByEngine(sum);
  renderPlan(plan);
  renderSubmissions(subs);
}

async function loadHistory() {
  const rows=[];
  for(const d of state.days.slice(-20)){
    const s=await api(`/api/day/${d}/summary`);
    rows.push([d, s.nav?.nav_end ?? "n/a", s.counts?.submissions ?? 0]);
  }
  el("historyTable").innerHTML = table(["day","nav_end","subs"], rows);
}

async function loadCharts() {
  const n = Number(el("navDays").value)||60;
  const nav = await api(`/api/series/nav?days=${n}`);
  renderBanners([nav]);
  el("chartDailyNav").innerHTML = svgLineChart(nav.points||[]);
  el("chartCumulative").innerHTML = svgLineChart(nav.points||[]);
}

function resetTimer(){
  if(state.timer) clearInterval(state.timer);
  state.timer=setInterval(()=>{
    loadStatus();
    if(state.view==="today") loadToday();
    if(state.view==="history") loadHistory();
    if(state.view==="charts") loadCharts();
  }, state.refreshSec*1000);
}

function wire(){
  el("refreshSelect").addEventListener("change",()=>{
    state.refreshSec=Number(el("refreshSelect").value)||60;
    resetTimer();
  });
  el("daySelect").addEventListener("change",()=>{
    state.day=el("daySelect").value;
    loadToday();
  });
  el("btnToday").addEventListener("click",()=>{setView("today");loadToday();});
  el("btnHistory").addEventListener("click",()=>{setView("history");loadHistory();});
  el("btnCharts").addEventListener("click",()=>{setView("charts");loadCharts();});
  el("btnReloadCharts").addEventListener("click",loadCharts);

  el("btnEvidenceClose").addEventListener("click", closeEvidence);
  el("evidenceBackdrop").addEventListener("click", closeEvidence);
}

(async function boot(){
  wire();
  await loadDays();
  await loadStatus();
  setView("today");
  await loadToday();
  resetTimer();
})();
