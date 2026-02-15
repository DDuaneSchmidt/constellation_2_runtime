"use strict";

/**
 * Read-only ops UI:
 * - Polls JSON API
 * - Renders tables + minimal SVG charts (no external deps)
 * - Fail-closed banners: surface explicit error codes + missing file pointers
 */

const el = (id) => document.getElementById(id);

const state = {
  view: "today",
  refreshSec: 60,
  timer: null,
  days: [],
  day: null,
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

function renderBanners(objs) {
  const banners = el("banners");
  banners.innerHTML = "";

  const errors = uniq((objs || []).flatMap(o => o.errors || []));
  const warnings = uniq((objs || []).flatMap(o => o.warnings || []));
  const missing = uniq((objs || []).flatMap(o => o.missing_paths || []));

  if (!errors.length && !warnings.length && !missing.length) {
    banners.innerHTML = `<div class="banner ok"><div class="code">OK</div></div>`;
    return;
  }

  errors.forEach(code => {
    banners.innerHTML += `<div class="banner err"><div class="code">ERROR: ${code}</div></div>`;
  });
  warnings.forEach(code => {
    banners.innerHTML += `<div class="banner warn"><div class="code">WARN: ${code}</div></div>`;
  });
  if (missing.length) {
    banners.innerHTML += `<div class="banner warn"><div class="code">MISSING PATHS</div><div class="small mono muted">${missing.slice(0, 20).join("<br/>")}</div></div>`;
  }
}

function table(headers, rows) {
  const h = headers.map(x => `<th>${x}</th>`).join("");
  const b = rows.map(r => `<tr>${r.map(x => `<td>${x}</td>`).join("")}</tr>`).join("");
  return `<table class="table"><thead><tr>${h}</tr></thead><tbody>${b}</tbody></table>`;
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
    el("engineJoinNote").innerText =
      "ENGINE_JOIN_NOT_POSSIBLE_WITHOUT_ENGINE_LINKAGE";
  } else {
    el("engineJoinNote").innerText = "";
  }
}

function renderPlan(planResp) {
  if (!(planResp.plans || []).length) {
    el("planTable").innerHTML =
      `<div class="mono small muted">NO_ORDER_PLAN_PRESENT</div>`;
    return;
  }
  const rows = planResp.plans.map(p => {
    const keys = Object.keys(p.order_plan || {}).slice(0,10).join(",");
    return [p.submission_id, keys];
  });
  el("planTable").innerHTML =
    table(["submission_id","order_plan_top_keys"], rows);
}

function renderSubmissions(subResp) {
  const subs = subResp.submissions || [];
  if (!subs.length) {
    el("submissionsTable").innerHTML =
      `<div class="mono small muted">NO_SUBMISSIONS_FOUND</div>`;
    return;
  }
  const rows = subs.map(s => {
    const b = s.broker_submission_record || {};
    const e = s.execution_event_record || {};
    return [
      s.submission_id,
      s.engine,
      b.status ?? "n/a",
      e.status ?? "NONE"
    ];
  });
  el("submissionsTable").innerHTML =
    table(["submission_id","engine","broker_status","event_status"], rows);
}

function svgLineChart(points) {
  const W = 900, H = 260, pad = 28;
  const vals = points.map(p => Number(p.nav_end)).filter(v => !isNaN(v));
  if (vals.length < 2) return `<div class="mono small muted">Insufficient data</div>`;
  const min = Math.min(...vals);
  const max = Math.max(...vals);
  const span = (max - min) || 1;
  const xs = points.map((_, i) =>
    pad + (i * (W - 2*pad) / Math.max(1, points.length-1)));
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
  const sel = el("daySelect");
  sel.innerHTML="";
  state.days.forEach(day=>{
    const o=document.createElement("option");
    o.value=day; o.textContent=day;
    if(day===state.day) o.selected=true;
    sel.appendChild(o);
  });
}

async function loadToday() {
  if(!state.day) return;
  const [sum,plan,subs] = await Promise.all([
    api(`/api/day/${state.day}/summary`),
    api(`/api/day/${state.day}/plan`),
    api(`/api/day/${state.day}/submissions`)
  ]);
  renderBanners([sum,plan,subs]);
  renderSummary(sum);
  renderByEngine(sum);
  renderPlan(plan);
  renderSubmissions(subs);
}

async function loadHistory() {
  const rows=[];
  for(const d of state.days.slice(-20)){
    const s=await api(`/api/day/${d}/summary`);
    rows.push([
      d,
      s.nav?.nav_end ?? "n/a",
      s.counts?.submissions ?? 0
    ]);
  }
  el("historyTable").innerHTML =
    table(["day","nav_end","subs"], rows);
}

async function loadCharts() {
  const n = Number(el("navDays").value)||60;
  const nav = await api(`/api/series/nav?days=${n}`);
  renderBanners([nav]);
  el("chartDailyNav").innerHTML =
    svgLineChart(nav.points||[]);
  el("chartCumulative").innerHTML =
    svgLineChart(nav.points||[]);
}

function resetTimer(){
  if(state.timer) clearInterval(state.timer);
  state.timer=setInterval(()=>{
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
}

(async function boot(){
  wire();
  await loadDays();
  setView("today");
  await loadToday();
  resetTimer();
})();
