import { escapeHtml, renderError, renderOperatorDiagnosticPanel } from "/operator_shell/shared_components/dom.js";
import { renderAegisMark, renderStatusPill } from "/operator_shell/aegis_components/index.js";
import {
  ROUTES,
  activeNavSelectionForPath,
  buildPaletteEntries,
  executeCandidateWorkflow,
  executeEdgeLabWorkflow,
  executeManualCaptureRecordWorkflow,
  executeConfigurationWorkflow,
  executeReliabilityWorkflow,
  executeOperatorQuery,
  executeResearchConsoleWorkflow,
  executeResearchDataAcquisitionWorkflow,
  loadRouteView,
  normalizeRoutePath,
  operatorShellRouteContractForPath,
  routeShouldPreserveDayQuery,
} from "/operator_shell/pages/index.js";
import {
  ENGINEERING_NAVIGATION_SCHEMA,
  NAVIGATION_SCHEMA,
  activeNavigationForPath,
} from "/operator_shell/navigation_schema.js";
import {
  executeAegisCommand,
  executeAegisVerifiedRuntimeAction,
  executePaperPromotionAction,
  appendOperatorActionEvent,
  fetchAegisCommandStatus,
  fetchAlerts,
  fetchFinancialState,
  fetchOperatorWorkflow,
  fetchRuntimeStatus,
  askAegisAiOperations,
  fetchStatusRail,
  fetchStatusSemantics,
  fetchSystemSummary,
  recordAegisCommand,
  recordAegisChangeControlDecision,
  saveAegisAdvisorBenchmark,
} from "/operator_shell/domain_client/index.js";
import { formatUsd } from "/operator_shell/pages/index.js";

// Legacy shell contract markers retained for readonly/operator workflow tests.
const LEGACY_ROUTE_CONTRACT = [
  { path: "/", id: "work-queue" },
  { path: "/control", id: "control" },
  { path: "/state", id: "state" },
  { path: "/advisory", id: "advisory" },
  { path: "/submission", id: "submission" },
  { path: "/lifecycle", id: "lifecycle" },
];
const OPERATIONAL_LAYOUT = "OPERATIONAL_LAYOUT";
const WORKFLOW_LAYOUT = "WORKFLOW_LAYOUT";
const OPERATIONAL_LAYOUT_ROUTE_IDS = new Set([
  "aegis_opportunities",
  "aegis_today",
  "aegis_runtime_timeline",
  "aegis_repair_center",
]);
const WORKFLOW_LAYOUT_ROUTE_IDS = new Set([
  "aegis_candidates",
  "aegis_candidate_funnel",
  "aegis_exit_review",
  "aegis_theses",
  "aegis_edge_lab",
  "aegis_paper_performance",
  "aegis_sleeve_validation",
  "aegis_sleeve_analytics",
  "aegis_performance",
  "aegis_journal",
  "aegis_captured_trades",
  "aegis_review",
  "aegis_research",
  "aegis_history",
  "research_lab",
  "research_start",
  "research_hypothesis_queue",
  "research_plans",
  "research_evidence",
  "research_paper_trials",
  "research_sleeve_reviews",
  "research_blocked_work",
  "research_backlog",
]);

function resolveWorkspaceLayoutMode(route, view = {}) {
  if (view.layoutMode === WORKFLOW_LAYOUT || view.layoutMode === OPERATIONAL_LAYOUT) return view.layoutMode;
  if (WORKFLOW_LAYOUT_ROUTE_IDS.has(route?.id)) return WORKFLOW_LAYOUT;
  if (OPERATIONAL_LAYOUT_ROUTE_IDS.has(route?.id)) return OPERATIONAL_LAYOUT;
  return OPERATIONAL_LAYOUT;
}

const LEGACY_ENDPOINT_CONTRACT = [
  'fetchJson("/api/shell/status-rail")',
  'fetchJson("/api/work-queue")',
  'fetchJson(`/api/workspace/${route.id}`)',
  'fetchJson("/api/system/summary")',
  'fetchJson("/api/shared/status-semantics")',
  'fetchJson("/api/operations")',
  'fetchJson("/api/advisory")',
  'fetchJson("/api/orders")',
  'fetchJson("/api/positions")',
  'fetchJson("/api/reconciliation")',
  'fetchJson("/api/alerts")',
  'fetchJson("/api/integrity")',
  'fetchJson("/api/system/actions")',
];

const state = {
  semantics: {},
  shell: {
    statusRail: [],
    systemSummary: null,
    operatorWorkflow: null,
    alerts: null,
    financialState: null,
    runtimeStatus: null,
    refreshedAt: null,
  },
  commandQueryText: "",
  commandQueryResult: null,
  configurationWorkflow: {
    activeDraftId: "",
    latestDraft: null,
    latestResult: null,
    lastAction: null,
    lastError: null,
  },
  reliabilityWorkflow: {
    lastAction: null,
    lastResult: null,
    lastAssessment: null,
    lastIssue: null,
    lastDraft: null,
    lastError: null,
  },
  connection: {
    state: "RECONNECTING",
    last_checked_at: null,
    recovery_command: "npm run aegis:ui:restart",
  },
  activeView: null,
  openCommandDetail: null,
  routeRenderInProgress: 0,
  routeRenderGeneration: 0,
  routeRenderedOnce: false,
  lastRenderedRouteId: "",
  routeCache: new Map(),
  bootDiagnostics: {
    bootStart: 0,
    boot_time_ms: 0,
    first_paint_ms: 0,
    data_ready_ms: 0,
    duplicate_fetch_count: 0,
    route_rerender_count: 0,
    content_clear_count: 0,
  },
  paletteOpen: false,
  sidebarMode: localStorage.getItem("aegis.sidebar.mode") || "expanded",
  operatorMode: "operator",
  engineeringDrawerOpen: false,
  sidebarOpenGroups: new Set(NAVIGATION_SCHEMA.flatMap((section) => section.domains.map((domain) => domain.id))),
};

function logTiming(phase, startedAt, extra = {}) {
  const durationMs = Math.round((performance.now() - startedAt) * 10) / 10;
  console.info("[aegis-ui-timing]", { phase, duration_ms: durationMs, ...extra });
}

function markBootEvent(source, extra = {}) {
  const now = performance.now();
  const bootStart = state.bootDiagnostics.bootStart || now;
  const event = { source, at_ms: Math.round((now - bootStart) * 10) / 10, ...extra };
  state.bootDiagnostics.events = [...(state.bootDiagnostics.events || []), event].slice(-80);
  if (typeof window !== "undefined") {
    window.__AEGIS_BOOT_DIAGNOSTICS = { ...state.bootDiagnostics };
  }
  console.info(`[aegis-boot] ${source}`, event);
}

function updateBootDiagnostic(key, value) {
  state.bootDiagnostics[key] = value;
  if (typeof window !== "undefined") {
    state.bootDiagnostics.duplicate_fetch_count = Number(window.__AEGIS_DUPLICATE_FETCH_COUNT || 0);
    window.__AEGIS_BOOT_DIAGNOSTICS = { ...state.bootDiagnostics };
  }
}

async function timedAsync(phase, action, extra = {}) {
  const startedAt = performance.now();
  try {
    return await action();
  } finally {
    logTiming(phase, startedAt, extra);
  }
}

function setWorkspaceUpdating({ active = false, warning = "" } = {}) {
  const host = document.getElementById("workspaceContent");
  if (!host) return;
  let indicator = host.querySelector(":scope > .workspace-refresh-indicator");
  if (!active && !warning) {
    indicator?.remove();
    return;
  }
  if (!indicator) {
    indicator = document.createElement("div");
    indicator.className = "workspace-refresh-indicator";
    host.prepend(indicator);
  }
  indicator.dataset.tone = warning ? "warning" : "updating";
  indicator.textContent = warning || "Updating...";
}

function formatHeaderTimestamp(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return "not reported";
  }
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) {
    return raw;
  }
  return parsed.toLocaleString(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
    timeZoneName: "short",
  });
}

function renderHeaderOperationalTimestamps(timestamps = {}) {
  const timing = timestamps.eod_pipeline_timing || {};
  const rows = [
    ["Operational day", timestamps.operational_day || "not reported"],
    ["Market data", formatHeaderTimestamp(timestamps.market_data_last_updated_at)],
    ["Candidate snapshot", formatHeaderTimestamp(timestamps.candidate_snapshot_timestamp)],
    ["Certification attempt", formatHeaderTimestamp(timestamps.last_certification_attempt_at)],
    ["Final EOD", timestamps.final_eod_certification_completed_at ? formatHeaderTimestamp(timestamps.final_eod_certification_completed_at) : (timing.final_certification_status || "pending/not reported")],
  ];
  return rows.map(([label, value]) => `<span class="header-operational-timestamp"><strong>${escapeHtml(label)}:</strong> ${escapeHtml(value)}</span>`).join("");
}

function formatDisplayLabel(value) {
  const text = String(value || "Unknown").trim();
  if (!text) {
    return "Unknown";
  }
  return text
    .toLowerCase()
    .split(/([\s/_-]+)/)
    .map((part) => /^[a-z0-9]/.test(part) ? part.charAt(0).toUpperCase() + part.slice(1) : part)
    .join("");
}

function normalizePath(path) {
  return normalizeRoutePath(path);
}

function currentRoute() {
  return operatorShellRouteContractForPath(window.location.pathname);
}

function renderBrand() {
  const brandMark = document.getElementById("brandMark");
  if (!brandMark || brandMark.dataset.staticBrand === "true") {
    return;
  }
  brandMark.innerHTML = renderAegisMark({ size: "sm", label: "Aegis" });
}

function renderNav() {
  const startedAt = performance.now();
  state.operatorMode = "operator";
  const navigationSchema = NAVIGATION_SCHEMA;
  const activeSelection = activeNavSelectionForPath(window.location.pathname);
  const engineeringRoutes = ENGINEERING_NAVIGATION_SCHEMA.flatMap((section) => section.domains || []);
  const activeEngineeringRoute = activeSelection.workspace_id === "engineering"
    ? engineeringRoutes.find((route) => route.id === activeSelection.nav_item_id || route.id === activeSelection.parent_nav_item_id) || null
    : null;
  const active = activeSelection.workspace_id === "engineering" ? null : activeNavigationForPath(window.location.pathname, navigationSchema);
  const query = String(document.getElementById("sidebarSearch")?.value || "").trim().toLowerCase();
  const queryActive = Boolean(query);
  state.sidebarMode = ["expanded", "collapsed"].includes(state.sidebarMode) ? state.sidebarMode : "collapsed";
  document.body.classList.toggle("sidebar-collapsed", state.sidebarMode === "collapsed");
  document.body.classList.toggle("sidebar-expanded", state.sidebarMode === "expanded");
  document.body.classList.toggle("sidebar-drawer-open", state.sidebarMode === "expanded" && window.matchMedia("(max-width: 900px)").matches);
  document.body.classList.toggle("engineering-drawer-open", state.engineeringDrawerOpen === true);
  const collapseButton = document.querySelector("[data-sidebar-collapse]");
  if (collapseButton) {
    const isNarrow = window.matchMedia("(max-width: 900px)").matches;
    const nextLabel = state.sidebarMode === "expanded" ? (isNarrow ? "Close" : "Collapse") : "Expand";
    collapseButton.setAttribute("aria-label", `${nextLabel} navigation`);
    collapseButton.querySelector("span").textContent = nextLabel;
    collapseButton.querySelector("strong").textContent = state.sidebarMode === "expanded" ? "‹" : "›";
  }
  const openButton = document.querySelector("[data-sidebar-open]");
  if (openButton) {
    openButton.hidden = state.sidebarMode === "expanded";
    openButton.setAttribute("aria-expanded", state.sidebarMode === "expanded" ? "true" : "false");
  }
  const scrim = document.querySelector("[data-sidebar-close]");
  if (scrim) {
    scrim.hidden = !(state.sidebarMode === "expanded" && window.matchMedia("(max-width: 900px)").matches);
  }
  const renderDomain = (domain) => {
    const children = Array.isArray(domain.children) ? domain.children : [];
    const matchesDomain = [domain.label, domain.description, domain.truthOwner].some((value) =>
      String(value || "").toLowerCase().includes(query),
    );
    const filteredChildren = children.filter((child) => {
      if (!query) return true;
      return [child.label, child.description, child.truthOwner].some((value) =>
        String(value || "").toLowerCase().includes(query),
      );
    });
    if (queryActive && !matchesDomain && filteredChildren.length === 0) return "";
    const isActiveParent = active?.id === domain.id || active?.parentId === domain.id;
    const visibleChildren = queryActive && filteredChildren.length ? filteredChildren : children;
    const domainBadge = [domain, ...children].reduce((total, item) => total + Number(item.badgeCount || 0), 0);
    const childrenId = `nav-children-${domain.id}`;
    const hasChildren = visibleChildren.length > 0;
    return `
      <div class="sidebar-group accent-${escapeHtml(domain.accent)} ${isActiveParent ? "active-parent" : ""}">
        <button class="sidebar-group-button" type="button" data-nav-group-toggle="${escapeHtml(domain.id)}" data-route="${escapeHtml(domain.route)}" aria-expanded="false" aria-controls="${escapeHtml(childrenId)}">
          <span class="nav-icon">${escapeHtml(domain.icon)}</span>
          <span class="nav-text">
            <strong>${escapeHtml(domain.label)}</strong>
            <small>${escapeHtml(domain.truthOwner || "")}</small>
          </span>
          ${domainBadge ? `<span class="nav-badge">${escapeHtml(String(domainBadge))}</span>` : ""}
          ${hasChildren ? `<span class="nav-caret" aria-hidden="true">›</span>` : ""}
        </button>
        ${hasChildren ? `<div class="sidebar-children" id="${escapeHtml(childrenId)}" hidden>${visibleChildren.map((child) => {
          const childActive = active?.id === child.id || (!active?.parentId && active?.route === child.route && child.id.endsWith("overview"));
          return `
            <a class="nav-link ${childActive ? "active" : ""}" href="${escapeHtml(child.route)}" data-route="${escapeHtml(child.route)}">
              <span class="nav-icon">${escapeHtml(child.icon)}</span>
              <span class="nav-text"><span>${escapeHtml(child.label)}</span><small>${escapeHtml(child.truthOwner || domain.truthOwner || "")}</small></span>
              ${child.badgeCount ? `<span class="nav-badge">${escapeHtml(String(child.badgeCount))}</span>` : ""}
            </a>`;
        }).join("")}</div>` : ""}
      </div>
    `;
  };
  const operatorSections = navigationSchema.map((section) => `
    <section class="sidebar-section" data-sidebar-operator-section="true">
      ${section.domains.map(renderDomain).join("")}
    </section>
  `).join("");
  const engineeringRouteById = Object.fromEntries(engineeringRoutes.map((route) => [route.id, route]));
  const engineeringGroups = [
    { label: "System", routes: ["aegis_dashboard", "aegis_verified_runtime", "runtime_timeline"].map((id) => engineeringRouteById[id]).filter(Boolean) },
    { label: "Diagnostics", routes: [
      engineeringRouteById.aegis_input_checks,
      { id: "aegis_market_data_coverage", label: "Market Data Coverage", operatorLabel: "Market Data Coverage", icon: "M", route: "/aegis-opportunities#market-data-coverage", truthOwner: "aegis_market_data_coverage_v1", description: "Certified market-data coverage, stale symbols, and downstream consumers." },
      engineeringRouteById.aegis_hash_lineage,
      engineeringRouteById.aegis_provider_health,
    ].filter(Boolean) },
    { label: "Audit", routes: [
      engineeringRouteById.aegis_candidate_funnel,
      engineeringRouteById.aegis_candidate_lineage,
      engineeringRouteById.aegis_evidence_ledger,
      { id: "aegis_legacy_captures", label: "Legacy / Partial Historical Trades", operatorLabel: "Legacy / Partial Historical Trades", icon: "L", route: "/aegis-journal#legacy-captures", truthOwner: "aegis_canonical_operator_state_v1", description: "Legacy historical trade records separated from open paper positions." },
    ].filter(Boolean) },
    { label: "Repairs", routes: [
      engineeringRouteById.repair_center,
      { id: "aegis_repair_commands", label: "Repair Commands", operatorLabel: "Repair Commands", icon: "R", route: "/aegis-repair-center#repair-commands", truthOwner: "aegis_repair_center_projection_v1", description: "Repair command inventory and guarded command actions." },
    ].filter(Boolean) },
  ].filter((group) => group.routes.length > 0);
  const runtimeStatusRows = state.shell.statusRail.filter((kernel) => String(kernel.kernel_id || "").toLowerCase() !== "control");
  const runtimeStatusPanel = state.engineeringDrawerOpen ? `
    <details class="engineering-runtime-status-panel">
      <summary>Runtime Status</summary>
      <div class="engineering-runtime-status-list">
        ${runtimeStatusRows.map((kernel) => {
          const routePath = normalizePath(kernel.href || "/");
          const label = kernel.label || kernel.kernel_id || "Kernel";
          const status = kernel.status?.label || kernel.status?.code || "Unknown";
          const tooltip = kernel.status?.reason_codes?.join(", ") || "";
          return `<a class="status-rail-item" href="${escapeHtml(routePath)}" data-route="${escapeHtml(routePath)}" title="${escapeHtml(tooltip)}"><span>${escapeHtml(label)}</span>${renderStatusPill(status, kernel.status?.semantic || "unknown", state.semantics)}</a>`;
        }).join("") || `<div class="muted-mini">Runtime status has not loaded yet.</div>`}
      </div>
      <div class="kernel-safety-strip">READ-ONLY GOVERNANCE · NO BROKER EXECUTION · MANUAL CAPTURE ONLY</div>
    </details>
  ` : "";
  const bootDiagnosticsPanel = state.engineeringDrawerOpen ? `
    <details class="engineering-runtime-status-panel boot-diagnostics-panel">
      <summary>Boot Diagnostics</summary>
      <div class="runtime-diagnostics-grid">
        <div><span>boot_time_ms</span><strong>${escapeHtml(String(state.bootDiagnostics.boot_time_ms || 0))}</strong></div>
        <div><span>first_paint_ms</span><strong>${escapeHtml(String(state.bootDiagnostics.first_paint_ms || 0))}</strong></div>
        <div><span>data_ready_ms</span><strong>${escapeHtml(String(state.bootDiagnostics.data_ready_ms || 0))}</strong></div>
        <div><span>duplicate_fetch_count</span><strong>${escapeHtml(String(state.bootDiagnostics.duplicate_fetch_count || 0))}</strong></div>
        <div><span>route_rerender_count</span><strong>${escapeHtml(String(state.bootDiagnostics.route_rerender_count || 0))}</strong></div>
      </div>
    </details>
  ` : "";
  const engineeringNavStatus = state.activeView?.engineeringNavStatus || state.activeView?.topReadinessLabel || "Internal routes";
  const engineeringDrawer = `
    <section class="sidebar-section sidebar-engineering-control">
      <button class="sidebar-group-button engineering-toggle-button ${state.engineeringDrawerOpen || activeEngineeringRoute ? "active open" : ""}" type="button" data-engineering-drawer-toggle="true" data-route="/aegis-opportunities" aria-expanded="${state.engineeringDrawerOpen ? "true" : "false"}" aria-controls="engineeringDrawer">
        <span class="nav-icon">⚙</span>
        <span class="nav-text"><strong>System Health</strong><small>${escapeHtml(engineeringNavStatus)}</small></span>
      </button>
    </section>
    <aside id="engineeringDrawer" class="engineering-drawer" data-engineering-drawer ${state.engineeringDrawerOpen ? "" : "hidden"}>
      <header class="engineering-drawer-header">
        <div><div class="drawer-eyebrow">System Health</div><h3>Internal tools</h3></div>
        <button class="ghost-button" type="button" data-engineering-drawer-close>Close</button>
      </header>
      ${runtimeStatusPanel}
      ${bootDiagnosticsPanel}
      <div class="engineering-route-list" data-engineering-route-list>
        ${state.engineeringDrawerOpen ? (engineeringGroups.length ? engineeringGroups.map((group) => `
          <section class="engineering-route-group" data-engineering-route-group="${escapeHtml(group.label)}">
            <h4>${escapeHtml(group.label)}</h4>
            <div class="engineering-route-group-list">
              ${group.routes.map((route) => {
                const routePath = normalizePath(String(route.route || "").split("#")[0] || "");
                const isRouteActive = activeEngineeringRoute?.id === route.id || normalizePath(window.location.pathname) === routePath;
                const label = route.operatorLabel || route.label;
                return `<a class="engineering-route-link ${isRouteActive ? "active" : ""}" href="${escapeHtml(route.route)}" data-route="${escapeHtml(route.route)}" data-engineering-route-id="${escapeHtml(route.id || label)}">
                  <span class="nav-icon">${escapeHtml(route.icon || "•")}</span>
                  <span><strong>${escapeHtml(label)}</strong><small>${escapeHtml(route.description || route.truthOwner || "")}</small></span>
                </a>`;
              }).join("")}
            </div>
          </section>
        `).join("") : `<div class="muted-mini" data-engineering-empty>No engineering tools available</div>`) : ""}
      </div>
    </aside>
  `;
  document.getElementById("workspaceNav").innerHTML = `${operatorSections}${engineeringDrawer}`;
  logTiming("workspace/navigation load", startedAt, { mode: state.sidebarMode, operatorMode: state.operatorMode, engineeringDrawerOpen: state.engineeringDrawerOpen });
}

function renderKernelRail() {
  const chip = document.getElementById("sidebarStatusChip");
  if (!chip) return;
  const summary = state.shell.systemSummary || {};
  const readiness = summary.readiness_summary || {};
  const runtimeStatus = state.shell.runtimeStatus || {};
  const activeMode = runtimeStatus.active_mode_readiness_status || readiness.active_mode_readiness_status || readiness.status || "";
  const truth = state.activeView?.operatorTruth || {};
  const label = truth.primaryStatus || state.activeView?.sidebarStatusLabel || (String(activeMode).toUpperCase() === "BLOCKED" || readiness.blocked ? "Paper Research Mode" : "Review only");
  chip.textContent = label;
  chip.hidden = false;
}

function renderTopBar() {
  const summary = state.shell.systemSummary || {};
  const runtimeStatus = state.shell.runtimeStatus || {};
  const financialState = state.shell.financialState || {};
  const readiness = summary.readiness_summary || {};
  const topLevelItems = Array.isArray(summary.top_level_items) ? summary.top_level_items : [];
  const trustPreserved = Array.isArray(summary.trust_preserved_items) ? summary.trust_preserved_items : [];
  const topAlert = trustPreserved[0] || topLevelItems[0] || null;
  const truth = state.activeView?.operatorTruth || {};
  const readinessStatus = truth.primaryStatus || state.activeView?.topReadinessLabel || (readiness.blocked ? "Paper Research Mode" : "Review only");
  const readinessTone = truth.primaryTone || state.activeView?.topReadinessTone || (readiness.blocked ? "warning" : "neutral");
  document.getElementById("headerPageTitle").textContent = state.activeView?.headerTitle || "Command / Overview";
  document.getElementById("headerEnvironment").textContent = state.activeView?.environmentLabel || "Paper Research Mode";
  document.getElementById("headerDataTimestamp").textContent = state.activeView?.dataTimestamp || "Operational timestamps loading";
  const topRuntimeMode = document.getElementById("topRuntimeMode");
  if (topRuntimeMode) topRuntimeMode.textContent = state.activeView?.runtimeModeLabel || formatDisplayLabel(summary.environment || summary.runtime_mode || runtimeStatus.runtime_mode || "UNKNOWN");
  const topConfigVersion = document.getElementById("topConfigVersion");
  if (topConfigVersion) topConfigVersion.textContent = formatDisplayLabel(summary.kernel_version || summary.summary_id || "governed");
  const topReadiness = document.getElementById("topReadiness");
  if (topReadiness) topReadiness.innerHTML = renderStatusPill(
    readinessStatus,
    readinessTone,
    state.semantics,
  );
  const topPortfolioValue = document.getElementById("topPortfolioValue");
  if (topPortfolioValue) {
    topPortfolioValue.textContent = financialState.financial_status === "FAIL_CLOSED"
      ? "fail closed"
      : formatUsd(financialState.investable_summary?.investable_assets_total_usd);
  }
  const notificationCount = document.getElementById("topNotificationCount");
  if (notificationCount) notificationCount.textContent = String(trustPreserved.length);
  const notificationBadge = document.getElementById("notificationBadge");
  if (notificationBadge) notificationBadge.textContent = String(Math.max(trustPreserved.length, 3));
  document.getElementById("bottomConfigVersion").textContent = formatDisplayLabel(summary.kernel_version || "governed");
  document.getElementById("bottomFreshness").textContent = summary.last_refresh_utc || state.shell.refreshedAt || "n/a";
  document.getElementById("bottomReadiness").textContent = readinessStatus;
  document.getElementById("bottomEnvironment").textContent = formatDisplayLabel(summary.environment || summary.runtime_mode || runtimeStatus.runtime_mode || "UNKNOWN");
  document.getElementById("bottomSession").textContent = summary.current_day || "UNKNOWN";
  const connectionState = state.connection?.state || "RECONNECTING";
  document.getElementById("bottomAlert").textContent =
    connectionState === "CONNECTED"
      ? (topAlert?.target_label || topAlert?.label || topAlert?.summary_message || "No elevated summary item")
      : `${connectionState}: ${state.connection?.recovery_command || "npm run aegis:ui:restart"}`;
}

function showCommandDetailPanel(panel) {
  if (!panel) return false;
  document.querySelectorAll("dialog[open][data-command-detail-panel]").forEach((dialog) => {
    if (dialog !== panel) {
      dialog.close?.();
      dialog.removeAttribute("open");
    }
  });
  if (panel.showModal && !panel.open) {
    try {
      panel.showModal();
    } catch (_error) {
      panel.setAttribute("open", "");
      panel.hidden = false;
    }
  } else if (!panel.open) {
    panel.setAttribute("open", "");
    panel.hidden = false;
  }
  return true;
}


function openAegisExitReviewDetailFallback(panelId, symbol = "Position", decision = "UNKNOWN", reason = "No reason reported") {
  if (!panelId) return false;
  let panel = document.getElementById(panelId);
  if (!panel) {
    panel = document.createElement("dialog");
    panel.id = panelId;
    panel.className = "command-detail-dialog";
    panel.setAttribute("data-command-detail-panel", "");
    panel.innerHTML = `<div class="command-detail-dialog-inner">
      <header class="command-detail-header"><div><p class="eyebrow">EXIT_REVIEW_DETAIL</p><h3>${escapeHtml(symbol || "Position")}</h3><p>${escapeHtml(decision || "UNKNOWN")} - ${escapeHtml(reason || "No reason reported")}</p></div><form method="dialog"><button class="ghost-button" type="submit">Close</button></form></header>
      <dl class="definition-list"><div><dt>Exit decision</dt><dd>${escapeHtml(decision || "UNKNOWN")}</dd></div><div><dt>Reason</dt><dd>${escapeHtml(reason || "No reason reported")}</dd></div><div><dt>Stop</dt><dd>See artifact</dd></div><div><dt>Target</dt><dd>See artifact</dd></div><div><dt>P&L</dt><dd>See artifact</dd></div><div><dt>Evidence</dt><dd>Artifact-backed exit_review_projection_v1 detail.</dd></div><div><dt>Next action</dt><dd>Manual review only.</dd></div><div><dt>Safety</dt><dd>Manual review only. No broker submission, order routing, or autonomous execution.</dd></div></dl>
    </div>`;
    document.body.appendChild(panel);
  }
  showCommandDetailPanel(panel);
  return Boolean(panel.open);
}

window.openAegisExitReviewDetailFallback = openAegisExitReviewDetailFallback;

function rememberOpenCommandDetail(commandElement, panelId) {
  state.openCommandDetail = {
    panelId,
    routePath: window.location.pathname,
    commandId: String(commandElement.getAttribute("data-aegis-command-id") || "").trim(),
    label: String(commandElement.textContent || commandElement.getAttribute("data-aegis-command-id") || "View details").trim(),
  };
}

function clearRememberedCommandDetail(panel) {
  if (state.routeRenderInProgress > 0) {
    return;
  }
  if (!panel || !document.body.contains(panel)) {
    return;
  }
  if (state.openCommandDetail?.panelId === panel?.id) {
    state.openCommandDetail = null;
  }
}

function resetPaperTradeDialog(dialog) {
  if (!dialog || !dialog.matches?.("[data-paper-entry-dialog]")) return;
  dialog.dataset.paperSubmitting = "false";
  dialog.querySelectorAll(".paper-candidate-action-form").forEach((form) => {
    form.dataset.paperSubmitting = "false";
    form.reset?.();
    const statusNode = form.querySelector("[data-paper-candidate-status]");
    if (statusNode) {
      statusNode.hidden = true;
      statusNode.textContent = "";
      statusNode.dataset.tone = "";
    }
    form.querySelectorAll("button[type='submit']").forEach((button) => {
      button.disabled = false;
      button.removeAttribute("aria-busy");
      if (button.dataset.originalLabel) {
        button.textContent = button.dataset.originalLabel;
      }
    });
  });
}

function closePaperTradeDialog(dialog) {
  if (!dialog || !dialog.matches?.("[data-paper-entry-dialog]")) return false;
  resetPaperTradeDialog(dialog);
  if (dialog.close) dialog.close("cancel");
  else dialog.removeAttribute("open");
  return true;
}


function wireCommandDetailClose(panel) {
  if (!panel || panel.dataset.commandDetailCloseWired === "true") return;
  panel.dataset.commandDetailCloseWired = "true";
  panel.addEventListener("close", () => {
    resetPaperTradeDialog(panel);
    clearRememberedCommandDetail(panel);
  });
  panel.addEventListener("cancel", () => {
    resetPaperTradeDialog(panel);
    clearRememberedCommandDetail(panel);
  });
}

function restoreOpenCommandDetailAfterRender() {
  const remembered = state.openCommandDetail;
  if (!remembered || remembered.routePath !== window.location.pathname) return;
  const panel = document.getElementById(remembered.panelId);
  if (!panel) return;
  showCommandDetailPanel(panel);
  panel.dataset.openedBy = remembered.label || remembered.commandId || "View details";
  panel.dataset.commandDetailOpenCommand = remembered.commandId || "";
  wireCommandDetailClose(panel);
  window.setTimeout(() => panel.focus?.({ preventScroll: true }), 0);
}

function setPageChrome(route, view) {
  document.title = `${view.title || route.label} | Aegis`;
  document.getElementById("workspaceTitle").textContent = view.title || route.label;
  document.getElementById("workspaceEyebrow").textContent = route.eyebrow || route.label;
  document.getElementById("workspaceMeta").textContent = view.meta || route.subtitle || "";
  document.getElementById("headerPageTitle").textContent = view.headerTitle || "Command / Overview";
  const truth = view.operatorTruth || {};
  if (truth.primaryStatus || view.topReadinessLabel) {
    const topReadiness = document.getElementById("topReadiness");
    if (topReadiness) topReadiness.innerHTML = renderStatusPill(truth.primaryStatus || view.topReadinessLabel, truth.primaryTone || view.topReadinessTone || "neutral", state.semantics);
  }
  const timestampHost = document.getElementById("headerDataTimestamp");
  if (view.hideHeaderTimestamp) {
    timestampHost.textContent = "";
    timestampHost.setAttribute("hidden", "");
  } else {
    timestampHost.removeAttribute("hidden");
    if (view.operationalTimestamps) {
      timestampHost.innerHTML = renderHeaderOperationalTimestamps(view.operationalTimestamps);
    } else if (view.dataTimestamp) {
      timestampHost.textContent = String(view.dataTimestamp).replace(new RegExp("^Data\\s+as\\s+of:\s*", "i"), "Updated: ");
    } else {
      timestampHost.textContent = "Operational timestamps unavailable";
    }
  }
}

function renderPalette() {
  const host = document.getElementById("commandPaletteResults");
  const input = document.getElementById("commandPaletteInput");
  const query = String(input.value || "").trim().toLowerCase();
  const entries = buildPaletteEntries(state).filter((entry) => {
    if (!query) {
      return true;
    }
    return [entry.label, entry.subtitle, entry.href].some((value) => String(value || "").toLowerCase().includes(query));
  });
  host.innerHTML = entries.map((entry) => `
    <button class="palette-result" type="button" data-route="${escapeHtml(entry.href)}">
      <span>${escapeHtml(entry.label)}</span>
      <small>${escapeHtml(entry.subtitle || entry.kind)}</small>
    </button>
  `).join("") || `<div class="empty-state">No matching routes or next steps.</div>`;
}

function togglePalette(forceOpen) {
  state.paletteOpen = typeof forceOpen === "boolean" ? forceOpen : !state.paletteOpen;
  document.getElementById("commandPalette").hidden = !state.paletteOpen;
  if (state.paletteOpen) {
    renderPalette();
    document.getElementById("commandPaletteInput").focus();
  }
}

async function loadSharedShellState({ summaryOnly = false } = {}) {
  const routePath = normalizePath(window.location.pathname || "/");
  const isAegisWorkflow = routePath.startsWith("/aegis-") || routePath === "/operator-inbox" || routePath === "/research-lab" || routePath === "/performance" || routePath === "/outcomes";
  const railParams = summaryOnly ? { summary: 1 } : {};
  if (isAegisWorkflow) {
    railParams.surface = "aegis";
  }
  const [semanticsPayload, railPayload, systemSummary, operatorWorkflow, alerts, financialState, runtimeStatus] = await timedAsync("readiness cards load", () => Promise.all([
    fetchStatusSemantics(),
    fetchStatusRail(railParams),
    fetchSystemSummary(),
    fetchOperatorWorkflow(),
    fetchAlerts(),
    fetchFinancialState(),
    fetchRuntimeStatus().catch(() => ({})),
  ]), { summary_only: summaryOnly });
  state.semantics = semanticsPayload.status_semantics || {};
  state.shell.statusRail = railPayload.kernels || [];
  state.shell.systemSummary = systemSummary || {};
  state.shell.operatorWorkflow = operatorWorkflow || {};
  state.shell.alerts = alerts || {};
  state.shell.financialState = financialState || {};
  state.shell.runtimeStatus = runtimeStatus || {};
  state.shell.refreshedAt = railPayload.generated_utc || systemSummary?.last_refresh_utc || null;
  renderKernelRail();
  renderTopBar();
}


function researchConsoleProgressText(actionLabel) {
  const label = String(actionLabel || "").toLowerCase();
  if (label.includes("accept")) return "Accepting hypothesis...";
  if (label.includes("readiness")) return "Assessing readiness...";
  if (label.includes("watchlist")) return "Moving to Watchlist...";
  if (label.includes("reject")) return "Rejecting hypothesis...";
  if (label.includes("archive")) return "Archiving hypothesis...";
  if (label.includes("convert")) return "Creating research plan...";
  if (label.includes("start")) return "Starting research...";
  return "Recording research action...";
}

function researchConsoleErrorText(error) {
  const payload = error?.payload || {};
  const message = payload.operator_message || payload.message || error?.message || "Research Pipeline action failed.";
  const reason = payload.failure_reason || payload.error || "";
  return reason ? `${message} ${reason}` : message;
}

async function renderRoute({ backgroundRefresh = false, source = "route" } = {}) {
  const startedAt = performance.now();
  const renderGeneration = ++state.routeRenderGeneration;
  state.routeRenderInProgress += 1;
  const route = currentRoute();
  if (source === "refresh") {
    markBootEvent("REFRESH_START", { route_id: route.id });
  } else {
    markBootEvent("RENDER_START", { route_id: route.id });
  }
  markBootEvent("ROUTE_RESOLVED", { route_id: route.id, path: route.path });
  renderNav();
  document.title = `${route.label} | Aegis`;
  const provisionalTitle = document.getElementById("workspaceTitle");
  if (provisionalTitle) provisionalTitle.textContent = route.label;
  const mainHost = document.getElementById("workspaceContent");
  const contextHost = document.getElementById("contextRailContent");
  const sameRenderedRoute = state.routeRenderedOnce && state.lastRenderedRouteId === route.id;
  const cachedView = state.routeCache.get(route.id);
  const shouldPreserveContent = backgroundRefresh || sameRenderedRoute || Boolean(cachedView);
  if (!shouldPreserveContent) {
    markBootEvent("CONTENT_CLEARED", { route_id: route.id, reason: "initial_route_load" });
    updateBootDiagnostic("content_clear_count", Number(state.bootDiagnostics.content_clear_count || 0) + 1);
    mainHost.innerHTML = `<div class="page-loading">Loading ${escapeHtml(route.label)}...</div>`;
    contextHost.innerHTML = "";
  } else {
    setWorkspaceUpdating({ active: true });
  }
  try {
    markBootEvent("STATE_FETCH_START", { route_id: route.id });
    const view = await timedAsync("workspace route load", () => loadRouteView(route.id, state), { route_id: route.id });
    markBootEvent("STATE_FETCH_END", { route_id: route.id });
    if (renderGeneration !== state.routeRenderGeneration) {
      markBootEvent("ROUTE_REPLACED", { route_id: route.id, reason: "stale_render_generation" });
      return;
    }
    if (state.routeRenderedOnce && state.lastRenderedRouteId === route.id) {
      updateBootDiagnostic("route_rerender_count", Number(state.bootDiagnostics.route_rerender_count || 0) + 1);
    }
    state.activeView = view;
    state.routeCache.set(route.id, view);
    renderNav();
    renderKernelRail();
    renderTopBar();
    setPageChrome(route, view);
    const layoutMode = resolveWorkspaceLayoutMode(route, view);
    const rightRailHidden = view.dashboardIncidentMode === true || view.hideContextRail === true || layoutMode === WORKFLOW_LAYOUT;
    document.body.classList.toggle("dashboard-incident-mode", view.dashboardIncidentMode === true);
    document.body.classList.toggle("dashboard-main-only", view.hideContextRail === true && layoutMode !== WORKFLOW_LAYOUT);
    document.body.classList.toggle("workflow-layout", layoutMode === WORKFLOW_LAYOUT);
    document.body.classList.toggle("operational-layout", layoutMode === OPERATIONAL_LAYOUT);
    document.body.dataset.workspaceLayoutMode = layoutMode;
    setWorkspaceUpdating({ active: false });
    mainHost.innerHTML = view.html;
    contextHost.hidden = rightRailHidden;
    contextHost.innerHTML = rightRailHidden ? "" : (view.contextHtml || `<div class="empty-state">No contextual evidence for this surface.</div>`);
    state.routeRenderedOnce = true;
    state.lastRenderedRouteId = route.id;
    if (!state.bootDiagnostics.first_paint_ms) {
      updateBootDiagnostic("first_paint_ms", Math.round((performance.now() - state.bootDiagnostics.bootStart) * 10) / 10);
    }
    restoreOpenCommandDetailAfterRender();
    revealCurrentHashTarget();
    markBootEvent("RENDER_END", { route_id: route.id });
  } catch (error) {
    if (source === "refresh") {
      markBootEvent("REFRESH_END", { route_id: route.id, error: true });
    } else {
      markBootEvent("RENDER_END", { route_id: route.id, error: true });
    }
    if (shouldPreserveContent && state.routeRenderedOnce) {
      setWorkspaceUpdating({ warning: error?.message || "Refresh failed; showing last good content." });
      return;
    }
    document.body.classList.remove("dashboard-incident-mode");
    document.body.classList.remove("dashboard-main-only");
    document.body.classList.remove("workflow-layout");
    document.body.classList.remove("operational-layout");
    delete document.body.dataset.workspaceLayoutMode;
    contextHost.hidden = false;
    const routePath = String(route.path || "");
    const isCapitalRoute = routePath === "/capital" || routePath.startsWith("/capital/");
    if (isCapitalRoute && error?.operatorSafe) {
      mainHost.innerHTML = renderOperatorDiagnosticPanel(error.operatorSafe);
    } else {
      mainHost.innerHTML = renderError(error.message || "Route render failed.");
    }
    contextHost.innerHTML = "";
  } finally {
    if (source === "refresh") {
      markBootEvent("REFRESH_END", { route_id: route.id });
    }
    logTiming("workspace render complete", startedAt, { route_id: route.id });
    updateBootDiagnostic("duplicate_fetch_count", typeof window !== "undefined" ? Number(window.__AEGIS_DUPLICATE_FETCH_COUNT || 0) : 0);
    state.routeRenderInProgress = Math.max(0, state.routeRenderInProgress - 1);
  }
}

async function navigateTo(path) {
  const rawTarget = String(path || "/").trim() || "/";
  const [pathAndQuery, rawHashPart] = rawTarget.split("#");
  const hashPart = String(rawHashPart || "").trim();
  const [rawPath, rawQuery] = pathAndQuery.split("?");
  const normalizedPath = normalizePath(rawPath);
  const shouldPreserveDynamicWorkOrderPath = rawPath.startsWith("/reliability/work-orders/") && rawPath !== "/reliability/work-orders";
  const finalPath = shouldPreserveDynamicWorkOrderPath ? rawPath : normalizedPath;
  const targetQuery = new URLSearchParams(rawQuery || "");
  const currentQuery = new URLSearchParams(window.location.search || "");
  ["day", "operational_day"].forEach((key) => {
    if (!targetQuery.has(key) && currentQuery.has(key) && routeShouldPreserveDayQuery(finalPath)) {
      targetQuery.set(key, currentQuery.get(key));
    }
  });
  const queryText = targetQuery.toString();
  const finalTarget = `${queryText ? `${finalPath}?${queryText}` : finalPath}${hashPart ? `#${hashPart}` : ""}`;
  if (activeNavSelectionForPath(finalPath).workspace_id !== "engineering") {
    state.engineeringDrawerOpen = false;
  }
  if (finalTarget !== `${window.location.pathname}${window.location.search}${window.location.hash}`) {
    window.history.pushState({}, "", finalTarget);
  }
  togglePalette(false);
  await renderRoute();
  if (hashPart) {
    revealCurrentHashTarget(hashPart);
  }
}

function revealChangeControlRecordTarget(targetId = "") {
  const normalizedTargetId = String(targetId || "").replace(/^#/, "").trim();
  if (!normalizedTargetId || !normalizedTargetId.startsWith("change-control-item-")) return false;
  const target = document.getElementById(normalizedTargetId);
  if (!target) return false;
  if (target.tagName === "DETAILS") {
    target.open = true;
  }
  target.closest("details")?.setAttribute("open", "");
  target.scrollIntoView({ behavior: "smooth", block: "start" });
  target.focus?.({ preventScroll: true });
  target.classList.remove("change-control-record-highlight");
  void target.offsetWidth;
  target.classList.add("change-control-record-highlight");
  window.setTimeout(() => target.classList.remove("change-control-record-highlight"), 2200);
  return true;
}

function revealCurrentHashTarget(forcedHash = "") {
  const hash = String(forcedHash || window.location.hash || "").replace(/^#/, "").trim();
  if (!hash) return false;
  if (revealChangeControlRecordTarget(hash)) return true;
  const anchor = document.getElementById(hash);
  if (anchor) {
    anchor.scrollIntoView({ behavior: "smooth", block: "start" });
    return true;
  }
  return false;
}

function revealHypothesisSummaryTarget({ sectionTargetId = "", cardTargetId = "" } = {}) {
  const section = sectionTargetId ? document.getElementById(sectionTargetId) : null;
  if (section?.tagName === "DETAILS") {
    section.open = true;
  }
  const target = cardTargetId ? document.getElementById(cardTargetId) : section;
  if (!target) {
    return false;
  }
  document.body.dataset.hypothesisLastFocusToken = String(Date.now());
  target.scrollIntoView({ behavior: "smooth", block: "start" });
  if (target.hasAttribute("data-hypothesis-card")) {
    target.focus?.({ preventScroll: true });
    target.classList.remove("hypothesis-card-highlight");
    void target.offsetWidth;
    target.classList.add("hypothesis-card-highlight");
    window.setTimeout(() => target.classList.remove("hypothesis-card-highlight"), 2400);
  } else {
    target.classList.remove("hypothesis-section-highlight");
    void target.offsetWidth;
    target.classList.add("hypothesis-section-highlight");
    window.setTimeout(() => target.classList.remove("hypothesis-section-highlight"), 1800);
  }
  return true;
}

async function openArtifact(path, title) {
  const startedAt = performance.now();
  document.getElementById("drawerTitle").textContent = title || "Loading evidence";
  document.getElementById("drawerMeta").textContent = path || "";
  document.getElementById("drawerContent").textContent = "Loading evidence artifact...";
  const response = await fetch(`/api/artifact?path=${encodeURIComponent(path)}`, { cache: "no-store" });
  const payload = await response.json();
  document.getElementById("drawerTitle").textContent = title || payload.path || "Artifact";
  document.getElementById("drawerMeta").textContent = payload.path || "";
  document.getElementById("drawerContent").textContent = payload.content || "";
  logTiming("evidence drawer load", startedAt, { ok: Boolean(payload.ok), path: payload.path || path || "" });
}

function resetDrawer() {
  document.getElementById("drawerTitle").textContent = "Select evidence";
  document.getElementById("drawerMeta").textContent = "Artifact content and provenance appear here.";
  document.getElementById("drawerContent").textContent = "";
}

function parseEvidencePayload(node) {
  const encoded = String(node?.getAttribute("data-evidence-payload") || "").trim();
  if (!encoded) {
    return {
      title: String(node?.getAttribute("data-evidence-title") || "Evidence"),
      explanation: String(node?.getAttribute("data-evidence-summary") || "Evidence is available for this item."),
      artifact_path: String(node?.getAttribute("data-artifact-path") || ""),
      artifact_hash: String(node?.getAttribute("data-artifact-hash") || ""),
    };
  }
  try {
    const payload = JSON.parse(decodeURIComponent(encoded));
    return payload && typeof payload === "object" ? payload : {};
  } catch {
    return { title: "Evidence", explanation: "Evidence payload could not be decoded." };
  }
}

function evidenceValue(value, fallback = "not reported") {
  if (Array.isArray(value)) return value.length ? value.join(", ") : fallback;
  const text = String(value ?? "").trim();
  return text || fallback;
}

function evidenceRowsHtml(rows) {
  return `<div class="evidence-drawer-definition-list">${rows.map((row) => `
    <div class="evidence-drawer-definition-row">
      <span>${escapeHtml(row.label || "Field")}</span>
      <strong>${escapeHtml(evidenceValue(row.value))}</strong>
    </div>
  `).join("")}</div>`;
}

function linkedLifecycleEventsHtml(events = []) {
  const rows = Array.isArray(events) ? events.slice(0, 12) : [];
  if (!rows.length) return `<div class="empty-state">No linked lifecycle events were provided for this evidence item.</div>`;
  return `<div class="table-wrap"><table class="data-table"><thead><tr><th>Event</th><th>Time</th><th>Source</th></tr></thead><tbody>${rows.map((row) => `
    <tr>
      <td>${escapeHtml(row.event_type || row.type || "event")}</td>
      <td>${escapeHtml(row.event_time || row.timestamp || row.generated_at_utc || "not reported")}</td>
      <td>${escapeHtml(row.source_artifact || row.artifact_path || row.path || "content-addressed projection")}</td>
    </tr>
  `).join("")}</tbody></table></div>`;
}

function openEvidenceDrawer(payload = {}) {
  const dialog = document.getElementById("evidenceDrawer");
  const titleNode = document.getElementById("evidenceDrawerTitle");
  const summaryNode = document.getElementById("evidenceDrawerSummary");
  const bodyNode = document.getElementById("evidenceDrawerBody");
  if (!dialog || !titleNode || !summaryNode || !bodyNode) return;
  const title = evidenceValue(payload.title || payload.statement || payload.metric || "Evidence", "Evidence");
  const explanation = evidenceValue(payload.explanation || payload.human_readable_explanation || payload.statement, "Technical provenance and audit details for this item.");
  const artifactPath = evidenceValue(payload.artifact_path || payload.supporting_artifact_path, "");
  const artifactHash = evidenceValue(payload.artifact_hash || payload.supporting_artifact_hash || payload.content_hash, "");
  const replayHash = evidenceValue(payload.replay_hash || payload.evaluation_hash || payload.artifact_content_hash, "");
  titleNode.textContent = title;
  summaryNode.textContent = explanation;
  bodyNode.innerHTML = `
    ${evidenceRowsHtml([
      { label: "Supporting metric", value: payload.supporting_metric || payload.metric || payload.raw_metric_key },
      { label: "Artifact family", value: payload.artifact_family || payload.sourceKey || payload.source_key },
      { label: "Artifact path", value: artifactPath },
      { label: "Artifact hash", value: artifactHash },
      { label: "Replay / evaluation hash", value: replayHash },
      { label: "Source timestamp", value: payload.source_timestamp || payload.generated_at_utc || payload.timestamp },
      { label: "Raw metric key", value: payload.raw_metric_key || payload.supporting_metric },
      { label: "Evidence status", value: payload.evidence_status },
      { label: "Confidence", value: payload.confidence },
    ])}
    <div class="evidence-drawer-actions">
      <button class="ghost-button" type="button" data-copy-text="${escapeHtml(artifactPath)}" ${artifactPath ? "" : "disabled"}>Copy path</button>
      <button class="ghost-button" type="button" data-copy-text="${escapeHtml(artifactHash || replayHash)}" ${artifactHash || replayHash ? "" : "disabled"}>Copy hash</button>
    </div>
    <section class="evidence-drawer-events">
      <h4>Linked lifecycle events</h4>
      ${linkedLifecycleEventsHtml(payload.linked_lifecycle_events || payload.lifecycle_events)}
    </section>
  `;
  if (dialog.showModal && !dialog.open) {
    try {
      dialog.showModal();
    } catch {
      dialog.hidden = false;
      dialog.setAttribute("open", "");
    }
  } else if (!dialog.open) {
    dialog.hidden = false;
    dialog.setAttribute("open", "");
  }
}

function parseExceptionPayload(node) {
  try {
    return JSON.parse(decodeURIComponent(String(node?.getAttribute("data-exception-payload") || "")));
  } catch {
    return { action: {}, title: "Exception" };
  }
}

function setExceptionActionState(node, message, tone = "info") {
  const card = node?.closest("[data-exception-card]");
  const status = card?.querySelector("[data-exception-action-state]");
  if (!status) {
    return;
  }
  status.textContent = message || "";
  status.dataset.tone = tone;
}

async function runExceptionAction(actionNode) {
  const payload = parseExceptionPayload(actionNode);
  const action = payload.action || {};
  const label = action.label || "Action";
  if (action.state === "empty") {
    setExceptionActionState(actionNode, action.empty_message || "No action target is available.", "empty");
    return;
  }
  setExceptionActionState(actionNode, `${label} loading...`, "loading");
  try {
    if (action.kind === "evidence") {
      const path = String(action.path || "").trim();
      if (!path) {
        setExceptionActionState(actionNode, action.empty_message || "No evidence artifact path is available.", "empty");
        return;
      }
      await openArtifact(path, payload.title || "Exception evidence");
      setExceptionActionState(actionNode, "Evidence loaded.", "ready");
      return;
    }
    if (action.kind === "route") {
      await navigateTo(action.route || "/advisory");
      return;
    }
    if (action.kind === "local") {
      setExceptionActionState(actionNode, action.message || "Acknowledged.", "ready");
      return;
    }
    setExceptionActionState(actionNode, "No handler is registered for this action.", "empty");
  } catch (error) {
    setExceptionActionState(actionNode, error?.message || `${label} failed.`, "error");
  }
}

function manualCaptureHasUnsavedDraft(formOrDialog) {
  const form = formOrDialog?.matches?.(".manual-capture-record-form") ? formOrDialog : formOrDialog?.querySelector?.(".manual-capture-record-form");
  const key = String(form?.dataset?.manualCaptureDraftKey || "").trim();
  if (!key) {
    return false;
  }
  try {
    return Boolean(localStorage.getItem(key));
  } catch (_error) {
    return false;
  }
}

function openManualCaptureDialog(modal) {
  if (!modal) {
    return;
  }
  if (modal.showModal) {
    modal.showModal();
  } else {
    modal.hidden = false;
  }
  document.body.classList.add("manual-capture-modal-open");
  window.setTimeout(() => {
    const firstInput = modal.querySelector("input[name='fill_time'], input[name='fill_price'], input[name='quantity']");
    refreshManualCaptureFormState(modal.querySelector(".manual-capture-record-form"));
    firstInput?.focus?.();
  }, 0);
}

function closeManualCaptureDialog(modal, { force = false } = {}) {
  if (!modal) {
    return false;
  }
  if (!force && manualCaptureHasUnsavedDraft(modal) && !window.confirm("Discard unsaved manual capture draft?")) {
    return false;
  }
  if (modal.close) {
    modal.close();
  } else {
    modal.hidden = true;
  }
  document.body.classList.remove("manual-capture-modal-open");
  return true;
}

function renderManualCaptureSuccess(form, result = {}) {
  const editable = form.querySelector("[data-manual-capture-editable]");
  const success = form.querySelector("[data-manual-capture-success]");
  const saveButton = form.querySelector("[data-manual-capture-save-button]");
  const closeButton = form.querySelector("[data-manual-capture-close]");
  const record = result?.record && typeof result.record === "object" ? result.record : {};
  const recordId = result?.manual_capture_record_id || result?.record_id || record?.record_id || "";
  const eventIds = Array.isArray(result?.event_ids) ? result.event_ids : (Array.isArray(record?.event_ids) ? record.event_ids : []);
  const symbol = String(record?.symbol || form.querySelector('input[name="symbol"]')?.value || "Ticket");
  const side = String(record?.side || "LONG");
  const quantity = String(record?.quantity || form.querySelector('input[name="quantity"]')?.value || "");
  const fillPrice = String(record?.fill_price || form.querySelector('input[name="fill_price"]')?.value || "");
  const fillTime = String(record?.fill_time || new Date().toISOString());
  if (editable) {
    editable.hidden = true;
  }
  if (success) {
    success.hidden = false;
    success.innerHTML = `
      <h4>Manual capture recorded</h4>
      <p>No broker action was taken.</p>
      <div class="manual-capture-success-ticket">
        <strong>${escapeHtml(symbol)} ${escapeHtml(side)}</strong>
        <span>${escapeHtml(quantity)} @ ${escapeHtml(fillPrice)}</span>
        <span>Recorded at ${escapeHtml(fillTime)}</span>
      </div>
      <div class="manual-capture-success-grid">
        <div><span>Record ID</span><strong>${escapeHtml(recordId || "recorded")}</strong></div>
        <div><span>Event IDs</span><strong>${escapeHtml(eventIds.join(", ") || "recorded")}</strong></div>
      </div>`;
  }
  if (saveButton) {
    saveButton.hidden = true;
  }
  if (closeButton) {
    closeButton.disabled = false;
    closeButton.textContent = "Close";
  }
}


function parseCommandPayload(rawValue) {
  try {
    const parsed = JSON.parse(String(rawValue || "{}").trim() || "{}");
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch (_error) {
    return {};
  }
}

function commandStatusTargets(commandElement) {
  const local = commandElement.closest(".candidate-action-toolbar, [data-command-surface], [data-domain-repair-card], [data-hypothesis-card], [data-domain-card]");
  const targetId = String(commandElement.getAttribute("data-aegis-command-target-id") || "").trim();
  const targetType = String(commandElement.getAttribute("data-aegis-command-target-type") || "").trim();
  const nodes = [];
  local?.querySelectorAll?.("[data-aegis-command-status]").forEach((node) => nodes.push(node));
  local?.querySelectorAll?.("[data-domain-repair-status]").forEach((node) => {
    if (!nodes.includes(node)) nodes.push(node);
  });
  if (targetType === "domain_certification" && targetId) {
    document.querySelectorAll(`[data-domain-repair-card="${CSS.escape(targetId)}"] [data-aegis-command-status], [data-domain-repair-card="${CSS.escape(targetId)}"] [data-domain-repair-status]`).forEach((node) => {
      if (!nodes.includes(node)) nodes.push(node);
    });
  }
  if (!nodes.length) {
    const card = commandElement.closest("[data-hypothesis-card]");
    const output = card?.querySelector?.("[data-hypothesis-card-message-output]");
    if (output) nodes.push(output);
  }
  return nodes;
}

function setCommandStatus(commandElement, message, tone = "") {
  commandStatusTargets(commandElement).forEach((node) => {
    node.hidden = false;
    node.textContent = message;
    if (tone) node.dataset.tone = tone;
  });
}

function setPaperPromotionStatus(actionElement, message, tone = "") {
  const card = actionElement.closest?.("[data-testid=\"paper-promotion-recommendation-item\"]");
  const status = card?.querySelector?.("[data-paper-promotion-status]");
  if (status) {
    status.hidden = false;
    status.textContent = message;
    if (tone) status.dataset.tone = tone;
  }
}

function paperPromotionSuccessMessage(action = "") {
  const normalized = String(action || "").trim().toUpperCase();
  if (normalized === "APPROVE_PAPER_TEST" || normalized === "APPROVED") return "Approved for paper research tracking.";
  if (normalized === "REJECT" || normalized === "REJECTED") return "Paper promotion rejected.";
  if (normalized === "DEFER" || normalized === "DEFERRED") return "Paper promotion deferred.";
  return "Paper promotion action recorded.";
}

function setOperatorActionStatus(actionElement, message, tone = "") {
  const card = actionElement.closest?.("[data-testid=\"canonical-action-card\"]");
  const status = card?.querySelector?.("[data-operator-action-status]");
  if (status) {
    status.hidden = false;
    status.textContent = message;
    if (tone) status.dataset.tone = tone;
  }
}

async function runCanonicalOperatorActionElement(actionElement) {
  const actionId = String(actionElement.getAttribute("data-action-id") || "").trim();
  const hypothesisId = String(actionElement.getAttribute("data-hypothesis-id") || "").trim();
  const actionType = String(actionElement.getAttribute("data-action-type") || "").trim();
  const buttonClicked = String(actionElement.getAttribute("data-button-clicked") || "").trim();
  const sourceStateHash = String(actionElement.getAttribute("data-source-state-hash") || "").trim();
  const routeDay = new URLSearchParams(window.location.search || "").get("day") || new URLSearchParams(window.location.search || "").get("operational_day") || "";
  if (!actionId || !hypothesisId || !actionType || !buttonClicked) {
    setOperatorActionStatus(actionElement, "Action failed: missing action metadata", "error");
    return;
  }
  const card = actionElement.closest?.("[data-testid=\"canonical-action-card\"]");
  const buttons = card ? Array.from(card.querySelectorAll("[data-operator-action-event]")) : [actionElement];
  buttons.forEach((button) => { button.disabled = true; });
  setOperatorActionStatus(actionElement, "Recording operator action event...", "loading");
  try {
    const result = await appendOperatorActionEvent({
      day_utc: routeDay,
      action_id: actionId,
      hypothesis_id: hypothesisId,
      action_type: actionType,
      button_clicked: buttonClicked,
      prior_state: actionType,
      new_state: buttonClicked,
      source_state_hash: sourceStateHash,
      actor: "David / operator",
    });
    if (!result?.ok) throw new Error(result?.message || result?.error_message || "operator action endpoint returned ok=false");
    setOperatorActionStatus(actionElement, `${buttonClicked} recorded.`, "ready");
    state.operatorActionEvent = { lastActionId: actionId, lastButton: buttonClicked, lastResult: result, lastError: null };
    await renderRoute({ source: "operator-action-event" });
  } catch (error) {
    const reason = error?.payload?.message || error?.payload?.error_message || error?.message || "unknown error";
    setOperatorActionStatus(actionElement, `Action failed: ${reason}`, "error");
    state.operatorActionEvent = { lastActionId: actionId, lastButton: buttonClicked, lastResult: null, lastError: reason };
    buttons.forEach((button) => { button.disabled = false; });
  }
}

async function runPaperPromotionActionElement(actionElement) {
  const action = String(actionElement.getAttribute("data-paper-promotion-action") || "").trim().toUpperCase();
  const hypothesisId = String(actionElement.getAttribute("data-hypothesis-id") || "").trim();
  const routeDay = new URLSearchParams(window.location.search || "").get("day") || new URLSearchParams(window.location.search || "").get("operational_day") || "";
  if (!action || !hypothesisId) {
    setPaperPromotionStatus(actionElement, "Approval failed: missing action or hypothesis id", "error");
    return;
  }
  const card = actionElement.closest?.("[data-testid=\"paper-promotion-recommendation-item\"]");
  const buttons = card ? Array.from(card.querySelectorAll("[data-paper-promotion-action]")) : [actionElement];
  buttons.forEach((button) => { button.disabled = true; });
  setPaperPromotionStatus(actionElement, "Recording paper promotion approval...", "loading");
  try {
    const result = await executePaperPromotionAction({
      day_utc: routeDay,
      hypothesis_id: hypothesisId,
      action,
      actor: "David / operator",
      reason: "Research UI paper promotion action.",
    });
    if (!result?.ok) {
      throw new Error(result?.message || result?.error_message || "approval endpoint returned ok=false");
    }
    setPaperPromotionStatus(actionElement, paperPromotionSuccessMessage(action), "ready");
    state.paperPromotionAction = {
      lastAction: action,
      lastHypothesisId: hypothesisId,
      lastResult: result,
      lastError: null,
    };
    await renderRoute({ source: "paper-promotion-action" });
    const refreshed = document.querySelector(`[data-testid="paper-promotion-recommendation-item"][data-hypothesis-id="${window.CSS?.escape ? CSS.escape(hypothesisId) : hypothesisId}"]`);
    const refreshedStatus = refreshed?.querySelector?.("[data-paper-promotion-status]");
    if (refreshedStatus && (action === "APPROVE_PAPER_TEST" || action === "APPROVED")) {
      refreshedStatus.hidden = false;
      refreshedStatus.textContent = "Approved for paper research tracking.";
      refreshedStatus.dataset.tone = "ready";
    }
  } catch (error) {
    const reason = error?.payload?.message || error?.payload?.error_message || error?.message || "unknown error";
    setPaperPromotionStatus(actionElement, `Approval failed: ${reason}`, "error");
    state.paperPromotionAction = {
      lastAction: action,
      lastHypothesisId: hypothesisId,
      lastResult: null,
      lastError: reason,
    };
    buttons.forEach((button) => { button.disabled = false; });
  }
}

function ensureDomainCommandResultSink(targetId, fallbackLabel = "") {
  if (!targetId || !window.CSS?.escape) return null;
  const region = document.querySelector("[data-domain-command-results-region]");
  if (!region) return null;
  region.hidden = false;
  const list = region.querySelector("[data-domain-command-result-list]") || region;
  let slot = region.querySelector(`[data-domain-command-result-sink="${CSS.escape(targetId)}"]`);
  if (!slot) {
    slot = document.createElement("div");
    slot.className = "runtime-domain-command-result-slot";
    slot.dataset.domainCommandResultSink = targetId;
    slot.dataset.commandResultDomain = fallbackLabel || targetId;
  }
  list.prepend(slot);
  slot.hidden = false;
  return slot;
}

function commandResultPanelTargets(commandElement) {
  const targetId = String(commandElement.getAttribute("data-aegis-command-target-id") || "").trim();
  const targetType = String(commandElement.getAttribute("data-aegis-command-target-type") || "").trim();
  if (targetType === "domain_certification") {
    const label = commandElement.closest("[data-domain-repair-card]")?.querySelector("strong")?.textContent?.trim() || targetId;
    const sink = ensureDomainCommandResultSink(targetId, label);
    return sink ? [sink] : [];
  }
  const local = commandElement.closest("[data-command-surface], [data-domain-repair-card], [data-hypothesis-card], [data-domain-card], [data-candidate-card], dialog, article, section");
  return local ? [local] : [];
}

function commandResultPanelHtml(result = {}, domainLabel = "") {
  const panel = result.command_result && typeof result.command_result === "object" ? result.command_result : {};
  const statusLabel = String(panel.status_label || result.result_status || (result.ok ? "Success" : "Failed"));
  const domain = String(panel.domain || panel.domain_id || domainLabel || "");
  const message = String(panel.plain_english_result || result.user_message || result.error_message || (result.ok ? "Command completed." : "Command failed."));
  const nextStep = String(panel.next_required_step || result.next_state || "Review the updated card state.");
  const timestamp = String(panel.timestamp || new Date().toLocaleString());
  const jobId = String(panel.job_id || result.job_id || "");
  const auditId = String(panel.audit_id || result.audit_id || "");
  const missingSource = String(panel.missing_source_name || "");
  const expectedPath = String(panel.expected_source_path || panel.copy_required_path || "");
  const failureReason = String(panel.failure_reason || result.error_message || "");
  const tone = String(panel.result_status || result.result_status || "").toLowerCase().replace(/[^a-z0-9_-]/g, "-") || (result.ok ? "success" : "failed");
  return `<div class="command-result-panel" data-command-result-panel data-result-status="${escapeHtml(tone)}" role="status" aria-live="polite">
    <div class="command-result-panel-heading">
      <strong>${escapeHtml(statusLabel)}</strong>
      <span>${escapeHtml(timestamp)}</span>
    </div>
    <p>${escapeHtml(message)}</p>
    <dl class="command-result-panel-facts">
      ${domain ? `<div><dt>Domain</dt><dd>${escapeHtml(domain)}</dd></div>` : ""}
      <div><dt>Next step</dt><dd>${escapeHtml(nextStep)}</dd></div>
      ${missingSource ? `<div><dt>Missing source</dt><dd>${escapeHtml(missingSource)}</dd></div>` : ""}
      ${expectedPath ? `<div><dt>Required path</dt><dd><code>${escapeHtml(expectedPath)}</code></dd></div>` : ""}
      ${jobId ? `<div><dt>Job ID</dt><dd><code>${escapeHtml(jobId)}</code></dd></div>` : ""}
      ${auditId ? `<div><dt>Audit ID</dt><dd><code>${escapeHtml(auditId)}</code></dd></div>` : ""}
      ${failureReason ? `<div><dt>Failure</dt><dd>${escapeHtml(failureReason)}</dd></div>` : ""}
    </dl>
    ${expectedPath ? `<button class="ghost-button command-result-copy-button" type="button" data-copy-text="${escapeHtml(expectedPath)}">Copy required path</button>` : ""}
  </div>`;
}

function showCommandResultPanel(commandElement, result = {}) {
  const targets = commandResultPanelTargets(commandElement);
  targets.forEach((target) => {
    target.querySelectorAll(":scope > [data-command-result-panel]").forEach((node) => node.remove());
    const domainLabel = target.getAttribute("data-command-result-domain") || commandElement.getAttribute("data-aegis-command-target-id") || "";
    target.insertAdjacentHTML("beforeend", commandResultPanelHtml(result, domainLabel));
    const panel = target.querySelector(":scope > [data-command-result-panel]");
    panel?.classList.add("command-result-panel-flash");
  });
}
function renderAskAegisGroundedResponse(payload = {}) {
  const response = payload.latest_response && typeof payload.latest_response === "object" ? payload.latest_response : payload;
  const sources = Array.isArray(response.source_artifacts) ? response.source_artifacts : [];
  const selected = Array.isArray(response.selected_evidence) ? response.selected_evidence : [];
  const unsupported = Array.isArray(response.unsupported_claims) ? response.unsupported_claims : [];
  const sourceRows = sources.map((source) => `<li><strong>${escapeHtml(source.source_id || source.artifact_family || "source")}</strong>: ${escapeHtml(source.status || "UNKNOWN")}<div class="muted-mini">${escapeHtml(source.path || "")}</div></li>`).join("");
  const evidenceRows = selected.slice(0, 6).map((item) => `<li><strong>${escapeHtml(item.title || item.evidence_id || "Evidence")}</strong>: ${escapeHtml(item.summary || "")}</li>`).join("");
  return `
    <div class="stack-card-title">${escapeHtml(response.question || "Ask Aegis")}</div>
    <p class="support-note">${escapeHtml(response.answer || "No grounded answer was returned.")}</p>
    <div class="metric-grid compact">
      <article class="metric-card"><span>Confidence</span><strong>${escapeHtml(response.confidence || "UNKNOWN")}</strong></article>
      <article class="metric-card"><span>Intent</span><strong>${escapeHtml(response.intent || "UNKNOWN")}</strong></article>
      <article class="metric-card"><span>Source Evidence</span><strong>${escapeHtml(String(sources.length))}</strong></article>
      <article class="metric-card"><span>Unsupported Claims</span><strong>${escapeHtml(String(unsupported.length))}</strong></article>
    </div>
    ${unsupported.length ? `<div class="status-banner warning">${escapeHtml(unsupported.join(" "))}</div>` : ""}
    <details class="operator-disclosure ask-aegis-evidence"><summary>Sources Used</summary><ul>${sourceRows || "<li>No source artifacts were returned.</li>"}</ul></details>
    <details class="operator-disclosure ask-aegis-evidence"><summary>Selected Evidence</summary><ul>${evidenceRows || "<li>No selected evidence was returned.</li>"}</ul></details>
    <p class="muted-mini">context_hash: ${escapeHtml(response.context_hash || "missing")} · generated_at: ${escapeHtml(response.generated_at || "unknown")}</p>
  `;
}

async function submitAskAegisForm(form) {
  const output = form.closest("#ask-aegis")?.querySelector("[data-ask-aegis-response]");
  const formData = new FormData(form);
  const question = String(formData.get("question") || "").trim();
  const routeDay = new URLSearchParams(window.location.search || "").get("day") || "";
  const dayUtc = String(formData.get("day_utc") || routeDay || "").trim();
  if (!question) {
    if (output) output.innerHTML = `<div class="stack-card-title">Question required</div><p class="support-note">Enter an operational question before asking Aegis.</p>`;
    return;
  }
  const submitter = form.querySelector('button[type="submit"]');
  if (submitter) {
    submitter.disabled = true;
    submitter.setAttribute("aria-busy", "true");
  }
  if (output) {
    output.innerHTML = `<div class="stack-card-title">Assembling evidence...</div><p class="support-note">Building deterministic context from Aegis operational artifacts.</p>`;
  }
  try {
    const result = await askAegisAiOperations({ question, day_utc: dayUtc });
    if (output) output.innerHTML = renderAskAegisGroundedResponse(result.latest_response || result);
  } catch (error) {
    if (output) {
      output.innerHTML = `<div class="stack-card-title">Ask Aegis failed</div><p class="support-note">${escapeHtml(error?.message || "AI operations assistant request failed.")}</p><p class="muted-mini">Endpoint: /api/aegis/ai-operations/ask</p>`;
    }
  } finally {
    if (submitter) {
      submitter.disabled = false;
      submitter.removeAttribute("aria-busy");
    }
  }
}


function openCommandDetail(commandElement) {
  const panelId = String(commandElement.getAttribute("data-command-detail-target") || commandElement.getAttribute("data-hypothesis-detail-target") || "").trim();
  const panel = panelId ? document.getElementById(panelId) : null;
  const card = commandElement.closest("[data-hypothesis-card]");
  if (!panel) {
    setCommandStatus(commandElement, "Detail panel is not available for this action.", "error");
    return;
  }
  showCommandDetailPanel(panel);
  rememberOpenCommandDetail(commandElement, panelId);
  wireCommandDetailClose(panel);
  panel.dataset.openedBy = String(commandElement.textContent || commandElement.getAttribute("data-aegis-command-id") || "View details").trim();
  panel.dataset.commandDetailOpenCommand = String(commandElement.getAttribute("data-aegis-command-id") || "").trim();
  document.body.dataset.hypothesisLastFocusToken = String(Date.now());
  panel.scrollIntoView({ behavior: "smooth", block: "nearest" });
  panel.focus?.({ preventScroll: true });
  card?.classList.remove("hypothesis-card-highlight");
  if (card) {
    void card.offsetWidth;
    card.classList.add("hypothesis-card-highlight");
    window.setTimeout(() => card.classList.remove("hypothesis-card-highlight"), 2400);
  }
}

function verifiedRuntimeActionValue(actionElement, attrName, sourceAttrName, fallback = "") {
  const sourceId = String(actionElement.getAttribute(sourceAttrName) || "").trim();
  if (sourceId) {
    const sourceNode = document.getElementById(sourceId);
    if (sourceNode) {
      const value = "value" in sourceNode ? String(sourceNode.value || "") : String(sourceNode.textContent || "");
      if (value.trim()) return value.trim();
    }
  }
  return String(actionElement.getAttribute(attrName) || fallback || "").trim();
}

async function runVerifiedRuntimeActionElement(actionElement) {
  const actionId = String(actionElement.getAttribute("data-aegis-verified-runtime-action") || "").trim();
  if (!actionId) return;
  const output = document.querySelector("[data-aegis-verified-runtime-output]");
  actionElement.disabled = true;
  if (output) output.textContent = `Running ${actionId}...`;
  try {
    const requestedDay = String(actionElement.getAttribute("data-requested-day") || actionElement.getAttribute("data-day") || "").trim();
    const args = {
      from_day: verifiedRuntimeActionValue(actionElement, "data-from-day", "data-from-day-source"),
      to_day: verifiedRuntimeActionValue(actionElement, "data-to-day", "data-to-day-source"),
      claim: verifiedRuntimeActionValue(actionElement, "data-claim", "data-claim-source"),
    };
    const result = await executeAegisVerifiedRuntimeAction({
      action_id: actionId,
      requested_day: requestedDay,
      args,
      operator_context: {
        source: "schmidtvault_portal",
        route: window.location?.pathname || "",
      },
    });
    if (output) {
      const command = Array.isArray(result.command_argv) ? result.command_argv.join(" ") : "";
      const renderedResult = [
        result.user_message || "Action completed.",
        `status: ${result.status || result.result_status || "UNKNOWN"}`,
        result.run_id ? `run_id: ${result.run_id}` : "",
        Number.isFinite(Number(result.exit_code)) ? `exit_code: ${result.exit_code}` : "",
        command ? `command: ${command}` : "",
        result.stdout_excerpt || result.stdout_tail || "",
        (result.stderr_excerpt || result.stderr_tail) ? `stderr:\n${result.stderr_excerpt || result.stderr_tail}` : "",
      ].filter(Boolean).join("\n\n");
      output.textContent = renderedResult;
      const latestCopy = document.getElementById("aegisVerifiedRuntimeLatestActionResultCopy");
      if (latestCopy) latestCopy.value = JSON.stringify(result, null, 2);
      const latestCopyButton = document.querySelector('[data-copy-source="aegisVerifiedRuntimeLatestActionResultCopy"]');
      if (latestCopyButton) latestCopyButton.disabled = false;
    }
  } catch (error) {
    if (output) output.textContent = error?.message || "Verified runtime action failed.";
  } finally {
    actionElement.disabled = false;
  }
}

function updateLastPaperTradeActionDiagnostic(detail = {}) {
  const payload = {
    timestamp: new Date().toISOString(),
    ...detail,
  };
  state.lastPaperTradeAction = payload;
  window.__AEGIS_LAST_PAPER_ENTRY_ACTION = payload;
  const rendered = JSON.stringify(payload, null, 2);
  document.querySelectorAll("[data-last-paper-entry-action]").forEach((node) => {
    node.textContent = rendered;
  });
}

function paperEntryCommandPayloadForDiagnostics(commandElement, parsedPayload = {}) {
  return {
    candidate_id: parsedPayload.candidate_id || commandElement.getAttribute("data-aegis-command-target-id") || "",
    candidate_contract_id: parsedPayload.candidate_contract_id || "",
    paper_session_id: parsedPayload.paper_session_id || commandElement.getAttribute("data-paper-session-id") || "",
    day_utc: parsedPayload.day_utc || parsedPayload.operational_day || "",
    action: parsedPayload.action || "",
    paper_entry_price: parsedPayload.paper_entry_price || parsedPayload.entry_price || "",
    paper_stop_price: parsedPayload.paper_stop_price || parsedPayload.stop_price || "",
    quantity: parsedPayload.quantity || "",
    notional: parsedPayload.notional || "",
  };
}



function workflowCommandLabel(commandType = "") {
  const normalized = String(commandType || "").toUpperCase();
  const labels = {
    APPROVE_CANDIDATE: "Approve",
    REJECT_CANDIDATE: "Reject",
    CONFIRM_CANDIDATE_CAPTURED: "Confirm Captured",
    MARK_CANDIDATE_NOT_CAPTURED: "Mark Not Captured",
    DEFER_CANDIDATE: "Defer",
    CORRECT_CANDIDATE_CAPTURE: "Correct Capture",
    RECORD_PAPER_ENTRY: "Record Entry",
    RECORD_PAPER_EXIT: "Record Exit",
    REVOKE_APPROVAL: "Reject Approval",
  };
  return labels[normalized] || "Workflow";
}

function updateOperatorWorkflowRowDebug(commandElement, detail = {}) {
  const region = commandElement.closest?.("[data-row-command-region], .candidate-action-toolbar");
  if (!region) return;
  const setField = (name, value) => {
    if (value === undefined || value === null) return;
    region.querySelectorAll(`[data-command-debug-field="${name}"]`).forEach((node) => {
      node.textContent = String(value || "-");
    });
  };
  setField("click_received_at", detail.click_received_at);
  setField("command_id", detail.command_id);
  setField("command_status", detail.command_status);
  setField("endpoint_status", detail.endpoint_status);
  setField("last_error", detail.last_error);
}

function operatorWorkflowCommandRecordPayload(commandElement, payload = {}) {
  const candidateId = String(payload.candidate_id || commandElement.getAttribute("data-aegis-command-target-id") || "").trim();
  const commandType = String(payload.command_type || commandElement.getAttribute("data-aegis-command-id") || "").trim();
  return {
    command_type: commandType,
    created_by: "operator",
    source_ui: String(payload.source_ui || "positions_today_candidates"),
    paper_session_id: String(payload.paper_session_id || commandElement.getAttribute("data-paper-session-id") || ""),
    candidate_id: candidateId,
    candidate_contract_id: String(payload.candidate_contract_id || candidateId),
    day_utc: String(payload.day_utc || payload.operational_day || ""),
    payload: { ...payload, command_type: commandType },
  };
}

function commandResultFromDurableStatus(statusPayload = {}) {
  const result = statusPayload.result && typeof statusPayload.result === "object" ? statusPayload.result : {};
  const command = statusPayload.command && typeof statusPayload.command === "object" ? statusPayload.command : {};
  const status = String(result.status || statusPayload.status || command.status || "RECEIVED");
  const ok = status === "EXECUTED" || status === "RECEIVED" || status === "VALIDATED";
  return {
    ok,
    result_status: status,
    user_message: result.message || `Command ${status}`,
    command_id: statusPayload.command_id || command.command_id || result.command_id || "",
    command_result: {
      status_label: status,
      result_status: status,
      plain_english_result: result.message || `Command ${status}`,
      next_required_step: statusPayload.terminal ? "Refresh projection and review row state." : "Waiting for command processor.",
      expected_source_path: "",
      audit_id: "",
      failure_reason: result.error_code || "",
      timestamp: result.processed_at || command.created_at || new Date().toISOString(),
    },
  };
}

async function pollPaperTradeCommandStatus(commandElement, commandId, dayUtc, targetId, payload) {
  const endpoint = "/api/aegis/commands/status";
  const label = workflowCommandLabel(commandElement.getAttribute("data-aegis-command-id") || payload?.command_type || "");
  for (let attempt = 0; attempt < 12; attempt += 1) {
    await new Promise((resolve) => window.setTimeout(resolve, attempt === 0 ? 600 : 1200));
    const statusPayload = await fetchAegisCommandStatus({ command_id: commandId, day: dayUtc });
    const result = statusPayload.result && typeof statusPayload.result === "object" ? statusPayload.result : {};
    const status = String(statusPayload.status || result.status || "RECEIVED");
    setCommandStatus(commandElement, `${label} submitted (${status})`, status === "FAILED" || status === "REJECTED" ? "error" : "loading");
    updateOperatorWorkflowRowDebug(commandElement, { command_id: commandId, command_status: status, endpoint_status: "200", last_error: status === "FAILED" || status === "REJECTED" ? (result.message || result.error_code || status) : "-" });
    updateLastPaperTradeActionDiagnostic({
      phase: statusPayload.terminal ? "terminal status received" : "status polled",
      last_clicked_candidate: targetId,
      command_id: commandId,
      endpoint,
      payload: paperEntryCommandPayloadForDiagnostics(commandElement, payload),
      http_status: 200,
      response_summary: result.message || status,
      receipt_id: result.receipt_id || "",
      receipt_path: result.receipt_path || "",
      ok: status === "EXECUTED",
    });
    if (statusPayload.terminal) {
      const panelResult = commandResultFromDurableStatus(statusPayload);
      setCommandStatus(commandElement, `${label} ${status}${result.message ? ` - ${result.message}` : ""}`, panelResult.ok ? "ready" : "error");
      updateOperatorWorkflowRowDebug(commandElement, { command_id: commandId, command_status: status, endpoint_status: "200", last_error: panelResult.ok ? "-" : (result.message || result.error_code || status) });
      showCommandResultPanel(commandElement, panelResult);
      window.setTimeout(() => renderRoute(), 250);
      return statusPayload;
    }
  }
  return null;
}

async function submitOperatorWorkflowCommand(commandElement, payload, targetId) {
  const endpoint = "/api/aegis/commands";
  const recordPayload = operatorWorkflowCommandRecordPayload(commandElement, payload);
  const label = workflowCommandLabel(recordPayload.command_type);
  const clickedAt = new Date().toISOString();
  setCommandStatus(commandElement, `${label} command submitted`, "loading");
  updateOperatorWorkflowRowDebug(commandElement, { click_received_at: clickedAt, command_status: "SUBMITTING", endpoint_status: "POST /api/aegis/commands", last_error: "-" });
  updateLastPaperTradeActionDiagnostic({
    phase: "click received",
    last_clicked_candidate: targetId,
    endpoint,
    payload: paperEntryCommandPayloadForDiagnostics(commandElement, payload),
    response_summary: `${label} command submitted`,
  });
  const recorded = await recordAegisCommand(recordPayload);
  if (!recorded.ok || !recorded.command_id) {
    updateOperatorWorkflowRowDebug(commandElement, { command_status: "RECORD_FAILED", endpoint_status: "ERROR", last_error: recorded.message || "Command could not be recorded." });
    throw Object.assign(new Error(recorded.message || "Command could not be recorded."), { payload: recorded, operatorSafe: { statusCode: 400 } });
  }
  setCommandStatus(commandElement, `${label} command submitted`, "loading");
  updateOperatorWorkflowRowDebug(commandElement, { command_id: recorded.command_id, command_status: "RECEIVED", endpoint_status: "202", last_error: "-" });
  updateLastPaperTradeActionDiagnostic({
    phase: "command received",
    last_clicked_candidate: targetId,
    command_id: recorded.command_id,
    endpoint,
    payload: paperEntryCommandPayloadForDiagnostics(commandElement, payload),
    http_status: 202,
    response_summary: recorded.message || "Command received",
    inbox_path: recorded.inbox_path || "",
    ok: true,
  });
  showCommandResultPanel(commandElement, {
    ok: true,
    result_status: "RECEIVED",
    user_message: `${label} command received.`,
    command_id: recorded.command_id,
    command_result: {
      status_label: "RECEIVED",
      result_status: "RECEIVED",
      plain_english_result: `${label} command received.`,
      next_required_step: "Waiting for command processor.",
      expected_source_path: "",
      audit_id: "",
      timestamp: recorded.command?.created_at || new Date().toISOString(),
    },
  });
  await pollPaperTradeCommandStatus(commandElement, recorded.command_id, recordPayload.day_utc, targetId, payload);
  return recorded;
}

async function runAegisCommandElement(commandElement) {
  const commandId = String(commandElement.getAttribute("data-aegis-command-id") || "").trim();
  const actionType = String(commandElement.getAttribute("data-aegis-command-action-type") || "").trim();
  const targetType = String(commandElement.getAttribute("data-aegis-command-target-type") || "").trim();
  const targetId = String(commandElement.getAttribute("data-aegis-command-target-id") || "").trim();
  if (!commandId) {
    setCommandStatus(commandElement, "This action is missing a command contract.", "error");
    return;
  }
  if (actionType === "IN_PAGE_DETAIL") {
    openCommandDetail(commandElement);
    return;
  }
  if (actionType === "SCROLL_FOCUS" || commandId === "SCROLL_TO_HYPOTHESIS_SECTION" || commandId === "FOCUS_HYPOTHESIS_CARD") {
    revealHypothesisSummaryTarget({
      sectionTargetId: String(commandElement.getAttribute("data-hypothesis-summary-section-target") || (commandId === "SCROLL_TO_HYPOTHESIS_SECTION" ? targetId : "") || "").trim(),
      cardTargetId: String(commandElement.getAttribute("data-hypothesis-summary-card-target") || (commandId === "FOCUS_HYPOTHESIS_CARD" ? targetId : "") || "").trim(),
    });
    return;
  }
  if (actionType === "EXPAND_SECTION") {
    const route = String(commandElement.getAttribute("data-route") || commandElement.getAttribute("href") || "").trim();
    if (route) {
      await navigateTo(route);
      return;
    }
    setCommandStatus(commandElement, "This command is declared for in-page handling only.", "ready");
    return;
  }
  if (actionType !== "API_COMMAND") {
    setCommandStatus(commandElement, "This command is declared for in-page handling only.", "ready");
    return;
  }
  const payload = parseCommandPayload(commandElement.getAttribute("data-aegis-command-payload"));
  const endpoint = "/api/aegis/commands/execute";
  const workflowCommandIds = new Set(["APPROVE_CANDIDATE", "REJECT_CANDIDATE", "CONFIRM_CANDIDATE_CAPTURED", "MARK_CANDIDATE_NOT_CAPTURED", "DEFER_CANDIDATE", "CORRECT_CANDIDATE_CAPTURE", "RECORD_PAPER_ENTRY", "RECORD_PAPER_EXIT", "REVOKE_APPROVAL"]);
  const isOperatorWorkflowCommand = workflowCommandIds.has(commandId);
  commandElement.disabled = true;
  if (isOperatorWorkflowCommand) {
    try {
      await submitOperatorWorkflowCommand(commandElement, { ...payload, command_type: commandId }, targetId);
    } catch (error) {
      const responseBody = error?.payload ? JSON.stringify(error.payload) : "";
      const failureResult = {
        ok: false,
        result_status: "FAILED",
        error_message: error?.message || "Command failed.",
        command_result: {
          status_label: "FAILED",
          result_status: "FAILED",
          plain_english_result: error?.message || "Command failed.",
          next_required_step: "Review the command recording error and try again after the underlying issue is resolved.",
          failure_reason: responseBody || error?.message || "Command failed.",
          timestamp: new Date().toLocaleString(),
        },
      };
      setCommandStatus(commandElement, failureResult.error_message, "error");
      updateOperatorWorkflowRowDebug(commandElement, { command_status: "FAILED", endpoint_status: error?.operatorSafe?.statusCode || "ERROR", last_error: failureResult.error_message });
      updateLastPaperTradeActionDiagnostic({
        phase: "command recording error",
        last_clicked_candidate: targetId,
        endpoint: "/api/aegis/commands",
        payload: paperEntryCommandPayloadForDiagnostics(commandElement, payload),
        http_status: error?.operatorSafe?.statusCode || null,
        response_summary: failureResult.error_message,
        response_body: responseBody,
        ok: false,
      });
      showCommandResultPanel(commandElement, failureResult);
    } finally {
      commandElement.disabled = false;
    }
    return;
  }
  setCommandStatus(commandElement, "Running command…", "loading");
  try {
    const requestEnvelope = {
      command_id: commandId,
      target_type: targetType,
      target_id: targetId,
      operational_day: payload.operational_day || payload.day_utc || "",
      payload,
    };
    const result = await executeAegisCommand(requestEnvelope);
    const message = result.user_message || result.message || (result.ok ? "Command completed." : "Command failed.");
    const statusLabel = result.command_result?.status_label || "";
    const receiptTimestamp = result.receipt?.timestamp_utc || result.receipt?.timestamp || "";
    const receiptId = result.receipt?.receipt_id || result.receipt?.paper_entry_receipt_id || (targetId && receiptTimestamp ? `paper-review:${targetId}:${receiptTimestamp}` : "");
    setCommandStatus(commandElement, message, result.ok ? "ready" : "error");
    showCommandResultPanel(commandElement, result);
    const card = commandElement.closest("[data-hypothesis-card]");
    if (commandId === "START_RESEARCH" && result.ok && card) {
      card.dataset.hypothesisStatus = result.next_state || "Queued";
      const badge = card.querySelector(".hypothesis-status");
      if (badge) badge.textContent = result.next_state || "Queued";
      card.classList.add("hypothesis-card-highlight");
    }
  } catch (error) {
    const responseBody = error?.payload ? JSON.stringify(error.payload) : "";
    const failureResult = {
      ok: false,
      result_status: "FAILED",
      error_message: error?.message || "Command failed.",
      command_result: {
        status_label: "Failed",
        result_status: "FAILED",
        plain_english_result: error?.message || "Command failed.",
        next_required_step: "Review the error and try again after the underlying issue is resolved.",
        failure_reason: responseBody || error?.message || "Command failed.",
        timestamp: new Date().toLocaleString(),
      },
    };
    setCommandStatus(commandElement, failureResult.error_message, "error");
    showCommandResultPanel(commandElement, failureResult);
  } finally {
    commandElement.disabled = false;
  }
}

async function handleClick(event) {
  if (event.defaultPrevented) return;

  const askAegisPrompt = event.target.closest("[data-ask-aegis-prompt]");
  if (askAegisPrompt) {
    event.preventDefault();
    const form = askAegisPrompt.closest("[data-ask-aegis-form]") || document.querySelector("[data-ask-aegis-form]");
    const input = form?.querySelector?.("[data-ask-aegis-question]");
    if (input) {
      input.value = String(askAegisPrompt.getAttribute("data-ask-aegis-prompt") || "");
      input.focus();
    }
    if (form) await submitAskAegisForm(form);
    return;
  }
  const paperEntryCancel = event.target.closest?.("[data-paper-entry-cancel]");
  if (paperEntryCancel) {
    event.preventDefault();
    closePaperTradeDialog(paperEntryCancel.closest("dialog[data-paper-entry-dialog]"));
    return;
  }
  if (event.target?.matches?.("dialog[data-paper-entry-dialog]")) {
    const dialog = event.target;
    if (dialog.dataset.paperSubmitting === "true") return;
    event.preventDefault();
    closePaperTradeDialog(dialog);
    return;
  }
  const researchQuickFilter = event.target.closest("[data-research-quick-filter]");
  if (researchQuickFilter) {
    event.preventDefault();
    const region = researchQuickFilter.closest("[data-research-hypothesis-search-region]");
    if (region) {
      region.dataset.activeFilter = String(researchQuickFilter.getAttribute("data-research-quick-filter") || "all");
      region.querySelectorAll("[data-research-quick-filter]").forEach((button) => {
        button.classList.toggle("is-active", button === researchQuickFilter);
      });
    }
    applyResearchHypothesisFilters(researchQuickFilter);
    return;
  }

  const manualCaptureOpen = event.target.closest("[data-manual-capture-open]");
  if (manualCaptureOpen) {
    event.preventDefault();
    const modalId = String(manualCaptureOpen.getAttribute("data-manual-capture-open") || "").trim();
    openManualCaptureDialog(modalId ? document.getElementById(modalId) : null);
    return;
  }

  const manualCaptureClose = event.target.closest("[data-manual-capture-close]");
  if (manualCaptureClose) {
    event.preventDefault();
    const modal = manualCaptureClose.closest("[data-manual-capture-dialog]");
    const form = modal?.querySelector?.(".manual-capture-record-form");
    const success = form?.querySelector?.("[data-manual-capture-success]");
    closeManualCaptureDialog(modal, { force: Boolean(success && !success.hidden) });
    return;
  }

  const verifiedRuntimeAction = event.target.closest("[data-aegis-verified-runtime-action]");
  if (verifiedRuntimeAction) {
    event.preventDefault();
    await runVerifiedRuntimeActionElement(verifiedRuntimeAction);
    return;
  }

  const canonicalOperatorAction = event.target.closest("[data-operator-action-event]");
  if (canonicalOperatorAction) {
    event.preventDefault();
    await runCanonicalOperatorActionElement(canonicalOperatorAction);
    return;
  }

  const paperPromotionAction = event.target.closest("[data-paper-promotion-action]");
  if (paperPromotionAction) {
    event.preventDefault();
    await runPaperPromotionActionElement(paperPromotionAction);
    return;
  }

  const aegisCommandAction = event.target.closest("[data-aegis-command-id]");
  if (aegisCommandAction) {
    event.preventDefault();
    await runAegisCommandElement(aegisCommandAction);
    return;
  }

  const candidateModalOpen = event.target.closest("[data-candidate-open-modal]");
  if (candidateModalOpen) {
    event.preventDefault();
    const modalId = String(candidateModalOpen.getAttribute("data-candidate-open-modal") || "").trim();
    const modal = modalId ? document.getElementById(modalId) : null;
    if (modal?.showModal) {
      modal.showModal();
    } else if (modal) {
      modal.hidden = false;
    }
    return;
  }

  const candidateModalClose = event.target.closest("[data-candidate-close-modal]");
  if (candidateModalClose) {
    event.preventDefault();
    const modal = candidateModalClose.closest("dialog");
    if (modal?.close) {
      modal.close();
    } else if (modal) {
      modal.hidden = true;
    }
    return;
  }

  const edgeModalOpen = event.target.closest("[data-edge-open-modal]");
  if (edgeModalOpen) {
    event.preventDefault();
    const modalId = String(edgeModalOpen.getAttribute("data-edge-open-modal") || "").trim();
    const modal = modalId ? document.getElementById(modalId) : null;
    if (modal?.showModal) {
      modal.showModal();
    } else if (modal) {
      modal.hidden = false;
    }
    return;
  }

  const edgeModalClose = event.target.closest("[data-edge-close-modal]");
  if (edgeModalClose) {
    event.preventDefault();
    const modal = edgeModalClose.closest("dialog");
    if (modal?.close) {
      modal.close();
    } else if (modal) {
      modal.hidden = true;
    }
    return;
  }

  const copyButton = event.target.closest("[data-copy-source], [data-copy-text]");
  if (copyButton) {
    event.preventDefault();
    let textToCopy = String(copyButton.getAttribute("data-copy-text") || "");
    if (!textToCopy) {
      const sourceId = String(copyButton.getAttribute("data-copy-source") || "").trim();
      if (sourceId) {
        const sourceNode = document.getElementById(sourceId);
        if (sourceNode) {
          textToCopy = "value" in sourceNode ? String(sourceNode.value || "") : String(sourceNode.textContent || "");
        }
      }
    }
    textToCopy = String(textToCopy || "").trim();
    if (textToCopy) {
      try {
        if (navigator?.clipboard?.writeText) {
          await navigator.clipboard.writeText(textToCopy);
        } else {
          const temp = document.createElement("textarea");
          temp.value = textToCopy;
          document.body.appendChild(temp);
          temp.select();
          document.execCommand("copy");
          temp.remove();
        }
      } catch {
        // Keep UI fail-closed; no throwing on clipboard failures.
      }
      const original = copyButton.dataset.originalLabel || copyButton.textContent || "Copy";
      copyButton.dataset.originalLabel = original;
      copyButton.textContent = "Copied";
      window.setTimeout(() => {
        copyButton.textContent = copyButton.dataset.originalLabel || original;
      }, 1400);
    }
    return;
  }

  const hypothesisDetailAction = event.target.closest("[data-hypothesis-detail-target]");
  if (hypothesisDetailAction) {
    event.preventDefault();
    const panelId = String(hypothesisDetailAction.getAttribute("data-hypothesis-detail-target") || "").trim();
    const panel = panelId ? document.getElementById(panelId) : null;
    const card = hypothesisDetailAction.closest("[data-hypothesis-card]");
    if (panel) {
      panel.hidden = false;
      panel.dataset.openedBy = String(hypothesisDetailAction.getAttribute("data-hypothesis-detail-action") || "View details");
      document.body.dataset.hypothesisLastFocusToken = String(Date.now());
      panel.scrollIntoView({ behavior: "smooth", block: "nearest" });
      panel.focus?.({ preventScroll: true });
      card?.classList.remove("hypothesis-card-highlight");
      if (card) {
        void card.offsetWidth;
        card.classList.add("hypothesis-card-highlight");
        window.setTimeout(() => card.classList.remove("hypothesis-card-highlight"), 2400);
      }
    }
    return;
  }

  const hypothesisCardMessage = event.target.closest("[data-hypothesis-card-message]");
  if (hypothesisCardMessage) {
    event.preventDefault();
    const card = hypothesisCardMessage.closest("[data-hypothesis-card]");
    const output = card?.querySelector("[data-hypothesis-card-message-output]");
    if (output) {
      output.hidden = false;
      output.textContent = String(hypothesisCardMessage.getAttribute("data-hypothesis-card-message") || "This action is not available for the current hypothesis state.");
      document.body.dataset.hypothesisLastFocusToken = String(Date.now());
      output.scrollIntoView({ behavior: "smooth", block: "nearest" });
    }
    return;
  }

  const hypothesisSummaryCardTarget = event.target.closest("[data-hypothesis-summary-card-target]");
  if (hypothesisSummaryCardTarget) {
    event.preventDefault();
    revealHypothesisSummaryTarget({
      sectionTargetId: String(hypothesisSummaryCardTarget.getAttribute("data-hypothesis-summary-section-target") || "").trim(),
      cardTargetId: String(hypothesisSummaryCardTarget.getAttribute("data-hypothesis-summary-card-target") || "").trim(),
    });
    return;
  }

  const hypothesisSummarySectionTarget = event.target.closest("[data-hypothesis-summary-section-target]");
  if (hypothesisSummarySectionTarget) {
    event.preventDefault();
    revealHypothesisSummaryTarget({
      sectionTargetId: String(hypothesisSummarySectionTarget.getAttribute("data-hypothesis-summary-section-target") || "").trim(),
    });
    return;
  }

  const scrollButton = event.target.closest("[data-scroll-target]");
  if (scrollButton) {
    event.preventDefault();
    const targetId = String(scrollButton.getAttribute("data-scroll-target") || "").trim();
    if (targetId) {
      const target = document.getElementById(targetId);
      if (target) {
        target.scrollIntoView({ behavior: "smooth", block: "start" });
      }
    }
    return;
  }

  const expandableRow = event.target.closest("[data-expand-row]");
  if (expandableRow && !event.target.closest("a,button,input,textarea,select,label")) {
    const targetId = String(expandableRow.getAttribute("data-expand-row") || "").trim();
    if (targetId) {
      const detailRow = document.getElementById(targetId);
      if (detailRow) {
        detailRow.hidden = !detailRow.hidden;
        expandableRow.setAttribute("aria-expanded", detailRow.hidden ? "false" : "true");
      }
    }
    event.preventDefault();
    return;
  }

  const engineeringDrawerToggle = event.target.closest("[data-engineering-drawer-toggle]");
  if (engineeringDrawerToggle) {
    event.preventDefault();
    const targetRoute = String(engineeringDrawerToggle.getAttribute("data-route") || "/aegis-opportunities").trim();
    const activeSelection = activeNavSelectionForPath(window.location.pathname);
    const alreadyInEngineering = activeSelection.workspace_id === "engineering";
    if (!alreadyInEngineering && targetRoute) {
      state.engineeringDrawerOpen = true;
      await navigateTo(targetRoute);
    } else {
      state.engineeringDrawerOpen = !state.engineeringDrawerOpen;
      renderNav();
    }
    return;
  }

  const engineeringDrawerClose = event.target.closest("[data-engineering-drawer-close]");
  if (engineeringDrawerClose) {
    event.preventDefault();
    state.engineeringDrawerOpen = false;
    renderNav();
    return;
  }


  const changeControlRecordLink = event.target.closest("[data-change-control-record-link]");
  if (changeControlRecordLink) {
    event.preventDefault();
    await navigateTo(changeControlRecordLink.getAttribute("href") || "/aegis-change-control");
    return;
  }

  const routeLink = event.target.closest("[data-route]");
  if (routeLink) {
    event.preventDefault();
    await navigateTo(routeLink.getAttribute("data-route"));
    return;
  }

  const exceptionAction = event.target.closest("[data-exception-action]");
  if (exceptionAction) {
    event.preventDefault();
    await runExceptionAction(exceptionAction);
    return;
  }

  const exceptionCard = event.target.closest("[data-exception-card]");
  if (exceptionCard && !event.target.closest("a,button,input,textarea,select,label")) {
    event.preventDefault();
    await runExceptionAction(exceptionCard);
    return;
  }

  const groupToggle = event.target.closest("[data-nav-group-toggle]");
  if (groupToggle) {
    event.preventDefault();
    const controlsId = String(groupToggle.getAttribute("aria-controls") || "").trim();
    const childrenHost = controlsId ? document.getElementById(controlsId) : null;
    if (state.sidebarMode === "collapsed" || !childrenHost) {
      const route = normalizePath(groupToggle.getAttribute("data-route") || "/aegis-command-center");
      history.pushState({}, "", route);
      await renderRoute();
      renderNav();
      return;
    }
    const groupId = String(groupToggle.getAttribute("data-nav-group-toggle") || "").trim();
    if (state.sidebarOpenGroups.has(groupId)) {
      state.sidebarOpenGroups.delete(groupId);
    } else {
      state.sidebarOpenGroups.add(groupId);
    }
    renderNav();
    return;
  }

  const sidebarCollapse = event.target.closest("[data-sidebar-collapse]");
  if (sidebarCollapse) {
    event.preventDefault();
    state.sidebarMode = state.sidebarMode === "expanded" ? "collapsed" : "expanded";
    localStorage.setItem("aegis.sidebar.mode", state.sidebarMode);
    renderNav();
    return;
  }

  const sidebarOpen = event.target.closest("[data-sidebar-open]");
  if (sidebarOpen) {
    event.preventDefault();
    state.sidebarMode = "expanded";
    localStorage.setItem("aegis.sidebar.mode", state.sidebarMode);
    renderNav();
    return;
  }

  const sidebarClose = event.target.closest("[data-sidebar-close]");
  if (sidebarClose) {
    event.preventDefault();
    state.sidebarMode = "collapsed";
    localStorage.setItem("aegis.sidebar.mode", state.sidebarMode);
    renderNav();
    return;
  }

  const advisoryEvidenceButton = event.target.closest("[data-load-advisory-evidence]");
  if (advisoryEvidenceButton) {
    event.preventDefault();
    const startedAt = performance.now();
    const host = document.getElementById("advisoryEvidenceRefs");
    if (host) {
      host.textContent = "Loading evidence refs...";
    }
    try {
      const response = await fetch("/api/advisory", { cache: "no-store" });
      const advisory = await response.json();
      const refs = Array.isArray(advisory.source_refs) ? advisory.source_refs : [];
      if (host) {
        host.classList.remove("empty-state");
        host.innerHTML = refs.length
          ? `<div class="evidence-list">${refs.map((ref) => `
              <button class="evidence-button" type="button" data-artifact-path="${escapeHtml(ref.artifact_path || ref.path || "")}" data-artifact-title="${escapeHtml(ref.label || ref.artifact_type || "Advisory evidence")}">
                ${escapeHtml(ref.label || ref.artifact_type || "Evidence")}
              </button>
            `).join("")}</div>`
          : `<div class="empty-state">No advisory evidence refs were returned.</div>`;
      }
      logTiming("evidence refs load", startedAt, { count: refs.length });
    } catch (error) {
      if (host) {
        host.innerHTML = renderError(error?.message || "Evidence refs failed to load.");
      }
      logTiming("evidence refs load", startedAt, { error: true });
    }
    return;
  }

  const refreshRouteButton = event.target.closest("[data-refresh-route]");
  if (refreshRouteButton) {
    event.preventDefault();
    await renderRoute({ backgroundRefresh: true, source: "refresh" });
    return;
  }

  const evidenceButton = event.target.closest("[data-evidence-payload]");
  if (evidenceButton) {
    event.preventDefault();
    openEvidenceDrawer(parseEvidencePayload(evidenceButton));
    return;
  }

  const artifactButton = event.target.closest("[data-artifact-path]");
  if (artifactButton) {
    event.preventDefault();
    await openArtifact(
      artifactButton.getAttribute("data-artifact-path"),
      artifactButton.getAttribute("data-artifact-title"),
    );
    return;
  }

  const paletteClose = event.target.closest("[data-command-palette-close]");
  if (paletteClose) {
    togglePalette(false);
  }
}

async function handleKeydown(event) {
  const manualForm = event.target.closest?.(".manual-capture-record-form");
  if (manualForm && event.key === "Enter" && !event.target.matches?.("textarea")) {
    const saveButton = manualForm.querySelector("[data-manual-capture-save-button]");
    if (!validateManualCaptureForm(manualForm, { showErrors: true }) || saveButton?.disabled) {
      event.preventDefault();
      return;
    }
    if (!event.target.matches?.("[data-manual-capture-save-button]")) {
      event.preventDefault();
      manualForm.requestSubmit?.(saveButton);
      return;
    }
  }
  if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
    event.preventDefault();
    togglePalette(true);
    return;
  }
  if (event.key === "Escape") {
    togglePalette(false);
    if (window.matchMedia("(max-width: 900px)").matches && state.sidebarMode === "expanded") {
      state.sidebarMode = "collapsed";
      localStorage.setItem("aegis.sidebar.mode", state.sidebarMode);
      renderNav();
    }
    return;
  }
  const exceptionCard = event.target.closest?.("[data-exception-card]");
  if (exceptionCard && (event.key === "Enter" || event.key === " ")) {
    event.preventDefault();
    await runExceptionAction(exceptionCard);
  }
}

function manualCaptureDraftPayload(form) {
  const payload = {};
  form.querySelectorAll("[data-manual-capture-draft-field]").forEach((field) => {
    const key = field.getAttribute("data-manual-capture-draft-field");
    if (!key || field.disabled) {
      return;
    }
    payload[key] = field.value || "";
  });
  payload.updated_at = new Date().toISOString();
  return payload;
}

function saveManualCaptureDraft(form) {
  const key = String(form?.dataset?.manualCaptureDraftKey || "").trim();
  if (!key) {
    return;
  }
  try {
    localStorage.setItem(key, JSON.stringify(manualCaptureDraftPayload(form)));
  } catch (_error) {
    // Draft preservation is best-effort and never evidence authority.
  }
}

function clearManualCaptureDraft(form) {
  const key = String(form?.dataset?.manualCaptureDraftKey || "").trim();
  if (!key) {
    return;
  }
  try {
    localStorage.removeItem(key);
  } catch (_error) {
    // Best-effort only.
  }
}

function clearManualCaptureFieldErrors(form) {
  form.querySelectorAll("[data-field-error-for]").forEach((node) => {
    node.textContent = node.getAttribute("data-field-error-for") === "fill_time" ? "Saved as UTC." : "";
    node.dataset.tone = "";
  });
  const summary = form.querySelector("[data-manual-capture-validation-summary]");
  if (summary) {
    summary.dataset.tone = "";
  }
}

function operatorManualCaptureMessage(error) {
  const raw = String(error?.payload?.human_message || error?.payload?.message || error?.message || "Manual capture could not be recorded.");
  const technicalPatterns = ["captured_manually", "capture_status", "ManualCaptureDomain", "lineage hash", "construction contract", "RuntimeEvaluation"];
  if (technicalPatterns.some((pattern) => raw.includes(pattern))) {
    if (raw.toLowerCase().includes("fill_time") || raw.toLowerCase().includes("fill time")) {
      return "Fill time is required.";
    }
    if (raw.toLowerCase().includes("refresh") || raw.toLowerCase().includes("lineage") || raw.toLowerCase().includes("runtime")) {
      return "This ticket changed since the page loaded. Refresh and try again.";
    }
    return "Manual capture could not be recorded. Refresh the ticket and try again.";
  }
  return raw;
}

function applyManualCaptureFieldErrors(form, fieldErrors = {}) {
  const entries = Object.entries(fieldErrors || {});
  entries.forEach(([field, message]) => {
    const node = form.querySelector(`[data-field-error-for="${field}"]`);
    if (node) {
      node.textContent = String(message || "Invalid value.");
      node.dataset.tone = "error";
    }
  });
  const summary = form.querySelector("[data-manual-capture-validation-summary]");
  if (summary && entries.length) {
    summary.textContent = entries.map(([, message]) => String(message)).join(" ");
    summary.dataset.tone = "error";
  }
}

function validateManualCaptureForm(form, { showErrors = false } = {}) {
  if (!form) {
    return false;
  }
  const data = new FormData(form);
  const fieldErrors = {};
  const rawFillTime = String(data.get("fill_time") || "").trim();
  const fillPrice = String(data.get("fill_price") || "").trim();
  const quantity = String(data.get("quantity") || "").trim();
  if (!rawFillTime) {
    fieldErrors.fill_time = "Fill time is required.";
  } else if (Number.isNaN(new Date(rawFillTime).getTime())) {
    fieldErrors.fill_time = "Fill time format is invalid.";
  }
  if (!fillPrice) {
    fieldErrors.fill_price = "Fill price is required.";
  } else if (!Number.isFinite(Number(fillPrice)) || Number(fillPrice) <= 0) {
    fieldErrors.fill_price = "Fill price must be positive.";
  }
  if (!quantity) {
    fieldErrors.quantity = "Quantity is required.";
  } else if (!Number.isFinite(Number(quantity)) || Number(quantity) <= 0) {
    fieldErrors.quantity = "Quantity must be positive.";
  }
  clearManualCaptureFieldErrors(form);
  if (showErrors && Object.keys(fieldErrors).length) {
    applyManualCaptureFieldErrors(form, fieldErrors);
  }
  const saveButton = form.querySelector("[data-manual-capture-save-button]");
  if (saveButton) {
    saveButton.disabled = Object.keys(fieldErrors).length > 0;
    saveButton.title = saveButton.disabled ? "Complete fill time, fill price, and quantity first." : "Record capture";
  }
  return Object.keys(fieldErrors).length === 0;
}

function refreshManualCaptureFormState(form) {
  validateManualCaptureForm(form, { showErrors: false });
}

function setManualCaptureSaving(form, saving) {
  form?.querySelectorAll?.("input, textarea, select, button").forEach((node) => {
    if (node.matches?.("[data-manual-capture-close]") && !saving) {
      node.disabled = false;
      return;
    }
    node.disabled = Boolean(saving);
  });
  form?.classList?.toggle("is-saving", Boolean(saving));
}

function applyResearchHypothesisFilters(root = document) {
  const pageRoot = root.closest?.("[data-research-page-root]") || document.querySelector("[data-research-page-root]") || document;
  const searchInput = pageRoot.querySelector("[data-research-hypothesis-search]");
  const filterRegion = pageRoot.querySelector("[data-research-hypothesis-search-region]");
  const rawQuery = String(searchInput?.value || "").trim();
  const query = rawQuery.toLowerCase();
  if (query && filterRegion) {
    filterRegion.dataset.activeFilter = "all";
    filterRegion.querySelectorAll("[data-research-quick-filter]").forEach((button) => {
      button.classList.toggle("is-active", button.getAttribute("data-research-quick-filter") === "all");
    });
  }
  const activeFilter = query ? "all" : String(filterRegion?.dataset?.activeFilter || "all").trim().toLowerCase() || "all";
  const inventoryRows = Array.from(pageRoot.querySelectorAll("[data-research-inventory-row]"));
  const filterTargets = inventoryRows.length ? inventoryRows : Array.from(pageRoot.querySelectorAll("[data-research-discovery-card]"));
  let visibleCount = 0;
  filterTargets.forEach((row) => {
    const text = String(row.getAttribute("data-research-search") || "").toLowerCase();
    const tokens = String(row.getAttribute("data-research-filter-tokens") || "all").toLowerCase().split(/\s+/);
    const matchQuery = !query || text.includes(query);
    const matchFilter = activeFilter === "all" || tokens.includes(activeFilter);
    const match = matchQuery && matchFilter;
    row.hidden = !match;
    if (match) visibleCount += 1;
  });
  const countNode = pageRoot.querySelector("[data-research-hypothesis-search-count]");
  if (countNode) {
    countNode.textContent = rawQuery ? `${visibleCount} results for ${rawQuery}` : `${visibleCount} hypotheses`;
  }
  const noResults = pageRoot.querySelector("[data-research-no-results]");
  if (noResults) {
    noResults.hidden = visibleCount !== 0;
  }
}

function handleInput(event) {
  const researchSearch = event.target.closest?.("[data-research-hypothesis-search]");
  if (researchSearch) {
    applyResearchHypothesisFilters(researchSearch);
    return;
  }

  const form = event.target.closest?.(".manual-capture-record-form");
  if (!form || !event.target.matches?.("[data-manual-capture-draft-field]")) {
    return;
  }
  saveManualCaptureDraft(form);
  validateManualCaptureForm(form, { showErrors: true });
}

function handleReset(event) {
  const form = event.target.closest?.(".manual-capture-record-form");
  if (!form) {
    return;
  }
  clearManualCaptureDraft(form);
  clearManualCaptureFieldErrors(form);
}

function handleCancel(event) {
  const paperEntryDialog = event.target.closest?.("dialog[data-paper-entry-dialog]");
  if (paperEntryDialog) {
    resetPaperTradeDialog(paperEntryDialog);
    return;
  }
  const modal = event.target.closest?.("[data-manual-capture-dialog]");
  if (!modal) {
    return;
  }
  if (manualCaptureHasUnsavedDraft(modal) && !window.confirm("Discard unsaved manual capture draft?")) {
    event.preventDefault();
    return;
  }
  document.body.classList.remove("manual-capture-modal-open");
}

async function handleSubmit(event) {


  const changeControlDecisionForm = event.target.closest(".change-control-decision-form");
  if (changeControlDecisionForm) {
    event.preventDefault();
    const formData = new FormData(changeControlDecisionForm);
    const statusNode = changeControlDecisionForm.querySelector("[data-change-control-decision-status]");
    const submitter = event.submitter || changeControlDecisionForm.querySelector('button[type="submit"]');
    const payload = Object.fromEntries(formData.entries());
    if (statusNode) {
      statusNode.textContent = "Recording decision...";
      statusNode.dataset.tone = "loading";
    }
    if (submitter) {
      submitter.disabled = true;
      submitter.setAttribute("aria-busy", "true");
    }
    try {
      const result = await recordAegisChangeControlDecision(payload);
      if (statusNode) {
        statusNode.textContent = result?.decision_record?.id ? `Decision recorded: ${result.decision_record.id}` : "Decision recorded.";
        statusNode.dataset.tone = "ready";
      }
      await renderRoute({ backgroundRefresh: false, source: "change-control-decision" });
    } catch (error) {
      if (statusNode) {
        statusNode.textContent = error?.payload?.message || error?.message || "Decision action failed.";
        statusNode.dataset.tone = "error";
      }
    } finally {
      if (submitter) {
        submitter.disabled = false;
        submitter.removeAttribute("aria-busy");
      }
    }
    return;
  }

  const askAegisForm = event.target.closest("[data-ask-aegis-form]");
  if (askAegisForm) {
    event.preventDefault();
    await submitAskAegisForm(askAegisForm);
    return;
  }

  const paperCandidateForm = event.target.closest(".paper-candidate-action-form");
  if (paperCandidateForm) {
    event.preventDefault();
    const formData = new FormData(paperCandidateForm);
    const commandId = String(formData.get("command_type") || formData.get("command_id") || "").trim();
    const candidateId = String(formData.get("candidate_id") || "").trim();
    const statusNode = paperCandidateForm.querySelector("[data-paper-candidate-status]");
    const submitter = event.submitter || paperCandidateForm.querySelector('button[type="submit"]');
    const dialog = paperCandidateForm.closest("dialog");
    if (statusNode) {
      statusNode.hidden = false;
      statusNode.textContent = "Recording simulated paper workflow...";
      statusNode.dataset.tone = "loading";
    }
    if (submitter) {
      submitter.dataset.originalLabel = submitter.dataset.originalLabel || String(submitter.textContent || "Record").trim();
      submitter.disabled = true;
      submitter.setAttribute("aria-busy", "true");
      submitter.innerHTML = `<span class="button-spinner" aria-hidden="true"></span>${escapeHtml(submitter.dataset.originalLabel)}`;
    }
    paperCandidateForm.dataset.paperSubmitting = "true";
    if (dialog?.matches?.("[data-paper-entry-dialog]")) dialog.dataset.paperSubmitting = "true";
    try {
      const payload = Object.fromEntries(formData.entries());
      let result;
      if (["APPROVE_CANDIDATE", "REJECT_CANDIDATE", "CONFIRM_CANDIDATE_CAPTURED", "MARK_CANDIDATE_NOT_CAPTURED", "DEFER_CANDIDATE", "CORRECT_CANDIDATE_CAPTURE", "RECORD_PAPER_ENTRY", "RECORD_PAPER_EXIT", "REVOKE_APPROVAL"].includes(commandId)) {
        result = await submitOperatorWorkflowCommand(submitter || paperCandidateForm, { ...payload, command_type: commandId }, candidateId);
        if (statusNode) {
          statusNode.textContent = `Command ${result?.command_id || ""}: RECEIVED`;
          statusNode.dataset.tone = result?.ok ? "ready" : "error";
        }
      } else {
        result = await executeAegisCommand({
          command_id: commandId,
          target_type: "paper_review_candidate",
          target_id: candidateId,
          operational_day: String(payload.day_utc || ""),
          payload,
        });
        if (statusNode) {
          statusNode.textContent = result?.user_message || result?.message || "Paper workflow updated.";
          statusNode.dataset.tone = result?.ok ? "ready" : "error";
        }
        showCommandResultPanel(submitter || paperCandidateForm, result);
      }
      if (result?.ok && dialog?.close) dialog.close();
    } catch (error) {
      if (statusNode) {
        statusNode.textContent = error?.payload?.user_message || error?.payload?.error_message || error?.message || "Paper workflow action failed.";
        statusNode.dataset.tone = "error";
      }
    } finally {
      paperCandidateForm.dataset.paperSubmitting = "false";
      if (dialog?.matches?.("[data-paper-entry-dialog]")) dialog.dataset.paperSubmitting = "false";
      if (submitter) {
        submitter.disabled = false;
        submitter.removeAttribute("aria-busy");
        if (submitter.dataset.originalLabel) submitter.textContent = submitter.dataset.originalLabel;
      }
    }
    return;
  }

  const advisorBenchmarkForm = event.target.closest(".advisor-benchmark-form");
  if (advisorBenchmarkForm) {
    event.preventDefault();
    const formData = new FormData(advisorBenchmarkForm);
    const payload = Object.fromEntries(formData.entries());
    const statusNode = advisorBenchmarkForm.querySelector("[data-advisor-benchmark-status]");
    const submitter = event.submitter || advisorBenchmarkForm.querySelector('button[type="submit"]');
    if (statusNode) {
      statusNode.hidden = false;
      statusNode.textContent = "Saving advisor benchmark...";
      statusNode.dataset.tone = "loading";
    }
    if (submitter) {
      submitter.disabled = true;
      submitter.setAttribute("aria-busy", "true");
    }
    try {
      const result = await saveAegisAdvisorBenchmark(payload);
      if (statusNode) {
        statusNode.textContent = result?.snapshot?.as_of_date ? `Saved advisor benchmark: ${result.snapshot.period_type} ${result.snapshot.return_pct}% as of ${result.snapshot.as_of_date}` : "Saved advisor benchmark.";
        statusNode.dataset.tone = "ready";
      }
      await renderRoute({ backgroundRefresh: true, source: "advisor-benchmark-save" });
    } catch (error) {
      const fieldErrors = error?.payload?.field_errors || {};
      const firstError = Object.values(fieldErrors)[0];
      if (statusNode) {
        statusNode.textContent = firstError || error?.payload?.message || error?.message || "Advisor benchmark save failed.";
        statusNode.dataset.tone = "error";
      }
    } finally {
      if (submitter) {
        submitter.disabled = false;
        submitter.removeAttribute("aria-busy");
      }
    }
    return;
  }

  const candidateActionForm = event.target.closest(".candidate-action-form");
  if (candidateActionForm) {
    event.preventDefault();
    const formData = new FormData(candidateActionForm);
    const statusNode = candidateActionForm.querySelector("[data-candidate-action-status]");
    if (statusNode) {
      statusNode.textContent = "Recording...";
      statusNode.dataset.tone = "loading";
    }
    try {
      const result = await executeCandidateWorkflow(formData, state);
      if (statusNode) {
        statusNode.textContent = result?.operator_statement || result?.message || "Operator action recorded.";
        statusNode.dataset.tone = "ready";
      }
      const dialog = candidateActionForm.closest("dialog");
      if (dialog?.close) {
        dialog.close();
      }
      await renderRoute();
    } catch (error) {
      if (statusNode) {
        statusNode.textContent = error?.payload?.message || error?.message || "Operator action failed.";
        statusNode.dataset.tone = "error";
      }
    }
    return;
  }


  const manualReceiptForm = event.target.closest(".manual-receipt-form");
  if (manualReceiptForm) {
    event.preventDefault();
    const formData = new FormData(manualReceiptForm);
    const payload = Object.fromEntries(formData.entries());
    payload.operator_attestation = formData.get("operator_attestation") === "true";
    const statusNode = manualReceiptForm.querySelector("[data-manual-receipt-status]");
    if (statusNode) {
      statusNode.hidden = false;
      statusNode.textContent = "Recording manual receipt...";
      statusNode.dataset.tone = "loading";
    }
    try {
      const result = await executeAegisCommand({
        command_id: "ADD_MANUAL_RECEIPT",
        target_type: "paper_trade",
        target_id: String(payload.target_id || payload.trade_id || ""),
        operational_day: String(payload.operational_day || ""),
        payload,
      });
      if (statusNode) {
        statusNode.textContent = result?.user_message || "Manual receipt recorded.";
        statusNode.dataset.tone = "ready";
      }
      showCommandResultPanel(manualReceiptForm.querySelector('button[type="submit"]') || manualReceiptForm, result);
      await renderRoute();
    } catch (error) {
      const message = error?.payload?.user_message || error?.payload?.error_message || error?.message || "Manual receipt failed.";
      if (statusNode) {
        statusNode.textContent = message;
        statusNode.dataset.tone = "error";
      }
    }
    return;
  }

  const manualCaptureRecordForm = event.target.closest(".manual-capture-record-form");
  if (manualCaptureRecordForm) {
    event.preventDefault();
    const formData = new FormData(manualCaptureRecordForm);
    if (event.submitter?.name) {
      formData.set(event.submitter.name, event.submitter.value || "");
    }
    saveManualCaptureDraft(manualCaptureRecordForm);
    if (!validateManualCaptureForm(manualCaptureRecordForm, { showErrors: true })) {
      return;
    }
    const statusNode = manualCaptureRecordForm.querySelector("[data-manual-capture-record-status]");
    if (statusNode) {
      statusNode.textContent = "Recording capture...";
      statusNode.dataset.tone = "loading";
    }
    setManualCaptureSaving(manualCaptureRecordForm, true);
    try {
      const result = await executeManualCaptureRecordWorkflow(formData, state);
      clearManualCaptureDraft(manualCaptureRecordForm);
      setManualCaptureSaving(manualCaptureRecordForm, false);
      renderManualCaptureSuccess(manualCaptureRecordForm, result);
      if (statusNode) {
        statusNode.textContent = result?.operator_statement || result?.message || "Manual capture recorded. No broker action was taken.";
        statusNode.dataset.tone = "ready";
      }
    } catch (error) {
      setManualCaptureSaving(manualCaptureRecordForm, false);
      applyManualCaptureFieldErrors(manualCaptureRecordForm, error?.fieldErrors || error?.payload?.field_errors || {});
      if (statusNode) {
        statusNode.textContent = operatorManualCaptureMessage(error);
        statusNode.dataset.tone = "error";
      }
      refreshManualCaptureFormState(manualCaptureRecordForm);
    }
    return;
  }

  const edgeLabActionForm = event.target.closest(".edge-lab-action-form");
  if (edgeLabActionForm) {
    event.preventDefault();
    const formData = new FormData(edgeLabActionForm);
    const statusNode = edgeLabActionForm.querySelector("[data-edge-lab-action-status]");
    if (statusNode) {
      statusNode.textContent = "Working...";
      statusNode.dataset.tone = "loading";
    }
    try {
      const result = await executeEdgeLabWorkflow(formData, state);
      if (statusNode) {
        statusNode.textContent = result?.message || "Edge Lab action completed.";
        statusNode.dataset.tone = "ready";
      }
      const dialog = edgeLabActionForm.closest("dialog");
      if (dialog?.close) {
        dialog.close();
      }
      await renderRoute();
    } catch (error) {
      if (statusNode) {
        statusNode.textContent = error?.payload?.message || error?.message || "Edge Lab action failed.";
        statusNode.dataset.tone = "error";
      }
    }
    return;
  }

  const researchDataAcquisitionForm = event.target.closest(".research-data-acquisition-form");
  if (researchDataAcquisitionForm) {
    event.preventDefault();
    const formData = new FormData(researchDataAcquisitionForm);
    const statusNode = researchDataAcquisitionForm.closest(".research-data-acquisition-plan")?.querySelector("[data-research-data-acquisition-status]");
    if (statusNode) {
      statusNode.textContent = "Queueing background refresh job...";
      statusNode.dataset.tone = "loading";
    }
    try {
      const result = await executeResearchDataAcquisitionWorkflow(formData, state);
      if (statusNode) {
        statusNode.textContent = result?.job_id ? `Background refresh queued: ${result.job_id} (${result.status || "QUEUED"})` : (result?.message || (result?.ok ? "Background refresh queued." : "Refresh job returned with warnings."));
        statusNode.dataset.tone = result?.ok === false ? "error" : "ready";
      }
      await renderRoute();
    } catch (error) {
      if (statusNode) {
        statusNode.textContent = error?.payload?.message || error?.message || "Market data fetch failed.";
        statusNode.dataset.tone = "error";
      }
    }
    return;
  }

  const researchConsoleForm = event.target.closest(".research-console-form");
  if (researchConsoleForm) {
    event.preventDefault();
    const formData = new FormData(researchConsoleForm);
    const submitter = event.submitter || researchConsoleForm.querySelector('button[type="submit"]');
    const statusNode = researchConsoleForm.querySelector("[data-research-console-status]") || document.querySelector("[data-research-console-status]");
    const actionLabel = submitter?.textContent?.trim() || "Research action";
    const progressText = researchConsoleProgressText(actionLabel);
    const card = researchConsoleForm.closest("[data-hypothesis-card]") || researchConsoleForm.closest("[data-research-proposal-card]") || researchConsoleForm.closest("tr");
    const buttons = Array.from(researchConsoleForm.querySelectorAll("button"));
    buttons.forEach((button) => {
      button.disabled = true;
      button.dataset.originalText = button.dataset.originalText || button.textContent || "";
    });
    if (submitter) {
      submitter.textContent = progressText;
    }
    card?.classList.add("research-action-pending");
    if (statusNode) {
      statusNode.textContent = progressText;
      statusNode.dataset.tone = "loading";
    }
    try {
      const result = await executeResearchConsoleWorkflow(formData, state);
      if (statusNode) {
        statusNode.textContent = result?.operator_message || result?.message || "Research Pipeline action completed.";
        statusNode.dataset.tone = "ready";
      }
      await renderRoute();
    } catch (error) {
      state.researchConsoleWorkflow = {
        lastAction: String(formData.get("research_action") || ""),
        lastResult: null,
        lastError: error?.payload || { message: error?.message || "Research Pipeline action failed." },
      };
      buttons.forEach((button) => {
        button.disabled = false;
        if (button.dataset.originalText) {
          button.textContent = button.dataset.originalText;
        }
      });
      card?.classList.remove("research-action-pending");
      if (statusNode) {
        statusNode.textContent = researchConsoleErrorText(error);
        statusNode.dataset.tone = "error";
      }
    }
    return;
  }

  const reliabilityFilterForm = event.target.closest(".reliability-filter-form");
  if (reliabilityFilterForm) {
    event.preventDefault();
    const formData = new FormData(reliabilityFilterForm);
    const search = new URLSearchParams();
    for (const [key, value] of formData.entries()) {
      const normalized = String(value || "").trim();
      if (!normalized) {
        continue;
      }
      search.set(key, normalized);
    }
    const basePath = String(reliabilityFilterForm.getAttribute("data-base-path") || window.location.pathname || "/reliability");
    const href = search.toString() ? `${basePath}?${search.toString()}` : basePath;
    await navigateTo(href);
    return;
  }

  const reliabilityActionForm = event.target.closest(".reliability-action-form");
  if (reliabilityActionForm) {
    event.preventDefault();
    const formData = new FormData(reliabilityActionForm);
    try {
      await executeReliabilityWorkflow(formData, state);
    } catch (error) {
      const submitted = Object.fromEntries([...formData.entries()].map(([key, value]) => [key, String(value || "")]));
      state.reliabilityWorkflow = {
        ...(state.reliabilityWorkflow || {}),
        lastAction: String(formData.get("reliability_action") || ""),
        lastFormInput: submitted,
        lastError: error?.payload?.errors?.join(", ")
          || error?.payload?.reason_codes?.join(", ")
          || error?.message
          || "Reliability action failed.",
      };
    }
    await renderRoute();
    return;
  }

  const configurationForm = event.target.closest(".configuration-workflow-form");
  if (configurationForm) {
    event.preventDefault();
    const formData = new FormData(configurationForm);
    try {
      await executeConfigurationWorkflow(formData, state);
    } catch (error) {
      state.configurationWorkflow = {
        ...(state.configurationWorkflow || {}),
        lastError: error?.payload?.reason_codes?.join(", ") || error?.message || "Configuration action failed.",
      };
    }
    await renderRoute();
    return;
  }

  const queryForm = event.target.closest(".operator-query-form");
  if (queryForm) {
    event.preventDefault();
    const input = queryForm.querySelector('input[name="query_text"]');
    try {
      await executeOperatorQuery(input?.value || "", state);
    } catch (error) {
      state.commandQueryResult = {
        query_response: {
          query_class_id: "operator_query_response_v1",
          response_status: "UNAVAILABLE",
          answer_blocks: [
            {
              block_id: "frontend_error",
              title: "Query Failed",
              lines: [error.message || "Operator query failed."],
              source_refs: [],
            },
          ],
        },
      };
    }
    await renderRoute();
  }
}

export async function bootOperatorShell() {
  const startedAt = performance.now();
  state.bootDiagnostics.bootStart = startedAt;
  markBootEvent("BOOT_START", { path: window.location.pathname });
  renderBrand();
  renderNav();
  renderTopBar();
  document.getElementById("workspaceContent").innerHTML = `<div class="page-loading">Loading workspace...</div>`;
  document.getElementById("contextRailContent").innerHTML = `<div class="empty-state">Evidence loads after the workspace summary is available.</div>`;
  logTiming("initial shell render", startedAt);
  markBootEvent("EVIDENCE_DRAWER_INIT", { mode: "idle" });
  if (document.body.dataset.aegisClickWired !== "true") {
    document.body.dataset.aegisClickWired = "true";
    document.body.addEventListener("click", (event) => {
      handleClick(event);
    });
  }
  try {
    markBootEvent("STATE_FETCH_START", { source: "shared_shell" });
    const sharedStateLoad = loadSharedShellState({ summaryOnly: false }).then(() => {
      updateBootDiagnostic("data_ready_ms", Math.round((performance.now() - startedAt) * 10) / 10);
      markBootEvent("STATE_FETCH_END", { source: "shared_shell" });
    });
    const routeRender = renderRoute({ source: "boot" });
    await Promise.all([sharedStateLoad, routeRender]);
  } catch (error) {
    console.error("[aegis-ui] shared shell load failed", error);
  }
  updateBootDiagnostic("boot_time_ms", Math.round((performance.now() - startedAt) * 10) / 10);

  document.getElementById("refreshButton")?.addEventListener("click", async () => {
    await loadSharedShellState({ summaryOnly: false });
    await renderRoute({ backgroundRefresh: true, source: "refresh" });
  });
  document.getElementById("commandPaletteButton")?.addEventListener("click", () => {
    togglePalette(true);
  });
  document.getElementById("commandPaletteInput").addEventListener("input", () => {
    renderPalette();
  });
  document.getElementById("sidebarSearch").addEventListener("input", () => {
    renderNav();
  });
  document.getElementById("drawerClose").addEventListener("click", () => {
    resetDrawer();
  });
  window.addEventListener("popstate", () => {
    renderRoute({ source: "popstate" });
  });
  window.addEventListener("resize", () => {
    renderNav();
  });
  window.addEventListener("aegis:connection-state", (event) => {
    state.connection = {
      ...state.connection,
      ...(event.detail || window.__AEGIS_CONNECTION_STATE || {}),
    };
    renderTopBar();
  });
  window.setInterval(() => {
    if (currentRoute().id === "aegis_runtime" && state.connection?.state && state.connection.state !== "CONNECTED") {
      renderRoute({ backgroundRefresh: true, source: "refresh" });
    }
  }, 5000);
  markBootEvent("REFRESH_TIMERS_ARMED", { runtime_retry_ms: 5000 });
  window.addEventListener("keydown", (event) => {
    handleKeydown(event);
  });
  document.body.addEventListener("input", (event) => {
    handleInput(event);
  });
  document.body.addEventListener("change", (event) => {
    handleInput(event);
  });
  document.body.addEventListener("reset", (event) => {
    handleReset(event);
  });
  document.body.addEventListener("cancel", (event) => {
    handleCancel(event);
  });
  document.body.addEventListener("submit", (event) => {
    handleSubmit(event);
  });
}
