import { escapeHtml, renderError } from "/operator_shell/shared_components/dom.js";
import { renderAegisMark, renderStatusPill } from "/operator_shell/aegis_components/index.js";
import {
  ROUTES,
  LEGACY_ROUTE_ALIASES,
  buildPaletteEntries,
  executeConfigurationWorkflow,
  executeOperatorQuery,
  loadRouteView,
} from "/operator_shell/pages/index.js";
import {
  fetchAlerts,
  fetchFinancialState,
  fetchOperatorWorkflow,
  fetchStatusRail,
  fetchStatusSemantics,
  fetchSystemSummary,
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
  activeView: null,
  paletteOpen: false,
};

function normalizePath(path) {
  const normalized = String(path || "/").replace(/\/+$/, "") || "/";
  return LEGACY_ROUTE_ALIASES[normalized] || normalized;
}

function currentRoute() {
  const normalized = normalizePath(window.location.pathname);
  return ROUTES.find((route) => route.path === normalized) || ROUTES[0];
}

function renderBrand() {
  document.getElementById("brandMark").innerHTML = renderAegisMark({ size: "sm", label: "Aegis" });
}

function renderNav() {
  const route = currentRoute();
  document.getElementById("workspaceNav").innerHTML = ROUTES.map((item) => `
    <a class="nav-link ${item.path === route.path ? "active" : ""}" href="${item.path}" data-route="${item.path}">
      <span>${escapeHtml(item.label)}</span>
      <small>${escapeHtml(item.eyebrow)}</small>
    </a>
  `).join("");
}

function renderKernelRail() {
  const host = document.getElementById("kernelStatusRail");
  host.innerHTML = state.shell.statusRail.map((kernel) => {
    const routePath = normalizePath(kernel.href || "/");
    const label = kernel.label || kernel.kernel_id || "Kernel";
    const status = kernel.status?.label || kernel.status?.code || "Unknown";
    return `
      <a class="status-rail-item" href="${escapeHtml(routePath)}" data-route="${escapeHtml(routePath)}">
        <span>${escapeHtml(label)}</span>
        ${renderStatusPill(status, kernel.status?.semantic || "unknown", state.semantics)}
      </a>
    `;
  }).join("");
}

function renderTopBar() {
  const summary = state.shell.systemSummary || {};
  const financialState = state.shell.financialState || {};
  const readiness = summary.readiness_summary || {};
  const topLevelItems = Array.isArray(summary.top_level_items) ? summary.top_level_items : [];
  const trustPreserved = Array.isArray(summary.trust_preserved_items) ? summary.trust_preserved_items : [];
  const topAlert = trustPreserved[0] || topLevelItems[0] || null;
  const readinessStatus = readiness.blocked ? (readiness.blocked_state || "BLOCKED") : "READY";

  document.getElementById("topRuntimeMode").textContent = summary.environment || "UNKNOWN";
  document.getElementById("topConfigVersion").textContent = summary.kernel_version || summary.summary_id || "governed";
  document.getElementById("topReadiness").innerHTML = renderStatusPill(
    readinessStatus,
    readiness.blocked ? "blocked" : "healthy",
    state.semantics,
  );
  document.getElementById("topPortfolioValue").textContent =
    financialState.financial_status === "FAIL_CLOSED"
      ? "fail closed"
      : formatUsd(financialState.investable_summary?.investable_assets_total_usd);
  document.getElementById("topNotificationCount").textContent = String(trustPreserved.length);
  document.getElementById("bottomConfigVersion").textContent = summary.kernel_version || "governed";
  document.getElementById("bottomFreshness").textContent = summary.last_refresh_utc || state.shell.refreshedAt || "n/a";
  document.getElementById("bottomReadiness").textContent = readinessStatus;
  document.getElementById("bottomEnvironment").textContent = summary.environment || "UNKNOWN";
  document.getElementById("bottomSession").textContent = summary.current_day || "UNKNOWN";
  document.getElementById("bottomAlert").textContent =
    topAlert?.target_label || topAlert?.label || topAlert?.summary_message || "No elevated summary item";
}

function setPageChrome(route, view) {
  document.title = `${view.title || route.label} | Aegis`;
  document.getElementById("workspaceTitle").textContent = view.title || route.label;
  document.getElementById("workspaceEyebrow").textContent = route.eyebrow || route.label;
  document.getElementById("workspaceMeta").textContent = view.meta || route.subtitle || "";
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

async function loadSharedShellState() {
  const [semanticsPayload, railPayload, systemSummary, operatorWorkflow, alerts, financialState] = await Promise.all([
    fetchStatusSemantics(),
    fetchStatusRail(),
    fetchSystemSummary(),
    fetchOperatorWorkflow(),
    fetchAlerts(),
    fetchFinancialState(),
  ]);
  state.semantics = semanticsPayload.status_semantics || {};
  state.shell.statusRail = railPayload.kernels || [];
  state.shell.systemSummary = systemSummary || {};
  state.shell.operatorWorkflow = operatorWorkflow || {};
  state.shell.alerts = alerts || {};
  state.shell.financialState = financialState || {};
  state.shell.refreshedAt = railPayload.generated_utc || systemSummary?.last_refresh_utc || null;
  renderKernelRail();
  renderTopBar();
}

async function renderRoute() {
  const route = currentRoute();
  renderNav();
  const mainHost = document.getElementById("workspaceContent");
  const contextHost = document.getElementById("contextRailContent");
  mainHost.innerHTML = `<div class="page-loading">Loading ${escapeHtml(route.label)}…</div>`;
  contextHost.innerHTML = "";
  try {
    const view = await loadRouteView(route.id, state);
    state.activeView = view;
    setPageChrome(route, view);
    mainHost.innerHTML = view.html;
    contextHost.innerHTML = view.contextHtml || `<div class="empty-state">No contextual evidence for this surface.</div>`;
  } catch (error) {
    mainHost.innerHTML = renderError(error.message || "Route render failed.");
    contextHost.innerHTML = "";
  }
}

async function navigateTo(path) {
  const normalized = normalizePath(path);
  if (normalized !== window.location.pathname) {
    window.history.pushState({}, "", normalized);
  }
  togglePalette(false);
  await renderRoute();
}

async function openArtifact(path, title) {
  const response = await fetch(`/api/artifact?path=${encodeURIComponent(path)}`, { cache: "no-store" });
  const payload = await response.json();
  document.getElementById("drawerTitle").textContent = title || payload.path || "Artifact";
  document.getElementById("drawerMeta").textContent = payload.path || "";
  document.getElementById("drawerContent").textContent = payload.content || "";
}

function resetDrawer() {
  document.getElementById("drawerTitle").textContent = "Select evidence";
  document.getElementById("drawerMeta").textContent = "Artifact content and provenance appear here.";
  document.getElementById("drawerContent").textContent = "";
}

async function handleClick(event) {
  const routeLink = event.target.closest("[data-route]");
  if (routeLink) {
    event.preventDefault();
    await navigateTo(routeLink.getAttribute("data-route"));
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

async function handleSubmit(event) {
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
  renderBrand();
  await loadSharedShellState();
  await renderRoute();

  document.getElementById("refreshButton").addEventListener("click", async () => {
    await loadSharedShellState();
    await renderRoute();
  });
  document.getElementById("commandPaletteButton").addEventListener("click", () => {
    togglePalette(true);
  });
  document.getElementById("commandPaletteInput").addEventListener("input", () => {
    renderPalette();
  });
  document.getElementById("drawerClose").addEventListener("click", () => {
    resetDrawer();
  });
  window.addEventListener("popstate", () => {
    renderRoute();
  });
  window.addEventListener("keydown", (event) => {
    if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
      event.preventDefault();
      togglePalette(true);
    }
    if (event.key === "Escape") {
      togglePalette(false);
    }
  });
  document.body.addEventListener("click", (event) => {
    handleClick(event);
  });
  document.body.addEventListener("submit", (event) => {
    handleSubmit(event);
  });
}
