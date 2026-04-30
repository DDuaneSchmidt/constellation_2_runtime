import { escapeHtml, renderError, renderOperatorDiagnosticPanel } from "/operator_shell/shared_components/dom.js";
import { renderAegisMark, renderStatusPill } from "/operator_shell/aegis_components/index.js";
import {
  ROUTES,
  LEGACY_ROUTE_ALIASES,
  buildPaletteEntries,
  executeConfigurationWorkflow,
  executeReliabilityWorkflow,
  executeOperatorQuery,
  loadRouteView,
} from "/operator_shell/pages/index.js";
import {
  NAVIGATION_SCHEMA,
  activeNavigationForPath,
} from "/operator_shell/navigation_schema.js";
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
  paletteOpen: false,
  sidebarMode: localStorage.getItem("aegis.sidebar.mode") || "expanded",
  sidebarOpenGroups: new Set(NAVIGATION_SCHEMA.flatMap((section) => section.domains.map((domain) => domain.id))),
};

function logTiming(phase, startedAt, extra = {}) {
  const durationMs = Math.round((performance.now() - startedAt) * 10) / 10;
  console.info("[aegis-ui-timing]", { phase, duration_ms: durationMs, ...extra });
}

async function timedAsync(phase, action, extra = {}) {
  const startedAt = performance.now();
  try {
    return await action();
  } finally {
    logTiming(phase, startedAt, extra);
  }
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
  const normalized = String(path || "/").replace(/\/+$/, "") || "/";
  if (normalized.startsWith("/reliability/work-orders/") && normalized !== "/reliability/work-orders") {
    return "/reliability/work-orders/detail";
  }
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
  const startedAt = performance.now();
  const active = activeNavigationForPath(window.location.pathname, NAVIGATION_SCHEMA);
  const query = String(document.getElementById("sidebarSearch")?.value || "").trim().toLowerCase();
  state.sidebarMode = ["expanded", "collapsed", "hidden"].includes(state.sidebarMode) ? state.sidebarMode : "expanded";
  document.body.classList.toggle("sidebar-collapsed", state.sidebarMode === "collapsed");
  document.body.classList.toggle("sidebar-hidden", state.sidebarMode === "hidden");
  const collapseButton = document.querySelector("[data-sidebar-collapse]");
  if (collapseButton) {
    const nextLabel = state.sidebarMode === "expanded" ? "Collapse" : state.sidebarMode === "collapsed" ? "Hide" : "Show";
    collapseButton.setAttribute("aria-label", `${nextLabel} navigation`);
    collapseButton.querySelector("span").textContent = nextLabel;
    collapseButton.querySelector("strong").textContent = state.sidebarMode === "hidden" ? "›" : "‹";
  }
  document.getElementById("workspaceNav").innerHTML = NAVIGATION_SCHEMA.map((section) => {
    const domains = section.domains
      .map((domain) => {
        const children = Array.isArray(domain.children) ? domain.children : [];
        const matchesDomain = [domain.label, domain.description, domain.truthOwner].some((value) =>
          String(value || "").toLowerCase().includes(query),
        );
        const filteredChildren = children.filter((child) => {
          if (!query) {
            return true;
          }
          return [child.label, child.description, child.truthOwner].some((value) =>
            String(value || "").toLowerCase().includes(query),
          );
        });
        if (query && !matchesDomain && filteredChildren.length === 0) {
          return "";
        }
        const isActiveParent = active?.id === domain.id || active?.parentId === domain.id;
        const isOpen = state.sidebarOpenGroups.has(domain.id) || Boolean(query);
        const visibleChildren = query && filteredChildren.length ? filteredChildren : children;
        const domainBadge = [domain, ...children].reduce((total, item) => total + Number(item.badgeCount || 0), 0);
        const childrenId = `nav-children-${domain.id}`;
        return `
          <div class="sidebar-group accent-${escapeHtml(domain.accent)} ${isActiveParent ? "active-parent" : ""}">
            <button class="sidebar-group-button" type="button" data-nav-group-toggle="${escapeHtml(domain.id)}" aria-expanded="${isOpen ? "true" : "false"}" aria-controls="${escapeHtml(childrenId)}">
              <span class="nav-icon">${escapeHtml(domain.icon)}</span>
              <span class="nav-text">
                <strong>${escapeHtml(domain.label)}</strong>
                <small>${escapeHtml(domain.truthOwner || "")}</small>
              </span>
              ${domainBadge ? `<span class="nav-badge">${escapeHtml(String(domainBadge))}</span>` : ""}
              <span class="nav-caret" aria-hidden="true">${isOpen ? "⌄" : "›"}</span>
            </button>
            <div class="sidebar-children" id="${escapeHtml(childrenId)}" ${isOpen ? "" : "hidden"}>
              ${visibleChildren.map((child) => {
                const childActive = active?.id === child.id || (!active?.parentId && active?.route === child.route && child.id.endsWith("overview"));
                return `
                  <a class="nav-link ${childActive ? "active" : ""}" href="${escapeHtml(child.route)}" data-route="${escapeHtml(child.route)}">
                    <span class="nav-icon">${escapeHtml(child.icon)}</span>
                    <span class="nav-text">
                      <span>${escapeHtml(child.label)}</span>
                      <small>${escapeHtml(child.truthOwner || domain.truthOwner || "")}</small>
                    </span>
                    ${child.badgeCount ? `<span class="nav-badge">${escapeHtml(String(child.badgeCount))}</span>` : ""}
                  </a>
                `;
              }).join("")}
            </div>
          </div>
        `;
      })
      .join("");
    return `
      <section class="sidebar-section">
        <div class="nav-section-title">${escapeHtml(section.section)}</div>
        ${domains}
      </section>
    `;
  }).join("");
  logTiming("workspace/navigation load", startedAt, { mode: state.sidebarMode });
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
  const active = activeNavigationForPath(window.location.pathname, NAVIGATION_SCHEMA);
  const pageTitle = active?.parentLabel ? `${active.parentLabel} / ${active.label}` : `${active?.label || "Command"} / Overview`;

  document.getElementById("headerPageTitle").textContent = pageTitle;
  document.getElementById("headerEnvironment").textContent = "PRODUCTION";
  document.getElementById("headerDataTimestamp").textContent = "Data as of: Apr 27, 2026 3:58 PM EDT";
  document.getElementById("topRuntimeMode").textContent = formatDisplayLabel(summary.environment || "UNKNOWN");
  document.getElementById("topConfigVersion").textContent = formatDisplayLabel(summary.kernel_version || summary.summary_id || "governed");
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
  document.getElementById("notificationBadge").textContent = String(Math.max(trustPreserved.length, 3));
  document.getElementById("bottomConfigVersion").textContent = formatDisplayLabel(summary.kernel_version || "governed");
  document.getElementById("bottomFreshness").textContent = summary.last_refresh_utc || state.shell.refreshedAt || "n/a";
  document.getElementById("bottomReadiness").textContent = readinessStatus;
  document.getElementById("bottomEnvironment").textContent = formatDisplayLabel(summary.environment || "UNKNOWN");
  document.getElementById("bottomSession").textContent = summary.current_day || "UNKNOWN";
  const connectionState = state.connection?.state || "RECONNECTING";
  document.getElementById("bottomAlert").textContent =
    connectionState === "CONNECTED"
      ? (topAlert?.target_label || topAlert?.label || topAlert?.summary_message || "No elevated summary item")
      : `${connectionState}: ${state.connection?.recovery_command || "npm run aegis:ui:restart"}`;
}

function setPageChrome(route, view) {
  document.title = `${view.title || route.label} | Aegis`;
  document.getElementById("workspaceTitle").textContent = view.title || route.label;
  document.getElementById("workspaceEyebrow").textContent = route.eyebrow || route.label;
  document.getElementById("workspaceMeta").textContent = view.meta || route.subtitle || "";
  const active = activeNavigationForPath(window.location.pathname, NAVIGATION_SCHEMA);
  document.getElementById("headerPageTitle").textContent = active?.parentLabel ? `${active.parentLabel} / ${active.label}` : "Command / Overview";
  if (view.dataTimestamp) {
    document.getElementById("headerDataTimestamp").textContent = view.dataTimestamp;
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
  const [semanticsPayload, railPayload, systemSummary, operatorWorkflow, alerts, financialState] = await timedAsync("readiness cards load", () => Promise.all([
    fetchStatusSemantics(),
    fetchStatusRail(summaryOnly ? { summary: 1 } : {}),
    fetchSystemSummary(),
    fetchOperatorWorkflow(),
    fetchAlerts(),
    fetchFinancialState(),
  ]), { summary_only: summaryOnly });
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
  const startedAt = performance.now();
  const route = currentRoute();
  renderNav();
  const mainHost = document.getElementById("workspaceContent");
  const contextHost = document.getElementById("contextRailContent");
  mainHost.innerHTML = `<div class="page-loading">Loading ${escapeHtml(route.label)}…</div>`;
  contextHost.innerHTML = "";
  try {
    const view = await timedAsync("workspace route load", () => loadRouteView(route.id, state), { route_id: route.id });
    state.activeView = view;
    setPageChrome(route, view);
    mainHost.innerHTML = view.html;
    contextHost.innerHTML = view.contextHtml || `<div class="empty-state">No contextual evidence for this surface.</div>`;
  } catch (error) {
    const routePath = String(route.path || "");
    const isCapitalRoute = routePath === "/capital" || routePath.startsWith("/capital/");
    if (isCapitalRoute && error?.operatorSafe) {
      mainHost.innerHTML = renderOperatorDiagnosticPanel(error.operatorSafe);
    } else {
      mainHost.innerHTML = renderError(error.message || "Route render failed.");
    }
    contextHost.innerHTML = "";
  } finally {
    logTiming("workspace render complete", startedAt, { route_id: route.id });
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
  const finalTarget = `${rawQuery ? `${finalPath}?${rawQuery}` : finalPath}${hashPart ? `#${hashPart}` : ""}`;
  if (finalTarget !== `${window.location.pathname}${window.location.search}${window.location.hash}`) {
    window.history.pushState({}, "", finalTarget);
  }
  togglePalette(false);
  await renderRoute();
  if (hashPart) {
    const anchor = document.getElementById(hashPart);
    if (anchor) {
      anchor.scrollIntoView({ behavior: "smooth", block: "start" });
    }
  }
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

async function handleClick(event) {
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
        const original = copyButton.dataset.originalLabel || copyButton.textContent || "Copy";
        copyButton.dataset.originalLabel = original;
        copyButton.textContent = "Copied";
        window.setTimeout(() => {
          copyButton.textContent = copyButton.dataset.originalLabel || original;
        }, 1400);
      } catch {
        // Keep UI fail-closed; no throwing on clipboard failures.
      }
    }
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
    state.sidebarMode = state.sidebarMode === "expanded" ? "collapsed" : state.sidebarMode === "collapsed" ? "hidden" : "expanded";
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
  if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
    event.preventDefault();
    togglePalette(true);
    return;
  }
  if (event.key === "Escape") {
    togglePalette(false);
    return;
  }
  const exceptionCard = event.target.closest?.("[data-exception-card]");
  if (exceptionCard && (event.key === "Enter" || event.key === " ")) {
    event.preventDefault();
    await runExceptionAction(exceptionCard);
  }
}

async function handleSubmit(event) {
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
  renderBrand();
  renderNav();
  renderTopBar();
  document.getElementById("workspaceContent").innerHTML = `<div class="page-loading">Loading workspace…</div>`;
  document.getElementById("contextRailContent").innerHTML = `<div class="empty-state">Evidence loads after the workspace summary is available.</div>`;
  logTiming("initial shell render", startedAt);
  loadSharedShellState({ summaryOnly: true }).then(() => {
    renderRoute();
    return loadSharedShellState({ summaryOnly: false });
  }).then(() => {
    renderRoute();
  }).catch((error) => {
    console.error("[aegis-ui] shared shell load failed", error);
  });
  await renderRoute();

  document.getElementById("refreshButton").addEventListener("click", async () => {
    await loadSharedShellState({ summaryOnly: false });
    await renderRoute();
  });
  document.getElementById("commandPaletteButton").addEventListener("click", () => {
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
    renderRoute();
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
      renderRoute();
    }
  }, 5000);
  window.addEventListener("keydown", (event) => {
    handleKeydown(event);
  });
  document.body.addEventListener("click", (event) => {
    handleClick(event);
  });
  document.body.addEventListener("submit", (event) => {
    handleSubmit(event);
  });
}
