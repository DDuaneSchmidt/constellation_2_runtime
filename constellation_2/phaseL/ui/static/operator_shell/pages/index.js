import {
  activateConfigurationDraft,
  createConfigurationDraft,
  fetchActionAudit,
  fetchActivityToday,
  fetchAegisLiteExecutionQueue,
  fetchAegisEventMonitoring,
  fetchAegisOperatorState,
  fetchRuntimeStatus,
  fetchAdvisory,
  fetchAlerts,
  fetchCapitalAccounts,
  fetchCapitalAllocation,
  fetchCapitalCashflow,
  fetchCapitalFlows,
  fetchCapitalHistory,
  fetchCapitalOverview,
  fetchCapitalValidation,
  fetchCommandOverview,
  fetchConfigurationCatalog,
  fetchConfigurationCurrent,
  fetchConfigurationDraft,
  fetchFinancialState,
  fetchPolicyEvolution,
  fetchRefinement,
  fetchIntegrity,
  fetchOutcomes,
  fetchStatusV2,
  fetchValue,
  fetchOperations,
  fetchOpportunities,
  fetchOperatorHome,
  fetchOperatorQuery,
  fetchReadinessKernel,
  fetchReconciliation,
  fetchSleeves,
  fetchTax,
  fetchSystemActions,
  fetchLatestReliabilityReadiness,
  fetchReliabilityNextActions,
  fetchReliabilityIssue,
  fetchReliabilityIssueVerifications,
  fetchReliabilityIssueWorkOrders,
  fetchReliabilityIssues,
  fetchReliabilityObservations,
  fetchReliabilityVerifications,
  fetchReliabilityWorkOrder,
  fetchReliabilityWorkOrders,
  fetchReliabilityFixAttempts,
  assessReliabilityReadiness,
  createReliabilityIssue,
  createWorkOrderFromIssue,
  draftReliabilityIssue,
  linkReliabilityObservation,
  recordReliabilityFixAttempt,
  updateReliabilityIssue,
  verifyReliabilityIssue,
  rejectConfigurationDraft,
  reviewConfigurationDraft,
  validateConfigurationDraft,
} from "/operator_shell/domain_client/index.js";
import { escapeHtml, renderEvidenceRefs } from "/operator_shell/shared_components/dom.js";
import { COMMAND_OVERVIEW_MOCK } from "/operator_shell/fixtures/mockData.js";
import {
  ContextRail,
  renderCommandOverview,
} from "/operator_shell/components/command_overview.js";
import {
  formatTimestamp,
  renderAegisMark,
  renderCardSection,
  renderDefinitionRows,
  renderGapState,
  renderList,
  renderMetricCard,
  renderPanelRows,
  renderSectionHeader,
  renderSemanticBadge,
  renderSimpleTable,
  renderSourceRefCard,
  renderStatusPill,
  renderTrustPanel,
  renderWorkflowStep,
} from "/operator_shell/aegis_components/index.js";

export const ROUTES = [
  {
    path: "/",
    id: "command",
    label: "Command",
    eyebrow: "Aegis Command",
    subtitle: "One governed entry point over readiness, trust, advisory guidance, and operator action.",
  },
  {
    path: "/capital",
    id: "capital_overview",
    label: "Capital",
    eyebrow: "Capital Overview",
    subtitle: "Household allocation truth over append-only snapshots, flows, and effective-dated classifications.",
  },
  {
    path: "/capital/accounts",
    id: "capital_accounts",
    label: "Capital Accounts",
    eyebrow: "Account Registry",
    subtitle: "Current account semantics, latest balances, include/exclude state, and traceable as-of basis.",
  },
  {
    path: "/capital/allocation",
    id: "capital_allocation",
    label: "Capital Allocation",
    eyebrow: "Allocation Lens",
    subtitle: "Control and bucket allocation with matrix breakdown and explainable denominator/numerator basis.",
  },
  {
    path: "/capital/history",
    id: "capital_history",
    label: "Capital History",
    eyebrow: "History",
    subtitle: "Investable total and Aegis allocation percentage over time from append-only snapshots.",
  },
  {
    path: "/capital/flows",
    id: "capital_flows",
    label: "Capital Flows",
    eyebrow: "Cash Flows",
    subtitle: "Append-only external contribution and withdrawal history with monthly summary rollups.",
  },
  {
    path: "/capital/cashflow",
    id: "capital_cashflow",
    label: "Capital Cashflow",
    eyebrow: "Cashflow Timeline",
    subtitle: "Deterministic monthly cashflow projection with scenario toggles and explicit basis disclosures.",
  },
  {
    path: "/capital/validation",
    id: "capital_validation",
    label: "Capital Validation",
    eyebrow: "Data Integrity",
    subtitle: "Validation findings, severity, impacted accounts, and explicit degraded-state diagnostics.",
  },
  {
    path: "/portfolio",
    id: "portfolio",
    label: "Portfolio",
    eyebrow: "Financial State",
    subtitle: "Positions and portfolio-operating facts from canonical runtime projections.",
  },
  {
    path: "/performance",
    id: "performance_cockpit",
    label: "Performance",
    eyebrow: "Performance Cockpit",
    subtitle: "Standalone Aegis performance cockpit rendered from generated read-only report HTML.",
  },
  {
    path: "/sleeves",
    id: "sleeves",
    label: "Sleeves",
    eyebrow: "Sleeve Evaluation",
    subtitle: "Sleeve proof belongs here once a canonical UI read model exists.",
  },
  {
    path: "/opportunities",
    id: "opportunities",
    label: "Opportunities",
    eyebrow: "Review Plane",
    subtitle: "Governed proactive opportunities, blocked review items, and changed-since-last-review summaries.",
  },
  {
    path: "/advisory",
    id: "advisory",
    label: "Advisory",
    eyebrow: "Recommendation Surface",
    subtitle: "Recommendation packets, operational availability, and household-facing advisory context.",
  },
  {
    path: "/tax",
    id: "tax",
    label: "Tax",
    eyebrow: "Tax Awareness",
    subtitle: "Tax readiness, account tax context, and explicit backend gaps from a canonical tax-state projection.",
  },
  {
    path: "/outcomes",
    id: "outcomes",
    label: "Value",
    eyebrow: "Value Proof",
    subtitle: "Governed realized value, sleeve linkage, bounded attribution, and explicit claim strength over already-certified truth.",
  },
  {
    path: "/refinement",
    id: "refinement",
    label: "Refinement",
    eyebrow: "Refinement Proof",
    subtitle: "Governed simplification, demotion, compression, reversibility, and before/after provenance over product truth.",
  },
  {
    path: "/policy",
    id: "policy",
    label: "Policy",
    eyebrow: "Policy Evolution",
    subtitle: "Governed temporal policy proposals over refinement and product emphasis, with explicit expiry, rollback, and trust overrides.",
  },
  {
    path: "/operations",
    id: "operations",
    label: "Operations",
    eyebrow: "Readiness & Runtime",
    subtitle: "Readiness ladders, runtime blocks, alerts, and governed operator actions.",
  },
  {
    path: "/aegis-runtime",
    id: "aegis_runtime",
    label: "Aegis Runtime",
    eyebrow: "Aegis Runtime",
    subtitle: "Read-only operator state derived from the latest immutable scan artifacts.",
  },
  {
    path: "/aegis-lite",
    id: "aegis_lite_queue",
    label: "Aegis Lite Queue",
    eyebrow: "Aegis Lite Execution Queue",
    subtitle: "Manual-only EOD queue from current Lite report truth.",
  },
  {
    path: "/aegis-events",
    id: "aegis_events",
    label: "Event Monitoring",
    eyebrow: "Aegis Event Monitoring",
    subtitle: "Read-only event rules, monitor status, event ledger, tactical packets, and alert gate evidence.",
  },
  {
    path: "/configuration",
    id: "configuration",
    label: "Configuration",
    eyebrow: "Control Plane",
    subtitle: "Governed draft/validate/review/activate workflow for safe authority-input configuration changes.",
  },
  {
    path: "/reliability",
    id: "reliability_dashboard",
    label: "Reliability",
    eyebrow: "Reliability Ledger",
    subtitle: "Deterministic readiness scoring and blockers for live-trading trust decisions.",
  },
  {
    path: "/reliability/issues",
    id: "reliability_issues",
    label: "Reliability Issues",
    eyebrow: "Issues",
    subtitle: "Structured issue ledger with recurrence, blocker state, and Codex workflow status.",
  },
  {
    path: "/reliability/issues/detail",
    id: "reliability_issue_detail",
    label: "Issue Detail",
    eyebrow: "Issue Detail",
    subtitle: "Single-issue evidence, recurrence timeline, codex task contract, and verification details.",
  },
  {
    path: "/reliability/observations",
    id: "reliability_observations",
    label: "Observations",
    eyebrow: "Observations Ledger",
    subtitle: "Append-only reliability observations with source/environment filters.",
  },
  {
    path: "/reliability/work-orders",
    id: "reliability_work_orders",
    label: "Work Orders",
    eyebrow: "Codex Queue",
    subtitle: "Actionable reliability repair requests with queue and assignment state.",
  },
  {
    path: "/reliability/work-orders/detail",
    id: "reliability_work_order_detail",
    label: "Work Order Detail",
    eyebrow: "Work Order",
    subtitle: "Work-order objective, constraints, codex prompt, and fix-attempt ledger.",
  },
  {
    path: "/reliability/verifications",
    id: "reliability_verifications",
    label: "Verifications",
    eyebrow: "Verification Ledger",
    subtitle: "Verification evidence and status across fix attempts.",
  },
  {
    path: "/reliability/ai",
    id: "reliability_ai",
    label: "AI Draft",
    eyebrow: "AI Draft Issue",
    subtitle: "Operator-assisted drafting and optional issue creation from incident descriptions.",
  },
  {
    path: "/audit",
    id: "audit",
    label: "Audit",
    eyebrow: "Historical Memory",
    subtitle: "Operator action logs, activity rollups, and trust/retrieval lineage.",
  },
  {
    path: "/reports",
    id: "reports",
    label: "Reports",
    eyebrow: "Immutable Snapshots",
    subtitle: "Presentation-quality snapshot surfaces rendered from governed report artifacts.",
  },
];

export const LEGACY_ROUTE_ALIASES = {
  "/control": "/operations",
  "/state": "/reports",
  "/submission": "/operations",
  "/lifecycle": "/audit",
  "/reliability/readiness": "/reliability",
  "/reliability/ai-draft": "/reliability/ai",
};

const TARGET_SURFACE_ROUTE = {
  operations: "/operations",
  alerts: "/operations",
  reconciliation: "/reports",
  positions: "/portfolio",
  orders: "/portfolio",
  admin: "/audit",
  advisory: "/advisory",
  reports: "/reports",
  outcomes: "/outcomes",
  reliability: "/reliability",
};

function routeForId(routeId) {
  return ROUTES.find((route) => route.id === routeId);
}

function routeHrefFromSurface(targetSurface) {
  if (!targetSurface) {
    return "/";
  }
  if (targetSurface.startsWith("/")) {
    return targetSurface;
  }
  return TARGET_SURFACE_ROUTE[targetSurface] || "/";
}

function safeList(value) {
  return Array.isArray(value) ? value : [];
}

function currentSearchParams() {
  if (typeof window === "undefined") {
    return new URLSearchParams();
  }
  return new URLSearchParams(window.location.search || "");
}

function currentPathname() {
  if (typeof window === "undefined") {
    return "/";
  }
  return String(window.location.pathname || "/");
}

function currentWorkOrderId() {
  const search = currentSearchParams();
  const searchId = String(search.get("work_order_id") || "").trim();
  if (searchId) {
    return searchId;
  }
  const path = currentPathname();
  const prefix = "/reliability/work-orders/";
  if (!path.startsWith(prefix)) {
    return "";
  }
  const remainder = path.slice(prefix.length).split("/")[0];
  if (!remainder || remainder === "detail") {
    return "";
  }
  return decodeURIComponent(remainder);
}

function truthyText(value) {
  return value ? "true" : "false";
}

function reliabilityIdCore(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return "";
  }
  return raw.includes(":") ? raw.split(":").pop() : raw;
}

function shortReliabilityId(value, prefix = "ID") {
  const core = reliabilityIdCore(value);
  return `${prefix}-${(core || "n/a").slice(0, 6)}`;
}

function shortIssueId(value) {
  return shortReliabilityId(value, "ISSUE");
}

function shortWorkOrderId(value) {
  return shortReliabilityId(value, "WO");
}

function shortVerificationId(value) {
  return shortReliabilityId(value, "VER");
}

function shortObservationId(value) {
  return shortReliabilityId(value, "OBS");
}

function shortFixAttemptId(value) {
  return shortReliabilityId(value, "FIX");
}

function reliabilityStatusBadge(status, palette = {}) {
  const normalized = String(status || "unknown").trim().toLowerCase() || "unknown";
  const defaultPalette = {
    queued: { bg: "#d1d5db", text: "#1f2937" },
    in_progress: { bg: "#3b82f6", text: "#ffffff" },
    tests_passed: { bg: "#f59e0b", text: "#111827" },
    needs_review: { bg: "#fb923c", text: "#111827" },
    accepted: { bg: "#16a34a", text: "#ffffff" },
    rejected: { bg: "#dc2626", text: "#ffffff" },
    pending: { bg: "#d1d5db", text: "#1f2937" },
    passed: { bg: "#16a34a", text: "#ffffff" },
    failed: { bg: "#dc2626", text: "#ffffff" },
    inconclusive: { bg: "#fb923c", text: "#111827" },
    open: { bg: "#d1d5db", text: "#1f2937" },
    triaged: { bg: "#9ca3af", text: "#111827" },
    fix_proposed: { bg: "#93c5fd", text: "#111827" },
    fix_submitted: { bg: "#fde68a", text: "#111827" },
    verification_pending: { bg: "#fcd34d", text: "#111827" },
    verified: { bg: "#16a34a", text: "#ffffff" },
    closed: { bg: "#10b981", text: "#ffffff" },
    duplicate: { bg: "#a3a3a3", text: "#111827" },
    wont_fix: { bg: "#6b7280", text: "#ffffff" },
    started: { bg: "#93c5fd", text: "#111827" },
    patch_submitted: { bg: "#fde68a", text: "#111827" },
    tests_failed: { bg: "#f87171", text: "#111827" },
    abandoned: { bg: "#9ca3af", text: "#111827" },
  };
  const colors = palette[normalized] || defaultPalette[normalized] || { bg: "#9ca3af", text: "#111827" };
  return `<span style="display:inline-block;padding:2px 10px;border-radius:999px;font-size:11px;font-weight:700;letter-spacing:0.04em;text-transform:uppercase;background:${colors.bg};color:${colors.text};">${escapeHtml(normalized)}</span>`;
}

function readinessBlockerBadge(value) {
  return value
    ? `<span style="display:inline-block;padding:2px 10px;border-radius:999px;font-size:11px;font-weight:700;background:#dc2626;color:#ffffff;">BLOCKER</span>`
    : `<span style="display:inline-block;padding:2px 10px;border-radius:999px;font-size:11px;font-weight:700;background:#e5e7eb;color:#374151;">non-blocker</span>`;
}

function aegisRuntimeStateBadge(state) {
  return reliabilityStatusBadge(state, {
    waiting_for_intent: { bg: "#16a34a", text: "#ffffff" },
    selected_intent_review_required: { bg: "#f59e0b", text: "#111827" },
    attention_required: { bg: "#dc2626", text: "#ffffff" },
    stale: { bg: "#6b7280", text: "#ffffff" },
    error: { bg: "#991b1b", text: "#ffffff" },
  });
}

const LEGACY_AEGIS_RUNTIME_READ_ONLY_MARKERS = ["webhook_enabled", "payload.data || payload.operator_state"];

function maybeNotifyAegisRuntime(operatorState = {}) {
  const alertState = operatorState.alert_state || {};
  const key = String(alertState.alert_key || "");
  if (!alertState.alertable || !key || typeof window === "undefined") {
    return { status: "not_alertable", key };
  }
  const storageKey = "aegis_runtime_last_alert_key_v1";
  let lastKey = "";
  try {
    lastKey = window.localStorage?.getItem(storageKey) || "";
  } catch (_err) {
    lastKey = "";
  }
  if (lastKey === key) {
    return { status: "suppressed_duplicate", key };
  }
  try {
    window.localStorage?.setItem(storageKey, key);
  } catch (_err) {
    // Storage is best-effort; notification support still controls delivery.
  }
  if (!("Notification" in window)) {
    return { status: "unsupported", key };
  }
  const title = `Aegis Runtime: ${operatorState.state || "UNKNOWN"}`;
  const body = operatorState.recommended_operator_action || operatorState.state_reason || "Review Aegis Runtime.";
  if (window.Notification.permission === "granted") {
    new window.Notification(title, { body, tag: key });
    return { status: "sent", key };
  }
  if (window.Notification.permission === "default") {
    window.Notification.requestPermission().then((permission) => {
      if (permission === "granted") {
        new window.Notification(title, { body, tag: key });
      }
    });
    return { status: "permission_requested", key };
  }
  return { status: "permission_denied", key };
}

function nextActionForWorkOrder(status) {
  const normalized = String(status || "").trim().toLowerCase();
  if (normalized === "queued") {
    return "Start fix";
  }
  if (normalized === "in_progress") {
    return "Continue fix";
  }
  if (normalized === "tests_passed") {
    return "Verify";
  }
  if (normalized === "needs_review") {
    return "Review";
  }
  if (normalized === "accepted") {
    return "Done";
  }
  if (normalized === "rejected") {
    return "Fix again";
  }
  return "Inspect";
}

function nextActionForVerification(status) {
  const normalized = String(status || "").trim().toLowerCase();
  if (normalized === "pending") {
    return "Review evidence";
  }
  if (normalized === "passed") {
    return "Close issue if verified";
  }
  if (normalized === "failed") {
    return "Create new work order";
  }
  if (normalized === "inconclusive") {
    return "Investigate";
  }
  return "Inspect";
}

function nextActionForFixAttempt(status) {
  const normalized = String(status || "").trim().toLowerCase();
  if (normalized === "started") {
    return "Continue fix";
  }
  if (normalized === "patch_submitted") {
    return "Run tests";
  }
  if (normalized === "tests_failed") {
    return "Fix again";
  }
  if (normalized === "tests_passed") {
    return "Verify";
  }
  if (normalized === "abandoned" || normalized === "rejected") {
    return "Create new attempt";
  }
  return "Inspect";
}

function issueRowNextAction(issue = {}) {
  const status = String(issue.status || "");
  if (status === "fix_submitted" || status === "verification_pending") {
    return "Record verification";
  }
  if (status === "fix_in_progress") {
    return "Continue fix";
  }
  if (status === "fix_proposed" || status === "triaged" || status === "open") {
    return "Create/assign work order";
  }
  if (status === "verified") {
    return "Close if confirmed";
  }
  if (status === "accepted" || status === "closed" || status === "duplicate" || status === "wont_fix") {
    return "Done";
  }
  return "Inspect";
}

function truncatedCell(value, maxWidthPx = 360) {
  const text = String(value || "").trim();
  if (!text) {
    return "n/a";
  }
  return `<span title="${escapeHtml(text)}" style="display:inline-block;max-width:${maxWidthPx}px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;vertical-align:bottom;">${escapeHtml(text)}</span>`;
}

function actionChip(label) {
  return `<span style="display:inline-block;padding:2px 10px;border-radius:999px;font-size:11px;font-weight:700;background:#111827;color:#f9fafb;">${escapeHtml(label || "Inspect")}</span>`;
}

function intOrNull(value) {
  if (typeof value === "number" && Number.isFinite(value)) {
    return Math.trunc(value);
  }
  if (typeof value === "string" && value.trim()) {
    const parsed = Number.parseInt(value.trim(), 10);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function sleeveReadinessGradeBadge(grade, tooltip = "") {
  const normalized = intOrNull(grade);
  if (normalized === null) {
    return `<span style="display:inline-block;padding:2px 10px;border-radius:999px;font-size:11px;font-weight:700;background:#9ca3af;color:#111827;">n/a</span>`;
  }
  const bg = normalized >= 6 ? "#16a34a" : normalized >= 4 ? "#f59e0b" : "#dc2626";
  const text = normalized >= 4 ? "#111827" : "#ffffff";
  const titleAttr = tooltip ? ` title="${escapeHtml(tooltip)}"` : "";
  return `<span${titleAttr} style="display:inline-block;padding:2px 10px;border-radius:999px;font-size:11px;font-weight:700;background:${bg};color:${text};">${escapeHtml(`${normalized} / 7`)}</span>`;
}

function sleeveReadinessWhyText({
  readiness_score,
  readiness_score_threshold,
  readiness_contributing_factors,
  readiness_grade_reason,
  readiness_grade_next_step,
} = {}) {
  const score = intOrNull(readiness_score);
  const threshold = intOrNull(readiness_score_threshold);
  const factors = safeList(readiness_contributing_factors)
    .map((item) => inlineText(item))
    .filter(Boolean);
  const scoreLine = `Score: ${score === null ? "n/a" : score}`;
  const thresholdLine = `Threshold: ${threshold === null ? "n/a" : threshold}`;
  const factorsLine = `Contributing factors: ${factors.length ? factors.join(", ") : "n/a"}`;
  const reasonLine = `Reason: ${inlineText(readiness_grade_reason) || "n/a"}`;
  const nextStepLine = `Next step: ${inlineText(readiness_grade_next_step) || "n/a"}`;
  return `${scoreLine}\n${thresholdLine}\n${factorsLine}\n${reasonLine}\n${nextStepLine}`;
}

function sleeveThresholdStatusLabel(grade, threshold) {
  const normalizedGrade = intOrNull(grade);
  const normalizedThreshold = intOrNull(threshold);
  if (normalizedGrade === null || normalizedThreshold === null) {
    return "Unavailable";
  }
  return normalizedGrade >= normalizedThreshold ? "Meets promotion threshold" : "Below promotion threshold";
}

function renderSleeveReadinessCell(row = {}) {
  const grade = intOrNull(row.readiness_grade_1_to_7);
  const thresholdGrade = intOrNull(row.score_threshold_grade_1_to_7);
  const why = sleeveReadinessWhyText(row);
  const reason = inlineText(row.readiness_grade_reason);
  const nextStep = inlineText(row.readiness_grade_next_step);
  const thresholdText = thresholdGrade === null ? "Threshold n/a" : `Threshold ${thresholdGrade} / 7`;
  const status = sleeveThresholdStatusLabel(grade, thresholdGrade);
  return `
    <div style="display:flex;flex-direction:column;gap:4px;">
      ${sleeveReadinessGradeBadge(grade, why)}
      <span style="font-size:11px;opacity:0.78;">${escapeHtml(thresholdText)}</span>
      <span title="${escapeHtml(why)}" style="font-size:11px;text-decoration:underline;text-decoration-style:dotted;cursor:help;">Why</span>
      <span style="font-size:11px;opacity:0.82;">${escapeHtml(status)}</span>
      ${reason ? `<span style="font-size:11px;opacity:0.82;">${escapeHtml(reason)}</span>` : ""}
      ${nextStep ? `<span style="font-size:11px;opacity:0.78;">Next: ${escapeHtml(nextStep)}</span>` : ""}
    </div>
  `;
}

function inlineText(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function ellipsisText(value, maxChars = 120) {
  const text = inlineText(value);
  if (!text) {
    return "n/a";
  }
  if (text.length <= maxChars) {
    return text;
  }
  return `${text.slice(0, Math.max(1, maxChars - 3)).trimEnd()}...`;
}

function renderReadinessKernelLadder(readinessKernel, state) {
  const layers = safeList(readinessKernel?.layers);
  const evidencePaths = safeList(readinessKernel?.evidence_paths).map((item) => inlineText(item)).filter(Boolean);
  const recoveryCommands = safeList(readinessKernel?.recovery_commands).map((item) => inlineText(item)).filter(Boolean);
  const deferred = safeList(readinessKernel?.deferred_phases).map((item) => inlineText(item)).filter(Boolean);
  const submit = readinessKernel?.submit || {};
  const productionVersion = readinessKernel?.production_version || {};
  const packet = readinessKernel?.packet || {};
  return `
    <div class="metric-grid">
      ${renderMetricCard({ label: "Runtime mode", value: readinessKernel?.runtime_mode || "UNKNOWN", semantic: readinessKernel?.runtime_mode === "PRODUCTION" ? "healthy" : "warning", semantics: state.semantics })}
      ${renderMetricCard({ label: "Production version", value: productionVersion.status || readinessKernel?.production_version_status || "UNKNOWN", semantic: productionVersion.status === "ACTIVE" ? "healthy" : "blocked", semantics: state.semantics })}
      ${renderMetricCard({ label: "Packet", value: packet.status || readinessKernel?.packet_status || "UNKNOWN", semantic: packet.status === "CURRENT" ? "healthy" : "blocked", semantics: state.semantics })}
      ${renderMetricCard({ label: "Submit", value: readinessKernel?.submit_status || "UNKNOWN", semantic: readinessKernel?.submit_status === "ALLOWED" ? "healthy" : "blocked", semantics: state.semantics })}
      ${renderMetricCard({ label: "Current phase", value: readinessKernel?.current_phase || "UNKNOWN", semantic: readinessKernel?.overall_status === "READY" ? "healthy" : "blocked", semantics: state.semantics })}
      ${renderMetricCard({ label: "Canonical blocker", value: readinessKernel?.canonical_blocker || "NONE", semantic: readinessKernel?.canonical_blocker ? "blocked" : "healthy", semantics: state.semantics })}
    </div>
    <div style="margin-top:12px;" class="stack-list">
      <div class="evidence-row">
        <div>
          <strong>${escapeHtml(readinessKernel?.current_phase || "UNKNOWN")}</strong>
          <div style="font-size:12px;opacity:0.78;">Owner: ${escapeHtml(readinessKernel?.blocker_owner || "UNKNOWN")}</div>
          <div style="font-size:12px;opacity:0.82;">Action: ${escapeHtml(readinessKernel?.recovery_action || readinessKernel?.operator_next_action || "No recovery action reported.")}</div>
        </div>
        <div style="text-align:right;min-width:160px;">
          ${renderSemanticBadge(readinessKernel?.canonical_blocker || "NONE", readinessKernel?.canonical_blocker ? "blocked" : "healthy", state.semantics)}
        </div>
      </div>
      <div class="evidence-row">
        <div>
          <strong>Submit firewall</strong>
          <div style="font-size:12px;opacity:0.78;">Source: ${escapeHtml(readinessKernel?.submit_source || submit.source || "submit_firewall")}</div>
          <div style="font-size:12px;opacity:0.82;">Blocker: ${escapeHtml(readinessKernel?.submit_canonical_blocker || submit.canonical_blocker || "NONE")}</div>
        </div>
        <div style="text-align:right;min-width:160px;">
          ${renderSemanticBadge(readinessKernel?.submit_status || submit.status || "UNKNOWN", readinessKernel?.submit_status === "ALLOWED" ? "healthy" : "blocked", state.semantics)}
        </div>
      </div>
      ${recoveryCommands.length ? `<div class="evidence-row"><div><strong>Recovery command</strong><div style="font-size:12px;opacity:0.82;">${recoveryCommands.map((cmd) => escapeHtml(cmd)).join("<br />")}</div></div></div>` : ""}
      ${evidencePaths.length ? `<div class="evidence-row"><div><strong>Evidence paths</strong><div style="font-size:12px;opacity:0.82;">${evidencePaths.map((path) => escapeHtml(path)).join("<br />")}</div></div></div>` : ""}
      ${deferred.length ? `<div class="evidence-row"><div><strong>Deferred downstream phases</strong><div class="chip-list" style="margin-top:6px;">${deferred.map((phase) => `<span class="support-chip">${escapeHtml(phase)}</span>`).join("")}</div></div></div>` : ""}
      ${layers.length ? `<div class="evidence-row"><div><strong>Phase results</strong><div style="font-size:12px;opacity:0.82;">${layers.map((row) => `${escapeHtml(row.layer_id || row.label || "phase")}: ${escapeHtml(row.status || "UNKNOWN")}`).join("<br />")}</div></div></div>` : ""}
    </div>
  `;
}

function objectiveIsVague(value) {
  const text = inlineText(value).toLowerCase();
  if (!text || text.length < 18) {
    return true;
  }
  const wordCount = text.split(/\s+/).filter(Boolean).length;
  if (wordCount < 4) {
    return true;
  }
  const vagueTokens = ["smoke", "todo", "tbd", "wip", "issue", "fix attempt", "create-work-order", "work order"];
  return vagueTokens.some((token) => text.includes(token)) && text.length < 64;
}

function workOrderFixLabel(workOrder = {}, issue = {}) {
  const objective = inlineText(workOrder.objective);
  const issueTitle = inlineText(issue.title);
  if (!objective || objectiveIsVague(objective)) {
    return issueTitle || objective || "Investigate and resolve issue";
  }
  return objective;
}

function includesAnyKeyword(value, keywords = []) {
  const text = inlineText(value).toLowerCase();
  if (!text) {
    return false;
  }
  return keywords.some((keyword) => text.includes(String(keyword || "").toLowerCase()));
}

function isSleeveLiveReadinessContext(workOrder = {}, issue = {}) {
  const keywords = [
    "sleeve",
    "grading",
    "scoring",
    "readiness",
    "live",
    "promotion",
    "scored every sleeve",
    "sleeve 1-7",
    "1-7",
    "promotion from paper to live",
    "ready to be promoted",
  ];
  const content = [
    issue.title,
    issue.actual_behavior,
    issue.expected_behavior,
    issue.impact_summary,
    workOrder.objective,
    workOrder.actual_behavior,
    workOrder.expected_behavior,
  ].filter(Boolean).join(" ");
  return includesAnyKeyword(content, keywords);
}

function isSystemSmokeTestWork(workOrder = {}, issue = {}) {
  const smokeTerms = [
    "smoke",
    "endpoint smoke",
    "workflow smoke",
    "create-work-order endpoint smoke",
  ];
  const content = [
    issue.title,
    issue.actual_behavior,
    issue.expected_behavior,
    issue.impact_summary,
    workOrder.objective,
    workOrder.actual_behavior,
    workOrder.expected_behavior,
  ].filter(Boolean).join(" ");
  const lowValueSmokeData = inlineText(workOrder.actual_behavior).toLowerCase() === "actual"
    && inlineText(workOrder.expected_behavior).toLowerCase() === "expected"
    && inlineText(issue.impact_summary).toLowerCase() === "smoke";
  return lowValueSmokeData || includesAnyKeyword(content, smokeTerms);
}

function workOrderNextStepLabel(status) {
  const normalized = String(status || "").trim().toLowerCase();
  if (normalized === "queued") {
    return "Run Codex";
  }
  if (normalized === "in_progress") {
    return "Continue Codex fix";
  }
  if (normalized === "tests_passed") {
    return "Record verification";
  }
  if (normalized === "needs_review") {
    return "Review fix";
  }
  if (normalized === "accepted") {
    return "Done";
  }
  if (normalized === "rejected") {
    return "Revise fix";
  }
  return "Inspect";
}

export function formatUsd(value) {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return "n/a";
  }
  return value.toLocaleString("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 2,
  });
}

function formatSignedUsd(value) {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return "n/a";
  }
  const sign = value > 0 ? "+" : "";
  return `${sign}${formatUsd(value)}`;
}

function toEvidenceRefs(paths = [], labelPrefix = "Artifact") {
  return safeList(paths)
    .filter((path) => typeof path === "string" && path.trim())
    .map((path, index) => ({
      label: `${labelPrefix} ${index + 1}`,
      path,
      artifact_type: "artifact_ref",
    }));
}

function topAlert(alertsPayload = {}) {
  return safeList(alertsPayload.alerts)[0] || null;
}

function renderHomePanelSection(homeBundle, semantics) {
  const homeView = homeBundle.home_view || {};
  return renderCardSection({
    eyebrow: "Home View",
    title: "Governed Home Panels",
    subtitle: "Rendered directly from the operator home contract and composition policy.",
    body: renderPanelRows(safeList(homeView.rendered_panels), semantics),
  });
}

function renderReadinessSnapshot(summary, workflow, semantics) {
  const readinessCards = [
    renderMetricCard({
      label: "Build",
      value: summary.build_status || "UNKNOWN",
      semantic: summary.build_status_semantic || "unknown",
      semantics,
    }),
    renderMetricCard({
      label: "Admission",
      value: summary.admission_status || "UNKNOWN",
      semantic: summary.admission_status_semantic || "unknown",
      semantics,
    }),
    renderMetricCard({
      label: "Boundary",
      value: summary.boundary_status || "UNKNOWN",
      semantic: summary.boundary_status_semantic || "unknown",
      semantics,
    }),
    renderMetricCard({
      label: "Control Plane",
      value: summary.control_plane_status || "UNKNOWN",
      semantic: summary.control_plane_status_semantic || "unknown",
      semantics,
    }),
    renderMetricCard({
      label: "Replay",
      value: summary.replay_status || "UNKNOWN",
      semantic: summary.replay_status_semantic || "unknown",
      semantics,
    }),
    renderMetricCard({
      label: "Workflow",
      value: workflow.workflow_state || "UNKNOWN",
      semantic: workflow.workflow_state === "ready" ? "healthy" : workflow.workflow_state === "attention_needed" ? "warning" : "blocked",
      semantics,
      detail: workflow.operator_priority ? `Priority ${workflow.operator_priority}` : "",
    }),
  ].join("");

  return renderCardSection({
    eyebrow: "Readiness",
    title: "Command Readiness Snapshot",
    subtitle: "Drillable readiness pillars from canonical system summary and workflow views.",
    body: `<div class="metric-grid">${readinessCards}</div>`,
  });
}

function renderAdvisorySummary(advisoryPayload = {}, semantics = {}) {
  const decisions = safeList(advisoryPayload.decisions).slice(0, 4);
  return renderCardSection({
    eyebrow: "Advisory",
    title: "Current Advisory Decision State",
    subtitle: "Governed advisory decisions rendered from certified truth only.",
    body: decisions.length
      ? renderList(decisions, {
          renderItem: (item) => `
            <article class="stack-card">
              <div class="stack-card-header">
                <div>
                  <div class="stack-card-title">${escapeHtml(item.advisory_item_id || item.advisory_surface_label || "Advisory decision")}</div>
                  <div class="stack-card-subtitle">${escapeHtml(item.decision_state || "decision")}</div>
                </div>
                ${renderSemanticBadge(item.actionability_state || "unknown", item.actionability_state === "promotion_eligible" || item.actionability_state === "actionable" ? "healthy" : item.actionability_state === "blocked" ? "blocked" : "warning", semantics)}
              </div>
              <div class="chip-list">
                <span class="support-chip">${escapeHtml(item.freshness_state || "unknown")}</span>
                <span class="support-chip">${escapeHtml(item.promotion_eligibility_state || "unknown")}</span>
              </div>
            </article>
          `,
        })
      : `<div class="empty-state">No advisory decisions were returned.</div>`,
  });
}

function renderOperatorQueryPanel(state) {
  const result = state.commandQueryResult;
  const resultMarkup = result
    ? renderCardSection({
        eyebrow: "Query Response",
        title: result.query_response?.query_class_id || "Operator Query",
        subtitle: `Status ${result.query_response?.response_status || "UNKNOWN"}`,
        body: `
          <div class="stack-list">
            ${safeList(result.query_response?.answer_blocks).map((block) => `
              <article class="stack-card">
                <div class="stack-card-title">${escapeHtml(block.title || block.block_id || "Answer block")}</div>
                <div class="line-list">
                  ${safeList(block.lines).map((line) => `<div>${escapeHtml(line)}</div>`).join("")}
                </div>
                ${safeList(block.source_refs).length ? `<div class="chip-list">${safeList(block.source_refs).map((ref) => `<button class="support-chip chip-button" type="button" data-artifact-path="${escapeHtml(ref)}" data-artifact-title="${escapeHtml(block.title || "Answer source")}">Open source</button>`).join("")}</div>` : ""}
              </article>
            `).join("")}
          </div>
        `,
      })
    : "";

  return `
    ${renderCardSection({
      eyebrow: "Ask Aegis",
      title: "Governed Operator Query",
      subtitle: "Query responses are built by the backend operator control plane and carry trust/retrieval manifests.",
      body: `
        <form class="operator-query-form">
          <label class="query-field">
            <span>Question</span>
            <input id="commandQueryInput" name="query_text" type="text" value="${escapeHtml(state.commandQueryText || "")}" placeholder="Why is readiness blocked today?" />
          </label>
          <button class="primary-button" type="submit">Run Query</button>
        </form>
      `,
    })}
    ${resultMarkup}
  `;
}

function renderAttentionSection(workflow = {}, semantics = {}) {
  return renderCardSection({
    eyebrow: "Attention",
    title: "What Needs Attention",
    subtitle: "Operator next steps from the existing workflow summary, not ad hoc UI heuristics.",
    body: safeList(workflow.next_steps).length
      ? renderList(
          safeList(workflow.next_steps).map((step) => ({
            ...step,
            target_surface: routeHrefFromSurface(step.target_surface),
          })),
          { renderItem: (step) => renderWorkflowStep(step, semantics) },
        )
      : `<div class="empty-state">No operator next steps were returned.</div>`,
  });
}

function renderAlertSection(alertsPayload = {}, semantics = {}) {
  const alerts = safeList(alertsPayload.alerts).slice(0, 5);
  return renderCardSection({
    eyebrow: "Alerts",
    title: "Current Alert Load",
    subtitle: "Alert state is backend-authored and traceable to its source artifacts.",
    body: alerts.length
      ? renderList(alerts, {
          renderItem: (alert) => `
            <article class="stack-card">
              <div class="stack-card-header">
                <div>
                  <div class="stack-card-title">${escapeHtml(alert.title || alert.entity_id || "Alert")}</div>
                  <div class="stack-card-subtitle">${escapeHtml(alert.summary || "No summary provided.")}</div>
                </div>
                ${renderSemanticBadge(alert.status || alert.severity || "UNKNOWN", alert.semantic || "warning", semantics)}
              </div>
              <div class="chip-list">
                ${safeList(alert.reason_codes).map((reasonCode) => `<span class="support-chip">${escapeHtml(reasonCode)}</span>`).join("")}
              </div>
            </article>
          `,
        })
      : `<div class="empty-state">No active alerts were returned.</div>`,
  });
}

async function renderCommandPage(state) {
  let overview;
  try {
    overview = await fetchCommandOverview();
  } catch (error) {
    overview = {
      ...COMMAND_OVERVIEW_MOCK,
      data_source_state: "MOCK / UNAVAILABLE",
      fallback_badge: "MOCK / UNAVAILABLE",
      as_of_label: "Data as of: MOCK / UNAVAILABLE",
      context: {
        ...(COMMAND_OVERVIEW_MOCK.context || {}),
        sourceOfTruth: "MOCK / UNAVAILABLE",
        freshness: "MOCK / UNAVAILABLE",
        dataQuality: "MOCK / UNAVAILABLE",
        dataSourceState: "MOCK / UNAVAILABLE",
      },
    };
  }
  return {
    title: "Command / Overview",
    meta: "Governed overview of operating truth, exceptions, policy state, evidence, and source lineage.",
    dataTimestamp: overview.as_of_label,
    html: renderCommandOverview(overview),
    contextHtml: ContextRail({ context: overview.context }),
  };
}

async function renderPortfolioPage(state) {
  const financialState = await fetchFinancialState();
  const accounts = safeList(financialState.account_rollups);
  const holdings = safeList(financialState.holdings_rollup?.items);
  const concentrations = safeList(financialState.concentration_summary?.top_symbol_exposures);
  return {
    title: "Portfolio",
    meta: "Canonical investable totals, account rollups, liquidity, and exposure summaries from the backend financial-state projection.",
    html: [
      renderCardSection({
        eyebrow: "Portfolio",
        title: "Portfolio Surface",
        subtitle: "Thin rendering over the canonical backend financial-state authority.",
        body: `<div class="metric-grid">
          ${renderMetricCard({ label: "Investable assets", value: formatUsd(financialState.investable_summary?.investable_assets_total_usd), detail: `As of ${formatTimestamp(financialState.as_of_utc)}` })}
          ${renderMetricCard({ label: "Cash", value: formatUsd(financialState.liquidity_summary?.cash_total_usd) })}
          ${renderMetricCard({ label: "Gross positions", value: formatUsd(financialState.investable_summary?.gross_positions_value_usd) })}
          ${renderMetricCard({ label: "Truth state", value: financialState.truth_state || "UNKNOWN" })}
        </div>`,
      }),
      renderSimpleTable({
        columns: [
          { key: "full_account_number", label: "Account" },
          { key: "base_currency", label: "Currency" },
          { key: "cash_usd", label: "Cash", render: (row) => escapeHtml(formatUsd(row.cash_usd)) },
          { key: "buying_power_usd", label: "Buying Power", render: (row) => escapeHtml(formatUsd(row.buying_power_usd)) },
          {
            key: "restrictions",
            label: "Restrictions",
            render: (row) => `
              <div class="chip-list">
                <span class="support-chip">${escapeHtml(row.restrictions?.submission_enabled === false ? "submission_blocked" : "submission_enabled")}</span>
                ${safeList(row.restrictions?.allowed_sleeve_ids).map((item) => `<span class="support-chip">${escapeHtml(item)}</span>`).join("")}
              </div>
            `,
          },
        ],
        rows: accounts,
        emptyMessage: "No accounts were returned by the financial-state projection.",
      }),
      renderCardSection({
        eyebrow: "Holdings",
        title: "Holdings Rollup",
        subtitle: "Holdings are rendered from backend-owned accounting and positions evidence, not recomputed from frontend joins.",
        body: renderSimpleTable({
          columns: [
            { key: "symbol", label: "Symbol" },
            { key: "kind", label: "Kind" },
            { key: "quantity", label: "Quantity" },
            { key: "market_value_usd", label: "Market Value", render: (row) => escapeHtml(formatUsd(row.market_value_usd)) },
            { key: "mark_source", label: "Mark Source" },
          ],
          rows: holdings,
          emptyMessage: "No holdings were returned by the financial-state projection.",
        }),
      }),
      renderCardSection({
        eyebrow: "Exposure",
        title: "Concentration Summary",
        subtitle: "Top symbol exposure rows come from the backend exposure authority referenced by financial-state.",
        body: renderSimpleTable({
          columns: [
            { key: "symbol", label: "Symbol" },
            { key: "gross_notional_usd", label: "Gross Notional", render: (row) => escapeHtml(formatUsd(row.gross_notional_usd)) },
            { key: "net_notional_usd", label: "Net Notional", render: (row) => escapeHtml(formatUsd(row.net_notional_usd)) },
            { key: "capital_at_risk_usd", label: "Capital At Risk", render: (row) => escapeHtml(formatUsd(row.capital_at_risk_usd)) },
            { key: "sector", label: "Sector" },
          ],
          rows: concentrations,
          emptyMessage: "No concentration rows were returned by the financial-state projection.",
        }),
      }),
    ].join(""),
    contextHtml: [
      renderSourceRefCard(safeList(financialState.source_refs), "Financial Evidence", "Source refs declared by the financial-state projection."),
      renderCardSection({
        eyebrow: "Liquidity",
        title: "Liquidity Summary",
        subtitle: "Cash and buying-power facts remain backend-authored and explicitly degraded when upstream inputs are incomplete.",
        body: renderDefinitionRows([
          { label: "Cash total", value: formatUsd(financialState.liquidity_summary?.cash_total_usd) },
          { label: "Available funds", value: formatUsd(financialState.liquidity_summary?.available_funds_usd) },
          { label: "Net liquidation value", value: formatUsd(financialState.liquidity_summary?.net_liquidation_value_usd) },
          { label: "Reserve status", value: financialState.reserve_summary?.status || "UNKNOWN" },
        ]),
      }),
      renderCardSection({
        eyebrow: "Warnings",
        title: "Financial Warnings",
        subtitle: "Explicit degraded-state markers surfaced by the backend authority.",
        body: safeList(financialState.financial_warnings).length
          ? `<div class="chip-list">${safeList(financialState.financial_warnings).map((warning) => `<span class="support-chip">${escapeHtml(warning)}</span>`).join("")}</div>`
          : `<div class="empty-state">No financial warnings were returned.</div>`,
      }),
    ].join(""),
  };
}

async function renderCapitalOverviewPage() {
  const payload = await fetchCapitalOverview();
  const overview = payload.overview || {};
  const basis = payload.basis || {};
  const reportBasis = payload.report_basis_metadata || overview.report_basis_metadata || {};
  const freshness = payload.freshness_completeness || overview.freshness_completeness || {};
  const controlRows = safeList(overview.allocation_by_control?.rows);
  const bucketRows = safeList(overview.allocation_by_bucket?.rows);
  const matrixRows = safeList(overview.bucket_control_matrix?.rows);
  const investableContributors = safeList(overview.investable_explainability?.contributors);
  const findings = safeList(payload.validation_findings);
  const topFindings = findings.slice(0, 6);
  return {
    title: "Capital Overview",
    meta: "Household capital allocation overview sourced from Capital bounded-domain derived surfaces.",
    html: [
      renderCardSection({
        eyebrow: "Capital",
        title: "Investable Capital Snapshot",
        subtitle: "All headline numbers use latest-per-account basis with explicit include/exclude counts.",
        body: `<div class="metric-grid">
          ${renderMetricCard({ label: "Investable total", value: formatUsd(overview.investable_total), detail: `As of ${escapeHtml(overview.as_of_date || "n/a")}` })}
          ${renderMetricCard({ label: "Advisor-controlled", value: formatUsd(overview.advisor_controlled_capital) })}
          ${renderMetricCard({ label: "Aegis-controlled", value: formatUsd(overview.aegis_controlled_capital) })}
          ${renderMetricCard({ label: "Passive-controlled", value: formatUsd(overview.passive_controlled_capital) })}
          ${renderMetricCard({ label: "Included accounts", value: String(overview.included_account_count ?? "0") })}
          ${renderMetricCard({ label: "Excluded accounts", value: String(overview.excluded_account_count ?? "0") })}
          ${renderMetricCard({ label: "Freshness", value: freshness.status || reportBasis.freshness_status || "UNKNOWN" })}
          ${renderMetricCard({ label: "Stale included accounts", value: String(reportBasis.stale_account_count ?? freshness.stale_account_count ?? 0) })}
        </div>`,
      }),
      renderCardSection({
        eyebrow: "Explainability",
        title: "Investable Total Contributors",
        subtitle: "Backend-derived contributor balances used in the investable denominator.",
        body: renderSimpleTable({
          columns: [
            { key: "account_id", label: "Account ID" },
            { key: "account_name", label: "Account" },
            { key: "balance", label: "Balance", render: (row) => escapeHtml(formatUsd(row.balance)) },
            { key: "control_type", label: "Control" },
            { key: "bucket_type", label: "Bucket" },
          ],
          rows: investableContributors,
          emptyMessage: "No contributor rows were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Allocation",
        title: "Allocation By Control",
        subtitle: "Numerator and denominator are explicit and backend-computed.",
        body: renderSimpleTable({
          columns: [
            { key: "control_type", label: "Control" },
            { key: "balance_total", label: "Balance", render: (row) => escapeHtml(formatUsd(row.balance_total)) },
            { key: "allocation_pct", label: "Pct", render: (row) => escapeHtml(formatPercent(row.allocation_pct)) },
            {
              key: "contributors",
              label: "Contributors",
              render: (row) =>
                escapeHtml(
                  safeList(row.contributors)
                    .map((item) => `${item.account_id}:${formatUsd(item.balance)}`)
                    .join(", ") || "none",
                ),
            },
          ],
          rows: controlRows,
          emptyMessage: "No control allocation rows were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Allocation",
        title: "Allocation By Bucket",
        subtitle: "Included-only denominator; excluded balances are reported separately.",
        body: renderSimpleTable({
          columns: [
            { key: "bucket_type", label: "Bucket" },
            { key: "balance_total", label: "Balance", render: (row) => escapeHtml(formatUsd(row.balance_total)) },
            { key: "allocation_pct", label: "Pct", render: (row) => escapeHtml(formatPercent(row.allocation_pct)) },
            {
              key: "contributors",
              label: "Contributors",
              render: (row) =>
                escapeHtml(
                  safeList(row.contributors)
                    .map((item) => `${item.account_id}:${formatUsd(item.balance)}`)
                    .join(", ") || "none",
                ),
            },
          ],
          rows: bucketRows,
          emptyMessage: "No bucket allocation rows were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Matrix",
        title: "Bucket x Control Matrix",
        subtitle: "Cross-tab of included balances by semantic bucket and control ownership.",
        body: renderSimpleTable({
          columns: [
            { key: "bucket_type", label: "Bucket" },
            { key: "control_type", label: "Control" },
            { key: "balance_total", label: "Balance", render: (row) => escapeHtml(formatUsd(row.balance_total)) },
            { key: "allocation_pct", label: "Pct", render: (row) => escapeHtml(formatPercent(row.allocation_pct)) },
          ],
          rows: matrixRows,
          emptyMessage: "No matrix rows were returned.",
        }),
      }),
    ].join(""),
    contextHtml: [
      renderCardSection({
        eyebrow: "Basis",
        title: "Data Basis",
        subtitle: "Discloses as-of, include/exclude count, and validation state for operator trust.",
        body: renderDefinitionRows([
          { label: "As-of date", value: basis.as_of_date || overview.as_of_date || "n/a" },
          { label: "Basis type", value: reportBasis.report_basis || basis.basis_type || "latest_per_account" },
          { label: "Basis description", value: reportBasis.basis_description || "n/a" },
          { label: "Included account count", value: String(reportBasis.included_account_count ?? basis.included_account_count ?? overview.included_account_count ?? 0) },
          { label: "Excluded account count", value: String(reportBasis.excluded_account_count ?? basis.excluded_account_count ?? overview.excluded_account_count ?? 0) },
          { label: "Stale account count", value: String(reportBasis.stale_account_count ?? 0) },
          { label: "Freshness status", value: reportBasis.freshness_status || freshness.status || "UNKNOWN" },
          { label: "Validation status", value: reportBasis.validation_status || payload.validation_status || "UNKNOWN" },
          { label: "Validation findings", value: String(payload.validation_error_count ?? 0) },
        ]),
      }),
      renderCardSection({
        eyebrow: "Validation",
        title: "Active Validation Findings",
        subtitle: "Non-empty finding set indicates degraded data quality but remains transparent to operators.",
        body: topFindings.length
          ? renderSimpleTable({
              columns: [
                { key: "severity", label: "Severity" },
                { key: "code", label: "Code" },
                { key: "message", label: "Message" },
                { key: "account_ids", label: "Accounts", render: (row) => escapeHtml(safeList(row.account_ids).join(", ") || "none") },
              ],
              rows: topFindings,
              emptyMessage: "No validation findings were returned.",
            })
          : `<div class="empty-state">No active validation findings.</div>`,
      }),
      renderSourceRefCard(safeList(payload.source_refs), "Capital Source Evidence", "Capital bounded-domain storage and derived projection references."),
    ].join(""),
  };
}

async function renderCapitalAccountsPage() {
  const payload = await fetchCapitalAccounts();
  const rows = safeList(payload.rows);
  const auditRows = safeList(payload.recent_audit_entries).slice(0, 12);
  const basis = payload.basis || {};
  const freshness = payload.freshness_completeness || {};
  return {
    title: "Capital Accounts",
    meta: "Account registry with latest balances and current effective-dated classifications.",
    html: [
      renderCardSection({
        eyebrow: "Accounts",
        title: "Account Registry",
        subtitle: "Each row carries latest balance + current semantic classification + include/exclude state.",
        body: renderSimpleTable({
          columns: [
            { key: "account_name", label: "Account" },
            { key: "latest_balance", label: "Latest Balance", render: (row) => escapeHtml(formatUsd(row.latest_balance)) },
            { key: "as_of_date", label: "As-of" },
            { key: "capital_type", label: "Capital Type" },
            { key: "control_type", label: "Control" },
            { key: "bucket_type", label: "Bucket" },
            { key: "include_in_allocation", label: "Included", render: (row) => escapeHtml(row.include_in_allocation ? "yes" : "no") },
            { key: "confidence_level", label: "Confidence", render: (row) => escapeHtml(typeof row.confidence_level === "number" ? row.confidence_level.toFixed(2) : "n/a") },
            { key: "confidence_band", label: "Confidence Band" },
            { key: "notes", label: "Notes" },
          ],
          rows,
          emptyMessage: "No Capital account rows were returned.",
        }),
      }),
    ].join(""),
    contextHtml: [
      renderCardSection({
        eyebrow: "Basis",
        title: "Latest Balance Basis",
        subtitle: "Rows are latest-per-account and do not silently aggregate missing balance/classification rows.",
        body: renderDefinitionRows([
          { label: "As-of date", value: basis.as_of_date || "n/a" },
          { label: "Basis type", value: basis.basis_type || "latest_per_account" },
          { label: "Returned rows", value: String(basis.row_count ?? rows.length) },
          { label: "Freshness status", value: freshness.status || "UNKNOWN" },
          { label: "Stale account count", value: String(freshness.stale_account_count ?? 0) },
        ]),
      }),
      renderCardSection({
        eyebrow: "Audit",
        title: "Recent Capital Audit Trail",
        subtitle: "Meaningful domain changes are captured as append-only audit entries.",
        body: renderSimpleTable({
          columns: [
            { key: "created_at", label: "When", render: (row) => formatTimestamp(row.created_at) },
            { key: "entity_type", label: "Entity Type" },
            { key: "entity_id", label: "Entity ID" },
            { key: "action", label: "Action" },
            { key: "reason", label: "Reason" },
          ],
          rows: auditRows,
          emptyMessage: "No audit entries were returned.",
        }),
      }),
      renderSourceRefCard(safeList(payload.source_refs), "Capital Source Evidence", "Capital bounded-domain storage and derived projection references."),
    ].join(""),
  };
}

async function renderCapitalAllocationPage() {
  const payload = await fetchCapitalAllocation();
  const controlRows = safeList(payload.allocation_by_control?.rows);
  const bucketRows = safeList(payload.allocation_by_bucket?.rows);
  const matrixRows = safeList(payload.bucket_control_matrix?.rows);
  const includeExclude = payload.included_excluded_summary || {};
  const basis = payload.basis || {};
  const reportBasis = payload.report_basis_metadata || payload.allocation_by_control?.report_basis_metadata || {};
  return {
    title: "Capital Allocation",
    meta: "Allocation by control and bucket with explicit numerator/denominator and included vs excluded disclosures.",
    html: [
      renderCardSection({
        eyebrow: "Included vs Excluded",
        title: "Allocation Basis Disclosure",
        subtitle: "Included balances drive allocation percentages; excluded balances remain visible and traceable.",
        body: `<div class="metric-grid">
          ${renderMetricCard({ label: "Included total", value: formatUsd(includeExclude.included_total) })}
          ${renderMetricCard({ label: "Excluded total", value: formatUsd(includeExclude.excluded_total) })}
          ${renderMetricCard({ label: "Included accounts", value: String(includeExclude.included_account_count ?? 0) })}
          ${renderMetricCard({ label: "Excluded accounts", value: String(includeExclude.excluded_account_count ?? 0) })}
        </div>`,
      }),
      renderCardSection({
        eyebrow: "Control",
        title: "Allocation By Control",
        subtitle: "Numerator = included balance by control; denominator = total included balance.",
        body: renderSimpleTable({
          columns: [
            { key: "control_type", label: "Control" },
            { key: "balance_total", label: "Balance", render: (row) => escapeHtml(formatUsd(row.balance_total)) },
            { key: "allocation_pct", label: "Pct", render: (row) => escapeHtml(formatPercent(row.allocation_pct)) },
            {
              key: "contributors",
              label: "Contributors",
              render: (row) =>
                escapeHtml(
                  safeList(row.contributors)
                    .map((item) => `${item.account_id}:${formatUsd(item.balance)}`)
                    .join(", ") || "none",
                ),
            },
          ],
          rows: controlRows,
          emptyMessage: "No control rows were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Bucket",
        title: "Allocation By Bucket",
        subtitle: "Bucket-level allocation is derived by backend service, not frontend chart math.",
        body: renderSimpleTable({
          columns: [
            { key: "bucket_type", label: "Bucket" },
            { key: "balance_total", label: "Balance", render: (row) => escapeHtml(formatUsd(row.balance_total)) },
            { key: "allocation_pct", label: "Pct", render: (row) => escapeHtml(formatPercent(row.allocation_pct)) },
            {
              key: "contributors",
              label: "Contributors",
              render: (row) =>
                escapeHtml(
                  safeList(row.contributors)
                    .map((item) => `${item.account_id}:${formatUsd(item.balance)}`)
                    .join(", ") || "none",
                ),
            },
          ],
          rows: bucketRows,
          emptyMessage: "No bucket rows were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Matrix",
        title: "Bucket x Control Matrix",
        subtitle: "Cross-matrix keeps numerators traceable to contributing account IDs.",
        body: renderSimpleTable({
          columns: [
            { key: "bucket_type", label: "Bucket" },
            { key: "control_type", label: "Control" },
            { key: "balance_total", label: "Balance", render: (row) => escapeHtml(formatUsd(row.balance_total)) },
            { key: "allocation_pct", label: "Pct", render: (row) => escapeHtml(formatPercent(row.allocation_pct)) },
            {
              key: "contributors",
              label: "Contributors",
              render: (row) =>
                escapeHtml(
                  safeList(row.contributors)
                    .map((item) => `${item.account_id}:${formatUsd(item.balance)}`)
                    .join(", ") || "none",
                ),
            },
          ],
          rows: matrixRows,
          emptyMessage: "No matrix rows were returned.",
        }),
      }),
    ].join(""),
    contextHtml: [
      renderCardSection({
        eyebrow: "Basis",
        title: "Allocation Basis and Denominator",
        subtitle: "Percentages are explainable and include account-count disclosures.",
        body: renderDefinitionRows([
          { label: "As-of date", value: reportBasis.as_of_date || basis.as_of_date || "n/a" },
          { label: "Basis type", value: reportBasis.report_basis || basis.basis_type || "latest_per_account" },
          { label: "Basis description", value: reportBasis.basis_description || "n/a" },
          { label: "Numerator", value: basis.numerator || "included_balance_by_dimension" },
          { label: "Denominator", value: basis.denominator || "total_included_balance" },
          { label: "Included account count", value: String(reportBasis.included_account_count ?? basis.included_account_count ?? 0) },
          { label: "Excluded account count", value: String(reportBasis.excluded_account_count ?? basis.excluded_account_count ?? 0) },
          { label: "Stale account count", value: String(reportBasis.stale_account_count ?? basis.stale_account_count ?? 0) },
          { label: "Freshness status", value: reportBasis.freshness_status || basis.freshness_status || "UNKNOWN" },
          { label: "Validation status", value: reportBasis.validation_status || basis.validation_status || "UNKNOWN" },
        ]),
      }),
      renderSourceRefCard(safeList(payload.source_refs), "Capital Source Evidence", "Capital bounded-domain storage and derived projection references."),
    ].join(""),
  };
}

async function renderCapitalHistoryPage() {
  const payload = await fetchCapitalHistory();
  const investablePoints = safeList(payload.investable_time_series?.points);
  const aegisPoints = safeList(payload.aegis_allocation_pct_time_series?.points);
  const basis = payload.basis || {};
  return {
    title: "Capital History",
    meta: "Investable total and Aegis allocation percentage over time from append-only balance snapshots.",
    html: [
      renderCardSection({
        eyebrow: "History",
        title: "Investable Capital Over Time",
        subtitle: "Latest-per-account-on-or-before-day basis; each point remains traceable to source account snapshots.",
        body: renderSimpleTable({
          columns: [
            { key: "day", label: "Day" },
            { key: "investable_total", label: "Investable Total", render: (row) => escapeHtml(formatUsd(row.investable_total)) },
            { key: "included_account_count", label: "Included Accounts" },
          ],
          rows: investablePoints,
          emptyMessage: "No investable history points were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "History",
        title: "Aegis Allocation Percentage Over Time",
        subtitle: "Aegis numerator and investable denominator are both displayed for explainability.",
        body: renderSimpleTable({
          columns: [
            { key: "day", label: "Day" },
            { key: "aegis_balance", label: "Aegis Balance", render: (row) => escapeHtml(formatUsd(row.aegis_balance)) },
            { key: "investable_total", label: "Investable Total", render: (row) => escapeHtml(formatUsd(row.investable_total)) },
            { key: "aegis_allocation_pct", label: "Aegis Pct", render: (row) => escapeHtml(formatPercent(row.aegis_allocation_pct)) },
          ],
          rows: aegisPoints,
          emptyMessage: "No Aegis-allocation history points were returned.",
        }),
      }),
    ].join(""),
    contextHtml: [
      renderCardSection({
        eyebrow: "Basis",
        title: "History Basis",
        subtitle: "Time series uses append-only snapshots; no hand-maintained totals are stored.",
        body: renderDefinitionRows([
          { label: "As-of date", value: basis.as_of_date || "n/a" },
          { label: "Basis type", value: basis.basis_type || "latest_per_account_on_or_before_day" },
          { label: "Day count", value: String(basis.day_count ?? investablePoints.length) },
        ]),
      }),
      renderSourceRefCard(safeList(payload.source_refs), "Capital Source Evidence", "Capital bounded-domain storage and derived projection references."),
    ].join(""),
  };
}

async function renderCapitalFlowsPage() {
  const payload = await fetchCapitalFlows();
  const rows = safeList(payload.rows);
  const periodRows = safeList(payload.summary_by_period?.rows);
  const basis = payload.basis || {};
  return {
    title: "Capital Flows",
    meta: "Append-only external contribution/withdrawal records and period summaries.",
    html: [
      renderCardSection({
        eyebrow: "Flows",
        title: "Flow Records",
        subtitle: "Flows are append-only and distinct from balance snapshots.",
        body: renderSimpleTable({
          columns: [
            { key: "flow_date", label: "Flow Date" },
            { key: "account_id", label: "Account ID" },
            { key: "flow_type", label: "Type" },
            { key: "flow_amount", label: "Amount", render: (row) => escapeHtml(formatUsd(row.flow_amount)) },
            { key: "notes", label: "Notes" },
            { key: "created_at", label: "Recorded At", render: (row) => formatTimestamp(row.created_at) },
          ],
          rows,
          emptyMessage: "No flow rows were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Flows",
        title: "Flow Summary By Month",
        subtitle: "Net flows and record counts by period are derived from append-only flow facts.",
        body: renderSimpleTable({
          columns: [
            { key: "period", label: "Period" },
            { key: "net_flow_amount", label: "Net Flow", render: (row) => escapeHtml(formatUsd(row.net_flow_amount)) },
            { key: "flow_count", label: "Flow Count" },
          ],
          rows: periodRows,
          emptyMessage: "No monthly flow summary rows were returned.",
        }),
      }),
    ].join(""),
    contextHtml: [
      renderCardSection({
        eyebrow: "Basis",
        title: "Flow Basis",
        subtitle: "Flow visibility is independent from allocation inclusion logic.",
        body: renderDefinitionRows([
          { label: "As-of date", value: basis.as_of_date || "n/a" },
          { label: "Basis type", value: basis.basis_type || "append_only_flows" },
          { label: "Row count", value: String(basis.row_count ?? rows.length) },
        ]),
      }),
      renderSourceRefCard(safeList(payload.source_refs), "Capital Source Evidence", "Capital bounded-domain storage and derived projection references."),
    ].join(""),
  };
}

async function renderCapitalCashflowPage(state = {}) {
  const query = new URLSearchParams(window.location.search || "");
  const scenarioRaw = String(query.get("scenario") || "florida").toLowerCase();
  const scenario = ["base", "florida", "chile"].includes(scenarioRaw) ? scenarioRaw : "florida";
  const includeInheritance = ["1", "true", "yes", "on"].includes(String(query.get("include_inheritance") || "").toLowerCase());
  const payload = await fetchCapitalCashflow({
    scenario,
    include_inheritance: includeInheritance ? "true" : "false",
  });
  const rows = safeList(payload.monthly_projection);
  const validation = payload.validation || {};
  const findings = safeList(validation.findings);
  const basis = payload.basis || {};
  const operatorConsole = payload.operator_console || {};
  const safety = operatorConsole.safety || {};
  const weakestMonth = operatorConsole.weakest_month || {};
  const failure = operatorConsole.failure || {};
  const drivers = operatorConsole.drivers || {};
  const trust = operatorConsole.trust || {};
  const semantics = state.semantics || {};
  const safetyAnswer = safety.answer || "UNKNOWN";
  const safetyLabel = {
    SAFE_WITHIN_HORIZON: "Safe",
    SAFE_BUT_DEGRADED: "Tight",
    NOT_SAFE: "Not Safe",
  }[safetyAnswer] || safetyAnswer;
  const safetySemantic = safetyAnswer === "SAFE_WITHIN_HORIZON" ? "healthy" : safetyAnswer === "SAFE_BUT_DEGRADED" ? "warning" : "blocked";
  const failureStatus = failure.status || "UNKNOWN";
  const failureSemantic = failureStatus === "NO_FAILURE_WITHIN_HORIZON" ? "healthy" : "blocked";
  const failureLabel = failure.month ? failure.month : failureStatus === "NO_FAILURE_WITHIN_HORIZON" ? "No failure" : "Fail closed";
  const weakestNet = typeof weakestMonth.net === "number" ? weakestMonth.net : null;
  const driverRows = safeList(drivers.contributors);
  const kernelUsd = (value) => (typeof value === "number" ? formatUsd(value) : "n/a");
  const kernelSignedUsd = (value) => (typeof value === "number" ? formatSignedUsd(value) : "n/a");
  const scenarioHref = (nextScenario) => {
    const search = new URLSearchParams();
    search.set("scenario", nextScenario);
    if (includeInheritance) {
      search.set("include_inheritance", "true");
    }
    return `/capital/cashflow?${search.toString()}`;
  };
  const includeToggleHref = () => {
    const search = new URLSearchParams();
    search.set("scenario", scenario);
    if (!includeInheritance) {
      search.set("include_inheritance", "true");
    }
    const suffix = search.toString();
    return `/capital/cashflow${suffix ? `?${suffix}` : ""}`;
  };
  return {
    title: "Capital Cashflow Console",
    meta: "Deterministic projection-kernel console for safety, weakest month, failure point, drivers, and calculation trust.",
    html: [
      renderCardSection({
        eyebrow: "Scenario",
        title: "Projection Inputs",
        subtitle: "Scenario controls only select the kernel inputs. The UI does not calculate the projection.",
        body: `
          <div class="chip-list">
            <a class="support-chip chip-button" href="${escapeHtml(scenarioHref("florida"))}" data-route="${escapeHtml(scenarioHref("florida"))}">Florida</a>
            <a class="support-chip chip-button" href="${escapeHtml(scenarioHref("chile"))}" data-route="${escapeHtml(scenarioHref("chile"))}">Chile</a>
            <a class="support-chip chip-button" href="${escapeHtml(scenarioHref("base"))}" data-route="${escapeHtml(scenarioHref("base"))}">Base</a>
            <a class="support-chip chip-button" href="${escapeHtml(includeToggleHref())}" data-route="${escapeHtml(includeToggleHref())}">
              Include inheritance: ${includeInheritance ? "ON" : "OFF"}
            </a>
          </div>
        `,
      }),
      renderCardSection({
        eyebrow: "Console",
        title: "Operator Answers",
        subtitle: "These answers are rendered from the projection kernel payload.",
        body: `<div class="metric-grid">
          ${renderMetricCard({ label: "Scenario", value: scenario.toUpperCase() })}
          ${renderMetricCard({ label: "Am I safe?", value: safetyLabel, semantic: safetySemantic, semantics })}
          ${renderMetricCard({
            label: "Weakest month",
            value: weakestMonth.month || "n/a",
            detail: weakestNet === null ? "Net n/a" : `Net ${formatSignedUsd(weakestNet)}`,
          })}
          ${renderMetricCard({
            label: "Failure point",
            value: failureLabel,
            detail: failure.condition || "n/a",
            semantic: failureSemantic,
            semantics,
          })}
        </div>`,
      }),
      renderCardSection({
        eyebrow: "Drivers",
        title: "Weakest-Month Drivers",
        subtitle: "Contributor rows come from the same projection row that produced the weakest month.",
        body: driverRows.length
          ? renderSimpleTable({
              columns: [
                { key: "event_name", label: "Event" },
                { key: "event_type", label: "Type" },
                { key: "scenario", label: "Scenario" },
                { key: "amount", label: "Amount", render: (row) => escapeHtml(kernelUsd(row.amount)) },
                { key: "frequency", label: "Frequency" },
                { key: "is_deterministic", label: "Deterministic", render: (row) => escapeHtml(truthyText(row.is_deterministic)) },
              ],
              rows: driverRows,
              emptyMessage: "No driver rows were returned by the projection kernel.",
            })
          : `<div class="empty-state">No driver rows were returned by the projection kernel.</div>`,
      }),
      renderCardSection({
        eyebrow: "Projection",
        title: "Kernel Monthly Rows",
        subtitle: "Every displayed amount is a field returned by the projection kernel.",
        body: renderSimpleTable({
          columns: [
            { key: "month", label: "Month" },
            { key: "income", label: "Income", render: (row) => escapeHtml(kernelUsd(row.income)) },
            { key: "expenses", label: "Expenses", render: (row) => escapeHtml(kernelUsd(row.expenses)) },
            { key: "net", label: "Net", render: (row) => escapeHtml(kernelSignedUsd(row.net)) },
            { key: "cumulative", label: "Cumulative", render: (row) => escapeHtml(kernelSignedUsd(row.cumulative)) },
          ],
          rows,
          emptyMessage: "No monthly projection rows were returned.",
        }),
      }),
    ].join(""),
    contextHtml: [
      renderCardSection({
        eyebrow: "Basis",
        title: "Calculation Trust",
        subtitle: "Authority, basis, validation status, and UI calculation policy are explicit.",
        body: renderDefinitionRows([
          { label: "Calculation authority", value: trust.calculation_authority || "CapitalDomainServiceV1.cashflow_projection" },
          { label: "Projection view", value: trust.projection_view || basis.report_basis || "v_capital_cashflow_projection_v1" },
          { label: "Source table", value: trust.source_table || "capital_cashflow_events_v1" },
          { label: "Basis description", value: basis.basis_description || "n/a" },
          { label: "Deterministic only", value: truthyText(trust.deterministic_only ?? basis.deterministic_only) },
          { label: "Inheritance excluded", value: truthyText(trust.inheritance_excluded ?? basis.inheritance_excluded) },
          { label: "Scenario scope", value: safeList(trust.scenario_scope || basis.scenario_scope).join(", ") || "n/a" },
          { label: "Start month", value: trust.start_month || basis.start_month || "n/a" },
          { label: "Horizon (months)", value: String(trust.horizon_months ?? basis.horizon_months ?? "n/a") },
          { label: "Event count", value: String(trust.event_count ?? basis.event_count ?? "0") },
          { label: "Validation status", value: trust.validation_status || validation.status || "n/a" },
          { label: "UI policy", value: trust.ui_calculation_policy || "UI renders projection-kernel fields and does not calculate financial truth." },
        ]),
      }),
      renderCardSection({
        eyebrow: "Validation",
        title: "Cashflow Validation Findings",
        subtitle: "Projection validity and risk findings are explicit and machine-derived.",
        body: findings.length
          ? renderSimpleTable({
              columns: [
                { key: "severity", label: "Severity" },
                { key: "code", label: "Code" },
                { key: "message", label: "Message" },
              ],
              rows: findings,
              emptyMessage: "No cashflow findings were returned.",
            })
          : `<div class="empty-state">No cashflow validation findings.</div>`,
      }),
      renderSourceRefCard(safeList(payload.source_refs), "Capital Source Evidence", "Capital bounded-domain storage and derived projection references."),
    ].join(""),
  };
}

async function renderCapitalValidationPage() {
  const payload = await fetchCapitalValidation();
  const validation = payload.validation || {};
  const findings = safeList(validation.findings);
  const basis = payload.basis || {};
  return {
    title: "Capital Validation",
    meta: "Validation failures are explicit, severity-ranked, and account-traceable.",
    html: [
      renderCardSection({
        eyebrow: "Validation",
        title: "Validation Status",
        subtitle: "Data quality state is exposed directly instead of hidden in chart rendering.",
        body: `<div class="metric-grid">
          ${renderMetricCard({ label: "Status", value: validation.status || "UNKNOWN" })}
          ${renderMetricCard({ label: "Findings", value: String(validation.error_count ?? 0) })}
          ${renderMetricCard({ label: "Critical", value: String((validation.severity_counts || {}).CRITICAL || 0) })}
          ${renderMetricCard({ label: "Warnings", value: String((validation.severity_counts || {}).WARNING || 0) })}
        </div>`,
      }),
      renderCardSection({
        eyebrow: "Findings",
        title: "Current Validation Findings",
        subtitle: "Severity, codes, impacted accounts, and messages are all backend-derived.",
        body: renderSimpleTable({
          columns: [
            { key: "severity", label: "Severity" },
            { key: "code", label: "Code" },
            { key: "message", label: "Message" },
            { key: "account_ids", label: "Impacted Accounts", render: (row) => escapeHtml(safeList(row.account_ids).join(", ") || "none") },
          ],
          rows: findings,
          emptyMessage: "No active validation findings.",
        }),
      }),
    ].join(""),
    contextHtml: [
      renderCardSection({
        eyebrow: "Basis",
        title: "Validation Basis",
        subtitle: "Validation uses effective-dated semantic state and latest snapshot coverage checks.",
        body: renderDefinitionRows([
          { label: "As-of date", value: basis.as_of_date || validation.as_of_date || "n/a" },
          { label: "Basis type", value: basis.basis_type || "validation_projection" },
          { label: "Finding count", value: String(basis.finding_count ?? findings.length) },
        ]),
      }),
      renderSourceRefCard(safeList(payload.source_refs), "Capital Source Evidence", "Capital bounded-domain storage and derived projection references."),
    ].join(""),
  };
}

function formatPercent(value) {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return "n/a";
  }
  return `${(value * 100).toFixed(2)}%`;
}

async function renderSleevesPage() {
  const sleevesPayload = await fetchSleeves();
  const valuePayload = await fetchValue();
  const sleeves = safeList(sleevesPayload.sleeves);
  const evaluatedSleeves = sleeves.filter((row) => row.recommendation?.recommendation_state !== "unavailable").length;
  const sleeveReadinessSummary = sleevesPayload.sleeve_live_readiness_summary || {};
  const summaryGrade = intOrNull(sleeveReadinessSummary.readiness_grade_1_to_7);
  const summaryThresholdGrade = intOrNull(sleeveReadinessSummary.score_threshold_grade_1_to_7);
  const summaryReason = inlineText(sleeveReadinessSummary.diagnostic_reason);
  const summaryNextStep = inlineText(sleeveReadinessSummary.diagnostic_next_step);
  const summaryWhy = sleeveReadinessWhyText({
    readiness_score: sleeveReadinessSummary.readiness_score,
    readiness_score_threshold: sleeveReadinessSummary.score_threshold,
    readiness_contributing_factors: sleeveReadinessSummary.contributing_factors,
    readiness_grade_reason: summaryReason,
    readiness_grade_next_step: summaryNextStep,
  });
  const summaryThresholdStatus = sleeveThresholdStatusLabel(summaryGrade, summaryThresholdGrade);

  return {
    title: "Sleeves",
    meta: "Canonical sleeve evaluation state from backend-owned policy, allocation, measurement, and governance artifacts.",
    html: [
      renderCardSection({
        eyebrow: "Sleeves",
        title: "Sleeve Command",
        subtitle: "The domain now renders a backend sleeve-evaluation contract instead of a generic blocked page.",
        body: `<div class="metric-grid">
          ${renderMetricCard({ label: "Sleeves", value: String(sleevesPayload.sleeve_registry_summary?.total_sleeves ?? 0), detail: `As of ${formatTimestamp(sleevesPayload.as_of_utc)}` })}
          ${renderMetricCard({ label: "Evaluated", value: String(evaluatedSleeves) })}
          ${renderMetricCard({ label: "Truth state", value: sleevesPayload.truth_state || "UNKNOWN" })}
          ${renderMetricCard({ label: "Current evaluation day", value: sleevesPayload.current_day || "UNKNOWN" })}
        </div>`,
      }),
      renderCardSection({
        eyebrow: "Readiness",
        title: "Sleeve Live Readiness Summary",
        subtitle: "Advisory-only sleeve grade and threshold status for Paper-to-Live promotion readiness.",
        body: `
          <div class="metric-grid">
            ${renderMetricCard({ label: "Sleeve readiness grade", value: summaryGrade === null ? "n/a" : `${summaryGrade} / 7` })}
            ${renderMetricCard({ label: "Promotion threshold", value: summaryThresholdGrade === null ? "n/a" : `${summaryThresholdGrade} / 7` })}
            ${renderMetricCard({ label: "Threshold status", value: summaryThresholdStatus })}
            ${renderMetricCard({ label: "Promotion candidate", value: typeof sleeveReadinessSummary.promotion_candidate === "boolean" ? truthyText(Boolean(sleeveReadinessSummary.promotion_candidate)) : "unknown" })}
          </div>
          <div style="margin-top:10px;">
            <span title="${escapeHtml(summaryWhy)}" style="font-size:12px;text-decoration:underline;text-decoration-style:dotted;cursor:help;">Why</span>
          </div>
          ${summaryReason ? `<div style="margin-top:8px;font-size:12px;opacity:0.82;">Reason: ${escapeHtml(summaryReason)}</div>` : ""}
          ${summaryNextStep ? `<div style="margin-top:4px;font-size:12px;opacity:0.82;">Next step: ${escapeHtml(summaryNextStep)}</div>` : ""}
        `,
      }),
      renderSimpleTable({
        columns: [
          { key: "display_name", label: "Sleeve" },
          { key: "sleeve_id", label: "ID" },
          { key: "priority_rank", label: "Priority" },
          { key: "actual_allocation_pct", label: "Actual Allocation", render: (row) => escapeHtml(formatPercent(row.actual_allocation_pct)) },
          { key: "effective_risk_budget_usd", label: "Risk Budget", render: (row) => escapeHtml(formatUsd(row.effective_risk_budget_usd)) },
          { key: "qualification_state", label: "Qualification" },
          { key: "readiness_grade_1_to_7", label: "Readiness Grade", render: (row) => renderSleeveReadinessCell(row) },
          { key: "edge_band", label: "Edge Band" },
          { key: "recommendation", label: "Recommendation", render: (row) => escapeHtml(row.recommendation?.recommendation_state || "unavailable") },
        ],
        rows: sleeves,
        emptyMessage: "No sleeves were returned by the sleeve-evaluation projection.",
      }),
      renderCardSection({
        eyebrow: "Measurement Gaps",
        title: "Explicitly Blocked Fields",
        subtitle: "Target allocations, tax efficiency, realized-return metrics, and non-risk budgets stay unavailable until backend authorities materialize them.",
        body: renderDefinitionRows([
          { label: "Target allocation policy", value: "backend gap" },
          { label: "Tax efficiency summary", value: "backend gap" },
          { label: "Realized value metrics", value: "served by governed value summary" },
          { label: "Non-risk budget", value: "backend gap" },
        ]),
      }),
      renderCardSection({
        eyebrow: "Value",
        title: "Sleeve Value Summary",
        subtitle: "Sleeve usefulness is rendered from governed value artifacts only and weakens when sleeve linkage is only execution-scope-level.",
        body: renderSimpleTable({
          columns: [
            { key: "sleeve_id", label: "Sleeve" },
            { key: "linkage_states", label: "Linkage" },
            { key: "value_rows", label: "Value rows" },
            { key: "observed_fact_count", label: "Observed fact" },
            { key: "bounded_association_count", label: "Bounded association" },
            { key: "withheld_count", label: "Withheld" },
          ],
          rows: safeList(valuePayload.sleeve_contribution_summary),
          emptyMessage: "No governed sleeve value summary was returned.",
        }),
      }),
    ].join(""),
    contextHtml: [
      renderSourceRefCard(safeList(sleevesPayload.source_refs), "Sleeve Evidence", "Policy, allocation, measurement, and governance refs used by the sleeve-evaluation projection."),
      renderCardSection({
        eyebrow: "Warnings",
        title: "Sleeve Warnings",
        subtitle: "Explicit backend gaps and missing materializations remain visible instead of being papered over in the UI.",
        body: safeList(sleevesPayload.sleeve_warnings).length
          ? `<div class="chip-list">${safeList(sleevesPayload.sleeve_warnings).map((warning) => `<span class="support-chip">${escapeHtml(warning)}</span>`).join("")}</div>`
          : `<div class="empty-state">No sleeve warnings were returned.</div>`,
      }),
    ].join(""),
  };
}

async function renderTaxPage() {
  const taxState = await fetchTax();
  const profiles = safeList(taxState.account_tax_profiles);
  const lots = safeList(taxState.lot_level_entries?.items);
  const harvestCandidates = safeList(taxState.harvesting_candidates?.items);
  const advisoryImpacts = safeList(taxState.advisory_impacts);

  return {
    title: "Tax",
    meta: "Governed deterministic tax state derived from canonical runtime truth.",
    html: [
      renderCardSection({
        eyebrow: "Tax",
        title: "Tax Command Center",
        subtitle: "The tax page renders the governed tax state artifact only. Blockers, opportunities, and advisory tax impact stay explicit.",
        body: `<div class="metric-grid">
          ${renderMetricCard({ label: "Tax status", value: taxState.tax_status || "UNKNOWN", detail: `As of ${formatTimestamp(taxState.as_of_utc)}` })}
          ${renderMetricCard({ label: "Completeness", value: taxState.completeness_state || "UNKNOWN" })}
          ${renderMetricCard({ label: "Freshness", value: taxState.freshness_state || "UNKNOWN" })}
          ${renderMetricCard({ label: "Profiles", value: String(profiles.length) })}
          ${renderMetricCard({ label: "Lots", value: String(taxState.lot_level_entries?.total_lots ?? 0) })}
          ${renderMetricCard({ label: "Harvest candidates", value: String(taxState.harvesting_candidates?.candidate_count ?? 0) })}
        </div>`,
      }),
      renderCardSection({
        eyebrow: "Tax Summary",
        title: "Current Tax State",
        subtitle: "Completeness, freshness, blocker, and advisory-binding states come from the canonical tax artifact.",
        body: renderDefinitionRows([
          { label: "Lot basis state", value: taxState.lot_basis_state || "UNKNOWN" },
          { label: "Holding-period state", value: taxState.holding_period_state || "UNKNOWN" },
          { label: "Wash-sale state", value: taxState.wash_sale_state || "UNKNOWN" },
          { label: "Realized/unrealized posture", value: taxState.realized_unrealized_tax_posture?.status || "UNKNOWN" },
          { label: "Advisory effect", value: taxState.advisory_binding_state?.effect_state || "UNKNOWN" },
          { label: "Primary rule", value: taxState.primary_rule_id || "UNKNOWN" },
        ]),
      }),
      renderSimpleTable({
        columns: [
          { key: "account_id", label: "Account" },
          { key: "environment", label: "Environment" },
          { key: "tax_profile_state", label: "Tax Profile" },
          { key: "lot_basis_state", label: "Lot Basis" },
          { key: "holding_period_state", label: "Holding Period" },
        ],
        rows: profiles,
        emptyMessage: "No account-level tax profiles were returned.",
      }),
      renderCardSection({
        eyebrow: "Tax Signals",
        title: "Current Blockers and Opportunities",
        subtitle: "Opportunity visibility is governed by the tax precedence matrix and never inferred locally.",
        body: renderDefinitionRows([
          { label: "Blockers", value: safeList(taxState.blocker_states).join(", ") || "None" },
          { label: "Opportunities", value: safeList(taxState.opportunity_states).join(", ") || "None" },
          { label: "Lot entries", value: `${taxState.lot_level_entries?.status || "UNKNOWN"} (${lots.length})` },
          { label: "Harvest candidates", value: `${taxState.harvesting_candidates?.status || "UNKNOWN"} (${harvestCandidates.length})` },
        ]),
      }),
      renderSimpleTable({
        columns: [
          { key: "lot_id", label: "Lot" },
          { key: "security_id", label: "Security" },
          { key: "account_id", label: "Account" },
          { key: "holding_period_state", label: "Holding Period" },
          { key: "basis_total", label: "Basis" },
        ],
        rows: lots,
        emptyMessage: "No governed tax lots were returned.",
      }),
      renderSimpleTable({
        columns: [
          { key: "lot_id", label: "Lot" },
          { key: "security_id", label: "Security" },
          { key: "account_id", label: "Account" },
          { key: "unrealized_loss_amount", label: "Unrealized Loss" },
          { key: "policy_eligibility_result", label: "Eligibility" },
        ],
        rows: harvestCandidates,
        emptyMessage: "No governed harvest candidates were returned.",
      }),
      renderSimpleTable({
        columns: [
          { key: "advisory_item_id", label: "Advisory item" },
          { key: "decision_state", label: "Decision" },
          { key: "actionability_state", label: "Actionability" },
          { key: "freshness_state", label: "Freshness" },
        ],
        rows: advisoryImpacts,
        emptyMessage: "No advisory decisions currently bind to this tax state.",
      }),
    ].join(""),
    contextHtml: [
      renderSourceRefCard(safeList(taxState.source_refs), "Tax Evidence", "Current runtime/economic refs backing the tax-state projection."),
      renderCardSection({
        eyebrow: "Warnings",
        title: "Tax Warnings",
        subtitle: "The current tax rule and blocker set stay visible rather than being hidden behind a placeholder seam.",
        body: safeList(taxState.tax_warnings).length
          ? `<div class="chip-list">${safeList(taxState.tax_warnings).map((warning) => `<span class="support-chip">${escapeHtml(warning)}</span>`).join("")}</div>`
          : `<div class="empty-state">No tax warnings were returned.</div>`,
      }),
    ].join(""),
  };
}

async function renderOpportunitiesPage() {
  const opportunities = await fetchOpportunities();
  return {
    title: "Opportunities",
    meta: "Governed proactive opportunities and review deltas derived from certified truth.",
    html: [
      renderCardSection({
        eyebrow: "Opportunity Plane",
        title: "Top Opportunities Now",
        subtitle: "This page renders the governed Bundle 11 opportunity artifacts only.",
        body: renderSimpleTable({
          columns: [
            { key: "opportunity_type", label: "Type" },
            { key: "opportunity_state", label: "State" },
            { key: "review_priority", label: "Priority" },
            { key: "delta_state", label: "Delta" },
            { key: "summary_message", label: "Summary" },
          ],
          rows: safeList(opportunities.top_opportunities),
          emptyMessage: "No governed opportunities were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Review Delta",
        title: "Changed Since Last Review",
        subtitle: "Delta rows come from the governed review snapshot, not UI-local comparison logic.",
        body: renderSimpleTable({
          columns: [
            { key: "opportunity_type", label: "Type" },
            { key: "delta_state", label: "Delta" },
            { key: "review_priority", label: "Priority" },
            { key: "summary_message", label: "Summary" },
          ],
          rows: safeList(opportunities.changed_since_last_review),
          emptyMessage: "No changed opportunities were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Blocked Items",
        title: "Blocked But Important",
        subtitle: "Blocked opportunities remain visible when review-critical.",
        body: renderSimpleTable({
          columns: [
            { key: "opportunity_type", label: "Type" },
            { key: "review_priority", label: "Priority" },
            { key: "blocker_states", label: "Blockers", render: (row) => escapeHtml(safeList(row.blocker_states).join(", ") || "None") },
            { key: "summary_message", label: "Summary" },
          ],
          rows: safeList(opportunities.blocked_items),
          emptyMessage: "No blocked review-critical opportunities were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Scenario Binding",
        title: "Scenario Comparisons That Matter",
        subtitle: "Scenario significance is bounded by the governed opportunity kernel.",
        body: renderSimpleTable({
          columns: [
            { key: "opportunity_type", label: "Type" },
            { key: "scenario_significance_state", label: "Scenario significance" },
            { key: "review_priority", label: "Priority" },
            { key: "summary_message", label: "Summary" },
          ],
          rows: safeList(opportunities.scenario_items),
          emptyMessage: "No material scenario comparisons were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Advisory Effect",
        title: "Opportunity Impact On Advisory",
        subtitle: "Advisory decisions may bind to opportunity artifacts but do not rank them locally.",
        body: renderSimpleTable({
          columns: [
            { key: "advisory_item_id", label: "Advisory item" },
            { key: "decision_state", label: "Decision" },
            { key: "actionability_state", label: "Actionability" },
            { key: "summary_message", label: "Summary" },
          ],
          rows: safeList(opportunities.advisory_impacts),
          emptyMessage: "No advisory decisions currently bind to opportunity state.",
        }),
      }),
    ].join(""),
    contextHtml: [
      renderSourceRefCard(safeList(opportunities.source_refs), "Opportunity Evidence", "Governed opportunity-state source refs."),
      renderCardSection({
        eyebrow: "Snapshot",
        title: "Review Snapshot Summary",
        subtitle: "Summary counts come from the current governed review snapshot.",
        body: renderDefinitionRows([
          { label: "Current", value: String(opportunities.review_snapshot_summary?.current_count ?? 0) },
          { label: "New", value: String(opportunities.review_snapshot_summary?.new_count ?? 0) },
          { label: "Changed", value: String(opportunities.review_snapshot_summary?.changed_count ?? 0) },
          { label: "Resolved", value: String(opportunities.review_snapshot_summary?.resolved_count ?? 0) },
          { label: "Review now", value: String(opportunities.review_snapshot_summary?.review_now_count ?? 0) },
        ]),
      }),
    ].join(""),
  };
}

async function renderOutcomesPage() {
  const valueState = await fetchValue();
  const rows = safeList(valueState.value_rows);
  return {
    title: "Value",
    meta: "A governed proof surface over realized truth, sleeve linkage, bounded attribution, and explicit claim strength.",
    html: [
      renderCardSection({
        eyebrow: "Value Summary",
        title: "Current Value Proof",
        subtitle: "This surface renders governed value artifacts only and distinguishes observed fact from attribution.",
        body: `
          <div class="metric-grid">
            ${renderMetricCard({ label: "Outcomes", value: String(rows.length) })}
            ${renderMetricCard({ label: "Observed fact", value: String((valueState.claim_strength_counts || {}).observed_fact || 0) })}
            ${renderMetricCard({ label: "Bounded association", value: String((valueState.claim_strength_counts || {}).bounded_association || 0) })}
            ${renderMetricCard({ label: "Withheld", value: String(((valueState.claim_strength_counts || {}).insufficient_evidence || 0) + ((valueState.claim_strength_counts || {}).not_yet_observable || 0)) })}
          </div>
        `,
      }),
      renderCardSection({
        eyebrow: "Scorecard",
        title: "Recommendation Effectiveness Scorecard",
        subtitle: "Effectiveness is governed and does not imply attribution or sleeve usefulness automatically.",
        body: renderSimpleTable({
          columns: [
            { key: "opportunity_type", label: "Subject" },
            { key: "sleeve_id", label: "Execution Scope" },
            { key: "realized_state", label: "Realized" },
            { key: "effectiveness_state", label: "Effectiveness" },
            { key: "attribution_state", label: "Attribution" },
            { key: "claim_strength", label: "Claim strength" },
          ],
          rows,
          emptyMessage: "No governed outcome artifacts were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Sleeves",
        title: "Sleeve Contribution / Usefulness Summary",
        subtitle: "Sleeve claims remain conservative and show execution-scope-only linkage explicitly when stronger economic sleeve attribution is not proven.",
        body: renderSimpleTable({
          columns: [
            { key: "sleeve_id", label: "Sleeve" },
            { key: "linkage_states", label: "Linkage" },
            { key: "value_rows", label: "Value rows" },
            { key: "observed_fact_count", label: "Observed fact" },
            { key: "bounded_association_count", label: "Bounded association" },
            { key: "withheld_count", label: "Withheld" },
          ],
          rows: safeList(valueState.sleeve_contribution_summary),
          emptyMessage: "No sleeve value summary was returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Tax Effect",
        title: "Tax Effect Summary",
        subtitle: "Tax-related outcomes are shown only where governed tax refs are present.",
        body: renderSimpleTable({
          columns: [
            { key: "opportunity_type", label: "Subject" },
            { key: "realized_state", label: "Realized" },
            { key: "claim_strength", label: "Claim strength" },
            { key: "primary_rule_id", label: "Rule" },
          ],
          rows: safeList(valueState.tax_effect_summary),
          emptyMessage: "No tax-related outcome proof was returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Missed",
        title: "Missed Opportunity Summary",
        subtitle: "Missed-outcome rows appear only when the governed outcome model supports them.",
        body: renderSimpleTable({
          columns: [
            { key: "opportunity_type", label: "Subject" },
            { key: "effectiveness_state", label: "Effectiveness" },
            { key: "claim_strength", label: "Claim strength" },
          ],
          rows: safeList(valueState.missed_opportunity_summary),
          emptyMessage: "No governed missed-opportunity rows were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Progress",
        title: "Bounded Progress Summary",
        subtitle: "Progress is shown as governed counts, not freeform performance storytelling.",
        body: renderDefinitionRows([
          { label: "Observed fact", value: String((valueState.bounded_progress_summary || {}).observed_fact_count || 0) },
          { label: "Bounded association", value: String((valueState.bounded_progress_summary || {}).bounded_association_count || 0) },
          { label: "Withheld", value: String((valueState.bounded_progress_summary || {}).withheld_count || 0) },
        ]),
      }),
    ].join(""),
    contextHtml: [
      renderSourceRefCard(safeList(valueState.source_refs), "Value Evidence", "Governed value refs backing the current proof surface."),
    ].join(""),
  };
}

async function renderPerformanceCockpitPage() {
  const cockpitUrl = "/performance/cockpit.html";
  return {
    title: "Performance",
    meta: "Read-only generated Aegis Performance Cockpit. Loaded only when this route is opened.",
    html: renderCardSection({
      eyebrow: "Generated Report",
      title: "AEGIS Performance Cockpit",
      subtitle: "The cockpit remains a standalone generated HTML artifact and is embedded here without merging it into shell logic.",
      body: `
        <div class="line-list">
          <div>Source: <code>${escapeHtml(cockpitUrl)}</code></div>
          <div><a href="${escapeHtml(cockpitUrl)}" target="_blank" rel="noopener noreferrer">Open Performance Cockpit</a></div>
        </div>
        <iframe
          title="AEGIS Performance Cockpit"
          src="${escapeHtml(cockpitUrl)}"
          loading="lazy"
          style="width:100%;height:78vh;border:1px solid var(--border-subtle);border-radius:8px;background:#090b10;"
        ></iframe>
      `,
    }),
    contextHtml: renderCardSection({
      eyebrow: "Boundary",
      title: "Read-only Artifact View",
      subtitle: "This route does not produce trading evidence, mutate artifacts, or alter readiness logic.",
      body: renderDefinitionRows([
        { label: "Route", value: "/performance" },
        { label: "Cockpit HTML", value: cockpitUrl },
        { label: "Load behavior", value: "lazy iframe on route open" },
      ]),
    }),
  };
}

async function renderRefinementPage() {
  const refinement = await fetchRefinement();
  return {
    title: "Refinement",
    meta: "Governed refinement proof over top-level preservation, compression, demotion, and drill-down retention.",
    html: [
      renderCardSection({
        eyebrow: "Refinement",
        title: "Current Refinement Summary",
        subtitle: "This page renders governed refinement artifacts only and preserves before/after visibility provenance.",
        body: `<div class="metric-grid">
          ${renderMetricCard({ label: "Top level", value: String(safeList(refinement.top_level_items).length) })}
          ${renderMetricCard({ label: "Compressed", value: String(safeList(refinement.compressed_items).length) })}
          ${renderMetricCard({ label: "Secondary", value: String(safeList(refinement.secondary_items).length) })}
          ${renderMetricCard({ label: "Withheld", value: String(safeList(refinement.withheld_items).length) })}
        </div>`,
      }),
      renderCardSection({
        eyebrow: "Proof",
        title: "Refinement Decisions",
        subtitle: "Each row shows what changed, why it changed, and whether the change is reversible.",
        body: renderSimpleTable({
          columns: [
            { key: "target_label", label: "Target" },
            { key: "before_state", label: "Before", render: (row) => escapeHtml(row.before_state?.surface_bucket || "UNKNOWN") },
            { key: "after_state", label: "After", render: (row) => escapeHtml(row.after_state?.surface_bucket || "UNKNOWN") },
            { key: "refinement_action", label: "Action" },
            { key: "reversibility_state", label: "Reversible" },
            { key: "summary_message", label: "Summary" },
          ],
          rows: safeList(refinement.proof_rows),
          emptyMessage: "No refinement proof rows were returned.",
        }),
      }),
    ].join(""),
    contextHtml: [
      renderSourceRefCard(safeList(refinement.source_refs), "Refinement Evidence", "Governed refinement refs backing the current product simplification decisions."),
    ].join(""),
  };
}

async function renderPolicyPage() {
  const policy = await fetchPolicyEvolution();
  return {
    title: "Policy Evolution",
    meta: "Governed temporal policy proposals over refinement and product emphasis, with explicit expiry, rollback, and trust overrides.",
    html: [
      renderCardSection({
        eyebrow: "Policy Evolution",
        title: "Current Active Policy Evolutions",
        subtitle: "This page renders governed policy-evolution artifacts only and never derives temporal tuning locally.",
        body: `<div class="metric-grid">
          ${renderMetricCard({ label: "Active", value: String(safeList(policy.active_policy_evolutions).length) })}
          ${renderMetricCard({ label: "Withheld", value: String(safeList(policy.withheld_items).length) })}
          ${renderMetricCard({ label: "Expiring", value: String(safeList(policy.expiring_items).length) })}
          ${renderMetricCard({ label: "Rollback", value: String(safeList(policy.rollback_items).length) })}
        </div>`,
      }),
      renderCardSection({
        eyebrow: "Active",
        title: "Current Policy Actions",
        subtitle: "Policy proposals remain explicit snapshots rather than hidden product drift.",
        body: renderSimpleTable({
          columns: [
            { key: "target_label", label: "Target" },
            { key: "proposed_policy_change", label: "Action", render: (row) => escapeHtml(row.proposed_policy_change?.action || "UNKNOWN") },
            { key: "threshold_result", label: "Threshold" },
            { key: "summary_message", label: "Summary" },
          ],
          rows: safeList(policy.active_policy_evolutions),
          emptyMessage: "No active policy-evolution rows were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Withheld",
        title: "Withheld Evolutions",
        subtitle: "Insufficient or unstable history results in explicit withholding instead of silent adaptation.",
        body: renderSimpleTable({
          columns: [
            { key: "target_label", label: "Target" },
            { key: "threshold_result", label: "Threshold" },
            { key: "summary_message", label: "Summary" },
          ],
          rows: safeList(policy.withheld_items),
          emptyMessage: "No withheld policy-evolution rows were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Expiry",
        title: "Expiring Or Re-Review Needed",
        subtitle: "Policy proposals expire explicitly when history goes stale.",
        body: renderSimpleTable({
          columns: [
            { key: "target_label", label: "Target" },
            { key: "expiry_state", label: "Expiry" },
            { key: "summary_message", label: "Summary" },
          ],
          rows: safeList(policy.expiring_items),
          emptyMessage: "No expiring policy-evolution rows were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Rollback",
        title: "Rollback Candidates",
        subtitle: "Trust-preserving overrides can force rollback to a safer prior policy.",
        body: renderSimpleTable({
          columns: [
            { key: "target_label", label: "Target" },
            { key: "trust_override_state", label: "Override" },
            { key: "summary_message", label: "Summary" },
          ],
          rows: safeList(policy.rollback_items),
          emptyMessage: "No rollback policy-evolution rows were returned.",
        }),
      }),
    ].join(""),
    contextHtml: [
      renderSourceRefCard(safeList(policy.source_refs), "Policy Evidence", "Governed policy-evolution refs backing the current temporal policy surface."),
      renderCardSection({
        eyebrow: "Before / After",
        title: "Before And After Policy Comparison",
        subtitle: "Before/after policy state and reversibility are explicit and auditable.",
        body: renderSimpleTable({
          columns: [
            { key: "target_label", label: "Target" },
            { key: "before_policy_state", label: "Before", render: (row) => escapeHtml(row.before_policy_state?.visibility_effect || "UNKNOWN") },
            { key: "after_policy_state", label: "After", render: (row) => escapeHtml(row.after_policy_state?.visibility_effect || "UNKNOWN") },
            { key: "reversibility_state", label: "Reversible" },
            { key: "drill_down_route", label: "Drill-down" },
          ],
          rows: safeList(policy.proof_rows),
          emptyMessage: "No policy-evolution proof rows were returned.",
        }),
      }),
    ].join(""),
  };
}

async function renderAdvisoryPage() {
  const advisory = await fetchAdvisory({ summary: 1 });
  const decisions = safeList(advisory.decisions);
  return {
    title: "Advisory",
    meta: "Governed advisory decision states derived from certified truth.",
    html: [
      renderCardSection({
        eyebrow: "Advisory",
        title: "Decision Set",
        subtitle: "Backend-authored advisory decisions with preserved authority, lineage, and invalidation state.",
        body: renderSimpleTable({
          columns: [
            { key: "advisory_item_id", label: "Advisory item" },
            { key: "decision_state", label: "Decision" },
            { key: "actionability_state", label: "Actionability" },
            { key: "freshness_state", label: "Freshness" },
            { key: "visibility_state", label: "Visibility" },
            { key: "promotion_eligibility_state", label: "Promotion eligibility" },
          ],
          rows: decisions,
          emptyMessage: "No advisory decisions were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Decision Basis",
        title: "Primary Decision Explanation",
        subtitle: "Explanation is table-driven and governance-bound to the advisory decision object.",
        body: renderDefinitionRows([
          { label: "Decision state", value: advisory.current_decision?.decision_state || "UNKNOWN" },
          { label: "Actionability", value: advisory.current_decision?.actionability_state || "UNKNOWN" },
          { label: "Reason", value: advisory.current_decision?.summary_message || "No decision explanation provided." },
        ]),
      }),
    ].join(""),
    contextHtml: [
      renderCardSection({
        eyebrow: "Evidence",
        title: "Advisory Evidence",
        subtitle: "Evidence refs are loaded on demand so the advisory shell is not blocked by artifact hydration.",
        body: `
          <button class="evidence-button" type="button" data-load-advisory-evidence>Load evidence refs</button>
          <div id="advisoryEvidenceRefs" class="evidence-lazy-slot empty-state">Evidence refs not loaded.</div>
        `,
      }),
      renderCardSection({
        eyebrow: "Warnings",
        title: "Advisory Warnings",
        subtitle: "Cross-surface drift or availability warnings raised by the advisory read model.",
        body: safeList(advisory.advisory_warnings).length
          ? `<div class="chip-list">${safeList(advisory.advisory_warnings).map((warning) => `<span class="support-chip">${escapeHtml(warning)}</span>`).join("")}</div>`
          : `<div class="empty-state">No advisory warnings were returned.</div>`,
      }),
    ].join(""),
  };
}

async function renderOperationsPage(state) {
  const [operations, readinessKernel, alertsPayload, integrity, actions, statusV2Payload, sleevesPayload] = await Promise.all([
    fetchOperations(),
    fetchReadinessKernel().catch(() => ({
      overall_status: "UNKNOWN",
      canonical_blocker: "READINESS_KERNEL_UNAVAILABLE",
      layers: [],
      warnings: [{ code: "READINESS_KERNEL_UNAVAILABLE" }],
    })),
    fetchAlerts(),
    fetchIntegrity(),
    fetchSystemActions(),
    fetchStatusV2().catch(() => ({})),
    fetchSleeves().catch(() => ({})),
  ]);
  const statusV2SleeveReadiness = statusV2Payload?.sleeve_live_readiness || {};
  const sleevesSummaryReadiness = sleevesPayload?.sleeve_live_readiness_summary || {};
  const statusV2HasGrade =
    intOrNull(statusV2SleeveReadiness.readiness_grade_1_to_7) !== null ||
    intOrNull(statusV2SleeveReadiness.score_threshold_grade_1_to_7) !== null;
  const sleeveReadiness = statusV2HasGrade ? statusV2SleeveReadiness : sleevesSummaryReadiness;
  const opsFactors = safeList(sleeveReadiness?.calibration_support?.score_contribution)
    .map((row) => {
      if (!row || typeof row !== "object") {
        return "";
      }
      const checkId = inlineText(row.check_id || "check");
      const scoreAwarded = intOrNull(row.score_awarded);
      const weight = intOrNull(row.weight);
      if (scoreAwarded !== null && weight !== null) {
        return `${checkId}:${scoreAwarded}/${weight}`;
      }
      if (scoreAwarded !== null) {
        return `${checkId}:${scoreAwarded}`;
      }
      return checkId;
    })
    .filter(Boolean);
  const normalizedOpsFactors = opsFactors.length
    ? opsFactors
    : safeList(sleeveReadiness?.contributing_factors).map((item) => inlineText(item)).filter(Boolean);
  const opsReason = inlineText(sleeveReadiness.diagnostic_reason || sleeveReadiness.readiness_grade_reason);
  const opsNextStep = inlineText(sleeveReadiness.diagnostic_next_step || sleeveReadiness.readiness_grade_next_step);
  const opsGrade = intOrNull(sleeveReadiness.readiness_grade_1_to_7);
  const opsThresholdGrade = intOrNull(sleeveReadiness.score_threshold_grade_1_to_7);
  const opsWhy = sleeveReadinessWhyText({
    readiness_score: sleeveReadiness.readiness_score,
    readiness_score_threshold: sleeveReadiness.score_threshold,
    readiness_contributing_factors: normalizedOpsFactors,
    readiness_grade_reason: opsReason,
    readiness_grade_next_step: opsNextStep,
  });
  const opsThresholdStatus = sleeveThresholdStatusLabel(opsGrade, opsThresholdGrade);

  return {
    title: "Operations",
    meta: "Readiness ladders, runtime blocks, alert state, and governed actions on one operational surface.",
    html: [
      renderCardSection({
        eyebrow: "Readiness",
        title: "Phase-Controlled Readiness",
        subtitle: "Rendered from aegis_control_plane_v1; requirement graph, kernel, day-run, submit boundary, action validity, and packet are supporting evidence only.",
        body: renderReadinessKernelLadder(readinessKernel, state),
      }),
      renderCardSection({
        eyebrow: "Readiness",
        title: "Sleeve Promotion Readiness",
        subtitle: "Advisory sleeve grade and threshold status from sleeve live-readiness artifacts.",
        body: `
          <div class="metric-grid">
            ${renderMetricCard({ label: "Sleeve readiness grade", value: opsGrade === null ? "n/a" : `${opsGrade} / 7` })}
            ${renderMetricCard({ label: "Promotion threshold", value: opsThresholdGrade === null ? "n/a" : `${opsThresholdGrade} / 7` })}
            ${renderMetricCard({ label: "Threshold status", value: opsThresholdStatus })}
            ${renderMetricCard({ label: "Promotion candidate", value: typeof sleeveReadiness.promotion_candidate === "boolean" ? truthyText(Boolean(sleeveReadiness.promotion_candidate)) : "unknown" })}
          </div>
          <div style="margin-top:10px;">
            <span title="${escapeHtml(opsWhy)}" style="font-size:12px;text-decoration:underline;text-decoration-style:dotted;cursor:help;">Why</span>
          </div>
          ${opsReason ? `<div style="margin-top:8px;font-size:12px;opacity:0.82;">Reason: ${escapeHtml(opsReason)}</div>` : ""}
          ${opsNextStep ? `<div style="margin-top:4px;font-size:12px;opacity:0.82;">Next step: ${escapeHtml(opsNextStep)}</div>` : ""}
        `,
      }),
      renderCardSection({
        eyebrow: "Blocking Conditions",
        title: "Current Blocks",
        subtitle: "Fail-closed runtime blockers and degradation codes surfaced by the backend operations view.",
        body: safeList(operations.blocking_conditions).length
          ? renderList(safeList(operations.blocking_conditions), {
              renderItem: (row) => `
                <article class="stack-card">
                  <div class="stack-card-header">
                    <div>
                      <div class="stack-card-title">${escapeHtml(row.source_name || "blocking_condition")}</div>
                      <div class="stack-card-subtitle">${escapeHtml(row.truth_state || "UNKNOWN")} / ${escapeHtml(row.data_condition || "UNKNOWN")}</div>
                    </div>
                    ${renderSemanticBadge(row.status || "UNKNOWN", row.semantic || "blocked", state.semantics)}
                  </div>
                  <div class="chip-list">${safeList(row.reason_codes).map((code) => `<span class="support-chip">${escapeHtml(code)}</span>`).join("")}</div>
                </article>
              `,
            })
          : `<div class="empty-state">No blocking conditions are currently reported.</div>`,
      }),
      renderAlertSection(alertsPayload, state.semantics),
      renderCardSection({
        eyebrow: "Actions",
        title: "Governed Operator Actions",
        subtitle: "Only supported, repo-proven actions are presented as actionable.",
        body: renderSimpleTable({
          columns: [
            { key: "action_name", label: "Action" },
            { key: "supported", label: "Supported" },
            { key: "reason", label: "Reason" },
          ],
          rows: safeList(actions.actions),
          emptyMessage: "No operator actions are registered.",
        }),
      }),
    ].join(""),
    contextHtml: [
      renderCardSection({
        eyebrow: "Runtime Facts",
        title: "Session Authority Details",
        subtitle: "Authoritative session/runtime fields surfaced by the operations workspace.",
        body: renderDefinitionRows([
          { label: "Environment", value: operations.session_authority_details?.environment || "UNKNOWN" },
          { label: "Submission authorization", value: operations.session_authority_details?.submission_authorization_status || "UNKNOWN" },
          { label: "Recommended action", value: operations.session_authority_details?.recommended_operator_action || "None" },
          { label: "Broker connectivity", value: operations.broker_connectivity_summary?.status || "UNKNOWN" },
          { label: "Integrity", value: operations.integrity_summary?.status || "UNKNOWN" },
        ]),
      }),
      renderSourceRefCard(safeList(operations.source_refs), "Operations Evidence", "Source refs for operations, replay, handshake, and integrity surfaces."),
      renderCardSection({
        eyebrow: "Integrity",
        title: "Integrity Alerts",
        subtitle: "Integrity issues remain explicit and drillable.",
        body: safeList(integrity.integrity_alerts).length
          ? renderList(safeList(integrity.integrity_alerts).slice(0, 6), {
              renderItem: (item) => `
                <article class="stack-card">
                  <div class="stack-card-title">${escapeHtml(item.title || "Integrity issue")}</div>
                  <div class="stack-card-subtitle">${escapeHtml(item.summary || "No summary provided.")}</div>
                </article>
              `,
            })
          : `<div class="empty-state">No integrity alerts were returned.</div>`,
      }),
    ].join(""),
  };
}

async function renderAegisRuntimePage() {
  let payload;
  try {
    payload = await fetchRuntimeStatus();
    try {
      localStorage.setItem("aegis.runtime.lastKnownTruth.v1", JSON.stringify({
        saved_at: new Date().toISOString(),
        payload,
      }));
    } catch {
      // best-effort browser cache only
    }
  } catch (error) {
    let cached = null;
    try {
      cached = JSON.parse(localStorage.getItem("aegis.runtime.lastKnownTruth.v1") || "null");
    } catch {
      cached = null;
    }
    const last = cached?.payload || {};
    return {
      title: "Aegis Runtime",
      meta: "Backend unavailable; showing operator recovery guidance and last known truth when available.",
      html: [
        renderCardSection({
          eyebrow: "BACKEND_UNAVAILABLE",
          title: "Aegis Runtime Backend Unavailable",
          subtitle: "The frontend cannot reach the read-only projection API. Trading truth remains owned by canonical artifacts.",
          body: renderDefinitionRows([
            { label: "Connection state", value: window.__AEGIS_CONNECTION_STATE?.state || "BACKEND_UNAVAILABLE" },
            { label: "Recovery command", value: "npm run aegis:ui:restart" },
            { label: "Endpoint", value: error?.operatorSafe?.endpointAttempted || "/api/runtime-status" },
            { label: "Last successful refresh", value: cached?.saved_at || "none" },
          ]),
        }),
        last.final_status ? renderCardSection({
          eyebrow: "Last Known Truth",
          title: "Cached Aegis Projection",
          subtitle: "This is stale browser-held state and is not an authority.",
          body: `<div class="metric-grid">
            ${renderMetricCard({ label: "Final status", value: last.final_status || "UNKNOWN" })}
            ${renderMetricCard({ label: "Phase", value: last.canonical_phase || "n/a" })}
            ${renderMetricCard({ label: "Blocker", value: last.canonical_blocker || "none" })}
            ${renderMetricCard({ label: "Selected intent", value: last.selected_intent_id || "none" })}
          </div>`,
        }) : "",
      ].join(""),
      contextHtml: renderCardSection({
        eyebrow: "Recovery",
        title: "Operator Recovery",
        subtitle: "Restart the supervised UI service, then reload this route.",
        body: renderDefinitionRows([
          { label: "Command", value: "npm run aegis:ui:restart" },
          { label: "Health", value: "http://127.0.0.1:8787/healthz" },
          { label: "Readiness", value: "http://127.0.0.1:8787/readyz" },
        ]),
      }),
    };
  }

  const paths = payload.artifact_paths || {};
  return {
    title: "Aegis Runtime",
    meta: "Read-only projection over canonical runtime truth.",
    html: [
      renderCardSection({
        eyebrow: payload.status || "UNKNOWN",
        title: "Aegis Runtime",
        subtitle: payload.operator_next_action || "No operator action reported.",
        body: `
          <div class="metric-grid">
            ${renderMetricCard({ label: "Final status", value: payload.final_status || "UNKNOWN", badge: aegisRuntimeStateBadge(payload.final_status) })}
            ${renderMetricCard({ label: "Canonical phase", value: payload.canonical_phase || "n/a" })}
            ${renderMetricCard({ label: "Canonical blocker", value: payload.canonical_blocker || "none" })}
            ${renderMetricCard({ label: "Selected intent", value: payload.selected_intent_id || "none" })}
            ${renderMetricCard({ label: "Source integrity", value: payload.source_integrity_status || "UNKNOWN" })}
            ${renderMetricCard({ label: "Portfolio state", value: payload.portfolio_state_status || "UNKNOWN" })}
            ${renderMetricCard({ label: "Portfolio scoring", value: payload.portfolio_scoring_status || "UNKNOWN" })}
            ${renderMetricCard({ label: "Last refresh", value: formatTimestamp(payload.generated_at_utc) })}
          </div>
        `,
      }),
      renderCardSection({
        eyebrow: "Projection",
        title: "Canonical Projection Inputs",
        subtitle: "The UI reads artifacts only; it does not own or mutate trading truth.",
        body: renderDefinitionRows([
          { label: "Projection contract", value: payload.projection_contract_version || "unknown" },
          { label: "Day", value: payload.day_utc || "UNKNOWN" },
          { label: "Truth root", value: payload.truth_root || "n/a" },
          { label: "Runtime truth root", value: payload.runtime_truth_root || "n/a" },
          { label: "Reason codes", value: safeList(payload.reason_codes).join(", ") || "none" },
        ]),
      }),
    ].join(""),
    contextHtml: [
      renderCardSection({
        eyebrow: "Evidence",
        title: "Projection Source Artifacts",
        subtitle: "All displayed values are read from canonical runtime artifacts.",
        body: renderDefinitionRows([
          { label: "Day run", value: paths.day_run || "n/a" },
          { label: "Selected pointer", value: paths.selected_intent_pointer || "n/a" },
          { label: "Portfolio state", value: paths.portfolio_state || "n/a" },
          { label: "Portfolio scoring", value: paths.portfolio_scoring || "n/a" },
          { label: "Decision ledger", value: paths.decision_ledger || "n/a" },
          { label: "Latest packet", value: paths.latest_packet || "n/a" },
        ]),
      }),
    ].join(""),
  };
}

async function renderAegisLiteQueuePage() {
  let payload;
  try {
    payload = await fetchAegisLiteExecutionQueue();
  } catch (error) {
    return {
      title: "Aegis Lite Queue",
      meta: "Backend unavailable; current Lite report truth could not be loaded.",
      html: renderCardSection({
        eyebrow: "NOT_READY",
        title: "No Current Lite Queue",
        subtitle: "The UI cannot reach the read-only Lite queue API.",
        body: renderDefinitionRows([
          { label: "Endpoint", value: error?.operatorSafe?.endpointAttempted || "/api/aegis/lite-execution-queue" },
          { label: "Manual execution status", value: "NOT_READY" },
          { label: "Broker submit required", value: "false" },
        ]),
      }),
      contextHtml: "",
    };
  }

  const executable = safeList(payload.executable_trades);
  const blocked = safeList(payload.blocked_or_advisory_trades);
  const summary = payload.queue_summary || {};
  const releaseMismatch = payload.release_match_status === "MISMATCH";
  const legacy = payload.operating_status?.legacy_paper_runtime_status || {};
  const degradedMessages = buildLiteDegradedMessages(payload);
  return {
    title: "Aegis Lite Queue",
    meta: "Manual-only EOD execution queue from current Aegis Lite report truth.",
    html: [
      renderCardSection({
        eyebrow: "MANUAL_ONLY",
        title: "THIS IS NOT BROKER AUTOMATION",
        subtitle: "Aegis Lite produces an operator checklist. David manually enters any supervised IB paper trade and stop.",
        body: renderDefinitionRows([
          { label: "Broker submit required", value: String(payload.broker_submit_required === true) },
          { label: "IB automation", value: payload.ib_automation_status || "DEFERRED" },
          { label: "Release match", value: payload.release_match_status || "UNKNOWN" },
          { label: "Ready disabled by mismatch", value: releaseMismatch ? "YES" : "NO" },
        ]),
      }),
      degradedMessages.length ? renderCardSection({
        eyebrow: "NOT_READY",
        title: "Lite Report Not Generated Yet",
        subtitle: "The operator surface is in degraded read-only mode and cannot authorize manual entry.",
        body: `
          <div class="callout danger">${degradedMessages.map((item) => escapeHtml(item)).join(" | ")}</div>
          ${renderDefinitionRows([
            { label: "Manual execution status", value: payload.manual_execution_status || "NOT_READY" },
            { label: "Readiness", value: payload.readiness_classification || "NOT_READY" },
            { label: "Broker submit required", value: String(payload.broker_submit_required === true) },
            { label: "Report", value: payload.report_path || "Runtime artifact unavailable" },
            { label: "Queue", value: payload.queue_path || "Runtime artifact unavailable" },
          ])}
        `,
      }) : "",
      renderCardSection({
        eyebrow: payload.readiness_classification || "NOT_READY",
        title: "Aegis Lite Execution Queue",
        subtitle: executable.length ? `${executable.length} executable manual-paper candidate(s).` : "No executable manual-paper candidates.",
        body: `
          <div class="metric-grid">
            ${renderMetricCard({ label: "Executable", value: String(summary.executable_trades_count ?? executable.length) })}
            ${renderMetricCard({ label: "Blocked", value: String(summary.blocked_trades_count ?? blocked.length) })}
            ${renderMetricCard({ label: "Distinct edges", value: String(summary.distinct_edge_count ?? 0) })}
            ${renderMetricCard({ label: "Concentration warnings", value: String(summary.concentration_warnings ?? 0) })}
            ${renderMetricCard({ label: "Unprotected positions", value: String(summary.open_unprotected_positions ?? 0) })}
            ${renderMetricCard({ label: "Release match", value: summary.active_release_match_status || payload.release_match_status || "UNKNOWN" })}
            ${renderMetricCard({ label: "Manual status", value: payload.manual_execution_status || "NOT_READY" })}
            ${renderMetricCard({ label: "Generated", value: formatTimestamp(payload.generated_at) })}
          </div>
        `,
      }),
      renderCardSection({
        eyebrow: executable.length ? "READY_FOR_MANUAL_ENTRY" : "EMPTY",
        title: "Executable Manual Trades",
        subtitle: "Only cards in this section may be considered for supervised IB paper entry.",
        body: renderLiteTradeCards(executable, true),
      }),
      renderCardSection({
        eyebrow: blocked.length ? "BLOCKED_OR_ADVISORY" : "CLEAR",
        title: "Blocked / Advisory Candidates",
        subtitle: "These cards are not executable.",
        body: renderLiteTradeCards(blocked, false),
      }),
    ].join(""),
    contextHtml: [
      renderCardSection({
        eyebrow: "Evidence",
        title: "Lite Source Artifacts",
        subtitle: "The queue page reads current Lite report/status artifacts, not stale operator state.",
        body: renderDefinitionRows([
          { label: "Report", value: payload.report_path || "missing" },
          { label: "Queue", value: payload.queue_path || "missing" },
          { label: "Status", value: payload.artifact_path || "missing" },
          { label: "Current blockers", value: safeList(payload.current_blockers).join(", ") || "none" },
          { label: "Warnings", value: safeList(payload.warnings).join(", ") || "none" },
          { label: "Legacy operator state", value: payload.legacy_operator_state?.diagnostic_only ? "diagnostic only" : "not used" },
          { label: "Lite operational spine", value: legacy.lite_operational_spine_active === false ? "inactive" : "active" },
          { label: "Legacy PAPER runtime", value: legacy.legacy_runtime_active ? "active" : "deferred" },
        ]),
      }),
    ].join(""),
  };
}

async function renderAegisEventMonitoringPage() {
  let payload;
  try {
    payload = await fetchAegisEventMonitoring();
  } catch (error) {
    return {
      title: "Event Monitoring",
      meta: "Backend unavailable; event monitoring truth could not be loaded.",
      html: renderCardSection({
        eyebrow: "DEGRADED",
        title: "Event Monitoring Unavailable",
        subtitle: "The read-only event monitoring API is not reachable.",
        body: renderDefinitionRows([
          { label: "Endpoint", value: error?.operatorSafe?.endpointAttempted || "/api/aegis/event-monitoring" },
          { label: "Broker submit required", value: "false" },
          { label: "Canonical EOD mutated", value: "false" },
        ]),
      }),
      contextHtml: "",
    };
  }

  const rules = safeList(payload.event_rules);
  const ledgerEvents = safeList(payload.event_ledger?.events);
  const packets = safeList(payload.actionable_packets);
  const blockedPackets = safeList(payload.blocked_packets);
  const advisoryPackets = safeList(payload.advisory_packets);
  const expiredPackets = safeList(payload.expired_packets);
  const researchOnlyPackets = safeList(payload.research_only_packets);
  const monitor = payload.monitor_status || {};
  return {
    title: "Event Monitoring",
    meta: "Read-only Event Rules / Monitor / Ledger surface. Event runs are non-canonical and manual-only.",
    html: [
      renderCardSection({
        eyebrow: "MANUAL_ONLY",
        title: "THIS IS NOT BROKER AUTOMATION",
        subtitle: "Event monitoring can alert the operator only after validity and alert gates pass. It never submits orders.",
        body: renderDefinitionRows([
          { label: "Email transport", value: payload.email_transport_status || "GATE_ONLY_NO_TRANSPORT" },
          { label: "SMS transport", value: payload.sms_transport_status || "GATE_ONLY_NO_TRANSPORT" },
          { label: "Broker submit required", value: String(payload.broker_submit_required === true) },
          { label: "Canonical EOD mutated", value: String(payload.canonical_eod_state_mutated === true) },
        ]),
      }),
      renderCardSection({
        eyebrow: "Monitor",
        title: "Event Monitor Status",
        subtitle: "Latest monitor run, data freshness, evaluated rules, blocked events, and alert results.",
        body: `
          <div class="metric-grid">
            ${renderMetricCard({ label: "Last run", value: monitor.monitor_run_id || "none" })}
            ${renderMetricCard({ label: "Rules evaluated", value: String(safeList(monitor.event_rule_ids_evaluated).length) })}
            ${renderMetricCard({ label: "Triggered", value: String(safeList(monitor.triggered_events).length) })}
            ${renderMetricCard({ label: "Blocked", value: String(safeList(monitor.blocked_events).length) })}
            ${renderMetricCard({ label: "Packets", value: String(safeList(monitor.tactical_packets_created).length) })}
            ${renderMetricCard({ label: "Generated", value: formatTimestamp(monitor.timestamp_utc) })}
          </div>
          ${renderDefinitionRows([
            { label: "Data snapshots", value: safeList(monitor.data_snapshot_refs).join(", ") || "n/a" },
            { label: "Alert gate results", value: safeList(monitor.alert_gate_results).join(", ") || "none" },
            { label: "Email delivery", value: safeList(monitor.email_delivery_results).join(", ") || "none" },
          ])}
        `,
      }),
      renderCardSection({
        eyebrow: "Rules",
        title: "Event Rules Registry",
        subtitle: "Business rules are visible here and not buried in monitor code.",
        body: renderSimpleTable({
          columns: [
            { label: "Event", key: "event_type" },
            { label: "Rule", key: "event_rule_id" },
            { label: "Enabled", key: "enabled_status" },
            { label: "Production", key: "production_status" },
            { label: "Research", key: "research_status" },
            { label: "Required inputs", render: (row) => escapeHtml(safeList(row.required_inputs).join(", ")) },
          ],
          rows: rules,
          emptyMessage: "No event rules registry loaded.",
        }),
      }),
      renderCardSection({
        eyebrow: "Ledger",
        title: "Event Awareness Ledger",
        subtitle: "Detected and blocked events with rule version, reason codes, tactical packet status, and alert gate state.",
        body: renderSimpleTable({
          columns: [
            { label: "Event", key: "event_type" },
            { label: "Level", key: "alert_level" },
            { label: "Rule", key: "event_rule_id" },
            { label: "Severity", key: "severity" },
            { label: "Confidence", key: "confidence" },
            { label: "Validity", key: "validity_gate_status" },
            { label: "Alert gate", key: "alert_gate_status" },
            { label: "Email", key: "email_delivery_status" },
          ],
          rows: ledgerEvents,
          emptyMessage: "No event ledger entries for this day.",
        }),
      }),
      renderCardSection({
        eyebrow: packets.length ? "ACTIONABLE_PACKET_DETAILS" : "NO_ACTIONABLE_PACKETS",
        title: "Actionable Event Packets",
        subtitle: packets.length ? "Manual packet details for validity-gated event opportunities." : "No validity-gated event tactical packets are currently available.",
        body: packets.map(renderEventPacketRows).join("") || `<div class="empty-state">No packets available.</div>`,
      }),
      renderCardSection({
        eyebrow: "NON_ACTIONABLE_EVENT_PACKETS",
        title: "Blocked / Advisory Event Packets",
        subtitle: "Packets that are demo, dry-run, research-only, stale, invalid, or not fully gate-approved.",
        body: [
          renderEventPacketTable("Blocked", blockedPackets),
          renderEventPacketTable("Expired", expiredPackets),
          renderEventPacketTable("Research only", researchOnlyPackets),
          renderEventPacketTable("Advisory", advisoryPackets),
        ].join(""),
      }),
    ].join(""),
    contextHtml: renderCardSection({
      eyebrow: "Learning",
      title: "Event Outcome / Research Learning",
      subtitle: "Event outcomes may create offline Research Lab tasks only.",
      body: renderDefinitionRows([
        { label: "Research boundary", value: payload.research_learning_boundary || "offline learning only" },
        { label: "Workflow", value: safeList(payload.operator_workflow).join(" | ") },
      ]),
    }),
  };
}

function renderEventPacketRows(packet) {
  return renderDefinitionRows([
    { label: "Runtime truth", value: packet.runtime_truth_classification || "UNKNOWN" },
    { label: "Symbol", value: packet.symbol || "n/a" },
    { label: "Side", value: packet.side || "n/a" },
    { label: "Entry reference", value: packet.entry_reference_price || "n/a" },
    { label: "Sizing", value: packet.quantity_or_sizing_guidance || "n/a" },
    { label: "Stop", value: packet.stop_price || packet.stop_logic || "n/a" },
    { label: "Risk", value: packet.risk_per_trade || "n/a" },
    { label: "Valid until", value: formatTimestamp(packet.valid_until) },
    { label: "Max slippage", value: packet.max_entry_slippage || "n/a" },
    { label: "Validity gate", value: packet.validity_gate_status || "NOT_RUN" },
    { label: "Alert gate", value: packet.alert_gate_status || "NOT_RUN" },
    { label: "Invalidation", value: safeList(packet.invalidation_conditions).join(", ") || "n/a" },
  ]);
}

function renderEventPacketTable(title, rows) {
  return `
    <h3>${escapeHtml(title)}</h3>
    ${renderSimpleTable({
      columns: [
        { label: "Runtime", key: "runtime_truth_classification" },
        { label: "Event", key: "event_type" },
        { label: "Symbol", key: "symbol" },
        { label: "Validity", key: "validity_gate_status" },
        { label: "Alert gate", key: "alert_gate_status" },
        { label: "Blockers", render: (row) => escapeHtml([...safeList(row.validity_gate_blockers), ...safeList(row.alert_gate_blockers)].join(", ") || "none") },
      ],
      rows,
      emptyMessage: `No ${title.toLowerCase()} packets.`,
    })}
  `;
}

function buildLiteDegradedMessages(payload) {
  const blockers = safeList(payload.current_blockers);
  const messages = [];
  if ((payload.readiness_classification || "NOT_READY") === "NOT_READY" || blockers.length) {
    if (!payload.report_path || blockers.includes("NO_CURRENT_LITE_REPORT")) messages.push("No current Lite report");
    if (!payload.queue_path || blockers.includes("NO_CURRENT_LITE_QUEUE")) messages.push("No executable queue");
    if (blockers.includes("STATUS_UNAVAILABLE")) messages.push("Lite report not generated yet");
    if (blockers.includes("FILE_NOT_FOUND") || blockers.includes("JSON_DECODE_ERROR") || blockers.includes("READ_ERROR")) {
      messages.push("Runtime artifact unavailable");
    }
  }
  return [...new Set(messages)];
}

function renderLiteTradeCards(cards, executable) {
  const rows = safeList(cards);
  if (!rows.length) {
    return `<div class="empty-state">${executable ? "No executable Lite trades." : "No blocked or advisory candidates."}</div>`;
  }
  return renderList(rows, { renderItem: (card) => renderLiteTradeCard(card, executable) });
}

function renderLiteTradeCard(card, executable) {
  const blockers = safeList(card.do_not_trade_blockers);
  return `
    <article class="stack-card" style="${executable ? "border-left:4px solid #16a34a;" : "border-left:4px solid #dc2626; opacity:.92;"}">
      <div class="stack-card-title">${escapeHtml(card.execution_order || "")}. ${escapeHtml(card.symbol || "UNKNOWN")} ${escapeHtml(card.side || "")}</div>
      <div class="stack-card-subtitle">${escapeHtml(card.trade_class || "UNKNOWN")} · ${escapeHtml(card.sleeve_owner || "UNKNOWN")} · ${escapeHtml(card.edge_cluster_id || "NO_EDGE_CLUSTER")}</div>
      <div class="metric-grid" style="margin-top:12px;">
        ${renderMetricCard({ label: "Priority", value: String(card.priority_rank || "") })}
        ${renderMetricCard({ label: "Direction", value: card.direction || "UNKNOWN" })}
        ${renderMetricCard({ label: "Quantity", value: String(card.quantity || 0) })}
        ${renderMetricCard({ label: "Entry", value: card.entry_instruction || "missing" })}
        ${renderMetricCard({ label: "Stop", value: card.stop_price || "missing" })}
        ${renderMetricCard({ label: "Stop qty/type", value: `${card.stop_quantity || 0} ${card.stop_order_type || "STP"}` })}
        ${renderMetricCard({ label: "Risk", value: card.risk_per_trade || "missing" })}
        ${renderMetricCard({ label: "Governance", value: card.governance_recommendation || "UNKNOWN" })}
        ${renderMetricCard({ label: "Promotion", value: card.promotion_status || "UNKNOWN" })}
        ${renderMetricCard({ label: "Report time", value: formatTimestamp(card.report_timestamp) })}
      </div>
      <div style="margin-top:12px;">
        ${renderStatusPill(executable ? "EXECUTABLE_MANUAL_ONLY" : "DO_NOT_TRADE")}
        ${safeList(card.execution_confidence_badges).map((badge) => renderStatusPill(badge)).join(" ")}
      </div>
      ${blockers.length ? `<div class="callout danger" style="margin-top:12px;">${escapeHtml(blockers.join(", "))}</div>` : ""}
      <div class="definition-list" style="margin-top:12px;">
        ${renderDefinitionRows([
          { label: "Manual IB recipe", value: card.manual_ib_recipe || "missing" },
          { label: "Source sleeve", value: card.source_sleeve || "missing" },
          { label: "Edge cluster", value: card.edge_cluster_id || "missing" },
          { label: "Steps", value: safeList(card.operator_steps).join(" | ") },
        ])}
      </div>
    </article>
  `;
}

async function renderAuditPage() {
  const [audit, activity, homeBundle] = await Promise.all([
    fetchActionAudit(),
    fetchActivityToday(),
    fetchOperatorHome(),
  ]);

  return {
    title: "Audit",
    meta: "Operator action history, daily activity rollups, and trust/retrieval lineage.",
    html: [
      renderCardSection({
        eyebrow: "Audit",
        title: "Operator Action Ledger",
        subtitle: "Append-only action audit entries surfaced by the current UI API.",
        body: renderSimpleTable({
          columns: [
            { key: "time", label: "Time" },
            { key: "action_name", label: "Action" },
            { key: "result", label: "Result" },
            { key: "message", label: "Message" },
          ],
          rows: safeList(audit.audit_entries),
          emptyMessage: "No action audit entries were recorded.",
        }),
      }),
      renderCardSection({
        eyebrow: "Activity",
        title: "Day Activity Rollup",
        subtitle: "Monitoring-ledger rollup and summary counts for the active day.",
        body: renderDefinitionRows([
          { label: "Day", value: activity.day_utc || "UNKNOWN" },
          { label: "Intent summary present", value: activity.intents_summary ? "YES" : "NO" },
          { label: "Submission summary present", value: activity.submissions_summary ? "YES" : "NO" },
          { label: "Rollup present", value: activity.rollup_asof ? "YES" : "NO" },
          { label: "Warnings", value: String(safeList(activity.warnings).length) },
        ]),
      }),
    ].join(""),
    contextHtml: [
      renderTrustPanel(homeBundle.trust_panel || {}),
      renderCardSection({
        eyebrow: "Retrieval",
        title: "Home Retrieval Lineage",
        subtitle: "Retrieval manifest preserved alongside the current home view.",
        body: renderDefinitionRows([
          { label: "Route classification", value: homeBundle.retrieval_manifest?.route_classification || "UNKNOWN" },
          { label: "Retrieval status", value: homeBundle.retrieval_manifest?.retrieval_status || "UNKNOWN" },
          { label: "Rejected artifacts", value: String(safeList(homeBundle.retrieval_manifest?.rejected_artifacts).length) },
        ]),
      }),
      renderSourceRefCard(toEvidenceRefs(activity.source_paths, "Activity source"), "Activity Sources", "Resolved source files used by the activity timeline endpoints."),
    ].join(""),
  };
}

async function renderReportsPage(state) {
  const [reconciliation, homeBundle] = await Promise.all([fetchReconciliation(), fetchOperatorHome()]);
  return {
    title: "Reports",
    meta: "Immutable report-like surfaces rendered from reconciliation and operator-home snapshot contracts.",
    html: [
      renderCardSection({
        eyebrow: "Reports",
        title: "Reconciliation Snapshot",
        subtitle: "Reconciliation remains an immutable report surface with explicit source refs and mismatch evidence.",
        body: `<div class="metric-grid">
          ${renderMetricCard({ label: "Execution reconciliation", value: reconciliation.execution_reconciliation_status || "UNKNOWN", detail: `As of ${formatTimestamp(reconciliation.as_of_utc)}` })}
          ${renderMetricCard({ label: "Fill ledger", value: reconciliation.fill_ledger_status || "UNKNOWN" })}
          ${renderMetricCard({ label: "Mismatches", value: String(safeList(reconciliation.mismatches).length) })}
          ${renderMetricCard({ label: "Truth state", value: reconciliation.truth_state || "UNKNOWN" })}
        </div>`,
      }),
      renderCardSection({
        eyebrow: "Snapshot",
        title: "Mismatch Review",
        subtitle: "Mismatches are preserved as evidence-backed rows, not recalculated in the UI.",
        body: renderSimpleTable({
          columns: [
            { key: "comparison_id", label: "Comparison" },
            { key: "status", label: "Status" },
            { key: "reason", label: "Reason" },
            { key: "truth_state", label: "Truth state" },
          ],
          rows: safeList(reconciliation.mismatches),
          emptyMessage: "No reconciliation mismatches were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Operator Home",
        title: "Home Snapshot Summary",
        subtitle: "The current governed home surface is also treated as an immutable report-like product view.",
        body: renderPanelRows(safeList(homeBundle.home_view?.rendered_panels), state.semantics),
      }),
    ].join(""),
    contextHtml: [
      renderSourceRefCard(safeList(reconciliation.source_refs), "Reconciliation Evidence", "Snapshot source refs from reconciliation views."),
      renderTrustPanel(homeBundle.trust_panel || {}),
    ].join(""),
  };
}

function _configFieldValue(proposed, current, name, fallback) {
  if (Object.prototype.hasOwnProperty.call(proposed || {}, name)) {
    return proposed[name];
  }
  if (Object.prototype.hasOwnProperty.call(current || {}, name)) {
    return current[name];
  }
  return fallback;
}

function _operatorParametersToText(rows = []) {
  return safeList(rows).map((row) => {
    const name = inlineText(row.parameter_name);
    const value = inlineText(row.value_text);
    return name && value ? `${name} = ${value}` : "";
  }).filter(Boolean).join("\n");
}

function _renderConfigurationCatalogInput(row, proposedValues, currentValues) {
  const key = inlineText(row.parameter_key || row.parameter_name);
  const label = inlineText(row.display_name || row.parameter_name || key);
  const type = inlineText(row.type || row.validation?.type || "text");
  const value = _configFieldValue(proposedValues, currentValues, key, row.default_value ?? "");
  const escapedValue = escapeHtml(String(value ?? ""));
  const minAttr = row.minimum !== undefined && row.minimum !== null ? ` min="${escapeHtml(String(row.minimum))}"` : "";
  const maxAttr = row.maximum !== undefined && row.maximum !== null ? ` max="${escapeHtml(String(row.maximum))}"` : "";
  if (type === "boolean") {
    return `<div class="line-list">
      <label>
        <input type="checkbox" name="${escapeHtml(key)}" value="true" ${Boolean(value) ? "checked" : ""} />
        ${escapeHtml(label)}
      </label>
    </div>`;
  }
  if (type === "enum") {
    return `<div class="line-list">
      <label for="cfg_${escapeHtml(key)}">${escapeHtml(label)}</label>
      <select id="cfg_${escapeHtml(key)}" name="${escapeHtml(key)}">
        ${safeList(row.allowed_values).map((item) => `<option value="${escapeHtml(String(item))}" ${String(item) === String(value) ? "selected" : ""}>${escapeHtml(String(item))}</option>`).join("")}
      </select>
    </div>`;
  }
  const inputType = type === "integer" ? "number" : type === "month" ? "month" : "text";
  const stepAttr = type === "integer" ? ` step="1"` : "";
  return `<div class="line-list">
    <label for="cfg_${escapeHtml(key)}">${escapeHtml(label)}</label>
    <input id="cfg_${escapeHtml(key)}" name="${escapeHtml(key)}" type="${inputType}"${minAttr}${maxAttr}${stepAttr} value="${escapedValue}" />
  </div>`;
}

async function renderConfigurationPage(state) {
  const [catalog, current] = await Promise.all([
    fetchConfigurationCatalog(),
    fetchConfigurationCurrent(),
  ]);
  const workflowState = state.configurationWorkflow || {};
  const explicitDraftId = String(
    workflowState.activeDraftId
    || workflowState.latestDraft?.draft_id
    || (new URLSearchParams(window.location.search || "")).get("draft_id")
    || "",
  ).trim();
  let draft = workflowState.latestDraft || null;
  let draftLoadError = null;
  if (explicitDraftId) {
    try {
      const draftPayload = await fetchConfigurationDraft(explicitDraftId);
      draft = draftPayload.draft || draft;
    } catch (error) {
      draftLoadError = error?.message || "Draft could not be loaded.";
    }
  }

  const currentValues = current.current_values || {};
  const proposedValues = draft?.proposed_values || {};
  const catalogFields = safeList(catalog.editable_fields);
  const scenario = String(
    _configFieldValue(proposedValues, currentValues, "scenario", "florida") || "florida",
  ).toLowerCase();
  const includeInheritance = Boolean(
    _configFieldValue(proposedValues, currentValues, "include_inheritance", false),
  );
  const horizonMonths = String(
    _configFieldValue(proposedValues, currentValues, "horizon_months", 24),
  );
  const startMonth = String(
    _configFieldValue(proposedValues, currentValues, "start_month", ""),
  );
  const operatorParameters = safeList(draft?.advisory_unmapped_parameters || []);
  const operatorParametersText = _operatorParametersToText(operatorParameters);
  const currentCatalogRows = catalogFields.map((row) => ({
    parameter: row.display_name || row.parameter_key,
    value: String(currentValues[row.parameter_key] ?? row.default_value ?? "n/a"),
    owner: row.owner_domain || row.owning_domain || "",
  }));
  const canRunDraftActions = Boolean(draft?.draft_id);
  const disableDraftAction = canRunDraftActions ? "" : "disabled";
  const validation = draft?.validation || {};
  const review = draft?.review || {};
  const activation = draft?.activation || {};
  const lastError = workflowState.lastError || null;

  return {
    title: "Configuration",
    meta: "Governed authority-input configuration using draft, validation, review, and explicit activation audit.",
    html: [
      renderCardSection({
        eyebrow: "Current",
        title: "Current Capital Cashflow Configuration",
        subtitle: "Current values come from active configuration state when available; fallback values are explicitly labeled.",
        body: renderDefinitionRows([
          { label: "State", value: current.status || "UNKNOWN" },
          { label: "Scenario", value: String(currentValues.scenario || "n/a") },
          { label: "Include inheritance", value: String(Boolean(currentValues.include_inheritance)) },
          { label: "Horizon (months)", value: String(currentValues.horizon_months ?? "n/a") },
          { label: "Start month", value: String(currentValues.start_month || "n/a") },
          { label: "Catalog path", value: catalog.catalog_path || "n/a" },
          { label: "Reason codes", value: safeList(current.reason_codes).join(", ") || "none" },
        ]),
      }),
      renderCardSection({
        eyebrow: "Catalog",
        title: "Editable Catalog Values",
        subtitle: "Only parameters present in the governed catalog can be drafted and activated.",
        body: renderSimpleTable({
          columns: [
            { key: "parameter", label: "Parameter" },
            { key: "value", label: "Current value" },
            { key: "owner", label: "Owner" },
          ],
          rows: currentCatalogRows,
          emptyMessage: "No editable catalog parameters are available.",
        }),
      }),
      renderCardSection({
        eyebrow: "Draft",
        title: "Catalog Draft Editor",
        subtitle: "Draft writes are non-authoritative until validation, review, and activation succeed.",
        body: `
          <form class="configuration-workflow-form" autocomplete="off">
            <input type="hidden" name="configuration_action" value="create_draft" />
            ${catalogFields.map((row) => _renderConfigurationCatalogInput(row, proposedValues, currentValues)).join("")}
            <div class="line-list">
              <label for="cfg_operator_parameters">Operator parameters</label>
              <textarea id="cfg_operator_parameters" name="operator_parameters_text" rows="6" placeholder="parameter.name = value">${escapeHtml(operatorParametersText)}</textarea>
            </div>
            <button type="submit">Create Draft</button>
          </form>
        `,
      }),
      renderCardSection({
        eyebrow: "Captured",
        title: "Operator Configuration Parameters",
        subtitle: "Draft-captured parameters are audited in the configuration source document.",
        body: renderSimpleTable({
          columns: [
            { key: "parameter_name", label: "Parameter" },
            { key: "value_kind", label: "Kind" },
            { key: "value_text", label: "Value" },
          ],
          rows: operatorParameters,
          emptyMessage: "No operator parameters are captured in the current draft/context.",
        }),
      }),
      renderCardSection({
        eyebrow: "Lifecycle",
        title: "Draft Lifecycle Actions",
        subtitle: "Actions are explicit and gated; activation is blocked until validation and review succeed.",
        body: `
          <div class="line-list">
            <div>Draft id: ${escapeHtml(draft?.draft_id || "none")}</div>
            <div>Status: ${escapeHtml(draft?.status || "DRAFT_NOT_CREATED")}</div>
          </div>
          <form class="configuration-workflow-form">
            <input type="hidden" name="configuration_action" value="validate_draft" />
            <input type="hidden" name="draft_id" value="${escapeHtml(draft?.draft_id || "")}" />
            <button type="submit" ${disableDraftAction}>Validate Draft</button>
          </form>
          <form class="configuration-workflow-form">
            <input type="hidden" name="configuration_action" value="review_draft" />
            <input type="hidden" name="draft_id" value="${escapeHtml(draft?.draft_id || "")}" />
            <button type="submit" ${disableDraftAction}>Review Draft</button>
          </form>
          <form class="configuration-workflow-form">
            <input type="hidden" name="configuration_action" value="activate_draft" />
            <input type="hidden" name="draft_id" value="${escapeHtml(draft?.draft_id || "")}" />
            <button type="submit" ${disableDraftAction}>Activate Draft</button>
          </form>
          <form class="configuration-workflow-form">
            <input type="hidden" name="configuration_action" value="reject_draft" />
            <input type="hidden" name="draft_id" value="${escapeHtml(draft?.draft_id || "")}" />
            <input type="text" name="reject_reason" placeholder="rejection reason (optional)" />
            <button type="submit" ${disableDraftAction}>Reject Draft</button>
          </form>
        `,
      }),
      renderCardSection({
        eyebrow: "Validation",
        title: "Validation Result",
        subtitle: "Validation must return PASS before review/activation.",
        body: renderDefinitionRows([
          { label: "Status", value: validation.status || "NOT_RUN" },
          { label: "Reason codes", value: safeList(validation.reason_codes).join(", ") || "none" },
        ]),
      }),
      renderCardSection({
        eyebrow: "Review",
        title: "Review Summary",
        subtitle: "Review shows exact proposed values and the explicit field delta from current values.",
        body: renderSimpleTable({
          columns: [
            { key: "field", label: "Field" },
            { key: "current_value", label: "Current" },
            { key: "proposed_value", label: "Proposed" },
          ],
          rows: safeList(review.changed_fields),
          emptyMessage: "No review delta is available.",
        }),
      }),
      renderCardSection({
        eyebrow: "Activation",
        title: "Activation Result",
        subtitle: "Activation writes audited configuration activation artifacts and current state reference.",
        body: renderDefinitionRows([
          { label: "Status", value: activation.status || "NOT_ACTIVATED" },
          { label: "Audit evidence path", value: activation.audit_evidence_path || "n/a" },
          { label: "Configuration state path", value: activation.configuration_state_path || "n/a" },
        ]),
      }),
      lastError
        ? renderCardSection({
            eyebrow: "Error",
            title: "Last Configuration Action Error",
            subtitle: "Action errors are explicit and fail closed.",
            body: `<div class="error-state">${escapeHtml(String(lastError))}</div>`,
          })
        : "",
      draftLoadError
        ? renderCardSection({
            eyebrow: "Warning",
            title: "Draft Retrieval Warning",
            subtitle: "The configured draft id could not be loaded.",
            body: `<div class="error-state">${escapeHtml(String(draftLoadError))}</div>`,
          })
        : "",
    ].join(""),
    contextHtml: [
      renderCardSection({
        eyebrow: "Locked",
        title: "Locked-By-Design Safety Fields",
        subtitle: "Kill switch, broker arming, submission authorization, and readiness attestations remain non-editable from this UI surface.",
        body: renderSimpleTable({
          columns: [
            { key: "parameter_name", label: "Parameter" },
            { key: "lock_class", label: "Lock Class" },
            { key: "reason", label: "Reason" },
          ],
          rows: safeList(catalog.locked_fields),
          emptyMessage: "No locked field declarations were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Coverage",
        title: "Configuration Coverage",
        subtitle: "Coverage classifies parameters as editable, visible-only, or locked-by-design.",
        body: renderSimpleTable({
          columns: [
            { key: "parameter_name", label: "Parameter" },
            { key: "state", label: "UI State" },
            { key: "owning_domain", label: "Domain" },
            { key: "reason", label: "Reason" },
          ],
          rows: safeList(catalog.coverage),
          emptyMessage: "No coverage rows were returned.",
        }),
      }),
    ].join(""),
  };
}

function openIssueStatus(issue = {}) {
  return ["open", "triaged", "fix_proposed", "fix_in_progress", "fix_submitted", "verification_pending"].includes(
    String(issue.status || ""),
  );
}

async function renderReliabilityDashboardPage(state) {
  const [latest, issuesPayload, workOrdersPayload, fixAttemptsPayload, verificationsPayload, nextActionsPayload] = await Promise.all([
    fetchLatestReliabilityReadiness(),
    fetchReliabilityIssues(),
    fetchReliabilityWorkOrders(),
    fetchReliabilityFixAttempts(),
    fetchReliabilityVerifications(),
    fetchReliabilityNextActions(),
  ]);
  const assessment = latest.assessment || null;
  const issues = safeList(issuesPayload.issues);
  const workOrders = safeList(workOrdersPayload.work_orders);
  const fixAttempts = safeList(fixAttemptsPayload.fix_attempts);
  const verifications = safeList(verificationsPayload.verifications);
  const nextActions = safeList(nextActionsPayload.actions);
  const openIssues = issues.filter((item) => openIssueStatus(item));
  const openCritical = openIssues.filter((item) => item.severity === "critical");
  const openHigh = openIssues.filter((item) => item.severity === "high");
  const openReadinessBlockers = openIssues.filter((item) => Boolean(item.readiness_blocker));
  const recurringIssues = openIssues.filter((item) => Number(item.occurrence_count || 0) > 1);
  const unverifiedFixes = issues.filter(
    (item) => ["fix_submitted", "verification_pending"].includes(String(item.status || ""))
      || (item.resolved_at && !item.verified_at),
  );
  const blockingIds = safeList(assessment?.blocking_issue_ids);
  const blockingIssues = issues.filter((item) => blockingIds.includes(item.id)).slice(0, 8);
  const queuedWorkOrders = workOrders.filter((item) => String(item.status || "") === "queued");
  const failedFixAttempts = fixAttempts.filter((item) => String(item.status || "") === "tests_failed");
  const awaitingVerification = fixAttempts.filter((item) => ["patch_submitted", "tests_passed"].includes(String(item.status || "")));
  const verifiedReadyToClose = issues.filter((item) => String(item.status || "") === "verified");
  const nextRecommended = nextActions[0] || null;
  const recentAction = state.reliabilityWorkflow?.lastAction || "";
  const recentMessage = state.reliabilityWorkflow?.lastError
    ? `Last action error: ${state.reliabilityWorkflow.lastError}`
    : recentAction
      ? `Last action: ${recentAction}`
      : "No reliability workflow action has been run from this session.";

  return {
    title: "Reliability Dashboard",
    meta: "Deterministic rule-based readiness result with explicit blockers and verification debt.",
    html: [
      renderCardSection({
        eyebrow: "Readiness",
        title: "Current Reliability Readiness",
        subtitle: "Readiness is derived from deterministic rules and persisted issue state only.",
        body: `<div class="metric-grid">
          ${renderMetricCard({ label: "Status", value: assessment?.status || "not_assessed" })}
          ${renderMetricCard({ label: "Score", value: String(assessment?.score ?? "n/a") })}
          ${renderMetricCard({ label: "Open critical", value: String(openCritical.length) })}
          ${renderMetricCard({ label: "Open high", value: String(openHigh.length) })}
          ${renderMetricCard({ label: "Open blockers", value: String(openReadinessBlockers.length) })}
          ${renderMetricCard({ label: "Recurring open", value: String(recurringIssues.length) })}
          ${renderMetricCard({ label: "Unverified fixes", value: String(unverifiedFixes.length) })}
          ${renderMetricCard({ label: "Queued work orders", value: String(queuedWorkOrders.length) })}
          ${renderMetricCard({ label: "Failed fix attempts", value: String(failedFixAttempts.length) })}
          ${renderMetricCard({ label: "Awaiting verification", value: String(awaitingVerification.length) })}
          ${renderMetricCard({ label: "Verified ready to close", value: String(verifiedReadyToClose.length) })}
          ${renderMetricCard({ label: "Blocking issue refs", value: String(blockingIds.length) })}
        </div>`,
      }),
      renderCardSection({
        eyebrow: "Reason",
        title: "Latest Assessment Reason",
        subtitle: "Explains why the current readiness state was assigned.",
        body: `
          <div class="line-list">
            <div>${escapeHtml(assessment?.decision_reason || "No readiness assessment exists yet.")}</div>
            <div>Assessment id: ${escapeHtml(assessment?.id || "n/a")}</div>
            <div>Rule version: ${escapeHtml(assessment?.rule_version || "n/a")}</div>
            <div>Generated by: ${escapeHtml(assessment?.generated_by || "n/a")}</div>
            <div>Next recommended action: ${escapeHtml(nextRecommended?.action_type || "none")}</div>
            <div>${escapeHtml(recentMessage)}</div>
          </div>
          <form class="reliability-action-form">
            <input type="hidden" name="reliability_action" value="assess_readiness" />
            <button type="submit">Run Fresh Assessment</button>
          </form>
        `,
      }),
      renderCardSection({
        eyebrow: "Blockers",
        title: "Top Blocking Issues",
        subtitle: "These issues currently prevent readiness according to the latest assessment.",
        body: renderSimpleTable({
          columns: [
            {
              key: "id",
              label: "Issue",
              render: (row) => {
                const route = `/reliability/issues/detail?issue_id=${encodeURIComponent(String(row.id || ""))}`;
                const title = String(row.title || "").trim();
                const label = title
                  ? `${title} (${shortIssueId(row.id)})`
                  : shortIssueId(row.id);
                return `<a href="${escapeHtml(route)}" data-route="${escapeHtml(route)}">${truncatedCell(label, 300)}</a>`;
              },
            },
            { key: "severity", label: "Severity", render: (row) => truncatedCell(row.severity || "n/a", 100) },
            { key: "status", label: "Status", render: (row) => reliabilityStatusBadge(row.status) },
            { key: "category", label: "Category", render: (row) => truncatedCell(row.category || "n/a", 130) },
            { key: "readiness_blocker", label: "Readiness", render: (row) => readinessBlockerBadge(Boolean(row.readiness_blocker)) },
          ],
          rows: blockingIssues,
          emptyMessage: "No blocking issue ids are attached to the latest readiness assessment.",
        }),
      }),
    ].join(""),
    contextHtml: [
      renderCardSection({
        eyebrow: "Rules",
        title: "Rule Evaluation Results",
        subtitle: "Every rule is persisted with pass/fail and explicit details.",
        body: renderSimpleTable({
          columns: [
            { key: "rule_id", label: "Rule" },
            { key: "rule_name", label: "Rule Name" },
            { key: "passed", label: "Passed", render: (row) => (row.passed ? "yes" : "no") },
            {
              key: "blocking_issue_ids",
              label: "Blocking IDs",
              render: (row) => escapeHtml(safeList(row.blocking_issue_ids).map((value) => shortIssueId(value)).join(", ") || "none"),
            },
          ],
          rows: safeList(latest.rule_results),
          emptyMessage: "No rule results are available.",
        }),
      }),
      renderCardSection({
        eyebrow: "Operator Questions",
        title: "Operational Answers",
        subtitle: "The ledger surfaces what is open, why not ready, and what remains unverified.",
        body: renderDefinitionRows([
          { label: "Open issues", value: String(openIssues.length) },
          { label: "Blocking live-trading issues", value: String(openReadinessBlockers.length) },
          { label: "Codex fixes awaiting verification", value: String(unverifiedFixes.length) },
          { label: "Work orders queued", value: String(queuedWorkOrders.length) },
          { label: "Failed fix attempts", value: String(failedFixAttempts.length) },
          { label: "Verification records", value: String(verifications.length) },
          { label: "Latest assessment timestamp", value: assessment?.created_at || "n/a" },
        ]),
      }),
      renderCardSection({
        eyebrow: "Next Actions",
        title: "Prioritized Queue",
        subtitle: "Operator-visible next steps generated from reliability workflow state.",
        body: renderSimpleTable({
          columns: [
            { key: "priority", label: "Priority" },
            { key: "action_type", label: "Action" },
            { key: "issue_id", label: "Issue", render: (row) => escapeHtml(shortIssueId(row.issue_id)) },
            { key: "work_order_id", label: "Work Order", render: (row) => escapeHtml(shortWorkOrderId(row.work_order_id)) },
            { key: "fix_attempt_id", label: "Fix Attempt", render: (row) => escapeHtml(shortFixAttemptId(row.fix_attempt_id)) },
            { key: "reason", label: "Reason", render: (row) => truncatedCell(row.reason || "n/a", 320) },
          ],
          rows: nextActions.slice(0, 12),
          emptyMessage: "No queued actions.",
        }),
      }),
    ].join(""),
  };
}

async function renderReliabilityIssuesPage(state = {}) {
  const search = currentSearchParams();
  const filters = {
    status: search.get("status") || "",
    type: search.get("type") || "",
    category: search.get("category") || "",
    severity: search.get("severity") || "",
    readiness_blocker: search.get("readiness_blocker") || "",
    canonical_key: search.get("canonical_key") || "",
    environment: search.get("environment") || "",
    date_from: search.get("date_from") || "",
    date_to: search.get("date_to") || "",
  };
  const payload = await fetchReliabilityIssues(filters);
  const issues = safeList(payload.issues);
  const workflow = state.reliabilityWorkflow || {};
  const lastError = workflow.lastAction === "create_issue" ? workflow.lastError : null;
  const createdIssue = workflow.lastAction === "create_issue" ? workflow.lastIssue : null;
  const formInput = workflow.lastAction === "create_issue" && workflow.lastFormInput ? workflow.lastFormInput : {};
  const selectValue = (key, fallback = "") => String(formInput[key] || fallback);
  const selected = (key, value, fallback = "") => selectValue(key, fallback) === value ? "selected" : "";
  const visibleIssues = createdIssue && !issues.some((issue) => String(issue.id || "") === String(createdIssue.id || ""))
    ? [createdIssue, ...issues]
    : issues;
  const issueRows = visibleIssues.map((row) => {
    const route = `/reliability/issues/detail?issue_id=${encodeURIComponent(String(row.id || ""))}`;
    const shortId = shortIssueId(row.id);
    const action = issueRowNextAction(row);
    return `
      <tr data-route="${escapeHtml(route)}" style="cursor:pointer;">
        <td>
          <div style="display:flex;flex-direction:column;gap:2px;">
            <strong>${truncatedCell(row.title || "Untitled issue", 280)}</strong>
            <span style="font-size:11px;opacity:0.78;">${escapeHtml(shortId)}</span>
          </div>
        </td>
        <td>${reliabilityStatusBadge(row.status)}</td>
        <td>${readinessBlockerBadge(Boolean(row.readiness_blocker))}</td>
        <td>${actionChip(action)}</td>
        <td>${truncatedCell(row.category || "n/a", 120)}</td>
        <td>${truncatedCell(row.severity || "n/a", 110)}</td>
        <td>${escapeHtml(String(row.occurrence_count ?? "n/a"))}</td>
        <td>${truncatedCell(row.last_seen_at || "n/a", 180)}</td>
      </tr>
    `;
  }).join("");
  const issuesTable = visibleIssues.length
    ? `
      <div class="table-wrap">
        <table class="data-table">
          <thead>
            <tr>
              <th>Issue</th>
              <th>Status</th>
              <th>Readiness</th>
              <th>Next Action</th>
              <th>Category</th>
              <th>Severity</th>
              <th>Occurrences</th>
              <th>Updated</th>
            </tr>
          </thead>
          <tbody>${issueRows}</tbody>
        </table>
      </div>
    `
    : `<div class="empty-state">No reliability issues matched the active filters.</div>`;
  return {
    title: "Reliability Issues",
    meta: "Structured issue ledger with recurrence, blocker flags, severity, and codex workflow progress.",
    html: [
      renderCardSection({
        eyebrow: "Create",
        title: "+ New Issue",
        subtitle: "Create a structured reliability issue in the existing ledger.",
        body: `
          <details ${lastError ? "open" : ""}>
            <summary><strong>+ New Issue</strong></summary>
            <form class="reliability-action-form" autocomplete="off" style="margin-top:12px;">
              <input type="hidden" name="reliability_action" value="create_issue" />
              <input type="hidden" name="codex_status" value="not_needed" />
              <div class="line-list"><label>Title <input type="text" name="title" value="${escapeHtml(formInput.title || "")}" required /></label></div>
              <div class="line-list"><label>Type
                <select name="type" required>
                  <option value="bug" ${selected("type", "bug", "bug")}>bug</option>
                  <option value="missing_functionality" ${selected("type", "missing_functionality", "bug")}>missing_functionality</option>
                  <option value="regression" ${selected("type", "regression", "bug")}>regression</option>
                  <option value="paper_trading_incident" ${selected("type", "paper_trading_incident", "bug")}>paper_trading_incident</option>
                  <option value="readiness_blocker" ${selected("type", "readiness_blocker", "bug")}>readiness_blocker</option>
                </select>
              </label></div>
              <div class="line-list"><label>Category
                <select name="category" required>
                  <option value="infrastructure" ${selected("category", "infrastructure", "infrastructure")}>infrastructure</option>
                  <option value="execution" ${selected("category", "execution", "infrastructure")}>execution</option>
                  <option value="data" ${selected("category", "data", "infrastructure")}>data</option>
                  <option value="strategy" ${selected("category", "strategy", "infrastructure")}>strategy</option>
                  <option value="ui_missing_functionality" ${selected("category", "ui_missing_functionality", "infrastructure")}>ui_missing_functionality</option>
                </select>
              </label></div>
              <div class="line-list"><label>Severity
                <select name="severity" required>
                  <option value="medium" ${selected("severity", "medium", "medium")}>medium</option>
                  <option value="low" ${selected("severity", "low", "medium")}>low</option>
                  <option value="high" ${selected("severity", "high", "medium")}>high</option>
                  <option value="critical" ${selected("severity", "critical", "medium")}>critical</option>
                </select>
              </label></div>
              <div class="line-list"><label>Readiness blocker
                <select name="readiness_blocker">
                  <option value="" ${selected("readiness_blocker", "", "")}>default</option>
                  <option value="true" ${selected("readiness_blocker", "true", "")}>true</option>
                  <option value="false" ${selected("readiness_blocker", "false", "")}>false</option>
                </select>
              </label></div>
              <div class="line-list"><label>Canonical key <input type="text" name="canonical_key" value="${escapeHtml(formInput.canonical_key || "")}" required /></label></div>
              <div class="line-list"><label>Impact summary <input type="text" name="impact_summary" value="${escapeHtml(formInput.impact_summary || "")}" required /></label></div>
              <div class="line-list"><label>Expected behavior <textarea name="expected_behavior" required>${escapeHtml(formInput.expected_behavior || "")}</textarea></label></div>
              <div class="line-list"><label>Actual behavior <textarea name="actual_behavior" required>${escapeHtml(formInput.actual_behavior || "")}</textarea></label></div>
              <div class="line-list"><label>Created by <input type="text" name="created_by" value="${escapeHtml(formInput.created_by || "operator")}" required /></label></div>
              <button type="submit">Create Issue</button>
            </form>
          </details>
        `,
      }),
      lastError
        ? renderCardSection({
            eyebrow: "Error",
            title: "Last Reliability Issue Create Error",
            subtitle: "The backend rejected the issue create request.",
            body: `<div class="error-state">${escapeHtml(String(lastError))}</div>`,
          })
        : "",
      createdIssue
        ? renderCardSection({
            eyebrow: "Created",
            title: "Issue Created",
            subtitle: "The issue list was refreshed from the reliability API.",
            body: `<div class="line-list"><div>Issue id: ${escapeHtml(shortIssueId(createdIssue.id))}</div><div>${escapeHtml(createdIssue.title || "")}</div></div>`,
          })
        : "",
      renderCardSection({
        eyebrow: "Filters",
        title: "Issue Filters",
        subtitle: "Filter by status, type, category, severity, blocker flag, environment, and date range.",
        body: `
          <form class="reliability-filter-form" data-base-path="/reliability/issues">
            <div class="line-list"><label>Status <input type="text" name="status" value="${escapeHtml(filters.status)}" /></label></div>
            <div class="line-list"><label>Type <input type="text" name="type" value="${escapeHtml(filters.type)}" /></label></div>
            <div class="line-list"><label>Category <input type="text" name="category" value="${escapeHtml(filters.category)}" /></label></div>
            <div class="line-list"><label>Severity <input type="text" name="severity" value="${escapeHtml(filters.severity)}" /></label></div>
            <div class="line-list"><label>Readiness blocker <input type="text" name="readiness_blocker" value="${escapeHtml(filters.readiness_blocker)}" /></label></div>
            <div class="line-list"><label>Canonical key <input type="text" name="canonical_key" value="${escapeHtml(filters.canonical_key)}" /></label></div>
            <div class="line-list"><label>Environment <input type="text" name="environment" value="${escapeHtml(filters.environment)}" /></label></div>
            <div class="line-list"><label>Date from <input type="text" name="date_from" value="${escapeHtml(filters.date_from)}" placeholder="YYYY-MM-DD" /></label></div>
            <div class="line-list"><label>Date to <input type="text" name="date_to" value="${escapeHtml(filters.date_to)}" placeholder="YYYY-MM-DD" /></label></div>
            <button type="submit">Apply Filters</button>
          </form>
        `,
      }),
      renderCardSection({
        eyebrow: "Issues",
        title: "Issue List",
        subtitle: "Click a row to open issue detail. IDs are shortened for readability.",
        body: issuesTable,
      }),
    ].join(""),
    contextHtml: renderCardSection({
      eyebrow: "Summary",
      title: "Issue Ledger Summary",
      subtitle: "Quick counts for triage and blocking review.",
      body: renderDefinitionRows([
        { label: "Total matching issues", value: String(payload.total_count || issues.length) },
        { label: "Open issues", value: String(visibleIssues.filter((item) => openIssueStatus(item)).length) },
        { label: "Open blockers", value: String(visibleIssues.filter((item) => openIssueStatus(item) && item.readiness_blocker).length) },
      ]),
    }),
  };
}

async function renderReliabilityIssueDetailPage() {
  const search = currentSearchParams();
  const issueId = String(search.get("issue_id") || "").trim();
  if (!issueId) {
    return {
      title: "Issue Detail",
      meta: "Select an issue from Reliability Issues to view detail.",
      html: `<div class="empty-state">Missing issue_id query parameter.</div>`,
      contextHtml: "",
    };
  }

  const [payload, workOrdersPayload, verificationsPayload, fixAttemptsPayload, nextActionsPayload] = await Promise.all([
    fetchReliabilityIssue(issueId),
    fetchReliabilityIssueWorkOrders(issueId),
    fetchReliabilityIssueVerifications(issueId),
    fetchReliabilityFixAttempts({ issue_id: issueId }),
    fetchReliabilityNextActions(),
  ]);
  const issue = payload.issue || {};
  const linked = safeList(payload.linked_observations);
  const timeline = safeList(payload.recurrence_timeline);
  const codexTask = payload.codex_task || {};
  const workOrders = safeList(workOrdersPayload.work_orders);
  const fixAttempts = safeList(fixAttemptsPayload.fix_attempts);
  const verifications = safeList(verificationsPayload.verifications);
  const nextActions = safeList(nextActionsPayload.actions).filter((item) => String(item.issue_id || "") === issueId);
  const issueLabel = shortIssueId(issue.id || issueId);

  const workOrdersTable = workOrders.length
    ? `
      <div class="table-wrap">
        <table class="data-table">
          <thead>
            <tr>
              <th>Work Order</th>
              <th>Status</th>
              <th>Next Action</th>
              <th>Objective</th>
              <th>Assigned</th>
              <th>Updated</th>
            </tr>
          </thead>
          <tbody>
            ${workOrders.map((row) => {
              const rowId = String(row.id || "");
              const detailRoute = `/reliability/work-orders/${encodeURIComponent(rowId)}`;
              return `
                <tr data-route="${escapeHtml(detailRoute)}" style="cursor:pointer;">
                  <td>${escapeHtml(shortWorkOrderId(rowId))}</td>
                  <td>${reliabilityStatusBadge(row.status)}</td>
                  <td>${actionChip(nextActionForWorkOrder(row.status))}</td>
                  <td>${truncatedCell(row.objective || "n/a", 300)}</td>
                  <td>${truncatedCell(row.assigned_agent || "unassigned", 140)}</td>
                  <td>${truncatedCell(row.updated_at || "n/a", 170)}</td>
                </tr>
              `;
            }).join("")}
          </tbody>
        </table>
      </div>
    `
    : `<div class="empty-state">No work orders are linked to this issue.</div>`;

  const fixAttemptsTable = fixAttempts.length
    ? `
      <div class="table-wrap">
        <table class="data-table">
          <thead>
            <tr>
              <th>Fix Attempt</th>
              <th>Work Order</th>
              <th>Status</th>
              <th>Next Action</th>
              <th>Diff Ref</th>
              <th>Updated</th>
            </tr>
          </thead>
          <tbody>
            ${fixAttempts.map((row) => {
              const workOrderId = String(row.work_order_id || "");
              const detailRoute = workOrderId ? `/reliability/work-orders/${encodeURIComponent(workOrderId)}` : "";
              const routeAttrs = detailRoute ? ` data-route="${escapeHtml(detailRoute)}" style="cursor:pointer;"` : "";
              return `
                <tr${routeAttrs}>
                  <td>${escapeHtml(shortFixAttemptId(row.id))}</td>
                  <td>${escapeHtml(shortWorkOrderId(workOrderId))}</td>
                  <td>${reliabilityStatusBadge(row.status)}</td>
                  <td>${actionChip(nextActionForFixAttempt(row.status))}</td>
                  <td>${truncatedCell(row.diff_ref || "n/a", 220)}</td>
                  <td>${truncatedCell(row.completed_at || row.started_at || "n/a", 170)}</td>
                </tr>
              `;
            }).join("")}
          </tbody>
        </table>
      </div>
    `
    : `<div class="empty-state">No fix attempts recorded for this issue.</div>`;

  const verificationsTable = verifications.length
    ? `
      <div class="table-wrap">
        <table class="data-table">
          <thead>
            <tr>
              <th>Verification</th>
              <th>Status</th>
              <th>Method</th>
              <th>Work Order</th>
              <th>Fix Attempt</th>
              <th>Verified</th>
              <th>Next Action</th>
            </tr>
          </thead>
          <tbody>
            ${verifications.map((row) => `
              <tr data-route="/reliability/issues/detail?issue_id=${encodeURIComponent(String(issue.id || issueId))}" style="cursor:pointer;">
                <td>${escapeHtml(shortVerificationId(row.id))}</td>
                <td>${reliabilityStatusBadge(row.status)}</td>
                <td>${truncatedCell(row.method || "n/a", 160)}</td>
                <td>${escapeHtml(shortWorkOrderId(row.work_order_id))}</td>
                <td>${escapeHtml(shortFixAttemptId(row.fix_attempt_id))}</td>
                <td>${truncatedCell(row.verified_at || row.created_at || "n/a", 170)}</td>
                <td>${actionChip(nextActionForVerification(row.status))}</td>
              </tr>
            `).join("")}
          </tbody>
        </table>
      </div>
    `
    : `<div class="empty-state">No verification records for this issue.</div>`;

  const nextActionsTable = nextActions.length
    ? `
      <div class="table-wrap">
        <table class="data-table">
          <thead>
            <tr>
              <th>Priority</th>
              <th>Action</th>
              <th>Work Order</th>
              <th>Fix Attempt</th>
              <th>Reason</th>
            </tr>
          </thead>
          <tbody>
            ${nextActions.map((row) => {
              const workOrderId = String(row.work_order_id || "");
              const route = workOrderId ? `/reliability/work-orders/${encodeURIComponent(workOrderId)}` : `/reliability/issues/detail?issue_id=${encodeURIComponent(String(issue.id || issueId))}`;
              return `
                <tr data-route="${escapeHtml(route)}" style="cursor:pointer;">
                  <td>${escapeHtml(String(row.priority ?? "n/a"))}</td>
                  <td>${truncatedCell(row.action_type || "n/a", 180)}</td>
                  <td>${escapeHtml(shortWorkOrderId(workOrderId))}</td>
                  <td>${escapeHtml(shortFixAttemptId(row.fix_attempt_id))}</td>
                  <td>${truncatedCell(row.reason || "n/a", 340)}</td>
                </tr>
              `;
            }).join("")}
          </tbody>
        </table>
      </div>
    `
    : `<div class="empty-state">No queued actions for this issue.</div>`;

  const linkedObservationsTable = linked.length
    ? `
      <div class="table-wrap">
        <table class="data-table">
          <thead>
            <tr>
              <th>Observation</th>
              <th>Link Type</th>
              <th>Observed</th>
              <th>Source</th>
              <th>Environment</th>
              <th>Summary</th>
            </tr>
          </thead>
          <tbody>
            ${linked.map((row) => `
              <tr>
                <td>${escapeHtml(shortObservationId(row.observation_id))}</td>
                <td>${truncatedCell(row.link_type || "n/a", 130)}</td>
                <td>${truncatedCell(row.observed_at || "n/a", 170)}</td>
                <td>${truncatedCell(row.source || "n/a", 140)}</td>
                <td>${truncatedCell(row.environment || "n/a", 120)}</td>
                <td>${truncatedCell(row.summary || "n/a", 320)}</td>
              </tr>
            `).join("")}
          </tbody>
        </table>
      </div>
    `
    : `<div class="empty-state">No linked observations for this issue.</div>`;

  const timelineTable = timeline.length
    ? `
      <div class="table-wrap">
        <table class="data-table">
          <thead>
            <tr>
              <th>Observed At</th>
              <th>Observation</th>
              <th>Link Type</th>
              <th>Summary</th>
            </tr>
          </thead>
          <tbody>
            ${timeline.map((row) => `
              <tr>
                <td>${truncatedCell(row.observed_at || "n/a", 170)}</td>
                <td>${escapeHtml(shortObservationId(row.observation_id))}</td>
                <td>${truncatedCell(row.link_type || "n/a", 130)}</td>
                <td>${truncatedCell(row.summary || "n/a", 340)}</td>
              </tr>
            `).join("")}
          </tbody>
        </table>
      </div>
    `
    : `<div class="empty-state">No recurrence timeline entries.</div>`;

  return {
    title: `Issue Detail ${issueLabel}`,
    meta: "Issue fields, evidence links, work orders, fix attempts, verification records, and next action.",
    html: [
      renderCardSection({
        eyebrow: "Issue",
        title: "Issue Fields",
        subtitle: "Canonical issue state and lifecycle markers.",
        body: renderDefinitionRows([
          { label: "Id", value: issueLabel },
          { label: "Title", value: issue.title || "n/a" },
          { label: "Type", value: issue.type || "n/a" },
          { label: "Category", value: issue.category || "n/a" },
          { label: "Severity", value: issue.severity || "n/a" },
          { label: "Status", value: issue.status || "n/a" },
          { label: "Canonical key", value: issue.canonical_key || "n/a" },
          { label: "Occurrence count", value: String(issue.occurrence_count ?? "n/a") },
          { label: "Readiness blocker", value: truthyText(Boolean(issue.readiness_blocker)) },
          { label: "Codex status", value: issue.codex_status || "n/a" },
          { label: "Resolved at", value: issue.resolved_at || "n/a" },
          { label: "Verified at", value: issue.verified_at || "n/a" },
          { label: "Verification method", value: issue.verification_method || "n/a" },
          { label: "Resolution summary", value: issue.resolution_summary || "n/a" },
          { label: "Next action", value: payload.next_action || "none" },
        ]),
      }),
      renderCardSection({
        eyebrow: "Technical",
        title: "Technical Details",
        subtitle: "Full identifiers are kept here for diagnostics.",
        body: `
          <details>
            <summary>Technical details</summary>
            <div class="line-list" style="margin-top:8px;">
              <div>Issue id: ${escapeHtml(issue.id || "n/a")}</div>
              <div>Canonical key: ${escapeHtml(issue.canonical_key || "n/a")}</div>
            </div>
          </details>
        `,
      }),
      renderCardSection({
        eyebrow: "Issue Actions",
        title: "Update Issue / Work Queue Actions",
        subtitle: "Move issue workflow, create work orders, record fix attempts, and record verification evidence.",
        body: `
          <form class="reliability-action-form">
            <input type="hidden" name="reliability_action" value="update_issue" />
            <input type="hidden" name="issue_id" value="${escapeHtml(issue.id || issueId)}" />
            <div class="line-list"><label>Status <input type="text" name="status" value="${escapeHtml(issue.status || "")}" /></label></div>
            <div class="line-list"><label>Codex status <input type="text" name="codex_status" value="${escapeHtml(issue.codex_status || "")}" /></label></div>
            <div class="line-list"><label>Verification method <input type="text" name="verification_method" value="${escapeHtml(issue.verification_method || "")}" /></label></div>
            <div class="line-list"><label>Resolution summary <input type="text" name="resolution_summary" value="${escapeHtml(issue.resolution_summary || "")}" /></label></div>
            <div class="line-list"><label>Readiness blocker <input type="text" name="readiness_blocker" value="${truthyText(Boolean(issue.readiness_blocker))}" /></label></div>
            <button type="submit">Update Issue</button>
          </form>
          <form class="reliability-action-form">
            <input type="hidden" name="reliability_action" value="link_observation" />
            <input type="hidden" name="issue_id" value="${escapeHtml(issue.id || issueId)}" />
            <div class="line-list"><label>Observation id <input type="text" name="observation_id" /></label></div>
            <div class="line-list"><label>Link type <input type="text" name="link_type" value="verification" /></label></div>
            <button type="submit">Link Observation</button>
          </form>
          <form class="reliability-action-form">
            <input type="hidden" name="reliability_action" value="create_work_order_from_issue" />
            <input type="hidden" name="issue_id" value="${escapeHtml(issue.id || issueId)}" />
            <div class="line-list"><label>Assigned agent <input type="text" name="assigned_agent" value="codex" /></label></div>
            <div class="line-list"><label>Operator instruction <input type="text" name="operator_instruction" placeholder="Optional instruction override" /></label></div>
            <button type="submit">Create Work Order</button>
          </form>
          <form class="reliability-action-form">
            <input type="hidden" name="reliability_action" value="record_fix_attempt" />
            <input type="hidden" name="issue_id" value="${escapeHtml(issue.id || issueId)}" />
            <div class="line-list"><label>Work order id <input type="text" name="work_order_id" /></label></div>
            <div class="line-list"><label>Status <input type="text" name="status" value="started" /></label></div>
            <div class="line-list"><label>Files changed (comma/newline) <textarea name="files_changed" rows="2"></textarea></label></div>
            <div class="line-list"><label>Diff ref <input type="text" name="diff_ref" /></label></div>
            <div class="line-list"><label>Tests run (comma/newline) <textarea name="tests_run" rows="2"></textarea></label></div>
            <div class="line-list"><label>Test results JSON <textarea name="test_results" rows="3">{}</textarea></label></div>
            <div class="line-list"><label>Codex summary <textarea name="codex_summary" rows="3"></textarea></label></div>
            <div class="line-list"><label>Risk notes <textarea name="risk_notes" rows="2"></textarea></label></div>
            <button type="submit">Record Fix Attempt</button>
          </form>
          <form class="reliability-action-form">
            <input type="hidden" name="reliability_action" value="record_verification" />
            <input type="hidden" name="issue_id" value="${escapeHtml(issue.id || issueId)}" />
            <div class="line-list"><label>Method <input type="text" name="method" value="manual_review" /></label></div>
            <div class="line-list"><label>Status <input type="text" name="status" value="pending" /></label></div>
            <div class="line-list"><label>Work order id <input type="text" name="work_order_id" /></label></div>
            <div class="line-list"><label>Fix attempt id <input type="text" name="fix_attempt_id" /></label></div>
            <div class="line-list"><label>Evidence JSON <textarea name="evidence" rows="3">{}</textarea></label></div>
            <div class="line-list"><label>Notes <textarea name="notes" rows="2"></textarea></label></div>
            <button type="submit">Record Verification</button>
          </form>
        `,
      }),
      renderCardSection({
        eyebrow: "Work Orders",
        title: "Issue Work Orders",
        subtitle: "Actionable repair requests linked to this issue.",
        body: workOrdersTable,
      }),
      renderCardSection({
        eyebrow: "Fix Attempts",
        title: "Issue Fix Attempts",
        subtitle: "Codex attempt records remain advisory until verification evidence is passed.",
        body: fixAttemptsTable,
      }),
      `<div id="issue-verifications-section">${
        renderCardSection({
          eyebrow: "Verifications",
          title: "Issue Verifications",
          subtitle: "Verification is independent evidence and is required before closure.",
          body: verificationsTable,
        })
      }</div>`,
      renderCardSection({
        eyebrow: "Next Action",
        title: "Next Recommended Action",
        subtitle: "Prioritized reliability queue entries for this issue.",
        body: nextActionsTable,
      }),
      renderCardSection({
        eyebrow: "Evidence",
        title: "Linked Observations",
        subtitle: "Evidence links from observation ledger to this issue.",
        body: linkedObservationsTable,
      }),
    ].join(""),
    contextHtml: [
      renderCardSection({
        eyebrow: "Timeline",
        title: "Recurrence Timeline",
        subtitle: "Observation chronology tied to this issue.",
        body: timelineTable,
      }),
      renderCardSection({
        eyebrow: "Codex",
        title: "Codex Task Contract",
        subtitle: "Codex status is advisory; verification evidence is still required for closure.",
        body: renderDefinitionRows([
          { label: "Issue id", value: shortIssueId(codexTask.issue_id || issue.id || issueId) },
          { label: "Title", value: codexTask.title || issue.title || "n/a" },
          { label: "Affected component", value: codexTask.affected_component || issue.category || "n/a" },
          { label: "Required tests", value: safeList(codexTask.required_tests).join(", ") || "n/a" },
          { label: "Verification criteria", value: safeList(codexTask.verification_criteria).join(", ") || "n/a" },
          { label: "Forbidden changes", value: safeList(codexTask.forbidden_changes).join(", ") || "n/a" },
        ]),
      }),
    ].join(""),
  };
}

async function renderReliabilityObservationsPage() {
  const search = currentSearchParams();
  const filters = {
    source: search.get("source") || "",
    component: search.get("component") || "",
    environment: search.get("environment") || "",
    run_id: search.get("run_id") || "",
    event_type: search.get("event_type") || "",
    severity_hint: search.get("severity_hint") || "",
    date_from: search.get("date_from") || "",
    date_to: search.get("date_to") || "",
  };
  const payload = await fetchReliabilityObservations(filters);
  const observations = safeList(payload.observations);
  const observationRows = observations.map((row) => `
    <tr>
      <td>
        <div style="display:flex;flex-direction:column;gap:2px;">
          <strong>${escapeHtml(shortObservationId(row.id))}</strong>
          <span style="font-size:11px;opacity:0.78;">${truncatedCell(row.event_type || "observation", 200)}</span>
        </div>
      </td>
      <td>${truncatedCell(row.source || "n/a", 130)}</td>
      <td>${truncatedCell(row.environment || "n/a", 120)}</td>
      <td>${truncatedCell(row.component || "n/a", 130)}</td>
      <td>${truncatedCell(row.severity_hint || "n/a", 100)}</td>
      <td>${truncatedCell(row.summary || "n/a", 340)}</td>
      <td>${truncatedCell(row.observed_at || "n/a", 170)}</td>
      <td>${truthyText(Boolean(row.ai_detected))}</td>
    </tr>
  `).join("");
  const observationTable = observations.length
    ? `
      <div class="table-wrap">
        <table class="data-table">
          <thead>
            <tr>
              <th>Observation</th>
              <th>Source</th>
              <th>Environment</th>
              <th>Component</th>
              <th>Severity</th>
              <th>Summary</th>
              <th>Observed</th>
              <th>AI</th>
            </tr>
          </thead>
          <tbody>${observationRows}</tbody>
        </table>
      </div>
    `
    : `<div class="empty-state">No observations matched the active filters.</div>`;
  return {
    title: "Observations Ledger",
    meta: "Append-only reliability observations for incidents, recurrences, and verification evidence.",
    html: [
      renderCardSection({
        eyebrow: "Filters",
        title: "Observation Filters",
        subtitle: "Filter observation ledger by source/component/environment/run/event/severity/date.",
        body: `
          <form class="reliability-filter-form" data-base-path="/reliability/observations">
            <div class="line-list"><label>Source <input type="text" name="source" value="${escapeHtml(filters.source)}" /></label></div>
            <div class="line-list"><label>Component <input type="text" name="component" value="${escapeHtml(filters.component)}" /></label></div>
            <div class="line-list"><label>Environment <input type="text" name="environment" value="${escapeHtml(filters.environment)}" /></label></div>
            <div class="line-list"><label>Run id <input type="text" name="run_id" value="${escapeHtml(filters.run_id)}" /></label></div>
            <div class="line-list"><label>Event type <input type="text" name="event_type" value="${escapeHtml(filters.event_type)}" /></label></div>
            <div class="line-list"><label>Severity hint <input type="text" name="severity_hint" value="${escapeHtml(filters.severity_hint)}" /></label></div>
            <div class="line-list"><label>Date from <input type="text" name="date_from" value="${escapeHtml(filters.date_from)}" placeholder="YYYY-MM-DD" /></label></div>
            <div class="line-list"><label>Date to <input type="text" name="date_to" value="${escapeHtml(filters.date_to)}" placeholder="YYYY-MM-DD" /></label></div>
            <button type="submit">Apply Filters</button>
          </form>
        `,
      }),
      renderCardSection({
        eyebrow: "Ledger",
        title: "Append-Only Observations",
        subtitle: "Observations are immutable facts and cannot be edited in place.",
        body: observationTable,
      }),
    ].join(""),
    contextHtml: renderCardSection({
      eyebrow: "Integrity",
      title: "Ledger Constraints",
      subtitle: "Corrections are represented by new observations, never edits to existing records.",
      body: renderDefinitionRows([
        { label: "Total matching observations", value: String(payload.total_count || observations.length) },
        { label: "Append-only policy", value: "enforced" },
      ]),
    }),
  };
}

async function renderReliabilityWorkOrdersPage() {
  const search = currentSearchParams();
  const showSmokeTests = String(search.get("show_smoke") || "").trim() === "1";
  const queryText = inlineText(search.get("q") || "");
  const queryLower = queryText.toLowerCase();
  const [workOrdersPayload, issuesPayload, verificationsPayload] = await Promise.all([
    fetchReliabilityWorkOrders(),
    fetchReliabilityIssues(),
    fetchReliabilityVerifications(),
  ]);
  const allWorkOrders = safeList(workOrdersPayload.work_orders);
  const issues = safeList(issuesPayload.issues);
  const verifications = safeList(verificationsPayload.verifications);
  const issueById = new Map(issues.map((item) => [String(item.id || ""), item]));
  const passedVerificationIssueIds = new Set(
    verifications
      .filter((item) => String(item.status || "") === "passed")
      .map((item) => String(item.issue_id || "")),
  );

  const statusAction = (entry) => {
    const status = String(entry.workOrder.status || "").toLowerCase();
    if (status === "queued" || status === "in_progress") {
      return { label: "Open Prompt", route: entry.workOrderRoute };
    }
    if (status === "tests_passed") {
      return { label: "Verify", route: entry.issueVerificationRoute };
    }
    if (status === "needs_review") {
      return { label: "Review", route: entry.workOrderRoute };
    }
    return { label: "Open", route: entry.workOrderRoute };
  };

  const entries = allWorkOrders.map((workOrder) => {
    const issueId = String(workOrder.issue_id || "");
    const issue = issueById.get(issueId) || {};
    const issueStatus = String(issue.status || "");
    const issueOpen = openIssueStatus(issue);
    const hasPassedVerification = passedVerificationIssueIds.has(issueId);
    const isSleeve = isSleeveLiveReadinessContext(workOrder, issue);
    const isSmoke = isSystemSmokeTestWork(workOrder, issue);
    const isUserRelevant = String(issue.created_by || "").toLowerCase() === "operator" || Boolean(issue.ai_assisted);
    const isReadinessBlocker = Boolean(issue.readiness_blocker);
    const isOpenWithoutPassedVerification = issueOpen && !hasPassedVerification;
    const isTestsPassedAwaitingVerification = String(workOrder.status || "") === "tests_passed";
    const isInfrastructureSmoke = isSmoke && String(issue.category || "").toLowerCase() === "infrastructure";
    const workOrderRoute = `/reliability/work-orders/${encodeURIComponent(String(workOrder.id || ""))}`;
    const issueVerificationRoute = `/reliability/issues/detail?issue_id=${encodeURIComponent(issueId)}#issue-verifications-section`;
    const whyFallback = isSleeve
      ? "Determines whether a sleeve is ready to promote from Paper to Live."
      : "Impact summary not recorded.";
    const whyItMatters = inlineText(issue.impact_summary)
      || (isSmoke ? "System smoke test coverage." : whyFallback);
    const searchCorpus = [
      issue.title,
      issue.actual_behavior,
      issue.expected_behavior,
      issue.impact_summary,
      workOrder.objective,
      workOrder.actual_behavior,
      workOrder.expected_behavior,
      workOrder.id,
      workOrder.issue_id,
    ].filter(Boolean).join(" ").toLowerCase();
    const priorityTuple = [
      isSleeve ? 0 : 1,
      isUserRelevant ? 0 : 1,
      isReadinessBlocker ? 0 : 1,
      isOpenWithoutPassedVerification ? 0 : 1,
      isTestsPassedAwaitingVerification ? 0 : 1,
      isSmoke ? 1 : 0,
      isInfrastructureSmoke ? 1 : 0,
    ];
    return {
      workOrder,
      issue,
      issueStatus,
      isSleeve,
      isSmoke,
      isUserRelevant,
      isReadinessBlocker,
      isOpenWithoutPassedVerification,
      isTestsPassedAwaitingVerification,
      isInfrastructureSmoke,
      whyItMatters,
      searchCorpus,
      priorityTuple,
      workOrderRoute,
      issueVerificationRoute,
    };
  });

  const prioritizedAll = [...entries].sort((a, b) => {
    for (let index = 0; index < a.priorityTuple.length; index += 1) {
      const diff = a.priorityTuple[index] - b.priorityTuple[index];
      if (diff !== 0) {
        return diff;
      }
    }
    return String(b.workOrder.updated_at || "").localeCompare(String(a.workOrder.updated_at || ""));
  });
  const nonSmokePriority = prioritizedAll.filter((entry) => !entry.isSmoke);
  const primaryEntry = nonSmokePriority[0] || prioritizedAll[0] || null;

  const smokeFiltered = showSmokeTests ? prioritizedAll : prioritizedAll.filter((entry) => !entry.isSmoke);
  const visibleEntries = queryLower
    ? smokeFiltered.filter((entry) => entry.searchCorpus.includes(queryLower))
    : smokeFiltered;
  const myOpenReliabilityWork = visibleEntries.filter((entry) => openIssueStatus(entry.issue)).slice(0, 8);

  const sleeveIssues = issues
    .filter((issue) => openIssueStatus(issue) && isSleeveLiveReadinessContext({}, issue))
    .sort((a, b) => String(b.updated_at || "").localeCompare(String(a.updated_at || "")));
  const sleeveIssue = sleeveIssues[0] || null;
  const sleeveWorkOrder = prioritizedAll.find((entry) => entry.isSleeve && !entry.isSmoke) || null;

  let sleeveCodexPrompt = "";
  if (sleeveWorkOrder) {
    try {
      const detail = await fetchReliabilityWorkOrder(sleeveWorkOrder.workOrder.id);
      sleeveCodexPrompt = String(detail.codex_ready_prompt || "").trim();
    } catch {
      sleeveCodexPrompt = "";
    }
  }

  const focusPanel = sleeveWorkOrder
    ? `
      <div class="line-list">
        <div><strong>Primary Action:</strong></div>
        <div style="font-size:20px;font-weight:700;line-height:1.35;">Fix sleeve grading for LIVE readiness</div>
        <div style="margin-top:10px;"><strong>Why:</strong></div>
        <div>Sleeves need deterministic 1–7 grading to decide when they can be promoted from Paper to Live.</div>
        <div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:12px;">
          <a href="${escapeHtml(sleeveWorkOrder.workOrderRoute)}" data-route="${escapeHtml(sleeveWorkOrder.workOrderRoute)}" class="support-chip">Open Work Order</a>
          ${sleeveCodexPrompt
            ? `<textarea id="focusSleeveCodexPrompt" hidden readonly>${escapeHtml(sleeveCodexPrompt)}</textarea>
               <button type="button" data-copy-source="focusSleeveCodexPrompt">Copy Codex Prompt</button>`
            : `<button type="button" disabled>Copy Codex Prompt</button>`}
        </div>
      </div>
    `
    : sleeveIssue
      ? `
        <div class="line-list">
          <div><strong>Primary Action:</strong></div>
          <div style="font-size:20px;font-weight:700;line-height:1.35;">Create work order for sleeve grading issue</div>
          <form class="reliability-action-form" style="margin-top:12px;">
            <input type="hidden" name="reliability_action" value="create_work_order_from_issue" />
            <input type="hidden" name="issue_id" value="${escapeHtml(String(sleeveIssue.id || ""))}" />
            <input type="hidden" name="assigned_agent" value="codex" />
            <input type="hidden" name="operator_instruction" value="Fix sleeve grading for LIVE readiness (deterministic 1-7 scoring)." />
            <button type="submit">Create Work Order</button>
          </form>
        </div>
      `
      : `
        <div class="line-list">
          <div><strong>Primary Action:</strong></div>
          <div style="font-size:20px;font-weight:700;line-height:1.35;">Create sleeve grading issue</div>
          <div style="margin-top:12px;">
            <a href="/reliability/ai" data-route="/reliability/ai" class="support-chip">Open AI Draft</a>
          </div>
        </div>
      `;

  const sleeveCallout = sleeveIssue
    ? renderCardSection({
        eyebrow: "Sleeve Focus",
        title: "Sleeve LIVE Readiness Work",
        subtitle: "Operator shortcut for the sleeve promotion readiness gap.",
        body: `
          <div class="line-list">
            <div>Missing sleeve grading prevents determining when a sleeve is ready for LIVE.</div>
            <div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:10px;">
              ${sleeveWorkOrder
                ? `<a href="${escapeHtml(sleeveWorkOrder.workOrderRoute)}" data-route="${escapeHtml(sleeveWorkOrder.workOrderRoute)}" class="support-chip">Open Work Order</a>`
                : `<form class="reliability-action-form">
                    <input type="hidden" name="reliability_action" value="create_work_order_from_issue" />
                    <input type="hidden" name="issue_id" value="${escapeHtml(String(sleeveIssue.id || ""))}" />
                    <input type="hidden" name="assigned_agent" value="codex" />
                    <input type="hidden" name="operator_instruction" value="Fix sleeve grading for LIVE readiness (deterministic 1-7 scoring)." />
                    <button type="submit">Create Work Order</button>
                  </form>`}
              ${sleeveWorkOrder && sleeveCodexPrompt
                ? `<textarea id="sleeveCalloutCodexPrompt" hidden readonly>${escapeHtml(sleeveCodexPrompt)}</textarea>
                   <button type="button" data-copy-source="sleeveCalloutCodexPrompt">Copy Codex Prompt</button>`
                : ""}
            </div>
          </div>
        `,
      })
    : "";

  const listCards = myOpenReliabilityWork.map((entry) => {
    const issueTitle = inlineText(entry.issue.title) || "Untitled issue";
    const fixText = workOrderFixLabel(entry.workOrder, entry.issue);
    const smokeBadge = entry.isSmoke
      ? `<span style="display:inline-block;padding:2px 10px;border-radius:999px;font-size:11px;font-weight:700;background:#9ca3af;color:#111827;">System smoke test</span>`
      : "";
    return `
      <article class="stack-card">
        <div class="stack-card-header">
          <div>
            <div class="stack-card-title">${escapeHtml(ellipsisText(issueTitle, 120))}</div>
            <div class="stack-card-subtitle">${escapeHtml(ellipsisText(fixText, 140))}</div>
          </div>
          ${reliabilityStatusBadge(entry.workOrder.status)}
        </div>
        <div class="chip-list" style="margin-top:8px;">
          ${actionChip(workOrderNextStepLabel(entry.workOrder.status))}
          ${smokeBadge}
          <span class="support-chip">${escapeHtml(shortWorkOrderId(entry.workOrder.id))}</span>
        </div>
      </article>
    `;
  }).join("");
  const myOpenPanel = myOpenReliabilityWork.length
    ? listCards
    : `<div class="empty-state">${showSmokeTests ? "No open work orders match the search." : "No open non-smoke work orders match the search."}</div>`;

  const tableRows = visibleEntries.map((entry) => {
    const row = entry.workOrder;
    const rowId = String(row.id || "");
    const issueId = String(row.issue_id || "");
    const issue = entry.issue;
    const issueTitle = inlineText(issue.title) || "Untitled issue";
    const fixText = workOrderFixLabel(row, issue);
    const action = statusAction(entry);
    const smokeBadge = entry.isSmoke
      ? `<div style="margin-top:4px;"><span style="display:inline-block;padding:2px 10px;border-radius:999px;font-size:11px;font-weight:700;background:#9ca3af;color:#111827;">System smoke test</span></div>`
      : "";
    return `
      <tr data-route="${escapeHtml(entry.workOrderRoute)}" style="cursor:pointer;">
        <td>
          <div style="display:flex;flex-direction:column;gap:2px;">
            <strong>${escapeHtml(ellipsisText(issueTitle, 120))}</strong>
            <span style="font-size:11px;opacity:0.78;">${escapeHtml(shortWorkOrderId(rowId))} · ${escapeHtml(shortIssueId(issueId))}</span>
            ${smokeBadge}
          </div>
        </td>
        <td>
          <span title="${escapeHtml(entry.whyItMatters)}">${escapeHtml(ellipsisText(entry.whyItMatters, 120))}</span>
        </td>
        <td>${reliabilityStatusBadge(row.status)}</td>
        <td>${actionChip(workOrderNextStepLabel(row.status))}</td>
        <td><a href="${escapeHtml(action.route)}" data-route="${escapeHtml(action.route)}">${escapeHtml(action.label)}</a></td>
      </tr>
    `;
  }).join("");

  const tableMarkup = visibleEntries.length
    ? `
      <div class="table-wrap">
        <table class="data-table">
          <thead>
            <tr>
              <th>Problem</th>
              <th>Why it matters</th>
              <th>Status</th>
              <th>Next Step</th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody>${tableRows}</tbody>
        </table>
      </div>
    `
    : `<div class="empty-state">No work orders match the current filters.</div>`;

  return {
    title: "Reliability Work Orders",
    meta: "Actionable reliability repair queue with ownership, objective, and workflow status.",
    html: [
      renderCardSection({
        eyebrow: "Operator Work",
        title: "My Open Reliability Work",
        subtitle: "Prioritized for user-relevant issues; smoke-test work is deprioritized.",
        body: myOpenPanel,
      }),
      renderCardSection({
        eyebrow: "Primary",
        title: "What Needs Attention?",
        subtitle: "Sleeve readiness actions are prioritized above smoke-test tasks.",
        body: focusPanel,
      }),
      sleeveCallout,
      renderCardSection({
        eyebrow: "Queue",
        title: "Work Order Queue",
        subtitle: "Click a row to open work-order detail. Use filters to focus on real operator work.",
        body: `
          <form class="reliability-filter-form" data-base-path="/reliability/work-orders">
            <label style="display:flex;flex-direction:column;gap:6px;margin-bottom:10px;">
              <span>Search</span>
              <input type="text" name="q" value="${escapeHtml(queryText)}" placeholder="Search issues or work orders..." />
            </label>
            <label style="display:inline-flex;align-items:center;gap:8px;margin-bottom:12px;">
              <input type="checkbox" name="show_smoke" value="1" ${showSmokeTests ? "checked" : ""} />
              <span>Show system smoke tests</span>
            </label>
            <button type="submit">Apply</button>
          </form>
          ${tableMarkup}
        `,
      }),
    ].join(""),
    contextHtml: renderCardSection({
      eyebrow: "Summary",
      title: "Queue Summary",
      subtitle: "Current reliability work-order state distribution.",
      body: renderDefinitionRows([
        { label: "Total work orders", value: String(workOrdersPayload.total_count || allWorkOrders.length) },
        { label: "Visible rows", value: String(visibleEntries.length) },
        { label: "Non-smoke prioritized rows", value: String(nonSmokePriority.length) },
        { label: "Sleeve-related work orders", value: String(prioritizedAll.filter((entry) => entry.isSleeve).length) },
        { label: "Queued", value: String(allWorkOrders.filter((item) => item.status === "queued").length) },
        { label: "In progress", value: String(allWorkOrders.filter((item) => item.status === "in_progress").length) },
        { label: "Tests passed", value: String(allWorkOrders.filter((item) => item.status === "tests_passed").length) },
        { label: "Needs review", value: String(allWorkOrders.filter((item) => item.status === "needs_review").length) },
        { label: "Smoke-test rows", value: String(prioritizedAll.filter((entry) => entry.isSmoke).length) },
        { label: "Primary highlighted work order", value: primaryEntry ? shortWorkOrderId(primaryEntry.workOrder.id) : "none" },
      ]),
    }),
  };
}

async function renderReliabilityWorkOrderDetailPage() {
  const workOrderId = currentWorkOrderId();
  if (!workOrderId) {
    return {
      title: "Work Order Detail",
      meta: "Select a work order from the queue.",
      html: `<div class="empty-state">Missing work order id.</div>`,
      contextHtml: "",
    };
  }
  const payload = await fetchReliabilityWorkOrder(workOrderId);
  const workOrder = payload.work_order || {};
  const issue = payload.issue || {};
  const fixAttempts = safeList(payload.fix_attempts);
  const verifications = safeList(payload.verifications);
  const codexPrompt = String(payload.codex_ready_prompt || "").trim();
  const promptSourceId = `codex-prompt-${reliabilityIdCore(workOrder.id || workOrderId) || "work-order"}`;
  const workOrderLabel = shortWorkOrderId(workOrder.id || workOrderId);
  const issueLabel = shortIssueId(issue.id);

  const fixAttemptsTable = fixAttempts.length
    ? `
      <div class="table-wrap">
        <table class="data-table">
          <thead>
            <tr>
              <th>Fix Attempt</th>
              <th>Status</th>
              <th>Next Action</th>
              <th>Diff Ref</th>
              <th>Updated</th>
            </tr>
          </thead>
          <tbody>
            ${fixAttempts.map((row) => `
              <tr>
                <td>${escapeHtml(shortFixAttemptId(row.id))}</td>
                <td>${reliabilityStatusBadge(row.status)}</td>
                <td>${actionChip(nextActionForFixAttempt(row.status))}</td>
                <td>${truncatedCell(row.diff_ref || "n/a", 260)}</td>
                <td>${truncatedCell(row.completed_at || row.started_at || "n/a", 170)}</td>
              </tr>
            `).join("")}
          </tbody>
        </table>
      </div>
    `
    : `<div class="empty-state">No attempts recorded for this work order.</div>`;

  const verificationsTable = verifications.length
    ? `
      <div class="table-wrap">
        <table class="data-table">
          <thead>
            <tr>
              <th>Verification</th>
              <th>Status</th>
              <th>Method</th>
              <th>Fix Attempt</th>
              <th>Verified</th>
              <th>Next Action</th>
            </tr>
          </thead>
          <tbody>
            ${verifications.map((row) => {
              const route = issue.id ? `/reliability/issues/detail?issue_id=${encodeURIComponent(String(issue.id || ""))}` : "";
              const routeAttrs = route ? ` data-route="${escapeHtml(route)}" style="cursor:pointer;"` : "";
              return `
                <tr${routeAttrs}>
                  <td>${escapeHtml(shortVerificationId(row.id))}</td>
                  <td>${reliabilityStatusBadge(row.status)}</td>
                  <td>${truncatedCell(row.method || "n/a", 150)}</td>
                  <td>${escapeHtml(shortFixAttemptId(row.fix_attempt_id))}</td>
                  <td>${truncatedCell(row.verified_at || row.created_at || "n/a", 170)}</td>
                  <td>${actionChip(nextActionForVerification(row.status))}</td>
                </tr>
              `;
            }).join("")}
          </tbody>
        </table>
      </div>
    `
    : `<div class="empty-state">No verification records for this work order.</div>`;

  return {
    title: `Work Order ${workOrderLabel}`,
    meta: "Work-order objective, constraints, codex prompt, related issue, and fix attempts.",
    html: [
      renderCardSection({
        eyebrow: "Work Order",
        title: "Work Order Fields",
        subtitle: "Deterministic workflow contract for reliability repair execution.",
        body: renderDefinitionRows([
          { label: "Id", value: workOrderLabel },
          { label: "Issue", value: issueLabel },
          { label: "Status", value: workOrder.status || "n/a" },
          { label: "Objective", value: workOrder.objective || "n/a" },
          { label: "Affected component", value: workOrder.affected_component || "n/a" },
          { label: "Assigned agent", value: workOrder.assigned_agent || "n/a" },
          { label: "Updated", value: workOrder.updated_at || "n/a" },
        ]),
      }),
      renderCardSection({
        eyebrow: "Technical",
        title: "Technical Details",
        subtitle: "Full identifiers are available here for diagnostics only.",
        body: `
          <details>
            <summary>Technical details</summary>
            <div class="line-list" style="margin-top:8px;">
              <div>Work order id: ${escapeHtml(workOrder.id || workOrderId)}</div>
              <div>Issue id: ${escapeHtml(issue.id || "n/a")}</div>
            </div>
          </details>
        `,
      }),
      renderCardSection({
        eyebrow: "Codex",
        title: "Run with Codex",
        subtitle: "Prepare and execute a manual Codex fix loop from this work order.",
        body: `
          <div class="line-list" style="gap:10px;">
            <div>Paste this prompt into Codex. After Codex finishes, return here and record the fix attempt.</div>
            <textarea id="${escapeHtml(promptSourceId)}" rows="20" readonly style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, Liberation Mono, monospace;">${escapeHtml(codexPrompt || "No prompt available.")}</textarea>
            <div style="display:flex;gap:10px;flex-wrap:wrap;">
              <button type="button" data-copy-source="${escapeHtml(promptSourceId)}">Copy Codex Prompt</button>
              <button type="button" data-scroll-target="work-order-record-fix-attempt">Record Fix Attempt</button>
            </div>
          </div>
        `,
      }),
      `<div id="work-order-record-fix-attempt">${
        renderCardSection({
          eyebrow: "Record Attempt",
          title: "Record Fix Attempt",
          subtitle: "Capture codex attempt output and tests without auto-verifying/closing the issue.",
          body: `
            <form class="reliability-action-form">
              <input type="hidden" name="reliability_action" value="record_fix_attempt" />
              <input type="hidden" name="issue_id" value="${escapeHtml(issue.id || "")}" />
              <input type="hidden" name="work_order_id" value="${escapeHtml(workOrder.id || workOrderId)}" />
              <div class="line-list"><label>Status <input type="text" name="status" value="patch_submitted" /></label></div>
              <div class="line-list"><label>Files changed (comma/newline) <textarea name="files_changed" rows="2"></textarea></label></div>
              <div class="line-list"><label>Diff ref <input type="text" name="diff_ref" /></label></div>
              <div class="line-list"><label>Tests run (comma/newline) <textarea name="tests_run" rows="2"></textarea></label></div>
              <div class="line-list"><label>Test results JSON <textarea name="test_results" rows="3">{}</textarea></label></div>
              <div class="line-list"><label>Codex summary <textarea name="codex_summary" rows="3"></textarea></label></div>
              <div class="line-list"><label>Risk notes <textarea name="risk_notes" rows="2"></textarea></label></div>
              <button type="submit">Record Fix Attempt</button>
            </form>
          `,
        })
      }</div>`,
    ].join(""),
    contextHtml: [
      renderCardSection({
        eyebrow: "Fix Attempts",
        title: "Fix Attempts",
        subtitle: "Attempt history for this work order.",
        body: fixAttemptsTable,
      }),
      renderCardSection({
        eyebrow: "Verifications",
        title: "Linked Verifications",
        subtitle: "Verification evidence linked to this work order.",
        body: verificationsTable,
      }),
    ].join(""),
  };
}

async function renderReliabilityVerificationsPage() {
  const [payload, issuesPayload] = await Promise.all([
    fetchReliabilityVerifications(),
    fetchReliabilityIssues(),
  ]);
  const verifications = safeList(payload.verifications);
  const issues = safeList(issuesPayload.issues);
  const issueById = new Map(issues.map((issue) => [String(issue.id || ""), issue]));

  const rankStatus = (status) => {
    const normalized = String(status || "").toLowerCase();
    if (normalized === "failed") {
      return 0;
    }
    if (normalized === "pending") {
      return 1;
    }
    if (normalized === "inconclusive") {
      return 2;
    }
    if (normalized === "passed") {
      return 3;
    }
    return 4;
  };

  const prioritized = [...verifications].sort((a, b) => {
    const rankDiff = rankStatus(a.status) - rankStatus(b.status);
    if (rankDiff !== 0) {
      return rankDiff;
    }
    const aTs = String(a.verified_at || a.created_at || "");
    const bTs = String(b.verified_at || b.created_at || "");
    return bTs.localeCompare(aTs);
  });
  const attention = prioritized[0] || null;
  const attentionIssue = attention ? issueById.get(String(attention.issue_id || "")) : null;
  const attentionIssueLabel = attentionIssue?.title || shortIssueId(attention?.issue_id || "");
  const attentionText = attention
    ? `Next Action: ${nextActionForVerification(attention.status)} ${attentionIssueLabel}`
    : "";

  const rows = prioritized.map((row) => {
    const issueId = String(row.issue_id || "");
    const workOrderId = String(row.work_order_id || "");
    const issue = issueById.get(issueId) || null;
    const issueTitle = String(issue?.title || "").trim();
    const issueRoute = issueId
      ? `/reliability/issues/detail?issue_id=${encodeURIComponent(issueId)}`
      : "/reliability/verifications";
    const issueLabel = issueTitle
      ? `${issueTitle} (${shortIssueId(issueId)})`
      : shortIssueId(issueId);
    return `
      <tr data-route="${escapeHtml(issueRoute)}" style="cursor:pointer;">
        <td>${reliabilityStatusBadge(row.status)}</td>
        <td>${truncatedCell(row.method || "n/a", 150)}</td>
        <td>${truncatedCell(issueLabel, 280)}</td>
        <td>${escapeHtml(shortWorkOrderId(workOrderId))}</td>
        <td>${truncatedCell(row.verified_at || row.created_at || "n/a", 180)}</td>
        <td>${actionChip(nextActionForVerification(row.status))}</td>
      </tr>
    `;
  }).join("");
  const tableMarkup = prioritized.length
    ? `
      <div class="table-wrap">
        <table class="data-table">
          <thead>
            <tr>
              <th>Status</th>
              <th>Method</th>
              <th>Issue</th>
              <th>Work Order</th>
              <th>Verified</th>
              <th>Next Action</th>
            </tr>
          </thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
    `
    : `<div class="empty-state">No verification records exist.</div>`;
  return {
    title: "Reliability Verifications",
    meta: "Verification evidence across reliability issues and fix attempts.",
    html: [
      attentionText
        ? renderCardSection({
            eyebrow: "Attention",
            title: "What Needs Attention?",
            subtitle: "Highest-priority verification action to execute now.",
            body: `<div style="font-size:20px;font-weight:700;line-height:1.35;">${escapeHtml(attentionText)}</div>`,
          })
        : "",
      renderCardSection({
        eyebrow: "Verification Ledger",
        title: "Verification Records",
        subtitle: "Click any row to open the related issue. Verification remains evidence-first and independent.",
        body: tableMarkup,
      }),
    ].join(""),
    contextHtml: renderCardSection({
      eyebrow: "Summary",
      title: "Verification Summary",
      subtitle: "Current verification totals by status.",
      body: renderDefinitionRows([
        { label: "Total verifications", value: String(payload.total_count || prioritized.length) },
        { label: "Passed", value: String(prioritized.filter((item) => item.status === "passed").length) },
        { label: "Failed", value: String(prioritized.filter((item) => item.status === "failed").length) },
        { label: "Pending", value: String(prioritized.filter((item) => item.status === "pending").length) },
      ]),
    }),
  };
}

async function renderReliabilityAiDraftPage(state) {
  const workflow = state.reliabilityWorkflow || {};
  const draft = workflow.lastDraft || null;
  const createdIssue = workflow.lastIssue || null;
  return {
    title: "AI Create Issue / Draft Review",
    meta: "Draft structured issues from operator incident text and optionally create issue + evidence links.",
    html: [
      renderCardSection({
        eyebrow: "AI Draft",
        title: "Draft Or Create Reliability Issue",
        subtitle: "Submit operator message, logs, and optional observation ids for structured issue output.",
        body: `
          <form class="reliability-action-form">
            <input type="hidden" name="reliability_action" value="ai_draft_issue" />
            <div class="line-list"><label>Operator message <textarea name="operator_message" rows="5" placeholder="Describe what happened and what is broken."></textarea></label></div>
            <div class="line-list"><label>Observation ids (comma or newline separated) <textarea name="observation_ids" rows="3"></textarea></label></div>
            <div class="line-list"><label>Raw log refs (comma or newline separated) <textarea name="raw_log_refs" rows="3"></textarea></label></div>
            <div class="line-list"><label>Environment <input type="text" name="environment" value="PAPER" /></label></div>
            <div class="line-list"><label>Component <input type="text" name="component" value="unspecified" /></label></div>
            <div class="line-list"><label>Run id <input type="text" name="run_id" /></label></div>
            <div class="line-list"><label><input type="checkbox" name="create" value="true" /> Create issue immediately</label></div>
            <button type="submit">Generate Draft</button>
          </form>
        `,
      }),
      renderCardSection({
        eyebrow: "Draft Result",
        title: "Latest Draft Output",
        subtitle: "Structured issue fields returned by the deterministic draft endpoint.",
        body: draft
          ? renderDefinitionRows([
              { label: "Title", value: draft.title || "n/a" },
              { label: "Type", value: draft.type || "n/a" },
              { label: "Category", value: draft.category || "n/a" },
              { label: "Severity", value: draft.severity || "n/a" },
              { label: "Canonical key", value: draft.canonical_key || "n/a" },
              { label: "Readiness blocker", value: truthyText(Boolean(draft.readiness_blocker)) },
              { label: "Confidence", value: String(draft.confidence ?? "n/a") },
              { label: "Expected behavior", value: draft.expected_behavior || "n/a" },
              { label: "Actual behavior", value: draft.actual_behavior || "n/a" },
              { label: "Impact summary", value: draft.impact_summary || "n/a" },
              { label: "Linked observations", value: safeList(draft.linked_observation_ids).join(", ") || "none" },
            ])
          : `<div class="empty-state">No draft generated in this session.</div>`,
      }),
      createdIssue
        ? renderCardSection({
            eyebrow: "Created",
            title: "Issue Created From Draft",
            subtitle: "The AI draft was accepted into the reliability ledger.",
            body: `<div class="line-list">
              <div>Issue id: <a href="/reliability/issues/detail?issue_id=${encodeURIComponent(String(createdIssue.id || ""))}" data-route="/reliability/issues/detail?issue_id=${encodeURIComponent(String(createdIssue.id || ""))}">${escapeHtml(shortIssueId(createdIssue.id))}</a></div>
              <div>Status: ${escapeHtml(createdIssue.status || "n/a")}</div>
              <div>Codex status: ${escapeHtml(createdIssue.codex_status || "n/a")}</div>
            </div>`,
          })
        : "",
    ].join(""),
    contextHtml: renderCardSection({
      eyebrow: "Constraints",
      title: "Codex Safety Constraints",
      subtitle: "AI/Codex output remains advisory and cannot close issues without verification evidence.",
      body: renderDefinitionRows([
        { label: "Close issue on patch submit", value: "forbidden" },
        { label: "Verification evidence required", value: "yes" },
      ]),
    }),
  };
}

function renderBlockedDomain(routeId) {
  const route = routeForId(routeId);
  return {
    title: route?.label || "Blocked Domain",
    meta: route?.subtitle || "Backend gap",
    html: renderGapState({
      title: `${route?.label || "Domain"} is backend-blocked`,
      summary: "This route is present in the shell but currently has no backend workflow handler.",
      bullets: [
        "No backend API projection was resolved for this route.",
        "The shell keeps the route visible to preserve navigation consistency.",
      ],
    }),
    contextHtml: renderCardSection({
      eyebrow: "Scope",
      title: "Why this stops here",
      subtitle: "The redesign must not invent frontend truth where no canonical backend contract is exposed.",
      body: `<div class="line-list">
        <div>The shell route exists so the product stays unified.</div>
        <div>The domain remains intentionally non-authoritative until the backend exposes a canonical read model.</div>
      </div>`,
    }),
  };
}

export function buildPaletteEntries(state) {
  const routeEntries = ROUTES.map((route) => ({
    kind: "route",
    label: route.label,
    subtitle: route.subtitle,
    href: route.path,
  }));
  const workflowEntries = safeList(state.shell?.operatorWorkflow?.next_steps).map((step) => ({
    kind: "next_step",
    label: step.title || "Next step",
    subtitle: step.rationale || step.next_step_kind || "",
    href: routeHrefFromSurface(step.target_surface),
  }));
  return [...routeEntries, ...workflowEntries];
}

export async function loadRouteView(routeId, state) {
  switch (routeId) {
    case "command":
      return renderCommandPage(state);
    case "capital_overview":
      return renderCapitalOverviewPage(state);
    case "capital_accounts":
      return renderCapitalAccountsPage(state);
    case "capital_allocation":
      return renderCapitalAllocationPage(state);
    case "capital_history":
      return renderCapitalHistoryPage(state);
    case "capital_flows":
      return renderCapitalFlowsPage(state);
    case "capital_cashflow":
      return renderCapitalCashflowPage(state);
    case "capital_validation":
      return renderCapitalValidationPage(state);
    case "portfolio":
      return renderPortfolioPage(state);
    case "performance_cockpit":
      return renderPerformanceCockpitPage(state);
    case "sleeves":
      return renderSleevesPage(state);
    case "opportunities":
      return renderOpportunitiesPage(state);
    case "tax":
      return renderTaxPage(state);
    case "advisory":
      return renderAdvisoryPage(state);
    case "operations":
      return renderOperationsPage(state);
    case "aegis_runtime":
      return renderAegisRuntimePage(state);
    case "aegis_lite_queue":
      return renderAegisLiteQueuePage(state);
    case "aegis_events":
      return renderAegisEventMonitoringPage(state);
    case "outcomes":
      return renderOutcomesPage(state);
    case "refinement":
      return renderRefinementPage(state);
    case "policy":
      return renderPolicyPage(state);
    case "audit":
      return renderAuditPage(state);
    case "reports":
      return renderReportsPage(state);
    case "configuration":
      return renderConfigurationPage(state);
    case "reliability_dashboard":
      return renderReliabilityDashboardPage(state);
    case "reliability_issues":
      return renderReliabilityIssuesPage(state);
    case "reliability_issue_detail":
      return renderReliabilityIssueDetailPage(state);
    case "reliability_observations":
      return renderReliabilityObservationsPage(state);
    case "reliability_work_orders":
      return renderReliabilityWorkOrdersPage(state);
    case "reliability_work_order_detail":
      return renderReliabilityWorkOrderDetailPage(state);
    case "reliability_verifications":
      return renderReliabilityVerificationsPage(state);
    case "reliability_ai":
      return renderReliabilityAiDraftPage(state);
    default:
      return {
        title: "Unknown Route",
        meta: "This route is not part of the current Aegis shell manifest.",
        html: `<div class="error-state">Unknown route.</div>`,
        contextHtml: "",
      };
  }
}

export async function executeConfigurationWorkflow(formData, state) {
  const action = String(formData?.get("configuration_action") || "").trim();
  const draftId = String(formData?.get("draft_id") || "").trim();
  let result;

  if (action === "create_draft") {
    const catalog = await fetchConfigurationCatalog();
    const payload = {};
    safeList(catalog.editable_fields).forEach((row) => {
      const key = String(row.parameter_key || row.parameter_name || "").trim();
      if (!key) {
        return;
      }
      payload[key] = _parseConfigurationCatalogValue(formData?.get(key), row);
    });
    result = await createConfigurationDraft({
      ...payload,
      operator_parameters: _parseOperatorParameters(formData?.get("operator_parameters_text")),
    });
  } else if (action === "validate_draft") {
    result = await validateConfigurationDraft(draftId);
  } else if (action === "review_draft") {
    result = await reviewConfigurationDraft(draftId);
  } else if (action === "activate_draft") {
    result = await activateConfigurationDraft(draftId);
  } else if (action === "reject_draft") {
    const reason = String(formData?.get("reject_reason") || "").trim();
    result = await rejectConfigurationDraft(draftId, { reason });
  } else {
    throw new Error("Unsupported configuration action.");
  }

  state.configurationWorkflow = {
    activeDraftId: result?.draft?.draft_id || draftId || "",
    latestDraft: result?.draft || null,
    latestResult: result || null,
    lastAction: action,
    lastError: null,
  };
}

function _parseDelimitedList(raw) {
  return String(raw || "")
    .split(/[\n,]/g)
    .map((item) => String(item || "").trim())
    .filter(Boolean);
}

function _parseBooleanText(raw) {
  return ["1", "true", "yes", "on"].includes(String(raw || "").trim().toLowerCase());
}

function _parseJsonInput(raw, fallback) {
  const text = String(raw || "").trim();
  if (!text) {
    return fallback;
  }
  try {
    return JSON.parse(text);
  } catch {
    return fallback;
  }
}

function _operatorParameterValueKind(value) {
  const text = String(value || "").trim();
  if (!text) {
    return "text";
  }
  if (text.startsWith("$")) {
    return "money";
  }
  if (text.endsWith("%")) {
    return "percent";
  }
  if (["true", "false"].includes(text.toLowerCase())) {
    return "boolean";
  }
  if ((text.startsWith("{") && text.endsWith("}")) || (text.startsWith("[") && text.endsWith("]"))) {
    return "json";
  }
  return Number.isFinite(Number(text)) ? "number" : "text";
}

function _parseOperatorParameters(raw) {
  return String(raw || "")
    .split(/\n/g)
    .map((line) => String(line || "").trim())
    .filter(Boolean)
    .map((line) => {
      const separatorIndex = line.indexOf("=");
      const parameterName = separatorIndex >= 0 ? line.slice(0, separatorIndex).trim() : line;
      const valueText = separatorIndex >= 0 ? line.slice(separatorIndex + 1).trim() : "";
      return {
        parameter_name: parameterName,
        value_kind: _operatorParameterValueKind(valueText),
        value_text: valueText,
        notes: "",
      };
    })
    .filter((row) => row.parameter_name && row.value_text);
}

function _parseConfigurationCatalogValue(raw, row) {
  const type = String(row?.type || row?.validation?.type || "text").trim();
  if (type === "boolean") {
    return ["1", "true", "yes", "on"].includes(String(raw || "").trim().toLowerCase());
  }
  if (type === "integer") {
    const parsed = Number.parseInt(String(raw || "").trim(), 10);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return String(raw || "").trim();
}

export async function executeReliabilityWorkflow(formData, state) {
  const action = String(formData?.get("reliability_action") || "").trim();
  let result = null;
  let lastIssue = null;
  let lastDraft = null;

  if (action === "assess_readiness") {
    result = await assessReliabilityReadiness({});
  } else if (action === "ai_draft_issue") {
    const payload = {
      operator_message: String(formData?.get("operator_message") || "").trim(),
      observation_ids: _parseDelimitedList(formData?.get("observation_ids")),
      raw_log_refs: _parseDelimitedList(formData?.get("raw_log_refs")),
      environment: String(formData?.get("environment") || "").trim(),
      component: String(formData?.get("component") || "").trim(),
      run_id: String(formData?.get("run_id") || "").trim(),
      create: _parseBooleanText(formData?.get("create")),
    };
    result = await draftReliabilityIssue(payload);
    lastDraft = result?.draft_issue || null;
    lastIssue = result?.issue || null;
  } else if (action === "create_issue") {
    const payload = {
      title: String(formData?.get("title") || "").trim(),
      type: String(formData?.get("type") || "").trim(),
      category: String(formData?.get("category") || "").trim(),
      severity: String(formData?.get("severity") || "").trim(),
      status: String(formData?.get("status") || "open").trim(),
      canonical_key: String(formData?.get("canonical_key") || "").trim(),
      impact_summary: String(formData?.get("impact_summary") || "").trim(),
      expected_behavior: String(formData?.get("expected_behavior") || "").trim(),
      actual_behavior: String(formData?.get("actual_behavior") || "").trim(),
      created_by: String(formData?.get("created_by") || "operator").trim() || "operator",
      codex_status: String(formData?.get("codex_status") || "not_needed").trim() || "not_needed",
      observation_ids: _parseDelimitedList(formData?.get("observation_ids")),
    };
    const readinessBlockerValue = String(formData?.get("readiness_blocker") || "").trim();
    if (readinessBlockerValue) {
      payload.readiness_blocker = _parseBooleanText(readinessBlockerValue);
    }
    result = await createReliabilityIssue(payload);
    lastIssue = result?.issue || null;
  } else if (action === "update_issue") {
    const issueId = String(formData?.get("issue_id") || "").trim();
    const patch = {};
    for (const key of ["status", "codex_status", "verification_method", "resolution_summary"]) {
      const value = String(formData?.get(key) || "").trim();
      if (value) {
        patch[key] = value;
      }
    }
    const readinessBlockerValue = String(formData?.get("readiness_blocker") || "").trim();
    if (readinessBlockerValue) {
      patch.readiness_blocker = _parseBooleanText(readinessBlockerValue);
    }
    result = await updateReliabilityIssue(issueId, patch);
    lastIssue = result?.issue || null;
  } else if (action === "link_observation") {
    const issueId = String(formData?.get("issue_id") || "").trim();
    const observationId = String(formData?.get("observation_id") || "").trim();
    const linkType = String(formData?.get("link_type") || "related").trim();
    result = await linkReliabilityObservation(issueId, {
      observation_id: observationId,
      link_type: linkType,
    });
    lastIssue = result?.issue || null;
  } else if (action === "create_work_order_from_issue") {
    const issueId = String(formData?.get("issue_id") || "").trim();
    result = await createWorkOrderFromIssue(issueId, {
      operator_instruction: String(formData?.get("operator_instruction") || "").trim(),
      assigned_agent: String(formData?.get("assigned_agent") || "").trim(),
    });
    lastIssue = result?.issue || null;
  } else if (action === "record_fix_attempt") {
    const issueId = String(formData?.get("issue_id") || "").trim();
    const workOrderId = String(formData?.get("work_order_id") || "").trim();
    result = await recordReliabilityFixAttempt(workOrderId, {
      status: String(formData?.get("status") || "started").trim(),
      files_changed: _parseDelimitedList(formData?.get("files_changed")),
      diff_ref: String(formData?.get("diff_ref") || "").trim(),
      tests_run: _parseDelimitedList(formData?.get("tests_run")),
      test_results: _parseJsonInput(formData?.get("test_results"), {}),
      codex_summary: String(formData?.get("codex_summary") || "").trim(),
      risk_notes: String(formData?.get("risk_notes") || "").trim(),
    });
    if (issueId) {
      const refreshed = await fetchReliabilityIssue(issueId);
      lastIssue = refreshed?.issue || null;
    } else {
      lastIssue = result?.issue || null;
    }
  } else if (action === "record_verification") {
    const issueId = String(formData?.get("issue_id") || "").trim();
    result = await verifyReliabilityIssue(issueId, {
      method: String(formData?.get("method") || "manual_review").trim(),
      status: String(formData?.get("status") || "pending").trim(),
      work_order_id: String(formData?.get("work_order_id") || "").trim(),
      fix_attempt_id: String(formData?.get("fix_attempt_id") || "").trim(),
      evidence: _parseJsonInput(formData?.get("evidence"), {}),
      notes: String(formData?.get("notes") || "").trim(),
    });
    lastIssue = result?.issue || null;
  } else {
    throw new Error("Unsupported reliability action.");
  }

  state.reliabilityWorkflow = {
    ...(state.reliabilityWorkflow || {}),
    lastAction: action,
    lastResult: result,
    lastAssessment: result?.assessment || state.reliabilityWorkflow?.lastAssessment || null,
    lastIssue: lastIssue || state.reliabilityWorkflow?.lastIssue || null,
    lastDraft: lastDraft || state.reliabilityWorkflow?.lastDraft || null,
    lastError: null,
  };
}

export async function executeOperatorQuery(queryText, state) {
  if (!queryText || !String(queryText).trim()) {
    state.commandQueryResult = null;
    state.commandQueryText = "";
    return;
  }
  state.commandQueryText = String(queryText);
  state.commandQueryResult = await fetchOperatorQuery(String(queryText).trim());
}
