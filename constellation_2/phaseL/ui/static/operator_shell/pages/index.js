import {
  activateConfigurationDraft,
  createConfigurationDraft,
  fetchActionAudit,
  fetchActivityToday,
  fetchAegisLiteExecutionQueue,
  fetchAegisEventMonitoring,
  fetchAegisOperatorState,
  fetchAegisOperatorCockpit,
  fetchAegisOperatorStateSnapshotLatest,
  fetchAegisJournalTimeline,
  fetchAegisThesisGraph,
  fetchAegisOperatorToday,
  fetchAegisOperatorTasks,
  fetchAegisOperatorDiagnostics,
  fetchAegisOperatorWhatChanged,
  fetchAegisOperatorHealth,
  fetchAegisOpportunitiesProjection,
  fetchAegisReviewLedger,
  fetchAegisEodOutcomeLatest,
  fetchResearchLabChallengerComparisonReports,
  fetchResearchLabEdgeProjection,
  fetchResearchLabExpectancyDrift,
  fetchResearchLabHumanReviewDossiers,
  fetchResearchLabPaperTrialInventory,
  fetchResearchLabRegimeFragility,
  fetchResearchLabSleeveStability,
  fetchResearchLabStatus,
  fetchResearchLabPaperTrialProposal,
  fetchResearchLabObservationCandidates,
  fetchResearchLabObservationClusters,
  fetchResearchLabHypothesisProposals,
  fetchResearchLabHypothesisProposalReviews,
  fetchResearchLabHypothesisIntake,
  fetchResearchIntakeQueue,
  fetchResearchConsole,
  fetchResearchIntakeDossier,
  startResearchIdea,
  reviewHypothesisProposal,
  assessHypothesisReadiness,
  convertHypothesisProposalToResearchPlan,
  runAegisDataRemediation,
  buildAegisHypothesisPlan,
  fetchAegisRuntimeTruth,
  fetchAegisRepairCenter,
  fetchAegisAdaptiveIntelligence,
  fetchAegisIntelligenceGovernance,
  executeAegisOperatorCommand,
  postAegisManualCaptureRecord,
  reviewAegisHypothesis,
  runAegisHypothesisTest,
  triageAegisHypothesis,
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

import {
  ROUTES,
  LEGACY_ROUTE_ALIASES,
  TARGET_SURFACE_ROUTE,
} from "./route_metadata.js";

export { ROUTES, LEGACY_ROUTE_ALIASES };

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

function safeDomId(value) {
  return String(value || "item").replace(/[^a-zA-Z0-9_-]+/g, "-");
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

function shortHash(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return "MISSING";
  }
  if (raw.length <= 18) {
    return raw;
  }
  return `${raw.slice(0, 8)}...${raw.slice(-6)}`;
}

function localDateTimeInputValue(date = new Date()) {
  const pad = (value) => String(value).padStart(2, "0");
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

function manualCaptureDraftKey(ticketId) {
  return `aegis.manual_capture_draft.${String(ticketId || "unknown").replace(/[^a-zA-Z0-9_.:-]/g, "_")}`;
}

function readManualCaptureDraft(ticketId) {
  if (!ticketId) {
    return {};
  }
  try {
    const raw = localStorage.getItem(manualCaptureDraftKey(ticketId));
    const parsed = raw ? JSON.parse(raw) : {};
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch (_error) {
    return {};
  }
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
  const title = `Legacy Runtime Diagnostics: ${operatorState.state || "UNKNOWN"}`;
  const body = operatorState.recommended_operator_action || operatorState.state_reason || "Review legacy runtime diagnostics.";
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
  const packetStatus = packet.packet_freshness_status || packet.status || readinessKernel?.packet_status || "UNKNOWN";
  const runtimeHash = readinessKernel?.runtime_evaluation_hash || "MISSING";
  const packetHash = packet.packet_runtime_evaluation_hash || "MISSING";
  return `
    <div class="metric-grid">
      ${renderMetricCard({ label: "Runtime mode", value: readinessKernel?.runtime_mode || "UNKNOWN", semantic: readinessKernel?.runtime_mode === "PRODUCTION" ? "healthy" : "warning", semantics: state.semantics })}
      ${renderMetricCard({ label: "RuntimeEvaluation", value: ellipsisText(runtimeHash, 14), semantic: runtimeHash === "MISSING" ? "blocked" : "healthy", semantics: state.semantics })}
      ${renderMetricCard({ label: "Control packet", value: packetStatus, semantic: packetStatus === "CURRENT" ? "healthy" : "warning", semantics: state.semantics })}
      ${renderMetricCard({ label: "Packet hash", value: ellipsisText(packetHash, 14), semantic: packetStatus === "CURRENT" ? "healthy" : "warning", semantics: state.semantics })}
      ${renderMetricCard({ label: "Production version", value: productionVersion.status || readinessKernel?.production_version_status || "UNKNOWN", semantic: productionVersion.status === "ACTIVE" ? "healthy" : "blocked", semantics: state.semantics })}
      ${renderMetricCard({ label: "Submit", value: readinessKernel?.submit_status || "UNKNOWN", semantic: readinessKernel?.submit_status === "ALLOWED" ? "healthy" : "blocked", semantics: state.semantics })}
      ${renderMetricCard({ label: "Current phase", value: readinessKernel?.current_phase || "UNKNOWN", semantic: readinessKernel?.overall_status === "READY" ? "healthy" : "blocked", semantics: state.semantics })}
      ${renderMetricCard({ label: "Canonical blocker", value: readinessKernel?.canonical_blocker || "NONE", semantic: readinessKernel?.canonical_blocker ? "blocked" : "healthy", semantics: state.semantics })}
    </div>
    <div style="margin-top:12px;" class="stack-list">
      <div class="evidence-row">
        <div>
          <strong>${escapeHtml(readinessKernel?.current_phase || "UNKNOWN")}</strong>
          <div style="font-size:12px;opacity:0.78;">Authority: RuntimeEvaluation ${escapeHtml(runtimeHash)}</div>
          <div style="font-size:12px;opacity:0.78;">Control packet: ${escapeHtml(packetStatus)} (${escapeHtml(packet.readiness_usage || "EXPLANATORY_ONLY")})</div>
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
      as_of_label: "Updated: MOCK / UNAVAILABLE",
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
      title: "Legacy Runtime Diagnostics",
      meta: "Backend unavailable; legacy/deferred diagnostics are not the primary Aegis Lite workflow.",
      html: [
        renderCardSection({
          eyebrow: "BACKEND_UNAVAILABLE",
          title: "Legacy Runtime Diagnostics Backend Unavailable",
          subtitle: "The frontend cannot reach this diagnostic API. Aegis Lite remains the primary manual workflow.",
          body: renderDefinitionRows([
            { label: "Connection state", value: window.__AEGIS_CONNECTION_STATE?.state || "BACKEND_UNAVAILABLE" },
            { label: "Recovery command", value: "npm run aegis:ui:restart" },
            { label: "Endpoint", value: error?.operatorSafe?.endpointAttempted || "/api/runtime-status" },
            { label: "Last successful refresh", value: cached?.saved_at || "none" },
          ]),
        }),
        last.final_status ? renderCardSection({
          eyebrow: "Last Known Truth",
          title: "Cached Legacy Projection",
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
    title: "Legacy Runtime Diagnostics",
    meta: "Read-only diagnostic projection for deferred/internal runtime state.",
    html: [
      renderCardSection({
        eyebrow: payload.status || "UNKNOWN",
        title: "Legacy Runtime Diagnostics",
        subtitle: payload.operator_next_action || "Diagnostic-only surface; use Aegis Lite for operator workflow.",
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

async function renderAegisRuntimeTruthPage() {
  let payload;
  try {
    payload = await fetchAegisRuntimeTruth();
  } catch (error) {
    return {
      title: "Runtime Truth",
      meta: "Backend unavailable; runtime truth kernel could not be loaded.",
      html: renderCardSection({
        eyebrow: "BACKEND_UNAVAILABLE",
        title: "Runtime Truth Kernel Unavailable",
        subtitle: "The read-only API could not return artifact and readiness truth.",
        body: renderDefinitionRows([
          { label: "Endpoint", value: error?.operatorSafe?.endpointAttempted || "/api/aegis/runtime-truth" },
          { label: "Recovery command", value: "npm run aegis:truth-kernel" },
          { label: "Audit command", value: "npm run aegis:audit" },
        ]),
      }),
      contextHtml: "",
    };
  }

  const missing = safeList(payload.missing_or_stale_sources);
  const blocked = safeList(payload.blocked_capabilities);
  const claims = safeList(payload.do_not_claim);
  const recovery = safeList(payload.recovery_plan);
  const statuses = safeList(payload.artifact_statuses);
  const domains = Array.from(new Set(statuses.map((row) => row.domain || "unknown"))).sort();
  const graph = payload.dependency_graph || {};
  const history = payload.runtime_state_history || {};
  const lastTransition = history.last_transition_summary || {};
  const manualCapture = payload.manual_capture_status || {};
  const captureProjection = {
    platform_capture_capability: payload.platform_capture_capability || (payload.manual_trade_capture_allowed === true ? "READY" : "NOT_READY"),
    capture_ticket_count: Number(payload.capture_ticket_count ?? 0),
    capture_ticket_status: payload.capture_ticket_status || "NONE_AVAILABLE",
    next_action: payload.capture_ticket_operator_next_action || "No action required",
  };
  const disabledByPolicy = safeList(payload.policy_disabled_capabilities);
  const optionalNotRequired = safeList(payload.optional_not_required_capabilities);
  const intelligenceSummaries = payload.intelligence_summaries || {};
  const suggestedCommands = [
    "npm run aegis:daily-operator",
    "npm run aegis:operator-inbox",
    "npm run aegis:evolution-engine",
    "npm run aegis:research-priorities",
    "npm run aegis:sleeve-attribution",
    "npm run aegis:risk-governance",
    "npm run aegis:truth-kernel",
    "npm run aegis:readiness",
    "npm run aegis:audit",
    "npm run aegis:capture-manual-trade -- --help",
  ];
  return {
    title: "Runtime Truth",
    meta: "Read-only deterministic readiness kernel. No broker actions are exposed.",
    html: [
      renderCardSection({
        eyebrow: "Target Mode",
        title: payload.target_operating_mode || "HUMAN_APPROVED_ADVISORY_RUNTIME",
        subtitle: "Aegis evaluates, advises when authorized, and records human-entered receipts. It does not execute trades.",
        body: renderDefinitionRows([
          { label: "Target-mode readiness", value: String(payload.human_approved_advisory_runtime_ready === true) },
          { label: "Advisory status", value: payload.advisory_status || "ADVISORY_NOT_EVALUATED" },
          { label: "Operator action required", value: String(payload.operator_action_required === true) },
          { label: "Operator action reason", value: payload.operator_action_reason || "none" },
          { label: "Live broker trading", value: payload.live_broker_trading_policy || "DISABLED_BY_DESIGN" },
          { label: "Autonomous execution", value: payload.autonomous_execution_policy || "DISABLED_BY_DESIGN" },
          { label: "Broker submit/transmit", value: payload.broker_submit_transmit_policy || "DISABLED_BY_DESIGN" },
          { label: "Manual capture", value: "JOURNALING_AUDIT_ONLY" },
        ]),
      }),
      renderCardSection({
        eyebrow: payload.runtime_truth_classification || "UNKNOWN",
        title: "Runtime Truth Kernel",
        subtitle: "Artifact evidence determines readiness; narrative cannot pass a capability gate.",
        body: `
          <div class="metric-grid">
            ${renderMetricCard({ label: "Runtime truth", value: payload.runtime_truth_classification || "UNKNOWN" })}
            ${renderMetricCard({ label: "Highest layer", value: payload.highest_readiness_layer || "UNKNOWN" })}
            ${renderMetricCard({ label: "Missing/stale/invalid", value: String(payload.missing_or_stale_source_count ?? missing.length) })}
            ${renderMetricCard({ label: "Blocked capabilities", value: String(blocked.length) })}
            ${renderMetricCard({ label: "Claim guard items", value: String(claims.length) })}
            ${renderMetricCard({ label: "Invalidations", value: String(history.invalidation_count ?? 0) })}
          </div>
        `,
      }),
      renderCardSection({
        eyebrow: "Manual Capture",
        title: "Capability And Tickets",
        subtitle: "Capability means Aegis can journal operator-entered receipts; tickets appear only when promoted candidates exist.",
        body: renderDefinitionRows([
          { label: "Manual capture capability", value: captureProjection.platform_capture_capability },
          { label: "IB capture tickets", value: String(captureProjection.capture_ticket_count) },
          { label: "Ticket status", value: captureProjection.capture_ticket_status },
          { label: "Operator action", value: captureProjection.next_action },
          { label: "Receipt status", value: manualCapture.result || manualCapture.status || "UNKNOWN" },
          { label: "Receipts today", value: String(manualCapture.manual_trade_receipt_count ?? 0) },
          { label: "Last receipt", value: manualCapture.last_receipt_id || "none" },
          { label: "Validation", value: manualCapture.validation_status || "UNKNOWN" },
          { label: "Broker submission by Aegis", value: String(manualCapture.broker_submission_by_aegis === true) },
          { label: "Autonomous execution", value: String(manualCapture.autonomous_execution === true) },
        ]),
      }),
      renderCardSection({
        eyebrow: "Policy",
        title: "Disabled And Optional Capabilities",
        subtitle: "These are not missing target-mode blockers.",
        body: renderDefinitionRows([
          { label: "Disabled by policy", value: disabledByPolicy.join(", ") || "none" },
          { label: "Optional / not required", value: optionalNotRequired.join(", ") || "none" },
          { label: "Live broker trading", value: payload.live_broker_trading_policy || "DISABLED_BY_DESIGN" },
          { label: "Autonomous execution", value: payload.autonomous_execution_policy || "DISABLED_BY_DESIGN" },
          { label: "Broker submit/transmit", value: payload.broker_submit_transmit_policy || "DISABLED_BY_DESIGN" },
        ]),
      }),
      renderCardSection({
        eyebrow: "History",
        title: "Runtime State History",
        subtitle: "Persisted snapshot, transition, invalidation, replay, and diff references.",
        body: renderDefinitionRows([
          { label: "Latest snapshot", value: history.latest_snapshot_id || "none" },
          { label: "Snapshot path", value: history.snapshot_path || "n/a" },
          { label: "Transition count", value: String(history.transition_count ?? 0) },
          { label: "Invalidation count", value: String(history.invalidation_count ?? 0) },
          { label: "Capabilities gained", value: safeList(history.capabilities_gained_since_prior_snapshot).join(", ") || "none" },
          { label: "Capabilities lost", value: safeList(history.capabilities_lost_since_prior_snapshot).join(", ") || "none" },
          { label: "Replay", value: history.replay_command_example || "npm run aegis:replay-state" },
          { label: "Diff", value: history.diff_command_example || "npm run aegis:state-diff" },
        ]),
      }),
      renderCardSection({
        eyebrow: "Intelligence",
        title: "Adaptive Research And Operations Reports",
        subtitle: "Report-only intelligence surfaces. Recommendations remain advisory and require human approval.",
        body: renderSimpleTable({
          columns: [
            { label: "Report", key: "report" },
            { label: "Status", key: "status" },
            { label: "Path", key: "path" },
          ],
          rows: Object.entries(intelligenceSummaries).map(([report, row]) => ({ report, ...((row || {})) })),
          emptyMessage: "No adaptive intelligence reports are available yet.",
        }),
      }),
      renderCardSection({
        eyebrow: "Readiness",
        title: "Layered Readiness",
        subtitle: "Every layer is computed from explicit dependencies.",
        body: renderSimpleTable({
          columns: [
            { label: "Layer", key: "layer" },
            { label: "Allowed", key: "allowed", render: (row) => renderStatusPill(row.allowed ? "true" : "false", row.allowed ? "healthy" : "blocked", {}) },
          ],
          rows: Object.entries(payload.layers || {}).map(([layer, allowed]) => ({ layer, allowed: Boolean(allowed) })),
        }),
      }),
      renderCardSection({
        eyebrow: "Artifacts",
        title: "Missing / Stale / Invalid Sources",
        subtitle: "Each row includes why it matters and which command regenerates it.",
        body: renderSimpleTable({
          columns: [
            { label: "Artifact", key: "artifact_id" },
            { label: "Status", key: "status" },
            { label: "Reason", key: "reason" },
            { label: "Run", key: "generated_by_command" },
            { label: "Blocks", key: "downstream_capabilities_blocked", render: (row) => safeList(row.downstream_capabilities_blocked).join(", ") || "none" },
          ],
          rows: missing.slice(0, 20),
          emptyMessage: "All registered artifacts are current and valid.",
        }),
      }),
      renderCardSection({
        eyebrow: "Capabilities",
        title: "Dependency Graph",
        subtitle: "Capabilities cannot pass without current evidence.",
        body: renderSimpleTable({
          columns: [
            { label: "Capability", key: "capability" },
            { label: "Allowed", key: "allowed", render: (row) => renderStatusPill(row.allowed ? "true" : "false", row.allowed ? "healthy" : "blocked", {}) },
            { label: "Blocking artifacts", key: "missing_or_blocking_artifacts", render: (row) => safeList(row.missing_or_blocking_artifacts).join(", ") || "none" },
            { label: "Reason", key: "reason" },
          ],
          rows: Object.entries(graph).map(([capability, row]) => ({ capability, ...(row || {}) })),
        }),
      }),
      renderCardSection({
        eyebrow: "Recovery",
        title: "Recovery Commands",
        subtitle: "Exact commands and validation evidence expected after recovery.",
        body: renderSimpleTable({
          columns: [
            { label: "Artifact", key: "artifact_id" },
            { label: "Run", key: "generated_by_command" },
            { label: "Validate", key: "validates_with_command" },
            { label: "Expected output", key: "expected_path" },
          ],
          rows: recovery.slice(0, 20),
          emptyMessage: "No recovery commands are required.",
        }),
      }),
      renderCardSection({
        eyebrow: "Claim Guard",
        title: "Do Not Claim",
        subtitle: "Forbidden claims remain visible even when advisory UI is available.",
        body: claims.length
          ? `<div class="line-list">${claims.map((item) => `<div>${escapeHtml(item)}</div>`).join("")}</div>`
          : `<div class="empty-state">No forbidden claims were emitted.</div>`,
      }),
      renderCardSection({
        eyebrow: "Commands",
        title: "Suggested Next Commands",
        subtitle: "Read-only checks first; manual capture only records externally executed fills.",
        body: `<div class="line-list">${suggestedCommands.map((item) => `<div>${escapeHtml(item)}</div>`).join("")}</div>`,
      }),
    ].join(""),
    contextHtml: renderCardSection({
      eyebrow: "Evidence Domains",
      title: "Domain Status",
      subtitle: "Registered artifacts grouped by truth domain.",
      body: [
        renderSimpleTable({
          columns: [
            { label: "Domain", key: "domain" },
            { label: "OK", key: "ok" },
            { label: "Blocked", key: "blocked" },
          ],
          rows: domains.map((domain) => {
            const rows = statuses.filter((row) => (row.domain || "unknown") === domain);
            return {
              domain,
              ok: String(rows.filter((row) => row.status === "OK").length),
              blocked: String(rows.filter((row) => row.status !== "OK").length),
            };
          }),
        }),
        renderDefinitionRows([
          { label: "Artifacts fixed", value: safeList(lastTransition.artifacts_fixed).join(", ") || "none" },
          { label: "Artifacts degraded", value: safeList(lastTransition.artifacts_degraded).join(", ") || "none" },
          { label: "Recovery burden delta", value: String(lastTransition.recovery_burden_delta ?? 0) },
        ]),
      ].join(""),
    }),
  };
}

async function renderAegisOperatorCockpitPage() {
  let payload;
  try {
    payload = ["opportunities", "today", "candidates", "journal", "runtime_timeline"].includes(workflow)
      ? await fetchAegisOperatorStateSnapshotLatest()
      : await fetchAegisOperatorCockpit();
  } catch (error) {
    return {
      title: "Operator Cockpit",
      meta: "Backend unavailable; canonical operator state could not be loaded.",
      html: renderCardSection({
        eyebrow: "BACKEND_UNAVAILABLE",
        title: "Operator Cockpit Unavailable",
        subtitle: "The read-only cockpit API could not return Canonical Operator State.",
        body: renderDefinitionRows([
          { label: "Endpoint", value: error?.operatorSafe?.endpointAttempted || "/api/aegis/operator-cockpit" },
          { label: "Build canonical state", value: "npm run aegis:canonical-operator-state" },
          { label: "Build operator brief", value: "npm run aegis:operator-brief" },
        ]),
      }),
      contextHtml: "",
    };
  }

  const canonical = payload.canonical_operator_state || {};
  const runtime = payload.runtime || {};
  const safety = payload.safety || canonical.safety || {};
  const candidates = payload.candidate_decisions_corrections || {};
  const topCandidates = safeList(payload.top_candidates);
  const actions = safeList(payload.actions_required);
  const sleeves = payload.sleeve_warnings || {};
  const research = payload.research_priorities || {};
  const governance = payload.governance_approvals || {};
  const eventTriggers = safeList(payload.event_triggers);
  const noAction = safeList(payload.no_action_now);
  const drilldowns = safeList(payload.drilldown_links);
  const missingInputs = safeList(payload.missing_inputs);
  const conflicts = safeList(payload.conflicts);
  const runtimeTruth = runtime.runtime_truth_classification || "UNKNOWN";
  const readinessLayer = runtime.highest_readiness_layer || "UNKNOWN";
  const disabled = runtime.disabled_by_policy || {};
  const manualCapture = runtime.manual_trade_capture_allowed ?? runtime.manual_capture_allowed;
  const advisoryStatus = runtime.advisory_status || "UNKNOWN";
  const allCandidateRows = [
    ...safeList(candidates.awaiting_decision),
    ...safeList(candidates.watchlisted),
    ...safeList(candidates.needs_more_evidence),
    ...safeList(candidates.dismissed),
    ...safeList(candidates.expired),
    ...safeList(candidates.approved_or_traded),
    ...safeList(candidates.ignored),
    ...safeList(candidates.deferred),
    ...safeList(candidates.awaiting_outcome),
    ...safeList(candidates.corrected),
  ];
  const correctedRows = safeList(candidates.corrected);

  return {
    title: "Operator Cockpit",
    meta: "Daily cockpit powered by Canonical Operator State. Read-only; no broker actions are exposed.",
    html: [
      renderCardSection({
        eyebrow: payload.status || "UNKNOWN",
        title: "Today",
        subtitle: "What matters today, from the canonical operator read model.",
        body: `
          <div class="metric-grid">
            ${renderMetricCard({ label: "Runtime truth", value: runtimeTruth })}
            ${renderMetricCard({ label: "Readiness layer", value: readinessLayer })}
            ${renderMetricCard({ label: "Manual capture", value: String(manualCapture === true) })}
            ${renderMetricCard({ label: "Advisory status", value: advisoryStatus })}
            ${renderMetricCard({ label: "Actions", value: String(actions.length) })}
            ${renderMetricCard({ label: "Top candidates", value: String(topCandidates.length) })}
            ${renderMetricCard({ label: "Missing inputs", value: String(missingInputs.length) })}
            ${renderMetricCard({ label: "Conflicts", value: String(conflicts.length) })}
          </div>
          ${runtimeTruth === "PARTIAL_CONTEXT" ? `<div class="callout danger" style="margin-top:12px;">PARTIAL_CONTEXT: missing, stale, or invalid source evidence is visible below and no hidden readiness upgrade is inferred.</div>` : ""}
          ${renderDefinitionRows([
            { label: "Target mode", value: runtime.target_operating_mode || canonical.target_operating_mode || "HUMAN_APPROVED_ADVISORY_RUNTIME" },
            { label: "Live broker trading", value: disabled.live_broker_trading || runtime.live_broker_trading_policy || "DISABLED_BY_DESIGN" },
            { label: "Autonomous execution", value: disabled.autonomous_execution || runtime.autonomous_execution_policy || "DISABLED_BY_DESIGN" },
            { label: "Broker submit/transmit", value: disabled.broker_submit_transmit || runtime.broker_submit_transmit_policy || "DISABLED_BY_DESIGN" },
            { label: "Source read model", value: payload.source_read_model || "canonical_operator_state.v1.json" },
            { label: "Operator brief", value: payload.brief_read_model || "operator_brief.v1.json" },
          ])}
        `,
      }),
      renderCardSection({
        eyebrow: actions.length ? "ACTION_REQUIRED" : "CLEAR",
        title: "Action Required",
        subtitle: "Highest-priority operator actions in deterministic canonical order.",
        body: renderSimpleTable({
          columns: [
            { label: "Priority", key: "priority" },
            { label: "Type", key: "type" },
            { label: "Title", key: "title" },
            { label: "Reason", key: "reason" },
            { label: "Command", key: "suggested_command" },
          ],
          rows: actions.slice(0, 16),
          emptyMessage: "No operator action is required right now.",
        }),
      }),
      renderCardSection({
        eyebrow: topCandidates.length ? "REVIEW" : "NO_CANDIDATES",
        title: "Top Candidates",
        subtitle: "Advisory candidates only. Commands record operator decisions and preserve append-only history.",
        body: renderCockpitCandidateCards(topCandidates),
      }),
      renderCardSection({
        eyebrow: "CANDIDATE_HISTORY",
        title: "Candidate Decision History",
        subtitle: "Latest state comes from Candidate Lifecycle; original decisions and corrections remain auditable.",
        body: [
          renderDefinitionRows([
            { label: "Review required", value: String(safeList(candidates.review_required || candidates.awaiting_decision).length) },
            { label: "Watchlisted", value: String(safeList(candidates.watchlisted).length) },
            { label: "Needs more evidence", value: String(safeList(candidates.needs_more_evidence).length) },
            { label: "Dismissed", value: String(safeList(candidates.dismissed).length) },
            { label: "Expired", value: String(safeList(candidates.expired).length) },
            { label: "Awaiting outcome", value: String(safeList(candidates.awaiting_outcome).length) },
            { label: "Corrected", value: String(correctedRows.length) },
          ]),
          renderSimpleTable({
            columns: [
              { label: "Candidate", key: "candidate_id" },
              { label: "Symbol", key: "symbol" },
              { label: "Review state", render: (row) => escapeHtml(row.review_state || row.operator_review_status || "UNKNOWN") },
              { label: "Latest note", render: (row) => escapeHtml(row.latest_operator_note || row.current_operator_note || "") },
              { label: "Review events", render: (row) => escapeHtml(row.review_action_history_count ?? row.audit_history_count ?? "0") },
              { label: "Expires", render: (row) => escapeHtml(row.review_expires_at || "n/a") },
              { label: "Executable?", render: (row) => escapeHtml(row.executable_status || "NON_EXECUTABLE") },
            ],
            rows: allCandidateRows.slice(0, 24),
            emptyMessage: "No candidate decisions or corrections are recorded.",
          }),
        ].join(""),
      }),
      renderCardSection({
        eyebrow: "REVIEW_LEDGER",
        title: "Candidate Review Ledger",
        subtitle: "Active and historical review candidates from the append-only review ledger. Review actions are audit-only.",
        body: renderCandidateReviewLedgerTables(payload.opportunities || {}),
      }),
      renderCardSection({
        eyebrow: "SLEEVES",
        title: "Sleeve Warnings",
        subtitle: "Sleeve challenger and attribution warnings are review items only.",
        body: renderSimpleTable({
          columns: [
            { label: "Bucket", key: "bucket" },
            { label: "Sleeve", key: "sleeve_id" },
            { label: "Recommendation", key: "recommendation" },
            { label: "Reason", key: "reason" },
          ],
          rows: flattenCockpitSleeves(sleeves),
          emptyMessage: "No sleeve warnings are available.",
        }),
      }),
      renderCardSection({
        eyebrow: "RESEARCH",
        title: "Research",
        subtitle: "Priority tasks, new failure-derived tasks, review requirements, and sleeve review candidates.",
        body: renderSimpleTable({
          columns: [
            { label: "Bucket", key: "bucket" },
            { label: "ID", key: "task_id" },
            { label: "Title", key: "title" },
            { label: "Status", key: "status" },
            { label: "Next", key: "recommended_next_step" },
          ],
          rows: flattenCockpitResearch(research),
          emptyMessage: "No research tasks require operator review.",
        }),
      }),
      renderCardSection({
        eyebrow: "REGIME_EVENTS",
        title: "Regime / Event Triggers",
        subtitle: "Triggered sleeve runs are advisory-only and require manual review.",
        body: [
          renderDefinitionRows([
            { label: "Regime status", value: payload.regime?.status || payload.regime?.evidence_quality || "UNKNOWN" },
            { label: "Volatility", value: payload.regime?.volatility_regime || "UNKNOWN" },
            { label: "Trend", value: payload.regime?.trend_regime || "UNKNOWN" },
            { label: "Event regime", value: payload.regime?.event_regime || "UNKNOWN" },
          ]),
          renderSimpleTable({
            columns: [
              { label: "Trigger", key: "trigger_id" },
              { label: "Condition", key: "detected_condition" },
              { label: "Sleeves", key: "selected_sleeve_ids", render: (row) => safeList(row.selected_sleeve_ids).join(", ") || "none" },
              { label: "Candidates", key: "candidate_count" },
              { label: "Status", key: "status" },
            ],
            rows: eventTriggers.slice(0, 12),
            emptyMessage: "No event/regime-triggered sleeve runs are recorded.",
          }),
        ].join(""),
      }),
      renderCardSection({
        eyebrow: "GOVERNANCE",
        title: "Governance Approvals",
        subtitle: "Human approval is required; this cockpit does not approve or mutate sleeves.",
        body: renderSimpleTable({
          columns: [
            { label: "Bucket", key: "bucket" },
            { label: "Recommendation", key: "recommendation_id" },
            { label: "Type", key: "type" },
            { label: "Target", key: "target" },
            { label: "Status", key: "approval_status" },
          ],
          rows: flattenCockpitGovernance(governance),
          emptyMessage: "No governance approvals are pending.",
        }),
      }),
      renderCardSection({
        eyebrow: "NO_ACTION_NOW",
        title: "No Action Now",
        subtitle: "Low-priority or explicitly inactive items.",
        body: noAction.length ? `<details><summary>${escapeHtml(String(noAction.length))} no-action item(s)</summary><div class="line-list">${noAction.map((item) => `<div>${escapeHtml(item)}</div>`).join("")}</div></details>` : `<div class="empty-state">No no-action items were reported.</div>`,
      }),
    ].join(""),
    contextHtml: [
      renderCardSection({
        eyebrow: "Drilldown",
        title: "Source Artifact Links",
        subtitle: "Raw JSON is drilldown only; the cockpit itself reads the canonical projection.",
        body: renderSimpleTable({
          columns: [
            { label: "ID", key: "id" },
            { label: "Path", key: "path" },
            { label: "Hash", key: "hash" },
          ],
          rows: [
            { id: "canonical_operator_state", path: payload.source_paths?.canonical_operator_state || "", hash: canonical.source_hashes?.canonical_operator_state || "" },
            { id: "operator_brief", path: payload.source_paths?.operator_brief || "", hash: canonical.source_hashes?.operator_brief || "" },
            ...drilldowns,
          ].filter((row) => row.path || row.id),
          emptyMessage: "No drilldown artifacts are available.",
        }),
      }),
      renderCardSection({
        eyebrow: "Safety",
        title: "Read-Only Boundary",
        subtitle: "No POST/PATCH cockpit endpoints are exposed.",
        body: renderDefinitionRows([
          { label: "UI read-only", value: String(payload.read_only === true) },
          { label: "Broker execution", value: String(safety.broker_execution_allowed === true) },
          { label: "Broker submit/transmit", value: String(safety.broker_submit_transmit_allowed === true) },
          { label: "Autonomous execution", value: String(safety.autonomous_execution_allowed === true) },
          { label: "Automatic approval", value: String(safety.automatic_approval_allowed === true) },
          { label: "Automatic sleeve mutation", value: String(safety.automatic_sleeve_mutation_allowed === true) },
        ]),
      }),
      missingInputs.length ? renderCardSection({
        eyebrow: "Missing Inputs",
        title: "Partial Canonical Sections",
        subtitle: "Missing inputs are visible and not invented.",
        body: `<div class="line-list">${missingInputs.map((item) => `<div>${escapeHtml(item)}</div>`).join("")}</div>`,
      }) : "",
    ].join(""),
  };
}

async function renderAegisWorkflowPage(workflow) {
  if (workflow === "theses") return renderAegisThesesWorkflow();
  let payload;
  try {
    payload = ["opportunities", "today", "candidates", "journal", "runtime_timeline"].includes(workflow)
      ? await fetchAegisOperatorStateSnapshotLatest()
      : await fetchAegisOperatorCockpit();
  } catch (error) {
    return {
      title: workflowTitle(workflow),
      meta: "Backend unavailable; canonical operator state could not be loaded.",
      html: renderWorkflowCard({
        payload: {},
        sourceKey: "canonical_operator_state",
        fieldPath: "canonical_operator_state",
        whyShown: "The workflow page cannot render without the canonical operator projection.",
        eyebrow: "BACKEND_UNAVAILABLE",
        title: "Canonical Operator State Unavailable",
        subtitle: "The workflow UI reads the operator cockpit API, which is backed by canonical_operator_state.v1.json.",
        body: renderDefinitionRows([
          { label: "Endpoint", value: error?.operatorSafe?.endpointAttempted || "/api/aegis/operator-cockpit" },
          { label: "Build canonical state", value: "npm run aegis:canonical-operator-state" },
        ]),
      }),
      contextHtml: "",
    };
  }
  if (payload && payload.data && typeof payload.data === "object" && payload.data.schema_id === "operator_state_snapshot") {
    payload = { ...payload.data, api_envelope: { ok: payload.ok, degraded: payload.degraded, errors: payload.errors, next_action: payload.next_action } };
  }
  if (["opportunities", "today", "candidates", "journal", "runtime_timeline"].includes(workflow) && payload.status === "MISSING") {
    return renderMissingCanonicalTodayWorkflow(payload);
  }
  if (["opportunities", "today", "candidates", "journal", "runtime_timeline"].includes(workflow)) {
    payload.operator_state_snapshot = payload.operator_state_snapshot || payload;
    payload.operator_today_projection = payload.operator_today_projection || payload.today_projection || {};
    payload.operator_task_projection = payload.operator_task_projection || payload.operator_tasks_projection || {};
    payload.active_opportunity_projection = payload.active_opportunity_projection || payload.opportunities_projection || {};
    payload.system_diagnostic_projection = payload.system_diagnostic_projection || payload.system_diagnostics_projection || {};
    payload.passive_health_projection = payload.passive_health_projection || {};
    payload.eod_opportunity_outcome_report = payload.eod_opportunity_outcome_report || payload.eod_outcome_projection || {};
    const ids = [
      payload.operator_today_projection?.snapshot_id,
      payload.operator_task_projection?.snapshot_id,
      payload.active_opportunity_projection?.snapshot_id,
      payload.system_diagnostic_projection?.snapshot_id,
      payload.passive_health_projection?.snapshot_id,
      payload.eod_opportunity_outcome_report?.snapshot_id,
    ].filter(Boolean);
    const mismatch = ids.length > 1 && new Set(ids).size > 1;
    payload.projection_snapshot_mismatch = mismatch;
  }
  if (workflow === "edge_lab" || workflow === "research") {
    try {
      const settled = await Promise.allSettled([
        fetchResearchLabEdgeProjection("slv_etf_drop_reversion_v1"),
        fetchResearchLabChallengerComparisonReports(),
        fetchResearchLabHumanReviewDossiers(),
        fetchResearchLabExpectancyDrift(),
        fetchResearchLabRegimeFragility(),
        fetchResearchLabSleeveStability(),
        fetchResearchLabPaperTrialInventory(),
        fetchResearchConsole(),
      ]);
      const valueAt = (index, fallback = {}) => settled[index]?.status === "fulfilled" ? settled[index].value : fallback;
      payload.research_lab_projection = valueAt(0, {
        ok: false,
        read_only: true,
        error: "Research Lab projection unavailable.",
        research_label: "Research / paper-trial information only. No broker execution. No live trading. No autonomous execution. No achieved portfolio performance claim. Backtests/model outputs are hypothetical research evidence.",
      });
      payload.research_lab_command_sources = {
        challengerComparison: valueAt(1),
        humanReviewDossiers: valueAt(2),
        expectancyDrift: valueAt(3),
        regimeFragility: valueAt(4),
        sleeveStability: valueAt(5),
        paperTrialInventory: valueAt(6),
        researchConsole: valueAt(7),
      };
    } catch (error) {
      payload.research_lab_projection = {
        ok: false,
        read_only: true,
        error: error?.message || "Research Lab projection unavailable.",
        research_label: "Research / paper-trial information only. No broker execution. No live trading. No autonomous execution. No achieved portfolio performance claim. Backtests/model outputs are hypothetical research evidence.",
      };
      payload.research_lab_command_sources = {};
    }
  }
  if (workflow === "opportunities" || workflow === "today") return withOperationalTimestamps(renderAegisTodayWorkflow(payload), payload);
  if (workflow === "candidates") return withOperationalTimestamps(renderAegisCandidatesWorkflow(payload), payload);
  if (workflow === "runtime_timeline" || workflow === "scheduler") return renderAegisRuntimeTimelineWorkflow(payload);
  if (workflow === "theses") return renderAegisThesesWorkflow();
  if (workflow === "performance" || workflow === "review") return withOperationalTimestamps(renderAegisReviewWorkflow(payload), payload);
  if (workflow === "edge_lab" || workflow === "research") return withOperationalTimestamps(renderAegisResearchWorkflow(payload), payload);
  return withOperationalTimestamps(renderAegisHistoryWorkflow(payload), payload);
}


async function renderAegisThesesWorkflow() {
  const consolePayload = await fetchResearchConsole().catch((error) => ({ ok: false, error: error?.message || "Hypotheses unavailable." }));
  return {
    title: "Hypotheses",
    meta: "Legacy deep link now opens the primary Hypotheses workspace.",
    html: renderHypothesesWorkspace(consolePayload, {}),
    contextHtml: "",
    hideContextRail: true,
  };
}

function renderMissingCanonicalTodayWorkflow(payload = {}) {
  const sourceStatus = payload.source_status || {};
  return {
    title: "Opportunities",
    meta: "What trades deserve attention right now?",
    html: [
      renderCardSection({
        eyebrow: "SETUP_REQUIRED",
        title: "Today's operator state has not been generated yet.",
        subtitle: "The cockpit reads canonical_operator_state.v1.json. Generate the operator state to populate today's candidates, actions, and workflow.",
        body: [
          `<div class="empty-state">Run the safe report command below, then refresh this page.</div>`,
          `<pre class="code-block">npm run aegis:canonical-operator-state && npm run aegis:operator-brief</pre>`,
          renderDefinitionRows([
            { label: "canonical_state", value: sourceStatus.canonical_state || "missing" },
            { label: "operator_brief", value: sourceStatus.operator_brief || "missing" },
            { label: "runtime_truth", value: sourceStatus.runtime_truth || "unavailable" },
            { label: "generated_at", value: "n/a" },
          ]),
        ].join(""),
      }),
      renderCardSection({
        eyebrow: "SAFETY",
        title: "Read-Only Boundary",
        subtitle: "Generating the operator state writes reports only. It does not execute trades or approve anything.",
        body: renderDefinitionRows([
          { label: "Broker execution", value: String(payload.safety?.broker_execution_allowed === true) },
          { label: "Broker submit/transmit", value: String(payload.safety?.broker_submit_transmit_allowed === true) },
          { label: "Autonomous execution", value: String(payload.safety?.autonomous_execution_allowed === true) },
        ]),
      }),
    ].join(""),
    contextHtml: workflowContextHtml(payload),
  };
}

function workflowTitle(workflow) {
  return workflow === "opportunities" || workflow === "today"
    ? "Dashboard"
    : workflow === "candidates"
      ? "Candidates"
      : workflow === "performance" || workflow === "review"
        ? "System Health"
        : workflow === "theses"
          ? "Hypotheses"
          : workflow === "edge_lab" || workflow === "research"
            ? "Research"
            : "Captured Trades";
}

function renderWorkflowCard({ payload, sourceKey, fieldPath, whyShown, eyebrow, title, subtitle, body }) {
  return renderCardSection({
    eyebrow,
    title,
    subtitle,
    body: `${body}${renderCanonicalEvidenceBlock(payload, sourceKey, fieldPath, whyShown)}`,
  });
}

function renderCanonicalEvidenceBlock(payload = {}, sourceKey = "canonical_operator_state", fieldPath = "", whyShown = "") {
  const canonical = payload.canonical_operator_state || {};
  const freshness = canonical.freshness || {};
  const source = freshness[sourceKey] || {};
  const sourcePaths = payload.source_paths || {};
  const canonicalPath = sourcePaths.canonical_operator_state || source.path || "";
  const sourcePath = source.path || canonicalPath || "Source artifact missing; run refresh command.";
  const freshnessLabel = source.freshness_status || (source.path || canonicalPath ? "Currentness not reported" : "Not generated yet today.");
  return `
    <details class="support-note" style="margin-top:12px;">
      <summary>Canonical evidence</summary>
      ${renderDefinitionRows([
        { label: "Canonical field path", value: fieldPath || "canonical_operator_state" },
        { label: "Source artifact path", value: sourcePath },
        { label: "Freshness status", value: readableStatus(freshnessLabel) },
        { label: "Generated / last updated", value: source.generated_at || canonical.generated_at_utc || payload.generated_at_utc || "Not generated yet today." },
        { label: "Why shown", value: whyShown || "Included by workflow projection." },
        { label: "Drilldown link", value: sourcePath },
      ])}
    </details>
  `;
}

function readableStatus(value) {
  const raw = String(value ?? "").trim();
  if (!raw) return "Not reported";
  if (raw === "MISSING") return "Not generated yet today.";
  if (raw === "UNKNOWN") return "Waiting for current-day evidence.";
  if (raw === "n/a") return "Not available for this section.";
  return raw;
}

function canonicalPayloadParts(payload = {}) {
  const canonical = payload.canonical_operator_state || {};
  return {
    canonical,
    runtime: payload.runtime || canonical.runtime || {},
    candidates: payload.candidate_decisions_corrections || canonical.candidates || {},
    opportunities: payload.opportunities || canonical.opportunities || {},
    edgeLab: canonical.edge_lab || {},
    performance: canonical.performance || {},
    positions: payload.positions || canonical.positions || canonical.opportunities?.position_management || {},
    journal: canonical.journal || {},
    sleeves: payload.sleeve_warnings || canonical.sleeves || {},
    research: mergeCanonicalResearchState(canonical.research || {}, payload.research_priorities || {}),
    governance: payload.governance_approvals || canonical.governance || {},
    regime: payload.regime || canonical.regime || {},
    eventTriggers: safeList(payload.event_triggers || canonical.event_triggers),
    actions: safeList(payload.actions_required || canonical.actions_required),
    topCandidates: safeList(payload.top_candidates || canonical.top_candidates),
    warnings: safeList(payload.warnings || canonical.warnings),
    drilldowns: safeList(payload.drilldown_links || canonical.drilldown_index),
  };
}

function mergeCanonicalResearchState(canonicalResearch = {}, topLevelResearch = {}) {
  const merged = { ...(canonicalResearch || {}) };
  const bucketKeys = [
    "captured_hypotheses",
    "active_hypotheses",
    "validated_hypotheses",
    "rejected_hypotheses",
    "archived_hypotheses",
    "needs_classification",
    "test_fixtures_excluded",
    "duplicates",
    "priority_tasks",
    "new_tasks",
    "review_required",
    "completed",
    "rejected",
  ];
  for (const key of bucketKeys) {
    const canonicalRows = safeList(canonicalResearch?.[key]);
    const topRows = safeList(topLevelResearch?.[key]);
    merged[key] = canonicalRows.length ? canonicalRows : topRows;
  }
  merged.classification_summary = canonicalResearch?.classification_summary || topLevelResearch?.classification_summary || {};
  merged.source_artifacts = safeList(canonicalResearch?.source_artifacts).length ? safeList(canonicalResearch.source_artifacts) : safeList(topLevelResearch?.source_artifacts);
  merged.source_hashes = canonicalResearch?.source_hashes || topLevelResearch?.source_hashes || {};
  merged.source_authority = canonicalResearch?.source_authority || topLevelResearch?.source_authority || "Research Lab artifacts";
  return merged;
}

function renderWorkflowEmptyState({ title, message, normality = "normal", nextActions = [] }) {
  return `
    <div class="empty-state">
      <strong>${escapeHtml(title)}</strong>
      <div>${escapeHtml(message)}</div>
      <div style="margin-top:8px;">${escapeHtml(normality)}</div>
      ${safeList(nextActions).length ? `<div class="line-list" style="margin-top:8px;">${safeList(nextActions).map((action) => `<div><code>${escapeHtml(action)}</code></div>`).join("")}</div>` : ""}
    </div>
  `;
}

function hasRows(rows) {
  return safeList(rows).length > 0;
}



function renderReadinessDomainStatuses(ticket = {}) {
  const statuses = ticket.domain_statuses || {};
  const blockersByDomain = ticket.blockers_by_domain || {};
  const labels = [
    ["market_data", "Market Data"],
    ["trade_construction", "Construction"],
    ["capital_authority", "Capital"],
    ["stop_risk", "Stop/Risk"],
    ["manual_capture", "Manual Capture"],
    ["paper_submit", "Paper Submit"],
    ["execution", "Execution"],
  ];
  return `
    <div class="compact-table" style="margin-top:10px;">
      ${renderSimpleTable({
        columns: [
          { label: "Domain", render: (row) => escapeHtml(row.label) },
          { label: "Status", render: (row) => escapeHtml(row.status || "UNKNOWN") },
          { label: "Blockers", render: (row) => escapeHtml(safeList(blockersByDomain[row.key]).map((blocker) => blocker.code || blocker.message || "blocker").join(", ") || "none") },
        ],
        rows: labels.map(([key, label]) => ({ key, label, status: statuses[key] || "UNKNOWN" })),
        emptyMessage: "No readiness domain evaluations are available.",
      })}
    </div>
  `;
}

function renderTradeTicketSummary(manual = {}, fieldValue = (value) => value) {
  const ticket = manual.trade_ticket_projection_v1 || (manual.schema_id === "trade_ticket_projection" ? manual : {}) || {};
  const complete = ticket.manual_capture_ready === true;
  const symbol = ticket.symbol || manual.symbol || "Selected exposure";
  const direction = ticket.direction || manual.direction || "";
  if (complete) {
    return `
      <div class="trade-ticket-compact ready">
        <div class="trade-ticket-heading"><strong>${escapeHtml(symbol)} ${escapeHtml(direction)}</strong></div>
        <div>Entry: ${escapeHtml(fieldValue(ticket.entry_reference_price))}</div>
        <div>Qty: ${escapeHtml(fieldValue(ticket.suggested_quantity ?? ticket.quantity))}</div>
        <div>Notional: ${escapeHtml(fieldValue(ticket.suggested_notional || ticket.notional))}</div>
        <div>Stop: ${escapeHtml(fieldValue(ticket.stop_price || ticket.invalidation_level))}</div>
        <div>Risk: ${escapeHtml(fieldValue(ticket.max_loss_estimate || ticket.risk_per_share))}</div>
        <div>Status: ${escapeHtml(ticket.capture_status || "Ready for manual paper capture")}</div>
      </div>
    `;
  }
  const blockersByDomain = ticket.blockers_by_domain || {};
  const blockerRows = Object.entries(blockersByDomain).flatMap(([domain, blockers]) => safeList(blockers).map((row) => ({ domain, ...row })));
  return `
    <div class="trade-ticket-compact blocked">
      <div class="trade-ticket-heading"><strong>${escapeHtml(symbol)} ${escapeHtml(direction)}</strong></div>
      <div>Status: Not capture-ready</div>
      <div class="metric-grid" style="margin-top:10px;">
        ${renderMetricCard({ label: "Entry", value: fieldValue(ticket.entry_reference_price) })}
        ${renderMetricCard({ label: "Qty", value: fieldValue(ticket.suggested_quantity ?? ticket.quantity) })}
        ${renderMetricCard({ label: "Stop", value: fieldValue(ticket.stop_price || ticket.invalidation_level) })}
        ${renderMetricCard({ label: "Risk", value: fieldValue(ticket.max_loss_estimate || ticket.risk_per_share) })}
      </div>
      <div class="section-eyebrow" style="margin-top:10px;">Domain blockers</div>
      <ul class="compact-list">
        ${blockerRows.length ? blockerRows.map((row) => `<li><strong>${escapeHtml(row.domain)}: ${escapeHtml(row.code || "BLOCKED")}</strong><div class="muted-mini">${escapeHtml(row.message || "Readiness domain is blocked.")}</div></li>`).join("") : `<li>${escapeHtml(ticket.trade_ticket_status || "blocked")}</li>`}
      </ul>
      <div class="section-eyebrow" style="margin-top:10px;">Next action</div>
      <div>${escapeHtml(safeList(ticket.next_required_actions)[0]?.action || ticket.next_action || "Resolve readiness domain blockers.")}</div>
    </div>
  `;
}

// Compatibility alias retained outside selected trade display authority: manual_capture_candidate_v1
function renderManualCaptureProjectionPanel(payload = {}) {
  const snapshot = payload.data && typeof payload.data === "object" ? payload.data : payload;
  // Legacy snapshot.trade_candidate_projection_v1 remains outside manual capture authority.
  const manual = snapshot.trade_ticket_projection_v1 || snapshot.trade_ticket_projection || (snapshot.schema_id === "trade_ticket_projection" ? snapshot : {}) || {};
  const market = manual.data_freshness_status || {};
  const blockers = safeList(manual.blockers);
  const firstBlocker = blockers[0]?.code || manual.conversion_blocker_code || "";
  const available = manual.candidate_available === true || Boolean(manual.selected_exposure_intent_id);
  const stale = blockers.some((row) => row.code === "STALE_MARKET_DATA_BLOCKS_CONVERSION") || market.status === "STALE_OR_MISSING";
  const staleSource = manual.stale_source === true;
  const suppressedCount = Number(manual.suppression_watchlist_context?.suppressed_count ?? 0);
  const ticket = manual;
  const caseState = manual.lifecycle_state || manual.current_state || manual.state || "OBSERVED";
  const captureRecord = manual.manual_capture_record && typeof manual.manual_capture_record === "object" ? manual.manual_capture_record : {};
  const completedCapture = manual.captured_read_only === true
    || ["CAPTURED_HISTORICAL", "CAPTURED_MANUALLY"].includes(String(caseState || manual.historical_state || "").toUpperCase())
    || ["captured_manually", "partial"].includes(String(captureRecord.capture_status || manual.capture_status || "").toLowerCase());
  const lineageStatus = manual.lineage_status || manual.trade_ticket_lineage_v1?.lineage_status || "MISSING_SUBMIT_BOUNDARY";
  const submitBoundaryStatus = manual.submit_boundary_status || manual.submit_boundary_precheck_v1?.validation_status || "MISSING";
  const lineageEditable = manual.editable === true && lineageStatus === "ACTIVE_CURRENT" && submitBoundaryStatus === "VALIDATED";
  const blocked = completedCapture ? false : manual.manual_capture_ready !== true;
  const blockedReviewLabel = "Review only - blocked";
  const statusLabel = completedCapture ? "Completed" : (!available ? "No selected exposure" : manual.manual_capture_ready === true ? "Complete trade ticket" : "Not capture-ready");
  const fieldValue = (value) => {
    if (value === null || value === undefined || value === "") return "MISSING";
    return String(value);
  };
  const summary = !available
    ? "No selected exposure for the latest run."
    : blocked
      ? `${manual.symbol || "Selected exposure"} trade lifecycle case is ${caseState}: ${firstBlocker || "MISSING_REQUIRED_TRADE_DATA"}.`
      : `${manual.symbol || "Selected exposure"} is CAPTURE_READY. Aegis did not execute or submit anything.`;
  if (completedCapture) {
    return renderWorkflowCard({
      payload,
      sourceKey: "captured_ticket_projection_v1",
      fieldPath: "captured_ticket_projection_v1",
      whyShown: "Completed captures are historical records. They are removed from the active manual-capture workflow and do not require current submit-boundary action.",
      eyebrow: "COMPLETED_CAPTURE",
      title: "Manual Capture Completed",
      subtitle: "No further action is required.",
      body: `
        <section class="completed-captures-section" aria-label="Completed Captures">
          <div class="completed-captures-section-header">
            <span>Historical Captures</span>
            <strong>Completed today</strong>
          </div>
          ${renderManualCaptureRecordForm(manual, { blocked: false, blocker: "", stale: false })}
        </section>
      `,
    });
  }

  const chips = [
    manual.symbol || "none",
    manual.direction || "not reported",
    manual.sleeve_id || manual.sleeve || "no sleeve",
    `Case: ${caseState}`,
    `Status: ${statusLabel}`,
    `Manual: ${manual.manual_capture_ready === true ? "READY" : "BLOCKED"}`,
    `Lineage: ${lineageStatus}`,
    `Submit precheck: ${submitBoundaryStatus}`,
    `Paper submit: ${manual.paper_submit_ready === true ? "READY" : "BLOCKED"}`,
    `Broker submit DISABLED`,
    `IB handshake NOT REQUIRED FOR MANUAL CAPTURE`,
    `Execution: ${manual.execution_ready === true ? "READY" : "NOT_ENABLED"}`,
    firstBlocker ? `Blocker: ${firstBlocker}` : "No readiness blocker",
    staleSource ? `stale_source: ${fieldValue(manual.source_day)}` : "stale_source: false",
  ];
  return renderWorkflowCard({
    payload,
    sourceKey: "trade_ticket_projection_v1",
    fieldPath: "trade_ticket_projection_v1",
    whyShown: "The UI renders selected trade/manual-capture state only from trade_ticket_projection_v1; raw selected exposures and Packet artifacts are not trade authority.",
    eyebrow: available ? "TRADE_TICKET_PROJECTION" : "NO_SELECTED_EXPOSURE",
    title: "Trade Lifecycle Case / Manual Capture",
    subtitle: statusLabel,
    body: `
      ${renderManualCaptureRecordForm(manual, { blocked, blocker: firstBlocker, stale })}
      <details class="raw-drawer manual-capture-diagnostics" style="margin-top:12px;">
        <summary>Technical diagnostics</summary>
        <div class="manual-capture-diagnostics-body">
          <div class="callout ${blocked ? "warning" : ""}"><strong>${escapeHtml(summary)}</strong></div>
          ${renderTradeTicketSummary(manual, fieldValue)}
          ${renderReadinessDomainStatuses(ticket)}
          <div class="compact-table" style="margin-top:10px;">
            <div class="section-eyebrow">Broker Submit / Paper Broker Sim readiness</div>
            ${renderSimpleTable({
              columns: [
                { label: "Readiness item", render: () => "ib_api_handshake_v1" },
                { label: "Manual capture", render: () => "NOT REQUIRED" },
                { label: "Broker submit / sim", render: () => "CHECK ONLY IF ENABLED" },
              ],
              rows: [{}],
              emptyMessage: "Broker submit and paper broker simulation are disabled.",
            })}
          </div>
          <div class="chip-list" style="margin:10px 0;">
            ${chips.map((chip) => `<span class="support-chip">${escapeHtml(chip)}</span>`).join("")}
          </div>
          <div class="metric-grid compact-manual-diagnostics-grid">
            ${renderMetricCard({ label: "Entry reference", value: fieldValue(ticket.entry_reference_price || manual.entry_reference_price) })}
            ${renderMetricCard({ label: "Quantity", value: fieldValue(ticket.suggested_quantity ?? manual.suggested_quantity ?? manual.quantity) })}
            ${renderMetricCard({ label: "Notional", value: fieldValue(ticket.suggested_notional || manual.suggested_notional || manual.notional_value) })}
            ${renderMetricCard({ label: "Stop / invalidation", value: fieldValue(ticket.stop_price || ticket.invalidation_level || manual.stop_price || manual.invalidation_level) })}
            ${renderMetricCard({ label: "Risk estimate", value: fieldValue(ticket.max_loss_estimate || ticket.risk_per_share || manual.max_loss_estimate || manual.risk_per_share) })}
            ${renderMetricCard({ label: "Allocation", value: fieldValue(manual.allocation_status || manual.submit_boundary_precheck_v1?.allocation_status) })}
            ${renderMetricCard({ label: "Allocation remaining", value: fieldValue(manual.allocation_limit_used?.remaining_risk || manual.submit_boundary_precheck_v1?.allocation_limit_used?.remaining_risk) })}
            ${renderMetricCard({ label: "Risk contract", value: fieldValue(manual.risk_contract_status || manual.submit_boundary_precheck_v1?.risk_contract_status) })}
            ${renderMetricCard({ label: "Risk limit", value: fieldValue(manual.risk_limit_used?.max_risk || manual.submit_boundary_precheck_v1?.risk_limit_used?.max_risk) })}
            ${renderMetricCard({ label: "Candidate identity", value: fieldValue(manual.candidate_identity_status || manual.submit_boundary_precheck_v1?.candidate_identity_status) })}
            ${renderMetricCard({ label: "Phase C status", value: fieldValue(manual.stale_order_plan_reason || manual.submit_boundary_precheck_v1?.stale_order_plan_reason || manual.actual_phasec_candidate_id) })}
            ${renderMetricCard({ label: "Fill price", value: fieldValue(manual.manual_capture_record?.fill_price || manual.fill_price) })}
            ${renderMetricCard({ label: "Fill time", value: fieldValue(manual.manual_capture_record?.fill_time) })}
            ${renderMetricCard({ label: "Capture status", value: fieldValue(manual.manual_capture_record?.capture_status) })}
          </div>
          <div class="compact-table" style="margin-top:10px;">
            ${renderSimpleTable({
              columns: [
                { label: "Selected exposure id", render: () => escapeHtml(fieldValue(manual.selected_exposure_intent_id)) },
                { label: "Source day/run", render: () => escapeHtml(`${fieldValue(manual.source_day)} / ${fieldValue(manual.source_run_id)}`) },
                { label: "RuntimeEvaluation", render: () => escapeHtml(fieldValue(manual.runtime_evaluation_hash)) },
                { label: "Lineage", render: () => escapeHtml(fieldValue(lineageStatus)) },
                { label: "Contract hash", render: () => escapeHtml(fieldValue(manual.construction_contract_hash)) },
                { label: "Paper intent", render: () => escapeHtml(fieldValue(manual.paper_intent_status)) },
                { label: "Conversion", render: () => escapeHtml(fieldValue(manual.conversion_evidence_status || manual.conversion_status)) },
                { label: "Allocation", render: () => escapeHtml(fieldValue(manual.allocation_status || manual.submit_boundary_precheck_v1?.allocation_status)) },
                { label: "Allocation hash", render: () => escapeHtml(fieldValue(manual.allocation_artifact_hash || manual.submit_boundary_precheck_v1?.allocation_artifact_hash)) },
                { label: "Risk contract", render: () => escapeHtml(fieldValue(manual.risk_contract_status || manual.submit_boundary_precheck_v1?.risk_contract_status)) },
                { label: "Risk hash", render: () => escapeHtml(fieldValue(manual.risk_contract_hash || manual.submit_boundary_precheck_v1?.risk_contract_hash)) },
                { label: "Risk measure", render: () => escapeHtml(fieldValue(manual.risk_measure || manual.submit_boundary_precheck_v1?.risk_measure)) },
                { label: "Candidate identity", render: () => escapeHtml(fieldValue(manual.candidate_identity_status || manual.submit_boundary_precheck_v1?.candidate_identity_status)) },
                { label: "Expected candidate", render: () => escapeHtml(fieldValue(manual.expected_candidate_id || manual.submit_boundary_precheck_v1?.expected_candidate_id)) },
                { label: "Actual Phase C", render: () => escapeHtml(fieldValue(manual.actual_phasec_candidate_id || manual.submit_boundary_precheck_v1?.actual_phasec_candidate_id)) },
                { label: "Submit", render: () => escapeHtml(fieldValue(submitBoundaryStatus)) },
                { label: "Market freshness", render: () => escapeHtml(fieldValue(manual.market_freshness_status || `${fieldValue(market.status)} ${fieldValue(market.observed_session)} -> ${fieldValue(market.expected_session)}`)) },
              ],
              rows: [manual],
              emptyMessage: "No selected exposure for the latest run.",
            })}
          </div>
          ${safeList(ticket.missing_fields || manual.missing_ticket_fields).length ? `<div class="compact-table" style="margin-top:10px;">${renderSimpleTable({
            columns: [
              { label: "Missing", render: (row) => escapeHtml(row.label || row.field || "") },
              { label: "Why", key: "why_missing" },
              { label: "Upstream step", key: "upstream_step" },
            ],
            rows: safeList(ticket.missing_fields || manual.missing_ticket_fields),
            emptyMessage: "No missing trade fields.",
          })}</div>` : ""}
          ${blockers.length ? `<div class="compact-table" style="margin-top:10px;">${renderSimpleTable({
            columns: [
              { label: "Blocker", key: "code" },
              { label: "Field", key: "field" },
              { label: "Message", key: "message" },
            ],
            rows: blockers,
            emptyMessage: "No missing trade fields.",
          })}</div>` : ""}
        </div>
      </details>
    `,
  });
}

function renderSuppressedWatchlistProjectionPanel(payload = {}) {
  const { opportunities } = canonicalPayloadParts(payload);
  const snapshot = payload.data && typeof payload.data === "object" ? payload.data : payload;
  const legacy = snapshot.suppressed_candidate_watchlist || snapshot.suppressed_candidate_watchlist_v1 || {};
  const suppressedRows = safeList(opportunities.suppressed_candidates).length ? safeList(opportunities.suppressed_candidates) : safeList(legacy.candidates);
  const watchlistRows = safeList(opportunities.watchlist_candidates);
  const rows = [...suppressedRows, ...watchlistRows].slice().sort((a, b) => Number(a.rank || 9999) - Number(b.rank || 9999));
  const byReason = rows.reduce((acc, row) => {
    const code = row.suppression_reason_code || row.selection_status || row.suppression_code || "WATCHLIST";
    acc[code] = (acc[code] || 0) + 1;
    return acc;
  }, {});
  const bySleeve = rows.reduce((acc, row) => {
    const sleeve = row.sleeve_id || "UNKNOWN";
    acc[sleeve] = (acc[sleeve] || 0) + 1;
    return acc;
  }, {});
  const reasonRows = Object.entries(byReason).map(([code, count]) => ({ code, count }));
  const sleeveRows = Object.entries(bySleeve).map(([sleeve_id, count]) => ({ sleeve_id, count }));
  return renderWorkflowCard({
    payload,
    sourceKey: safeList(opportunities.suppressed_candidates).length ? "candidate_portfolio_selection" : "operator_state_snapshot_v1",
    fieldPath: safeList(opportunities.suppressed_candidates).length ? "opportunities.suppressed_candidates" : "suppressed_candidate_watchlist_v1",
    whyShown: "Suppressed and watchlist candidates remain visible for operator context and future opportunity-cost learning, but they are not converted into trades.",
    eyebrow: rows.length ? "SUPPRESSED_OR_WATCHLIST" : "NO_SUPPRESSED_CANDIDATES",
    title: "Suppressed / Watchlist candidates",
    subtitle: `${rows.length} candidates. Grouped by reason and sleeve; top 10 shown by score band, not raw score.`,
    body: [
      `<div class="metric-grid">
        ${renderMetricCard({ label: "Suppressed", value: String(suppressedRows.length) })}
        ${renderMetricCard({ label: "Watchlist", value: String(watchlistRows.length) })}
        ${renderMetricCard({ label: "Reason groups", value: String(reasonRows.length) })}
        ${renderMetricCard({ label: "Sleeve groups", value: String(sleeveRows.length) })}
      </div>`,
      rows.length ? `<details class="raw-drawer" style="margin-top:12px;" open><summary>Suppressed / Watchlist candidates</summary>
        <div class="chip-list" style="margin:10px 0;">${reasonRows.map((row) => `<span class="support-chip">${escapeHtml(row.code)}: ${escapeHtml(String(row.count))}</span>`).join("")}</div>
        <div class="chip-list" style="margin:10px 0;">${sleeveRows.map((row) => `<span class="support-chip">${escapeHtml(row.sleeve_id)}: ${escapeHtml(String(row.count))}</span>`).join("")}</div>
        ${renderSimpleTable({
          columns: [
            { label: "Symbol", render: (row) => `<strong>${escapeHtml(row.symbol || "Candidate")}</strong>` },
            { label: "Sleeve", key: "sleeve_id" },
            { label: "Score band", render: (row) => escapeHtml(row.score_band || row.priority || "UNKNOWN") },
            { label: "Exposure cluster", render: (row) => escapeHtml(row.exposure_cluster || "UNKNOWN") },
            { label: "Reason", render: (row) => escapeHtml(row.operator_explanation || row.suppression_reason || row.why_not || "Suppressed by portfolio policy.") },
          ],
          rows: rows.slice(0, 10),
          emptyMessage: "No suppressed candidates are available.",
        })}
      </details>` : renderWorkflowEmptyState({ title: "No suppressed candidates.", message: "No same-sleeve or watchlist suppressions are available in the current projection.", normality: "Normal when there are no candidate collisions." }),
      rows.length ? `<details class="raw-drawer" style="margin-top:12px;"><summary>Show all advanced raw scores</summary>${renderSimpleTable({
        columns: [
          { label: "Symbol", key: "symbol" },
          { label: "Rank", render: (row) => escapeHtml(String(row.rank ?? row.pre_suppression_rank_in_bucket ?? "")) },
          { label: "Raw score", render: (row) => escapeHtml(String(row.score ?? row.ranking_score ?? row.pre_suppression_score_total ?? "")) },
          { label: "Code", render: (row) => escapeHtml(row.suppression_reason_code || row.suppression_code || "") },
        ],
        rows: rows.slice(0, 20),
        emptyMessage: "No raw score details are available.",
      })}</details>` : "",
    ].join(""),
  });
}

function renderManualCaptureRecordForm(manual = {}, { blocked = false, blocker = "", stale = false } = {}) {
  if (manual.candidate_available !== true && !manual.selected_exposure_intent_id && !manual.ticket_id) {
    return `<div class="empty-state">Manual Capture Record is unavailable until a selected exposure exists.</div>`;
  }
  const lineage = manual.trade_ticket_lineage_v1 && typeof manual.trade_ticket_lineage_v1 === "object" ? manual.trade_ticket_lineage_v1 : {};
  const submitBoundary = manual.submit_boundary_precheck_v1 && typeof manual.submit_boundary_precheck_v1 === "object" ? manual.submit_boundary_precheck_v1 : {};
  const selectedId = manual.selected_exposure_intent_id || lineage.selected_exposure_intent_id || "";
  const ticketId = manual.ticket_id || lineage.ticket_id || "";
  const modalId = `manual-capture-modal-${ticketId.replace(/[^a-zA-Z0-9_-]/g, "_") || "ticket"}`;
  const symbol = manual.symbol || lineage.symbol || "";
  const side = manual.side || manual.direction || lineage.side || "";
  const sleeve = manual.sleeve_id || manual.engine_id || manual.sleeve || lineage.sleeve_id || "";
  const runtimeHash = manual.runtime_evaluation_hash || lineage.runtime_evaluation_hash || submitBoundary.runtime_evaluation_hash || "";
  const lineageHash = manual.ticket_lineage_hash || manual.lineage_hash || lineage.lineage_hash || lineage.evidence_hash || "";
  const lineagePath = manual.trade_ticket_lineage_path || lineage.artifact_path || lineage.path || "";
  const submitBoundaryHash = manual.submit_boundary_hash || submitBoundary.submit_boundary_hash || submitBoundary.evidence_hash || "";
  const submitBoundaryPath = manual.submit_boundary_path || submitBoundary.artifact_path || submitBoundary.path || "";
  const contractHash = manual.construction_contract_hash || lineage.construction_contract_hash || "";
  const contractPath = manual.construction_contract_path || lineage.construction_contract_path || "";
  const lineageStatus = manual.lineage_status || lineage.lineage_status || "MISSING_SUBMIT_BOUNDARY";
  const submitBoundaryStatus = manual.submit_boundary_status || submitBoundary.validation_status || lineage.evidence_status?.submit_boundary || "MISSING";
  const policyReason = manual.editing_policy_reason || "";
  const record = manual.manual_capture_record && typeof manual.manual_capture_record === "object" ? manual.manual_capture_record : {};
  const alreadyCaptured = manual.captured_read_only === true
    || ["CAPTURED_HISTORICAL", "CAPTURED_MANUALLY"].includes(String(manual.lifecycle_state || manual.current_state || manual.historical_state || "").toUpperCase())
    || ["captured_manually", "partial"].includes(String(record.capture_status || manual.capture_status || "").toLowerCase());
  const captureReady = manual.manual_capture_ready === true && manual.editable === true && lineageStatus === "ACTIVE_CURRENT" && submitBoundaryStatus === "VALIDATED" && Boolean(ticketId && lineageHash && runtimeHash && submitBoundaryHash) && !alreadyCaptured && !blocked && !stale;
  const staleMessage = "This ticket changed since the page loaded. Refresh to load the current governed ticket.";
  const disabledReason = alreadyCaptured
    ? "This ticket already has an immutable manual capture record and is read-only."
    : (blocked ? (blocker || "Governance blocker is present.") : (stale ? staleMessage : (policyReason || `lineage=${lineageStatus}; submit_boundary=${submitBoundaryStatus}`)));
  const entryReference = manual.entry_reference_price || manual.entry || manual.paper_trade_construction?.entry_reference_price || "";
  const plannedQuantity = manual.quantity ?? manual.suggested_quantity ?? manual.paper_trade_construction?.suggested_quantity ?? "";
  const stopPrice = manual.stop_price || manual.stop || manual.paper_trade_construction?.stop_price || "";
  const risk = manual.risk || manual.max_loss_estimate || manual.paper_trade_construction?.max_loss_estimate || "";
  const notional = manual.notional || manual.suggested_notional || manual.paper_trade_construction?.suggested_notional || "";
  const draft = readManualCaptureDraft(ticketId);
  let operatorDefault = "David";
  try {
    operatorDefault = localStorage.getItem("aegis.operator_id") || operatorDefault;
  } catch (_error) {
    operatorDefault = "David";
  }
  const captureStatusValue = draft.capture_status || "captured";
  const fillPriceValue = draft.fill_price ?? String(entryReference || "");
  const fillTimeValue = draft.fill_time ?? localDateTimeInputValue();
  const quantityValue = draft.quantity ?? String(plannedQuantity || "");
  const stopPriceValue = draft.stop_price ?? String(stopPrice || "");
  const operatorValue = draft.operator_id ?? operatorDefault;
  const notesValue = draft.notes ?? "";
  const invalidationValue = draft.invalidation_level ?? String(manual.invalidation_level || "");
  const externalReferenceValue = draft.external_reference ?? "";
  const badge = (label, tone = "ready") => `<span class="manual-capture-badge" data-tone="${escapeHtml(tone)}">${escapeHtml(label)}</span>`;
  const statusLabel = captureReady ? "CAPTURE_READY" : (alreadyCaptured ? "CAPTURE_RECORDED" : "READ_ONLY");
  if (alreadyCaptured) {
    const records = Array.isArray(manual.manual_capture_records) ? manual.manual_capture_records : [];
    const captureRecord = Object.keys(record).length ? record : (records.length ? records[records.length - 1] : {});
    const capturedQuantity = captureRecord.quantity ?? manual.quantity ?? plannedQuantity;
    const capturedPrice = captureRecord.fill_price || manual.fill_price || entryReference;
    const capturedAt = captureRecord.fill_time || manual.captured_at_utc || manual.fill_time || "";
    const recordId = captureRecord.record_id || captureRecord.manual_capture_record_id || manual.capture_record_id || "";
    const eventIds = Array.isArray(captureRecord.event_ids) ? captureRecord.event_ids : (Array.isArray(manual.event_ids) ? manual.event_ids : []);
    return `
      <article class="completed-capture-card manual-capture-history-card" aria-label="Completed capture">
        <div class="completed-capture-mark manual-capture-history-mark">✓</div>
        <div class="completed-capture-body manual-capture-history-body">
          <div class="completed-capture-kicker">Completed Captures</div>
          <h3>Capture recorded successfully</h3>
          <div class="completed-capture-ticket"><strong>${escapeHtml(symbol || "Ticket")} ${escapeHtml(side || "")}</strong><span>${escapeHtml(sleeve || "")}</span></div>
          <div class="completed-capture-fill"><strong>${escapeHtml(String(capturedQuantity || "MISSING"))} @ ${escapeHtml(String(capturedPrice || "MISSING"))}</strong></div>
          <div class="completed-capture-meta">Captured: ${escapeHtml(String(capturedAt || "MISSING"))}</div>
          <div class="completed-capture-meta">Record ID: ${escapeHtml(String(recordId || "MISSING"))}</div>
          <p class="completed-capture-note">No further action is required. This is a read-only historical record. No broker order was placed or transmitted.</p>
          <div class="completed-capture-actions manual-capture-history-actions">
            <button class="ghost-button" type="button" data-copy-text="${escapeHtml(String(recordId || ""))}">Export capture</button>
            <details class="raw-drawer completed-capture-evidence manual-capture-history-evidence"><summary>View evidence</summary>
              <div class="manual-capture-history-meta">Events: ${escapeHtml(eventIds.join(", ") || "MISSING")}</div>
              <div class="manual-capture-history-meta">Original lineage: ${escapeHtml(shortHash(manual.original_lineage_hash || manual.lineage_hash || lineageHash))}</div>
              <div class="manual-capture-history-meta">Original RuntimeEvaluation: ${escapeHtml(shortHash(manual.original_runtime_evaluation_hash || runtimeHash))}</div>
            </details>
          </div>
        </div>
      </article>`;
  }
  const ticketSummary = `
    <div class="manual-capture-ticket-card">
      <div><span>Entry reference</span><strong>${escapeHtml(String(entryReference || "MISSING"))}</strong></div>
      <div><span>Quantity</span><strong>${escapeHtml(String(plannedQuantity || "MISSING"))}</strong></div>
      <div><span>Stop</span><strong>${escapeHtml(String(stopPrice || "MISSING"))}</strong></div>
      <div><span>Risk</span><strong>${escapeHtml(String(risk || "MISSING"))}</strong></div>
      <div><span>Notional</span><strong>${escapeHtml(String(notional || "MISSING"))}</strong></div>
      <div><span>Runtime</span><strong>${escapeHtml(shortHash(runtimeHash))}</strong></div>
    </div>`;
  const compactCard = `
    <div class="manual-capture-launch-card" data-tone="${captureReady ? "ready" : "blocked"}">
      <div class="manual-capture-launch-main">
        <div class="manual-capture-operator-title"><strong>${escapeHtml(symbol || "Ticket")} ${escapeHtml(side || "")}</strong><span>${escapeHtml(sleeve || "")}</span></div>
        <div class="manual-capture-badges">
          ${badge(statusLabel, captureReady ? "ready" : "blocked")}
          ${badge(submitBoundaryStatus === "VALIDATED" ? "Submit-boundary VALIDATED" : `Submit-boundary ${submitBoundaryStatus}`, submitBoundaryStatus === "VALIDATED" ? "ready" : "blocked")}
          ${badge("Manual capture only", "safe")}
          ${badge("Broker submit disabled", "safe")}
        </div>
      </div>
      <div class="manual-capture-operator-facts">
        <div><span>Entry</span><strong>${escapeHtml(String(entryReference || "MISSING"))}</strong></div>
        <div><span>Qty</span><strong>${escapeHtml(String(plannedQuantity || "MISSING"))}</strong></div>
        <div><span>Stop</span><strong>${escapeHtml(String(stopPrice || "MISSING"))}</strong></div>
        <div><span>Risk</span><strong>${escapeHtml(String(risk || "MISSING"))}</strong></div>
        <div><span>Runtime</span><strong>${escapeHtml(shortHash(runtimeHash))}</strong></div>
      </div>
      <div class="manual-capture-primary-row">
        ${captureReady ? `<button class="primary-button manual-capture-primary-cta" type="button" data-aegis-command-id="OPEN_IN_PAGE_MODAL" data-aegis-command-action-type="IN_PAGE_DETAIL" data-aegis-command-target-type="manual_capture_ticket" data-aegis-command-target-id="${escapeHtml(modalId)}" data-command-detail-target="${escapeHtml(modalId)}" data-manual-capture-open="${escapeHtml(modalId)}">Mark IB capture complete</button>` : `<div class="manual-capture-readonly-reason"><strong>Read-only</strong><span>${escapeHtml(disabledReason)}</span></div>`}
      </div>
    </div>`;
  if (!captureReady) {
    return compactCard;
  }
  return `
    ${compactCard}
    <dialog id="${escapeHtml(modalId)}" class="manual-capture-dialog" data-manual-capture-dialog>
      <form class="manual-capture-record-form manual-capture-modal-shell" method="post" data-ticket-id="${escapeHtml(ticketId)}" data-manual-capture-draft-key="${escapeHtml(manualCaptureDraftKey(ticketId))}">
        <input type="hidden" name="ticket_id" value="${escapeHtml(ticketId)}">
        <input type="hidden" name="selected_exposure_intent_id" value="${escapeHtml(selectedId)}">
        <input type="hidden" name="trade_lifecycle_case_id" value="${escapeHtml(manual.trade_lifecycle_case_id || manual.case_id || "")}">
        <input type="hidden" name="runtime_evaluation_hash" value="${escapeHtml(runtimeHash)}">
        <input type="hidden" name="ticket_lineage_hash" value="${escapeHtml(lineageHash)}">
        <input type="hidden" name="submit_boundary_hash" value="${escapeHtml(submitBoundaryHash)}">
        <input type="hidden" name="construction_contract_hash" value="${escapeHtml(contractHash)}">
        <input type="hidden" name="candidate_snapshot_id" value="${escapeHtml(manual.candidate_snapshot_id || manual.candidate_snapshot?.snapshot_id || manual.trade_candidate_projection_id || manual.trade_candidate_id || selectedId)}">
        <input type="hidden" name="candidate_certification_state" value="${escapeHtml(manual.candidate_certification_state || manual.certification_state || "CANDIDATES_CERTIFIED")}">
        <input type="hidden" name="input_market_data_snapshot_ids" value="${escapeHtml(safeList(manual.input_market_data_snapshot_ids || manual.candidate_snapshot?.input_market_data_snapshot_ids).join(","))}">
        <input type="hidden" name="symbol" value="${escapeHtml(symbol)}">
        <input type="hidden" name="sleeve" value="${escapeHtml(sleeve)}">
        <header class="manual-capture-modal-header">
          <div>
            <h3>Mark IB capture complete</h3>
            <p>${escapeHtml(symbol || "Ticket")} ${escapeHtml(side || "")} · ${escapeHtml(sleeve || "")}</p>
          </div>
          <div class="manual-capture-badges">
            ${badge("CAPTURE_READY", "ready")}
            ${badge("Manual capture only", "safe")}
            ${badge("Broker submit disabled", "safe")}
          </div>
        </header>
        <div class="manual-capture-modal-body">
          <div class="manual-capture-validation-summary" data-manual-capture-validation-summary>Recording this documents your manual IB capture. Aegis will not place, route, submit, or transmit an order.</div>
          ${ticketSummary}
          <div data-manual-capture-editable>
            <input type="hidden" name="capture_status" data-manual-capture-draft-field="capture_status" value="${escapeHtml(captureStatusValue === "partial" ? "partial" : "captured")}">
            <input type="hidden" name="stop_price" data-manual-capture-draft-field="stop_price" value="${escapeHtml(String(stopPriceValue || ""))}">
            <input type="hidden" name="operator_id" data-manual-capture-draft-field="operator_id" value="${escapeHtml(String(operatorValue || ""))}">
            <input name="invalidation_level" data-manual-capture-draft-field="invalidation_level" value="${escapeHtml(String(invalidationValue || ""))}" hidden>
            <input name="external_reference" data-manual-capture-draft-field="external_reference" value="${escapeHtml(String(externalReferenceValue || ""))}" hidden>
            <div class="manual-capture-field-grid two-column compact-transaction-fields">
              <label class="edge-form-field"><span>Fill time</span><input name="fill_time" data-manual-capture-draft-field="fill_time" type="datetime-local" value="${escapeHtml(String(fillTimeValue || ""))}" required><small data-field-error-for="fill_time">Saved as UTC.</small></label>
              <label class="edge-form-field"><span>Fill price</span><input name="fill_price" data-manual-capture-draft-field="fill_price" inputmode="decimal" placeholder="0.00" value="${escapeHtml(String(fillPriceValue || ""))}" required><small data-field-error-for="fill_price"></small></label>
              <label class="edge-form-field"><span>Quantity</span><input name="quantity" data-manual-capture-draft-field="quantity" type="number" min="1" step="1" value="${escapeHtml(String(quantityValue || ""))}" required><small data-field-error-for="quantity"></small></label>
              <label class="edge-form-field"><span>Notes</span><textarea name="notes" data-manual-capture-draft-field="notes" placeholder="Optional note. No broker action is implied.">${escapeHtml(String(notesValue || ""))}</textarea><small data-field-error-for="notes"></small></label>
            </div>
          </div>
          <div class="manual-capture-success" data-manual-capture-success hidden></div>
          <div class="edge-action-status" data-manual-capture-record-status></div>
        </div>
        <footer class="manual-capture-modal-footer">
          <button class="ghost-button" type="button" data-manual-capture-close>Cancel</button>
          <button class="primary-button" type="submit" data-manual-capture-save-button>Mark capture complete</button>
        </footer>
      </form>
    </dialog>
  `;
}

function renderOperatorReadinessProjectionPanel(payload = {}) {
  const snapshot = payload.data && typeof payload.data === "object" ? payload.data : payload;
  const readiness = snapshot.readiness || {};
  const runtime = snapshot.runtime_status || {};
  const market = snapshot.market_data_freshness || {};
  const blockers = safeList(snapshot.blockers);
  const errors = safeList(payload.errors || snapshot.errors);
  return renderWorkflowCard({
    payload,
    sourceKey: "operator_state_snapshot_v1",
    fieldPath: "readiness",
    whyShown: "Operator readiness is projected from the stable snapshot so missing raw artifacts degrade one panel, not the shell.",
    eyebrow: readiness.status || runtime.status || "READINESS",
    title: "Operator Readiness",
    subtitle: errors.length ? "Some source artifacts are missing or malformed; the panel is degraded but usable." : "Read-only readiness projection.",
    body: `
      ${renderDefinitionRows([
        { label: "Runtime", value: runtime.status || "UNKNOWN" },
        { label: "Governance", value: runtime.governance_mode || "Governed" },
        { label: "Environment", value: runtime.environment || "Production" },
        { label: "Readiness", value: readiness.status || "UNKNOWN" },
        { label: "Blockers", value: String(readiness.blocker_count ?? blockers.length) },
        { label: "Data warnings", value: String(readiness.stale_market_data_count ?? (market.stale_market_data ? 1 : 0)) },
        { label: "Next action", value: safeList(snapshot.next_actions)[0] || payload.next_action || "Review canonical operator state." },
      ])}
      ${blockers.length ? renderSimpleTable({ columns: [
        { label: "Code", key: "code" },
        { label: "Symbol", key: "symbol" },
        { label: "Message", key: "message" },
      ], rows: blockers, emptyMessage: "No blockers." }) : ""}
      ${errors.length ? renderSimpleTable({ columns: [
        { label: "Error", key: "code" },
        { label: "Message", key: "message" },
        { label: "Source", key: "source_path" },
      ], rows: errors, emptyMessage: "No source errors." }) : ""}
    `,
  });
}

const DASHBOARD_DEGRADED_READ_ONLY_STATES = new Set(["PENDING_VENDOR_DATA", "READ_ONLY_PRIOR_DAY_FALLBACK", "PARTIAL_DATA_AVAILABLE"]);

function dashboardCurrentDayPayload(payload = {}) {
  const snapshot = payload.operator_state_snapshot || {};
  const currentTruth = payload.current_operator_truth || snapshot.current_operator_truth || {};
  const currentDay = payload.current_day_status || snapshot.current_day_status || currentTruth.current_day_status || {};
  const today = snapshot.operator_today_projection || payload.operator_today_projection || payload.today_projection || {};
  const historical = payload.historical_fallback || snapshot.historical_fallback || currentTruth.historical_fallback || {};
  return { snapshot, currentTruth, currentDay, today, historical };
}

function dashboardOperationalTimestamps(payload = {}) {
  const { currentTruth, currentDay, today, snapshot } = dashboardCurrentDayPayload(payload);
  const timing = currentDay.eod_certification_timing || today.eod_certification_timing || {};
  const retryJob = dashboardRetryJob(currentDay);
  return {
    operational_day: currentTruth.requested_day || today.current_runtime_day || payload.requested_day || payload.day_utc || snapshot.requested_day || snapshot.day_utc || "",
    market_data_last_updated_at: currentDay.market_data_last_updated_at || today.market_data_last_updated_at || currentDay.last_market_data_attempt?.attempted_at_utc || currentDay.last_attempt_time || "",
    candidate_snapshot_timestamp: currentDay.candidate_snapshot_generated_at || today.candidate_snapshot_generated_at || payload.generated_at_utc || payload.generated_at || "",
    last_certification_attempt_at: currentDay.last_certification_attempt_at || today.last_certification_attempt_at || retryJob.last_attempt?.attempted_at_utc || "",
    final_eod_certification_completed_at: currentDay.final_eod_certification_completed_at || today.final_eod_certification_completed_at || "",
    eod_pipeline_timing: {
      market_close_at: timing.market_close_at || currentDay.market_close_at || "",
      vendor_lag_window: timing.vendor_lag_window || currentDay.vendor_lag_window || "",
      certification_pending: timing.certification_pending === true || currentDay.final_eod_certification_pending === true || today.final_eod_certification_pending === true,
      estimated_next_certification_attempt_at: timing.estimated_next_certification_attempt_at || currentDay.estimated_next_certification_attempt_at || currentDay.next_retry_utc || retryJob.next_retry_utc || "",
      final_certification_status: timing.final_certification_status || currentDay.final_eod_certification_status || today.final_eod_certification_status || "",
    },
  };
}

function withOperationalTimestamps(view = {}, payload = {}) {
  return { ...view, operationalTimestamps: dashboardOperationalTimestamps(payload) };
}

function renderDashboardOperationalTimestamps(payload = {}) {
  const timestamps = dashboardOperationalTimestamps(payload);
  return renderWorkflowCard({
    payload,
    sourceKey: "operator_state_snapshot_v1",
    fieldPath: "operator_today_projection.operational_timestamps",
    whyShown: "Top-level operator timestamps come from the live operator-state projection, not shell build metadata.",
    eyebrow: "RUNTIME_TIMELINE",
    title: "Runtime Timeline",
    subtitle: "Active operational day, market data refresh, candidate snapshot, and certification attempts.",
    body: renderDefinitionRows([
      { label: "Operational day", value: timestamps.operational_day || "not reported" },
      { label: "Market data last updated", value: formatTimestamp(timestamps.market_data_last_updated_at) },
      { label: "Candidate snapshot timestamp", value: formatTimestamp(timestamps.candidate_snapshot_timestamp) },
      { label: "Last certification attempt", value: formatTimestamp(timestamps.last_certification_attempt_at) },
      { label: "Final EOD certification completed at", value: formatTimestamp(timestamps.final_eod_certification_completed_at) },
    ]),
  });
}

function renderDashboardEodPipelineTiming(payload = {}) {
  const timestamps = dashboardOperationalTimestamps(payload);
  const timing = timestamps.eod_pipeline_timing || {};
  return renderWorkflowCard({
    payload,
    sourceKey: "operator_state_snapshot_v1",
    fieldPath: "operator_today_projection.eod_certification_timing",
    whyShown: "EOD timing explains why certification may still be pending after market close without treating vendor lag as an execution-ready state.",
    eyebrow: "CERTIFICATION_PROGRESS",
    title: "Certification Progress",
    subtitle: "Market close, vendor lag, next certification attempt, and final status.",
    body: renderDefinitionRows([
      { label: "Market close", value: formatTimestamp(timing.market_close_at) },
      { label: "Vendor lag window", value: timing.vendor_lag_window || "not reported" },
      { label: "Certification pending", value: String(timing.certification_pending === true) },
      { label: "Estimated next certification attempt", value: formatTimestamp(timing.estimated_next_certification_attempt_at) },
      { label: "Final certification status", value: timing.final_certification_status || "not reported" },
    ]),
  });
}

function dashboardDegradedReadOnlyState(payload = {}) {
  const { currentTruth, currentDay, today } = dashboardCurrentDayPayload(payload);
  const rawState = String(currentDay.freshness_state || currentDay.validation_status || currentDay.status || currentTruth.current_truth_status || today.current_day_run_status || payload.current_truth_status || "").toUpperCase();
  return DASHBOARD_DEGRADED_READ_ONLY_STATES.has(rawState) ? rawState : "";
}

function dashboardRetryJob(currentDay = {}) {
  return currentDay.retry_job && typeof currentDay.retry_job === "object" ? currentDay.retry_job : {};
}

function dashboardIncidentState(payload = {}) {
  if (dashboardDegradedReadOnlyState(payload)) return "";
  const { currentTruth, currentDay, today } = dashboardCurrentDayPayload(payload);
  const rawState = String(currentDay.status || currentTruth.current_truth_status || today.current_day_run_status || payload.current_truth_status || "").toUpperCase();
  const rawBlocker = String(currentDay.blocker || currentDay.canonical_blocker || payload.blocker || payload.blockers?.[0]?.code || "").toUpperCase();
  const finalEodStatus = String(currentDay.final_eod_certification_status || currentTruth.final_eod_certification_status || today.final_eod_certification_status || "").toUpperCase();
  const hardStates = new Set(["CURRENT_DAY_FAILED", "MARKET_DATA_FETCH_FAILED", "INTRADAY_OPERATIONAL_FAILED", "FINAL_EOD_BLOCKED"]);
  if (hardStates.has(rawState)) return rawState;
  if (hardStates.has(rawBlocker)) return rawBlocker;
  if (finalEodStatus === "FINAL_EOD_BLOCKED") return "FINAL_EOD_BLOCKED";
  return "";
}

function dashboardIncidentDateLabel(dayValue = "") {
  const raw = String(dayValue || "").trim();
  const match = raw.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (!match) return raw || "today";
  const monthNames = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  const monthIndex = Math.max(0, Math.min(11, Number(match[2]) - 1));
  return `${monthNames[monthIndex]} ${Number(match[3])}`;
}

function dashboardIncidentField(payload = {}, fieldName = "") {
  const snapshot = payload.operator_state_snapshot || {};
  const currentTruth = payload.current_operator_truth || snapshot.current_operator_truth || {};
  const currentDay = payload.current_day_status || snapshot.current_day_status || currentTruth.current_day_status || {};
  const today = snapshot.operator_today_projection || payload.operator_today_projection || payload.today_projection || {};
  const historical = payload.historical_fallback || snapshot.historical_fallback || currentTruth.historical_fallback || {};
  if (fieldName === "runtimeDay") return currentTruth.requested_day || today.current_runtime_day || payload.day_utc || snapshot.requested_day || snapshot.day_utc || "";
  if (fieldName === "fallbackDay") return historical.source_day || currentTruth.displayed_artifact_day || payload.displayed_artifact_day || snapshot.displayed_artifact_day || "";
  if (fieldName === "fallbackPath") return historical.artifact_path || historical.path || "";
  if (fieldName === "runtimeMode") return currentTruth.runtime_mode || today.runtime_mode || today.operational_mode || currentDay.runtime_mode || currentDay.market_data_mode || "";
  if (fieldName === "finalEodStatus") return currentDay.final_eod_certification_status || currentTruth.final_eod_certification_status || today.final_eod_certification_status || "";
  if (fieldName === "lastAttempt") return currentDay.last_attempt_time || currentTruth.generated_at_utc || payload.generated_at_utc || "";
  if (fieldName === "blocker") return currentDay.blocker || currentDay.canonical_blocker || payload.blocker || payload.blockers?.[0]?.code || dashboardIncidentState(payload) || "";
  return "";
}

function renderDashboardIncidentPage(payload = {}) {
  const snapshot = payload.operator_state_snapshot || {};
  const currentTruth = payload.current_operator_truth || snapshot.current_operator_truth || {};
  const currentDay = payload.current_day_status || snapshot.current_day_status || currentTruth.current_day_status || {};
  const runtimeDay = dashboardIncidentField(payload, "runtimeDay");
  const runtimeDayLabel = dashboardIncidentDateLabel(runtimeDay);
  const fallbackDay = dashboardIncidentField(payload, "fallbackDay");
  const fallbackDayLabel = dashboardIncidentDateLabel(fallbackDay);
  const fallbackPath = dashboardIncidentField(payload, "fallbackPath");
  const missingSymbols = safeList(currentDay.critical_missing_symbols).length ? safeList(currentDay.critical_missing_symbols) : safeList(currentDay.missing_symbols);
  const retrySymbols = missingSymbols.join(",");
  const rawBlocker = dashboardIncidentField(payload, "blocker") || dashboardIncidentState(payload) || "UNKNOWN";
  const retryButton = `<form class="dashboard-incident-retry research-data-acquisition-form research-data-acquisition-plan" data-day="${escapeHtml(runtimeDay)}">
      <input type="hidden" name="data_action" value="fetch-market-data-now">
      <input type="hidden" name="day_utc" value="${escapeHtml(runtimeDay)}">
      <input type="hidden" name="playbook_id" value="${escapeHtml(currentDay.retry_action_playbook_id || "refresh_required_symbol_data")}">
      <input type="hidden" name="symbols" value="${escapeHtml(retrySymbols)}">
      <button class="primary-button dashboard-incident-primary-action" type="submit" data-command="retry-market-data-refresh" data-day="${escapeHtml(runtimeDay)}">Retry market data refresh</button>
      <span class="edge-action-status" data-research-data-acquisition-status></span>
    </form>`;
  return `<section class="dashboard-incident-page" data-dashboard-incident-page aria-labelledby="dashboardIncidentTitle">
    <div class="dashboard-incident-shell">
      <div class="dashboard-incident-copy">
        <p class="dashboard-incident-kicker">Current-day incident</p>
        <h2 id="dashboardIncidentTitle">Today’s run failed</h2>
        <div class="dashboard-incident-summary" data-dashboard-incident-primary-copy>
          <div>
            <span>Cause</span>
            <strong>Market data refresh failed for ${escapeHtml(runtimeDayLabel)}.</strong>
          </div>
          <div>
            <span>Impact</span>
            <p>Current-day candidates are unavailable.<br>Do not use stale prior-day prices.</p>
          </div>
          <div class="dashboard-incident-next-step">
            <span>Next step</span>
            <strong>Retry market data refresh.</strong>
            ${retryButton}
          </div>
          <div>
            <span>Last known good</span>
            <p>${escapeHtml(fallbackDayLabel)} is available as read-only historical fallback.</p>
          </div>
        </div>
      </div>
      <details class="dashboard-incident-details" data-dashboard-incident-details>
        <summary>Advanced details</summary>
        ${renderDefinitionRows([
          { label: "Raw blocker code", value: rawBlocker },
          { label: "Missing symbols", value: missingSymbols.length ? missingSymbols.join(", ") : "none reported" },
          { label: "Last attempt", value: formatTimestamp(dashboardIncidentField(payload, "lastAttempt")) },
          { label: "Runtime mode", value: dashboardIncidentField(payload, "runtimeMode") || "unknown" },
          { label: "Final EOD status", value: dashboardIncidentField(payload, "finalEodStatus") || "unknown" },
          { label: "Stale fallback path", value: fallbackPath || "not reported" },
        ])}
      </details>
    </div>
  </section>`;
}

function renderDashboardDegradedReadOnlyBanner(payload = {}) {
  const { currentTruth, currentDay, today, historical, snapshot } = dashboardCurrentDayPayload(payload);
  const runtimeDay = currentTruth.requested_day || today.current_runtime_day || payload.day_utc || snapshot.requested_day || snapshot.day_utc || "";
  const displayedDay = currentTruth.displayed_artifact_day || payload.displayed_artifact_day || snapshot.displayed_artifact_day || historical.source_day || "";
  const missingSymbols = safeList(currentDay.critical_missing_symbols).length ? safeList(currentDay.critical_missing_symbols) : safeList(currentDay.missing_symbols);
  const state = dashboardDegradedReadOnlyState(payload) || "DEGRADED_READ_ONLY";
  const retryJob = dashboardRetryJob(currentDay);
  const lastAttempt = currentDay.last_market_data_attempt?.attempted_at_utc || currentDay.last_attempt?.attempted_at_utc || currentDay.last_attempt_time || retryJob.last_attempt?.attempted_at_utc || currentTruth.generated_at_utc || payload.generated_at_utc || "";
  const nextRetry = currentDay.next_retry_utc || retryJob.next_retry_utc || "";
  const validationState = currentDay.validation_status || currentDay.freshness_state || retryJob.validation_status || state;
  const retryActionAvailable = currentDay.retry_action_available === true || currentDay.retry_action_endpoint === "/api/aegis/data-remediation/run";
  const retryButton = retryActionAvailable
    ? `<form class="degraded-read-only-retry research-data-acquisition-form research-data-acquisition-plan" data-day="${escapeHtml(runtimeDay)}">
        <input type="hidden" name="data_action" value="fetch-market-data-now">
        <input type="hidden" name="day_utc" value="${escapeHtml(runtimeDay)}">
        <input type="hidden" name="playbook_id" value="${escapeHtml(currentDay.retry_action_playbook_id || "refresh_required_symbol_data")}">
        <input type="hidden" name="symbols" value="${escapeHtml(missingSymbols.join(","))}">
        <button class="action-button degraded-read-only-retry-button" type="submit" data-command="retry-market-data-refresh" data-day="${escapeHtml(runtimeDay)}">Queue refresh retry</button>
        <span class="edge-action-status" data-research-data-acquisition-status>${escapeHtml(retryJob.status ? `Background job: ${retryJob.status}` : "Runs asynchronously")}</span>
      </form>`
    : "";
  return `<section class="current-day-status-banner degraded-read-only" data-degraded-read-only-banner aria-label="Market data pending">
    <div class="degraded-read-only-copy">
      <p class="degraded-read-only-kicker">Operational Mode: DEGRADED_READ_ONLY</p>
      <h2>Market data pending</h2>
      <p>Using validated prior-day data in read-only mode until vendor certification completes.</p>
    </div>
    <div class="degraded-read-only-grid">
      <div><span>Runtime day</span><strong>${escapeHtml(runtimeDay || "unknown")}</strong></div>
      <div><span>Read-only data day</span><strong>${escapeHtml(displayedDay || "not reported")}</strong></div>
      <div><span>Validation state</span><strong>${escapeHtml(validationState || state)}</strong></div>
      <div><span>Last attempt</span><strong>${escapeHtml(formatTimestamp(lastAttempt))}</strong></div>
      <div><span>Next retry</span><strong>${escapeHtml(formatTimestamp(nextRetry))}</strong></div>
      <div><span>Background job</span><strong>${escapeHtml([retryJob.job_id, retryJob.status].filter(Boolean).join(" / ") || "not queued")}</strong></div>
    </div>
    <div class="candidate-generation-readiness" data-candidate-generation-gated>
      <strong>Current-day candidate generation unlocks after validated EOD data.</strong>
      <button class="action-button" type="button" disabled aria-disabled="true">Generate current-day candidates</button>
    </div>
    ${retryButton}
  </section>`;
}

function renderCurrentDayStatusBanner(payload = {}) {
  if (dashboardDegradedReadOnlyState(payload)) return renderDashboardDegradedReadOnlyBanner(payload);
  const snapshot = payload.operator_state_snapshot || {};
  const currentTruth = payload.current_operator_truth || snapshot.current_operator_truth || {};
  const currentDay = payload.current_day_status || snapshot.current_day_status || currentTruth.current_day_status || {};
  const historical = payload.historical_fallback || snapshot.historical_fallback || currentTruth.historical_fallback || {};
  const today = snapshot.operator_today_projection || payload.operator_today_projection || {};
  const runtimeDay = currentTruth.requested_day || today.current_runtime_day || payload.day_utc || snapshot.requested_day || snapshot.day_utc || "";
  const displayedDay = currentTruth.displayed_artifact_day || payload.displayed_artifact_day || snapshot.displayed_artifact_day || historical.source_day || runtimeDay;
  const state = String(currentDay.status || currentTruth.current_truth_status || today.current_day_run_status || "").toUpperCase();
  const missingSymbols = safeList(currentDay.critical_missing_symbols).length ? safeList(currentDay.critical_missing_symbols) : safeList(currentDay.missing_symbols);
  const failedStep = currentDay.failed_step || "current-day run";
  const runtimeMode = currentTruth.runtime_mode || today.runtime_mode || today.operational_mode || currentDay.runtime_mode || currentDay.market_data_mode || "";
  const marketDataMode = runtimeMode || currentDay.market_data_mode || currentTruth.market_data_mode || today.market_data_mode || "";
  const finalEodStatus = currentDay.final_eod_certification_status || currentTruth.final_eod_certification_status || today.final_eod_certification_status || "";
  const finalEodPending = currentDay.final_eod_certification_pending === true || today.final_eod_certification_pending === true || String(finalEodStatus).toUpperCase() === "PENDING";
  const nextAction = currentDay.next_action || "Review current-day run artifacts.";
  const lastAttempt = currentDay.last_attempt_time || currentTruth.generated_at_utc || payload.generated_at_utc || "";
  const schedulerFailure = currentDay.scheduler_failure || {};
  const schedulerLine = schedulerFailure.status === "FAILED"
    ? `<span>Scheduler: ${escapeHtml(schedulerFailure.blocker || "failed")} - ${escapeHtml(schedulerFailure.next_action || "Review scheduler artifact.")}</span>`
    : "";
  let title = "Today’s run is current.";
  let tone = "healthy";
  if (state === "INTRADAY_OPERATIONAL_READY") {
    title = "Today’s intraday sleeve run is current. Final EOD certification is pending.";
    tone = "healthy";
  } else if (state === "MARKET_NOT_FINALIZED_YET") {
    title = currentDay.message || "Market data is not finalized yet. Aegis will retry at 16:30, 17:00, and 18:00.";
    tone = "warning";
  } else if (state === "PROVIDER_TIMEOUT") {
    title = currentDay.message || "Provider timeout. Some symbols were not fetched.";
    tone = "danger";
  } else if (state === "PROVIDER_SOURCE_UNAVAILABLE") {
    title = currentDay.message || "Provider source unavailable. Current-day final data is not available.";
    tone = "danger";
  } else if (state === "CURRENT_DAY_DATA_STALE") {
    title = currentDay.message || "Current-day market data is stale.";
    tone = "danger";
  } else if (state === "PARTIAL_PROVIDER_SUCCESS") {
    title = currentDay.message || `Partial market data available. Missing: ${missingSymbols.join(", ") || "none reported"}.`;
    tone = "warning";
  } else if (state === "FINAL_EOD_READY") {
    title = currentDay.message || "Final EOD market data is ready.";
    tone = "healthy";
  } else if (["CURRENT_DAY_FAILED"].includes(state)) {
    title = currentDay.message || "Today’s run failed: market data unavailable.";
    tone = "danger";
  } else if (["CURRENT_DAY_BLOCKED", "HISTORICAL_ONLY"].includes(state)) {
    title = currentDay.message || "Today’s candidates are blocked.";
    tone = "warning";
  } else if (displayedDay && runtimeDay && displayedDay !== runtimeDay) {
    title = "Showing latest completed historical result from " + displayedDay + ".";
    tone = "warning";
  }
  const staleLine = displayedDay && runtimeDay && displayedDay !== runtimeDay
    ? `<div class="callout warning"><strong>READ_ONLY_PRIOR_DAY_FALLBACK</strong> Showing latest completed historical result from ${escapeHtml(displayedDay)}. Current-day candidate generation waits for validation.</div>`
    : "";
  const retryActionAvailable = currentDay.retry_action_available === true || currentDay.retry_action_endpoint === "/api/aegis/data-remediation/run";
  const retryButton = retryActionAvailable && ["CURRENT_DAY_FAILED", "CURRENT_DAY_BLOCKED", "MARKET_NOT_FINALIZED_YET", "PROVIDER_TIMEOUT", "PROVIDER_SOURCE_UNAVAILABLE", "CURRENT_DAY_DATA_STALE", "PARTIAL_PROVIDER_SUCCESS", "PENDING_VENDOR_DATA", "PARTIAL_DATA_AVAILABLE", "READ_ONLY_PRIOR_DAY_FALLBACK"].includes(state) && String(failedStep).includes("market_data")
    ? `<form class="research-data-acquisition-form research-data-acquisition-plan" data-day="${escapeHtml(runtimeDay)}">
        <input type="hidden" name="data_action" value="fetch-market-data-now">
        <input type="hidden" name="day_utc" value="${escapeHtml(runtimeDay)}">
        <input type="hidden" name="playbook_id" value="${escapeHtml(currentDay.retry_action_playbook_id || "refresh_required_symbol_data")}">
        <input type="hidden" name="symbols" value="${escapeHtml(missingSymbols.join(","))}">
        <button class="action-button" type="submit" data-command="retry-market-data-refresh" data-day="${escapeHtml(runtimeDay)}">Retry market data refresh</button>
        <span class="edge-action-status" data-research-data-acquisition-status></span>
      </form>`
    : "";
  return `
    <section class="current-day-status-banner ${escapeHtml(tone)}" aria-label="Current Day Run Status">
      <div class="current-day-status-banner-main">
        <strong>${escapeHtml(title)}</strong>
        <span>${escapeHtml(runtimeDay ? `Affected day: ${runtimeDay}` : "Affected day: unknown")}</span>
      </div>
      <div class="current-day-status-banner-details">
        <span>Mode: ${escapeHtml(marketDataMode || "unknown")}</span>
        <span>Final EOD: ${escapeHtml(finalEodPending ? "PENDING" : (finalEodStatus || "unknown"))}</span>
        <span>${escapeHtml(finalEodPending ? "Final EOD certification pending." : "Final EOD certification current.")}</span>
        <span>Failed step: ${escapeHtml(failedStep)}</span>
        <span>Last attempt: ${escapeHtml(formatTimestamp(lastAttempt))}</span>
        <span>Missing symbols: ${escapeHtml(missingSymbols.length ? missingSymbols.join(", ") : "none reported")}</span>
        <span>Next action: ${escapeHtml(nextAction)}</span>
        ${schedulerLine}
      </div>
      ${retryButton}
      ${staleLine}
    </section>
  `;
}

function renderAegisTodayWorkflow(payload) {
  const today = payload.operator_today_projection || payload.today_projection || {};
  const currentDay = payload.current_day_status || {};
  const incidentState = dashboardIncidentState(payload);
  if (incidentState) {
    return {
      title: "Dashboard",
      meta: "Current-day incident. Normal dashboard content is hidden until the hard failure clears.",
      html: renderDashboardIncidentPage(payload),
      contextHtml: "",
      dashboardIncidentMode: true,
      dashboardIncidentState: incidentState,
    };
  }
  const currentTruth = payload.current_operator_truth || {};
  const runtimeDay = today.current_runtime_day || payload.requested_day || payload.day_utc || "unknown";
  const mode = today.runtime_mode || today.operational_mode || payload.runtime_mode || "UNKNOWN";
  const intradayStatus = today.current_day_run_status || currentDay.status || payload.current_truth_status || "UNKNOWN";
  const marketDataState = today.market_data_state || currentDay.market_data_state || payload.market_data_state || currentTruth.market_data_state || "MARKET_DATA_PENDING";
  const candidateCertificationState = today.candidate_certification_state || currentDay.candidate_certification_state || payload.candidate_certification_state || currentTruth.candidate_certification_state || "CANDIDATES_PROVISIONAL";
  const executionEligibilityState = today.execution_eligibility_state || currentDay.execution_eligibility_state || payload.execution_eligibility_state || currentTruth.execution_eligibility_state || "EXECUTION_LOCKED_NON_CERTIFIED";
  const finalStatus = today.final_eod_certification_pending === true ? "PENDING" : (today.final_eod_certification_status || currentDay.final_eod_certification_status || "PENDING");
  const cards = [
    renderCurrentDayStatusBanner(payload),
    renderDashboardSystemStatus(payload, { runtimeDay, mode, intradayStatus, marketDataState, candidateCertificationState, executionEligibilityState, finalStatus }),
    renderDashboardOperationalTimestamps(payload),
    renderDashboardEodPipelineTiming(payload),
    renderDashboardThesisSignals(payload),
    renderDashboardTodaySummary(payload),
    renderDashboardCandidateFunnel(payload),
    renderDashboardSleeveHealth(payload),
    renderDashboardRegimeActivity(payload),
    renderDashboardCaptureReadyTrend(payload),
    renderDashboardAttentionRequired(payload),
    renderDashboardRecentEvents(payload),
  ];
  return {
    title: "Dashboard",
    meta: "System health, today's run, critical blockers, and operator attention.",
    html: cards.filter(Boolean).join(""),
    contextHtml: "",
    hideContextRail: true,
  };
}

function dashboardCaptureProjection(payload = {}) {
  const today = payload.operator_today_projection || {};
  const runtime = payload.runtime_truth_kernel || payload.readiness_kernel || payload.runtime_truth || {};
  const active = payload.active_operator_candidate || payload.trade_ticket_projection_v1 || {};
  const manual = payload.trade_ticket_projection_v1 || payload.trade_ticket_projection || {};
  const completed = manual.captured_read_only === true || ["CAPTURED_HISTORICAL", "CAPTURED_MANUALLY"].includes(String(manual.lifecycle_state || manual.current_state || manual.state || manual.historical_state || "").toUpperCase());
  const fallbackTicket = manual.manual_capture_ready === true && (manual.selected_exposure_intent_id || manual.candidate_available) ? 1 : 0;
  const count = completed ? 0 : Number(today.capture_ticket_count ?? today.manual_capture_ticket_count ?? runtime.capture_ticket_count ?? active.manual_capture_ticket_count ?? fallbackTicket ?? 0);
  const capability = String(today.platform_capture_capability || runtime.platform_capture_capability || (runtime.manual_trade_capture_allowed === true ? "READY" : "READY"));
  const status = String(today.capture_ticket_status || runtime.capture_ticket_status || (count > 0 ? "TICKET_READY" : "NONE_AVAILABLE"));
  return {
    platform_capture_capability: capability,
    capture_ticket_count: Number.isFinite(count) ? count : 0,
    capture_ticket_status: status,
    next_action: count > 0 ? "Review the available IB capture ticket." : "No action required",
  };
}

function renderDashboardSystemStatus(payload = {}, { runtimeDay = "unknown", mode = "UNKNOWN", intradayStatus = "UNKNOWN", marketDataState = "MARKET_DATA_PENDING", candidateCertificationState = "CANDIDATES_PROVISIONAL", executionEligibilityState = "EXECUTION_LOCKED_NON_CERTIFIED", finalStatus = "PENDING" } = {}) {
  const today = payload.operator_today_projection || {};
  const currentDay = payload.current_day_status || {};
  const statusProjection = payload.current_operator_truth || {};
  const timestamps = dashboardOperationalTimestamps(payload);
  const timing = timestamps.eod_pipeline_timing || {};
  const retryJob = dashboardRetryJob(currentDay);
  const latestRun = payload.generated_at_utc || payload.generated_at || currentDay.last_attempt_time || "";
  const resolvedMarketDataState = marketDataState || today.market_data_state || currentDay.market_data_state || payload.market_data_state || statusProjection.market_data_state || "MARKET_DATA_PENDING";
  const resolvedCandidateCertificationState = candidateCertificationState || today.candidate_certification_state || currentDay.candidate_certification_state || payload.candidate_certification_state || statusProjection.candidate_certification_state || "CANDIDATES_PROVISIONAL";
  const resolvedExecutionEligibilityState = executionEligibilityState || today.execution_eligibility_state || currentDay.execution_eligibility_state || payload.execution_eligibility_state || statusProjection.execution_eligibility_state || "EXECUTION_LOCKED_NON_CERTIFIED";
  const fallbackDay = statusProjection.displayed_artifact_day || payload.displayed_artifact_day || statusProjection.historical_fallback?.source_day || "";
  const requestedDay = statusProjection.requested_day || today.current_runtime_day || payload.day_utc || runtimeDay;
  const fallbackState = fallbackDay && fallbackDay !== requestedDay ? `Read-only fallback showing ${fallbackDay}` : "Live projection for requested day";
  const degradedReadOnly = mode === "DEGRADED_READ_ONLY" || Boolean(dashboardDegradedReadOnlyState(payload));
  const readOnlyState = payload.read_only === true || degradedReadOnly ? "Read-only safeguards active" : "Read/write UI actions limited by governance";
  const readyStatus = resolvedMarketDataState === "MARKET_DATA_VALIDATED" || String(intradayStatus || "").includes("READY");
  const executionLocked = resolvedExecutionEligibilityState === "EXECUTION_LOCKED_NON_CERTIFIED";
  const validationExplanation = currentDay.validation_explanation || currentDay.message || currentDay.blocker || (readyStatus ? "Market data and current-day projection are valid for dashboard display." : "Validation is pending or incomplete; execution-facing workflows stay locked.");
  const retryState = retryJob.status || retryJob.state || currentDay.retry_state || (degradedReadOnly ? "waiting_for_validated_data" : "not scheduled");
  const nextScheduledAction = retryJob.next_run_at || retryJob.scheduled_for || timing.estimated_next_certification_attempt_at || currentDay.retry_at_utc || currentDay.next_scheduled_action || "not scheduled";
  const certificationTiming = timing.final_certification_status || finalStatus || "not reported";
  const blocker = degradedReadOnly || readyStatus ? "None" : (currentDay.blocker || payload.blockers?.[0]?.code || "None");
  const marketInputs = payload.market_data_inputs || payload.market_data_inputs_payload || {};
  const downstreamInvariant = marketInputs.downstream_certification_invariant || payload.downstream_certification_invariant || {};
  const downstreamMessage = downstreamInvariant.status === "DOWNSTREAM_ONLY_BLOCKER" ? (downstreamInvariant.operator_message || "US_EQUITIES_EOD is certified, but downstream market-data readiness is still blocked.") : "";
  const captureProjection = dashboardCaptureProjection(payload);
  const blockerLabel = executionLocked ? "Execution eligibility" : (degradedReadOnly ? "Candidate generation" : "Critical blocker");
  const nextAction = executionLocked
    ? "Execution-facing workflows remain locked until candidate certification completes."
    : degradedReadOnly
      ? "Use read-only workspaces; current-day candidate generation unlocks after validated EOD data."
      : (readyStatus ? "Monitor Candidates; Aegis will create a manual IB capture ticket only after certification and selection." : (currentDay.next_action || "Open System Health for blockers."));
  return renderWorkflowCard({
    payload,
    sourceKey: "canonical_operator_state",
    fieldPath: "operator_today_projection",
    whyShown: "Dashboard System Status is the single authoritative operational status area. Sidebar freshness, validation, fallback, and evidence details are consolidated here.",
    eyebrow: "SYSTEM_STATUS",
    title: "System Status",
    subtitle: "Current operational status. Historical fallback cannot overwrite this section.",
    body: `
      <div class="operator-summary-strip operator-dashboard-summary dashboard-status-metrics">
        ${renderMetricCard({ label: "Runtime day", value: runtimeDay })}
        ${renderMetricCard({ label: "Market data state", value: resolvedMarketDataState })}
        ${renderMetricCard({ label: "Candidate certification state", value: resolvedCandidateCertificationState })}
        ${renderMetricCard({ label: "Execution eligibility", value: resolvedExecutionEligibilityState })}
        ${renderMetricCard({ label: "Manual capture capability", value: captureProjection.platform_capture_capability })}
        ${renderMetricCard({ label: "IB capture tickets", value: String(captureProjection.capture_ticket_count), detail: captureProjection.capture_ticket_status })}
        ${renderMetricCard({ label: "Operator action", value: captureProjection.next_action })}
        ${renderMetricCard({ label: "Fallback / read-only state", value: fallbackState, detail: readOnlyState })}
        ${renderMetricCard({ label: "Certification timing", value: certificationTiming, detail: `Next: ${formatTimestamp(nextScheduledAction)}` })}
      </div>
      ${downstreamMessage ? `<div class="callout warning" data-downstream-certification-blocker>${escapeHtml(downstreamMessage)}</div>` : ""}
      <div class="dashboard-status-grid">
        <div class="operator-next-action-card dashboard-status-primary-action">
          <span>Next required action</span>
          <strong>${escapeHtml(nextAction)}</strong>
          <small>${escapeHtml(blockerLabel)}: ${escapeHtml(executionLocked ? resolvedExecutionEligibilityState : (degradedReadOnly ? "Waiting for validated EOD data" : blocker))}</small>
        </div>
        <div class="operator-next-action-card">
          <span>Validation explanation</span>
          <strong>${escapeHtml(validationExplanation)}</strong>
          <small>Retry state: ${escapeHtml(retryState)} · Next scheduled action: ${escapeHtml(formatTimestamp(nextScheduledAction))}</small>
        </div>
      </div>
      <div class="dashboard-status-grid dashboard-status-secondary">
        ${renderDefinitionRows([
          { label: "Market data last updated", value: formatTimestamp(timestamps.market_data_last_updated_at) },
          { label: "Candidate snapshot", value: formatTimestamp(timestamps.candidate_snapshot_timestamp) },
          { label: "Last certification attempt", value: formatTimestamp(timestamps.last_certification_attempt_at) },
          { label: "Final certification completed", value: formatTimestamp(timestamps.final_eod_certification_completed_at) },
        ])}
        ${renderDefinitionRows([
          { label: "Market close", value: formatTimestamp(timing.market_close_at) },
          { label: "Vendor lag window", value: timing.vendor_lag_window || "not reported" },
          { label: "Certification pending", value: String(timing.certification_pending === true) },
          { label: "Final certification status", value: timing.final_certification_status || "not reported" },
        ])}
      </div>
      ${renderDashboardEvidenceDrawer(payload)}
    `,
  });
}

function renderDashboardEvidenceDrawer(payload = {}) {
  const canonical = payload.canonical_operator_state || {};
  const sourcePaths = payload.source_paths || {};
  const runtimePath = sourcePaths.runtime_truth || safeList(payload.drilldown_links).find((row) => row.id === "runtime_truth")?.path || "";
  const rows = [
    { id: "canonical operator state", path: sourcePaths.canonical_operator_state || "", hash: canonical.source_hashes?.canonical_operator_state || "" },
    { id: "operator brief", path: sourcePaths.operator_brief || "", hash: canonical.source_hashes?.operator_brief || "" },
    { id: "runtime truth", path: runtimePath, hash: canonical.source_hashes?.runtime_truth || "" },
    ...safeList(payload.drilldown_links),
  ].filter((row) => row.path || row.id);
  return `
    <details class="dashboard-evidence-drawer support-note">
      <summary>Evidence Drawer</summary>
      ${renderSimpleTable({
        columns: [
          { label: "Artifact", key: "id" },
          { label: "Path", key: "path" },
          { label: "Hash", key: "hash" },
        ],
        rows,
        emptyMessage: "No evidence links are available.",
      })}
    </details>
  `;
}


function renderDashboardThesisSignals(payload = {}) {
  const projection = payload.thesis_graph_projection || payload.operator_today_projection?.thesis_graph_projection || {};
  const mission = payload.mission_control_thesis_summary || payload.operator_today_projection?.mission_control_thesis_summary || projection.mission_control_thesis_summary || {};
  const thesisCount = projection.thesis_count ?? safeList(projection.thesis_cards).length ?? 0;
  return renderWorkflowCard({
    payload,
    sourceKey: "thesis_graph_projection",
    fieldPath: "thesis_graph_projection.mission_control_thesis_summary",
    whyShown: "Mission Control surfaces strengthening, weakening, contradicted, investigation-linked, capture-producing, and blocked market theses without changing candidate certification or execution eligibility.",
    eyebrow: "MARKET_THESES",
    title: "Hypotheses",
    subtitle: "Persistent thesis graph signals for investigations, intents, captures, outcomes, and evidence.",
    body: `
      <div class="operator-summary-strip operator-dashboard-summary" data-testid="dashboard-thesis-signals">
        ${renderMetricCard({ label: "Theses", value: String(thesisCount) })}
        ${renderMetricCard({ label: "Strengthening", value: String(safeList(mission.strengthening_theses).length) })}
        ${renderMetricCard({ label: "Weakening", value: String(safeList(mission.weakening_theses).length) })}
        ${renderMetricCard({ label: "Contradicted", value: String(safeList(mission.contradicted_theses).length) })}
        ${renderMetricCard({ label: "Active investigations", value: String(safeList(mission.theses_with_active_investigations).length) })}
        ${renderMetricCard({ label: "Capture-producing", value: String(safeList(mission.theses_producing_capture_recommendations).length) })}
      </div>
      <div class="dashboard-workspace-links"><a class="ghost-button" href="/aegis-theses">Open Hypotheses</a></div>
    `,
  });
}

function renderDashboardTodaySummary(payload = {}) {
  const today = payload.operator_today_projection || {};
  const currentIntraday = today.current_intraday_candidate_count ?? today.current_day_candidate_count ?? 0;
  const selected = today.selected_candidate_count ?? 0;
  const captureProjection = dashboardCaptureProjection(payload);
  const captureReady = captureProjection.capture_ticket_count;
  const blockedSelected = today.blocked_selected_candidate_count ?? 0;
  const blockedSleeves = today.blocked_sleeves ?? 0;
  const completedToday = today.completed_capture_count ?? today.completed_captures_today ?? 0;
  return renderWorkflowCard({
    payload,
    sourceKey: "canonical_operator_state",
    fieldPath: "operator_today_projection.today_summary",
    whyShown: "Dashboard summary is a compact count view. Candidate details live in Candidates; historical captures live in Captured Trades.",
    eyebrow: "TODAYS_SUMMARY",
    title: "Today's Summary",
    subtitle: "Counts only. Open the dedicated workspace for row-level details.",
    body: `
      <div class="operator-summary-strip operator-dashboard-summary">
        ${renderMetricCard({ label: "Current intraday candidates", value: String(currentIntraday) })}
        ${renderMetricCard({ label: "Selected candidates", value: String(selected) })}
        ${renderMetricCard({ label: "IB capture tickets", value: String(captureReady), detail: captureProjection.capture_ticket_status })}
        ${renderMetricCard({ label: "Blocked selected", value: String(blockedSelected) })}
        ${renderMetricCard({ label: "Blocked sleeves", value: String(blockedSleeves) })}
        ${renderMetricCard({ label: "Completed captures today", value: String(completedToday) })}
        ${renderMetricCard({ label: "Final certified candidates", value: String(today.final_eod_certified_candidate_count ?? 0) })}
      </div>
      <div class="dashboard-workspace-links">
        <a class="primary-button" href="/aegis-candidates">Open Candidates</a>
        <a class="ghost-button" href="/aegis-journal">Open Captured Trades</a>
      </div>
    `,
  });
}

function renderDashboardAttentionRequired(payload = {}) {
  const today = payload.operator_today_projection || {};
  const currentDay = payload.current_day_status || {};
  const blockers = safeList(payload.blockers);
  const intentSummary = payload.intent_lifecycle_summary || currentDay.intent_lifecycle_summary || {};
  const captureProjection = dashboardCaptureProjection(payload);
  const captureRecommendations = Number(intentSummary.manual_ib_capture_recommended_count ?? today.manual_ib_capture_recommended_count ?? captureProjection.capture_ticket_count ?? 0);
  const systemRepair = Number(today.system_repair_required_count ?? 0);
  const needs = [];
  if (captureRecommendations > 0) {
    needs.push({
      title: `Manual IB capture recommended (${captureRecommendations})`,
      detail: "Aegis confidence, stability, and certification checks passed for an intent. Broker automation remains disabled.",
      action: "Open Intents",
      href: "/aegis-candidates",
    });
  }
  if (systemRepair > 0 || blockers.some((blocker) => String(blocker.code || "").toUpperCase().includes("CORRUPT") || String(blocker.code || "").toUpperCase().includes("MALFORMED"))) {
    needs.push({
      title: "System repair required",
      detail: currentDay.message || "Aegis detected an integrity issue that prevents normal automatic workflow.",
      action: "Open System Health",
      href: "/aegis-performance",
    });
  }
  const body = needs.length
    ? `<div class="attention-list">${needs.map((item) => `
        <article class="attention-item">
          <div><strong>${escapeHtml(item.title)}</strong><p>${escapeHtml(item.detail)}</p></div>
          <a class="ghost-button" href="${escapeHtml(item.href)}">${escapeHtml(item.action)}</a>
        </article>
      `).join("")}</div>`
    : `<div class="empty-state">Manual capture capability: ${escapeHtml(captureProjection.platform_capture_capability)}. IB capture tickets: 0. No action required.</div>`;
  return renderWorkflowCard({
    payload,
    sourceKey: "operator_task_projection",
    fieldPath: "intent_lifecycle_summary.manual_ib_capture_recommended_count",
    whyShown: "Dashboard tasks are limited to manual IB capture tickets and genuine system repair items. Manual IB capture recommendations only appear after confidence, stability, and certification gates pass.",
    eyebrow: "YOUR_TASKS",
    title: "Your tasks",
    subtitle: "Aegis handles discovery, qualification, scoring, selection, confidence, stability, and certification before recommending manual IB capture.",
    body,
  });
}

function candidatePipelineObservability(payload = {}) {
  return payload.operator_today_projection?.candidate_pipeline_observability
    || payload.current_day_status?.candidate_pipeline_observability
    || payload.candidate_pipeline_observability
    || {};
}

function renderDashboardCandidateFunnel(payload = {}) {
  const obs = candidatePipelineObservability(payload);
  const metrics = obs.metrics || {};
  return renderWorkflowCard({
    payload,
    sourceKey: "candidate_pipeline_observability",
    fieldPath: "candidate_pipeline_observability.metrics",
    whyShown: "Candidate funnel distinguishes healthy low-opportunity regimes from sleeve generation, scoring, and certification bottlenecks.",
    eyebrow: "CANDIDATE_FUNNEL",
    title: "Candidate funnel",
    subtitle: "Generated, qualified, suppressed, blocked, selected, certified, and IB ticket counts.",
    body: `
      <div class="operator-summary-strip operator-dashboard-summary">
        ${renderMetricCard({ label: "Generated", value: String(metrics.candidates_generated ?? 0) })}
        ${renderMetricCard({ label: "Qualified", value: `${metrics.qualified_count ?? 0} (${Math.round(Number(metrics.qualified_rate || 0) * 100)}%)` })}
        ${renderMetricCard({ label: "Suppressed", value: `${metrics.suppressed_count ?? 0} (${Math.round(Number(metrics.suppressed_rate || 0) * 100)}%)` })}
        ${renderMetricCard({ label: "Blocked", value: `${metrics.blocked_count ?? 0} (${Math.round(Number(metrics.blocked_rate || 0) * 100)}%)` })}
        ${renderMetricCard({ label: "Selected", value: `${metrics.selected_count ?? 0} (${Math.round(Number(metrics.selected_rate || 0) * 100)}%)` })}
        ${renderMetricCard({ label: "Certified", value: `${metrics.certified_count ?? 0} (${Math.round(Number(metrics.certified_rate || 0) * 100)}%)` })}
        ${renderMetricCard({ label: "IB ticket candidates", value: `${metrics.manual_ib_capture_ready_count ?? 0} (${Math.round(Number(metrics.manual_ib_capture_ready_rate || 0) * 100)}%)` })}
      </div>
    `,
  });
}

function renderDashboardSleeveHealth(payload = {}) {
  const obs = candidatePipelineObservability(payload);
  const bySleeve = obs.by_sleeve && typeof obs.by_sleeve === "object" ? obs.by_sleeve : {};
  const rows = Object.entries(bySleeve).map(([sleeve, metrics]) => ({ sleeve, ...(metrics || {}) }));
  return renderWorkflowCard({
    payload,
    sourceKey: "candidate_pipeline_observability",
    fieldPath: "candidate_pipeline_observability.by_sleeve",
    whyShown: "Sleeve health shows whether individual sleeves generated rows, qualified candidates, or were suppressed/blocked.",
    eyebrow: "SLEEVE_HEALTH",
    title: "Sleeve health",
    subtitle: "Per-sleeve candidate generation and funnel rates for the current trading day.",
    body: renderSimpleTable({
      columns: [
        { label: "Sleeve", key: "sleeve" },
        { label: "Generated", render: (row) => escapeHtml(String(row.candidates_generated ?? 0)) },
        { label: "Qualified", render: (row) => escapeHtml(`${row.qualified_count ?? 0} (${Math.round(Number(row.qualified_rate || 0) * 100)}%)`) },
        { label: "Suppressed", render: (row) => escapeHtml(`${row.suppressed_count ?? 0} (${Math.round(Number(row.suppressed_rate || 0) * 100)}%)`) },
        { label: "Blocked", render: (row) => escapeHtml(`${row.blocked_count ?? 0} (${Math.round(Number(row.blocked_rate || 0) * 100)}%)`) },
        { label: "Selected", render: (row) => escapeHtml(String(row.selected_count ?? 0)) },
        { label: "Certified", render: (row) => escapeHtml(String(row.certified_count ?? 0)) },
        { label: "IB ticket candidates", render: (row) => escapeHtml(String(row.manual_ib_capture_ready_count ?? 0)) },
      ],
      rows,
      emptyMessage: "No sleeve candidate rows are available for the current trading day.",
    }),
  });
}

function renderDashboardRegimeActivity(payload = {}) {
  const obs = candidatePipelineObservability(payload);
  const alerts = safeList(obs.alerts);
  const diagnostics = obs.suppression_diagnostics || {};
  const topSuppression = safeList(diagnostics.top_suppression_reasons).slice(0, 3).map((row) => `${row.reason}: ${row.count}`).join("; ") || "none";
  const topBlockers = safeList(diagnostics.top_governance_blockers).slice(0, 3).map((row) => `${row.reason}: ${row.count}`).join("; ") || "none";
  const alertRows = alerts.map((alert) => ({
    code: alert.code || "PIPELINE_ALERT",
    severity: alert.severity || "warning",
    sleeve: alert.sleeve_id || "all sleeves",
    message: alert.message || "Candidate pipeline diagnostic alert.",
  }));
  return renderWorkflowCard({
    payload,
    sourceKey: "candidate_pipeline_observability",
    fieldPath: "candidate_pipeline_observability.alerts",
    whyShown: "Regime activity separates normal quiet markets from generation, suppression, scoring, data, and certification anomalies.",
    eyebrow: "REGIME_ACTIVITY_LEVEL",
    title: "Regime activity level",
    subtitle: escapeHtml(obs.regime_activity_level || "not reported"),
    body: `
      <div class="operator-next-action-card">
        <span>Top suppression reasons</span>
        <strong>${escapeHtml(topSuppression)}</strong>
        <small>Top governance blockers: ${escapeHtml(topBlockers)}</small>
      </div>
      ${renderSimpleTable({
        columns: [
          { label: "Severity", key: "severity" },
          { label: "Alert", key: "code" },
          { label: "Sleeve", key: "sleeve" },
          { label: "Message", key: "message" },
        ],
        rows: alertRows,
        emptyMessage: "No candidate pipeline alerts.",
      })}
    `,
  });
}

function renderDashboardCaptureReadyTrend(payload = {}) {
  const obs = candidatePipelineObservability(payload);
  const rows = safeList(obs.capture_ready_trend).slice(-20);
  return renderWorkflowCard({
    payload,
    sourceKey: "candidate_pipeline_observability",
    fieldPath: "candidate_pipeline_observability.capture_ready_trend",
    whyShown: "IB ticket trend shows whether certified manual IB tickets are emerging without treating a quiet 1-2 day period as a failure.",
    eyebrow: "IB_TICKET_TREND",
    title: "IB ticket trend",
    subtitle: "Rolling manual IB ticket count and rate.",
    body: renderSimpleTable({
      columns: [
        { label: "Trading day", key: "trading_day" },
        { label: "IB tickets", render: (row) => escapeHtml(String(row.count ?? 0)) },
        { label: "Rate", render: (row) => escapeHtml(`${Math.round(Number(row.rate || 0) * 100)}%`) },
      ],
      rows,
      emptyMessage: "No IB ticket history is available yet.",
    }),
  });
}

function renderDashboardRecentEvents(payload = {}) {
  const today = payload.operator_today_projection || {};
  const manual = payload.manual_capture_candidate || payload.trade_ticket_projection_v1 || {};
  const events = [];
  if (today.latest_historical_capture_label || today.historical_latest_capture_day) {
    events.push({ label: "Latest historical capture", value: today.latest_historical_capture_label || today.historical_latest_capture_day, href: "/aegis-journal" });
  }
  if (manual.captured_read_only || manual.historical_state === "CAPTURED_HISTORICAL") {
    events.push({ label: "Manual capture recorded", value: manual.manual_capture_record?.record_id || manual.capture_record_id || "Recorded", href: "/aegis-journal" });
  }
  events.push({ label: "Latest operational refresh", value: formatTimestamp(payload.generated_at_utc || payload.generated_at), href: "/aegis-performance" });
  return renderWorkflowCard({
    payload,
    sourceKey: "canonical_operator_state",
    fieldPath: "recent_events",
    whyShown: "Recent events provide confidence without turning Dashboard into an audit console.",
    eyebrow: "RECENT_EVENTS",
    title: "Recent Events",
    subtitle: "Latest captures, transitions, and refreshes.",
    body: `<div class="recent-event-list">${events.map((event) => `<a class="recent-event-row" href="${escapeHtml(event.href)}"><span>${escapeHtml(event.label)}</span><strong>${escapeHtml(String(event.value || "not reported"))}</strong></a>`).join("")}</div>`,
  });
}

function operatorFriendlyBlockerTitle(code = "") {
  const text = String(code || "").toUpperCase();
  if (text.includes("MARKET") || text.includes("DATA")) return "Waiting for data";
  if (text.includes("UNIVERSE")) return "Universe input unavailable";
  if (text.includes("CAPITAL")) return "Capital authority blocked";
  if (text.includes("RISK")) return "Risk authority blocked";
  return String(code || "Blocked").replace(/_/g, " ").toLowerCase().replace(/^./, (c) => c.toUpperCase());
}

function candidateSemanticTone(value = "") {
  const code = String(value || "").toUpperCase();
  if (["QUALIFIED", "EXECUTION_ELIGIBLE", "MANUAL_IB_CAPTURE_READY"].includes(code)) return "healthy";
  if (["REVIEWABLE", "MONITOR_ONLY", "EXECUTION_LOCKED_NON_CERTIFIED"].includes(code)) return "warning";
  if (["BLOCKED", "NON_EXECUTABLE", "SYSTEM_REPAIR_REQUIRED"].includes(code)) return "blocked";
  if (["SUPPRESSED", "NO_USER_ACTION"].includes(code)) return "neutral";
  return "neutral";
}

function renderCandidateSemanticPill(value = "", title = "") {
  const text = readableStatus(value || "not reported");
  return `<span title="${escapeHtml(title)}">${renderStatusPill(text, candidateSemanticTone(value), {})}</span>`;
}


function runtimeTimelineProjection(payload = {}) {
  const timeline = payload.runtime_timeline_projection || payload.operator_state_snapshot?.runtime_timeline_projection || {};
  if (!timeline || typeof timeline !== "object") return {};
  return {
    ...timeline,
    day_utc: timeline.day_utc || payload.day_utc || payload.displayed_artifact_day || payload.operator_state_snapshot?.day_utc || payload.operator_state_snapshot?.displayed_artifact_day || "",
  };
}

function timelineTone(status = "") {
  const code = String(status || "").toUpperCase();
  if (["COMPLETE", "READY", "CURRENT", "COMPLETED"].includes(code)) return "healthy";
  if (["WAITING", "PENDING", "RETRYING", "QUEUED", "UPCOMING", "PAST_WINDOW", "HISTORICAL_REPLAY_PAST_EVENT"].includes(code)) return "warning";
  if (["BLOCKED", "FAILED", "MISSED"].includes(code)) return "blocked";
  return "neutral";
}

function runtimeParseMs(value) {
  const raw = String(value || "").trim();
  if (!raw) return null;
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.getTime();
}

function runtimeNowMs(timeline = {}) {
  return runtimeParseMs(timeline.generated_at_utc) || Date.now();
}

function runtimeLocalDateKey(value, timeline = {}) {
  const ms = runtimeParseMs(value);
  if (!ms) return "";
  return new Intl.DateTimeFormat("en-CA", { timeZone: "America/New_York", year: "numeric", month: "2-digit", day: "2-digit" }).format(new Date(ms));
}

function runtimeFormatTime(value, timeline = {}) {
  const ms = runtimeParseMs(value);
  if (!ms) return "Not scheduled today";
  const now = runtimeNowMs(timeline);
  const label = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    hour: "numeric",
    minute: "2-digit",
    timeZoneName: "short",
  }).format(new Date(ms));
  const dateKey = runtimeLocalDateKey(value, timeline);
  const nowKey = runtimeLocalDateKey(new Date(now).toISOString(), timeline);
  const tomorrowKey = runtimeLocalDateKey(new Date(now + 24 * 60 * 60 * 1000).toISOString(), timeline);
  if (dateKey && dateKey === nowKey) return `Today ${label}`;
  if (dateKey && dateKey === tomorrowKey) return `Tomorrow ${label}`;
  const dateLabel = new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", month: "short", day: "numeric" }).format(new Date(ms));
  return `${dateLabel} ${label}`;
}

function runtimeFormatClock(value) {
  const ms = runtimeParseMs(value);
  if (!ms) return "Not scheduled";
  return new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    hour: "numeric",
    minute: "2-digit",
    timeZoneName: "short",
  }).format(new Date(ms));
}

function runtimeScheduleDayLabel(value, timeline = {}) {
  const ms = runtimeParseMs(value);
  if (!ms) return "SCHEDULED";
  const dateKey = runtimeLocalDateKey(value, timeline);
  const now = runtimeNowMs(timeline);
  const todayKey = runtimeLocalDateKey(new Date(now).toISOString(), timeline);
  const tomorrowKey = runtimeLocalDateKey(new Date(now + 24 * 60 * 60 * 1000).toISOString(), timeline);
  if (dateKey === todayKey) return "TODAY";
  if (dateKey === tomorrowKey) return "TOMORROW";
  return new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", weekday: "short", month: "short", day: "numeric" }).format(new Date(ms)).toUpperCase();
}

function runtimeOperatorStateLabel(status = "", value = "", timeline = {}) {
  const state = runtimeDisplayState(status, value, timeline);
  if (state === "completed") return "Done";
  if (state === "active") return "In progress";
  if (state === "blocked") return "Action needed";
  if (state === "delayed") return "Waiting";
  return "Scheduled";
}


function runtimeCountdown(value, timeline = {}) {
  const ms = runtimeParseMs(value);
  if (!ms) return "Not scheduled today";
  const diffMs = ms - runtimeNowMs(timeline);
  const absMinutes = Math.max(0, Math.round(Math.abs(diffMs) / 60000));
  const hours = Math.floor(absMinutes / 60);
  const minutes = absMinutes % 60;
  const compact = hours ? `${hours}h ${minutes}m` : `${minutes}m`;
  if (diffMs > 0) return `in ${compact}`;
  return absMinutes <= 5 ? "now" : `${compact} ago`;
}

function runtimeOptionalTimestamp(value, timeline = {}) {
  return runtimeParseMs(value) ? runtimeFormatTime(value, timeline) : "Not scheduled today";
}

function runtimeStageById(pipeline = {}, stageId = "") {
  return safeList(pipeline.stages).find((stage) => String(stage.stage_id || "") === stageId) || {};
}

function runtimeJobById(jobs = [], jobId = "") {
  return safeList(jobs).find((job) => String(job.job_id || "") === jobId) || {};
}

function runtimeStateClass(status = "") {
  const code = String(status || "").toUpperCase();
  if (["COMPLETED", "COMPLETE", "SUCCEEDED", "CURRENT", "READY", "VALID", "CERTIFIED", "PASS", "SUPERSEDED"].some((token) => code.includes(token))) return "completed";
  if (["BLOCKED", "FAILED", "MISSED", "TIMEOUT"].some((token) => code.includes(token))) return "blocked";
  if (["WAITING ON DATA", "WAITING_ON_DATA", "SCHEDULED", "PLANNED"].some((token) => code.includes(token))) return "pending";
  if (["DELAYED", "RETRY", "LATE"].some((token) => code.includes(token))) return "delayed";
  if (["ACTIVE", "RUNNING", "VALIDATING", "IN_PROGRESS"].some((token) => code.includes(token))) return "active";
  return "pending";
}

function runtimeStateLabel(state = "") {
  return String(state || "pending").replace(/_/g, " ").toLowerCase().replace(/^./, (c) => c.toUpperCase());
}

function runtimeIsReplayMode(timeline = {}) {
  const modeText = JSON.stringify({
    replay_mode: timeline.replay_mode,
    runtime_mode: timeline.runtime_mode,
    mode: timeline.mode,
    source_mode: timeline.source_mode,
  }).toUpperCase();
  return timeline.replay_mode === true || modeText.includes("REPLAY");
}

function runtimeDisplayState(status = "", value = "", timeline = {}) {
  const raw = String(status || "").toUpperCase();
  const state = runtimeStateClass(status);
  const ms = runtimeParseMs(value);
  if (raw.includes("WAITING ON DATA") || raw.includes("WAITING_ON_DATA")) return "pending";
  if (raw.includes("SCHEDULED") || raw.includes("PLANNED")) return state;
  if (!runtimeIsReplayMode(timeline) && ms && ms > runtimeNowMs(timeline) && state === "completed") return "pending";
  if (ms && ms <= runtimeNowMs(timeline) && state === "active") return "active";
  if (ms && ms <= runtimeNowMs(timeline) && state === "pending") return "delayed";
  return state;
}

function runtimeStatusForTime(value, timeline = {}, fallback = "pending") {
  const ms = runtimeParseMs(value);
  if (!ms) return fallback;
  return runtimeNowMs(timeline) >= ms ? "completed" : "pending";
}

function runtimeProjectedEventToScheduleRow(row = {}) {
  return {
    id: row.event_key || row.event_id || row.id || "runtime_event",
    eventId: row.event_id || "",
    event: row.event_name || row.event || "Runtime event",
    scheduledTime: row.scheduled_at_utc || row.scheduled_at || row.scheduledTime || "",
    purpose: row.purpose || "Scheduled runtime step.",
    status: row.operator_status || row.status || row.run_status || "Scheduled",
    runStatus: row.run_status || "",
    statusReason: row.status_reason || "",
    expectedOutcome: row.expected_result || row.expectedOutcome || "Aegis advances the governed runtime state.",
    sortTime: row.scheduled_at_utc || row.scheduled_at || row.scheduledTime || "",
    triggerType: row.trigger_type || "",
    eventCategory: row.event_category || row.eventCategory || "",
    visibilitySurface: row.visibility_surface || row.visibilitySurface || "",
    operatorRelevanceReason: row.operator_relevance_reason || row.operatorRelevanceReason || "",
    sourceJobId: row.source_job_id || row.owning_job || "",
    escalationState: row.escalation_state || "",
    escalationMessage: row.escalation_message || "",
    operatorNextAction: row.operator_next_action || "",
    dependencyAgeMinutes: row.dependency_age_minutes ?? "",
    minutesPastGrace: row.minutes_past_grace ?? "",
    retryExhaustionThreshold: row.retry_exhaustion_threshold ?? "",
    providerFeedName: row.provider_feed_name || "",
    nextRetryUtc: row.next_retry_utc || "",
    providerSlaMinutes: row.provider_sla_minutes ?? "",
    escalationThresholdMinutes: row.escalation_threshold_minutes ?? "",
    missingArtifacts: row.missing_artifacts || [],
    lastSuccessfulArtifact: row.last_successful_artifact || "",
    diagnosticsRecommended: row.diagnostics_recommended === true,
  };
}

function runtimeRowNeedsAttention(row = {}, timeline = {}) {
  const escalation = String(row.escalationState || row.escalation_state || "").toUpperCase();
  if (["OVERDUE", "ESCALATED", "FAILED"].includes(escalation)) return true;
  const status = String(row.status || row.operatorStatus || "").toUpperCase();
  const reason = String(row.statusReason || "").toUpperCase();
  const runStatus = String(row.runStatus || "").toUpperCase();
  const scheduled = runtimeParseMs(row.scheduledTime);
  const isPast = Boolean(scheduled && scheduled <= runtimeNowMs(timeline));
  if (["FAILED", "MISSED", "BLOCKED"].some((token) => status.includes(token) || runStatus.includes(token))) return true;
  if (status.includes("WAITING ON DATA") && isPast) return true;
  if (status.includes("RUNNING") && isPast) return true;
  return isPast && ["DEPENDENCY", "WAIT", "MISSING", "REJECT", "TIMEOUT", "BLOCK"].some((token) => reason.includes(token));
}

function runtimeBuildEventGroups(timeline = {}) {
  const groups = timeline.event_groups || {};
  const mapRows = (rows) => safeList(rows).map(runtimeProjectedEventToScheduleRow).filter((row) => runtimeParseMs(row.scheduledTime));
  const byTime = (a, b) => runtimeScheduleSortTime(a) - runtimeScheduleSortTime(b);
  const attentionPriority = (row) => {
    const text = `${row.status || ""} ${row.runStatus || ""} ${row.statusReason || ""}`.toUpperCase();
    if (text.includes("FAILED")) return 0;
    if (text.includes("MISSED")) return 1;
    if (text.includes("BLOCK")) return 2;
    if (text.includes("WAITING ON DATA") || text.includes("DEPENDENCY")) return 3;
    if (text.includes("RUNNING")) return 4;
    return 5;
  };
  const needsAttention = mapRows(groups.needs_attention || timeline.needs_attention_events).sort((a, b) => attentionPriority(a) - attentionPriority(b) || byTime(a, b));
  const upcoming = mapRows(groups.upcoming || timeline.upcoming_events).filter((row) => runtimeParseMs(row.scheduledTime) > runtimeNowMs(timeline)).sort(byTime);
  const completedToday = mapRows(groups.completed_today || timeline.completed_today_events).sort((a, b) => runtimeScheduleSortTime(b) - runtimeScheduleSortTime(a));
  return { needsAttention, upcoming, completedToday };
}

function runtimeBuildOperatorSchedule(timeline = {}) {
  return runtimeBuildEventGroups(timeline).upcoming;
}

function runtimeStepState({ time = "", status = "", current = false, blocked = false, delayed = false } = {}, timeline = {}) {
  if (blocked) return "blocked";
  if (delayed) return "delayed";
  const stateFromStatus = runtimeDisplayState(status, time, timeline);
  if (current && !["completed", "blocked"].includes(stateFromStatus)) return "active";
  if (["blocked", "delayed", "completed", "pending"].includes(stateFromStatus)) return stateFromStatus;
  return runtimeStatusForTime(time, timeline, "pending");
}

function runtimeScheduleSortTime(row = {}) {
  return runtimeParseMs(row.sortTime || row.scheduledTime) || Number.POSITIVE_INFINITY;
}

function runtimeScheduleDisplayState(row = {}, timeline = {}) {
  return runtimeDisplayState(row.status, row.sortTime || row.scheduledTime, timeline);
}

function runtimeHeroEligible(row = {}) {
  const event = String(row.event || "").toUpperCase();
  const id = String(row.id || "").toUpperCase();
  if (event.includes("RETRY") || id.includes("RETRY")) return false;
  if (event.includes("WINDOW OPENS")) return false;
  if (event.includes("PRELIMINARY RECOMMENDATION WINDOW")) return false;
  return true;
}

function runtimeNextEvent(schedule = [], timeline = {}) {
  const actionable = schedule
    .map((row) => ({ ...row, timeMs: runtimeScheduleSortTime(row), displayState: runtimeScheduleDisplayState(row, timeline) }))
    .filter((row) => row.displayState !== "completed")
    .filter(runtimeHeroEligible)
    .sort((a, b) => a.timeMs - b.timeMs);
  if (actionable.length) return actionable[0];
  const blocked = schedule.find((row) => runtimeScheduleDisplayState(row, timeline) === "blocked");
  if (blocked) return { ...blocked, event: blocked.event || "Operator review required", fallbackTime: blocked.fallbackTime || "Blocked now", expectedOutcome: blocked.expectedOutcome || "Resolve the blocker before the pipeline can advance." };
  return {
    event: "Runtime complete",
    scheduledTime: "",
    fallbackTime: "No more scheduled events today",
    status: "COMPLETE",
    expectedOutcome: "Aegis is complete for the visible runtime schedule.",
  };
}

function runtimeHumanStatusForEvent(row = {}, timeline = {}) {
  const escalationState = String(row.escalationState || row.escalation_state || "").toUpperCase();
  if (escalationState === "FAILED") return row.escalationMessage || row.escalation_message || "Certification failed";
  if (escalationState === "ESCALATED") return row.escalationMessage || row.escalation_message || "Vendor dependency exceeded operational threshold";
  if (escalationState === "OVERDUE") return row.escalationMessage || row.escalation_message || "Vendor data overdue";
  if (escalationState === "DELAYED") return row.escalationMessage || row.escalation_message || "Vendor data delayed";
  const raw = String(row.status || "").trim();
  const reason = String(row.statusReason || "").toUpperCase();
  const context = `${row.event || ""} ${row.purpose || ""} ${row.expectedOutcome || ""} ${row.sourceJobId || ""} ${reason}`.toUpperCase();
  const scheduled = runtimeParseMs(row.scheduledTime);
  const isPast = Boolean(scheduled && scheduled <= runtimeNowMs(timeline));
  const code = raw.toUpperCase();
  if (code.includes("WAITING ON DATA")) {
    if (context.includes("VENDOR") || context.includes("FINAL_EOD") || context.includes("CERTIFICATION") || context.includes("MARKET DATA")) return "Waiting on vendor data";
    return "Waiting on required data";
  }
  if (code.includes("FAILED")) return "Failed";
  if (code.includes("MISSED")) return "Missed";
  if (code.includes("BLOCKED")) return "Blocked";
  if (code.includes("RUNNING")) return "Running";
  if (code.includes("SUPERSEDED")) return "Superseded";
  if (code.includes("COMPLETED") || code.includes("SUCCEEDED") || code.includes("COMPLETE")) return "Completed";
  if (isPast && (code.includes("SCHEDULED") || code.includes("PLANNED"))) return "Missed";
  if (code.includes("SCHEDULED") || code.includes("PLANNED") || code.includes("WAITING")) return "Scheduled";
  return raw || "Scheduled";
}

function runtimeHeroEvent(timeline = {}, groups = {}) {
  const projectedNext = timeline.next_event && (timeline.next_event.scheduled_at_utc || timeline.next_event.event_name) ? runtimeProjectedEventToScheduleRow(timeline.next_event) : null;
  if (groups.needsAttention?.length) return { ...groups.needsAttention[0], heroKind: "attention" };
  if (projectedNext && runtimeRowNeedsAttention(projectedNext, timeline)) return { ...projectedNext, heroKind: "attention" };
  if (groups.upcoming?.length) return { ...groups.upcoming[0], heroKind: "upcoming" };
  if (projectedNext && runtimeParseMs(projectedNext.scheduledTime) > runtimeNowMs(timeline)) return { ...projectedNext, heroKind: "upcoming" };
  return {
    event: "Runtime schedule clear",
    scheduledTime: "",
    fallbackTime: "No more scheduled events today",
    status: "Completed",
    expectedOutcome: "No operator action is required unless a new runtime event is scheduled.",
    heroKind: "clear",
  };
}

function renderRuntimeNextEventHero(timeline = {}, groups = {}) {
  const clock = timeline.runtime_clock_semantics || {};
  const session = timeline.market_session_timeline || {};
  const next = runtimeHeroEvent(timeline, groups);
  const scheduleForEta = [...safeList(groups.upcoming), ...safeList(groups.needsAttention)];
  const certEvent = scheduleForEta.find((row) => row.id === "final_eod_certification" || row.id === "certification_window" || row.id === "certification") || {};
  const captureEvent = scheduleForEta.find((row) => row.id === "manual_capture_window" || row.id === "final_recommendation_window") || {};
  const certificationEta = certEvent.scheduledTime || session.certification_window_end_utc || session.final_eod_certification_target_utc || next.scheduledTime || "";
  const captureTiming = captureEvent.scheduledTime ? `Expected ${runtimeFormatTime(captureEvent.scheduledTime, timeline)}` : "Expected after certification";
  const lastUpdate = clock.operator_snapshot_timestamp || timeline.generated_at_utc || "";
  const attention = next.heroKind === "attention";
  const statusText = runtimeHumanStatusForEvent(next, timeline);
  const escalationState = String(next.escalationState || "").toUpperCase();
  const pastGrace = Number(next.minutesPastGrace || 0);
  const age = Number(next.dependencyAgeMinutes || 0);
  const overdueText = pastGrace > 0 ? `${Math.floor(pastGrace / 60)}h ${pastGrace % 60}m past grace window` : (age > 0 ? `${Math.floor(age / 60)}h ${age % 60}m past expected window` : "");
  const timeText = next.scheduledTime ? (attention && overdueText ? overdueText : `${attention ? "Expected" : "Expected by"} ${runtimeFormatClock(next.scheduledTime)}`) : (next.fallbackTime || "Not scheduled");
  const reason = next.statusReason ? `Reason: ${next.statusReason.replace(/_/g, " ").toLowerCase().replace(/^./, (c) => c.toUpperCase())}` : (next.expectedOutcome || "Aegis advances the governed runtime state.");
  const userAction = attention
    ? (next.operatorNextAction || (escalationState === "NORMAL_WAIT" ? "No operator action required; Aegis is waiting on provider data." : "Diagnostics recommended."))
    : "No action required until this event starts.";
  const escalationClass = escalationState ? ` runtime-escalation-${escalationState.toLowerCase().replace(/_/g, "-")}` : "";
  return `<section class="runtime-mission-banner${attention ? " runtime-mission-attention" : ""}${escalationClass}" data-runtime-next-event aria-label="What needs attention or happens next">
    <div class="runtime-clean-meta" data-runtime-clean-meta>
      <span><strong>Operational day</strong>${escapeHtml(timeline.day_utc || "Not reported")}</span>
      <span><strong>Last update</strong>${escapeHtml(runtimeOptionalTimestamp(lastUpdate, timeline))}</span>
      <span><strong>Certification ETA</strong>${escapeHtml(certificationEta ? runtimeFormatTime(certificationEta, timeline) : "Not scheduled")}</span>
    </div>
    <div class="runtime-mission-main">
      <div class="runtime-next-copy">
        <span class="runtime-kicker">${attention ? "NEEDS ATTENTION" : "NEXT"}</span>
        <h2>${escapeHtml(next.event || "Next runtime event")}</h2>
        <p>${escapeHtml(timeText)}</p>
        <small class="runtime-event-status">${escapeHtml(statusText)}${next.scheduledTime ? ` · ${escapeHtml(runtimeCountdown(next.scheduledTime, timeline))}` : ""}</small>
        <small class="runtime-event-reason">${escapeHtml(reason)}</small>
        <small class="runtime-event-action">${escapeHtml(userAction)}</small>
      </div>
      <div class="runtime-capture-expectation">
        <span>Capture recommendations</span>
        <strong>${escapeHtml(captureTiming)}</strong>
      </div>
    </div>
  </section>`;
}

function renderRuntimeHorizontalTimeline(timeline = {}, schedule = []) {
  const session = timeline.market_session_timeline || {};
  const pipeline = timeline.current_pipeline_state || {};
  const alerts = safeList(timeline.alerts);
  const currentPhase = String(session.current_operational_phase || "").toUpperCase();
  const finalPending = String(timeline.certification_timing?.final_eod_certification_status || "").toUpperCase() === "PENDING";
  const certificationDelayed = finalPending && runtimeParseMs(session.certification_window_end_utc) && runtimeNowMs(timeline) > runtimeParseMs(session.certification_window_end_utc);
  const scheduleById = Object.fromEntries(schedule.map((row) => [row.id, row]));
  const manualCaptureStage = runtimeStageById(pipeline, "manual_capture_ready");
  const steps = [
    { label: "Market Open", time: session.market_open_utc, status: runtimeStatusForTime(session.market_open_utc, timeline), current: currentPhase === "PRE_MARKET" },
    { label: "Market Close", time: session.market_close_utc, status: runtimeStatusForTime(session.market_close_utc, timeline), current: currentPhase === "INTRADAY_OPERATIONAL" || currentPhase === "WAITING_FOR_INTRADAY_DATA" },
    { label: "Preliminary AI Run", time: scheduleById.preliminary_ai_run?.scheduledTime || session.market_close_utc, status: scheduleById.preliminary_ai_run?.status || "WAITING" },
    { label: "Certification Window", time: session.certification_window_start_utc, status: currentPhase === "VENDOR_LAG_WINDOW" || currentPhase === "FINAL_EOD_CERTIFICATION_PENDING" ? "ACTIVE" : runtimeStatusForTime(session.certification_window_start_utc, timeline), current: currentPhase === "VENDOR_LAG_WINDOW" },
    { label: "Final EOD Certification", time: session.certification_window_end_utc, status: scheduleById.certification?.status || "WAITING", current: currentPhase === "FINAL_EOD_CERTIFICATION_PENDING", delayed: certificationDelayed },
    { label: "Manual IB Capture Window", time: scheduleById.manual_capture_window?.scheduledTime || "", fallback: scheduleById.manual_capture_window?.fallbackTime || "After certification", status: manualCaptureStage.status || scheduleById.manual_capture_window?.status || "WAITING", blocked: alerts.some((alert) => String(alert.alert_type || "") === "pipeline_stage_blocked") && String(manualCaptureStage.status || "").toUpperCase() === "BLOCKED" },
  ];
  return renderWorkflowCard({
    payload: { runtime_timeline_projection: timeline },
    sourceKey: "operator_state_snapshot_v1",
    fieldPath: "runtime_timeline_projection.market_session_timeline",
    whyShown: "The Runtime Timeline is schedule-first so operators can quickly see what happened, what is active, and what remains before capture recommendations.",
    eyebrow: "RUNTIME_TIMELINE",
    title: "Horizontal Runtime Timeline",
    subtitle: "Market day and Aegis runtime milestones in operator order.",
    body: `<div class="runtime-horizontal-timeline" data-runtime-horizontal-timeline>${steps.map((step, index) => {
      const state = runtimeStepState(step, timeline);
      return `<div class="runtime-timeline-step runtime-state-${escapeHtml(state)}${step.current ? " runtime-current-phase" : ""}" data-runtime-state="${escapeHtml(state)}">
        <div class="runtime-step-marker">${escapeHtml(String(index + 1))}</div>
        <div class="runtime-step-body">
          <strong>${escapeHtml(step.label)}</strong>
          <span>${escapeHtml(step.time ? runtimeOptionalTimestamp(step.time, timeline) : (step.fallback || "Not scheduled today"))}</span>
          <small>${escapeHtml(runtimeStateLabel(state))}</small>
        </div>
      </div>`;
    }).join('<div class="runtime-timeline-connector" aria-hidden="true">&rarr;</div>')}</div>`,
  });
}

function renderRuntimeScheduleRows(rows = [], timeline = {}, { compact = false } = {}) {
  if (!rows.length) return "";
  const grouped = rows.reduce((acc, row) => {
    const label = runtimeScheduleDayLabel(row.scheduledTime, timeline);
    if (!acc[label]) acc[label] = [];
    acc[label].push({
      ...row,
      clock: runtimeFormatClock(row.scheduledTime),
      operatorState: runtimeHumanStatusForEvent(row, timeline),
    });
    return acc;
  }, {});
  return Object.entries(grouped).map(([label, groupRows]) => `<div class="runtime-schedule-group">
    <h3>${escapeHtml(label)}</h3>
    <div class="runtime-schedule-list">
      ${groupRows.map((row) => `<details class="runtime-schedule-row${compact ? " runtime-schedule-row-compact" : ""}" data-runtime-schedule-row>
        <summary>
          <time>${escapeHtml(row.clock)}</time>
          <span>${escapeHtml(row.event)}</span>
          <small>${escapeHtml(row.operatorState)}</small>
        </summary>
        <p>${escapeHtml(row.purpose || "Scheduled runtime step.")}</p>
        <p>${escapeHtml(row.statusReason ? `Reason: ${row.statusReason.replace(/_/g, " ")}` : (row.expectedOutcome || "Aegis advances the governed runtime state."))}</p>
        ${row.operatorNextAction ? `<p>${escapeHtml(row.operatorNextAction)}</p>` : ""}
        ${row.diagnosticsRecommended ? `<p>${escapeHtml(`Diagnostics recommended · retry ${row.retryExhaustionThreshold ? `threshold ${row.retryExhaustionThreshold}` : "threshold not reported"}`)}</p>` : ""}
      </details>`).join("")}
    </div>
  </div>`).join("");
}

const DOMAIN_CERTIFICATION_SEMANTICS = {
  certified: { color: "green" },
  partial: { color: "blue" },
  degraded: { color: "yellow" },
  delayed: { color: "yellow" },
  conflicted: { color: "orange" },
  failed: { color: "red" },
  ready: { color: "green" },
  blocked: { color: "orange" },
  warning: { color: "yellow" },
  unknown: { color: "yellow" },
};

function runtimeDomainTone(status = "") {
  const code = String(status || "").toUpperCase();
  if (code === "CERTIFIED") return "certified";
  if (code === "PARTIAL") return "partial";
  if (code === "DEGRADED") return "degraded";
  if (code === "DELAYED") return "delayed";
  if (code === "CONFLICTED") return "conflicted";
  if (code === "FAILED") return "failed";
  if (code === "READY") return "ready";
  if (code === "BLOCKED") return "blocked";
  if (code === "READY_WITH_WARNINGS") return "warning";
  return "unknown";
}

function runtimeRepairSourceLabel(source = {}) {
  const label = String(source.label || source.artifact_type || "Required source/artifact");
  const path = String(source.path || "").trim();
  return path ? `${label}: ${path}` : label;
}

function renderRuntimeCommandResultPanel(result = {}, options = {}) {
  if (!result || typeof result !== "object") return "";
  const statusLabel = String(result.status_label || result.result_status || "Command result");
  const domain = String(options.domain || result.domain || result.domain_id || "");
  const message = String(result.plain_english_result || "Command completed.");
  const nextStep = String(result.next_required_step || "Review the updated card state.");
  const timestamp = String(result.timestamp || "");
  const jobId = String(result.job_id || "");
  const auditId = String(result.audit_id || "");
  const missingSource = String(result.missing_source_name || "");
  const expectedPath = String(result.expected_source_path || result.copy_required_path || "");
  const failureReason = String(result.failure_reason || "");
  const tone = String(result.result_status || "").toLowerCase().replace(/[^a-z0-9_-]/g, "-") || "result";
  return `<div class="command-result-panel" data-command-result-panel data-result-status="${escapeHtml(tone)}" role="status" aria-live="polite">
    <div class="command-result-panel-heading">
      <strong>${escapeHtml(statusLabel)}</strong>
      ${timestamp ? `<span>${escapeHtml(timestamp)}</span>` : ""}
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

function renderRuntimeDomainRepairPlan(plan = {}) {
  const domainId = String(plan.domain_id || "Domain");
  const sleeves = safeList(plan.affected_sleeves);
  const hypotheses = safeList(plan.affected_hypotheses);
  const affected = [
    sleeves.length ? `Sleeves: ${sleeves.join(", ")}` : "Sleeves: none directly blocked",
    hypotheses.length ? `Hypotheses: ${hypotheses.join(", ")}` : "Hypotheses: no direct mapping recorded",
  ].join(" | ");
  return `<article class="runtime-domain-repair-card" data-domain-repair-card="${escapeHtml(domainId)}" tabindex="-1">
    <div class="runtime-domain-repair-header">
      <div>
        <strong>${escapeHtml(plan.domain_name || domainId)}</strong>
        <span>${escapeHtml(plan.next_retry || "Aegis will retry automatically")}</span>
      </div>
      <button class="primary-button runtime-domain-repair-button" type="button" data-aegis-command-id="${escapeHtml(plan.command_id || "REPAIR_DOMAIN")}" data-aegis-command-action-type="API_COMMAND" data-aegis-command-target-type="domain_certification" data-aegis-command-target-id="${escapeHtml(domainId)}" data-aegis-command-payload="${escapeHtml(JSON.stringify({ domain_id: domainId, day_utc: plan.day_utc || "", operational_day: plan.day_utc || "" }))}">${escapeHtml(plan.repair_button_label || "Repair")}</button>
    </div>
    <dl class="runtime-domain-repair-facts">
      <div><dt>Reason</dt><dd>${escapeHtml(plan.reason || "Domain delayed")}</dd></div>
      <div><dt>Affected sleeves/hypotheses</dt><dd>${escapeHtml(affected)}</dd></div>
      <div><dt>Required source/artifact</dt><dd>${escapeHtml(runtimeRepairSourceLabel(plan.required_source_artifact || {}))}</dd></div>
      <div><dt>Last attempt</dt><dd>${escapeHtml(plan.last_attempt || "No certification attempt recorded")}</dd></div>
      <div><dt>Next retry</dt><dd>${escapeHtml(plan.next_retry || "Aegis will retry automatically")}</dd></div>
      <div><dt>Repair action</dt><dd>${escapeHtml(plan.repair_action || "Review source and re-run domain certification")}</dd></div>
    </dl>
    <ol class="runtime-domain-repair-steps">
      ${safeList(plan.repair_steps).map((step) => `<li>${escapeHtml(step)}</li>`).join("")}
    </ol>
    <p class="runtime-domain-repair-status" data-aegis-command-status data-domain-repair-status>${escapeHtml(plan.source_setup_required ? "Source setup required" : "Aegis will retry automatically")}</p>
  </article>`;
}

function renderRuntimeDomainCommandResults(repairPlans = []) {
  const plans = safeList(repairPlans).filter((plan) => plan && plan.domain_id);
  const hasResults = plans.some((plan) => plan.latest_command_result);
  const slots = plans.map((plan) => {
    const domainId = String(plan.domain_id || "Domain");
    const result = plan.latest_command_result;
    return `<div class="runtime-domain-command-result-slot" data-domain-command-result-sink="${escapeHtml(domainId)}" data-command-result-domain="${escapeHtml(plan.domain_name || domainId)}"${result ? "" : " hidden"}>${result ? renderRuntimeCommandResultPanel(result, { domain: plan.domain_name || domainId }) : ""}</div>`;
  }).join("");
  return `<section class="runtime-domain-command-results" data-domain-command-results-region aria-label="Domain Command Results"${hasResults ? "" : " hidden"}>
    <div class="runtime-section-heading compact">
      <h3>Domain Command Results</h3>
      <p>Latest repair command outcomes. Results are kept out of the domain card grid so domain status remains readable.</p>
    </div>
    <div class="runtime-domain-command-result-list" data-domain-command-result-list>${slots}</div>
  </section>`;
}

function renderRuntimeRepairCenterSummary(repairPlans = []) {
  const items = safeList(repairPlans);
  const openCount = items.length;
  const runningCount = items.filter((item) => ["QUEUED", "RUNNING", "VALIDATING", "RECERTIFYING"].includes(String(item.latest_command_result?.result_status || item.latest_command_result?.status_label || "").toUpperCase())).length;
  const sourceCount = items.filter((item) => item.source_setup_required).length;
  return `<section class="runtime-repair-summary" data-runtime-repair-summary aria-label="Repair Center Summary">
    <div>
      <strong>${escapeHtml(String(openCount))} repair items open</strong>
      <span>${escapeHtml(String(runningCount))} repair running | ${escapeHtml(String(sourceCount))} source setup required</span>
    </div>
    <button class="primary-button runtime-repair-center-link" type="button" data-aegis-command-id="OPEN_VALID_ROUTE" data-aegis-command-action-type="EXPAND_SECTION" data-aegis-command-target-type="navigation" data-aegis-command-target-id="aegis_repair_center" data-route="/aegis-repair-center">Open Repair Center</button>
  </section>`;
}

function renderRuntimeDomainCertificationGrid(timeline = {}) {
  const report = timeline.domain_certification || {};
  const rows = safeList(timeline.domain_certification_grid || report.domains);
  const repairPlans = safeList(timeline.domain_repair_actions || report.domain_repair_actions || rows.map((row) => row.repair_plan).filter(Boolean));
  const domainCommandDay = String(timeline.day_utc || report.day_utc || "");
  const sleeveRows = safeList(timeline.sleeve_domain_statuses || report.sleeve_domain_statuses);
  const blockedByDomain = Object.fromEntries(rows.map((row) => [row.domain_id, safeList(row.blocking_sleeves)]));
  return `<section class="runtime-domain-certification" data-runtime-domain-certification aria-label="Domain Certification">
    <div class="runtime-section-heading">
      <h2>Domain Certification</h2>
      <p>Independent data domains certify separately. Unaffected sleeves can continue when unrelated domains are delayed.</p>
    </div>
    <div class="runtime-domain-grid">
      ${rows.map((row) => {
        const delayed = String(row.certification_status || "").toUpperCase() === "DELAYED";
        return `<article class="runtime-domain-card runtime-domain-${escapeHtml(String(row.certification_status || "").toLowerCase())}" data-domain-card="${escapeHtml(row.domain_id || "Domain")}">
          <div>
            <strong>${escapeHtml(row.domain_id || "Domain")}</strong>
            ${renderStatusPill(row.certification_status || "UNKNOWN", runtimeDomainTone(row.certification_status), DOMAIN_CERTIFICATION_SEMANTICS)}
          </div>
          <p>${escapeHtml(row.reason_codes?.[0] || (row.single_provider_risk ? "Single-provider risk recorded" : "Domain snapshot available"))}</p>
          <small>${escapeHtml(blockedByDomain[row.domain_id]?.length ? `Blocks ${blockedByDomain[row.domain_id].join(", ")}` : "No required sleeve blocked")}</small>
          ${delayed ? `<span class="runtime-domain-card-next">Repair Center</span>` : ""}
        </article>`;
      }).join("")}
    </div>
    ${renderRuntimeRepairCenterSummary(repairPlans)}
    <details class="runtime-domain-sleeves">
      <summary>Sleeve domain blockers</summary>
      ${renderSimpleTable({
        columns: [
          { label: "Sleeve", key: "sleeve_id" },
          { label: "Status", render: (row) => renderStatusPill(row.sleeve_certification_status || "UNKNOWN", runtimeDomainTone(row.sleeve_certification_status), DOMAIN_CERTIFICATION_SEMANTICS) },
          { label: "Blocking domains", render: (row) => escapeHtml(safeList(row.blocking_domains).join(", ") || "None") },
          { label: "Next action", render: (row) => escapeHtml(row.operator_next_action || "No domain action required.") },
        ],
        rows: sleeveRows,
        emptyMessage: "No sleeve domain status rows are available.",
      })}
    </details>
  </section>`;
}


function renderRuntimeNeedsAttention(timeline = {}, groups = {}) {
  const rows = safeList(groups.needsAttention);
  return `<section class="runtime-compact-schedule runtime-needs-attention" data-runtime-needs-attention aria-label="Needs Attention">
    <div class="runtime-section-heading">
      <h2>Needs Attention</h2>
      <p>Unresolved overdue, blocked, failed, missed, or dependency-waiting events.</p>
    </div>
    ${rows.length ? renderRuntimeScheduleRows(rows, timeline) : `<p class="runtime-empty-note">No overdue runtime event needs operator attention.</p>`}
  </section>`;
}

function renderRuntimeUpcoming(timeline = {}, groups = {}) {
  const rows = safeList(groups.upcoming).filter((row) => runtimeParseMs(row.scheduledTime) > runtimeNowMs(timeline)).slice(0, 8);
  return `<section class="runtime-compact-schedule runtime-upcoming-events" data-runtime-upcoming-events aria-label="Upcoming">
    <div class="runtime-section-heading">
      <h2>Upcoming</h2>
      <p>Future scheduled events only. Past unresolved events stay in Needs Attention.</p>
    </div>
    ${rows.length ? renderRuntimeScheduleRows(rows, timeline) : `<p class="runtime-empty-note">No future scheduled runtime events.</p>`}
  </section>`;
}

function renderRuntimeCompletedToday(timeline = {}, groups = {}) {
  const rows = safeList(groups.completedToday).slice(0, 8);
  return `<details class="runtime-completed-today" data-runtime-completed-today open>
    <summary>Completed Today</summary>
    ${rows.length ? renderRuntimeScheduleRows(rows, timeline, { compact: true }) : `<p class="runtime-empty-note">No completed runtime events are recorded for this operational day.</p>`}
  </details>`;
}

function renderRuntimeCompactSchedule(timeline = {}, schedule = []) {
  const groups = Array.isArray(schedule) ? { needsAttention: [], upcoming: schedule, completedToday: [] } : schedule;
  return [
    renderRuntimeNeedsAttention(timeline, groups),
    renderRuntimeUpcoming(timeline, groups),
  ].join("");
}

function runtimeProgressStepState({ stage = {}, time = "", forceActive = false } = {}, timeline = {}) {
  const raw = String(stage.status || "").toUpperCase();
  if (raw.includes("BLOCKED") || raw.includes("FAIL")) return "blocked";
  if (forceActive) return "active";
  if (raw.includes("COMPLETE") || raw.includes("VALID") || raw.includes("READY") || runtimeStatusForTime(time, timeline) === "completed") return "completed";
  return "pending";
}

function renderRuntimePipelineProgress(timeline = {}, schedule = []) {
  const session = timeline.market_session_timeline || {};
  const pipeline = timeline.current_pipeline_state || {};
  const scheduleById = Object.fromEntries(schedule.map((row) => [row.id, row]));
  const currentPhase = String(session.current_operational_phase || "").toUpperCase();
  const finalPending = String(timeline.certification_timing?.final_eod_certification_status || "").toUpperCase() === "PENDING";
  const aiRow = scheduleById.afternoon_ai_run || scheduleById.morning_ai_run || scheduleById.preliminary_ai_run || {};
  const steps = [
    { label: "Market Close", time: session.market_close_utc, state: runtimeStatusForTime(session.market_close_utc, timeline) === "completed" ? "completed" : "pending" },
    { label: "AI Workflow", time: aiRow.scheduledTime, state: runtimeProgressStepState({ stage: runtimeStageById(pipeline, "candidate_generation"), time: aiRow.scheduledTime }, timeline) },
    { label: "Certification", time: scheduleById.final_eod_certification?.scheduledTime || session.certification_window_end_utc, state: runtimeProgressStepState({ stage: runtimeStageById(pipeline, "certification"), time: scheduleById.final_eod_certification?.scheduledTime, forceActive: finalPending || currentPhase.includes("CERTIFICATION") || currentPhase === "VENDOR_LAG_WINDOW" }, timeline) },
    { label: "Capture Window", time: scheduleById.manual_capture_window?.scheduledTime, state: runtimeProgressStepState({ stage: runtimeStageById(pipeline, "manual_capture_ready"), time: scheduleById.manual_capture_window?.scheduledTime }, timeline) },
    { label: "Reconciliation", time: scheduleById.reconciliation?.scheduledTime, state: runtimeProgressStepState({ stage: runtimeStageById(pipeline, "reconciliation"), time: scheduleById.reconciliation?.scheduledTime }, timeline) },
  ];
  const symbol = { completed: "✓", active: "●", blocked: "!", pending: "○" };
  const label = { completed: "Done", active: "Active", blocked: "Action needed", pending: "Pending" };
  return `<section class="runtime-minimal-progress" data-runtime-minimal-progression aria-label="Pipeline Stages">
    ${steps.map((step) => `<div class="runtime-progress-dot runtime-state-${escapeHtml(step.state)}" data-runtime-state="${escapeHtml(step.state)}">
      <strong>${escapeHtml(symbol[step.state] || "○")}</strong>
      <span>${escapeHtml(step.label)}</span>
      <small>${escapeHtml(step.state === "pending" && step.time ? runtimeScheduleDayLabel(step.time, timeline) : label[step.state])}</small>
    </div>`).join('<div class="runtime-progress-line" aria-hidden="true"></div>')}
  </section>`;
}

function renderRuntimeDiagnostics(timeline = {}) {
  const retry = timeline.retry_visibility || {};
  const retryRows = safeList(retry.active_retries);
  const alerts = safeList(timeline.alerts);
  const jobs = safeList(timeline.scheduled_jobs);
  const diagnosticsSurfaceRows = [...safeList(timeline.diagnostics_only_events), ...safeList(timeline.hidden_events)];
  const infrastructureRows = [...safeList(timeline.infrastructure_events), ...safeList(timeline.diagnostic_events)]
    .map(runtimeProjectedEventToScheduleRow)
    .filter((row) => runtimeParseMs(row.scheduledTime));
  const diagnosticsRows = diagnosticsSurfaceRows;
  const escalationCount = diagnosticsRows.filter((row) => ["OVERDUE", "ESCALATED", "FAILED"].includes(String(row.escalation_state || row.escalationState || "").toUpperCase())).length;
  const clock = timeline.runtime_clock_semantics || {};
  return `<details class="runtime-diagnostics runtime-diagnostics-compact${escalationCount ? " runtime-diagnostics-escalated" : ""}" data-runtime-diagnostics>
      <summary>${escapeHtml(escalationCount ? `Show Diagnostics (${escalationCount} escalation detail${escalationCount === 1 ? "" : "s"})` : "Show Diagnostics")}</summary>
      <div class="runtime-diagnostics-grid">
        <div>
          <h4>Retry queue depth</h4>
          <p>${escapeHtml(String(retry.retry_queue_depth ?? 0))}</p>
          ${renderSimpleTable({
            columns: [
              { label: "Job", key: "job_id" },
              { label: "Status", render: (row) => renderStatusPill(readableStatus(row.status), timelineTone(row.status), {}) },
              { label: "Next retry", render: (row) => escapeHtml(runtimeOptionalTimestamp(row.next_retry_utc, timeline)) },
              { label: "Raw backoff", render: (row) => escapeHtml(row.backoff_state || "Not active") },
              { label: "Failure", render: (row) => escapeHtml(row.failure_reason || "None reported") },
            ],
            rows: retryRows,
            emptyMessage: "No active retries or queued remediation jobs.",
          })}
        </div>
        <div>
          <h4>Scheduler drift and alerts</h4>
          ${renderSimpleTable({
            columns: [
              { label: "Severity", render: (row) => renderStatusPill(row.severity || "INFO", String(row.severity || "").toUpperCase() === "HIGH" ? "blocked" : "warning", {}) },
              { label: "Type", key: "alert_type" },
              { label: "Message", key: "message" },
            ],
            rows: alerts,
            emptyMessage: "No scheduler drift, missed jobs, retry exhaustion, missing snapshots, or stage deadlocks.",
          })}
        </div>
        <div>
          <h4>Low-level scheduler fields</h4>
          ${renderSimpleTable({
            columns: [
              { label: "Job", render: (row) => escapeHtml(row.job_name || row.job_id || "Job") },
              { label: "Trigger", render: (row) => escapeHtml(readableStatus(row.trigger_type)) },
              { label: "Last run", render: (row) => escapeHtml(runtimeOptionalTimestamp(row.last_run_at_utc, timeline)) },
              { label: "Next run", render: (row) => escapeHtml(runtimeOptionalTimestamp(row.next_scheduled_run_utc, timeline)) },
              { label: "Lineage metadata", render: (row) => escapeHtml(row.source_artifact?.artifact_id || row.schedule_source || "Not reported") },
            ],
            rows: jobs,
            emptyMessage: "No scheduler jobs are available in diagnostics.",
          })}
        </div>
        <div>
          <h4>Internal Infrastructure Events</h4>
          ${renderSimpleTable({
            columns: [
              { label: "Event", render: (row) => escapeHtml(row.event || row.event_name || row.event_key || "Infrastructure event") },
              { label: "Time", render: (row) => escapeHtml(runtimeOptionalTimestamp(row.scheduledTime || row.scheduled_at_utc, timeline)) },
              { label: "Status", render: (row) => escapeHtml(runtimeHumanStatusForEvent(row, timeline)) },
              { label: "Owning job", render: (row) => escapeHtml(row.sourceJobId || row.owning_job || "Not reported") },
            ],
            rows: infrastructureRows,
            emptyMessage: "No internal refresh, retry, cache rebuild, or provider-sync jobs are available.",
          })}
        </div>
        <div>
          <h4>Schedule event ledger</h4>
          ${renderSimpleTable({
            columns: [
              { label: "Event", render: (row) => escapeHtml(row.event_name || row.event_key || "Runtime event") },
              { label: "Operator status", render: (row) => escapeHtml(row.operator_status || row.status || "Not reported") },
              { label: "Escalation", render: (row) => escapeHtml(row.escalation_state || row.escalationState || "NORMAL_WAIT") },
              { label: "Provider/feed", render: (row) => escapeHtml(row.provider_feed_name || row.providerFeedName || "Not reported") },
              { label: "Expected artifacts", render: (row) => escapeHtml(row.expected_artifact_type || "Not reported") },
              { label: "Missing artifacts", render: (row) => escapeHtml(safeList(row.missing_artifacts || row.missingArtifacts).join(", ") || "None") },
              { label: "Actual artifacts", render: (row) => escapeHtml(String((row.observed_artifact_ids || []).length)) },
              { label: "Retry count", render: (row) => escapeHtml(String(row.retry_count ?? 0)) },
              { label: "Retry exhaustion", render: (row) => escapeHtml(String(row.retry_exhaustion_threshold ?? row.retryExhaustionThreshold ?? "Not reported")) },
              { label: "Dependency age", render: (row) => escapeHtml(`${row.dependency_age_minutes ?? row.dependencyAgeMinutes ?? 0} min`) },
              { label: "SLA threshold", render: (row) => escapeHtml(`${row.provider_sla_minutes ?? row.providerSlaMinutes ?? "Not reported"} min`) },
              { label: "Escalation threshold", render: (row) => escapeHtml(`${row.escalation_threshold_minutes ?? row.escalationThresholdMinutes ?? "Not reported"} min`) },
              { label: "Next retry", render: (row) => escapeHtml(runtimeOptionalTimestamp(row.next_retry_utc || row.nextRetryUtc, timeline)) },
              { label: "Last successful artifact", render: (row) => escapeHtml(row.last_successful_artifact || row.lastSuccessfulArtifact || "Not reported") },
              { label: "Status reason", render: (row) => escapeHtml(row.status_reason || row.statusReason || "Not reported") },
              { label: "Owning job", render: (row) => escapeHtml(row.owning_job || row.sourceJobId || "Not reported") },
            ],
            rows: diagnosticsRows,
            emptyMessage: "No schedule event ledger rows are available.",
          })}
        </div>
        <div>
          <h4>Runtime clock fields</h4>
          ${renderDefinitionRows([
            { label: "Operator snapshot", value: runtimeOptionalTimestamp(clock.operator_snapshot_timestamp, timeline) },
            { label: "Market data", value: runtimeOptionalTimestamp(clock.market_data_timestamp, timeline) },
            { label: "Candidate snapshot", value: runtimeOptionalTimestamp(clock.candidate_snapshot_timestamp, timeline) },
            { label: "Certification", value: runtimeOptionalTimestamp(clock.certification_timestamp, timeline) },
          ])}
        </div>
      </div>
    </details>`;
}

function repairStatusTone(status = "") {
  const code = String(status || "").toUpperCase();
  if (["COMPLETED", "SUCCEEDED"].includes(code)) return "healthy";
  if (["QUEUED", "RUNNING", "VALIDATING", "RECERTIFYING"].includes(code)) return "warning";
  if (["FAILED"].includes(code)) return "blocked";
  if (["WAITING_FOR_INPUT", "OPEN"].includes(code)) return "warning";
  return "neutral";
}

function repairModeLabel(mode = "") {
  return String(mode || "").toLowerCase().replaceAll("_", " ").replace(/^\w/, (value) => value.toUpperCase()) || "Repair";
}

function renderRepairProgress(item = {}) {
  const stages = safeList(item.progress_stages);
  const status = String(item.status || "OPEN").toUpperCase();
  const activeIndex = status === "OPEN" || status === "WAITING_FOR_INPUT" ? 0 : status === "QUEUED" ? 0 : status === "RUNNING" ? 1 : status === "VALIDATING" ? 3 : status === "RECERTIFYING" ? 4 : 5;
  return `<div class="repair-progress" aria-label="Repair progress">
    ${stages.map((stage, index) => `<span class="repair-progress-step ${index < activeIndex ? "complete" : index === activeIndex ? "active" : "pending"}">${escapeHtml(stage)}</span>`).join("")}
  </div>`;
}

function renderRepairJobDialog(item = {}, panelId = "") {
  return `<dialog class="hypothesis-detail-modal repair-job-modal" id="${escapeHtml(panelId)}" data-command-detail-panel tabindex="-1">
    <form method="dialog">
      <button class="modal-close-button" value="close" aria-label="Close repair details">×</button>
      <section class="hypothesis-detail-panel">
        <div class="hypothesis-detail-header">
          <span class="section-eyebrow">REPAIR JOB</span>
          <h3>${escapeHtml(item.domain_id || "Repair item")}</h3>
          <p>${escapeHtml(item.plain_english_problem || item.reason || "Repair details")}</p>
        </div>
        <dl class="hypothesis-detail-facts">
          <div><dt>Repair ID</dt><dd>${escapeHtml(item.repair_id || "")}</dd></div>
          <div><dt>Status</dt><dd>${escapeHtml(item.status || "OPEN")}</dd></div>
          <div><dt>Job ID</dt><dd>${escapeHtml(item.job_id || "No job yet")}</dd></div>
          <div><dt>Audit ID</dt><dd>${escapeHtml(item.audit_id || "No audit yet")}</dd></div>
          <div><dt>Requested</dt><dd>${escapeHtml(item.requested_at || "Not requested yet")}</dd></div>
          <div><dt>Started</dt><dd>${escapeHtml(item.started_at || "Not started")}</dd></div>
          <div><dt>Completed</dt><dd>${escapeHtml(item.completed_at || "Not complete")}</dd></div>
          <div><dt>Result</dt><dd>${escapeHtml(item.result?.plain_english_result || item.result?.status_label || "No result yet")}</dd></div>
          <div><dt>Next step</dt><dd>${escapeHtml(item.next_step || "Review repair state.")}</dd></div>
        </dl>
      </section>
    </form>
  </dialog>`;
}

function renderRepairCommand(command = {}, className = "ghost-button", detailPanelId = "") {
  return renderCommandButton(command, className, { detailPanelId });
}

function renderRepairCard(item = {}) {
  const panelId = `repair-job-${String(item.repair_id || item.domain_id || "repair").replace(/[^a-zA-Z0-9_-]/g, "-")}`;
  const sourcePath = String(item.required_source?.path || "");
  const primary = item.primary_command || {};
  const secondary = safeList(item.secondary_commands).filter((command) => command.command_id && command.command_id !== primary.command_id);
  return `<article class="repair-center-card" data-command-surface data-repair-card data-repair-id="${escapeHtml(item.repair_id || "")}" data-repair-status="${escapeHtml(item.status || "OPEN")}">
    <div class="repair-center-card-header">
      <div>
        <span class="section-eyebrow">${escapeHtml(repairModeLabel(item.repair_mode))}</span>
        <h3>${escapeHtml(item.plain_english_problem || item.domain_id || "Repair item")}</h3>
      </div>
      ${renderStatusPill(item.status || "OPEN", repairStatusTone(item.status), {})}
    </div>
    <p class="repair-impact">${escapeHtml(item.impact || "No impact recorded.")}</p>
    <dl class="repair-card-facts">
      <div><dt>Domain</dt><dd>${escapeHtml(item.domain_id || "")}</dd></div>
      <div><dt>Issue</dt><dd>${escapeHtml(item.issue_type || item.reason || "")}</dd></div>
      <div><dt>Required source/artifact</dt><dd>${escapeHtml(item.required_artifact_source || "No source required")}</dd></div>
      ${item.source_config_key ? `<div><dt>Source config key</dt><dd>${escapeHtml(item.source_config_key)}</dd></div>` : ""}
      <div><dt>Next step</dt><dd>${escapeHtml(item.next_step || "Review repair guidance.")}</dd></div>
      <div><dt>Last attempt</dt><dd>${escapeHtml(item.last_attempt || item.requested_at || "No attempt recorded")}</dd></div>
      <div><dt>Next retry</dt><dd>${escapeHtml(item.next_retry || "Not scheduled")}</dd></div>
      <div><dt>Provider/feed</dt><dd>${escapeHtml(item.provider_feed_name || "Not provider-bound")}</dd></div>
      <div><dt>Escalation</dt><dd>${escapeHtml(item.timeout_escalation_threshold || "No escalation threshold reported")}</dd></div>
    </dl>
    ${sourcePath ? `<div class="repair-source-path"><code>${escapeHtml(sourcePath)}</code><button class="ghost-button command-result-copy-button" type="button" data-copy-text="${escapeHtml(sourcePath)}">Copy required path</button></div>` : ""}
    ${renderRepairProgress(item)}
    ${item.result && Object.keys(item.result).length ? renderRuntimeCommandResultPanel(item.result, { domain: item.domain_id }) : ""}
    <div class="repair-card-actions">
      ${renderRepairCommand(primary, "primary-button", panelId)}
      ${secondary.length ? `<details class="hypothesis-more-menu repair-more-menu"><summary>More</summary><div>${secondary.map((command) => renderRepairCommand(command, "ghost-button", panelId)).join("")}</div></details>` : ""}
      <p class="repair-command-status" data-aegis-command-status hidden></p>
    </div>
    ${renderRepairJobDialog(item, panelId)}
  </article>`;
}

function renderRepairSection(section = {}) {
  const items = safeList(section.items);
  return `<section class="repair-center-section" data-repair-section="${escapeHtml(section.section_id || "")}" aria-label="${escapeHtml(section.label || "Repair section")}">
    <div class="runtime-section-heading compact">
      <h2>${escapeHtml(section.label || "Repair section")}</h2>
      <p>${escapeHtml(String(section.count || items.length))} item${Number(section.count || items.length) === 1 ? "" : "s"}</p>
    </div>
    ${items.length ? `<div class="repair-center-grid">${items.map(renderRepairCard).join("")}</div>` : `<p class="runtime-empty-note">No repair items in this group.</p>`}
  </section>`;
}

function renderRepairCenterWorkspace(payload = {}) {
  const data = payload.data || payload;
  const summary = data.summary || {};
  const sections = safeList(data.sections);
  return `<section class="repair-center-workspace" data-repair-center-workspace>
    <div class="repair-center-hero">
      <div>
        <span class="section-eyebrow">REPAIR CENTER</span>
        <h1>Repair Center</h1>
        <p>Delayed domains, missing sources, provider waits, repair jobs, and recertification status.</p>
      </div>
      <button class="ghost-button" type="button" data-aegis-command-id="OPEN_VALID_ROUTE" data-aegis-command-action-type="EXPAND_SECTION" data-aegis-command-target-type="navigation" data-aegis-command-target-id="aegis_runtime_timeline" data-route="/aegis-runtime-timeline">Back to Runtime Timeline</button>
    </div>
    <div class="repair-summary-strip">
      ${renderMetricCard({ label: "Open", value: String(summary.total_open ?? 0), detail: "Repair items needing attention" })}
      ${renderMetricCard({ label: "Automatic", value: String(summary.automatic_available ?? 0), detail: "Aegis can queue repair" })}
      ${renderMetricCard({ label: "Source setup", value: String(summary.source_setup_required ?? 0), detail: "User/admin input required" })}
      ${renderMetricCard({ label: "Running", value: String(summary.repair_running ?? 0), detail: "Queued/running/validating" })}
      ${renderMetricCard({ label: "Failed", value: String(summary.repair_failed ?? 0), detail: "Needs review" })}
      ${renderMetricCard({ label: "Completed", value: String(summary.repair_completed ?? 0), detail: "Resolved repairs" })}
    </div>
    <div class="repair-center-sections">${sections.map(renderRepairSection).join("")}</div>
  </section>`;
}

async function renderAegisRepairCenterPage() {
  const payload = await fetchAegisRepairCenter().catch((error) => ({ ok: false, data: { sections: [], summary: {} }, error: error?.message || "Repair Center unavailable." }));
  return {
    title: "Repair Center",
    meta: "Repair delayed domains, missing sources, provider waits, failed jobs, and recertification status.",
    html: renderRepairCenterWorkspace(payload),
    contextHtml: "",
    hideContextRail: true,
  };
}

function renderAegisRuntimeTimelineWorkflow(payload = {}) {
  const timeline = runtimeTimelineProjection(payload);
  const eventGroups = runtimeBuildEventGroups(timeline);
  const schedule = [...eventGroups.needsAttention, ...eventGroups.upcoming];
  const body = [
    renderRuntimeNextEventHero(timeline, eventGroups),
    renderRuntimeDomainCertificationGrid(timeline),
    renderRuntimeNeedsAttention(timeline, eventGroups),
    renderRuntimeUpcoming(timeline, eventGroups),
    renderRuntimePipelineProgress(timeline, schedule),
    renderRuntimeCompletedToday(timeline, eventGroups),
    renderRuntimeDiagnostics(timeline),
  ];
  return {
    title: "Runtime Timeline",
    meta: "Simple runtime view: next event, check-again time, alerts, and capture recommendation timing.",
    html: body.join(""),
    contextHtml: "",
    hideContextRail: true,
    hideHeaderTimestamp: true,
  };
}

function renderAegisCandidatesWorkflow(payload = {}) {
  const rows = safeList(payload.current_day_candidate_rows || payload.current_day_candidates || payload.current_day_status?.candidate_rows);
  const today = payload.operator_today_projection || {};
  const actionRows = rows.filter((row) => ["MANUAL_IB_CAPTURE_READY", "SYSTEM_REPAIR_REQUIRED"].includes(String(row.operator_task_state || row.operator_affordance || "").toUpperCase()));
  const qualifiedRows = rows.filter((row) => String(row.analytical_state || "").toUpperCase() === "QUALIFIED" && !actionRows.includes(row));
  const blockedRows = rows.filter((row) => String(row.analytical_state || row.status || "").toUpperCase() === "BLOCKED" && !actionRows.includes(row));
  const suppressedRows = rows.filter((row) => String(row.analytical_state || row.status || "").toUpperCase() === "SUPPRESSED" && !actionRows.includes(row));
  const otherRows = rows.filter((row) => !actionRows.includes(row) && !qualifiedRows.includes(row) && !blockedRows.includes(row) && !suppressedRows.includes(row));
  const tableRows = [...actionRows, ...qualifiedRows, ...blockedRows, ...suppressedRows, ...otherRows];
  const analyticalCounts = today.analytical_state_counts || payload.current_day_status?.analytical_state_counts || {};
  const affordanceCounts = today.operator_task_state_counts || today.operator_affordance_counts || payload.current_day_status?.operator_task_state_counts || payload.current_day_status?.operator_affordance_counts || {};
  const executionCounts = today.execution_state_counts || payload.current_day_status?.execution_state_counts || {};
  const qualifiedCount = Number(analyticalCounts.QUALIFIED ?? qualifiedRows.length ?? 0);
  const reviewableCount = Number(analyticalCounts.REVIEWABLE ?? otherRows.filter((row) => String(row.analytical_state || "").toUpperCase() === "REVIEWABLE").length ?? 0);
  const blockedCount = Number(analyticalCounts.BLOCKED ?? blockedRows.length ?? 0);
  const suppressedCount = Number(analyticalCounts.SUPPRESSED ?? suppressedRows.length ?? 0);
  const actionAvailableCount = Number(today.user_task_available_count ?? today.operator_action_available_count ?? payload.current_day_status?.user_task_available_count ?? payload.current_day_status?.operator_action_available_count ?? actionRows.length ?? 0);
  const manualCaptureCount = Number(today.manual_ib_capture_ready_count ?? today.manual_capture_available_count ?? payload.current_day_status?.manual_ib_capture_ready_count ?? payload.current_day_status?.manual_capture_available_count ?? affordanceCounts.MANUAL_IB_CAPTURE_READY ?? 0);
  const systemRepairCount = Number(today.system_repair_required_count ?? payload.current_day_status?.system_repair_required_count ?? affordanceCounts.SYSTEM_REPAIR_REQUIRED ?? 0);
  const executionEligibleCount = Number(today.execution_eligible_row_count ?? payload.current_day_status?.execution_eligible_row_count ?? executionCounts.EXECUTION_ELIGIBLE ?? tableRows.filter((row) => row.execution_eligible === true).length);
  const executionLockedCount = Number(today.execution_locked_non_certified_count ?? payload.current_day_status?.execution_locked_non_certified_count ?? executionCounts.EXECUTION_LOCKED_NON_CERTIFIED ?? tableRows.filter((row) => String(row.execution_state || "").toUpperCase() === "EXECUTION_LOCKED_NON_CERTIFIED").length);
  const body = tableRows.length ? renderSimpleTable({
    columns: [
      { label: "Symbol", render: (row) => `<strong>${escapeHtml(row.symbol || "-")}</strong><div class="muted-mini">${escapeHtml(row.candidate_id || row.raw_intent_id || "candidate id unavailable")}</div>` },
      { label: "Sleeve", render: (row) => escapeHtml(row.sleeve_id || row.engine_id || "-") },
      { label: "Intent state", render: (row) => renderCandidateSemanticPill(row.intent_state || "DISCOVERED", "Intent lifecycle state derived from governed candidate, scoring, certification, and promotion evidence.") },
      { label: "Confidence", render: (row) => row.confidence_score === null || row.confidence_score === undefined ? "-" : `${Math.round(Number(row.confidence_score || 0) * 100)}%` },
      { label: "Stability", render: (row) => row.stability_score === null || row.stability_score === undefined ? "-" : `${Math.round(Number(row.stability_score || 0) * 100)}%` },
      { label: "Convergence", render: (row) => row.certification_convergence_score === null || row.certification_convergence_score === undefined ? "-" : `${Math.round(Number(row.certification_convergence_score || 0) * 100)}%` },
      { label: "Capture guidance", render: (row) => renderCandidateSemanticPill(row.capture_guidance || "NO_USER_ACTION", "Manual IB capture is recommended only after confidence, stability, and certification gates pass.") },
      { label: "Guidance reason", render: (row) => escapeHtml(row.capture_guidance_reason || row.next_action || "No user action.") },
      { label: "Certification", render: (row) => escapeHtml(row.certification_state || row.final_eod_certification_status || "not reported") },
      { label: "Score", render: (row) => row.portfolio_score_available ? escapeHtml(String(row.portfolio_score_total)) : `<span class="muted-mini">${escapeHtml(row.score_unavailable_reason || "score unavailable")}</span>` },
      { label: "Rank", render: (row) => row.portfolio_score_rank ? escapeHtml(String(row.portfolio_score_rank)) : "-" },
      { label: "Your task", render: (row) => escapeHtml(row.capture_guidance === "MANUAL_IB_CAPTURE_RECOMMENDED" ? "Record manual IB capture if already done externally" : "No manual IB capture recommendation") },
    ],
    rows: tableRows,
    emptyMessage: "No current-day candidate rows are available.",
  }) : renderWorkflowEmptyState({
    title: "No current-day candidates yet.",
    message: "Run the intraday sleeve workflow to populate candidate rows.",
    normality: "Normal before the first governed intraday run.",
    nextActions: ["TARGET_DAY=2026-05-21 npm run aegis:run-sleeves-now"],
  });
  return {
    title: "Current-Day Candidates",
    meta: "Today's intents, confidence, stability, certification convergence, and capture guidance.",
    html: [
      renderWorkflowCard({
        payload,
        sourceKey: "candidate_generation_manifest_v1",
        fieldPath: "current_day_candidate_rows",
        whyShown: "Candidate rows are projected from the governed current-day candidate generation manifest.",
        eyebrow: "CANDIDATE_BLOTTER",
        title: "Intent Pipeline",
        subtitle: "Selection is separate from confidence, stability, certification convergence, and manual IB capture guidance.",
        body: `
          <div class="operator-summary-strip operator-dashboard-summary">
            ${renderMetricCard({ label: "Operational day", value: today.current_runtime_day || payload.day_utc || "unknown" })}
            ${renderMetricCard({ label: "Operational mode", value: today.runtime_mode || payload.runtime_mode || "unknown" })}
            ${renderMetricCard({ label: "Qualified", value: String(qualifiedCount) })}
            ${renderMetricCard({ label: "Reviewable", value: String(reviewableCount) })}
            ${renderMetricCard({ label: "Your tasks", value: String(actionAvailableCount) })}
            ${renderMetricCard({ label: "IB recommendations", value: String((payload.intent_lifecycle_summary || payload.current_day_status?.intent_lifecycle_summary || {}).manual_ib_capture_recommended_count ?? 0) })}
            ${renderMetricCard({ label: "System repair", value: String(systemRepairCount) })}
            ${renderMetricCard({ label: "Execution eligible", value: String(executionEligibleCount) })}
            ${renderMetricCard({ label: "Locked non-certified", value: String(executionLockedCount) })}
            ${renderMetricCard({ label: "Blocked", value: String(blockedCount) })}
            ${renderMetricCard({ label: "Suppressed", value: String(suppressedCount) })}
          </div>
          ${body}
        `,
      }),
    ].join(""),
    contextHtml: "",
    hideContextRail: true,
  };
}

function renderSystemDomainCertificationPanel(payload = {}) {
  const report = payload.domain_certification || payload.runtime_timeline_projection?.domain_certification || {};
  const rows = safeList(report.domains);
  return renderWorkflowCard({
    payload,
    sourceKey: "domain_certification",
    fieldPath: "domain_certification.domains",
    whyShown: "Provider resilience is enforced by domain-level certification. One delayed domain blocks only dependent sleeves, not the whole platform.",
    eyebrow: "DOMAIN_CERTIFICATION",
    title: "Provider / Domain Status",
    subtitle: "Certified, partial, delayed, conflicted, and failed domains with affected sleeves.",
    body: renderSimpleTable({
      columns: [
        { label: "Domain", key: "domain_id" },
        { label: "Status", render: (row) => renderStatusPill(row.certification_status || "UNKNOWN", runtimeDomainTone(row.certification_status), DOMAIN_CERTIFICATION_SEMANTICS) },
        { label: "Completeness", render: (row) => escapeHtml(String(row.completeness_score ?? "")) },
        { label: "Risk", render: (row) => escapeHtml(row.single_provider_risk ? "Single provider" : row.quorum_state || "Compared") },
        { label: "Blocking sleeves", render: (row) => escapeHtml(safeList(row.blocking_sleeves).join(", ") || "None") },
      ],
      rows,
      emptyMessage: "No domain certification report is available.",
    }),
  });
}

function renderAegisReviewWorkflow(payload) {
  const { candidates, performance, sleeves, research, regime } = canonicalPayloadParts(payload);
  const advisoryQuality = performance.advisory_quality || {};
  const pendingRows = safeList(candidates.awaiting_outcome);
  const ignoredRows = safeList(candidates.ignored).filter((row) => row.outcome_status || row.outcome_metrics || row.outcome_window);
  const correctedRows = safeList(candidates.corrected);
  const approvedPendingRows = safeList(candidates.approved_or_traded).filter((row) => !row.outcome_status || ["OUTCOME_PENDING", "OUTCOME_UNKNOWN"].includes(String(row.outcome_status || "").toUpperCase()));
  const outcomeFollowups = [...pendingRows, ...approvedPendingRows];
  const metricRows = performanceMetricRows(advisoryQuality);
  const allMetricsInsufficient = metricRows.length > 0 && metricRows.every((row) => row.status === "INSUFFICIENT_DATA");
  const meaningfulMetricRows = metricRows.filter((row) => row.status === "OK" || Number(String(row.sample).split("/")[0] || 0) > 0);
  const attributionReadiness = attributionReadinessSummary(metricRows);
  const challengerRows = flattenCockpitSleeves(sleeves).filter((row) =>
    ["WATCH", "CHALLENGED", "DEGRADED", "SUSPENSION_REVIEW", "RETIREMENT_REVIEW", "INVESTIGATE", "MODIFY_RESEARCH"].includes(String(row.recommendation || "").toUpperCase()) ||
    ["watch", "challenged", "degraded"].includes(String(row.bucket || "").toLowerCase()),
  );
  const researchLessonRows = [
    ...safeList(research.new_tasks),
    ...safeList(research.review_required),
  ];
  const regimeLessonRows = [
    ...safeList(regime.sleeve_outcomes_by_regime),
    ...safeList(regime.candidate_outcomes_by_regime),
    ...safeList(regime.lessons),
  ];
  const eodHasSignal = [
    advisoryQuality.candidate_count,
    advisoryQuality.lifecycle_candidate_count,
    advisoryQuality.executed_count,
    advisoryQuality.ignored_candidate_count,
    advisoryQuality.expired_candidate_count,
  ].some((value) => Number(value || 0) > 0);
  const reviewHasSignal = outcomeFollowups.length || ignoredRows.length || correctedRows.length || meaningfulMetricRows.length || eodHasSignal || challengerRows.length || researchLessonRows.length || regimeLessonRows.length;
  const cards = [renderSystemDomainCertificationPanel(payload)];
  if (!reviewHasSignal && !metricRows.length) {
    cards.push(renderCardSection({
      eyebrow: "NO_REVIEW_SIGNAL",
      title: "No review work yet.",
      subtitle: "Review becomes useful after candidates are approved, ignored, expired, corrected, or assigned outcomes.",
      body: renderWorkflowEmptyState({
        title: "No review work yet.",
        message: "Review becomes useful after candidates are approved, ignored, expired, corrected, or assigned outcomes.",
        normality: "Good/normal before candidates and outcome windows exist.",
        nextActions: [
          "Review Opportunities for candidates.",
          "Record candidate decisions when available.",
          "npm run aegis:update-candidate-outcomes",
          "Run EOD after market close.",
        ],
      }),
    }));
    return {
      title: "Performance",
      meta: "What is actually working?",
      html: cards.join(""),
      contextHtml: workflowContextHtml(payload),
    };
  }
  cards.push(renderWorkflowCard({
    payload,
    sourceKey: "sleeve_performance_analytics",
    fieldPath: "performance.advisory_quality",
    whyShown: "Review summary is derived from canonical candidate, performance, sleeve, research, and regime sections.",
    eyebrow: "REVIEW_SUMMARY",
      title: "Performance Readiness",
      subtitle: performanceSummaryText({ reviewHasSignal, allMetricsInsufficient, attributionReadiness }),
    body: renderDefinitionRows([
      { label: "Outcome follow-up", value: outcomeFollowups.length ? `${outcomeFollowups.length} candidates need outcome follow-up` : "No outcome follow-up required." },
      { label: "Changed items", value: correctedRows.length || meaningfulMetricRows.length ? `${correctedRows.length} corrected decisions, ${meaningfulMetricRows.length} meaningful attribution metrics` : "No outcome or attribution changes yet." },
      { label: "Attribution readiness", value: attributionReadiness },
      { label: "Safety", value: "Review is read-only; no broker execution or autonomous execution." },
    ]),
  }));
  if (outcomeFollowups.length || ignoredRows.length) {
    cards.push(renderWorkflowCard({
      payload,
      sourceKey: "candidate_lifecycle",
      fieldPath: "candidates.awaiting_outcome",
      whyShown: "Pending outcomes need follow-up before attribution can learn from traded manual decisions.",
      eyebrow: "OUTCOMES",
      title: "Outcome Follow-up",
      subtitle: "Manual trade follow-up remains journaling/audit only.",
      body: renderCandidateStateTable([...outcomeFollowups, ...ignoredRows]),
    }));
  }
  if (correctedRows.length || meaningfulMetricRows.length) {
    cards.push(renderWorkflowCard({
      payload,
      sourceKey: correctedRows.length ? "candidate_lifecycle" : "sleeve_performance_analytics",
      fieldPath: correctedRows.length ? "candidates.corrected" : "performance.advisory_quality",
      whyShown: "Changed items are copied from Candidate Lifecycle and Performance Attribution. The UI does not recalculate them.",
      eyebrow: "WHAT_CHANGED",
      title: "Outcome / Attribution Changes",
      subtitle: "Outcome, correction, and attribution changes that matter for review.",
      body: [
        correctedRows.length ? renderCandidateStateTable(correctedRows) : `<p>No corrected decisions.</p>`,
        meaningfulMetricRows.length ? renderSimpleTable({
          columns: [
            { label: "Metric", key: "metric" },
            { label: "Status", key: "status" },
            { label: "Sample", key: "sample" },
            { label: "Confidence", key: "confidence" },
          ],
          rows: meaningfulMetricRows.slice(0, 8),
          emptyMessage: "No meaningful attribution changes yet.",
        }) : `<p>No outcome or attribution changes yet.</p>`,
      ].join(""),
    }));
  } else {
    cards.push(renderWorkflowCard({
      payload,
      sourceKey: "candidate_lifecycle",
      fieldPath: "candidates",
      whyShown: "A compact change statement is shown because no outcome, correction, or meaningful attribution change exists.",
      eyebrow: "WHAT_CHANGED",
      title: "What Changed",
      subtitle: "No outcome or attribution changes yet.",
      body: `<p>No outcome or attribution changes yet.</p>`,
    }));
  }
  if (meaningfulMetricRows.length || challengerRows.length || researchLessonRows.length || regimeLessonRows.length) {
    cards.push(renderWorkflowCard({
      payload,
      sourceKey: challengerRows.length ? "sleeve_challenger" : "sleeve_performance_analytics",
      fieldPath: "review.lessons_learned",
      whyShown: "Lessons are shown only when canonical attribution, sleeve challenger, research, or regime sections contain meaningful signal.",
      eyebrow: "LESSONS",
      title: "What We Learned",
      subtitle: "Findings that can improve future sleeve and research decisions.",
      body: reviewLessonsHtml({ meaningfulMetricRows, challengerRows, researchLessonRows, regimeLessonRows }),
    }));
  } else if (allMetricsInsufficient) {
    cards.push(renderWorkflowCard({
      payload,
      sourceKey: "sleeve_performance_analytics",
      fieldPath: "performance.advisory_quality",
      whyShown: "Performance Attribution exists, but sample sizes are too small for useful interpretation.",
      eyebrow: "LESSONS_PENDING",
      title: "What We Learned",
      subtitle: "Not enough candidate outcome history yet for meaningful attribution review.",
      body: `<div class="callout warning">${escapeHtml(`Attribution metrics are initialized but not statistically meaningful yet. Current sample size is ${attributionReadiness}.`)}</div>`,
    }));
  }
  if (eodHasSignal) {
    cards.push(renderWorkflowCard({
      payload,
      sourceKey: "advisory_quality",
      fieldPath: "performance.advisory_quality",
      whyShown: "EOD/EOW and advisory quality follow-up are reviewed from canonical performance summaries.",
      eyebrow: "EOD_EOW",
      title: "EOD / EOW Review Items",
      subtitle: "Review uses canonical performance/advisory quality pointers and does not parse raw EOD/EOW reports.",
      body: renderDefinitionRows([
        { label: "Generated candidates", value: String(advisoryQuality.candidate_count ?? advisoryQuality.lifecycle_candidate_count ?? 0) },
        { label: "Executed/captured", value: String(advisoryQuality.executed_count ?? 0) },
        { label: "Ignored", value: String(advisoryQuality.ignored_candidate_count ?? 0) },
        { label: "Expired", value: String(advisoryQuality.expired_candidate_count ?? 0) },
      ]),
    }));
  }
  if (metricRows.length) {
    cards.push(renderWorkflowCard({
      payload,
      sourceKey: "sleeve_performance_analytics",
      fieldPath: "performance.advisory_quality",
      whyShown: "Raw metric scaffolding is available as advanced drilldown, not primary operator review content.",
      eyebrow: "ADVANCED",
      title: "Advanced Metrics",
      subtitle: "Collapsed by default. Includes raw attribution metric status and sample-size limits.",
      body: `<details><summary>Show ${metricRows.length} advanced attribution metrics</summary>${renderSimpleTable({
        columns: [
          { label: "Metric", key: "metric" },
          { label: "Status", key: "status" },
          { label: "Sample", key: "sample" },
          { label: "Confidence", key: "confidence" },
        ],
        rows: metricRows.slice(0, 20),
        emptyMessage: "No attribution metrics are available.",
      })}</details>`,
    }));
  }
  return {
    title: "Performance",
    meta: "What is actually working?",
    html: cards.join(""),
    contextHtml: workflowContextHtml(payload),
  };
}

function performanceSummaryText({ reviewHasSignal, allMetricsInsufficient, attributionReadiness } = {}) {
  if (reviewHasSignal) {
    return "Performance review items require attention.";
  }
  if (allMetricsInsufficient) {
    return `Attribution is not meaningful yet because outcome history is insufficient. Current sample size is ${attributionReadiness}.`;
  }
  return "No review work yet.";
}

function attributionReadinessSummary(metricRows = []) {
  const rows = safeList(metricRows);
  if (!rows.length) {
    return "No attribution metrics initialized.";
  }
  const samplePairs = rows
    .map((row) => {
      const parts = String(row.sample || "0/0").split("/");
      return {
        sample: Number(parts[0] || 0),
        minimum: Number(parts[1] || 0),
      };
    })
    .filter((row) => Number.isFinite(row.sample) && Number.isFinite(row.minimum));
  if (!samplePairs.length) {
    return "sample size unknown";
  }
  const maxSample = Math.max(...samplePairs.map((row) => row.sample));
  const minRequired = Math.max(...samplePairs.map((row) => row.minimum));
  return `${maxSample}/${minRequired}`;
}

function reviewLessonsHtml({ meaningfulMetricRows = [], challengerRows = [], researchLessonRows = [], regimeLessonRows = [] } = {}) {
  const sections = [];
  if (meaningfulMetricRows.length) {
    sections.push(renderSimpleTable({
      columns: [
        { label: "Metric", key: "metric" },
        { label: "Status", key: "status" },
        { label: "Sample", key: "sample" },
        { label: "Confidence", key: "confidence" },
      ],
      rows: meaningfulMetricRows.slice(0, 8),
      emptyMessage: "No meaningful attribution findings.",
    }));
  }
  if (challengerRows.length) {
    sections.push(renderSimpleTable({
      columns: [
        { label: "Bucket", key: "bucket" },
        { label: "Sleeve", key: "sleeve_id" },
        { label: "Recommendation", key: "recommendation" },
        { label: "Confidence", key: "confidence" },
      ],
      rows: challengerRows.slice(0, 8),
      emptyMessage: "No sleeve challenger findings.",
    }));
  }
  if (researchLessonRows.length) {
    sections.push(renderResearchTaskTable(researchLessonRows));
  }
  if (regimeLessonRows.length) {
    sections.push(renderSimpleTable({
      columns: [
        { label: "Regime", key: "regime" },
        { label: "Sleeve", key: "sleeve_id" },
        { label: "Candidate", key: "candidate_id" },
        { label: "Status", key: "status" },
      ],
      rows: regimeLessonRows.slice(0, 8),
      emptyMessage: "No regime outcome lessons.",
    }));
  }
  return sections.length ? sections.join("") : `<p>Not enough candidate outcome history yet for meaningful attribution review.</p>`;
}

function renderAegisResearchWorkflow(payload) {
  const consolePayload = payload.research_lab_command_sources?.researchConsole || payload.research_console || {};
  return {
    title: "Hypotheses",
    meta: "See hypotheses, start AI research, monitor progress, review findings, and check recommendation readiness.",
    html: renderHypothesesWorkspace(consolePayload, {}),
    contextHtml: "",
    hideContextRail: true,
  };
}

function renderEdgeLabCommandHeader({ payload = {}, projection = {}, sources = {} }) {
  const chain = projection?.evidence_chain || {};
  const latest = chain.latest_summary || {};
  const drift = sources.expectancyDrift || {};
  const fragility = sources.regimeFragility || {};
  const stability = sources.sleeveStability || {};
  const challenger = latestResearchRow(sources.challengerComparison, ["challenger_comparison_reports", "comparison_reports", "reports"]);
  const answer = edgeLabAnswerText({ projection, sources });
  return renderWorkflowCard({
    payload,
    sourceKey: "research_lab_projection",
    fieldPath: "edge_lab.command_header",
    whyShown: "The command header answers whether the operator needs to do anything and compresses sleeve state into plain-English lifecycle badges.",
    eyebrow: "RESEARCH_COMMAND_WORKSPACE",
    title: "Do I need to do anything?",
    subtitle: "Research Command Workspace answer box. It shows only human attention state; Aegis continues background monitoring.",
    body: `
      <div class="edge-answer-box">
        <span>Answer</span>
        <strong>${escapeHtml(answer)}</strong>
      </div>
      <div class="edge-command-header">
        ${edgeHeaderChip("Sleeve", projection?.title || latest.sleeve_name || projection?.sleeve_id || chain.sleeve_id || "unknown")}
        ${edgeHeaderChip("Health", edgePlainStatus(projection?.health || chain.overall_health, "Monitoring"))}
        ${edgeHeaderChip("Drift", edgePlainStatus(drift.expectancy_drift_status || drift.drift_status || projection?.drift_status, "Monitoring"))}
        ${edgeHeaderChip("Fragility", edgePlainStatus(fragility.regime_fragility_status || fragility.fragility_status || projection?.fragility_status, "Monitoring"))}
        ${edgeHeaderChip("Paper trial", edgePlainStatus(projection?.paper_trial_status || chain.paper_trial_status, "Waiting for More Data"))}
        ${edgeHeaderChip("Challenger", challenger && Object.keys(challenger).length ? "Needs Review" : edgePlainStatus(projection?.challenge_type || chain.challenge_type, "Monitoring"))}
        ${edgeHeaderChip("Evidence", edgeEvidenceStatus(projection?.evidence_quality || chain.evidence_quality))}
        ${edgeHeaderChip("Next", edgeNextHumanAction({ projection, sources }))}
      </div>
      <div class="compact-safety-strip">READ-ONLY GOVERNANCE · NO BROKER EXECUTION · MANUAL REVIEW REQUIRED <span class="muted-mini">READ_ONLY_GOVERNANCE · NO_BROKER_EXECUTION · MANUAL_REVIEW_REQUIRED</span></div>
    `,
  });
}

function edgeHeaderChip(label, value) {
  return `<div class="edge-command-chip"><span>${escapeHtml(label)}</span><strong>${escapeHtml(String(value || "not reported"))}</strong></div>`;
}

function latestResearchRow(payload = {}, keys = []) {
  if (!payload || typeof payload !== "object") return {};
  for (const key of keys) {
    const rows = safeList(payload[key]);
    if (rows.length) return rows[0] || {};
  }
  if (payload.report && typeof payload.report === "object") return payload.report;
  if (payload.dossier && typeof payload.dossier === "object") return payload.dossier;
  if (payload.challenger_comparison_report && typeof payload.challenger_comparison_report === "object") return payload.challenger_comparison_report;
  return {};
}

function edgePlainStatus(value, fallback = "No Action Needed") {
  const raw = String(value || "").trim().toLowerCase();
  if (!raw) return fallback;
  if (raw.includes("block") || raw.includes("missing") || raw.includes("fail") || raw.includes("error")) return "Blocked";
  if (raw.includes("paper") && raw.includes("active")) return "Paper Trial Active";
  if (raw === "active") return "Paper Trial Active";
  if (raw.includes("dossier") || raw.includes("human") || raw.includes("review")) return "Dossier Ready";
  if (raw.includes("watch") || raw.includes("challeng") || raw.includes("degrad") || raw.includes("underperform") || raw.includes("fragil") || raw.includes("drift")) return "Needs Review";
  if (raw.includes("pending") || raw.includes("insufficient") || raw.includes("waiting") || raw.includes("zero") || raw.includes("more data")) return "Waiting for More Data";
  if (raw.includes("monitor") || raw.includes("continue") || raw.includes("ready") || raw.includes("generated") || raw.includes("pass") || raw.includes("moderate") || raw.includes("preliminary")) return "Monitoring";
  return fallback;
}

function edgeEvidenceStatus(value) {
  const raw = String(value || "").trim().toLowerCase();
  if (!raw) return "Monitoring";
  if (raw.includes("insufficient")) return "Waiting for More Data";
  if (raw.includes("missing") || raw.includes("fail")) return "Blocked";
  return "Monitoring";
}

function edgeLabAnswerText({ projection = {}, sources = {} }) {
  const chain = projection?.evidence_chain || {};
  const hasDossier = Object.keys(latestResearchRow(sources.humanReviewDossiers, ["human_review_dossiers", "dossiers"])).length > 0;
  const hasMissing = safeList(chain.missing_artifacts || projection?.missing_artifacts).length > 0;
  const paperActive = String(projection?.paper_trial_status || chain.paper_trial_status || "").toLowerCase() === "active";
  const driftNeedsReview = edgePlainStatus(sources.expectancyDrift?.expectancy_drift_status || sources.expectancyDrift?.drift_status, "Monitoring") === "Needs Review";
  const fragilityNeedsReview = edgePlainStatus(sources.regimeFragility?.regime_fragility_status || sources.regimeFragility?.fragility_status, "Monitoring") === "Needs Review";
  if (hasMissing) return "Research needs your attention.";
  if (hasDossier && paperActive) return "Review required: open human review dossier and continue paper-trial observation.";
  if (hasDossier) return "Open human review dossier.";
  if (paperActive) return "Record paper observation.";
  if (driftNeedsReview || fragilityNeedsReview) return "Review required: degrading sleeve evidence needs attention.";
  return "No action needed. Aegis continues monitoring.";
}

function edgeNextHumanAction({ projection = {}, sources = {} }) {
  const answer = edgeLabAnswerText({ projection, sources });
  if (answer.startsWith("No action needed")) return "No Action Needed";
  if (answer.includes("blocked evidence")) return "Investigate Blocked Evidence";
  if (answer.includes("paper observation")) return "Record Paper Observation";
  if (answer.includes("dossier")) return "Review Dossier";
  if (answer.includes("degrading")) return "Review Degrading Sleeve";
  return "Decide Whether To Continue Observation";
}

function renderEdgeLabWhatShouldIDo({ payload = {}, projection = {}, sources = {}, research = {}, edgeLab = {} }) {
  const chain = projection?.evidence_chain || {};
  const dossier = latestResearchRow(sources.humanReviewDossiers, ["human_review_dossiers", "dossiers"]);
  const drift = sources.expectancyDrift || {};
  const fragility = sources.regimeFragility || {};
  const stability = sources.sleeveStability || {};
  const paperInventory = sources.paperTrialInventory || {};
  const missing = safeList(chain.missing_artifacts || projection?.missing_artifacts);
  const focusRows = safeList(edgeLab.recommended_focus_today || research.recommended_focus_today);
  const focusTasks = focusRows.map((row) => ({
    title: row.title || row.hypothesis_id || "Research hypothesis",
    reason: `${row.tier || "Priority"} · attention ${row.attention_score ?? "n/a"} · ${row.blocker_summary || row.recommended_action || "Review priority"}`,
    source: row.hypothesis_id || "hypothesis_priority_report_v1",
    action: row.operator_attention_required ? "Review now" : "Open priority report",
    href: "/api/research-lab/research-backlog-priority",
    governance: "Research prioritization only; no trade advice or execution.",
  }));
  const tasks = [
    ...focusTasks,
    dossier && Object.keys(dossier).length ? {
      title: "Review Dossier",
      reason: dossier.reason || dossier.review_reason || "Human review dossier is available for this sleeve.",
      source: dossier.human_review_dossier_id || dossier.dossier_id || "human_review_dossier",
      action: "Review Dossier",
      href: `/api/research-lab/human-review-dossiers/${encodeURIComponent(String(dossier.human_review_dossier_id || dossier.dossier_id || ""))}`,
      governance: "Human review only; no automatic promotion.",
    } : null,
    ["Needs Review", "Blocked"].includes(edgePlainStatus(stability.overall_stability_status || stability.sleeve_stability_status || drift.expectancy_drift_status || fragility.regime_fragility_status, "Monitoring")) ? {
      title: "Review Degrading Sleeve",
      reason: stability.summary || drift.summary || fragility.summary || "Sleeve evidence needs human review.",
      source: stability.sleeve_stability_report_id || drift.expectancy_drift_report_id || fragility.regime_fragility_report_id || "sleeve_stability_latest",
      action: "Review Degrading Sleeve",
      href: "/api/research-lab/stability/sleeve-stability/latest",
      governance: "Review only; no sleeve change.",
    } : null,
    String(projection?.paper_trial_status || chain.paper_trial_status || "").toLowerCase() === "active" ? {
      title: "Record Paper Observation",
      reason: "Paper trial is active and needs repeated observation records.",
      source: safeList(chain.paper_trial_ids)[0] || "paper_trial",
      action: "Record Paper Observation",
      href: "/api/research-lab/paper-trial-inventory",
      governance: "Observation only; no trade, no order, no achieved performance claim.",
    } : null,
    missing.length ? {
      title: "Investigate Blocked Evidence",
      reason: `${missing.length} missing or incomplete research artifact(s).`,
      source: missing[0]?.path || "research_store",
      action: "Open Integrity Details",
      href: "/api/research-lab/health",
      governance: "Fail closed; do not fabricate missing evidence.",
    } : null,
    {
      title: "Decide Whether To Continue Observation",
      reason: projection?.recommended_action || chain.recommended_action || latestResearchRow(paperInventory, ["priority_rows", "trials"])?.recommended_next_action || "Continue research observation until evidence improves.",
      source: "edge_lab_projection",
      action: "Continue Observation",
      href: "/api/research-lab/research-backlog-priority",
      governance: "Manual review required before any lifecycle change.",
    },
  ].filter(Boolean);
  const focusSummary = focusRows.length ? `<div class="edge-answer-box"><span>Recommended focus today</span><strong>${escapeHtml(focusRows.slice(0, 5).map((row, index) => `${index + 1}. ${row.title || row.hypothesis_id || "Research hypothesis"}`).join("  "))}</strong></div>` : "";
  return renderWorkflowCard({
    payload,
    sourceKey: "research_lab_tasks",
    fieldPath: "edge_lab.what_should_i_do",
    whyShown: "This is the primary Research Pipeline attention queue: blocked ideas, stale evidence, failed trials, approvals, and manual review items surface here first.",
    eyebrow: "NEEDS_ATTENTION",
    title: "What deserves attention now",
    subtitle: "Tier 1, blocked high-priority, paper-trial, IB-ticket, and operator-decision items only. Everything else stays below.",
    body: [
      focusSummary,
      renderSimpleTable({
        columns: [
          { label: "Task", render: (row) => `<strong>${escapeHtml(row.title)}</strong>` },
          { label: "Reason", key: "reason" },
          { label: "Source", key: "source" },
          { label: "Allowed action", render: (row) => row.href ? `<a class="primary-button research-action-button" href="${escapeHtml(row.href)}" target="_blank" rel="noopener noreferrer">${escapeHtml(row.action)}</a>` : `<button class="primary-button research-action-button" type="button">${escapeHtml(row.action)}</button>` },
          { label: "Governance", key: "governance" },
        ],
        rows: tasks,
        emptyMessage: "No action needed. Aegis continues monitoring.",
      }),
    ].join(""),
  });
}

function renderEdgeLabAutomaticPipelineStatus({ payload = {}, projection = {}, sources = {}, research = {}, edgeLab = {} }) {
  const chain = projection?.evidence_chain || {};
  const challenger = latestResearchRow(sources.challengerComparison, ["challenger_comparison_reports", "comparison_reports", "reports"]);
  const dossier = latestResearchRow(sources.humanReviewDossiers, ["human_review_dossiers", "dossiers"]);
  const rows = [
    { lifecycle: "Hypothesis active", status: safeList(research.active_hypotheses || research.captured_hypotheses).length || projection?.sleeve_id ? "Monitoring" : "Waiting for More Data", plain: "Research is already active. Aegis is monitoring this hypothesis. No manual research start is needed." },
    { lifecycle: "Evidence generated", status: chain.event_study_evidence_id || chain.backtest_evidence_id ? "Monitoring" : "Waiting for More Data", plain: chain.event_study_evidence_id || chain.backtest_evidence_id ? "Aegis has generated research evidence and keeps the source artifacts linked." : "Aegis is waiting for enough evidence artifacts." },
    { lifecycle: "Paper trial active", status: edgePlainStatus(projection?.paper_trial_status || chain.paper_trial_status, "Waiting for More Data"), plain: String(projection?.paper_trial_status || chain.paper_trial_status || "").toLowerCase() === "active" ? "Aegis is collecting forward observations. David only needs to record observations when prompted." : "No active paper trial requires action right now." },
    { lifecycle: "Drift monitored", status: edgePlainStatus(sources.expectancyDrift?.expectancy_drift_status || sources.expectancyDrift?.drift_status, "Monitoring"), plain: "Aegis watches expectancy drift and surfaces degradation only when human review is needed." },
    { lifecycle: "Challenger comparison ready", status: challenger && Object.keys(challenger).length ? "Needs Review" : "Monitoring", plain: challenger && Object.keys(challenger).length ? "A challenger comparison exists for review; it does not change sleeves automatically." : "Aegis is monitoring challengers. No manual comparison is needed." },
    { lifecycle: "Human review prepared", status: dossier && Object.keys(dossier).length ? "Dossier Ready" : "No Action Needed", plain: dossier && Object.keys(dossier).length ? "A human review dossier is ready for David." : "No action needed. Aegis continues monitoring." },
  ];
  return renderWorkflowCard({
    payload,
    sourceKey: "research_lab_lifecycle",
    fieldPath: "edge_lab.automatic_pipeline_status",
    whyShown: "This section separates Aegis background research lifecycle work from the human attention queue.",
    eyebrow: "AUTOMATIC_PIPELINE",
    title: "Background research activity",
    subtitle: "Plain-English status for monitored research processes. No operator action is needed unless surfaced above.",
    body: renderSimpleTable({
      columns: [
        { label: "Lifecycle", key: "lifecycle" },
        { label: "Status", render: (row) => renderStatusPill(row.status, row.status === "Blocked" ? "blocked" : row.status === "Needs Review" || row.status === "Dossier Ready" ? "warn" : "healthy", {}) },
        { label: "Plain English", key: "plain" },
      ],
      rows,
      emptyMessage: "No action needed. Aegis continues monitoring.",
    }),
  });
}

function renderEdgeLabResearchDetails({
  payload = {},
  research = {},
  edgeLab = {},
  sleeves = {},
  projection = {},
  sources = {},
  capturedRows = [],
  activeRows = [],
  validatedRows = [],
  pipeline = {},
  pipelineCounts = {},
}) {
  const chain = projection?.evidence_chain || {};
  const pipelineRows = Object.values(pipeline || {}).flatMap((items) => safeList(items));
  const hypothesisRows = safeList(activeRows).length
    ? safeList(activeRows)
    : safeList(capturedRows).length
      ? safeList(capturedRows)
      : pipelineRows;
  const evidenceIds = [
    { label: "Event study evidence", value: projection?.event_study_evidence_id || chain.event_study_evidence_id || "No action needed. Aegis continues monitoring." },
    { label: "Backtest evidence", value: projection?.backtest_evidence_id || chain.backtest_evidence_id || "No action needed. Aegis continues monitoring." },
    { label: "Longitudinal run", value: safeList(chain.longitudinal_run_ids)[0] || "No action needed. Aegis continues monitoring." },
    { label: "Paper trial", value: safeList(chain.paper_trial_ids)[0] || "No action needed. Aegis continues monitoring." },
  ];
  return renderWorkflowCard({
    payload,
    sourceKey: "research_details",
    fieldPath: "edge_lab.research_details",
    whyShown: "Research details are available for audit and advanced review, but they are collapsed so the primary page stays focused on human attention.",
    eyebrow: "ADVANCED_DIAGNOSTICS",
    title: "Advanced diagnostics",
    subtitle: "Collapsed by default. Includes evidence ids, artifact references, raw metadata, and internal research operations.",
    body: `
      <details class="support-note">
        <summary>Advanced diagnostics</summary>
        <h3>Hypotheses</h3>
        ${renderPlainEnglishHypothesisTable(hypothesisRows, { projection, sources })}
        <details class="support-note">
          <summary>Evidence ids</summary>
          ${renderDefinitionRows(evidenceIds)}
        </details>
        ${renderAdvancedResearchOperations()}
        <details class="support-note">
          <summary>Challenger Comparison</summary>
          ${renderChallengerComparisonPanel({ payload, projection, sources })}
        </details>
        <details class="support-note">
          <summary>Human Review Workspace</summary>
          ${renderHumanReviewWorkspaceCard({ payload, research, projection, sources })}
        </details>
        <details class="support-note">
          <summary>Evidence Intelligence Panels</summary>
          ${renderEvidenceIntelligencePanels({ payload, research, sleeves, projection, sources })}
        </details>
        <details class="support-note">
          <summary>What Changed</summary>
          ${renderEdgeLabWhatChanged({ payload, projection, sources, sleeves })}
        </details>
        ${Object.values(pipeline || {}).some((rows) => safeList(rows).length) ? `<details class="support-note"><summary>Hypothesis pipeline gates</summary>${renderResearchPipelineKanban(pipeline, pipelineCounts)}</details>` : ""}
        ${projection ? renderResearchLabEvidenceProjectionCard(projection) : ""}
      </details>
    `,
  });
}

function renderPlainEnglishHypothesisTable(rows = [], { projection = {}, sources = {} } = {}) {
  const safeRows = safeList(rows);
  const chain = projection?.evidence_chain || {};
  const fallbackRow = {
    hypothesis_id: projection?.hypothesis_id || chain.hypothesis_id || projection?.sleeve_id || "research_hypothesis",
    readable_title: projection?.title || "Active research hypothesis",
  };
  const displayRows = safeRows.length ? safeRows : [fallbackRow];
  const nextAction = edgeNextHumanAction({ projection, sources });
  const attention = nextAction === "No Action Needed"
    ? "No action needed. Aegis continues monitoring."
    : nextAction;
  return renderSimpleTable({
    columns: [
      { label: "Hypothesis", render: (row) => `<strong>${escapeHtml(row.readable_title || row.title || row.hypothesis_id || row.id || "Research hypothesis")}</strong><div class="muted-mini">${escapeHtml(row.hypothesis_id || row.id || "")}</div>` },
      { label: "Current Status", render: () => edgePlainStatus(projection?.paper_trial_status || projection?.health, "Monitoring") },
      { label: "What Aegis Has Done", render: () => chain.event_study_evidence_id || chain.backtest_evidence_id ? "Generated evidence and monitoring outcomes" : "Research is already active. Aegis is monitoring this hypothesis. No manual research start is needed." },
      { label: "What Needs Attention", render: () => attention },
      { label: "Next Human Action", render: () => nextAction === "No Action Needed" ? "No action needed. Aegis continues monitoring." : nextAction },
    ],
    rows: displayRows,
    emptyMessage: "No action needed. Aegis continues monitoring.",
  });
}

function renderAdvancedResearchOperations() {
  const actions = [
    "Open Research",
    "Generate Evidence",
    "Run Event Study",
    "Create Challenger",
    "Compare Challengers",
    "Prepare Human Review",
    "Archive Hypothesis",
  ];
  return `
    <details class="support-note">
      <summary>Advanced research operations — normally not needed.</summary>
      <p class="muted-mini">These are internal research-engine operations. The main Research Pipeline view only shows what needs human attention.</p>
      <div class="research-action-toolbar">${actions.map((action) => `<button class="ghost-button research-action-button" type="button" title="Advanced governed research action">${escapeHtml(action)}</button>`).join("")}</div>
    </details>
  `;
}

function renderEdgeLabWhatChanged({ payload = {}, projection = {}, sources = {}, sleeves = {} }) {
  const chain = projection?.evidence_chain || {};
  const changes = [];
  const challenger = latestResearchRow(sources.challengerComparison, ["challenger_comparison_reports", "comparison_reports", "reports"]);
  const dossier = latestResearchRow(sources.humanReviewDossiers, ["human_review_dossiers", "dossiers"]);
  if (challenger && Object.keys(challenger).length) changes.push({ change: "new challenger comparison ready", detail: challenger.challenger_comparison_report_id || "challenger comparison available" });
  if (dossier && Object.keys(dossier).length) changes.push({ change: "human review dossier ready", detail: dossier.human_review_dossier_id || dossier.reason || "dossier available" });
  if (sources.sleeveStability?.overall_stability_status || sources.sleeveStability?.sleeve_stability_status) changes.push({ change: "sleeve stability status", detail: sources.sleeveStability.overall_stability_status || sources.sleeveStability.sleeve_stability_status });
  if (sources.expectancyDrift?.expectancy_drift_status || sources.expectancyDrift?.drift_status) changes.push({ change: "expectancy drift status", detail: sources.expectancyDrift.expectancy_drift_status || sources.expectancyDrift.drift_status });
  if (sources.regimeFragility?.regime_fragility_status || sources.regimeFragility?.fragility_status) changes.push({ change: "regime fragility watch", detail: sources.regimeFragility.regime_fragility_status || sources.regimeFragility.fragility_status });
  if (String(projection?.paper_trial_status || chain.paper_trial_status || "").toLowerCase() === "active") changes.push({ change: "paper trial active", detail: safeList(chain.paper_trial_ids).join(", ") || "active paper trial" });
  flattenCockpitSleeves(sleeves).filter((row) => String(row.bucket || row.recommendation || "").toUpperCase().includes("BLOCK")).forEach((row) => changes.push({ change: "blocked sleeve", detail: row.sleeve_id || row.reason || "blocked" }));
  return renderWorkflowCard({
    payload,
    sourceKey: "research_lab_changes",
    fieldPath: "edge_lab.what_changed",
    whyShown: "Recent meaningful changes are surfaced above raw artifact metadata so operators can see what needs attention.",
    eyebrow: "WHAT_CHANGED",
    title: "What Changed",
    subtitle: "Challengers, human-review dossiers, stability, drift, fragility, paper-trial state, and blocked evidence.",
    body: renderSimpleTable({
      columns: [
        { label: "Change", key: "change" },
        { label: "Evidence / reason", key: "detail" },
      ],
      rows: changes,
      emptyMessage: "No meaningful research changes were reported.",
    }),
  });
}

function renderChallengerComparisonPanel({ payload = {}, projection = {}, sources = {} }) {
  const comparison = sources.challengerComparison || {};
  const report = latestResearchRow(comparison, ["challenger_comparison_reports", "comparison_reports", "reports"]);
  const rows = safeList(report?.comparison_rows || report?.challengers || comparison.comparison_rows || comparison.challengers).slice(0, 3);
  return renderWorkflowCard({
    payload,
    sourceKey: "challenger_comparison_report",
    fieldPath: "research_lab.challenger_comparison",
    whyShown: "Challenger comparison is the most actionable research review artifact when a sleeve is on watch or challenged.",
    eyebrow: "CHALLENGER_COMPARISON",
    title: "Challenger Comparison",
    subtitle: "Incumbent versus deterministic challenger ranking. Research evidence only.",
    body: [
      renderDefinitionRows([
        { label: "Incumbent sleeve", value: report?.incumbent_sleeve_id || projection?.sleeve_id || "not reported" },
        { label: "Top research review candidate", value: report?.top_research_review_candidate_id || report?.top_challenger_id || "not reported" },
        { label: "Active challengers", value: String(report?.active_challenger_count ?? safeList(report?.active_challengers).length ?? rows.length) },
        { label: "Excluded challengers", value: String(report?.excluded_challenger_count ?? safeList(report?.excluded_challengers).length ?? 0) },
        { label: "Evidence sufficiency", value: report?.evidence_sufficiency || report?.evidence_quality || "not reported" },
        { label: "Deterministic rank order", value: safeList(report?.deterministic_rank_order).join(", ") || "ranking from report order" },
      ]),
      renderSimpleTable({
        columns: [
          { label: "Rank", render: (row, index) => escapeHtml(row.rank ?? index + 1) },
          { label: "Challenger", render: (row) => escapeHtml(row.challenger_id || row.candidate_id || row.hypothesis_id || "UNKNOWN") },
          { label: "Evidence", render: (row) => escapeHtml(row.evidence_quality || row.evidence_sufficiency || "not reported") },
          { label: "Expectancy delta", render: (row) => escapeHtml(row.expectancy_delta_vs_incumbent ?? row.expectancy_delta ?? "not reported") },
          { label: "Drift", render: (row) => escapeHtml(row.drift_status || "not reported") },
          { label: "Fragility", render: (row) => escapeHtml(row.fragility_status || "not reported") },
        ],
        rows,
        emptyMessage: "No challenger rows are available.",
      }),
      `<div class="research-action-toolbar">
        <a class="primary-button research-action-button" href="/api/research-lab/challenger-comparison-reports" target="_blank" rel="noopener noreferrer">Compare Challengers</a>
        <a class="ghost-button research-action-button" href="/api/research-lab/challenger-tracks" target="_blank" rel="noopener noreferrer">Open Challenger Tracks</a>
      </div>`,
    ].join(""),
  });
}

function renderResearchWorkbenchCard({ payload = {}, research = {}, edgeLab = {}, projection = {}, sources = {} }) {
  const pipeline = edgeLab.pipeline || research.pipeline || {};
  const rows = Object.values(pipeline).flatMap((items) => safeList(items));
  const activeRows = rows.length ? rows : safeList(research.active_hypotheses || research.captured_hypotheses);
  const actions = [
    "Open Research",
    "Open Human Review",
    "Open Evidence",
    "Generate Evidence",
    "Run Event Study",
    "Create Challenger",
    "Start Paper Trial",
    "Review Drift",
    "Review Regime Fragility",
    "Compare Challengers",
    "Prepare Human Review",
    "Archive Hypothesis",
  ];
  return renderWorkflowCard({
    payload,
    sourceKey: "research_pipeline",
    fieldPath: "edge_lab.research_workbench",
    whyShown: "Research Pipeline is the governed workspace for moving hypotheses through idea, research, validation, paper trial, ready, captured, blocked, and archived states.",
    eyebrow: "RESEARCH_WORKBENCH",
    title: "Research Workbench",
    subtitle: "Evidence generation, challenger review, paper-trial preparation, and human review. No automatic promotion or sleeve mutation.",
    body: [
      `<div class="research-action-toolbar">${actions.map((action) => `<button class="ghost-button research-action-button" type="button" title="Governed research action">${escapeHtml(action)}</button>`).join("")}</div>`,
      renderSimpleTable({
        columns: [
          { label: "Hypothesis", render: (row) => `<strong>${escapeHtml(row.hypothesis_id || row.id || "UNKNOWN")}</strong><div class="muted-mini">${escapeHtml(row.readable_title || row.title || row.summary || "")}</div>` },
          { label: "Evidence", render: (row) => escapeHtml(row.evidence_quality || row.latest_result_status || projection?.evidence_quality || "not reported") },
          { label: "Learning", render: (row) => escapeHtml(row.longitudinal_learning || projection?.longitudinal_status || "not reported") },
          { label: "Drift", render: (row) => escapeHtml(row.drift_status || row.expectancy_drift_status || "not reported") },
          { label: "Fragility", render: (row) => escapeHtml(row.fragility_status || row.regime_fragility_status || "not reported") },
          { label: "Challenger", render: (row) => escapeHtml(row.challenger_status || projection?.challenge_type || "not reported") },
          { label: "Governance", render: (row) => escapeHtml(row.current_gate || row.governance_state || projection?.lane || "Research Only") },
          { label: "Open tasks", render: (row) => escapeHtml(row.next_action || safeList(row.next_allowed_actions).join(", ") || "No task reported") },
          { label: "Expand", render: (row) => `<details><summary>Inline research context</summary>${renderDefinitionRows([
            { label: "Next command fallback", value: row.primary_command || row.next_command || "not required for normal UI flow" },
            { label: "Evidence needed", value: safeList(row.evidence_needed || row.evidence_required).join(", ") || "not reported" },
            { label: "Latest result", value: row.latest_result_summary || "not reported" },
          ])}</details>` },
        ],
        rows: activeRows.slice(0, 16),
        emptyMessage: "No active hypotheses are available in the research workbench.",
      }),
    ].join(""),
  });
}

function renderHumanReviewWorkspaceCard({ payload = {}, research = {}, projection = {}, sources = {} }) {
  const chain = projection?.evidence_chain || {};
  const latest = chain.latest_summary || {};
  const nextActions = safeList(projection?.next_allowed_actions || chain.next_allowed_actions);
  const dossier = latestResearchRow(sources.humanReviewDossiers, ["human_review_dossiers", "dossiers"]);
  const rows = [
    { dossier: "HumanReviewDossier", status: projection?.latest_review_decision || chain.latest_review_decision || "not recorded", summary: latest.human_review || dossier.reason || "Operator dossier available when generated." },
    { dossier: "ChallengerComparisonReport", status: projection?.challenge_type || chain.challenge_type || "not reported", summary: latest.challenger_comparison || "Compare incumbent and challenger evidence." },
    { dossier: "ExpectancyDriftReport", status: projection?.drift_status || "not reported", summary: latest.expectancy_drift || "Track whether expectancy is degrading." },
    { dossier: "RegimeFragilityReport", status: projection?.fragility_status || "not reported", summary: latest.regime_fragility || "Show where the edge is fragile by regime." },
    { dossier: "SleeveStabilityReport", status: projection?.health || chain.overall_health || "not reported", summary: latest.sleeve_stability || "Stability and review state." },
  ];
  const decisions = ["Open Dossier", "Open Challenger Paper Trial", "Request More Evidence", "Continue Observation"];
  const openQuestions = safeList(dossier.open_questions || dossier.governance_questions || dossier.required_operator_decisions);
  return renderWorkflowCard({
    payload,
    sourceKey: "research_lab_human_review",
    fieldPath: "research.human_review_workspace",
    whyShown: "Human review pulls dossiers, challenger comparison, drift, fragility, stability, and required decisions into one operator workspace.",
    eyebrow: "HUMAN_REVIEW",
    title: "Human Review Workspace",
    subtitle: "Review-only decisions. Promotion, sleeve changes, and live operation remain forbidden without future explicit human approval paths.",
    body: [
      renderDefinitionRows([
        { label: "Dossier id", value: dossier.human_review_dossier_id || dossier.dossier_id || "not reported" },
        { label: "Reason dossier exists", value: dossier.reason || dossier.review_reason || "not reported" },
        { label: "Executive summary", value: projection?.latest_summary || latest.summary || "Research governance view only." },
        { label: "What changed recently", value: latest.what_changed || "No recent change summary reported." },
        { label: "Why sleeve degraded", value: latest.degradation_reason || projection?.challenge_type || "No degradation reason reported." },
        { label: "Top review candidate", value: dossier.top_review_candidate_id || latest.top_challenger || "No challenger reported." },
        { label: "Open questions", value: openQuestions.join(", ") || "No open governance questions reported." },
        { label: "Required human decision options", value: safeList(dossier.required_human_decision_options || dossier.required_decision_options).join(", ") || "continue_observation, request_more_evidence, open_challenger_paper_trial" },
        { label: "Recommended next action", value: dossier.recommended_next_action || nextActions[0] || "continue_observation" },
      ]),
      renderSimpleTable({
        columns: [
          { label: "Dossier / Report", key: "dossier" },
          { label: "Status", key: "status" },
          { label: "Summary", key: "summary" },
        ],
        rows,
        emptyMessage: "No human review dossiers are available.",
      }),
      `<div class="research-action-toolbar">${decisions.map((decision) => `<a class="ghost-button research-action-button" href="/api/research-lab/human-review-dossiers" target="_blank" rel="noopener noreferrer">${escapeHtml(decision)}</a>`).join("")}</div>`,
      `<div class="compact-safety-strip">Human review records intent only. No automatic promotion. No sleeve mutation.</div>`,
    ].join(""),
  });
}

function renderEvidenceIntelligencePanels({ payload = {}, research = {}, sleeves = {}, projection = {}, sources = {} }) {
  const sleeveRows = flattenCockpitSleeves(sleeves);
  const chain = projection?.evidence_chain || {};
  const refs = safeList(projection?.artifact_refs || chain.artifact_refs);
  const refFor = (type) => refs.find((ref) => ref.artifact_type === type) || {};
  const latest = chain.latest_summary || {};
  const panels = [
    { name: "Event Study Summary", metric: latest.event_study || refFor("event_study_evidence").summary || "not reported", status: projection?.evidence_quality || "research_simulation", source: projection?.event_study_evidence_id || chain.event_study_evidence_id || refFor("event_study_evidence").artifact_id, href: `/api/research-lab/evidence/${encodeURIComponent(String(chain.event_study_evidence_id || ""))}/summary` },
    { name: "Backtest Summary", metric: latest.backtest || refFor("backtest_evidence").summary || "not reported", status: chain.backtest_health || "not reported", source: projection?.backtest_evidence_id || chain.backtest_evidence_id || refFor("backtest_evidence").artifact_id, href: `/api/research-lab/evidence/${encodeURIComponent(String(chain.backtest_evidence_id || ""))}/summary` },
    { name: "Longitudinal Learning", metric: refFor("longitudinal_run").summary || `${safeList(chain.longitudinal_run_ids).length} runs`, status: projection?.longitudinal_status || "not reported", source: safeList(chain.longitudinal_run_ids)[0] || refFor("longitudinal_run").artifact_id, href: "/api/research-lab/research-backlog-priority" },
    { name: "Expectancy Drift", metric: sources.expectancyDrift?.summary || sources.expectancyDrift?.expectancy_drift_status || "not reported", status: sources.expectancyDrift?.expectancy_drift_status || sources.expectancyDrift?.drift_status || "not reported", source: sources.expectancyDrift?.expectancy_drift_report_id || "expectancy_drift_latest", href: "/api/research-lab/stability/expectancy-drift/latest" },
    { name: "Regime Fragility", metric: sources.regimeFragility?.summary || sources.regimeFragility?.regime_fragility_status || "not reported", status: sources.regimeFragility?.regime_fragility_status || sources.regimeFragility?.fragility_status || "not reported", source: sources.regimeFragility?.regime_fragility_report_id || "regime_fragility_latest", href: "/api/research-lab/stability/regime-fragility/latest" },
    { name: "Sleeve Stability", metric: sources.sleeveStability?.summary || sources.sleeveStability?.overall_stability_status || "not reported", status: sources.sleeveStability?.overall_stability_status || sources.sleeveStability?.sleeve_stability_status || projection?.health || "not reported", source: sources.sleeveStability?.sleeve_stability_report_id || "sleeve_stability_latest", href: "/api/research-lab/stability/sleeve-stability/latest" },
    { name: "Paper Trial Progress", metric: refFor("paper_trial").summary || `${safeList(chain.paper_trial_ids).length} paper trials`, status: projection?.paper_trial_status || chain.paper_trial_status || "not reported", source: safeList(chain.paper_trial_ids)[0] || refFor("paper_trial").artifact_id, href: "/api/research-lab/paper-trial-inventory" },
  ];
  return renderWorkflowCard({
    payload,
    sourceKey: "research_lab_intelligence",
    fieldPath: "research.intelligence_panels",
    whyShown: "Compact research intelligence panels make drift, fragility, challenger comparison, evidence trend, and observation growth visible without predictive ML claims.",
    eyebrow: "RESEARCH_INTELLIGENCE",
    title: "Evidence Intelligence Panels",
    subtitle: "Research Intelligence Panels for Rolling expectancy, drift, fragility, challenger comparison, and Observation growth. Simple deterministic summaries only.",
    body: `
      <div class="mini-chart-panel research-intelligence-grid">
        ${panels.map((panel) => `
          <div>
            <strong>${escapeHtml(panel.name)}</strong>
            <span>${escapeHtml(panel.metric)}</span>
            <span>Status: ${escapeHtml(panel.status)}</span>
            <span>Source: ${escapeHtml(panel.source || "not reported")}</span>
            <a class="ghost-button research-action-button" href="${escapeHtml(panel.href)}" target="_blank" rel="noopener noreferrer">Open Evidence</a>
          </div>
        `).join("")}
        <div><strong>Observation Growth</strong><span>${escapeHtml(String(safeList(research.paper_trials || research.priority_tasks).length || sleeveRows.length || 0))} tracked rows</span><span>Source: canonical research projection</span></div>
      </div>
    `,
  });
}

function renderResearchLabEvidenceProjectionCard(projection = {}) {
  const chain = projection.evidence_chain || {};
  const refs = safeList(projection.artifact_refs || chain.artifact_refs);
  const missing = safeList(chain.missing_artifacts || projection.missing_artifacts);
  const errors = safeList(chain.errors || projection.errors);
  return renderCardSection({
    eyebrow: "RESEARCH_STORE_METADATA",
    title: "Research Store Metadata",
    subtitle: "Artifact references, provenance, integrity details, and raw metadata are collapsed by default.",
    body: [
      `<details class="support-note">
        <summary>Artifact References (${refs.length})</summary>
        ${renderSimpleTable({
          columns: [
            { label: "Type", key: "artifact_type" },
            { label: "ID", key: "artifact_id" },
            { label: "Status", key: "status" },
            { label: "Hash", key: "content_hash" },
            { label: "Path", key: "source_path" },
          ],
          rows: refs,
          emptyMessage: "No artifact references are available.",
        })}
      </details>`,
      `<details class="support-note">
        <summary>Provenance</summary>
        ${renderDefinitionRows([
          { label: "Sleeve", value: projection.sleeve_id || chain.sleeve_id || "not reported" },
          { label: "Sleeve version", value: projection.sleeve_version_id || chain.sleeve_version_id || "not reported" },
          { label: "Dataset snapshot", value: chain.dataset_snapshot_id || "not reported" },
          { label: "Regime snapshot", value: chain.regime_snapshot_id || "not reported" },
          { label: "Cost model", value: chain.cost_model_snapshot_id || "not reported" },
          { label: "Updated", value: projection.updated_at || "not reported" },
        ])}
      </details>`,
      `<details class="support-note">
        <summary>Integrity Details (${missing.length + errors.length})</summary>
          ${renderSimpleTable({
            columns: [
              { label: "Path", key: "path" },
              { label: "Reason", render: (row) => escapeHtml(row.reason || row.error || "") },
              { label: "Artifact", key: "artifact_id" },
            ],
            rows: [...missing, ...errors],
            emptyMessage: "No missing artifacts or integrity errors.",
          })}
      </details>`,
      `<details class="support-note">
        <summary>Raw Metadata</summary>
        <pre class="edge-command-block"><code>${escapeHtml(JSON.stringify({
          lane: projection.lane,
          health: projection.health,
          challenge_type: projection.challenge_type,
          recommended_action: projection.recommended_action,
          paper_trial_status: projection.paper_trial_status,
          latest_review_decision: projection.latest_review_decision,
          next_allowed_actions: projection.next_allowed_actions,
        }, null, 2))}</code></pre>
      </details>`,
      `<div class="compact-safety-strip">${escapeHtml(projection.research_label || chain.research_label || "Research governance view only. No broker execution. No live trading. Manual review required.")}</div>`,
    ].join(""),
  });
}

function renderResearchPipelineKanban(pipeline = {}, counts = {}) {
  const columns = [
    { key: "active", label: "Active", sources: ["active", "tier_1_active"] },
    { key: "promising", label: "Promising", sources: ["promising", "tier_2_promising"] },
    { key: "experimental", label: "Experimental", sources: ["experimental", "tier_3_experimental"] },
    { key: "watchlist", label: "Watchlist", sources: ["watchlist", "tier_4_watchlist"] },
    { key: "blocked", label: "Blocked", sources: ["blocked"] },
    { key: "captured", label: "Captured", sources: ["captured"] },
    { key: "archived", label: "Archived", sources: ["archived", "tier_5_archive", "rejected_archived"] },
  ];
  const rowsForColumn = (column) => {
    const rows = column.sources.flatMap((source) => safeList(pipeline[source]));
    if (column.key === "blocked" && !rows.length) {
      return Object.entries(pipeline || {}).flatMap(([source, values]) =>
        ["archived", "rejected_archived"].includes(source) ? [] : safeList(values).filter((row) => row.blocker || row.blocker_summary || ["BLOCKED", "NEEDS_OPERATOR"].includes(String(row.gate_status || "").toUpperCase())),
      );
    }
    return rows;
  };
  const visibleRows = (column, rows) => ["experimental", "watchlist", "archived"].includes(column.key) ? rows.slice(0, 5) : rows;
  return `
    <div class="edge-kanban research-pipeline-board" aria-label="Research Pipeline">
      ${columns.map((column) => {
        const rows = rowsForColumn(column).sort((a, b) => Number(a.rank || 9999) - Number(b.rank || 9999));
        const mappedCount = counts[column.key.toUpperCase()] ?? counts[`TIER_${column.key.toUpperCase()}`] ?? column.sources.reduce((total, source) => total + Number(counts[source.toUpperCase()] ?? 0), 0);
        const count = mappedCount || rows.length;
        const visible = visibleRows(column, rows);
        const hiddenCount = Math.max(0, rows.length - visible.length);
        return `
          <section class="edge-kanban-column research-pipeline-column" data-pipeline-state="${escapeHtml(column.key.toUpperCase())}">
            <div class="edge-kanban-header">
              <div>
                <div class="edge-kanban-title">${escapeHtml(column.label)}</div>
                <div class="edge-kanban-count">${escapeHtml(String(count))} ideas</div>
              </div>
            </div>
            <div class="edge-kanban-stack">
              ${visible.length ? visible.map(renderResearchPipelineCard).join("") : `<div class="edge-kanban-empty">No ideas in ${escapeHtml(column.label)}.</div>`}
              ${hiddenCount ? `<details class="edge-cli-fallback"><summary>${escapeHtml(String(hiddenCount))} lower-priority items folded</summary>${rows.slice(visible.length).map(renderResearchPipelineCard).join("")}</details>` : ""}
            </div>
          </section>
        `;
      }).join("")}
    </div>
  `;
}

function renderResearchPipelineCard(item = {}) {
  const command = item.primary_command || item.next_command || "";
  const commands = safeList(item.alternate_commands).length
    ? safeList(item.alternate_commands)
    : (command ? [{ label: "Next command", command }] : []);
  const evidenceRequired = safeList(item.evidence_required || item.evidence_needed || item.missing_evidence);
  const evidenceCount = safeList(item.evidence_available || item.source_artifacts).length;
  const transitions = safeList(item.allowed_transitions);
  const latestResult = item.latest_result_status || item.latest_result?.test_status || item.latest_result?.latest_result || "No result yet";
  const latestSummary = item.latest_result_summary || "No test result has been recorded.";
  const operatorBlocker = researchOperatorBlocker(item);
  const hasOperatorBlocker = operatorBlocker.state && operatorBlocker.state !== "NO_BLOCKER";
  const actions = edgeLabActionsForItem(item);
  const tier = item.tier || "TIER_4_WATCHLIST";
  const tierLabel = researchTierLabel(tier);
  const blockerSummary = item["blocker_summary"] || "";
  const symbols = safeList(item.symbols || item.related_symbols || item.universe).join(" ") || item.symbol || "Symbols not set";
  const confidence = item.confidence || item.confidence_label || item.evidence_quality || "Not enough evidence yet";
  const owner = item.owner || "Aegis";
  const lastUpdated = item.updated_at_utc || item.generated_at_utc || item.last_update || "Not reported";
  const paperTrialStatus = item.paper_trial_status || item.latest_paper_trial_status || (String(item.current_gate || "").toUpperCase() === "PAPER_TRIAL" ? "Active" : "Not started");
  return `
    <article class="edge-pipeline-card research-hypothesis-card research-tier-${escapeHtml(String(tier).toLowerCase().replaceAll("_", "-"))}${hasOperatorBlocker ? " research-hypothesis-card-blocked" : ""}">
      <div class="edge-pipeline-title">${escapeHtml(item.readable_title || item.title || item.hypothesis_id || "Research idea")}</div>
      <div class="edge-pipeline-id">${escapeHtml(symbols)}</div>
      <div class="edge-badge-row">
        <span class="edge-badge">${escapeHtml(tierLabel)}</span>
        <span class="edge-badge edge-badge-muted">${escapeHtml(operatorResearchStateLabel(item))}</span>
      </div>
      ${item.short_summary ? `<p class="edge-pipeline-muted">${escapeHtml(item.short_summary)}</p>` : ""}
      <div class="research-card-facts">
        <div><span>Priority</span><strong>${escapeHtml(String(item.priority_score ?? "n/a"))}</strong></div>
        <div><span>Attention</span><strong>${escapeHtml(String(item.attention_score ?? "n/a"))}</strong></div>
        <div><span>Evidence</span><strong>${escapeHtml(String(item.evidence_strength ?? evidenceCount))}</strong></div>
        <div><span>Paper trial</span><strong>${escapeHtml(paperTrialStatus)}</strong></div>
        <div><span>Test status</span><strong>${escapeHtml(String(latestResult))}</strong></div>
        <div><span>Sample size</span><strong>${escapeHtml(String(item.latest_result?.sample_size ?? item.sample_size ?? "n/a"))}</strong></div>
        <div><span>Confidence</span><strong>${escapeHtml(String(item.confidence ?? confidence))}</strong></div>
        <div><span>Action required</span><strong>${escapeHtml(item.operator_attention_required ? "Yes" : "No")}</strong></div>
      </div>
      ${hasOperatorBlocker ? renderResearchOperatorBlocker(operatorBlocker) : `
        <div class="edge-pipeline-section">
          <div class="edge-pipeline-label">Next required action</div>
          <div>${escapeHtml(item.recommended_action || item.next_action || "No action needed")}</div>
        </div>
      `}
      ${blockerSummary ? `<div class="edge-pipeline-section"><div class="edge-pipeline-label">Blocker summary</div><div>${escapeHtml(blockerSummary)}</div></div>` : ""}
      <div class="edge-pipeline-muted">Last update: ${escapeHtml(lastUpdated)}</div>
      ${actions.length ? `
        <div class="edge-action-row">
          ${actions.map((action) => `
            <button class="${action.primary ? "primary-button" : "ghost-button"} edge-action-button" type="button" data-edge-open-modal="${escapeHtml(action.modalId)}">
              ${escapeHtml(action.label)}
            </button>
          `).join("")}
        </div>
        ${actions.map((action) => renderEdgeLabActionDialog(item, action)).join("")}
      ` : ""}
      <details class="edge-cli-fallback">
        <summary>Advanced diagnostics</summary>
        <div class="edge-pipeline-section">
          <div class="edge-pipeline-label">Internal state</div>
          <div>${escapeHtml(item.current_gate || "UNKNOWN")}</div>
        </div>
        ${hasOperatorBlocker ? `
          <div class="edge-pipeline-section">
            <div class="edge-pipeline-label">Raw blockers</div>
            <div>${escapeHtml(safeList(operatorBlocker.raw_blockers).join(", ") || item.blocker || "No raw blocker")}</div>
          </div>
        ` : ""}
        <div class="edge-pipeline-section">
          <div class="edge-pipeline-label">Latest result</div>
          <div>${escapeHtml(latestResult)}</div>
          <p class="edge-pipeline-muted">${escapeHtml(latestSummary)}</p>
        </div>
        ${evidenceRequired.length ? `<div class="edge-pipeline-muted"><strong>Evidence needed:</strong> ${escapeHtml(evidenceRequired.join(", "))}</div>` : ""}
        ${transitions.length ? `<div class="edge-pipeline-muted"><strong>Allowed transitions:</strong> ${escapeHtml(transitions.join(", "))}</div>` : ""}
        ${safeList(item.expected_output).length ? `<div class="edge-pipeline-muted"><strong>Expected output:</strong> ${escapeHtml(safeList(item.expected_output).join(", "))}</div>` : ""}
        ${commands.length ? `
          <div class="edge-pipeline-section">
            <div class="edge-pipeline-label">CLI fallback</div>
            ${commands.map((row) => `
              <div class="edge-command-wrap">
                <div class="edge-command-label">${escapeHtml(row.label || "Command")}</div>
                <pre class="edge-command-block"><code>${escapeHtml(row.command || "")}</code></pre>
              </div>
            `).join("")}
          </div>
        ` : ""}
      </details>
    </article>
  `;
}

function researchOperatorBlocker(item = {}) {
  const candidate = item.operator_blocker;
  if (candidate && typeof candidate === "object" && !Array.isArray(candidate)) {
    return candidate;
  }
  const rawBlocker = String(item.blocker || item.gate_status || item.status || "").toLowerCase();
  const missingItems = safeList(item.missing_symbols || item.required_symbols || item.symbols || item.related_symbols || item.universe);
  if (rawBlocker.includes("earnings_event_calendar_required") || rawBlocker.includes("external earnings calendar") || rawBlocker.includes("nvidia historical earnings")) {
    return {
      state: "BLOCKED_EXTERNAL_EARNINGS_CALENDAR_REQUIRED",
      title: "External earnings calendar required",
      summary: "Aegis cannot run the NVIDIA event-dislocation study until validated earnings dates and timing are available.",
      why_it_matters: "The event-study sample cannot be defined without a reproducible earnings calendar.",
      missing_items: ["NVDA historical earnings dates/times"],
      recovery_strategy: "WAITING_ON_OPERATOR_UPLOAD",
      expected_recovery: "Recovery after the earnings calendar CSV is imported and the Research Pipeline refreshes.",
      operator_action_required: true,
      next_action: "External earnings calendar required. Upload CSV with symbol,event_date,event_time,timing,event_type,source.",
      action_kind: "UPLOAD_EARNINGS_EVENT_CALENDAR",
      data_acquisition_plan: {
        data_needed: ["NVDA historical earnings dates/times"],
        why_needed: "Validated earnings dates and release timing are required for the NVIDIA event-dislocation study.",
        source_type: "USER_MUST_UPLOAD",
        action_label: "Upload earnings calendar",
        action_kind: "UPLOAD_EARNINGS_EVENT_CALENDAR",
        action_available: false,
        accepted_format: "CSV with columns: symbol,event_date,event_time,timing,event_type,source. Optional columns: eps_actual,eps_estimate,revenue_actual,revenue_estimate,guidance_flag,notes.",
        last_attempt_status: "UPLOAD_REQUIRED",
        last_failure_reason: "Governed earnings calendar artifact is missing.",
        next_action: "External earnings calendar required.",
      },
      raw_blockers: safeList(item.blocker || item.gate_status || item.status),
    };
  }
  if (rawBlocker.includes("event_window_ohlcv_data_required") || rawBlocker.includes("event-window ohlcv")) {
    return {
      state: "BLOCKED_EVENT_WINDOW_DATASET_REQUIRED",
      title: "Event-window OHLCV dataset required",
      summary: "Aegis needs validated OHLCV rows around NVIDIA earnings events before it can compute forward returns.",
      why_it_matters: "Forward returns, gap size, spillover, and volatility context must come from governed event-window data.",
      missing_items: ["NVDA, SMH, SOXX, QQQ, XLK, SPY, and VIX event-window OHLCV"],
      recovery_strategy: "WAITING_ON_OPERATOR_UPLOAD",
      expected_recovery: "Recovery after the event-window OHLCV CSV is imported and the research test reruns.",
      operator_action_required: true,
      next_action: "Upload event-window OHLCV, then rerun the NVIDIA event study.",
      action_kind: "UPLOAD_EVENT_WINDOW_OHLCV",
      data_acquisition_plan: {
        data_needed: ["NVDA, SMH, SOXX, QQQ, XLK, SPY, and VIX event-window OHLCV"],
        why_needed: "Event windows and forward returns require validated OHLCV rows.",
        source_type: "USER_MUST_UPLOAD",
        action_label: "Upload event-window OHLCV",
        action_kind: "UPLOAD_EVENT_WINDOW_OHLCV",
        action_available: false,
        accepted_format: "CSV with columns: symbol,date,open,high,low,close,volume.",
        last_attempt_status: "UPLOAD_REQUIRED",
        last_failure_reason: "Governed research_event_window_dataset_v1 artifact is missing.",
        next_action: "Upload event-window OHLCV, then rerun the NVIDIA event study.",
      },
      raw_blockers: safeList(item.blocker || item.gate_status || item.status),
    };
  }
  if (rawBlocker.includes("stale")) {
    return {
      state: "BLOCKED_STALE_MARKET_DATA",
      title: "Blocked: Market data is stale",
      summary: "Latest market-session data is not fresh enough for this research step.",
      why_it_matters: "Aegis will not validate a hypothesis against stale market data.",
      missing_items: missingItems,
      recovery_strategy: "WAITING_ON_MARKET_REFRESH",
      expected_recovery: "Recovery after the next market data refresh completes.",
      operator_action_required: false,
      next_action: "Wait for the next market session refresh. No operator action required.",
      action_kind: "RETRY_REFRESH",
      raw_blockers: safeList(item.blocker || item.gate_status || item.status),
    };
  }
  if (rawBlocker.includes("data_needed") || rawBlocker.includes("needs_data") || rawBlocker.includes("missing_required_symbols") || rawBlocker.includes("missing_symbols")) {
    return {
      state: "BLOCKED_WAITING_FOR_MARKET_DATA",
      title: "Waiting for current intraday market data",
      summary: "Current intraday market data is not yet available for every required symbol.",
      why_it_matters: "The hypothesis cannot be tested until required current price data is present and fresh.",
      missing_items: missingItems,
      recovery_strategy: "WAITING_ON_MARKET_REFRESH",
      expected_recovery: "Aegis will retry automatically at the next market refresh window.",
      operator_action_required: false,
      next_action: "Automatic retry scheduled. No operator action required.",
      action_kind: "RETRY_DATA_FETCH",
      raw_blockers: safeList(item.blocker || item.gate_status || item.status),
    };
  }
  if (rawBlocker.includes("operator") || rawBlocker.includes("review")) {
    return {
      state: "BLOCKED_WAITING_ON_OPERATOR_DECISION",
      title: "Needs review",
      summary: "Aegis needs an operator decision before this idea can move forward.",
      why_it_matters: "Research lifecycle changes require explicit human review.",
      missing_items: [],
      recovery_strategy: "WAITING_ON_OPERATOR_DECISION",
      expected_recovery: "Recovery after an operator records a review decision.",
      operator_action_required: true,
      next_action: "Review the hypothesis and choose the next lifecycle step.",
      action_kind: "REVIEW_HYPOTHESIS",
      raw_blockers: safeList(item.blocker || item.gate_status || item.status),
    };
  }
  if (item.blocker) {
    return {
      state: "BLOCKED_RESEARCH_INPUT_REQUIRED",
      title: "Blocked: Research input required",
      summary: "Aegis cannot continue this research step with the currently available inputs.",
      why_it_matters: "The next research result would not be reliable until the missing input is resolved.",
      missing_items: missingItems,
      recovery_strategy: "WAITING_ON_EXTERNAL_SOURCE",
      expected_recovery: "Recovery after the missing input becomes available and the pipeline refreshes.",
      operator_action_required: false,
      next_action: "Wait for the missing input to refresh. No operator action required unless this remains blocked.",
      action_kind: "IGNORE_WARNING",
      raw_blockers: safeList(item.blocker || item.gate_status || item.status),
    };
  }
  return {
    state: "NO_BLOCKER",
    title: "No blocker",
    summary: "No blocker is active.",
    why_it_matters: "No recovery is needed.",
    missing_items: [],
    recovery_strategy: "NO_BLOCKER",
    expected_recovery: "No recovery needed.",
    operator_action_required: false,
    next_action: item.next_action || "No action needed.",
    action_kind: "NONE",
    raw_blockers: [],
  };
}

function renderResearchOperatorBlocker(blocker = {}) {
  const plan = researchDataAcquisitionPlan(blocker);
  const operatorRequired = blocker.operator_action_required === true;
  return `
    <section class="research-blocker-panel" data-recovery-strategy="${escapeHtml(blocker.recovery_strategy || "UNKNOWN")}">
      <div class="research-blocker-title">${escapeHtml(blocker.title || "Blocked")}</div>
      <p>${escapeHtml(blocker.summary || "Aegis cannot continue this item yet.")}</p>
      ${renderResearchDataAcquisitionPlan(plan)}
      <div class="research-blocker-why">${escapeHtml(blocker.why_it_matters || plan.why_needed || "This protects research quality.")}</div>
      <div class="research-next-action">
        <span>Next action</span>
        <strong>${escapeHtml(blocker.next_action || plan.next_action || "No action required.")}</strong>
      </div>
      <div class="research-recovery-row">
        <span>${escapeHtml(operatorRequired ? "Operator action required" : "No operator action required")}</span>
        <span>${escapeHtml(blocker.expected_recovery || plan.next_scheduled_retry || "Recovery condition not reported.")}</span>
      </div>
    </section>
  `;
}

function researchDataAcquisitionPlan(blocker = {}) {
  const plan = blocker.data_acquisition_plan;
  if (plan && typeof plan === "object" && !Array.isArray(plan)) {
    return plan;
  }
  const missing = safeList(blocker.missing_items);
  const actionKind = String(blocker.action_kind || "").toUpperCase();
  if (actionKind === "UPLOAD_DATASET") {
    return {
      data_needed: missing,
      why_needed: blocker.why_it_matters || "A required external dataset is missing.",
      source_type: "USER_MUST_UPLOAD",
      action_label: "Upload dataset",
      action_kind: "UPLOAD_DATASET",
      action_available: false,
      next_action: blocker.next_action || "Upload the required dataset through governed evidence intake.",
      accepted_format: "CSV with documented columns for the dataset type.",
      last_attempt_status: "UPLOAD_REQUIRED",
      last_failure_reason: "Dataset is not present in the evidence store.",
    };
  }
  if (actionKind === "RETRY_REFRESH") {
    return {
      data_needed: missing,
      why_needed: blocker.why_it_matters || "Current market data is required before Aegis can continue.",
      source_type: "AEGIS_CAN_FETCH_AUTOMATICALLY",
      action_label: "Fetch market data now",
      action_kind: "FETCH_MARKET_DATA_NOW",
      action_available: true,
      retry_at_utc: "",
      next_scheduled_retry: "Next configured market refresh window",
      last_attempt_status: "STALE",
      last_failure_reason: "Latest market data is stale.",
      next_action: "Fetch market data now, or wait for the next automatic market refresh.",
    };
  }
  return {
    data_needed: missing,
    why_needed: blocker.why_it_matters || "Current market data is required before Aegis can continue.",
    source_type: actionKind === "CONFIGURE_PROVIDER" ? "USER_MUST_CONFIGURE_PROVIDER" : "AEGIS_CAN_FETCH_AUTOMATICALLY",
    action_label: actionKind === "CONFIGURE_PROVIDER" ? "Configure provider" : "Fetch market data now",
    action_kind: actionKind === "CONFIGURE_PROVIDER" ? "CONFIGURE_PROVIDER" : "FETCH_MARKET_DATA_NOW",
    action_available: actionKind !== "CONFIGURE_PROVIDER",
    retry_at_utc: "",
    next_scheduled_retry: "Next configured market refresh window",
    last_attempt_status: "MISSING",
    last_failure_reason: "Required market data is not present in the current cache.",
    next_action: blocker.next_action || "Fetch market data now, or wait for the next automatic market refresh.",
  };
}

function renderResearchDataAcquisitionPlan(plan = {}) {
  const dataNeeded = safeList(plan.data_needed);
  const requiredConfig = safeList(plan.required_config_keys);
  const retry = plan.retry_at_utc || plan.next_scheduled_retry || "Not scheduled";
  const lastAttempt = [plan.last_attempt_status, plan.last_failure_reason].filter(Boolean).join(" - ") || "No fetch attempt reported.";
  const acceptedFormat = plan.accepted_format ? `<div><span>Accepted format</span><strong>${escapeHtml(plan.accepted_format)}</strong></div>` : "";
  const configKeys = requiredConfig.length ? `<div><span>Required config</span><strong>${escapeHtml(requiredConfig.join(", "))}</strong></div>` : "";
  return `
    <div class="research-data-acquisition-plan">
      <div><span>Data needed</span><strong>${escapeHtml(dataNeeded.join(", ") || "Required data not reported")}</strong></div>
      <div><span>Why needed</span><strong>${escapeHtml(plan.why_needed || "Aegis needs this data before the research can continue.")}</strong></div>
      <div><span>Source type</span><strong>${escapeHtml(researchSourceTypeLabel(plan.source_type))}</strong></div>
      <div><span>Next scheduled retry</span><strong>${escapeHtml(retry)}</strong></div>
      <div><span>Last fetch attempt</span><strong>${escapeHtml(lastAttempt)}</strong></div>
      ${acceptedFormat}
      ${configKeys}
      ${renderResearchDataAcquisitionAction(plan)}
      <div class="edge-action-status" data-research-data-acquisition-status></div>
    </div>
  `;
}

function renderResearchDataAcquisitionAction(plan = {}) {
  if (plan.action_available !== true) {
    return "";
  }
  const actionKind = String(plan.action_kind || "").toUpperCase();
  if (actionKind === "FETCH_MARKET_DATA_NOW") {
    return `
      <form class="research-data-acquisition-form" method="post">
        <input type="hidden" name="data_action" value="fetch-market-data-now">
        <input type="hidden" name="playbook_id" value="refresh_required_symbol_data">
        <input type="hidden" name="symbols" value="${escapeHtml(safeList(plan.data_needed).join(","))}">
        <button class="primary-button edge-action-button" type="submit">${escapeHtml(plan.action_label || "Fetch market data now")}</button>
      </form>
    `;
  }
  if (actionKind === "REFRESH_PIPELINE") {
    return `<button class="ghost-button edge-action-button" type="button" data-route="/research-lab">${escapeHtml(plan.action_label || "Refresh pipeline")}</button>`;
  }
  return "";
}

function researchSourceTypeLabel(value) {
  const normalized = String(value || "").toUpperCase();
  if (normalized === "AEGIS_CAN_FETCH_AUTOMATICALLY") return "Aegis can fetch automatically";
  if (normalized === "USER_MUST_UPLOAD") return "User must upload";
  if (normalized === "USER_MUST_CONFIGURE_PROVIDER") return "User must configure provider";
  if (normalized === "NOT_CURRENTLY_SUPPORTED") return "Not currently supported";
  if (normalized === "OPERATOR_DECISION_REQUIRED") return "Operator decision required";
  if (normalized === "EXTERNAL_SOURCE_REQUIRED") return "External source required";
  if (normalized === "NO_DATA_REQUIRED") return "No data required";
  return readableStatus(value || "Unknown");
}

function researchTierLabel(tier) {
  const normalized = String(tier || "").toUpperCase();
  if (normalized === "TIER_1_ACTIVE") return "Tier 1 Active";
  if (normalized === "TIER_2_PROMISING") return "Tier 2 Promising";
  if (normalized === "TIER_3_EXPERIMENTAL") return "Tier 3 Experimental";
  if (normalized === "TIER_4_WATCHLIST") return "Tier 4 Watchlist";
  if (normalized === "TIER_5_ARCHIVE") return "Tier 5 Archive";
  return readableStatus(tier || "Watchlist");
}

function operatorResearchStateLabel(item = {}) {
  const state = String(item.operator_lifecycle_state || item.lifecycle_state || item.current_gate || "IDEA").toUpperCase();
  if (["INBOX", "IDEA"].includes(state)) return "Idea";
  if (["TRIAGE", "TEST_PLAN", "RESEARCHING"].includes(state)) return "Researching";
  if (["TESTING", "RESULT_REVIEW", "VALIDATING"].includes(state)) return "Validating";
  if (["PAPER_TRIAL"].includes(state)) return "Paper Trial";
  if (["SLEEVE_REVIEW", "ACTIVE_OR_ADOPTED", "READY"].includes(state)) return "Ready";
  if (["CAPTURE_READY"].includes(state)) return "Capture Ready";
  if (["CAPTURED"].includes(state)) return "Captured";
  if (["REJECTED_ARCHIVED", "ARCHIVED"].includes(state)) return "Archived";
  if (state === "BLOCKED") return "Blocked";
  return readableStatus(state);
}

function edgeLabActionsForItem(item = {}) {
  const operatorBlocker = researchOperatorBlocker(item);
  if (operatorBlocker.state && operatorBlocker.state !== "NO_BLOCKER" && operatorBlocker.operator_action_required !== true) {
    return [];
  }
  if (String(operatorBlocker.action_kind || "").toUpperCase() === "UPLOAD_DATASET") {
    return [];
  }
  const hypothesisId = item.hypothesis_id || "HYPOTHESIS_ID";
  const gate = String(item.current_gate || "").toUpperCase();
  const prefix = safeDomId(`${hypothesisId}-${gate}`);
  if (gate === "INBOX") {
    return [
      { label: "Queue Test", action: "triage", decision: "QUEUE_TEST_PLAN", modalId: `${prefix}-queue-test`, primary: true, title: "Queue hypothesis for testing?", requiresReason: true },
      { label: "Reject", action: "triage", decision: "REJECT", modalId: `${prefix}-reject`, title: "Reject hypothesis?", requiresReason: true },
      { label: "Needs Clarification", action: "triage", decision: "NEEDS_CLARIFICATION", modalId: `${prefix}-clarify`, title: "Request clarification?", requiresReason: true },
    ];
  }
  if (gate === "TEST_PLAN") {
    return [{ label: "Build Test Plan", action: "build-plan", modalId: `${prefix}-build-plan`, primary: true, title: "Build research test plan?", requiresReason: false }];
  }
  if (gate === "TESTING") {
    return [{ label: "Run Test", action: "run-test", modalId: `${prefix}-run-test`, primary: true, title: "Run research test?", requiresReason: false }];
  }
  if (gate === "RESULT_REVIEW") {
    return [
      { label: "Needs More Evidence", action: "review", decision: "NEEDS_MORE_EVIDENCE", modalId: `${prefix}-more-evidence`, title: "Record needs-more-evidence decision?", requiresReason: true },
      { label: "Reject", action: "review", decision: "REJECTED", modalId: `${prefix}-review-reject`, title: "Reject research result?", requiresReason: true },
      { label: "Archive", action: "review", decision: "ARCHIVE", modalId: `${prefix}-archive`, title: "Archive hypothesis?", requiresReason: true },
      { label: "Accept for Paper Trial", action: "review", decision: "PAPER_TEST_CANDIDATE", modalId: `${prefix}-paper`, primary: true, title: "Accept for paper trial review?", requiresReason: true },
      { label: "Promote to Sleeve Review", action: "review", decision: "SLEEVE_REVIEW_CANDIDATE", modalId: `${prefix}-sleeve-review`, title: "Promote to sleeve review?", requiresReason: true },
    ];
  }
  return [];
}

function renderEdgeLabActionDialog(item = {}, action = {}) {
  const hypothesisId = item.hypothesis_id || "";
  const needsReason = action.requiresReason !== false;
  const testDetails = action.action === "build-plan" || action.action === "run-test" ? renderDefinitionRows([
    { label: "Test question", value: item.test_question || "Not enough current evidence yet." },
    { label: "Expected test type", value: item.test_type || "UNKNOWN" },
    { label: "Required datasets", value: safeList(item.required_datasets).join(", ") || "Not enough current evidence yet." },
    { label: "Expected outputs", value: safeList(item.expected_output).join(", ") || "Not enough current evidence yet." },
    { label: "Success criteria", value: safeList(item.success_criteria).join(", ") || "Evidence-backed result" },
    { label: "Failure criteria", value: safeList(item.failure_criteria).join(", ") || "Insufficient evidence" },
  ]) : "";
  const resultDetails = action.action === "review" ? renderDefinitionRows([
    { label: "Test status", value: item.latest_result_status || "UNKNOWN" },
    { label: "Latest result", value: item.latest_result_summary || "No result summary available." },
    { label: "Sample size", value: String(item.latest_result?.sample_size ?? "n/a") },
    { label: "Evidence quality", value: item.latest_result?.evidence_quality || "Not enough current evidence yet." },
    { label: "Warning", value: "This records an append-only review decision. It does not activate or mutate sleeves." },
  ]) : "";
  return `
    <dialog class="edge-action-modal" id="${escapeHtml(action.modalId)}">
      <form class="edge-lab-action-form" method="post">
        <input type="hidden" name="edge_action" value="${escapeHtml(action.action)}">
        <input type="hidden" name="hypothesis_id" value="${escapeHtml(hypothesisId)}">
        ${action.decision ? `<input type="hidden" name="decision" value="${escapeHtml(action.decision)}">` : ""}
        <div class="edge-modal-header">
          <div>
            <div class="section-eyebrow">EDGE LAB WORKFLOW</div>
            <h3>${escapeHtml(action.title || action.label || "Confirm research action")}</h3>
            <p>${escapeHtml(item.readable_title || item.title || hypothesisId)}</p>
          </div>
          <button class="icon-button" type="button" data-edge-close-modal aria-label="Close">×</button>
        </div>
        ${testDetails}
        ${resultDetails}
        ${action.action === "run-test" ? `<div class="support-note">This does not trade or change sleeves.</div>` : ""}
        <label class="edge-form-field">
          <span>Operator</span>
          <input name="operator" value="David" required>
        </label>
        <label class="edge-form-field">
          <span>Reason${needsReason ? "" : " / note"}</span>
          <textarea name="reason" ${needsReason ? "required" : ""} placeholder="${needsReason ? "Required reason" : "Optional confirmation note"}">${escapeHtml(defaultEdgeReason(action))}</textarea>
        </label>
        <label class="edge-form-field">
          <span>Optional note</span>
          <textarea name="note" placeholder="Optional operator note"></textarea>
        </label>
        <div class="edge-modal-safety">
          Research workflow only. No broker execution, no autonomous execution, no sleeve mutation, no automatic approval.
        </div>
        <div class="edge-modal-actions">
          <button class="ghost-button" type="button" data-edge-close-modal>Cancel</button>
          <button class="primary-button" type="submit">${escapeHtml(action.label || "Confirm")}</button>
        </div>
        <div class="edge-action-status" data-edge-lab-action-status></div>
      </form>
    </dialog>
  `;
}

function defaultEdgeReason(action = {}) {
  if (action.decision === "QUEUE_TEST_PLAN") {
    return "Worth testing";
  }
  if (action.decision === "REJECT") {
    return "Not worth testing";
  }
  if (action.decision === "NEEDS_CLARIFICATION") {
    return "Needs clearer test definition";
  }
  if (action.decision === "PAPER_TEST_CANDIDATE") {
    return "Passed initial research criteria";
  }
  if (action.decision === "REJECTED") {
    return "Research result not strong enough";
  }
  return "";
}

function researchInventorySummary({
  capturedRows = [],
  activeRows = [],
  validatedRows = [],
  rejectedRows = [],
  archivedRows = [],
  needsClassificationRows = [],
  duplicateRows = [],
  fixtureRows = [],
} = {}) {
  const canonicalCount = capturedRows.length + validatedRows.length + rejectedRows.length + archivedRows.length + needsClassificationRows.length;
  const parts = [
    `${activeRows.length || capturedRows.length} active/system-generated`,
    `${validatedRows.length} validated`,
    `${rejectedRows.length} rejected`,
    `${archivedRows.length} archived`,
    `${needsClassificationRows.length} need classification`,
    `${duplicateRows.length} legacy duplicates linked`,
    `${fixtureRows.length} test fixtures excluded`,
  ];
  return `${canonicalCount} canonical research hypotheses found: ${parts.join(", ")}.`;
}

function renderAegisHistoryWorkflow(payload) {
  const captureSource = safeList(payload.historical_captures).length
    ? safeList(payload.historical_captures)
    : safeList(payload.captured_trades).length
      ? safeList(payload.captured_trades)
      : (payload.latest_captured_trade_projection ? [payload.latest_captured_trade_projection] : []);
  const captures = safeList(captureSource);
  const fallback = payload.historical_fallback || {};
  const rows = captures.length ? captures : (fallback.source_day ? [{
    trade_date: fallback.source_day,
    symbol: fallback.symbol,
    sleeve_id: fallback.sleeve_id,
    quantity: "recorded",
    lifecycle_state: "Historical",
    replay_status: "Available",
    capture_record_id: fallback.selected_exposure_intent_id,
    artifact_path: fallback.artifact_path,
  }] : []);
  return {
    title: "Captured Trades",
    meta: "Authoritative historical manual-capture ledger.",
    html: [
      renderWorkflowCard({
        payload,
        sourceKey: "captured_ticket_projection_v1",
        fieldPath: "historical_captures",
        whyShown: "Captured Trades is the only primary workspace for historical manual captures. Historical captures are not active candidates.",
        eyebrow: "HISTORICAL_LEDGER",
        title: "Captured Trades",
        subtitle: "Read-only historical records. Export and evidence drilldown only; no capture actions appear here.",
        body: `
          <div class="operator-summary-strip operator-dashboard-summary">
            ${renderMetricCard({ label: "Historical captures", value: String(rows.length) })}
            ${renderMetricCard({ label: "Latest capture day", value: payload.operator_today_projection?.latest_historical_capture_label || fallback.source_day || "none" })}
            ${renderMetricCard({ label: "Current-day captures", value: String(payload.operator_today_projection?.completed_capture_count ?? 0) })}
          </div>
          ${renderCapturedTradeTable(rows)}
        `,
      }),
    ].join(""),
    contextHtml: workflowContextHtml(payload),
  };
}

function renderCapturedTradeTable(rows = []) {
  return renderSimpleTable({
    columns: [
      { label: "Trade date", render: (row) => escapeHtml(row.trade_date || row.day_utc || String(row.captured_at_utc || "").slice(0, 10) || "-") },
      { label: "Symbol", render: (row) => `<strong>${escapeHtml(row.symbol || "-")}</strong>` },
      { label: "Side", render: (row) => escapeHtml(row.side || row.direction || "-") },
      { label: "Sleeve", render: (row) => escapeHtml(row.sleeve_id || "-") },
      { label: "Entry", render: (row) => escapeHtml(String(row.fill_price ?? row.entry_price ?? "-")) },
      { label: "Quantity", render: (row) => escapeHtml(String(row.quantity ?? "-")) },
      { label: "Captured", render: (row) => escapeHtml(formatTimestamp(row.captured_at_utc || row.recorded_at_utc || "")) },
      { label: "Operator", render: (row) => escapeHtml(row.operator_id || "-") },
      { label: "Lifecycle", render: (row) => renderStatusPill(row.lifecycle_state || "Historical", "healthy", {}) },
      { label: "Replay", render: (row) => escapeHtml(row.replay_status || "not reported") },
      { label: "Record", render: (row) => `<span class="muted-mini">${escapeHtml(row.capture_record_id || row.record_id || "-")}</span>` },
    ],
    rows,
    emptyMessage: "No captured trades are recorded yet.",
  });
}

function renderJournalHeader(payload, summary = {}, events = []) {
  const familyCounts = summary.family_counts || {};
  const filters = ["All", "Candidates", "Edges", "Sleeves", "Performance", "Runtime", "Governance", "System"];
  return renderWorkflowCard({
    payload,
    sourceKey: "journal_timeline",
    fieldPath: "journal.timeline_summary",
    whyShown: "The Journal starts with a chronological memory summary before raw audit drilldowns.",
    eyebrow: "JOURNAL_TIMELINE",
    title: "Event Log → Timeline → Entity History → Audit Drilldown",
    subtitle: "Journal is for memory, evidence, replay, and accountability, not daily action.",
    body: `
      <div class="journal-hero">
        <div class="journal-stat"><span>Total events</span><strong>${escapeHtml(String(summary.total_events ?? events.length ?? 0))}</strong></div>
        <div class="journal-stat"><span>Latest event</span><strong>${escapeHtml(summary.latest_event_at || "No events yet")}</strong></div>
        <div class="journal-stat"><span>Evidence families</span><strong>${escapeHtml(Object.keys(familyCounts).join(", ") || "None yet")}</strong></div>
      </div>
      <div class="journal-toolbar" aria-label="Journal filters">
        <input class="journal-search" type="search" placeholder="Search by candidate, hypothesis, sleeve, report, or actor" aria-label="Search Journal" />
        <div class="journal-filter-row">
          ${filters.map((filter) => `<button class="journal-filter-chip" type="button">${escapeHtml(filter)}</button>`).join("")}
        </div>
      </div>
    `,
  });
}

function renderJournalTimelineFeed(payload, events = []) {
  const rows = safeList(events);
  return renderWorkflowCard({
    payload,
    sourceKey: "journal_timeline",
    fieldPath: "journal.recent_events",
    whyShown: "Recent Journal events explain what happened, why it mattered, who/what acted, and where evidence lives.",
    eyebrow: "TIMELINE_FEED",
    title: "Timeline Feed",
    subtitle: "Newest events first. Raw evidence remains collapsed until needed.",
    body: rows.length
      ? `<div class="journal-timeline">${rows.slice(0, 80).map(renderJournalEventCard).join("")}</div>`
      : renderWorkflowEmptyState({
          title: "No journal events yet.",
          message: "Journal will populate as Aegis generates candidates, records decisions, progresses hypotheses, updates performance, and writes audit artifacts.",
          normality: "Normal before operational artifacts have been generated.",
          nextActions: [
            "npm run aegis:journal-timeline",
            "npm run aegis:canonical-operator-state",
            "npm run aegis:audit",
          ],
        }),
  });
}

function renderJournalEventCard(event = {}) {
  const family = String(event.event_family || "SYSTEM").toUpperCase();
  const entity = [event.entity_type, event.entity_id].filter(Boolean).join(": ");
  return `
    <article class="journal-event-card" data-family="${escapeHtml(family)}">
      <div class="journal-event-meta">
        <span class="journal-family-badge">${escapeHtml(family)}</span>
        <span>${escapeHtml(event.timestamp_utc || "Timestamp not available")}</span>
        <span>${escapeHtml(event.actor || "Aegis")}</span>
      </div>
      <h3 class="journal-event-title">${escapeHtml(event.title || event.event_type || "Journal event")}</h3>
      <p class="journal-event-summary">${escapeHtml(event.summary || "Event summary unavailable.")}</p>
      <p class="journal-event-why"><strong>Why it matters:</strong> ${escapeHtml(event.why_it_matters || "Evidence was recorded for replay and accountability.")}</p>
      <div class="journal-event-entity">${escapeHtml(entity || "report")}</div>
      <details class="journal-audit-details">
        <summary>Evidence and state hashes</summary>
        ${renderDefinitionRows([
          { label: "Event ID", value: event.event_id || "" },
          { label: "Event type", value: event.event_type || "" },
          { label: "Source artifact path", value: event.source_artifact_path || "" },
          { label: "Source hash", value: event.source_hash || "" },
          { label: "Before state hash", value: event.before_state_hash || "" },
          { label: "After state hash", value: event.after_state_hash || "" },
        ])}
      </details>
    </article>
  `;
}

function renderJournalEntityHistory(payload, entityIndex = {}, events = []) {
  const buckets = [
    { label: "Candidates", key: "candidates" },
    { label: "Hypotheses", key: "hypotheses" },
    { label: "Sleeves", key: "sleeves" },
    { label: "Regimes", key: "regimes" },
  ];
  const body = buckets.map((bucket) => {
    const rows = safeList(entityIndex[bucket.key]);
    return `
      <details class="journal-entity-drawer" ${rows.length ? "open" : ""}>
        <summary>${escapeHtml(bucket.label)} (${rows.length})</summary>
        ${rows.length ? renderSimpleTable({
          columns: [
            { label: "Entity", key: "entity_id" },
            { label: "Events", key: "event_count" },
            { label: "Latest", key: "latest_event_at" },
          ],
          rows: rows.slice(0, 80),
          emptyMessage: "No entity history in this bucket yet.",
        }) : `<div class="empty-state">No ${escapeHtml(bucket.label.toLowerCase())} history yet.</div>`}
      </details>
    `;
  }).join("");
  return renderWorkflowCard({
    payload,
    sourceKey: "journal_timeline",
    fieldPath: "journal.entity_index",
    whyShown: "Entity history groups the event log by candidate, hypothesis, sleeve, and regime for replay.",
    eyebrow: "ENTITY_HISTORY",
    title: "Entity History",
    subtitle: "Open an entity group to see its timeline index and related event counts.",
    body,
  });
}

function renderJournalAuditDetails(payload, auditDrilldowns = [], diagnostics = []) {
  const drillRows = safeList(auditDrilldowns);
  return renderWorkflowCard({
    payload,
    sourceKey: "journal_timeline",
    fieldPath: "journal.audit_drilldowns",
    whyShown: "Raw artifacts, hashes, and diagnostics are preserved for audit, but stay below the timeline.",
    eyebrow: "AUDIT_DRILLDOWN",
    title: "System / Audit Details",
    subtitle: "Raw details are collapsed by default; use them when investigating evidence or replaying a decision.",
    body: `
      <details class="journal-audit-details">
        <summary>Source artifacts and hashes</summary>
        ${renderSimpleTable({
          columns: [
            { label: "ID", key: "id" },
            { label: "Type", key: "type" },
            { label: "Path", key: "path" },
            { label: "Hash", key: "hash" },
          ],
          rows: drillRows,
          emptyMessage: "No audit drilldowns are available.",
        })}
      </details>
      <details class="journal-audit-details">
        <summary>Diagnostics</summary>
        ${diagnostics.length ? renderSimpleTable({
          columns: [
            { label: "Diagnostic", key: "type" },
            { label: "Status", key: "status" },
            { label: "Detail", key: "message" },
          ],
          rows: diagnostics,
          emptyMessage: "No diagnostics emitted.",
        }) : `<div class="empty-state">No diagnostic exceptions are attached to the main timeline.</div>`}
      </details>
    `,
  });
}

function renderPortfolioSelectedCandidateTable(rows, opportunities = {}) {
  const safeRows = safeList(rows);
  if (!safeRows.length) {
    return renderNoOpportunityExplanation(opportunities);
  }
  return `
    <div class="operator-workspace-grid">
      <div class="operator-main-pane">
        ${renderSimpleTable({
          columns: [
            { label: "Symbol", render: (row) => `<strong>${escapeHtml(row.symbol || "Candidate")}</strong><div class="muted-mini">${escapeHtml(row.candidate_id || "candidate id unavailable")}</div>` },
            { label: "Sleeve", render: (row) => escapeHtml(row.sleeve_id || "Sleeve link unavailable") },
            { label: "Confidence", render: (row) => escapeHtml(row.confidence || "UNKNOWN") },
            { label: "Score band", render: (row) => escapeHtml(row.score_band || row.priority || "UNKNOWN") },
            { label: "Exposure cluster", render: (row) => escapeHtml(row.exposure_cluster || "UNKNOWN") },
            { label: "Why selected", render: (row) => escapeHtml(row.why_selected || row.operator_explanation || row.selection_reason || "Selected by portfolio selection policy.") },
            { label: "Why not", render: (row) => escapeHtml(row.why_not || "No broker execution, autonomous execution, or automatic approval is allowed.") },
            { label: "Suggested action", render: (row) => escapeHtml(row.suggested_operator_action || candidateTrustContext(row).recommended_operator_action_label || "Review manually") },
            { label: "Quick actions", render: (row) => renderCandidateActionButtons(row) },
          ],
          rows: safeRows.slice(0, 18),
          emptyMessage: "No selected candidate rows are available.",
        })}
      </div>
      <aside class="candidate-evidence-drawer" id="candidate-evidence-preview">
        <div class="section-eyebrow">Portfolio Selection</div>
        <h3>${escapeHtml(safeRows[0]?.symbol || "Candidate")}</h3>
        ${renderDefinitionRows([
          { label: "Policy", value: opportunities.candidate_portfolio_selection?.selection_policy_name || "DEFAULT_ONE_PER_SLEEVE_WITH_DISTINCT_EXPOSURE_EXCEPTION" },
          { label: "Selected", value: String(safeList(opportunities.selected_candidates).length || safeRows.length) },
          { label: "Suppressed", value: String(safeList(opportunities.suppressed_candidates).length) },
          { label: "Safety", value: "Read-only advisory; no broker execution." },
        ])}
      </aside>
    </div>
    <div class="compact-safety-strip">READ-ONLY GOVERNANCE · NO BROKER EXECUTION · NO AUTONOMOUS EXECUTION · MANUAL REVIEW ONLY</div>
  `;
}

function renderCandidatePortfolioSelectionPanel(opportunities = {}, payload = {}) {
  const selected = safeList(opportunities.selected_candidates);
  const summary = safeList(opportunities.same_sleeve_selection_summary);
  const clusters = safeList(opportunities.exposure_cluster_summary);
  const policy = opportunities.portfolio_selection_policy || {};
  if (!selected.length && !summary.length && !clusters.length) return "";
  return renderWorkflowCard({
    payload,
    sourceKey: "candidate_portfolio_selection",
    fieldPath: "opportunities.selected_candidates",
    whyShown: "Portfolio selection ranks same-sleeve candidates, permits distinct high-quality extras, and keeps suppressed candidates visible for learning.",
    eyebrow: selected.length ? "SELECTED_BY_PORTFOLIO_POLICY" : "PORTFOLIO_SELECTION_PENDING",
    title: "Selected Candidates",
    subtitle: "Selected candidates appear first. Same-sleeve extras require distinct exposure and sufficient confidence or score band.",
    body: `
      ${renderPortfolioSelectedCandidateTable(selected, opportunities)}
      <details class="raw-drawer" style="margin-top:12px;"><summary>Same-sleeve selection summary</summary>
        ${renderSimpleTable({
          columns: [
            { label: "Sleeve", key: "sleeve_id" },
            { label: "Run", key: "run_id" },
            { label: "Candidates", render: (row) => escapeHtml(String(row.same_sleeve_candidate_count ?? "")) },
            { label: "Selected", render: (row) => escapeHtml(String(row.selected_count ?? "")) },
            { label: "Suppressed", render: (row) => escapeHtml(String(row.suppressed_count ?? "")) },
            { label: "Policy", key: "policy" },
          ],
          rows: summary,
          emptyMessage: "No same-sleeve grouping is available.",
        })}
      </details>
      <details class="raw-drawer" style="margin-top:12px;"><summary>Exposure cluster summary</summary>
        ${renderSimpleTable({
          columns: [
            { label: "Cluster", key: "exposure_cluster" },
            { label: "Risk theme", key: "risk_theme" },
            { label: "Candidates", render: (row) => escapeHtml(String(row.candidate_count ?? "")) },
            { label: "Selected", render: (row) => escapeHtml(String(row.selected_count ?? "")) },
            { label: "Symbols", render: (row) => escapeHtml(safeList(row.symbols).join(", ")) },
          ],
          rows: clusters,
          emptyMessage: "No exposure cluster summary is available.",
        })}
      </details>
      <details class="raw-drawer" style="margin-top:12px;"><summary>Advanced policy</summary><pre class="code-block">${escapeHtml(JSON.stringify(policy, null, 2))}</pre></details>
    `,
  });
}

function renderWorkflowCandidateTable(rows, opportunities = {}) {
  const safeRows = safeList(rows);
  if (!safeRows.length) {
    return renderNoOpportunityExplanation(opportunities);
  }
  if (safeRows.some((row) => row.selection_status === "SELECTED" || row.selection_reason === "SELECTED_BY_PORTFOLIO_SELECTION_POLICY")) {
    return renderPortfolioSelectedCandidateTable(safeRows, opportunities);
  }
  const selected = safeRows[0] || {};
  return `
    <div class="operator-workspace-grid">
      <div class="operator-main-pane">
        ${renderSimpleTable({
          columns: [
            { label: "Symbol", render: (row) => `<strong>${escapeHtml(row.symbol || "Candidate")}</strong><div class="muted-mini">#${escapeHtml(row.rank ?? "unranked")} · ${escapeHtml(row.candidate_id || "candidate id unavailable")}</div>` },
            { label: "Direction", render: (row) => escapeHtml(candidateDirectionLabel(row)) },
            { label: "Sleeve", render: (row) => escapeHtml(row.sleeve_id || "Sleeve link unavailable") },
            { label: "Trust classification", render: (row) => renderStatusPill(candidateTrustContext(row).trust_classification_label, candidateTrustContext(row).trust_classification === "blocked" || candidateTrustContext(row).trust_classification === "unsupported" ? "warning" : "healthy", {}) },
            { label: "Guidance", render: (row) => escapeHtml(candidateTrustContext(row).decision_guidance_label) },
            { label: "Evidence completeness", render: (row) => escapeHtml(candidateTrustContext(row).evidence_completeness) },
            { label: "Main missing item", render: (row) => escapeHtml(candidateTrustContext(row).main_missing_item) },
            { label: "Next action", render: (row) => escapeHtml(candidateTrustContext(row).recommended_operator_action_label) },
            { label: "Review", render: (row) => escapeHtml(row.review_state || row.operator_review_status || "REVIEW_REQUIRED") },
            { label: "Age", render: (row) => escapeHtml(candidateAgeLabel(row)) },
            { label: "Quick actions", render: (row) => renderCandidateActionButtons(row) },
          ],
          rows: safeRows.slice(0, 18),
          emptyMessage: "No candidate rows are available.",
        })}
      </div>
      <aside class="candidate-evidence-drawer" id="candidate-evidence-preview">
        <div class="section-eyebrow">Why Should I Trust This?</div>
        <h3>${escapeHtml(selected.symbol || "Candidate")} ${escapeHtml(selected.direction || "")}</h3>
        ${renderCandidateEvidencePreview(selected)}
      </aside>
    </div>
    <div class="compact-safety-strip">READ-ONLY GOVERNANCE · NO BROKER EXECUTION · MANUAL CAPTURE ONLY</div>
  `;
}

function renderResearchObservationCandidateTable(rows = []) {
  const safeRows = safeList(rows);
  return renderSimpleTable({
    columns: [
      { label: "Symbol", render: (row) => `<strong>${escapeHtml(row.symbol || "Candidate")}</strong>` },
      { label: "Direction", render: (row) => escapeHtml(candidateDirectionLabel(row)) },
      { label: "Sleeve", render: (row) => escapeHtml(row.sleeve_id || "Sleeve link unavailable") },
      { label: "Reason", render: (row) => escapeHtml(row.research_observation_reason || "Supporting evidence incomplete") },
      { label: "Missing evidence", render: (row) => escapeHtml(String(row.missing_evidence_count ?? safeList(row.candidate_decision_projection?.missing_evidence_with_reason).length)) },
      { label: "Next action", render: () => "Request More Evidence" },
      { label: "Secondary actions", render: (row) => renderResearchObservationActions(row) },
    ],
    rows: safeRows,
    emptyMessage: "No generated candidate is waiting on supporting evidence.",
  });
}

function renderResearchObservationActions(candidate = {}) {
  const id = candidate.candidate_id || candidate.id || "CANDIDATE_ID";
  const prefix = safeDomId(`observation-${id}`);
  const actions = [
    { command: "REQUEST_MORE_EVIDENCE", label: "Request More Evidence", modalId: `${prefix}-evidence`, kind: "review", action: "needs-more-evidence", note: "Need more evidence before operator decision." },
    { command: "WATCHLIST_CANDIDATE", label: "Watchlist", modalId: `${prefix}-watch`, kind: "review", action: "watchlist", note: "Monitor this setup." },
    { command: "DISMISS_CANDIDATE", label: "Dismiss", modalId: `${prefix}-dismiss`, kind: "review", action: "dismiss", note: "Dismissed after review." },
  ];
  return `
    <div class="candidate-action-row">
      ${actions.map((action) => `<button class="ghost-button candidate-action-button" type="button" data-aegis-command-id="OPEN_IN_PAGE_MODAL" data-aegis-command-action-type="IN_PAGE_DETAIL" data-aegis-command-target-type="candidate" data-aegis-command-target-id="${escapeHtml(action.modalId)}" data-command-detail-target="${escapeHtml(action.modalId)}" data-candidate-open-modal="${escapeHtml(action.modalId)}">${escapeHtml(action.label)}</button>`).join("")}
      <a class="ghost-button candidate-action-button" href="#candidate-evidence-preview">Open Evidence</a>
    </div>
    ${actions.map((action) => renderCandidateReviewDialog(candidate, action)).join("")}
  `;
}

function candidateDirectionLabel(row = {}) {
  const direction = row.direction || row.candidate_direction;
  if (!direction) return "Direction unavailable";
  return direction;
}

function candidateEvidenceValue(row = {}, keys = [], unavailableReason = "Evidence projection incomplete.") {
  for (const key of keys) {
    const value = row[key];
    if (value !== undefined && value !== null && String(value).trim() && !["UNKNOWN", "not reported", "No summary available."].includes(String(value).trim())) {
      return String(value);
    }
  }
  return unavailableReason;
}

function candidateTrustContext(candidate = {}) {
  const brief = candidate.decision_support_brief && typeof candidate.decision_support_brief === "object"
    ? candidate.decision_support_brief
    : null;
  if (brief) {
    const missing = safeList(brief.missing_evidence);
    return {
      ...brief,
      why_exists: brief.why_triggered,
      expectancy: safeList(brief.historical_support).join(" ") || "No historical support linked.",
      evidence_quality: brief.trust_score_label || brief.trust_classification,
      event_study: safeList(brief.historical_support).find((item) => String(item).toLowerCase().includes("event study")) || "Event-study support is missing or not linked.",
      drift: safeList(brief.weakening_factors).find((item) => String(item).toLowerCase().includes("drift")) || "No drift weakness reported by the brief.",
      fragility: safeList(brief.weakening_factors).find((item) => String(item).toLowerCase().includes("fragility")) || "No fragility weakness reported by the brief.",
      observation_count: missing.find((item) => item.item === "Paper-trial context") ? "Paper-trial observations missing or insufficient." : "Observation context present.",
      challenger: safeList(brief.weakening_factors).find((item) => String(item).toLowerCase().includes("challenger")) || "No challenger context linked.",
      confidence: brief.trust_score_label || "Insufficient evidence",
      trust: brief.trust_score_label || "Unsupported",
      trust_classification_label: String(brief.trust_classification || "unsupported").replace(/_/g, " "),
      decision_guidance_label: String(brief.decision_guidance || "not_recommended_due_to_missing_evidence").replace(/_/g, " "),
      recommended_operator_action_label: String(brief.recommended_operator_action || "request_more_evidence").replace(/_/g, " "),
      evidence_state: missing.length ? `Incomplete: ${missing[0].item}` : "Complete enough for manual review",
      evidence_completeness: missing.length ? `Incomplete (${missing.length} missing)` : "Complete enough for review",
      main_missing_item: missing[0]?.item || "No critical missing evidence reported",
      stability_state: brief.sleeve_health_context || "Sleeve health context present",
      guidance: brief.direct_answer || brief.operator_summary,
      evidence_ids: safeList(brief.source_artifacts).join(", ") || "Supporting evidence ids unavailable.",
    };
  }
  const whyExists = candidateEvidenceValue(candidate, ["why_now", "why_this_trade", "explanation"], "Candidate generated by sleeve signal, but why-now explanation is unavailable in the current projection.");
  const expectancy = candidateEvidenceValue(candidate, ["historical_expectancy", "post_cost_expectancy_reference", "expectancy"], "Supporting research evidence is incomplete; expectancy artifact unavailable.");
  const evidenceQuality = candidateEvidenceValue(candidate, ["evidence_quality", "evidence_status"], "Evidence projection incomplete.");
  const eventStudy = candidateEvidenceValue(candidate, ["event_study_summary", "evidence_summary"], "Event-study artifact unavailable for this candidate.");
  const drift = candidateEvidenceValue(candidate, ["drift_state", "expectancy_drift_status"], "Drift report unavailable.");
  const fragility = candidateEvidenceValue(candidate, ["fragility_state", "regime_fragility_status"], "Regime fragility report unavailable.");
  const observationCount = candidateEvidenceValue(candidate, ["observation_count", "measured_candidate_count", "sample_size"], "Insufficient observations or observation count unavailable.");
  const challenger = safeList(candidate.related_challengers).join(", ") || candidate.challenger_context || "No challenger context linked.";
  const evidenceIds = safeList(candidate.evidence_artifacts || candidate.source_artifacts || candidate.supporting_evidence_ids);
  const missingReasons = [
    expectancy.includes("incomplete") || expectancy.includes("unavailable") ? "expectancy artifact unavailable" : "",
    eventStudy.includes("unavailable") ? "event-study artifact unavailable" : "",
    drift.includes("unavailable") ? "drift report unavailable" : "",
    fragility.includes("unavailable") ? "fragility report unavailable" : "",
    observationCount.includes("unavailable") || observationCount.includes("Insufficient") ? "insufficient observations" : "",
  ].filter(Boolean);
  const evidenceState = missingReasons.length ? `Partial: ${missingReasons[0]}` : "Complete";
  const confidence = candidateConfidenceLabel(candidate, evidenceQuality, missingReasons);
  const stability = missingReasons.some((reason) => reason.includes("drift") || reason.includes("fragility"))
    ? "Incomplete stability evidence"
    : candidateEvidenceValue(candidate, ["sleeve_health", "stability_state"], "Stable enough for review");
  const guidance = missingReasons.length
    ? "Candidate generated, but supporting research evidence is incomplete. Manual capture is not recommended until evidence projection succeeds."
    : "Human review still required before any manual external capture.";
  return {
    why_exists: whyExists,
    expectancy,
    evidence_quality: evidenceQuality,
    event_study: eventStudy,
    drift,
    fragility,
    observation_count: observationCount,
    challenger,
    confidence,
    trust_classification: missingReasons.length ? "unsupported" : "weakly_supported",
    trust_classification_label: missingReasons.length ? "unsupported" : "weakly supported",
    decision_guidance_label: missingReasons.length ? "not recommended due to missing evidence" : "paper observation preferred",
    recommended_operator_action_label: missingReasons.length ? "request more evidence" : "paper observe only",
    evidence_completeness: missingReasons.length ? `Incomplete (${missingReasons.length} missing)` : "Complete enough for review",
    main_missing_item: missingReasons[0] || "No critical missing evidence reported",
    trust: missingReasons.length ? "Incomplete" : confidence.includes("Moderate") ? "Moderate" : confidence.includes("Strong") ? "Strong" : "Weak",
    evidence_state: evidenceState,
    stability_state: stability,
    guidance,
    evidence_ids: evidenceIds.length ? evidenceIds.join(", ") : (candidate.source_artifact_path || "Supporting evidence ids unavailable."),
  };
}

function candidateConfidenceLabel(candidate = {}, evidenceQuality = "", missingReasons = []) {
  const raw = String(candidate.confidence || evidenceQuality || "").toLowerCase();
  if (missingReasons.length) return "Insufficient evidence";
  if (raw.includes("strong")) return "Strong longitudinal support";
  if (raw.includes("moderate")) return "Moderate historical support";
  if (raw.includes("weak") || raw.includes("low")) return "Weak evidence";
  if (raw.includes("complete") || raw.includes("ready")) return "Moderate historical support";
  return "Insufficient evidence";
}

function renderOpportunitiesSummaryStrip({ runtime = {}, candidates = {}, opportunities = {}, operatorActions = [], topCandidates = [], sleeves = {}, payload = {} }) {
  const today = payload.operator_today_projection || payload.today_projection || {};
  const active = payload.active_opportunity_projection || payload.opportunities_projection || {};
  const manual = payload.trade_ticket_projection_v1 || payload.trade_ticket_projection || payload.manual_capture_candidate || {};
  const manualCaptureCompleted = manual.captured_read_only === true
    || ["CAPTURED_HISTORICAL", "CAPTURED_MANUALLY"].includes(String(manual.lifecycle_state || manual.current_state || manual.historical_state || "").toUpperCase())
    || ["captured_manually", "partial"].includes(String(manual.manual_capture_record?.capture_status || manual.capture_status || "").toLowerCase());
  const projectedBlockedSleeves = today.blocked_sleeves_count ?? today.blocked_sleeves;
  const fallbackBlockedSleeves = safeList(opportunities.sleeve_run_summary || []).filter((row) => String(row.run_status || "").toUpperCase() === "BLOCKED").length
    || flattenCockpitSleeves(sleeves).filter((row) => String(row.bucket || row.recommendation || "").toUpperCase().includes("BLOCK")).length;
  const blockedSleevesNumber = Number(projectedBlockedSleeves);
  const blockedSleeves = Number.isFinite(blockedSleevesNumber) ? blockedSleevesNumber : fallbackBlockedSleeves;
  const projectedDataWarnings = today.data_warnings_count ?? today.data_warnings;
  const fallbackDataWarnings = safeList(opportunities.market_data_summary?.stale_symbols).length + safeList(opportunities.market_data_summary?.missing_symbols).length;
  const dataWarningsNumber = Number(projectedDataWarnings);
  const staleWarnings = Number.isFinite(dataWarningsNumber) ? dataWarningsNumber : fallbackDataWarnings;
  const fallbackSleeveCandidateCount = topCandidates.length || safeList(opportunities.open).length || (manual.selected_exposure_intent_id || manual.capture_record_id ? 1 : 0);
  const sleeveCandidateCount = Number(today.sleeve_candidate_count ?? today.candidate_count ?? active.candidate_count ?? fallbackSleeveCandidateCount);
  const currentRuntimeDay = today.current_runtime_day || payload.day_utc || "";
  const displayedArtifactDay = today.displayed_artifact_day || payload.displayed_artifact_day || currentRuntimeDay;
  const sameDisplayedDay = !currentRuntimeDay || !displayedArtifactDay || currentRuntimeDay === displayedArtifactDay;
  const completedCaptures = Number(today.completed_capture_count ?? today.completed_captures_today ?? active.completed_capture_count ?? (manualCaptureCompleted && sameDisplayedDay ? 1 : 0));
  const captureProjection = dashboardCaptureProjection(payload);
  const manualCaptureTickets = captureProjection.capture_ticket_count;
  const operatorReviewCount = Number(today.open_task_count ?? "") || operatorActions.length + safeList(candidates.review_required || candidates.awaiting_decision).length;
  return renderWorkflowCard({
    payload,
    sourceKey: "canonical_operator_state",
    fieldPath: "opportunities.summary_strip",
    whyShown: "Operators need current readiness, sleeve-candidate count, manual-capture tickets, blocked sleeves, review load, stale data, and run timestamp before opening details.",
    eyebrow: "OPERATOR_SUMMARY",
    title: "Today’s Operator Strip",
    subtitle: "Compact readiness and attention status. No execution controls are exposed.",
    body: `
      <div class="operator-summary-strip">
        ${renderMetricCard({ label: "Current runtime day", value: today.current_runtime_day || payload.day_utc || "unknown" })}
        ${renderMetricCard({ label: "Operational mode", value: today.runtime_mode || today.operational_mode || payload.runtime_mode || "unknown" })}
        ${renderMetricCard({ label: "Displayed artifact day", value: today.displayed_artifact_day || payload.displayed_artifact_day || payload.day_utc || "unknown" })}
        ${renderMetricCard({ label: "Current-day run status", value: today.current_day_run_status || today.readiness_status || today.readiness || runtime.highest_readiness_layer || "UNKNOWN" })}
        ${renderMetricCard({ label: "Current intraday candidates", value: String(today.current_intraday_candidate_count ?? today.current_day_candidate_count ?? sleeveCandidateCount) })}
        ${renderMetricCard({ label: "Final EOD certified candidates", value: String(today.final_eod_certified_candidate_count ?? 0) })}
        ${renderMetricCard({ label: "Historical candidates", value: String(today.historical_candidate_count ?? 0) })}
        ${renderMetricCard({ label: "Current-day completed captures", value: String(completedCaptures) })}
        ${renderMetricCard({ label: "Historical captures", value: String(today.historical_completed_capture_count ?? 0) })}
        ${renderMetricCard({ label: "Latest historical capture", value: String(today.latest_historical_capture_label || today.historical_latest_capture_day || (Number(today.historical_completed_capture_count ?? 0) ? displayedArtifactDay : 0)) })}
        ${renderMetricCard({ label: "IB capture tickets", value: String(manualCaptureTickets), detail: captureProjection.capture_ticket_status })}
        ${renderMetricCard({ label: "Blocked sleeves", value: String(blockedSleeves) })}
        ${renderMetricCard({ label: "Data warnings", value: String(staleWarnings) })}
        ${renderMetricCard({ label: "Latest run", value: formatTimestamp(payload.generated_at_utc || payload.generated_at || runtime.generated_at_utc) })}
      </div>
    `,
  });
}

function operatorSnapshotHasUnresolvedHealth(payload = {}) {
  const snapshot = payload.operator_state_snapshot || payload;
  const healthFacts = snapshot.health_facts || payload.health_facts || {};
  const validationStatus = String(snapshot.validation_result?.status || payload.validation_result?.status || "").toUpperCase();
  return Boolean(payload.projection_snapshot_mismatch)
    || (validationStatus && validationStatus !== "PASS")
    || Number(healthFacts.blocked_sleeve_count || healthFacts.blocked_sleeves_count || 0) > 0
    || Number(healthFacts.data_warning_count || healthFacts.data_warnings_count || 0) > 0
    || ["unavailable", "partial"].includes(String(healthFacts.runtime_truth_status || "").toLowerCase())
    || String(healthFacts.remediation_status || "").toLowerCase() === "still_blocked"
    || safeList(healthFacts.missing_artifacts).length > 0;
}

function renderOperatorAttentionQueue({ payload = {} }) {
  const projectedTasks = safeList(payload.operator_task_projection?.tasks).filter((task) =>
    task.owner_type === "operator" &&
    task.primary_command &&
    task.workflow_route &&
    task.ui_resolvable === true &&
    task.resolution_state === "open"
  );
  return renderWorkflowCard({
    payload,
    sourceKey: "operator_task_projection",
    fieldPath: "operator_task_projection.tasks",
    whyShown: "Only server-projected, operator-owned, UI-resolvable tasks appear here. Diagnostics, passive health, and informational changes are rendered elsewhere.",
    eyebrow: projectedTasks.length ? "MY_TASKS" : "NO_OPERATOR_TASKS",
    title: "My Tasks / Attention Queue",
    subtitle: "Human-owned decisions only. Every row has a direct governed command and workflow route.",
    body: renderSimpleTable({
      columns: [
        { label: "Priority", key: "priority" },
        { label: "Task", key: "title" },
        { label: "Why it matters", key: "why_it_matters" },
        { label: "Direct action", key: "direct_action" },
        { label: "Workflow", key: "resolution_workflow" },
      ],
      rows: projectedTasks,
      emptyMessage: payload.operator_task_projection?.no_action_message || "No operator-owned actionable workflow is open.",
    }),
  });
}

function renderSystemDiagnosticsPanel({ payload = {} }) {
  const diagnostics = safeList(payload.system_diagnostic_projection?.diagnostics);
  const unresolvedHealth = operatorSnapshotHasUnresolvedHealth(payload);
  const hasCritical = diagnostics.some((row) => ["critical", "high"].includes(String(row.severity || "").toLowerCase())) || unresolvedHealth;
  const diagnosticTitle = diagnostics.length ? `System Diagnostics (${diagnostics.length})` : "System Diagnostics";
  return renderWorkflowCard({
    payload,
    sourceKey: "system_diagnostic_projection",
    fieldPath: "system_diagnostic_projection.diagnostics",
    whyShown: "System diagnostics are separated from My Tasks because they describe infrastructure, data, runtime, or projection state rather than operator-owned candidate decisions.",
    eyebrow: diagnostics.length || unresolvedHealth ? "SYSTEM_DIAGNOSTICS_PRESENT" : "SYSTEM_CLEAR",
    title: diagnosticTitle,
    subtitle: hasCritical ? "Critical diagnostics require attention and are expanded because they can affect advisory confidence." : "Expandable diagnostics panel. No row is an operator task.",
    body: `
      <details ${hasCritical ? "open" : ""}>
        <summary>${escapeHtml(diagnostics.length ? `${diagnostics.length} diagnostic item(s)` : (unresolvedHealth ? "Projection mismatch — refresh required" : "No system diagnostics"))}</summary>
        ${renderSimpleTable({
          columns: [
            { label: "Severity", key: "severity" },
            { label: "Issue", render: (row) => escapeHtml(row.title || row.issue || "") },
            { label: "Affected area", render: (row) => escapeHtml(row.affected_area || row.affected_system || "") },
            { label: "Operator impact", key: "operator_impact" },
            { label: "Remediation", render: (row) => {
              const actions = safeList(row.remediation_actions).map((action) => action.label || action.command_id).join(", ");
              return escapeHtml(actions || row.system_managed_message || "System-managed issue; no operator remediation available.");
            } },
            { label: "Playbook", render: (row) => escapeHtml(row.remediation_playbook_id || row.remediation_summary?.playbook_used || "none") },
            { label: "Providers tried", render: (row) => escapeHtml(safeList(row.remediation_providers_tried || row.remediation_summary?.providers_tried).join(", ") || "none") },
            { label: "Validation", render: (row) => escapeHtml(safeList(row.remediation_validation_results || row.remediation_summary?.validation_results).join(", ") || "not attempted") },
            { label: "Remediation result", render: (row) => escapeHtml(row.remediation_message || row.remediation_summary?.message || "Approved deterministic remediation has not run yet.") },
          ],
          rows: diagnostics,
          emptyMessage: unresolvedHealth ? "Projection mismatch — refresh required" : "No runtime, market-data, source-artifact, or sleeve-input diagnostic is active.",
        })}
      </details>
    `,
  });
}

function renderWhatChangedPanel({ payload = {} }) {
  const changed = safeList(payload.what_changed_projection?.events);
  return renderWorkflowCard({
    payload,
    sourceKey: "what_changed_projection",
    fieldPath: "what_changed_projection.events",
    whyShown: "What Changed is informational only. Actions, when required, are emitted separately as operator-owned tasks.",
    eyebrow: changed.length ? "WHAT_CHANGED" : (operatorSnapshotHasUnresolvedHealth(payload) ? "SYSTEM_DIAGNOSTICS_PRESENT" : "NO_CHANGE"),
    title: "What Changed",
    subtitle: "Informational timeline. No row is a required action.",
    body: renderSimpleTable({
      columns: [
        { label: "Change", key: "title" },
        { label: "Summary", key: "plain_english_summary" },
        { label: "Action required", render: (row) => escapeHtml(row.action_required ? "Yes" : "No") },
      ],
      rows: changed,
      emptyMessage: "No material candidate, evidence, sleeve, or paper-trial change was detected.",
    }),
  });
}

function renderPassiveHealthStrip({ payload = {} }) {
  const healthRows = safeList(payload.passive_health_projection?.health);
  return renderWorkflowCard({
    payload,
    sourceKey: "passive_health_projection",
    fieldPath: "passive_health_projection.health",
    whyShown: "Passive health is compact status only. It never emits pseudo-actions.",
    eyebrow: "PASSIVE_HEALTH",
    title: "Health Status",
    subtitle: "Readiness, advisory, data, and blocked-sleeve status. No actions are attached here.",
    body: `
      <div class="operator-summary-strip">
        ${healthRows.map((row) => renderMetricCard({ label: row.label || row.health_type, value: String(row.value ?? "unknown") })).join("")}
      </div>
    `,
  });
}

function renderEodOutcomeLedgerPanel({ payload = {} }) {
  const report = payload.eod_opportunity_outcome_report && typeof payload.eod_opportunity_outcome_report === "object" ? payload.eod_opportunity_outcome_report : {};
  const summary = report.aggregate_summary || {};
  const bucketCounts = summary.bucket_counts || {};
  const rows = safeList(report.sleeve_outcomes);
  return renderWorkflowCard({
    payload,
    sourceKey: "eod_opportunity_outcome_report",
    fieldPath: "eod_opportunity_outcome_report.sleeve_outcomes",
    whyShown: "Every sleeve receives a deterministic EOD outcome, including no-signal, rejected, unsupported, blocked, and operator-eligible states.",
    eyebrow: "EOD_OUTCOME_LEDGER",
    title: "What happened today?",
    subtitle: "Research-only sleeve outcome ledger. No broker execution, no live trading, no sleeve mutation.",
    body: `
      <div class="operator-summary-strip">
        ${renderMetricCard({ label: "Sleeves expected", value: String(summary.sleeves_expected ?? rows.length) })}
        ${renderMetricCard({ label: "Sleeves ran", value: String(summary.sleeves_ran ?? 0) })}
        ${renderMetricCard({ label: "Blocked", value: String(report.blocked_sleeve_count ?? bucketCounts.BLOCKED_DATA_OR_INPUT ?? 0) })}
        ${renderMetricCard({ label: "No signal", value: String(report.no_signal_count ?? bucketCounts.NO_RAW_SIGNAL ?? 0) })}
        ${renderMetricCard({ label: "Rejected", value: String(report.rejected_count ?? 0) })}
        ${renderMetricCard({ label: "Unsupported observations", value: String(report.unsupported_observation_count ?? 0) })}
        ${renderMetricCard({ label: "Operator eligible", value: String(report.operator_eligible_count ?? 0) })}
        ${renderMetricCard({ label: "Manual captures", value: String(report.manual_capture_count ?? 0) })}
      </div>
      ${renderSimpleTable({
        columns: [
          { label: "Sleeve", key: "sleeve_id" },
          { label: "Outcome bucket", key: "primary_outcome_bucket" },
          { label: "Explanation", key: "explanation" },
          { label: "Flags", render: (row) => safeList(row.secondary_flags).join(", ") || "none" },
        ],
        rows,
        emptyMessage: "No EOD sleeve outcome report is available yet.",
      })}
    `,
  });
}

function candidateAgeLabel(row = {}) {
  const generated = row.generated_at || row.generated_at_utc || "";
  if (!generated) return "unknown";
  return formatTimestamp(generated);
}

function renderCandidateActionButtons(candidate = {}) {
  const id = candidate.candidate_id || candidate.id || "CANDIDATE_ID";
  const prefix = safeDomId(`candidate-${id}`);
  const allowedCommands = safeList(candidate.candidate_decision_projection?.allowed_commands);
  const allowed = allowedCommands.length ? new Set(allowedCommands) : new Set(["RECORD_MANUAL_EXTERNAL_CAPTURE", "OPEN_EVIDENCE"]);
  const taskState = String(candidate.operator_task_state || candidate.operator_affordance || "NO_USER_ACTION").toUpperCase();
  const captureGuidance = String(candidate.capture_guidance || "NO_USER_ACTION").toUpperCase();
  const captureAllowed = captureGuidance === "MANUAL_IB_CAPTURE_RECOMMENDED" && allowed.has("RECORD_MANUAL_EXTERNAL_CAPTURE");
  const actions = captureAllowed
    ? [{ command: "RECORD_MANUAL_EXTERNAL_CAPTURE", label: "Record IB capture complete", ariaLabel: "Record manual IB capture", modalId: `${prefix}-capture`, kind: "capture", primary: true }]
    : [];
  return `
    <div class="candidate-action-row">
      ${actions.map((action) => `<button class="${action.primary ? "primary-button" : "ghost-button"} candidate-action-button" type="button" data-aegis-command-id="OPEN_IN_PAGE_MODAL" data-aegis-command-action-type="IN_PAGE_DETAIL" data-aegis-command-target-type="candidate" data-aegis-command-target-id="${escapeHtml(action.modalId)}" data-command-detail-target="${escapeHtml(action.modalId)}" data-candidate-open-modal="${escapeHtml(action.modalId)}">${escapeHtml(action.label)}</button>`).join("")}
      ${allowed.has("OPEN_EVIDENCE") ? `<a class="ghost-button candidate-action-button" href="#candidate-evidence-preview">Open Evidence</a>` : ""}
      <a class="ghost-button candidate-action-button" href="/aegis-edge-lab" data-aegis-command-id="OPEN_VALID_ROUTE" data-aegis-command-action-type="EXPAND_SECTION" data-aegis-command-target-type="navigation" data-aegis-command-target-id="aegis_edge_lab" data-route="/aegis-edge-lab">Open Sleeve</a>
      <a class="ghost-button candidate-action-button" href="/aegis-edge-lab" data-aegis-command-id="OPEN_VALID_ROUTE" data-aegis-command-action-type="EXPAND_SECTION" data-aegis-command-target-type="navigation" data-aegis-command-target-id="aegis_edge_lab_research_context" data-route="/aegis-edge-lab">Open Research Context</a>
    </div>
    ${actions.map((action) => renderManualCaptureDialog(candidate, action.modalId)).join("")}
    <details class="edge-cli-fallback"><summary>Advanced / CLI fallback</summary>${renderCandidateCommandSnippets(candidate)}</details>
  `;
}

function renderCandidateReviewDialog(candidate = {}, action = {}) {
  const id = candidate.candidate_id || candidate.id || "";
  const fingerprint = candidate.candidate_decision_projection?.projection_fingerprint || candidate.decision_support_brief?.content_hash || "";
  return `
    <dialog class="edge-action-modal candidate-action-modal" id="${escapeHtml(action.modalId)}">
      <form class="candidate-action-form" method="post">
        <input type="hidden" name="candidate_workflow_action" value="review">
        <input type="hidden" name="candidate_id" value="${escapeHtml(id)}">
        <input type="hidden" name="review_action" value="${escapeHtml(action.action)}">
        <input type="hidden" name="source_projection_fingerprint" value="${escapeHtml(fingerprint)}">
        <div class="edge-modal-header">
          <div>
            <div class="section-eyebrow">OPERATOR REVIEW</div>
            <h3>${escapeHtml(action.label)}</h3>
            <p>${escapeHtml(candidate.symbol || "Candidate")} · ${escapeHtml(candidate.sleeve_id || "Sleeve link unavailable")}</p>
          </div>
          <button class="icon-button" type="button" data-candidate-close-modal aria-label="Close">×</button>
        </div>
        <label class="edge-form-field"><span>Operator</span><input name="operator" value="David" required></label>
        <label class="edge-form-field"><span>Review note</span><textarea name="operator_note" required>${escapeHtml(action.note || "")}</textarea></label>
        <div class="edge-modal-safety">Review only. No broker execution, no order routing, no automatic approval, no autonomous action.</div>
        <div class="edge-modal-actions">
          <button class="ghost-button" type="button" data-candidate-close-modal>Cancel</button>
          <button class="primary-button" type="submit">Record Review</button>
        </div>
        <div class="edge-action-status" data-candidate-action-status></div>
      </form>
    </dialog>
  `;
}

function renderManualCaptureDialog(candidate = {}, modalId = "") {
  const id = candidate.candidate_id || candidate.id || "";
  const context = candidateTrustContext(candidate);
  const fingerprint = candidate.candidate_decision_projection?.projection_fingerprint || candidate.decision_support_brief?.content_hash || "";
  const unsupportedOrBlocked = ["unsupported", "blocked"].includes(String(context.trust_classification || "").toLowerCase());
  return `
    <dialog class="edge-action-modal candidate-action-modal" id="${escapeHtml(modalId)}">
      <form class="candidate-action-form" method="post">
        <input type="hidden" name="candidate_workflow_action" value="manual_capture">
        <input type="hidden" name="candidate_id" value="${escapeHtml(id)}">
        <input type="hidden" name="source_projection_fingerprint" value="${escapeHtml(fingerprint)}">
        <div class="edge-modal-header">
          <div>
            <div class="section-eyebrow">MANUAL IB CAPTURE RECOMMENDATION</div>
            <h3>Record IB capture complete</h3>
            <p>${escapeHtml(candidate.symbol || "Candidate")} · ${escapeHtml(candidate.sleeve_id || "Sleeve link unavailable")}</p>
          </div>
          <button class="icon-button" type="button" data-candidate-close-modal aria-label="Close">×</button>
        </div>
        ${unsupportedOrBlocked ? `<div class="edge-modal-safety"><strong>Manual capture is not recommended because supporting evidence is incomplete.</strong> Aegis is advisory-only and did not execute this trade. Recording is allowed only as an operator-declared external action.</div>
        <label class="edge-form-field"><span>Unsupported evidence acknowledgement</span><label><input name="unsupported_evidence_acknowledgement" type="checkbox" required> I understand Aegis evidence is incomplete and Aegis did not execute this trade.</label></label>` : ""}
        <div class="edge-modal-safety"><strong>Aegis did not execute this trade.</strong> This records an operator-declared external action only. Broker automation remains disabled.</div>
        <label class="edge-form-field"><span>Manually captured</span><select name="manually_captured" required><option value="true">yes</option><option value="false">no</option></select></label>
        <label class="edge-form-field"><span>Quantity</span><input name="quantity" type="number" min="1" step="1" required></label>
        <label class="edge-form-field"><span>Capture timestamp</span><input name="capture_timestamp" type="datetime-local" required></label>
        <label class="edge-form-field"><span>External execution venue (optional)</span><input name="external_execution_venue" placeholder="Broker/platform name, optional"></label>
        <label class="edge-form-field"><span>Operator notes</span><textarea name="operator_notes" required placeholder="What did you manually do outside Aegis, and why?"></textarea></label>
        <label class="edge-form-field"><span>Confidence override (optional)</span><input name="confidence_override" placeholder="Optional operator confidence note"></label>
        <label class="edge-form-field"><span>Paper trade only</span><select name="paper_trade_only" required><option value="true">true</option><option value="false">false</option></select></label>
        <input type="hidden" name="review_decision" value="MANUAL_CAPTURE_RECORDED">
        <label class="edge-form-field"><span>Operator</span><input name="operator" value="David" required></label>
        <div class="edge-modal-actions">
          <button class="ghost-button" type="button" data-candidate-close-modal>Cancel</button>
          <button class="primary-button" type="submit">Mark capture complete</button>
        </div>
        <div class="edge-action-status" data-candidate-action-status></div>
      </form>
    </dialog>
  `;
}

function renderCandidateEvidencePreview(candidate = {}) {
  const context = candidateTrustContext(candidate);
  return [
    `<section class="trust-context-panel">
      <h4>Should I manually capture this?</h4>
      <p><strong>${escapeHtml(context.guidance)}</strong></p>
      <h4>Why This Candidate Exists</h4>
      <p>${escapeHtml(context.why_exists)}</p>
      <h4>Evidence For</h4>
      ${renderDefinitionRows([
        { label: "Post-cost expectancy", value: context.expectancy },
        { label: "Event-study support", value: context.event_study },
        { label: "Win rate", value: candidateEvidenceValue(candidate, ["win_rate", "historical_win_rate"], "Win-rate evidence unavailable; this prevents a stronger trust classification." ) },
        { label: "Observation count", value: context.observation_count },
        { label: "Benchmark-relative result", value: candidateEvidenceValue(candidate, ["benchmark_relative_result", "excess_return"], "Benchmark-relative evidence unavailable.") },
      ])}
      <h4>Evidence Against / Missing</h4>
      ${renderEvidenceChecklist(context)}
      <h4>What weakens this candidate?</h4>
      ${renderLineList(safeList(context.weakening_factors || candidate.weakening_factors).length ? safeList(context.weakening_factors || candidate.weakening_factors) : [context.main_missing_item])}
      <h4>Sleeve Health</h4>
      ${renderDefinitionRows([
        { label: "Drift status", value: context.drift },
        { label: "Fragility status", value: context.fragility },
        { label: "Sleeve health", value: context.stability_state },
      ])}
      <h4>Review Window / Expiry</h4>
      <p>${escapeHtml(context.expected_holding_window || "No holding window was supplied by this sleeve. Use manual review only.")}</p>
      <h4>What would invalidate this setup?</h4>
      ${renderLineList(safeList(context.invalidation_conditions).length ? safeList(context.invalidation_conditions) : [
        "Setup invalid if review deadline passes or candidate expires.",
        "Setup invalid if market data freshness becomes stale.",
        "Setup invalid if the next candidate run reverses direction or removes the candidate.",
      ])}
      <h4>Recommended Operator Action</h4>
      <p>${escapeHtml(context.recommended_operator_action_label)}</p>
    </section>`,
    renderDefinitionRows([
      { label: "Trust classification", value: context.trust_classification_label },
      { label: "Decision guidance", value: context.decision_guidance_label },
      { label: "Related challenger context", value: context.challenger },
      { label: "Review notes", value: candidate.latest_operator_note || candidate.operator_note || "No operator review notes yet." },
      { label: "Key supporting evidence ids", value: context.evidence_ids },
    ]),
    `<div class="mini-chart-panel">
      <div><strong>Historical expectancy</strong><span>${escapeHtml(context.expectancy)}</span></div>
      <div><strong>Drift trend</strong><span>${escapeHtml(context.drift)}</span></div>
      <div><strong>Regime fragility</strong><span>${escapeHtml(context.fragility)}</span></div>
      <div><strong>Evidence quality trend</strong><span>${escapeHtml(context.evidence_quality)}</span></div>
    </div>`,
  ].join("");
}

function renderEvidenceChecklist(context = {}) {
  const rows = safeList(context.evidence_checklist || context.missing_evidence);
  return renderSimpleTable({
    columns: [
      { label: "Evidence item", render: (row) => escapeHtml(row.item || "evidence item") },
      { label: "Status", render: (row) => escapeHtml(row.status || "missing") },
      { label: "Why it matters", render: (row) => escapeHtml(row.why_it_matters || "Missing evidence lowers trust.") },
      { label: "Blocks recommendation", render: (row) => escapeHtml(row.blocks_manual_capture_recommendation === true ? "yes" : "no") },
      { label: "Source checked", render: (row) => escapeHtml(row.source_checked || "candidate projection") },
    ],
    rows,
    emptyMessage: "Evidence checklist unavailable; request more evidence before relying on this candidate.",
  });
}

function renderLineList(items = []) {
  return `<div class="line-list">${safeList(items).map((item) => `<div>${escapeHtml(String(item))}</div>`).join("")}</div>`;
}

function renderPositionManagementPanel(payload = {}, positions = {}, candidates = {}) {
  const rows = [
    ...safeList(positions.open_positions),
    ...safeList(positions.pending_risk_plan),
    ...safeList(positions.pending_stop_review),
    ...safeList(positions.stopped_positions),
    ...safeList(positions.corrected_position_events),
  ];
  const uniqueRows = [];
  const seen = new Set();
  for (const row of rows) {
    const key = `${row.candidate_id || ""}:${row.stop_status || ""}`;
    if (seen.has(key)) continue;
    seen.add(key);
    uniqueRows.push(row);
  }
  const approvedCount = safeList(candidates.approved_or_traded).length;
  return renderWorkflowCard({
    payload,
    sourceKey: "position_management",
    fieldPath: "positions",
    whyShown: "Approved or manually traded candidates need append-only risk plans, stop event records, corrections, and final outcome follow-up.",
    eyebrow: uniqueRows.length ? "POSITION_MANAGEMENT" : approvedCount ? "RISK_PLAN_PENDING" : "NO_POSITIONS",
    title: "Position Management",
    subtitle: "Aegis tracks risk plans and stop events only. No broker execution or automatic stop execution is available.",
    body: `
      ${renderSimpleTable({
        columns: [
          { label: "Candidate", render: (row) => `<strong>${escapeHtml(row.symbol || row.candidate_id || "Candidate")}</strong><div class="muted-mini">${escapeHtml(row.candidate_id || "candidate id unavailable")}</div>` },
          { label: "Sleeve", key: "sleeve_id" },
          { label: "Entry price", render: (row) => escapeHtml(fieldValue(row.entry_price)) },
          { label: "Quantity", render: (row) => escapeHtml(fieldValue(row.quantity)) },
          { label: "Stop price", render: (row) => escapeHtml(fieldValue(row.stop_price)) },
          { label: "Max planned loss", render: (row) => escapeHtml(fieldValue(row.max_planned_loss)) },
          { label: "Risk plan status", render: (row) => escapeHtml(row.stop_status === "PENDING_RISK_PLAN" ? "risk plan pending" : "risk plan recorded") },
          { label: "Stop event status", render: (row) => escapeHtml(row.stop_status || "PENDING_RISK_PLAN") },
          { label: "Actions", render: (row) => renderPositionManagementActions(row) },
        ],
        rows: uniqueRows,
        emptyMessage: "No approved or traded candidate needs position management yet.",
      })}
      <details class="journal-audit-details">
        <summary>Advanced CLI fallback</summary>
        <pre><code>${escapeHtml(positionManagementCliFallback(uniqueRows[0] || safeList(candidates.approved_or_traded)[0] || {}))}</code></pre>
      </details>
    `,
  });
}

function renderPositionManagementActions(row = {}) {
  const candidateId = row.candidate_id || "CANDIDATE_ID";
  return `
    <div class="candidate-action-row">
      <button class="ghost-button candidate-action-button" type="button" data-api-endpoint="/api/aegis/position/risk-plan" data-candidate-id="${escapeHtml(candidateId)}">Add Risk Plan</button>
      <button class="ghost-button candidate-action-button" type="button" data-api-endpoint="/api/aegis/position/stop-event" data-candidate-id="${escapeHtml(candidateId)}">Record Stop Event</button>
      <button class="ghost-button candidate-action-button" type="button" data-api-endpoint="/api/aegis/position/correction" data-candidate-id="${escapeHtml(candidateId)}">Correct Risk Plan</button>
      <button class="ghost-button candidate-action-button" type="button" data-api-endpoint="/api/aegis/position/final-outcome" data-candidate-id="${escapeHtml(candidateId)}">Record Final Outcome</button>
    </div>
  `;
}

function positionManagementCliFallback(row = {}) {
  const candidateId = row.candidate_id || "CANDIDATE_ID";
  return [
    `npm run aegis:record-position-risk-plan -- --candidate-id ${candidateId} --quantity 25 --entry-price 100 --stop-type HARD_STOP --stop-price 96 --operator David --reason "risk plan"`,
    `npm run aegis:record-stop-event -- --candidate-id ${candidateId} --stop-triggered true --exit-price 95.80 --exit-reason STOPPED_OUT --operator David --reason "Manual stop honored"`,
    `npm run aegis:correct-position-event -- --candidate-id ${candidateId} --field stop_price --new-value 95.50 --operator David --reason "Correct entered stop"`,
    "No broker execution. No autonomous execution. No automatic order placement. No automatic stop execution.",
  ].join("\n");
}

function renderCandidateReviewLedgerPanel(payload, opportunities = {}) {
  return renderWorkflowCard({
    payload,
    sourceKey: "candidate_review_ledger",
    fieldPath: "opportunities.candidate_review_ledger",
    whyShown: "The review ledger separates active review candidates from dismissed or expired history while preserving append-only audit events.",
    eyebrow: "REVIEW_LEDGER",
    title: "Review Ledger",
    subtitle: "Watchlisted, dismissed, needs-more-evidence, and expired review candidates remain inspectable. No review action executes or approves trades.",
    body: renderCandidateReviewLedgerTables(opportunities),
  });
}

function renderCandidateReviewLedgerTables(opportunities = {}) {
  const ledger = opportunities.candidate_review_ledger || {};
  const allRows = safeList(opportunities.candidate_review_ledger_rows || ledger.candidates);
  const activeRows = safeList(opportunities.candidate_review_active_rows).length
    ? safeList(opportunities.candidate_review_active_rows)
    : allRows.filter((row) => row.active === true);
  const historicalRows = safeList(opportunities.candidate_review_historical_rows).length
    ? safeList(opportunities.candidate_review_historical_rows)
    : allRows.filter((row) => row.active === false);
  const filterLine = [
    `active ${ledger.active_count ?? activeRows.length}`,
    `watchlisted ${ledger.watchlisted_count ?? 0}`,
    `needs_more_evidence ${ledger.needs_more_evidence_count ?? 0}`,
    `dismissed ${ledger.dismissed_count ?? 0}`,
    `expired ${ledger.expired_count ?? 0}`,
    `expired watchlisted ${ledger.expired_watchlisted_count ?? 0}`,
    `all ${ledger.candidate_count ?? allRows.length}`,
  ].join(" · ");
  const columns = [
    { label: "Candidate", key: "candidate_id" },
    { label: "Symbol", key: "symbol" },
    { label: "Sleeve", key: "sleeve_id" },
    { label: "Direction", key: "direction" },
    { label: "Review state", key: "current_review_state" },
    { label: "Expired from", render: (row) => escapeHtml(row.expired_from_review_state || "n/a") },
    { label: "Expiry", key: "expiry" },
    { label: "Latest note", key: "latest_operator_note" },
    { label: "Audit actions", key: "audit_action_count" },
    { label: "Executable?", render: (row) => escapeHtml(row.no_execution_status?.executable_status || "NON_EXECUTABLE") },
    { label: "History", render: (row) => renderReviewActionHistory(row) },
  ];
  return [
    `<div class="callout warning">Review ledger is audit-only. It exposes no approve-to-trade, order routing, allocation, broker, or autonomous execution action.</div>`,
    `<div class="line-list" style="margin-top:8px;"><div>${escapeHtml(filterLine)}</div><div><code>npm run aegis:candidate-review-ledger -- --filter active</code></div></div>`,
    `<h3>Active Review Candidates</h3>`,
    renderSimpleTable({
      columns,
      rows: activeRows,
      emptyMessage: "No active review candidates.",
    }),
    `<h3>Review History</h3>`,
    renderSimpleTable({
      columns,
      rows: historicalRows,
      emptyMessage: "No dismissed or expired review candidates.",
    }),
  ].join("");
}

function renderReviewActionHistory(row = {}) {
  const history = safeList(row.audit_action_history);
  if (!history.length) return "No review actions";
  return `
    <details>
      <summary>${escapeHtml(String(history.length))} event${history.length === 1 ? "" : "s"}</summary>
      <div class="line-list">
        ${history.map((event) => `<div>${escapeHtml(event.timestamp_utc || "")} ${escapeHtml(event.action || "")}: ${escapeHtml(event.prior_state || "")} -> ${escapeHtml(event.new_state || "")} ${event.operator_note ? `(${escapeHtml(event.operator_note)})` : ""}</div>`).join("")}
      </div>
    </details>
  `;
}

function renderNoonPreflightAlert(opportunities = {}) {
  const alert = opportunities.noon_preflight_alert || {};
  if (alert.available !== true) {
    return "";
  }
  return renderWorkflowCard({
    payload: { freshness: {}, source_paths: {} },
    sourceKey: "noon_preflight",
    fieldPath: "opportunities.noon_preflight_alert",
    whyShown: "Noon preflight failed and operator email was not delivered.",
    eyebrow: "NOON_PREFLIGHT_ALERT",
    title: "Noon preflight failed and no email alert was delivered.",
    subtitle: "Candidate-generation readiness failed before the trading window.",
    body: `
      <div class="callout danger">
        <strong>Noon preflight failed and no email alert was delivered.</strong>
        <div>${escapeHtml(alert.reason || "Noon preflight failed.")}</div>
      </div>
      ${renderDefinitionRows([
        { label: "Timestamp", value: alert.timestamp || "Not reported" },
        { label: "Next action", value: alert.next_action || "npm run aegis:candidate-diagnostics" },
        { label: "Email status", value: alert.email_status || "UNKNOWN" },
        { label: "Setup", value: alert.setup_instructions || "Configure C2_EMAIL_SMTP_HOST, C2_EMAIL_SMTP_PORT, C2_EMAIL_USERNAME, C2_EMAIL_PASSWORD, C2_EMAIL_FROM, and C2_EMAIL_TO." },
      ])}
    `,
  });
}

function renderNoOpportunityExplanation(opportunities = {}) {
  const explanation = opportunities.no_opportunity_explanation || {};
  const diagnostics = opportunities.diagnostics || {};
  if (!diagnostics.schema_id && explanation.reason === "CANDIDATE_DIAGNOSTICS_MISSING") {
    return renderMissingOpportunityDiagnostics(explanation);
  }
  if (!diagnostics.schema_id && !explanation.available) {
    return renderMissingOpportunityDiagnostics({
      title: "Candidate diagnostics missing.",
      message: "Run candidate diagnostics before claiming there are no opportunities.",
      recommended_next_steps: ["Run candidate diagnostics."],
    });
  }
  const interpretation = explanation.operator_interpretation || diagnostics.operator_interpretation || "UNKNOWN";
  const message = noOpportunityMessage(interpretation, explanation.message || diagnostics.zero_candidate_explanation);
  const whyZero = explanation.why_zero || explanation.message || diagnostics.zero_candidate_explanation || message;
  const summary = explanation.summary || {
    sleeves_evaluated: diagnostics.total_sleeves_expected || 0,
    sleeves_run: diagnostics.total_sleeves_run || 0,
    raw_signals: diagnostics.total_raw_signals || 0,
    candidates_generated: diagnostics.total_candidates_generated || 0,
    candidates_rejected: diagnostics.total_candidates_rejected || 0,
  };
  const rejectedRows = safeList(opportunities.rejected_candidate_summary);
  const rejectionReasons = rejectedRows.length
    ? rejectedRows
    : safeList(opportunities.sleeve_run_summary).flatMap((row) => safeList(row.rejection_reasons).map((reason) => ({ sleeve_id: row.sleeve_id, reason })));
  const trigger = opportunities.trigger_summary || diagnostics.trigger_evaluation || {};
  const nextSteps = safeList(explanation.recommended_next_steps || diagnostics.recommended_next_steps);
  const readinessRows = safeList(opportunities.sleeve_readiness_summary || diagnostics.per_sleeve_readiness);
  const globalContextRows = safeList(opportunities.global_context_summary);
  const marketDataSummary = opportunities.market_data_summary || diagnostics.market_data_summary || {};
  const noonPreflight = opportunities.noon_preflight || {};
  const providerConfig = marketDataSummary.provider_config || {};
  const providerConfigured = providerConfig.configured === true || Boolean(providerConfig.primary || providerConfig.fallback);
  const missingSymbols = safeList(marketDataSummary.missing_symbols);
  const staleSymbols = safeList(marketDataSummary.stale_symbols);
  const providerIssueMessage = !providerConfigured
    ? "Market data provider is not configured. Aegis cannot evaluate sleeve opportunities."
    : missingSymbols.length
      ? "Candidate generation blocked because required symbols are missing current data."
      : staleSymbols.length
        ? "Candidate generation blocked because required symbols are stale."
        : "";
  const expectedRunLine = `${summary.sleeves_evaluated ?? diagnostics.total_sleeves_expected ?? 0} sleeves expected, ${summary.sleeves_run ?? diagnostics.total_sleeves_run ?? 0} ran`;
  const partialRunCompleted = interpretation === "PARTIAL_RUN" || explanation.partial_run_completed === true;
  return [
    `<div class="empty-state">
      <strong>No opportunities today.</strong>
      <div>${partialRunCompleted ? "Partial run completed" : "Candidate run completed"}</div>
      <div>0 opportunities generated</div>
      <div>${escapeHtml(expectedRunLine)}</div>
      <div>${escapeHtml(message)}</div>
    </div>`,
    renderNoOpportunitySection({
      eyebrow: interpretation,
      title: "Summary",
      subtitle: "Candidate-generation diagnostics explain whether zero candidates is healthy or suspicious.",
      body: renderDefinitionRows([
        { label: "Sleeves evaluated", value: String(summary.sleeves_evaluated ?? 0) },
        { label: "Sleeves ready", value: String(diagnostics.total_sleeves_ready ?? 0) },
        { label: "Sleeves ready with warnings", value: String(diagnostics.total_sleeves_ready_with_warnings ?? 0) },
        { label: "Sleeves blocked", value: String(diagnostics.total_sleeves_blocked ?? 0) },
        { label: "Sleeves run", value: String(summary.sleeves_run ?? 0) },
        { label: "Raw signals rejected", value: String(summary.candidates_rejected ?? 0) },
        { label: "Candidates passed filters", value: String(summary.candidates_generated ?? 0) },
        { label: "Why zero", value: whyZero },
        { label: "Candidate generation", value: diagnostics.candidate_generation_status || explanation.candidate_generation_status || "UNKNOWN" },
        { label: "Interpretation", value: interpretation },
      ]),
    }),
    renderNoOpportunitySection({
      eyebrow: "MARKET_DATA",
      title: "Market Data Provider",
      subtitle: providerIssueMessage || "Provider configuration and symbol freshness for candidate-generation inputs.",
      body: [
        renderDefinitionRows([
          { label: "Provider configured", value: providerConfigured ? "yes" : "no" },
          { label: "Primary provider", value: providerConfig.primary || "not configured" },
          { label: "Fallback provider", value: providerConfig.fallback || "not configured" },
          { label: "Market data status", value: marketDataSummary.status || "UNKNOWN" },
          { label: "Universe mode", value: marketDataSummary.runtime_universe_mode || "unknown" },
          { label: "Dataset snapshot", value: marketDataSummary.production_scan_dataset_id || marketDataSummary.dataset_snapshot_id || "none" },
          { label: "Production scan universe", value: String(marketDataSummary.production_scan_universe_count ?? 0) },
          { label: "Sleeve required symbols", value: String(marketDataSummary.sleeve_required_symbol_count ?? 0) },
          { label: "Total requested symbols", value: String(marketDataSummary.total_requested_symbol_count ?? safeList(marketDataSummary.requested_symbols).length) },
          { label: "Universe source", value: marketDataSummary.requested_symbols_source || "unknown" },
          { label: "Requested symbols", value: safeList(marketDataSummary.requested_symbols).join(", ") || "none" },
          { label: "Current symbols", value: safeList(marketDataSummary.fetched_symbols).join(", ") || "none" },
          { label: "Missing symbols", value: missingSymbols.join(", ") || "none" },
          { label: "Stale symbols", value: staleSymbols.join(", ") || "none" },
          { label: "Mapping missing", value: safeList(marketDataSummary.mapping_missing_symbols).join(", ") || "none" },
          { label: "Provider failed symbols", value: safeList(marketDataSummary.provider_failed_symbols).join(", ") || "none" },
          { label: "Failure reason", value: marketDataSummary.failure_reason || "none" },
          { label: "Next step", value: providerConfigured ? "place manual CSV drop, then run npm run aegis:repair-candidate-readiness" : "configure provider env, or place manual CSV drop, then run npm run aegis:repair-candidate-readiness" },
        ]),
        `<pre class="code-block">npm run aegis:repair-candidate-readiness</pre>`,
      ].join(""),
    }),
    renderNoOpportunitySection({
      eyebrow: "NOON_PREFLIGHT",
      title: "Noon Preflight Status",
      subtitle: "Preflight distinguishes READY_FULL, READY_PARTIAL, and BLOCKED candidate generation states.",
      body: renderDefinitionRows([
        { label: "Readiness status", value: noonPreflight.readiness_status || "UNKNOWN" },
        { label: "Preflight status", value: noonPreflight.status || "UNKNOWN" },
        { label: "Candidate generation ready", value: String(noonPreflight.candidate_generation_ready === true) },
        { label: "Data ready", value: String(noonPreflight.data_ready === true) },
        { label: "Email status", value: noonPreflight.email_status || "UNKNOWN" },
        { label: "Alert reason", value: noonPreflight.alert_reason || "Not reported" },
      ]),
    }),
    renderNoOpportunitySection({
      eyebrow: "SLEEVE_READINESS",
      title: "Sleeve Readiness Summary",
      subtitle: "Per-sleeve input contracts decide candidate-generation readiness.",
      body: renderSimpleTable({
        columns: [
          { label: "Sleeve", key: "sleeve_id" },
          { label: "Readiness", render: (row) => escapeHtml(row.readiness || "UNKNOWN") },
          { label: "Required missing input", render: (row) => escapeHtml(safeList(row.blocking_inputs).join(", ") || "none") },
          { label: "Optional missing input", render: (row) => escapeHtml(safeList(row.warning_inputs).join(", ") || "none") },
          { label: "Reason", render: (row) => escapeHtml(row.reason || "") },
        ],
        rows: readinessRows,
        emptyMessage: "No sleeve readiness rows are available.",
      }),
    }),
    renderNoOpportunitySection({
      eyebrow: "SLEEVE_RUN_STATUS",
      title: "Sleeve Run Status",
      subtitle: "Per-sleeve status for candidate generation and filtering.",
      body: renderSimpleTable({
        columns: [
          { label: "Sleeve", key: "sleeve_id" },
          { label: "Status", render: (row) => escapeHtml(row.run_status || "UNKNOWN") },
          { label: "Readiness", render: (row) => escapeHtml(row.readiness || "UNKNOWN") },
          { label: "Blocker", render: (row) => escapeHtml(row.canonical_blocker || row.reason_no_candidate || "none") },
          { label: "Warning", render: (row) => escapeHtml(safeList(row.warning_inputs).join(", ") || "none") },
          { label: "Required missing input", render: (row) => escapeHtml(safeList(row.blocking_inputs || row.missing_inputs).join(", ") || "none") },
          { label: "Optional missing input", render: (row) => escapeHtml(safeList(row.warning_inputs).join(", ") || "none") },
          { label: "Next action", render: (row) => escapeHtml(row.next_repair_action || "No action needed.") },
          { label: "Raw signals", key: "raw_signal_count" },
          { label: "Candidates", key: "candidate_count" },
          { label: "Rejected", key: "rejected_count" },
        ],
        rows: safeList(opportunities.sleeve_run_summary || diagnostics.sleeves),
        emptyMessage: "No sleeve diagnostics are available.",
      }),
    }),
    renderNoOpportunitySection({
      eyebrow: "GLOBAL_CONTEXT",
      title: "Global Context",
      subtitle: "Global context; not necessarily required by every sleeve.",
      body: renderSimpleTable({
        columns: [
          { label: "Item", key: "data_item_id" },
          { label: "Status", key: "status" },
          { label: "Provider", key: "provider" },
          { label: "Timestamp", key: "data_timestamp_utc" },
          { label: "Notes", render: (row) => escapeHtml(safeList(row.notes).join(", ") || row.label || "") },
        ],
        rows: globalContextRows,
        emptyMessage: "No global context registry rows are available.",
      }),
    }),
    renderNoOpportunitySection({
      eyebrow: "REJECTION_REASONS",
      title: "Rejection Reasons",
      subtitle: "Raw signals rejected during lifecycle, promotion, portfolio gate, or ranking stay visible and separate from blocked sleeves. Examples: volatility filter, confidence threshold, regime mismatch, insufficient data, missing event packet.",
      body: renderSimpleTable({
        columns: [
          { label: "Sleeve", key: "sleeve_id" },
          { label: "Raw signal", render: (row) => escapeHtml(row.raw_signal_id || row.candidate_id || "") },
          { label: "Stage", render: (row) => escapeHtml(row.rejection_stage || "") },
          { label: "Reason", render: (row) => escapeHtml(row.rejection_reason || row.reason || "") },
          { label: "Explanation", render: (row) => escapeHtml(row.human_readable_explanation || "") },
          { label: "Required next action", render: (row) => escapeHtml(row.required_next_action || "") },
          { label: "Classification", render: (row) => escapeHtml(row.rejection_classification || "") },
          { label: "Rejected", key: "rejected_count" },
        ],
        rows: rejectionReasons,
        emptyMessage: "No rejected raw signals were reported.",
      }),
    }),
    renderNoOpportunitySection({
      eyebrow: "TRIGGER_EVENT_STATUS",
      title: "Trigger/Event Status",
      subtitle: "Event and regime trigger evaluation status.",
      body: renderDefinitionRows([
        { label: "Triggers ran", value: String(trigger.ran === true) },
        { label: "Triggers evaluated", value: String(trigger.trigger_count ?? 0) },
        { label: "Activated", value: String(trigger.activated_count ?? 0) },
        { label: "Skipped", value: String(trigger.skipped_count ?? 0) },
        { label: "Reason", value: trigger.reason || "Not reported" },
      ]),
    }),
    renderNoOpportunitySection({
      eyebrow: "NEXT_STEP",
      title: "Recommended Next Step",
      subtitle: "Operator action remains manual and diagnostic only.",
      body: nextSteps.length ? `<div class="line-list">${nextSteps.map((step) => `<div>${escapeHtml(step)}</div>`).join("")}</div>` : `<div class="empty-state">No action needed.</div>`,
    }),
  ].join("");
}

function renderMissingOpportunityDiagnostics(explanation = {}) {
  return renderNoOpportunitySection({
    eyebrow: "DIAGNOSTICS_MISSING",
    title: "Candidate diagnostics missing.",
    subtitle: "The UI will not claim there are no opportunities until candidate diagnostics exist.",
    body: [
      `<div class="empty-state">${escapeHtml(explanation.message || "Run candidate diagnostics before claiming there are no opportunities.")}</div>`,
      `<pre class="code-block">npm run aegis:candidate-diagnostics</pre>`,
    ].join(""),
  });
}

function renderNoOpportunitySection({ eyebrow = "", title = "", subtitle = "", body = "" }) {
  return `
    <section class="support-note no-opportunity-section">
      ${eyebrow ? `<div class="eyebrow">${escapeHtml(eyebrow)}</div>` : ""}
      <h3>${escapeHtml(title)}</h3>
      ${subtitle ? `<p>${escapeHtml(subtitle)}</p>` : ""}
      ${body}
    </section>
  `;
}

function noOpportunityMessage(interpretation, fallback = "") {
  const normalized = String(interpretation || "").toUpperCase();
  if (normalized === "NORMAL_NO_SIGNAL") return "Candidate generation ran. No sleeves produced qualifying candidates.";
  if (normalized === "PARTIAL_RUN") return "Partial run: some sleeves were blocked, but ready sleeves were allowed to run.";
  if (normalized === "CONTRACT_BLOCKED") return "No candidates because one or more sleeve input contracts are missing.";
  if (normalized === "SUSPICIOUS_NO_SIGNAL" || normalized === "ENGINE_NOT_RUN") return "No candidates found, but candidate generation did not fully run.";
  if (normalized === "DATA_BLOCKED") return "No opportunities because candidate generation could not run.";
  if (normalized === "PARTIAL_CONTEXT") return "Only part of the candidate pipeline ran.";
  return fallback || "No candidates found, but candidate generation status is unknown.";
}

function renderCandidateStateTable(rows, emptyMessage = "No candidate rows are available.") {
  return renderSimpleTable({
    columns: [
      { label: "Candidate", key: "candidate_id" },
      { label: "Symbol", key: "symbol" },
      { label: "Sleeve", key: "sleeve_id" },
      { label: "Decision", key: "current_operator_decision" },
      { label: "Shares", key: "current_intended_shares" },
      { label: "Corrections", key: "correction_count" },
      { label: "Outcome", key: "outcome_status" },
    ],
    rows: safeList(rows).slice(0, 24),
    emptyMessage,
  });
}

function renderCandidateCommandSnippets(candidate = {}) {
  const id = candidate.candidate_id || "CANDIDATE_ID";
  return `
    <details>
      <summary>CLI</summary>
      <div class="line-list">
        <code>npm run aegis:record-candidate-review -- --candidate-id ${escapeHtml(id)} --action watchlist --operator OPERATOR --operator-note "Monitor this setup"</code>
        <code>npm run aegis:record-candidate-review -- --candidate-id ${escapeHtml(id)} --action dismiss --operator OPERATOR --operator-note "Not taking this setup"</code>
        <code>npm run aegis:record-candidate-review -- --candidate-id ${escapeHtml(id)} --action needs-more-evidence --operator OPERATOR --operator-note "Need more evidence before review"</code>
        <code>npm run aegis:record-candidate-review -- --candidate-id ${escapeHtml(id)} --action add-note --operator OPERATOR --operator-note "Operator note"</code>
        <span>Review actions are audit-only. They do not approve, route, allocate, or execute orders.</span>
      </div>
    </details>
  `;
}

function filterOperatorActions(actions = []) {
  return safeList(actions).filter((action) => {
    const priority = String(action.priority || "").toUpperCase();
    const type = String(action.type || "").toUpperCase();
    if (type === "RUNTIME_REVIEW" && !["CRITICAL", "HIGH"].includes(priority)) return false;
    if (["CRITICAL", "HIGH", "MEDIUM"].includes(priority)) return true;
    return [
      "CANDIDATE_DECISION",
      "CANDIDATE_REVIEW",
      "CANDIDATE_OUTCOME",
      "GOVERNANCE_APPROVAL",
      "MISSING_CANONICAL_STATE",
      "SLEEVE_REVIEW",
      "RESEARCH_REVIEW",
      "RUNTIME_REVIEW",
    ].includes(type);
  });
}

function runtimeStatusMessage(runtimeClass) {
  if (runtimeClass === "REAL_RUNTIME") return "Runtime evidence is current for the target operating mode.";
  if (runtimeClass === "PARTIAL_CONTEXT") return "Runtime evidence is partial today. Refresh evidence if you expect live/current data.";
  if (runtimeClass === "BLOCKED") return "Runtime is blocked. Resolve the listed blocker before relying on today's workflow.";
  return "Runtime evidence state is unknown. Refresh runtime truth if this is unexpected.";
}

function triggerOperatorImpact(row = {}) {
  const count = Number(row.candidate_count || 0);
  const status = String(row.status || row.decision || "").toUpperCase();
  if (count > 0) return "CANDIDATES_GENERATED";
  if (["INSUFFICIENT_EVIDENCE", "NO_CONFIRMED_SLEEVE", "NO_MAPPING"].includes(status)) return "INSUFFICIENT_EVIDENCE";
  if (["REVIEW_REQUIRED", "PARTIAL"].includes(status)) return "REVIEW_REQUIRED";
  return "NO_ACTION";
}

function renderTriggeredEventSummary(eventTriggers = []) {
  const rows = safeList(eventTriggers).map((row) => ({
    ...row,
    operator_impact: triggerOperatorImpact(row),
    skipped_triggers: safeList(row.skipped_triggers).join(", ") || row.reason_skipped || "",
    last_evaluated_at: row.completed_at || row.generated_at || row.started_at || row.detected_at || "",
  }));
  if (!rows.length) {
    return `<div class="empty-state">No material event/regime-triggered sleeve action.</div>`;
  }
  return renderSimpleTable({
    columns: [
      { label: "Trigger", render: (row) => escapeHtml(row.trigger_key || row.trigger_id || row.detected_condition || "UNKNOWN") },
      { label: "Detected condition", key: "detected_condition" },
      { label: "Selected sleeves", key: "selected_sleeve_ids", render: (row) => safeList(row.selected_sleeve_ids).join(", ") || "none" },
      { label: "Candidates", key: "candidate_count" },
      { label: "Skipped", key: "skipped_triggers" },
      { label: "Last evaluated", key: "last_evaluated_at" },
      { label: "Operator impact", key: "operator_impact" },
    ],
    rows: rows.slice(0, 12),
    emptyMessage: "No material event/regime-triggered sleeve action.",
  });
}

function renderResearchTaskTable(rows) {
  return renderSimpleTable({
    columns: [
      { label: "Task", key: "task_id" },
      { label: "Title", key: "title" },
      { label: "Status", key: "status" },
      { label: "Next", key: "recommended_next_step" },
    ],
    rows: safeList(rows).slice(0, 16),
    emptyMessage: "No research rows are available.",
  });
}

function renderResearchHypothesisTable(rows) {
  return renderSimpleTable({
    columns: [
      { label: "Hypothesis", key: "hypothesis_id" },
      { label: "Title", key: "title" },
      { label: "Status", key: "status" },
      { label: "Normalized", key: "normalized_status" },
      { label: "Stage", render: (row) => escapeHtml(row.lifecycle_stage || row.stage || hypothesisStage(row)) },
      { label: "Class", key: "classification" },
      { label: "Edge", render: (row) => escapeHtml(row.edge_type || row.edge_family || "Not enough current evidence yet.") },
      { label: "Next Step", render: (row) => escapeHtml(row.next_step || hypothesisNextStep(row)) },
      { label: "Source", render: (row) => row.legacy_source ? "legacy" : (row.source || "canonical") },
      { label: "Artifact", key: "source_artifact" },
    ],
    rows: safeList(rows).slice(0, 24),
    emptyMessage: "No captured hypotheses are available.",
  });
}

function hypothesisStage(row = {}) {
  const status = String(row.normalized_status || row.status || row.classification || "UNKNOWN").toUpperCase();
  if (["IDEA", "ACTIVE", "PROPOSED"].includes(status)) return "IDEA";
  if (["QUEUED", "RESEARCH_QUEUED"].includes(status)) return "RESEARCH_QUEUED";
  if (status === "DATA_NEEDED") return "DATA_NEEDED";
  if (status === "BACKTEST_READY") return "BACKTEST_READY";
  if (status === "BACKTEST_RUNNING") return "BACKTEST_RUNNING";
  if (["VALIDATED", "COMPLETED", "BACKTEST_COMPLETE"].includes(status)) return "BACKTEST_COMPLETE";
  if (["REVIEW_REQUIRED", "RESULT_REVIEW_REQUIRED"].includes(status)) return "REVIEW_REQUIRED";
  if (status === "REJECTED") return "REJECTED";
  if (status === "ARCHIVED") return "ARCHIVED";
  return "NEEDS_MORE_EVIDENCE";
}

function hypothesisNextStep(row = {}) {
  const stage = hypothesisStage(row);
  const nextByStage = {
    IDEA: "queue research",
    RESEARCH_QUEUED: "attach dataset",
    DATA_NEEDED: "attach dataset",
    BACKTEST_READY: "run backtest",
    BACKTEST_RUNNING: "review result",
    BACKTEST_COMPLETE: "review result",
    REVIEW_REQUIRED: "review result",
    PAPER_TEST_CANDIDATE: "promote to sleeve review",
    SLEEVE_REVIEW_CANDIDATE: "promote to sleeve review",
    REJECTED: "reject/archive",
    ARCHIVED: "no action needed",
  };
  return nextByStage[stage] || "needs manual classification";
}

function flattenCandidateBuckets(candidates = {}) {
  const rows = [];
  for (const bucket of ["awaiting_decision", "approved_or_traded", "ignored", "deferred", "awaiting_outcome", "corrected"]) {
    for (const row of safeList(candidates[bucket])) {
      rows.push({ bucket, ...(row || {}) });
    }
  }
  return rows;
}

function performanceMetricRows(advisoryQuality = {}) {
  const orderedMetrics = [
    "candidate_hit_rate",
    "false_positive_rate",
    "false_negative_proxy",
    "ignored_candidate_opportunity_cost",
    "realized_vs_advisory_gap",
    "recommendation_accuracy",
    "regime_specific_candidate_quality",
    "sleeve_candidate_quality",
    "traded_vs_ignored_performance",
  ];
  return Object.entries(advisoryQuality)
    .filter(([metric, value]) => orderedMetrics.includes(metric) && value && typeof value === "object" && ("metric_status" in value || "sample_size" in value))
    .map(([metric, value]) => ({
      metric,
      status: value.metric_status || "UNKNOWN",
      sample: `${value.sample_size ?? 0}/${value.minimum_sample_size ?? "?"}`,
      confidence: value.confidence || "UNKNOWN",
    }))
    .sort((left, right) => orderedMetrics.indexOf(left.metric) - orderedMetrics.indexOf(right.metric));
}

function workflowContextHtml(payload) {
  const canonical = payload.canonical_operator_state || {};
  const operatorSnapshot = payload.operator_state_snapshot || {};
  const currentTruth = payload.current_operator_truth || operatorSnapshot.current_operator_truth || {};
  const sourceStatus = payload.source_status || {};
  const sourcePaths = payload.source_paths || {};
  const runtimePath = sourcePaths.runtime_truth || safeList(payload.drilldown_links).find((row) => row.id === "runtime_truth")?.path || "";
  const currentTruthStatus = currentTruth.current_truth_status || payload.current_truth_status || operatorSnapshot.current_truth_status || "";
  const currentDay = payload.current_day_status || operatorSnapshot.current_day_status || currentTruth.current_day_status || {};
  const requestedTruthDay = currentTruth.requested_day || payload.operator_today_projection?.current_runtime_day || operatorSnapshot.requested_day || payload.day_utc || "";
  const marketDataState = payload.operator_today_projection?.market_data_state || currentDay.market_data_state || payload.market_data_state || currentTruth.market_data_state || operatorSnapshot.market_data_state || "MARKET_DATA_PENDING";
  const candidateCertificationState = payload.operator_today_projection?.candidate_certification_state || currentDay.candidate_certification_state || payload.candidate_certification_state || currentTruth.candidate_certification_state || operatorSnapshot.candidate_certification_state || "CANDIDATES_PROVISIONAL";
  const executionEligibilityState = payload.operator_today_projection?.execution_eligibility_state || currentDay.execution_eligibility_state || payload.execution_eligibility_state || currentTruth.execution_eligibility_state || operatorSnapshot.execution_eligibility_state || "EXECUTION_LOCKED_NON_CERTIFIED";
  const fallbackDay = currentTruth.displayed_artifact_day || payload.displayed_artifact_day || operatorSnapshot.displayed_artifact_day || currentTruth.historical_fallback?.source_day || "";
  const degradedReadOnly = Boolean(dashboardDegradedReadOnlyState(payload));
  const currentTruthValue = currentTruthStatus
    ? `${degradedReadOnly ? "degraded_read_only" : currentTruthStatus.toLowerCase()} ${requestedTruthDay || ""}`.trim()
    : `${sourceStatus.canonical_state || (sourcePaths.canonical_operator_state ? "available" : "not loaded in this view")} ${formatTimestamp(sourceStatus.canonical_generated_at || canonical.generated_at_utc || payload.generated_at_utc)}`;
  const dataWaitLabel = degradedReadOnly ? "Data validation" : "Current-day blocker";
  const dataWaitValue = degradedReadOnly ? "Vendor data pending; read-only workspaces remain available." : (currentDay.blocker || currentDay.message || "none");
  const operatorBriefValue = `${sourceStatus.operator_brief || (sourcePaths.operator_brief ? "available" : "not loaded in this view")} ${formatTimestamp(sourceStatus.operator_brief_generated_at)}`;
  const runtimeTruthValue = `${sourceStatus.runtime_truth || (runtimePath || payload.runtime_evaluation_authority?.runtime_evaluation_path ? "available" : "not loaded in this view")} ${formatTimestamp(sourceStatus.runtime_truth_generated_at || payload.runtime_evaluation_authority?.generated_at_utc)}`;
  return [
    renderCardSection({
      eyebrow: "Freshness",
      title: "State Freshness",
      subtitle: "Workflow pages read the current operator truth projection and do not own truth.",
      body: renderDefinitionRows([
        { label: "Market data state", value: marketDataState },
        { label: "Candidate certification", value: candidateCertificationState },
        { label: "Execution eligibility", value: executionEligibilityState },
        { label: "Operational state", value: currentTruthValue },
        { label: dataWaitLabel, value: dataWaitValue },
        { label: "Historical fallback", value: fallbackDay && fallbackDay !== requestedTruthDay ? `showing ${fallbackDay}` : "not active" },
        { label: "Operator brief", value: operatorBriefValue },
        { label: "Runtime truth", value: runtimeTruthValue },
        { label: "Read only", value: String(payload.read_only === true) },
      ]),
    }),
    renderCardSection({
      eyebrow: "Safety",
      title: "Safety Boundary",
      subtitle: "The UI is read-only and exposes no broker, autonomous, approval, or sleeve mutation actions.",
      body: renderDefinitionRows([
        { label: "Read-only UI", value: String(payload.read_only === true) },
        { label: "No broker execution", value: String(payload.safety?.broker_execution_allowed !== true) },
        { label: "No submit/transmit", value: String(payload.safety?.broker_submit_transmit_allowed !== true) },
        { label: "No autonomous execution", value: String(payload.safety?.autonomous_execution_allowed !== true) },
        { label: "No automatic approval", value: String(payload.safety?.automatic_approval_allowed !== true) },
      ]),
    }),
    renderCardSection({
      eyebrow: "Drilldown",
      title: "Source Artifacts",
      subtitle: "Raw evidence is collapsed by default.",
      body: `
        <details>
          <summary>Open drilldown links</summary>
          ${renderSimpleTable({
            columns: [
              { label: "Artifact", key: "id" },
              { label: "Path", key: "path" },
              { label: "Hash", key: "hash" },
            ],
            rows: [
              { id: "canonical operator state", path: sourcePaths.canonical_operator_state || "", hash: canonical.source_hashes?.canonical_operator_state || "" },
              { id: "operator brief", path: sourcePaths.operator_brief || "", hash: canonical.source_hashes?.operator_brief || "" },
              { id: "runtime truth", path: runtimePath, hash: canonical.source_hashes?.runtime_truth || "" },
              ...safeList(payload.drilldown_links),
            ].filter((row) => row.path || row.id),
            emptyMessage: "No drilldown links are available.",
          })}
        </details>
      `,
    }),
  ].join("");
}

function flattenCockpitSleeves(sleeves = {}) {
  const rows = [];
  for (const bucket of ["watch", "challenged", "insufficient_data", "healthy"]) {
    for (const row of safeList(sleeves[bucket])) {
      rows.push({ bucket, ...(row || {}) });
    }
  }
  return rows;
}

function flattenCockpitResearch(research = {}) {
  const rows = [];
  for (const bucket of ["priority_tasks", "new_tasks", "review_required"]) {
    for (const row of safeList(research[bucket])) {
      rows.push({ bucket, ...(row || {}) });
    }
  }
  return rows;
}

function flattenCockpitGovernance(governance = {}) {
  const rows = [];
  for (const bucket of ["awaiting_approval", "approved", "rejected", "deferred"]) {
    for (const row of safeList(governance[bucket])) {
      rows.push({ bucket, ...(row || {}) });
    }
  }
  return rows;
}

function renderCockpitCandidateCards(candidates) {
  const rows = safeList(candidates);
  if (!rows.length) {
    return `<div class="empty-state">No ranked candidates are available for review.</div>`;
  }
  return renderList(rows, { renderItem: (candidate) => renderCockpitCandidateCard(candidate) });
}

function renderCockpitCandidateCard(candidate) {
  const candidateId = candidate.candidate_id || candidate.id || "CANDIDATE_ID";
  const sleeveId = candidate.sleeve_id || candidate.sleeve || "UNKNOWN";
  const reviewState = candidate.review_state || candidate.operator_review_status || "REVIEW_REQUIRED";
  const watchlistCommand = `npm run aegis:record-candidate-review -- --candidate-id ${candidateId} --action watchlist --operator OPERATOR --operator-note "Monitor this setup"`;
  const dismissCommand = `npm run aegis:record-candidate-review -- --candidate-id ${candidateId} --action dismiss --operator OPERATOR --operator-note "Dismissed after review"`;
  const evidenceCommand = `npm run aegis:record-candidate-review -- --candidate-id ${candidateId} --action needs-more-evidence --operator OPERATOR --operator-note "Need more evidence before review"`;
  const noteCommand = `npm run aegis:record-candidate-review -- --candidate-id ${candidateId} --action add-note --operator OPERATOR --operator-note "Operator note"`;
  return `
    <article class="stack-card">
      <div class="stack-card-header">
        <div>
          <div class="stack-card-title">#${escapeHtml(candidate.rank ?? "n/a")} ${escapeHtml(candidate.symbol || "UNKNOWN")} ${escapeHtml(candidate.direction || "")}</div>
          <div class="stack-card-subtitle">${escapeHtml(sleeveId)} · ${escapeHtml(candidate.priority || "UNKNOWN")} · ${escapeHtml(reviewState)}</div>
        </div>
        ${renderStatusPill(candidate.priority || "UNKNOWN")}
      </div>
      <div class="callout warning" style="margin-top:12px;">Operator review only. This candidate is non-executable; Aegis cannot approve, allocate, route, submit, or transmit orders.</div>
      <div class="metric-grid" style="margin-top:12px;">
        ${renderMetricCard({ label: "Score", value: String(candidate.ranking_score ?? "n/a") })}
        ${renderMetricCard({ label: "Review state", value: reviewState })}
        ${renderMetricCard({ label: "Review events", value: String(candidate.review_action_history_count ?? 0) })}
        ${renderMetricCard({ label: "Confidence", value: candidate.confidence || "UNKNOWN" })}
      </div>
      ${renderDefinitionRows([
        { label: "Why this setup", value: candidate.why_this_trade || candidate.explanation || "UNKNOWN" },
        { label: "Why now", value: candidate.why_now || "UNKNOWN" },
        { label: "Why not", value: candidate.why_not || "UNKNOWN" },
        { label: "Promotion", value: candidate.promotion_status || "UNKNOWN" },
        { label: "Executable status", value: candidate.executable_status || "NON_EXECUTABLE" },
        { label: "Latest note", value: candidate.latest_operator_note || "none" },
        { label: "Expires", value: candidate.review_expires_at || "n/a" },
        { label: "Sleeve context", value: candidate.sleeve_context || sleeveId },
        { label: "Regime context", value: candidate.regime_context || "UNKNOWN" },
        { label: "Event context", value: candidate.event_context || "UNKNOWN" },
      ])}
      <details style="margin-top:12px;">
        <summary>Review commands</summary>
        <div class="line-list">
          <div><strong>Watchlist:</strong> <code>${escapeHtml(watchlistCommand)}</code></div>
          <div><strong>Dismiss:</strong> <code>${escapeHtml(dismissCommand)}</code></div>
          <div><strong>Needs more evidence:</strong> <code>${escapeHtml(evidenceCommand)}</code></div>
          <div><strong>Add note:</strong> <code>${escapeHtml(noteCommand)}</code></div>
          <div>Review commands write audit-only state. They do not approve or execute trades.</div>
        </div>
      </details>
    </article>
  `;
}

async function renderAegisAdaptiveIntelligencePage() {
  let payload;
  try {
    payload = await fetchAegisAdaptiveIntelligence();
  } catch (error) {
    return {
      title: "Adaptive Intelligence",
      meta: "Backend unavailable; adaptive intelligence summaries could not be loaded.",
      html: renderCardSection({
        eyebrow: "BACKEND_UNAVAILABLE",
        title: "Adaptive Intelligence Unavailable",
        subtitle: "The read-only API could not return adaptive intelligence reports.",
        body: renderDefinitionRows([
          { label: "Endpoint", value: error?.operatorSafe?.endpointAttempted || "/api/aegis/adaptive-intelligence" },
          { label: "Recovery command", value: "npm run aegis:adaptive-governance" },
        ]),
      }),
      contextHtml: "",
    };
  }

  const summaries = payload.summaries || {};
  const adaptive = payload.adaptive_payloads || {};
  const regime = adaptive.regime_context || adaptive.regime_detection || {};
  const event = adaptive.event_interpretation || {};
  const governance = adaptive.adaptive_governance || {};
  const failure = adaptive.failure_analysis || {};
  const crossSleeve = adaptive.cross_sleeve_analysis || adaptive.cross_sleeve_interaction || {};
  const queue = adaptive.research_queue_optimizer || {};
  const capital = adaptive.capital_allocation_intelligence || {};
  const sleevePerformance = adaptive.sleeve_performance_analytics || {};
  const adaptiveKeys = ["regime_context", "sleeve_performance_analytics", "failure_analysis", "research_memory_graph", "research_queue_optimizer", "cross_sleeve_analysis", "adaptive_governance", "regime_detection", "capital_allocation_intelligence", "cross_sleeve_interaction", "event_interpretation"];
  return {
    title: "Adaptive Intelligence",
    meta: "Read-only adaptive research and sleeve governance. No broker actions are exposed.",
    html: [
      renderCardSection({
        eyebrow: "Read Only",
        title: "Adaptive Research And Sleeve Governance",
        subtitle: "Recommendations require human approval and cannot execute trades or mutate sleeves.",
        body: renderDefinitionRows([
          { label: "Execution allowed", value: String(payload.execution_allowed === true) },
          { label: "Broker submit/transmit", value: String(payload.broker_submit_transmit_allowed === true) },
          { label: "Autonomous execution", value: String(payload.autonomous_execution_allowed === true) },
          { label: "Current regime confidence", value: regime.confidence || regime.evidence_quality || "UNKNOWN" },
          { label: "Event importance", value: event.event_importance || "UNKNOWN" },
          { label: "Concentration risk", value: crossSleeve.concentration_risk || "UNKNOWN" },
        ]),
      }),
      renderCardSection({
        eyebrow: "Reports",
        title: "Adaptive Engine Status",
        subtitle: "AVAILABLE means a report artifact exists; missing reports are not broker execution blockers.",
        body: renderSimpleTable({
          columns: [
            { label: "Engine", key: "engine" },
            { label: "Status", key: "status" },
            { label: "Summary", key: "summary", render: (row) => JSON.stringify(row.summary || {}) },
          ],
          rows: Object.entries(summaries).filter(([key]) => adaptiveKeys.includes(key)).map(([engine, row]) => ({ engine, ...((row || {})) })),
          emptyMessage: "No adaptive intelligence reports are available yet.",
        }),
      }),
      renderCardSection({
        eyebrow: "Governance",
        title: "Adaptive Recommendations",
        subtitle: "Every recommendation is advisory and requires human approval.",
        body: renderSimpleTable({
          columns: [
            { label: "Type", key: "type" },
            { label: "Target", key: "target" },
            { label: "Confidence", key: "confidence" },
            { label: "Human Approval", key: "human_approval_required" },
          ],
          rows: safeList(governance.recommendations).slice(0, 12),
          emptyMessage: "No adaptive governance recommendations are available.",
        }),
      }),
      renderCardSection({
        eyebrow: "Research",
        title: "Optimized Research Queue",
        subtitle: "Deterministic scoring; no automatic promotion is performed.",
        body: renderSimpleTable({
          columns: [
            { label: "Item", key: "item_id" },
            { label: "Title", key: "title" },
            { label: "Score", key: "priority_score" },
            { label: "Next Step", key: "recommended_next_step" },
          ],
          rows: safeList(queue.optimized_research_queue).slice(0, 12),
          emptyMessage: "No optimized research queue is available.",
        }),
      }),
      renderCardSection({
        eyebrow: "Sleeves",
        title: "Trust Guidance",
        subtitle: "Qualitative trust guidance only; no allocation percentages or order instructions.",
        body: renderSimpleTable({
          columns: [
            { label: "Sleeve", key: "sleeve_id" },
            { label: "Status", key: "current_status" },
            { label: "Trust", key: "suggested_trust_adjustment" },
            { label: "Priority", key: "suggested_attention_priority" },
          ],
          rows: safeList(sleevePerformance.sleeve_metrics).length ? safeList(sleevePerformance.sleeve_metrics).map((row) => ({ sleeve_id: row.sleeve_id, current_status: row.confidence_trend || "UNKNOWN", suggested_trust_adjustment: "HUMAN_REVIEW", suggested_attention_priority: row.return === "INSUFFICIENT_DATA" ? "MEDIUM" : "LOW" })).slice(0, 12) : safeList(capital.sleeve_guidance).slice(0, 12),
          emptyMessage: "No sleeve trust guidance is available.",
        }),
      }),
      renderCardSection({
        eyebrow: "Cross-Sleeve",
        title: "Interaction Warnings",
        subtitle: "Correlations stay UNKNOWN until pairwise evidence exists.",
        body: renderSimpleTable({
          columns: [
            { label: "Warning", key: "type" },
            { label: "Confidence", key: "confidence" },
            { label: "Reason", key: "reason" },
          ],
          rows: safeList(crossSleeve.interaction_warnings).slice(0, 8),
          emptyMessage: "No cross-sleeve warnings are available.",
        }),
      }),
      renderCardSection({
        eyebrow: "Failure",
        title: "Failure Patterns",
        subtitle: "Suspected causes are not causality claims.",
        body: renderSimpleTable({
          columns: [
            { label: "Pattern", key: "failure_type" },
            { label: "Sleeves", key: "affected_sleeves", render: (row) => safeList(row.affected_sleeves).join(", ") || "none" },
            { label: "Confidence", key: "confidence" },
            { label: "Cause", key: "suspected_cause" },
          ],
          rows: safeList(failure.failure_patterns).slice(0, 12),
          emptyMessage: "No failure analysis is available.",
        }),
      }),
    ].join(""),
    contextHtml: renderCardSection({
      eyebrow: "Commands",
      title: "Adaptive Intelligence Commands",
      subtitle: "All commands are report-only.",
      body: `<div class="line-list">${[
        "npm run aegis:regime-detection",
        "npm run aegis:capital-allocation-intelligence",
        "npm run aegis:failure-analysis",
        "npm run aegis:research-memory-graph",
        "npm run aegis:cross-sleeve-interaction",
        "npm run aegis:research-queue-optimizer",
        "npm run aegis:event-interpretation",
        "npm run aegis:adaptive-governance",
      ].map((item) => `<div>${escapeHtml(item)}</div>`).join("")}</div>`,
    }),
  };
}

async function renderAegisIntelligenceGovernancePage() {
  let payload;
  try {
    payload = await fetchAegisIntelligenceGovernance();
  } catch (error) {
    return {
      title: "Intelligence Governance",
      meta: "Backend unavailable; intelligence governance summaries could not be loaded.",
      html: renderCardSection({
        eyebrow: "BACKEND_UNAVAILABLE",
        title: "Intelligence Governance Unavailable",
        subtitle: "The read-only API could not return intelligence governance reports.",
        body: renderDefinitionRows([
          { label: "Endpoint", value: error?.operatorSafe?.endpointAttempted || "/api/aegis/intelligence-governance" },
          { label: "Recovery command", value: "npm run aegis:intelligence-governance" },
        ]),
      }),
      contextHtml: "",
    };
  }

  const kernel = payload.kernel || {};
  const recommendations = safeList(payload.recommendations);
  const validation = kernel.validation_summary || kernel.validation || {};
  const approval = kernel.approval_summary || {};
  const aiUsage = kernel.ai_usage || {};
  const boundary = kernel.dual_kernel_boundary || {};
  const safety = kernel.safety_constraints || kernel.safety || {};
  const evidenceQuality = kernel.evidence_quality_summary || {};
  return {
    title: "Intelligence Governance",
    meta: "Read-only governance for facts, metrics, interpretations, recommendations, AI outputs, and human approvals.",
    html: [
      renderCardSection({
        eyebrow: "Dual Kernel",
        title: "Intelligence Governance Kernel",
        subtitle: "Runtime Truth remains the sole authority for readiness and permissions.",
        body: renderDefinitionRows([
          { label: "Recommendations", value: String(kernel.recommendation_count ?? recommendations.length) },
          { label: "Awaiting approval", value: String(approval.awaiting_approval ?? 0) },
          { label: "Needs more evidence", value: String(approval.needs_more_evidence ?? 0) },
          { label: "Approved", value: String(approval.approved ?? 0) },
          { label: "Rejected", value: String(approval.rejected ?? 0) },
          { label: "AI used", value: String(aiUsage.ai_used === true) },
          { label: "Deterministic fallback", value: String(aiUsage.deterministic_fallback !== false) },
          { label: "Validation issues", value: String(validation.issue_count ?? 0) },
        ]),
      }),
      renderCardSection({
        eyebrow: "Boundary",
        title: "Safety Boundary",
        subtitle: "This surface does not approve, execute, or mutate operational state.",
        body: renderDefinitionRows([
          { label: "Runtime truth mutation", value: String(boundary.may_mutate_runtime_truth === true || payload.runtime_truth_mutation_allowed === true) },
          { label: "Broker execution", value: String(safety.broker_execution_allowed === true || payload.broker_execution_allowed === true) },
          { label: "Autonomous execution", value: String(safety.autonomous_execution_allowed === true || payload.autonomous_execution_allowed === true) },
          { label: "Automated sleeve mutation", value: String(safety.automated_sleeve_mutation_allowed === true) },
          { label: "Runtime Truth authority", value: boundary.runtime_truth_kernel || "SOLE_AUTHORITY_FOR_READINESS_AND_PERMISSIONS" },
          { label: "Intelligence scope", value: safeList(boundary.intelligence_governance_kernel).join(", ") || "UNKNOWN" },
        ]),
      }),
      renderCardSection({
        eyebrow: "Evidence",
        title: "Evidence Quality",
        subtitle: "Low or insufficient evidence keeps recommendations approval-gated and conservative.",
        body: renderDefinitionRows([
          { label: "HIGH", value: String(evidenceQuality.HIGH ?? 0) },
          { label: "MEDIUM", value: String(evidenceQuality.MEDIUM ?? 0) },
          { label: "LOW", value: String(evidenceQuality.LOW ?? 0) },
          { label: "INSUFFICIENT", value: String(evidenceQuality.INSUFFICIENT ?? 0) },
          { label: "UNKNOWN", value: String(evidenceQuality.UNKNOWN ?? 0) },
        ]),
      }),
      renderCardSection({
        eyebrow: "Recommendations",
        title: "Governed Recommendations",
        subtitle: "All recommendations require explicit human approval and cannot directly perform an action.",
        body: renderSimpleTable({
          columns: [
            { label: "ID", key: "recommendation_id" },
            { label: "Type", key: "type" },
            { label: "Target", key: "target" },
            { label: "Status", key: "approval_status" },
            { label: "Confidence", key: "confidence" },
            { label: "Human Approval", key: "approval_required" },
          ],
          rows: recommendations.slice(0, 16),
          emptyMessage: "No governed recommendations are available.",
        }),
      }),
    ].join(""),
    contextHtml: [
      renderCardSection({
        eyebrow: "Ledger",
        title: "Approval Ledger",
        subtitle: "Approval records are append-only JSONL events; this UI is read-only.",
        body: renderDefinitionRows([
          { label: "Kernel report", value: payload.kernel_path || "NOT_FOUND" },
          { label: "Recommendations", value: payload.recommendations_path || "NOT_FOUND" },
          { label: "Ledger snapshot", value: payload.approval_ledger_snapshot_path || "NOT_FOUND" },
          { label: "Append-only ledger", value: kernel.approval_ledger_path || "NOT_FOUND" },
        ]),
      }),
      renderCardSection({
        eyebrow: "Command",
        title: "Refresh Intelligence Governance",
        subtitle: "Report-only command.",
        body: `<div class="line-list"><div>${escapeHtml("npm run aegis:intelligence-governance")}</div></div>`,
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
  const runtimeTruth = payload.runtime_truth_classification || "REAL_RUNTIME";
  const alertTransport = payload.alert_transport_status || "GATE_ONLY_NO_TRANSPORT";
  return {
    title: "Aegis Lite",
    meta: "Tactical intelligence, event awareness, and broker-independent manual execution support.",
    html: [
      renderCardSection({
        eyebrow: "MANUAL_ONLY",
        title: "Aegis Lite Control Plane",
        subtitle: "Advisory-only tactical intelligence for operator-entered trades. This is not broker automation.",
        body: renderDefinitionRows([
          { label: "Runtime truth", value: runtimeTruth },
          { label: "Manual execution only", value: String(payload.manual_execution_only === true) },
          { label: "Advisory-only", value: payload.readiness_classification === "ADVISORY_ONLY" ? "true" : "false" },
          { label: "Alert transport", value: alertTransport },
          { label: "Noon preflight", value: payload.latest_preflight_result || "UNKNOWN" },
          { label: "09:50 UTC and 14:50 UTC sleeve run at risk", value: String(payload.latest_preflight_canonical_eod_at_risk === true) },
          { label: "Preflight email", value: payload.latest_preflight_email_alert_sent ? "sent" : (payload.latest_preflight_operator_alert_status || "alert not live") },
          { label: "Broker submit required", value: String(payload.broker_submit_required === true) },
          { label: "IB automation", value: payload.ib_automation_status || "DEFERRED" },
          { label: "Release match", value: payload.release_match_status || "UNKNOWN" },
          { label: "Ready disabled by mismatch", value: releaseMismatch ? "YES" : "NO" },
        ]),
      }),
      renderCardSection({
        eyebrow: "PRIMARY_NAVIGATION",
        title: "Aegis Lite Operator Workflow",
        subtitle: "Primary post-pivot surfaces for manual paper trading and Research feedback.",
        body: renderAegisLitePrimaryNav(),
      }),
      renderCardSection({
        eyebrow: "TODAY",
        title: "What Needs Action?",
        subtitle: "Current EOD queue state, receipt/outcome gaps, event monitor state, and next operator step.",
        body: `
          <div class="metric-grid">
            ${renderMetricCard({ label: "Actionable trades", value: String(summary.executable_trades_count ?? executable.length) })}
            ${renderMetricCard({ label: "Blocked/advisory", value: String(summary.blocked_trades_count ?? blocked.length) })}
            ${renderMetricCard({ label: "Receipts missing", value: String(summary.missing_receipts ?? payload.missing_receipts_count ?? 0) })}
            ${renderMetricCard({ label: "Outcomes missing", value: String(summary.missing_outcomes ?? payload.missing_outcomes_count ?? 0) })}
            ${renderMetricCard({ label: "Event monitor", value: payload.event_monitor_status || legacy.event_monitor_status || "see Event Monitoring" })}
            ${renderMetricCard({ label: "Preflight next", value: payload.latest_preflight_next_action || "No preflight artifact" })}
            ${renderMetricCard({ label: "Next step", value: executable.length ? "Review manual packet" : "No manual entry" })}
          </div>
        `,
      }),
      renderCardSection({
        eyebrow: "EVENT_AWARENESS",
        title: "Event Monitoring",
        subtitle: "View monitored event rules, monitor status, event ledger, and actionable/advisory packet separation.",
        body: `<a class="inline-link" href="/aegis-events" data-route="/aegis-events">Open Event Monitoring</a>`,
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

function renderAegisLitePrimaryNav() {
  const items = [
    ["Today / Operator Status", "/aegis-lite", "What ran today, what needs action, what is blocked."],
    ["EOD Queue", "/aegis-lite", "Promoted-sleeve manual execution queue."],
    ["Event Monitoring", "/aegis-events", "Event rules, monitor status, ledger, actionable/blocked/advisory packets."],
    ["Manual Trade Packets", "/reports", "Manual packet artifacts and operator checklist evidence."],
    ["Receipts / Outcomes", "/outcomes", "Operator-entered fills, stops, exits, and outcome ledger."],
    ["Sleeve Performance", "/performance", "Sleeve return, slippage, stop behavior, and attribution."],
    ["AI Feedback / EOD-EOW Review", "/aegis-ai-feedback", "Evidence-gated deterministic review and Research task suggestions."],
    ["Research Lab", "/research-lab", "Offline hypothesis testing and sleeve validation."],
    ["Operator Inbox", "/operator-inbox", "Lightweight idea capture; promotion remains strict."],
  ];
  return `
    <div class="stack-list">
      ${items.map(([label, href, detail]) => `
        <article class="stack-card">
          <div class="stack-card-header">
            <div>
              <div class="stack-card-title">${escapeHtml(label)}</div>
              <div class="stack-card-subtitle">${escapeHtml(detail)}</div>
            </div>
            <a class="inline-link" href="${escapeHtml(href)}" data-route="${escapeHtml(href)}">Open</a>
          </div>
        </article>
      `).join("")}
    </div>
  `;
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
  const runtimeTruth = card.runtime_truth_classification || (card.demo_mode ? "DEMO_ONLY" : card.dry_run_only ? "DRY_RUN_ONLY" : "REAL_RUNTIME");
  return `
    <article class="stack-card" style="${executable ? "border-left:4px solid #16a34a;" : "border-left:4px solid #dc2626; opacity:.92;"}">
      <div class="stack-card-title">${escapeHtml(card.execution_order || "")}. ${escapeHtml(card.symbol || "UNKNOWN")} ${escapeHtml(card.side || "")}</div>
      <div class="stack-card-subtitle">${escapeHtml(card.trade_class || "UNKNOWN")} · ${escapeHtml(card.sleeve_owner || "UNKNOWN")} · ${escapeHtml(card.edge_cluster_id || "NO_EDGE_CLUSTER")}</div>
      <div class="metric-grid" style="margin-top:12px;">
        ${renderMetricCard({ label: "Runtime truth", value: runtimeTruth })}
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
        ${renderStatusPill(runtimeTruth)}
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

async function renderAegisAiFeedbackPage() {
  return {
    title: "AI Feedback / EOD-EOW Review",
    meta: "Evidence-gated sleeve review and Research feedback loop. Deterministic fallback only.",
    html: [
      renderCardSection({
        eyebrow: "EVIDENCE_GATED",
        title: "AI Feedback Engine",
        subtitle: "Reviews sleeve performance after EOD/EOW and can recommend offline Research tasks only when the Evidence Gate permits.",
        body: renderDefinitionRows([
          { label: "AI runtime", value: "deterministic fallback only" },
          { label: "Human review required", value: "true" },
          { label: "Production mutation", value: "false" },
          { label: "Broker action allowed", value: "false" },
          { label: "Auto-promotion", value: "false" },
          { label: "Auto-demotion", value: "false" },
        ]),
      }),
      renderCardSection({
        eyebrow: "OPERATOR_COMMANDS",
        title: "Build Reviews",
        subtitle: "Run from the active safe truth root after sleeve performance reports exist.",
        body: renderDefinitionRows([
          { label: "EOD", value: "python3 ops/tools/build_ai_eod_feedback_review_v1.py --truth_root <truth_root> --day <YYYY-MM-DD>" },
          { label: "EOW", value: "python3 ops/tools/build_ai_eow_feedback_review_v1.py --truth_root <truth_root> --week_ending <YYYY-MM-DD>" },
          { label: "Outputs", value: "evidence_gate.v1 + ai_feedback_review.v1 + gated offline Research tasks" },
        ]),
      }),
      renderCardSection({
        eyebrow: "TASK_GATE",
        title: "Evidence Gate",
        subtitle: "One or two trades are observations only. Strong conclusions are blocked until evidence is clean and sufficient.",
        body: renderDefinitionRows([
          { label: "1-2 trades", value: "OBSERVATION_ONLY; no automatic Research task creation" },
          { label: "3-9 trades", value: "WEAK_SIGNAL; low-priority offline Research task may be created" },
          { label: "10-19 trades", value: "REVIEWABLE_PATTERN; normal offline Research task may be created" },
          { label: "20+ trades", value: "STRONGER_PATTERN; higher-priority offline Research task may be created" },
        ]),
      }),
    ].join(""),
    contextHtml: renderAegisLitePrimaryNav(),
  };
}

function renderResearchOSStatusPanel(payload = {}) {
  const report = payload.research_os_status_report || payload.status_report || {};
  if (!payload.ok || !report.research_os_status_report_id) {
    return renderCardSection({
      eyebrow: "READ_ONLY_STATUS",
      title: "Research OS Status",
      subtitle: "No Research OS Status report is available yet.",
      body: renderDefinitionRows([
        { label: "Next command", value: "npm run research:packet23" },
        { label: "Lifecycle mutation", value: "not available from this panel" },
      ]),
    });
  }
  const blockers = safeList(report.current_blockers);
  const actions = safeList(report.next_recommended_research_actions);
  const challenger = report.challenger_summary || {};
  const human = report.human_review_summary || {};
  const paper = report.paper_trial_summary || {};
  return renderCardSection({
    eyebrow: "READ_ONLY_STATUS",
    title: "Research OS Status",
    subtitle: "Unified research-state projection. It surfaces blockers and recommendations without repair, promotion, trading, or paper-trial creation.",
    body: [
      `<div class="metric-grid">
        ${renderMetricCard({ label: "Overall status", value: report.overall_status || "UNKNOWN" })}
        ${renderMetricCard({ label: "Readiness", value: report.readiness_level || "UNKNOWN" })}
        ${renderMetricCard({ label: "Integrity", value: report.integrity_status || "UNKNOWN" })}
        ${renderMetricCard({ label: "Blockers", value: String(blockers.length) })}
        ${renderMetricCard({ label: "Materialized challengers", value: String(report.materialized_challenger_count ?? 0) })}
        ${renderMetricCard({ label: "Latest decision", value: human.latest_decision || "none" })}
      </div>`,
      renderDefinitionRows([
        { label: "Status report", value: report.research_os_status_report_id },
        { label: "Integrity report", value: report.latest_integrity_report_id || "missing" },
        { label: "Duplicate registry issues", value: String(report.duplicate_registry_issue_count ?? 0) },
        { label: "Legacy lineage warnings", value: String(report.lineage_warning_count ?? 0) },
        { label: "Challenger evidence", value: `${challenger.evidence_completeness_status || "unknown"} · ${challenger.materialized_challenger_count ?? report.materialized_challenger_count ?? 0} materialized` },
        { label: "Human review", value: human.latest_human_review_decision_id || "no decision recorded" },
        { label: "Paper trial status", value: `${report.paper_trial_status || "UNKNOWN"} · eligible=${paper.paper_trial_proposal_eligible === true ? "yes" : "no"}` },
      ]),
      blockers.length
        ? renderSimpleTable({
            columns: [
              { label: "Severity", key: "severity" },
              { label: "Source", key: "source" },
              { label: "Finding", render: (row) => escapeHtml(row.message || row.reason || row.artifact_id || "blocker") },
            ],
            rows: blockers.slice(0, 8),
            emptyMessage: "No blockers.",
          })
        : `<div class="empty-state">No current blockers in the latest status report.</div>`,
      renderList(actions, { emptyMessage: "No recommended research actions.", renderItem: (item) => `<span>${escapeHtml(item)}</span>` }),
      `<div class="callout">Read-only references only. No promote, open trade, allocate, repair automatically, or paper-trial creation actions are available here.</div>`,
    ].join(""),
  });
}

function renderPaperTrialProposalPanel(payload = {}) {
  const proposal = payload.paper_trial_proposal || {};
  if (!payload.ok || !proposal.paper_trial_proposal_id) {
    return renderCardSection({
      eyebrow: "READ_ONLY_GATE",
      title: "Paper Trial Proposals",
      subtitle: "No paper-trial proposal artifact is available yet.",
      body: renderDefinitionRows([
        { label: "Next command", value: "npm run research:packet24" },
        { label: "Paper trial created", value: "NO" },
      ]),
    });
  }
  const blockers = safeList(proposal.blockers);
  const assertion = proposal.no_lifecycle_mutation_assertion || {};
  const statusLabel = {
    blocked: "Proposal blocked",
    needs_review: "Proposal needs review",
    eligible: "Proposal eligible",
  }[proposal.proposal_status] || proposal.proposal_status || "UNKNOWN";
  return renderCardSection({
    eyebrow: "READ_ONLY_GATE",
    title: "Paper Trial Proposals",
    subtitle: "Proposal gate only. A proposal is not a paper trial and cannot mutate sleeves, challengers, candidates, evidence, or lifecycle state.",
    body: [
      `<div class="metric-grid">
        ${renderMetricCard({ label: "Proposal status", value: statusLabel })}
        ${renderMetricCard({ label: "Eligibility", value: proposal.eligibility_status || "UNKNOWN" })}
        ${renderMetricCard({ label: "Blockers", value: String(blockers.length) })}
        ${renderMetricCard({ label: "Lifecycle mutation", value: assertion.paper_trial_created === false ? "NO" : "UNKNOWN" })}
      </div>`,
      renderDefinitionRows([
        { label: "Proposal id", value: proposal.paper_trial_proposal_id },
        { label: "Decision id", value: proposal.human_review_decision_id || "missing" },
        { label: "Dossier id", value: proposal.human_review_dossier_id || "missing" },
        { label: "Challenger id", value: proposal.challenger_id || "missing" },
        { label: "Integrity report", value: proposal.latest_integrity_report_id || "missing" },
        { label: "Research OS status", value: proposal.latest_research_os_status_report_id || "missing" },
      ]),
      blockers.length
        ? renderSimpleTable({
            columns: [
              { label: "Code", key: "blocker_code" },
              { label: "Severity", key: "severity" },
              { label: "Message", key: "blocker_message" },
            ],
            rows: blockers,
            emptyMessage: "No proposal blockers.",
          })
        : `<div class="empty-state">No proposal blockers.</div>`,
      `<div class="callout">No lifecycle mutation assertion: paper_trial_created=${escapeHtml(String(assertion.paper_trial_created))}, challenger_promoted=${escapeHtml(String(assertion.challenger_promoted))}, evidence_mutated=${escapeHtml(String(assertion.evidence_mutated))}.</div>`,
    ].join(""),
  });
}

function renderObservationCandidatesPanel(payload = {}) {
  const batch = payload.observation_candidate_batch || {};
  const observations = safeList(payload.top_observations);
  if (!payload.ok || !batch.observation_candidate_batch_id) {
    return renderCardSection({
      eyebrow: "RESEARCH_ONLY",
      title: "Observation Candidates",
      subtitle: "Research-only anomalies surfaced for possible future study. These are not trading signals.",
      body: renderDefinitionRows([
        { label: "Next command", value: "npm run research:packet25" },
        { label: "Actionability", value: "non-actionable research only" },
      ]),
    });
  }
  const scannerStatuses = safeList(batch.scanner_statuses);
  return renderCardSection({
    eyebrow: "RESEARCH_ONLY",
    title: "Observation Candidates",
    subtitle: "Research-only anomalies surfaced for possible future study. These are not trading signals.",
    body: [
      `<div class="metric-grid">
        ${renderMetricCard({ label: "Batch", value: batch.observation_candidate_batch_id })}
        ${renderMetricCard({ label: "Observation count", value: String(batch.observation_count ?? 0) })}
        ${renderMetricCard({ label: "Integrity report", value: batch.latest_integrity_report_id || "missing" })}
        ${renderMetricCard({ label: "Research OS status", value: batch.latest_research_os_status_report_id || "missing" })}
      </div>`,
      renderDefinitionRows([
        { label: "Type counts", value: JSON.stringify(batch.observation_type_counts || {}) },
        { label: "Severity counts", value: JSON.stringify(batch.severity_counts || {}) },
        { label: "Scanner statuses", value: scannerStatuses.map((row) => `${row.scanner}:${row.status}`).join(", ") || "none" },
        { label: "Actionability", value: "non-actionable research only" },
      ]),
      renderSimpleTable({
        columns: [
          { label: "Type", key: "observation_type" },
          { label: "Family", key: "observation_family" },
          { label: "Severity", key: "severity" },
          { label: "Summary", key: "observation_summary" },
          { label: "Anomaly", render: (row) => escapeHtml(String(row.anomaly_score ?? "")) },
          { label: "Actionability", key: "actionability_status" },
        ],
        rows: observations,
        emptyMessage: "No observation candidates in latest batch.",
      }),
      `<div class="callout">Observation candidates are read-only research records. They cannot mutate lifecycle state, authorize market action, or create downstream artifacts.</div>`,
    ].join(""),
  });
}

function renderObservationClustersPanel(payload = {}) {
  const batch = payload.observation_cluster_batch || {};
  const clusters = safeList(payload.top_clusters);
  if (!payload.ok || !batch.observation_cluster_batch_id) {
    return renderCardSection({
      eyebrow: "RESEARCH_ONLY",
      title: "Observation Clusters",
      subtitle: "Related research-only observations grouped for later review. These are not hypotheses or trading signals.",
      body: renderDefinitionRows([
        { label: "Next command", value: "npm run research:packet26" },
        { label: "Actionability", value: "non-actionable research only" },
      ]),
    });
  }
  return renderCardSection({
    eyebrow: "RESEARCH_ONLY",
    title: "Observation Clusters",
    subtitle: "Related research-only observations grouped for later review. These are not hypotheses or trading signals.",
    body: [
      `<div class="metric-grid">
        ${renderMetricCard({ label: "Cluster batch", value: batch.observation_cluster_batch_id })}
        ${renderMetricCard({ label: "Cluster count", value: String(batch.cluster_count ?? 0) })}
        ${renderMetricCard({ label: "Integrity report", value: batch.latest_integrity_report_id || "missing" })}
        ${renderMetricCard({ label: "Research OS status", value: batch.latest_research_os_status_report_id || "missing" })}
      </div>`,
      renderDefinitionRows([
        { label: "Family counts", value: JSON.stringify(batch.cluster_family_counts || {}) },
        { label: "Status counts", value: JSON.stringify(batch.cluster_status_counts || {}) },
        { label: "Highest priority clusters", value: safeList(batch.highest_priority_cluster_ids).join(", ") || "none" },
        { label: "Actionability", value: "non-actionable research only" },
      ]),
      renderSimpleTable({
        columns: [
          { label: "Cluster", key: "cluster_label" },
          { label: "Family", key: "cluster_family" },
          { label: "Status", key: "cluster_status" },
          { label: "Recurrence", render: (row) => escapeHtml(String(row.recurrence_count ?? 0)) },
          { label: "Severity", render: (row) => escapeHtml(String(row.severity_score ?? "")) },
          { label: "Persistence", render: (row) => escapeHtml(String(row.persistence_score ?? "")) },
          { label: "Priority", render: (row) => escapeHtml(String(row.research_priority_score ?? "")) },
          { label: "Linked observations", render: (row) => escapeHtml(String(safeList(row.observation_candidate_ids).length)) },
          { label: "Actionability", key: "actionability_status" },
        ],
        rows: clusters,
        emptyMessage: "No observation clusters in latest batch.",
      }),
      `<div class="callout">Observation clusters are read-only research groupings. They cannot mutate lifecycle state, authorize market action, or create downstream artifacts.</div>`,
    ].join(""),
  });
}


function renderResearchIntakeQueuePanel(payload = {}) {
  const rows = safeList(payload.queue || payload.hypothesis_proposals);
  const lanes = payload.lanes || {};
  return renderCardSection({
    eyebrow: "RESEARCH_INTAKE",
    title: "Pipeline Review",
    subtitle: "Read-only event-intake proposals by review lane. Explicit assessment and review artifacts are append-only.",
    body: [
      `<div class="metric-grid">
        ${renderMetricCard({ label: "Queue count", value: String(rows.length) })}
        ${renderMetricCard({ label: "Proposed", value: String(safeList(lanes.proposed).length) })}
        ${renderMetricCard({ label: "Watchlist", value: String(safeList(lanes.watchlist).length) })}
        ${renderMetricCard({ label: "Waiting for data", value: String(safeList(lanes.needs_data).length) })}
      </div>`,
      renderSimpleTable({
        columns: [
          { label: "Title", render: (row) => `<strong>${escapeHtml(row.title || row.hypothesis_proposal_id || "proposal")}</strong><div class="muted-mini">${escapeHtml(row.hypothesis_proposal_id || "")}</div>` },
          { label: "Family", key: "event_family_id" },
          { label: "Confidence", key: "confidence_level" },
          { label: "Governance", key: "governance_classification" },
          { label: "Data", key: "data_requirement_status" },
          { label: "Priority", key: "priority_bucket" },
          { label: "Ready", render: (row) => escapeHtml(row.ready_for_research ? "true" : "false") },
          { label: "Next", key: "recommended_next_action" },
          { label: "Review", key: "latest_review_decision" },
          { label: "Dossier", render: (row) => `<a class="ghost-button research-action-button" href="/api/research-lab/hypothesis-proposals/${encodeURIComponent(String(row.hypothesis_proposal_id || ""))}/dossier" target="_blank" rel="noopener noreferrer">Open Dossier</a>` },
        ],
        rows,
        emptyMessage: "No Packet 30 event-intake proposals are queued.",
      }),
      `<div class="callout">Research intake only. No broker execution. No live trading. No autonomous execution. No sleeve creation. No investment recommendation.</div>`,
    ].join(""),
  });
}

function renderHypothesisProposalsPanel(payload = {}) {
  const batch = payload.hypothesis_proposal_batch || {};
  const proposals = safeList(payload.top_hypothesis_proposals);
  if (!payload.ok || !batch.hypothesis_proposal_batch_id) {
    return renderCardSection({
      eyebrow: "RESEARCH_ONLY",
      title: "Hypothesis Proposals",
      subtitle: "Research-only proposals generated from observation clusters. These are not active hypotheses or trading signals.",
      body: renderDefinitionRows([
        { label: "Next command", value: "npm run research:packet27" },
        { label: "Actionability", value: "non-actionable research only" },
      ]),
    });
  }
  return renderCardSection({
    eyebrow: "RESEARCH_ONLY",
    title: "Hypothesis Proposals",
    subtitle: "Research-only proposals generated from observation clusters. These are not active hypotheses or trading signals.",
    body: [
      `<div class="metric-grid">
        ${renderMetricCard({ label: "Proposal batch", value: batch.hypothesis_proposal_batch_id })}
        ${renderMetricCard({ label: "Proposal count", value: String(batch.proposal_count ?? 0) })}
        ${renderMetricCard({ label: "Blocked", value: String(batch.blocked_count ?? 0) })}
        ${renderMetricCard({ label: "System research only", value: String(batch.system_research_only_count ?? 0) })}
      </div>`,
      renderDefinitionRows([
        { label: "Status counts", value: JSON.stringify(batch.proposal_status_counts || {}) },
        { label: "Family counts", value: JSON.stringify(batch.proposal_family_counts || {}) },
        { label: "Source cluster batch", value: batch.source_observation_cluster_batch_id || "missing" },
        { label: "Integrity report", value: batch.latest_integrity_report_id || "missing" },
        { label: "Research OS status", value: batch.latest_research_os_status_report_id || "missing" },
        { label: "Actionability", value: "non-actionable research only" },
      ]),
      renderSimpleTable({
        columns: [
          { label: "Proposal", key: "proposal_label" },
          { label: "Family", key: "proposal_family" },
          { label: "Status", key: "proposal_status" },
          { label: "Question", key: "proposed_research_question" },
          { label: "Source cluster", key: "source_observation_cluster_id" },
          { label: "Blockers", render: (row) => escapeHtml(safeList(row.blockers).map((item) => item.blocker_code || item.severity || "blocker").join(", ") || "none") },
          { label: "Next evidence", render: (row) => escapeHtml(safeList(row.required_next_evidence).join("; ") || "none") },
          { label: "Test design", render: (row) => escapeHtml(row.proposed_test_design?.test_type || "") },
          { label: "Actionability", key: "actionability_status" },
        ],
        rows: proposals,
        emptyMessage: "No hypothesis proposals in latest batch.",
      }),
      `<div class="callout">Hypothesis proposals are read-only review inputs. They cannot mutate lifecycle state, authorize market action, or activate research objects.</div>`,
    ].join(""),
  });
}

function renderHypothesisProposalReviewsPanel(payload = {}) {
  const batch = payload.hypothesis_proposal_review_batch || {};
  const reviews = safeList(payload.top_hypothesis_proposal_reviews);
  if (!payload.ok || !batch.hypothesis_proposal_review_batch_id) {
    return renderCardSection({
      eyebrow: "RESEARCH_ONLY",
      title: "Hypothesis Proposal Reviews",
      subtitle: "Recorded research review decisions for hypothesis proposals. These do not create active hypotheses or trading signals.",
      body: renderDefinitionRows([
        { label: "Next command", value: "npm run research:packet28" },
        { label: "Actionability", value: "non-actionable research only" },
      ]),
    });
  }
  return renderCardSection({
    eyebrow: "RESEARCH_ONLY",
    title: "Hypothesis Proposal Reviews",
    subtitle: "Recorded research review decisions for hypothesis proposals. These do not create active hypotheses or trading signals.",
    body: [
      `<div class="metric-grid">
        ${renderMetricCard({ label: "Review batch", value: batch.hypothesis_proposal_review_batch_id })}
        ${renderMetricCard({ label: "Review count", value: String(batch.review_count ?? 0) })}
        ${renderMetricCard({ label: "Integrity report", value: batch.latest_integrity_report_id || "missing" })}
        ${renderMetricCard({ label: "Research OS status", value: batch.latest_research_os_status_report_id || "missing" })}
      </div>`,
      renderDefinitionRows([
        { label: "Decision counts", value: JSON.stringify(batch.review_decision_counts || {}) },
        { label: "Status counts", value: JSON.stringify(batch.review_status_counts || {}) },
        { label: "Source proposal batch", value: batch.source_hypothesis_proposal_batch_id || "missing" },
        { label: "Actionability", value: "non-actionable research only" },
      ]),
      renderSimpleTable({
        columns: [
          { label: "Proposal", key: "hypothesis_proposal_id" },
          { label: "Family", key: "source_proposal_family" },
          { label: "Source status", key: "source_proposal_status" },
          { label: "Decision", key: "review_decision" },
          { label: "Review status", key: "review_status" },
          { label: "Rationale", key: "review_rationale" },
          { label: "Next research actions", render: (row) => escapeHtml(safeList(row.allowed_next_research_actions).join(", ") || "none") },
          { label: "Disallowed actions", render: (row) => escapeHtml(safeList(row.disallowed_actions).join(", ") || "none") },
          { label: "Blockers", render: (row) => escapeHtml(safeList(row.blockers).map((item) => item.blocker_code || item.severity || "blocker").join(", ") || "none") },
          { label: "Actionability", key: "actionability_status" },
        ],
        rows: reviews,
        emptyMessage: "No hypothesis proposal reviews in latest batch.",
      }),
      `<div class="callout">Hypothesis proposal reviews are append-only research records. They cannot mutate lifecycle state, authorize market action, or activate research objects.</div>`,
    ].join(""),
  });
}

function renderResearchHypothesisIntakePanel(payload = {}) {
  const batch = payload.hypothesis_intake_batch || {};
  const decisions = safeList(payload.top_hypothesis_intake_decisions);
  if (!payload.ok || !batch.hypothesis_intake_batch_id) {
    return renderCardSection({
      eyebrow: "RESEARCH_ONLY",
      title: "Research Hypothesis Intake",
      subtitle: "Governed intake decisions for reviewed hypothesis proposals. Accepted items become inactive research hypotheses only; they are not strategies or trading signals.",
      body: renderDefinitionRows([
        { label: "Next command", value: "npm run research:packet29" },
        { label: "Actionability", value: "non-actionable research only" },
      ]),
    });
  }
  return renderCardSection({
    eyebrow: "RESEARCH_ONLY",
    title: "Research Hypothesis Intake",
    subtitle: "Governed intake decisions for reviewed hypothesis proposals. Accepted items become inactive research hypotheses only; they are not strategies or trading signals.",
    body: [
      `<div class="metric-grid">
        ${renderMetricCard({ label: "Intake batch", value: batch.hypothesis_intake_batch_id })}
        ${renderMetricCard({ label: "Intake count", value: String(batch.intake_count ?? 0) })}
        ${renderMetricCard({ label: "Accepted", value: String(batch.accepted_count ?? 0) })}
        ${renderMetricCard({ label: "Blocked", value: String(batch.blocked_count ?? 0) })}
        ${renderMetricCard({ label: "Deferred", value: String(batch.deferred_count ?? 0) })}
      </div>`,
      renderDefinitionRows([
        { label: "Created research hypotheses", value: safeList(batch.created_research_hypothesis_ids).join(", ") || "none" },
        { label: "Source review batch", value: batch.source_hypothesis_proposal_review_batch_id || "missing" },
        { label: "Integrity report", value: batch.latest_integrity_report_id || "missing" },
        { label: "Research OS status", value: batch.latest_research_os_status_report_id || "missing" },
        { label: "Actionability", value: "non-actionable research only" },
      ]),
      renderSimpleTable({
        columns: [
          { label: "Review", key: "source_hypothesis_proposal_review_id" },
          { label: "Proposal", key: "source_hypothesis_proposal_id" },
          { label: "Status", key: "intake_status" },
          { label: "Decision", key: "intake_decision" },
          { label: "Created hypothesis", render: (row) => escapeHtml(row.created_research_hypothesis_id || "none") },
          { label: "Rationale", key: "intake_rationale" },
          { label: "Blockers", render: (row) => escapeHtml(safeList(row.blockers).map((item) => item.blocker_code || item.severity || "blocker").join(", ") || "none") },
          { label: "Actionability", key: "actionability_status" },
        ],
        rows: decisions,
        emptyMessage: "No hypothesis intake decisions in latest batch.",
      }),
      `<div class="callout">Research hypothesis intake is append-only and read-only in this shell. It cannot change hypothesis, challenger, sleeve, paper-trial, market, allocation, or promotion state.</div>`,
    ].join(""),
  });
}

function renderResearchSafetyStrip(payload = {}) {
  const labels = safeList(payload.safety_labels).length
    ? safeList(payload.safety_labels)
    : ["READ-ONLY GOVERNANCE", "NO BROKER EXECUTION", "NO LIVE TRADING", "MANUAL REVIEW REQUIRED"];
  return `<div class="compact-safety-strip">${labels.map(escapeHtml).join(" · ")}<br>${escapeHtml(payload.hypothetical_evidence_label || "Backtests and model outputs are hypothetical research evidence, not achieved portfolio performance.")}</div>`;
}

function renderResearchConsolePanel(payload = {}) {
  const examples = safeList(payload.start_research_examples);
  const options = safeList(payload.event_family_options);
  const focusedHypothesisId = String(currentSearchParams().get("hypothesis_id") || "").trim();
  const model = hypothesisViewModel(payload);
  const readyItems = safeList(model.sections?.ready_to_start?.visible_items);
  const focused = readyItems.find((item) => item.hypothesis_id === focusedHypothesisId) || null;
  const focusedSymbols = focused ? hypothesisSymbolsText(focused) : "";
  const panelTitle = focused ? `Start ${focused.title}` : "New Research Idea";
  const panelSubtitle = focused ? "Queue AI research for this hypothesis. Aegis records a run ledger event and keeps broker controls disabled." : "Capture the idea in plain English. Aegis will keep this research-only until it is reviewed.";
  const buttonLabel = focused ? `Start ${focused.title}` : "Start Research";
  return renderCardSection({
    eyebrow: focused ? "START_HYPOTHESIS_RESEARCH" : "NEW_RESEARCH_IDEA",
    title: panelTitle,
    subtitle: panelSubtitle,
    body: [
      renderResearchSafetyStrip(payload),
      focused ? `<div class="callout" data-start-focused-hypothesis>Ready to start: ${escapeHtml(focused.title)}</div>` : "",
      `<form class="research-console-form research-idea-form" id="start-research">
        <h3>${escapeHtml(panelTitle)}</h3>
        ${focused ? `<input type="hidden" name="hypothesis_id" value="${escapeHtml(focused.hypothesis_id)}" />` : ""}
        <label>What are you investigating?<textarea name="idea" rows="3" ${focused ? "" : "required"} placeholder="Example: Oil shock reversals after energy sector overreaction">${escapeHtml(focused?.title || "")}</textarea></label>
        <label>Symbols <span class="muted-mini">optional</span><input name="symbols" type="text" placeholder="SPY QQQ XLE" value="${escapeHtml(focused ? focusedSymbols : "")}" /></label>
        <details class="support-note research-advanced-settings">
          <summary>Advanced research settings</summary>
          <div class="form-grid">
            <label>Event family<select name="event_family"><option value="">Infer from idea</option>${options.map((row) => `<option value="${escapeHtml(row.value)}">${escapeHtml(row.label)}</option>`).join("")}</select></label>
            <label>Priority<select name="priority"><option value="watchlist">Watchlist</option><option value="low">Low</option><option value="medium">Medium</option><option value="high">High</option></select></label>
          </div>
          <label>Notes<textarea name="notes" rows="3" placeholder="Research-only notes"></textarea></label>
          <div class="muted-mini">Examples: ${examples.map(escapeHtml).join(" · ")}</div>
        </details>
        <button class="primary-button research-action-button" type="submit">${escapeHtml(buttonLabel)}</button>
        <span data-research-console-status class="muted-mini"></span>
      </form>`,
    ].join(""),
  });
}

function renderHypothesisQueueConsolePanel(payload = {}, state = {}) {
  const queuePayload = payload.hypothesis_queue || payload;
  const rows = safeList(queuePayload.queue || queuePayload.hypothesis_proposals);
  const lanes = queuePayload.lanes || {};
  const laneNames = ["proposed", "watchlist", "needs_data", "accepted_for_research", "rejected", "archived", "needs_review"];
  const emptyState = queuePayload.empty_state || {};
  const warnings = safeList(queuePayload.integrity_warnings);
  const errors = safeList(queuePayload.integrity_errors);
  const trustedEmptyMessage = emptyState.trusted_empty
    ? "No hypotheses are queued; projection build confirmed source registry and proposal artifacts are empty."
    : (queuePayload.operator_message || "Projection not built or not trusted empty.");
  return renderCardSection({
    eyebrow: "HYPOTHESIS QUEUE",
    title: "Pipeline Review",
    subtitle: "Projection-backed operator queue. Accept and conversion stay disabled until readiness and approval gates pass.",
    body: [
      queuePayload.integrity_status && queuePayload.integrity_status !== "ok" ? `<div class="callout warning">Queue integrity warning: ${escapeHtml(queuePayload.operator_message || "some source artifacts could not be hydrated. They are shown under Needs Review.")}</div>` : "",
      `<div class="metric-grid">
        ${renderMetricCard({ label: "Built", value: queuePayload.projection_built_at || "not built" })}
        ${renderMetricCard({ label: "Integrity", value: queuePayload.integrity_status || "source_missing" })}
        ${renderMetricCard({ label: "Source proposals", value: String(queuePayload.source_proposal_count ?? queuePayload.source_summary?.hypothesis_proposal_registry_row_count ?? 0) })}
        ${renderMetricCard({ label: "Projected items", value: String(queuePayload.projected_item_count ?? rows.length) })}
        ${renderMetricCard({ label: "Recovered", value: String(queuePayload.recovered_count ?? rows.filter((row) => row.recovered).length) })}
        ${renderMetricCard({ label: "Needs operator review", value: String(queuePayload.needs_operator_review_count ?? rows.filter((row) => row.needs_operator_review).length) })}
      </div>`,
      `<div class="research-action-toolbar compact" aria-label="Pipeline filters"><button class="ghost-button research-action-button" type="button">All</button><button class="ghost-button research-action-button" type="button">Recovered</button><button class="ghost-button research-action-button" type="button">Needs Operator Review</button><button class="ghost-button research-action-button" type="button">Waiting for Data</button></div>`,
      warnings.length || errors.length ? renderDefinitionRows([
        { label: "Warnings", value: warnings.join("; ") || "none" },
        { label: "Errors", value: errors.join("; ") || "none" },
      ]) : "",
      renderResearchConsoleActionFeedback(state.researchConsoleWorkflow || {}),
      `<div class="metric-grid">${laneNames.map((lane) => renderMetricCard({ label: researchLaneLabel(lane), value: String(safeList(lanes[lane]).length) })).join("")}</div>`,
      renderResearchProposalLaneBoard(lanes),
      `<details class="support-note research-diagnostics-collapsed"><summary>Advanced proposal table</summary>`,
      renderSimpleTable({
        columns: [
          { label: "Title", render: (row) => `<strong>${escapeHtml(row.title || "Hypothesis proposal")}</strong><div class="muted-mini">${escapeHtml(row.hypothesis_summary || row.hypothesis_proposal_id || "")}</div>` },
          { label: "Lane", render: (row) => escapeHtml(researchLaneLabel(row.lane || row.proposal_status || "")) },
          { label: "Family", render: (row) => escapeHtml(row.event_family || row.event_family_id || "") },
          { label: "Confidence", key: "confidence_level" },
          { label: "Governance", key: "governance_classification" },
          { label: "Readiness", render: (row) => escapeHtml(researchReadinessLabel(row.data_requirement_status || row.readiness?.data_requirement_status || row.lane || "")) },
          { label: "Priority", key: "priority_bucket" },
          { label: "Blocking", render: (row) => escapeHtml(researchBlockingSummary(row)) },
          { label: "Recovery", render: (row) => escapeHtml(row.needs_operator_review ? "needs operator review" : row.recovered ? "recovered" : "canonical") },
          { label: "Next", key: "recommended_next_action" },
          { label: "Actions", render: (row) => renderResearchProposalActions(row) },
        ],
        rows,
        emptyMessage: trustedEmptyMessage,
      }),
      `</details>`,
      renderResearchSafetyStrip(payload),
    ].join(""),
  });
}


function renderResearchConsoleActionFeedback(workflow = {}) {
  const result = workflow.lastResult || null;
  const error = workflow.lastError || null;
  if (!result && !error) {
    return `<div class="research-action-feedback-placeholder" data-research-console-feedback>No research action has run in this view yet.</div>`;
  }
  if (error) {
    const message = error.operator_message || error.message || "Research Pipeline action failed.";
    const reason = error.failure_reason || error.error || "";
    return `<div class="research-action-feedback error" data-research-console-feedback role="alert">
      <strong>Unable to complete action</strong>
      <span>${escapeHtml(reason ? `${message} ${reason}` : message)}</span>
    </div>`;
  }
  const fromLane = result.previous_lane || result.transition?.from?.lane || "unknown";
  const toLane = result.new_lane || result.updated_lane || result.transition?.to?.lane || "unknown";
  const message = result.operator_message || result.message || "Research Pipeline action completed.";
  const timestamp = result.timestamp || result.reviewed_at || result.created_at || "just now";
  return `<div class="research-action-feedback success" data-research-console-feedback role="status">
    <strong>✓ ${escapeHtml(message)}</strong>
    <span>${escapeHtml(researchLaneLabel(fromLane))} → ${escapeHtml(researchLaneLabel(toLane))} · ${escapeHtml(timestamp)}</span>
  </div>`;
}

function renderResearchProposalLaneBoard(lanes = {}) {
  const laneOrder = ["proposed", "accepted_for_research", "needs_data", "watchlist", "rejected", "archived", "needs_review"];
  return `<div class="research-proposal-lanes" aria-label="Research proposal state lanes">
    ${laneOrder.map((lane) => {
      const rows = safeList(lanes[lane]);
      return `<section class="research-proposal-lane" data-research-lane="${escapeHtml(lane)}">
        <div class="research-proposal-lane-header">
          <strong>${escapeHtml(researchLaneLabel(lane))}</strong>
          <span>${escapeHtml(String(rows.length))}</span>
        </div>
        <div class="research-proposal-lane-stack">
          ${rows.length ? rows.slice(0, 6).map(renderResearchProposalCard).join("") : `<div class="edge-kanban-empty">No items.</div>`}
        </div>
      </section>`;
    }).join("")}
  </div>`;
}

function renderResearchProposalCard(row = {}) {
  const id = String(row.hypothesis_proposal_id || row.item_id || "");
  return `<article class="research-proposal-card" data-research-proposal-card data-hypothesis-proposal-id="${escapeHtml(id)}" data-research-lane="${escapeHtml(row.lane || row.proposal_status || "proposed")}">
    <div class="research-proposal-card-title">${escapeHtml(row.title || "Hypothesis proposal")}</div>
    <p>${escapeHtml(row.hypothesis_summary || id)}</p>
    <div class="edge-badge-row">
      <span class="edge-badge">${escapeHtml(researchLaneLabel(row.lane || row.proposal_status || ""))}</span>
      <span class="edge-badge edge-badge-muted">${escapeHtml(researchReadinessLabel(row.data_requirement_status || row.readiness?.data_requirement_status || row.lane || ""))}</span>
    </div>
    ${row.blocking_items?.length ? `<div class="muted-mini">Blocked: ${escapeHtml(safeList(row.blocking_items).join(", "))}</div>` : ""}
    <div class="muted-mini">Next: ${escapeHtml(row.recommended_next_action || "Review hypothesis")}</div>
    ${renderResearchProposalActions(row)}
  </article>`;
}

function renderResearchProposalActions(row = {}) {
  const id = escapeHtml(row.hypothesis_proposal_id || row.item_id || "");
  const ready = row.ready_for_research === true;
  const accepted = row.proposal_status === "accepted_for_research" || row.lane === "accepted_for_research";
  const canAccept = ready && !accepted;
  const datasetId = escapeHtml(row.default_dataset_snapshot_id || "");
  const universeId = escapeHtml(row.default_universe_snapshot_id || "");
  const start = escapeHtml(row.convert_start || "");
  const end = escapeHtml(row.convert_end || "");
  const canConvert = accepted && row.convert_to_research_plan_enabled === true;
  const conversionReason = accepted && !canConvert ? "Conversion needs a ready dataset, universe, and date range." : "Convert accepted research into a governed research plan.";
  const form = (body) => `<form class="research-console-form inline">${body}<span class="research-action-inline-status" data-research-console-status></span></form>`;
  return `<div class="research-action-toolbar compact">
    <a class="ghost-button research-action-button" href="/research-lab/hypotheses?dossier=${id}" data-aegis-command-id="OPEN_VALID_ROUTE" data-aegis-command-action-type="EXPAND_SECTION" data-aegis-command-target-type="navigation" data-aegis-command-target-id="research_hypothesis_dossier" data-route="/research-lab/hypotheses?dossier=${id}">Open Dossier</a>
    ${form(`<input type="hidden" name="research_action" value="assess-readiness"><input type="hidden" name="hypothesis_proposal_id" value="${id}"><button class="ghost-button research-action-button" type="submit">Assess Readiness</button>`)}
    ${["watchlist", "reject", "archive"].map((decision) => form(`<input type="hidden" name="research_action" value="review"><input type="hidden" name="hypothesis_proposal_id" value="${id}"><input type="hidden" name="decision" value="${decision}"><input type="hidden" name="reason" value="Operator review from Research Pipeline"><button class="ghost-button research-action-button" type="submit">${decision === "reject" ? "Reject" : decision.charAt(0).toUpperCase() + decision.slice(1)}</button>`)).join("")}
    ${form(`<input type="hidden" name="research_action" value="review"><input type="hidden" name="hypothesis_proposal_id" value="${id}"><input type="hidden" name="decision" value="accept_for_research"><input type="hidden" name="reason" value="Accepted from Research Console"><button class="primary-button research-action-button" type="submit" ${canAccept ? "" : "disabled"}>Accept for Research</button>`)}
    ${form(`<input type="hidden" name="research_action" value="convert"><input type="hidden" name="hypothesis_proposal_id" value="${id}"><input type="hidden" name="approve" value="true"><input type="hidden" name="dataset_snapshot_id" value="${datasetId}"><input type="hidden" name="universe_snapshot_id" value="${universeId}"><input type="hidden" name="start" value="${start}"><input type="hidden" name="end" value="${end}"><button class="ghost-button research-action-button" type="submit" title="${escapeHtml(conversionReason)}" ${canConvert ? "" : "disabled"}>Convert to Research Plan</button>`)}
    ${accepted && !canConvert ? `<span class="research-action-inline-status">${escapeHtml(conversionReason)}</span>` : ""}
  </div>`;
}

function researchLaneLabel(value) {
  const normalized = String(value || "").toLowerCase();
  const labels = {
    proposed: "Ideas",
    watchlist: "Watchlist",
    needs_data: "Data or input pending",
    accepted_for_research: "Accepted for research",
    needs_review: "Needs review",
    rejected: "Rejected",
    archived: "Archived",
  };
  return labels[normalized] || readableStatus(value || "Not reported");
}

function researchReadinessLabel(value) {
  const normalized = String(value || "").toLowerCase();
  if (!normalized) return "Not reported";
  if (normalized.includes("final_eod")) return "Final EOD certification pending";
  if (normalized.includes("macro_event_calendar")) return "Waiting for external macro dataset upload";
  if (normalized.includes("missing_required_symbols") || normalized.includes("needs_data") || normalized.includes("data_needed")) {
    return "Waiting for current intraday market data";
  }
  if (normalized.includes("stale")) return "Market data is stale";
  if (normalized.includes("ready")) return "Ready";
  return readableStatus(value);
}

function researchBlockingSummary(row = {}) {
  const blockers = safeList(row.blocking_items || row.why_blocked);
  const text = blockers.join(" ").toLowerCase();
  if (!blockers.length) return "none";
  if (text.includes("macro_event_calendar")) return "Waiting for external macro dataset upload";
  if (text.includes("final_eod")) return "Final EOD certification pending";
  if (text.includes("missing_required_symbols") || text.includes("needs_data") || text.includes("data_needed")) {
    return "Waiting for current intraday market data";
  }
  if (text.includes("stale")) return "Market data is stale";
  if (text.includes("operator") || text.includes("review")) return "Waiting for operator review";
  return blockers.map((item) => readableStatus(item)).join(", ");
}

function researchNormalizeInventoryRow(row = {}) {
  const id = String(row.hypothesis_id || row.hypothesis_proposal_id || row.item_id || "").trim();
  const symbols = safeList(row.symbols || row.required_symbols || row.missing_symbols).map((symbol) => String(symbol));
  const sourceType = row.source_type || researchSourceLabel(row);
  const sourceSystem = sourceType === "Governed Research" ? "aegis_research_pipeline" : "research_store";
  return {
    ...row,
    hypothesis_id: id,
    hypothesis_proposal_id: id,
    item_id: id,
    source_type: sourceType,
    source_system: row.source_system || sourceSystem,
    title: row.title || row.hypothesis_summary || id || "Research hypothesis",
    lifecycle_state: row.lifecycle_state || row.lane || "Ideas",
    lane: row.lifecycle_state || row.lane || "Ideas",
    event_family: row.event_type || row.event_family || row.event_family_id || "",
    event_family_id: row.event_type || row.event_family || row.event_family_id || "",
    tier: row.tier || row.priority_bucket || "Watchlist",
    priority_bucket: row.tier || row.priority_bucket || "Watchlist",
    rank: row.rank,
    operator_data_status: row.data_status || row.operator_data_status || row.data_requirement_status || "Research status available.",
    data_requirement_status: row.data_status || row.data_requirement_status || "",
    required_symbols: symbols,
    missing_symbols: safeList(row.missing_symbols),
    blocker_reason: row.blocker_summary || row.blocker_reason || "",
    recommended_next_action: row.next_action || row.recommended_next_action || "Review hypothesis",
    operator_action_required: row.operator_action_required === true || row.operator_attention_required === true,
    operator_attention_required: row.operator_action_required === true || row.operator_attention_required === true,
    created_at: row.created_at || "",
    updated_at: row.updated_at || row.created_at || "",
    search_text: row.search_text || [id, row.title, sourceType, symbols.join(" "), row.event_type, row.lifecycle_state, row.tier, row.rank, row.data_status, row.blocker_summary, row.next_action].filter(Boolean).join(" ").toLowerCase(),
  };
}

function researchAllHypothesisRows(payload = {}) {
  const rows = safeList(payload.all_hypotheses).map(researchNormalizeInventoryRow);
  const seen = new Set();
  return rows.filter((row) => {
    const id = String(row.hypothesis_proposal_id || row.item_id || row.title || "").trim();
    if (!id || seen.has(id)) return false;
    seen.add(id);
    return true;
  });
}

function researchLegacySourceHypothesisCount(payload = {}) {
  const queueCount = safeList(payload.hypothesis_queue?.queue || payload.hypothesis_queue?.hypothesis_proposals).length;
  const aegisCount = safeList(payload.aegis_research_pipeline?.items).length;
  return queueCount + aegisCount;
}

function researchInventoryDatasourceError(payload = {}, rows = []) {
  const normalizedProvided = Array.isArray(payload.all_hypotheses);
  const legacyCount = researchLegacySourceHypothesisCount(payload);
  if (!normalizedProvided && legacyCount > 0 && rows.length === 0) {
    return `Research datasource contract mismatch: API returned ${legacyCount} legacy hypothesis rows but did not return all_hypotheses[]. Restart the operator server or rebuild the Research Console API.`;
  }
  if (normalizedProvided && Number(payload.combined_hypothesis_count || 0) > 0 && rows.length === 0) {
    return `Research datasource contract mismatch: API combined_hypothesis_count is ${payload.combined_hypothesis_count} but the inventory normalized to 0 rows.`;
  }
  return "";
}


function researchIsArchived(row = {}) {
  const text = String(row.lifecycle_state || row.lane || row.proposal_status || "").toLowerCase();
  return text === "archived" || text === "rejected" || text === "rejected_archived";
}

function researchLifecycleState(row = {}) {
  if (row.lifecycle_state) return String(row.lifecycle_state);
  const lane = String(row.lane || row.proposal_status || "").toLowerCase();
  if (lane === "archived" || lane === "rejected") return "Archived";
  if (lane === "needs_data") return "Blocked";
  if (lane === "accepted_for_research") return "Researching";
  if (lane === "paper_trial") return "Paper Trial";
  if (lane === "ready") return "Ready";
  return "Ideas";
}

function researchHypothesisTierLabel(row = {}) {
  return row.tier || row.priority_bucket || "Watchlist";
}

function researchOperatorActionRequired(row = {}) {
  if (row.operator_attention_required === true || row.operator_action_required === true || row.needs_operator_review === true) return true;
  const text = [row.recommended_next_action, row.blocker_reason, safeList(row.blocking_items).join(" ")].join(" ").toLowerCase();
  return text.includes("operator") || text.includes("upload_external_dataset") || text.includes("calendar required") || text.includes("missing_macro_event_calendar") || text.includes("review_dossier");
}

function researchTimestamp(row = {}) {
  return row.updated_at || row.created_at || row.reviewed_at || row.scored_at || "";
}

function researchTimestampScore(row = {}) {
  const parsed = Date.parse(researchTimestamp(row));
  return Number.isFinite(parsed) ? parsed : 0;
}

function researchAgeHours(row = {}, field = "updated_at") {
  const raw = row[field] || (field === "updated_at" ? row.created_at : "");
  const parsed = Date.parse(raw || "");
  if (!Number.isFinite(parsed)) return Infinity;
  return Math.max(0, (Date.now() - parsed) / 36e5);
}

function researchIsNew(row = {}) {
  return researchAgeHours(row, "created_at") <= 72;
}

function researchIsRecentlyUpdated(row = {}) {
  return researchAgeHours(row, "updated_at") <= 72 || row.recently_updated === true;
}

function researchTierScore(row = {}) {
  const value = String(researchHypothesisTierLabel(row)).toLowerCase();
  if (value.includes("tier_1") || value.includes("critical") || value.includes("high")) return 5;
  if (value.includes("tier_2") || value.includes("medium")) return 4;
  if (value.includes("watchlist") || value.includes("tier_4")) return 3;
  if (value.includes("blocked")) return 2;
  if (value.includes("low") || value.includes("tier_3") || value.includes("tier_5")) return 1;
  return 0;
}

function researchRankScore(row = {}) {
  const rank = Number(row.rank);
  return Number.isFinite(rank) && rank > 0 ? Math.max(0, 1000 - rank) : 0;
}

function researchSourceLabel(row = {}) {
  if (row.source_type) return String(row.source_type);
  return row.source_system === "aegis_research_pipeline" ? "Governed Research" : "Research Store";
}

function researchSourceScore(row = {}) {
  return row.source_system === "aegis_research_pipeline" ? 2 : 1;
}

function researchLifecycleScore(row = {}) {
  const value = researchLifecycleState(row).toLowerCase();
  if (value === "validating") return 6;
  if (value === "researching") return 5;
  if (value === "ready") return 4;
  if (value === "blocked") return 3;
  if (value === "paper trial") return 2;
  return 1;
}

function researchDiscoverySort(a = {}, b = {}) {
  return (
    Number(researchOperatorActionRequired(b)) - Number(researchOperatorActionRequired(a)) ||
    researchSourceScore(b) - researchSourceScore(a) ||
    researchLifecycleScore(b) - researchLifecycleScore(a) ||
    researchTierScore(b) - researchTierScore(a) ||
    researchRankScore(b) - researchRankScore(a) ||
    researchTimestampScore(b) - researchTimestampScore(a) ||
    String(a.title || a.item_id || "").localeCompare(String(b.title || b.item_id || ""))
  );
}

function researchFocusCandidateRows(rows = []) {
  return safeList(rows)
    .filter((row) => !researchIsArchived(row))
    .filter((row) => researchOperatorActionRequired(row) || researchIsNew(row) || researchIsRecentlyUpdated(row) || researchTierScore(row) >= 4 || ["Researching", "Validating", "Ready", "Blocked"].includes(researchLifecycleState(row)))
    .sort(researchDiscoverySort);
}

function researchFocusGroups(rows = []) {
  const candidates = researchFocusCandidateRows(rows);
  const governed = candidates.filter((row) => row.source_system === "aegis_research_pipeline").slice(0, 5);
  const store = candidates.filter((row) => row.source_system !== "aegis_research_pipeline").slice(0, Math.max(0, 10 - governed.length));
  return [
    { label: "Governed Research", rows: governed },
    { label: "Research Store Proposals", rows: store },
  ].filter((group) => group.rows.length);
}

function researchFocusRows(rows = []) {
  return researchFocusGroups(rows).flatMap((group) => group.rows);
}

function researchDiscoverySearchText(row = {}) {
  return [
    row.search_text,
    row.title,
    row.hypothesis_summary,
    row.hypothesis_proposal_id,
    row.item_id,
    row.event_family,
    row.event_family_id,
    researchLifecycleState(row),
    row.recommended_next_action,
    row.blocker_reason,
    row.operator_data_status,
    row.data_requirement_status,
    safeList(row.required_symbols).join(" "),
    safeList(row.missing_symbols).join(" "),
    safeList(row.blocking_items).join(" "),
  ].filter(Boolean).join(" ").toLowerCase();
}

function researchDataReadinessText(row = {}) {
  return row.operator_data_status || researchReadinessLabel(row.data_requirement_status || row.required_data_mode || row.lane || "");
}

function researchOperatorDataStatusText(row = {}) {
  const raw = researchDataReadinessText(row);
  const normalized = String(raw || "").toLowerCase();
  if (!normalized) return "Not reported";
  if (normalized.includes("inconclusive_sample_size")) return "Inconclusive sample size";
  if (normalized.includes("waiting_for_event_data")) return "Waiting for earnings/event data";
  if (normalized.includes("earnings_event_calendar_required")) return "Earnings/event calendar required";
  if (normalized.includes("upload_external_dataset")) return "External dataset required";
  if (normalized.includes("ready_for_human_review")) return "Ready for review";
  if (normalized.includes("needs_operator")) return "Needs operator decision";
  if (normalized.includes("research data required")) return "Research data required";
  if (normalized.includes("no blocker") || normalized.includes("no operator action")) return "No action required";
  return researchReadinessLabel(raw);
}


function researchPrimaryStatusText(row = {}) {
  const blocker = row.blocker_reason || researchBlockingSummary(row);
  if (blocker && blocker !== "none") return blocker;
  return row.recommended_next_action || researchDataReadinessText(row) || "Review hypothesis";
}

function researchOperatorActionDescriptor(row = {}) {
  const raw = [
    row.data_status,
    row.operator_data_status,
    row.data_requirement_status,
    row.blocker_reason,
    row.blocker_summary,
    row.recommended_next_action,
    row.next_action,
    row.latest_result,
    row.test_status,
    row.current_status,
    row.current_gate,
    safeList(row.blocking_items).join(" "),
  ].filter(Boolean).join(" ").toLowerCase();
  const href = `/research-lab/hypotheses?dossier=${encodeURIComponent(String(row.hypothesis_proposal_id || row.item_id || row.hypothesis_id || ""))}`;
  if (raw.includes("inconclusive_sample_size")) {
    return { label: "Review inconclusive result", button: "Review result", href, disabled: false, tone: "review" };
  }
  if (raw.includes("waiting_for_event_data") || raw.includes("earnings_event_calendar_required") || raw.includes("event calendar required")) {
    return { label: "Provide earnings/event calendar", button: "View data format", href, disabled: false, tone: "data" };
  }
  if (raw.includes("upload_external_dataset")) {
    return { label: "Upload required dataset", button: "View requirements", href, disabled: false, tone: "data" };
  }
  if (raw.includes("research data required") || raw.includes("blocked_research_data_required") || raw.includes("data required before testing")) {
    return { label: "Data required before testing", button: "View missing data", href, disabled: false, tone: "data" };
  }
  if (raw.includes("ready_for_human_review") || raw.includes("result_review") || raw.includes("review result") || raw.includes("needs review")) {
    return { label: "Review and decide", button: "Review", href, disabled: false, tone: "review" };
  }
  if (row.operator_action_required === true || row.operator_attention_required === true) {
    return { label: "Review and decide", button: "Review", href, disabled: false, tone: "review" };
  }
  return { label: "No action required", button: "", href, disabled: true, tone: "none" };
}

function researchOperatorActionButton(row = {}) {
  const action = researchOperatorActionDescriptor(row);
  if (action.disabled || !action.button) {
    return `<span class="research-action-none" data-research-action-label="No action required">No action required</span>`;
  }
  return `<a class="ghost-button research-action-button research-row-action research-action-${escapeHtml(action.tone)}" href="${escapeHtml(action.href)}" data-research-action-label="${escapeHtml(action.label)}">${escapeHtml(action.button)}</a>`;
}



const HYPOTHESIS_USER_STATUSES = ["Ready to Start", "Queued", "Scheduled", "Researching", "Waiting", "Complete", "Blocked", "Recommendation Ready"];
const HYPOTHESIS_VIEW_SECTION_LABELS = {
  recommendations_ready: "Recommendations Ready",
  ready_to_start: "Ready to Start",
  researching: "Researching",
  waiting: "Waiting",
  blocked: "Blocked",
  completed: "Completed",
};
const HYPOTHESIS_VIEW_SECTION_ORDER = ["recommendations_ready", "ready_to_start", "researching", "waiting", "blocked", "completed"];

function hypothesisCleanUserText(value, fallback = "Not reported") {
  const raw = String(value || "").trim();
  if (!raw) return fallback;
  return readableStatus(raw)
    .replace(/\boperator\b/gi, "you")
    .replace(/\blifecycle\b/gi, "status")
    .replace(/\bgovernance\b/gi, "review")
    .replace(/\bprojection\b/gi, "view")
    .replace(/\breplay\b/gi, "history")
    .replace(/\bscheduler\b/gi, "schedule")
    .replace(/_/g, " ");
}

function hypothesisRunState(row = {}) {
  return row.research_run_state && typeof row.research_run_state === "object" ? row.research_run_state : {};
}

function hypothesisUserStatus(row = {}) {
  const runState = hypothesisRunState(row);
  const status = String(row.user_facing_status || row.status || runState.user_facing_status || "Ready to Start").trim();
  return HYPOTHESIS_USER_STATUSES.includes(status) ? status : "Ready to Start";
}

function hypothesisCurrentStage(row = {}) {
  if (row.current_stage) return hypothesisCleanUserText(row.current_stage, "Ready to start");
  const status = hypothesisUserStatus(row);
  if (status === "Recommendation Ready") return "Manual review";
  if (status === "Researching") return "AI research";
  if (status === "Scheduled") return "Scheduled overnight";
  if (status === "Waiting") return "Waiting for data or time window";
  if (status === "Blocked") return "Blocked before research can continue";
  if (status === "Complete") return "Findings ready to review";
  return "Ready to start";
}

function hypothesisSymbolsText(row = {}) {
  return safeList(row.symbols || row.required_symbols || row.related_symbols || row.missing_symbols).map((symbol) => String(symbol).trim()).filter(Boolean).join(", ") || "Symbols not specified";
}

function hypothesisLastUpdatedText(row = {}) {
  const raw = researchTimestamp(row) || row.last_updated || "";
  if (!raw) return "Not updated yet";
  const parsed = new Date(raw);
  if (!Number.isFinite(parsed.getTime())) return hypothesisCleanUserText(raw, "Not updated yet");
  const now = new Date();
  const localDate = new Date(parsed.getFullYear(), parsed.getMonth(), parsed.getDate());
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const dayDiff = Math.round((today.getTime() - localDate.getTime()) / 86400000);
  const timeText = new Intl.DateTimeFormat(undefined, { hour: "numeric", minute: "2-digit" }).format(parsed);
  if (dayDiff === 0) return `Today ${timeText}`;
  if (dayDiff === 1) return `Yesterday ${timeText}`;
  if (dayDiff > 1 && dayDiff < 7) {
    const weekday = new Intl.DateTimeFormat(undefined, { weekday: "long" }).format(parsed);
    return `${weekday} ${timeText}`;
  }
  const dateText = new Intl.DateTimeFormat(undefined, { month: "short", day: "numeric", year: parsed.getFullYear() === now.getFullYear() ? undefined : "numeric" }).format(parsed);
  return `${dateText} ${timeText}`;
}

function hypothesisLastRunText(row = {}) {
  const runState = hypothesisRunState(row);
  const lastRun = row.last_research_run || runState.last_research_run || runState.last_run || {};
  if (!lastRun || typeof lastRun !== "object" || !lastRun.research_run_id) return "No research run yet";
  const status = hypothesisCleanUserText(lastRun.run_status, "Run recorded");
  const when = lastRun.started_at || lastRun.queued_at || lastRun.completed_at || lastRun.requested_at || "";
  return when ? `${status} - ${hypothesisLastUpdatedText({ updated_at: when })}` : status;
}

function hypothesisTriggerSourceText(row = {}) {
  const runState = hypothesisRunState(row);
  const source = row.trigger_source || runState.trigger_source || (runState.run_count ? "Research run" : "None");
  if (!source || source === "None") return "No run trigger yet";
  if (source === "USER_INITIATED") return "Started by you";
  if (source === "SCHEDULED_RUN") return "Scheduled research";
  if (source === "SYSTEM_MONITOR") return "System monitor";
  if (source === "IMPORTED_LEGACY_STATE") return "Imported legacy state";
  if (source === "BACKGROUND_REFRESH") return "Background refresh";
  return hypothesisCleanUserText(source, "Research run");
}

function hypothesisConfidenceSummary(row = {}) {
  const value = row.confidence_summary || row.confidence_label || row.evidence_quality || row.priority_bucket || row.tier || row.confidence_score;
  if (value === undefined || value === null || value === "") return "Not enough evidence yet";
  if (typeof value === "number") return `${Math.round(value * 100)}% confidence`;
  return hypothesisCleanUserText(value, "Not enough evidence yet");
}

function hypothesisRecommendationSummary(row = {}) {
  return row.recommendation_summary || (hypothesisUserStatus(row) === "Recommendation Ready" ? "Recommendation Ready" : "No recommendation yet");
}

function hypothesisUserNextStep(row = {}) {
  if (row.user_facing_explanation) return hypothesisCleanUserText(row.user_facing_explanation, "Review hypothesis status.");
  const status = hypothesisUserStatus(row);
  if (status === "Recommendation Ready") return "Review findings and manually capture in IB only if the recommendation still fits your workflow.";
  if (status === "Blocked") return "Resolve the blocker shown on this card.";
  if (status === "Researching") return "Wait for AI research to finish, then review findings.";
  if (status === "Scheduled") return "Scheduled overnight; check results after the run completes.";
  if (status === "Waiting") return "Waiting for data or the next scheduled research window.";
  if (status === "Complete") return "Open findings and decide whether follow-up research is needed.";
  return "Ready for you to start AI research.";
}

function renderHypothesisStatusBadge(status) {
  const safeStatus = HYPOTHESIS_USER_STATUSES.includes(status) ? status : "Ready to Start";
  return `<span class="hypothesis-status hypothesis-status-${escapeHtml(safeStatus.toLowerCase().replaceAll(" ", "-"))}">${escapeHtml(safeStatus)}</span>`;
}

function commandPayloadAttribute(command = {}, extra = {}) {
  const payload = { ...(command.payload || {}), ...extra };
  return escapeHtml(JSON.stringify(payload));
}

function hypothesisPrimaryAction(row = {}) {
  if (row.primary_command && typeof row.primary_command === "object" && row.primary_command.command_id) return row.primary_command;
  return {
    command_id: "",
    label: "Action unavailable",
    action_type: "IN_PAGE_DETAIL",
    enabled: false,
    disabled_reason: "This hypothesis is missing a declared command contract.",
  };
}

function hypothesisSecondaryActions(row = {}) {
  return safeList(row.secondary_commands).filter((action) => action && action.command_id);
}

function renderHypothesisDetailPanel(row = {}, panelId = "") {
  const status = hypothesisUserStatus(row);
  const title = String(row.title || row.hypothesis_summary || "Research hypothesis");
  const blocker = row.blocker_summary || row.blocker_reason || safeList(row.blocking_items || row.why_blocked).join(", ") || "No blocker is reported for this hypothesis.";
  const nextStep = hypothesisUserNextStep(row);
  const findings = row.findings_summary || row.result_summary || row.confidence_summary || row.evidence_summary || "No findings yet; research is queued, running, waiting, or has not started.";
  const waiting = row.waiting_reason || row.queue_reason || row.user_facing_explanation || "This hypothesis is waiting for its queued research run, data, or the next research window.";
  const recommendation = hypothesisRecommendationSummary(row);
  return `<section id="${escapeHtml(panelId)}" class="hypothesis-inline-detail" data-hypothesis-inline-detail hidden tabindex="-1">
    <div class="hypothesis-inline-detail-header">
      <strong>${escapeHtml(title)}</strong>
      <span>${escapeHtml(status)}</span>
    </div>
    <dl class="hypothesis-inline-detail-grid">
      <div><dt>Why this state</dt><dd>${escapeHtml(status === "Blocked" ? blocker : status === "Waiting" || status === "Queued" || status === "Scheduled" ? waiting : nextStep)}</dd></div>
      <div><dt>Findings</dt><dd>${escapeHtml(findings)}</dd></div>
      <div><dt>Recommendation</dt><dd>${escapeHtml(recommendation)}</dd></div>
      <div><dt>Affected symbols</dt><dd>${escapeHtml(hypothesisSymbolsText(row))}</dd></div>
    </dl>
  </section>`;
}

function renderCommandButton(command = {}, className = "ghost-button", { detailPanelId = "", hypothesisId = "", title = "", symbols = "" } = {}) {
  const commandId = String(command.command_id || "");
  const label = String(command.label || "Action unavailable");
  const actionType = String(command.action_type || "IN_PAGE_DETAIL");
  const enabled = command.enabled !== false && Boolean(commandId);
  const disabledReason = String(command.disabled_reason || command.failure_behavior || "This action is not available.");
  const payload = commandPayloadAttribute(command, { hypothesis_id: hypothesisId, title, idea: title, symbols });
  const baseAttrs = `data-aegis-command-id="${escapeHtml(commandId)}" data-aegis-command-action-type="${escapeHtml(actionType)}" data-aegis-command-target-type="${escapeHtml(command.target_type || "hypothesis")}" data-aegis-command-target-id="${escapeHtml(command.target_id || hypothesisId)}" data-aegis-command-payload="${payload}"`;
  if (!enabled) {
    return `<button class="${escapeHtml(className)} research-action-button" type="button" ${baseAttrs} disabled aria-disabled="true" title="${escapeHtml(disabledReason)}">${escapeHtml(label)}</button>`;
  }
  const detailAttrs = actionType === "IN_PAGE_DETAIL" ? ` data-command-detail-target="${escapeHtml(detailPanelId)}" data-hypothesis-detail-target="${escapeHtml(detailPanelId)}" data-hypothesis-detail-action="${escapeHtml(label)}"` : "";
  return `<button class="${escapeHtml(className)} research-action-button${className.includes("primary") ? " hypothesis-primary-action" : ""}" type="button" ${baseAttrs}${detailAttrs}>${escapeHtml(label)}</button>`;
}

function renderHypothesisActionBar(row = {}, detailPanelId = "") {
  const primary = hypothesisPrimaryAction(row);
  const secondary = hypothesisSecondaryActions(row);
  const hypothesisId = String(row.hypothesis_id || row.hypothesis_proposal_id || row.item_id || "");
  const title = String(row.title || row.hypothesis_summary || "Research hypothesis");
  const symbols = hypothesisSymbolsText(row);
  const commandContext = { detailPanelId, hypothesisId, title, symbols };
  const secondaryMarkup = secondary.length
    ? `<details class="hypothesis-more-menu"><summary>More</summary><div>${secondary.map((action) => renderCommandButton(action, "ghost-button", commandContext)).join("")}</div></details>`
    : "";
  return `<div class="hypothesis-card-actions" aria-label="Hypothesis actions" data-command-surface="hypothesis-card">
    ${renderCommandButton(primary, "primary-button", commandContext)}
    <span class="research-action-inline-status" data-aegis-command-status data-research-console-status></span>
    ${secondaryMarkup}
  </div>`;
}

function renderHypothesisCard(row = {}) {
  const id = String(row.hypothesis_proposal_id || row.item_id || row.hypothesis_id || "");
  const status = hypothesisUserStatus(row);
  const recommendation = hypothesisRecommendationSummary(row);
  const anchor = row.card_anchor_id || safeDomId(`hypothesis-card-${id}`);
  const detailPanelId = `${anchor}-detail`;
  return `<article id="${escapeHtml(anchor)}" class="hypothesis-card" data-hypothesis-card data-hypothesis-status="${escapeHtml(status)}" data-hypothesis-id="${escapeHtml(id)}" tabindex="-1">
    <header class="hypothesis-card-header">
      <div>
        <h3>${escapeHtml(row.title || row.hypothesis_summary || "Research hypothesis")}</h3>
        <p>${escapeHtml(hypothesisUserNextStep(row))}</p>
      </div>
      ${renderHypothesisStatusBadge(status)}
    </header>
    <dl class="hypothesis-card-facts">
      <div><dt>Status</dt><dd>${escapeHtml(status)}</dd></div>
      <div><dt>Current stage</dt><dd>${escapeHtml(hypothesisCurrentStage(row))}</dd></div>
      <div><dt>Symbols</dt><dd>${escapeHtml(hypothesisSymbolsText(row))}</dd></div>
      <div><dt>Last run</dt><dd>${escapeHtml(hypothesisLastRunText(row))}</dd></div>
      <div><dt>Trigger source</dt><dd>${escapeHtml(hypothesisTriggerSourceText(row))}</dd></div>
      <div><dt>Recommendation</dt><dd>${escapeHtml(recommendation)}</dd></div>
    </dl>
    ${recommendation === "Recommendation Ready" ? `<div class="hypothesis-recommendation-callout"><strong>Manual IB capture guidance</strong><span>Review related symbols and confidence before taking any external action.</span></div>` : ""}
    ${renderHypothesisDetailPanel(row, detailPanelId)}
    <div class="hypothesis-card-message" data-hypothesis-card-message-output hidden></div>
    ${renderHypothesisActionBar(row, detailPanelId)}
  </article>`;
}

function emptyHypothesisViewSection(sectionId) {
  return {
    section_id: sectionId,
    label: HYPOTHESIS_VIEW_SECTION_LABELS[sectionId] || sectionId,
    count: 0,
    top_item: null,
    visible_items: [],
    overflow_count: 0,
    collapsed_by_default: true,
  };
}

function hypothesisViewModel(payload = {}) {
  const model = payload.hypothesis_view_model_v1 && typeof payload.hypothesis_view_model_v1 === "object" ? payload.hypothesis_view_model_v1 : {};
  const sections = model.sections && typeof model.sections === "object" ? model.sections : {};
  const sectionOrder = safeList(model.section_order).length ? safeList(model.section_order) : HYPOTHESIS_VIEW_SECTION_ORDER;
  return {
    schema_id: model.schema_id || "hypothesis_view_model.v1",
    section_order: sectionOrder,
    sections: Object.fromEntries(sectionOrder.map((sectionId) => [sectionId, sections[sectionId] || emptyHypothesisViewSection(sectionId)])),
    summary_cards: safeList(model.summary_cards),
    primary_cta: model.primary_cta || null,
    raw_hypothesis_count: Number(model.raw_hypothesis_count || 0),
    rendered_hypothesis_count: Number(model.rendered_hypothesis_count || 0),
    unmapped_hypothesis_ids: safeList(model.unmapped_hypothesis_ids),
  };
}

function renderHypothesesSummaryCounts(viewModel = {}) {
  const cards = safeList(viewModel.summary_cards).length
    ? safeList(viewModel.summary_cards)
    : safeList(viewModel.section_order).map((sectionId) => {
        const section = viewModel.sections?.[sectionId] || emptyHypothesisViewSection(sectionId);
        return {
          section_id: sectionId,
          label: section.label,
          count: section.count,
          top_item: section.top_item,
          href: section.top_item ? `#${section.top_item.card_anchor_id}` : `#hypothesis-section-${sectionId}`,
        };
      });
  return `<section class="hypotheses-summary-counts" data-hypotheses-summary-counts>
    ${cards.map((card) => {
      const topTitle = card.top_item?.title || "No visible hypothesis";
      const count = Number(card.count || 0);
      const sectionTarget = `hypothesis-section-${card.section_id}`;
      const cardTarget = card.top_item?.card_anchor_id || "";
      const sectionControl = count > 0
        ? `<button class="hypotheses-summary-section-button" type="button" data-aegis-command-id="SCROLL_TO_HYPOTHESIS_SECTION" data-aegis-command-action-type="SCROLL_FOCUS" data-aegis-command-target-type="hypotheses_summary" data-aegis-command-target-id="${escapeHtml(sectionTarget)}" data-hypothesis-summary-section-target="${escapeHtml(sectionTarget)}" aria-label="Show ${escapeHtml(card.label)} hypotheses"><span>${escapeHtml(card.label)}</span><strong>${escapeHtml(String(count))}</strong></button>`
        : `<div class="hypotheses-summary-section-label" aria-label="${escapeHtml(card.label)} hypotheses"><span>${escapeHtml(card.label)}</span><strong>${escapeHtml(String(count))}</strong></div>`;
      const cardControl = cardTarget
        ? `<button class="hypotheses-summary-card-button" type="button" data-aegis-command-id="FOCUS_HYPOTHESIS_CARD" data-aegis-command-action-type="SCROLL_FOCUS" data-aegis-command-target-type="hypotheses_summary" data-aegis-command-target-id="${escapeHtml(cardTarget)}" data-hypothesis-summary-card-target="${escapeHtml(cardTarget)}" data-hypothesis-summary-section-target="${escapeHtml(sectionTarget)}" aria-label="Show ${escapeHtml(topTitle)}"><small>${escapeHtml(topTitle)}</small></button>`
        : `<small class="hypotheses-summary-card-label">${escapeHtml(topTitle)}</small>`;
      return `<div class="hypotheses-summary-count${count > 0 ? " is-interactive" : " is-empty"}" data-hypotheses-summary-card="${escapeHtml(card.section_id)}">
        ${sectionControl}
        ${cardControl}
      </div>`;
    }).join("")}
  </section>`;
}

function renderHypothesesHeroCta(viewModel = {}) {
  const cta = viewModel.primary_cta;
  if (!cta || !cta.label || !cta.href) {
    return `<a class="ghost-button research-action-button" href="#hypothesis-section-ready_to_start">Choose ready hypothesis</a>`;
  }
  return `<a class="primary-button research-action-button" data-top-ready-cta href="${escapeHtml(cta.href)}">${escapeHtml(cta.label)}</a>
    <a class="ghost-button research-action-button" href="${escapeHtml(cta.target_anchor || "#hypothesis-section-ready_to_start")}">Show card</a>`;
}

function renderHypothesisSection(section = {}) {
  const sectionId = section.section_id || "unknown";
  const label = section.label || HYPOTHESIS_VIEW_SECTION_LABELS[sectionId] || sectionId;
  const rows = safeList(section.visible_items);
  const count = Number(section.count || rows.length || 0);
  const overflow = Number(section.overflow_count || 0);
  const heading = `<div class="hypothesis-section-heading"><h2>${escapeHtml(label)}</h2><span>${escapeHtml(String(count))}</span></div>`;
  const body = `<div class="hypothesis-card-grid">
      ${rows.length ? rows.map(renderHypothesisCard).join("") : `<div class="hypothesis-empty-state">No hypotheses in this section.</div>`}
      ${overflow > 0 ? `<div class="hypothesis-empty-state">${escapeHtml(String(overflow))} more in diagnostics.</div>` : ""}
    </div>`;
  if (section.collapsed_by_default || count === 0) {
    return `<details class="hypothesis-section hypothesis-section-collapsed" data-hypothesis-section="${escapeHtml(label)}" id="hypothesis-section-${escapeHtml(sectionId)}">
      <summary>${heading}<small>${count === 0 ? "Empty" : `Show ${escapeHtml(String(count))} items`}</small></summary>
      ${body}
    </details>`;
  }
  return `<section class="hypothesis-section" data-hypothesis-section="${escapeHtml(label)}" id="hypothesis-section-${escapeHtml(sectionId)}">
    ${heading}
    ${body}
  </section>`;
}

function renderHypothesesDiagnosticsPanel(payload = {}, state = {}) {
  return `<details class="hypotheses-diagnostics" data-hypotheses-diagnostics>
    <summary>Diagnostics</summary>
    ${renderResearchInventoryPanel(payload)}
    ${renderResearchPipelineSummaryPanel(payload)}
    ${renderResearchSecondaryPanels(payload, state)}
  </details>`;
}

function renderHypothesesWorkspace(payload = {}, state = {}) {
  const viewModel = hypothesisViewModel(payload);
  const allRows = researchAllHypothesisRows(payload);
  const datasourceError = researchInventoryDatasourceError(payload, allRows);
  return `<div class="hypotheses-workspace" data-hypotheses-workspace data-hypothesis-view-model="${escapeHtml(viewModel.schema_id)}">
    <section class="hypotheses-hero" data-hypotheses-primary-actions>
      <div>
        <p class="section-eyebrow">HYPOTHESES</p>
        <h1>Hypotheses</h1>
        <p>See hypotheses, start AI research, monitor progress, review findings, and check whether recommendations emerged.</p>
      </div>
      <div class="hypotheses-hero-actions" data-hypotheses-total-count>
        <div class="hypotheses-total-count"><span>Total hypotheses</span><strong>${escapeHtml(String(viewModel.rendered_hypothesis_count || 0))}</strong></div>
      </div>
    </section>
    ${datasourceError ? `<div class="callout warning" data-research-datasource-fatal>${escapeHtml(datasourceError)}</div>` : ""}
    ${state.researchConsoleWorkflow?.lastResult || state.researchConsoleWorkflow?.lastError ? renderResearchConsoleActionFeedback(state.researchConsoleWorkflow || {}) : ""}
    ${viewModel.unmapped_hypothesis_ids.length ? `<details class="callout warning" data-unmapped-hypotheses><summary>Unmapped hypotheses: ${escapeHtml(String(viewModel.unmapped_hypothesis_ids.length))}</summary><div>${viewModel.unmapped_hypothesis_ids.map(escapeHtml).join(", ")}</div></details>` : ""}
    ${renderHypothesesSummaryCounts(viewModel)}
    <div class="hypotheses-sections">
      ${safeList(viewModel.section_order).map((sectionId) => renderHypothesisSection(viewModel.sections[sectionId] || emptyHypothesisViewSection(sectionId))).join("")}
    </div>
  </div>`;
}

function researchFilterTokens(row = {}) {
  const lifecycle = researchLifecycleState(row).toLowerCase();
  const tokens = ["all", lifecycle.replaceAll(" ", "-")];
  if (researchIsNew(row)) tokens.push("new");
  if (researchIsRecentlyUpdated(row)) tokens.push("recently-updated");
  if (researchOperatorActionRequired(row)) tokens.push("needs-review");
  if (lifecycle === "blocked") tokens.push("blocked");
  if (researchTierScore(row) >= 4) tokens.push("tier-12");
  return tokens.join(" ");
}

function renderResearchCompactHypothesisRow(row = {}, options = {}) {
  const id = String(row.hypothesis_proposal_id || row.item_id || "");
  const lifecycle = researchLifecycleState(row);
  const attention = researchOperatorActionRequired(row);
  const searchText = researchDiscoverySearchText(row);
  const newBadge = researchIsNew(row) ? `<span class="edge-badge research-badge-new">New</span>` : "";
  const recentBadge = researchIsRecentlyUpdated(row) ? `<span class="edge-badge research-badge-recent">Recently updated</span>` : "";
  const compactClass = options.focus ? " research-focus-row" : "";
  const title = row.title || "Research hypothesis";
  return `<article class="research-compact-row${compactClass}${researchIsRecentlyUpdated(row) ? " is-recent" : ""}" data-research-discovery-card data-hypothesis-proposal-id="${escapeHtml(id)}" data-research-search="${escapeHtml(searchText)}" data-research-filter-tokens="${escapeHtml(researchFilterTokens(row))}" data-research-lifecycle="${escapeHtml(lifecycle)}" data-research-operator-action-required="${attention ? "true" : "false"}">
    <div class="research-compact-main">
      <strong class="research-compact-title">${escapeHtml(title)}</strong>
      <div class="research-compact-meta">
        <span>${escapeHtml(lifecycle)}</span>
        <span>${escapeHtml(researchSourceLabel(row))}</span>
        <span>${escapeHtml(researchHypothesisTierLabel(row))}</span>
        ${row.rank ? `<span>Rank ${escapeHtml(String(row.rank))}</span>` : ""}
        <span>${escapeHtml(researchOperatorActionDescriptor(row).label)}</span>
      </div>
    </div>
    <div class="research-compact-status">${escapeHtml(researchOperatorActionDescriptor(row).label)}</div>
    <div class="research-compact-time">${newBadge}${recentBadge}<span>${escapeHtml(researchTimestamp(row) || "not updated")}</span></div>
    ${researchOperatorActionButton(row)}
  </article>`;
}

function researchInitialSearchQuery() {
  return String(currentSearchParams().get("research_search") || "").trim();
}

function researchMatchesInventorySearch(row = {}, query = "") {
  const needle = String(query || "").trim().toLowerCase();
  if (!needle) return true;
  return researchDiscoverySearchText(row).includes(needle);
}

function renderResearchInventoryPanel(payload = {}) {
  const rows = researchAllHypothesisRows(payload).filter((row) => !researchIsArchived(row)).sort(researchDiscoverySort);
  const datasourceError = researchInventoryDatasourceError(payload, rows);
  const initialQuery = researchInitialSearchQuery();
  const filteredRows = rows.filter((row) => researchMatchesInventorySearch(row, initialQuery));
  const noResultsHidden = filteredRows.length ? " hidden" : "";
  const rowMarkup = rows.map((row) => {
    const id = String(row.hypothesis_proposal_id || row.item_id || "");
    const lifecycle = researchLifecycleState(row);
    const attention = researchOperatorActionRequired(row);
    const searchText = researchDiscoverySearchText(row);
    const hidden = researchMatchesInventorySearch(row, initialQuery) ? "" : " hidden";
    const blockerRaw = row.blocker_reason || researchBlockingSummary(row) || "None";
    const blockerText = String(blockerRaw).trim().toLowerCase();
    const blocker = blockerText === "none" || blockerText === "no blocker" ? "None" : blockerRaw;
    const operatorAction = researchOperatorActionDescriptor(row);
    const nextAction = operatorAction.label;
    const symbols = safeList(row.required_symbols).join(", ") || "not specified";
    const rank = row.rank ? ` / ${row.rank}` : "";
    return `<tr${hidden} data-research-inventory-row data-research-discovery-card data-hypothesis-proposal-id="${escapeHtml(id)}" data-research-search="${escapeHtml(searchText)}" data-research-filter-tokens="${escapeHtml(researchFilterTokens(row))}" data-research-lifecycle="${escapeHtml(lifecycle)}" data-research-operator-action-required="${attention ? "true" : "false"}">
      <td><strong class="research-inventory-title">${escapeHtml(row.title || "Research hypothesis")}</strong><div class="muted-mini">${escapeHtml(id)}</div></td>
      <td>${escapeHtml(researchSourceLabel(row))}</td>
      <td class="research-inventory-symbols">${escapeHtml(symbols)}</td>
      <td>${escapeHtml(lifecycle)}</td>
      <td>${escapeHtml(`${researchHypothesisTierLabel(row)}${rank}`)}</td>
      <td>${escapeHtml(researchOperatorDataStatusText(row) || "not reported")}</td>
      <td>${escapeHtml(blocker)}</td>
      <td>${escapeHtml(nextAction)}</td>
      <td>${attention ? "Yes" : "No"}</td>
      <td>${researchOperatorActionButton(row)}</td>
      <td>${escapeHtml(researchTimestamp(row) || "not updated")}</td>
      <td><a class="ghost-button research-action-button research-open-details" href="/research-lab/hypotheses?dossier=${escapeHtml(id)}" data-aegis-command-id="OPEN_VALID_ROUTE" data-aegis-command-action-type="EXPAND_SECTION" data-aegis-command-target-type="navigation" data-aegis-command-target-id="research_inventory_detail" data-route="/research-lab/hypotheses?dossier=${escapeHtml(id)}">Open</a></td>
    </tr>`;
  }).join("");
  return renderCardSection({
    eyebrow: "RESEARCH_INVENTORY",
    title: "Research Inventory",
    subtitle: "Searchable, sortable inventory of every non-archived governed and store hypothesis.",
    body: `${datasourceError ? `<div class="support-note research-datasource-fatal" data-research-datasource-fatal>${escapeHtml(datasourceError)}</div>` : ""}${renderResearchHypothesisSearchControl(rows, initialQuery, filteredRows.length)}
      <div class="research-inventory-scroll">
        <table class="research-inventory-table">
          <thead><tr><th>Title</th><th>Source</th><th>Symbols</th><th>Lifecycle</th><th>Tier / Rank</th><th>Data Status</th><th>Blocker</th><th>Next Action</th><th>Operator Action Required</th><th>Action</th><th>Last Updated</th><th>Open</th></tr></thead>
          <tbody>${rowMarkup}</tbody>
        </table>
      </div>
      <div class="edge-kanban-empty research-inventory-no-results" data-research-no-results${noResultsHidden}>No hypotheses found.</div>`,
  }).replace('class="panel-card"', 'class="panel-card research-inventory-panel"');
}

function renderResearchHypothesisSearchControl(rows = [], initialQuery = "", visibleCount = rows.length) {
  const filters = [
    ["all", "All"],
    ["new", "New"],
    ["researching", "Researching"],
    ["needs-review", "Needs Review"],
    ["blocked", "Blocked"],
    ["tier-12", "Tier 1/2"],
    ["recently-updated", "Recently Updated"],
  ];
  const countText = initialQuery ? `${visibleCount} results for ${initialQuery}` : `${rows.length} hypotheses`;
  return `<div class="research-search-filter-bar" data-research-hypothesis-search-region data-active-filter="all">
    <label class="operator-search-label research-search-box">Search<input data-research-hypothesis-search type="search" value="${escapeHtml(initialQuery)}" placeholder="Title, id, symbol, event type, lifecycle, next action, blocker" aria-label="Search all research hypotheses" /></label>
    <span class="muted-mini research-result-count" data-research-hypothesis-search-count>${escapeHtml(countText)}</span>
    <div class="research-quick-filters" aria-label="Research quick filters">${filters.map(([value, label], index) => `<button class="ghost-button research-action-button${index === 0 ? " is-active" : ""}" type="button" data-research-quick-filter="${escapeHtml(value)}">${escapeHtml(label)}</button>`).join("")}</div>
  </div>`;
}

function renderResearchFocusAttentionPanel(payload = {}) {
  const groups = researchFocusGroups(researchAllHypothesisRows(payload));
  const body = groups.length
    ? groups.map((group) => `<div class="research-source-focus-group" data-research-source-group="${escapeHtml(group.label)}"><div class="research-source-focus-heading"><strong>${escapeHtml(group.label)}</strong><span>${escapeHtml(String(group.rows.length))}</span></div><div class="research-focus-list">${group.rows.map((row) => renderResearchCompactHypothesisRow(row, { focus: true })).join("")}</div></div>`).join("")
    : `<div class="edge-kanban-empty">No urgent research items.</div>`;
  return renderCardSection({
    eyebrow: "FOCUS_ATTENTION",
    title: "Focus / Attention",
    subtitle: "Top operator actions, new research, recent updates, high-tier work, and important blockers. Discovery still starts in Research Inventory.",
    body: `<div data-research-focus-list>${body}</div>`,
  }).replace('class="panel-card"', 'class="panel-card research-compact-panel research-focus-panel"');
}

function renderResearchPipelineSummaryPanel(payload = {}) {
  const rows = researchAllHypothesisRows(payload);
  const columns = ["Ideas", "Researching", "Validating", "Paper Trial", "Ready", "Blocked", "Archived"];
  return renderCardSection({
    eyebrow: "PIPELINE_SUMMARY",
    title: "Pipeline Summary",
    subtitle: "Lifecycle counts and top examples only. Full discovery and validation remain in Research Inventory.",
    body: `<div class="research-compact-kanban" aria-label="Research lifecycle summary columns">
      ${columns.map((column) => {
        const allColumnRows = rows.filter((row) => researchLifecycleState(row) === column).sort(researchDiscoverySort);
        const columnRows = allColumnRows.slice(0, 3);
        return `<section class="research-compact-column" data-research-lifecycle-column="${escapeHtml(column)}">
          <div class="research-compact-column-header"><strong>${escapeHtml(column)}</strong><span>${escapeHtml(String(allColumnRows.length))}</span></div>
          <div class="research-compact-column-stack">${columnRows.length ? columnRows.map(renderResearchCompactHypothesisRow).join("") : `<div class="edge-kanban-empty">No items.</div>`}</div>
        </section>`;
      }).join("")}
    </div>`,
  }).replace('class="panel-card"', 'class="panel-card research-compact-panel"');
}

function renderResearchBackgroundBlockedPanel(payload = {}) {
  const rows = researchAllHypothesisRows(payload)
    .filter((row) => researchLifecycleState(row) === "Blocked" && (!researchOperatorActionRequired(row) || researchTierScore(row) < 4))
    .sort(researchDiscoverySort);
  return `<details class="support-note research-background-blocked" data-research-background-blocked>
    <summary>Background Blocked Research (${escapeHtml(String(rows.length))})</summary>
    ${rows.length ? `<div class="research-focus-list">${rows.map(renderResearchCompactHypothesisRow).join("")}</div>` : `<div class="edge-kanban-empty">No low-priority legacy blocked backlog was found.</div>`}
  </details>`;
}

function renderResearchAdvancedDiagnosticsPanel(payload = {}, state = {}) {
  return `<details class="support-note research-diagnostics-collapsed" data-research-advanced-diagnostics>
    <summary>Diagnostics / Advanced</summary>
    ${renderWhatNeedsAttentionPanel(payload)}
    ${renderBlockedEvidencePanel(payload)}
    ${renderResearchBacklogPanel(payload)}
    <div id="hypothesis-queue-diagnostics">${renderHypothesisQueueConsolePanel(payload, state)}</div>
  </details>`;
}

function renderResearchSecondaryPanels(payload = {}, state = {}) {
  return `<details class="support-note research-secondary-collapsed" data-research-secondary-panels>
    <summary>Secondary / Diagnostics</summary>
    ${renderResearchBackgroundBlockedPanel(payload)}
    <div id="paper-trials" class="research-compact-table-wrap">${renderPaperTrialsConsolePanel(payload)}</div>
    ${renderResearchAdvancedDiagnosticsPanel(payload, state)}
  </details>`;
}

function renderResearchDiscoveryPanels(payload = {}, state = {}) {
  return [renderHypothesesWorkspace(payload, state)];
}

function researchBlockedWhy(row = {}) {
  const text = safeList(row.why_blocked || row.blocking_items).join(" ").toLowerCase();
  if (text.includes("missing_required_symbols") || text.includes("needs_data") || text.includes("data_needed")) {
    return "Aegis is waiting for current intraday market data before this research can continue.";
  }
  if (text.includes("stale")) {
    return "Latest market data is stale for this research item.";
  }
  if (text.includes("operator") || text.includes("review")) {
    return "An operator decision is required before this item can move forward.";
  }
  return safeList(row.why_blocked).map((item) => readableStatus(item)).join(", ") || "not reported";
}

function renderBlockedEvidencePanel(payload = {}) {
  const rows = safeList(payload.blocked_evidence?.blocked_items || payload.blocked_items);
  return renderCardSection({
    eyebrow: "BLOCKED WORK",
    title: "Blocked Items",
    subtitle: "Research work waiting on data, outcomes, evidence, or an operator decision.",
    body: renderSimpleTable({
      columns: [
        { label: "Blocked", render: (row) => `<strong>${escapeHtml(row.title || row.what_is_blocked || "Blocked item")}</strong><div class="muted-mini">${escapeHtml(row.item_type || "")}</div>` },
        { label: "Why", render: (row) => escapeHtml(researchBlockedWhy(row)) },
        { label: "Missing", render: (row) => escapeHtml(safeList(row.missing).join(", ") || "not reported") },
        { label: "Next action", key: "next_action" },
      ],
      rows,
      emptyMessage: "No blocked research work items were reported.",
    }),
  });
}

function renderPaperTrialsConsolePanel(payload = {}) {
  const rows = safeList(payload.paper_trials?.paper_trials || payload.paper_trials);
  return renderCardSection({
    eyebrow: "PAPER TRIALS",
    title: "Paper Trials",
    subtitle: "Continue observation and outcome measurement. No execution is available here.",
    body: [
      rows.some((row) => row.due_outcomes_need_measurement) ? `<div class="callout warning">Due outcomes need measurement</div>` : "",
      renderSimpleTable({
        columns: [
          { label: "Paper trial", key: "paper_trial_id" },
          { label: "Sleeve", key: "sleeve_name" },
          { label: "Status", key: "status" },
          { label: "Observations", render: (row) => escapeHtml(String(row.observation_count ?? 0)) },
          { label: "Candidates", render: (row) => escapeHtml(String(row.candidate_count_total ?? 0)) },
          { label: "Due", render: (row) => escapeHtml(String(row.due_outcomes ?? row.pending_due_outcomes ?? 0)) },
          { label: "Measured", render: (row) => escapeHtml(String(row.measured_candidate_count ?? 0)) },
          { label: "Next", key: "recommended_next_action" },
          { label: "Actions", render: (row) => `<div class="research-action-toolbar compact"><a class="ghost-button research-action-button" href="/api/research-lab/paper-trials/${encodeURIComponent(String(row.paper_trial_id || ""))}/summary" target="_blank" rel="noopener noreferrer">Open Summary</a><button class="ghost-button research-action-button" type="button">Record Observation</button><button class="ghost-button research-action-button" type="button">Measure Outcomes</button><button class="ghost-button research-action-button" type="button">Review Paper Trial</button></div>` },
        ],
        rows,
        emptyMessage: "No paper trials were found.",
      }),
    ].join(""),
  });
}

function renderSleeveReviewCenterPanel(payload = {}) {
  const rows = safeList(payload.sleeve_review_center?.sleeves || payload.sleeves);
  return renderCardSection({
    eyebrow: "SLEEVE REVIEW CENTER",
    title: "Ready Review",
    subtitle: "Review sleeve health and evidence chains. Decisions remain append-only.",
    body: renderSimpleTable({
      columns: [
        { label: "Sleeve", render: (row) => `<strong>${escapeHtml(row.sleeve_name || row.sleeve_id || "Sleeve")}</strong>` },
        { label: "Health", key: "overall_health" },
        { label: "Drift", key: "drift_status" },
        { label: "Fragility", key: "fragility_status" },
        { label: "Paper trial", key: "paper_trial_status" },
        { label: "Challenge", key: "challenge_type" },
        { label: "Next", key: "recommended_action" },
        { label: "Latest review", key: "latest_review" },
        { label: "Actions", render: (row) => `<div class="research-action-toolbar compact"><a class="ghost-button research-action-button" href="/api/research-lab/sleeves/${encodeURIComponent(String(row.sleeve_id || ""))}/evidence-chain" target="_blank" rel="noopener noreferrer">Open Evidence Chain</a><button class="ghost-button research-action-button" type="button">Review Sleeve</button><button class="ghost-button research-action-button" type="button">Continue Observation</button></div>` },
      ],
      rows,
      emptyMessage: "No sleeve review rows were found.",
    }),
  });
}

function renderResearchBacklogPanel(payload = {}) {
  const rows = safeList(payload.research_backlog?.backlog || payload.backlog);
  return renderCardSection({
    eyebrow: "RESEARCH BACKLOG",
    title: "Research Backlog",
    subtitle: "Prioritized hypotheses, plans, paper trials, and sleeve review rows.",
    body: renderSimpleTable({
      columns: [
        { label: "Rank", render: (row) => escapeHtml(String(row.rank ?? "")) },
        { label: "Item", key: "label" },
        { label: "Type", key: "type" },
        { label: "Priority", key: "priority_bucket" },
        { label: "Status", key: "status" },
        { label: "Next", key: "recommended_next_action" },
        { label: "Blocking", render: (row) => escapeHtml(researchBlockingSummary(row)) },
      ],
      rows,
      emptyMessage: "No research backlog rows were found.",
    }),
  });
}



function renderWhatNeedsAttentionPanel(payload = {}) {
  const attention = payload.what_needs_attention || {};
  const rows = [
    { label: "Top blocked item", value: attention.top_blocked_item?.title || attention.top_blocked_item?.what_is_blocked || "No blocked item reported", next: attention.top_blocked_item?.next_action || attention.top_blocked_item?.recommended_next_action || "Review blocked work" },
    { label: "Top hypothesis to review", value: attention.top_hypothesis_to_review?.title || "No hypothesis waiting for review", next: attention.top_hypothesis_to_review?.recommended_next_action || "Review Research Pipeline" },
    { label: "Top paper trial action", value: attention.top_paper_trial_action?.sleeve_name || attention.top_paper_trial_action?.paper_trial_id || "No paper trial action reported", next: attention.top_paper_trial_action?.recommended_next_action || "Continue Paper Trials" },
    { label: "Top sleeve review action", value: attention.top_sleeve_review_action?.sleeve_name || "No sleeve review action reported", next: attention.top_sleeve_review_action?.recommended_action || "Review Sleeves" },
  ];
  return renderCardSection({
    eyebrow: "WHAT NEEDS MY ATTENTION",
    title: "What needs my attention?",
    subtitle: "A short operator queue across hypotheses, data blockers, paper trials, and sleeves.",
    body: renderSimpleTable({
      columns: [
        { label: "Area", key: "label" },
        { label: "Item", key: "value" },
        { label: "Next action", key: "next" },
      ],
      rows,
      emptyMessage: "No attention items were reported.",
    }),
  });
}


function renderResearchDossierPanel(payload = {}) {
  if (!payload || payload.ok === false) {
    return renderCardSection({
      eyebrow: "HYPOTHESIS",
      title: "Hypothesis unavailable",
      subtitle: payload?.error || "The hypothesis detail could not be loaded.",
      body: `<div class="callout warning">Open the Hypotheses workspace and try again.</div>`,
    });
  }
  const sections = payload.operator_sections || {};
  const readiness = payload.readiness || {};
  const priority = payload.priority || {};
  const row = researchNormalizeInventoryRow({
    ...payload,
    title: payload.title || sections["What is the hypothesis?"] || payload.hypothesis || payload.hypothesis_proposal_id,
    required_symbols: readiness.required_symbols || payload.required_symbols || payload.symbols,
    missing_symbols: readiness.missing_symbols || payload.missing_symbols,
    operator_data_status: readiness.operator_data_status || payload.operator_data_status,
    recommended_next_action: priority.recommended_next_action || payload.recommended_next_action,
    blocker_reason: safeList(readiness.blocking_items).join(", ") || payload.blocker_reason,
  });
  const status = hypothesisUserStatus(row);
  const mainSections = [
    { title: "Findings", body: sections["What can I do next?"] || priority.recommended_next_action || "Findings are not available yet." },
    { title: "Evidence", body: sections["Why might it matter?"] || payload.why_it_might_matter || "Evidence has not been summarized yet." },
    { title: "Affected symbols", body: hypothesisSymbolsText(row) },
    { title: "Candidate impacts", body: sections["What regimes should be checked?"] || "No candidate impacts reported yet." },
    { title: "Recommendation status", body: hypothesisRecommendationSummary(row) },
  ];
  return `<div class="hypothesis-detail" data-hypothesis-detail>
    <section class="hypothesis-detail-summary">
      <div>
        <a class="ghost-button research-action-button" href="/research-lab/hypotheses" data-aegis-command-id="OPEN_VALID_ROUTE" data-aegis-command-action-type="EXPAND_SECTION" data-aegis-command-target-type="navigation" data-aegis-command-target-id="research_hypotheses" data-route="/research-lab/hypotheses">Back to Hypotheses</a>
        <h1>${escapeHtml(row.title || "Hypothesis")}</h1>
        <p>${escapeHtml(hypothesisUserNextStep(row))}</p>
      </div>
      ${renderHypothesisStatusBadge(status)}
    </section>
    <section class="hypothesis-detail-facts">
      ${renderDefinitionRows([
        { label: "Status", value: status },
        { label: "Current stage", value: hypothesisCurrentStage(row) },
        { label: "Expected next step", value: hypothesisUserNextStep(row) },
        { label: "Expected completion", value: status === "Queued" ? "After the overnight run" : status === "Researching" ? "When AI research completes" : "Not scheduled today" },
      ])}
    </section>
    <section class="hypothesis-detail-grid">
      ${mainSections.map((section) => renderCardSection({
        eyebrow: "HYPOTHESIS",
        title: section.title,
        subtitle: "",
        body: `<p>${escapeHtml(section.body)}</p>`,
      })).join("")}
    </section>
    ${hypothesisHasRecommendation(row) ? `<section class="hypothesis-recommendation-panel"><h2>Recommendation Ready</h2><p>Confidence: ${escapeHtml(hypothesisConfidenceSummary(row))}</p><p>Related symbols: ${escapeHtml(hypothesisSymbolsText(row))}</p><p>Manual IB capture guidance: review findings before taking any external action.</p></section>` : ""}
    <details class="hypotheses-diagnostics" data-hypothesis-detail-diagnostics>
      <summary>Diagnostics</summary>
      ${renderDefinitionRows([
        { label: "Runtime history", value: payload.hypothesis_proposal_id || "Not reported" },
        { label: "Raw evidence", value: safeList(payload.required_data).join(", ") || "Not reported" },
        { label: "Audit", value: readiness.research_readiness_assessment_id || "Not reported" },
        { label: "Lineage", value: priority.proposal_priority_score_id || "Not reported" },
        { label: "Scheduler details", value: payload.created_at || "Not reported" },
      ])}
    </details>
  </div>`;
}

function renderResearchPlansConsolePanel(payload = {}) {
  const rows = safeList(payload.research_plans?.research_plans || payload.research_plans || []);
  return renderCardSection({
    eyebrow: "RESEARCH PLANS",
    title: "Advanced Research Plans",
    subtitle: "Accepted research plans. Study and backtest actions are explicit human actions only.",
    body: renderSimpleTable({
      columns: [
        { label: "Hypothesis", render: (row) => `<strong>${escapeHtml(row.hypothesis || row.title || row.research_plan_id || "Research plan")}</strong>` },
        { label: "Status", key: "status" },
        { label: "Dataset readiness", key: "dataset_readiness" },
        { label: "Last run", key: "last_run" },
        { label: "Evidence", key: "evidence_package" },
        { label: "Next action", key: "next_action" },
        { label: "Actions", render: () => `<div class="research-action-toolbar compact"><button class="ghost-button research-action-button" type="button">Run Event Study</button><button class="ghost-button research-action-button" type="button">Run Backtest</button><button class="ghost-button research-action-button" type="button">View Evidence</button><button class="ghost-button research-action-button" type="button">Create Candidate Generator</button></div>` },
      ],
      rows,
      emptyMessage: "No accepted research plans are queued.",
    }),
  });
}

function renderEvidenceConsolePanel(payload = {}) {
  const evidencePayload = payload.evidence || payload;
  const rows = safeList(evidencePayload.evidence_packages || evidencePayload.evidence || []);
  const tabs = evidencePayload.tabs || {};
  const tabNames = ["Event Studies", "Backtests", "Regime Analysis", "Cost Analysis", "Longitudinal Results"];
  const byTab = (name) => safeList(tabs[name] || rows.filter((row) => String(row.evidence_type || row.category || "").toLowerCase().includes(name.toLowerCase().split(" ")[0])));
  return renderCardSection({
    eyebrow: "EVIDENCE",
    title: "Evidence",
    subtitle: "Readable evidence summaries. Backtests and model outputs are hypothetical research evidence, not achieved portfolio performance.",
    body: [
      renderResearchSafetyStrip(payload),
      ...tabNames.map((tab) => {
        const tabRows = byTab(tab);
        return `<section class="research-evidence-tab"><h3>${escapeHtml(tab)}</h3>${renderSimpleTable({
          columns: [
            { label: "Hypothesis", render: (row) => escapeHtml(row.hypothesis || row.title || "Evidence package") },
            { label: "Evidence quality", key: "evidence_quality" },
            { label: "Event count", render: (row) => escapeHtml(String(row.event_count ?? row.sample_size ?? "not reported")) },
            { label: "Post-cost results", key: "post_cost_results" },
            { label: "Regime notes", key: "regime_notes" },
            { label: "Conclusion/status", render: (row) => escapeHtml(row.conclusion || row.status || "not reported") },
            { label: "Advanced / Provenance", render: (row) => `<details><summary>Advanced / Provenance</summary><div class="muted-mini">Artifact: ${escapeHtml(row.evidence_package_id || row.artifact_id || "not reported")}</div><div class="muted-mini">Path: ${escapeHtml(row.storage_path || "not shown")}</div></details>` },
          ],
          rows: tabRows,
          emptyMessage: `No ${tab.toLowerCase()} evidence packages were reported.`,
        })}</section>`;
      }),
    ].join(""),
  });
}

function renderResearchHomeSummaryPanel(payload = {}) {
  const queueRows = safeList(payload.hypothesis_queue?.queue || payload.hypothesis_queue?.hypothesis_proposals || []);
  const blockedRows = safeList(payload.blocked_work?.blocked_items || payload.blocked_evidence?.blocked_items || []);
  const paperRows = safeList(payload.paper_trials?.paper_trials || []);
  const sleeveRows = safeList(payload.sleeve_review_center?.sleeves || []);
  return renderCardSection({
    eyebrow: "RESEARCH HOME",
    title: "Research Pipeline",
    subtitle: "What can I do now?",
    body: [
      `<div class="metric-grid">
        ${renderMetricCard({ label: "Recently created hypotheses", value: String(queueRows.length) })}
        ${renderMetricCard({ label: "Blocked hypotheses", value: String(blockedRows.length) })}
        ${renderMetricCard({ label: "Active paper trials", value: String(paperRows.length) })}
        ${renderMetricCard({ label: "Sleeves needing review", value: String(sleeveRows.length) })}
      </div>`,
      `<div class="research-action-toolbar">
        <a class="primary-button research-action-button" href="/research-lab/start" data-aegis-command-id="OPEN_VALID_ROUTE" data-aegis-command-action-type="EXPAND_SECTION" data-aegis-command-target-type="navigation" data-aegis-command-target-id="research_start" data-route="/research-lab/start">+ Start Research</a>
        <a class="ghost-button research-action-button" href="/research-lab/hypotheses" data-aegis-command-id="OPEN_VALID_ROUTE" data-aegis-command-action-type="EXPAND_SECTION" data-aegis-command-target-type="navigation" data-aegis-command-target-id="research_hypotheses" data-route="/research-lab/hypotheses">Review Hypotheses</a>
        <a class="ghost-button research-action-button" href="/research-lab/plans" data-aegis-command-id="OPEN_VALID_ROUTE" data-aegis-command-action-type="EXPAND_SECTION" data-aegis-command-target-type="navigation" data-aegis-command-target-id="research_plans" data-route="/research-lab/plans">Run/Continue Studies</a>
        <a class="ghost-button research-action-button" href="/research-lab/paper-trials" data-aegis-command-id="OPEN_VALID_ROUTE" data-aegis-command-action-type="EXPAND_SECTION" data-aegis-command-target-type="navigation" data-aegis-command-target-id="research_paper_trials" data-route="/research-lab/paper-trials">Continue Paper Trials</a>
        <a class="ghost-button research-action-button" href="/research-lab/sleeve-reviews" data-aegis-command-id="OPEN_VALID_ROUTE" data-aegis-command-action-type="EXPAND_SECTION" data-aegis-command-target-type="navigation" data-aegis-command-target-id="research_sleeve_reviews" data-route="/research-lab/sleeve-reviews">Review Sleeves</a>
        <a class="ghost-button research-action-button" href="/research-lab/blocked-work" data-aegis-command-id="OPEN_VALID_ROUTE" data-aegis-command-action-type="EXPAND_SECTION" data-aegis-command-target-type="navigation" data-aegis-command-target-id="research_blocked_work" data-route="/research-lab/blocked-work">Investigate Blocked Work</a>
      </div>`,
    ].join(""),
  });
}

function renderResearchPageBoundaryCard() {
  return renderCardSection({
    eyebrow: "READ-ONLY GOVERNANCE",
    title: "Research Safety Boundary",
    subtitle: "This console creates review and research artifacts only.",
    body: renderDefinitionRows([
      { label: "Broker execution", value: "not available" },
      { label: "Live trading", value: "not available" },
      { label: "Automatic promotion", value: "not available" },
      { label: "Sleeve mutation", value: "not available" },
    ]),
  });
}

async function renderResearchLabPage(state, mode = "home") {
  const dossierId = mode === "hypotheses" ? String(currentSearchParams().get("dossier") || "").trim() : "";
  const [consolePayload, dossierPayload] = await Promise.all([
    fetchResearchConsole().catch((error) => ({ ok: false, error: error?.message || "Hypotheses unavailable." })),
    dossierId ? fetchResearchIntakeDossier(dossierId).catch((error) => ({ ok: false, error: error?.message || "Hypothesis detail unavailable." })) : Promise.resolve(null),
  ]);
  const titles = {
    home: ["Hypotheses", "See hypotheses, start AI research, monitor progress, review findings, and check recommendation readiness."],
    start: ["Start Research", "Capture a plain-English hypothesis for AI research."],
    hypotheses: ["Hypotheses", "Review hypotheses by simple status, current stage, and recommendation readiness."],
    plans: ["Hypotheses", "Research plans are available inside diagnostics; the primary workflow remains hypotheses."],
    evidence: ["Hypotheses", "Review findings from the hypothesis detail view."],
    paper_trials: ["Hypotheses", "Paper-trial details are available inside diagnostics; the primary workflow remains hypotheses."],
    sleeve_reviews: ["Hypotheses", "Sleeve details are available inside diagnostics; the primary workflow remains hypotheses."],
    blocked_work: ["Hypotheses", "Blocked hypotheses appear in the primary workflow with plain-language next steps."],
    backlog: ["Hypotheses", "Backlog details are available inside diagnostics; the primary workflow remains hypotheses."],
  };
  const [title, meta] = titles[mode] || titles.home;
  const panels = [];
  if (mode === "start") {
    panels.push(renderHypothesesWorkspace(consolePayload, state), renderResearchConsolePanel(consolePayload));
  } else if (mode === "hypotheses" && dossierPayload) {
    panels.push(renderResearchDossierPanel(dossierPayload), renderHypothesesWorkspace(consolePayload, state));
  } else {
    panels.push(renderHypothesesWorkspace(consolePayload, state));
  }
  return {
    title,
    meta,
    html: panels.join(""),
    contextHtml: "",
    hideContextRail: true,
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
    case "aegis_opportunities":
      return renderAegisWorkflowPage("opportunities");
    case "aegis_candidates":
      return renderAegisWorkflowPage("candidates");
    case "aegis_runtime_timeline":
      return renderAegisWorkflowPage("runtime_timeline");
    case "aegis_repair_center":
      return renderAegisRepairCenterPage();
    case "aegis_theses":
      return renderAegisWorkflowPage("theses");
    case "aegis_edge_lab":
      return renderAegisWorkflowPage("edge_lab");
    case "aegis_performance":
      return renderAegisWorkflowPage("performance");
    case "aegis_journal":
      return renderAegisWorkflowPage("journal");
    case "aegis_today":
      return renderAegisWorkflowPage("opportunities");
    case "aegis_review":
      return renderAegisWorkflowPage("performance");
    case "aegis_research":
      return renderAegisWorkflowPage("edge_lab");
    case "aegis_history":
      return renderAegisWorkflowPage("journal");
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
    case "aegis_runtime_truth":
      return renderAegisRuntimeTruthPage(state);
    case "aegis_operator_cockpit":
      return renderAegisOperatorCockpitPage(state);
    case "aegis_adaptive_intelligence":
      return renderAegisAdaptiveIntelligencePage(state);
    case "aegis_intelligence_governance":
      return renderAegisIntelligenceGovernancePage(state);
    case "aegis_lite_queue":
      return renderAegisLiteQueuePage(state);
    case "aegis_events":
      return renderAegisEventMonitoringPage(state);
    case "aegis_ai_feedback":
      return renderAegisAiFeedbackPage(state);
    case "research_lab":
      return renderResearchLabPage(state, "home");
    case "research_start":
      return renderResearchLabPage(state, "start");
    case "research_hypothesis_queue":
      return renderResearchLabPage(state, "hypotheses");
    case "research_plans":
      return renderResearchLabPage(state, "plans");
    case "research_evidence":
      return renderResearchLabPage(state, "evidence");
    case "research_paper_trials":
      return renderResearchLabPage(state, "paper_trials");
    case "research_sleeve_reviews":
      return renderResearchLabPage(state, "sleeve_reviews");
    case "research_blocked_work":
      return renderResearchLabPage(state, "blocked_work");
    case "research_backlog":
      return renderResearchLabPage(state, "backlog");
    case "operator_inbox":
      return renderOperatorInboxPage(state);
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

export async function executeEdgeLabWorkflow(formData, state) {
  const action = String(formData?.get("edge_action") || "").trim();
  const payload = {
    hypothesis_id: String(formData?.get("hypothesis_id") || "").trim(),
    operator: String(formData?.get("operator") || "David").trim() || "David",
    reason: String(formData?.get("reason") || "").trim(),
    note: String(formData?.get("note") || "").trim(),
  };
  let result;
  if (action === "triage") {
    result = await triageAegisHypothesis({
      ...payload,
      decision: String(formData?.get("decision") || "").trim(),
    });
  } else if (action === "build-plan") {
    result = await buildAegisHypothesisPlan(payload);
  } else if (action === "run-test") {
    result = await runAegisHypothesisTest(payload);
  } else if (action === "review") {
    result = await reviewAegisHypothesis({
      ...payload,
      decision: String(formData?.get("decision") || "").trim(),
    });
  } else {
    throw new Error("Unsupported research action.");
  }
  state.edgeLabWorkflow = {
    lastAction: action,
    lastResult: result,
    lastError: null,
  };
  return result;
}


function localDateTimeToUtcIso(rawValue) {
  const raw = String(rawValue || "").trim();
  if (!raw) {
    return "";
  }
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) {
    return "INVALID";
  }
  return parsed.toISOString();
}

function manualCaptureValidationError(message, fieldErrors = {}) {
  const error = new Error(message);
  error.fieldErrors = fieldErrors;
  return error;
}

export function buildManualCaptureRecordPayload(formData) {
  const rawFillTime = String(formData?.get("fill_time") || "").trim();
  const overrideStatus = String(formData?.get("capture_status_override") || "").trim();
  const operatorId = String(formData?.get("operator_id") || "").trim();
  const captureStatus = overrideStatus || String(formData?.get("capture_status") || "captured").trim();
  const fillTimeUtc = localDateTimeToUtcIso(rawFillTime);
  const payload = {
    ticket_id: String(formData?.get("ticket_id") || "").trim(),
    selected_exposure_intent_id: String(formData?.get("selected_exposure_intent_id") || "").trim(),
    trade_lifecycle_case_id: String(formData?.get("trade_lifecycle_case_id") || "").trim(),
    runtime_evaluation_hash: String(formData?.get("runtime_evaluation_hash") || "").trim(),
    ticket_lineage_hash: String(formData?.get("ticket_lineage_hash") || "").trim(),
    submit_boundary_hash: String(formData?.get("submit_boundary_hash") || "").trim(),
    construction_contract_hash: String(formData?.get("construction_contract_hash") || "").trim(),
    candidate_snapshot_id: String(formData?.get("candidate_snapshot_id") || "").trim(),
    candidate_certification_state: String(formData?.get("candidate_certification_state") || "").trim(),
    input_market_data_snapshot_ids: String(formData?.get("input_market_data_snapshot_ids") || "").trim(),
    symbol: String(formData?.get("symbol") || "").trim(),
    sleeve: String(formData?.get("sleeve") || "").trim(),
    capture_status: captureStatus,
    quantity: String(formData?.get("quantity") || "").trim(),
    fill_price: String(formData?.get("fill_price") || "").trim(),
    fill_time_local: rawFillTime,
    fill_time_utc: fillTimeUtc === "INVALID" ? "" : fillTimeUtc,
    stop_price: String(formData?.get("stop_price") || "").trim(),
    invalidation_level: String(formData?.get("invalidation_level") || "").trim(),
    notes: String(formData?.get("notes") || "").trim(),
    operator_id: operatorId,
    external_reference: String(formData?.get("external_reference") || "").trim(),
  };
  return { payload, rawFillTime, fillTimeUtc };
}

export async function executeManualCaptureRecordWorkflow(formData, state) {
  const { payload, rawFillTime, fillTimeUtc } = buildManualCaptureRecordPayload(formData);
  const fieldErrors = {};
  if (!payload.operator_id) {
    fieldErrors.operator_id = "Operator ID is required.";
  }
  if (!payload.ticket_id || !payload.ticket_lineage_hash || !payload.submit_boundary_hash || !payload.runtime_evaluation_hash) {
    throw manualCaptureValidationError("Refresh required. This ticket changed since the page loaded.", fieldErrors);
  }
  if (["captured", "captured_manually", "partial"].includes(payload.capture_status)) {
    const hasStopOrInvalidation = Boolean(payload.stop_price || payload.invalidation_level);
    if (!payload.quantity) {
      fieldErrors.quantity = "Quantity is required.";
    } else if (!Number.isFinite(Number(payload.quantity)) || Number(payload.quantity) <= 0) {
      fieldErrors.quantity = "Quantity must be positive.";
    }
    if (!payload.fill_price) {
      fieldErrors.fill_price = "Fill price is required.";
    } else if (!Number.isFinite(Number(payload.fill_price)) || Number(payload.fill_price) <= 0) {
      fieldErrors.fill_price = "Fill price must be positive.";
    }
    if (!rawFillTime) {
      fieldErrors.fill_time = "Fill time is required.";
    } else if (fillTimeUtc === "INVALID") {
      fieldErrors.fill_time = "Fill time format is invalid.";
    }
    if (!hasStopOrInvalidation) {
      fieldErrors.stop_price = "Stop price or invalidation level is required.";
    }
  }
  if (Object.keys(fieldErrors).length) {
    throw manualCaptureValidationError("Fix highlighted fields before saving.", fieldErrors);
  }
  try {
    localStorage.setItem("aegis.operator_id", payload.operator_id);
  } catch (_error) {
    // localStorage may be unavailable in tests or private contexts.
  }
  state.manualCaptureRecordWorkflow = {
    lastPayload: payload,
    lastError: null,
  };
  const result = await postAegisManualCaptureRecord(payload);
  state.manualCaptureRecordWorkflow = {
    lastPayload: payload,
    lastResult: result,
    lastError: null,
  };
  return result;
}

export async function executeCandidateWorkflow(formData, state) {
  const action = String(formData?.get("candidate_workflow_action") || "").trim();
  const candidateId = String(formData?.get("candidate_id") || "").trim();
  let commandType = "";
  let payload = {};
  let operatorNote = "";
  if (action === "review") {
    const reviewAction = String(formData?.get("review_action") || "").trim();
    commandType = {
      "watchlist": "WATCHLIST_CANDIDATE",
      "dismiss": "DISMISS_CANDIDATE",
      "needs-more-evidence": "REQUEST_MORE_EVIDENCE",
      "add-note": "REVIEW_CANDIDATE",
    }[reviewAction] || "REVIEW_CANDIDATE";
    operatorNote = String(formData?.get("operator_note") || "").trim();
    payload = { operator_note: operatorNote };
  } else if (action === "manual_capture") {
    const rawTimestamp = String(formData?.get("capture_timestamp") || "").trim();
    commandType = "RECORD_MANUAL_EXTERNAL_CAPTURE";
    operatorNote = String(formData?.get("operator_notes") || "").trim();
    payload = {
      manually_captured: String(formData?.get("manually_captured") || "false").trim(),
      quantity: String(formData?.get("quantity") || "").trim(),
      capture_timestamp: rawTimestamp ? new Date(rawTimestamp).toISOString() : "",
      external_execution_venue: String(formData?.get("external_execution_venue") || "").trim(),
      operator_notes: operatorNote,
      confidence_override: String(formData?.get("confidence_override") || "").trim(),
      paper_trade_only: String(formData?.get("paper_trade_only") || "true").trim(),
      review_decision: String(formData?.get("review_decision") || "MANUAL_CAPTURE_RECORDED").trim(),
      unsupported_evidence_acknowledgement: String(formData?.get("unsupported_evidence_acknowledgement") || "").trim() === "on",
    };
  } else {
    throw new Error("Unsupported candidate workflow action.");
  }
  const result = await executeAegisOperatorCommand({
    command_type: commandType,
    target_id: candidateId,
    idempotency_key: `${commandType}:${candidateId}:${JSON.stringify(payload)}:${operatorNote}`,
    source_projection_fingerprint: String(formData?.get("source_projection_fingerprint") || "").trim(),
    actor: String(formData?.get("operator") || "David").trim() || "David",
    operator_note: operatorNote,
    payload,
  });
  state.candidateWorkflow = {
    lastAction: action,
    lastResult: result,
    lastError: null,
  };
  return result;
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
export async function executeResearchDataAcquisitionWorkflow(formData, state) {
  const action = String(formData?.get("data_action") || "").trim();
  if (action === "fetch-market-data-now") {
    const payload = {
      playbook_id: String(formData?.get("playbook_id") || "refresh_required_symbol_data").trim(),
      day_utc: String(formData?.get("day_utc") || "").trim(),
      symbols: String(formData?.get("symbols") || "").trim(),
      force: true,
    };
    const result = await runAegisDataRemediation(payload);
    state.researchDataAcquisitionWorkflow = {
      lastAction: action,
      lastPayload: payload,
      lastResult: result,
      lastError: null,
    };
    return result;
  }
  if (action === "refresh-pipeline") {
    state.researchDataAcquisitionWorkflow = {
      lastAction: action,
      lastResult: { ok: true, message: "Research Pipeline refreshed." },
      lastError: null,
    };
    return state.researchDataAcquisitionWorkflow.lastResult;
  }
  throw new Error("Unsupported data acquisition action.");
}

export async function executeResearchConsoleWorkflow(formData, state) {
  const action = String(formData?.get("research_action") || "start").trim() || "start";
  let result;
  if (action === "start") {
    result = await startResearchIdea({
      hypothesis_id: String(formData?.get("hypothesis_id") || "").trim(),
      idea: String(formData?.get("idea") || "").trim(),
      event_family: String(formData?.get("event_family") || "").trim(),
      symbols: String(formData?.get("symbols") || "").trim(),
      priority: String(formData?.get("priority") || "watchlist").trim(),
      notes: String(formData?.get("notes") || "").trim(),
    });
  } else if (action === "assess-readiness") {
    result = await assessHypothesisReadiness(String(formData?.get("hypothesis_proposal_id") || "").trim(), {});
  } else if (action === "review") {
    result = await reviewHypothesisProposal(String(formData?.get("hypothesis_proposal_id") || "").trim(), {
      decision: String(formData?.get("decision") || "").trim(),
      reason: String(formData?.get("reason") || "Operator review from Research Pipeline").trim(),
      reviewed_by: String(formData?.get("reviewed_by") || "operator-ui").trim(),
    });
  } else if (action === "convert") {
    result = await convertHypothesisProposalToResearchPlan(String(formData?.get("hypothesis_proposal_id") || "").trim(), {
      approve: String(formData?.get("approve") || "false").toLowerCase() === "true",
      dataset_snapshot_id: String(formData?.get("dataset_snapshot_id") || "").trim(),
      universe_snapshot_id: String(formData?.get("universe_snapshot_id") || "").trim(),
      start: String(formData?.get("start") || "").trim(),
      end: String(formData?.get("end") || "").trim(),
    });
  } else {
    throw new Error("Unsupported research action.");
  }
  state.researchConsoleWorkflow = { lastAction: action, lastResult: result, lastError: null };
  return result;
}

