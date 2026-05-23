import { fetchJson, patchJson, postJson } from "/operator_shell/api_client/index.js";

function query(path, params = {}) {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params || {})) {
    if (value === undefined || value === null || value === "") {
      continue;
    }
    search.set(key, String(value));
  }
  const suffix = search.toString() ? `?${search.toString()}` : "";
  return fetchJson(`${path}${suffix}`);
}

export function fetchStatusSemantics() {
  return query("/api/shared/status-semantics");
}

export function fetchStatusRail(params = {}) {
  return query("/api/shell/status-rail", params);
}

export function fetchSystemSummary() {
  return query("/api/system/summary");
}

export function fetchStatusV2(params = {}) {
  return query("/api/status_v2", params);
}

export function fetchRefinement() {
  return query("/api/refinement");
}

export function fetchPolicyEvolution() {
  return query("/api/policy-evolution");
}

export function fetchOperatorWorkflow() {
  return query("/api/operator-workflow");
}

export function fetchOperatorHome() {
  return query("/api/operator/home");
}

export function fetchOperatorQuery(queryText) {
  return query("/api/operator/query", { q: queryText });
}

export function fetchAlerts() {
  return query("/api/alerts");
}

export function fetchOperations() {
  return query("/api/operations");
}

export function fetchAegisOperatorState() {
  const path = "/api/aegis/operator-state";
  if (typeof window === "undefined") {
    return query(path);
  }
  return fetchJson(new URL(path, window.location.origin).toString());
}

export function fetchAegisLiteExecutionQueue(params = {}) {
  return query("/api/aegis/lite-execution-queue", params);
}

export function fetchAegisEventMonitoring(params = {}) {
  return query("/api/aegis/event-monitoring", params);
}

export function fetchAegisRuntimeTruth(params = {}) {
  return query("/api/aegis/runtime-truth", params);
}

export function fetchAegisRepairCenter(params = {}) {
  return query("/api/aegis/repair-center", params);
}

export function fetchAegisOperatorCockpit(params = {}) {
  return query("/api/aegis/operator-cockpit", params);
}

export function fetchAegisOperatorStateSnapshotLatest(params = {}) {
  return query("/api/aegis/operator/state-snapshot/latest", params);
}

export function fetchAegisManualCaptureLatest(params = {}) {
  return query("/api/aegis/operator/manual-capture/latest", params);
}

export function fetchAegisSuppressedWatchlistLatest(params = {}) {
  return query("/api/aegis/operator/suppressed-watchlist/latest", params);
}

export function fetchAegisManualCaptureRecords(params = {}) {
  return query("/api/aegis/operator/manual-capture-records", params);
}

export function fetchAegisManualCaptureRecordLatest(params = {}) {
  return query("/api/aegis/operator/manual-capture-records/latest", params);
}

export function postAegisManualCaptureRecord(payload = {}) {
  return postJson("/api/aegis/operator/manual-capture-records", payload);
}

export function fetchAegisJournalTimeline(params = {}) {
  return query("/api/aegis/journal-timeline", params);
}

export function fetchAegisThesisGraph(params = {}) {
  return query("/api/aegis/thesis-graph", params);
}

export function fetchResearchLabEdgeProjection(sleeveId = "slv_etf_drop_reversion_v1", params = {}) {
  const encoded = encodeURIComponent(String(sleeveId || ""));
  return query(`/api/research-lab/sleeves/${encoded}/edge-lab-projection`, params);
}

export function fetchResearchLabChallengerComparisonReports(params = {}) {
  return query("/api/research-lab/challenger-comparison-reports", params);
}

export function fetchResearchLabHumanReviewDossiers(params = {}) {
  return query("/api/research-lab/human-review-dossiers", params);
}

export function fetchResearchLabExpectancyDrift(params = {}) {
  return query("/api/research-lab/stability/expectancy-drift/latest", params);
}

export function fetchResearchLabRegimeFragility(params = {}) {
  return query("/api/research-lab/stability/regime-fragility/latest", params);
}

export function fetchResearchLabSleeveStability(params = {}) {
  return query("/api/research-lab/stability/sleeve-stability/latest", params);
}

export function fetchResearchLabPaperTrialInventory(params = {}) {
  return query("/api/research-lab/paper-trial-inventory", params);
}

export function fetchResearchLabStatus(params = {}) {
  return query("/api/research-lab/status/latest", params);
}

export function fetchResearchLabPaperTrialProposal(params = {}) {
  return query("/api/research-lab/paper-trial-proposals/latest", params);
}

export function fetchResearchLabObservationCandidates(params = {}) {
  return query("/api/research-lab/observation-candidate-batches/latest", params);
}

export function fetchResearchLabObservationClusters(params = {}) {
  return query("/api/research-lab/observation-cluster-batches/latest", params);
}

export function fetchResearchLabHypothesisProposals(params = {}) {
  return query("/api/research-lab/hypothesis-proposal-batches/latest", params);
}

export function fetchResearchIntakeQueue(params = {}) {
  return query("/api/research-lab/research-intake/queue", params);
}

export function fetchResearchLabHypothesisProposalReviews(params = {}) {
  return query("/api/research-lab/hypothesis-proposal-review-batches/latest", params);
}

export function fetchResearchLabHypothesisIntake(params = {}) {
  return query("/api/research-lab/hypothesis-intake-batches/latest", params);
}

export function triageAegisHypothesis(payload = {}) {
  return postJson("/api/aegis/edge-lab/hypothesis/triage", payload);
}

export function buildAegisHypothesisPlan(payload = {}) {
  return postJson("/api/aegis/edge-lab/hypothesis/build-plan", payload);
}

export function runAegisHypothesisTest(payload = {}) {
  return postJson("/api/aegis/edge-lab/hypothesis/run-test", payload);
}

export function reviewAegisHypothesis(payload = {}) {
  return postJson("/api/aegis/edge-lab/hypothesis/review", payload);
}

export function fetchAegisOperatorToday(params = {}) {
  return query("/api/aegis/operator/today", params);
}

export function fetchAegisOperatorTasks(params = {}) {
  return query("/api/aegis/operator/tasks", params);
}

export function fetchAegisOperatorDiagnostics(params = {}) {
  return query("/api/aegis/operator/diagnostics", params);
}

export function fetchAegisOperatorWhatChanged(params = {}) {
  return query("/api/aegis/operator/what-changed", params);
}

export function fetchAegisOperatorHealth(params = {}) {
  return query("/api/aegis/operator/health", params);
}

export function fetchAegisOpportunitiesProjection(params = {}) {
  return query("/api/aegis/opportunities", params);
}

export function fetchAegisCandidateDecision(candidateId, params = {}) {
  const encoded = encodeURIComponent(String(candidateId || ""));
  return query(`/api/aegis/candidates/${encoded}/decision`, params);
}

export function fetchAegisReviewLedger(params = {}) {
  return query("/api/aegis/review-ledger", params);
}

export function fetchAegisEodOutcomeLatest(params = {}) {
  return query("/api/aegis/eod-outcomes/latest", params);
}

export function fetchAegisEodOutcomeBySession(tradingSession, params = {}) {
  const encoded = encodeURIComponent(String(tradingSession || ""));
  return query(`/api/aegis/eod-outcomes/by-session/${encoded}`, params);
}

export function fetchAegisEodSleeveHistory(sleeveId, params = {}) {
  const encoded = encodeURIComponent(String(sleeveId || ""));
  return query(`/api/aegis/eod-outcomes/sleeves/${encoded}/history`, params);
}

export function executeAegisOperatorCommand(payload = {}) {
  return postJson("/api/aegis/operator/commands", payload);
}

export function fetchAegisCommandRegistry(params = {}) {
  return query("/api/aegis/commands/registry", params);
}

export function executeAegisCommand(payload = {}) {
  return postJson("/api/aegis/commands/execute", payload);
}

export function fetchAegisAdaptiveIntelligence(params = {}) {
  return query("/api/aegis/adaptive-intelligence", params);
}

export function fetchAegisIntelligenceGovernance(params = {}) {
  return query("/api/aegis/intelligence-governance", params);
}

export function fetchRuntimeStatus() {
  return query("/api/runtime-status");
}

export function fetchUiServiceAuthority() {
  return query("/api/ui-service-authority");
}

export function fetchLatestPacket() {
  return query("/api/latest-packet");
}

export function fetchReadinessKernel(params = {}) {
  return query("/api/readiness-kernel", params);
}

export function fetchCommandOverview(params = {}) {
  return query("/api/command/overview", params);
}

export function fetchIntegrity() {
  return query("/api/integrity");
}

export function fetchAdvisory(params = {}) {
  return query("/api/advisory", params);
}

export function fetchOpportunities() {
  return query("/api/opportunities");
}

export function fetchValue() {
  return query("/api/value");
}

export function fetchOutcomes() {
  return fetchValue();
}

export function fetchFinancialState() {
  return query("/api/financial-state");
}

export function fetchCapitalOverview() {
  return query("/api/capital/overview");
}

export function fetchCapitalAccounts() {
  return query("/api/capital/accounts");
}

export function fetchCapitalAllocation() {
  return query("/api/capital/allocation");
}

export function fetchCapitalHistory() {
  return query("/api/capital/history");
}

export function fetchCapitalFlows() {
  return query("/api/capital/flows");
}

export function fetchCapitalCashflow(params = {}) {
  return query("/api/capital/cashflow", params);
}

export function fetchCapitalValidation() {
  return query("/api/capital/validation");
}

export function fetchPositions() {
  return query("/api/positions");
}

export function fetchOrders() {
  return query("/api/orders");
}

export function fetchReconciliation() {
  return query("/api/reconciliation");
}

export function fetchSleeves() {
  return query("/api/sleeves");
}

export function fetchTax() {
  return query("/api/tax");
}

export function fetchSystemActions() {
  return query("/api/system/actions");
}

export function fetchActionAudit() {
  return query("/api/system/action-audit");
}

export function fetchActivityToday() {
  return query("/api/activity/today");
}

export function fetchConfigurationCatalog() {
  return query("/api/configuration/catalog");
}

export function fetchConfigurationCurrent() {
  return query("/api/configuration/current");
}

export function fetchConfigurationDraft(draftId) {
  return query(`/api/configuration/drafts/${encodeURIComponent(String(draftId || ""))}`);
}

export function createConfigurationDraft(payload = {}) {
  return postJson("/api/configuration/drafts", payload);
}

export function validateConfigurationDraft(draftId) {
  return postJson(`/api/configuration/drafts/${encodeURIComponent(String(draftId || ""))}/validate`, {});
}

export function reviewConfigurationDraft(draftId) {
  return postJson(`/api/configuration/drafts/${encodeURIComponent(String(draftId || ""))}/review`, {});
}

export function activateConfigurationDraft(draftId) {
  return postJson(`/api/configuration/drafts/${encodeURIComponent(String(draftId || ""))}/activate`, {});
}

export function rejectConfigurationDraft(draftId, payload = {}) {
  return postJson(`/api/configuration/drafts/${encodeURIComponent(String(draftId || ""))}/reject`, payload);
}

export function fetchReliabilityObservations(params = {}) {
  return query("/api/reliability/observations", params);
}

export function createReliabilityObservation(payload = {}) {
  return postJson("/api/reliability/observations", payload);
}

export function fetchReliabilityIssues(params = {}) {
  return query("/api/reliability/issues", params);
}

export function createReliabilityIssue(payload = {}) {
  return postJson("/api/reliability/issues", payload);
}

export function fetchReliabilityIssue(issueId) {
  return query(`/api/reliability/issues/${encodeURIComponent(String(issueId || ""))}`);
}

export function updateReliabilityIssue(issueId, payload = {}) {
  return patchJson(`/api/reliability/issues/${encodeURIComponent(String(issueId || ""))}`, payload);
}

export function linkReliabilityObservation(issueId, payload = {}) {
  return postJson(
    `/api/reliability/issues/${encodeURIComponent(String(issueId || ""))}/link-observation`,
    payload,
  );
}

export function draftReliabilityIssue(payload = {}) {
  return postJson("/api/reliability/ai/draft-issue", payload);
}

export function assessReliabilityReadiness(payload = {}) {
  return postJson("/api/reliability/readiness/assess", payload);
}

export function fetchLatestReliabilityReadiness() {
  return query("/api/reliability/readiness/latest");
}

export function fetchReliabilityReadiness(assessmentId) {
  return query(`/api/reliability/readiness/${encodeURIComponent(String(assessmentId || ""))}`);
}

export function fetchReliabilityWorkOrders(params = {}) {
  return query("/api/reliability/work-orders", params);
}

export function createReliabilityWorkOrder(payload = {}) {
  return postJson("/api/reliability/work-orders", payload);
}

export function fetchReliabilityWorkOrder(workOrderId) {
  return query(`/api/reliability/work-orders/${encodeURIComponent(String(workOrderId || ""))}`);
}

export function updateReliabilityWorkOrder(workOrderId, payload = {}) {
  return patchJson(`/api/reliability/work-orders/${encodeURIComponent(String(workOrderId || ""))}`, payload);
}

export function fetchReliabilityIssueWorkOrders(issueId) {
  return query(`/api/reliability/issues/${encodeURIComponent(String(issueId || ""))}/work-orders`);
}

export function createReliabilityIssueWorkOrder(issueId, payload = {}) {
  return postJson(`/api/reliability/issues/${encodeURIComponent(String(issueId || ""))}/work-orders`, payload);
}

export function createWorkOrderFromIssue(issueId, payload = {}) {
  return postJson(`/api/reliability/issues/${encodeURIComponent(String(issueId || ""))}/create-work-order`, payload);
}

export function fetchReliabilityFixAttempts(params = {}) {
  return query("/api/reliability/fix-attempts", params);
}

export function createReliabilityFixAttempt(payload = {}) {
  return postJson("/api/reliability/fix-attempts", payload);
}

export function fetchReliabilityFixAttempt(fixAttemptId) {
  return query(`/api/reliability/fix-attempts/${encodeURIComponent(String(fixAttemptId || ""))}`);
}

export function updateReliabilityFixAttempt(fixAttemptId, payload = {}) {
  return patchJson(`/api/reliability/fix-attempts/${encodeURIComponent(String(fixAttemptId || ""))}`, payload);
}

export function fetchReliabilityWorkOrderFixAttempts(workOrderId) {
  return query(`/api/reliability/work-orders/${encodeURIComponent(String(workOrderId || ""))}/fix-attempts`);
}

export function createReliabilityWorkOrderFixAttempt(workOrderId, payload = {}) {
  return postJson(`/api/reliability/work-orders/${encodeURIComponent(String(workOrderId || ""))}/fix-attempts`, payload);
}

export function recordReliabilityFixAttempt(workOrderId, payload = {}) {
  return postJson(`/api/reliability/work-orders/${encodeURIComponent(String(workOrderId || ""))}/record-fix-attempt`, payload);
}

export function fetchReliabilityVerifications(params = {}) {
  return query("/api/reliability/verifications", params);
}

export function createReliabilityVerification(payload = {}) {
  return postJson("/api/reliability/verifications", payload);
}

export function fetchReliabilityVerification(verificationId) {
  return query(`/api/reliability/verifications/${encodeURIComponent(String(verificationId || ""))}`);
}

export function updateReliabilityVerification(verificationId, payload = {}) {
  return patchJson(`/api/reliability/verifications/${encodeURIComponent(String(verificationId || ""))}`, payload);
}

export function fetchReliabilityIssueVerifications(issueId) {
  return query(`/api/reliability/issues/${encodeURIComponent(String(issueId || ""))}/verifications`);
}

export function createReliabilityIssueVerification(issueId, payload = {}) {
  return postJson(`/api/reliability/issues/${encodeURIComponent(String(issueId || ""))}/verifications`, payload);
}

export function verifyReliabilityIssue(issueId, payload = {}) {
  return postJson(`/api/reliability/issues/${encodeURIComponent(String(issueId || ""))}/verify`, payload);
}

export function fetchReliabilityNextActions() {
  return query("/api/reliability/next-actions");
}

export function fetchResearchConsole(params = {}) {
  return query("/api/research-lab/console", params);
}

export function fetchResearchOperatorHome(params = {}) {
  return query("/api/research-lab/operator-home", params);
}

export function fetchStartResearchOptions(params = {}) {
  return query("/api/research-lab/start-research/options", params);
}

export function startResearchIdea(payload = {}) {
  return postJson("/api/research-lab/start-research", payload);
}

export function fetchHypothesisQueue(params = {}) {
  return query("/api/research-lab/hypothesis-queue", params);
}

export function fetchResearchIntakeDossier(hypothesisProposalId, params = {}) {
  const encoded = encodeURIComponent(String(hypothesisProposalId || ""));
  return query(`/api/research-lab/hypothesis-proposals/${encoded}/dossier`, params);
}

export function reviewHypothesisProposal(hypothesisProposalId, payload = {}) {
  const encoded = encodeURIComponent(String(hypothesisProposalId || ""));
  return postJson(`/api/research-lab/hypothesis-proposals/${encoded}/review`, payload);
}

export function assessHypothesisReadiness(hypothesisProposalId, payload = {}) {
  const encoded = encodeURIComponent(String(hypothesisProposalId || ""));
  return postJson(`/api/research-lab/hypothesis-proposals/${encoded}/assess-readiness`, payload);
}

export function convertHypothesisProposalToResearchPlan(hypothesisProposalId, payload = {}) {
  const encoded = encodeURIComponent(String(hypothesisProposalId || ""));
  return postJson(`/api/research-lab/hypothesis-proposals/${encoded}/convert-to-research-plan`, payload);
}

export function fetchResearchPlans(params = {}) {
  return query("/api/research-lab/research-plans", params);
}

export function fetchResearchEvidence(params = {}) {
  return query("/api/research-lab/evidence", params);
}

export function fetchResearchPaperTrials(params = {}) {
  return query("/api/research-lab/paper-trials", params);
}

export function fetchSleeveReviewCenter(params = {}) {
  return query("/api/research-lab/sleeve-review-center", params);
}

export function fetchBlockedEvidence(params = {}) {
  return query("/api/research-lab/blocked-evidence", params);
}

export function fetchBlockedWork(params = {}) {
  return query("/api/research-lab/blocked-work", params);
}

export function runAegisDataRemediation(payload = {}) {
  return postJson("/api/aegis/data-remediation/run", payload);
}

export function fetchResearchBacklog(params = {}) {
  return query("/api/research-lab/research-backlog", params);
}
