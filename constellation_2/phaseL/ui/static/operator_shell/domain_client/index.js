import { fetchJson, postJson } from "/operator_shell/api_client/index.js";

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

export function fetchStatusRail() {
  return query("/api/shell/status-rail");
}

export function fetchSystemSummary() {
  return query("/api/system/summary");
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

export function fetchIntegrity() {
  return query("/api/integrity");
}

export function fetchAdvisory() {
  return query("/api/advisory");
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
