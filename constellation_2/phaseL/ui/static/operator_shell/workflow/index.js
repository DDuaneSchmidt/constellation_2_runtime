import { escapeHtml, renderEmpty, renderEvidenceRefs, renderState } from "/operator_shell/shared_components/dom.js";

const WORKFLOW_SEMANTICS = {
  canonical: "canonical",
  derived: "derived",
  reconstructed: "reconstructed",
  stale: "stale",
  degraded: "degraded",
  unknown: "unknown",
  fail_closed: "fail_closed",
  blocked: "blocked",
  fresh: "healthy",
  ready: "healthy",
  attention_needed: "warning",
  investigate: "degraded",
  action_available: "canonical",
  allowed: "canonical",
  unavailable: "unknown",
  low: "healthy",
  medium: "warning",
  high: "degraded",
  critical: "blocked",
};

function semanticName(value) {
  return WORKFLOW_SEMANTICS[String(value || "").trim()] || "unknown";
}

function renderNextSteps(steps, semantics) {
  if (!steps.length) {
    return renderEmpty("No workflow next steps.");
  }
  return `<div class="warning-list">${steps.map((step) => `
    <div class="warning-row">
      <div>
        <div>${escapeHtml(step.title || "Untitled step")}</div>
        <div class="muted">${escapeHtml(step.rationale || "")}</div>
        <div class="muted" style="margin-top:6px;">
          <a class="nav-link" href="#/${escapeHtml(step.target_surface || "operations")}">${escapeHtml(step.target_surface || "operations")}</a>
          ${step.target_entity_id ? ` · <span class="mono">${escapeHtml(step.target_entity_id)}</span>` : ""}
          ${step.action_id ? ` · <span class="mono">${escapeHtml(step.action_id)}</span>` : ""}
        </div>
        <div style="margin-top:8px;">${renderEvidenceRefs(step.evidence_refs || [])}</div>
      </div>
      <div>${renderState(step.next_step_kind || "UNKNOWN", semanticName(step.next_step_kind === "open_admin_action" ? "attention_needed" : "investigate"), semantics)}</div>
    </div>
  `).join("")}</div>`;
}

function renderWorkflowCards(cards, semantics) {
  const order = ["operations", "alerts", "reconciliation", "positions", "orders"];
  return `<div class="warning-list">${order.map((key) => {
    const card = cards?.[key];
    if (!card) return "";
    return `
      <div class="warning-row">
        <div>
          <div>${escapeHtml(key)}</div>
          <div class="muted">${escapeHtml((card.reasons || []).join(" | ") || "No reasons recorded.")}</div>
          <div class="muted" style="margin-top:6px;">${escapeHtml(card.derived_from_surface?.contract_id || "UNKNOWN")}</div>
        </div>
        <div>
          <div>${renderState(card.workflow_state || "UNKNOWN", semanticName(card.workflow_state), semantics)}</div>
          <div style="margin-top:6px;">${renderState(card.operator_priority || "UNKNOWN", semanticName(card.operator_priority), semantics)}</div>
        </div>
      </div>
    `;
  }).join("") || `<div class="empty-state">No workflow cards.</div>`}</div>`;
}

export function renderWorkflow(view, semantics) {
  return `
    <div class="panel-grid">
      <section class="panel">
        <h3>Workflow State</h3>
        <div class="kv-list">
          <div class="kv-row">
            <div><div>Overall state</div></div>
            <div>${renderState(view.workflow_state || "UNKNOWN", semanticName(view.workflow_state), semantics)}</div>
          </div>
          <div class="kv-row">
            <div><div>Operator priority</div></div>
            <div>${renderState(view.operator_priority || "UNKNOWN", semanticName(view.operator_priority), semantics)}</div>
          </div>
          <div class="kv-row">
            <div><div>Action readiness</div></div>
            <div>${renderState(view.action_readiness || "UNKNOWN", semanticName(view.action_readiness), semantics)}</div>
          </div>
          <div class="kv-row">
            <div><div>Truth state</div></div>
            <div>${renderState(view.truth_state || "UNKNOWN", semanticName(view.truth_state), semantics)}</div>
          </div>
          <div class="kv-row">
            <div><div>Data condition</div></div>
            <div>${renderState(view.data_condition || "UNKNOWN", semanticName(view.data_condition), semantics)}</div>
          </div>
        </div>
      </section>
      <section class="panel-wide">
        <h3>Next Steps</h3>
        ${renderNextSteps(view.next_steps || [], semantics)}
      </section>
      <section class="panel-wide">
        <h3>Workflow Cards</h3>
        ${renderWorkflowCards(view.workflow_cards || {}, semantics)}
      </section>
      <section class="panel-wide">
        <h3>Evidence</h3>
        ${renderEvidenceRefs(view.evidence_refs || [])}
      </section>
    </div>
  `;
}
