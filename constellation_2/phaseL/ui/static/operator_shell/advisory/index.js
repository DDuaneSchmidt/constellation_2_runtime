import { renderEvidenceRefs, renderMarkers } from "/operator_shell/shared_components/dom.js";

export function renderAdvisory(view, semantics) {
  const rows = view.decisions || [];
  return `
    <div class="panel-grid">
      <section class="panel-wide">
        <h3>Advisory Decision State</h3>
        <div class="muted">Advisory day: ${view.advisory_day || "n/a"} · Operations day: ${view.current_day || "n/a"}</div>
        ${(view.advisory_warnings || []).length ? `<div class="warning-list">${view.advisory_warnings.map((warning) => `<div class="warning-row">${warning}</div>`).join("")}</div>` : ""}
      </section>
      ${rows.map((row) => `
        <section class="panel">
          <h3>${row.advisory_item_id || row.advisory_surface_label || "Advisory decision"}</h3>
          <div class="muted">${row.decision_state} · ${row.actionability_state}</div>
          <div style="margin-top:10px;">Promotion eligibility: <strong>${row.promotion_eligibility_state}</strong></div>
          <div class="muted">${row.summary_message || row.invalidation_rule_id || "No explanation available."}</div>
          <div style="margin-top:10px;">${renderMarkers(row.provenance_markers || [], semantics)}</div>
          ${renderEvidenceRefs(row.evidence_refs || [])}
        </section>
      `).join("")}
    </div>
  `;
}
