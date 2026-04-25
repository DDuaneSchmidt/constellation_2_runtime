import { renderEvidenceRefs, renderKeyValue } from "/operator_shell/shared_components/dom.js";

export function renderReconciliation(view, semantics) {
  return `
    <div class="panel-grid">
      <section class="panel">
        <h3>Execution / Fill Status</h3>
        ${renderKeyValue([
          { label: "Execution reconciliation", value: view.execution_reconciliation_status, semantic: view.execution_reconciliation_status ? "canonical" : "unknown" },
          { label: "Fill ledger", value: view.fill_ledger_status, semantic: view.fill_ledger_status === "OK" ? "healthy" : "unknown" },
        ], semantics)}
      </section>
      <section class="panel">
        <h3>Agreement Summary</h3>
        ${renderKeyValue([
          { label: "Canonical vs sleeve", value: view.canonical_vs_sleeve_agreement_summary?.status, semantic: view.canonical_vs_sleeve_agreement_summary?.semantic, detail: view.canonical_vs_sleeve_agreement_summary?.reason },
          { label: "Orphan / reconstructed", value: view.orphan_reconstructed_lineage_summary?.status, semantic: view.orphan_reconstructed_lineage_summary?.semantic, detail: view.orphan_reconstructed_lineage_summary?.reason },
        ], semantics)}
      </section>
      <section class="panel-wide">
        <h3>Mismatches</h3>
        <div class="warning-list">
          ${(view.mismatches || []).map((item) => `
            <div class="warning-row">
              <div>
                <div>${item.comparison_id}</div>
                <div class="muted">${item.reason || "n/a"}</div>
              </div>
              <div>${item.status}</div>
            </div>
          `).join("") || `<div class="empty-state">No mismatches surfaced for this day.</div>`}
        </div>
      </section>
      <section class="panel-wide">
        <h3>Evidence</h3>
        ${renderEvidenceRefs(view.source_refs || [])}
      </section>
    </div>
  `;
}
