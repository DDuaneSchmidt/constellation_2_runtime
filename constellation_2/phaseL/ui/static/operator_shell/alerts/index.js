import { renderEvidenceRefs, renderMarkers } from "/operator_shell/shared_components/dom.js";

function renderAlertList(items, semantics) {
  return `<div class="warning-list">${items.map((item) => `
    <div class="warning-row">
      <div>
        <div>${item.title}</div>
        <div class="muted">${item.summary || ""}</div>
        <div style="margin-top:6px;">${renderMarkers(item.provenance_markers || [], semantics)}</div>
      </div>
      <div>
        <div>${item.severity}</div>
        <div class="muted">${item.status}</div>
      </div>
    </div>
  `).join("") || `<div class="empty-state">No current items.</div>`}</div>`;
}

export function renderAlerts(alertsView, integrityView, semantics) {
  return `
    <div class="panel-grid">
      <section class="panel">
        <h3>Alerts</h3>
        ${renderAlertList(alertsView.alerts || [], semantics)}
      </section>
      <section class="panel">
        <h3>Integrity</h3>
        ${renderAlertList(integrityView.integrity_alerts || [], semantics)}
      </section>
      <section class="panel-wide">
        <h3>Alert Evidence</h3>
        ${renderEvidenceRefs([...(alertsView.source_refs || []), ...(integrityView.source_refs || [])])}
      </section>
    </div>
  `;
}
