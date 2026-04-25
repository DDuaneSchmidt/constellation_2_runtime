import { renderEvidenceRefs, renderMarkers } from "/operator_shell/shared_components/dom.js";
import { statePill } from "/operator_shell/shared_status/index.js";

export function renderOrders(view, semantics) {
  const rows = view.orders || [];
  return `
    <div class="table-panel">
      <h3>Order Lifecycle Visibility</h3>
      <table>
        <thead>
          <tr>
            <th>Submission</th>
            <th>Symbol</th>
            <th>Side</th>
            <th>Qty</th>
            <th>Type / TIF</th>
            <th>Broker</th>
            <th>Lifecycle</th>
            <th>Markers</th>
          </tr>
        </thead>
        <tbody>
          ${rows.map((row) => `
            <tr>
              <td class="mono">${row.submission_id}</td>
              <td>${row.symbol || "n/a"}</td>
              <td>${row.side || "n/a"}</td>
              <td>${row.qty ?? "n/a"}</td>
              <td>${row.order_type || "n/a"} / ${row.tif || "n/a"}</td>
              <td>${statePill(row.broker_status || "UNKNOWN", row.broker_status ? "canonical" : "unknown", semantics)}</td>
              <td>
                ${statePill(row.lifecycle_status || "UNKNOWN", row.lifecycle_semantic || "unknown", semantics)}
                <div class="mono muted">${row.last_update || "n/a"}</div>
              </td>
              <td>${renderMarkers(row.provenance_markers || [], semantics)}</td>
            </tr>
            <tr>
              <td colspan="8">${renderEvidenceRefs(row.evidence_refs || [])}</td>
            </tr>
          `).join("")}
        </tbody>
      </table>
    </div>
  `;
}
