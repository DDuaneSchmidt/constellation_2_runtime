import { renderEvidenceRefs, renderMarkers } from "/operator_shell/shared_components/dom.js";

export function renderPositions(view, semantics) {
  const rows = view.positions || [];
  return `
    <div class="table-panel">
      <h3>Positions</h3>
      <table>
        <thead>
          <tr>
            <th>Position</th>
            <th>Symbol</th>
            <th>Engine</th>
            <th>Qty</th>
            <th>Status</th>
            <th>Avg Price</th>
            <th>Open-order linkage</th>
            <th>Markers</th>
          </tr>
        </thead>
        <tbody>
          ${rows.map((row) => `
            <tr>
              <td class="mono">${row.position_id}</td>
              <td>${row.symbol || "n/a"}</td>
              <td>${row.engine_id || "n/a"}</td>
              <td>${row.qty ?? "n/a"}</td>
              <td>${row.status || "UNKNOWN"}</td>
              <td>${row.avg_price ?? "n/a"}</td>
              <td>${(row.open_order_linkage || []).map((item) => `${item.submission_id}:${item.lifecycle_status}`).join(", ") || "n/a"}</td>
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
