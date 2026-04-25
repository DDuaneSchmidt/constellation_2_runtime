import { markerPill, statePill } from "/operator_shell/shared_status/index.js";

export function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll("\"", "&quot;");
}

export function renderEmpty(message) {
  return `<div class="empty-state">${escapeHtml(message)}</div>`;
}

export function renderError(message) {
  return `<div class="error-state">${escapeHtml(message)}</div>`;
}

export function renderEvidenceRefs(refs = []) {
  if (!refs.length) return renderEmpty("No evidence refs.");
  return `<div class="evidence-list">${refs.map((ref) => `
    <div class="evidence-row">
      <div>
        <div>${escapeHtml(ref.label || ref.artifact_type || "artifact")}</div>
        <div class="mono muted">${escapeHtml(ref.path || "")}</div>
      </div>
      <button class="evidence-button" type="button" data-artifact-path="${escapeHtml(ref.path || "")}" data-artifact-title="${escapeHtml(ref.label || ref.artifact_type || "artifact")}">Open</button>
    </div>
  `).join("")}</div>`;
}

export function renderMarkers(markers = [], semantics) {
  return markers.map((marker) => markerPill(marker, semantics)).join("");
}

export function renderState(label, semantic, semantics) {
  return statePill(escapeHtml(label), semantic, semantics);
}

export function renderKeyValue(rows, semantics) {
  return `<div class="kv-list">${rows.map((row) => `
    <div class="kv-row">
      <div>
        <div>${escapeHtml(row.label)}</div>
        ${row.detail ? `<div class="muted">${escapeHtml(row.detail)}</div>` : ""}
      </div>
      <div>${renderState(row.value || "UNKNOWN", row.semantic || "unknown", semantics)}</div>
    </div>
  `).join("")}</div>`;
}
