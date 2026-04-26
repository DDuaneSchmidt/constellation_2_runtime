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

export function renderOperatorDiagnosticPanel(diagnostic = {}) {
  const endpointAttempted = diagnostic.endpointAttempted || "unknown";
  const failureClass = diagnostic.failureClass || "UNKNOWN";
  const backendUnreachable = diagnostic.backendUnreachable ? "yes" : "no";
  const routeMissing = diagnostic.routeMissing ? "yes" : "no";
  const payloadInvalid = diagnostic.payloadInvalid ? "yes" : "no";
  const statusCode = diagnostic.statusCode === null || diagnostic.statusCode === undefined
    ? "n/a"
    : String(diagnostic.statusCode);
  const nextAction = diagnostic.nextAction || "Inspect backend logs and browser network traces.";

  return `
    <article class="stack-card error-state">
      <div class="stack-card-title">Capital API Diagnostic</div>
      <div class="stack-card-subtitle">Fail-closed operator guidance for transport and payload contract failures.</div>
      <div class="line-list">
        <div><strong>Endpoint attempted:</strong> <span class="mono">${escapeHtml(endpointAttempted)}</span></div>
        <div><strong>Failure class:</strong> ${escapeHtml(failureClass)}</div>
        <div><strong>HTTP status:</strong> ${escapeHtml(statusCode)}</div>
        <div><strong>Backend unreachable:</strong> ${escapeHtml(backendUnreachable)}</div>
        <div><strong>Route missing:</strong> ${escapeHtml(routeMissing)}</div>
        <div><strong>Payload invalid:</strong> ${escapeHtml(payloadInvalid)}</div>
      </div>
      <div class="stack-card-subtitle">${escapeHtml(nextAction)}</div>
    </article>
  `;
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
