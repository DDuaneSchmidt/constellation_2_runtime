import { escapeHtml, renderEvidenceRefs } from "/operator_shell/shared_components/dom.js";
import { markerPill, semanticTone, statePill } from "/operator_shell/shared_status/index.js";

export function renderAegisMark({ size = "md", label = "Aegis" } = {}) {
  return `
    <div class="aegis-mark aegis-mark-${escapeHtml(size)}" aria-label="${escapeHtml(label)}">
      <svg viewBox="0 0 120 120" role="img" aria-hidden="true">
        <defs>
          <linearGradient id="aegisRing" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" stop-color="#4de0c7"></stop>
            <stop offset="100%" stop-color="#3a8dff"></stop>
          </linearGradient>
          <linearGradient id="aegisCore" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" stop-color="#15324f"></stop>
            <stop offset="100%" stop-color="#09131f"></stop>
          </linearGradient>
        </defs>
        <circle cx="60" cy="60" r="49" class="aegis-ring"></circle>
        <path d="M60 22 L95 86 L25 86 Z" class="aegis-triangle"></path>
        <circle cx="60" cy="60" r="11" class="aegis-node"></circle>
      </svg>
    </div>
  `;
}

export function formatTimestamp(value) {
  if (!value) {
    return "n/a";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return escapeHtml(String(value));
  }
  return escapeHtml(
    parsed.toLocaleString("en-US", {
      year: "numeric",
      month: "short",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit",
      timeZoneName: "short",
    }),
  );
}

export function renderSemanticBadge(label, semantic, semantics) {
  return statePill(escapeHtml(label || "Unknown"), semantic || "unknown", semantics);
}

export function renderMarker(marker, semantics) {
  return markerPill(marker, semantics);
}

export function renderMetricCard({ label, value, detail = "", semantic = "", semantics = {}, badge = "" }) {
  const badgeMarkup = badge ? `<div class="metric-card-badge">${badge}</div>` : "";
  const normalizedValue = value ?? "n/a";
  const stateMarkup = semantic ? renderSemanticBadge(normalizedValue, semantic, semantics) : `<strong class="metric-card-value">${escapeHtml(normalizedValue)}</strong>`;
  return `
    <article class="metric-card">
      <div class="metric-card-header">
        <span class="metric-card-label">${escapeHtml(label)}</span>
        ${badgeMarkup}
      </div>
      <div class="metric-card-main">${stateMarkup}</div>
      ${detail ? `<div class="metric-card-detail">${escapeHtml(detail)}</div>` : ""}
    </article>
  `;
}

export function renderSectionHeader({ eyebrow = "", title = "", subtitle = "" }) {
  return `
    <div class="section-header">
      ${eyebrow ? `<div class="section-eyebrow">${escapeHtml(eyebrow)}</div>` : ""}
      <h3>${escapeHtml(title)}</h3>
      ${subtitle ? `<p>${escapeHtml(subtitle)}</p>` : ""}
    </div>
  `;
}

export function renderCardSection({ eyebrow = "", title = "", subtitle = "", body = "" }) {
  return `
    <section class="panel-card">
      ${renderSectionHeader({ eyebrow, title, subtitle })}
      <div class="panel-card-body">
        ${body}
      </div>
    </section>
  `;
}

export function renderDefinitionRows(rows = []) {
  if (!rows.length) {
    return `<div class="empty-state">No values available.</div>`;
  }
  return `
    <div class="definition-list">
      ${rows.map((row) => `
        <div class="definition-row">
          <span class="definition-term">${escapeHtml(row.label || "Field")}</span>
          <div class="definition-value">
            <strong>${escapeHtml(row.value ?? "n/a")}</strong>
            ${row.detail ? `<div class="definition-detail">${escapeHtml(row.detail)}</div>` : ""}
          </div>
        </div>
      `).join("")}
    </div>
  `;
}

export function renderSimpleTable({ columns = [], rows = [], emptyMessage = "No rows available." }) {
  if (!rows.length) {
    return `<div class="empty-state">${escapeHtml(emptyMessage)}</div>`;
  }
  return `
    <div class="table-wrap">
      <table class="data-table">
        <thead>
          <tr>
            ${columns.map((column) => `<th>${escapeHtml(column.label || "")}</th>`).join("")}
          </tr>
        </thead>
        <tbody>
          ${rows.map((row) => `
            <tr>
              ${columns.map((column) => `<td>${column.render ? column.render(row) : escapeHtml(row[column.key] ?? "n/a")}</td>`).join("")}
            </tr>
          `).join("")}
        </tbody>
      </table>
    </div>
  `;
}

export function renderList(items = [], { className = "stack-list", renderItem } = {}) {
  if (!items.length) {
    return `<div class="empty-state">Nothing to show.</div>`;
  }
  return `<div class="${escapeHtml(className)}">${items.map((item) => renderItem(item)).join("")}</div>`;
}

export function renderTrustPanel(trustPanel = {}) {
  const limitations = Array.isArray(trustPanel.bounded_limitations) ? trustPanel.bounded_limitations : [];
  const suppressionRules = Array.isArray(trustPanel.suppression_rules_applied) ? trustPanel.suppression_rules_applied : [];
  return renderCardSection({
    eyebrow: "Trust",
    title: "Presentation Trust",
    subtitle: "Governed exactness and integrity state for the active operator surface.",
    body: `
      ${renderDefinitionRows([
        { label: "Trust classification", value: trustPanel.trust_classification || "UNAVAILABLE" },
        { label: "Exactness", value: trustPanel.exactness_classification || "UNAVAILABLE" },
        { label: "Finalization", value: trustPanel.finalization_state || "UNAVAILABLE" },
        { label: "Integrity", value: trustPanel.integrity_state || "UNAVAILABLE" },
      ])}
      ${limitations.length ? `<div class="chip-list">${limitations.map((line) => `<span class="support-chip">${escapeHtml(line)}</span>`).join("")}</div>` : `<div class="empty-state">No bounded limitations were reported.</div>`}
      ${suppressionRules.length ? `<div class="support-note">Suppression rules: ${escapeHtml(suppressionRules.join(", "))}</div>` : ""}
    `,
  });
}

export function renderSourceRefCard(refs = [], title = "Source Evidence", subtitle = "Immutable artifact references backing this surface.") {
  return renderCardSection({
    eyebrow: "Evidence",
    title,
    subtitle,
    body: renderEvidenceRefs(refs),
  });
}

export function renderGapState({ title, summary, bullets = [], refs = [] }) {
  return renderCardSection({
    eyebrow: "Backend Gap",
    title,
    subtitle: summary,
    body: `
      <div class="gap-state">
        <ul class="gap-list">
          ${bullets.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}
        </ul>
        ${refs.length ? renderEvidenceRefs(refs) : ""}
      </div>
    `,
  });
}

export function renderWorkflowStep(step = {}, semantics = {}) {
  const refs = Array.isArray(step.evidence_refs) ? step.evidence_refs : [];
  return `
    <article class="stack-card">
      <div class="stack-card-header">
        <div>
          <div class="stack-card-title">${escapeHtml(step.title || "Next step")}</div>
          <div class="stack-card-subtitle">${escapeHtml(step.rationale || "No rationale available.")}</div>
        </div>
        ${step.target_surface ? `<a class="inline-link" href="${escapeHtml(step.target_surface)}" data-route="${escapeHtml(step.target_surface)}">Open</a>` : ""}
      </div>
      <div class="stack-card-meta">
        ${step.next_step_kind ? `<span class="support-chip">${escapeHtml(step.next_step_kind)}</span>` : ""}
        ${step.target_entity_id ? `<span class="support-chip">${escapeHtml(step.target_entity_id)}</span>` : ""}
      </div>
      ${refs.length ? `<div class="embedded-evidence">${renderEvidenceRefs(refs)}</div>` : ""}
    </article>
  `;
}

export function renderPanelRows(rows = [], semantics = {}) {
  if (!rows.length) {
    return `<div class="empty-state">No panels available.</div>`;
  }
  return rows.map((panel) => `
    <article class="stack-card ${panel.suppressed ? "is-suppressed" : ""}">
      <div class="stack-card-header">
        <div>
          <div class="stack-card-title">${escapeHtml(panel.title || panel.panel_id || "Panel")}</div>
          <div class="stack-card-subtitle">${escapeHtml(panel.panel_id || "")}</div>
        </div>
        ${renderSemanticBadge(panel.state || "UNKNOWN", panel.suppressed ? "warning" : "healthy", semantics)}
      </div>
      <div class="line-list">
        ${(Array.isArray(panel.content_lines) ? panel.content_lines : []).map((line) => `<div>${escapeHtml(line)}</div>`).join("")}
      </div>
      ${(Array.isArray(panel.source_refs) && panel.source_refs.length)
        ? `<div class="chip-list">${panel.source_refs.map((ref) => `<button class="support-chip chip-button" type="button" data-artifact-path="${escapeHtml(ref)}" data-artifact-title="${escapeHtml(panel.title || panel.panel_id || "Panel source")}">Open source</button>`).join("")}</div>`
        : ""}
    </article>
  `).join("");
}

export function renderStatusPill(label, semantic, semantics = {}) {
  return `<span class="status-pill ${semanticTone(semantic || "unknown", semantics)}">${escapeHtml(label || "Unknown")}</span>`;
}
