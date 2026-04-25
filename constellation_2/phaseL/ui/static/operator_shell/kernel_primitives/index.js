import { escapeHtml } from "/operator_shell/shared_components/dom.js";
import { statePill } from "/operator_shell/shared_status/index.js";

function renderFieldRows(fields = []) {
  if (!fields.length) {
    return `<div class="empty-state">No fields.</div>`;
  }
  return `<div class="artifact-field-list">${fields.map((field) => `
    <div class="artifact-field-row">
      <span class="artifact-field-label">${escapeHtml(field.label || "Field")}</span>
      <strong class="artifact-field-value">${escapeHtml(field.value || "n/a")}</strong>
      ${field.detail ? `<div class="artifact-field-detail">${escapeHtml(field.detail)}</div>` : ""}
    </div>
  `).join("")}</div>`;
}

function renderReasonCodes(reasonCodes = []) {
  if (!reasonCodes.length) {
    return "";
  }
  return `<div class="reason-code-list">${reasonCodes.map((reasonCode) => `
    <span class="marker-pill tone-red">${escapeHtml(reasonCode)}</span>
  `).join("")}</div>`;
}

export function renderAuthorityArtifactCard(card, semantics) {
  return `
    <article class="kernel-card authority-artifact-card">
      <div class="card-header">
        <div>
          <div class="workspace-eyebrow">Authority Artifact</div>
          <h3>${escapeHtml(card.artifact_type || "Artifact")}</h3>
        </div>
        <div>${statePill(escapeHtml(card.status || "unknown"), card.semantic || "unknown", semantics)}</div>
      </div>
      <div class="artifact-identity">
        <span class="mono">${escapeHtml(card.artifact_id || "n/a")}</span>
        ${card.artifact_path ? `<button class="evidence-button" type="button" data-artifact-path="${escapeHtml(card.artifact_path)}" data-artifact-title="${escapeHtml(card.artifact_type || "Artifact")}">Open</button>` : ""}
      </div>
      <div class="artifact-meta">Effective ${escapeHtml(card.effective_time || "n/a")}</div>
      ${renderFieldRows(card.fields || [])}
      ${renderReasonCodes(card.reason_codes || [])}
    </article>
  `;
}

export function renderDecisionCard(card, semantics) {
  return `
    <article class="kernel-card decision-card">
      <div class="card-header">
        <div>
          <div class="workspace-eyebrow">Decision / Gate</div>
          <h3>${escapeHtml(card.artifact_type || "Decision")}</h3>
        </div>
        <div>${statePill(escapeHtml(card.status || "unknown"), card.semantic || "unknown", semantics)}</div>
      </div>
      <div class="artifact-identity">
        <span class="mono">${escapeHtml(card.artifact_id || "n/a")}</span>
        ${card.artifact_path ? `<button class="evidence-button" type="button" data-artifact-path="${escapeHtml(card.artifact_path)}" data-artifact-title="${escapeHtml(card.artifact_type || "Decision")}">Open</button>` : ""}
      </div>
      <div class="artifact-meta">Effective ${escapeHtml(card.effective_time || "n/a")}</div>
      ${renderFieldRows(card.fields || [])}
      ${renderReasonCodes(card.reason_codes || [])}
    </article>
  `;
}

export function renderRunEnvelopePanel(panel, semantics) {
  const refs = panel.artifact_refs || {};
  return `
    <article class="kernel-card envelope-card">
      <div class="card-header">
        <div>
          <div class="workspace-eyebrow">Run Envelope</div>
          <h3>${escapeHtml(panel.artifact_type || "Run Envelope")}</h3>
        </div>
        <div>${statePill(escapeHtml(panel.status || "unknown"), panel.semantic || "unknown", semantics)}</div>
      </div>
      <div class="artifact-identity">
        <span class="mono">${escapeHtml(panel.artifact_id || "n/a")}</span>
        ${panel.artifact_path ? `<button class="evidence-button" type="button" data-artifact-path="${escapeHtml(panel.artifact_path)}" data-artifact-title="${escapeHtml(panel.artifact_type || "Run Envelope")}">Open</button>` : ""}
      </div>
      <div class="artifact-meta">Produced ${escapeHtml(panel.effective_time || "n/a")}</div>
      <div class="artifact-field-list">
        ${Object.entries(refs).map(([key, value]) => `
          <div class="artifact-field-row">
            <span class="artifact-field-label">${escapeHtml(key)}</span>
            <strong class="artifact-field-value">${escapeHtml(String(value ?? ""))}</strong>
          </div>
        `).join("")}
      </div>
      ${renderReasonCodes(panel.reason_codes || [])}
    </article>
  `;
}

export function renderDerivedSummaryCard(card) {
  return `
    <article class="kernel-card derived-summary-card">
      <div class="card-header">
        <div>
          <div class="workspace-eyebrow">Derived / Non-Authoritative</div>
          <h3>${escapeHtml(card.title || "Derived Summary")}</h3>
        </div>
        <div><span class="marker-pill tone-blue">derived</span></div>
      </div>
      ${renderFieldRows(card.fields || [])}
    </article>
  `;
}

export function renderLineageChain(chain, semantics) {
  const nodes = chain?.nodes || [];
  if (!nodes.length) {
    return `<div class="empty-state">No explicit lineage.</div>`;
  }
  return `
    <article class="kernel-card lineage-chain-card">
      <div class="card-header">
        <div>
          <div class="workspace-eyebrow">Lineage Chain</div>
          <h3>Upstream Authority Chain</h3>
        </div>
      </div>
      <div class="lineage-chain-list">
        ${nodes.map((node) => `
          <div class="lineage-chain-node">
            <div>
              <div class="artifact-field-label">${escapeHtml(node.label || "Node")}</div>
              <strong>${escapeHtml(node.artifact_type || "artifact")}</strong>
              <div class="mono">${escapeHtml(node.artifact_id || "missing")}</div>
            </div>
            <div class="lineage-node-actions">
              ${statePill(escapeHtml(node.status || "unknown"), node.status === "current" ? "healthy" : "unknown", semantics)}
              ${node.artifact_path ? `<button class="evidence-button" type="button" data-artifact-path="${escapeHtml(node.artifact_path)}" data-artifact-title="${escapeHtml(node.artifact_type || "artifact")}">Open</button>` : ""}
            </div>
          </div>
        `).join("")}
      </div>
    </article>
  `;
}

export function renderSupersessionBanner(banner) {
  if (!banner?.show) {
    return "";
  }
  return `
    <section class="supersession-banner">
      <strong>Superseded Artifact</strong>
      <span>${escapeHtml(banner.message || "This artifact is no longer current.")}</span>
    </section>
  `;
}

function inputValueMarkup(input) {
  if (input.type === "checkbox") {
    return `<input type="checkbox" name="${escapeHtml(input.name)}" ${input.value ? "checked" : ""} />`;
  }
  if (input.type === "hidden") {
    return `<input type="hidden" name="${escapeHtml(input.name)}" value="${escapeHtml(input.value ?? "")}" />`;
  }
  return `
    <label class="command-input">
      <span>${escapeHtml(input.label || input.name)}</span>
      <input type="text" name="${escapeHtml(input.name)}" value="${escapeHtml(input.value ?? "")}" ${input.required ? "required" : ""} />
    </label>
  `;
}

function renderCommandResult(result) {
  if (!result) {
    return "";
  }
  const artifactRef = result.authority_artifact_ref;
  const envelopeRef = result.envelope_ref;
  return `
    <div class="command-result">
      <div class="artifact-field-row">
        <span class="artifact-field-label">Outcome</span>
        <strong class="artifact-field-value">${escapeHtml(result.outcome || "unknown")}</strong>
      </div>
      ${envelopeRef ? `
        <div class="artifact-field-row">
          <span class="artifact-field-label">Envelope</span>
          <strong class="artifact-field-value mono">${escapeHtml(envelopeRef.artifact_id || "n/a")}</strong>
        </div>
      ` : ""}
      ${artifactRef ? `
        <div class="artifact-field-row">
          <span class="artifact-field-label">Authority Artifact</span>
          <strong class="artifact-field-value mono">${escapeHtml(artifactRef.artifact_id || "n/a")}</strong>
        </div>
      ` : ""}
      ${renderReasonCodes(result.reason_codes || [])}
    </div>
  `;
}

export function renderCommandPanel(command, result) {
  if (!command) {
    return "";
  }
  const inputs = command.inputs || [];
  return `
    <article class="kernel-card command-panel-card">
      <div class="card-header">
        <div>
          <div class="workspace-eyebrow">Command Panel</div>
          <h3>${escapeHtml(command.label || "Command")}</h3>
        </div>
        <div>${command.supported ? `<span class="marker-pill tone-green">artifact-bound</span>` : `<span class="marker-pill tone-red">blocked</span>`}</div>
      </div>
      ${command.artifact_binding ? `
        <div class="artifact-meta mono">${escapeHtml(JSON.stringify(command.artifact_binding))}</div>
      ` : `<div class="artifact-meta">No explicit artifact binding.</div>`}
      <form class="command-form" data-command-endpoint="${escapeHtml(command.endpoint)}" data-command-id="${escapeHtml(command.command_id)}">
        ${inputs.map((input) => inputValueMarkup(input)).join("")}
        <button class="action-button" type="submit" ${command.supported ? "" : "disabled"}>${escapeHtml(command.label || "Run Command")}</button>
      </form>
      ${!command.supported ? renderReasonCodes(command.reason_codes || []) : ""}
      ${renderCommandResult(result)}
    </article>
  `;
}
