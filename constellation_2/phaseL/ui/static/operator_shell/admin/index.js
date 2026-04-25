import { renderEvidenceRefs } from "/operator_shell/shared_components/dom.js";

export function renderAdmin(inventory) {
  const actions = inventory.actions || [];
  const audit = inventory.audit_entries || [];
  return `
    <div class="panel-grid">
      <section class="panel-wide">
        <h3>Governed Actions</h3>
        <div class="audit-list">
          ${actions.map((action) => `
            <div class="audit-row">
              <div>
                <div>${action.action_name}</div>
                <div class="muted">${action.reason || ""}</div>
              </div>
              <div>
                ${action.supported ? `<button class="action-button" type="button" data-run-action="${action.action_name}">Run</button>` : `<span class="muted">Unsupported</span>`}
              </div>
            </div>
          `).join("")}
        </div>
      </section>
      <section class="panel-wide">
        <h3>Action Audit</h3>
        <div class="audit-list">
          ${audit.map((entry) => `
            <div class="audit-row">
              <div>
                <div>${entry.action_name}</div>
                <div class="muted mono">${entry.time} · ${entry.result}</div>
              </div>
              <div class="mono">${(entry.target || {}).day_utc || ""}</div>
            </div>
          `).join("") || `<div class="empty-state">No action audit entries yet.</div>`}
        </div>
      </section>
    </div>
  `;
}
