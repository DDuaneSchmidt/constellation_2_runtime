import { renderEvidenceRefs, renderKeyValue } from "/operator_shell/shared_components/dom.js";

export function renderOperations(view, semantics) {
  return `
    <div class="panel-grid">
      <section class="panel">
        <h3>Readiness Ladder</h3>
        ${renderKeyValue((view.readiness_ladder || []).map((row) => ({
          label: row.label,
          value: row.status,
          semantic: row.semantic,
          detail: row.key,
        })), semantics)}
      </section>
      <section class="panel">
        <h3>Session Authority</h3>
        ${renderKeyValue([
          { label: "Active day", value: view.session_authority_details?.active_day, semantic: "canonical" },
          { label: "Target day", value: view.session_authority_details?.target_day, semantic: "canonical" },
          { label: "Authorization", value: view.session_authority_details?.submission_authorization_status, semantic: view.state_summary?.status_severity === "WARNING" ? "warning" : "healthy" },
          { label: "Traceability", value: view.session_authority_details?.traceability_status, semantic: "derived" },
        ], semantics)}
      </section>
      <section class="panel">
        <h3>Replay / Certification</h3>
        ${renderKeyValue([
          { label: "Replay gate", value: view.replay_certification_summary?.status, semantic: view.replay_certification_summary?.semantic },
        ], semantics)}
      </section>
      <section class="panel">
        <h3>Broker Connectivity</h3>
        ${renderKeyValue([
          { label: "Handshake", value: view.broker_connectivity_summary?.status, semantic: view.broker_connectivity_summary?.semantic },
        ], semantics)}
      </section>
      <section class="panel-wide">
        <h3>Integrity Summary</h3>
        ${renderKeyValue([
          { label: "Integrity", value: view.integrity_summary?.status, semantic: view.integrity_summary?.semantic, detail: view.integrity_summary?.executed_at },
        ], semantics)}
      </section>
      <section class="panel-wide">
        <h3>Evidence</h3>
        ${renderEvidenceRefs(view.source_refs || [])}
      </section>
    </div>
  `;
}
