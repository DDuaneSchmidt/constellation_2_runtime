import { escapeHtml } from "/operator_shell/shared_components/dom.js";

function toneClass(tone) {
  const normalized = String(tone || "neutral").toLowerCase();
  if (["ready", "warning", "error", "info", "muted", "high", "medium", "low", "quiet"].includes(normalized)) {
    return normalized;
  }
  return "neutral";
}

function statusClass(value, fallback = "muted") {
  const normalized = String(value || fallback).trim().toLowerCase();
  if (["ready", "pass", "success", "healthy", "operational", "approved", "granted", "updated", "active", "complete", "closed"].some((token) => normalized.includes(token))) {
    return "status-ready";
  }
  if (["warning", "medium", "pending", "draft", "expiring"].some((token) => normalized.includes(token))) {
    return "status-warning";
  }
  if (["error", "fail", "blocked", "high", "critical", "expired"].some((token) => normalized.includes(token))) {
    return "status-error";
  }
  if (["info", "low", "review"].some((token) => normalized.includes(token))) {
    return "status-info";
  }
  return "status-muted";
}

function semanticStatusClass(semantic) {
  const normalized = String(semantic || "").toLowerCase();
  if (normalized === "healthy") {
    return "status-ready";
  }
  if (normalized === "warning") {
    return "status-warning";
  }
  if (normalized === "blocked") {
    return "status-error";
  }
  if (normalized === "info") {
    return "status-info";
  }
  return statusClass(normalized || "muted");
}

function numericPercent(value) {
  const parsed = Number.parseFloat(String(value || "0").replace("%", ""));
  if (!Number.isFinite(parsed)) {
    return 0;
  }
  return Math.max(0, Math.min(100, parsed));
}

function easternMarketSessionState(now = new Date()) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    weekday: "short",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).formatToParts(now).reduce((acc, part) => {
    acc[part.type] = part.value;
    return acc;
  }, {});
  const weekday = parts.weekday || "";
  const hour = Number.parseInt(parts.hour || "0", 10);
  const minute = Number.parseInt(parts.minute || "0", 10);
  const minutes = hour * 60 + minute;
  const isWeekday = !["Sat", "Sun"].includes(weekday);
  const isOpen = isWeekday && minutes >= 570 && minutes < 960;
  return {
    code: isOpen ? "IN_SESSION" : "OUT_OF_SESSION",
    label: isOpen ? "Market session is open." : "Market is closed or outside the regular evaluation session.",
  };
}

function hasMeaningfulUnavailable(data) {
  const sourceState = String(data?.data_source_state || "");
  if (sourceState.includes("UNAVAILABLE")) {
    return true;
  }
  const summary = Array.isArray(data?.summary) ? data.summary : [];
  const tiles = Array.isArray(data?.readiness?.tiles) ? data.readiness.tiles : [];
  return [...summary, ...tiles].filter((item) => String(item?.value || "").includes("UNAVAILABLE")).length >= 3;
}

function isStaleFreshness(freshness) {
  const parsed = Date.parse(String(freshness || ""));
  if (!Number.isFinite(parsed)) {
    return false;
  }
  return Date.now() - parsed > 6 * 60 * 60 * 1000;
}

function classifyOperatorState(data = {}) {
  const session = easternMarketSessionState();
  const sourceState = String(data.data_source_state || "");
  const fallbackBadge = String(data.fallback_badge || "");
  const summary = Array.isArray(data.summary) ? data.summary : [];
  const exceptions = Array.isArray(data.exceptions) ? data.exceptions : [];
  const combinedText = JSON.stringify({
    sourceState,
    fallbackBadge,
    summary,
    exceptions,
    context: data.context || {},
  }).toUpperCase();
  const hasAuthorityData = sourceState === "REAL";
  const unavailable = hasMeaningfulUnavailable(data);
  const stale = isStaleFreshness(data.context?.freshness);
  const hasRealBlockers = exceptions.some((exception) => {
    const title = String(exception?.title || "").toUpperCase();
    return title && title !== "NO ACTIVE BLOCKERS";
  }) || summary.some((item) => /BLOCK|FAIL|REJECT|ERROR/.test(String(item?.value || "").toUpperCase()));
  const marketClosed = session.code === "OUT_OF_SESSION" || /MARKET_CLOSED|MARKET_NOT_OPEN|OUT_OF_SESSION|SESSION_CLOSED/.test(combinedText);

  if (marketClosed && (unavailable || !hasAuthorityData || hasRealBlockers)) {
    return {
      state: "MARKET_CLOSED / OUT_OF_SESSION",
      semantic: "info",
      why: "System is not currently evaluating readiness because the market is closed.",
      nextAction: "Reopen Command / Overview during the next market session or inspect raw artifacts if reviewing prior evidence.",
      collapseSecondary: unavailable || !hasAuthorityData,
    };
  }
  if (stale) {
    return {
      state: "STALE_DATA",
      semantic: "warning",
      why: `Latest command authority freshness is ${data.context?.freshness}.`,
      nextAction: "Refresh the runtime authorities before using this surface for current operational decisions.",
      collapseSecondary: false,
    };
  }
  if (unavailable || !hasAuthorityData) {
    return {
      state: "PARTIAL_STATE",
      semantic: "warning",
      why: "One or more command authority artifacts are missing or unavailable.",
      nextAction: "Open raw artifacts or rerun the relevant authority producer before treating readiness as current.",
      collapseSecondary: true,
    };
  }
  if (hasRealBlockers) {
    const first = exceptions.find((exception) => String(exception?.title || "").toUpperCase() !== "NO ACTIVE BLOCKERS");
    return {
      state: "BLOCKED",
      semantic: "blocked",
      why: first?.body || first?.title || "A runtime authority reported a blocker.",
      nextAction: first?.primary_action ? String(first.primary_action) : "Review the first active exception and its supporting artifact.",
      collapseSecondary: false,
    };
  }
  return {
    state: "READY",
    semantic: "healthy",
    why: "Current command authority payload is complete and no active blocker is reported.",
    nextAction: "Continue with the governed operator workflow for the current session.",
    collapseSecondary: false,
  };
}

export function OperatorStateSummary({ state = {} } = {}) {
  return `
    <section class="command-section operator-state-summary ${semanticStatusClass(state.semantic)}">
      <div class="command-section-header">
        <div>
          <div class="section-eyebrow">Operator State</div>
          <h2>${escapeHtml(state.state || "PARTIAL_STATE")}</h2>
        </div>
      </div>
      <div class="metric-grid">
        <article class="metric-card">
          <div class="metric-label">State</div>
          <div class="metric-value">${escapeHtml(state.state || "PARTIAL_STATE")}</div>
        </article>
        <article class="metric-card">
          <div class="metric-label">Why</div>
          <div class="metric-value" style="font-size:16px;line-height:1.35;">${escapeHtml(state.why || "No explanation available.")}</div>
        </article>
        <article class="metric-card">
          <div class="metric-label">Next Action</div>
          <div class="metric-value" style="font-size:16px;line-height:1.35;">${escapeHtml(state.nextAction || "Inspect raw artifacts.")}</div>
        </article>
      </div>
    </section>
  `;
}

function actionPayload(exception, action) {
  return encodeURIComponent(JSON.stringify({
    title: exception?.title || "Exception",
    severity: exception?.severity || "MEDIUM",
    action,
  }));
}

export function StatusStrip({ summary = [] } = {}) {
  return `
    <section class="command-status-strip" aria-label="System status summary">
      ${summary.map((item) => `
        <article class="command-status-card tone-${toneClass(item.tone)} ${statusClass(`${item.label} ${item.value} ${item.tone}`)}">
          <div class="status-card-icon" aria-hidden="true">${statusClass(`${item.value} ${item.tone}`) === "status-ready" ? "✓" : statusClass(`${item.value} ${item.tone}`) === "status-warning" ? "!" : "•"}</div>
          <div class="command-card-label">${escapeHtml(item.label)}</div>
          <div class="command-card-value">${escapeHtml(item.value)}</div>
          <div class="command-card-detail">${escapeHtml(item.detail || "")}</div>
        </article>
      `).join("")}
    </section>
  `;
}

export function ExceptionCard({ exception } = {}) {
  const severity = String(exception?.severity || "MEDIUM").toUpperCase();
  const actions = Array.isArray(exception?.actions) ? exception.actions : [];
  const primaryAction = exception?.primary_action || actions[0]?.id || "";
  return `
    <article class="exception-card severity-${severity.toLowerCase()} ${statusClass(severity)}" tabindex="0" role="group" data-exception-card data-exception-primary-action="${escapeHtml(primaryAction)}" data-exception-payload="${actionPayload(exception, actions.find((action) => action.id === primaryAction) || actions[0] || {})}" aria-label="${escapeHtml(`${severity} exception: ${exception?.title || "Exception"}`)}">
      <div class="exception-icon" aria-hidden="true">${escapeHtml(exception?.icon || "!")}</div>
      <div class="exception-body">
        <div class="exception-kicker">
          <span>${escapeHtml(severity)}</span>
          <time>${escapeHtml(exception?.timestamp || "")}</time>
        </div>
        <h3>${escapeHtml(exception?.title || "Exception")}</h3>
        <p>${escapeHtml(exception?.body || "")}</p>
        <div class="exception-source">Source: ${escapeHtml(exception?.source || "UNKNOWN")}</div>
        <div class="exception-action-state" data-exception-action-state aria-live="polite"></div>
        <div class="exception-actions" aria-label="Exception actions">
          ${actions.length ? actions.map((action) => `
            <button class="exception-action-button" type="button" data-exception-action="${escapeHtml(action.id || "")}" data-exception-payload="${actionPayload(exception, action)}">
              ${escapeHtml(action.label || "Open")}
            </button>
          `).join("") : `<div class="exception-action-empty">No actions available.</div>`}
        </div>
      </div>
      <div class="exception-chevron" aria-hidden="true">›</div>
    </article>
  `;
}

export function ReadinessTile({ tile } = {}) {
  const tone = String(tile?.tone || (tile?.value === "WARNING" ? "warning" : "ready")).toLowerCase();
  return `
    <article class="readiness-tile tone-${toneClass(tone)} ${statusClass(`${tile?.value || ""} ${tone}`)}">
      <div class="readiness-label">${escapeHtml(tile?.label || "")}</div>
      <div class="readiness-value">${escapeHtml(tile?.value || "UNKNOWN")}</div>
    </article>
  `;
}

export function PolicyState({ policy = [], runtime = {} } = {}) {
  const total = policy.reduce((acc, item) => acc + Number(item.value || 0), 0) || 1;
  const active = policy.find((item) => item.label === "Active")?.value || 0;
  const activePct = Math.round((Number(active) / total) * 100);
  const runtimeState = runtime.state || "UNKNOWN";
  return `
    <section class="command-section policy-state-panel">
      <div class="command-section-header">
        <div>
          <div class="section-eyebrow">Policy State</div>
          <h2>Runtime Policy Mix</h2>
        </div>
      </div>
      <div class="policy-state-body">
        <div class="policy-donut" style="--policy-active:${activePct}" aria-label="${activePct}% active policies">
          <span>${activePct}%</span>
        </div>
        <div class="policy-state-list">
          ${policy.map((item) => `
            <div class="policy-state-row tone-${toneClass(item.tone)} ${statusClass(`${item.label} ${item.tone}`)}">
              <span>${escapeHtml(item.label)}</span>
              <strong>${escapeHtml(String(item.value))}</strong>
            </div>
          `).join("")}
        </div>
      </div>
      <div class="policy-runtime-banner tone-${toneClass(runtime.tone)} ${statusClass(`${runtimeState} ${runtime.tone}`)}">
        <span>Runtime</span>
        <strong>${escapeHtml(runtimeState)}</strong>
        <small>${escapeHtml(runtime.expired ? `${runtime.reason || ""} ${runtime.remediation || ""}`.trim() : runtime.reason || "No elapsed runtime expiry was found.")}</small>
      </div>
    </section>
  `;
}

export function DecisionList({ decisions = [] } = {}) {
  return `
    <section class="command-section">
      <div class="command-section-header">
        <div>
          <div class="section-eyebrow">Recent Decisions</div>
          <h2>Latest Governed Actions</h2>
        </div>
      </div>
      <div class="decision-list">
        ${decisions.map((decision) => `
          <article class="decision-item">
            <div class="decision-status" aria-hidden="true">✓</div>
            <div>
              <h3>${escapeHtml(decision.title)}</h3>
              <p>${escapeHtml(decision.description)}</p>
              <div class="decision-meta">${escapeHtml(decision.timestamp)} • ${escapeHtml(decision.actor)}</div>
            </div>
            <span class="decision-badge">${escapeHtml(decision.status)}</span>
          </article>
        `).join("")}
      </div>
    </section>
  `;
}

export function EvidenceSummary({ evidence = [] } = {}) {
  return `
    <section class="command-section">
      <div class="command-section-header">
        <div>
          <div class="section-eyebrow">Evidence Summary</div>
          <h2>Supporting References</h2>
        </div>
      </div>
      <div class="evidence-summary-grid">
        ${evidence.map((item) => `
          <article class="evidence-summary-card">
            <div class="command-card-label">${escapeHtml(item.label)}</div>
            <div class="command-card-value">${escapeHtml(item.value)}</div>
          </article>
        `).join("")}
      </div>
    </section>
  `;
}

export function DataLineage({ lineage = [] } = {}) {
  return `
    <section class="command-section">
      <div class="command-section-header">
        <div>
          <div class="section-eyebrow">Data Lineage</div>
          <h2>Source Flow</h2>
        </div>
      </div>
      <div class="lineage-flow">
        ${lineage.map((node, index) => `
          <article class="lineage-node">
            <div class="lineage-live">${escapeHtml(node.status === "UNAVAILABLE" ? "UNAVAILABLE" : "LIVE")}</div>
            <h3>${escapeHtml(node.name)}</h3>
            <p>${escapeHtml(node.description)}</p>
          </article>
          ${index < lineage.length - 1 ? `<div class="lineage-arrow" aria-hidden="true">→</div>` : ""}
        `).join("")}
      </div>
    </section>
  `;
}

export function ContextRail({ context = {} } = {}) {
  const sourceRefs = Array.isArray(context.sourceRefs) ? context.sourceRefs : [];
  return `
    <section class="context-card">
      <div class="context-eyebrow">Context</div>
      <p class="context-answer">${escapeHtml(context.answer || "")}</p>
    </section>
    <section class="context-card">
      <div class="context-eyebrow">You Can</div>
      <ul class="context-action-list">
        ${(context.can || []).map((item) => `<li>${escapeHtml(item)}</li>`).join("")}
      </ul>
    </section>
    <section class="context-card">
      <div class="context-eyebrow">Source Of Truth</div>
      <strong>${escapeHtml(context.sourceOfTruth || "UNKNOWN")}</strong>
      ${context.dataSourceState ? `<div class="command-source-badge">${escapeHtml(context.dataSourceState)}</div>` : ""}
    </section>
    <section class="context-card context-metrics">
      <div><span>Freshness</span><strong>${escapeHtml(context.freshness || "UNKNOWN")}</strong></div>
      <div><span>Data Quality</span><strong>${escapeHtml(context.dataQuality || "UNKNOWN")}</strong></div>
      <div><span>Access</span><strong>${escapeHtml(context.access || "UNKNOWN")}</strong></div>
    </section>
    <section class="context-card">
      <div class="context-eyebrow">Authority Evidence</div>
      <ul class="context-source-list">
        ${sourceRefs.slice(0, 6).map((ref) => {
          const timestamp = ref.last_update_utc || ref.produced_utc || "UNAVAILABLE";
          const path = ref.path || "UNAVAILABLE";
          return `<li><span>${escapeHtml(ref.name || ref.artifact_type || "artifact")}</span><small>${escapeHtml(`${path} • ${timestamp}`)}</small></li>`;
        }).join("")}
      </ul>
    </section>
  `;
}

export function renderCommandOverview(data) {
  const sourceState = data.data_source_state || "";
  const fallbackBadge = data.fallback_badge || "";
  const exceptions = Array.isArray(data.exceptions) ? data.exceptions : [];
  const summary = Array.isArray(data.summary) ? data.summary : [];
  const readiness = data.readiness || { score: "0%", tiles: [] };
  const readinessTiles = Array.isArray(readiness.tiles) ? readiness.tiles : [];
  const operatorState = classifyOperatorState(data);
  const collapsedSecondary = operatorState.collapseSecondary;
  return `
    <div class="command-overview">
      ${OperatorStateSummary({ state: operatorState })}
      ${sourceState && sourceState !== "REAL" ? `<div class="command-source-banner">${escapeHtml(fallbackBadge || sourceState)} data source: Command Overview is not fully authority-backed for this day. Secondary tiles are collapsed to avoid implying live readiness.</div>` : ""}
      ${collapsedSecondary ? "" : StatusStrip({ summary })}
      <section class="command-section what-matters">
        <div class="command-section-header">
          <div>
            <div class="section-eyebrow">What Matters Now</div>
            <h2>Actionable Exceptions</h2>
          </div>
          <span class="section-count">${escapeHtml(String(exceptions.length))} active</span>
        </div>
        <div class="exception-grid">
          ${exceptions.map((exception) => ExceptionCard({ exception })).join("")}
        </div>
      </section>
      <section class="command-two-column">
        <div class="command-section readiness-panel">
          <div class="command-section-header">
            <div>
              <div class="section-eyebrow">Command Readiness</div>
              <h2>Readiness Score</h2>
            </div>
            <div class="readiness-score">${escapeHtml(readiness.score)}</div>
          </div>
          ${collapsedSecondary
            ? `<div class="empty-state">Secondary readiness tiles are hidden because the primary operator state is ${escapeHtml(operatorState.state)}. Raw artifact evidence remains available in the context rail.</div>`
            : `
              <div class="readiness-progress" style="--readiness-score:${numericPercent(readiness.score)}" aria-label="Readiness ${escapeHtml(readiness.score)}">
                <span></span>
              </div>
              <div class="readiness-grid">
                ${readinessTiles.map((tile) => ReadinessTile({ tile })).join("")}
              </div>
            `}
        </div>
        ${PolicyState({ policy: Array.isArray(data.policy) ? data.policy : [], runtime: data.policy_runtime || {} })}
      </section>
      <section class="command-two-column">
        ${DecisionList({ decisions: Array.isArray(data.decisions) ? data.decisions : [] })}
        ${EvidenceSummary({ evidence: Array.isArray(data.evidence) ? data.evidence : [] })}
      </section>
      ${DataLineage({ lineage: Array.isArray(data.lineage) ? data.lineage : [] })}
    </div>
  `;
}
