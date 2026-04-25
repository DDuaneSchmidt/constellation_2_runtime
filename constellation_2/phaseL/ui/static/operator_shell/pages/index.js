import {
  activateConfigurationDraft,
  createConfigurationDraft,
  fetchActionAudit,
  fetchActivityToday,
  fetchAdvisory,
  fetchAlerts,
  fetchCapitalAccounts,
  fetchCapitalAllocation,
  fetchCapitalCashflow,
  fetchCapitalFlows,
  fetchCapitalHistory,
  fetchCapitalOverview,
  fetchCapitalValidation,
  fetchConfigurationCatalog,
  fetchConfigurationCurrent,
  fetchConfigurationDraft,
  fetchFinancialState,
  fetchPolicyEvolution,
  fetchRefinement,
  fetchIntegrity,
  fetchOutcomes,
  fetchValue,
  fetchOperations,
  fetchOpportunities,
  fetchOperatorHome,
  fetchOperatorQuery,
  fetchReconciliation,
  fetchSleeves,
  fetchTax,
  fetchSystemActions,
  rejectConfigurationDraft,
  reviewConfigurationDraft,
  validateConfigurationDraft,
} from "/operator_shell/domain_client/index.js";
import { escapeHtml, renderEvidenceRefs } from "/operator_shell/shared_components/dom.js";
import {
  formatTimestamp,
  renderAegisMark,
  renderCardSection,
  renderDefinitionRows,
  renderGapState,
  renderList,
  renderMetricCard,
  renderPanelRows,
  renderSectionHeader,
  renderSemanticBadge,
  renderSimpleTable,
  renderSourceRefCard,
  renderStatusPill,
  renderTrustPanel,
  renderWorkflowStep,
} from "/operator_shell/aegis_components/index.js";

export const ROUTES = [
  {
    path: "/",
    id: "command",
    label: "Command",
    eyebrow: "Aegis Command",
    subtitle: "One governed entry point over readiness, trust, advisory guidance, and operator action.",
  },
  {
    path: "/capital",
    id: "capital_overview",
    label: "Capital",
    eyebrow: "Capital Overview",
    subtitle: "Household allocation truth over append-only snapshots, flows, and effective-dated classifications.",
  },
  {
    path: "/capital/accounts",
    id: "capital_accounts",
    label: "Capital Accounts",
    eyebrow: "Account Registry",
    subtitle: "Current account semantics, latest balances, include/exclude state, and traceable as-of basis.",
  },
  {
    path: "/capital/allocation",
    id: "capital_allocation",
    label: "Capital Allocation",
    eyebrow: "Allocation Lens",
    subtitle: "Control and bucket allocation with matrix breakdown and explainable denominator/numerator basis.",
  },
  {
    path: "/capital/history",
    id: "capital_history",
    label: "Capital History",
    eyebrow: "History",
    subtitle: "Investable total and Aegis allocation percentage over time from append-only snapshots.",
  },
  {
    path: "/capital/flows",
    id: "capital_flows",
    label: "Capital Flows",
    eyebrow: "Cash Flows",
    subtitle: "Append-only external contribution and withdrawal history with monthly summary rollups.",
  },
  {
    path: "/capital/cashflow",
    id: "capital_cashflow",
    label: "Capital Cashflow",
    eyebrow: "Cashflow Timeline",
    subtitle: "Deterministic monthly cashflow projection with scenario toggles and explicit basis disclosures.",
  },
  {
    path: "/capital/validation",
    id: "capital_validation",
    label: "Capital Validation",
    eyebrow: "Data Integrity",
    subtitle: "Validation findings, severity, impacted accounts, and explicit degraded-state diagnostics.",
  },
  {
    path: "/portfolio",
    id: "portfolio",
    label: "Portfolio",
    eyebrow: "Financial State",
    subtitle: "Positions and portfolio-operating facts from canonical runtime projections.",
  },
  {
    path: "/sleeves",
    id: "sleeves",
    label: "Sleeves",
    eyebrow: "Sleeve Evaluation",
    subtitle: "Sleeve proof belongs here once a canonical UI read model exists.",
  },
  {
    path: "/opportunities",
    id: "opportunities",
    label: "Opportunities",
    eyebrow: "Review Plane",
    subtitle: "Governed proactive opportunities, blocked review items, and changed-since-last-review summaries.",
  },
  {
    path: "/advisory",
    id: "advisory",
    label: "Advisory",
    eyebrow: "Recommendation Surface",
    subtitle: "Recommendation packets, operational availability, and household-facing advisory context.",
  },
  {
    path: "/tax",
    id: "tax",
    label: "Tax",
    eyebrow: "Tax Awareness",
    subtitle: "Tax readiness, account tax context, and explicit backend gaps from a canonical tax-state projection.",
  },
  {
    path: "/outcomes",
    id: "outcomes",
    label: "Value",
    eyebrow: "Value Proof",
    subtitle: "Governed realized value, sleeve linkage, bounded attribution, and explicit claim strength over already-certified truth.",
  },
  {
    path: "/refinement",
    id: "refinement",
    label: "Refinement",
    eyebrow: "Refinement Proof",
    subtitle: "Governed simplification, demotion, compression, reversibility, and before/after provenance over product truth.",
  },
  {
    path: "/policy",
    id: "policy",
    label: "Policy",
    eyebrow: "Policy Evolution",
    subtitle: "Governed temporal policy proposals over refinement and product emphasis, with explicit expiry, rollback, and trust overrides.",
  },
  {
    path: "/operations",
    id: "operations",
    label: "Operations",
    eyebrow: "Readiness & Runtime",
    subtitle: "Readiness ladders, runtime blocks, alerts, and governed operator actions.",
  },
  {
    path: "/configuration",
    id: "configuration",
    label: "Configuration",
    eyebrow: "Control Plane",
    subtitle: "Governed draft/validate/review/activate workflow for safe authority-input configuration changes.",
  },
  {
    path: "/audit",
    id: "audit",
    label: "Audit",
    eyebrow: "Historical Memory",
    subtitle: "Operator action logs, activity rollups, and trust/retrieval lineage.",
  },
  {
    path: "/reports",
    id: "reports",
    label: "Reports",
    eyebrow: "Immutable Snapshots",
    subtitle: "Presentation-quality snapshot surfaces rendered from governed report artifacts.",
  },
];

export const LEGACY_ROUTE_ALIASES = {
  "/control": "/operations",
  "/state": "/reports",
  "/submission": "/operations",
  "/lifecycle": "/audit",
};

const TARGET_SURFACE_ROUTE = {
  operations: "/operations",
  alerts: "/operations",
  reconciliation: "/reports",
  positions: "/portfolio",
  orders: "/portfolio",
  admin: "/audit",
  advisory: "/advisory",
  reports: "/reports",
  outcomes: "/outcomes",
};

function routeForId(routeId) {
  return ROUTES.find((route) => route.id === routeId);
}

function routeHrefFromSurface(targetSurface) {
  if (!targetSurface) {
    return "/";
  }
  if (targetSurface.startsWith("/")) {
    return targetSurface;
  }
  return TARGET_SURFACE_ROUTE[targetSurface] || "/";
}

function safeList(value) {
  return Array.isArray(value) ? value : [];
}

export function formatUsd(value) {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return "n/a";
  }
  return value.toLocaleString("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 2,
  });
}

function formatSignedUsd(value) {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return "n/a";
  }
  const sign = value > 0 ? "+" : "";
  return `${sign}${formatUsd(value)}`;
}

function toEvidenceRefs(paths = [], labelPrefix = "Artifact") {
  return safeList(paths)
    .filter((path) => typeof path === "string" && path.trim())
    .map((path, index) => ({
      label: `${labelPrefix} ${index + 1}`,
      path,
      artifact_type: "artifact_ref",
    }));
}

function topAlert(alertsPayload = {}) {
  return safeList(alertsPayload.alerts)[0] || null;
}

function renderHomePanelSection(homeBundle, semantics) {
  const homeView = homeBundle.home_view || {};
  return renderCardSection({
    eyebrow: "Home View",
    title: "Governed Home Panels",
    subtitle: "Rendered directly from the operator home contract and composition policy.",
    body: renderPanelRows(safeList(homeView.rendered_panels), semantics),
  });
}

function renderReadinessSnapshot(summary, workflow, semantics) {
  const readinessCards = [
    renderMetricCard({
      label: "Build",
      value: summary.build_status || "UNKNOWN",
      semantic: summary.build_status_semantic || "unknown",
      semantics,
    }),
    renderMetricCard({
      label: "Admission",
      value: summary.admission_status || "UNKNOWN",
      semantic: summary.admission_status_semantic || "unknown",
      semantics,
    }),
    renderMetricCard({
      label: "Boundary",
      value: summary.boundary_status || "UNKNOWN",
      semantic: summary.boundary_status_semantic || "unknown",
      semantics,
    }),
    renderMetricCard({
      label: "Control Plane",
      value: summary.control_plane_status || "UNKNOWN",
      semantic: summary.control_plane_status_semantic || "unknown",
      semantics,
    }),
    renderMetricCard({
      label: "Replay",
      value: summary.replay_status || "UNKNOWN",
      semantic: summary.replay_status_semantic || "unknown",
      semantics,
    }),
    renderMetricCard({
      label: "Workflow",
      value: workflow.workflow_state || "UNKNOWN",
      semantic: workflow.workflow_state === "ready" ? "healthy" : workflow.workflow_state === "attention_needed" ? "warning" : "blocked",
      semantics,
      detail: workflow.operator_priority ? `Priority ${workflow.operator_priority}` : "",
    }),
  ].join("");

  return renderCardSection({
    eyebrow: "Readiness",
    title: "Command Readiness Snapshot",
    subtitle: "Drillable readiness pillars from canonical system summary and workflow views.",
    body: `<div class="metric-grid">${readinessCards}</div>`,
  });
}

function renderAdvisorySummary(advisoryPayload = {}, semantics = {}) {
  const decisions = safeList(advisoryPayload.decisions).slice(0, 4);
  return renderCardSection({
    eyebrow: "Advisory",
    title: "Current Advisory Decision State",
    subtitle: "Governed advisory decisions rendered from certified truth only.",
    body: decisions.length
      ? renderList(decisions, {
          renderItem: (item) => `
            <article class="stack-card">
              <div class="stack-card-header">
                <div>
                  <div class="stack-card-title">${escapeHtml(item.advisory_item_id || item.advisory_surface_label || "Advisory decision")}</div>
                  <div class="stack-card-subtitle">${escapeHtml(item.decision_state || "decision")}</div>
                </div>
                ${renderSemanticBadge(item.actionability_state || "unknown", item.actionability_state === "promotion_eligible" || item.actionability_state === "actionable" ? "healthy" : item.actionability_state === "blocked" ? "blocked" : "warning", semantics)}
              </div>
              <div class="chip-list">
                <span class="support-chip">${escapeHtml(item.freshness_state || "unknown")}</span>
                <span class="support-chip">${escapeHtml(item.promotion_eligibility_state || "unknown")}</span>
              </div>
            </article>
          `,
        })
      : `<div class="empty-state">No advisory decisions were returned.</div>`,
  });
}

function renderOperatorQueryPanel(state) {
  const result = state.commandQueryResult;
  const resultMarkup = result
    ? renderCardSection({
        eyebrow: "Query Response",
        title: result.query_response?.query_class_id || "Operator Query",
        subtitle: `Status ${result.query_response?.response_status || "UNKNOWN"}`,
        body: `
          <div class="stack-list">
            ${safeList(result.query_response?.answer_blocks).map((block) => `
              <article class="stack-card">
                <div class="stack-card-title">${escapeHtml(block.title || block.block_id || "Answer block")}</div>
                <div class="line-list">
                  ${safeList(block.lines).map((line) => `<div>${escapeHtml(line)}</div>`).join("")}
                </div>
                ${safeList(block.source_refs).length ? `<div class="chip-list">${safeList(block.source_refs).map((ref) => `<button class="support-chip chip-button" type="button" data-artifact-path="${escapeHtml(ref)}" data-artifact-title="${escapeHtml(block.title || "Answer source")}">Open source</button>`).join("")}</div>` : ""}
              </article>
            `).join("")}
          </div>
        `,
      })
    : "";

  return `
    ${renderCardSection({
      eyebrow: "Ask Aegis",
      title: "Governed Operator Query",
      subtitle: "Query responses are built by the backend operator control plane and carry trust/retrieval manifests.",
      body: `
        <form class="operator-query-form">
          <label class="query-field">
            <span>Question</span>
            <input id="commandQueryInput" name="query_text" type="text" value="${escapeHtml(state.commandQueryText || "")}" placeholder="Why is readiness blocked today?" />
          </label>
          <button class="primary-button" type="submit">Run Query</button>
        </form>
      `,
    })}
    ${resultMarkup}
  `;
}

function renderAttentionSection(workflow = {}, semantics = {}) {
  return renderCardSection({
    eyebrow: "Attention",
    title: "What Needs Attention",
    subtitle: "Operator next steps from the existing workflow summary, not ad hoc UI heuristics.",
    body: safeList(workflow.next_steps).length
      ? renderList(
          safeList(workflow.next_steps).map((step) => ({
            ...step,
            target_surface: routeHrefFromSurface(step.target_surface),
          })),
          { renderItem: (step) => renderWorkflowStep(step, semantics) },
        )
      : `<div class="empty-state">No operator next steps were returned.</div>`,
  });
}

function renderAlertSection(alertsPayload = {}, semantics = {}) {
  const alerts = safeList(alertsPayload.alerts).slice(0, 5);
  return renderCardSection({
    eyebrow: "Alerts",
    title: "Current Alert Load",
    subtitle: "Alert state is backend-authored and traceable to its source artifacts.",
    body: alerts.length
      ? renderList(alerts, {
          renderItem: (alert) => `
            <article class="stack-card">
              <div class="stack-card-header">
                <div>
                  <div class="stack-card-title">${escapeHtml(alert.title || alert.entity_id || "Alert")}</div>
                  <div class="stack-card-subtitle">${escapeHtml(alert.summary || "No summary provided.")}</div>
                </div>
                ${renderSemanticBadge(alert.status || alert.severity || "UNKNOWN", alert.semantic || "warning", semantics)}
              </div>
              <div class="chip-list">
                ${safeList(alert.reason_codes).map((reasonCode) => `<span class="support-chip">${escapeHtml(reasonCode)}</span>`).join("")}
              </div>
            </article>
          `,
        })
      : `<div class="empty-state">No active alerts were returned.</div>`,
  });
}

async function renderCommandPage(state) {
  const financialState = await fetchFinancialState();
  const summary = state.shell.systemSummary || {};
  const readiness = summary.readiness_summary || {};
  const topLevel = safeList(summary.top_level_items);
  const compressed = safeList(summary.compressed_items);
  const secondary = safeList(summary.secondary_items);
  const drilldownOnly = safeList(summary.drilldown_only_items);
  const trustPreserved = safeList(summary.trust_preserved_items);
  const withheld = safeList(summary.withheld_items);
  const expiring = safeList(summary.expiring_items);
  const rollback = safeList(summary.rollback_items);
  const active = safeList(summary.active_policy_evolutions);

  const heroCards = `
    <div class="hero-band">
      <div class="hero-brand">
        ${renderAegisMark({ size: "lg", label: "Aegis Command" })}
        <div>
          <div class="section-eyebrow">Certified Policy Evolution Plane</div>
          <h2>Aegis Command</h2>
          <p>One governed temporal policy layer over refinement and value proof, with explicit proposals, expiry, rollback, and trust-preserving overrides instead of hidden adaptation.</p>
        </div>
      </div>
      <div class="metric-grid">
        ${renderMetricCard({
          label: "Top level",
          value: String(topLevel.length),
          badge: `<span class="support-chip">policy_evolution_state_v1</span>`,
        })}
        ${renderMetricCard({
          label: "Environment",
          value: summary.environment || "UNKNOWN",
          detail: summary.current_day ? `Day ${summary.current_day}` : "",
        })}
        ${renderMetricCard({
          label: "Active policy",
          value: String(active.length),
          detail: rollback.length ? `${rollback.length} rollback candidate(s)` : "No rollback candidate",
        })}
        ${renderMetricCard({
          label: "Trust preserved",
          value: String(trustPreserved.length),
          detail: withheld.length ? `${withheld.length} withheld evolution(s)` : "No withheld evolution",
        })}
        ${renderMetricCard({
          label: "Readiness",
          value: readiness.blocked ? readiness.blocked_state || "BLOCKED" : "READY",
          semantic: readiness.blocked ? "blocked" : "healthy",
          semantics: state.semantics,
        })}
        ${renderMetricCard({
          label: "Investable assets",
          value: formatUsd(financialState.investable_summary?.investable_assets_total_usd),
          detail: financialState.as_of_utc ? `As of ${formatTimestamp(financialState.as_of_utc)}` : "",
        })}
        ${renderMetricCard({
          label: "Re-review",
          value: String(expiring.length),
          detail: drilldownOnly.length ? `${drilldownOnly.length} drill-down only` : "No drill-down-only item",
        })}
      </div>
    </div>
  `;

  return {
    title: "Aegis Command",
    meta: "One governed policy-evolution summary over already-certified refinement, value, and product artifacts.",
    html: [
      heroCards,
      renderCardSection({
        eyebrow: "Top Level",
        title: "What Matters Now",
        subtitle: "Top-level visibility now comes from governed policy snapshots only; the shell does not adapt prominence locally.",
        body: renderSimpleTable({
          columns: [
            { key: "target_label", label: "Item" },
            { key: "proposed_policy_change", label: "Policy action", render: (row) => escapeHtml(row.proposed_policy_change?.action || "UNKNOWN") },
            { key: "summary_message", label: "Summary" },
            { key: "drill_down_route", label: "Drill-down" },
          ],
          rows: topLevel,
          emptyMessage: "No top-level policy rows were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Trust",
        title: "Trust-Preserved Visibility",
        subtitle: "Blocked, degraded, claim-strength, and insufficient-evidence distinctions stay visible when the policy-evolution plane says they must.",
        body: renderSimpleTable({
          columns: [
            { key: "target_label", label: "Item" },
            { key: "preserved_visibility_flags", label: "Protected", render: (row) => escapeHtml(safeList(row.preserved_visibility_flags).join(", ") || "None") },
            { key: "threshold_result", label: "Threshold" },
            { key: "summary_message", label: "Summary" },
          ],
          rows: trustPreserved,
          emptyMessage: "No trust-preserved policy rows were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Compressed",
        title: "Compressed Summaries",
        subtitle: "Compression proposals are governed and reversible; they cannot hide a trust-critical distinction.",
        body: renderSimpleTable({
          columns: [
            { key: "target_label", label: "Item" },
            { key: "evolution_strength", label: "Strength" },
            { key: "summary_message", label: "Summary" },
          ],
          rows: compressed,
          emptyMessage: "No compressed policy rows were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Secondary",
        title: "Demoted To Secondary",
        subtitle: "Secondary rows remain visible and drillable; demotion proposals are evidence-bound rather than route-local cleanup.",
        body: renderSimpleTable({
          columns: [
            { key: "target_label", label: "Item" },
            { key: "proposed_policy_change", label: "Policy action", render: (row) => escapeHtml(row.proposed_policy_change?.action || "UNKNOWN") },
            { key: "summary_message", label: "Summary" },
            { key: "drill_down_route", label: "Drill-down" },
          ],
          rows: secondary,
          emptyMessage: "No secondary policy rows were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Drill-Down",
        title: "Preserved Drill-Down Only",
        subtitle: "These items are no longer top-level, but governed drill-down access remains explicit, trust-preserving, and reversible.",
        body: renderSimpleTable({
          columns: [
            { key: "target_label", label: "Item" },
            { key: "proposed_policy_change", label: "Policy action", render: (row) => escapeHtml(row.proposed_policy_change?.action || "UNKNOWN") },
            { key: "summary_message", label: "Summary" },
            { key: "drill_down_route", label: "Drill-down" },
          ],
          rows: drilldownOnly,
          emptyMessage: "No drill-down-only policy rows were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Re-review",
        title: "Expiring Or Withheld Evolutions",
        subtitle: "Weak or stale history does not silently mutate product behavior; proposals are withheld or forced back to review explicitly.",
        body: renderSimpleTable({
          columns: [
            { key: "target_label", label: "Target" },
            { key: "threshold_result", label: "Threshold" },
            { key: "expiry_state", label: "Expiry" },
            { key: "summary_message", label: "Summary" },
          ],
          rows: [...expiring, ...withheld],
          emptyMessage: "No expiring or withheld policy rows were returned.",
        }),
      }),
    ].join(""),
    contextHtml: [
      renderCardSection({
        eyebrow: "Readiness",
        title: "Readiness Summary",
        subtitle: "The product summary preserves readiness state as a drillable governed field.",
        body: renderDefinitionRows([
          { label: "Blocked", value: readiness.blocked ? "yes" : "no" },
          { label: "Blocked state", value: readiness.blocked_state || "READY" },
          { label: "Operator status", value: readiness.operator_status || "UNKNOWN" },
        ]),
      }),
      renderSourceRefCard(safeList(summary.source_refs), "Policy Evidence", "Governed refs backing the current active policy-evolution decisions."),
      renderCardSection({
        eyebrow: "Policy Proof",
        title: "Why These Policy Decisions Were Made",
        subtitle: "Evidence windows, trust overrides, expiry, rollback, and before/after state stay explicit instead of living in UI-only temporal tuning.",
        body: renderSimpleTable({
          columns: [
            { key: "target_label", label: "Target" },
            { key: "proposed_policy_change", label: "Action", render: (row) => escapeHtml(row.proposed_policy_change?.action || "UNKNOWN") },
            { key: "threshold_result", label: "Threshold" },
            { key: "expiry_state", label: "Expiry" },
            { key: "reversibility_state", label: "Reversible" },
          ],
          rows: safeList(summary.proof_rows),
          emptyMessage: "No policy proof rows were returned.",
        }),
      }),
    ].join(""),
  };
}

async function renderPortfolioPage(state) {
  const financialState = await fetchFinancialState();
  const accounts = safeList(financialState.account_rollups);
  const holdings = safeList(financialState.holdings_rollup?.items);
  const concentrations = safeList(financialState.concentration_summary?.top_symbol_exposures);
  return {
    title: "Portfolio",
    meta: "Canonical investable totals, account rollups, liquidity, and exposure summaries from the backend financial-state projection.",
    html: [
      renderCardSection({
        eyebrow: "Portfolio",
        title: "Portfolio Surface",
        subtitle: "Thin rendering over the canonical backend financial-state authority.",
        body: `<div class="metric-grid">
          ${renderMetricCard({ label: "Investable assets", value: formatUsd(financialState.investable_summary?.investable_assets_total_usd), detail: `As of ${formatTimestamp(financialState.as_of_utc)}` })}
          ${renderMetricCard({ label: "Cash", value: formatUsd(financialState.liquidity_summary?.cash_total_usd) })}
          ${renderMetricCard({ label: "Gross positions", value: formatUsd(financialState.investable_summary?.gross_positions_value_usd) })}
          ${renderMetricCard({ label: "Truth state", value: financialState.truth_state || "UNKNOWN" })}
        </div>`,
      }),
      renderSimpleTable({
        columns: [
          { key: "full_account_number", label: "Account" },
          { key: "base_currency", label: "Currency" },
          { key: "cash_usd", label: "Cash", render: (row) => escapeHtml(formatUsd(row.cash_usd)) },
          { key: "buying_power_usd", label: "Buying Power", render: (row) => escapeHtml(formatUsd(row.buying_power_usd)) },
          {
            key: "restrictions",
            label: "Restrictions",
            render: (row) => `
              <div class="chip-list">
                <span class="support-chip">${escapeHtml(row.restrictions?.submission_enabled === false ? "submission_blocked" : "submission_enabled")}</span>
                ${safeList(row.restrictions?.allowed_sleeve_ids).map((item) => `<span class="support-chip">${escapeHtml(item)}</span>`).join("")}
              </div>
            `,
          },
        ],
        rows: accounts,
        emptyMessage: "No accounts were returned by the financial-state projection.",
      }),
      renderCardSection({
        eyebrow: "Holdings",
        title: "Holdings Rollup",
        subtitle: "Holdings are rendered from backend-owned accounting and positions evidence, not recomputed from frontend joins.",
        body: renderSimpleTable({
          columns: [
            { key: "symbol", label: "Symbol" },
            { key: "kind", label: "Kind" },
            { key: "quantity", label: "Quantity" },
            { key: "market_value_usd", label: "Market Value", render: (row) => escapeHtml(formatUsd(row.market_value_usd)) },
            { key: "mark_source", label: "Mark Source" },
          ],
          rows: holdings,
          emptyMessage: "No holdings were returned by the financial-state projection.",
        }),
      }),
      renderCardSection({
        eyebrow: "Exposure",
        title: "Concentration Summary",
        subtitle: "Top symbol exposure rows come from the backend exposure authority referenced by financial-state.",
        body: renderSimpleTable({
          columns: [
            { key: "symbol", label: "Symbol" },
            { key: "gross_notional_usd", label: "Gross Notional", render: (row) => escapeHtml(formatUsd(row.gross_notional_usd)) },
            { key: "net_notional_usd", label: "Net Notional", render: (row) => escapeHtml(formatUsd(row.net_notional_usd)) },
            { key: "capital_at_risk_usd", label: "Capital At Risk", render: (row) => escapeHtml(formatUsd(row.capital_at_risk_usd)) },
            { key: "sector", label: "Sector" },
          ],
          rows: concentrations,
          emptyMessage: "No concentration rows were returned by the financial-state projection.",
        }),
      }),
    ].join(""),
    contextHtml: [
      renderSourceRefCard(safeList(financialState.source_refs), "Financial Evidence", "Source refs declared by the financial-state projection."),
      renderCardSection({
        eyebrow: "Liquidity",
        title: "Liquidity Summary",
        subtitle: "Cash and buying-power facts remain backend-authored and explicitly degraded when upstream inputs are incomplete.",
        body: renderDefinitionRows([
          { label: "Cash total", value: formatUsd(financialState.liquidity_summary?.cash_total_usd) },
          { label: "Available funds", value: formatUsd(financialState.liquidity_summary?.available_funds_usd) },
          { label: "Net liquidation value", value: formatUsd(financialState.liquidity_summary?.net_liquidation_value_usd) },
          { label: "Reserve status", value: financialState.reserve_summary?.status || "UNKNOWN" },
        ]),
      }),
      renderCardSection({
        eyebrow: "Warnings",
        title: "Financial Warnings",
        subtitle: "Explicit degraded-state markers surfaced by the backend authority.",
        body: safeList(financialState.financial_warnings).length
          ? `<div class="chip-list">${safeList(financialState.financial_warnings).map((warning) => `<span class="support-chip">${escapeHtml(warning)}</span>`).join("")}</div>`
          : `<div class="empty-state">No financial warnings were returned.</div>`,
      }),
    ].join(""),
  };
}

async function renderCapitalOverviewPage() {
  const payload = await fetchCapitalOverview();
  const overview = payload.overview || {};
  const basis = payload.basis || {};
  const reportBasis = payload.report_basis_metadata || overview.report_basis_metadata || {};
  const freshness = payload.freshness_completeness || overview.freshness_completeness || {};
  const controlRows = safeList(overview.allocation_by_control?.rows);
  const bucketRows = safeList(overview.allocation_by_bucket?.rows);
  const matrixRows = safeList(overview.bucket_control_matrix?.rows);
  const investableContributors = safeList(overview.investable_explainability?.contributors);
  const findings = safeList(payload.validation_findings);
  const topFindings = findings.slice(0, 6);
  return {
    title: "Capital Overview",
    meta: "Household capital allocation overview sourced from Capital bounded-domain derived surfaces.",
    html: [
      renderCardSection({
        eyebrow: "Capital",
        title: "Investable Capital Snapshot",
        subtitle: "All headline numbers use latest-per-account basis with explicit include/exclude counts.",
        body: `<div class="metric-grid">
          ${renderMetricCard({ label: "Investable total", value: formatUsd(overview.investable_total), detail: `As of ${escapeHtml(overview.as_of_date || "n/a")}` })}
          ${renderMetricCard({ label: "Advisor-controlled", value: formatUsd(overview.advisor_controlled_capital) })}
          ${renderMetricCard({ label: "Aegis-controlled", value: formatUsd(overview.aegis_controlled_capital) })}
          ${renderMetricCard({ label: "Passive-controlled", value: formatUsd(overview.passive_controlled_capital) })}
          ${renderMetricCard({ label: "Included accounts", value: String(overview.included_account_count ?? "0") })}
          ${renderMetricCard({ label: "Excluded accounts", value: String(overview.excluded_account_count ?? "0") })}
          ${renderMetricCard({ label: "Freshness", value: freshness.status || reportBasis.freshness_status || "UNKNOWN" })}
          ${renderMetricCard({ label: "Stale included accounts", value: String(reportBasis.stale_account_count ?? freshness.stale_account_count ?? 0) })}
        </div>`,
      }),
      renderCardSection({
        eyebrow: "Explainability",
        title: "Investable Total Contributors",
        subtitle: "Backend-derived contributor balances used in the investable denominator.",
        body: renderSimpleTable({
          columns: [
            { key: "account_id", label: "Account ID" },
            { key: "account_name", label: "Account" },
            { key: "balance", label: "Balance", render: (row) => escapeHtml(formatUsd(row.balance)) },
            { key: "control_type", label: "Control" },
            { key: "bucket_type", label: "Bucket" },
          ],
          rows: investableContributors,
          emptyMessage: "No contributor rows were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Allocation",
        title: "Allocation By Control",
        subtitle: "Numerator and denominator are explicit and backend-computed.",
        body: renderSimpleTable({
          columns: [
            { key: "control_type", label: "Control" },
            { key: "balance_total", label: "Balance", render: (row) => escapeHtml(formatUsd(row.balance_total)) },
            { key: "allocation_pct", label: "Pct", render: (row) => escapeHtml(formatPercent(row.allocation_pct)) },
            {
              key: "contributors",
              label: "Contributors",
              render: (row) =>
                escapeHtml(
                  safeList(row.contributors)
                    .map((item) => `${item.account_id}:${formatUsd(item.balance)}`)
                    .join(", ") || "none",
                ),
            },
          ],
          rows: controlRows,
          emptyMessage: "No control allocation rows were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Allocation",
        title: "Allocation By Bucket",
        subtitle: "Included-only denominator; excluded balances are reported separately.",
        body: renderSimpleTable({
          columns: [
            { key: "bucket_type", label: "Bucket" },
            { key: "balance_total", label: "Balance", render: (row) => escapeHtml(formatUsd(row.balance_total)) },
            { key: "allocation_pct", label: "Pct", render: (row) => escapeHtml(formatPercent(row.allocation_pct)) },
            {
              key: "contributors",
              label: "Contributors",
              render: (row) =>
                escapeHtml(
                  safeList(row.contributors)
                    .map((item) => `${item.account_id}:${formatUsd(item.balance)}`)
                    .join(", ") || "none",
                ),
            },
          ],
          rows: bucketRows,
          emptyMessage: "No bucket allocation rows were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Matrix",
        title: "Bucket x Control Matrix",
        subtitle: "Cross-tab of included balances by semantic bucket and control ownership.",
        body: renderSimpleTable({
          columns: [
            { key: "bucket_type", label: "Bucket" },
            { key: "control_type", label: "Control" },
            { key: "balance_total", label: "Balance", render: (row) => escapeHtml(formatUsd(row.balance_total)) },
            { key: "allocation_pct", label: "Pct", render: (row) => escapeHtml(formatPercent(row.allocation_pct)) },
          ],
          rows: matrixRows,
          emptyMessage: "No matrix rows were returned.",
        }),
      }),
    ].join(""),
    contextHtml: [
      renderCardSection({
        eyebrow: "Basis",
        title: "Data Basis",
        subtitle: "Discloses as-of, include/exclude count, and validation state for operator trust.",
        body: renderDefinitionRows([
          { label: "As-of date", value: basis.as_of_date || overview.as_of_date || "n/a" },
          { label: "Basis type", value: reportBasis.report_basis || basis.basis_type || "latest_per_account" },
          { label: "Basis description", value: reportBasis.basis_description || "n/a" },
          { label: "Included account count", value: String(reportBasis.included_account_count ?? basis.included_account_count ?? overview.included_account_count ?? 0) },
          { label: "Excluded account count", value: String(reportBasis.excluded_account_count ?? basis.excluded_account_count ?? overview.excluded_account_count ?? 0) },
          { label: "Stale account count", value: String(reportBasis.stale_account_count ?? 0) },
          { label: "Freshness status", value: reportBasis.freshness_status || freshness.status || "UNKNOWN" },
          { label: "Validation status", value: reportBasis.validation_status || payload.validation_status || "UNKNOWN" },
          { label: "Validation findings", value: String(payload.validation_error_count ?? 0) },
        ]),
      }),
      renderCardSection({
        eyebrow: "Validation",
        title: "Active Validation Findings",
        subtitle: "Non-empty finding set indicates degraded data quality but remains transparent to operators.",
        body: topFindings.length
          ? renderSimpleTable({
              columns: [
                { key: "severity", label: "Severity" },
                { key: "code", label: "Code" },
                { key: "message", label: "Message" },
                { key: "account_ids", label: "Accounts", render: (row) => escapeHtml(safeList(row.account_ids).join(", ") || "none") },
              ],
              rows: topFindings,
              emptyMessage: "No validation findings were returned.",
            })
          : `<div class="empty-state">No active validation findings.</div>`,
      }),
      renderSourceRefCard(safeList(payload.source_refs), "Capital Source Evidence", "Capital bounded-domain storage and derived projection references."),
    ].join(""),
  };
}

async function renderCapitalAccountsPage() {
  const payload = await fetchCapitalAccounts();
  const rows = safeList(payload.rows);
  const auditRows = safeList(payload.recent_audit_entries).slice(0, 12);
  const basis = payload.basis || {};
  const freshness = payload.freshness_completeness || {};
  return {
    title: "Capital Accounts",
    meta: "Account registry with latest balances and current effective-dated classifications.",
    html: [
      renderCardSection({
        eyebrow: "Accounts",
        title: "Account Registry",
        subtitle: "Each row carries latest balance + current semantic classification + include/exclude state.",
        body: renderSimpleTable({
          columns: [
            { key: "account_name", label: "Account" },
            { key: "latest_balance", label: "Latest Balance", render: (row) => escapeHtml(formatUsd(row.latest_balance)) },
            { key: "as_of_date", label: "As-of" },
            { key: "capital_type", label: "Capital Type" },
            { key: "control_type", label: "Control" },
            { key: "bucket_type", label: "Bucket" },
            { key: "include_in_allocation", label: "Included", render: (row) => escapeHtml(row.include_in_allocation ? "yes" : "no") },
            { key: "confidence_level", label: "Confidence", render: (row) => escapeHtml(typeof row.confidence_level === "number" ? row.confidence_level.toFixed(2) : "n/a") },
            { key: "confidence_band", label: "Confidence Band" },
            { key: "notes", label: "Notes" },
          ],
          rows,
          emptyMessage: "No Capital account rows were returned.",
        }),
      }),
    ].join(""),
    contextHtml: [
      renderCardSection({
        eyebrow: "Basis",
        title: "Latest Balance Basis",
        subtitle: "Rows are latest-per-account and do not silently aggregate missing balance/classification rows.",
        body: renderDefinitionRows([
          { label: "As-of date", value: basis.as_of_date || "n/a" },
          { label: "Basis type", value: basis.basis_type || "latest_per_account" },
          { label: "Returned rows", value: String(basis.row_count ?? rows.length) },
          { label: "Freshness status", value: freshness.status || "UNKNOWN" },
          { label: "Stale account count", value: String(freshness.stale_account_count ?? 0) },
        ]),
      }),
      renderCardSection({
        eyebrow: "Audit",
        title: "Recent Capital Audit Trail",
        subtitle: "Meaningful domain changes are captured as append-only audit entries.",
        body: renderSimpleTable({
          columns: [
            { key: "created_at", label: "When", render: (row) => formatTimestamp(row.created_at) },
            { key: "entity_type", label: "Entity Type" },
            { key: "entity_id", label: "Entity ID" },
            { key: "action", label: "Action" },
            { key: "reason", label: "Reason" },
          ],
          rows: auditRows,
          emptyMessage: "No audit entries were returned.",
        }),
      }),
      renderSourceRefCard(safeList(payload.source_refs), "Capital Source Evidence", "Capital bounded-domain storage and derived projection references."),
    ].join(""),
  };
}

async function renderCapitalAllocationPage() {
  const payload = await fetchCapitalAllocation();
  const controlRows = safeList(payload.allocation_by_control?.rows);
  const bucketRows = safeList(payload.allocation_by_bucket?.rows);
  const matrixRows = safeList(payload.bucket_control_matrix?.rows);
  const includeExclude = payload.included_excluded_summary || {};
  const basis = payload.basis || {};
  const reportBasis = payload.report_basis_metadata || payload.allocation_by_control?.report_basis_metadata || {};
  return {
    title: "Capital Allocation",
    meta: "Allocation by control and bucket with explicit numerator/denominator and included vs excluded disclosures.",
    html: [
      renderCardSection({
        eyebrow: "Included vs Excluded",
        title: "Allocation Basis Disclosure",
        subtitle: "Included balances drive allocation percentages; excluded balances remain visible and traceable.",
        body: `<div class="metric-grid">
          ${renderMetricCard({ label: "Included total", value: formatUsd(includeExclude.included_total) })}
          ${renderMetricCard({ label: "Excluded total", value: formatUsd(includeExclude.excluded_total) })}
          ${renderMetricCard({ label: "Included accounts", value: String(includeExclude.included_account_count ?? 0) })}
          ${renderMetricCard({ label: "Excluded accounts", value: String(includeExclude.excluded_account_count ?? 0) })}
        </div>`,
      }),
      renderCardSection({
        eyebrow: "Control",
        title: "Allocation By Control",
        subtitle: "Numerator = included balance by control; denominator = total included balance.",
        body: renderSimpleTable({
          columns: [
            { key: "control_type", label: "Control" },
            { key: "balance_total", label: "Balance", render: (row) => escapeHtml(formatUsd(row.balance_total)) },
            { key: "allocation_pct", label: "Pct", render: (row) => escapeHtml(formatPercent(row.allocation_pct)) },
            {
              key: "contributors",
              label: "Contributors",
              render: (row) =>
                escapeHtml(
                  safeList(row.contributors)
                    .map((item) => `${item.account_id}:${formatUsd(item.balance)}`)
                    .join(", ") || "none",
                ),
            },
          ],
          rows: controlRows,
          emptyMessage: "No control rows were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Bucket",
        title: "Allocation By Bucket",
        subtitle: "Bucket-level allocation is derived by backend service, not frontend chart math.",
        body: renderSimpleTable({
          columns: [
            { key: "bucket_type", label: "Bucket" },
            { key: "balance_total", label: "Balance", render: (row) => escapeHtml(formatUsd(row.balance_total)) },
            { key: "allocation_pct", label: "Pct", render: (row) => escapeHtml(formatPercent(row.allocation_pct)) },
            {
              key: "contributors",
              label: "Contributors",
              render: (row) =>
                escapeHtml(
                  safeList(row.contributors)
                    .map((item) => `${item.account_id}:${formatUsd(item.balance)}`)
                    .join(", ") || "none",
                ),
            },
          ],
          rows: bucketRows,
          emptyMessage: "No bucket rows were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Matrix",
        title: "Bucket x Control Matrix",
        subtitle: "Cross-matrix keeps numerators traceable to contributing account IDs.",
        body: renderSimpleTable({
          columns: [
            { key: "bucket_type", label: "Bucket" },
            { key: "control_type", label: "Control" },
            { key: "balance_total", label: "Balance", render: (row) => escapeHtml(formatUsd(row.balance_total)) },
            { key: "allocation_pct", label: "Pct", render: (row) => escapeHtml(formatPercent(row.allocation_pct)) },
            {
              key: "contributors",
              label: "Contributors",
              render: (row) =>
                escapeHtml(
                  safeList(row.contributors)
                    .map((item) => `${item.account_id}:${formatUsd(item.balance)}`)
                    .join(", ") || "none",
                ),
            },
          ],
          rows: matrixRows,
          emptyMessage: "No matrix rows were returned.",
        }),
      }),
    ].join(""),
    contextHtml: [
      renderCardSection({
        eyebrow: "Basis",
        title: "Allocation Basis and Denominator",
        subtitle: "Percentages are explainable and include account-count disclosures.",
        body: renderDefinitionRows([
          { label: "As-of date", value: reportBasis.as_of_date || basis.as_of_date || "n/a" },
          { label: "Basis type", value: reportBasis.report_basis || basis.basis_type || "latest_per_account" },
          { label: "Basis description", value: reportBasis.basis_description || "n/a" },
          { label: "Numerator", value: basis.numerator || "included_balance_by_dimension" },
          { label: "Denominator", value: basis.denominator || "total_included_balance" },
          { label: "Included account count", value: String(reportBasis.included_account_count ?? basis.included_account_count ?? 0) },
          { label: "Excluded account count", value: String(reportBasis.excluded_account_count ?? basis.excluded_account_count ?? 0) },
          { label: "Stale account count", value: String(reportBasis.stale_account_count ?? basis.stale_account_count ?? 0) },
          { label: "Freshness status", value: reportBasis.freshness_status || basis.freshness_status || "UNKNOWN" },
          { label: "Validation status", value: reportBasis.validation_status || basis.validation_status || "UNKNOWN" },
        ]),
      }),
      renderSourceRefCard(safeList(payload.source_refs), "Capital Source Evidence", "Capital bounded-domain storage and derived projection references."),
    ].join(""),
  };
}

async function renderCapitalHistoryPage() {
  const payload = await fetchCapitalHistory();
  const investablePoints = safeList(payload.investable_time_series?.points);
  const aegisPoints = safeList(payload.aegis_allocation_pct_time_series?.points);
  const basis = payload.basis || {};
  return {
    title: "Capital History",
    meta: "Investable total and Aegis allocation percentage over time from append-only balance snapshots.",
    html: [
      renderCardSection({
        eyebrow: "History",
        title: "Investable Capital Over Time",
        subtitle: "Latest-per-account-on-or-before-day basis; each point remains traceable to source account snapshots.",
        body: renderSimpleTable({
          columns: [
            { key: "day", label: "Day" },
            { key: "investable_total", label: "Investable Total", render: (row) => escapeHtml(formatUsd(row.investable_total)) },
            { key: "included_account_count", label: "Included Accounts" },
          ],
          rows: investablePoints,
          emptyMessage: "No investable history points were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "History",
        title: "Aegis Allocation Percentage Over Time",
        subtitle: "Aegis numerator and investable denominator are both displayed for explainability.",
        body: renderSimpleTable({
          columns: [
            { key: "day", label: "Day" },
            { key: "aegis_balance", label: "Aegis Balance", render: (row) => escapeHtml(formatUsd(row.aegis_balance)) },
            { key: "investable_total", label: "Investable Total", render: (row) => escapeHtml(formatUsd(row.investable_total)) },
            { key: "aegis_allocation_pct", label: "Aegis Pct", render: (row) => escapeHtml(formatPercent(row.aegis_allocation_pct)) },
          ],
          rows: aegisPoints,
          emptyMessage: "No Aegis-allocation history points were returned.",
        }),
      }),
    ].join(""),
    contextHtml: [
      renderCardSection({
        eyebrow: "Basis",
        title: "History Basis",
        subtitle: "Time series uses append-only snapshots; no hand-maintained totals are stored.",
        body: renderDefinitionRows([
          { label: "As-of date", value: basis.as_of_date || "n/a" },
          { label: "Basis type", value: basis.basis_type || "latest_per_account_on_or_before_day" },
          { label: "Day count", value: String(basis.day_count ?? investablePoints.length) },
        ]),
      }),
      renderSourceRefCard(safeList(payload.source_refs), "Capital Source Evidence", "Capital bounded-domain storage and derived projection references."),
    ].join(""),
  };
}

async function renderCapitalFlowsPage() {
  const payload = await fetchCapitalFlows();
  const rows = safeList(payload.rows);
  const periodRows = safeList(payload.summary_by_period?.rows);
  const basis = payload.basis || {};
  return {
    title: "Capital Flows",
    meta: "Append-only external contribution/withdrawal records and period summaries.",
    html: [
      renderCardSection({
        eyebrow: "Flows",
        title: "Flow Records",
        subtitle: "Flows are append-only and distinct from balance snapshots.",
        body: renderSimpleTable({
          columns: [
            { key: "flow_date", label: "Flow Date" },
            { key: "account_id", label: "Account ID" },
            { key: "flow_type", label: "Type" },
            { key: "flow_amount", label: "Amount", render: (row) => escapeHtml(formatUsd(row.flow_amount)) },
            { key: "notes", label: "Notes" },
            { key: "created_at", label: "Recorded At", render: (row) => formatTimestamp(row.created_at) },
          ],
          rows,
          emptyMessage: "No flow rows were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Flows",
        title: "Flow Summary By Month",
        subtitle: "Net flows and record counts by period are derived from append-only flow facts.",
        body: renderSimpleTable({
          columns: [
            { key: "period", label: "Period" },
            { key: "net_flow_amount", label: "Net Flow", render: (row) => escapeHtml(formatUsd(row.net_flow_amount)) },
            { key: "flow_count", label: "Flow Count" },
          ],
          rows: periodRows,
          emptyMessage: "No monthly flow summary rows were returned.",
        }),
      }),
    ].join(""),
    contextHtml: [
      renderCardSection({
        eyebrow: "Basis",
        title: "Flow Basis",
        subtitle: "Flow visibility is independent from allocation inclusion logic.",
        body: renderDefinitionRows([
          { label: "As-of date", value: basis.as_of_date || "n/a" },
          { label: "Basis type", value: basis.basis_type || "append_only_flows" },
          { label: "Row count", value: String(basis.row_count ?? rows.length) },
        ]),
      }),
      renderSourceRefCard(safeList(payload.source_refs), "Capital Source Evidence", "Capital bounded-domain storage and derived projection references."),
    ].join(""),
  };
}

async function renderCapitalCashflowPage() {
  const query = new URLSearchParams(window.location.search || "");
  const scenarioRaw = String(query.get("scenario") || "florida").toLowerCase();
  const scenario = ["base", "florida", "chile"].includes(scenarioRaw) ? scenarioRaw : "florida";
  const includeInheritance = ["1", "true", "yes", "on"].includes(String(query.get("include_inheritance") || "").toLowerCase());
  const payload = await fetchCapitalCashflow({
    scenario,
    include_inheritance: includeInheritance ? "true" : "false",
  });
  const rows = safeList(payload.monthly_projection);
  const validation = payload.validation || {};
  const findings = safeList(validation.findings);
  const basis = payload.basis || {};
  const firstNet = rows.length ? Number(rows[0].net) : null;
  const minNet = typeof payload.min_net === "number" ? payload.min_net : null;
  const statusLabel = payload.operator_status || (payload.status === "DEGRADED" ? "TIGHT" : payload.status || "UNKNOWN");
  const maxAbsNet = Math.max(
    1,
    ...rows.map((row) => Math.abs(Number(row.net || 0))),
  );
  const scenarioHref = (nextScenario) => {
    const search = new URLSearchParams();
    search.set("scenario", nextScenario);
    if (includeInheritance) {
      search.set("include_inheritance", "true");
    }
    return `/capital/cashflow?${search.toString()}`;
  };
  const includeToggleHref = () => {
    const search = new URLSearchParams();
    search.set("scenario", scenario);
    if (!includeInheritance) {
      search.set("include_inheritance", "true");
    }
    const suffix = search.toString();
    return `/capital/cashflow${suffix ? `?${suffix}` : ""}`;
  };
  return {
    title: "Capital Cashflow Timeline",
    meta: "Month-by-month deterministic cashflow survival projection with scenario controls and explainable basis.",
    html: [
      renderCardSection({
        eyebrow: "Scenario",
        title: "Cashflow Timeline Controls",
        subtitle: "Scenario toggles are explicit. Inheritance stays excluded by default.",
        body: `
          <div class="chip-list">
            <a class="support-chip chip-button" href="${escapeHtml(scenarioHref("florida"))}" data-route="${escapeHtml(scenarioHref("florida"))}">Florida</a>
            <a class="support-chip chip-button" href="${escapeHtml(scenarioHref("chile"))}" data-route="${escapeHtml(scenarioHref("chile"))}">Chile</a>
            <a class="support-chip chip-button" href="${escapeHtml(scenarioHref("base"))}" data-route="${escapeHtml(scenarioHref("base"))}">Base</a>
            <a class="support-chip chip-button" href="${escapeHtml(includeToggleHref())}" data-route="${escapeHtml(includeToggleHref())}">
              Include inheritance: ${includeInheritance ? "ON" : "OFF"}
            </a>
          </div>
        `,
      }),
      renderCardSection({
        eyebrow: "Projection",
        title: "Monthly Cashflow Snapshot",
        subtitle: "Deterministic-first projection answers whether monthly net turns negative.",
        body: `<div class="metric-grid">
          ${renderMetricCard({ label: "Scenario", value: scenario.toUpperCase() })}
          ${renderMetricCard({ label: "Monthly net (first month)", value: firstNet === null ? "n/a" : formatSignedUsd(firstNet) })}
          ${renderMetricCard({ label: "Lowest projected net", value: minNet === null ? "n/a" : formatSignedUsd(minNet) })}
          ${renderMetricCard({ label: "Status", value: statusLabel })}
        </div>`,
      }),
      renderCardSection({
        eyebrow: "Chart",
        title: "Monthly Net Cashflow",
        subtitle: "Bars are backend-derived net values by month (income minus expenses).",
        body: rows.length
          ? `<div class="stack-list">${rows.map((row) => {
              const net = Number(row.net || 0);
              const width = Math.max(2, Math.round((Math.abs(net) / maxAbsNet) * 100));
              const barColor = net < 0 ? "#b00020" : "#1f6f43";
              return `
                <article class="stack-card">
                  <div class="stack-card-header">
                    <div class="stack-card-title">${escapeHtml(String(row.month || "n/a"))}</div>
                    <div class="stack-card-subtitle">${escapeHtml(formatSignedUsd(net))}</div>
                  </div>
                  <div style="height:10px;border-radius:4px;background:${barColor};width:${width}%"></div>
                </article>
              `;
            }).join("")}</div>`
          : `<div class="empty-state">No projection rows were returned.</div>`,
      }),
      renderCardSection({
        eyebrow: "Projection",
        title: "Monthly Projection Table",
        subtitle: "Every month shows income, expenses, net, and cumulative path.",
        body: renderSimpleTable({
          columns: [
            { key: "month", label: "Month" },
            { key: "income", label: "Income", render: (row) => escapeHtml(formatUsd(Number(row.income || 0))) },
            { key: "expenses", label: "Expenses", render: (row) => escapeHtml(formatUsd(Number(row.expenses || 0))) },
            { key: "net", label: "Net", render: (row) => escapeHtml(formatSignedUsd(Number(row.net || 0))) },
            { key: "cumulative", label: "Cumulative", render: (row) => escapeHtml(formatSignedUsd(Number(row.cumulative || 0))) },
          ],
          rows,
          emptyMessage: "No monthly projection rows were returned.",
        }),
      }),
    ].join(""),
    contextHtml: [
      renderCardSection({
        eyebrow: "Basis",
        title: "Projection Basis",
        subtitle: "Deterministic basis and inheritance exclusion are explicit.",
        body: renderDefinitionRows([
          { label: "Projection view", value: basis.report_basis || "v_capital_cashflow_projection_v1" },
          { label: "Basis description", value: basis.basis_description || "n/a" },
          { label: "Deterministic only", value: basis.deterministic_only ? "true" : "false" },
          { label: "Inheritance excluded", value: basis.inheritance_excluded ? "true" : "false" },
          { label: "Scenario scope", value: safeList(basis.scenario_scope).join(", ") || "n/a" },
          { label: "Start month", value: basis.start_month || "n/a" },
          { label: "Horizon (months)", value: String(basis.horizon_months ?? "n/a") },
          { label: "Event count", value: String(basis.event_count ?? "0") },
        ]),
      }),
      renderCardSection({
        eyebrow: "Validation",
        title: "Cashflow Validation Findings",
        subtitle: "Projection validity and risk findings are explicit and machine-derived.",
        body: findings.length
          ? renderSimpleTable({
              columns: [
                { key: "severity", label: "Severity" },
                { key: "code", label: "Code" },
                { key: "message", label: "Message" },
              ],
              rows: findings,
              emptyMessage: "No cashflow findings were returned.",
            })
          : `<div class="empty-state">No cashflow validation findings.</div>`,
      }),
      renderSourceRefCard(safeList(payload.source_refs), "Capital Source Evidence", "Capital bounded-domain storage and derived projection references."),
    ].join(""),
  };
}

async function renderCapitalValidationPage() {
  const payload = await fetchCapitalValidation();
  const validation = payload.validation || {};
  const findings = safeList(validation.findings);
  const basis = payload.basis || {};
  return {
    title: "Capital Validation",
    meta: "Validation failures are explicit, severity-ranked, and account-traceable.",
    html: [
      renderCardSection({
        eyebrow: "Validation",
        title: "Validation Status",
        subtitle: "Data quality state is exposed directly instead of hidden in chart rendering.",
        body: `<div class="metric-grid">
          ${renderMetricCard({ label: "Status", value: validation.status || "UNKNOWN" })}
          ${renderMetricCard({ label: "Findings", value: String(validation.error_count ?? 0) })}
          ${renderMetricCard({ label: "Critical", value: String((validation.severity_counts || {}).CRITICAL || 0) })}
          ${renderMetricCard({ label: "Warnings", value: String((validation.severity_counts || {}).WARNING || 0) })}
        </div>`,
      }),
      renderCardSection({
        eyebrow: "Findings",
        title: "Current Validation Findings",
        subtitle: "Severity, codes, impacted accounts, and messages are all backend-derived.",
        body: renderSimpleTable({
          columns: [
            { key: "severity", label: "Severity" },
            { key: "code", label: "Code" },
            { key: "message", label: "Message" },
            { key: "account_ids", label: "Impacted Accounts", render: (row) => escapeHtml(safeList(row.account_ids).join(", ") || "none") },
          ],
          rows: findings,
          emptyMessage: "No active validation findings.",
        }),
      }),
    ].join(""),
    contextHtml: [
      renderCardSection({
        eyebrow: "Basis",
        title: "Validation Basis",
        subtitle: "Validation uses effective-dated semantic state and latest snapshot coverage checks.",
        body: renderDefinitionRows([
          { label: "As-of date", value: basis.as_of_date || validation.as_of_date || "n/a" },
          { label: "Basis type", value: basis.basis_type || "validation_projection" },
          { label: "Finding count", value: String(basis.finding_count ?? findings.length) },
        ]),
      }),
      renderSourceRefCard(safeList(payload.source_refs), "Capital Source Evidence", "Capital bounded-domain storage and derived projection references."),
    ].join(""),
  };
}

function formatPercent(value) {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return "n/a";
  }
  return `${(value * 100).toFixed(2)}%`;
}

async function renderSleevesPage() {
  const sleevesPayload = await fetchSleeves();
  const valuePayload = await fetchValue();
  const sleeves = safeList(sleevesPayload.sleeves);
  const evaluatedSleeves = sleeves.filter((row) => row.recommendation?.recommendation_state !== "unavailable").length;

  return {
    title: "Sleeves",
    meta: "Canonical sleeve evaluation state from backend-owned policy, allocation, measurement, and governance artifacts.",
    html: [
      renderCardSection({
        eyebrow: "Sleeves",
        title: "Sleeve Command",
        subtitle: "The domain now renders a backend sleeve-evaluation contract instead of a generic blocked page.",
        body: `<div class="metric-grid">
          ${renderMetricCard({ label: "Sleeves", value: String(sleevesPayload.sleeve_registry_summary?.total_sleeves ?? 0), detail: `As of ${formatTimestamp(sleevesPayload.as_of_utc)}` })}
          ${renderMetricCard({ label: "Evaluated", value: String(evaluatedSleeves) })}
          ${renderMetricCard({ label: "Truth state", value: sleevesPayload.truth_state || "UNKNOWN" })}
          ${renderMetricCard({ label: "Current evaluation day", value: sleevesPayload.current_day || "UNKNOWN" })}
        </div>`,
      }),
      renderSimpleTable({
        columns: [
          { key: "display_name", label: "Sleeve" },
          { key: "sleeve_id", label: "ID" },
          { key: "priority_rank", label: "Priority" },
          { key: "actual_allocation_pct", label: "Actual Allocation", render: (row) => escapeHtml(formatPercent(row.actual_allocation_pct)) },
          { key: "effective_risk_budget_usd", label: "Risk Budget", render: (row) => escapeHtml(formatUsd(row.effective_risk_budget_usd)) },
          { key: "qualification_state", label: "Qualification" },
          { key: "edge_band", label: "Edge Band" },
          { key: "recommendation", label: "Recommendation", render: (row) => escapeHtml(row.recommendation?.recommendation_state || "unavailable") },
        ],
        rows: sleeves,
        emptyMessage: "No sleeves were returned by the sleeve-evaluation projection.",
      }),
      renderCardSection({
        eyebrow: "Measurement Gaps",
        title: "Explicitly Blocked Fields",
        subtitle: "Target allocations, tax efficiency, realized-return metrics, and non-risk budgets stay unavailable until backend authorities materialize them.",
        body: renderDefinitionRows([
          { label: "Target allocation policy", value: "backend gap" },
          { label: "Tax efficiency summary", value: "backend gap" },
          { label: "Realized value metrics", value: "served by governed value summary" },
          { label: "Non-risk budget", value: "backend gap" },
        ]),
      }),
      renderCardSection({
        eyebrow: "Value",
        title: "Sleeve Value Summary",
        subtitle: "Sleeve usefulness is rendered from governed value artifacts only and weakens when sleeve linkage is only execution-scope-level.",
        body: renderSimpleTable({
          columns: [
            { key: "sleeve_id", label: "Sleeve" },
            { key: "linkage_states", label: "Linkage" },
            { key: "value_rows", label: "Value rows" },
            { key: "observed_fact_count", label: "Observed fact" },
            { key: "bounded_association_count", label: "Bounded association" },
            { key: "withheld_count", label: "Withheld" },
          ],
          rows: safeList(valuePayload.sleeve_contribution_summary),
          emptyMessage: "No governed sleeve value summary was returned.",
        }),
      }),
    ].join(""),
    contextHtml: [
      renderSourceRefCard(safeList(sleevesPayload.source_refs), "Sleeve Evidence", "Policy, allocation, measurement, and governance refs used by the sleeve-evaluation projection."),
      renderCardSection({
        eyebrow: "Warnings",
        title: "Sleeve Warnings",
        subtitle: "Explicit backend gaps and missing materializations remain visible instead of being papered over in the UI.",
        body: safeList(sleevesPayload.sleeve_warnings).length
          ? `<div class="chip-list">${safeList(sleevesPayload.sleeve_warnings).map((warning) => `<span class="support-chip">${escapeHtml(warning)}</span>`).join("")}</div>`
          : `<div class="empty-state">No sleeve warnings were returned.</div>`,
      }),
    ].join(""),
  };
}

async function renderTaxPage() {
  const taxState = await fetchTax();
  const profiles = safeList(taxState.account_tax_profiles);
  const lots = safeList(taxState.lot_level_entries?.items);
  const harvestCandidates = safeList(taxState.harvesting_candidates?.items);
  const advisoryImpacts = safeList(taxState.advisory_impacts);

  return {
    title: "Tax",
    meta: "Governed deterministic tax state derived from canonical runtime truth.",
    html: [
      renderCardSection({
        eyebrow: "Tax",
        title: "Tax Command Center",
        subtitle: "The tax page renders the governed tax state artifact only. Blockers, opportunities, and advisory tax impact stay explicit.",
        body: `<div class="metric-grid">
          ${renderMetricCard({ label: "Tax status", value: taxState.tax_status || "UNKNOWN", detail: `As of ${formatTimestamp(taxState.as_of_utc)}` })}
          ${renderMetricCard({ label: "Completeness", value: taxState.completeness_state || "UNKNOWN" })}
          ${renderMetricCard({ label: "Freshness", value: taxState.freshness_state || "UNKNOWN" })}
          ${renderMetricCard({ label: "Profiles", value: String(profiles.length) })}
          ${renderMetricCard({ label: "Lots", value: String(taxState.lot_level_entries?.total_lots ?? 0) })}
          ${renderMetricCard({ label: "Harvest candidates", value: String(taxState.harvesting_candidates?.candidate_count ?? 0) })}
        </div>`,
      }),
      renderCardSection({
        eyebrow: "Tax Summary",
        title: "Current Tax State",
        subtitle: "Completeness, freshness, blocker, and advisory-binding states come from the canonical tax artifact.",
        body: renderDefinitionRows([
          { label: "Lot basis state", value: taxState.lot_basis_state || "UNKNOWN" },
          { label: "Holding-period state", value: taxState.holding_period_state || "UNKNOWN" },
          { label: "Wash-sale state", value: taxState.wash_sale_state || "UNKNOWN" },
          { label: "Realized/unrealized posture", value: taxState.realized_unrealized_tax_posture?.status || "UNKNOWN" },
          { label: "Advisory effect", value: taxState.advisory_binding_state?.effect_state || "UNKNOWN" },
          { label: "Primary rule", value: taxState.primary_rule_id || "UNKNOWN" },
        ]),
      }),
      renderSimpleTable({
        columns: [
          { key: "account_id", label: "Account" },
          { key: "environment", label: "Environment" },
          { key: "tax_profile_state", label: "Tax Profile" },
          { key: "lot_basis_state", label: "Lot Basis" },
          { key: "holding_period_state", label: "Holding Period" },
        ],
        rows: profiles,
        emptyMessage: "No account-level tax profiles were returned.",
      }),
      renderCardSection({
        eyebrow: "Tax Signals",
        title: "Current Blockers and Opportunities",
        subtitle: "Opportunity visibility is governed by the tax precedence matrix and never inferred locally.",
        body: renderDefinitionRows([
          { label: "Blockers", value: safeList(taxState.blocker_states).join(", ") || "None" },
          { label: "Opportunities", value: safeList(taxState.opportunity_states).join(", ") || "None" },
          { label: "Lot entries", value: `${taxState.lot_level_entries?.status || "UNKNOWN"} (${lots.length})` },
          { label: "Harvest candidates", value: `${taxState.harvesting_candidates?.status || "UNKNOWN"} (${harvestCandidates.length})` },
        ]),
      }),
      renderSimpleTable({
        columns: [
          { key: "lot_id", label: "Lot" },
          { key: "security_id", label: "Security" },
          { key: "account_id", label: "Account" },
          { key: "holding_period_state", label: "Holding Period" },
          { key: "basis_total", label: "Basis" },
        ],
        rows: lots,
        emptyMessage: "No governed tax lots were returned.",
      }),
      renderSimpleTable({
        columns: [
          { key: "lot_id", label: "Lot" },
          { key: "security_id", label: "Security" },
          { key: "account_id", label: "Account" },
          { key: "unrealized_loss_amount", label: "Unrealized Loss" },
          { key: "policy_eligibility_result", label: "Eligibility" },
        ],
        rows: harvestCandidates,
        emptyMessage: "No governed harvest candidates were returned.",
      }),
      renderSimpleTable({
        columns: [
          { key: "advisory_item_id", label: "Advisory item" },
          { key: "decision_state", label: "Decision" },
          { key: "actionability_state", label: "Actionability" },
          { key: "freshness_state", label: "Freshness" },
        ],
        rows: advisoryImpacts,
        emptyMessage: "No advisory decisions currently bind to this tax state.",
      }),
    ].join(""),
    contextHtml: [
      renderSourceRefCard(safeList(taxState.source_refs), "Tax Evidence", "Current runtime/economic refs backing the tax-state projection."),
      renderCardSection({
        eyebrow: "Warnings",
        title: "Tax Warnings",
        subtitle: "The current tax rule and blocker set stay visible rather than being hidden behind a placeholder seam.",
        body: safeList(taxState.tax_warnings).length
          ? `<div class="chip-list">${safeList(taxState.tax_warnings).map((warning) => `<span class="support-chip">${escapeHtml(warning)}</span>`).join("")}</div>`
          : `<div class="empty-state">No tax warnings were returned.</div>`,
      }),
    ].join(""),
  };
}

async function renderOpportunitiesPage() {
  const opportunities = await fetchOpportunities();
  return {
    title: "Opportunities",
    meta: "Governed proactive opportunities and review deltas derived from certified truth.",
    html: [
      renderCardSection({
        eyebrow: "Opportunity Plane",
        title: "Top Opportunities Now",
        subtitle: "This page renders the governed Bundle 11 opportunity artifacts only.",
        body: renderSimpleTable({
          columns: [
            { key: "opportunity_type", label: "Type" },
            { key: "opportunity_state", label: "State" },
            { key: "review_priority", label: "Priority" },
            { key: "delta_state", label: "Delta" },
            { key: "summary_message", label: "Summary" },
          ],
          rows: safeList(opportunities.top_opportunities),
          emptyMessage: "No governed opportunities were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Review Delta",
        title: "Changed Since Last Review",
        subtitle: "Delta rows come from the governed review snapshot, not UI-local comparison logic.",
        body: renderSimpleTable({
          columns: [
            { key: "opportunity_type", label: "Type" },
            { key: "delta_state", label: "Delta" },
            { key: "review_priority", label: "Priority" },
            { key: "summary_message", label: "Summary" },
          ],
          rows: safeList(opportunities.changed_since_last_review),
          emptyMessage: "No changed opportunities were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Blocked Items",
        title: "Blocked But Important",
        subtitle: "Blocked opportunities remain visible when review-critical.",
        body: renderSimpleTable({
          columns: [
            { key: "opportunity_type", label: "Type" },
            { key: "review_priority", label: "Priority" },
            { key: "blocker_states", label: "Blockers", render: (row) => escapeHtml(safeList(row.blocker_states).join(", ") || "None") },
            { key: "summary_message", label: "Summary" },
          ],
          rows: safeList(opportunities.blocked_items),
          emptyMessage: "No blocked review-critical opportunities were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Scenario Binding",
        title: "Scenario Comparisons That Matter",
        subtitle: "Scenario significance is bounded by the governed opportunity kernel.",
        body: renderSimpleTable({
          columns: [
            { key: "opportunity_type", label: "Type" },
            { key: "scenario_significance_state", label: "Scenario significance" },
            { key: "review_priority", label: "Priority" },
            { key: "summary_message", label: "Summary" },
          ],
          rows: safeList(opportunities.scenario_items),
          emptyMessage: "No material scenario comparisons were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Advisory Effect",
        title: "Opportunity Impact On Advisory",
        subtitle: "Advisory decisions may bind to opportunity artifacts but do not rank them locally.",
        body: renderSimpleTable({
          columns: [
            { key: "advisory_item_id", label: "Advisory item" },
            { key: "decision_state", label: "Decision" },
            { key: "actionability_state", label: "Actionability" },
            { key: "summary_message", label: "Summary" },
          ],
          rows: safeList(opportunities.advisory_impacts),
          emptyMessage: "No advisory decisions currently bind to opportunity state.",
        }),
      }),
    ].join(""),
    contextHtml: [
      renderSourceRefCard(safeList(opportunities.source_refs), "Opportunity Evidence", "Governed opportunity-state source refs."),
      renderCardSection({
        eyebrow: "Snapshot",
        title: "Review Snapshot Summary",
        subtitle: "Summary counts come from the current governed review snapshot.",
        body: renderDefinitionRows([
          { label: "Current", value: String(opportunities.review_snapshot_summary?.current_count ?? 0) },
          { label: "New", value: String(opportunities.review_snapshot_summary?.new_count ?? 0) },
          { label: "Changed", value: String(opportunities.review_snapshot_summary?.changed_count ?? 0) },
          { label: "Resolved", value: String(opportunities.review_snapshot_summary?.resolved_count ?? 0) },
          { label: "Review now", value: String(opportunities.review_snapshot_summary?.review_now_count ?? 0) },
        ]),
      }),
    ].join(""),
  };
}

async function renderOutcomesPage() {
  const valueState = await fetchValue();
  const rows = safeList(valueState.value_rows);
  return {
    title: "Value",
    meta: "A governed proof surface over realized truth, sleeve linkage, bounded attribution, and explicit claim strength.",
    html: [
      renderCardSection({
        eyebrow: "Value Summary",
        title: "Current Value Proof",
        subtitle: "This surface renders governed value artifacts only and distinguishes observed fact from attribution.",
        body: `
          <div class="metric-grid">
            ${renderMetricCard({ label: "Outcomes", value: String(rows.length) })}
            ${renderMetricCard({ label: "Observed fact", value: String((valueState.claim_strength_counts || {}).observed_fact || 0) })}
            ${renderMetricCard({ label: "Bounded association", value: String((valueState.claim_strength_counts || {}).bounded_association || 0) })}
            ${renderMetricCard({ label: "Withheld", value: String(((valueState.claim_strength_counts || {}).insufficient_evidence || 0) + ((valueState.claim_strength_counts || {}).not_yet_observable || 0)) })}
          </div>
        `,
      }),
      renderCardSection({
        eyebrow: "Scorecard",
        title: "Recommendation Effectiveness Scorecard",
        subtitle: "Effectiveness is governed and does not imply attribution or sleeve usefulness automatically.",
        body: renderSimpleTable({
          columns: [
            { key: "opportunity_type", label: "Subject" },
            { key: "sleeve_id", label: "Execution Scope" },
            { key: "realized_state", label: "Realized" },
            { key: "effectiveness_state", label: "Effectiveness" },
            { key: "attribution_state", label: "Attribution" },
            { key: "claim_strength", label: "Claim strength" },
          ],
          rows,
          emptyMessage: "No governed outcome artifacts were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Sleeves",
        title: "Sleeve Contribution / Usefulness Summary",
        subtitle: "Sleeve claims remain conservative and show execution-scope-only linkage explicitly when stronger economic sleeve attribution is not proven.",
        body: renderSimpleTable({
          columns: [
            { key: "sleeve_id", label: "Sleeve" },
            { key: "linkage_states", label: "Linkage" },
            { key: "value_rows", label: "Value rows" },
            { key: "observed_fact_count", label: "Observed fact" },
            { key: "bounded_association_count", label: "Bounded association" },
            { key: "withheld_count", label: "Withheld" },
          ],
          rows: safeList(valueState.sleeve_contribution_summary),
          emptyMessage: "No sleeve value summary was returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Tax Effect",
        title: "Tax Effect Summary",
        subtitle: "Tax-related outcomes are shown only where governed tax refs are present.",
        body: renderSimpleTable({
          columns: [
            { key: "opportunity_type", label: "Subject" },
            { key: "realized_state", label: "Realized" },
            { key: "claim_strength", label: "Claim strength" },
            { key: "primary_rule_id", label: "Rule" },
          ],
          rows: safeList(valueState.tax_effect_summary),
          emptyMessage: "No tax-related outcome proof was returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Missed",
        title: "Missed Opportunity Summary",
        subtitle: "Missed-outcome rows appear only when the governed outcome model supports them.",
        body: renderSimpleTable({
          columns: [
            { key: "opportunity_type", label: "Subject" },
            { key: "effectiveness_state", label: "Effectiveness" },
            { key: "claim_strength", label: "Claim strength" },
          ],
          rows: safeList(valueState.missed_opportunity_summary),
          emptyMessage: "No governed missed-opportunity rows were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Progress",
        title: "Bounded Progress Summary",
        subtitle: "Progress is shown as governed counts, not freeform performance storytelling.",
        body: renderDefinitionRows([
          { label: "Observed fact", value: String((valueState.bounded_progress_summary || {}).observed_fact_count || 0) },
          { label: "Bounded association", value: String((valueState.bounded_progress_summary || {}).bounded_association_count || 0) },
          { label: "Withheld", value: String((valueState.bounded_progress_summary || {}).withheld_count || 0) },
        ]),
      }),
    ].join(""),
    contextHtml: [
      renderSourceRefCard(safeList(valueState.source_refs), "Value Evidence", "Governed value refs backing the current proof surface."),
    ].join(""),
  };
}

async function renderRefinementPage() {
  const refinement = await fetchRefinement();
  return {
    title: "Refinement",
    meta: "Governed refinement proof over top-level preservation, compression, demotion, and drill-down retention.",
    html: [
      renderCardSection({
        eyebrow: "Refinement",
        title: "Current Refinement Summary",
        subtitle: "This page renders governed refinement artifacts only and preserves before/after visibility provenance.",
        body: `<div class="metric-grid">
          ${renderMetricCard({ label: "Top level", value: String(safeList(refinement.top_level_items).length) })}
          ${renderMetricCard({ label: "Compressed", value: String(safeList(refinement.compressed_items).length) })}
          ${renderMetricCard({ label: "Secondary", value: String(safeList(refinement.secondary_items).length) })}
          ${renderMetricCard({ label: "Withheld", value: String(safeList(refinement.withheld_items).length) })}
        </div>`,
      }),
      renderCardSection({
        eyebrow: "Proof",
        title: "Refinement Decisions",
        subtitle: "Each row shows what changed, why it changed, and whether the change is reversible.",
        body: renderSimpleTable({
          columns: [
            { key: "target_label", label: "Target" },
            { key: "before_state", label: "Before", render: (row) => escapeHtml(row.before_state?.surface_bucket || "UNKNOWN") },
            { key: "after_state", label: "After", render: (row) => escapeHtml(row.after_state?.surface_bucket || "UNKNOWN") },
            { key: "refinement_action", label: "Action" },
            { key: "reversibility_state", label: "Reversible" },
            { key: "summary_message", label: "Summary" },
          ],
          rows: safeList(refinement.proof_rows),
          emptyMessage: "No refinement proof rows were returned.",
        }),
      }),
    ].join(""),
    contextHtml: [
      renderSourceRefCard(safeList(refinement.source_refs), "Refinement Evidence", "Governed refinement refs backing the current product simplification decisions."),
    ].join(""),
  };
}

async function renderPolicyPage() {
  const policy = await fetchPolicyEvolution();
  return {
    title: "Policy Evolution",
    meta: "Governed temporal policy proposals over refinement and product emphasis, with explicit expiry, rollback, and trust overrides.",
    html: [
      renderCardSection({
        eyebrow: "Policy Evolution",
        title: "Current Active Policy Evolutions",
        subtitle: "This page renders governed policy-evolution artifacts only and never derives temporal tuning locally.",
        body: `<div class="metric-grid">
          ${renderMetricCard({ label: "Active", value: String(safeList(policy.active_policy_evolutions).length) })}
          ${renderMetricCard({ label: "Withheld", value: String(safeList(policy.withheld_items).length) })}
          ${renderMetricCard({ label: "Expiring", value: String(safeList(policy.expiring_items).length) })}
          ${renderMetricCard({ label: "Rollback", value: String(safeList(policy.rollback_items).length) })}
        </div>`,
      }),
      renderCardSection({
        eyebrow: "Active",
        title: "Current Policy Actions",
        subtitle: "Policy proposals remain explicit snapshots rather than hidden product drift.",
        body: renderSimpleTable({
          columns: [
            { key: "target_label", label: "Target" },
            { key: "proposed_policy_change", label: "Action", render: (row) => escapeHtml(row.proposed_policy_change?.action || "UNKNOWN") },
            { key: "threshold_result", label: "Threshold" },
            { key: "summary_message", label: "Summary" },
          ],
          rows: safeList(policy.active_policy_evolutions),
          emptyMessage: "No active policy-evolution rows were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Withheld",
        title: "Withheld Evolutions",
        subtitle: "Insufficient or unstable history results in explicit withholding instead of silent adaptation.",
        body: renderSimpleTable({
          columns: [
            { key: "target_label", label: "Target" },
            { key: "threshold_result", label: "Threshold" },
            { key: "summary_message", label: "Summary" },
          ],
          rows: safeList(policy.withheld_items),
          emptyMessage: "No withheld policy-evolution rows were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Expiry",
        title: "Expiring Or Re-Review Needed",
        subtitle: "Policy proposals expire explicitly when history goes stale.",
        body: renderSimpleTable({
          columns: [
            { key: "target_label", label: "Target" },
            { key: "expiry_state", label: "Expiry" },
            { key: "summary_message", label: "Summary" },
          ],
          rows: safeList(policy.expiring_items),
          emptyMessage: "No expiring policy-evolution rows were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Rollback",
        title: "Rollback Candidates",
        subtitle: "Trust-preserving overrides can force rollback to a safer prior policy.",
        body: renderSimpleTable({
          columns: [
            { key: "target_label", label: "Target" },
            { key: "trust_override_state", label: "Override" },
            { key: "summary_message", label: "Summary" },
          ],
          rows: safeList(policy.rollback_items),
          emptyMessage: "No rollback policy-evolution rows were returned.",
        }),
      }),
    ].join(""),
    contextHtml: [
      renderSourceRefCard(safeList(policy.source_refs), "Policy Evidence", "Governed policy-evolution refs backing the current temporal policy surface."),
      renderCardSection({
        eyebrow: "Before / After",
        title: "Before And After Policy Comparison",
        subtitle: "Before/after policy state and reversibility are explicit and auditable.",
        body: renderSimpleTable({
          columns: [
            { key: "target_label", label: "Target" },
            { key: "before_policy_state", label: "Before", render: (row) => escapeHtml(row.before_policy_state?.visibility_effect || "UNKNOWN") },
            { key: "after_policy_state", label: "After", render: (row) => escapeHtml(row.after_policy_state?.visibility_effect || "UNKNOWN") },
            { key: "reversibility_state", label: "Reversible" },
            { key: "drill_down_route", label: "Drill-down" },
          ],
          rows: safeList(policy.proof_rows),
          emptyMessage: "No policy-evolution proof rows were returned.",
        }),
      }),
    ].join(""),
  };
}

async function renderAdvisoryPage() {
  const advisory = await fetchAdvisory();
  const decisions = safeList(advisory.decisions);
  return {
    title: "Advisory",
    meta: "Governed advisory decision states derived from certified truth.",
    html: [
      renderCardSection({
        eyebrow: "Advisory",
        title: "Decision Set",
        subtitle: "Backend-authored advisory decisions with preserved authority, lineage, and invalidation state.",
        body: renderSimpleTable({
          columns: [
            { key: "advisory_item_id", label: "Advisory item" },
            { key: "decision_state", label: "Decision" },
            { key: "actionability_state", label: "Actionability" },
            { key: "freshness_state", label: "Freshness" },
            { key: "visibility_state", label: "Visibility" },
            { key: "promotion_eligibility_state", label: "Promotion eligibility" },
          ],
          rows: decisions,
          emptyMessage: "No advisory decisions were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Decision Basis",
        title: "Primary Decision Explanation",
        subtitle: "Explanation is table-driven and governance-bound to the advisory decision object.",
        body: renderDefinitionRows([
          { label: "Decision state", value: advisory.current_decision?.decision_state || "UNKNOWN" },
          { label: "Actionability", value: advisory.current_decision?.actionability_state || "UNKNOWN" },
          { label: "Reason", value: advisory.current_decision?.summary_message || "No decision explanation provided." },
        ]),
      }),
    ].join(""),
    contextHtml: [
      renderSourceRefCard(safeList(advisory.source_refs), "Advisory Evidence", "Governed advisory decision-state refs."),
      renderCardSection({
        eyebrow: "Warnings",
        title: "Advisory Warnings",
        subtitle: "Cross-surface drift or availability warnings raised by the advisory read model.",
        body: safeList(advisory.advisory_warnings).length
          ? `<div class="chip-list">${safeList(advisory.advisory_warnings).map((warning) => `<span class="support-chip">${escapeHtml(warning)}</span>`).join("")}</div>`
          : `<div class="empty-state">No advisory warnings were returned.</div>`,
      }),
    ].join(""),
  };
}

async function renderOperationsPage(state) {
  const [operations, alertsPayload, integrity, actions] = await Promise.all([
    fetchOperations(),
    fetchAlerts(),
    fetchIntegrity(),
    fetchSystemActions(),
  ]);

  return {
    title: "Operations",
    meta: "Readiness ladders, runtime blocks, alert state, and governed actions on one operational surface.",
    html: [
      renderCardSection({
        eyebrow: "Readiness",
        title: "Runtime Readiness Ladder",
        subtitle: "Directly rendered from the operations workspace composition.",
        body: `<div class="metric-grid">
          ${safeList(operations.readiness_ladder).map((row) => renderMetricCard({
            label: row.label || row.key,
            value: row.status || "UNKNOWN",
            semantic: row.semantic || "unknown",
            semantics: state.semantics,
          })).join("")}
        </div>`,
      }),
      renderCardSection({
        eyebrow: "Blocking Conditions",
        title: "Current Blocks",
        subtitle: "Fail-closed runtime blockers and degradation codes surfaced by the backend operations view.",
        body: safeList(operations.blocking_conditions).length
          ? renderList(safeList(operations.blocking_conditions), {
              renderItem: (row) => `
                <article class="stack-card">
                  <div class="stack-card-header">
                    <div>
                      <div class="stack-card-title">${escapeHtml(row.source_name || "blocking_condition")}</div>
                      <div class="stack-card-subtitle">${escapeHtml(row.truth_state || "UNKNOWN")} / ${escapeHtml(row.data_condition || "UNKNOWN")}</div>
                    </div>
                    ${renderSemanticBadge(row.status || "UNKNOWN", row.semantic || "blocked", state.semantics)}
                  </div>
                  <div class="chip-list">${safeList(row.reason_codes).map((code) => `<span class="support-chip">${escapeHtml(code)}</span>`).join("")}</div>
                </article>
              `,
            })
          : `<div class="empty-state">No blocking conditions are currently reported.</div>`,
      }),
      renderAlertSection(alertsPayload, state.semantics),
      renderCardSection({
        eyebrow: "Actions",
        title: "Governed Operator Actions",
        subtitle: "Only supported, repo-proven actions are presented as actionable.",
        body: renderSimpleTable({
          columns: [
            { key: "action_name", label: "Action" },
            { key: "supported", label: "Supported" },
            { key: "reason", label: "Reason" },
          ],
          rows: safeList(actions.actions),
          emptyMessage: "No operator actions are registered.",
        }),
      }),
    ].join(""),
    contextHtml: [
      renderCardSection({
        eyebrow: "Runtime Facts",
        title: "Session Authority Details",
        subtitle: "Authoritative session/runtime fields surfaced by the operations workspace.",
        body: renderDefinitionRows([
          { label: "Environment", value: operations.session_authority_details?.environment || "UNKNOWN" },
          { label: "Submission authorization", value: operations.session_authority_details?.submission_authorization_status || "UNKNOWN" },
          { label: "Recommended action", value: operations.session_authority_details?.recommended_operator_action || "None" },
          { label: "Broker connectivity", value: operations.broker_connectivity_summary?.status || "UNKNOWN" },
          { label: "Integrity", value: operations.integrity_summary?.status || "UNKNOWN" },
        ]),
      }),
      renderSourceRefCard(safeList(operations.source_refs), "Operations Evidence", "Source refs for operations, replay, handshake, and integrity surfaces."),
      renderCardSection({
        eyebrow: "Integrity",
        title: "Integrity Alerts",
        subtitle: "Integrity issues remain explicit and drillable.",
        body: safeList(integrity.integrity_alerts).length
          ? renderList(safeList(integrity.integrity_alerts).slice(0, 6), {
              renderItem: (item) => `
                <article class="stack-card">
                  <div class="stack-card-title">${escapeHtml(item.title || "Integrity issue")}</div>
                  <div class="stack-card-subtitle">${escapeHtml(item.summary || "No summary provided.")}</div>
                </article>
              `,
            })
          : `<div class="empty-state">No integrity alerts were returned.</div>`,
      }),
    ].join(""),
  };
}

async function renderAuditPage() {
  const [audit, activity, homeBundle] = await Promise.all([
    fetchActionAudit(),
    fetchActivityToday(),
    fetchOperatorHome(),
  ]);

  return {
    title: "Audit",
    meta: "Operator action history, daily activity rollups, and trust/retrieval lineage.",
    html: [
      renderCardSection({
        eyebrow: "Audit",
        title: "Operator Action Ledger",
        subtitle: "Append-only action audit entries surfaced by the current UI API.",
        body: renderSimpleTable({
          columns: [
            { key: "time", label: "Time" },
            { key: "action_name", label: "Action" },
            { key: "result", label: "Result" },
            { key: "message", label: "Message" },
          ],
          rows: safeList(audit.audit_entries),
          emptyMessage: "No action audit entries were recorded.",
        }),
      }),
      renderCardSection({
        eyebrow: "Activity",
        title: "Day Activity Rollup",
        subtitle: "Monitoring-ledger rollup and summary counts for the active day.",
        body: renderDefinitionRows([
          { label: "Day", value: activity.day_utc || "UNKNOWN" },
          { label: "Intent summary present", value: activity.intents_summary ? "YES" : "NO" },
          { label: "Submission summary present", value: activity.submissions_summary ? "YES" : "NO" },
          { label: "Rollup present", value: activity.rollup_asof ? "YES" : "NO" },
          { label: "Warnings", value: String(safeList(activity.warnings).length) },
        ]),
      }),
    ].join(""),
    contextHtml: [
      renderTrustPanel(homeBundle.trust_panel || {}),
      renderCardSection({
        eyebrow: "Retrieval",
        title: "Home Retrieval Lineage",
        subtitle: "Retrieval manifest preserved alongside the current home view.",
        body: renderDefinitionRows([
          { label: "Route classification", value: homeBundle.retrieval_manifest?.route_classification || "UNKNOWN" },
          { label: "Retrieval status", value: homeBundle.retrieval_manifest?.retrieval_status || "UNKNOWN" },
          { label: "Rejected artifacts", value: String(safeList(homeBundle.retrieval_manifest?.rejected_artifacts).length) },
        ]),
      }),
      renderSourceRefCard(toEvidenceRefs(activity.source_paths, "Activity source"), "Activity Sources", "Resolved source files used by the activity timeline endpoints."),
    ].join(""),
  };
}

async function renderReportsPage(state) {
  const [reconciliation, homeBundle] = await Promise.all([fetchReconciliation(), fetchOperatorHome()]);
  return {
    title: "Reports",
    meta: "Immutable report-like surfaces rendered from reconciliation and operator-home snapshot contracts.",
    html: [
      renderCardSection({
        eyebrow: "Reports",
        title: "Reconciliation Snapshot",
        subtitle: "Reconciliation remains an immutable report surface with explicit source refs and mismatch evidence.",
        body: `<div class="metric-grid">
          ${renderMetricCard({ label: "Execution reconciliation", value: reconciliation.execution_reconciliation_status || "UNKNOWN", detail: `As of ${formatTimestamp(reconciliation.as_of_utc)}` })}
          ${renderMetricCard({ label: "Fill ledger", value: reconciliation.fill_ledger_status || "UNKNOWN" })}
          ${renderMetricCard({ label: "Mismatches", value: String(safeList(reconciliation.mismatches).length) })}
          ${renderMetricCard({ label: "Truth state", value: reconciliation.truth_state || "UNKNOWN" })}
        </div>`,
      }),
      renderCardSection({
        eyebrow: "Snapshot",
        title: "Mismatch Review",
        subtitle: "Mismatches are preserved as evidence-backed rows, not recalculated in the UI.",
        body: renderSimpleTable({
          columns: [
            { key: "comparison_id", label: "Comparison" },
            { key: "status", label: "Status" },
            { key: "reason", label: "Reason" },
            { key: "truth_state", label: "Truth state" },
          ],
          rows: safeList(reconciliation.mismatches),
          emptyMessage: "No reconciliation mismatches were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Operator Home",
        title: "Home Snapshot Summary",
        subtitle: "The current governed home surface is also treated as an immutable report-like product view.",
        body: renderPanelRows(safeList(homeBundle.home_view?.rendered_panels), state.semantics),
      }),
    ].join(""),
    contextHtml: [
      renderSourceRefCard(safeList(reconciliation.source_refs), "Reconciliation Evidence", "Snapshot source refs from reconciliation views."),
      renderTrustPanel(homeBundle.trust_panel || {}),
    ].join(""),
  };
}

function _configFieldValue(proposed, current, name, fallback) {
  if (Object.prototype.hasOwnProperty.call(proposed || {}, name)) {
    return proposed[name];
  }
  if (Object.prototype.hasOwnProperty.call(current || {}, name)) {
    return current[name];
  }
  return fallback;
}

async function renderConfigurationPage(state) {
  const [catalog, current] = await Promise.all([
    fetchConfigurationCatalog(),
    fetchConfigurationCurrent(),
  ]);
  const workflowState = state.configurationWorkflow || {};
  const explicitDraftId = String(
    workflowState.activeDraftId
    || workflowState.latestDraft?.draft_id
    || (new URLSearchParams(window.location.search || "")).get("draft_id")
    || "",
  ).trim();
  let draft = workflowState.latestDraft || null;
  let draftLoadError = null;
  if (explicitDraftId) {
    try {
      const draftPayload = await fetchConfigurationDraft(explicitDraftId);
      draft = draftPayload.draft || draft;
    } catch (error) {
      draftLoadError = error?.message || "Draft could not be loaded.";
    }
  }

  const currentValues = current.current_values || {};
  const proposedValues = draft?.proposed_values || {};
  const scenario = String(
    _configFieldValue(proposedValues, currentValues, "scenario", "florida") || "florida",
  ).toLowerCase();
  const includeInheritance = Boolean(
    _configFieldValue(proposedValues, currentValues, "include_inheritance", false),
  );
  const horizonMonths = String(
    _configFieldValue(proposedValues, currentValues, "horizon_months", 24),
  );
  const startMonth = String(
    _configFieldValue(proposedValues, currentValues, "start_month", ""),
  );
  const canRunDraftActions = Boolean(draft?.draft_id);
  const disableDraftAction = canRunDraftActions ? "" : "disabled";
  const validation = draft?.validation || {};
  const review = draft?.review || {};
  const activation = draft?.activation || {};
  const lastError = workflowState.lastError || null;

  return {
    title: "Configuration",
    meta: "Governed authority-input configuration using draft, validation, review, and explicit activation audit.",
    html: [
      renderCardSection({
        eyebrow: "Current",
        title: "Current Capital Cashflow Configuration",
        subtitle: "Current values come from active configuration state when available; fallback values are explicitly labeled.",
        body: renderDefinitionRows([
          { label: "State", value: current.status || "UNKNOWN" },
          { label: "Scenario", value: String(currentValues.scenario || "n/a") },
          { label: "Include inheritance", value: String(Boolean(currentValues.include_inheritance)) },
          { label: "Horizon (months)", value: String(currentValues.horizon_months ?? "n/a") },
          { label: "Start month", value: String(currentValues.start_month || "n/a") },
          { label: "Reason codes", value: safeList(current.reason_codes).join(", ") || "none" },
        ]),
      }),
      renderCardSection({
        eyebrow: "Draft",
        title: "Capital Cashflow Draft Editor",
        subtitle: "Draft writes are non-authoritative until validation, review, and activation succeed.",
        body: `
          <form class="configuration-workflow-form" autocomplete="off">
            <input type="hidden" name="configuration_action" value="create_draft" />
            <div class="line-list">
              <label for="cfg_scenario">Scenario</label>
              <select id="cfg_scenario" name="scenario">
                ${safeList(catalog.editable_fields?.find((item) => item.parameter_name === "capital_cashflow.scenario")?.validation?.enum || ["florida", "chile", "base"])
                  .map((item) => `<option value="${escapeHtml(String(item))}" ${String(item) === scenario ? "selected" : ""}>${escapeHtml(String(item))}</option>`).join("")}
              </select>
            </div>
            <div class="line-list">
              <label>
                <input type="checkbox" name="include_inheritance" value="true" ${includeInheritance ? "checked" : ""} />
                Include inheritance
              </label>
            </div>
            <div class="line-list">
              <label for="cfg_horizon_months">Horizon (months)</label>
              <input id="cfg_horizon_months" name="horizon_months" type="number" min="1" max="120" step="1" value="${escapeHtml(horizonMonths)}" />
            </div>
            <div class="line-list">
              <label for="cfg_start_month">Start month</label>
              <input id="cfg_start_month" name="start_month" type="month" value="${escapeHtml(startMonth)}" />
            </div>
            <button type="submit">Create Draft</button>
          </form>
        `,
      }),
      renderCardSection({
        eyebrow: "Lifecycle",
        title: "Draft Lifecycle Actions",
        subtitle: "Actions are explicit and gated; activation is blocked until validation and review succeed.",
        body: `
          <div class="line-list">
            <div>Draft id: ${escapeHtml(draft?.draft_id || "none")}</div>
            <div>Status: ${escapeHtml(draft?.status || "DRAFT_NOT_CREATED")}</div>
          </div>
          <form class="configuration-workflow-form">
            <input type="hidden" name="configuration_action" value="validate_draft" />
            <input type="hidden" name="draft_id" value="${escapeHtml(draft?.draft_id || "")}" />
            <button type="submit" ${disableDraftAction}>Validate Draft</button>
          </form>
          <form class="configuration-workflow-form">
            <input type="hidden" name="configuration_action" value="review_draft" />
            <input type="hidden" name="draft_id" value="${escapeHtml(draft?.draft_id || "")}" />
            <button type="submit" ${disableDraftAction}>Review Draft</button>
          </form>
          <form class="configuration-workflow-form">
            <input type="hidden" name="configuration_action" value="activate_draft" />
            <input type="hidden" name="draft_id" value="${escapeHtml(draft?.draft_id || "")}" />
            <button type="submit" ${disableDraftAction}>Activate Draft</button>
          </form>
          <form class="configuration-workflow-form">
            <input type="hidden" name="configuration_action" value="reject_draft" />
            <input type="hidden" name="draft_id" value="${escapeHtml(draft?.draft_id || "")}" />
            <input type="text" name="reject_reason" placeholder="rejection reason (optional)" />
            <button type="submit" ${disableDraftAction}>Reject Draft</button>
          </form>
        `,
      }),
      renderCardSection({
        eyebrow: "Validation",
        title: "Validation Result",
        subtitle: "Validation must return PASS before review/activation.",
        body: renderDefinitionRows([
          { label: "Status", value: validation.status || "NOT_RUN" },
          { label: "Reason codes", value: safeList(validation.reason_codes).join(", ") || "none" },
        ]),
      }),
      renderCardSection({
        eyebrow: "Review",
        title: "Review Summary",
        subtitle: "Review shows exact proposed values and the explicit field delta from current values.",
        body: renderSimpleTable({
          columns: [
            { key: "field", label: "Field" },
            { key: "current_value", label: "Current" },
            { key: "proposed_value", label: "Proposed" },
          ],
          rows: safeList(review.changed_fields),
          emptyMessage: "No review delta is available.",
        }),
      }),
      renderCardSection({
        eyebrow: "Activation",
        title: "Activation Result",
        subtitle: "Activation writes audited configuration activation artifacts and current state reference.",
        body: renderDefinitionRows([
          { label: "Status", value: activation.status || "NOT_ACTIVATED" },
          { label: "Audit evidence path", value: activation.audit_evidence_path || "n/a" },
          { label: "Configuration state path", value: activation.configuration_state_path || "n/a" },
        ]),
      }),
      lastError
        ? renderCardSection({
            eyebrow: "Error",
            title: "Last Configuration Action Error",
            subtitle: "Action errors are explicit and fail closed.",
            body: `<div class="error-state">${escapeHtml(String(lastError))}</div>`,
          })
        : "",
      draftLoadError
        ? renderCardSection({
            eyebrow: "Warning",
            title: "Draft Retrieval Warning",
            subtitle: "The configured draft id could not be loaded.",
            body: `<div class="error-state">${escapeHtml(String(draftLoadError))}</div>`,
          })
        : "",
    ].join(""),
    contextHtml: [
      renderCardSection({
        eyebrow: "Locked",
        title: "Locked-By-Design Safety Fields",
        subtitle: "Kill switch, broker arming, submission authorization, and readiness attestations remain non-editable from this UI surface.",
        body: renderSimpleTable({
          columns: [
            { key: "parameter_name", label: "Parameter" },
            { key: "lock_class", label: "Lock Class" },
            { key: "reason", label: "Reason" },
          ],
          rows: safeList(catalog.locked_fields),
          emptyMessage: "No locked field declarations were returned.",
        }),
      }),
      renderCardSection({
        eyebrow: "Coverage",
        title: "Configuration Coverage",
        subtitle: "Coverage classifies parameters as editable, visible-only, or locked-by-design.",
        body: renderSimpleTable({
          columns: [
            { key: "parameter_name", label: "Parameter" },
            { key: "state", label: "UI State" },
            { key: "owning_domain", label: "Domain" },
            { key: "reason", label: "Reason" },
          ],
          rows: safeList(catalog.coverage),
          emptyMessage: "No coverage rows were returned.",
        }),
      }),
    ].join(""),
  };
}

function renderBlockedDomain(routeId) {
  const route = routeForId(routeId);
  return {
    title: route?.label || "Blocked Domain",
    meta: route?.subtitle || "Backend gap",
    html: renderGapState({
      title: `${route?.label || "Domain"} is backend-blocked`,
      summary: "This route is present in the shell but currently has no backend workflow handler.",
      bullets: [
        "No backend API projection was resolved for this route.",
        "The shell keeps the route visible to preserve navigation consistency.",
      ],
    }),
    contextHtml: renderCardSection({
      eyebrow: "Scope",
      title: "Why this stops here",
      subtitle: "The redesign must not invent frontend truth where no canonical backend contract is exposed.",
      body: `<div class="line-list">
        <div>The shell route exists so the product stays unified.</div>
        <div>The domain remains intentionally non-authoritative until the backend exposes a canonical read model.</div>
      </div>`,
    }),
  };
}

export function buildPaletteEntries(state) {
  const routeEntries = ROUTES.map((route) => ({
    kind: "route",
    label: route.label,
    subtitle: route.subtitle,
    href: route.path,
  }));
  const workflowEntries = safeList(state.shell?.operatorWorkflow?.next_steps).map((step) => ({
    kind: "next_step",
    label: step.title || "Next step",
    subtitle: step.rationale || step.next_step_kind || "",
    href: routeHrefFromSurface(step.target_surface),
  }));
  return [...routeEntries, ...workflowEntries];
}

export async function loadRouteView(routeId, state) {
  switch (routeId) {
    case "command":
      return renderCommandPage(state);
    case "capital_overview":
      return renderCapitalOverviewPage(state);
    case "capital_accounts":
      return renderCapitalAccountsPage(state);
    case "capital_allocation":
      return renderCapitalAllocationPage(state);
    case "capital_history":
      return renderCapitalHistoryPage(state);
    case "capital_flows":
      return renderCapitalFlowsPage(state);
    case "capital_cashflow":
      return renderCapitalCashflowPage(state);
    case "capital_validation":
      return renderCapitalValidationPage(state);
    case "portfolio":
      return renderPortfolioPage(state);
    case "sleeves":
      return renderSleevesPage(state);
    case "opportunities":
      return renderOpportunitiesPage(state);
    case "tax":
      return renderTaxPage(state);
    case "advisory":
      return renderAdvisoryPage(state);
    case "operations":
      return renderOperationsPage(state);
    case "outcomes":
      return renderOutcomesPage(state);
    case "refinement":
      return renderRefinementPage(state);
    case "policy":
      return renderPolicyPage(state);
    case "audit":
      return renderAuditPage(state);
    case "reports":
      return renderReportsPage(state);
    case "configuration":
      return renderConfigurationPage(state);
    default:
      return {
        title: "Unknown Route",
        meta: "This route is not part of the current Aegis shell manifest.",
        html: `<div class="error-state">Unknown route.</div>`,
        contextHtml: "",
      };
  }
}

export async function executeConfigurationWorkflow(formData, state) {
  const action = String(formData?.get("configuration_action") || "").trim();
  const draftId = String(formData?.get("draft_id") || "").trim();
  let result;

  if (action === "create_draft") {
    const scenario = String(formData?.get("scenario") || "florida").toLowerCase();
    const includeInheritance = ["1", "true", "yes", "on"].includes(String(formData?.get("include_inheritance") || "").toLowerCase());
    const horizonRaw = String(formData?.get("horizon_months") || "").trim();
    const horizonMonths = Number.parseInt(horizonRaw, 10);
    const startMonth = String(formData?.get("start_month") || "").trim();
    result = await createConfigurationDraft({
      scenario,
      include_inheritance: includeInheritance,
      horizon_months: Number.isFinite(horizonMonths) ? horizonMonths : null,
      start_month: startMonth,
    });
  } else if (action === "validate_draft") {
    result = await validateConfigurationDraft(draftId);
  } else if (action === "review_draft") {
    result = await reviewConfigurationDraft(draftId);
  } else if (action === "activate_draft") {
    result = await activateConfigurationDraft(draftId);
  } else if (action === "reject_draft") {
    const reason = String(formData?.get("reject_reason") || "").trim();
    result = await rejectConfigurationDraft(draftId, { reason });
  } else {
    throw new Error("Unsupported configuration action.");
  }

  state.configurationWorkflow = {
    activeDraftId: result?.draft?.draft_id || draftId || "",
    latestDraft: result?.draft || null,
    latestResult: result || null,
    lastAction: action,
    lastError: null,
  };
}

export async function executeOperatorQuery(queryText, state) {
  if (!queryText || !String(queryText).trim()) {
    state.commandQueryResult = null;
    state.commandQueryText = "";
    return;
  }
  state.commandQueryText = String(queryText);
  state.commandQueryResult = await fetchOperatorQuery(String(queryText).trim());
}
