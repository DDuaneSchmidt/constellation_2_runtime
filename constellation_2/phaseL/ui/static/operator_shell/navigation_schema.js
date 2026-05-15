export const NAVIGATION_SCHEMA = [
  {
    section: "CORE",
    domains: [
      {
        id: "command",
        label: "Command",
        icon: "⌁",
        accent: "cyan",
        route: "/",
        truthOwner: "policy_runtime",
        description: "Governed command plane, exceptions, decisions, and evidence.",
        children: [
          { id: "command_overview", label: "Overview", icon: "◫", route: "/", truthOwner: "policy_runtime", description: "Command readiness and operating truth." },
          { id: "command_exceptions", label: "Exceptions", icon: "!", route: "/operations", badgeCount: 3, truthOwner: "exception_authority", description: "Active governed exceptions." },
          { id: "command_policy", label: "Policy", icon: "§", route: "/policy", truthOwner: "policy_evolution_state_v1", description: "Policy state and governed changes." },
          { id: "command_configuration", label: "Configuration", icon: "▣", route: "/configuration", truthOwner: "configuration_activation_authority_v1", description: "Governed draft, validation, review, and activation for editable Aegis parameters." },
          { id: "command_evidence", label: "Evidence", icon: "□", route: "/reports", truthOwner: "evidence_store", description: "Evidence references and report artifacts." },
          { id: "command_decisions", label: "Decisions", icon: "✓", route: "/audit", truthOwner: "decision_log", description: "Governed decision history." },
        ],
      },
      {
        id: "capital",
        label: "Capital",
        icon: "$",
        accent: "green",
        route: "/capital",
        truthOwner: "capital_authority",
        description: "Capital state, accounts, allocations, and validation.",
        children: [
          { id: "capital_overview", label: "Overview", icon: "◫", route: "/capital", truthOwner: "capital_overview_view" },
          { id: "capital_accounts", label: "Accounts", icon: "▤", route: "/capital/accounts", truthOwner: "account_registry" },
          { id: "capital_allocation", label: "Allocation", icon: "◍", route: "/capital/allocation", truthOwner: "capital_allocation_view" },
          { id: "capital_cashflows", label: "Cashflows", icon: "⇄", route: "/capital/cashflow", truthOwner: "capital_cashflow_view" },
          { id: "capital_validation", label: "Validation", icon: "◇", route: "/capital/validation", truthOwner: "capital_validation_view" },
          { id: "capital_history", label: "History", icon: "↺", route: "/capital/history", truthOwner: "capital_history_view" },
        ],
      },
      {
        id: "portfolio",
        label: "Portfolio",
        icon: "◈",
        accent: "purple",
        route: "/portfolio",
        truthOwner: "financial_state_authority",
        description: "Portfolio state, exposure, performance, and reconciliation.",
        children: [
          { id: "portfolio_overview", label: "Overview", icon: "◫", route: "/portfolio", truthOwner: "financial_state_authority" },
          { id: "portfolio_positions", label: "Positions", icon: "▦", route: "/portfolio", truthOwner: "positions_view" },
          { id: "portfolio_performance", label: "Performance", icon: "⌁", route: "/performance", truthOwner: "aegis_performance_showcase_v1" },
          { id: "portfolio_exposure", label: "Exposure", icon: "◎", route: "/portfolio", truthOwner: "exposure_authority" },
          { id: "portfolio_reconciliation", label: "Reconciliation", icon: "↔", route: "/reports", truthOwner: "reconciliation_view" },
        ],
      },
    ],
  },
  {
    section: "INTELLIGENCE",
    domains: [
      {
        id: "advisory",
        label: "Advisory",
        icon: "✦",
        accent: "amber",
        route: "/advisory",
        truthOwner: "advisory_read_model",
        description: "Insights, recommendations, scenarios, and signals.",
        children: [
          { id: "advisory_insights", label: "Insights", icon: "✧", route: "/advisory", truthOwner: "advisory_read_model" },
          { id: "advisory_recommendations", label: "Recommendations", icon: "→", route: "/advisory", truthOwner: "advisory_read_model" },
          { id: "advisory_scenarios", label: "Scenarios", icon: "◇", route: "/advisory", truthOwner: "scenario_view" },
          { id: "advisory_signals", label: "Signals", icon: "⌁", route: "/opportunities", truthOwner: "opportunity_state_view" },
        ],
      },
      {
        id: "taxes",
        label: "Taxes",
        icon: "%",
        accent: "blue",
        route: "/tax",
        truthOwner: "tax_state_view",
        description: "Tax liabilities, events, strategy, and filings.",
        children: [
          { id: "tax_liabilities", label: "Liabilities", icon: "▤", route: "/tax", truthOwner: "tax_state_view" },
          { id: "tax_events", label: "Events", icon: "!", route: "/tax", truthOwner: "tax_event_view" },
          { id: "tax_strategy", label: "Strategy", icon: "§", route: "/tax", truthOwner: "tax_strategy_view" },
          { id: "tax_filings", label: "Filings", icon: "□", route: "/tax", truthOwner: "tax_filing_view" },
        ],
      },
    ],
  },
  {
    section: "SYSTEM",
    domains: [
      {
        id: "audit",
        label: "Audit",
        icon: "☷",
        accent: "orange",
        route: "/audit",
        truthOwner: "operator_action_audit",
        description: "Audit memory and operator action logs.",
        children: [],
      },
      {
        id: "system_health",
        label: "System Health",
        icon: "◆",
        accent: "red",
        route: "/reliability",
        truthOwner: "reliability_ledger_v1",
        description: "Reliability, issue tracking, and integration health.",
        children: [
          { id: "system_reliability", label: "Reliability", icon: "◉", route: "/reliability", truthOwner: "reliability_readiness" },
          { id: "system_aegis_runtime", label: "Aegis Runtime", icon: "◇", route: "/aegis-runtime", truthOwner: "aegis_operator_state_v1" },
          { id: "system_aegis_lite_queue", label: "Aegis Lite Queue", icon: "▣", route: "/aegis-lite", truthOwner: "aegis_lite_eod_report_v1" },
          { id: "system_bug_log", label: "Bug Log", icon: "!", route: "/reliability/issues", badgeCount: 5, truthOwner: "reliability_issue_ledger" },
          { id: "system_integrations", label: "Integrations", icon: "↔", route: "/operations", truthOwner: "runtime_service_authority_v1" },
        ],
      },
    ],
  },
];

export function flattenNavigation(schema = NAVIGATION_SCHEMA) {
  return schema.flatMap((section) =>
    section.domains.flatMap((domain) => [
      { ...domain, section: section.section, parentId: null, isDomain: true },
      ...(domain.children || []).map((child) => ({
        ...child,
        section: section.section,
        parentId: domain.id,
        parentLabel: domain.label,
        accent: domain.accent,
        isDomain: false,
      })),
    ]),
  );
}

export function activeNavigationForPath(pathname, schema = NAVIGATION_SCHEMA) {
  const path = String(pathname || "/").replace(/\/+$/, "") || "/";
  const flattened = flattenNavigation(schema);
  const exact = flattened.find((item) => item.route === path);
  if (exact) {
    return exact;
  }
  return flattened.find((item) => item.route !== "/" && path.startsWith(item.route)) || flattened.find((item) => item.route === "/");
}
