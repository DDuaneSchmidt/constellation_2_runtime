import { activeNavSelectionForPath } from "./pages/route_metadata.js";

export const OPERATOR_WORKSPACES = [
  {
    section: "AI CIO",
    domains: [
      {
        id: "cio_briefing",
        label: "CIO Briefing",
        icon: "CIO",
        accent: "green",
        route: "/",
        truthOwner: "AI CIO Briefing",
        description: "Capital allocation, portfolio oversight, and investment research for David & Carolyn.",
        children: [],
      },
      {
        id: "cio_capital_map",
        label: "Capital Map",
        icon: "$",
        accent: "blue",
        route: "/capital-map",
        truthOwner: "Capital allocation",
        description: "Oak Harvest, Fidelity, cash, annuity, paper trade, Portfolio123, and Aegis research capital roles.",
        children: [],
      },
      {
        id: "cio_portfolios",
        label: "Portfolios",
        icon: "PF",
        accent: "amber",
        route: "/portfolios",
        truthOwner: "Portfolio oversight",
        description: "Allocation, benchmarks, fee drag, concentration, and underperformance flags.",
        children: [],
      },
      {
        id: "cio_research_lab",
        label: "Research Lab",
        icon: "RL",
        accent: "green",
        route: "/research-lab",
        truthOwner: "Investment research",
        description: "Companion, Ultra-Safe, Portfolio123 screens, Aegis / Paul trade logic, and new strategy ideas.",
        children: [],
      },
      {
        id: "cio_opportunities",
        label: "Opportunities",
        icon: "O",
        accent: "purple",
        route: "/cio-opportunities",
        truthOwner: "Investment opportunities",
        description: "Investment-focused opportunity queue and next capital-review actions.",
        children: [],
      },
      {
        id: "cio_retirement_simulator",
        label: "Retirement Simulator",
        icon: "SIM",
        accent: "amber",
        route: "/retirement-simulator",
        truthOwner: "Scenario planning",
        description: "Age 58 to 95 flight simulator with Chile 2027 and Azario scenario toggles.",
        children: [],
      },
      {
        id: "cio_advisor_oversight",
        label: "Advisor Oversight",
        icon: "AO",
        accent: "blue",
        route: "/advisor-oversight",
        truthOwner: "Advisor review",
        description: "Oak Harvest benchmark, fee drag, allocation drift, and source-backed comparison status.",
        children: [],
      },
      {
        id: "cio_documents",
        label: "Documents",
        icon: "D",
        accent: "cyan",
        route: "/documents",
        truthOwner: "Document vault",
        description: "Statements, exports, research evidence, and planning documents.",
        children: [],
      },
      {
        id: "cio_carolyn",
        label: "Carolyn",
        icon: "C",
        accent: "purple",
        route: "/carolyn",
        truthOwner: "Family planning support",
        description: "Secondary planning and household continuity context.",
        children: [],
      },
    ],
  },
];


export const WORKSPACE_ROUTE_GROUPS = [
  { workspaceId: "aegis_paper_performance", routes: ["/aegis-paper-performance", "/aegis-sleeve-validation", "/aegis-sleeve-analytics"] },
  { workspaceId: "workspace_research", routes: ["/aegis-research-workspace", "/research-lab", "/research-lab/review", "/research-lab/blocked-work", "/research-lab/hypotheses", "/research-lab/start", "/research-lab/plans", "/research-lab/evidence", "/research-lab/paper-trials", "/research-lab/sleeve-reviews", "/research-lab/backlog"] },
];

export const ENGINEERING_NAVIGATION_SCHEMA = [
  {
    section: "ENGINEERING MODE",
    domains: [
      { id: "aegis_dashboard", label: "System Health", operatorLabel: "System Health", icon: "H", accent: "cyan", route: "/aegis-opportunities", truthOwner: "aegis_canonical_operator_state_v1", description: "Operational health, dependencies, blockers, and recovery guidance.", hidden_by_default: true, available_in_engineering_mode: true, children: [] },
      { id: "aegis_change_control", label: "Change Control", icon: "C", accent: "amber", route: "/aegis-change-control", truthOwner: "aegis_change_control_register_v1", description: "Read-only enhancement requests, audit findings, validation status, and V1.1 backlog.", hidden_by_default: true, available_in_engineering_mode: true, children: [
        { id: "aegis_change_control_lab", label: "Intelligence Lab", route: "/aegis-change-control-lab", truthOwner: "aegis_change_control_advisor_score_v1", description: "Advisory-only next-action recommendations." },
      ] },
      { id: "aegis_candidates", label: "Candidates", icon: "C", accent: "blue", route: "/aegis-candidates", truthOwner: "aegis_canonical_operator_state_v1", description: "Current-day provisional candidates and readiness.", hidden_by_default: true, available_in_engineering_mode: true, children: [] },
      { id: "aegis_candidate_funnel", label: "Candidate Funnel", operatorLabel: "Candidate Details", icon: "F", accent: "cyan", route: "/aegis-candidate-funnel", truthOwner: "candidate_funnel_projection", description: "Candidate blocker attribution and capture-ticket counts.", hidden_by_default: true, available_in_engineering_mode: true, children: [] },
      { id: "aegis_exit_review", label: "Exit Review", icon: "E", accent: "green", route: "/aegis-exit-review", truthOwner: "exit_review_projection_v1", description: "Open paper and legacy historical position exit review.", hidden_by_default: true, available_in_engineering_mode: true, children: [] },
      { id: "aegis_open_paper_positions", label: "Open Paper Positions", icon: "O", accent: "green", route: "/aegis-open-paper-positions", truthOwner: "Open holdings", description: "Canonical simulated paper positions.", hidden_by_default: true, available_in_engineering_mode: true, children: [] },
      { id: "aegis_positions_diagnostics", label: "Positions Diagnostics", icon: "D", accent: "blue", route: "/aegis-positions-diagnostics", truthOwner: "aegis_candidate_lifecycle_projection_v1", description: "Lifecycle diagnostics, carry-forward context, command status, session details, and engineering metadata.", hidden_by_default: true, available_in_engineering_mode: true, children: [] },
      { id: "closed_trades", label: "Closed Trades", operatorLabel: "Trade History", icon: "C", accent: "green", route: "/aegis-journal", truthOwner: "aegis_canonical_operator_state_v1", description: "Closed governed trades and legacy historical records.", hidden_by_default: true, available_in_engineering_mode: true, children: [] },
      { id: "system_health", label: "Performance", icon: "P", accent: "purple", route: "/aegis-performance", truthOwner: "paper_trade_evaluation_projection_v1", description: "Paper and historical trade evaluation and P&L evidence.", hidden_by_default: true, available_in_engineering_mode: true, children: [] },
      { id: "aegis_sleeve_validation", label: "Sleeve Validation", icon: "V", accent: "green", route: "/aegis-sleeve-validation", truthOwner: "sleeve_performance_truth_v1", description: "Sleeve-level validation of working, underpowered, and failing paper research sleeves.", hidden_by_default: true, available_in_engineering_mode: true, children: [] },
      { id: "aegis_sleeve_analytics", label: "Sleeve Analytics", icon: "S", accent: "green", route: "/aegis-sleeve-analytics", truthOwner: "aegis_sleeve_analytics_v1", description: "Canonical sleeve analytics scorecard and silent sleeve diagnostics.", hidden_by_default: true, available_in_engineering_mode: true, children: [] },
      { id: "research_pipeline", label: "Hypotheses", icon: "R", accent: "amber", route: "/research-lab", truthOwner: "Research workflow", description: "Hypotheses and research findings.", hidden_by_default: true, available_in_engineering_mode: true, children: [
        { id: "research_review", label: "Research Review", route: "/research-lab/review", truthOwner: "aegis_research_review_brief_v1", description: "Operator-readable briefs for recommendation-ready hypotheses." },
        { id: "aegis_research_portfolio", label: "Research Portfolio", route: "/aegis-research-portfolio", truthOwner: "aegis_research_portfolio_v1", description: "Hypothesis-centered research portfolio and allocation recommendations." },
      ] },
      { id: "research_queue", label: "Queue", icon: "Q", accent: "amber", route: "/research-lab", truthOwner: "Research workflow", description: "Research queue and autonomous execution monitoring.", hidden_by_default: true, available_in_engineering_mode: true, children: [] },
      { id: "research_review", label: "Review", icon: "R", accent: "amber", route: "/research-lab/review", truthOwner: "aegis_research_review_brief_v1", description: "Operator-readable briefs for recommendation-ready hypotheses.", hidden_by_default: true, available_in_engineering_mode: true, children: [] },
      { id: "research_diagnostics", label: "Diagnostics", icon: "D", accent: "amber", route: "/research-lab/blocked-work", truthOwner: "Research workflow", description: "Blocked research work, raw diagnostics, and exact next actions.", hidden_by_default: true, available_in_engineering_mode: true, children: [] },
      { id: "runtime_truth", label: "Runtime Truth", icon: "T", accent: "indigo", route: "/aegis-runtime-truth", truthOwner: "runtime_truth_kernel_v1", description: "Global runtime truth and readiness classification.", hidden_by_default: true, available_in_engineering_mode: true, children: [] },
      { id: "runtime_timeline", label: "Runtime Timeline", operatorLabel: "Run History", icon: "T", accent: "indigo", route: "/aegis-runtime-timeline", truthOwner: "operator_state_snapshot_v1.runtime_timeline_projection", description: "Timed runs, retries, and pipeline-stage visibility.", hidden_by_default: true, available_in_engineering_mode: true, children: [] },
      { id: "repair_center", label: "Repair Center", operatorLabel: "Fix Issues", icon: "R", accent: "orange", route: "/aegis-repair-center", truthOwner: "aegis_repair_center_projection_v1", description: "Repair delayed domains and source setup issues.", hidden_by_default: true, available_in_engineering_mode: true, children: [] },
      { id: "aegis_verified_runtime", label: "Verified Graph", operatorLabel: "System Evidence", icon: "V", accent: "indigo", route: "/aegis-verified-runtime", truthOwner: "portal_runtime_model_v1", description: "Verified graph status, evidence hashes, blockers, and claim guard.", hidden_by_default: true, available_in_engineering_mode: true, children: [] },
      { id: "aegis_hash_lineage", label: "Hash Lineage", operatorLabel: "Data Lineage", icon: "H", accent: "indigo", route: "/aegis-verified-runtime#lineage", truthOwner: "aegis_hash_lineage_v1", description: "Market-data and downstream hash lineage.", hidden_by_default: true, available_in_engineering_mode: true, children: [] },
      { id: "aegis_candidate_lineage", label: "Candidate Lineage", icon: "L", accent: "indigo", route: "/aegis-candidate-lineage", truthOwner: "aegis_candidate_lineage_forensics_v1", description: "Forensic candidate provenance.", hidden_by_default: true, available_in_engineering_mode: true, children: [] },
      { id: "aegis_evidence_ledger", label: "Evidence Ledger", icon: "E", accent: "indigo", route: "/aegis-verified-runtime#evidence", truthOwner: "evidence_ledger_v1", description: "Evidence ledger and artifact references.", hidden_by_default: true, available_in_engineering_mode: true, children: [] },
      { id: "aegis_input_checks", label: "Input Checks", icon: "I", accent: "cyan", route: "/aegis-opportunities#input-checks", truthOwner: "input_contract_reconciliation_v1", description: "Input contract reconciliation and producer checks.", hidden_by_default: true, available_in_engineering_mode: true, children: [] },
      { id: "aegis_provider_health", label: "Provider Health", icon: "S", accent: "cyan", route: "/aegis-opportunities#provider-health", truthOwner: "provider_source_status_v1", description: "Provider/source blockers and health details.", hidden_by_default: true, available_in_engineering_mode: true, children: [] },
      { id: "aegis_operator_cockpit", label: "Operator Cockpit", icon: "O", accent: "green", route: "/aegis-operator-cockpit", truthOwner: "aegis_operator_cockpit_v1", description: "Legacy consolidated cockpit surface.", hidden_by_default: true, available_in_engineering_mode: true, children: [] },
      { id: "aegis_adaptive_intelligence", label: "Adaptive Intelligence", icon: "A", accent: "purple", route: "/aegis-adaptive-intelligence", truthOwner: "adaptive_intelligence_v1", description: "Legacy analytics and regime surfaces.", hidden_by_default: true, available_in_engineering_mode: true, children: [] },
    ],
  },
];

export const NAVIGATION_SCHEMA = OPERATOR_WORKSPACES;

export function navigationSchemaForMode(mode = "operator") {
  return String(mode || "operator").toLowerCase() === "engineering"
    ? [...OPERATOR_WORKSPACES, ...ENGINEERING_NAVIGATION_SCHEMA]
    : OPERATOR_WORKSPACES;
}

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
  const selection = activeNavSelectionForPath(pathname);
  const flattened = flattenNavigation(schema);
  if (selection.is_unknown_route) return null;
  return (
    flattened.find((item) => item.id === selection.nav_item_id) ||
    flattened.find((item) => item.id === selection.parent_nav_item_id) ||
    null
  );
}

export const LEGACY_CORE_NAVIGATION_REFERENCE = [
  { section: "CORE", id: "command", label: "Command", route: "/", truthOwner: "policy_runtime", badgeCount: 3 },
  { section: "CORE", id: "command_configuration", label: "Configuration", route: "/configuration", truthOwner: "configuration_activation_authority_v1" },
  { section: "INTELLIGENCE", id: "advisory", label: "Advisory", route: "/advisory", truthOwner: "advisory_read_model" },
  { section: "INTELLIGENCE", id: "taxes", label: "Taxes", route: "/tax", truthOwner: "tax_state_view" },
  { section: "SYSTEM", id: "system_bug_log", label: "Bug Log", route: "/reliability/issues", truthOwner: "reliability_issue_ledger", badgeCount: 5 },
  { section: "SYSTEM", id: "performance_cockpit", label: "Performance Drilldown", route: "/performance", truthOwner: "aegis_performance_showcase_v1" },
];

export const LEGACY_NAVIGATION_REFERENCE = [...ENGINEERING_NAVIGATION_SCHEMA, ...LEGACY_CORE_NAVIGATION_REFERENCE];
