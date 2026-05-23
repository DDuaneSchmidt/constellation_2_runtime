export const NAVIGATION_SCHEMA = [
  {
    section: "OPERATOR WORKFLOW",
    domains: [
      {
        id: "aegis_dashboard",
        label: "Dashboard",
        icon: "D",
        accent: "cyan",
        route: "/aegis-opportunities",
        truthOwner: "aegis_canonical_operator_state_v1",
        description: "System health, today's run, critical blockers, and operator attention.",
        children: [],
      },
      {
        id: "aegis_candidates",
        label: "Candidates",
        icon: "C",
        accent: "blue",
        route: "/aegis-candidates",
        truthOwner: "aegis_canonical_operator_state_v1",
        description: "Current-day provisional candidates, selected opportunities, blockers, and readiness.",
        children: [],
      },
      {
        id: "research_pipeline",
        label: "Hypotheses",
        icon: "R",
        accent: "amber",
        route: "/research-lab",
        truthOwner: "research_lab",
        description: "See hypotheses, start AI research, monitor progress, review findings, and check recommendation readiness.",
        children: [],
      },
      {
        id: "captured_trades",
        label: "Captured Trades",
        icon: "C",
        accent: "green",
        route: "/aegis-journal",
        truthOwner: "aegis_canonical_operator_state_v1",
        description: "Completed manual captures and historical trade records. Read-only evidence and export actions only.",
        children: [],
      },
      {
        id: "runtime_timeline",
        label: "Runtime Timeline",
        icon: "T",
        accent: "indigo",
        route: "/aegis-runtime-timeline",
        truthOwner: "operator_state_snapshot_v1.runtime_timeline_projection",
        description: "Timed runs, retries, certification windows, and pipeline-stage visibility.",
        children: [],
      },
      {
        id: "repair_center",
        label: "Repair Center",
        icon: "R",
        accent: "orange",
        route: "/aegis-repair-center",
        truthOwner: "aegis_repair_center_projection_v1",
        description: "Repair delayed domains, source setup issues, provider waits, and failed remediation jobs.",
        children: [],
      },
      {
        id: "system_health",
        label: "System Health",
        icon: "H",
        accent: "purple",
        route: "/aegis-performance",
        truthOwner: "aegis_canonical_operator_state_v1",
        description: "Readiness, warnings, replay, and diagnostics. No execution controls.",
        children: [],
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
  return flattened.find((item) => item.route !== "/" && path.startsWith(item.route)) || flattened.find((item) => item.route === "/aegis-opportunities") || flattened[0];
}

// Legacy route metadata retained for tests and deep-link discoverability only.
// It is not rendered by the sidebar because NAVIGATION_SCHEMA above is the
// workflow-first operator navigation.
export const LEGACY_NAVIGATION_REFERENCE = [
  { section: "CORE", id: "command", label: "Command", route: "/", truthOwner: "policy_runtime", badgeCount: 3 },
  { section: "CORE", id: "command_configuration", label: "Configuration", route: "/configuration", truthOwner: "configuration_activation_authority_v1" },
  { section: "INTELLIGENCE", id: "advisory", label: "Advisory", route: "/advisory", truthOwner: "advisory_read_model" },
  { section: "INTELLIGENCE", id: "taxes", label: "Taxes", route: "/tax", truthOwner: "tax_state_view" },
  { section: "SYSTEM", id: "system_bug_log", label: "Bug Log", route: "/reliability/issues", truthOwner: "reliability_issue_ledger", badgeCount: 5 },
  { section: "SYSTEM", id: "performance_cockpit", label: "Performance Drilldown", route: "/performance", truthOwner: "aegis_performance_showcase_v1" },
  {
    section: "RESEARCH",
    domains: [
      {
        id: "research_lab",
        label: "Hypotheses",
        icon: "R",
        accent: "amber",
        route: "/research-lab",
        truthOwner: "research_lab",
        description: "Start research, review hypotheses, inspect evidence, and check recommendation readiness.",
        children: [
          { id: "research_start", label: "Start Research", icon: "+", route: "/research-lab/start", truthOwner: "research_lab" },
          { id: "research_hypothesis_queue", label: "Hypothesis Queue", icon: "H", route: "/research-lab/hypotheses", truthOwner: "research_lab" },
          { id: "research_plans", label: "Research Plans", icon: "P", route: "/research-lab/plans", truthOwner: "research_lab" },
          { id: "research_evidence", label: "Evidence", icon: "E", route: "/research-lab/evidence", truthOwner: "research_lab" },
          { id: "research_paper_trials", label: "Paper Trials", icon: "T", route: "/research-lab/paper-trials", truthOwner: "research_lab" },
          { id: "research_sleeve_reviews", label: "Sleeve Reviews", icon: "S", route: "/research-lab/sleeve-reviews", truthOwner: "research_lab" },
          { id: "research_blocked_work", label: "Blocked Work", icon: "B", route: "/research-lab/blocked-work", truthOwner: "research_lab" },
          { id: "research_backlog", label: "Research Backlog", icon: "L", route: "/research-lab/backlog", truthOwner: "research_lab" },
        ],
      },
    ],
  },
];
