# Aegis Daily Research Integrity Audit Requirements v1

## Intent
Create a compact daily audit proving whether the latest Aegis research run produced coherent state across candidates, paper observations, entry marks, exits, generated hypotheses, sleeves, allocation recommendations, UI visibility, and safety evidence.

## Scope
The audit is read-only. It must not change trading, broker behavior, safety gates, candidate logic, paper lifecycle, exit logic, validation rules, allocation weights, or hypothesis state. It may identify narrow source-mapping defects for UI visibility.

## Required Artifact
`aegis_daily_research_integrity_audit_v1` at `reports/aegis_daily_research_integrity_audit_v1/{day}/daily_research_integrity_audit.v1.json`.

## Required Checks
The audit must cover candidate integrity, paper observation integrity, exit integrity, sleeve health, generated hypothesis integrity, entry-price lineage, allocation visibility, and audit/safety state.

Oil Shock blocker text must be compared against authoritative `aegis_oil_shock_candidate_flow_v1`. Macro Calendar data blockers remain separate. REDESIGN and PAUSE sleeves are warning/follow-through items, not blockers.

## Required Output Fields
The artifact must expose `integrity_status`, `primary_issue`, `issue_count`, `blocker_count`, `warning_count`, `david_action_count`, and `issue_rows`.

Each issue row must include `issue_id`, `severity`, `category`, `affected_artifact`, `affected_id`, `expected_value`, `actual_value`, `source_artifact_path`, and `recommended_next_step`.

## UI Requirement
Command Center must render a compact Daily Integrity section below Today's Research Result with status, primary issue, blockers, warnings, David actions, and top three issues. Full issue details stay below the fold.
