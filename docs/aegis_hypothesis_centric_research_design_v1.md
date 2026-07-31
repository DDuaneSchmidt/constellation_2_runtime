# Aegis Hypothesis-Centric Research Design v1

## Design

The upgrade adds a research portfolio layer above existing Aegis artifacts. It does not replace sleeve analytics, candidate diagnostics, evidence lineage integrity, validation, or scorecards. It consumes those artifacts and emits thesis/hypothesis-level truth.

## Modules

- `research_mapping_rules_v1`: deterministic legacy sleeve-to-thesis/hypothesis migration rules.
- `research_thesis_registry_v1`: daily thesis registry.
- `hypothesis_registry_v1`: daily hypothesis registry and evidence links.
- `hypothesis_state_machine_v1`: deterministic thesis, hypothesis, and sleeve relationship transitions.
- `research_allocation_score_v1`: deterministic allocation scoring.
- `research_portfolio_manager_v1`: orchestration and daily portfolio artifact.
- `research_portfolio_self_check_v1`: audit gate.

## Migration

Existing sleeve-centered artifacts are migrated by sleeve ID. Mappings are marked `LEGACY_INFERRED` unless a producer artifact declares `hypothesis_id` and `thesis_id` directly. Legacy inference is allowed only for known sleeve IDs and emits migration report rows requiring future source-declared confirmation.

## Failure Modes

- Unknown sleeve ID with no source-declared hypothesis mapping.
- Candidate with no sleeve and no source-declared hypothesis.
- Paper position with no candidate, sleeve, or inherited hypothesis mapping.
- Validation sample without source-declared or deterministic inherited hypothesis mapping.
- Hypothesis without thesis.
- Conflicting thesis IDs for the same hypothesis.
- Allocation recommendation without reason codes.

## Operator Exposure

The operator shell exposes one research portfolio panel that summarizes counts, top allocation recommendations, and a compact hypothesis-to-sleeve map. It reads the canonical portfolio artifact and does not compute portfolio truth in the browser.
