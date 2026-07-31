# Aegis Architecture Authority Model v1

## 1. Purpose

This document defines which Aegis artifacts own strategic truth going forward and which Aegis Lite artifacts remain only as compatibility bridges until they can be safely replaced.

The target authority classification is:

```text
STRATEGIC_SYSTEM_OF_RECORD:
Paper Trading + Hypothesis Validation Architecture

LEGACY_COMPATIBILITY_LAYER:
Aegis Lite
```

This is an authority clarification and controlled migration model. It does not remove Aegis Lite, weaken runtime truth, enable trade advice, enable manual capture, enable broker execution, or create new strategic Lite product scope.

## 2. Current Problem

Aegis currently contains two overlapping vocabularies:

1. The newer strategic architecture: hypothesis validation, candidate governance, paper sessions, paper ledgers, outcome validation, research portfolio, and operator action model.
2. The older Aegis Lite architecture: Lite operating status, Lite EOD reports, operator execution queues, manual trade packets, manual receipt bridges, and historical Lite reports.

Runtime truth still consumes some Lite-era artifacts. Therefore Aegis Lite cannot be abruptly removed. However, treating Lite as the strategic architecture creates confusion in planning, UI language, research promotion, and audit interpretation.

The model here separates runtime dependency from strategic authority:

- Runtime truth may temporarily depend on Lite compatibility artifacts.
- Strategic product design should use Paper Trading + Hypothesis Validation as the system of record.
- Operator UI should avoid centering Lite as the main architecture.

## 3. Strategic System of Record

The strategic system of record is the Paper Trading + Hypothesis Validation Architecture.

Strategic authority flows through:

```text
Thesis / Hypothesis Registry
  -> Research Validation Engine
  -> Statistical Sufficiency / Promotion Gate
  -> Candidate Generation and Diagnostics
  -> Paper Review / Paper Session
  -> Paper Position Ledger
  -> Outcome Registry and Validation
  -> Research Portfolio / Capital Allocation
  -> Operator Action Model
```

Strategic artifacts are allowed to define future product truth, current-day paper state, validation state, candidate state, outcome state, and operator action semantics.

`SUPPORTED` research still means eligible for candidate review only. It does not mean tradeable, executable, broker-approved, or live-trading approved.

## 4. Legacy Compatibility Layer

Aegis Lite is a legacy compatibility layer.

It remains present because current runtime truth and some manual-paper compatibility flows still consume Lite-era artifacts. Its status is:

```text
LEGACY_COMPATIBILITY_LAYER
```

Aegis Lite may continue to:

- preserve historical Lite reports,
- generate compatibility manual packets while runtime truth requires them,
- bridge prior manual execution receipt workflows,
- support lineage and migration evidence.

Aegis Lite must not become the center of new strategic architecture, new product language, new research authority, or new operator workflows.

## 5. Authority Table

Allowed `authority_status` values:

- `STRATEGIC_AUTHORITY`
- `COMPATIBILITY_AUTHORITY`
- `LEGACY_READ_ONLY`
- `DEPRECATED`
- `REMOVED`

Allowed `migration_state` values:

- `ACTIVE`
- `MIGRATING`
- `LEGACY_COMPATIBILITY`
- `DEPRECATED`
- `REMOVED`

| artifact_name | current_owner | strategic_owner | authority_status | migration_state | replacement_artifact | deprecation_criteria |
|---|---|---|---|---|---|---|
| thesis_registry | Research / thesis lifecycle | Thesis Registry | STRATEGIC_AUTHORITY | ACTIVE | n/a | n/a |
| research_hypothesis_registry | Research Validation Engine | Hypothesis Registry | STRATEGIC_AUTHORITY | ACTIVE | n/a | n/a |
| candidate_generation | Candidate generation producers | Candidate Generation | STRATEGIC_AUTHORITY | ACTIVE | n/a | n/a |
| candidate_generation_diagnostics | Candidate diagnostics | Candidate Diagnostics | STRATEGIC_AUTHORITY | ACTIVE | n/a | n/a |
| paper_position_ledger | Paper ledger | Paper Position Ledger | STRATEGIC_AUTHORITY | ACTIVE | n/a | n/a |
| outcome_registry | Paper outcomes / outcome validation | Outcome Registry | STRATEGIC_AUTHORITY | ACTIVE | n/a | n/a |
| research_validation_samples | Research validation samples | Validation Samples | STRATEGIC_AUTHORITY | ACTIVE | n/a | n/a |
| statistical_sufficiency | Research validation engine | Statistical Sufficiency | STRATEGIC_AUTHORITY | ACTIVE | n/a | n/a |
| hypothesis_state_machine | Research validation / qualification | Hypothesis State Machine | STRATEGIC_AUTHORITY | ACTIVE | n/a | n/a |
| research_portfolio | Research portfolio | Research Portfolio | STRATEGIC_AUTHORITY | ACTIVE | n/a | n/a |
| research_capital_allocation | Research capital allocation | Research Capital Allocation | STRATEGIC_AUTHORITY | ACTIVE | n/a | n/a |
| operator_action_model | Operator action model | Operator Action Model | STRATEGIC_AUTHORITY | ACTIVE | n/a | n/a |
| aegis_lite_operating_status | Aegis Lite | Paper session / scheduled run readiness | COMPATIBILITY_AUTHORITY | LEGACY_COMPATIBILITY | scheduled_run_readiness + paper_session_ledger + operator_action_model | Runtime truth no longer requires `aegis_lite_operating_status`; replacement artifacts have current-day audit evidence. |
| aegis_lite_eod_report | Aegis Lite | Candidate diagnostics + paper review queue + paper session ledger | COMPATIBILITY_AUTHORITY | LEGACY_COMPATIBILITY | candidate_generation_diagnostics + paper_review_queue + paper_session_ledger | Runtime truth no longer requires `aegis_lite_eod_report`; current-day candidate/paper review evidence replaces Lite report semantics. |
| operator_execution_queue | Aegis Lite | Operator Action Model | COMPATIBILITY_AUTHORITY | LEGACY_COMPATIBILITY | operator_action_model + paper_review_queue | Runtime truth no longer requires `operator_execution_queue`; operator action model owns all actionable/non-actionable row semantics. |
| manual_trade_packet | Aegis Lite | Paper review / paper open authorization | COMPATIBILITY_AUTHORITY | LEGACY_COMPATIBILITY | paper_open_authorization + paper_review_queue + paper_trade_construction | Runtime truth no longer requires `manual_trade_packet`; manual capture remains disabled unless future policy explicitly changes. |
| manual_execution_receipt | Manual receipt bridge | Paper receipts / paper position ledger | COMPATIBILITY_AUTHORITY | LEGACY_COMPATIBILITY | paper_receipt + paper_position_ledger + paper_trade_outcomes | Runtime truth no longer requires `manual_execution_receipt`; paper receipts and ledger prove current-day paper outcomes. |
| historical_lite_reports | Aegis Lite archive | Evidence / audit archive | LEGACY_READ_ONLY | LEGACY_COMPATIBILITY | evidence lineage / audit detail | Historical Lite artifacts are no longer used for current-day readiness and remain read-only for lineage only. |

## 6. Lite Artifact Migration Plan

### aegis_lite_operating_status

- Why it still exists: current runtime truth uses it as part of `DATA_READY` and Lite run status evidence.
- What consumes it: runtime truth kernel, pure runtime evaluator, repair/readiness flows, historical Lite UI/API paths.
- Strategic replacement: `scheduled_run_readiness`, `paper_session_ledger`, and `operator_action_model`.
- Evidence needed before replacement: current-day scheduled run readiness, paper session authority, operator action model self-check, portal smoke, and audit evidence.
- Audit dependency to change: remove `aegis_lite_operating_status` from runtime truth `DATA_READY` evidence only after replacement artifacts are current and verified.
- UI language to change: replace “Lite operating status” with “scheduled paper run status” or “manual compatibility layer status.”
- Deprecation condition: runtime truth passes without requiring this artifact and historical paths remain read-only.

### aegis_lite_eod_report

- Why it still exists: current runtime truth uses it as a broad EOD report and current-day paper/candidate compatibility artifact.
- What consumes it: runtime truth, mode readiness, repair center, current operator truth resolver, historical reporting.
- Strategic replacement: `candidate_generation_diagnostics`, `paper_review_queue`, `paper_session_ledger`, and `operator_action_model`.
- Evidence needed before replacement: candidate diagnostics and review queue agree for the requested day; paper session authority exists; operator action counts are governed by the action model.
- Audit dependency to change: remove `aegis_lite_eod_report` from runtime readiness once strategic artifacts fully cover the same claims.
- UI language to change: avoid “Lite EOD” as primary status; use “candidate evaluation” or “paper review readiness.”
- Deprecation condition: no current-day surface or runtime gate needs the Lite EOD report for readiness.

### operator_execution_queue

- Why it still exists: current runtime truth uses it as a manual queue compatibility artifact.
- What consumes it: runtime truth, paper golden path, operational maturity hardening, lineage tools.
- Strategic replacement: `operator_action_model`, `paper_review_queue`, and candidate state projections.
- Evidence needed before replacement: operator action model explains all actionable, waiting, monitor-only, and blocked states without queue fallback.
- Audit dependency to change: runtime truth should depend on operator action model evidence instead of Lite queue evidence.
- UI language to change: avoid “execution queue” unless explicitly labeled legacy/manual compatibility.
- Deprecation condition: all operator action surfaces are action-model-driven and audit no longer needs the Lite queue.

### manual_trade_packet

- Why it still exists: current runtime truth uses it for manual packet readiness and compatibility with older manual-paper flow.
- What consumes it: runtime truth, paper golden path, sleeve attribution, lineage, receipt/outcome tools.
- Strategic replacement: `paper_open_authorization`, `paper_review_queue`, `paper_trade_construction`, and paper receipts.
- Evidence needed before replacement: paper authorization and construction artifacts prove candidate-to-position path; manual capture remains disabled unless separately governed.
- Audit dependency to change: manual packet should stop being a readiness authority and become historical/manual compatibility evidence.
- UI language to change: “manual compatibility packet,” not “primary trading packet.”
- Deprecation condition: paper review/open authorization owns the paper path and runtime truth no longer requires manual packet evidence.

### manual_execution_receipt

- Why it still exists: current runtime truth and receipt/outcome bridge use it to prove manual paper capture or lack of capture.
- What consumes it: runtime truth, manual receipt recorder, outcome recording, paper golden path, sleeve attribution.
- Strategic replacement: `paper_receipt`, `paper_position_ledger`, and `paper_trade_outcomes`.
- Evidence needed before replacement: paper receipts and ledger prove current-day paper state; outcome validation reconciles positions/outcomes.
- Audit dependency to change: manual receipt bridge should be replaced by paper receipt/ledger proof for paper-mode readiness.
- UI language to change: “paper receipt / ledger evidence” rather than “manual execution receipt” except in diagnostics.
- Deprecation condition: runtime truth can prove paper receipts and outcomes without the manual execution bridge.

## 7. Deprecation Criteria

No Lite artifact may be marked `DEPRECATED` while runtime truth still requires it.

A Lite artifact may move from `LEGACY_COMPATIBILITY` to `DEPRECATED` only when all of the following are true:

1. A replacement artifact is named and implemented.
2. The replacement artifact has current-day audit evidence.
3. Runtime truth no longer requires the Lite artifact for readiness.
4. Operator UI no longer presents the Lite artifact as primary product truth.
5. Historical Lite artifacts remain readable as lineage/audit evidence.
6. Safety gates remain disabled unless a separate governed policy explicitly changes them.

A Lite artifact may move to `REMOVED` only after deprecation has been validated across audit, portal smoke, route inventory, and historical lineage tests.

## 8. Operator UI Language Rules

Preferred operator language:

- Paper Trading
- Hypothesis Validation
- Research Portfolio
- Outcome Validation
- Operator Action Model
- Manual Compatibility Layer
- Paper session
- Paper ledger
- Paper review

Avoid strategic emphasis on:

- Aegis Lite
- Lite Control Plane
- Lite Report as primary status
- Lite as the main architecture
- Lite EOD as the top-level daily status

If Lite appears in UI, label it clearly as one of:

- Legacy compatibility layer
- Manual compatibility bridge
- Historical Lite evidence

Lite labels belong in diagnostics, historical evidence, compatibility details, or migration status. They should not be the first operator-facing explanation of current Aegis strategy.

## 9. Audit / Runtime Truth Impact

This authority model does not change runtime truth dependencies immediately.

Current runtime truth may continue to require Lite artifacts until replacements are proven. The model requires audit to remain conservative:

- Do not infer readiness from strategic naming alone.
- Do not mark Lite artifacts deprecated while runtime truth still requires them.
- Do not weaken `DATA_READY`, paper readiness, manual receipt, trade-advice, or broker gates.
- Do not hide Lite blockers. Translate them as compatibility-layer blockers in operator language.

The immediate audit impact is documentation and registry evidence only. Runtime truth dependency migration is a future governed change.

## 10. Future Enhancement Rules

New strategic work must target the Paper Trading + Hypothesis Validation Architecture unless explicitly scoped as Lite compatibility maintenance.

Allowed future Lite work:

- bug fixes needed to keep runtime truth honest,
- compatibility bridge maintenance,
- lineage preservation,
- migration evidence,
- read-only historical access.

Disallowed future Lite work:

- new strategic product workflows,
- new primary UI surfaces centered on Lite,
- new research promotion authority into Lite,
- new broker, live, autonomous, or manual-capture authority,
- new readiness shortcuts that bypass paper/hypothesis validation artifacts.

Any future change that proposes Lite as a strategic owner must be rejected or escalated to a formal architecture decision record.
