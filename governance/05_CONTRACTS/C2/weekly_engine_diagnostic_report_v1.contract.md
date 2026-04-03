# Weekly Engine Diagnostic Report v1 Contract

id: C2_WEEKLY_ENGINE_DIAGNOSTIC_REPORT_CONTRACT_V1
status: DRAFT
version: 1
owner: research
type: contract

## Purpose

This contract defines the deterministic, read-only weekly diagnostic report
generator for Constellation 2.0.

This report is a research and diagnostics artifact only.

It does not control trading.
It does not modify runtime truth.
It does not place orders.
It does not adapt parameters automatically.

## Authoritative input surfaces proven for v1

The generator may consume only the following proven surfaces:

- `constellation_2/runtime/truth/accounting_v2/nav/<DAY>/nav.v2.json`
- `constellation_2/runtime/truth/accounting_v2/attribution/<DAY>/engine_attribution.v2.json`
- `constellation_2/runtime/truth/intents_v1/day_rollup/<DAY>/intents_day_rollup.v1.json`
- `constellation_2/runtime/truth/fill_ledger_v1/<DAY>/*.fill_ledger.v1.json`
- `constellation_2/runtime/truth/monitoring_v1/engine_daily_returns_v1/<DAY>/engine_daily_returns.v1.json`
- `constellation_2/runtime/truth/monitoring_v1/engine_correlation_matrix/<DAY>/engine_correlation_matrix.v1.json`
- `constellation_2/runtime/truth/monitoring_v1/engine_heartbeat_v1/<DAY>/<ENGINE>/engine_heartbeat.v1.json`
- `constellation_2/runtime/truth/allocation_v1/decisions/<DAY>/*.allocation_decision.v1.json`

## Invalid artifact handling

The generator must ignore files with `.INVALID_` in the filename for metric computation.

The generator must still report invalid artifact counts in Section 1
and Section 11 as dataset integrity warnings.

## Output contract

The generator output is markdown text only.

The markdown must preserve this exact section order:

1. System Overview
2. Engine Activity Analysis
3. Intent Pipeline Diagnostics
4. Parameter Efficiency Review
5. Risk and Capital Efficiency
6. Trade Outcome Diagnostics
7. Regime Analysis
8. Engine Health Table
9. Parameter Adjustment Recommendations
10. Sandbox Experiment Proposals
11. Critical Risks
12. Weekly Summary

## Deterministic behavior

The generator must:

- require `--week-end YYYY-MM-DD`
- default to a 7-calendar-day inclusive window
- operate fail-closed on missing truth root
- operate read-only
- emit explicit warnings for missing daily artifacts
- avoid any adaptive, probabilistic, or AI-driven trading logic
- derive all conclusions only from the proven surfaces above

## Required limitations for v1

If a metric cannot be computed from the proven input surfaces, the generator
must print:

`NOT_AVAILABLE_FROM_PROVEN_INPUTS`

This applies to any field that would otherwise require unproven parameter
surfaces, candidate-opportunity surfaces, symbol-level trade outcomes,
or regime artifacts not explicitly proven for this generator version.

## Minimal grading posture for v1

Engine grading in v1 is diagnostic and conservative.

Grade inputs may use only:

- intent counts
- allocation decision counts
- fill counts
- engine heartbeat status
- engine daily return proxy when present

If trade-level expectancy is not provable from current surfaces,
the report must explicitly label the expectancy field as:

`EXPECTANCY_NOT_PROVEN_V1`

## Tool binding

Authoritative tool path for this contract:

- `ops/tools/run_weekly_engine_diagnostic_report_v1.py`

## Safety boundary

This report is strictly research and diagnostics.
It must never be used as trading control.
