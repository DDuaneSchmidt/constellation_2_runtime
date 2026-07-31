# Aegis Lite Operating Status Migration Requirements v1

## Purpose

Move runtime-truth authority for `aegis_lite_operating_status` and `aegis_lite_eod_report` from Aegis Lite to the strategic Paper Trading + Hypothesis Validation architecture without removing Aegis Lite.

Aegis Lite remains a `LEGACY_COMPATIBILITY_LAYER`. The strategic system of record is Paper Trading + Hypothesis Validation.

## Scope

In scope:
- `aegis_lite_operating_status`
- `aegis_lite_eod_report`
- strategic replacement projections that compose existing strategic evidence
- runtime-truth dependency migration only after equivalence passes

Out of scope:
- `manual_trade_packet`
- `operator_execution_queue`
- `manual_execution_receipt`
- trade advice enablement
- manual capture enablement
- broker execution
- live/autonomous trading

## Current Lite Responsibilities

`aegis_lite_operating_status` currently proves that the Lite EOD timer/service configuration exists, legacy paper timers are deferred, the latest Lite EOD report and queue are linked, release integrity is recorded, and broker/manual policy flags remain safe.

`aegis_lite_eod_report` currently summarizes EOD readiness, candidate consumption, blockers, report status, selected trade candidates, manual execution status, source lineage, and no-broker/no-autonomous policy assertions.

## Consumers

Known consumers include:
- `ops/aegis/runtime_truth_kernel_v1.py`
- `ops/aegis/pure_runtime_evaluator_v1.py`
- `ops/aegis/mode_readiness_v1.py`
- `constellation_2/phaseL/ui_api/aegis_lite_execution_queue_read_model.py`
- legacy Lite UI and tests

## Runtime Truth Dependencies

Before migration, `DATA_READY` depends on:
- `aegis_lite_operating_status`
- `aegis_lite_eod_report`
- `operator_execution_queue`
- `event_market_snapshot`

This migration may replace only the first two with strategic artifacts.

## Strategic Replacement Requirements

The replacements must compose existing strategic truth rather than create duplicate authority.

Required strategic projections:
- `aegis_strategic_operating_status_v1`
- `aegis_research_eod_summary_v1`

They must answer:
- what ran today
- what was ready
- what was blocked
- candidate generation result
- paper monitoring result
- outcome/validation state
- research allocation state
- David action state
- trade advice/manual capture/broker execution policy state

## Equivalence Criteria

Migration is safe only if the equivalence checker proves:
- replacement artifacts are current for the requested day
- replacement artifacts include required evidence fields
- replacement artifacts preserve safe policy flags
- replacement artifacts cover Lite operating status responsibilities
- replacement artifacts cover Lite EOD report responsibilities
- no manual packet, manual capture, broker, live, autonomous, or trade-advice behavior changes

## Migration Risks

Risks:
- runtime truth could stop seeing blockers that Lite used to surface
- non-trading-day semantics could be misclassified
- UI could overstate readiness if strategic projections are too optimistic
- audit sequencing could evaluate runtime truth before replacement artifacts are generated

Mitigation:
- equivalence checker is blocking
- audit builds strategic projections before final truth evaluation
- Lite artifacts remain available as compatibility evidence
- runtime truth remains fail-closed if strategic evidence is missing

## Rollback Plan

If audit or equivalence fails:
- leave Lite artifacts generated
- restore runtime truth `DATA_READY` dependencies to Lite status/report
- keep strategic projections as diagnostics only
- do not remove or alter manual packet dependencies

## Safety Rules

This migration must not enable:
- trade advice
- manual capture
- broker submit/transmit
- live trading
- autonomous trading
