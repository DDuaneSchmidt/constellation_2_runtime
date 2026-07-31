# Aegis Generated Hypothesis Governance Bridge Spec v1

## Scope

The governance bridge converts an approved generated hypothesis into governed research metadata: deterministic sleeve, policy, validation, blueprint, readiness, and tracking setup identifiers. It is metadata-only and cannot create raw signals, candidate contracts, paper observations, outcomes, trades, allocations, broker orders, or safety changes.

## Authoritative Inputs

- hypothesis proposal and promotion pipeline
- evidence packet
- shadow trial
- promotion packet
- paper promotion approval queue / approval event
- generated hypothesis paper setup bridge
- market data universe consistency
- workflow state

## Required Statuses

`GOVERNANCE_BRIDGE_READY` is emitted only when the approval event is present and policy authority is sufficient to map generated-research defaults. Missing approval emits `APPROVAL_EVENT_MISSING`. Missing policy authority emits `GOVERNANCE_POLICY_MAPPING_MISSING`.

## Oil Shock Defaults

For Oil Shock, the deterministic generated metadata is:

- `generated_sleeve_id`: `GENERATED_OIL_SHOCK_REVERSAL_ETF_V1`
- `sleeve_id`: `GENERATED_OIL_SHOCK_REVERSAL_ETF_V1`
- `risk_policy_id`: `GENERATED_RESEARCH_PAPER_RISK_POLICY_V1`
- `exit_policy_id`: `GENERATED_RESEARCH_PAPER_EXIT_POLICY_V1`
- `candidate_construction_policy_id`: `GENERATED_OIL_SHOCK_CANDIDATE_CONSTRUCTION_POLICY_V1`
- `validation_plan_id`: `GENERATED_OIL_SHOCK_VALIDATION_PLAN_V1`

These are generated-research defaults. They authorize paper research metadata only; they do not define new economic entry logic or change candidate, certification, paper lifecycle, validation, trading, or safety gates.

## Downstream Contract

`aegis_generated_hypothesis_paper_setup_bridge_v1` consumes this artifact. If governance is ready, the paper setup bridge may use the deterministic IDs as governed metadata. Oil Shock candidate construction then consumes the paper setup bridge and may proceed only through normal gates.

## Approval Event Lineage Integration

`aegis_generated_hypothesis_approval_event_lineage_v1` is the canonical approval-event input for generated-hypothesis governance. The governance bridge must consume `approval_event`, `approval_event_hash`, source path, and source hash from that lineage artifact instead of inferring approval from queue state. Queue state without an immutable event remains non-authoritative and keeps governance blocked with the lineage status as the reason code.

## Approval Lineage Consumption

Governance readiness consumes `aegis_generated_hypothesis_approval_event_lineage_v1`. When lineage reports `APPROVAL_EVENT_FOUND`, the bridge uses the lineage approval event and hash even if the current approval queue is only a regenerated recommendation state. Queue state alone remains non-authoritative.

## Historical Approval Lineage

Governance may consume `APPROVAL_EVENT_FOUND` from approval lineage even when `source_target_day` precedes `target_day`, provided the lineage artifact includes `HISTORICAL_APPROVAL_EVENT_REUSED` and the source path/hash point to an existing immutable approval event. Governance must not consume future-dated events or approved queue state without immutable lineage.

