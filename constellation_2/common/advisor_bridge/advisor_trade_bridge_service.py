from __future__ import annotations

from typing import Any

from constellation_2.phaseD.lib.canon_json_v1 import canonical_sha256_hex_v1
from constellation_2.common.advisor_execution.blocked_action_v1 import BlockedActionV1
from constellation_2.common.advisor_execution.decision_action_v1 import DecisionActionV1
from constellation_2.common.advisor_execution.decision_plan_v1 import DecisionPlanV1
from constellation_2.common.advisor_execution.planning_snapshot_v1 import PlanningSnapshotV1
from constellation_2.common.advisor_bridge.advisor_trade_intent_proposal_v1 import AdvisorTradeIntentProposalV1
from constellation_2.common.advisor_bridge.advisor_trade_translation_v1 import AdvisorTradeTranslationRecordV1, AdvisorTradeTranslationV1


def compute_run_id(*, artifact_family: str, metadata: dict[str, Any]) -> str:
    payload = {
        "day_utc": metadata["day_utc"],
        "mode": metadata["mode"],
        "artifact_family": artifact_family,
        "source_artifact_refs": sorted(metadata["source_artifact_refs"]),
    }
    return canonical_sha256_hex_v1(payload)


def _base(*, schema_id: str, artifact_family: str, metadata: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_id": schema_id,
        "schema_version": "v1",
        "produced_utc": metadata["produced_utc"],
        "run_id": compute_run_id(artifact_family=artifact_family, metadata=metadata),
    }


def _proposal_id(*, planning_snapshot_id: str, decision_plan_id: str, decision_action_id: str) -> str:
    return canonical_sha256_hex_v1({
        "artifact_family": "advisor_trade_intent_proposal_v1",
        "planning_snapshot_id": planning_snapshot_id,
        "decision_plan_id": decision_plan_id,
        "decision_action_id": decision_action_id,
    })


def _ensure_plan_matches_snapshot(*, decision_plan: DecisionPlanV1, planning_snapshot: PlanningSnapshotV1) -> None:
    if decision_plan.planning_snapshot_id != planning_snapshot.planning_snapshot_id:
        raise ValueError("PLANNING_SNAPSHOT_MISMATCH")


def _selected_amount(action: DecisionActionV1) -> int | None:
    if action.periodicity == "annual" and action.annual_amount > 0:
        return action.annual_amount
    if action.periodic_amount > 0:
        return action.periodic_amount
    if action.annual_amount > 0:
        return action.annual_amount
    return None


def _withdrawal_notes(action: DecisionActionV1) -> tuple[str, ...]:
    notes: list[str] = []
    if action.action_status != "planned":
        notes.append("ACTION_STATUS_NOT_PLANNED")
    if action.support_status not in {"fully_supported", "provisional"}:
        notes.append("SUPPORT_STATUS_NOT_ELIGIBLE")
    if action.blockers:
        notes.extend(f"BLOCKER:{item}" for item in action.blockers)
    if action.source_account is None:
        notes.append("SOURCE_ACCOUNT_MISSING")
    if _selected_amount(action) is None:
        notes.append("AMOUNT_MISSING")
    return tuple(notes)


def _translate_decision_action(action: DecisionActionV1) -> AdvisorTradeTranslationRecordV1:
    if action.action_type == "raise_cash_reserve":
        return AdvisorTradeTranslationRecordV1(action.action_id, action.action_type, "non_trade_action", "non_trade_action", action.source_account, None, action.periodicity, action.rationale_refs, action.evidence_refs, ())
    if action.action_type == "hold_annuity":
        return AdvisorTradeTranslationRecordV1(action.action_id, action.action_type, "non_trade_action", "non_trade_action", action.source_account, None, action.periodicity, action.rationale_refs, action.evidence_refs, ())
    if action.action_type == "collect_missing_input":
        return AdvisorTradeTranslationRecordV1(action.action_id, action.action_type, "blocked", "blocked", action.source_account, None, action.periodicity, action.rationale_refs, action.evidence_refs, tuple(f"BLOCKER:{item}" for item in action.blockers) or ("MISSING_INPUT_REQUIRED",))
    if action.action_type != "withdraw_from_taxable":
        raise ValueError(f"UNSUPPORTED_ACTION_TYPE: {action.action_type}")
    amount = _selected_amount(action)
    notes = _withdrawal_notes(action)
    if notes:
        return AdvisorTradeTranslationRecordV1(action.action_id, action.action_type, "blocked", "candidate_trade_action", action.source_account, amount, action.periodicity, action.rationale_refs, action.evidence_refs, notes)
    return AdvisorTradeTranslationRecordV1(action.action_id, action.action_type, "proposed", "candidate_trade_action", action.source_account, amount, action.periodicity, action.rationale_refs, action.evidence_refs, ("ADVISORY_ONLY_NON_EXECUTING",))


def _translate_blocked_action(action: BlockedActionV1) -> AdvisorTradeTranslationRecordV1:
    if action.action_type in {"raise_cash_reserve", "hold_annuity"}:
        return AdvisorTradeTranslationRecordV1(action.action_id, action.action_type, "non_trade_action", "non_trade_action", None, None, None, action.rationale_refs, action.evidence_refs, tuple(f"BLOCKER:{item}" for item in action.blockers))
    if action.action_type == "collect_missing_input":
        return AdvisorTradeTranslationRecordV1(action.action_id, action.action_type, "blocked", "blocked", None, None, None, action.rationale_refs, action.evidence_refs, tuple(f"BLOCKER:{item}" for item in action.blockers) or ("MISSING_INPUT_REQUIRED",))
    if action.action_type == "withdraw_from_taxable":
        return AdvisorTradeTranslationRecordV1(action.action_id, action.action_type, "blocked", "candidate_trade_action", None, None, None, action.rationale_refs, action.evidence_refs, tuple(f"BLOCKER:{item}" for item in action.blockers) or ("BLOCKED_WITHDRAWAL",))
    raise ValueError(f"UNSUPPORTED_ACTION_TYPE: {action.action_type}")


def build_advisor_trade_translation(*, decision_plan: DecisionPlanV1, planning_snapshot: PlanningSnapshotV1, metadata: dict[str, Any]) -> AdvisorTradeTranslationV1:
    _ensure_plan_matches_snapshot(decision_plan=decision_plan, planning_snapshot=planning_snapshot)
    records = [_translate_decision_action(action).to_dict() for action in decision_plan.actions]
    records.extend(_translate_blocked_action(action).to_dict() for action in decision_plan.blocked_actions)
    obj = _base(schema_id="advisor_trade_translation", artifact_family="advisor_trade_translation_v1", metadata=metadata)
    obj.update({
        "planning_snapshot_id": planning_snapshot.planning_snapshot_id,
        "decision_plan_id": decision_plan.plan_id,
        "translations": records,
    })
    return AdvisorTradeTranslationV1.from_dict(obj)


def build_advisor_trade_intent_proposal(*, decision_plan: DecisionPlanV1, planning_snapshot: PlanningSnapshotV1, metadata: dict[str, Any]) -> AdvisorTradeIntentProposalV1:
    _ensure_plan_matches_snapshot(decision_plan=decision_plan, planning_snapshot=planning_snapshot)
    matching = [action for action in decision_plan.actions if _translate_decision_action(action).translation_status == "proposed"]
    if not matching:
        raise ValueError("NO_PROPOSED_TRADE_ACTION")
    if len(matching) != 1:
        raise ValueError("AMBIGUOUS_PROPOSED_TRADE_ACTIONS")
    action = matching[0]
    amount = _selected_amount(action)
    if action.source_account is None:
        raise ValueError("SOURCE_ACCOUNT_MISSING")
    if amount is None:
        raise ValueError("AMOUNT_MISSING")
    obj = _base(schema_id="advisor_trade_intent_proposal", artifact_family="advisor_trade_intent_proposal_v1", metadata=metadata)
    obj.update({
        "proposal_id": _proposal_id(planning_snapshot_id=planning_snapshot.planning_snapshot_id, decision_plan_id=decision_plan.plan_id, decision_action_id=action.action_id),
        "planning_snapshot_id": planning_snapshot.planning_snapshot_id,
        "decision_plan_id": decision_plan.plan_id,
        "decision_action_id": action.action_id,
        "translation_status": "proposed",
        "proposal_class": "withdrawal_candidate",
        "source_account": action.source_account,
        "proposed_amount_cents": amount,
        "periodicity": action.periodicity,
        "rationale_refs": list(action.rationale_refs),
        "evidence_refs": list(action.evidence_refs),
        "notes": ["ADVISORY_ONLY_NON_EXECUTING"],
    })
    return AdvisorTradeIntentProposalV1.from_dict(obj)
