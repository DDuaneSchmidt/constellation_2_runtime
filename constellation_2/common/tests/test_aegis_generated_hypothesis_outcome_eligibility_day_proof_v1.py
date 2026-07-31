from __future__ import annotations

import json
from pathlib import Path

from ops.aegis.generated_hypothesis_outcome_eligibility_day_proof_v1 import (
    build_generated_hypothesis_outcome_eligibility_day_proof_v1,
    generated_hypothesis_outcome_eligibility_day_proof_path_v1,
    write_generated_hypothesis_outcome_eligibility_day_proof_v1,
)
from ops.aegis.intelligence_common_v1 import write_json_v1

DAY = "2026-06-03"
SAME_DAY = "2026-06-02"
HID = "ehp_cdbd8fe683acb622"
RAW = "c2_oil_shock_reversal_uso_2026-06-02_v1"
CID = "candidate_contract_a3dd44d21952f8098131aa04"
PID = "paper-position:candidate_contract_a3dd44d21952f8098131aa04"
OID = "outcome_cb02619614a054cdf57eb086"
SLEEVE = "C2_OIL_SHOCK_REVERSAL_V1"


def _report(root: Path, day: str, family: str, filename: str) -> Path:
    return root / "reports" / family / day / filename


def _seed(
    root: Path,
    *,
    day: str = DAY,
    position: dict | None = None,
    closure: dict | None = None,
    exit_recommendation: dict | None = None,
    outcome: dict | None = None,
    p18: dict | None = None,
    p19: dict | None = None,
    validation_proof: dict | None = None,
) -> None:
    write_json_v1(_report(root, day, "aegis_paper_position_ledger_v1", "paper_position_ledger.v1.json"), {"positions": [position] if position else [], "open_positions": [position] if position else []})
    write_json_v1(_report(root, day, "aegis_paper_outcome_auto_closure_v1", "paper_outcome_auto_closure.v1.json"), {"rows": [closure] if closure else []})
    write_json_v1(_report(root, day, "aegis_exit_recommendations_v1", "exit_recommendations.v1.json"), {"recommendations": [exit_recommendation] if exit_recommendation else []})
    write_json_v1(_report(root, day, "aegis_outcome_registry_v1", "outcome_registry.v1.json"), {"outcomes": [outcome] if outcome else []})
    write_json_v1(_report(root, day, "aegis_generated_hypothesis_paper_observation_to_outcome_v1", "generated_hypothesis_paper_observation_to_outcome.v1.json"), p18 or {"summary": {"generated_hypothesis_id": HID, "sleeve_id": SLEEVE, "candidate_contract_id": CID, "paper_observation_id": PID, "paper_position_id": PID, "outcome_row_created": False, "outcome_status": "OUTCOME_NOT_READY", "exit_price_source": "market_data.jsonl"}})
    write_json_v1(_report(root, day, "aegis_generated_hypothesis_outcome_maturity_monitor_v1", "generated_hypothesis_outcome_maturity_monitor.v1.json"), p19 or {"summary": {"total_open_generated_hypothesis_observations": 1}})
    write_json_v1(_report(root, day, "aegis_generated_hypothesis_validation_proof_v1", "generated_hypothesis_validation_proof.v1.json"), validation_proof or {"summary": {"furthest_stage_reached": "PAPER_OBSERVATION_FLOW", "outcome_flow_state": "BLOCKED", "outcome_row_created": False}})


def _position(*, entry_time: str = "2026-06-02T20:11:45Z") -> dict:
    return {
        "candidate_id": CID,
        "position_id": PID,
        "hypothesis_id": HID,
        "sleeve_id": SLEEVE,
        "entry_price": "135.3914",
        "entry_time": entry_time,
        "originating_day": entry_time[:10],
        "paper_tracking_mode": "AUTO_PROMOTED_RESEARCH_OBSERVATION",
        "candidate_lineage": {"candidate_id": CID, "raw_signal_id": RAW, "hypothesis_id": HID, "sleeve_id": SLEEVE},
    }


def _closure(
    *,
    recommendation: str = "HOLD",
    trigger: str | None = None,
    reasons: list[str] | None = None,
    exit_mark=None,
    source: str = "market_data.jsonl",
    timestamp: str = "2026-06-03T20:00:00Z",
    state: str = "AUTO_CLOSURE_NOT_ELIGIBLE",
    cert: str = "CERTIFIED",
) -> dict:
    return {
        "candidate_id": CID,
        "position_id": PID,
        "hypothesis_id": HID,
        "sleeve_id": SLEEVE,
        "auto_closure_state": state,
        "lifecycle_state": state,
        "auto_closure_reason_codes": reasons if reasons is not None else ["HOLD_RECOMMENDATION"],
        "entry_mark": 135.3914,
        "exit_mark": exit_mark,
        "exit_price_source_artifact": source,
        "exit_price_timestamp": timestamp if source else "",
        "exit_mark_certification_status": cert,
        "exit_recommendation": recommendation,
        "exit_trigger": trigger if trigger is not None else ("NO_EXIT_RULE_TRIGGERED" if recommendation == "HOLD" else "STOP_LOSS_THRESHOLD_REACHED"),
    }


def _exit_rec(recommendation: str = "HOLD") -> dict:
    return {"candidate_id": CID, "position_id": PID, "sleeve_id": SLEEVE, "exit_recommendation": recommendation}


def _open_outcome() -> dict:
    return {"candidate_id": CID, "position_id": PID, "outcome_id": OID, "outcome_state": "OPEN", "hypothesis_id": HID, "sleeve_id": SLEEVE, "entry_mark": 135.3914, "exit_mark": None}


def _closed_outcome() -> dict:
    return {**_open_outcome(), "outcome_state": "CLOSED", "exit_mark": 130.0, "exit_trigger": "STOP_LOSS_THRESHOLD_REACHED", "trigger_timestamp": "2026-06-03T20:00:00Z"}


def _row(payload: dict) -> dict:
    return payload["oil_shock"]


def test_observation_is_same_day_and_not_eligible(tmp_path: Path) -> None:
    _seed(tmp_path, day=SAME_DAY, position=_position(), closure=_closure(reasons=["SAME_DAY_AUTO_PROMOTED_RESEARCH_OBSERVATION"], timestamp="2026-06-02T20:00:00Z"), exit_recommendation=_exit_rec(), outcome=_open_outcome())
    row = _row(build_generated_hypothesis_outcome_eligibility_day_proof_v1(truth_root=tmp_path, day_utc=SAME_DAY))
    assert row["holding_period_status"] == "NOT_ELAPSED"
    assert row["age_in_calendar_days"] == 0
    assert row["age_in_trading_days"] == 0
    assert row["minimum_holding_period"] == 1
    assert row["blocker_code"] == "HOLDING_PERIOD_NOT_ELAPSED"
    assert row["outcome_row_created"] is False


def test_observation_is_one_trading_day_old_and_eligible(tmp_path: Path) -> None:
    _seed(tmp_path, position=_position(), closure=_closure(exit_mark=136.0), exit_recommendation=_exit_rec(), outcome=_open_outcome())
    row = _row(build_generated_hypothesis_outcome_eligibility_day_proof_v1(truth_root=tmp_path, day_utc=DAY))
    assert row["holding_period_status"] == "ELAPSED"
    assert row["age_in_calendar_days"] == 1
    assert row["age_in_trading_days"] == 1
    assert row["minimum_holding_period"] == 1
    assert row["blocker_code"] == "CLOSE_CONDITION_NOT_MET"


def test_outcome_created_only_when_deterministic_close_inputs_exist(tmp_path: Path) -> None:
    _seed(
        tmp_path,
        position=_position(),
        closure=_closure(recommendation="EXIT", reasons=["STOP_LOSS_THRESHOLD_REACHED"], exit_mark=130.0, state="AUTO_CLOSED_PAPER_OUTCOME"),
        exit_recommendation=_exit_rec("EXIT"),
        outcome=_closed_outcome(),
        p18={"summary": {"outcome_row_created": True, "outcome_status": "OUTCOME_CREATED", "outcome_id": OID}},
    )
    row = _row(build_generated_hypothesis_outcome_eligibility_day_proof_v1(truth_root=tmp_path, day_utc=DAY))
    assert row["holding_period_status"] == "ELAPSED"
    assert row["outcome_readiness_status"] == "READY"
    assert row["outcome_status"] == "OUTCOME_CREATED"
    assert row["outcome_row_created"] is True
    assert row["furthest_stage_reached"] == "OUTCOME_FLOW"
    assert row["remaining_blocker"] == "NONE"
    assert row["validation_proof_advancement_allowed"] is True


def test_missing_mark_data_blocks_with_mark_data_missing(tmp_path: Path) -> None:
    _seed(tmp_path, position=_position(), closure=_closure(trigger="MISSING_CURRENT_MARK", reasons=["MISSING_CURRENT_MARK"], source="market_data.jsonl", timestamp="", cert="MISSING_MARK"), exit_recommendation=_exit_rec(), outcome=_open_outcome())
    row = _row(build_generated_hypothesis_outcome_eligibility_day_proof_v1(truth_root=tmp_path, day_utc=DAY))
    assert row["holding_period_status"] == "ELAPSED"
    assert row["mark_data_status"] == "MISSING"
    assert row["blocker_code"] == "MARK_DATA_MISSING"
    assert row["david_action_required"] is False


def test_missing_exit_price_blocks_with_exit_price_missing(tmp_path: Path) -> None:
    _seed(tmp_path, position=_position(), closure=_closure(recommendation="EXIT", reasons=["MISSING_EXIT_MARK"], exit_mark=None, trigger="STOP_LOSS_THRESHOLD_REACHED"), exit_recommendation=_exit_rec("EXIT"), outcome=_open_outcome())
    row = _row(build_generated_hypothesis_outcome_eligibility_day_proof_v1(truth_root=tmp_path, day_utc=DAY))
    assert row["close_condition_status"] == "MET"
    assert row["mark_data_status"] == "EXIT_PRICE_MISSING"
    assert row["blocker_code"] == "EXIT_PRICE_MISSING"
    assert "exit_price" in row["missing_fields"]


def test_missing_close_rule_blocks_with_close_rule_missing(tmp_path: Path) -> None:
    _seed(tmp_path, position=_position(), closure=None, exit_recommendation=None, outcome=_open_outcome())
    row = _row(build_generated_hypothesis_outcome_eligibility_day_proof_v1(truth_root=tmp_path, day_utc=DAY))
    assert row["close_rule_status"] == "MISSING"
    assert row["blocker_code"] == "CLOSE_RULE_MISSING"
    assert "exit_recommendation" in row["missing_fields"]


def test_close_condition_not_met_blocks_with_close_condition_not_met(tmp_path: Path) -> None:
    _seed(tmp_path, position=_position(), closure=_closure(recommendation="HOLD", trigger="NO_EXIT_RULE_TRIGGERED", exit_mark=136.0), exit_recommendation=_exit_rec("HOLD"), outcome=_open_outcome())
    row = _row(build_generated_hypothesis_outcome_eligibility_day_proof_v1(truth_root=tmp_path, day_utc=DAY))
    assert row["mark_data_status"] == "PRESENT"
    assert row["close_condition_status"] == "NOT_MET"
    assert row["blocker_code"] == "CLOSE_CONDITION_NOT_MET"
    assert row["outcome_row_created"] is False


def test_validation_proof_advances_only_when_outcome_row_exists(tmp_path: Path) -> None:
    validation = {"summary": {"furthest_stage_reached": "PAPER_OBSERVATION_FLOW", "outcome_flow_state": "BLOCKED", "outcome_row_created": False}}
    _seed(tmp_path, position=_position(), closure=_closure(recommendation="EXIT", exit_mark=None), exit_recommendation=_exit_rec("EXIT"), outcome=_open_outcome(), validation_proof=validation)
    open_payload = build_generated_hypothesis_outcome_eligibility_day_proof_v1(truth_root=tmp_path, day_utc=DAY)
    assert open_payload["oil_shock"]["validation_proof_advancement_allowed"] is False
    _seed(tmp_path, position=_position(), closure=_closure(recommendation="EXIT", exit_mark=130.0, state="AUTO_CLOSED_PAPER_OUTCOME"), exit_recommendation=_exit_rec("EXIT"), outcome=_closed_outcome(), validation_proof=validation)
    closed_payload = build_generated_hypothesis_outcome_eligibility_day_proof_v1(truth_root=tmp_path, day_utc=DAY)
    proof_path = _report(tmp_path, DAY, "aegis_generated_hypothesis_validation_proof_v1", "generated_hypothesis_validation_proof.v1.json")
    proof = json.loads(proof_path.read_text(encoding="utf-8"))
    assert closed_payload["oil_shock"]["validation_proof_advancement_allowed"] is True
    assert proof["summary"]["furthest_stage_reached"] == "PAPER_OBSERVATION_FLOW"


def test_no_validation_sample_or_research_quality_result_is_fabricated(tmp_path: Path) -> None:
    _seed(tmp_path, position=_position(), closure=_closure(recommendation="EXIT", exit_mark=130.0), exit_recommendation=_exit_rec("EXIT"), outcome=_closed_outcome())
    payload = build_generated_hypothesis_outcome_eligibility_day_proof_v1(truth_root=tmp_path, day_utc=DAY)
    row = payload["oil_shock"]
    assert payload["validation_sample_created_by_this_artifact"] is False
    assert payload["research_quality_result_created_by_this_artifact"] is False
    assert row["validation_sample_created_by_this_artifact"] is False
    assert row["research_quality_result_created_by_this_artifact"] is False


def test_safety_gates_remain_unchanged_and_artifact_write_path(tmp_path: Path) -> None:
    _seed(tmp_path, position=_position(), closure=_closure(), exit_recommendation=_exit_rec(), outcome=_open_outcome())
    payload = build_generated_hypothesis_outcome_eligibility_day_proof_v1(truth_root=tmp_path, day_utc=DAY)
    row = payload["oil_shock"]
    assert payload["read_only"] is True
    assert payload["trade_advice_allowed"] is False
    assert payload["broker_execution_allowed"] is False
    assert payload["safety_gates_changed"] is False
    assert row["outcome_created_by_this_artifact"] is False
    path = write_generated_hypothesis_outcome_eligibility_day_proof_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)
    assert path == generated_hypothesis_outcome_eligibility_day_proof_path_v1(truth_root=tmp_path, day_utc=DAY)
    assert path.exists()
