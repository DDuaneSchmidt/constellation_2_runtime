from __future__ import annotations

import json
from pathlib import Path

from ops.aegis.generated_hypothesis_outcome_maturity_monitor_v1 import (
    build_generated_hypothesis_outcome_maturity_monitor_v1,
    generated_hypothesis_outcome_maturity_monitor_path_v1,
    write_generated_hypothesis_outcome_maturity_monitor_v1,
)
from ops.aegis.intelligence_common_v1 import write_json_v1

DAY = "2026-06-02"
HID = "ehp_test_generated"
RAW = "generated-signal-1"
CID = "candidate_contract_generated_1"
PID = "paper-position:generated-1"
OID = "outcome:generated-1"
SLEEVE = "C2_TEST_GENERATED_HYPOTHESIS_V1"


def _report(root: Path, family: str, filename: str) -> Path:
    return root / "reports" / family / DAY / filename


def _seed(
    root: Path,
    *,
    position: dict | None = None,
    closure: dict | None = None,
    exit_recommendation: dict | None = None,
    outcome: dict | None = None,
    validation_proof: dict | None = None,
) -> None:
    write_json_v1(_report(root, "aegis_paper_position_ledger_v1", "paper_position_ledger.v1.json"), {"positions": [position] if position else [], "open_positions": [position] if position else []})
    write_json_v1(_report(root, "aegis_paper_outcome_auto_closure_v1", "paper_outcome_auto_closure.v1.json"), {"rows": [closure] if closure else []})
    write_json_v1(_report(root, "aegis_exit_recommendations_v1", "exit_recommendations.v1.json"), {"recommendations": [exit_recommendation] if exit_recommendation else []})
    write_json_v1(_report(root, "aegis_outcome_registry_v1", "outcome_registry.v1.json"), {"outcomes": [outcome] if outcome else []})
    write_json_v1(_report(root, "aegis_generated_hypothesis_paper_observation_to_outcome_v1", "generated_hypothesis_paper_observation_to_outcome.v1.json"), {"summary": {"outcome_row_created": False}})
    write_json_v1(_report(root, "aegis_generated_hypothesis_validation_proof_v1", "generated_hypothesis_validation_proof.v1.json"), validation_proof or {"summary": {"furthest_stage_reached": "PAPER_OBSERVATION_FLOW", "outcome_flow_state": "BLOCKED"}})


def _position(*, entry_time: str = "2026-06-02T15:00:00Z", lineage: dict | None = None, tracking_mode: str = "AUTO_PROMOTED_RESEARCH_OBSERVATION") -> dict:
    return {
        "candidate_id": CID,
        "position_id": PID,
        "hypothesis_id": HID,
        "sleeve_id": SLEEVE,
        "entry_price": "100.00",
        "entry_time": entry_time,
        "originating_day": entry_time[:10],
        "paper_tracking_mode": tracking_mode,
        "candidate_lineage": lineage if lineage is not None else {"candidate_id": CID, "raw_signal_id": RAW, "hypothesis_id": HID, "sleeve_id": SLEEVE},
    }


def _closure(*, recommendation: str = "HOLD", reasons: list[str] | None = None, exit_mark=None, source: str = "market_data.jsonl", state: str = "AUTO_CLOSURE_NOT_ELIGIBLE", trigger: str | None = None) -> dict:
    return {
        "candidate_id": CID,
        "position_id": PID,
        "hypothesis_id": HID,
        "sleeve_id": SLEEVE,
        "auto_closure_state": state,
        "lifecycle_state": state,
        "auto_closure_reason_codes": reasons if reasons is not None else ["HOLD_RECOMMENDATION"],
        "entry_mark": 100.0,
        "exit_mark": exit_mark,
        "exit_price_source_artifact": source,
        "exit_price_timestamp": "2026-06-02T20:00:00Z" if source else "",
        "exit_recommendation": recommendation,
        "exit_trigger": trigger if trigger is not None else ("NO_EXIT_RULE_TRIGGERED" if recommendation == "HOLD" else "STOP_LOSS_THRESHOLD_REACHED"),
    }


def _exit_rec(recommendation: str = "HOLD") -> dict:
    return {"candidate_id": CID, "position_id": PID, "sleeve_id": SLEEVE, "exit_recommendation": recommendation}


def _open_outcome() -> dict:
    return {"candidate_id": CID, "position_id": PID, "outcome_id": OID, "outcome_state": "OPEN", "hypothesis_id": HID, "sleeve_id": SLEEVE, "entry_mark": 100.0, "exit_mark": None}


def _first(payload: dict) -> dict:
    rows = payload["open_generated_hypothesis_observations"]
    assert len(rows) == 1
    return rows[0]


def test_same_day_paper_observation_reports_holding_period_not_elapsed(tmp_path: Path) -> None:
    _seed(tmp_path, position=_position(), closure=_closure(reasons=["HOLD_RECOMMENDATION", "SAME_DAY_AUTO_PROMOTED_RESEARCH_OBSERVATION"]), exit_recommendation=_exit_rec(), outcome=_open_outcome())
    payload = build_generated_hypothesis_outcome_maturity_monitor_v1(truth_root=tmp_path, day_utc=DAY)
    row = _first(payload)
    assert row["outcome_readiness_status"] == "HOLDING_PERIOD_NOT_ELAPSED"
    assert row["holding_period_status"] == "NOT_ELAPSED"
    assert row["age_in_trading_days"] == 0
    assert row["minimum_holding_period"] == 1
    assert row["earliest_outcome_eligible_date"] == "2026-06-03"
    assert row["expected_next_check_date"] == "2026-06-03"
    assert payload["portfolio_summary"]["holding_period_not_elapsed_count"] == 1


def test_matured_observation_reports_outcome_ready_when_close_inputs_exist(tmp_path: Path) -> None:
    _seed(tmp_path, position=_position(entry_time="2026-06-01T15:00:00Z"), closure=_closure(recommendation="EXIT", reasons=["STOP_LOSS_THRESHOLD_REACHED"], exit_mark=97.0, state="AUTO_CLOSURE_READY"), exit_recommendation=_exit_rec("EXIT"), outcome=_open_outcome())
    row = _first(build_generated_hypothesis_outcome_maturity_monitor_v1(truth_root=tmp_path, day_utc=DAY))
    assert row["outcome_readiness_status"] == "OUTCOME_READY"
    assert row["remaining_blocker"] == "NONE"
    assert row["exit_condition_status"] == "MET"
    assert row["stop_condition_status"] == "TRIGGERED"
    assert row["expected_next_check_date"] == DAY


def test_missing_market_data_reports_waiting_for_market_data(tmp_path: Path) -> None:
    _seed(tmp_path, position=_position(entry_time="2026-06-01T15:00:00Z"), closure=_closure(recommendation="EXIT", reasons=[], source="", trigger="NO_EXIT_RULE_TRIGGERED"), exit_recommendation=_exit_rec("EXIT"), outcome=_open_outcome())
    row = _first(build_generated_hypothesis_outcome_maturity_monitor_v1(truth_root=tmp_path, day_utc=DAY))
    assert row["outcome_readiness_status"] == "WAITING_FOR_MARKET_DATA"
    assert row["mark_data_status"] == "MISSING"
    assert row["owner"] == "AEGIS_SYSTEM"
    assert row["david_action_required"] is False


def test_missing_close_rule_reports_close_rule_missing(tmp_path: Path) -> None:
    _seed(tmp_path, position=_position(entry_time="2026-06-01T15:00:00Z"), closure=None, exit_recommendation=None, outcome=_open_outcome())
    row = _first(build_generated_hypothesis_outcome_maturity_monitor_v1(truth_root=tmp_path, day_utc=DAY))
    assert row["outcome_readiness_status"] == "CLOSE_RULE_MISSING"
    assert row["close_rule_status"] == "MISSING"
    assert row["remaining_blocker"] == "CLOSE_RULE_MISSING"


def test_missing_exit_price_reports_exit_price_missing(tmp_path: Path) -> None:
    _seed(tmp_path, position=_position(entry_time="2026-06-01T15:00:00Z"), closure=_closure(recommendation="EXIT", reasons=["MISSING_EXIT_MARK"], exit_mark=None, source="market_data.jsonl"), exit_recommendation=_exit_rec("EXIT"), outcome=_open_outcome())
    row = _first(build_generated_hypothesis_outcome_maturity_monitor_v1(truth_root=tmp_path, day_utc=DAY))
    assert row["outcome_readiness_status"] == "EXIT_PRICE_MISSING"
    assert row["remaining_blocker"] == "EXIT_PRICE_MISSING"


def test_lineage_mismatch_reports_lineage_mismatch(tmp_path: Path) -> None:
    bad_position = _position(entry_time="2026-06-01T15:00:00Z", lineage={}, tracking_mode="AUTO_PROMOTED_RESEARCH_OBSERVATION")
    bad_position.pop("hypothesis_id")
    _seed(tmp_path, position=bad_position, closure=_closure(), exit_recommendation=_exit_rec(), outcome=_open_outcome())
    row = _first(build_generated_hypothesis_outcome_maturity_monitor_v1(truth_root=tmp_path, day_utc=DAY))
    assert row["outcome_readiness_status"] == "LINEAGE_MISMATCH"
    assert row["lineage_status"] == "MISMATCH"


def test_monitor_does_not_create_outcomes(tmp_path: Path) -> None:
    _seed(tmp_path, position=_position(entry_time="2026-06-01T15:00:00Z"), closure=_closure(recommendation="EXIT", reasons=["STOP_LOSS_THRESHOLD_REACHED"], exit_mark=97.0, state="AUTO_CLOSURE_READY"), exit_recommendation=_exit_rec("EXIT"), outcome=_open_outcome())
    payload = build_generated_hypothesis_outcome_maturity_monitor_v1(truth_root=tmp_path, day_utc=DAY)
    path = write_generated_hypothesis_outcome_maturity_monitor_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)
    outcome_path = _report(tmp_path, "aegis_outcome_registry_v1", "outcome_registry.v1.json")
    outcome_payload = json.loads(outcome_path.read_text(encoding="utf-8"))
    assert outcome_payload["outcomes"][0]["outcome_state"] == "OPEN"
    assert payload["outcome_created_by_this_artifact"] is False
    assert path == generated_hypothesis_outcome_maturity_monitor_path_v1(truth_root=tmp_path, day_utc=DAY)


def test_monitor_does_not_advance_validation_proof(tmp_path: Path) -> None:
    validation = {"summary": {"furthest_stage_reached": "PAPER_OBSERVATION_FLOW", "outcome_flow_state": "BLOCKED", "outcome_row_created": False}}
    _seed(tmp_path, position=_position(entry_time="2026-06-01T15:00:00Z"), closure=_closure(recommendation="EXIT", reasons=["STOP_LOSS_THRESHOLD_REACHED"], exit_mark=97.0, state="AUTO_CLOSURE_READY"), exit_recommendation=_exit_rec("EXIT"), outcome=_open_outcome(), validation_proof=validation)
    payload = build_generated_hypothesis_outcome_maturity_monitor_v1(truth_root=tmp_path, day_utc=DAY)
    proof_path = _report(tmp_path, "aegis_generated_hypothesis_validation_proof_v1", "generated_hypothesis_validation_proof.v1.json")
    proof = json.loads(proof_path.read_text(encoding="utf-8"))
    assert payload["validation_proof_advancement_allowed"] is False
    assert proof["summary"]["furthest_stage_reached"] == "PAPER_OBSERVATION_FLOW"
    assert proof["summary"]["outcome_flow_state"] == "BLOCKED"


def test_safety_gates_remain_unchanged(tmp_path: Path) -> None:
    _seed(tmp_path, position=_position(), closure=_closure(), exit_recommendation=_exit_rec(), outcome=_open_outcome())
    payload = build_generated_hypothesis_outcome_maturity_monitor_v1(truth_root=tmp_path, day_utc=DAY)
    row = _first(payload)
    assert payload["read_only"] is True
    assert payload["monitoring_only"] is True
    assert payload["trade_advice_allowed"] is False
    assert payload["broker_execution_allowed"] is False
    assert payload["safety_gates_changed"] is False
    assert row["outcome_created_by_this_artifact"] is False
    assert row["validation_sample_created_by_this_artifact"] is False
