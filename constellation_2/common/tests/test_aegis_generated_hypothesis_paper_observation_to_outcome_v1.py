from __future__ import annotations

from pathlib import Path

from ops.aegis.generated_hypothesis_paper_observation_to_outcome_v1 import (
    build_generated_hypothesis_paper_observation_to_outcome_v1,
    generated_hypothesis_paper_observation_to_outcome_path_v1,
    write_generated_hypothesis_paper_observation_to_outcome_v1,
)
from ops.aegis.intelligence_common_v1 import write_json_v1

DAY = "2026-06-02"
HID = "ehp_cdbd8fe683acb622"
RAW = "c2_oil_shock_reversal_uso_2026-06-02_v1"
CID = "candidate_contract_a3dd44d21952f8098131aa04"
PID = "paper-position:oil"
OID = "outcome:oil"
SID = "sample:oil"
SLEEVE = "C2_OIL_SHOCK_REVERSAL_V1"


def _report(root: Path, family: str, filename: str) -> Path:
    return root / "reports" / family / DAY / filename


def _seed(
    root: Path,
    *,
    candidate_to_paper: dict | None = None,
    lifecycle: dict | None = None,
    position: dict | None = None,
    outcome: dict | None = None,
    closure: dict | None = None,
    exit_recommendation: dict | None = None,
    sample: dict | None = None,
    quality: dict | None = None,
    performance: dict | None = None,
) -> None:
    write_json_v1(_report(root, "aegis_generated_hypothesis_candidate_to_paper_v1", "generated_hypothesis_candidate_to_paper.v1.json"), {"oil_shock": candidate_to_paper or _candidate_to_paper()})
    write_json_v1(_report(root, "aegis_candidate_to_paper_lifecycle_v1", "candidate_to_paper_lifecycle.v1.json"), {"rows": [lifecycle or _lifecycle()]})
    write_json_v1(_report(root, "aegis_paper_position_ledger_v1", "paper_position_ledger.v1.json"), {"positions": [position] if position else [], "open_positions": [position] if position else []})
    write_json_v1(_report(root, "aegis_paper_outcome_auto_closure_v1", "paper_outcome_auto_closure.v1.json"), {"rows": [closure] if closure else []})
    write_json_v1(_report(root, "aegis_exit_recommendations_v1", "exit_recommendations.v1.json"), {"recommendations": [exit_recommendation] if exit_recommendation else []})
    write_json_v1(_report(root, "aegis_outcome_registry_v1", "outcome_registry.v1.json"), {"outcomes": [outcome] if outcome else []})
    write_json_v1(_report(root, "aegis_validation_samples_v1", "validation_samples.v1.json"), {"samples": [sample] if sample else []})
    write_json_v1(_report(root, "aegis_outcome_performance_metrics_v1", "outcome_performance_metrics.v1.json"), {"hypotheses": [performance] if performance else []})
    write_json_v1(_report(root, "aegis_research_quality_engine_v1", "research_quality_engine.v1.json"), {"hypotheses": [quality] if quality else []})


def _candidate_to_paper() -> dict:
    return {
        "hypothesis_id": HID,
        "hypothesis_name": "Oil shock reversals across energy ETFs",
        "raw_signal_id": RAW,
        "candidate_id": CID,
        "paper_position_id": PID,
        "outcome_id": OID,
        "candidate_to_paper_status": "PAPER_OBSERVATION_CREATED",
        "paper_construction_missing_fields": [],
        "reason_codes": [],
        "current_stop_reason": "NONE",
    }


def _lifecycle() -> dict:
    return {
        "hypothesis_id": HID,
        "raw_signal_id": RAW,
        "candidate_id": CID,
        "paper_position_id": PID,
        "outcome_id": OID,
        "blocker_reason_codes": [],
    }


def _position() -> dict:
    return {
        "candidate_id": CID,
        "position_id": PID,
        "hypothesis_id": HID,
        "sleeve_id": SLEEVE,
        "entry_price": "135.3914",
        "entry_time": "2026-06-02T20:11:45Z",
        "originating_day": DAY,
        "paper_tracking_mode": "AUTO_PROMOTED_RESEARCH_OBSERVATION",
        "candidate_lineage": {"candidate_id": CID, "raw_signal_id": RAW, "hypothesis_id": HID, "sleeve_id": SLEEVE},
    }


def _open_outcome() -> dict:
    return {"candidate_id": CID, "position_id": PID, "outcome_id": OID, "outcome_state": "OPEN", "hypothesis_id": HID, "sleeve_id": SLEEVE, "entry_mark": 135.3914, "exit_mark": None}


def _closure(*, state: str = "AUTO_CLOSURE_NOT_ELIGIBLE", reasons: list[str] | None = None, exit_mark=None, source: str = "market_data.jsonl", recommendation: str = "HOLD") -> dict:
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
        "exit_price_timestamp": "2026-06-02T17:47:22Z" if source else "",
        "exit_mark_certification_status": "CERTIFIED",
        "exit_recommendation": recommendation,
        "exit_trigger": "NO_EXIT_RULE_TRIGGERED" if recommendation == "HOLD" else "STOP_LOSS_THRESHOLD_REACHED",
    }


def _exit_rec(recommendation: str = "HOLD") -> dict:
    return {"candidate_id": CID, "position_id": PID, "sleeve_id": SLEEVE, "exit_recommendation": recommendation}


def test_authoritative_paper_observation_found_and_required_fields_reported(tmp_path: Path) -> None:
    _seed(tmp_path, position=_position(), outcome=_open_outcome(), closure=_closure(reasons=["HOLD_RECOMMENDATION", "SAME_DAY_AUTO_PROMOTED_RESEARCH_OBSERVATION"]), exit_recommendation=_exit_rec())
    row = build_generated_hypothesis_paper_observation_to_outcome_v1(truth_root=tmp_path, day_utc=DAY)["oil_shock"]
    assert row["paper_observation_found"] is True
    assert row["paper_position_id"] == PID
    assert row["candidate_contract_id"] == CID
    assert row["sleeve_id"] == SLEEVE
    assert row["paper_position_ledger_source"].endswith("paper_position_ledger.v1.json")
    assert row["outcome_row_created"] is False
    assert row["furthest_stage_reached"] == "PAPER_OBSERVATION_FLOW"


def test_outcome_created_when_authoritative_closed_outcome_exists(tmp_path: Path) -> None:
    _seed(
        tmp_path,
        position=_position(),
        outcome={**_open_outcome(), "outcome_state": "CLOSED", "exit_mark": 130.0, "exit_trigger": "STOP_LOSS_THRESHOLD_REACHED"},
        closure=_closure(state="AUTO_CLOSED_PAPER_OUTCOME", reasons=["AUTO_CLOSED_PAPER_OUTCOME"], exit_mark=130.0, recommendation="EXIT"),
        exit_recommendation=_exit_rec("EXIT"),
        sample={"candidate_id": CID, "position_id": PID, "outcome_id": OID, "sample_id": SID, "inclusion_status": "INCLUDED"},
    )
    row = build_generated_hypothesis_paper_observation_to_outcome_v1(truth_root=tmp_path, day_utc=DAY)["oil_shock"]
    assert row["outcome_readiness_status"] == "READY"
    assert row["outcome_status"] == "OUTCOME_CREATED"
    assert row["outcome_row_created"] is True
    assert row["remaining_blocker"] == "NONE"
    assert row["furthest_stage_reached"] == "OUTCOME_FLOW"
    assert row["validation_sample_created_by_this_artifact"] is False


def test_position_still_open_when_hold_recommendation_has_no_close_trigger(tmp_path: Path) -> None:
    _seed(tmp_path, position=_position(), outcome=_open_outcome(), closure=_closure(reasons=["HOLD_RECOMMENDATION"]), exit_recommendation=_exit_rec())
    row = build_generated_hypothesis_paper_observation_to_outcome_v1(truth_root=tmp_path, day_utc=DAY)["oil_shock"]
    assert row["outcome_status"] == "OUTCOME_NOT_READY"
    assert row["blocker_code"] == "POSITION_STILL_OPEN"
    assert row["close_condition_status"] == "NOT_MET"
    assert row["close_condition_reason"] == "HOLD_RECOMMENDATION"
    assert row["david_action_required"] is False


def test_same_day_auto_promoted_observation_preserves_holding_period_blocker(tmp_path: Path) -> None:
    _seed(tmp_path, position=_position(), outcome=_open_outcome(), closure=_closure(reasons=["HOLD_RECOMMENDATION", "SAME_DAY_AUTO_PROMOTED_RESEARCH_OBSERVATION"]), exit_recommendation=_exit_rec())
    row = build_generated_hypothesis_paper_observation_to_outcome_v1(truth_root=tmp_path, day_utc=DAY)["oil_shock"]
    assert row["blocker_code"] == "HOLDING_PERIOD_NOT_ELAPSED"
    assert row["holding_period_status"] == "NOT_ELAPSED"
    assert row["close_condition_reason"] == "SAME_DAY_AUTO_PROMOTED_RESEARCH_OBSERVATION"


def test_missing_mark_or_exit_data_produces_precise_blocker(tmp_path: Path) -> None:
    _seed(tmp_path, position=_position(), outcome=_open_outcome(), closure=_closure(reasons=["MISSING_EXIT_MARK"], source="", recommendation="EXIT"), exit_recommendation=_exit_rec("EXIT"))
    row = build_generated_hypothesis_paper_observation_to_outcome_v1(truth_root=tmp_path, day_utc=DAY)["oil_shock"]
    assert row["blocker_code"] == "EXIT_PRICE_MISSING"
    assert row["mark_data_status"] == "MISSING"
    assert "exit_price" in row["missing_fields"]


def test_missing_close_rule_produces_precise_blocker(tmp_path: Path) -> None:
    _seed(tmp_path, position=_position(), outcome=_open_outcome(), closure=_closure(reasons=["EXIT_EVALUATION_MISSING"], recommendation=""), exit_recommendation=None)
    row = build_generated_hypothesis_paper_observation_to_outcome_v1(truth_root=tmp_path, day_utc=DAY)["oil_shock"]
    assert row["blocker_code"] == "CLOSE_RULE_MISSING"
    assert row["close_condition_reason"] == "CLOSE_RULE_MISSING"
    assert row["required_inputs"] == ["aegis_exit_recommendations_v1 row"]


def test_generated_hypothesis_lineage_is_preserved(tmp_path: Path) -> None:
    _seed(tmp_path, position=_position(), outcome=_open_outcome(), closure=_closure(), exit_recommendation=_exit_rec())
    row = build_generated_hypothesis_paper_observation_to_outcome_v1(truth_root=tmp_path, day_utc=DAY)["oil_shock"]
    assert row["generated_hypothesis_id"] == HID
    assert row["hypothesis_id"] == HID
    assert row["raw_signal_id"] == RAW
    assert row["candidate_contract_id"] == CID


def test_no_validation_sample_or_research_quality_is_fabricated(tmp_path: Path) -> None:
    _seed(tmp_path, position=_position(), outcome=_open_outcome(), closure=_closure(), exit_recommendation=_exit_rec())
    row = build_generated_hypothesis_paper_observation_to_outcome_v1(truth_root=tmp_path, day_utc=DAY)["oil_shock"]
    assert row["validation_sample_status"] == "NOT_READY"
    assert row["validation_sample_id"] == ""
    assert row["research_quality_consumed"] is False
    assert row["research_quality_consumed_reason"] == "RESEARCH_QUALITY_ROW_MISSING"


def test_missing_paper_position_preserves_candidate_to_paper_blocker(tmp_path: Path) -> None:
    _seed(
        tmp_path,
        candidate_to_paper={
            **_candidate_to_paper(),
            "paper_position_id": "",
            "outcome_id": "",
            "candidate_to_paper_status": "PAPER_CONSTRUCTION_FAILED",
            "paper_construction_missing_fields": ["stop_price"],
            "current_stop_reason": "PAPER_CONSTRUCTION_FAILED: missing stop_price",
            "reason_codes": ["PAPER_CONSTRUCTION_FAILED", "MISSING_STOP_PRICE"],
        },
        lifecycle={**_lifecycle(), "paper_position_id": "", "outcome_id": "", "blocker_reason_codes": ["PAPER_CONSTRUCTION_FAILED", "MISSING_STOP_PRICE"]},
    )
    row = build_generated_hypothesis_paper_observation_to_outcome_v1(truth_root=tmp_path, day_utc=DAY)["oil_shock"]
    assert row["paper_observation_found"] is False
    assert row["blocker_code"] == "PAPER_CONSTRUCTION_FAILED"
    assert row["missing_fields"] == ["stop_price"]
    assert "MISSING_STOP_PRICE" in row["reason_codes"]


def test_safety_gates_remain_unchanged_and_artifact_write_path(tmp_path: Path) -> None:
    _seed(tmp_path, position=_position(), outcome=_open_outcome(), closure=_closure(), exit_recommendation=_exit_rec())
    payload = build_generated_hypothesis_paper_observation_to_outcome_v1(truth_root=tmp_path, day_utc=DAY)
    row = payload["oil_shock"]
    assert row["no_broker_execution"] is True
    assert row["trade_advice_allowed"] is False
    assert row["safety_gates_changed"] is False
    assert payload["summary"]["outcome_row_created"] is False
    path = write_generated_hypothesis_paper_observation_to_outcome_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)
    assert path == generated_hypothesis_paper_observation_to_outcome_path_v1(truth_root=tmp_path, day_utc=DAY)
    assert path.exists()
