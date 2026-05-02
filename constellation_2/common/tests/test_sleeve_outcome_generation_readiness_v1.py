from __future__ import annotations

import json
from pathlib import Path

from ops.tools import run_sleeve_outcome_generation_readiness_v1 as report


DAY = "2026-05-01"


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _report(root: Path, family: str, filename: str) -> Path:
    return root / "reports" / family / DAY / filename


def _seed(root: Path, execution_root: Path) -> None:
    _write(
        _report(root, "sleeve_intent_quality_diagnostics_v1", "sleeve_intent_quality_diagnostics.v1.json"),
        {
            "intent_diagnostics": [
                {"sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "intent_id": "trend", "signal_trigger_reason": "SIGNAL_CHANGED", "missing_inputs": [], "symbol": "SPY"},
                {"sleeve_id": "C2_VOL_INCOME_DEFINED_RISK_V1", "intent_id": "vol", "signal_trigger_reason": "SIGNAL_CHANGED", "missing_inputs": ["OPTIONS_CHAIN_SNAPSHOT_MISSING"], "symbol": "IWM"},
                {"sleeve_id": "C2_CROSS_ASSET_TREND_V1", "intent_id": "cross", "signal_trigger_reason": "SIGNAL_CHANGED", "missing_inputs": [], "symbol": "DBC"},
            ]
        },
    )
    _write(
        _report(root, "portfolio_scoring_v1", "portfolio_scoring.v1.json"),
        {
            "rankings": [
                {"rank": 1, "sleeve_id": "C2_VOL_INCOME_DEFINED_RISK_V1", "intent_id": "vol", "symbol": "IWM", "executable_eligible": True, "reason_codes": ["SCORING_EXECUTABLE_ELIGIBLE"]},
                {"rank": 2, "sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "intent_id": "trend", "symbol": "SPY", "executable_eligible": True, "reason_codes": ["SCORING_EXECUTABLE_ELIGIBLE"]},
                {"rank": 3, "sleeve_id": "C2_CROSS_ASSET_TREND_V1", "intent_id": "cross", "symbol": "DBC", "executable_eligible": False, "reason_codes": ["SCORING_NOT_EXECUTABLE_SUPPRESS"]},
            ]
        },
    )
    _write(
        _report(root, "risk_sizing_authority_v1", "risk_sizing_authority.v1.json"),
        {
            "sizing_decisions": [
                {"intent_id": "trend", "intent_hash": "a" * 64, "intent_path": "/intent/trend.json", "sizing_state": "SIZED", "execution_package_path": "", "final_quantity": None, "reason_code": "", "account_net_liquidation_cents": 1000000, "allowed_risk_cents": 20000},
                {"intent_id": "vol", "intent_hash": "b" * 64, "intent_path": "/intent/vol.json", "sizing_state": "SIZED", "execution_package_path": "", "final_quantity": None, "reason_code": "", "account_net_liquidation_cents": 1000000, "allowed_risk_cents": 20000},
            ]
        },
    )
    _write(_report(root, "market_data_authority_v1", "market_data_authority.v1.json"), {"status": "FAIL", "first_blocker": "OPTIONS_CHAIN_SNAPSHOT_MISSING", "required_symbols": ["IWM"]})
    _write(_report(root, "submit_boundary_status_v1", "submit_boundary_status.v1.json"), {"submit_allowed": False, "canonical_blocker": "AEGIS_NOT_READY"})
    _write(_report(root, "trade_outcome_v1", "trade_outcome.v1.json"), {"intent_id": "vol", "outcome_status": "UNKNOWN"})
    _write(_report(root, "sleeve_intent_trade_attribution_v1", "sleeve_intent_trade_attribution.v1.json"), {"opportunities": []})
    for intent_hash in ["a" * 64, "b" * 64]:
        _write(execution_root / "engine_activity_v1" / "authorization_v1" / DAY / f"{intent_hash}.authorization.v1.json", {"status": "REJECTED", "authorization": {"decision": "REJECTED", "authorized_quantity": 0}, "reason_codes": ["BUNDLE_B_HEADROOM_REJECTED"]})


def test_only_scoring_eligible_sleeves_are_reported(tmp_path: Path) -> None:
    _seed(tmp_path, tmp_path / "exec")

    payload = report.build_sleeve_outcome_generation_readiness_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path / "exec")

    assert {row["sleeve_id"] for row in payload["sleeve_results"]} == {"C2_TREND_EQ_PRIMARY_V1", "C2_VOL_INCOME_DEFINED_RISK_V1"}
    assert payload["summary"]["eligible_intent_count"] == 2


def test_missing_execution_submit_and_outcome_evidence_blocks_readiness(tmp_path: Path) -> None:
    _seed(tmp_path, tmp_path / "exec")

    rows = {row["intent_id"]: row for row in report.build_sleeve_outcome_generation_readiness_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path / "exec")["sleeve_results"]}

    assert rows["trend"]["execution_ready_if_aegis_ready"] is False
    assert "EXECUTION_PACKAGE_MISSING" in rows["trend"]["outcome_generation_blockers"]
    assert "SUBMIT_DECISION_TRACE_MISSING" in rows["trend"]["outcome_generation_blockers"]
    assert "COMPLETED_OUTCOME_MISSING" in rows["trend"]["outcome_generation_blockers"]
    assert "AUTHORIZATION_REJECTED" in rows["trend"]["outcome_generation_blockers"]
    assert rows["trend"]["authorization_details"]["authorization_status"] == "REJECTED"
    assert rows["trend"]["authorization_details"]["authorized_quantity"] == 0
    assert rows["trend"]["execution_package_readiness"]["status"] == "BLOCKED_BY_AUTHORIZATION_REJECTED"
    assert rows["trend"]["submit_trace_readiness"]["status"] == "EXPECTED_MISSING_SUBMIT_BOUNDARY_NOT_REACHED"
    assert "EXECUTION_PACKAGE_DOWNSTREAM_OF_AUTHORIZATION" in rows["trend"]["root_cause_classification"]


def test_options_market_data_blocker_is_kept_on_vol_income(tmp_path: Path) -> None:
    _seed(tmp_path, tmp_path / "exec")

    rows = {row["intent_id"]: row for row in report.build_sleeve_outcome_generation_readiness_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path / "exec")["sleeve_results"]}

    assert "OPTIONS_CHAIN_SNAPSHOT_MISSING" in rows["vol"]["outcome_generation_blockers"]
    assert rows["vol"]["market_inputs"]["missing_inputs"] == ["OPTIONS_CHAIN_SNAPSHOT_MISSING"]
    assert "OPTIONS_CHAIN_SNAPSHOT_MISSING" not in rows["trend"]["outcome_generation_blockers"]
    assert rows["trend"]["market_inputs"]["missing_inputs"] == []
    assert rows["vol"]["next_governed_producer"] == "ops/tools/run_options_chain_snapshot_required_day_v1.py"
    assert "MISSING_OPTIONS_CHAIN_INPUT" in rows["vol"]["root_cause_classification"]


def test_report_is_diagnostic_only_and_cannot_change_allocation_or_submit(tmp_path: Path) -> None:
    _seed(tmp_path, tmp_path / "exec")

    payload = report.build_sleeve_outcome_generation_readiness_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path / "exec")

    assert payload["authority"] == "DIAGNOSTIC_ONLY"
    assert payload["readiness_effect"] == "NONE"
    assert payload["submit_effect"] == "NONE"
    assert payload["allocation_effect"] == "NONE"
    assert payload["summary"]["execution_ready_count"] == 0
