from __future__ import annotations

import json
from pathlib import Path

from ops.tools import run_sleeve_intent_quality_diagnostics_v1 as diag


DAY = "2026-05-01"


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _report(root: Path, family: str, filename: str) -> Path:
    return root / "reports" / family / DAY / filename


def _seed(root: Path) -> None:
    outcomes = []
    for sleeve_id, intent_id, symbol in [
        ("C2_TREND_EQ_PRIMARY_V1", "trend-spy", "SPY"),
        ("C2_VOL_INCOME_DEFINED_RISK_V1", "vol-iwm", "IWM"),
        ("C2_CROSS_ASSET_TREND_V1", "cross-dbc", "DBC"),
        ("C2_MARKET_NEUTRAL_SPREAD_V1", "spread-spy", "SPY"),
    ]:
        outcomes.append(
            {
                "sleeve_id": sleeve_id,
                "engine_id": sleeve_id,
                "status": "INTENT_CREATED",
                "reason_codes": ["EXISTING_ENGINE_INTENT_EVALUATED", "SIGNAL_CHANGED"],
                "producer_requested_symbols": [symbol],
                "input_artifacts": [{"artifact_type": "engine_registry", "path": "/registry"}],
                "output_intents": [{"intent_id": intent_id, "symbol": symbol}],
                "rejected_intents": [],
            }
        )
    for sleeve_id, symbol in [
        ("C2_MEAN_REVERSION_EQ_V1", "QQQ"),
        ("C2_EVENT_DISLOCATION_V1", "GLD"),
        ("C2_DEFENSIVE_TAIL_V1", "TLT"),
    ]:
        outcomes.append(
            {
                "sleeve_id": sleeve_id,
                "engine_id": sleeve_id,
                "status": "NO_INTENT",
                "reason_codes": ["NO_INTENT_DECLARED"],
                "producer_requested_symbols": [symbol],
                "output_intents": [],
                "rejected_intents": [],
            }
        )
    _write(_report(root, "sleeve_evaluation_kernel_v1", "sleeve_evaluation_rollup.v1.json"), {"outcomes": outcomes})
    _write(
        _report(root, "portfolio_scoring_v1", "portfolio_scoring.v1.json"),
        {
            "rankings": [
                {"intent_id": "trend-spy", "sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "symbol": "SPY", "score_total": 40, "reason_codes": ["SCORING_EXECUTABLE_ELIGIBLE"], "evidence_paths": ["/scoring"]},
                {"intent_id": "vol-iwm", "sleeve_id": "C2_VOL_INCOME_DEFINED_RISK_V1", "symbol": "IWM", "score_total": 41, "reason_codes": ["SCORING_EXECUTABLE_ELIGIBLE"], "evidence_paths": ["/scoring"]},
                {"intent_id": "cross-dbc", "sleeve_id": "C2_CROSS_ASSET_TREND_V1", "symbol": "DBC", "score_total": 0, "reason_codes": ["SCORING_NOT_EXECUTABLE_SUPPRESS"], "evidence_paths": ["/scoring"]},
                {"intent_id": "spread-spy", "sleeve_id": "C2_MARKET_NEUTRAL_SPREAD_V1", "symbol": "SPY", "score_total": 0, "reason_codes": ["PAIRED_EXECUTION_NOT_SUPPORTED"], "evidence_paths": ["/scoring"]},
            ]
        },
    )
    _write(
        _report(root, "risk_sizing_authority_v1", "risk_sizing_authority.v1.json"),
        {
            "status": "FAIL",
            "risk_sizing_state": "RISK_BLOCKED",
            "first_blocker": "CAPITAL_RISK_ENVELOPE_NOT_PASS",
            "risk_envelope": {"nav_total_cents": 0},
            "sizing_decisions": [
                {"intent_id": "trend-spy", "reason_code": "CAPITAL_RISK_ENVELOPE_NOT_PASS"},
                {"intent_id": "vol-iwm", "reason_code": "CAPITAL_RISK_ENVELOPE_NOT_PASS"},
                {"intent_id": "cross-dbc", "reason_code": "CAPITAL_RISK_ENVELOPE_NOT_PASS"},
                {"intent_id": "spread-spy", "reason_code": "CAPITAL_RISK_ENVELOPE_NOT_PASS"},
            ],
        },
    )
    _write(_report(root, "market_data_authority_v1", "market_data_authority.v1.json"), {"status": "FAIL", "first_blocker": "OPTIONS_CHAIN_SNAPSHOT_MISSING"})
    _write(_report(root, "portfolio_account_authority_v1", "portfolio_account_authority.v1.json"), {"status": "PASS", "account_values": {"net_liquidation_cents": None}})


def test_generated_intents_have_signal_input_and_blocker_trace(tmp_path: Path) -> None:
    _seed(tmp_path)

    payload = diag.build_sleeve_intent_quality_diagnostics_v1(day_utc=DAY, truth_root=tmp_path)

    intents = {row["intent_id"]: row for row in payload["intent_diagnostics"]}
    assert set(intents) == {"trend-spy", "vol-iwm", "cross-dbc", "spread-spy"}
    assert all(row["signal_id"] for row in intents.values())
    assert all(row["inputs_used"] for row in intents.values())
    assert all(row["intent_reached_risk_sizing"] is True for row in intents.values())
    assert all(row["downstream_blocker"] == "CAPITAL_RISK_ENVELOPE_NOT_PASS" for row in intents.values())


def test_no_intent_sleeves_have_explicit_reason(tmp_path: Path) -> None:
    _seed(tmp_path)

    rows = {row["sleeve_id"]: row for row in diag.build_sleeve_intent_quality_diagnostics_v1(day_utc=DAY, truth_root=tmp_path)["sleeve_diagnostics"]}

    assert rows["C2_MEAN_REVERSION_EQ_V1"]["no_intent_reason"] == "NO_INTENT_DECLARED"
    assert rows["C2_EVENT_DISLOCATION_V1"]["no_intent_reason"] == "NO_INTENT_DECLARED"
    assert rows["C2_DEFENSIVE_TAIL_V1"]["no_intent_reason"] == "NO_INTENT_DECLARED"


def test_missing_account_and_nav_are_diagnostic_not_authoritative(tmp_path: Path) -> None:
    _seed(tmp_path)

    payload = diag.build_sleeve_intent_quality_diagnostics_v1(day_utc=DAY, truth_root=tmp_path)

    assert payload["authority"] == "DIAGNOSTIC_ONLY"
    assert payload["readiness_effect"] == "NONE"
    assert payload["submit_effect"] == "NONE"
    assert payload["allocation_effect"] == "NONE"
    assert payload["risk_sizing_diagnostics"]["root_cause"] != "NONE"


def test_same_symbol_trigger_is_reported_as_differentiation_diagnostic(tmp_path: Path) -> None:
    _seed(tmp_path)

    diff = diag.build_sleeve_intent_quality_diagnostics_v1(day_utc=DAY, truth_root=tmp_path)["differentiation_diagnostics"]

    assert diff["possible_hidden_correlation"] is True
    assert diff["overlapping_symbol_groups"][0]["symbol"] == "SPY"
