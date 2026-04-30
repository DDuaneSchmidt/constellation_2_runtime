from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from ops.tools.run_decision_ledger_v1 import build_decision_ledger_v1, decision_ledger_path
from ops.tools.run_intent_arbitration_v1 import build_intent_arbitration
from ops.tools.run_portfolio_activation_gate_v1 import build_portfolio_activation_gate_v1, portfolio_activation_gate_path
from ops.tools.run_portfolio_state_v1 import portfolio_state_path


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _outcome(engine_id: str, symbol: str, *, status: str = "INTENT_CREATED") -> dict:
    intent_id = f"{engine_id.lower()}_{symbol.lower()}_intent"
    return {
        "sleeve_id": engine_id,
        "engine_id": engine_id,
        "status": status,
        "signal_state": {"state": "ACTIVE" if status == "INTENT_CREATED" else "INACTIVE", "duration_cycles": 1},
        "output_intents": [
            {
                "intent_id": intent_id,
                "intent_hash": f"hash_{intent_id}",
                "intent_path": f"/tmp/{intent_id}.json",
                "symbol": symbol,
            }
        ]
        if status == "INTENT_CREATED"
        else [],
        "artifact_path": f"/tmp/{engine_id}.outcome.json",
        "reason_codes": [],
    }


def _rollup(path: Path, day: str, outcomes: list[dict]) -> Path:
    _write_json(
        path,
        {
            "schema_id": "sleeve_scan_session",
            "schema_version": "v1",
            "day_utc": day,
            "status": "PASS",
            "outcomes": outcomes,
            "artifact_path": str(path),
        },
    )
    return path


def _state(truth: Path, day: str, **overrides: str) -> Path:
    payload = {
        "schema_id": "portfolio_state",
        "schema_version": "v1",
        "day_utc": day,
        "status": "PASS",
        "regime": "TREND",
        "trend_strength": "HIGH",
        "volatility_regime": "NORMAL",
        "correlation_regime": "NORMAL",
        "dispersion_regime": "NORMAL",
        "equity_beta_state": "HIGH",
        "artifact_path": str(portfolio_state_path(truth_root=truth, day_utc=day)),
    }
    payload.update(overrides)
    _write_json(portfolio_state_path(truth_root=truth, day_utc=day), payload)
    return portfolio_state_path(truth_root=truth, day_utc=day)


def test_portfolio_gate_preserves_raw_signals_and_suppresses_redundant_cross_asset(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    _state(truth, day, regime="TREND", trend_strength="HIGH", equity_beta_state="HIGH")
    rollup = _rollup(
        tmp_path / "rollup.json",
        day,
        [_outcome("C2_TREND_EQ_PRIMARY_V1", "SPY"), _outcome("C2_CROSS_ASSET_TREND_V1", "SPY")],
    )

    payload = build_portfolio_activation_gate_v1(day_utc=day, truth_root=truth, source_rollup_path=rollup)

    assert len(payload["raw_sleeve_signals"]) == 2
    by_sleeve = {row["sleeve_id"]: row for row in payload["decisions"]}
    assert by_sleeve["C2_TREND_EQ_PRIMARY_V1"]["portfolio_gate_decision"] == "ALLOW"
    assert by_sleeve["C2_CROSS_ASSET_TREND_V1"]["portfolio_gate_decision"] == "SUPPRESS"
    assert "CROSS_ASSET_SUPPRESSED_EQUITY_BETA_REDUNDANT" in by_sleeve["C2_CROSS_ASSET_TREND_V1"]["reason_codes"]


def test_crisis_override_allows_defensive_and_suppresses_vol_income(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    _state(truth, day, regime="CRISIS", volatility_regime="SHOCK")
    rollup = _rollup(
        tmp_path / "rollup.json",
        day,
        [_outcome("C2_DEFENSIVE_TAIL_V1", "TLT"), _outcome("C2_VOL_INCOME_DEFINED_RISK_V1", "IWM")],
    )

    payload = build_portfolio_activation_gate_v1(day_utc=day, truth_root=truth, source_rollup_path=rollup)

    by_sleeve = {row["sleeve_id"]: row for row in payload["decisions"]}
    assert by_sleeve["C2_DEFENSIVE_TAIL_V1"]["portfolio_gate_decision"] == "ALLOW"
    assert by_sleeve["C2_VOL_INCOME_DEFINED_RISK_V1"]["portfolio_gate_decision"] == "SUPPRESS"
    assert "CRISIS_OVERRIDE_SUPPRESSES_VOL_INCOME" in by_sleeve["C2_VOL_INCOME_DEFINED_RISK_V1"]["reason_codes"]


def test_cross_asset_macro_leadership_allowed_and_market_neutral_signal_only(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    _state(truth, day, regime="TREND", trend_strength="HIGH", equity_beta_state="HIGH")
    rollup = _rollup(
        tmp_path / "rollup.json",
        day,
        [_outcome("C2_CROSS_ASSET_TREND_V1", "DBC"), _outcome("C2_MARKET_NEUTRAL_SPREAD_V1", "SPY")],
    )

    payload = build_portfolio_activation_gate_v1(day_utc=day, truth_root=truth, source_rollup_path=rollup)

    by_sleeve = {row["sleeve_id"]: row for row in payload["decisions"]}
    assert by_sleeve["C2_CROSS_ASSET_TREND_V1"]["portfolio_gate_decision"] == "ALLOW"
    assert "CROSS_ASSET_MACRO_DIVERSIFIER_LEADERSHIP" in by_sleeve["C2_CROSS_ASSET_TREND_V1"]["reason_codes"]
    assert by_sleeve["C2_MARKET_NEUTRAL_SPREAD_V1"]["portfolio_gate_decision"] == "SIGNAL_ONLY"
    assert by_sleeve["C2_MARKET_NEUTRAL_SPREAD_V1"]["allowed_by_portfolio_gate"] is False


def test_arbitration_ignores_suppress_and_signal_only(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    _state(truth, day, regime="TREND", trend_strength="HIGH")
    rollup = _rollup(
        tmp_path / "rollup.json",
        day,
        [_outcome("C2_MARKET_NEUTRAL_SPREAD_V1", "SPY"), _outcome("C2_MEAN_REVERSION_EQ_V1", "QQQ")],
    )
    gate = build_portfolio_activation_gate_v1(day_utc=day, truth_root=truth, source_rollup_path=rollup)

    payload = build_intent_arbitration(day_utc=day, truth_root=truth, source_rollup_path=rollup, portfolio_gate_path=Path(gate["artifact_path"]))

    assert payload["status"] == "NO_EXECUTABLE_INTENT"
    assert payload["candidate_intents"] == []
    reasons = {row["rejection_reason"] for row in payload["rejected_or_filtered_intents"]}
    assert "PORTFOLIO_GATE_SIGNAL_ONLY" in reasons
    assert "PORTFOLIO_GATE_SUPPRESSED" in reasons


def test_decision_ledger_records_paths_blocker_and_reason_codes(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    _write_json(
        truth / "pointers" / "selected_intent_pointer.v1.json",
        {"schema_id": "selected_intent_pointer", "day_utc": day, "selected_intent": {}, "status": "NO_EXECUTABLE_INTENT"},
    )
    _write_json(portfolio_activation_gate_path(truth_root=truth, day_utc=day), {"schema_id": "portfolio_activation_gate", "day_utc": day})
    _write_json(portfolio_state_path(truth_root=truth, day_utc=day), {"schema_id": "portfolio_state", "day_utc": day})
    day_run = {
        "day_utc": day,
        "final_status": "NOT_READY",
        "canonical_phase": "AUTHORIZATION_FINAL",
        "canonical_blocker": "NO_EXECUTABLE_INTENT",
        "root_cause_chain": [{"phase": "AUTHORIZATION_FINAL", "canonical_blocker": "NO_EXECUTABLE_INTENT"}],
        "operator_next_action": "Accept no-trade day.",
        "phase_results": {},
    }

    payload = build_decision_ledger_v1(day_utc=day, truth_root=truth, aegis_day_payload=day_run)

    assert Path(payload["artifact_path"]) == decision_ledger_path(truth_root=truth, day_utc=day)
    assert payload["portfolio_state_path"]
    assert payload["portfolio_activation_gate_path"]
    assert payload["canonical_blocker"] == "NO_EXECUTABLE_INTENT"
    assert "NO_EXECUTABLE_INTENT" in payload["reason_codes"]
