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
from ops.tools.run_portfolio_scoring_v1 import build_portfolio_scoring_v1, portfolio_scoring_path
from ops.tools.run_portfolio_state_v1 import portfolio_state_path


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _outcome(engine_id: str, symbol: str, *, status: str = "INTENT_CREATED", signal_strength: float | None = None) -> dict:
    intent_id = f"{engine_id.lower()}_{symbol.lower()}_intent"
    signal_state = {"state": "ACTIVE" if status == "INTENT_CREATED" else "INACTIVE", "duration_cycles": 1}
    if signal_strength is not None:
        signal_state["signal_strength"] = signal_strength
    return {
        "sleeve_id": engine_id,
        "engine_id": engine_id,
        "status": status,
        "signal_state": signal_state,
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


def test_portfolio_gate_disposes_every_multi_output_intent(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    _state(truth, day, regime="TREND", trend_strength="HIGH", equity_beta_state="HIGH")
    trend = _outcome("C2_TREND_EQ_PRIMARY_V1", "AAA")
    trend["output_intents"].append(
        {
            "intent_id": "c2_trend_eq_bbb_intent",
            "intent_hash": "hash_c2_trend_eq_bbb_intent",
            "intent_path": "/tmp/c2_trend_eq_bbb_intent.json",
            "symbol": "BBB",
        }
    )
    rollup = _rollup(tmp_path / "rollup.json", day, [trend])

    gate = build_portfolio_activation_gate_v1(day_utc=day, truth_root=truth, source_rollup_path=rollup)
    scoring = build_portfolio_scoring_v1(
        day_utc=day,
        truth_root=truth,
        source_rollup_path=rollup,
        portfolio_gate_path_arg=Path(gate["artifact_path"]),
    )
    payload = build_intent_arbitration(
        day_utc=day,
        truth_root=truth,
        source_rollup_path=rollup,
        portfolio_gate_path=Path(gate["artifact_path"]),
        portfolio_scoring_path_arg=Path(scoring["artifact_path"]),
    )

    assert [row["raw_intent_id"] for row in gate["raw_sleeve_signals"]] == [
        "c2_trend_eq_primary_v1_aaa_intent",
        "c2_trend_eq_bbb_intent",
    ]
    assert len(gate["decisions"]) == 2
    assert len(scoring["rankings"]) == 2
    assert payload["status"] == "SELECTED"
    assert len(payload["raw_candidate_intents"]) == 2
    assert len(payload["candidate_intents"]) == 1
    assert {row["rejection_reason"] for row in payload["rejected_or_filtered_intents"]} == {
        "PORTFOLIO_GATE_SUPPRESSED"
    }


def test_portfolio_gate_preserves_source_rollup_lifecycle_over_latest_day_lifecycle(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    _state(truth, day, regime="TREND", trend_strength="HIGH")
    outcome = _outcome("C2_VOL_INCOME_DEFINED_RISK_V1", "IWM")
    outcome["status"] = "BLOCKED"
    outcome["canonical_blocker"] = "POSITION_STATE_STALE"
    outcome["lifecycle_decision"] = "BLOCKED"
    outcome["lifecycle_reason_codes"] = ["POSITION_STATE_STALE"]
    outcome["output_intents"] = []
    outcome["intent_signature"] = [
        {"engine_id": "C2_VOL_INCOME_DEFINED_RISK_V1", "intent_id": "stale_vol", "intent_hash": "hash_stale_vol", "symbol": "IWM"}
    ]
    rollup = _rollup(tmp_path / "rollup.json", day, [outcome])
    lifecycle_path = truth / "reports/intent_lifecycle_state_v1" / day / "intent_lifecycle_state.v1.json"
    _write_json(
        lifecycle_path,
        {
            "schema_id": "intent_lifecycle_state",
            "schema_version": "v1",
            "day_utc": day,
            "rows": [
                {
                    "engine_id": "C2_VOL_INCOME_DEFINED_RISK_V1",
                    "sleeve_id": "C2_VOL_INCOME_DEFINED_RISK_V1",
                    "lifecycle_decision": "INTENT_CREATED",
                    "lifecycle_reason_codes": ["SIGNAL_CHANGED"],
                    "symbol": "IWM",
                }
            ],
        },
    )

    gate = build_portfolio_activation_gate_v1(day_utc=day, truth_root=truth, source_rollup_path=rollup)

    row = gate["decisions"][0]
    assert row["raw_intent_id"] == ""
    assert row["raw_signal_status"] == "BLOCKED"
    assert row["portfolio_gate_decision"] == "DEGRADED"
    assert row["lifecycle_decision"] == "BLOCKED"


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


def test_portfolio_scoring_ranks_allowed_intents_and_arbitration_selects_highest(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    _state(truth, day, regime="TREND", trend_strength="HIGH", volatility_regime="NORMAL")
    rollup = _rollup(
        tmp_path / "rollup.json",
        day,
        [_outcome("C2_VOL_INCOME_DEFINED_RISK_V1", "IWM"), _outcome("C2_TREND_EQ_PRIMARY_V1", "SPY")],
    )
    gate = build_portfolio_activation_gate_v1(day_utc=day, truth_root=truth, source_rollup_path=rollup)

    scoring = build_portfolio_scoring_v1(
        day_utc=day,
        truth_root=truth,
        source_rollup_path=rollup,
        portfolio_gate_path_arg=Path(gate["artifact_path"]),
    )
    ranked = [row for row in scoring["rankings"] if row["rank"]]

    assert Path(scoring["artifact_path"]) == portfolio_scoring_path(truth_root=truth, day_utc=day)
    assert scoring["scoring_policy_id"] == "portfolio_scoring_v1"
    assert scoring["intents_scored_count"] == 2
    assert ranked[0]["sleeve_id"] == "C2_TREND_EQ_PRIMARY_V1"
    assert set(ranked[0]["score_components"]) == {
        "signal_strength",
        "regime_alignment",
        "diversification_bonus",
        "overlap_penalty",
        "risk_penalty",
        "data_quality_penalty",
        "execution_readiness_penalty",
    }
    assert ranked[0]["executable_eligible"] is True

    payload = build_intent_arbitration(
        day_utc=day,
        truth_root=truth,
        source_rollup_path=rollup,
        portfolio_gate_path=Path(gate["artifact_path"]),
        portfolio_scoring_path_arg=Path(scoring["artifact_path"]),
    )

    assert payload["status"] == "SELECTED"
    assert payload["selected_intent"]["sleeve_id"] == "C2_TREND_EQ_PRIMARY_V1"
    assert payload["selected_intent"]["arbitration_reason"] == "HIGHEST_PORTFOLIO_SCORE_V1"
    assert payload["selected_intent"]["portfolio_score_rank"] == 1
    assert payload["selected_intent_rank"] == 1
    assert payload["selected_intent_score"] == payload["selected_intent"]["portfolio_score_total"]
    assert payload["portfolio_ranking"][0]["sleeve_id"] == "C2_TREND_EQ_PRIMARY_V1"
    assert payload["portfolio_scoring_path"] == scoring["artifact_path"]


def test_portfolio_scoring_tie_breaks_are_deterministic_without_file_order(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    _state(truth, day, regime="UNKNOWN", status="DEGRADED")
    rollup = _rollup(
        tmp_path / "rollup.json",
        day,
        [_outcome("C2_VOL_INCOME_DEFINED_RISK_V1", "IWM", signal_strength=0.5), _outcome("C2_TREND_EQ_PRIMARY_V1", "SPY", signal_strength=0.5)],
    )
    gate = build_portfolio_activation_gate_v1(day_utc=day, truth_root=truth, source_rollup_path=rollup)

    first = build_portfolio_scoring_v1(day_utc=day, truth_root=truth, source_rollup_path=rollup, portfolio_gate_path_arg=Path(gate["artifact_path"]))
    reversed_rollup = _rollup(
        tmp_path / "rollup_reversed.json",
        day,
        [_outcome("C2_TREND_EQ_PRIMARY_V1", "SPY", signal_strength=0.5), _outcome("C2_VOL_INCOME_DEFINED_RISK_V1", "IWM", signal_strength=0.5)],
    )
    gate_reversed = build_portfolio_activation_gate_v1(day_utc=day, truth_root=truth, source_rollup_path=reversed_rollup)
    second = build_portfolio_scoring_v1(day_utc=day, truth_root=truth, source_rollup_path=reversed_rollup, portfolio_gate_path_arg=Path(gate_reversed["artifact_path"]))

    assert [row["sleeve_id"] for row in first["rankings"] if row["rank"]] == [row["sleeve_id"] for row in second["rankings"] if row["rank"]]


def test_cross_asset_overlap_penalty_and_macro_diversification_bonus(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    _state(truth, day, regime="TREND", trend_strength="HIGH", equity_beta_state="NORMAL")
    gate_path = portfolio_activation_gate_path(truth_root=truth, day_utc=day)
    _write_json(
        gate_path,
        {
            "schema_id": "portfolio_activation_gate",
            "day_utc": day,
            "status": "PASS",
            "artifact_path": str(gate_path),
            "decisions": [
                {"sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "raw_signal_status": "ACTIVE", "raw_intent_id": "trend_spy", "raw_intent_symbol": "SPY", "portfolio_gate_decision": "ALLOW", "allowed_by_portfolio_gate": True, "overlap_group": "trend", "regime_bucket": "TREND", "reason_codes": []},
                {"sleeve_id": "C2_CROSS_ASSET_TREND_V1", "raw_signal_status": "ACTIVE", "raw_intent_id": "cross_spy", "raw_intent_symbol": "SPY", "portfolio_gate_decision": "ALLOW", "allowed_by_portfolio_gate": True, "overlap_group": "trend", "regime_bucket": "TREND", "reason_codes": []},
                {"sleeve_id": "C2_CROSS_ASSET_TREND_V1", "raw_signal_status": "ACTIVE", "raw_intent_id": "cross_dbc", "raw_intent_symbol": "DBC", "portfolio_gate_decision": "ALLOW", "allowed_by_portfolio_gate": True, "overlap_group": "macro_trend", "regime_bucket": "TREND", "reason_codes": []},
            ],
        },
    )

    scoring = build_portfolio_scoring_v1(day_utc=day, truth_root=truth, portfolio_gate_path_arg=gate_path)
    by_id = {row["intent_id"]: row for row in scoring["rankings"]}

    assert by_id["cross_spy"]["score_components"]["overlap_penalty"] < 0
    assert by_id["cross_dbc"]["score_components"]["diversification_bonus"] > by_id["cross_spy"]["score_components"]["diversification_bonus"]


def test_defensive_tail_ranks_above_carry_and_trend_during_crisis(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    _state(truth, day, regime="CRISIS", volatility_regime="SHOCK")
    rollup = _rollup(
        tmp_path / "rollup.json",
        day,
        [
            _outcome("C2_VOL_INCOME_DEFINED_RISK_V1", "IWM"),
            _outcome("C2_TREND_EQ_PRIMARY_V1", "SPY"),
            _outcome("C2_DEFENSIVE_TAIL_V1", "TLT"),
        ],
    )
    gate = build_portfolio_activation_gate_v1(day_utc=day, truth_root=truth, source_rollup_path=rollup)

    scoring = build_portfolio_scoring_v1(day_utc=day, truth_root=truth, source_rollup_path=rollup, portfolio_gate_path_arg=Path(gate["artifact_path"]))
    ranked = [row for row in scoring["rankings"] if row["rank"]]

    assert ranked[0]["sleeve_id"] == "C2_DEFENSIVE_TAIL_V1"
    suppressed = {row["sleeve_id"]: row for row in scoring["rankings"] if not row["executable_eligible"]}
    assert suppressed["C2_VOL_INCOME_DEFINED_RISK_V1"]["portfolio_gate_decision"] == "SUPPRESS"


def test_market_neutral_signal_only_is_not_executable_scored_selection(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    _state(truth, day, regime="DISPERSION", dispersion_regime="HIGH")
    rollup = _rollup(tmp_path / "rollup.json", day, [_outcome("C2_MARKET_NEUTRAL_SPREAD_V1", "SPY")])
    gate = build_portfolio_activation_gate_v1(day_utc=day, truth_root=truth, source_rollup_path=rollup)

    scoring = build_portfolio_scoring_v1(day_utc=day, truth_root=truth, source_rollup_path=rollup, portfolio_gate_path_arg=Path(gate["artifact_path"]))
    payload = build_intent_arbitration(day_utc=day, truth_root=truth, source_rollup_path=rollup, portfolio_gate_path=Path(gate["artifact_path"]), portfolio_scoring_path_arg=Path(scoring["artifact_path"]))

    assert scoring["intents_scored_count"] == 0
    assert scoring["rankings"][0]["executable_eligible"] is False
    assert payload["status"] == "NO_EXECUTABLE_INTENT"


def test_decision_ledger_records_paths_blocker_and_reason_codes(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    _write_json(
        truth / "pointers" / "selected_intent_pointer.v1.json",
        {"schema_id": "selected_intent_pointer", "day_utc": day, "selected_intent": {}, "status": "NO_EXECUTABLE_INTENT"},
    )
    _write_json(portfolio_activation_gate_path(truth_root=truth, day_utc=day), {"schema_id": "portfolio_activation_gate", "day_utc": day})
    _write_json(
        portfolio_scoring_path(truth_root=truth, day_utc=day),
        {"schema_id": "portfolio_scoring", "day_utc": day, "intents_scored_count": 1, "rankings": [{"intent_id": "x", "executable_eligible": False, "reason_codes": ["SCORING_NOT_EXECUTABLE_SUPPRESS"]}]},
    )
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
    assert payload["portfolio_scoring_path"]
    assert payload["scored_intents_count"] == 1
    assert "SCORING_NOT_EXECUTABLE_SUPPRESS" in payload["top_rejected_or_suppressed_reasons"]
    assert payload["canonical_blocker"] == "NO_EXECUTABLE_INTENT"
    assert "NO_EXECUTABLE_INTENT" in payload["reason_codes"]
