from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from ops.tools.run_decision_ledger_v1 import build_decision_ledger_v1
from ops.tools.run_intent_arbitration_v1 import build_intent_arbitration
from ops.tools.run_intent_lifecycle_state_v1 import build_intent_lifecycle_state_v1, intent_lifecycle_state_path
from ops.tools.run_portfolio_activation_gate_v1 import build_portfolio_activation_gate_v1
from ops.tools.run_portfolio_scoring_v1 import build_portfolio_scoring_v1
from ops.tools.run_portfolio_state_v1 import portfolio_state_path


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _intent(engine_id: str, symbol: str = "SPY", *, exposure_type: str = "LONG_EQUITY") -> dict:
    return {
        "intent_id": f"{engine_id.lower()}_{symbol.lower()}_intent",
        "intent_hash": f"hash_{engine_id}_{symbol}",
        "intent_path": f"/tmp/{engine_id}.{symbol}.json",
        "symbol": symbol,
        "engine_id": engine_id,
        "exposure_type": exposure_type,
    }


def _outcome(engine_id: str, symbol: str = "SPY", *, status: str = "INTENT_CREATED", exposure_type: str = "LONG_EQUITY") -> dict:
    intents = [_intent(engine_id, symbol, exposure_type=exposure_type)] if status == "INTENT_CREATED" else []
    return {
        "day_utc": "2026-04-30",
        "environment": "PAPER",
        "sleeve_id": engine_id,
        "engine_id": engine_id,
        "status": status,
        "current_status": status,
        "producer_requested_symbol": symbol,
        "output_intents": intents,
        "intent_signature": [{"intent_id": intents[0]["intent_id"], "intent_hash": intents[0]["intent_hash"], "symbol": symbol, "engine_id": engine_id}] if intents else [],
        "reason_codes": [],
        "artifact_path": f"/tmp/{engine_id}.outcome.json",
    }


def _previous_same(outcome: dict) -> dict:
    return {
        "current_status": "INTENT_CREATED",
        "signal_state": {"state": "ACTIVE", "duration_cycles": 1},
        "intent_signature": outcome["intent_signature"],
    }


def _positions(truth: Path, day: str, items: list[dict]) -> None:
    _write_json(
        truth / "positions_v1" / "snapshots" / day / "positions_snapshot.v5.json",
        {"schema_id": "positions_snapshot", "schema_version": "v5", "day_utc": day, "status": "OK", "items": items},
    )


def _state(truth: Path, day: str) -> None:
    _write_json(
        portfolio_state_path(truth_root=truth, day_utc=day),
        {
            "schema_id": "portfolio_state",
            "day_utc": day,
            "status": "PASS",
            "regime": "TREND",
            "trend_strength": "HIGH",
            "volatility_regime": "NORMAL",
            "dispersion_regime": "NORMAL",
            "equity_beta_state": "NORMAL",
            "artifact_path": str(portfolio_state_path(truth_root=truth, day_utc=day)),
        },
    )


def _build(truth: Path, outcome: dict, previous: dict | None = None) -> dict:
    return build_intent_lifecycle_state_v1(
        day_utc="2026-04-30",
        truth_root=truth,
        environment="PAPER",
        intent_truth_root=truth,
        outcomes=[outcome],
        previous_by_engine={outcome["engine_id"]: previous or {}},
    )["rows"][0]


def test_inactive_signal_is_no_intent(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    outcome = _outcome("ENGINE_A", status="NO_INTENT")
    row = _build(truth, outcome)

    assert row["signal_state"] == "INACTIVE"
    assert row["lifecycle_decision"] == "NO_INTENT"
    assert row["lifecycle_reason_codes"] == ["SIGNAL_INACTIVE"]


def test_changed_active_signal_creates_intent(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    outcome = _outcome("ENGINE_A")
    row = _build(truth, outcome, {"current_status": "NO_INTENT", "signal_state": {"state": "INACTIVE"}, "intent_signature": []})

    assert row["unchanged_signal"] is False
    assert row["lifecycle_decision"] == "INTENT_CREATED"
    assert "SIGNAL_CHANGED" in row["lifecycle_reason_codes"]


def test_unchanged_active_signal_without_position_or_order_creates_reentry_intent(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _positions(truth, "2026-04-30", [])
    outcome = _outcome("ENGINE_A")
    row = _build(truth, outcome, _previous_same(outcome))

    assert row["unchanged_signal"] is True
    assert row["matching_position_state"] == "NO_POSITION"
    assert row["matching_order_state"] == "NO_ORDER"
    assert row["lifecycle_decision"] == "INTENT_CREATED"
    assert row["reentry_eligible"] is True
    assert "PERSISTENT_SIGNAL_NO_POSITION" in row["lifecycle_reason_codes"]


def test_unchanged_signal_with_matching_open_position_suppresses_duplicate(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _positions(truth, "2026-04-30", [{"engine_id": "ENGINE_A", "symbol": "SPY", "quantity": 10, "exposure_type": "LONG_EQUITY"}])
    outcome = _outcome("ENGINE_A")
    row = _build(truth, outcome, _previous_same(outcome))

    assert row["matching_position_state"] == "POSITION_OPEN"
    assert row["lifecycle_decision"] == "NO_INTENT"
    assert "POSITION_ALREADY_OPEN" in row["lifecycle_reason_codes"]


def test_unchanged_signal_with_pending_order_suppresses_duplicate(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _positions(truth, "2026-04-30", [])
    _write_json(
        truth / "submission_index_v1" / "2026-04-30" / "submission_index.v1.json",
        {"orders": [{"engine_id": "ENGINE_A", "symbol": "SPY", "status": "SUBMITTED", "quantity": 10, "exposure_type": "LONG_EQUITY"}]},
    )
    outcome = _outcome("ENGINE_A")
    row = _build(truth, outcome, _previous_same(outcome))

    assert row["matching_order_state"] == "ORDER_PENDING"
    assert row["lifecycle_decision"] == "NO_INTENT"
    assert "ORDER_ALREADY_PENDING" in row["lifecycle_reason_codes"]


def test_stale_position_snapshot_blocks_reentry(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    outcome = _outcome("ENGINE_A")
    row = _build(truth, outcome, _previous_same(outcome))

    assert row["matching_position_state"] == "POSITION_STATE_STALE"
    assert row["lifecycle_decision"] == "BLOCKED"
    assert "POSITION_STATE_STALE" in row["lifecycle_reason_codes"]


def test_preopen_unknown_signal_is_data_dependent_not_blocked(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("C2_TRADING_DAY_READINESS_NOW_UTC", "2026-04-30T12:00:00Z")
    truth = tmp_path / "truth"
    outcome = _outcome("C2_DEFENSIVE_TAIL_V1", "TLT", status="BLOCKED", exposure_type="TAIL_HEDGE")
    row = build_intent_lifecycle_state_v1(
        day_utc="2026-05-01",
        truth_root=truth,
        environment="PAPER",
        intent_truth_root=truth,
        outcomes=[outcome],
        previous_by_engine={},
    )["rows"][0]

    assert row["signal_state"] == "UNKNOWN"
    assert row["lifecycle_decision"] == "NO_INTENT"
    assert row["lifecycle_reason_codes"] == ["PREOPEN_INPUTS_NOT_REQUIRED"]


def test_unknown_sleeve_attribution_is_uncertain_not_suppressed(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _positions(truth, "2026-04-30", [{"symbol": "SPY", "quantity": 10, "exposure_type": "LONG_EQUITY"}])
    outcome = _outcome("ENGINE_A")
    row = _build(truth, outcome, _previous_same(outcome))

    assert row["matching_position_state"] == "POSITION_MATCH_UNCERTAIN"
    assert row["lifecycle_decision"] == "BLOCKED"
    assert "POSITION_MATCH_UNCERTAIN" in row["lifecycle_reason_codes"]


def test_market_neutral_single_leg_does_not_count_as_full_pair_position(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _positions(truth, "2026-04-30", [{"engine_id": "C2_MARKET_NEUTRAL_SPREAD_V1", "symbol": "SPY", "quantity": 10, "exposure_type": "LONG_EQUITY"}])
    outcome = _outcome("C2_MARKET_NEUTRAL_SPREAD_V1", exposure_type="MARKET_NEUTRAL_PAIR")
    row = _build(truth, outcome, _previous_same(outcome))

    assert row["matching_position_state"] == "NO_POSITION"
    assert row["lifecycle_decision"] == "INTENT_CREATED"
    assert "PERSISTENT_SIGNAL_NO_POSITION" in row["lifecycle_reason_codes"]


def test_only_lifecycle_created_reentry_flows_to_gate_scoring_and_arbitration(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    _state(truth, day)
    created = _outcome("C2_TREND_EQ_PRIMARY_V1", "SPY")
    created.update(
        {
            "lifecycle_decision": "INTENT_CREATED",
            "lifecycle_reason_codes": ["UNCHANGED_SIGNAL", "PERSISTENT_SIGNAL_NO_POSITION"],
            "lifecycle_state_path": str(intent_lifecycle_state_path(truth_root=truth, day_utc=day)),
            "reentry_eligible": True,
            "unchanged_signal": True,
        }
    )
    no_intent = _outcome("C2_CROSS_ASSET_TREND_V1", "DBC")
    no_intent.update(
        {
            "status": "NO_INTENT",
            "output_intents": [],
            "lifecycle_decision": "NO_INTENT",
            "lifecycle_reason_codes": ["POSITION_ALREADY_OPEN"],
            "lifecycle_state_path": str(intent_lifecycle_state_path(truth_root=truth, day_utc=day)),
            "position_match_status": "POSITION_OPEN",
        }
    )
    blocked = _outcome("C2_VOL_INCOME_DEFINED_RISK_V1", "IWM")
    blocked.update(
        {
            "status": "BLOCKED",
            "canonical_blocker": "POSITION_MATCH_UNCERTAIN",
            "output_intents": [],
            "lifecycle_decision": "BLOCKED",
            "lifecycle_reason_codes": ["POSITION_MATCH_UNCERTAIN"],
            "lifecycle_state_path": str(intent_lifecycle_state_path(truth_root=truth, day_utc=day)),
            "position_match_status": "POSITION_MATCH_UNCERTAIN",
        }
    )
    rollup = tmp_path / "rollup.json"
    _write_json(rollup, {"schema_id": "sleeve_scan_session", "day_utc": day, "status": "PASS", "outcomes": [created, no_intent, blocked]})

    gate = build_portfolio_activation_gate_v1(day_utc=day, truth_root=truth, source_rollup_path=rollup)
    scoring = build_portfolio_scoring_v1(day_utc=day, truth_root=truth, source_rollup_path=rollup, portfolio_gate_path_arg=Path(gate["artifact_path"]))
    arbitration = build_intent_arbitration(
        day_utc=day,
        truth_root=truth,
        source_rollup_path=rollup,
        portfolio_gate_path=Path(gate["artifact_path"]),
        portfolio_scoring_path_arg=Path(scoring["artifact_path"]),
    )

    assert [row["sleeve_id"] for row in gate["approved_executable_intents"]] == ["C2_TREND_EQ_PRIMARY_V1"]
    assert scoring["intents_scored_count"] == 1
    assert [row["sleeve_id"] for row in scoring["rankings"] if row["executable_eligible"]] == ["C2_TREND_EQ_PRIMARY_V1"]
    assert arbitration["selected_intent"]["sleeve_id"] == "C2_TREND_EQ_PRIMARY_V1"
    assert {row["sleeve_id"] for row in arbitration["raw_candidate_intents"]} == {"C2_TREND_EQ_PRIMARY_V1"}


def test_portfolio_gate_consumes_canonical_lifecycle_overlay(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    _state(truth, day)
    stale_blocked = _outcome("C2_DEFENSIVE_TAIL_V1", "TLT", status="BLOCKED", exposure_type="TAIL_HEDGE")
    stale_blocked.update(
        {
            "lifecycle_decision": "BLOCKED",
            "lifecycle_reason_codes": ["SIGNAL_STATE_UNKNOWN"],
            "lifecycle_state_path": "",
        }
    )
    lifecycle_path = intent_lifecycle_state_path(truth_root=truth, day_utc=day)
    _write_json(
        lifecycle_path,
        {
            "schema_id": "intent_lifecycle_state",
            "day_utc": day,
            "rows": [
                {
                    "sleeve_id": "C2_DEFENSIVE_TAIL_V1",
                    "engine_id": "C2_DEFENSIVE_TAIL_V1",
                    "symbol": "TLT",
                    "exposure_type": "TAIL_HEDGE",
                    "lifecycle_decision": "NO_INTENT",
                    "lifecycle_reason_codes": ["PREOPEN_INPUTS_NOT_REQUIRED"],
                    "matching_position_state": "NO_POSITION",
                    "matching_order_state": "NO_ORDER",
                    "reentry_eligible": False,
                    "unchanged_signal": False,
                }
            ],
        },
    )
    rollup = tmp_path / "rollup.json"
    _write_json(rollup, {"schema_id": "sleeve_scan_session", "day_utc": day, "status": "PASS", "outcomes": [stale_blocked]})

    gate = build_portfolio_activation_gate_v1(day_utc=day, truth_root=truth, source_rollup_path=rollup)
    defensive = gate["decisions"][0]

    assert defensive["portfolio_gate_decision"] == "SUPPRESS"
    assert defensive["lifecycle_reason_codes"] == ["PREOPEN_INPUTS_NOT_REQUIRED"]
    assert defensive["lifecycle_state_path"] == str(lifecycle_path)


def test_decision_ledger_records_lifecycle_path_and_counts(tmp_path: Path) -> None:
    day = "2026-04-30"
    truth = tmp_path / "truth"
    lifecycle_path = intent_lifecycle_state_path(truth_root=truth, day_utc=day)
    _write_json(
        lifecycle_path,
        {
            "schema_id": "intent_lifecycle_state",
            "day_utc": day,
            "counts": {
                "reentry_intent_count": 1,
                "suppressed_position_count": 2,
                "suppressed_order_count": 3,
                "uncertain_position_count": 4,
            },
            "rows": [
                {"lifecycle_decision": "INTENT_CREATED", "lifecycle_reason_codes": ["PERSISTENT_SIGNAL_NO_POSITION"]},
                {"lifecycle_decision": "NO_INTENT", "lifecycle_reason_codes": ["POSITION_ALREADY_OPEN"]},
            ],
        },
    )
    _write_json(truth / "pointers" / "selected_intent_pointer.v1.json", {"schema_id": "selected_intent_pointer", "day_utc": day, "selected_intent": {}})
    _write_json(truth / "reports" / "portfolio_scoring_v1" / day / "portfolio_scoring.v1.json", {"schema_id": "portfolio_scoring", "day_utc": day, "rankings": []})
    day_run = {"day_utc": day, "final_status": "NOT_READY", "canonical_phase": "AUTHORIZATION_FINAL", "canonical_blocker": "NO_EXECUTABLE_INTENT", "phase_results": {}}

    ledger = build_decision_ledger_v1(day_utc=day, truth_root=truth, aegis_day_payload=day_run)

    assert ledger["intent_lifecycle_state_path"] == str(lifecycle_path)
    assert ledger["lifecycle_decision_counts"] == {"INTENT_CREATED": 1, "NO_INTENT": 1}
    assert ledger["reentry_intent_count"] == 1
    assert ledger["suppressed_position_count"] == 2
    assert ledger["suppressed_order_count"] == 3
    assert ledger["uncertain_position_count"] == 4
