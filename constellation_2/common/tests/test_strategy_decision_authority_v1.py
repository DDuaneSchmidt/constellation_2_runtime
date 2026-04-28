from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.strategy_decision_authority_v1 import evaluate_strategy_decision_authority_v1


DAY = "2026-04-27"
ENGINE = "C2_VOL_INCOME_DEFINED_RISK_V1"
INTENT_HASH = "1" * 64


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _write_generation(root: Path, *, required: bool = True, status: str = "PASS", reason_codes: list[str] | None = None) -> None:
    _write_json(
        root / "reports" / "trading_day_intent_generation_v1" / DAY / "trading_day_intent_generation.v1.json",
        {
            "producer_topology": [{"engine_id": ENGINE, "required": required, "script_path": "/strategy.py"}],
            "producer_results": [{"engine_id": ENGINE, "status": status, "reason_codes": reason_codes or [], "output_paths": []}],
        },
    )


def _write_intent(root: Path) -> None:
    _write_json(
        root / "intents_v1" / "snapshots" / DAY / f"{INTENT_HASH}.exposure_intent.v1.json",
        {"intent_id": "intent_1", "engine": {"engine_id": ENGINE}, "underlying": {"symbol": "SPY"}},
    )


def test_enabled_strategy_creates_intent(tmp_path: Path) -> None:
    _write_generation(tmp_path)
    _write_intent(tmp_path)

    payload = evaluate_strategy_decision_authority_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path)

    assert payload["strategy_decision_state"] == "INTENT_CREATED"
    assert payload["intent_mappings"][0]["strategy_id"] == ENGINE
    assert payload["intent_mappings"][0]["intent_id"] == "intent_1"
    assert payload["intent_mappings"][0]["intent_hash"] == INTENT_HASH


def test_enabled_strategy_evaluates_and_skips(tmp_path: Path) -> None:
    _write_generation(tmp_path, status="PASS", reason_codes=["NO_SIGNAL"])

    payload = evaluate_strategy_decision_authority_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path)

    assert payload["strategy_decision_state"] == "EVALUATED_NO_INTENT"
    assert payload["zero_intent_reason"] == "EVALUATED_NO_INTENT"


def test_strategy_disabled(tmp_path: Path) -> None:
    _write_generation(tmp_path, required=False, status="DISABLED")

    payload = evaluate_strategy_decision_authority_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path)

    assert payload["strategy_decision_state"] == "STRATEGY_DISABLED"


def test_missing_signal_artifact(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "reports" / "trading_day_intent_generation_v1" / DAY / "trading_day_intent_generation.v1.json",
        {"producer_topology": [{"engine_id": ENGINE, "required": True, "script_path": "/strategy.py"}], "producer_results": []},
    )

    payload = evaluate_strategy_decision_authority_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path)

    assert payload["strategy_decision_state"] == "SIGNAL_MISSING"


def test_market_data_blocked(tmp_path: Path) -> None:
    _write_generation(tmp_path)
    _write_json(
        tmp_path / "reports" / "market_data_authority_v1" / DAY / "market_data_authority.v1.json",
        {"status": "FAIL", "operator_impact": "PRE_SUBMIT_BLOCKER"},
    )

    payload = evaluate_strategy_decision_authority_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path)

    assert payload["strategy_decision_state"] == "MARKET_DATA_BLOCKED"
    assert payload["first_blocker"] == "MARKET_DATA_AUTHORITY_BLOCKED"
