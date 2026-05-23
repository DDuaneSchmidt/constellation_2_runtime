from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.candidate_lifecycle_v1 import build_candidate_lifecycle_v1
from ops.aegis.selected_intent_promotion_contract_v1 import build_selected_intent_promotion_v1


DAY = "2026-05-18"
INTENT_ID = "c2_cross_asset_trend_qqq_2026-05-18_v1"


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def _base_truth(root: Path, *, omit: str = "") -> None:
    intent_path = root / "intents" / "qqq.exposure_intent.v1.json"
    if omit != "intent.max_risk":
        constraints = {"max_risk_pct": "0.02"}
    else:
        constraints = {}
    _write_json(
        intent_path,
        {
            "schema_id": "exposure_intent",
            "schema_version": "v1",
            "intent_id": INTENT_ID,
            "exposure_type": "LONG_EQUITY",
            "target_notional_pct": "0.10",
            "constraints": constraints,
            "underlying": {"symbol": "QQQ", "currency": "USD"},
        },
    )
    selected = {
        "intent_id": INTENT_ID,
        "sleeve_id": "C2_CROSS_ASSET_TREND_V1",
        "engine_id": "C2_CROSS_ASSET_TREND_V1",
        "symbol": "QQQ",
        "intent_path": str(intent_path),
        "portfolio_scoring_status": "SCORED",
        "portfolio_score_rank": 1,
        "portfolio_score_total": "37.4",
        "executable_eligible": True,
    }
    _write_json(
        root / "reports/intent_arbitration_v1" / DAY / "intent_arbitration.v1.json",
        {"schema_id": "intent_arbitration", "day_utc": DAY, "status": "SELECTED", "selected_intent": selected, "rejected_or_filtered_intents": []},
    )
    _write_json(
        root / "reports/portfolio_scoring_v1" / DAY / "portfolio_scoring.v1.json",
        {"schema_id": "portfolio_scoring", "day_utc": DAY, "rankings": [selected]},
    )
    market_symbol = {
        "symbol": "QQQ",
        "canonical_symbol": "QQQ",
        "last_price": "705.88",
        "close": "705.88",
        "freshness_status": "CURRENT",
        "market_session_date": DAY,
        "provider": "STOOQ",
    }
    if omit == "market.price":
        market_symbol["freshness_status"] = "MISSING"
        market_symbol["last_price"] = ""
        market_symbol["close"] = ""
    _write_json(
        root / "reports/aegis_market_data_v1" / DAY / "market_data.v1.json",
        {"schema_id": "aegis_market_data", "day_utc": DAY, "symbols": {"QQQ": market_symbol}},
    )
    _write_json(
        root / "reports/aegis_data_registry_v1" / DAY / "data_registry.v1.json",
        {
            "schema_id": "aegis_data_registry",
            "day_utc": DAY,
            "data_items": [
                {
                    "data_item_id": "market.price.QQQ",
                    "status": market_symbol["freshness_status"],
                    "market_session_date": DAY,
                    "provider": "STOOQ",
                }
            ],
        },
    )
    _write_json(
        root / "reports/aegis_sleeve_readiness_v1" / DAY / "sleeve_readiness.v1.json",
        {"schema_id": "aegis_sleeve_readiness", "day_utc": DAY, "sleeves": [{"sleeve_id": "C2_CROSS_ASSET_TREND_V1", "readiness": "READY_WITH_WARNINGS"}]},
    )


def test_scored_selected_intent_promotes_to_review_only_candidate(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _base_truth(root)

    payload = build_selected_intent_promotion_v1(truth_root=root, day_utc=DAY)
    lifecycle = build_candidate_lifecycle_v1(truth_root=root, day_utc=DAY)

    assert payload["status"] == "PROMOTED_TO_OPERATOR_REVIEW"
    assert payload["promoted_candidate_count"] == 1
    assert payload["candidate"]["review_only"] is True
    assert payload["candidate"]["suggested_quantity"] == 0
    assert payload["candidate"]["broker_execution_allowed"] is False
    assert payload["candidate"]["autonomous_execution_allowed"] is False
    assert payload["candidate"]["automatic_approval_allowed"] is False
    assert lifecycle["candidate_count"] == 1
    candidate = lifecycle["candidates"][0]
    assert candidate["symbol"] == "QQQ"
    assert candidate["review_only"] is True
    assert candidate["current_operator_decision"] == "GENERATED"


def test_selected_intent_fails_closed_when_required_contract_field_missing(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _base_truth(root, omit="intent.max_risk")

    payload = build_selected_intent_promotion_v1(truth_root=root, day_utc=DAY)

    assert payload["status"] == "CONTRACT_FAILED"
    assert payload["promoted_candidate_count"] == 0
    assert "intent.constraints.max_risk_pct" in payload["missing_contract_fields"]
    assert payload["broker_execution_allowed"] is False
    assert payload["autonomous_execution_allowed"] is False


def test_missing_market_price_blocks_promotion_without_fake_opportunity(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _base_truth(root, omit="market.price")

    payload = build_selected_intent_promotion_v1(truth_root=root, day_utc=DAY)

    assert payload["status"] == "CONTRACT_FAILED"
    assert payload["candidate"] == {}
    assert "market.price.status" in payload["missing_contract_fields"]
    assert "market.price.last_price" in payload["missing_contract_fields"]
