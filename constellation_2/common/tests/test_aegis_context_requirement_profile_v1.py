from __future__ import annotations

import json
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from constellation_2.common.aegis_market_context_v1 import build_event_market_snapshot_v1
from ops.aegis.context_requirement_profile_v1 import build_context_requirement_profile_v1, write_context_requirement_profile_v1
from ops.aegis.market_context_demand_v1 import build_market_context_demand_v1, market_context_snapshot_inputs_v1
from ops.aegis.market_context_provider_health_v1 import build_provider_health_v1


DAY = "2026-05-27"
NOW = "2026-05-27T16:10:00Z"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n", encoding="utf-8")


def _history(symbol: str, count: int = 60) -> list[dict]:
    start = date.fromisoformat(DAY) - timedelta(days=count)
    return [
        {"symbol": symbol, "timestamp_utc": f"{(start + timedelta(days=i)).isoformat()}T20:55:00Z", "close": 100 + i}
        for i in range(count)
    ]


def _seed_prior_vix(root: Path) -> None:
    market = {
        "day_utc": DAY,
        "status": "STALE",
        "market_data_mode": "INTRADAY_OPERATIONAL",
        "symbols": {
            "SPY": {"last_price": 160.0, "close": 160.0, "market_session_date": DAY, "freshness_status": "CURRENT", "provider": "LOCAL_CACHE", "data_timestamp_utc": f"{DAY}T15:55:00Z"},
            "QQQ": {"last_price": 260.0, "close": 260.0, "market_session_date": DAY, "freshness_status": "CURRENT", "provider": "LOCAL_CACHE", "data_timestamp_utc": f"{DAY}T15:55:00Z"},
            "VIX": {"last_price": 16.7, "close": 16.7, "market_session_date": "2026-05-22", "freshness_status": "STALE", "provider": "LOCAL_CACHE", "data_timestamp_utc": "2026-05-22T21:00:00Z"},
        },
        "breadth": {"advance_decline_delta": 100, "breadth_down_pct": 48, "freshness_status": "CURRENT", "market_session_date": DAY, "source": "BREADTH_PROXY", "data_timestamp_utc": f"{DAY}T15:55:00Z"},
        "provider_results": [],
    }
    registry = {
        "data_items": [
            {"data_item_id": "market.price.SPY", "status": "CURRENT", "provider": "LOCAL_CACHE", "market_session_date": DAY, "data_timestamp_utc": f"{DAY}T15:55:00Z", "source_artifact_path": "market", "source_hash": "spy", "value": 160.0},
            {"data_item_id": "market.price.QQQ", "status": "CURRENT", "provider": "LOCAL_CACHE", "market_session_date": DAY, "data_timestamp_utc": f"{DAY}T15:55:00Z", "source_artifact_path": "market", "source_hash": "qqq", "value": 260.0},
            {"data_item_id": "market.volatility.VIX", "status": "STALE", "provider": "LOCAL_CACHE", "market_session_date": "2026-05-22", "data_timestamp_utc": "2026-05-22T21:00:00Z", "source_artifact_path": "market", "source_hash": "vix", "value": 16.7},
        ]
    }
    _write_json(root / "reports" / "aegis_market_data_v1" / DAY / "market_data.v1.json", market)
    _write_json(root / "reports" / "aegis_data_registry_v1" / DAY / "data_registry.v1.json", registry)
    _write_jsonl(root / "market_data_snapshot_v1" / "SPY" / "2026.jsonl", _history("SPY"))
    _write_jsonl(root / "market_data_snapshot_v1" / "QQQ" / "2026.jsonl", _history("QQQ", 10))
    _write_jsonl(root / "market_data_snapshot_v1" / "VIX" / "2026.jsonl", [{"symbol": "VIX", "timestamp_utc": "2026-05-22T21:00:00Z", "close": 16.7}])


def test_profile_artifact_declares_operating_modes(tmp_path: Path) -> None:
    payload = build_context_requirement_profile_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    path = write_context_requirement_profile_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)["json"]

    assert Path(path).exists()
    assert payload["active_profile_id"] == "HUMAN_REVIEWED_PAPER_MODE"
    assert set(payload["profiles"]) == {
        "HUMAN_REVIEWED_PAPER_MODE",
        "CANDIDATE_VISIBILITY_ONLY",
        "EOD_ADVISORY_MODE",
        "STRICT_CURRENT_SESSION_MODE",
    }
    vix = next(row for row in payload["active_profile"]["requirements"] if row["required_evidence_item"] == "market.volatility.VIX")
    assert vix["freshness_requirement"] == "PRIOR_EOD_REFERENCE_ALLOWED"
    assert vix["blocker_severity"] == "NON_BLOCKING"


def test_human_reviewed_paper_mode_accepts_prior_certified_eod_vix(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("AEGIS_CONTEXT_REQUIREMENT_PROFILE", "HUMAN_REVIEWED_PAPER_MODE")
    root = tmp_path / "truth"
    _seed_prior_vix(root)

    health = build_provider_health_v1(truth_root=root, day_utc=DAY, generated_at_utc=NOW)
    by_health = {row["context_item_id"]: row for row in health["provider_items"]}
    assert by_health["vix_level"]["active_context_profile_id"] == "HUMAN_REVIEWED_PAPER_MODE"
    assert by_health["vix_level"]["certification_status"] == "CERTIFIED_REFERENCE"
    assert by_health["vix_level"]["source_label"] == "PRIOR_CERTIFIED_EOD_VIX"

    demand = build_market_context_demand_v1(truth_root=root, day_utc=DAY, generated_at_utc=NOW)
    by_item = {row["context_item_id"]: row for row in demand["market_context_items"]}
    assert by_item["vix_level"]["fulfillment_status"] == "CONTEXT_CERTIFIED"
    assert by_item["vix_level"]["blocker_severity"] == "NON_BLOCKING"

    snapshot_inputs = market_context_snapshot_inputs_v1(demand, truth_root=root, day_utc=DAY)
    snapshot = build_event_market_snapshot_v1(day_utc=DAY, generated_at_utc=NOW, market_data=snapshot_inputs, macro_calendar=[])
    assert snapshot["market_context_overall_status"] == "CONTEXT_CERTIFIED"
    assert snapshot["stale_data_status"] == "FRESH"
    assert not any("vix" in code.lower() for code in snapshot["reason_codes"])


def test_strict_current_session_profile_blocks_prior_vix(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("AEGIS_CONTEXT_REQUIREMENT_PROFILE", "STRICT_CURRENT_SESSION_MODE")
    root = tmp_path / "truth"
    _seed_prior_vix(root)

    demand = build_market_context_demand_v1(truth_root=root, day_utc=DAY, generated_at_utc=NOW)
    by_item = {row["context_item_id"]: row for row in demand["market_context_items"]}

    assert by_item["vix_level"]["fulfillment_status"] == "CONTEXT_STALE"
    assert by_item["vix_level"]["blocker_severity"] == "BLOCKING"
    assert "vix_level" in [row["context_item_id"] for row in demand["market_context_items"] if row["fulfillment_status"] != "CONTEXT_CERTIFIED"]
