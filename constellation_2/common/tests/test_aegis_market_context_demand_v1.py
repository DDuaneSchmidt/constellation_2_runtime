from __future__ import annotations

import json
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from constellation_2.common.aegis_market_context_v1 import build_event_market_snapshot_v1
from ops.aegis.market_context_demand_v1 import build_market_context_demand_v1, market_context_snapshot_inputs_v1
from ops.aegis.market_context_provider_health_v1 import build_provider_health_v1


DAY = "2026-05-26"
NOW = "2026-05-26T21:10:00Z"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n", encoding="utf-8")


def _history(symbol: str, *, start_close: float, count: int, end_day: str = DAY) -> list[dict]:
    end = date.fromisoformat(end_day)
    start = end - timedelta(days=count)
    rows = []
    close = start_close
    for offset in range(count):
        day = start + timedelta(days=offset)
        rows.append({"symbol": symbol, "timestamp_utc": f"{day.isoformat()}T20:55:00Z", "close": round(close, 2)})
        close += 1.0
    return rows


def _seed_context_root(tmp_path: Path, *, breadth: dict | None = None, vix_status: str = "CURRENT", vix_history_count: int = 5) -> Path:
    root = tmp_path / "truth"
    market = {
        "day_utc": DAY,
        "status": "PARTIAL",
        "market_data_mode": "FINAL_EOD_CERTIFIED",
        "symbols": {
            "SPY": {"last_price": 110.0, "close": 110.0, "market_session_date": DAY, "freshness_status": "CURRENT", "provider": "LOCAL_CACHE", "data_timestamp_utc": f"{DAY}T20:55:56Z"},
            "QQQ": {"last_price": 210.0, "close": 210.0, "market_session_date": DAY, "freshness_status": "CURRENT", "provider": "LOCAL_CACHE", "data_timestamp_utc": f"{DAY}T20:55:57Z"},
            "VIX": {"last_price": 20.0, "close": 20.0, "market_session_date": DAY if vix_status == "CURRENT" else "2026-05-22", "freshness_status": "CURRENT" if vix_status == "CURRENT" else "STALE", "provider": "CBOE", "data_timestamp_utc": f"{DAY if vix_status == 'CURRENT' else '2026-05-22'}T20:55:58Z"},
        },
        "breadth": breadth if breadth is not None else {"advance_decline_delta": 350, "breadth_down_pct": 41.5, "freshness_status": "CURRENT", "market_session_date": DAY, "source": "BREADTH_PROXY", "data_timestamp_utc": f"{DAY}T20:56:00Z"},
        "provider_results": [{"provider": "CBOE", "request_status": "SUCCESS" if vix_status == "CURRENT" else "FAILED", "timestamp_utc": f"{DAY}T20:55:58Z", "returned_data_date": DAY if vix_status == "CURRENT" else ""}, {"provider": "FRED", "request_status": "SOURCE_UNAVAILABLE", "timestamp_utc": f"{DAY}T20:55:59Z", "returned_data_date": ""}],
    }
    registry = {
        "data_items": [
            {"data_item_id": "market.price.SPY", "status": "CURRENT", "provider": "LOCAL_CACHE", "market_session_date": DAY, "data_timestamp_utc": f"{DAY}T20:55:56Z", "source_artifact_path": str(root / "reports" / "market_data_inputs_v1" / DAY / "market_data_inputs.v1.json"), "source_hash": "spyhash", "value": 110.0},
            {"data_item_id": "market.price.QQQ", "status": "CURRENT", "provider": "LOCAL_CACHE", "market_session_date": DAY, "data_timestamp_utc": f"{DAY}T20:55:57Z", "source_artifact_path": str(root / "reports" / "market_data_inputs_v1" / DAY / "market_data_inputs.v1.json"), "source_hash": "qqqhash", "value": 210.0},
            {"data_item_id": "market.volatility.VIX", "status": vix_status, "provider": "CBOE", "market_session_date": DAY if vix_status == "CURRENT" else "2026-05-22", "data_timestamp_utc": f"{DAY if vix_status == 'CURRENT' else '2026-05-22'}T20:55:58Z", "source_artifact_path": str(root / "reports" / "market_data_inputs_v1" / DAY / "market_data_inputs.v1.json"), "source_hash": "vixhash", "value": 20.0},
        ]
    }
    _write_json(root / "reports" / "aegis_market_data_v1" / DAY / "market_data.v1.json", market)
    _write_json(root / "reports" / "aegis_data_registry_v1" / DAY / "data_registry.v1.json", registry)
    _write_json(root / "reports" / "market_data_inputs_v1" / DAY / "market_data_inputs.v1.json", {"day_utc": DAY})
    _write_jsonl(root / "market_data_snapshot_v1" / "SPY" / "2026.jsonl", _history("SPY", start_close=50.0, count=60))
    _write_jsonl(root / "market_data_snapshot_v1" / "QQQ" / "2026.jsonl", _history("QQQ", start_close=150.0, count=5))
    _write_jsonl(root / "market_data_snapshot_v1" / "VIX" / "2026.jsonl", ([{"symbol": "VIX", "timestamp_utc": "2026-05-22T20:55:00Z", "close": 19.0}] if vix_history_count else []))
    return root


def test_market_context_demand_computes_certified_returns_and_trend(tmp_path: Path) -> None:
    root = _seed_context_root(tmp_path)
    payload = build_market_context_demand_v1(truth_root=root, day_utc=DAY, generated_at_utc=NOW)
    by_id = {row["context_item_id"]: row for row in payload["market_context_items"]}

    assert by_id["spy_return_pct"]["fulfillment_status"] == "CONTEXT_CERTIFIED"
    assert by_id["qqq_return_pct"]["fulfillment_status"] == "CONTEXT_CERTIFIED"
    assert by_id["spy_20d_return_pct"]["fulfillment_status"] == "CONTEXT_CERTIFIED"
    assert by_id["spy_above_50dma"]["fulfillment_status"] == "CONTEXT_CERTIFIED"
    assert by_id["spy_return_pct"]["value"] != ""
    assert by_id["spy_20d_return_pct"]["formula_version"] == "aegis_market_context_formulas.v1"
    assert by_id["spy_above_50dma"]["value"] is True
    assert payload["provider_health"]["status_counts"]["CONTEXT_CERTIFIED"] >= 3


def test_provider_health_certifies_current_vix_and_breadth(tmp_path: Path) -> None:
    root = _seed_context_root(tmp_path)
    payload = build_provider_health_v1(truth_root=root, day_utc=DAY, generated_at_utc=NOW)
    by_id = {row["context_item_id"]: row for row in payload["provider_items"]}

    assert by_id["vix_level"]["health_status"] == "CONTEXT_CERTIFIED"
    assert by_id["advance_decline_delta"]["health_status"] == "CONTEXT_CERTIFIED"
    assert by_id["breadth_down_pct"]["health_status"] == "CONTEXT_CERTIFIED"
    assert by_id["vix_level"]["allowed_provider_chain"][:3] == ["CBOE", "FRED", "MANUAL_CSV_DROP"]
    assert by_id["advance_decline_delta"]["allowed_provider_chain"][:2] == ["BREADTH_PROXY", "MANUAL_CSV_DROP"]


def test_market_context_demand_reports_provider_not_configured_for_missing_breadth(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("AEGIS_MARKET_CONTEXT_BREADTH_PROVIDER_CHAIN", "NONE")
    root = _seed_context_root(
        tmp_path,
        breadth={"advance_decline_delta": None, "breadth_down_pct": None, "freshness_status": "MISSING", "market_session_date": None, "source": None, "data_timestamp_utc": None},
    )
    payload = build_market_context_demand_v1(truth_root=root, day_utc=DAY, generated_at_utc=NOW)
    by_id = {row["context_item_id"]: row for row in payload["market_context_items"]}
    health = {row["context_item_id"]: row for row in payload["provider_health"]["provider_items"]}

    assert health["advance_decline_delta"]["health_status"] == "PROVIDER_NOT_CONFIGURED"
    assert health["breadth_down_pct"]["health_status"] == "PROVIDER_NOT_CONFIGURED"
    assert by_id["advance_decline_delta"]["fulfillment_status"] == "CONTEXT_NOT_FETCHED"
    assert by_id["breadth_down_pct"]["fulfillment_status"] == "CONTEXT_NOT_FETCHED"
    assert by_id["advance_decline_delta"]["failure_reason"].startswith("PROVIDER_NOT_CONFIGURED:")
    assert health["advance_decline_delta"]["provider_configured"] is False


def test_uncertified_breadth_blocks(tmp_path: Path) -> None:
    root = _seed_context_root(
        tmp_path,
        breadth={"advance_decline_delta": 10, "breadth_down_pct": 55.0, "freshness_status": "PRIOR_CLOSE", "market_session_date": DAY, "source": "MANUAL_CSV_DROP", "data_timestamp_utc": f"{DAY}T20:56:00Z"},
    )
    payload = build_market_context_demand_v1(truth_root=root, day_utc=DAY, generated_at_utc=NOW)
    by_id = {row["context_item_id"]: row for row in payload["market_context_items"]}

    assert by_id["advance_decline_delta"]["fulfillment_status"] == "CONTEXT_UNCERTIFIED"
    assert by_id["breadth_down_pct"]["fulfillment_status"] == "CONTEXT_UNCERTIFIED"
    assert by_id["breadth_down_pct"]["next_repair_command"] == "npm run aegis:repair-context-readiness"


def test_prior_day_vix_reference_clears_intraday_context(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("AEGIS_MARKET_CONTEXT_VIX_FRESHNESS_POLICY_MODE", "INTRADAY_ADVISORY")
    root = _seed_context_root(tmp_path, vix_status="STALE")
    demand = build_market_context_demand_v1(truth_root=root, day_utc=DAY, generated_at_utc=NOW)
    by_id = {row["context_item_id"]: row for row in demand["market_context_items"]}
    health = {row["context_item_id"]: row for row in demand["provider_health"]["provider_items"]}
    market_data = market_context_snapshot_inputs_v1(demand, truth_root=root, day_utc=DAY)
    snapshot = build_event_market_snapshot_v1(day_utc=DAY, generated_at_utc=NOW, market_data=market_data, macro_calendar=[], source_lineage=[])

    assert health["vix_level"]["certification_status"] == "CERTIFIED_REFERENCE"
    assert by_id["vix_level"]["certification_status"] == "CERTIFIED_REFERENCE"
    assert by_id["vix_level"]["fulfillment_status"] == "CONTEXT_CERTIFIED"
    assert by_id["vix_change_pct"]["fulfillment_status"] == "CONTEXT_CERTIFIED"
    assert snapshot["market_context_demand_path"].endswith("market_context_demand.v1.json")
    assert snapshot["market_context_overall_status"] == "CONTEXT_CERTIFIED"
    assert snapshot["stale_data_status"] == "FRESH"
    assert snapshot["volatility_classification"] == "REFERENCE_BASED"
    assert snapshot["volatility_reference_mode"] == "REFERENCE_BASED"


def test_prior_day_vix_rejected_for_strict_policy(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("AEGIS_MARKET_CONTEXT_VIX_FRESHNESS_POLICY_MODE", "STRICT_CURRENT_SESSION")
    root = _seed_context_root(tmp_path, vix_status="STALE")
    demand = build_market_context_demand_v1(truth_root=root, day_utc=DAY, generated_at_utc=NOW)
    by_id = {row["context_item_id"]: row for row in demand["market_context_items"]}
    assert by_id["vix_level"]["fulfillment_status"] == "CONTEXT_STALE"
    assert by_id["vix_change_pct"]["fulfillment_status"] == "CONTEXT_STALE"


def test_vix_change_requires_prior_certified_value(tmp_path: Path) -> None:
    root = _seed_context_root(tmp_path, vix_history_count=0)
    demand = build_market_context_demand_v1(truth_root=root, day_utc=DAY, generated_at_utc=NOW)
    by_id = {row["context_item_id"]: row for row in demand["market_context_items"]}

    assert by_id["vix_level"]["fulfillment_status"] == "CONTEXT_CERTIFIED"
    assert by_id["vix_change_pct"]["fulfillment_status"] == "CONTEXT_NOT_COMPUTABLE"
    assert "prior sessions of history" in by_id["vix_change_pct"]["failure_reason"]
