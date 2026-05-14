from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.market_data_authority_v1 import (
    SCOPE_SELECTED_INTENT_REQUIREMENT_GRAPH,
    evaluate_market_data_authority_v1,
)


DAY = "2026-04-27"
INTENT_HASH = "1" * 64


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _write_option_intent(root: Path, *, symbol: str = "SPY") -> None:
    _write_json(
        root / "intents_v1" / "snapshots" / DAY / f"{INTENT_HASH}.exposure_intent.v1.json",
        {"intent_id": "intent", "underlying": {"symbol": symbol}, "option": {"strategy": "put_spread"}},
    )


def _write_long_equity_intent(root: Path, *, symbol: str = "SPY", intent_id: str = "intent_spy") -> None:
    _write_json(
        root / "intents_v1" / "snapshots" / DAY / f"{intent_id}.exposure_intent.v1.json",
        {"intent_id": intent_id, "underlying": {"symbol": symbol}, "exposure_type": "LONG_EQUITY"},
    )


def _write_requirement_graph(root: Path, *, symbol: str, intent_id: str, family: str) -> Path:
    if family == "EQUITY":
        artifacts = ["underlying_spot", "bid_ask_quotes", "freshness_certificate"]
    else:
        artifacts = ["option_chain", "freshness_certificate", "options_snapshot_artifact"]
    rows = [
        {
            "owner_phase": "MARKET_DATA",
            "source_type": "ACTIVE_INTENT",
            "source_id": intent_id,
            "instrument": symbol,
            "required_artifact": artifact,
            "market_data_family": family,
            "requirement_id": f"MARKET_DATA:{intent_id}:{symbol}:{artifact}",
        }
        for artifact in artifacts
    ]
    path = root / "reports" / "aegis_requirement_graph_v1" / DAY / "requirement_graph.v1.json"
    _write_json(path, {"day_utc": DAY, "requirements": rows})
    return path


def _write_snapshot(root: Path, *, symbol: str = "SPY", cert_until: str = "2026-04-27T16:00:00Z", contracts: int = 1) -> None:
    capture = root / "options_chain_snapshot_v1" / DAY / f"ib_capture_{symbol}"
    _write_json(
        capture / "options_chain_snapshot.v1.json",
        {"schema_id": "options_chain_snapshot", "underlying": {"symbol": symbol}, "contracts": [{"conid": idx} for idx in range(contracts)]},
    )
    _write_json(capture / "freshness_certificate.v1.json", {"valid_until_utc": cert_until})


def _write_equity_snapshot(
    root: Path,
    *,
    symbol: str = "SPY",
    cert_until: str = "2026-04-27T16:00:00Z",
    quote_time: str = "2026-04-27T15:00:00Z",
) -> None:
    day_root = root / "market_data_snapshot_v1" / "snapshots" / DAY
    _write_json(
        day_root / f"{symbol}.market_data_snapshot.v1.json",
        {
            "schema_id": "market_data_snapshot_v1",
            "schema_version": "v1",
            "day_utc": DAY,
            "symbol": symbol,
            "bid": "499.95",
            "ask": "500.05",
            "last": "500.00",
            "close": "499.80",
            "quote_as_of_utc": quote_time,
        },
    )
    _write_json(day_root / f"{symbol}.freshness_certificate.v1.json", {"valid_until_utc": cert_until})


def test_no_active_intents_is_no_intents(tmp_path: Path) -> None:
    payload = evaluate_market_data_authority_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path, produced_utc="2026-04-27T15:00:00Z")

    assert payload["market_data_state"] == "NO_INTENTS"


def test_active_spy_option_intent_with_valid_snapshot_is_ready(tmp_path: Path) -> None:
    _write_option_intent(tmp_path, symbol="SPY")
    _write_snapshot(tmp_path, symbol="SPY", cert_until="2026-04-27T16:00:00Z")

    payload = evaluate_market_data_authority_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path, produced_utc="2026-04-27T15:00:00Z")

    assert payload["market_data_state"] == "READY"
    assert payload["required_symbols"] == ["SPY"]


def test_active_option_intent_missing_snapshot_is_missing_required_data(tmp_path: Path) -> None:
    _write_option_intent(tmp_path, symbol="SPY")

    payload = evaluate_market_data_authority_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path, produced_utc="2026-04-27T15:00:00Z")

    assert payload["market_data_state"] == "MISSING_REQUIRED_DATA"


def test_stale_snapshot_is_stale(tmp_path: Path) -> None:
    _write_option_intent(tmp_path, symbol="SPY")
    _write_snapshot(tmp_path, symbol="SPY", cert_until="2026-04-27T14:00:00Z")

    payload = evaluate_market_data_authority_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path, produced_utc="2026-04-27T15:00:00Z")

    assert payload["market_data_state"] == "STALE"
    assert payload["status"] == "FAIL"
    assert payload["operator_impact"] == "PRE_SUBMIT_BLOCKER"


def test_stale_snapshot_after_dry_run_completion_is_diagnostic(tmp_path: Path) -> None:
    _write_option_intent(tmp_path, symbol="SPY")
    _write_snapshot(tmp_path, symbol="SPY", cert_until="2026-04-27T14:00:00Z")
    _write_json(
        tmp_path / "reports" / "submit_boundary_status_v1" / DAY / "submit_boundary_status.v1.json",
        {"boundary_status": "DRY_RUN_COMPLETE", "submit_mode_status": "DRY_RUN_COMPLETE"},
    )

    payload = evaluate_market_data_authority_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path, produced_utc="2026-04-27T15:00:00Z")

    assert payload["market_data_state"] == "STALE"
    assert payload["status"] == "WARN"
    assert payload["operator_impact"] == "POST_SUBMIT_DIAGNOSTIC"


def test_snapshot_missing_symbol_coverage_is_gap(tmp_path: Path) -> None:
    _write_option_intent(tmp_path, symbol="QQQ")
    _write_snapshot(tmp_path, symbol="SPY", cert_until="2026-04-27T16:00:00Z")

    payload = evaluate_market_data_authority_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path, produced_utc="2026-04-27T15:00:00Z")

    assert payload["market_data_state"] in {"MISSING_REQUIRED_DATA", "COVERAGE_GAP"}


def test_selected_spy_long_equity_ignores_non_selected_iwm_option_intent(tmp_path: Path) -> None:
    _write_long_equity_intent(tmp_path, symbol="SPY", intent_id="intent_spy")
    _write_option_intent(tmp_path, symbol="IWM")
    graph_path = _write_requirement_graph(tmp_path, symbol="SPY", intent_id="intent_spy", family="EQUITY")
    _write_equity_snapshot(tmp_path, symbol="SPY")

    payload = evaluate_market_data_authority_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        execution_root=tmp_path,
        produced_utc="2026-04-27T15:00:00Z",
        evaluation_scope=SCOPE_SELECTED_INTENT_REQUIREMENT_GRAPH,
        requirement_graph_path=graph_path,
    )

    assert payload["status"] == "PASS"
    assert payload["market_data_state"] == "READY"
    assert payload["evaluation_scope"] == SCOPE_SELECTED_INTENT_REQUIREMENT_GRAPH
    assert payload["required_symbols"] == ["SPY"]
    assert payload["coverage"][0]["market_data_family"] == "EQUITY"


def test_selected_iwm_option_missing_snapshot_still_blocks(tmp_path: Path) -> None:
    _write_option_intent(tmp_path, symbol="IWM")
    graph_path = _write_requirement_graph(tmp_path, symbol="IWM", intent_id="intent", family="OPTIONS")

    payload = evaluate_market_data_authority_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        execution_root=tmp_path,
        produced_utc="2026-04-27T15:00:00Z",
        evaluation_scope=SCOPE_SELECTED_INTENT_REQUIREMENT_GRAPH,
        requirement_graph_path=graph_path,
    )

    assert payload["status"] == "FAIL"
    assert payload["market_data_state"] == "MISSING_REQUIRED_DATA"
    assert payload["required_symbols"] == ["IWM"]
    assert payload["first_blocker"] == "OPTIONS_CHAIN_SNAPSHOT_MISSING"


def test_all_active_intents_diagnostic_still_reports_non_selected_iwm_option_gap(tmp_path: Path) -> None:
    _write_long_equity_intent(tmp_path, symbol="SPY", intent_id="intent_spy")
    _write_option_intent(tmp_path, symbol="IWM")
    _write_equity_snapshot(tmp_path, symbol="SPY")

    payload = evaluate_market_data_authority_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        execution_root=tmp_path,
        produced_utc="2026-04-27T15:00:00Z",
    )

    assert payload["status"] == "FAIL"
    assert payload["required_symbols"] == ["IWM"]
    assert payload["first_blocker"] == "OPTIONS_CHAIN_SNAPSHOT_MISSING"


def test_selected_spy_long_equity_missing_equity_evidence_blocks(tmp_path: Path) -> None:
    _write_long_equity_intent(tmp_path, symbol="SPY", intent_id="intent_spy")
    _write_option_intent(tmp_path, symbol="IWM")
    graph_path = _write_requirement_graph(tmp_path, symbol="SPY", intent_id="intent_spy", family="EQUITY")

    payload = evaluate_market_data_authority_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        execution_root=tmp_path,
        produced_utc="2026-04-27T15:00:00Z",
        evaluation_scope=SCOPE_SELECTED_INTENT_REQUIREMENT_GRAPH,
        requirement_graph_path=graph_path,
    )

    assert payload["status"] == "FAIL"
    assert payload["market_data_state"] == "MISSING_REQUIRED_DATA"
    assert payload["required_symbols"] == ["SPY"]
    assert payload["first_blocker"] == "EQUITY_MARKET_DATA_SNAPSHOT_MISSING"
