from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.market_data_authority_v1 import evaluate_market_data_authority_v1


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


def _write_snapshot(root: Path, *, symbol: str = "SPY", cert_until: str = "2026-04-27T16:00:00Z", contracts: int = 1) -> None:
    capture = root / "options_chain_snapshot_v1" / DAY / f"ib_capture_{symbol}"
    _write_json(
        capture / "options_chain_snapshot.v1.json",
        {"schema_id": "options_chain_snapshot", "underlying": {"symbol": symbol}, "contracts": [{"conid": idx} for idx in range(contracts)]},
    )
    _write_json(capture / "freshness_certificate.v1.json", {"valid_until_utc": cert_until})


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
