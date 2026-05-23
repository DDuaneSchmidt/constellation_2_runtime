from __future__ import annotations

import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.candidate_observability_v1 import build_candidate_generation_manifest_v1
from ops.aegis.candidate_snapshot_plane_v1 import (
    build_candidate_snapshot_v1,
    execution_firewall_validate_candidate_snapshot_v1,
    promote_certified_candidate_snapshot_v1,
    replay_candidate_snapshot_v1,
    write_candidate_snapshot_v1,
)
from ops.aegis.market_data.freshness_policy_v1 import (
    CERTIFICATION_PENDING,
    PARTIAL_DATA_AVAILABLE,
    READ_ONLY_PRIOR_DAY_FALLBACK,
    VALIDATED_CURRENT_DAY,
    classify_market_data_freshness_v1,
)
from ops.aegis.market_data.market_data_snapshot_plane_v1 import build_market_data_snapshot_v1, write_market_data_snapshot_v1


DAY = "2026-05-22"


def _write_market_report(root: Path, *, certification_state: str, freshness_state: str, fetched: list[str] | None = None, missing: list[str] | None = None) -> dict:
    report = {
        "schema_id": "aegis_market_data",
        "artifact_id": "aegis_market_data_v1",
        "day_utc": DAY,
        "generated_at_utc": "2026-05-22T17:00:00Z",
        "source": "TEST_VENDOR",
        "freshness_state": freshness_state,
        "validation_status": freshness_state,
        "certification_state": certification_state,
        "final_eod_ready": certification_state == "CERTIFIED",
        "final_eod_certification_status": "VALID" if certification_state == "CERTIFIED" else "PENDING",
        "requested_symbols": ["SPY", "QQQ"],
        "fetched_symbols": fetched or ["SPY", "QQQ"],
        "missing_symbols": missing or [],
        "stale_symbols": [],
        "symbols": {symbol: {"symbol": symbol, "close": 100, "freshness_status": "CURRENT", "market_session_date": DAY} for symbol in (fetched or ["SPY", "QQQ"])},
        "normalized_records": [],
    }
    snapshot = build_market_data_snapshot_v1(truth_root=root, day_utc=DAY, market_report=report)
    paths = write_market_data_snapshot_v1(truth_root=root, snapshot=snapshot)
    report["market_data_snapshot_id"] = paths["snapshot_id"]
    report["market_data_snapshot_path"] = paths["json"]
    report["market_data_snapshot_hash"] = paths["hash"]
    report["input_market_data_snapshot_ids"] = [paths["snapshot_id"]]
    report["candidate_lane"] = "CERTIFIED" if certification_state == "CERTIFIED" else "PROVISIONAL"
    path = root / "reports" / "aegis_market_data_v1" / DAY / "market_data.v1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, sort_keys=True), encoding="utf-8")
    return report


def _manifest(root: Path) -> dict:
    return build_candidate_generation_manifest_v1(
        day_utc=DAY,
        environment="PAPER",
        truth_root=root,
        outcomes=[
            {
                "engine_id": "C2_TREND_EQ_PRIMARY_V1",
                "sleeve_id": "C2_TREND_EQ_PRIMARY_V1",
                "status": "INTENT_CREATED",
                "output_intents": [{"intent_id": "intent-spy", "intent_path": "/tmp/intent.json", "intent_hash": "a" * 64, "symbol": "SPY"}],
                "reason_codes": ["INTENT_OUTPUT_CREATED"],
            }
        ],
        run_id=f"sleeve_evaluation_kernel_v1:{DAY}",
        produced_at_utc="2026-05-22T17:01:00Z",
        source_rollup_path="/tmp/rollup.json",
        run_mode="INTRADAY_OPERATIONAL",
    )


def test_vendor_lag_with_provisional_candidates_visible(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _write_market_report(root, certification_state=CERTIFICATION_PENDING, freshness_state=VALIDATED_CURRENT_DAY)
    manifest = _manifest(root)
    assert manifest["candidate_lane"] == "PROVISIONAL"
    assert manifest["certification_label"] == "NON_CERTIFIED"
    assert manifest["read_only"] is True
    assert manifest["execution_eligible"] is False
    assert manifest["candidate_rows"][0]["input_market_data_snapshot_ids"]


def test_partial_feed_with_provisional_candidates_visible(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _write_market_report(root, certification_state=PARTIAL_DATA_AVAILABLE, freshness_state=PARTIAL_DATA_AVAILABLE, fetched=["SPY"], missing=["QQQ"])
    manifest = _manifest(root)
    snapshot = build_candidate_snapshot_v1(truth_root=root, day_utc=DAY, candidate_manifest=manifest)
    assert snapshot["lane"] == "PROVISIONAL"
    assert snapshot["candidate_count"] == 1
    assert snapshot["read_only"] is True


def test_stale_feed_rejected_by_firewall(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _write_market_report(root, certification_state="STALE", freshness_state=READ_ONLY_PRIOR_DAY_FALLBACK)
    snapshot = build_candidate_snapshot_v1(truth_root=root, day_utc=DAY, candidate_manifest=_manifest(root))
    result = execution_firewall_validate_candidate_snapshot_v1(snapshot)
    assert result["status"] == "REJECTED"
    assert result["reason"] == "NON_CERTIFIED_CANDIDATE_SNAPSHOT"


def test_early_close_and_market_holiday_are_calendar_aware(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    early = classify_market_data_freshness_v1(
        truth_root=root,
        day_utc="2026-07-02",
        required_symbols=["SPY"],
        fetched_symbols=["SPY"],
        missing_symbols=[],
        stale_symbols=[],
        current_session_symbols=["SPY"],
        as_of_utc="2026-07-02T17:30:00Z",
        vendor_lag_minutes=30,
    )
    assert early.market_calendar["equity_early_close"] is True
    holiday = classify_market_data_freshness_v1(
        truth_root=root,
        day_utc="2026-07-04",
        required_symbols=["SPY"],
        fetched_symbols=[],
        missing_symbols=["SPY"],
        stale_symbols=[],
        current_session_symbols=[],
        as_of_utc="2026-07-04T17:30:00Z",
    )
    assert holiday.freshness_state == READ_ONLY_PRIOR_DAY_FALLBACK


def test_eod_certification_promotion_and_no_overwrite(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    provisional_report = _write_market_report(root, certification_state=CERTIFICATION_PENDING, freshness_state=VALIDATED_CURRENT_DAY)
    provisional_snapshot = build_candidate_snapshot_v1(truth_root=root, day_utc=DAY, candidate_manifest=_manifest(root))
    provisional_paths = write_candidate_snapshot_v1(truth_root=root, snapshot=provisional_snapshot)
    certified_report = _write_market_report(root, certification_state="CERTIFIED", freshness_state=VALIDATED_CURRENT_DAY)
    promoted = promote_certified_candidate_snapshot_v1(
        truth_root=root,
        day_utc=DAY,
        provisional_snapshot=provisional_snapshot,
        certified_market_data_snapshot_id=certified_report["market_data_snapshot_id"],
    )
    certified_paths = write_candidate_snapshot_v1(truth_root=root, snapshot=promoted)
    assert provisional_report["market_data_snapshot_id"] != certified_report["market_data_snapshot_id"]
    assert provisional_paths["json"] != certified_paths["json"]
    assert Path(provisional_paths["json"]).exists()
    assert execution_firewall_validate_candidate_snapshot_v1(promoted)["status"] == "PASS"


def test_failed_certification_rollback_keeps_provisional_snapshot(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _write_market_report(root, certification_state=CERTIFICATION_PENDING, freshness_state=VALIDATED_CURRENT_DAY)
    provisional = build_candidate_snapshot_v1(truth_root=root, day_utc=DAY, candidate_manifest=_manifest(root))
    paths = write_candidate_snapshot_v1(truth_root=root, snapshot=provisional)
    assert Path(paths["json"]).exists()
    assert execution_firewall_validate_candidate_snapshot_v1(provisional)["status"] == "REJECTED"


def test_replay_reconstructs_candidate_run_from_snapshot_ids(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _write_market_report(root, certification_state=CERTIFICATION_PENDING, freshness_state=VALIDATED_CURRENT_DAY)
    snapshot = build_candidate_snapshot_v1(truth_root=root, day_utc=DAY, candidate_manifest=_manifest(root))
    paths = write_candidate_snapshot_v1(truth_root=root, snapshot=snapshot)
    replay = replay_candidate_snapshot_v1(truth_root=root, day_utc=DAY, snapshot_ids=[paths["snapshot_id"]])
    assert replay["replay_status"] == "PASS"
    assert replay["snapshots"][0]["candidate_count"] == 1
    assert replay["snapshots"][0]["input_market_data_snapshot_ids"]
