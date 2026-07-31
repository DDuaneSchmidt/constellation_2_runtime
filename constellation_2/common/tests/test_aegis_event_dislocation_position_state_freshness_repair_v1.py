from __future__ import annotations

import json
from pathlib import Path

from ops.aegis.event_dislocation_position_state_freshness_repair_v1 import (
    build_event_dislocation_position_state_freshness_repair_v1,
)

DAY = "2026-06-02"


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _sleeve_root(root: Path) -> Path:
    return root.parent / "truth_sleeves" / "PRIMARY" / "PAPER"


def _seed_t04(root: Path, blocker: str = "POSITION_STATE_STALE") -> None:
    _write_json(
        root / "reports" / "aegis_missing_market_data_repair_v1" / DAY / "missing_market_data_repair.v1.json",
        {
            "sleeves": [
                {
                    "sleeve_id": "C2_EVENT_DISLOCATION_V1",
                    "repair_status": "REPAIRED",
                    "repair_type": "INGESTION_COMPLETENESS_REPAIR",
                    "remaining_blocker": blocker,
                    "post_repair_t02_reason_code": blocker,
                    "original_t03_resolution_type": "SOURCE_AVAILABLE_BUT_INCOMPLETE",
                }
            ]
        },
    )


def _seed_fresh_source(root: Path, day: str = DAY) -> Path:
    return _write_json(
        _sleeve_root(root) / "positions_snapshot_v2" / "snapshots" / DAY / "positions_snapshot.v2.json",
        {
            "schema_id": "C2_POSITIONS_SNAPSHOT_V2",
            "schema_version": 2,
            "day_utc": day,
            "status": "BOOTSTRAP",
            "produced_utc": f"{day}T00:00:00Z",
            "positions": {"asof_utc": f"{day}T00:00:00Z", "items": []},
            "input_manifest": [
                {
                    "type": "bootstrap_failure_evidence",
                    "path": str(_sleeve_root(root) / "positions_v1" / "failures" / DAY / "positions_snapshot.v5.failure.json"),
                    "sha256": "abc",
                }
            ],
            "reason_codes": ["DAY0_BOOTSTRAP_EMPTY_POSITIONS_COMPAT_BRIDGE"],
        },
    )


def _seed_post_t01(root: Path, status: str = "BLOCKED", blocker: str = "SIGNALS_PRESENT_BUT_NO_CANDIDATES") -> None:
    _write_json(
        root / "reports" / "aegis_sleeve_throughput_diagnostics_v1" / DAY / "sleeve_throughput_diagnostics.v1.json",
        {
            "sleeves": [
                {
                    "sleeve_id": "C2_EVENT_DISLOCATION_V1",
                    "throughput_status": status,
                    "blocker_code": blocker,
                    "raw_signal_count": 19,
                    "candidate_count": 0,
                }
            ]
        },
    )


def _seed_post_t02(root: Path) -> None:
    _write_json(
        root / "reports" / "aegis_dormant_sleeve_signal_generation_diagnostics_v1" / DAY / "dormant_sleeve_signal_generation_diagnostics.v1.json",
        {"dormant_sleeves": []},
    )


def test_position_state_stale_after_t04_is_detected_and_repaired_from_fresh_source(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_t04(root)
    source = _seed_fresh_source(root)
    _seed_post_t01(root)
    _seed_post_t02(root)

    payload = build_event_dislocation_position_state_freshness_repair_v1(truth_root=root, day_utc=DAY)
    routed = _sleeve_root(root) / "positions_v1" / "snapshots" / DAY / "positions_snapshot.v2.json"

    assert payload["original_blocker"] == "POSITION_STATE_STALE"
    assert payload["source_state_found"] is True
    assert payload["repair_status"] == "REPAIRED"
    assert payload["repair_type"] == "POSITION_STATE_ROUTING_REPAIR"
    assert payload["post_repair_position_state_status"] == "FRESH"
    assert routed.exists()
    assert routed.read_text(encoding="utf-8") == source.read_text(encoding="utf-8")


def test_fresh_authoritative_position_state_clears_position_blocker(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_t04(root)
    _seed_fresh_source(root)
    _seed_post_t01(root, status="FLOWING", blocker="NONE")
    _seed_post_t02(root)

    payload = build_event_dislocation_position_state_freshness_repair_v1(truth_root=root, day_utc=DAY)

    assert payload["post_repair_position_state_status"] == "FRESH"
    assert payload["remaining_blocker"] == "NONE"
    assert payload["owner"] == "NONE"


def test_missing_position_state_fails_closed(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_t04(root)
    _seed_post_t01(root)
    _seed_post_t02(root)

    payload = build_event_dislocation_position_state_freshness_repair_v1(truth_root=root, day_utc=DAY)

    assert payload["repair_status"] == "NOT_REPAIRED_SOURCE_TRULY_MISSING"
    assert payload["remaining_blocker"] == "SOURCE_TRULY_MISSING"
    assert payload["david_action_required"] is False


def test_stale_position_state_fails_closed(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_t04(root)
    _seed_fresh_source(root, day="2026-06-01")
    _seed_post_t01(root)
    _seed_post_t02(root)

    payload = build_event_dislocation_position_state_freshness_repair_v1(truth_root=root, day_utc=DAY)

    assert payload["repair_status"] == "NOT_REPAIRED_SOURCE_STALE"
    assert payload["source_state_fresh_after_repair"] is False
    assert "2026-06-01" in payload["blocker_reason"]


def test_misdated_state_is_not_timestamp_normalized_without_target_day_authority(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_t04(root)
    _seed_fresh_source(root, day="2026-06-01")
    _seed_post_t01(root)
    _seed_post_t02(root)

    build_event_dislocation_position_state_freshness_repair_v1(truth_root=root, day_utc=DAY)
    routed = _sleeve_root(root) / "positions_v1" / "snapshots" / DAY / "positions_snapshot.v2.json"

    assert not routed.exists()


def test_state_routing_repair_does_not_fabricate_position_state(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_t04(root)
    source = _seed_fresh_source(root)
    source_before = source.read_bytes()
    _seed_post_t01(root)
    _seed_post_t02(root)

    build_event_dislocation_position_state_freshness_repair_v1(truth_root=root, day_utc=DAY)

    assert source.read_bytes() == source_before


def test_post_repair_reports_next_non_position_blocker(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_t04(root)
    _seed_fresh_source(root)
    _seed_post_t01(root, status="BLOCKED", blocker="SIGNALS_PRESENT_BUT_NO_CANDIDATES")
    _seed_post_t02(root)

    payload = build_event_dislocation_position_state_freshness_repair_v1(truth_root=root, day_utc=DAY)

    assert payload["post_repair_t02_reason_code"] == "NO_T02_ROW"
    assert payload["remaining_blocker"] == "SIGNALS_PRESENT_BUT_NO_CANDIDATES"
    assert "Position-state freshness blocker cleared" in payload["blocker_reason"]


def test_t04_market_data_repair_status_remains_traceable(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_t04(root)
    _seed_fresh_source(root)
    _seed_post_t01(root)
    _seed_post_t02(root)

    payload = build_event_dislocation_position_state_freshness_repair_v1(truth_root=root, day_utc=DAY)

    assert payload["original_t04_status"] == "REPAIRED"
    assert payload["source_artifact_paths"]["t04"].endswith("missing_market_data_repair.v1.json")


def test_strategy_thresholds_and_safety_gates_remain_unchanged(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_t04(root)
    _seed_fresh_source(root)
    _seed_post_t01(root)
    _seed_post_t02(root)

    payload = build_event_dislocation_position_state_freshness_repair_v1(truth_root=root, day_utc=DAY)

    assert payload["no_strategy_logic_mutation"] is True
    assert payload["no_threshold_mutation"] is True
    assert payload["no_candidate_scoring_mutation"] is True
    assert payload["no_risk_policy_mutation"] is True
    assert payload["broker_execution_allowed"] is False
    assert payload["live_trading_allowed"] is False
    assert payload["autonomous_execution_allowed"] is False


def test_no_repair_required_when_consumer_route_is_already_fresh(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_t04(root, blocker="NONE")
    source = _seed_fresh_source(root)
    routed = _sleeve_root(root) / "positions_v1" / "snapshots" / DAY / "positions_snapshot.v2.json"
    _write_json(routed, json.loads(source.read_text(encoding="utf-8")))
    _seed_post_t01(root, status="FLOWING", blocker="NONE")
    _seed_post_t02(root)

    payload = build_event_dislocation_position_state_freshness_repair_v1(truth_root=root, day_utc=DAY)

    assert payload["repair_status"] == "NO_REPAIR_REQUIRED"
    assert payload["repair_attempted"] is False
