from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.execution_evidence_current_head_v1 import (
    evaluate_execution_evidence_current_head_v1,
)


DAY = "2026-04-24"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _pointer(*, day: str, status: str, target: Path, produced_utc: str) -> dict:
    return {
        "day_utc": day,
        "status": status,
        "produced_utc": produced_utc,
        "pointers": {"submissions_day_dir": str(target)},
    }


def test_latest_pointer_old_day_fails(tmp_path: Path) -> None:
    execution_root = tmp_path
    target = execution_root / "execution_evidence_v1" / "submissions" / "2026-04-23"
    target.mkdir(parents=True, exist_ok=True)
    _write_json(
        execution_root / "execution_evidence_v1" / "latest_pointer.v1.json",
        _pointer(day="2026-04-23", status="OK", target=target, produced_utc="2026-04-24T10:00:00Z"),
    )
    payload = evaluate_execution_evidence_current_head_v1(day_utc=DAY, execution_root=execution_root)
    assert payload["status"] == "FAIL"
    assert any(item["reason"] == "STALE_DAY" for item in payload["rejected_candidates"])


def test_latest_pointer_failed_status_fails(tmp_path: Path) -> None:
    execution_root = tmp_path
    target = execution_root / "execution_evidence_v1" / "submissions" / DAY
    target.mkdir(parents=True, exist_ok=True)
    _write_json(
        execution_root / "execution_evidence_v1" / "latest_pointer.v1.json",
        _pointer(day=DAY, status="FAIL_SCHEMA_VIOLATION", target=target, produced_utc="2026-04-24T10:00:00Z"),
    )
    payload = evaluate_execution_evidence_current_head_v1(day_utc=DAY, execution_root=execution_root)
    assert payload["status"] == "FAIL"
    assert any(item["reason"] == "FAILED_STATUS" for item in payload["rejected_candidates"])


def test_valid_same_day_pointer_passes(tmp_path: Path) -> None:
    execution_root = tmp_path
    submissions_day = execution_root / "execution_evidence_v1" / "submissions" / DAY
    attempt = submissions_day / ("a" * 64)
    attempt.mkdir(parents=True, exist_ok=True)
    _write_json(
        execution_root / "execution_evidence_v1" / "latest_pointer.v1.json",
        _pointer(day=DAY, status="OK", target=submissions_day, produced_utc="2026-04-24T10:00:00Z"),
    )
    payload = evaluate_execution_evidence_current_head_v1(day_utc=DAY, execution_root=execution_root)
    assert payload["status"] == "PASS"
    assert payload["selected_attempt_id"] == ("a" * 64)


def test_quarantined_pointer_is_ignored(tmp_path: Path) -> None:
    execution_root = tmp_path
    submissions_day = execution_root / "execution_evidence_v1" / "submissions" / DAY
    submissions_day.mkdir(parents=True, exist_ok=True)
    quarantined = execution_root / "execution_evidence_v1" / "QUARANTINED.latest_pointer.v1.json"
    _write_json(
        quarantined,
        _pointer(day=DAY, status="OK", target=submissions_day, produced_utc="2026-04-24T10:00:00Z"),
    )
    payload = evaluate_execution_evidence_current_head_v1(day_utc=DAY, execution_root=execution_root)
    assert payload["status"] == "FAIL"


def test_missing_target_fails(tmp_path: Path) -> None:
    execution_root = tmp_path
    missing_target = execution_root / "execution_evidence_v1" / "submissions" / DAY
    _write_json(
        execution_root / "execution_evidence_v1" / "latest_pointer.v1.json",
        _pointer(day=DAY, status="OK", target=missing_target, produced_utc="2026-04-24T10:00:00Z"),
    )
    payload = evaluate_execution_evidence_current_head_v1(day_utc=DAY, execution_root=execution_root)
    assert payload["status"] == "FAIL"
    assert any(item["reason"] == "TARGET_MISSING" for item in payload["rejected_candidates"])


def test_multiple_candidates_choose_newest_valid_same_day(tmp_path: Path) -> None:
    execution_root = tmp_path
    submissions_day = execution_root / "execution_evidence_v1" / "submissions" / DAY
    first_attempt = submissions_day / ("a" * 64)
    second_attempt = submissions_day / ("b" * 64)
    first_attempt.mkdir(parents=True, exist_ok=True)
    second_attempt.mkdir(parents=True, exist_ok=True)

    _write_json(
        execution_root / "execution_evidence_v1" / "latest_pointer.v1.json",
        _pointer(day=DAY, status="OK", target=submissions_day, produced_utc="2026-04-24T09:00:00Z"),
    )
    _write_json(
        execution_root / "execution_evidence_v1" / "submissions" / DAY / "latest_pointer.v1.json",
        _pointer(day=DAY, status="OK", target=submissions_day, produced_utc="2026-04-24T10:00:00Z"),
    )
    payload = evaluate_execution_evidence_current_head_v1(day_utc=DAY, execution_root=execution_root)
    assert payload["status"] == "PASS"
    assert payload["selected_artifact_path"] == str(submissions_day.resolve())


def test_replay_snapshot_can_become_current_head_when_pointer_is_stale(tmp_path: Path) -> None:
    execution_root = tmp_path
    stale_target = execution_root / "execution_evidence_v1" / "submissions" / "2026-04-23"
    stale_target.mkdir(parents=True, exist_ok=True)
    _write_json(
        execution_root / "execution_evidence_v1" / "latest_pointer.v1.json",
        _pointer(day="2026-04-23", status="OK", target=stale_target, produced_utc="2026-04-24T09:00:00Z"),
    )
    replay_snapshot = (
        execution_root
        / "execution_stream_v1"
        / "replays"
        / DAY
        / "replay_20260426_034644"
        / "execution_stream_snapshot.v1.json"
    )
    _write_json(
        replay_snapshot,
        {
            "schema_version": "execution_stream_snapshot_replay.v1",
            "day": DAY,
            "status": "PASS",
            "generated_at_utc": "2026-04-24T10:00:00Z",
        },
    )

    payload = evaluate_execution_evidence_current_head_v1(day_utc=DAY, execution_root=execution_root)
    assert payload["status"] == "PASS"
    assert payload["selected_artifact_path"] == str(replay_snapshot.resolve())
