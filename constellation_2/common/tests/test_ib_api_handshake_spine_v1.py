from __future__ import annotations

import json
from pathlib import Path

from ops.tools.run_ib_api_handshake_spine_v1 import (
    _paths_for_day,
    _write_latest_pointer_if_monotonic,
    main,
)


DAY = "2026-04-10"


def test_latest_pointer_rolls_forward_across_days(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    broker_events_root = truth_root / "execution_evidence_v1" / "broker_events"
    paths = _paths_for_day(DAY, truth_root=truth_root, broker_events_root=broker_events_root)
    paths.latest_path.parent.mkdir(parents=True, exist_ok=True)
    paths.latest_path.write_text(
        json.dumps(
            {
                "schema_id": "C2_IB_API_HANDSHAKE_LATEST_POINTER_V1",
                "schema_version": 1,
                "day_utc": "2026-04-09",
                "pointers": {
                    "handshake_path": str((truth_root / "ib_api_handshake" / "2026-04-09" / "ib_api_handshake.v1.json").resolve()),
                    "handshake_sha256": "b" * 64,
                },
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )
    _write_latest_pointer_if_monotonic(day_utc=DAY, paths=paths, out_sha256="a" * 64, status="OK", reason_codes=[])
    latest = json.loads(paths.latest_path.read_text(encoding="utf-8"))
    assert latest["day_utc"] == DAY
    assert latest["pointers"]["handshake_sha256"] == "a" * 64


def test_handshake_spine_succeeds_when_later_pointer_already_exists(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    broker_day_dir = truth_root / "execution_evidence_v1" / "broker_events" / DAY
    broker_day_dir.mkdir(parents=True, exist_ok=True)
    (broker_day_dir / "broker_event_log.v1.jsonl").write_text(
        json.dumps(
            {
                "event_type": "nextValidId",
                "ib_fields": {"args": [{"value": "orderId=44"}]},
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )
    latest_path = truth_root / "ib_api_handshake" / "latest_pointer.v1.json"
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(
        json.dumps(
            {
                "schema_id": "C2_IB_API_HANDSHAKE_LATEST_POINTER_V1",
                "schema_version": 1,
                "day_utc": "2026-05-02",
                "status": "OK",
                "reason_codes": ["HANDSHAKE_OK_NEXTVALIDID_SEEN_NO_504_AFTER"],
                "pointers": {
                    "handshake_path": str((truth_root / "ib_api_handshake" / "2026-05-02" / "ib_api_handshake.v1.json").resolve()),
                    "handshake_sha256": "b" * 64,
                },
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )

    rc = main(["--day_utc", DAY, "--truth_root", str(truth_root)])
    assert rc == 0
    handshake_path = truth_root / "ib_api_handshake" / DAY / "ib_api_handshake.v1.json"
    handshake = json.loads(handshake_path.read_text(encoding="utf-8"))
    latest = json.loads(latest_path.read_text(encoding="utf-8"))
    assert handshake["day_utc"] == DAY
    assert handshake["status"] == "OK"
    assert latest["day_utc"] == "2026-05-02"


def test_handshake_spine_writes_produced_utc_and_current_pointer(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    broker_day_dir = truth_root / "execution_evidence_v1" / "broker_events" / DAY
    broker_day_dir.mkdir(parents=True, exist_ok=True)
    (broker_day_dir / "broker_event_log.v1.jsonl").write_text(
        json.dumps(
            {
                "event_type": "nextValidId",
                "ib_fields": {"args": [{"value": "orderId=44"}]},
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )
    rc = main(["--day_utc", DAY, "--truth_root", str(truth_root)])
    assert rc == 0
    handshake_path = truth_root / "ib_api_handshake" / DAY / "ib_api_handshake.v1.json"
    pointer_path = truth_root / "ib_api_handshake" / "latest_pointer.v1.json"
    handshake = json.loads(handshake_path.read_text(encoding="utf-8"))
    latest = json.loads(pointer_path.read_text(encoding="utf-8"))
    assert handshake["produced_utc"] == f"{DAY}T00:00:00Z"
    assert latest["day_utc"] == DAY
