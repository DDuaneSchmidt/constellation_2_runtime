from __future__ import annotations

import json
import sys
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


REPO_ROOT = _repo_root()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_path_alignment_v1 import resolve_runtime_ledger_path  # noqa: E402
from constellation_2.common.runtime_ledger_v1 import append_runtime_ledger_events_v1  # noqa: E402


DAY = "2026-04-14"


def _event(*, event_type: str, payload_hash: str, identity_key: str) -> dict[str, object]:
    return {
        "event_type": event_type,
        "produced_utc": f"{DAY}T00:00:00Z",
        "owner_plane": "SESSION_PLANE",
        "owner_tool": "tests/runtime_ledger_v1",
        "owner_run_id": "run-1",
        "run_id": "run-1",
        "session_id": "paper_session:2026-04-14:PAPER",
        "submission_id": "",
        "order_id": "",
        "perm_id": "",
        "payload_ref": "/tmp/bootstrap",
        "payload_hash": payload_hash,
        "identity_key": identity_key,
        "identity_tuple": {
            "day_utc": DAY,
            "owner_plane": "SESSION_PLANE",
            "owner_run_id": "run-1",
            "session_id": "paper_session:2026-04-14:PAPER",
            "submission_id": "",
            "order_id": "",
            "perm_id": "",
        },
        "event_payload_summary": {"status": event_type},
    }


def test_runtime_ledger_appends_new_events_without_rewriting_existing_history(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    first = _event(
        event_type="BOOTSTRAP_STARTED",
        payload_hash="a" * 64,
        identity_key="paper_session:2026-04-14:PAPER:BOOTSTRAP_STARTED",
    )
    second = _event(
        event_type="BOOTSTRAP_READY",
        payload_hash="b" * 64,
        identity_key="paper_session:2026-04-14:PAPER:BOOTSTRAP_READY",
    )

    append_runtime_ledger_events_v1(truth_root=truth_root, day_utc=DAY, events=[first])
    append_runtime_ledger_events_v1(truth_root=truth_root, day_utc=DAY, events=[first])
    append_runtime_ledger_events_v1(truth_root=truth_root, day_utc=DAY, events=[second])

    ledger_path = resolve_runtime_ledger_path(truth_root=truth_root, day_utc=DAY)
    rows = [json.loads(line) for line in ledger_path.read_text(encoding="utf-8").splitlines()]
    assert [row["event_type"] for row in rows] == ["BOOTSTRAP_STARTED", "BOOTSTRAP_READY"]
    assert len({row["ledger_event_id"] for row in rows}) == 2


def test_runtime_ledger_accepts_position_and_exit_plane_events(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    event = {
        "event_type": "EXIT_DECISION_RECORDED",
        "produced_utc": f"{DAY}T00:00:00Z",
        "owner_plane": "EXIT_DECISION_PLANE",
        "owner_tool": "tests/exit_decision_engine_v1",
        "owner_run_id": "run-2",
        "run_id": "run-2",
        "session_id": "paper_session:2026-04-14:PAPER",
        "submission_id": "",
        "order_id": "",
        "perm_id": "",
        "payload_ref": "/tmp/exit_decision.v1.json",
        "payload_hash": "c" * 64,
        "identity_key": "paper_session:2026-04-14:PAPER:EXIT_DECISION:pos-1",
        "identity_tuple": {
            "day_utc": DAY,
            "owner_plane": "EXIT_DECISION_PLANE",
            "owner_run_id": "run-2",
            "session_id": "paper_session:2026-04-14:PAPER",
            "submission_id": "",
            "order_id": "",
            "perm_id": "",
        },
        "event_payload_summary": {"position_id": "pos-1", "management_state": "EXIT_PENDING"},
    }
    append_runtime_ledger_events_v1(truth_root=truth_root, day_utc=DAY, events=[event])
    ledger_path = resolve_runtime_ledger_path(truth_root=truth_root, day_utc=DAY)
    rows = [json.loads(line) for line in ledger_path.read_text(encoding="utf-8").splitlines()]
    assert rows[0]["event_type"] == "EXIT_DECISION_RECORDED"
    assert rows[0]["owner_plane"] == "EXIT_DECISION_PLANE"
