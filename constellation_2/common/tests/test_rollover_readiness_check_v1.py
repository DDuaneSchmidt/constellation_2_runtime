from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools import run_rollover_readiness_check_v1 as rollover  # noqa: E402


DAY = "2026-04-22"


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True) + "\n", encoding="utf-8")


def test_rollover_not_ready_when_consistency_gate_fails(tmp_path: Path, monkeypatch) -> None:
    truth_root = (tmp_path / "truth").resolve()
    truth_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(
        rollover,
        "evaluate_next_day_readiness_consistency_gate_v1",
        lambda **_kwargs: SimpleNamespace(status="FAIL", blocking_reason_codes=("CONSISTENCY_GATE_FAILURE",)),
    )
    payload = rollover.classify_rollover_readiness_v1(truth_root=truth_root, day_utc=DAY)
    assert payload["classification"] == "NOT_READY_STRUCTURAL"
    assert "CONSISTENCY_GATE_FAILURE" in payload["reason_codes"]


def test_rollover_awaits_live_only_when_preopen_has_live_only_blockers(tmp_path: Path, monkeypatch) -> None:
    truth_root = (tmp_path / "truth").resolve()
    _write_json(
        truth_root / "reports" / "pre_open_bundle_v1" / DAY / "pre_open_bundle.v1.json",
        {
            "materialization_state": "BLOCKED",
            "reason_codes": [
                "TARGET_DAY_DATE_MISMATCH",
                "IB_API_HANDSHAKE_NOT_OK",
                "BROKER_EVENTS_MISSING",
            ],
        },
    )
    monkeypatch.setattr(
        rollover,
        "evaluate_next_day_readiness_consistency_gate_v1",
        lambda **_kwargs: SimpleNamespace(status="PASS", blocking_reason_codes=()),
    )
    payload = rollover.classify_rollover_readiness_v1(truth_root=truth_root, day_utc=DAY)
    assert payload["classification"] == "AWAITING_TOMORROW_LIVE_ONLY"


def test_rollover_not_ready_when_preopen_blocker_is_not_live_only(tmp_path: Path, monkeypatch) -> None:
    truth_root = (tmp_path / "truth").resolve()
    _write_json(
        truth_root / "reports" / "pre_open_bundle_v1" / DAY / "pre_open_bundle.v1.json",
        {
            "materialization_state": "BLOCKED",
            "reason_codes": ["TARGET_DAY_DATE_MISMATCH", "PRIMARY_AUTHORIZATION_VERDICT_NOT_READY"],
        },
    )
    monkeypatch.setattr(
        rollover,
        "evaluate_next_day_readiness_consistency_gate_v1",
        lambda **_kwargs: SimpleNamespace(status="PASS", blocking_reason_codes=()),
    )
    payload = rollover.classify_rollover_readiness_v1(truth_root=truth_root, day_utc=DAY)
    assert payload["classification"] == "NOT_READY_STRUCTURAL"
    assert "PRIMARY_AUTHORIZATION_VERDICT_NOT_READY" in payload["reason_codes"]


def test_rollover_structurally_ready_when_consistency_passes_and_no_preopen_block(tmp_path: Path, monkeypatch) -> None:
    truth_root = (tmp_path / "truth").resolve()
    truth_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(
        rollover,
        "evaluate_next_day_readiness_consistency_gate_v1",
        lambda **_kwargs: SimpleNamespace(status="PASS", blocking_reason_codes=()),
    )
    payload = rollover.classify_rollover_readiness_v1(truth_root=truth_root, day_utc=DAY)
    assert payload["classification"] == "STRUCTURALLY_READY_TONIGHT"
