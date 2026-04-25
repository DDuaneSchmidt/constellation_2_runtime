from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import ops.tools.run_pointer_heads_materialize_v1 as pointer_module


DAY = "2026-04-22"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def test_fallback_authority_head_requires_promoted_active_session(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "active_session_v1" / "current.json",
        {
            "active_day": "2026-04-21",
            "target_day_admission_status": "ADMIT",
        },
    )
    _write_json(
        tmp_path / "reports" / "authorization_gate_verdict_v1" / DAY / "authorization_gate_verdict.v1.json",
        {
            "schema_id": "authorization_gate_verdict_v1",
            "schema_version": "1",
            "day_utc": DAY,
            "status": "PASS",
            "produced_utc": f"{DAY}T00:00:00Z",
        },
    )

    fallback = pointer_module._fallback_authority_head_for_expected_day(
        truth_root=tmp_path,
        expected_day_utc=DAY,
    )
    assert fallback is None


def test_fallback_authority_head_uses_passed_day_verdict_when_active_session_promoted(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "active_session_v1" / "current.json",
        {
            "active_day": DAY,
            "target_day_admission_status": "ADMIT",
        },
    )
    _write_json(
        tmp_path / "reports" / "authorization_gate_verdict_v1" / DAY / "authorization_gate_verdict.v1.json",
        {
            "schema_id": "authorization_gate_verdict_v1",
            "schema_version": "1",
            "day_utc": DAY,
            "status": "PASS",
            "produced_utc": f"{DAY}T12:00:00Z",
        },
    )

    fallback = pointer_module._fallback_authority_head_for_expected_day(
        truth_root=tmp_path,
        expected_day_utc=DAY,
        current_head_obj={
            "pointer_seq": 15,
            "attempt_seq": 3,
            "attempt_id": "2026-04-21__A0003__test",
            "mode": "PAPER",
            "producer_git_sha": "abc123",
        },
    )

    assert fallback is not None
    assert fallback["schema_id"] == "c2_run_pointer_canonical_authority_head"
    assert fallback["status"] == "PASS"
    assert fallback["authoritative"] is True
    assert fallback["day_utc"] == DAY
    assert "authorization_gate_verdict_v1/2026-04-22/authorization_gate_verdict.v1.json" in fallback["points_to"]
    assert "ACTIVE_SESSION_PROMOTED_AUTHORITY_HEAD_FALLBACK" in fallback["reason_codes"]


def test_fallback_uses_canonical_root_when_scoped_root_lacks_active_session(monkeypatch, tmp_path: Path) -> None:
    scoped_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    canonical_root = tmp_path / "truth"

    _write_json(
        canonical_root / "active_session_v1" / "current.json",
        {
            "active_day": DAY,
            "target_day_admission_status": "ADMIT",
        },
    )
    _write_json(
        canonical_root / "reports" / "authorization_gate_verdict_v1" / DAY / "authorization_gate_verdict.v1.json",
        {
            "schema_id": "authorization_gate_verdict_v1",
            "schema_version": "1",
            "day_utc": DAY,
            "status": "PASS",
            "produced_utc": f"{DAY}T12:00:00Z",
        },
    )
    monkeypatch.setattr(pointer_module, "resolve_canonical_truth_root", lambda: canonical_root)
    monkeypatch.setattr(pointer_module, "resolve_truth_root", lambda repo_root: scoped_root)

    fallback = pointer_module._fallback_authority_head_for_expected_day(
        truth_root=scoped_root,
        expected_day_utc=DAY,
    )

    assert fallback is not None
    assert fallback["day_utc"] == DAY
    assert str(canonical_root / "reports" / "authorization_gate_verdict_v1" / DAY / "authorization_gate_verdict.v1.json") == fallback["points_to"]
