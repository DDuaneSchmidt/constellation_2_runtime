from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.aegis_chatgpt_packet as packet


DAY = "2026-04-27"
SUBMISSION_ID = "9" * 64


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def test_packet_reports_dry_run_submit_without_live_failure_stage(tmp_path: Path) -> None:
    runtime_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    submission_dir = runtime_root / "execution_evidence_v1" / "submissions" / DAY / SUBMISSION_ID
    _write_json(
        submission_dir / "broker_submission_record.v2.json",
        {
            "submission_id": SUBMISSION_ID,
            "submitted_at_utc": f"{DAY}T15:00:00Z",
            "status": "PENDINGSUBMIT",
            "broker_ids": {"order_id": None, "perm_id": None},
            "error": {"code": "DRY_RUN_NO_BROKER_ID"},
        },
    )
    _write_json(
        submission_dir / "broker_submit_attempt_v1.json",
        {"submission_id": SUBMISSION_ID, "dry_run": True, "reason_codes": ["DRY_RUN_SUBMIT_ATTEMPT"]},
    )
    _write_json(
        tmp_path / "truth" / "reports" / "execution_mode_authority_v1" / DAY / "execution_mode_authority.v1.json",
        {
            "mode_state": "DRY_RUN_LOCKED",
            "dry_run_policy": "YES",
            "broker_transmit_enabled": False,
            "missing_broker_ids_blocker": False,
            "missing_broker_ids_diagnostic": True,
        },
    )

    section = packet._build_latest_attempt_section(  # noqa: SLF001
        packet.RootResolution(
            canonical_truth_root=tmp_path / "truth",
            runtime_truth_root=runtime_root,
            truth_sleeves_root=tmp_path / "truth_sleeves",
            authority_source="test",
            evidence="test",
            error="",
        )
    )

    assert "- dry_run_policy: YES" in section
    assert "- broker_transmit_enabled: false" in section
    assert "- execution_mode: DRY_RUN_LOCKED" in section
    assert "- submit_mode_status: DRY_RUN_COMPLETE" in section
    assert "- broker_order_transmitted: NO" in section
    assert "- missing_broker_ids: DIAGNOSTIC" in section
    assert "- failure_stage: NONE" in section
