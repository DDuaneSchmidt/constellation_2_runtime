from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.phaseL.ui_api.readiness_kernel_v1 import build_readiness_kernel_v1  # noqa: E402


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def test_final_readiness_comes_only_from_control_plane(tmp_path: Path) -> None:
    day = "2026-04-29"
    truth = tmp_path / "production_truth"
    sleeve = tmp_path / "sleeve"
    _write(
        truth / "reports" / "aegis_control_plane_v1" / day / "control_plane.v1.json",
        {
            "day_utc": day,
            "runtime_mode": "PRODUCTION",
            "final_status": "NOT_READY",
            "current_phase": "SESSION_AUTHORITY",
            "canonical_blocker": "SESSION_AUTHORITY_MISSING",
            "blocker_owner": "session_authority",
            "recovery_action": "Run governed session authority.",
            "recovery_commands": ["session command"],
            "evidence_paths": [str(truth / "active_session_v1/current.json")],
            "deferred_phases": ["BROKER_HEALTH"],
            "phase_results": [],
        },
    )
    _write(
        truth / "reports" / "aegis_day_run_v1" / day / "day_run.v1.json",
        {"day_utc": day, "final_status": "PAPER_READY", "canonical_blocker": ""},
    )

    payload = build_readiness_kernel_v1(day, truth_root=truth, sleeve_truth_root=sleeve)

    assert payload["final_readiness_authority"] == "aegis_control_plane_v1"
    assert payload["overall_status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "SESSION_AUTHORITY_MISSING"


def test_supporting_surfaces_cannot_override_control_plane(tmp_path: Path) -> None:
    day = "2026-04-29"
    truth = tmp_path / "production_truth"
    sleeve = tmp_path / "sleeve"
    _write(
        truth / "reports" / "aegis_control_plane_v1" / day / "control_plane.v1.json",
        {
            "day_utc": day,
            "runtime_mode": "PRODUCTION",
            "final_status": "NOT_READY",
            "current_phase": "BROKER_HEALTH",
            "canonical_blocker": "BROKER_EVENT_LOG_MISSING",
            "blocker_owner": "broker_health_authority",
            "recovery_action": "Start broker observer.",
            "recovery_commands": ["broker command"],
            "evidence_paths": [str(truth / "broker_event_log.v1.jsonl")],
            "deferred_phases": ["BOD_INPUTS"],
            "phase_results": [],
        },
    )
    _write(
        truth / "reports" / "unified_truth_kernel_v1" / day / "unified_truth_kernel.v1.json",
        {"day_utc": day, "final_status": "READY", "canonical_blocker": ""},
    )

    payload = build_readiness_kernel_v1(day, truth_root=truth, sleeve_truth_root=sleeve)

    assert payload["overall_status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "BROKER_EVENT_LOG_MISSING"
    assert payload["supporting_evidence_only_paths"]["unified_truth_kernel"].endswith("unified_truth_kernel.v1.json")
