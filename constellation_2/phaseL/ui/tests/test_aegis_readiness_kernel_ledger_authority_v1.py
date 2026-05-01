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


def test_final_readiness_comes_only_from_day_run_ledger(tmp_path: Path) -> None:
    day = "2026-04-29"
    truth = tmp_path / "truth"
    sleeve = tmp_path / "sleeve"
    _write(
        truth / "reports" / "aegis_day_run_v1" / day / "day_run.v1.json",
        {"day_utc": day, "final_status": "NOT_READY", "canonical_blocker": "SOURCE_REPRODUCIBILITY_BLOCKED", "operator_next_action": "Clean repo."},
    )
    _write(
        truth / "reports" / "submit_boundary_status_v1" / day / "submit_boundary_status.v1.json",
        {"day_utc": day, "status": "READY", "submission_authorized": True, "canonical_blocker": ""},
    )

    payload = build_readiness_kernel_v1(day, truth_root=truth, sleeve_truth_root=sleeve)

    assert payload["final_readiness_authority"] == "aegis_day_run_ledger_v1"
    assert payload["overall_status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "SOURCE_REPRODUCIBILITY_BLOCKED"


def test_diagnostic_surface_cannot_override_ready_ledger(tmp_path: Path) -> None:
    day = "2026-04-29"
    truth = tmp_path / "truth"
    sleeve = tmp_path / "sleeve"
    _write(
        truth / "reports" / "aegis_day_run_v1" / day / "day_run.v1.json",
        {"day_utc": day, "final_status": "PAPER_READY", "canonical_blocker": "", "operator_next_action": "No action."},
    )
    _write(
        truth / "reports" / "market_open_data_gate_v1" / day / "market_open_data_gate.v1.json",
        {"day_utc": day, "status": "BLOCKED", "canonical_blocker": "MARKET_DATA_BLOCKED"},
    )

    payload = build_readiness_kernel_v1(day, truth_root=truth, sleeve_truth_root=sleeve)

    assert payload["overall_status"] == "READY"
    assert payload["canonical_blocker"] == ""
    assert "MARKET_DATA" in payload["supporting_evidence_only_layers"]
