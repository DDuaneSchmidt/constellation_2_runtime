from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.truth_lifecycle_orchestrator_v1 import LifecycleRunContextV1
from ops.tools import run_gate_authority_plane_v1 as plane


DAY = "2026-04-21"
PRODUCED_UTC = "2026-04-21T00:00:00Z"


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True) + "\n", encoding="utf-8")


def _context(truth_root: Path) -> LifecycleRunContextV1:
    return LifecycleRunContextV1(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc=DAY,
        produced_utc=PRODUCED_UTC,
        mode="PAPER",
    )


def test_phase_day_admission_blocks_when_not_admit(tmp_path: Path) -> None:
    truth_root = (tmp_path / "truth").resolve()
    _write_json(
        truth_root / "target_day_admission_v1" / f"{DAY}.json",
        {"admission_status": "BLOCKED"},
    )
    result = plane._phase_day_admission(_context(truth_root), {"phase_id": "DAY_ADMISSION"})
    assert result.status == "BLOCKED"
    assert result.first_blocker_code == "TARGET_DAY_ADMISSION_NOT_ADMIT"


def test_phase_context_authority_blocks_when_build_not_closed(tmp_path: Path) -> None:
    truth_root = (tmp_path / "truth").resolve()
    _write_json(
        truth_root / "target_day_build_v1" / f"{DAY}.json",
        {"build_status": "COMPLETE", "closure_status": "OPEN"},
    )
    result = plane._phase_context_authority(_context(truth_root), {"phase_id": "CONTEXT_AUTHORITY"})
    assert result.status == "BLOCKED"
    assert result.first_blocker_code == "TARGET_DAY_BUILD_NOT_CLOSED"


def test_phase_prior_day_close_materializes_truth_continuity(tmp_path: Path, monkeypatch) -> None:
    truth_root = (tmp_path / "truth").resolve()
    prev_day = "2026-04-20"
    recon_path = (
        truth_root / "reports" / "reconciliation_report_v3" / prev_day / "reconciliation_report.v3.json"
    ).resolve()
    exit_path = (
        truth_root / "exit_reconciliation_v1" / prev_day / "exit_reconciliation.v1.json"
    ).resolve()

    def _fake_recon(*, day_utc, truth_root):  # noqa: ANN001
        assert day_utc == prev_day
        _write_json(recon_path, {"schema_id": "reconciliation_report_v3", "day_utc": day_utc})

    def _fake_exit(*, day_utc, truth_root):  # noqa: ANN001
        assert day_utc == prev_day
        _write_json(exit_path, {"schema_id": "exit_reconciliation_v1", "day_utc": day_utc})

    monkeypatch.setattr(plane, "_refresh_reconciliation_report", _fake_recon)
    monkeypatch.setattr(plane, "_refresh_exit_reconciliation", _fake_exit)

    result = plane._phase_prior_day_close(_context(truth_root), {"phase_id": "PRIOR_DAY_CLOSE"})
    assert result.status == "PASS"
    assert str(recon_path) in result.produced_artifacts
    assert str(exit_path) in result.produced_artifacts


def test_phase_post_execution_reconciliation_skips_without_submissions(tmp_path: Path) -> None:
    truth_root = (tmp_path / "truth").resolve()
    result = plane._phase_post_execution_reconciliation(
        _context(truth_root),
        {"phase_id": "POST_EXECUTION_RECONCILIATION"},
    )
    assert result.status == "SKIPPED"
    assert "POST_EXECUTION_RECONCILIATION_NO_SUBMISSION_DIR" in result.reason_codes


def test_phase_post_execution_reconciliation_refreshes_fill_and_recon(tmp_path: Path, monkeypatch) -> None:
    truth_root = (tmp_path / "truth").resolve()
    submission_dir = (
        truth_root / "execution_evidence_v1" / "submissions" / DAY / ("a" * 64)
    ).resolve()
    submission_dir.mkdir(parents=True, exist_ok=True)
    fill_ledger_path = (
        truth_root / "fill_ledger_v1" / DAY / (("a" * 64) + ".fill_ledger.v1.json")
    ).resolve()
    recon_path = (
        truth_root
        / "reports"
        / "execution_reconciliation_v1"
        / DAY
        / "execution_reconciliation.v1.json"
    ).resolve()

    def _fake_run(cmd, cwd, capture_output, text, check):  # noqa: ANN001
        if str(cmd[1]).endswith("run_fill_ledger_day_v1.py"):
            _write_json(fill_ledger_path, {"schema_id": "fill_ledger", "day_utc": DAY})
            return subprocess.CompletedProcess(cmd, 0, stdout="OK: FILL_LEDGER_WRITTEN", stderr="")
        if str(cmd[1]).endswith("run_execution_reconciliation_day_v1.py"):
            _write_json(recon_path, {"schema_id": "execution_reconciliation_v1", "day_utc": DAY})
            return subprocess.CompletedProcess(cmd, 0, stdout="OK", stderr="")
        return subprocess.CompletedProcess(cmd, 0, stdout="OK", stderr="")

    monkeypatch.setattr(plane.subprocess, "run", _fake_run)
    result = plane._phase_post_execution_reconciliation(
        _context(truth_root),
        {"phase_id": "POST_EXECUTION_RECONCILIATION"},
    )
    assert result.status == "PASS"
    assert str(fill_ledger_path.parent) in result.produced_artifacts
    assert str(recon_path) in result.produced_artifacts


def test_phase_day_close_runs_ledger_refresh(tmp_path: Path, monkeypatch) -> None:
    truth_root = (tmp_path / "truth").resolve()
    ledger_path = (
        truth_root
        / "reports"
        / "paper_session_ledger_v1"
        / DAY
        / "paper_session_ledger.v1.json"
    ).resolve()

    def _fake_run(cmd, cwd, capture_output, text, check):  # noqa: ANN001
        _write_json(ledger_path, {"schema_id": "paper_session_ledger", "day_utc": DAY})
        return subprocess.CompletedProcess(cmd, 0, stdout="OK", stderr="")

    monkeypatch.setattr(plane.subprocess, "run", _fake_run)
    result = plane._phase_day_close(_context(truth_root), {"phase_id": "DAY_CLOSE"})
    assert result.status == "PASS"
    assert str(ledger_path) in result.produced_artifacts
