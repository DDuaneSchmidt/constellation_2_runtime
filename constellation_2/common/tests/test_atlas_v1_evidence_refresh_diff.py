from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = REPO_ROOT / "ops" / "tools" / "build_atlas_v1_evidence_refresh_diff.py"
FROM_DAY = "2026-06-03"
TO_DAY = "2026-06-04"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.build_atlas_v1_evidence_refresh_diff import build_evidence_refresh_diff
from ops.tools.validate_aegis_research_journal_v0 import validate_journal


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _seed_graph(truth_root: Path, day: str, status: str = "READY") -> None:
    payload = {"graph_status": status}
    if status != "READY":
        payload["capabilities"] = [
            {
                "module_id": "runtime_truth_kernel",
                "capability_id": "fixture_capability",
                "blockers": ["REQUIRED_EVIDENCE_MISSING:fixture"],
            }
        ]
        payload["audit_blocker_count"] = 1
    _write_json(
        truth_root / "reports" / "aegis_verified_runtime_graph_v1" / day / "verified_runtime_graph.v1.json",
        payload,
    )


def _seed_day(truth_root: Path, day: str, *, include_mean_sample: bool, graph_status: str = "READY") -> None:
    _seed_graph(truth_root, day, graph_status)
    reports = truth_root / "reports"
    _write_json(
        reports / "aegis_paper_position_ledger_v1" / day / "paper_position_ledger.v1.json",
        {
            "positions": [
                {"position_id": "trend-open", "sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "current_status": "OPEN"},
                {"position_id": "trend-closed", "sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "current_status": "CLOSED"},
                {"position_id": "mean-open", "sleeve_id": "C2_MEAN_REVERSION_EQ_V1", "current_status": "OPEN"},
            ]
        },
    )
    _write_json(
        reports / "aegis_sleeve_performance_truth_v1" / day / "sleeve_performance_truth.v1.json",
        {
            "sleeves": [
                {"sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "open_paper_position_count": 1, "closed_paper_position_count": 1},
                {"sleeve_id": "C2_MEAN_REVERSION_EQ_V1", "open_paper_position_count": 1, "closed_paper_position_count": 0},
            ]
        },
    )
    samples = [
        {"sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "inclusion_status": "INCLUDED"},
        {"sleeve_id": "C2_MEAN_REVERSION_EQ_V1", "inclusion_status": "EXCLUDED"},
    ]
    if include_mean_sample:
        samples.append({"sleeve_id": "C2_MEAN_REVERSION_EQ_V1", "inclusion_status": "INCLUDED"})
    _write_json(reports / "aegis_validation_samples_v1" / day / "validation_samples.v1.json", {"samples": samples})
    _write_json(
        reports / "aegis_sleeve_evidence_certification_v1" / day / "sleeve_evidence_certification.v1.json",
        {
            "sleeves": [
                {
                    "sleeve_id": "C2_TREND_EQ_PRIMARY_V1",
                    "active_position_count": 1,
                    "closed_position_count": 1,
                    "sample_status": "BUILDING_SAMPLE",
                    "evidence_status": "UNDERPOWERED",
                },
                {
                    "sleeve_id": "C2_MEAN_REVERSION_EQ_V1",
                    "active_position_count": 1,
                    "closed_position_count": 0,
                    "sample_status": "ZERO_SAMPLE",
                    "evidence_status": "UNDERPOWERED",
                },
            ]
        },
    )


def _seed_sources(root: Path, *, blocked_to_day: bool = False) -> tuple[Path, Path]:
    truth_root = root / "truth"
    journal_root = root / "journal"
    (journal_root / "reports").mkdir(parents=True)
    _seed_day(truth_root, FROM_DAY, include_mean_sample=False)
    _seed_day(truth_root, TO_DAY, include_mean_sample=True, graph_status="BLOCKED" if blocked_to_day else "READY")
    return truth_root, journal_root


def _snapshot(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): path.read_text(encoding="utf-8")
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def test_diff_output_is_deterministic(tmp_path: Path) -> None:
    truth_root, journal_root = _seed_sources(tmp_path)

    first = build_evidence_refresh_diff(truth_root, journal_root, FROM_DAY, TO_DAY)
    second = build_evidence_refresh_diff(truth_root, journal_root, FROM_DAY, TO_DAY)

    assert first == second
    assert "# Evidence Refresh Diff Summary" in first


def test_required_sections_are_present(tmp_path: Path) -> None:
    truth_root, journal_root = _seed_sources(tmp_path)

    brief = build_evidence_refresh_diff(truth_root, journal_root, FROM_DAY, TO_DAY)

    for section in (
        "# Evidence Refresh Diff Summary",
        "# Day Validity",
        "# Sample Count Changes",
        "# Evidence Concentration Changes",
        "# First-Sample Changes",
        "# Stalled Path Changes",
        "# Not Assessable Items",
        "# Source Paths",
    ):
        assert section in brief


def test_handles_blocked_to_day_deterministically(tmp_path: Path) -> None:
    truth_root, journal_root = _seed_sources(tmp_path, blocked_to_day=True)

    first = build_evidence_refresh_diff(truth_root, journal_root, FROM_DAY, TO_DAY)
    second = build_evidence_refresh_diff(truth_root, journal_root, FROM_DAY, TO_DAY)

    assert first == second
    assert "Diff status: NOT_ASSESSABLE." in first
    assert "`2026-06-04`: NOT_ASSESSABLE_BLOCKED" in first
    assert "REQUIRED_EVIDENCE_MISSING:fixture" in first


def test_source_paths_are_included(tmp_path: Path) -> None:
    truth_root, journal_root = _seed_sources(tmp_path)

    brief = build_evidence_refresh_diff(truth_root, journal_root, FROM_DAY, TO_DAY)

    assert "# Source Paths" in brief
    assert "verified_runtime_graph.v1.json" in brief
    assert "paper_position_ledger.v1.json" in brief
    assert "validation_samples.v1.json" in brief


def test_script_is_read_only(tmp_path: Path) -> None:
    truth_root, journal_root = _seed_sources(tmp_path)
    before = _snapshot(tmp_path)

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--truth-root",
            str(truth_root),
            "--journal-root",
            str(journal_root),
            "--from-day",
            FROM_DAY,
            "--to-day",
            TO_DAY,
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    after = _snapshot(tmp_path)
    assert before == after
    assert "NON_AUTHORITATIVE_READ_ONLY_EVIDENCE_REFRESH_DIFF" in result.stdout
    assert result.stderr == ""


def test_same_day_comparison_produces_zero_delta(tmp_path: Path) -> None:
    truth_root, journal_root = _seed_sources(tmp_path)

    brief = build_evidence_refresh_diff(truth_root, journal_root, FROM_DAY, FROM_DAY)

    assert "Diff status: ASSESSABLE." in brief
    assert "Open positions: 2 -> 2 (0)." in brief
    assert "Closed positions: 1 -> 1 (0)." in brief
    assert "Included validation samples: 1 -> 1 (0)." in brief


def test_valid_days_report_first_sample_change(tmp_path: Path) -> None:
    truth_root, journal_root = _seed_sources(tmp_path)

    brief = build_evidence_refresh_diff(truth_root, journal_root, FROM_DAY, TO_DAY)

    assert "`C2_MEAN_REVERSION_EQ_V1` moved from zero included samples" in brief
    assert "Sample-producing sleeves: 1 -> 2 (+1)." in brief


def test_research_journal_validation_still_passes() -> None:
    assert validate_journal(REPO_ROOT / "research_journal") == []
