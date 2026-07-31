from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = REPO_ROOT / "ops" / "tools" / "build_atlas_v1_outcome_follow_through_comparator.py"
DAY = "2026-06-03"
BASELINE_DAY = "2026-06-02"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.build_atlas_v1_outcome_follow_through_comparator import build_comparator_brief
from ops.tools.validate_aegis_research_journal_v0 import validate_journal


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _seed_day(root: Path, day: str) -> None:
    reports = root / "reports"
    positions = [
        {"position_id": "trend-open-1", "sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "current_status": "OPEN"},
        {"position_id": "trend-closed-1", "sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "current_status": "CLOSED"},
        {"position_id": "trend-closed-2", "sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "current_status": "CLOSED"},
        {"position_id": "cross-open-1", "sleeve_id": "C2_CROSS_ASSET_TREND_V1", "current_status": "OPEN"},
        {"position_id": "cross-open-2", "sleeve_id": "C2_CROSS_ASSET_TREND_V1", "current_status": "OPEN"},
        {"position_id": "cross-open-3", "sleeve_id": "C2_CROSS_ASSET_TREND_V1", "current_status": "OPEN"},
        {"position_id": "cross-open-4", "sleeve_id": "C2_CROSS_ASSET_TREND_V1", "current_status": "OPEN"},
        {"position_id": "cross-open-5", "sleeve_id": "C2_CROSS_ASSET_TREND_V1", "current_status": "OPEN"},
        {"position_id": "mean-open-1", "sleeve_id": "C2_MEAN_REVERSION_EQ_V1", "current_status": "OPEN"},
        {"position_id": "mean-open-2", "sleeve_id": "C2_MEAN_REVERSION_EQ_V1", "current_status": "OPEN"},
        {"position_id": "vol-open-1", "sleeve_id": "C2_VOL_INCOME_DEFINED_RISK_V1", "current_status": "OPEN"},
        {"position_id": "vol-open-2", "sleeve_id": "C2_VOL_INCOME_DEFINED_RISK_V1", "current_status": "OPEN"},
        {"position_id": "oil-open-1", "sleeve_id": "C2_OIL_SHOCK_REVERSAL_V1", "current_status": "OPEN"},
    ]
    _write_json(reports / "aegis_paper_position_ledger_v1" / day / "paper_position_ledger.v1.json", {"positions": positions})
    _write_json(
        reports / "aegis_sleeve_performance_truth_v1" / day / "sleeve_performance_truth.v1.json",
        {
            "sleeves": [
                {"sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "open_paper_position_count": 1, "closed_paper_position_count": 2, "data_quality_status": "PASS"},
                {"sleeve_id": "C2_CROSS_ASSET_TREND_V1", "open_paper_position_count": 5, "closed_paper_position_count": 0, "data_quality_status": "PASS"},
                {"sleeve_id": "C2_MEAN_REVERSION_EQ_V1", "open_paper_position_count": 2, "closed_paper_position_count": 0, "data_quality_status": "PASS"},
                {"sleeve_id": "C2_VOL_INCOME_DEFINED_RISK_V1", "open_paper_position_count": 2, "closed_paper_position_count": 0, "data_quality_status": "PASS"},
                {"sleeve_id": "C2_OIL_SHOCK_REVERSAL_V1", "open_paper_position_count": 1, "closed_paper_position_count": 0, "data_quality_status": "PASS"},
            ]
        },
    )
    _write_json(
        reports / "aegis_exit_recommendations_v1" / day / "exit_recommendations.v1.json",
        {"recommendations": [{"position_id": row["position_id"], "recommendation": "HOLD"} for row in positions if row["current_status"] == "OPEN"]},
    )
    _write_json(
        reports / "aegis_outcome_registry_v1" / day / "outcome_registry.v1.json",
        {"outcomes": [{"outcome_id": f"outcome-{row['position_id']}", "sleeve_id": row["sleeve_id"]} for row in positions]},
    )
    _write_json(reports / "aegis_outcome_flow_audit_v1" / day / "outcome_flow_audit.v1.json", {"day": day})
    _write_json(
        reports / "aegis_generated_hypothesis_outcome_maturity_monitor_v1" / day / "generated_hypothesis_outcome_maturity_monitor.v1.json",
        {"day": day},
    )
    samples = [
        {"sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "inclusion_status": "INCLUDED"},
        {"sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "inclusion_status": "INCLUDED"},
    ]
    for row in positions:
        if row["current_status"] == "OPEN" and row["sleeve_id"] != "C2_TREND_EQ_PRIMARY_V1":
            samples.append({"sleeve_id": row["sleeve_id"], "inclusion_status": "EXCLUDED", "sample_state": "EXCLUDED_OPEN_POSITION"})
    _write_json(reports / "aegis_validation_samples_v1" / day / "validation_samples.v1.json", {"samples": samples})
    _write_json(
        reports / "aegis_sleeve_evidence_certification_v1" / day / "sleeve_evidence_certification.v1.json",
        {
            "sleeves": [
                {"sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "active_position_count": 1, "closed_position_count": 2, "evidence_status": "UNDERPOWERED", "sample_status": "BUILDING_SAMPLE"},
                {"sleeve_id": "C2_CROSS_ASSET_TREND_V1", "active_position_count": 5, "closed_position_count": 0, "evidence_status": "UNDERPOWERED", "sample_status": "ZERO_SAMPLE"},
                {"sleeve_id": "C2_MEAN_REVERSION_EQ_V1", "active_position_count": 2, "closed_position_count": 0, "evidence_status": "UNDERPOWERED", "sample_status": "ZERO_SAMPLE"},
                {"sleeve_id": "C2_VOL_INCOME_DEFINED_RISK_V1", "active_position_count": 2, "closed_position_count": 0, "evidence_status": "UNDERPOWERED", "sample_status": "ZERO_SAMPLE"},
                {"sleeve_id": "C2_OIL_SHOCK_REVERSAL_V1", "active_position_count": 1, "closed_position_count": 0, "evidence_status": "UNDERPOWERED", "sample_status": "ZERO_SAMPLE"},
            ]
        },
    )


def _seed_sources(root: Path, journal_root: Path) -> None:
    _seed_day(root, BASELINE_DAY)
    _seed_day(root, DAY)
    for name in (
        "first_sample_bottleneck_review_001.md",
        "outcome_evidence_concentration_watch_001.md",
        "outcome_maturity_acceleration_review_001.md",
        "outcome_bottleneck_decomposition_review_001.md",
        "evidence_accumulation_forecast_review_001.md",
        "open_paper_position_outcome_follow_through_review_001.md",
    ):
        _write(journal_root / "reports" / name, f"# {name}\n\nRead-only fixture report.\n")


def _snapshot(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): path.read_text(encoding="utf-8")
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def test_comparator_output_is_deterministic(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    journal_root = tmp_path / "journal"
    _seed_sources(truth_root, journal_root)

    first = build_comparator_brief(truth_root, journal_root, DAY, BASELINE_DAY)
    second = build_comparator_brief(truth_root, journal_root, DAY, BASELINE_DAY)

    assert first == second
    assert "# Evidence Flow Summary" in first
    assert "# Distributed Validation Samples" in first
    assert "# Stalled Paths" in first
    assert "# First-Sample Opportunities" in first
    assert "# First-Sample Priority View" in first


def test_output_includes_source_citations(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    journal_root = tmp_path / "journal"
    _seed_sources(truth_root, journal_root)

    brief = build_comparator_brief(truth_root, journal_root, DAY, BASELINE_DAY)

    assert "# Source Paths" in brief
    assert "paper_position_ledger.v1.json" in brief
    assert "sleeve_performance_truth.v1.json" in brief
    assert "exit_recommendations.v1.json" in brief
    assert "outcome_registry.v1.json" in brief
    assert "outcome_flow_audit.v1.json" in brief
    assert "generated_hypothesis_outcome_maturity_monitor.v1.json" in brief
    assert "validation_samples.v1.json" in brief
    assert "sleeve_evidence_certification.v1.json" in brief
    assert "first_sample_bottleneck_review_001.md" in brief
    assert "outcome_evidence_concentration_watch_001.md" in brief


def test_concentration_section_compares_current_to_baseline(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    journal_root = tmp_path / "journal"
    _seed_sources(truth_root, journal_root)

    brief = build_comparator_brief(truth_root, journal_root, DAY, BASELINE_DAY)

    assert "# Evidence Concentration" in brief
    assert "Highest included-sample sleeve: `C2_TREND_EQ_PRIMARY_V1`" in brief
    assert "Sample-producing sleeve count: 1" in brief
    assert "Current-vs-baseline distribution: UNCHANGED_CONCENTRATION" in brief
    assert "not a readiness, quality, trade, exit, or allocation claim" in brief


def test_first_sample_priority_view_reports_expected_focus_sleeves(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    journal_root = tmp_path / "journal"
    _seed_sources(truth_root, journal_root)

    brief = build_comparator_brief(truth_root, journal_root, DAY, BASELINE_DAY)

    assert "# First-Sample Priority View" in brief
    assert "Priority type: evidence-flow priority only" in brief
    assert "not trade priority, exit priority, allocation priority, return-quality evidence, or readiness evidence" in brief
    assert "C2_MEAN_REVERSION_EQ_V1`: closest first-sample sleeve" in brief
    assert "C2_VOL_INCOME_DEFINED_RISK_V1`: closest first-sample sleeve" in brief
    assert "C2_CROSS_ASSET_TREND_V1`: highest distribution impact" in brief
    assert "C2_OIL_SHOCK_REVERSAL_V1`: furthest reviewed focus sleeve" in brief
    assert "Read-only evidence follow-through" in brief
    assert "Priority source paths:" in brief


def test_stalled_and_first_sample_sections_present(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    journal_root = tmp_path / "journal"
    _seed_sources(truth_root, journal_root)

    brief = build_comparator_brief(truth_root, journal_root, DAY, BASELINE_DAY)

    assert "Sleeves accumulating paper observations but no included outcomes/samples:" in brief
    assert "`C2_CROSS_ASSET_TREND_V1`: 5 paper observation(s), 5 open, 0 closed, 0 included validation samples." in brief
    assert "`C2_OIL_SHOCK_REVERSAL_V1`: open paper observations exist but no included sample" in brief


def test_no_match_behavior_is_deterministic(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    journal_root = tmp_path / "journal"
    (truth_root / "reports").mkdir(parents=True)
    (journal_root / "reports").mkdir(parents=True)

    first = build_comparator_brief(truth_root, journal_root, DAY, BASELINE_DAY)
    second = build_comparator_brief(truth_root, journal_root, DAY, BASELINE_DAY)

    assert first == second
    assert "No sleeve outcome-flow rows were found" in first
    assert "# First-Sample Priority View" in first
    assert "No first-sample priority view is available" in first
    assert "# Source Paths" in first
    assert "Missing source paths:" in first


def test_script_is_read_only_and_supports_baseline(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    journal_root = tmp_path / "journal"
    _seed_sources(truth_root, journal_root)
    before = {"truth": _snapshot(truth_root), "journal": _snapshot(journal_root)}

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--truth-root",
            str(truth_root),
            "--journal-root",
            str(journal_root),
            "--day",
            DAY,
            "--baseline-day",
            BASELINE_DAY,
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    after = {"truth": _snapshot(truth_root), "journal": _snapshot(journal_root)}
    assert before == after
    assert "NON_AUTHORITATIVE_READ_ONLY_COMPARATOR" in result.stdout
    assert "Current-vs-baseline distribution: UNCHANGED_CONCENTRATION" in result.stdout
    assert "# First-Sample Priority View" in result.stdout
    assert "recommend trades" in result.stdout
    assert "allocate capital" in result.stdout
    assert result.stderr == ""


def test_research_journal_validation_still_passes() -> None:
    assert validate_journal(REPO_ROOT / "research_journal") == []
