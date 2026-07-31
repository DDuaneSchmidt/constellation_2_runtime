from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = REPO_ROOT / "ops" / "tools" / "build_atlas_v1_north_star_metrics.py"
DAY = "2026-06-03"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.build_atlas_v1_north_star_metrics import build_north_star_metrics
from ops.tools.validate_aegis_research_journal_v0 import validate_journal


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _seed_sources(root: Path, journal_root: Path) -> None:
    reports = root / "reports"
    _write_json(
        reports / "aegis_validation_samples_v1" / DAY / "validation_samples.v1.json",
        {
            "trade_advice_allowed": False,
            "broker_execution_allowed": False,
            "live_trading_allowed": False,
            "samples": [
                {"sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "hypothesis_id": "HYP_TREND", "inclusion_status": "INCLUDED"},
                {"sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "hypothesis_id": "HYP_TREND", "inclusion_status": "INCLUDED"},
                {"sleeve_id": "C2_MEAN_REVERSION_EQ_V1", "hypothesis_id": "HYP_MEAN", "inclusion_status": "EXCLUDED", "sample_state": "EXCLUDED_OPEN_POSITION"},
            ],
        },
    )
    _write_json(
        reports / "aegis_sleeve_evidence_certification_v1" / DAY / "sleeve_evidence_certification.v1.json",
        {
            "sleeves": [
                {"sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "sample_status": "BUILDING_SAMPLE", "evidence_status": "UNDERPOWERED", "closed_position_count": 2, "active_position_count": 1},
                {"sleeve_id": "C2_MEAN_REVERSION_EQ_V1", "sample_status": "ZERO_SAMPLE", "evidence_status": "UNDERPOWERED", "closed_position_count": 0, "active_position_count": 1},
            ]
        },
    )
    _write_json(
        reports / "aegis_paper_position_ledger_v1" / DAY / "paper_position_ledger.v1.json",
        {
            "positions": [
                {"position_id": "trend-open", "sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "current_status": "OPEN"},
                {"position_id": "trend-closed-1", "sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "current_status": "CLOSED"},
                {"position_id": "trend-closed-2", "sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "current_status": "CLOSED"},
                {"position_id": "mean-open", "sleeve_id": "C2_MEAN_REVERSION_EQ_V1", "current_status": "OPEN"},
            ]
        },
    )
    _write_json(
        reports / "aegis_candidate_generation_diagnostics_v1" / DAY / "candidate_generation_diagnostics.v1.json",
        {
            "total_raw_signals": 10,
            "total_candidates_generated": 2,
            "valid_candidate_contracts": 1,
            "rejected_raw_signal_details": [{"id": "raw-reject-1"}, {"id": "raw-reject-2"}],
            "rejected_candidate_contracts": [{"id": "contract-reject-1"}],
        },
    )
    _write_json(
        reports / "aegis_candidate_contracts_v1" / DAY / "candidate_contracts.v1.json",
        {"pre_contract_suppressed_count": 7, "candidate_contracts": [{"id": "valid-1"}]},
    )
    _write_json(
        reports / "aegis_research_quality_engine_v1" / DAY / "research_quality_engine.v1.json",
        {
            "summary": {"hypothesis_count": 2, "ready_for_capital_review_count": 0},
            "hypotheses": [
                {"hypothesis_id": "HYP_TREND", "quality_status": "UNDERPOWERED"},
                {"hypothesis_id": "HYP_MEAN", "quality_status": "BLOCKED"},
            ],
        },
    )
    _write_json(
        reports / "aegis_statistical_sufficiency_v1" / DAY / "statistical_sufficiency.v1.json",
        {"summary": {"hypothesis_count": 2, "underpowered": 1, "validation_ready": 1, "validated": 0}},
    )
    _write_json(
        reports / "aegis_verified_runtime_graph_v1" / DAY / "verified_runtime_graph.v1.json",
        {"graph_status": "READY", "audit_blocker_count": 0, "trade_advice_allowed": False, "broker_execution_allowed": False, "live_trading_allowed": False},
    )
    for name in (
        "aegis_north_star_metrics_review_001.md",
        "atlas_north_star_dashboard_spec_001.md",
        "evidence_flow_bottleneck_review_001.md",
        "outcome_bottleneck_decomposition_review_001.md",
        "decision_risk_monitor_001.md",
    ):
        _write(journal_root / "reports" / name, f"# {name}\n\nRead-only fixture.\n")


def _snapshot(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): path.read_text(encoding="utf-8")
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def test_north_star_output_is_deterministic(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    journal_root = tmp_path / "journal"
    _seed_sources(truth_root, journal_root)

    first = build_north_star_metrics(truth_root, journal_root, DAY)
    second = build_north_star_metrics(truth_root, journal_root, DAY)

    assert first == second
    assert "# Atlas V1 North Star Metrics" in first
    assert "# North Star Summary" in first


def test_required_metric_sections_are_present(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    journal_root = tmp_path / "journal"
    _seed_sources(truth_root, journal_root)

    brief = build_north_star_metrics(truth_root, journal_root, DAY)

    for section in (
        "# Distributed Mature Validation Evidence",
        "# Evidence Maturity Status",
        "# Outcome Flow",
        "# Candidate-To-Paper Conversion Quality",
        "# Evidence Concentration",
        "# Guardrails",
        "# Anti-Metrics Boundary",
        "# Source Paths",
    ):
        assert section in brief


def test_metrics_are_source_bound_and_not_readiness_claims(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    journal_root = tmp_path / "journal"
    _seed_sources(truth_root, journal_root)

    brief = build_north_star_metrics(truth_root, journal_root, DAY)

    assert "Included validation samples: 2" in brief
    assert "Sample-producing sleeves: 1" in brief
    assert "Sample-producing hypotheses: 1" in brief
    assert "Paper-position to included-sample conversion: 2 / 4 (50.0%)." in brief
    assert "Raw signal to valid contract conversion: 1 / 10 (10.0%)." in brief
    assert "Accumulated paper positions versus current valid-contract diagnostic: 4 / 1 (400.0%)." in brief
    assert "paper positions are accumulated ledger inventory" in brief
    assert "Largest included-sample sleeve: `C2_TREND_EQ_PRIMARY_V1` with 2 / 2 (100.0%)." in brief
    assert "does not validate truth, infer readiness, recommend trades" in brief
    assert "Raw signals, generated candidates, total paper positions" in brief


def test_source_paths_are_included(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    journal_root = tmp_path / "journal"
    _seed_sources(truth_root, journal_root)

    brief = build_north_star_metrics(truth_root, journal_root, DAY)

    assert "validation_samples.v1.json" in brief
    assert "sleeve_evidence_certification.v1.json" in brief
    assert "paper_position_ledger.v1.json" in brief
    assert "candidate_generation_diagnostics.v1.json" in brief
    assert "candidate_contracts.v1.json" in brief
    assert "research_quality_engine.v1.json" in brief
    assert "statistical_sufficiency.v1.json" in brief
    assert "verified_runtime_graph.v1.json" in brief
    assert "atlas_north_star_dashboard_spec_001.md" in brief


def test_no_match_behavior_is_deterministic(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    journal_root = tmp_path / "journal"
    (truth_root / "reports").mkdir(parents=True)
    (journal_root / "reports").mkdir(parents=True)

    first = build_north_star_metrics(truth_root, journal_root, DAY)
    second = build_north_star_metrics(truth_root, journal_root, DAY)

    assert first == second
    assert "Included validation samples: 0" in first
    assert "Missing source paths:" in first


def test_script_is_read_only(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    journal_root = tmp_path / "journal"
    _seed_sources(truth_root, journal_root)
    before = _snapshot(tmp_path)

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
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    after = _snapshot(tmp_path)
    assert before == after
    assert "NON_AUTHORITATIVE_READ_ONLY_NORTH_STAR_METRICS" in result.stdout
    assert "# Evidence Concentration" in result.stdout
    assert result.stderr == ""


def test_research_journal_validation_still_passes() -> None:
    assert validate_journal(REPO_ROOT / "research_journal") == []
