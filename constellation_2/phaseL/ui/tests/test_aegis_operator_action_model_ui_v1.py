from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]


def test_command_center_visual_requirements_document_exists() -> None:
    doc = (REPO_ROOT / "docs/aegis_command_center_visual_design_requirements_v1.md").read_text(encoding="utf-8")
    assert "Readability first" in doc
    assert "Strong contrast" in doc
    assert "PAPER MODE visually dominates" in doc
    assert "Safety policy remains secondary and collapsed" in doc


def test_command_center_renders_paper_mode_operating_center() -> None:
    source = (REPO_ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js").read_text(encoding="utf-8")
    command_block = source.split("async function renderCommandCenterWorkspace", 1)[1].split("function positionsCaptureSummary", 1)[0]
    primary_block = command_block.split("renderCommandCenterSafetyStrip(payload)", 1)[0]
    assert "renderCommandCenterStatusHeader(payload, truth)" in command_block
    assert "renderCommandCenterPrimaryOverview(payload)" in command_block
    assert "renderCommandCenterValidationPipeline(payload)" in command_block
    assert "PAPER MODE" in source
    assert "Research observations only" in source
    assert "Paper research active. Research observations only. No live trading. No broker execution." in source
    for label in ["Candidate Generation", "Paper Observations", "Outcome Validation", "Research Allocation", "Current Bottleneck", "David Action"]:
        assert label in source
    assert "Validation Pipeline" in source
    assert "Safety policy" in source
    assert "command-center-visual-surface" in source
    assert "command-center-primary-label" in source
    assert "command-center-primary-value" in source
    assert "renderCommandCenterCapabilityMatrix(payload)" not in command_block
    assert "renderTodayActivity(payload)" not in command_block
    for forbidden in ["Trade Recommendation", "Manual Capture", "Estimated Value", "Aegis Lite"]:
        assert forbidden not in primary_block


def test_operator_today_api_exposes_operator_action_model() -> None:
    source = (REPO_ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py").read_text(encoding="utf-8")
    assert '"operator_action_model_v1": operator_action_model' in source
    assert '"operator_headline"' in source
    assert '"operator_answer"' in source


def test_command_center_renders_candidate_generation_by_sleeve() -> None:
    source = (REPO_ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js").read_text(encoding="utf-8")
    assert "renderCommandCenterCandidateGenerationDiagnostics" in source
    assert "Candidate Generation By Sleeve" in source
    assert "No setup" in source
    assert "Rejected signals" in source
    assert "Blocked Sleeves" in source
    assert "Certified Price Candidates" in source
    assert "Review Eligible" in source
    assert "Promotion Eligible" in source
    assert "Paper Positions Created" in source
    assert "auto_promoted_to_paper_tracking_count" in source
    assert "Blocked From Paper" in source


def test_operator_today_api_exposes_candidate_generation_diagnostics() -> None:
    source = (REPO_ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py").read_text(encoding="utf-8")
    assert "_today_candidate_generation_diagnostics_v1" in source
    assert '"candidate_generation_diagnostics": candidate_generation_diagnostics' in source
    assert '"blocked_sleeves"' in source
    assert '"blocked_data_sleeves"' in source
    assert '"blocked_config_sleeves"' in source
    assert '"sleeves_with_no_setup"' in source
    assert '"candidate_lifecycle_counts"' in source
    assert '"review_eligible_count"' in source
    assert '"blocked_from_paper_count"' in source
    assert '"VALID_CONTRACT_CREATED"' in source
    assert '"BLOCKED_DATA"' in source
    assert '"BLOCKED_CONFIG"' in source
