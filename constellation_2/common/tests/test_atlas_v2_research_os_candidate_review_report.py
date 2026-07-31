from __future__ import annotations

import json
from pathlib import Path

import pytest

from constellation_2.common.atlas_v2_research_os.candidate_review_governance import (
    CandidateReviewGovernanceError,
    validate_candidate_review_allowed,
)
from constellation_2.common.atlas_v2_research_os.candidate_review_report import (
    audit_candidate_review_reports,
    build_candidate_review_report,
    write_candidate_review_report,
)
from constellation_2.common.atlas_v2_research_os.methodology_trial import build_methodology_trial


def test_candidate_review_report_reviews_12_replay_supported_candidates() -> None:
    trial = build_methodology_trial(count=100, day="2026-06-05")
    report = build_candidate_review_report(trial, source_report_path="methodology_trial/latest.json", day="2026-06-05")

    assert report["candidates_reviewed"] == 12
    assert len(report["top_candidates"]) == 12
    assert report["top_candidates"][0]["edge_score"] >= report["top_candidates"][-1]["edge_score"]
    assert {row["replay_status"] for row in report["top_candidates"]} == {"REPLAY_POSITIVE"}
    assert report["authority_boundary"]["allowed_scope"] == "review for paper-forward observation"
    assert report["authority_boundary"]["trade_recommendation_authorized"] is False
    assert report["authority_boundary"]["capital_allocation_authorized"] is False
    assert report["authority_boundary"]["position_sizing_authorized"] is False
    assert report["authority_boundary"]["live_trading_authorized"] is False
    assert report["authority_boundary"]["broker_execution_authorized"] is False


def test_candidate_review_governance_rejects_authority_expansion() -> None:
    trial = build_methodology_trial(count=100, day="2026-06-05")
    report = build_candidate_review_report(trial, source_report_path="methodology_trial/latest.json", day="2026-06-05")
    report["authority_boundary"]["capital_allocation_authorized"] = True

    with pytest.raises(CandidateReviewGovernanceError):
        validate_candidate_review_allowed(report)


def test_candidate_review_governance_rejects_action_text() -> None:
    trial = build_methodology_trial(count=100, day="2026-06-05")
    report = build_candidate_review_report(trial, source_report_path="methodology_trial/latest.json", day="2026-06-05")
    report["top_candidates"][0]["human_action_required"] = "Submit order now."

    with pytest.raises(CandidateReviewGovernanceError):
        validate_candidate_review_allowed(report)


def test_write_candidate_review_report_outputs_latest_and_day_files(tmp_path: Path) -> None:
    trial = build_methodology_trial(count=100, day="2026-06-05")
    paths = write_candidate_review_report(
        trial,
        root=tmp_path / "candidate_review",
        source_report_path="methodology_trial/latest.json",
        day="2026-06-05",
    )

    payload = json.loads(paths["json"].read_text(encoding="utf-8"))
    latest = json.loads(paths["latest_json"].read_text(encoding="utf-8"))
    assert payload["candidates_reviewed"] == 12
    assert latest["top_candidates"][0]["candidate_id"] == payload["top_candidates"][0]["candidate_id"]
    assert paths["summary"].exists()
    assert paths["latest_summary"].exists()
    assert audit_candidate_review_reports(tmp_path / "candidate_review")["candidate_review_audit_ok"] is True
