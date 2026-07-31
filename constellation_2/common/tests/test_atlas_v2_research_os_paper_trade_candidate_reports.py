from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import json

from constellation_2.common.atlas_v2_research_os.edge_qualification_models import EdgeQualificationInput
from constellation_2.common.atlas_v2_research_os.paper_trade_candidate_reports import (
    audit_paper_trade_candidate_reports,
    build_paper_trade_candidate_report,
    certify_paper_trade_candidate,
    create_paper_trade_candidate,
    write_paper_trade_candidate_report,
)


def strong_input() -> EdgeQualificationInput:
    return EdgeQualificationInput(
        source_artifact_ids=["artifact-1"],
        source_hypothesis_ids=["hyp-1"],
        source_experiment_ids=["exp-1"],
        source_memory_ids=["mem-1"],
        evidence_maturity=0.90,
        research_effectiveness=0.85,
        hypothesis_survival=0.80,
        failure_history=0.10,
        duplicate_risk=0.10,
        regime_coverage=0.80,
        candidate_quality_trend=0.80,
        learning_validation_trend=0.80,
        lineage_complete=True,
        governance_pass=True,
        mechanism_tags=["seasonality"],
        regime_context={"regime": "risk_on"},
        evidence_level="PAPER_FORWARD_OBSERVATION",
        lifecycle_state="SUPPORTED",
    )


def test_create_paper_trade_candidate_from_qualified_edge() -> None:
    candidate = create_paper_trade_candidate(strong_input(), candidate_id="ptc-1", created_at="2026-06-05T00:00:00Z")
    assert candidate["candidate_id"] == "ptc-1"
    assert candidate["paper_trade_eligible"] is True
    assert candidate["human_review_required"] is True
    assert candidate["edge_score"] >= 0.70


def test_certify_candidate_for_human_review_only() -> None:
    candidate = create_paper_trade_candidate(strong_input(), candidate_id="ptc-1")
    cert = certify_paper_trade_candidate(candidate, certification_id="cert-1")
    assert cert["status"] == "CERTIFIED_FOR_HUMAN_REVIEW"
    assert cert["paper_trade_eligible"] is True
    assert cert["live_trading_authorized"] is False
    assert cert["capital_authorized"] is False


def test_certification_fails_closed_for_authority_escalation() -> None:
    candidate = create_paper_trade_candidate(strong_input(), candidate_id="ptc-1")
    candidate["capital_authorized"] = True
    cert = certify_paper_trade_candidate(candidate)
    assert cert["status"] == "GOVERNANCE_FAIL"
    assert cert["paper_trade_eligible"] is False


def test_build_report_contains_score_explanation_and_boundaries() -> None:
    report = build_paper_trade_candidate_report(strong_input(), candidate_id="ptc-1", created_at="2026-06-05T00:00:00Z")
    assert report["candidate"]["paper_trade_eligible"] is True
    assert report["certification"]["status"] == "CERTIFIED_FOR_HUMAN_REVIEW"
    assert any("contribution" in line for line in report["score_explanation"])
    assert "capital allocation" in report["limitations"][0]


def test_write_report_outputs_dated_and_latest_files(tmp_path: Path) -> None:
    paths = write_paper_trade_candidate_report(strong_input(), root=tmp_path / "paper_trade_candidates", day="2026-06-05", candidate_id="ptc-1")
    payload = json.loads(paths["json"].read_text(encoding="utf-8"))
    assert payload["candidate"]["candidate_id"] == "ptc-1"
    assert paths["summary"].exists()
    assert paths["latest_json"].exists()
    assert paths["latest_summary"].exists()
    assert "Atlas PaperTradeCandidate Report" in paths["summary"].read_text(encoding="utf-8")
    assert audit_paper_trade_candidate_reports(tmp_path / "paper_trade_candidates")["paper_trade_candidate_audit_ok"] is True
