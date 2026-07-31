from __future__ import annotations

from pathlib import Path

from constellation_2.common.atlas_v2_research_os.cli import main
from constellation_2.common.atlas_v2_research_os.manual_fundamental_claim_intake import build_manual_pegy_claim_intake, run_manual_pegy_claim_intake

NOW = "2026-06-05T00:00:00Z"


def test_manual_pegy_claim_intake_captures_claim_and_research_frame(tmp_path: Path) -> None:
    report = build_manual_pegy_claim_intake(root=tmp_path, created_at=NOW)
    claim = report["manual_claim"]
    assert claim["claim_name"] == "PEGY ratio"
    assert claim["claim_type"] == "FUNDAMENTAL_VALUATION"
    assert claim["mechanism_family"] == "VALUE_GROWTH_YIELD"
    assert "Do low-PEGY stocks outperform" in claim["research_framing"]
    assert claim["edge_label_allowed"] is False
    assert report["routing"]["route_to_paper_trade_candidates"] is False


def test_manual_pegy_claim_intake_generates_hypotheses_and_backlog_item(tmp_path: Path) -> None:
    report = build_manual_pegy_claim_intake(root=tmp_path, created_at=NOW)
    assert [row["hypothesis_id"] for row in report["initial_hypothesis_specs"]] == ["H1", "H2", "H3", "H4", "H5"]
    backlog = report["research_backlog_item"]
    assert backlog["item_type"] == "FUNDAMENTAL_CLAIM_INVESTIGATION"
    assert backlog["state"] == "READY_IF_DATA_AVAILABLE"
    assert backlog["metadata"]["source"] == "MANUAL_USER_CLAIM"
    assert backlog["metadata"]["claim"] == "PEGY ratio"


def test_manual_pegy_claim_intake_data_audit_classifies_missing_data_required(tmp_path: Path) -> None:
    report = build_manual_pegy_claim_intake(root=tmp_path, created_at=NOW)
    audit = report["data_availability_audit"]
    assert audit["overall_classification"] == "DATA_REQUIRED"
    assert {row["classification"] for row in audit["questions"]} == {"DATA_REQUIRED"}
    assert report["bias_risks"]["today_fundamentals_for_past_returns_allowed"] is False


def test_manual_pegy_claim_intake_writes_dated_and_latest_outputs(tmp_path: Path) -> None:
    report = run_manual_pegy_claim_intake(root=tmp_path, created_at=NOW)
    assert report["summary"]["claim_captured"] is True
    report_root = tmp_path / "manual_fundamental_claims" / "pegy_ratio"
    assert (report_root / "latest.json").exists()
    assert (report_root / "latest_summary.md").exists()
    assert (report_root / "2026-06-05" / "pegy_manual_claim.json").exists()
    assert (report_root / "2026-06-05" / "pegy_manual_claim_summary.md").exists()


def test_manual_pegy_claim_intake_cli_writes_report(tmp_path: Path) -> None:
    rc = main(["--root", str(tmp_path), "--manual-pegy-claim-intake"])
    assert rc == 0
    report_root = tmp_path / "manual_fundamental_claims" / "pegy_ratio"
    assert (report_root / "latest.json").exists()
