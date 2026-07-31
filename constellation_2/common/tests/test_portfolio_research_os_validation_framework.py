from __future__ import annotations

from constellation_2.common.portfolio_research_os.validation_framework import (
    AUTHORITY_BOUNDARY,
    build_validation_framework,
    run_validation_framework,
)


NOW = "2026-06-06T00:00:00Z"


def test_p008_oak_harvest_proxy_is_labeled_user_assumption(tmp_path) -> None:
    report = build_validation_framework(root=tmp_path, created_at=NOW)
    oak = next(row for row in report["benchmark_definitions"] if row["benchmark_id"] == "OAK_HARVEST_PROXY")

    assert oak["gross_return_assumption"] == "6.00%"
    assert oak["fee_assumption"] == "1.00%"
    assert oak["net_return_assumption"] == "5.00%"
    assert oak["assumption_status"] == "USER_ASSUMPTION"


def test_p009_walk_forward_windows_are_deterministic(tmp_path) -> None:
    first = build_validation_framework(root=tmp_path, created_at=NOW)
    second = build_validation_framework(root=tmp_path, created_at=NOW)

    assert first["walk_forward_results"] == second["walk_forward_results"]
    assert first["walk_forward_results"][0]["window_id"] == "WF001"
    assert first["walk_forward_results"][0]["embargo_gap_days"] == 30


def test_p010_bias_audit_reports_missing_point_in_time_data(tmp_path) -> None:
    report = build_validation_framework(root=tmp_path, created_at=NOW)
    pit = next(row for row in report["bias_integrity_audit"] if row["audit_item"] == "point-in-time fundamental availability")

    assert pit["classification"] == "DATA_INSUFFICIENT"
    assert pit["evidence_status"] == "MISSING"


def test_p011_decision_does_not_claim_success_without_data(tmp_path) -> None:
    report = build_validation_framework(root=tmp_path, created_at=NOW)

    assert report["validation_decision"][0]["decision"] == "DATA_INSUFFICIENT"
    assert report["summary"]["validation_decision"] == "DATA_INSUFFICIENT"


def test_p012_no_trading_or_recommendation_authority_emitted(tmp_path) -> None:
    report = run_validation_framework(root=tmp_path, created_at=NOW)

    assert report["authority_boundary"] == AUTHORITY_BOUNDARY
    assert report["summary"]["no_trading_authority"] is True
    assert report["summary"]["no_replacement_recommendation_authority"] is True
    assert "No live portfolio" in report["latest_summary"] if "latest_summary" in report else True
    assert (tmp_path / "validation_framework" / "latest.json").exists()
