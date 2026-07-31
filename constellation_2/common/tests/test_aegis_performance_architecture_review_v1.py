from __future__ import annotations

import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.build_aegis_performance_architecture_review_v1 import (  # noqa: E402
    build_performance_architecture_review_v1,
    duplicate_source_detection_v1,
    write_performance_architecture_review_v1,
)


DAY = "2026-05-27"


def test_performance_architecture_review_artifact_generation(tmp_path: Path) -> None:
    report = build_performance_architecture_review_v1(repo_root=tmp_path, truth_root=tmp_path, day_utc=DAY)

    assert report["schema_id"] == "aegis_performance_architecture_review"
    assert report["day_utc"] == DAY
    assert report["review_only"] is True
    assert report["no_runtime_policy_changes"] is True
    assert report["broker_execution_allowed"] is False
    assert report["trade_advice_allowed"] is False
    assert len(report["existing_components"]) >= 15
    assert report["architecture_recommendations"]["minimal_durable_next_step"]

    path = write_performance_architecture_review_v1(repo_root=tmp_path, truth_root=tmp_path, day_utc=DAY, payload=report)
    assert path == tmp_path / "reports" / "aegis_performance_architecture_review_v1" / DAY / "performance_architecture_review.v1.json"
    written = json.loads(path.read_text(encoding="utf-8"))
    assert written["artifact_id"] == "aegis_performance_architecture_review_v1"


def test_duplicate_source_detection_flags_overlapping_pnl_and_exit_sources(tmp_path: Path) -> None:
    report = build_performance_architecture_review_v1(repo_root=tmp_path, truth_root=tmp_path, day_utc=DAY)
    duplicates = duplicate_source_detection_v1(report["existing_components"])
    by_capability = {row["capability"]: row for row in duplicates}

    assert "realized_pnl" in by_capability
    assert by_capability["realized_pnl"]["conflict_risk"] == "HIGH"
    assert "paper_position_ledger" in by_capability["realized_pnl"]["component_ids"]
    assert "sleeve_realized_pnl" in by_capability["realized_pnl"]["component_ids"]
    assert "exit_reason_frequency" in by_capability


def test_canonical_source_labeling_and_runtime_policy_safety(tmp_path: Path) -> None:
    report = build_performance_architecture_review_v1(repo_root=tmp_path, truth_root=tmp_path, day_utc=DAY)
    labels = {row["component_id"]: row for row in report["canonical_vs_duplicate_sources"]}

    assert labels["paper_position_ledger"]["canonicality"] == "CANONICAL_FOR_SIMULATED_PAPER_POSITIONS"
    assert labels["sleeve_economic_truth_pipeline"]["canonical_recommendation"] == "PROMOTE_AFTER_CONTRACT_AND_RECONCILIATION"
    assert labels["paper_trade_outcomes"]["canonical_recommendation"] == "KEEP_READ_ONLY_LEGACY_UNTIL_CONSUMERS_MIGRATE"
    assert report["safety"] == {
        "review_only": True,
        "no_runtime_policy_changes": True,
        "broker_execution_allowed": False,
        "broker_submit_transmit_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
        "order_routing_allowed": False,
    }
