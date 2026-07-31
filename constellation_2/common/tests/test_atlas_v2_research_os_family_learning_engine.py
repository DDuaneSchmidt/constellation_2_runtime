from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.family_learning_engine import build_family_learning_report
from constellation_2.common.atlas_v2_research_os.family_learning_models import FAMILY_LEARNING_REQUIRED_FIELDS
from constellation_2.common.atlas_v2_research_os.family_observation_log import CSV_FIELDS

NOW = "2026-06-05T00:00:00Z"


def _write_latest(root: Path, name: str, payload: dict) -> None:
    path = root / name / "latest.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_csv(root: Path) -> None:
    path = root / "family_observation_log" / "latest_template.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        _row("family-a", "true", "false", "supporting"),
        _row("family-a", "true", "false", "supporting"),
        _row("family-a", "false", "false", "inconclusive"),
        _row("family-b", "false", "true", "invalidating"),
        _row("family-b", "false", "true", "invalidating"),
        _row("family-c", "", "", "needs_data"),
        {field: "" for field in CSV_FIELDS} | {"family_id": "family-d"},
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def _row(family_id: str, observed: str, invalidated: str, outcome: str) -> dict[str, str]:
    return {
        "date": "2026-06-05",
        "family_id": family_id,
        "mechanism": "BREAKOUT",
        "regime": "CHOP",
        "timeframe": "30M",
        "symbol_or_universe": "SPY",
        "observation_condition_met": observed,
        "invalidation_condition_met": invalidated,
        "paper_outcome": outcome,
        "return_observed": "",
        "notes": "",
        "reviewer": "reviewer",
    }


def _seed_inputs(root: Path) -> None:
    _write_latest(
        root,
        "candidate_family_discovery",
        {
            "report_type": "CANDIDATE_FAMILY_DISCOVERY",
            "families": [
                {"family_id": "family-a", "family_name": "Family A", "classification": "ROBUST_ENOUGH_TO_OBSERVE", "best_rank": 1},
                {"family_id": "family-b", "family_name": "Family B", "classification": "ROBUST_ENOUGH_TO_OBSERVE", "best_rank": 2},
                {"family_id": "family-c", "family_name": "Family C", "classification": "PROMISING_BUT_DATA_BLOCKED", "best_rank": 3},
                {"family_id": "family-d", "family_name": "Family D", "classification": "ROBUST_ENOUGH_TO_OBSERVE", "best_rank": 4},
            ],
        },
    )
    _write_latest(
        root,
        "family_robustness_review",
        {
            "report_type": "FAMILY_ROBUSTNESS_REVIEW",
            "family_reviews": [
                {"family_id": "family-a", "family_name": "Family A", "classification": "ROBUST_ENOUGH_TO_OBSERVE", "best_rank": 1},
                {"family_id": "family-b", "family_name": "Family B", "classification": "ROBUST_ENOUGH_TO_OBSERVE", "best_rank": 2},
                {"family_id": "family-c", "family_name": "Family C", "classification": "PROMISING_BUT_DATA_BLOCKED", "best_rank": 3, "proxy_data_dependence": {"requires_direct_data_validation": True}},
                {"family_id": "family-d", "family_name": "Family D", "classification": "ROBUST_ENOUGH_TO_OBSERVE", "best_rank": 4},
            ],
        },
    )
    _write_latest(
        root,
        "edge_magnitude_estimation",
        {
            "report_type": "EDGE_MAGNITUDE_ESTIMATION",
            "families": [
                {"family_id": "family-a", "classification": "PLAUSIBLE", "direct_validation_status": "READY"},
                {"family_id": "family-c", "classification": "PLAUSIBLE", "direct_validation_status": "NEEDS_DIRECT_DATA", "data_blockers": ["direct data"]},
            ],
        },
    )
    _write_latest(root, "portfolio_relevance_estimate", {"report_type": "PORTFOLIO_RELEVANCE_ESTIMATE", "summary": {"base_signal_magnitude_could_plausibly_matter": True}})
    _write_latest(
        root,
        "family_paper_forward_observation",
        {
            "report_type": "FAMILY_PAPER_FORWARD_OBSERVATION",
            "plans": [
                {"family_id": "family-d", "plan_id": "plan-d", "observation_condition": "manual condition"},
            ],
        },
    )
    _write_csv(root)


def test_family_learning_engine_updates_family_confidence_from_observations(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)

    report = build_family_learning_report(root=tmp_path, created_at=NOW)
    rows = {row["family_id"]: row for row in report["family_learning_updates"]}

    assert report["report_type"] == "FAMILY_LEARNING_REPORT"
    assert set(FAMILY_LEARNING_REQUIRED_FIELDS) <= set(rows["family-a"])
    assert rows["family-a"]["status_after_update"] == "STRENGTHENED"
    assert rows["family-a"]["supporting_observations"] == 2
    assert rows["family-a"]["neutral_observations"] == 1
    assert rows["family-a"]["confidence_after"] > rows["family-a"]["confidence_before"]
    assert rows["family-b"]["status_after_update"] == "RETIRED"
    assert rows["family-c"]["status_after_update"] == "NEEDS_DIRECT_DATA"
    assert rows["family-d"]["status_after_update"] == "NEEDS_MORE_OBSERVATIONS"
    assert rows["family-d"]["observations_logged"] == 0


def test_family_learning_report_summary_counts_statuses(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)

    report = build_family_learning_report(root=tmp_path, created_at=NOW)

    assert report["summary"]["families_processed"] == 4
    assert report["summary"]["families_strengthened"] == 1
    assert report["summary"]["families_retired"] == 1
    assert report["summary"]["families_needing_data"] == 1
    assert report["summary"]["families_needing_more_observations"] == 1
    assert report["authority_boundary"]["trade_recommendation_authorized"] is False
    assert "No broker execution." in report["guardrails"]
