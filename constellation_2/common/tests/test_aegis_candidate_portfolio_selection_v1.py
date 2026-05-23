from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.adaptive_governance.sleeve_performance_analytics_v1 import build_sleeve_performance_analytics_v1
from ops.aegis.candidate_portfolio_selection_v1 import build_candidate_portfolio_selection_v1, write_candidate_portfolio_selection_reports_v1
from ops.aegis.canonical_operator_state_v1 import build_canonical_operator_state_v1


DAY = "2026-05-20"
SLEEVE = "SLEEVE_MULTI_SYMBOL_V1"


def test_same_sleeve_same_exposure_selects_only_highest_ranked_by_default(tmp_path: Path) -> None:
    _write_candidates(tmp_path, [("c-nvda", "NVDA"), ("c-amd", "AMD")])
    _write_ranking(tmp_path, [("c-nvda", "NVDA", 100, "HIGH"), ("c-amd", "AMD", 95, "HIGH")])

    payload = build_candidate_portfolio_selection_v1(truth_root=tmp_path, day_utc=DAY)

    assert [row["symbol"] for row in payload["selected_candidates"]] == ["NVDA"]
    suppressed = payload["suppressed_candidates"][0]
    assert suppressed["symbol"] == "AMD"
    assert suppressed["suppression_reason_code"] == "DUPLICATE_EXPOSURE_CLUSTER"
    assert "same-sleeve candidate already represents AI_SEMIS" in suppressed["operator_explanation"]


def test_same_sleeve_distinct_exposure_can_select_multiple_candidates(tmp_path: Path) -> None:
    _write_candidates(tmp_path, [("c-xbi", "XBI"), ("c-gld", "GLD")])
    _write_ranking(tmp_path, [("c-xbi", "XBI", 100, "HIGH"), ("c-gld", "GLD", 95, "HIGH")])

    payload = build_candidate_portfolio_selection_v1(truth_root=tmp_path, day_utc=DAY)

    assert [row["symbol"] for row in payload["selected_candidates"]] == ["XBI", "GLD"]
    assert payload["same_sleeve_selection_summary"][0]["selected_count"] == 2
    assert payload["selected_candidates"][1]["exposure_cluster"] == "MACRO_DEFENSIVE"


def test_extra_same_sleeve_candidate_requires_high_confidence_or_score_threshold(tmp_path: Path) -> None:
    _write_candidates(tmp_path, [("c-xbi", "XBI"), ("c-gld", "GLD"), ("c-hyg", "HYG")])
    _write_ranking(
        tmp_path,
        [
            ("c-xbi", "XBI", 100, "HIGH"),
            ("c-gld", "GLD", 99, "LOW"),
            ("c-hyg", "HYG", 0, "LOW"),
        ],
    )

    payload = build_candidate_portfolio_selection_v1(truth_root=tmp_path, day_utc=DAY)

    assert [row["symbol"] for row in payload["selected_candidates"]] == ["XBI", "GLD"]
    assert payload["suppressed_candidates"][0]["symbol"] == "HYG"
    assert payload["suppressed_candidates"][0]["suppression_reason_code"] == "LOWER_RANKED_SAME_SLEEVE"


def test_absolute_max_per_sleeve_per_run_is_enforced(tmp_path: Path) -> None:
    rows = [("c-xbi", "XBI"), ("c-gld", "GLD"), ("c-hyg", "HYG"), ("c-tlt", "TLT")]
    _write_candidates(tmp_path, rows)
    _write_ranking(tmp_path, [(cid, symbol, 100 - idx, "HIGH") for idx, (cid, symbol) in enumerate(rows)])

    payload = build_candidate_portfolio_selection_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        policy={"max_per_sleeve_if_distinct_exposure": 3, "absolute_max_per_sleeve_per_run": 3},
    )

    assert len(payload["selected_candidates"]) == 3
    assert payload["suppressed_candidates"][0]["symbol"] == "TLT"
    assert payload["suppressed_candidates"][0]["suppression_reason_code"] == "LOWER_RANKED_SAME_SLEEVE"


def test_suppressed_candidates_get_readable_reasons_and_policy_fields(tmp_path: Path) -> None:
    _write_candidates(tmp_path, [("c-nvda", "NVDA"), ("c-amd", "AMD")])
    _write_ranking(tmp_path, [("c-nvda", "NVDA", 100, "HIGH"), ("c-amd", "AMD", 90, "HIGH")])

    payload = build_candidate_portfolio_selection_v1(truth_root=tmp_path, day_utc=DAY)
    row = payload["suppressed_candidates"][0]

    assert row["selection_status"] == "SUPPRESSED"
    assert row["operator_explanation"].startswith("Suppressed AMD because")
    assert row["same_sleeve_group"]["policy"] == "DEFAULT_ONE_PER_SLEEVE_WITH_DISTINCT_EXPOSURE_EXCEPTION"
    assert payload["portfolio_selection_policy"]["default_max_per_sleeve_per_run"] == 1
    assert payload["broker_execution_allowed"] is False
    assert payload["autonomous_execution_allowed"] is False


def test_canonical_state_includes_same_sleeve_selection_summary(tmp_path: Path) -> None:
    _write_candidates(tmp_path, [("c-xbi", "XBI"), ("c-gld", "GLD")])
    _write_ranking(tmp_path, [("c-xbi", "XBI", 100, "HIGH"), ("c-gld", "GLD", 95, "HIGH")])
    selection = build_candidate_portfolio_selection_v1(truth_root=tmp_path, day_utc=DAY)
    write_candidate_portfolio_selection_reports_v1(truth_root=tmp_path, day_utc=DAY, payload=selection)

    canonical = build_canonical_operator_state_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)

    opportunities = canonical["opportunities"]
    assert [row["symbol"] for row in opportunities["selected_candidates"]] == ["XBI", "GLD"]
    assert opportunities["same_sleeve_selection_summary"][0]["same_sleeve_candidate_count"] == 2
    assert opportunities["exposure_cluster_summary"]
    assert opportunities["portfolio_selection_policy"]["absolute_max_per_sleeve_per_run"] == 3


def test_performance_attribution_includes_selection_learning_metrics(tmp_path: Path) -> None:
    _write_candidates(tmp_path, [("c-nvda", "NVDA"), ("c-amd", "AMD")])
    _write_ranking(tmp_path, [("c-nvda", "NVDA", 100, "HIGH"), ("c-amd", "AMD", 90, "HIGH")])
    selection = build_candidate_portfolio_selection_v1(truth_root=tmp_path, day_utc=DAY)
    write_candidate_portfolio_selection_reports_v1(truth_root=tmp_path, day_utc=DAY, payload=selection)

    payload = build_sleeve_performance_analytics_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)
    advisory = payload["advisory_quality"]

    for key in [
        "selected_vs_suppressed_outcome",
        "suppressed_candidate_opportunity_cost",
        "same_sleeve_selection_accuracy",
        "exposure_cluster_selection_quality",
    ]:
        assert key in advisory
        assert advisory[key]["metric_status"] in {"MISSING_INPUT", "INSUFFICIENT_DATA", "OK"}


def test_no_broker_or_autonomous_execution_added() -> None:
    text = (REPO_ROOT / "ops/aegis/candidate_portfolio_selection_v1.py").read_text(encoding="utf-8")
    for forbidden in ["placeOrder", "submit_order", "broker_api", "orderId", "transmit=True", "automatic_order_placement_allowed\": True"]:
        assert forbidden not in text
    assert "broker_execution_allowed" in text
    assert "autonomous_execution_allowed" in text
    assert "automatic_order_placement_allowed" in text


def _write_candidates(root: Path, candidates: list[tuple[str, str]]) -> None:
    path = root / "reports" / "promoted_candidate_set_v1" / DAY / "selected" / "promoted_candidate_set.v1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "day_utc": DAY,
                "generated_at": "2026-05-20T13:00:00Z",
                "candidates": [
                    {
                        "candidate_id": candidate_id,
                        "generated_at": "2026-05-20T13:00:00Z",
                        "sleeve_id": SLEEVE,
                        "symbol": symbol,
                        "direction": "LONG",
                        "run_id": "RUN-1",
                    }
                    for candidate_id, symbol in candidates
                ],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def _write_ranking(root: Path, rows: list[tuple[str, str, float, str]]) -> None:
    path = root / "reports" / "aegis_candidate_ranking_v1" / DAY / "candidate_ranking.v1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema_id": "aegis_candidate_ranking",
                "schema_version": "v1",
                "day_utc": DAY,
                "ranked_candidates": [
                    {
                        "candidate_id": candidate_id,
                        "symbol": symbol,
                        "sleeve_id": SLEEVE,
                        "run_id": "RUN-1",
                        "rank": idx,
                        "ranking_score": score,
                        "confidence": confidence,
                    }
                    for idx, (candidate_id, symbol, score, confidence) in enumerate(rows, start=1)
                ],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
