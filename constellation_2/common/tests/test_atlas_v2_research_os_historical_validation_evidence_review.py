from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.historical_validation_evidence_review import (
    FINAL_DECISIONS,
    build_historical_validation_evidence_review,
    write_historical_validation_evidence_review,
)

NOW = "2026-06-06T00:00:00Z"


def test_historical_validation_review_monitor_only_when_oos_missing(tmp_path: Path) -> None:
    _write_json(tmp_path / "exact_replay_without_fallback" / "latest.json", {"candidate_results": []})
    _write_json(tmp_path / "net_of_cost_evidence" / "latest.json", {"candidate_results": []})
    _write_json(
        tmp_path / "expansion_evidence_review" / "latest.json",
        {
            "summary": {"decision": "CONTINUE_TSLA_ONLY", "rationale": "TSLA-only"},
            "evidence_state": {
                "exact_tsla_classification": "EXACT_CONFIRMED_STRONG",
                "exact_tsla_samples": 338,
                "tsla_cost_classification": "POSSIBLY_VIABLE",
                "tsla_net_expectancy_10bps": 0.000496,
            },
        },
    )

    report = build_historical_validation_evidence_review(root=tmp_path, created_at=NOW)

    assert report["summary"]["decision"] == "MONITOR_ONLY"
    assert report["summary"]["required_answer"] == "NOT_PROVEN_HISTORICAL_OOS_SPLIT_VALIDATION_MISSING"
    assert report["summary"]["allowed_decisions"] == FINAL_DECISIONS
    assert report["summary"]["trading_authority"] is False
    assert report["summary"]["broker_execution_authority"] is False
    assert any(row["failure_mode"] == "HISTORICAL_OOS_SPLIT_VALIDATION_MISSING" for row in report["failure_modes"])


def test_historical_validation_review_continues_when_all_validation_survives(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "historical_oos_split_validation" / "latest.json",
        {
            "summary": {"oos_classification": "OOS_SURVIVED", "degradation_detected": False},
            "split_results": [
                {"split": "train", "net_expectancy": 0.001},
                {"split": "validation", "net_expectancy": 0.0008},
                {"split": "observation", "net_expectancy": 0.0007},
            ],
        },
    )
    _write_json(tmp_path / "walk_forward_validation" / "latest.json", {"summary": {"classification": "WALK_FORWARD_SURVIVED"}})
    _write_json(tmp_path / "temporal_robustness_decay" / "latest.json", {"summary": {"classification": "DECAY_ACCEPTABLE"}})
    _write_json(tmp_path / "null_model_randomized_control" / "latest.json", {"summary": {"classification": "NULL_REJECTED"}})
    _write_json(tmp_path / "exact_replay_without_fallback" / "latest.json", {"candidate_results": []})
    _write_json(tmp_path / "net_of_cost_evidence" / "latest.json", {"candidate_results": []})
    _write_json(
        tmp_path / "expansion_evidence_review" / "latest.json",
        {
            "summary": {"decision": "CONTINUE_TSLA_ONLY", "rationale": "TSLA-only"},
            "evidence_state": {
                "exact_tsla_classification": "EXACT_CONFIRMED_STRONG",
                "exact_tsla_samples": 338,
                "tsla_cost_classification": "POSSIBLY_VIABLE",
                "tsla_net_expectancy_10bps": 0.000496,
            },
        },
    )

    report = build_historical_validation_evidence_review(root=tmp_path, created_at=NOW)

    assert report["summary"]["decision"] == "CONTINUE_TSLA_ONLY"
    assert report["summary"]["required_answer"] == "YES_SOURCE_BACKED_OOS_SURVIVED"
    assert report["failure_modes"] == []


def test_historical_validation_review_writes_requested_outputs(tmp_path: Path) -> None:
    report = build_historical_validation_evidence_review(root=tmp_path, created_at=NOW)
    paths = write_historical_validation_evidence_review(report, root=tmp_path)

    assert paths["latest_json"].name == "latest.json"
    assert paths["latest_summary"].name == "latest_summary.md"
    assert paths["scorecard"].name == "validation_scorecard.csv"
    assert paths["failure_modes"].name == "failure_modes.csv"
    assert paths["next_decision"].name == "next_decision.csv"
    for path in paths.values():
        assert path.exists()
    payload = json.loads(paths["latest_json"].read_text(encoding="utf-8"))
    assert payload["schema_id"] == "atlas_v2_research_os_historical_validation_evidence_review"
    assert "Historical Validation Evidence Review" in paths["latest_summary"].read_text(encoding="utf-8")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
