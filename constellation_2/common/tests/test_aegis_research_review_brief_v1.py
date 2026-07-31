from __future__ import annotations

from pathlib import Path

from ops.aegis.research_lab.hypothesis_validation_v1 import (
    research_validation_self_check_v1,
    write_paper_testing_sleeve_v1,
)
from ops.aegis.research_lab.research_review_brief_v1 import (
    build_research_review_brief_v1,
    self_check_research_review_brief_v1,
    write_research_review_brief_v1,
)


def test_recommendation_ready_hypotheses_get_review_briefs(tmp_path: Path) -> None:
    payload = build_research_review_brief_v1(truth_root=tmp_path, day_utc="2026-05-29")

    briefs = payload["briefs"]
    assert payload["artifact_id"] == "aegis_research_review_brief_v1"
    assert payload["summary"]["recommendation_ready_count"] == len(briefs)
    assert len(briefs) >= 1
    for brief in briefs:
        assert brief["status"] in {"RECOMMENDATION_READY", "BLOCKED", "STALE"}
        assert brief["conclusion"]
        assert brief["confidence"] in {"LOW", "MEDIUM", "HIGH"}
        assert brief["evidence_summary"]
        assert brief["key_evidence"]
        assert brief["risks"]
        assert brief["decision_needed"]
        assert "REVIEW_BRIEF" in brief["allowed_actions"]
        assert brief["safety"] if "safety" in brief else True


def test_research_review_self_check_requires_briefs(tmp_path: Path) -> None:
    payload = build_research_review_brief_v1(truth_root=tmp_path, day_utc="2026-05-29")
    write_research_review_brief_v1(truth_root=tmp_path, day_utc="2026-05-29", payload=payload)

    report = self_check_research_review_brief_v1(truth_root=tmp_path, day_utc="2026-05-29")

    assert report["ok"] is True
    assert report["review_brief_count"] >= report["recommendation_ready_count"]
    assert all(check["ok"] for check in report["checks"])



def test_inconclusive_sample_size_review_brief_maps_to_collecting_evidence(tmp_path: Path) -> None:
    hypothesis_id = "rh-process-test-etf-drop-mean-reversion-v1"
    result_path = tmp_path / "reports" / "aegis_research_test_results_v1" / "2026-05-29" / hypothesis_id / "research_test_result.v1.json"
    result_path.parent.mkdir(parents=True, exist_ok=True)
    result_path.write_text(
        """{
  "day_utc": "2026-05-29",
  "hypothesis_id": "rh-process-test-etf-drop-mean-reversion-v1",
  "test_status": "INCONCLUSIVE_SAMPLE_SIZE",
  "latest_result": "INCONCLUSIVE_SAMPLE_SIZE",
  "result_summary": "ETF drop mean-reversion runner executed against governed market rows. sample_size=0/20; no paper validation until enough forward-return events exist.",
  "blocker": "INSUFFICIENT_FORWARD_RETURN_SAMPLE",
  "sample_size": 0,
  "minimum_sample_size": 20,
  "required_symbols": ["SPY", "QQQ", "IWM", "VIX"]
}
""",
        encoding="utf-8",
    )

    payload = build_research_review_brief_v1(truth_root=tmp_path, day_utc="2026-05-29")
    brief = next(row for row in payload["briefs"] if row["hypothesis_id"] == hypothesis_id)

    assert brief["validation_ui"]["state_label"] == "Collecting Evidence"
    assert brief["validation_ui"]["required_samples"] == 20
    assert brief["validation_ui"]["current_samples"] == 0
    assert brief["validation_ui"]["missing_samples"] == 20
    assert brief["validation_ui"]["next_sample_expected_at"] == "next market close"
    assert brief["validation_ui"]["estimated_completion_date"] == "after 20 valid observations"
    assert brief["validation_ui"]["expected_trading_days_remaining"] == 20
    assert brief["validation_ui"]["operator_action_required"] is False
    assert brief["validation_ui"]["paper_testing_sleeve"] == "Not created yet"
    assert brief["validation_ui"]["reason"] == "Qualification requires more evidence"
    assert brief["decision_needed"] == "No operator action required. Aegis is collecting forward-return observations."
    assert payload["summary"]["collecting_evidence_count"] >= 1



def test_research_validation_self_check_rejects_collecting_evidence_ambiguity(tmp_path: Path) -> None:
    payload = {
        "briefs": [
            {
                "hypothesis_id": "rh-process-test-etf-drop-mean-reversion-v1",
                "validation_ui": {
                    "state_label": "Collecting Evidence",
                    "required_samples": 20,
                    "current_samples": 0,
                    "missing_samples": 20,
                    "next_sample_expected_at": "next market close",
                    "operator_action_required": False,
                },
                "diagnostics": {"operator_action_required": False},
                "decision_needed": "No operator action required. Aegis is collecting forward-return observations.",
            }
        ]
    }
    write_research_review_brief_v1(truth_root=tmp_path, day_utc="2026-05-29", payload=payload)
    write_paper_testing_sleeve_v1(truth_root=tmp_path, day_utc="2026-05-29", payload={"sleeves": []})

    report = research_validation_self_check_v1(truth_root=tmp_path, day_utc="2026-05-29")

    assert report["ok"] is True
    assert report["collecting_evidence_count"] == 1
    assert all(check["ok"] for check in report["checks"])
