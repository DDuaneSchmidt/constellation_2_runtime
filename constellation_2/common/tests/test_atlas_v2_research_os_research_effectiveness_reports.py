from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.research_effectiveness_reports import build_research_effectiveness_report, write_research_effectiveness_report

NOW = "2026-06-04T00:00:00Z"


def _activities() -> list[dict]:
    return [
        {
            "activity_id": "good",
            "created_at": NOW,
            "mechanism": "OPENING_RANGE",
            "worker": "worker-a",
            "backlog_type": "FAILURE_ANALYSIS",
            "experiment_type": "CHEAP_EXPERIMENT",
            "failure_category": "REGIME_MISMATCH",
            "regime": "HIGH_VOL",
            "evidence_maturity": "PAPER_FORWARD_OBSERVATION",
            "cost_estimate": 1.0,
            "input_uncertainty": 0.9,
            "output_uncertainty": 0.2,
            "failures_before": 5,
            "failures_after": 2,
        },
        {
            "activity_id": "expensive",
            "created_at": NOW,
            "mechanism": "PAIR_TRADE",
            "worker": "worker-b",
            "backlog_type": "EVIDENCE_GAP",
            "experiment_type": "REPLAY",
            "failure_category": "UNKNOWN",
            "regime": "UNKNOWN",
            "evidence_maturity": "GENERATED_ONLY",
            "cost_estimate": 6.0,
        },
    ]


def test_report_sections_answer_effectiveness_questions() -> None:
    report = build_research_effectiveness_report(_activities())
    assert report["top_contributors"][0]["activity_id"] == "good"
    assert report["worst_contributors"][0]["activity_id"] == "expensive"
    assert report["high_cost_low_value_research"][0]["activity_id"] == "expensive"
    assert report["high_value_low_cost_research"][0]["activity_id"] == "good"
    assert "Most Valuable Research Paths" in report["research_roi_estimates"]


def test_write_report_outputs_research_effectiveness_directory(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    paths = write_research_effectiveness_report(_activities(), day="2026-06-05")
    assert paths["json"].as_posix() == "reports/atlas_v2_research_os/research_effectiveness/2026-06-05/research_effectiveness_report.json"
    assert paths["summary"].exists()
    assert paths["latest_json"].exists()



def test_write_report_with_no_activities_regenerates_insufficient_data_latest(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    paths = write_research_effectiveness_report([], day="2026-06-05")
    assert paths["latest_json"].exists()
    import json
    report = json.loads(paths["latest_json"].read_text(encoding="utf-8"))
    assert report["activity_count"] == 0
    assert report["certification"]["result"] == "INSUFFICIENT_DATA"
