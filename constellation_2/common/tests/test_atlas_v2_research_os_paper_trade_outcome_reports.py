from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.paper_trade_outcome_reports import build_paper_trade_outcome_report, write_paper_trade_outcome_report


def _outcome(outcome_id: str, **overrides: object) -> dict:
    row = {
        "outcome_id": outcome_id,
        "candidate_id": outcome_id.replace("outcome", "candidate"),
        "test_plan_id": "plan-1",
        "created_at": "2026-06-05T00:00:00Z",
        "observation_start": "2026-06-01",
        "observation_end": "2026-06-05",
        "sample_size": 10,
        "wins": 7,
        "losses": 3,
        "average_return": 0.03,
        "expectancy": 0.02,
        "max_drawdown": -0.05,
        "profit_factor": 1.8,
        "regime_context": "HIGH_VOLATILITY",
        "hypothesis_confirmed": True,
        "hypothesis_weakened": False,
        "hypothesis_falsified": False,
        "failure_reasons": [],
        "success_reasons": ["positive expectancy"],
        "source_artifact_ids": ["artifact-1"],
        "metadata": {"mechanism": "EVENT_REACTION"},
    }
    row.update(overrides)
    return row


def test_report_includes_survival_feedback_memory_and_effectiveness(tmp_path: Path) -> None:
    report = build_paper_trade_outcome_report(tmp_path, [
        _outcome("outcome-good"),
        _outcome("outcome-bad", wins=1, losses=9, expectancy=-0.04, profit_factor=0.4, hypothesis_confirmed=False, hypothesis_falsified=True, failure_reasons=["negative expectancy"]),
        _outcome("outcome-small", sample_size=2, wins=2, losses=0),
    ])
    assert report["candidate_survival_counts"]["SURVIVED_INITIAL_TEST"] == 1
    assert report["candidate_survival_counts"]["FALSIFIED"] == 1
    assert report["candidate_survival_counts"]["NEEDS_MORE_DATA"] == 1
    assert report["feedback_signals"]
    assert report["memory_updates"]
    assert report["research_effectiveness_updates"]
    assert report["authority_boundary"]["live_trading_allowed"] is False


def test_write_report_paths(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    paths = write_paper_trade_outcome_report(tmp_path / "store", [_outcome("outcome-good")], day="2026-06-05")
    assert paths["json"].as_posix() == "reports/atlas_v2_research_os/paper_trade_outcomes/2026-06-05/paper_trade_outcome_report.json"
    assert paths["summary"].exists()
    assert paths["latest_json"].exists()
