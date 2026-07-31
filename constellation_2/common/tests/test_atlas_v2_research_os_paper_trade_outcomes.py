from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.paper_trade_outcomes import list_paper_trade_outcomes, record_paper_trade_outcome

NOW = "2026-06-05T00:00:00Z"


def _outcome(**overrides: object) -> dict:
    row = {
        "candidate_id": "candidate-1",
        "test_plan_id": "paper-plan-1",
        "created_at": NOW,
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


def test_record_paper_trade_outcome_is_append_only(tmp_path: Path) -> None:
    first = record_paper_trade_outcome(tmp_path, **_outcome())
    assert first["outcome_id"].startswith("pto-")
    assert first["metadata"]["paper_only"] is True
    assert list_paper_trade_outcomes(tmp_path) == [first]


def test_record_paper_trade_outcome_rejects_duplicate_id(tmp_path: Path) -> None:
    row = _outcome(outcome_id="outcome-1")
    record_paper_trade_outcome(tmp_path, **row)
    try:
        record_paper_trade_outcome(tmp_path, **row)
    except ValueError as exc:
        assert "paper outcome exists" in str(exc)
    else:
        raise AssertionError("duplicate outcome was accepted")
