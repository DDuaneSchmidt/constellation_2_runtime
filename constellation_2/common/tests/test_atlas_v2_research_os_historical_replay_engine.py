from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.historical_replay_engine import calculate_replay_metrics, create_historical_replay_request, run_historical_replay

NOW = "2026-06-05T00:00:00Z"


def test_calculates_metrics() -> None:
    metrics = calculate_replay_metrics([{"return": 0.02, "regime": "A"}, {"return": -0.01, "regime": "A"}, {"return": 0.03, "regime": "B"}])
    assert metrics["sample_size"] == 3
    assert metrics["win_rate"] == 0.666667
    assert metrics["expectancy"] == 0.013333
    assert metrics["profit_factor"] == 5.0
    assert "A" in metrics["regime_specific_performance"]


def test_handles_empty_sample() -> None:
    metrics = calculate_replay_metrics([])
    assert metrics["sample_size"] == 0
    assert metrics["win_rate"] is None
    assert metrics["historical_replay_score"] == 0.0


def test_handles_large_sample() -> None:
    request = create_historical_replay_request(hypothesis_id="hyp-large", mechanism_tags=["MEAN_REVERSION"], time_window="5y", replay_id="replay-large", created_at=NOW)
    samples = [{"return": 0.002 if idx % 3 else -0.001, "regime": "R"} for idx in range(1000)]
    result = run_historical_replay(request, samples, created_at=NOW)
    assert result["sample_size"] == 1000
    assert result["metrics"]["max_drawdown"] is not None
