from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.historical_replay_engine import create_historical_replay_request, run_historical_replay
from constellation_2.common.atlas_v2_research_os.historical_replay_models import HistoricalReplayEvidence, HistoricalReplayResult, HistoricalReplaySummary

NOW = "2026-06-05T00:00:00Z"


def test_creates_replay_request_result_evidence_and_summary() -> None:
    request = create_historical_replay_request(hypothesis_id="hyp-1", mechanism_tags=["OPENING_RANGE"], regime_context={"label": "OPEN"}, time_window="90d", source_artifact_ids=["art-1"], replay_id="replay-1", created_at=NOW)
    assert request["evidence_level"] == "HISTORICAL_REPLAY"
    result = run_historical_replay(request, [{"return": 0.01}, {"return": -0.002}, {"return": 0.004}, {"return": 0.003}, {"return": 0.002}], created_at=NOW)
    assert result["sample_size"] == 5
    evidence = HistoricalReplayEvidence(**result["evidence"]).to_dict()
    assert evidence["evidence_level"] == "HISTORICAL_REPLAY"
    assert HistoricalReplayResult(**result).to_dict()["replay_id"] == "replay-1"
    summary = HistoricalReplaySummary(status=result["certification"]["status"], score=result["metrics"]["historical_replay_score"], metrics=result["metrics"], **{k: result[k] for k in ["replay_id", "hypothesis_id", "mechanism_tags", "regime_context", "time_window", "sample_size", "result_count", "evidence_level", "created_at", "source_artifact_ids", "metadata"]}).to_dict()
    assert summary["status"] in {"REPLAY_POSITIVE", "REPLAY_NEUTRAL", "REPLAY_NEGATIVE", "INSUFFICIENT_SAMPLE"}


def test_replay_evidence_level_never_escalates() -> None:
    request = create_historical_replay_request(hypothesis_id="hyp-1", mechanism_tags=["OPENING_RANGE"], replay_id="replay-2", created_at=NOW)
    result = run_historical_replay(request, [{"return": 0.01}], created_at=NOW)
    assert result["evidence_level"] == "HISTORICAL_REPLAY"
    assert result["certification"]["trading_authorized"] is False
