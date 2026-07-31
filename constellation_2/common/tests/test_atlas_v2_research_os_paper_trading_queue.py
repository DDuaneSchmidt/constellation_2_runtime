from __future__ import annotations

import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _plan(**overrides):
    from constellation_2.common.atlas_v2_research_os.paper_trade_test_plan import create_paper_trade_test_plan

    data = dict(
        test_plan_id="plan-1",
        candidate_id="candidate-1",
        mechanism_tags=["MEAN_REVERSION"],
        regime_context="LOW_VOL",
        entry_condition_description="Observe entry when paper signal crosses threshold.",
        exit_condition_description="Observe exit when paper signal mean reverts or window expires.",
        invalidating_conditions=["Regime changes", "Data unavailable"],
        paper_observation_window={"days": 30},
        minimum_sample_size=20,
        source_artifact_ids=["artifact-1"],
        created_at="2026-06-05T00:00:00Z",
    )
    data.update(overrides)
    return create_paper_trade_test_plan(**data)

from constellation_2.common.atlas_v2_research_os.paper_trading_queue import approve_for_paper_test, enqueue_paper_trade_candidate, list_ready_for_review, prioritize_paper_trading_queue, reject_paper_trade_candidate


def test_enqueue_prioritize_and_approve_for_paper_test(tmp_path: Path) -> None:
    low = _plan(test_plan_id="plan-low", candidate_id="candidate-low")
    high = _plan(test_plan_id="plan-high", candidate_id="candidate-high")
    enqueue_paper_trade_candidate(candidate_id="candidate-low", test_plan=low, queue_item_id="queue-low", priority=0.2, root=tmp_path, created_at="2026-06-05T00:00:00Z")
    high_item = enqueue_paper_trade_candidate(candidate_id="candidate-high", test_plan=high, queue_item_id="queue-high", priority=0.9, root=tmp_path, created_at="2026-06-05T00:01:00Z")
    ranked = prioritize_paper_trading_queue(tmp_path)
    assert [row["queue_item_id"] for row in ranked] == ["queue-high", "queue-low"]
    assert [row["queue_item_id"] for row in list_ready_for_review(tmp_path)] == ["queue-high", "queue-low"]
    approved = approve_for_paper_test(high_item["queue_item_id"], reviewer="human", root=tmp_path)
    assert approved["state"] == "APPROVED_FOR_PAPER_TEST"
    assert approved["review_status"] == "APPROVED"
    assert approved["metadata"]["approved_for_paper_test_only"] is True


def test_enqueue_is_idempotent_for_same_queue_item(tmp_path: Path) -> None:
    plan = _plan()
    first = enqueue_paper_trade_candidate(candidate_id="candidate-1", test_plan=plan, queue_item_id="queue-1", root=tmp_path)
    second = enqueue_paper_trade_candidate(candidate_id="candidate-1", test_plan=plan, queue_item_id="queue-1", root=tmp_path)
    assert first == second


def test_reject_paper_trade_candidate(tmp_path: Path) -> None:
    plan = _plan()
    item = enqueue_paper_trade_candidate(candidate_id="candidate-1", test_plan=plan, queue_item_id="queue-1", root=tmp_path)
    rejected = reject_paper_trade_candidate(item["queue_item_id"], reviewer="human", reasons=["Needs clearer invalidation"], root=tmp_path)
    assert rejected["state"] == "REJECTED"
    assert rejected["review_status"] == "REJECTED"
    assert "Needs clearer invalidation" in rejected["blocked_reason"]
