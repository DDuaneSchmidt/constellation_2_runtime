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

from constellation_2.common.atlas_v2_research_os.paper_trading_queue import enqueue_paper_trade_candidate
from constellation_2.common.atlas_v2_research_os.paper_trading_queue_reports import audit_paper_trading_queue_report, build_paper_trading_queue_report, write_paper_trading_queue_report


def test_paper_trading_queue_report_written(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "store"
    plan = _plan()
    enqueue_paper_trade_candidate(candidate_id="candidate-1", test_plan=plan, queue_item_id="queue-1", priority=0.7, root=root)
    monkeypatch.chdir(tmp_path)
    paths = write_paper_trading_queue_report(root, day="2026-06-05")
    report = json.loads(paths["json"].read_text())
    assert report["queue_item_count"] == 1
    assert report["test_plan_count"] == 1
    assert report["state_counts"]["READY_FOR_HUMAN_REVIEW"] == 1
    assert report["ready_for_review"][0]["queue_item_id"] == "queue-1"
    assert paths["latest_json"].exists()
    assert "no broker execution" in paths["summary"].read_text().lower()
    assert audit_paper_trading_queue_report(report)["paper_trading_queue_audit_ok"] is True


def test_empty_report_is_valid(tmp_path: Path) -> None:
    report = build_paper_trading_queue_report(tmp_path)
    assert report["queue_item_count"] == 0
    assert report["governance_result"] == "PASS"
