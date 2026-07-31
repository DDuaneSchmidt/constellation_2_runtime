from __future__ import annotations

import json
from pathlib import Path

from ops.aegis.approved_hypothesis_paper_setup_v1 import build_all_approved_hypothesis_paper_setup_v1
from ops.aegis.hypothesis_proposal_promotion_v1 import (
    build_all_hypothesis_proposal_promotion_v1,
    record_paper_promotion_approval_event_v1,
)
from ops.aegis.hypothesis_workflow_state_v1 import (
    append_operator_action_event_v1,
    build_generated_hypothesis_throughput_v1,
    build_hypothesis_workflow_replay_verification_v1,
    build_operator_action_queue_v1,
    build_hypothesis_workflow_state_v1,
    build_workflow_state_resolver_v1,
    operator_action_event_log_path_v1,
    write_hypothesis_workflow_state_v1,
    write_operator_action_queue_v1,
)
from constellation_2.common.tests.test_aegis_approved_hypothesis_paper_setup_v1 import _write_required_runtime_truth
from constellation_2.common.tests.test_aegis_hypothesis_proposal_promotion_v1 import _seed_proposal


DAY = "2026-06-01"


def _seed_portfolio(truth: Path) -> None:
    path = truth / "reports" / "aegis_research_portfolio_v1" / DAY / "research_portfolio.v1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_id": "aegis_research_portfolio_v1",
        "day_utc": DAY,
        "generated_at": f"{DAY}T00:00:00Z",
        "content_hash": "portfolio-hash",
        "hypotheses": [
            {"hypothesis_id": "HYP_SEEDED_ONE", "name": "Seeded One", "state": "ACCUMULATING_EVIDENCE", "validation_state": "UNVALIDATED", "sample_count": 3, "reason_codes": ["LOW_SAMPLE_COUNT"]},
            {"hypothesis_id": "HYP_SEEDED_TWO", "name": "Seeded Two", "state": "ACCUMULATING_EVIDENCE", "validation_state": "UNVALIDATED", "sample_count": 0, "reason_codes": []},
        ],
    }
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _seed_generated(repo: Path, truth: Path) -> None:
    _seed_proposal(repo, "ehp-oil", "Oil shock reversals across energy ETFs", content_hash="hash-oil")
    _seed_proposal(repo, "ehp-macro", "Macro calendar event dislocation watch", content_hash="hash-macro", family="macro_headline_shock", symbols=["SPY"], blockers=["missing_macro_event_calendar"], ready=False)
    build_all_hypothesis_proposal_promotion_v1(truth_root=truth, day_utc=DAY, repo_root=repo)
    record_paper_promotion_approval_event_v1(
        truth_root=truth,
        day_utc=DAY,
        hypothesis_id="ehp-oil",
        proposal_id="ehp-oil",
        promotion_packet_hash="packet-hash",
        prior_state="PAPER_PROMOTION_RECOMMENDED",
        decision="APPROVED",
        generated_at_utc=f"{DAY}T12:00:00Z",
    )
    build_all_hypothesis_proposal_promotion_v1(truth_root=truth, day_utc=DAY, repo_root=repo)
    _write_required_runtime_truth(truth)
    build_all_approved_hypothesis_paper_setup_v1(truth_root=truth, day_utc=DAY)


def test_workflow_state_contains_seeded_and_generated_hypotheses_and_blocks_oil_regression(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    truth = tmp_path / "truth"
    _seed_portfolio(truth)
    _seed_generated(repo, truth)
    workflow = build_hypothesis_workflow_state_v1(truth_root=truth, day_utc=DAY)
    rows = {row["display_name"]: row for row in workflow["hypotheses"]}
    assert "Seeded One" in rows
    assert "Seeded Two" in rows
    assert rows["Oil shock reversals across energy ETFs"]["current_state"] in {"PAPER_TRACKING_READY", "PAPER_TRACKING_BLOCKED", "PAPER_PROMOTION_APPROVED", "PAPER_SETUP_RUNNING"}
    assert rows["Oil shock reversals across energy ETFs"]["current_state"] != "PAPER_PROMOTION_RECOMMENDED"
    oil = rows["Oil shock reversals across energy ETFs"]
    assert oil["next_action"] != "APPROVE_PAPER_TEST"
    assert oil["resolver_rule_id"] in {"paper_tracking_ready_after_setup", "paper_tracking_blocked_overrides_ready", "approved_overrides_recommended", "paper_setup_running_after_approval"}
    assert "state_entered_at_utc" in oil
    assert "time_in_state_days" in oil
    assert "previous_state_hash" in oil
    assert "current_state_hash" in oil


def test_macro_calendar_produces_provide_data_source_action_and_exact_buttons(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    truth = tmp_path / "truth"
    _seed_portfolio(truth)
    _seed_generated(repo, truth)
    workflow = build_hypothesis_workflow_state_v1(truth_root=truth, day_utc=DAY)
    macro = next(row for row in workflow["hypotheses"] if row["display_name"] == "Macro calendar event dislocation watch")
    assert macro["current_state"] == "NEEDS_DATA"
    assert macro["next_action"] == "PROVIDE_DATA_SOURCE"
    assert macro["missing_dataset"] == "macro event calendar"
    assert macro["required_fields"] == ["event_name", "event_type", "release_datetime", "actual", "consensus", "prior", "importance", "affected_assets"]
    queue = build_operator_action_queue_v1(workflow, truth_root=truth, day_utc=DAY)
    assert len(queue["actions"]) == 1
    assert queue["actions"][0]["action_type"] == "PROVIDE_DATA_SOURCE"
    assert queue["actions"][0]["exact_buttons"] == ["Connect Source", "Upload Dataset", "Mark Not Available", "Defer"]
    assert queue["actions"][0]["priority"] == "NORMAL"
    assert queue["actions"][0]["impact_area"] == "EDGE_DISCOVERY"
    assert queue["actions"][0]["blocking_what"] == "shadow validation and promotion eligibility"
    assert queue["actions"][0]["action_age_days"] == macro["time_in_state_days"]


def test_queue_contains_only_real_david_actions_and_rerun_is_deterministic(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    truth = tmp_path / "truth"
    _seed_portfolio(truth)
    _seed_generated(repo, truth)
    first = build_hypothesis_workflow_state_v1(truth_root=truth, day_utc=DAY)
    second = build_hypothesis_workflow_state_v1(truth_root=truth, day_utc=DAY)
    assert first["content_hash"] == second["content_hash"]
    queue = build_operator_action_queue_v1(first, truth_root=truth, day_utc=DAY)
    assert all(item["action_type"] in {"APPROVE_PAPER_TEST", "PROVIDE_DATA_SOURCE", "REVIEW_RETIREMENT", "REVIEW_CAPITAL"} for item in queue["actions"])
    assert "needs review" not in json.dumps(queue).lower()


def test_operator_action_event_log_appends_safety_event(tmp_path: Path) -> None:
    event = append_operator_action_event_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        action_id="aegis-action:test",
        hypothesis_id="ehp-macro",
        action_type="PROVIDE_DATA_SOURCE",
        button_clicked="Mark Not Available",
        prior_state="NEEDS_DATA",
        new_state="NEEDS_DATA",
        source_state_hash="state-hash",
        event_timestamp_utc=f"{DAY}T12:00:00Z",
    )
    assert event["no_broker_execution"] is True
    assert event["no_trade_advice"] is True
    assert event["no_live_trading"] is True
    assert event["no_real_capital"] is True
    assert operator_action_event_log_path_v1(truth_root=tmp_path, day_utc=DAY).exists()


def test_phase2_resolver_replay_and_throughput_reports(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    truth = tmp_path / "truth"
    _seed_portfolio(truth)
    _seed_generated(repo, truth)
    resolver = build_workflow_state_resolver_v1(truth_root=truth, day_utc=DAY)
    assert resolver["schema_id"] == "aegis_workflow_state_resolver_v1"
    assert [rule["rule_id"] for rule in resolver["rules"]][:2] == ["needs_data_overrides_ready", "paper_tracking_blocked_overrides_ready"]
    write_hypothesis_workflow_state_v1(truth_root=truth, day_utc=DAY)
    write_operator_action_queue_v1(truth_root=truth, day_utc=DAY)
    replay = build_hypothesis_workflow_replay_verification_v1(truth_root=truth, day_utc=DAY)
    assert replay["verification_status"] == "PASS"
    throughput = build_generated_hypothesis_throughput_v1(truth_root=truth, day_utc=DAY)
    rows = {row["hypothesis_name"]: row for row in throughput["generated_hypotheses"]}
    assert rows["Oil shock reversals across energy ETFs"]["throughput_status"] == "PAPER_TRACKING_READY"
    assert rows["Oil shock reversals across energy ETFs"]["next_expected_step"] == "candidate generation"
    assert rows["Oil shock reversals across energy ETFs"]["no_david_action_required_unless_blocked"] is True
    assert rows["Macro calendar event dislocation watch"]["throughput_status"] == "NEEDS_DATA"
