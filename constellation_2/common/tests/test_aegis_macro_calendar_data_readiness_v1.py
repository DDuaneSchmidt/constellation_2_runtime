from __future__ import annotations

import json
from pathlib import Path

from ops.aegis.hypothesis_proposal_promotion_v1 import build_all_hypothesis_proposal_promotion_v1
from ops.aegis.hypothesis_workflow_state_v1 import build_hypothesis_workflow_state_v1, build_operator_action_queue_v1
from ops.aegis.macro_calendar_data_readiness_v1 import (
    REQUIRED_FIELDS,
    build_macro_calendar_data_readiness_v1,
    macro_calendar_source_path_v1,
)
from constellation_2.common.tests.test_aegis_hypothesis_proposal_promotion_v1 import _seed_proposal

DAY = "2026-06-01"


def _write_source(truth: Path, rows: list[dict]) -> None:
    path = macro_calendar_source_path_v1(truth_root=truth, day_utc=DAY)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"events": rows}, sort_keys=True), encoding="utf-8")


def _valid_row() -> dict:
    return {
        "event_name": "CPI",
        "event_type": "CPI",
        "release_datetime": "2026-06-01T08:30:00-04:00",
        "actual": 3.1,
        "consensus": 3.0,
        "prior": 3.2,
        "importance": "HIGH",
        "affected_assets": ["SPY", "QQQ"],
        "source": "operator_provided_fixture",
        "source_timestamp": "2026-06-01T12:00:00Z",
        "timezone": "America/New_York",
        "data_quality_status": "GOVERNED",
    }


def _seed_macro_generated(repo: Path, truth: Path) -> None:
    _seed_proposal(repo, "ehp-macro", "Macro calendar event dislocation watch", content_hash="hash-macro", family="macro_headline_shock", symbols=["SPY"], blockers=["missing_macro_event_calendar"], ready=False)
    build_all_hypothesis_proposal_promotion_v1(truth_root=truth, day_utc=DAY, repo_root=repo)


def test_missing_source_creates_needs_source() -> None:
    payload = build_macro_calendar_data_readiness_v1(truth_root=Path("/tmp/nonexistent-aegis-macro-source"), day_utc=DAY)
    assert payload["status"] == "NEEDS_SOURCE"
    assert payload["macro_calendar_ready"] is False
    assert payload["david_action_required"] is True
    assert payload["buttons"] == ["Connect Source", "Upload Dataset", "Mark Not Available", "Defer"]
    assert payload["missing_fields"] == []
    assert payload["safety"]["no_broker_execution"] is True


def test_incomplete_source_lists_missing_fields_deterministically(tmp_path: Path) -> None:
    _write_source(tmp_path, [{"event_name": "CPI", "event_type": "CPI"}])
    payload = build_macro_calendar_data_readiness_v1(truth_root=tmp_path, day_utc=DAY)
    assert payload["status"] == "SOURCE_INCOMPLETE"
    assert payload["macro_calendar_ready"] is False
    assert payload["missing_fields"] == [field for field in REQUIRED_FIELDS if field not in {"event_name", "event_type"}]
    assert payload["david_action_required"] is True


def test_valid_source_creates_ready_without_forcing_shadow_pass(tmp_path: Path) -> None:
    _write_source(tmp_path, [_valid_row()])
    payload = build_macro_calendar_data_readiness_v1(truth_root=tmp_path, day_utc=DAY)
    assert payload["status"] == "READY"
    assert payload["macro_calendar_ready"] is True
    assert payload["david_action_required"] is False
    assert payload["downstream_effects"]["shadow_validation"] == "eligible_to_rerun"
    assert payload["downstream_effects"]["shadow_validation_pass_forced"] is False
    assert payload["safety"]["no_trade_advice"] is True


def test_macro_calendar_remains_needs_data_until_readiness_ready(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    truth = tmp_path / "truth"
    _seed_macro_generated(repo, truth)
    workflow = build_hypothesis_workflow_state_v1(truth_root=truth, day_utc=DAY)
    macro = next(row for row in workflow["hypotheses"] if row["display_name"] == "Macro calendar event dislocation watch")
    assert macro["current_state"] == "NEEDS_DATA"
    assert macro["next_action"] == "PROVIDE_DATA_SOURCE"
    queue = build_operator_action_queue_v1(workflow, truth_root=truth, day_utc=DAY)
    assert queue["actions"][0]["action_type"] == "PROVIDE_DATA_SOURCE"


def test_ready_source_allows_shadow_trial_rerun_but_does_not_pass_it(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    truth = tmp_path / "truth"
    _seed_macro_generated(repo, truth)
    _write_source(truth, [_valid_row()])
    readiness = build_macro_calendar_data_readiness_v1(truth_root=truth, day_utc=DAY)
    from ops.aegis.macro_calendar_data_readiness_v1 import write_macro_calendar_data_readiness_v1

    write_macro_calendar_data_readiness_v1(truth_root=truth, day_utc=DAY, payload=readiness)
    workflow = build_hypothesis_workflow_state_v1(truth_root=truth, day_utc=DAY)
    macro = next(row for row in workflow["hypotheses"] if row["display_name"] == "Macro calendar event dislocation watch")
    assert macro["current_state"] == "READY_FOR_SHADOW_TRIAL"
    assert macro["next_action"] == "WAIT_FOR_AUTOMATIC_PROCESSING"
    assert macro["macro_calendar_ready"] is True
    assert "MACRO_CALENDAR_DATA_READY" in macro["reason_codes"]
    assert "SHADOW_VALIDATION_PASSED" not in json.dumps(macro)
