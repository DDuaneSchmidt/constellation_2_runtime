from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.adaptive_governance.sleeve_performance_analytics_v1 import build_sleeve_performance_analytics_v1
from ops.aegis.candidate_lifecycle_v1 import append_candidate_decision_v1, update_candidate_outcomes_v1
from ops.aegis.journal.journal_event_v1 import build_journal_timeline_v1
from ops.aegis.position_management_v1 import (
    append_position_event_correction_v1,
    append_position_risk_plan_v1,
    append_stop_event_v1,
    build_position_management_v1,
    position_events_path_v1,
)


DAY = "2026-05-20"
CID = "risk-candidate-001"


def test_risk_plan_can_be_recorded_for_approved_candidate(tmp_path: Path) -> None:
    _approved_candidate(tmp_path)

    event = _risk_plan(tmp_path)
    state = build_position_management_v1(truth_root=tmp_path, day_utc=DAY)
    row = state["positions"][0]

    assert event["event_type"] == "POSITION_RISK_PLAN_RECORDED"
    assert row["candidate_id"] == CID
    assert row["risk_plan"]["stop_price"] == 96.0
    assert row["risk_plan"]["max_planned_loss"] == 100.0
    assert event["broker_execution_allowed"] is False
    assert event["autonomous_execution_allowed"] is False


def test_risk_plan_requires_candidate_id(tmp_path: Path) -> None:
    _approved_candidate(tmp_path)

    try:
        append_position_risk_plan_v1(
            truth_root=tmp_path,
            day_utc=DAY,
            candidate_id="",
            quantity=25,
            entry_price=100,
            stop_type="HARD_STOP",
            stop_price=96,
            operator="David",
            reason="risk plan",
        )
    except ValueError as exc:
        assert "candidate_id is required" in str(exc)
    else:
        raise AssertionError("risk plan accepted without candidate_id")


def test_stop_event_can_be_recorded_and_requires_operator_reason(tmp_path: Path) -> None:
    _approved_candidate(tmp_path)
    _risk_plan(tmp_path)

    event = append_stop_event_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        candidate_id=CID,
        stop_triggered=True,
        exit_price=95.8,
        exit_reason="STOPPED_OUT",
        operator="David",
        reason="Manual IB stop honored",
    )

    assert event["event_type"] == "STOP_EVENT_RECORDED"
    assert event["stop_triggered"] is True
    assert event["operator_confirmed"] is True
    assert event["automatic_stop_execution_allowed"] is False

    for kwargs in ({"operator": ""}, {"reason": ""}):
        try:
            append_stop_event_v1(
                truth_root=tmp_path,
                day_utc=DAY,
                candidate_id=CID,
                stop_triggered=False,
                exit_reason="MANUAL_EXIT",
                operator=kwargs.get("operator", "David"),
                reason=kwargs.get("reason", "reviewed"),
            )
        except ValueError as exc:
            assert "operator is required" in str(exc) or "reason is required" in str(exc)
        else:
            raise AssertionError("stop event accepted without operator/reason")


def test_correction_appends_event_and_latest_state_reflects_correction(tmp_path: Path) -> None:
    _approved_candidate(tmp_path)
    _risk_plan(tmp_path)
    before_rows = _jsonl_rows(position_events_path_v1(truth_root=tmp_path, day_utc=DAY))

    correction = append_position_event_correction_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        candidate_id=CID,
        field="stop_price",
        new_value="95.5",
        operator="David",
        reason="Correct entered stop",
    )
    after_rows = _jsonl_rows(position_events_path_v1(truth_root=tmp_path, day_utc=DAY))
    row = build_position_management_v1(truth_root=tmp_path, day_utc=DAY)["positions"][0]

    assert correction["event_type"] == "POSITION_EVENT_CORRECTED"
    assert len(after_rows) == len(before_rows) + 1
    assert after_rows[0]["event_type"] == "POSITION_RISK_PLAN_RECORDED"
    assert row["risk_plan"]["stop_price"] == 95.5
    assert row["correction_count"] == 1


def test_candidate_outcome_consumes_stop_event(tmp_path: Path) -> None:
    _approved_candidate(tmp_path)
    _risk_plan(tmp_path)
    append_stop_event_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        candidate_id=CID,
        stop_triggered=True,
        exit_price=95.8,
        exit_reason="STOPPED_OUT",
        operator="David",
        reason="Manual stop honored",
    )

    payload = update_candidate_outcomes_v1(truth_root=tmp_path, day_utc=DAY, candidate_id=CID)
    row = payload["outcomes"][0]

    assert row["stopped_out"] is True
    assert row["stop_price"] == 96.0
    assert row["exit_price"] == 95.8
    assert row["stop_loss_amount"] == 105.0
    assert row["stop_loss_pct"] == 0.042
    assert row["stop_honored"] is True
    assert row["stop_effectiveness_status"] in {"PROTECTED_CAPITAL", "UNKNOWN"}


def test_performance_attribution_includes_stop_metrics(tmp_path: Path) -> None:
    _approved_candidate(tmp_path)
    _risk_plan(tmp_path)
    append_stop_event_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        candidate_id=CID,
        stop_triggered=False,
        exit_reason="STOP_NOT_HONORED",
        operator="David",
        reason="Discipline tracking",
    )

    payload = build_sleeve_performance_analytics_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)
    advisory = payload["advisory_quality"]

    for key in ["stop_out_rate", "average_stop_loss", "stop_honored_rate", "stopped_out_then_recovered_rate", "sleeve_stop_effectiveness", "regime_stop_effectiveness", "operator_stop_discipline"]:
        assert key in advisory
        assert advisory[key]["metric_status"] in {"INSUFFICIENT_DATA", "OK"}
    assert "stop_attribution" in payload


def test_journal_shows_stop_event_history(tmp_path: Path) -> None:
    _approved_candidate(tmp_path)
    _risk_plan(tmp_path)
    append_stop_event_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        candidate_id=CID,
        stop_triggered=True,
        exit_price=95.8,
        exit_reason="STOPPED_OUT",
        operator="David",
        reason="Manual stop honored",
    )
    update_candidate_outcomes_v1(truth_root=tmp_path, day_utc=DAY, candidate_id=CID)

    journal = build_journal_timeline_v1(truth_root=tmp_path, day_utc=DAY)
    event_types = [row["event_type"] for row in journal["recent_events"] if row["entity_id"] == CID]

    assert "candidate.generated" in event_types
    assert "candidate.decision_recorded" in event_types
    assert "position.risk_plan_recorded" in event_types
    assert "position.stop_event_recorded" in event_types


def test_no_broker_or_autonomous_execution_added() -> None:
    text = "\\n".join(
        path.read_text(encoding="utf-8")
        for path in [
            REPO_ROOT / "ops/aegis/position_management_v1.py",
            REPO_ROOT / "ops/tools/record_aegis_position_risk_plan_v1.py",
            REPO_ROOT / "ops/tools/record_aegis_stop_event_v1.py",
            REPO_ROOT / "ops/tools/correct_aegis_position_event_v1.py",
        ]
    )

    for forbidden in ["placeOrder", "submit_order", "broker_api", "orderId", "transmit=True"]:
        assert forbidden not in text
    assert "broker_execution_allowed\": False" in text or "broker_execution_allowed" in text
    assert "autonomous_execution_allowed\": False" in text or "autonomous_execution_allowed" in text


def _approved_candidate(root: Path) -> None:
    path = root / "reports" / "promoted_candidate_set_v1" / DAY / "selected" / "promoted_candidate_set.v1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "day_utc": DAY,
                "generated_at": "2026-05-20T13:00:00Z",
                "candidates": [
                    {
                        "candidate_id": CID,
                        "generated_at": "2026-05-20T13:00:00Z",
                        "sleeve_id": "SLEEVE_A",
                        "symbol": "QQQ",
                        "direction": "LONG",
                    }
                ],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    append_candidate_decision_v1(
        truth_root=root,
        day_utc=DAY,
        candidate_id=CID,
        decision="TRADED_MANUALLY",
        reason="Approved for manual capture",
        operator="David",
        intended_shares=25,
        executed_confirmed=True,
    )


def _risk_plan(root: Path) -> dict:
    return append_position_risk_plan_v1(
        truth_root=root,
        day_utc=DAY,
        candidate_id=CID,
        quantity=25,
        entry_price=100,
        stop_type="HARD_STOP",
        stop_price=96,
        operator="David",
        reason="risk plan",
        stop_reason="below setup invalidation level",
    )


def _jsonl_rows(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
