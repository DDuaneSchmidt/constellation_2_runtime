from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


REPO_ROOT = _repo_root()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_lite_event_awareness_v1 import (  # noqa: E402
    build_event_alert_v1,
    build_event_awareness_ledger_v1,
    build_event_tactical_packet_v1,
    build_event_validity_gate_v1,
    build_trade_capture_alert_gate_v1,
    build_trade_capture_alert_ledger_v1,
    validate_event_awareness_artifact_v1,
    write_event_awareness_artifact_v1,
)
from constellation_2.common.aegis_research_lab_v1 import (  # noqa: E402
    build_learning_tasks_from_outcomes_v1,
    build_manual_execution_receipt_v1,
    build_outcome_ledger_v1,
    validate_research_lab_artifact_v1,
)
from ops.tools.run_event_awareness_v1 import main as awareness_main  # noqa: E402
from ops.tools.run_event_tactical_review_v1 import main as tactical_main  # noqa: E402
from ops.tools.run_event_validity_gate_v1 import main as gate_main  # noqa: E402
from ops.tools.run_trade_capture_alert_gate_v1 import main as alert_gate_main  # noqa: E402


DAY = "2026-05-15"
NOW = "2026-05-15T18:00:00Z"


def _packet(**overrides: object) -> dict[str, object]:
    base = {
        "event_id": "event-1",
        "event_run_id": "event-run-1",
        "day_utc": DAY,
        "symbol": "SPY",
        "side": "BUY",
        "instrument_type": "ETF",
        "entry_reference_price": "500.00",
        "order_type_suggestion": "MANUAL_LIMIT",
        "quantity_or_sizing_guidance": "1 share dry-run sizing",
        "stop_price": "494.00",
        "stop_logic": "protective stop below stabilization low",
        "risk_per_trade": "small fixed risk",
        "event_type": "PANIC_EXHAUSTION",
        "edge_family": "PANIC_EXHAUSTION",
        "regime_state": "PANIC",
        "confidence": "MEDIUM",
        "execution_sensitivity": "MEDIUM",
        "valid_until": "2026-05-15T19:00:00Z",
        "max_entry_slippage": "50bps",
        "invalidation_conditions": ["fresh low"],
        "inclusion_reason": "panic exhaustion review",
    }
    base.update(overrides)
    return build_event_tactical_packet_v1(**base)


def test_lite_sleeve_timer_runs_at_0950_and_1450_utc() -> None:
    timer = (REPO_ROOT / "ops/systemd/user/aegis-lite-eod-report-v1.timer").read_text(encoding="utf-8")

    assert "OnCalendar=*-*-* 09:50:00 UTC" in timer
    assert "OnCalendar=*-*-* 14:50:00 UTC" in timer
    assert "15:50:00" not in timer


def test_event_awareness_cannot_mutate_canonical_eod_state(tmp_path: Path) -> None:
    ledger = build_event_awareness_ledger_v1(
        run_id="event-run-1",
        day_utc=DAY,
        generated_at_utc=NOW,
        events=[
            build_event_alert_v1(
                event_id="event-1",
                run_id="event-run-1",
                day_utc=DAY,
                timestamp_utc=NOW,
                event_type="VOLATILITY_SPIKE",
                alert_level="WATCH",
                severity="MEDIUM",
                confidence="LOW",
                tactical_review_requested=False,
            )
        ],
    )

    validate_event_awareness_artifact_v1(ledger)
    path = write_event_awareness_artifact_v1(truth_root=tmp_path, payload=ledger)
    assert path.exists()
    assert ledger["non_canonical_event_layer"] is True
    assert ledger["canonical_eod_state_mutated"] is False
    assert not (tmp_path / "reports" / "aegis_lite_eod_report_v1").exists()


def test_info_watch_cannot_create_actionable_trade() -> None:
    with pytest.raises(ValueError, match="INFO_WATCH_EVENTS_CANNOT_CREATE_ACTIONABLE_PACKET"):
        build_event_alert_v1(
            event_id="event-1",
            run_id="event-run-1",
            day_utc=DAY,
            timestamp_utc=NOW,
            event_type="BREADTH_COLLAPSE",
            alert_level="WATCH",
            severity="LOW",
            confidence="LOW",
            event_packet_created=True,
        )


def test_actionable_requires_validity_gate_pass() -> None:
    with pytest.raises(ValueError, match="ACTIONABLE_EVENT_REQUIRES_VALIDITY_GATE_PASS"):
        build_event_alert_v1(
            event_id="event-1",
            run_id="event-run-1",
            day_utc=DAY,
            timestamp_utc=NOW,
            event_type="PANIC_EXHAUSTION",
            alert_level="ACTIONABLE",
            severity="HIGH",
            confidence="MEDIUM",
            validity_gate_status="BLOCKED",
        )

    alert = build_event_alert_v1(
        event_id="event-1",
        run_id="event-run-1",
        day_utc=DAY,
        timestamp_utc=NOW,
        event_type="PANIC_EXHAUSTION",
        alert_level="ACTIONABLE",
        severity="HIGH",
        confidence="MEDIUM",
        validity_gate_status="PASS",
        event_packet_created=True,
    )
    assert alert["alert_level"] == "ACTIONABLE"


@pytest.mark.parametrize(
    ("field", "blocker"),
    [
        ("valid_until", "MISSING_VALID_UNTIL"),
        ("stop_price", "MISSING_STOP_PRICE"),
        ("stop_logic", "MISSING_STOP_LOGIC"),
        ("max_entry_slippage", "MISSING_MAX_ENTRY_SLIPPAGE"),
        ("risk_per_trade", "MISSING_RISK_PER_TRADE"),
        ("quantity_or_sizing_guidance", "MISSING_QUANTITY_OR_SIZING_GUIDANCE"),
    ],
)
def test_event_validity_gate_blocks_missing_required_fields(field: str, blocker: str) -> None:
    packet = _packet(**{field: ""})
    gate = build_event_validity_gate_v1(gate_id="gate-1", event_packet=packet, evaluated_at_utc=NOW, current_price="500.10")

    assert gate["gate_status"] == "BLOCKED"
    assert blocker in gate["blockers"]


def test_extreme_execution_sensitivity_blocks() -> None:
    gate = build_event_validity_gate_v1(gate_id="gate-1", event_packet=_packet(execution_sensitivity="EXTREME"), evaluated_at_utc=NOW)

    assert gate["gate_status"] == "BLOCKED"
    assert "EXECUTION_SENSITIVITY_EXTREME_NOT_MANUAL_CAPTURE_SAFE" in gate["blockers"]


def test_stale_event_packet_blocks() -> None:
    gate = build_event_validity_gate_v1(
        gate_id="gate-1",
        event_packet=_packet(valid_until="2026-05-15T17:59:00Z"),
        evaluated_at_utc=NOW,
    )

    assert gate["gate_status"] == "BLOCKED"
    assert "EVENT_PACKET_STALE_BEYOND_VALID_UNTIL" in gate["blockers"]


def test_current_price_outside_max_slippage_blocks() -> None:
    gate = build_event_validity_gate_v1(gate_id="gate-1", event_packet=_packet(max_entry_slippage="10bps"), evaluated_at_utc=NOW, current_price="502.00")

    assert gate["gate_status"] == "BLOCKED"
    assert "CURRENT_PRICE_OUTSIDE_MAX_ENTRY_SLIPPAGE" in gate["blockers"]


def test_high_sensitivity_passes_only_with_warning() -> None:
    gate = build_event_validity_gate_v1(gate_id="gate-1", event_packet=_packet(execution_sensitivity="HIGH"), evaluated_at_utc=NOW, current_price="500.10")

    assert gate["gate_status"] == "PASS"
    assert "HIGH_EXECUTION_SENSITIVITY_REQUIRES_OPERATOR_SPEED_WARNING" in gate["warnings"]


def test_manual_receipt_can_reference_event_packet() -> None:
    receipt = build_manual_execution_receipt_v1(
        receipt_id="receipt-event-1",
        recommended_trade_id="event:event-1:SPY:BUY",
        actual_symbol="SPY",
        actual_side="BUY",
        actual_quantity=1,
        order_type="LMT",
        fill_price="500.10",
        fill_timestamp=NOW,
        stop_order_entered=True,
        stop_price="494.00",
        operator_notes="Manual event tactical entry.",
        deviations_from_recommendation=["event packet acted manually"],
        source_packet_type="EVENT_TACTICAL_PACKET",
        event_id="event-1",
        event_run_id="event-run-1",
        deviation_from_entry_reference_price="0.10",
        fill_before_valid_until=True,
        max_entry_slippage_respected=True,
    )

    validate_research_lab_artifact_v1(receipt)
    assert receipt["source_packet_type"] == "EVENT_TACTICAL_PACKET"
    assert receipt["event_id"] == "event-1"
    assert receipt["broker_submit_required"] is False


def test_outcome_ledger_can_attribute_event_packet_and_create_only_offline_tasks() -> None:
    ledger = build_outcome_ledger_v1(
        generated_at_utc=NOW,
        outcome_rows=[
            {
                "trade_id": "event:event-1:SPY:BUY",
                "source_packet_type": "EVENT_TACTICAL_PACKET",
                "event_id": "event-1",
                "event_type": "PANIC_EXHAUSTION",
                "execution_sensitivity": "HIGH",
                "validity_gate_status": "PASS",
                "valid_until_respected": False,
                "entry_slippage_respected": False,
                "hypothesis_id": "hyp-event-1",
                "outcome_status": "stopped_out",
                "failure_reason": "event overlap loss",
                "operator_deviation": "manual delay",
            }
        ],
    )
    queue = build_learning_tasks_from_outcomes_v1(generated_at_utc=NOW, outcome_ledger=ledger)

    validate_research_lab_artifact_v1(ledger)
    validate_research_lab_artifact_v1(queue)
    task_types = {row["task_type"] for row in queue["tasks"]}
    assert {"event_failure_review", "event_stale_entry_review", "event_false_positive_review", "event_overlap_review"}.issubset(task_types)
    assert all(row["output_expected"] == "experiment_result.v1" for row in queue["tasks"])
    assert ledger["runtime_mutation_allowed"] is False
    assert ledger["broker_submit_required"] is False


def test_event_clis_require_explicit_truth_root_and_do_not_touch_eod_or_broker(tmp_path: Path) -> None:
    rc = awareness_main(
        [
            "--truth_root",
            str(tmp_path),
            "--day_utc",
            DAY,
            "--event_id",
            "event-cli-1",
            "--event_type",
            "VOLATILITY_SPIKE",
            "--alert_level",
            "TACTICAL",
            "--tactical_review_requested",
        ]
    )
    assert rc == 0
    tactical_rc = tactical_main(
        [
            "--truth_root",
            str(tmp_path),
            "--day_utc",
            DAY,
            "--event_id",
            "event-cli-1",
            "--event_run_id",
            f"event_awareness_v1:{DAY}",
            "--symbol",
            "SPY",
            "--side",
            "BUY",
            "--entry_reference_price",
            "500",
            "--quantity_or_sizing_guidance",
            "1 share",
            "--stop_price",
            "494",
            "--stop_logic",
            "protective stop",
            "--risk_per_trade",
            "small",
            "--event_type",
            "VOLATILITY_SPIKE",
            "--edge_family",
            "VOLATILITY_SPIKE",
            "--execution_sensitivity",
            "MEDIUM",
            "--valid_until",
            "2026-05-15T19:00:00Z",
            "--max_entry_slippage",
            "50bps",
            "--invalidation_conditions",
            "fresh low",
            "--inclusion_reason",
            "CLI smoke",
        ]
    )
    assert tactical_rc == 0
    packet_path = next((tmp_path / "reports" / "event_tactical_packet_v1").rglob("event_tactical_packet.v1.json"))
    gate_rc = gate_main(["--truth_root", str(tmp_path), "--event_packet_json", str(packet_path), "--evaluated_at_utc", NOW, "--current_price", "500.10"])
    assert gate_rc == 0
    assert not (tmp_path / "reports" / "aegis_lite_eod_report_v1").exists()
    assert not (tmp_path / "broker").exists()
    assert not (tmp_path / "ib").exists()


def _validity(packet: dict[str, object], **overrides: object) -> dict[str, object]:
    gate = build_event_validity_gate_v1(gate_id="validity-1", event_packet=packet, evaluated_at_utc=NOW, current_price="500.10")
    gate.update(overrides)
    return gate


def test_no_validity_pass_means_no_trade_capture_alert() -> None:
    packet = _packet()
    validity = _validity(packet, gate_status="BLOCKED")
    gate = build_trade_capture_alert_gate_v1(gate_id="alert-gate-1", source_packet=packet, event_validity_gate=validity, evaluated_at_utc=NOW)

    validate_event_awareness_artifact_v1(gate)
    assert gate["email_sms_allowed"] is False
    assert gate["alert_gate_status"] == "BLOCKED"
    assert "EVENT_VALIDITY_GATE_NOT_PASS" in gate["blockers"]


@pytest.mark.parametrize(
    ("field", "blocker"),
    [
        ("valid_until", "MISSING_VALID_UNTIL"),
        ("risk_per_trade", "MISSING_RISK_PER_TRADE"),
        ("quantity_or_sizing_guidance", "MISSING_QUANTITY_OR_SIZING_GUIDANCE"),
    ],
)
def test_incomplete_trade_capture_packet_does_not_alert(field: str, blocker: str) -> None:
    packet = _packet(**{field: ""})
    gate = build_trade_capture_alert_gate_v1(gate_id="alert-gate-1", source_packet=packet, event_validity_gate=_validity(packet), evaluated_at_utc=NOW)

    assert gate["email_sms_allowed"] is False
    assert blocker in gate["blockers"]


def test_missing_stop_price_and_stop_logic_does_not_alert() -> None:
    packet = _packet(stop_price="", stop_logic="")
    gate = build_trade_capture_alert_gate_v1(gate_id="alert-gate-1", source_packet=packet, event_validity_gate=_validity(packet), evaluated_at_utc=NOW)

    assert gate["email_sms_allowed"] is False
    assert "MISSING_STOP_PRICE_OR_STOP_LOGIC" in gate["blockers"]


def test_expired_and_extreme_packets_do_not_alert() -> None:
    expired = _packet(valid_until="2026-05-15T17:59:00Z")
    expired_gate = build_trade_capture_alert_gate_v1(gate_id="alert-gate-expired", source_packet=expired, event_validity_gate=_validity(expired, gate_status="PASS"), evaluated_at_utc=NOW)
    extreme = _packet(execution_sensitivity="EXTREME")
    extreme_gate = build_trade_capture_alert_gate_v1(gate_id="alert-gate-extreme", source_packet=extreme, event_validity_gate=_validity(extreme, gate_status="PASS"), evaluated_at_utc=NOW)

    assert expired_gate["alert_gate_status"] == "EXPIRED"
    assert expired_gate["email_sms_allowed"] is False
    assert extreme_gate["email_sms_allowed"] is False
    assert "EXECUTION_SENSITIVITY_EXTREME_NOT_ALERTABLE" in extreme_gate["blockers"]


@pytest.mark.parametrize(
    ("sensitivity", "valid_until", "allowed"),
    [
        ("LOW", "2026-05-15T18:29:59Z", False),
        ("LOW", "2026-05-15T18:30:00Z", True),
        ("MEDIUM", "2026-05-15T18:14:59Z", False),
        ("MEDIUM", "2026-05-15T18:15:00Z", True),
        ("HIGH", "2026-05-15T18:04:59Z", False),
        ("HIGH", "2026-05-15T18:05:00Z", True),
    ],
)
def test_trade_capture_alert_time_remaining_rules(sensitivity: str, valid_until: str, allowed: bool) -> None:
    packet = _packet(execution_sensitivity=sensitivity, valid_until=valid_until)
    gate = build_trade_capture_alert_gate_v1(gate_id="alert-gate-1", source_packet=packet, event_validity_gate=_validity(packet), evaluated_at_utc=NOW)

    assert gate["email_sms_allowed"] is allowed
    if not allowed:
        assert gate["alert_gate_status"] == "MISSED_VALIDITY_WINDOW"
        assert "INSUFFICIENT_TIME_REMAINING" in gate["blockers"]
    if allowed and sensitivity == "HIGH":
        assert gate["alert_gate_status"] == "URGENT_ACTIONABLE_TRADE"


def test_duplicate_packet_suppresses_second_sms() -> None:
    packet = _packet()
    first_gate = build_trade_capture_alert_gate_v1(gate_id="alert-gate-1", source_packet=packet, event_validity_gate=_validity(packet), evaluated_at_utc=NOW)
    ledger = build_trade_capture_alert_ledger_v1(run_id="alert-run-1", day_utc=DAY, generated_at_utc=NOW, alert_gates=[first_gate])
    second_gate = build_trade_capture_alert_gate_v1(
        gate_id="alert-gate-2",
        source_packet=packet,
        event_validity_gate=_validity(packet),
        evaluated_at_utc=NOW,
        prior_alert_attempts=ledger["alert_attempts"],
    )

    validate_event_awareness_artifact_v1(ledger)
    assert first_gate["email_sms_allowed"] is True
    assert second_gate["email_sms_allowed"] is False
    assert second_gate["duplicate_suppressed"] is True
    assert "DUPLICATE_ALERT_SUPPRESSED" in second_gate["blockers"]


def test_alert_ledger_records_blocked_no_alert_reason() -> None:
    packet = _packet(valid_until="")
    gate = build_trade_capture_alert_gate_v1(gate_id="alert-gate-1", source_packet=packet, event_validity_gate=_validity(packet), evaluated_at_utc=NOW)
    ledger = build_trade_capture_alert_ledger_v1(run_id="alert-run-1", day_utc=DAY, generated_at_utc=NOW, alert_gates=[gate])

    validate_event_awareness_artifact_v1(ledger)
    attempt = ledger["alert_attempts"][0]
    assert attempt["delivery_status"] == "NOT_SENT"
    assert "MISSING_VALID_UNTIL" in attempt["no_alert_reason"]


def test_manual_receipt_can_reference_alert_id() -> None:
    receipt = build_manual_execution_receipt_v1(
        receipt_id="receipt-alert-1",
        recommended_trade_id="event:event-1:SPY:BUY",
        actual_symbol="SPY",
        actual_side="BUY",
        actual_quantity=1,
        order_type="LMT",
        fill_price="500.10",
        fill_timestamp=NOW,
        stop_order_entered=True,
        stop_price="494.00",
        operator_notes="Manual alert-linked entry.",
        source_packet_type="EVENT_TACTICAL_PACKET",
        alert_id="alert:event_event-1_SPY_BUY",
        source_packet_id="event:event-1:SPY:BUY",
        event_id="event-1",
        event_run_id="event-run-1",
        fill_timestamp_utc=NOW,
        fill_before_valid_until=True,
        max_entry_slippage_respected=True,
    )

    validate_research_lab_artifact_v1(receipt)
    assert receipt["alert_id"] == "alert:event_event-1_SPY_BUY"
    assert receipt["source_packet_id"] == "event:event-1:SPY:BUY"


def test_outcome_ledger_can_attribute_alert_usefulness() -> None:
    ledger = build_outcome_ledger_v1(
        generated_at_utc=NOW,
        outcome_rows=[
            {
                "trade_id": "event:event-1:SPY:BUY",
                "alert_id": "alert:event_event-1_SPY_BUY",
                "source_packet_type": "EVENT_TACTICAL_PACKET",
                "event_id": "event-1",
                "event_type": "PANIC_EXHAUSTION",
                "alert_gate_status": "ACTIONABLE_TRADE",
                "execution_sensitivity": "MEDIUM",
                "valid_until_respected": True,
                "entry_slippage_respected": True,
                "operator_action_taken": "ENTERED",
                "hypothesis_id": "hyp-event-1",
                "outcome_status": "win",
            }
        ],
    )
    queue = build_learning_tasks_from_outcomes_v1(generated_at_utc=NOW, outcome_ledger=ledger)

    assert ledger["outcomes"][0]["alert_id"] == "alert:event_event-1_SPY_BUY"
    assert ledger["outcomes"][0]["alert_gate_status"] == "ACTIONABLE_TRADE"
    assert ledger["outcomes"][0]["operator_action_taken"] == "ENTERED"
    assert {row["task_type"] for row in queue["tasks"]} == {"event_success_review"}


def test_trade_capture_alert_cli_does_not_touch_eod_or_broker(tmp_path: Path) -> None:
    packet = _packet()
    packet_path = tmp_path / "event_tactical_packet.v1.json"
    packet_path.write_text(json.dumps(packet), encoding="utf-8")
    validity = _validity(packet)
    validity_path = tmp_path / "event_validity_gate.v1.json"
    validity_path.write_text(json.dumps(validity), encoding="utf-8")

    rc = alert_gate_main(
        [
            "--truth_root",
            str(tmp_path),
            "--event_packet_json",
            str(packet_path),
            "--event_validity_gate_json",
            str(validity_path),
            "--evaluated_at_utc",
            NOW,
            "--alert_channel",
            "SMS",
        ]
    )

    assert rc == 0
    assert list((tmp_path / "reports" / "trade_capture_alert_ledger_v1").rglob("trade_capture_alert_ledger.v1.json"))
    assert not (tmp_path / "reports" / "aegis_lite_eod_report_v1").exists()
    assert not (tmp_path / "broker").exists()
    assert not (tmp_path / "ib").exists()
