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

from constellation_2.common.aegis_event_monitoring_v1 import (  # noqa: E402
    DEFAULT_EVENT_RULES_REGISTRY_PATH,
    build_event_monitoring_operator_surface_v1,
    build_tactical_review_gate_v1,
    load_event_rules_registry_v1,
    run_event_monitor_v1,
)
from constellation_2.common.aegis_lite_event_awareness_v1 import (  # noqa: E402
    EVENT_TYPES,
    build_event_tactical_packet_v1,
    build_event_validity_gate_v1,
    build_trade_capture_alert_gate_v1,
    validate_event_awareness_artifact_v1,
)
from ops.tools.run_aegis_event_monitor_v1 import main as monitor_main  # noqa: E402


DAY = "2026-05-15"
NOW = "2026-05-15T19:00:00Z"


def _snapshot(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "generated_at_utc": "2026-05-15T18:45:00Z",
        "data_snapshot_refs": ["fixture://event-snapshot"],
        "symbols": ["SPY"],
        "assets_affected": ["SPY", "QQQ"],
        "current_prices": {"SPY": "500.10"},
        "promoted_sleeves": [{"event_type": "PANIC_EXHAUSTION", "sleeve_id": "panic-demo"}],
        "inputs": {
            "index_return_pct": "-2.0",
            "vix_change_pct": "10.0",
            "breadth_down_pct": "80.0",
            "late_session_stabilization": True,
        },
        "tactical_packet": {
            "symbol": "SPY",
            "side": "BUY",
            "instrument_type": "ETF",
            "entry_reference_price": "500.00",
            "quantity_or_sizing_guidance": "1 share supervised dry-run",
            "stop_price": "494.00",
            "stop_logic": "protective stop below event low",
            "risk_per_trade": "6.00",
            "execution_sensitivity": "MEDIUM",
            "valid_until": "2026-05-15T19:45:00Z",
            "max_entry_slippage": "50bps",
            "invalidation_conditions": ["new intraday low"],
            "inclusion_reason": "panic exhaustion rule passed",
        },
    }
    payload.update(overrides)
    return payload


def test_event_rule_exists_for_every_monitored_event_type() -> None:
    registry = load_event_rules_registry_v1(DEFAULT_EVENT_RULES_REGISTRY_PATH)
    assert {row["event_type"] for row in registry["event_rules"]} == EVENT_TYPES
    validate_event_awareness_artifact_v1(registry)


def test_missing_event_rule_fails_closed(tmp_path: Path) -> None:
    registry = load_event_rules_registry_v1(DEFAULT_EVENT_RULES_REGISTRY_PATH)
    registry["event_rules"] = [row for row in registry["event_rules"] if row["event_type"] != "VOLATILITY_SPIKE"]
    registry["canonical_json_hash"] = "0" * 64
    path = tmp_path / "registry.json"
    path.write_text(json.dumps(registry, sort_keys=True), encoding="utf-8")

    with pytest.raises(ValueError, match="EVENT_RULES_REGISTRY_MISSING_EVENT_TYPES:VOLATILITY_SPIKE"):
        load_event_rules_registry_v1(path)


def test_disabled_event_rule_does_not_trigger(tmp_path: Path) -> None:
    registry = load_event_rules_registry_v1(DEFAULT_EVENT_RULES_REGISTRY_PATH)
    for row in registry["event_rules"]:
        if row["event_type"] == "PANIC_EXHAUSTION":
            row["enabled_status"] = "DISABLED"
    registry["canonical_json_hash"] = "0" * 64
    registry_path = tmp_path / "registry.json"
    registry_path.write_text(json.dumps(registry, sort_keys=True), encoding="utf-8")

    result = run_event_monitor_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        market_snapshot=_snapshot(),
        event_rules_registry_path=registry_path,
        monitor_run_id="monitor-disabled",
        timestamp_utc=NOW,
    )

    assert result["monitoring_status"]["triggered_events"] == []
    assert "event_rule:PANIC_EXHAUSTION:v1" in result["monitoring_status"]["blocked_events"]
    assert not (tmp_path / "reports" / "aegis_lite_eod_report_v1").exists()


def test_research_only_rule_cannot_generate_actionable_alert(tmp_path: Path) -> None:
    registry = load_event_rules_registry_v1(DEFAULT_EVENT_RULES_REGISTRY_PATH)
    for row in registry["event_rules"]:
        if row["event_type"] == "PANIC_EXHAUSTION":
            row["production_status"] = "RESEARCH_ONLY"
            row["research_status"] = "RESEARCH_ONLY"
    registry["canonical_json_hash"] = "0" * 64
    registry_path = tmp_path / "registry.json"
    registry_path.write_text(json.dumps(registry, sort_keys=True), encoding="utf-8")

    result = run_event_monitor_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        market_snapshot=_snapshot(),
        event_rules_registry_path=registry_path,
        monitor_run_id="monitor-research-only",
        timestamp_utc=NOW,
    )

    ledger_events = result["event_awareness_ledger"]["events"]
    panic = [row for row in ledger_events if row["event_type"] == "PANIC_EXHAUSTION"][0]
    assert panic["alert_level"] == "BLOCKED"
    assert panic["alert_gate_status"] == "NOT_RUN"


def test_stale_data_blocks_event_evaluation(tmp_path: Path) -> None:
    result = run_event_monitor_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        market_snapshot=_snapshot(generated_at_utc="2026-05-15T16:00:00Z"),
        monitor_run_id="monitor-stale",
        timestamp_utc=NOW,
    )

    panic = [row for row in result["event_awareness_ledger"]["events"] if row["event_type"] == "PANIC_EXHAUSTION"][0]
    assert panic["alert_level"] == "BLOCKED"
    assert panic["stale_data_status"] == "STALE"
    assert "event_rule:PANIC_EXHAUSTION:v1:STALE_DATA" in result["monitoring_status"]["pass_fail_reason_codes"]


def test_event_monitor_creates_lineaged_packet_gate_and_dry_run_email(tmp_path: Path) -> None:
    result = run_event_monitor_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        market_snapshot=_snapshot(),
        monitor_run_id="monitor-pass",
        timestamp_utc=NOW,
    )

    status = result["monitoring_status"]
    ledger = result["event_awareness_ledger"]
    assert status["triggered_events"]
    assert status["tactical_packets_created"]
    assert status["alert_gate_results"][0].endswith(":ACTIONABLE_TRADE")
    assert status["email_delivery_results"][0].endswith(":DRY_RUN_MESSAGE_BODY_ONLY")
    event = [row for row in ledger["events"] if row["event_type"] == "PANIC_EXHAUSTION"][0]
    assert event["event_rule_id"] == "event_rule:PANIC_EXHAUSTION:v1"
    assert event["event_rule_version"] == "v1"
    assert event["validity_gate_status"] == "PASS"
    assert event["alert_gate_status"] == "ACTIONABLE_TRADE"
    assert event["canonical_eod_state_mutated"] is False
    assert result["broker_submit_required"] is False


def test_tactical_review_gate_blocks_low_confidence() -> None:
    rule = [row for row in load_event_rules_registry_v1()["event_rules"] if row["event_type"] == "PANIC_EXHAUSTION"][0]
    gate = build_tactical_review_gate_v1(
        gate_id="gate-low",
        event_id="event-low",
        event_rule=rule,
        event_run_id="run",
        day_utc=DAY,
        evaluated_at_utc=NOW,
        severity="LOW",
        confidence="LOW",
        required_inputs_present=True,
        stale_data_status="FRESH",
        promoted_sleeve_available=True,
    )

    validate_event_awareness_artifact_v1(gate)
    assert gate["gate_status"] == "LOW_CONFIDENCE"
    assert gate["tactical_review_allowed"] is False


def test_validity_gate_blocks_research_only_or_disabled_packet() -> None:
    packet = build_event_tactical_packet_v1(
        event_id="event-1",
        event_run_id="run-1",
        day_utc=DAY,
        symbol="SPY",
        side="BUY",
        instrument_type="ETF",
        entry_reference_price="500.00",
        order_type_suggestion="MANUAL_LIMIT",
        quantity_or_sizing_guidance="1 share",
        stop_price="494.00",
        stop_logic="protective stop",
        risk_per_trade="6.00",
        event_type="PANIC_EXHAUSTION",
        edge_family="PANIC_EXHAUSTION",
        regime_state="PANIC",
        confidence="MEDIUM",
        execution_sensitivity="MEDIUM",
        valid_until="2026-05-15T19:45:00Z",
        max_entry_slippage="50bps",
        invalidation_conditions=["new low"],
        inclusion_reason="test",
        production_status="RESEARCH_ONLY",
    )
    gate = build_event_validity_gate_v1(gate_id="validity-1", event_packet=packet, evaluated_at_utc=NOW)

    assert gate["gate_status"] == "BLOCKED"
    assert "RESEARCH_ONLY_EVENT_NOT_ACTIONABLE" in gate["blockers"]


def test_alert_gate_blocks_non_actionable_event() -> None:
    packet = build_event_tactical_packet_v1(
        event_id="event-1",
        event_run_id="run-1",
        day_utc=DAY,
        symbol="SPY",
        side="BUY",
        instrument_type="ETF",
        entry_reference_price="500.00",
        order_type_suggestion="MANUAL_LIMIT",
        quantity_or_sizing_guidance="1 share",
        stop_price="494.00",
        stop_logic="protective stop",
        risk_per_trade="6.00",
        event_type="PANIC_EXHAUSTION",
        edge_family="PANIC_EXHAUSTION",
        regime_state="PANIC",
        confidence="MEDIUM",
        execution_sensitivity="MEDIUM",
        valid_until="2026-05-15T19:45:00Z",
        max_entry_slippage="50bps",
        invalidation_conditions=["new low"],
        inclusion_reason="test",
    )
    gate = build_trade_capture_alert_gate_v1(
        gate_id="alert-1",
        source_packet=packet,
        event_validity_gate={"gate_status": "BLOCKED", "event_id": "event-1", "event_run_id": "run-1", "day_utc": DAY},
        evaluated_at_utc=NOW,
        alert_channel="EMAIL",
    )

    assert gate["alert_gate_status"] == "BLOCKED"
    assert gate["email_sms_allowed"] is False


def test_event_operator_surface_exposes_rules_ledger_and_packets(tmp_path: Path) -> None:
    run_event_monitor_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        market_snapshot=_snapshot(),
        monitor_run_id="monitor-surface",
        timestamp_utc=NOW,
    )
    surface = build_event_monitoring_operator_surface_v1(truth_root=tmp_path, day_utc=DAY)

    assert surface["event_rules"]
    assert surface["event_ledger"]["events"]
    assert surface["actionable_packets"]
    assert surface["blocked_packets"] == []
    assert surface["email_transport_status"] == "GATE_ONLY_NO_TRANSPORT"
    assert surface["broker_submit_required"] is False


def test_event_tactical_packet_demo_only_and_dry_run_only_are_blocked() -> None:
    for runtime_class, blocker in [
        ("DEMO_ONLY", "DEMO_ONLY_EVENT_PACKET_NOT_ACTIONABLE"),
        ("DRY_RUN_ONLY", "DRY_RUN_ONLY_EVENT_PACKET_NOT_ACTIONABLE"),
    ]:
        packet = build_event_tactical_packet_v1(
            event_id=f"event-{runtime_class}",
            event_run_id="run-1",
            day_utc=DAY,
            symbol="SPY",
            side="BUY",
            instrument_type="ETF",
            entry_reference_price="500.00",
            order_type_suggestion="MANUAL_LIMIT",
            quantity_or_sizing_guidance="1 share",
            stop_price="494.00",
            stop_logic="protective stop",
            risk_per_trade="6.00",
            event_type="PANIC_EXHAUSTION",
            edge_family="PANIC_EXHAUSTION",
            regime_state="PANIC",
            confidence="MEDIUM",
            execution_sensitivity="MEDIUM",
            valid_until="2026-05-15T19:45:00Z",
            max_entry_slippage="50bps",
            invalidation_conditions=["new low"],
            inclusion_reason="test",
            runtime_truth_classification=runtime_class,
        )
        gate = build_event_validity_gate_v1(gate_id=f"validity-{runtime_class}", event_packet=packet, evaluated_at_utc=NOW)
        alert_gate = build_trade_capture_alert_gate_v1(
            gate_id=f"alert-{runtime_class}",
            source_packet=packet,
            event_validity_gate=gate,
            evaluated_at_utc=NOW,
            alert_channel="EMAIL",
        )

        assert packet["runtime_truth_classification"] == runtime_class
        assert gate["gate_status"] == "BLOCKED"
        assert blocker in gate["blockers"]
        assert alert_gate["email_sms_allowed"] is False
        assert alert_gate["alert_gate_status"] == "BLOCKED"


def test_event_operator_surface_classifies_non_actionable_packets(tmp_path: Path) -> None:
    run_event_monitor_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        market_snapshot=_snapshot(runtime_truth_classification="DEMO_ONLY"),
        monitor_run_id="monitor-demo",
        timestamp_utc=NOW,
    )

    surface = build_event_monitoring_operator_surface_v1(truth_root=tmp_path, day_utc=DAY)

    assert surface["actionable_packets"] == []
    assert surface["blocked_packets"]
    assert surface["blocked_packets"][0]["runtime_truth_classification"] == "DEMO_ONLY"
    assert "DEMO_ONLY_EVENT_PACKET_NOT_ACTIONABLE" in surface["blocked_packets"][0]["validity_gate_blockers"]


def test_event_monitor_cli_smoke(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    snapshot_path = tmp_path / "snapshot.json"
    snapshot_path.write_text(json.dumps(_snapshot(), sort_keys=True), encoding="utf-8")

    rc = monitor_main(
        [
            "--truth_root",
            str(tmp_path),
            "--day_utc",
            DAY,
            "--market_snapshot_json",
            str(snapshot_path),
            "--monitor_run_id",
            "monitor-cli",
            "--timestamp_utc",
            NOW,
        ]
    )

    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["broker_submit_required"] is False
    assert out["canonical_eod_state_mutated"] is False
    assert Path(out["monitoring_status_path"]).exists()
