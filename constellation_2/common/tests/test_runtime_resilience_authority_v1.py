from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_runtime_resilience_authority_v1 as runtime_module
from ops.tools.run_intent_lifecycle_state_v1 import build_intent_lifecycle_state_v1
from ops.tools.run_runtime_resilience_authority_v1 import build_runtime_resilience_authority_v1
from ops.tools.run_submit_boundary_status_v1 import _runtime_resilience_boundary_check_v1

DAY = "2026-05-01"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row, sort_keys=True, separators=(",", ":")) for row in rows) + "\n", encoding="utf-8")


def _patch_probe_config(monkeypatch, truth: Path, execution: Path) -> None:
    config = runtime_module.probe.ProbeConfig(
        day_utc=DAY,
        environment="PAPER",
        expected_account="DU123",
        host="127.0.0.1",
        port=4002,
        observer_client_id=7,
        probe_client_id=8,
        broker_event_log_path=execution / "execution_evidence_v1" / "broker_events" / DAY / "broker_event_log.v1.jsonl",
        timeout_seconds=12.0,
        freshness_seconds=300.0,
        request_open_orders=False,
    )
    monkeypatch.setattr(runtime_module.probe, "_resolve_config", lambda *_args, **_kwargs: (config, truth))


def _intraday_readiness(truth: Path, execution: Path, *, submit_allowed: bool = True) -> None:
    _write_json(
        truth / "reports" / "trading_day_readiness_authority_v1" / DAY / "trading_day_readiness_authority.v1.json",
        {
            "schema_id": "C2_TRADING_DAY_READINESS_AUTHORITY_V1",
            "target_day": DAY,
            "day_utc": DAY,
            "readiness_mode": "INTRADAY_SUBMIT_READY" if submit_allowed else "PREOPEN_BUILD",
            "requires_same_day_broker_event_log": submit_allowed,
            "submit_allowed_by_mode": submit_allowed,
            "canonical_blocker": "" if submit_allowed else "SUBMIT_NOT_ALLOWED_BY_TRADING_DAY_MODE",
            "evidence_policy": {},
            "allowed_carry_forward_sources": ["broker_event_log"] if not submit_allowed else [],
            "artifact_paths": {
                "same_day_broker_event_log": str(execution / "execution_evidence_v1" / "broker_events" / DAY / "broker_event_log.v1.jsonl")
            },
        },
    )


def _healthy_runtime_inputs(truth: Path, execution: Path, *, event_day: str = DAY, connected: bool = True) -> None:
    _write_json(
        truth / "reports" / "ib_broker_event_probe_v1" / DAY / "ib_broker_event_probe.v1.json",
        {"status": "PASS" if connected else "BLOCKED", "connection": {"connected": connected}, "expected_host": "127.0.0.1", "expected_port": 4002, "execution_observer_client_id": 7},
    )
    _write_json(
        truth / "reports" / "broker_supply_v1" / DAY / "broker_supply.v1.json",
        {
            "status": "PASS",
            "account": "DU123",
            "generated_at_utc": f"{DAY}T14:00:00Z",
            "account_values": {"net_liquidation_cents": 100000, "total_cash_value_cents": 90000},
        },
    )
    _write_json(truth / "reports" / "capital_supply_v1" / DAY / "capital_supply.v1.json", {"status": "PASS"})
    _write_json(truth / "reports" / "runtime_service_authority_v1" / DAY / "runtime_service_authority.v1.json", {"status": "PASS", "service_state": "MANUAL_MODE_READY", "produced_utc": f"{DAY}T14:00:00Z"})
    _write_json(truth / "reports" / "market_data_authority_v1" / DAY / "market_data_authority.v1.json", {"status": "PASS", "produced_at_utc": f"{DAY}T14:00:00Z"})
    _write_json(truth / "reports" / "position_lifecycle_state_v1" / DAY / "position_lifecycle_state.v1.json", {"status": "PASS", "counts": {"pending_order_count": 0, "open_position_count": 0}, "rows": []})
    _write_json(execution / "submission_index_v1" / DAY / "submission_index.v1.json", {"status": "PASS", "attempts": []})
    _write_json(execution / "execution_evidence_v1" / "current_head" / DAY / "current_head.v1.json", {"status": "PASS"})
    _write_jsonl(
        execution / "execution_evidence_v1" / "broker_events" / DAY / "broker_event_log.v1.jsonl",
        [
            {"event_type": "nextValidId", "received_utc": f"{event_day}T14:00:00Z"},
            {"event_type": "accountSummary", "received_utc": f"{event_day}T14:00:01Z"},
            {"event_type": "positionEnd", "received_utc": f"{event_day}T14:00:02Z"},
        ],
    )


def _build(tmp_path: Path, monkeypatch, *, submit_allowed: bool = False, event_day: str = DAY, connected: bool = True) -> tuple[Path, Path, Path, dict]:
    truth = tmp_path / "truth"
    execution = tmp_path / "execution"
    runtime = tmp_path / "runtime"
    _patch_probe_config(monkeypatch, truth, execution)
    _intraday_readiness(truth, execution, submit_allowed=submit_allowed)
    _healthy_runtime_inputs(truth, execution, event_day=event_day, connected=connected)
    payload = build_runtime_resilience_authority_v1(day_utc=DAY, truth_root=truth, execution_root=execution, runtime_root=runtime, environment="PAPER", broker_account="DU123")
    return truth, execution, runtime, payload


def test_socket_connected_missing_account_summary_is_degraded(tmp_path: Path, monkeypatch) -> None:
    truth, execution, runtime, _ = _build(tmp_path, monkeypatch)
    _write_json(truth / "reports" / "broker_supply_v1" / DAY / "broker_supply.v1.json", {"status": "PASS", "account_values": {}})

    payload = build_runtime_resilience_authority_v1(day_utc=DAY, truth_root=truth, execution_root=execution, runtime_root=runtime, environment="PAPER", broker_account="DU123")

    assert payload["ib_connection_state"] == "STALE"
    assert payload["account_summary_state"] in {"MISSING", "DEGRADED"}
    assert payload["status"] == "DEGRADED"
    assert payload["canonical_blocker"] == "ACCOUNT_SUMMARY_MISSING"


def test_disconnected_ib_blocks(tmp_path: Path, monkeypatch) -> None:
    _truth, _execution, _runtime, payload = _build(tmp_path, monkeypatch, connected=False)

    assert payload["ib_connection_state"] == "DISCONNECTED"
    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "IB_DISCONNECTED"


def test_stale_broker_event_log_blocks_intraday_submit(tmp_path: Path, monkeypatch) -> None:
    _truth, _execution, _runtime, payload = _build(tmp_path, monkeypatch, submit_allowed=True, event_day="2026-04-30")

    assert payload["broker_event_log_state"] == "STALE"
    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "BROKER_EVENT_LOG_STALE"


def test_reconnecting_state_blocks_submit(tmp_path: Path, monkeypatch) -> None:
    truth, execution, runtime, _ = _build(tmp_path, monkeypatch)
    _write_json(truth / "reports" / "runtime_service_authority_v1" / DAY / "runtime_service_authority.v1.json", {"status": "RECONNECTING", "service_state": "RECONNECTING"})

    payload = build_runtime_resilience_authority_v1(day_utc=DAY, truth_root=truth, execution_root=execution, runtime_root=runtime, environment="PAPER", broker_account="DU123")

    assert payload["ib_connection_state"] == "RECONNECTING"
    assert payload["canonical_blocker"] == "IB_RECONNECTING"
    assert payload["submit_blocked_during_recovery"] is True


def test_restart_detected_requires_recovery(tmp_path: Path, monkeypatch) -> None:
    truth, execution, runtime, _ = _build(tmp_path, monkeypatch)
    _write_json(runtime / "process_state" / "supervisor_state.json", {"restart_detected": True})

    payload = build_runtime_resilience_authority_v1(day_utc=DAY, truth_root=truth, execution_root=execution, runtime_root=runtime, environment="PAPER", broker_account="DU123")

    assert payload["restart_detected"] is True
    assert payload["recovery_status"] in {"PASS", "BLOCKED", "IN_PROGRESS"}
    assert "RESTART_RECOVERY_REQUIRED" in payload["reason_codes"]


def _active_outcome() -> dict:
    return {
        "status": "INTENT_CREATED",
        "sleeve_id": "SLEEVE_A",
        "engine_id": "SLEEVE_A",
        "output_intents": [{"intent_id": "intent_a", "symbol": "IWM", "exposure_type": "LONG_EQUITY"}],
        "signal_state": {"state": "ACTIVE"},
    }


def test_pending_order_after_restart_suppresses_reentry(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _write_json(
        truth / "reports" / "position_lifecycle_state_v1" / DAY / "position_lifecycle_state.v1.json",
        {"status": "PASS", "rows": [{"intent_id": "intent_a", "sleeve_id": "SLEEVE_A", "symbol": "IWM", "lifecycle_state": "ORDER_PENDING"}]},
    )

    payload = build_intent_lifecycle_state_v1(day_utc=DAY, truth_root=truth, intent_truth_root=truth, outcomes=[_active_outcome()], previous_by_engine={"SLEEVE_A": {"current_status": "INTENT_CREATED", "signal_state": {"state": "ACTIVE"}, "intent_signature": [{"intent_id": "intent_a", "symbol": "IWM"}]}})

    row = payload["rows"][0]
    assert row["lifecycle_decision"] == "NO_INTENT"
    assert "ORDER_ALREADY_PENDING" in row["lifecycle_reason_codes"]


def test_open_position_after_restart_suppresses_reentry(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _write_json(
        truth / "reports" / "position_lifecycle_state_v1" / DAY / "position_lifecycle_state.v1.json",
        {"status": "PASS", "rows": [{"intent_id": "intent_a", "sleeve_id": "SLEEVE_A", "symbol": "IWM", "lifecycle_state": "POSITION_OPEN", "quantity_open": 1}]},
    )

    payload = build_intent_lifecycle_state_v1(day_utc=DAY, truth_root=truth, intent_truth_root=truth, outcomes=[_active_outcome()], previous_by_engine={"SLEEVE_A": {"current_status": "INTENT_CREATED", "signal_state": {"state": "ACTIVE"}, "intent_signature": [{"intent_id": "intent_a", "symbol": "IWM"}]}})

    row = payload["rows"][0]
    assert row["lifecycle_decision"] == "NO_INTENT"
    assert "POSITION_ALREADY_OPEN" in row["lifecycle_reason_codes"]


def test_unknown_position_or_order_truth_blocks_or_degrades(tmp_path: Path, monkeypatch) -> None:
    truth, execution, runtime, _ = _build(tmp_path, monkeypatch)
    _write_json(
        truth / "reports" / "position_lifecycle_state_v1" / DAY / "position_lifecycle_state.v1.json",
        {"status": "FAIL", "rows": [{"symbol": "IWM", "lifecycle_state": "UNKNOWN", "reason_codes": ["POSITION_STATE_STALE"]}]},
    )

    payload = build_runtime_resilience_authority_v1(day_utc=DAY, truth_root=truth, execution_root=execution, runtime_root=runtime, environment="PAPER", broker_account="DU123")

    assert payload["status"] in {"BLOCKED", "DEGRADED"}
    assert payload["canonical_blocker"] == "POSITION_OR_ORDER_TRUTH_UNKNOWN"


def test_submit_boundary_consumes_runtime_resilience_blocker() -> None:
    ok, codes, blocker = _runtime_resilience_boundary_check_v1(
        payload={
            "status": "BLOCKED",
            "canonical_blocker": "IB_DISCONNECTED",
            "ib_connection_state": "DISCONNECTED",
            "recovery_status": "NOT_REQUIRED",
            "pending_orders_reconciled": True,
            "open_positions_reconciled": True,
            "submit_blocked_during_recovery": True,
            "reason_codes": ["IB_DISCONNECTED"],
        }
    )

    assert ok is False
    assert blocker == "IB_DISCONNECTED"
    assert "IB_DISCONNECTED" in codes


def test_preopen_carry_forward_does_not_permit_submit(tmp_path: Path, monkeypatch) -> None:
    _truth, _execution, _runtime, payload = _build(tmp_path, monkeypatch, submit_allowed=False, event_day="2026-04-30")

    assert payload["readiness_mode"] == "PREOPEN_BUILD"
    assert payload["broker_event_log_state"] == "PRESENT"
    assert payload["status"] in {"PASS", "DEGRADED"}
    assert payload["canonical_blocker"] != "BROKER_EVENT_LOG_MISSING"
