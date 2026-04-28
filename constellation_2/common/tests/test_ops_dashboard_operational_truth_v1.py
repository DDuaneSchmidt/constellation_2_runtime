from __future__ import annotations

import json
import importlib.util
import sys
import types
from pathlib import Path


def _load_server_module():
    stub_c2 = types.ModuleType("stub_c2_ops_status_v2")
    stub_c2.build_status_v2 = lambda *args, **kwargs: {}
    stub_c2.discover_attempts = lambda *args, **kwargs: ([], [], [], {}, [])
    stub_c2.select_preferred_attempt = lambda *args, **kwargs: None
    sys.modules["constellation_2.phaseL.ui.server.c2_ops_cockpit_status_v2_collector_v1"] = stub_c2

    stub_c3 = types.ModuleType("stub_c3_ui_status")
    stub_c3.build_c3_ui_status = lambda *args, **kwargs: {}
    sys.modules["constellation_2.phaseL.ui.server.c3_ui_status_collector_v1"] = stub_c3

    server_path = Path(__file__).resolve().parents[3] / "constellation_2" / "phaseL" / "ui" / "server" / "run_ops_dashboard_v1.py"
    spec = importlib.util.spec_from_file_location("ops_dashboard_runtime_test_module", server_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


SERVER = _load_server_module()
build_operational_truth_v1 = SERVER.build_operational_truth_v1


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _write_ready_surfaces(truth_root: Path, day: str) -> None:
    _write_json(truth_root / "target_day_build_v1" / f"{day}.json", {"build_status": "COMPLETE"})
    _write_json(truth_root / "target_day_admission_v1" / f"{day}.json", {"admission_status": "ADMIT"})
    _write_json(
        truth_root / "reports" / "submit_boundary_status_v1" / day / "submit_boundary_status.v1.json",
        {"boundary_status": "AUTHORIZED", "submission_authorized": True},
    )
    _write_json(
        truth_root / "reports" / "paper_session_ledger_v1" / day / "paper_session_ledger.v1.json",
        {"control_state": {"authority_status": "GRANTED"}, "evidence_status": "READY"},
    )
    _write_json(
        truth_root / "reports" / "paper_day_control_plane_v1" / day / "paper_day_control_plane.v1.json",
        {"final_start_decision": "READY_NOW"},
    )
    _write_json(
        truth_root / "session_authority_status_v1" / "current.json",
        {
            "submission_authorization_status": "AUTHORIZED",
            "traceability_status": "VALID",
            "monitoring_checks": [{"check_name": "canonical_readiness_authority", "status": "PASS", "summary": "aligned"}],
            "top_blocker_reason_codes": [],
        },
    )
    _write_json(truth_root / "reports" / "execution_reconciliation_v1" / day / "execution_reconciliation.v1.json", {"status": "PASS", "reason_codes": []})


def _write_order_surfaces(
    truth_root: Path,
    day: str,
    submission_id: str,
    *,
    broker_order_id: int = 75,
    perm_id: int = 1870974300,
    stream_status: str = "SUBMITTED",
    fill_status: str = "FILLED",
) -> None:
    stream_hash = "abc123"
    _write_json(
        truth_root / "execution_stream_v1" / day / "stream1.execution_event_stream_record.v1.json",
        {
            "submission_id": submission_id,
            "canonical_json_hash": stream_hash,
            "observed_at_utc": "2026-04-12T14:23:13Z",
            "event_time_utc": "2026-04-12T14:23:13Z",
            "broker_ids": {"order_id": broker_order_id, "perm_id": perm_id},
            "order_state": {"status": stream_status, "filled_qty": 1, "remaining_qty": 0},
            "reason_codes": [],
        },
    )
    _write_json(
        truth_root / "execution_evidence_v1" / "submissions" / day / submission_id / "broker_submission_record.v2.json",
        {
            "status": stream_status,
            "submission_id": submission_id,
            "submitted_at_utc": "2026-04-12T14:20:00Z",
            "broker_ids": {"order_id": broker_order_id, "perm_id": perm_id},
        },
    )
    _write_json(
        truth_root / "execution_evidence_v1" / "submissions" / day / submission_id / "execution_event_record.v1.json",
        {
            "event_time_utc": "2026-04-12T14:23:13Z",
            "raw_broker_status": stream_status,
            "upstream_hash": stream_hash,
        },
    )
    _write_json(
        truth_root / "execution_evidence_v1" / "submissions" / day / submission_id / "equity_order_plan.v1.json",
        {
            "symbol": "SPY",
            "action": "BUY",
            "qty_shares": 1,
            "order_terms": {"order_type": "MARKET", "time_in_force": "DAY"},
        },
    )
    _write_json(
        truth_root / "fill_ledger_v1" / day / f"{submission_id}.fill_ledger.v1.json",
        {
            "filled_qty": 1,
            "remaining_qty": 0,
            "lifecycle_status": fill_status,
            "event_hashes": [stream_hash],
            "produced_utc": f"{day}T00:00:00Z",
        },
    )


def _write_lifecycle_authority(
    truth_root: Path,
    day: str,
    submission_id: str,
    *,
    state: str,
    broker_order_id: int = 75,
    perm_id: int = 1870974300,
) -> None:
    _write_json(
        truth_root / "reports" / "execution_lifecycle_authority_v1" / day / "execution_lifecycle_authority.v1.json",
        {
            "schema_id": "C2_EXECUTION_LIFECYCLE_AUTHORITY_V1",
            "schema_version": 1,
            "day_utc": day,
            "sleeve": "PRIMARY",
            "environment": "PAPER",
            "produced_utc": f"{day}T15:00:00Z",
            "authority_scope": "POST_SUBMIT_EXECUTION_LIFECYCLE",
            "status": "PASS",
            "current_lifecycle_state": state,
            "first_blocker_or_gap": "",
            "submission_count": 1,
            "submissions": [
                {
                    "submission_id": submission_id,
                    "current_lifecycle_state": state,
                    "broker_order_id": broker_order_id,
                    "broker_perm_id": perm_id,
                    "first_blocker_or_gap": "",
                }
            ],
        },
    )


def test_build_operational_truth_v1_reads_canonical_panels(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day = "2026-04-13"
    submission_id = "sub123"
    stream_hash = "abc123"
    snapshot_path = truth_root / "positions_v1" / "snapshots" / day / "positions_snapshot.v4.json"

    _write_json(
        truth_root / "positions_v1" / "effective_v1" / "days" / day / "positions_effective_pointer.v1.json",
        {
            "pointers": {"snapshot_path": str(snapshot_path)},
            "status": "OK",
        },
    )
    _write_json(
        snapshot_path,
        {
            "positions": {
                "asof_utc": f"{day}T00:00:00Z",
                "items": [
                    {
                        "position_id": "pos1",
                        "engine_id": "C2_TREND_EQ_PRIMARY_V1",
                        "qty": 1,
                        "avg_cost_cents": 12345,
                        "status": "OPEN",
                        "instrument": {"symbol": "SPY", "currency": "USD"},
                    }
                ],
            },
            "status": "OK",
        },
    )
    _write_json(
        truth_root / "execution_stream_v1" / day / "stream1.execution_event_stream_record.v1.json",
        {
            "submission_id": submission_id,
            "canonical_json_hash": stream_hash,
            "observed_at_utc": "2026-04-12T14:23:13Z",
            "event_time_utc": "2026-04-12T14:23:13Z",
            "broker_ids": {"order_id": 75, "perm_id": 1870974300},
            "order_state": {"status": "SUBMITTED", "filled_qty": 1, "remaining_qty": 0},
            "reason_codes": [],
        },
    )
    _write_json(
        truth_root / "execution_evidence_v1" / "submissions" / day / submission_id / "broker_submission_record.v2.json",
        {
            "status": "SUBMITTED",
            "submission_id": submission_id,
            "submitted_at_utc": "2026-04-12T14:20:00Z",
            "broker_ids": {"order_id": 75, "perm_id": 1870974300},
        },
    )
    _write_json(
        truth_root / "execution_evidence_v1" / "submissions" / day / submission_id / "execution_event_record.v1.json",
        {
            "event_time_utc": "2026-04-12T14:23:13Z",
            "raw_broker_status": "SUBMITTED",
            "upstream_hash": stream_hash,
        },
    )
    _write_json(
        truth_root / "execution_evidence_v1" / "submissions" / day / submission_id / "equity_order_plan.v1.json",
        {
            "symbol": "SPY",
            "action": "BUY",
            "qty_shares": 1,
            "order_terms": {"order_type": "MARKET", "time_in_force": "DAY"},
        },
    )
    _write_json(
        truth_root / "fill_ledger_v1" / day / f"{submission_id}.fill_ledger.v1.json",
        {
            "filled_qty": 1,
            "remaining_qty": 0,
            "lifecycle_status": "FILLED",
            "event_hashes": [stream_hash],
            "produced_utc": f"{day}T00:00:00Z",
        },
    )
    _write_json(
        truth_root / "target_day_build_v1" / f"{day}.json",
        {"build_status": "COMPLETE", "completeness_result": "COMPLETE", "closure_status": "CLOSED", "generated_utc": "2026-04-12T05:18:55Z"},
    )
    _write_json(
        truth_root / "target_day_admission_v1" / f"{day}.json",
        {"admission_status": "ADMIT", "closure_status": "CLOSED", "generated_utc": "2026-04-12T05:18:55Z"},
    )
    _write_json(
        truth_root / "reports" / "submit_boundary_status_v1" / day / "submit_boundary_status.v1.json",
        {"boundary_status": "AUTHORIZED", "submission_authorized": True, "produced_at_utc": "2026-04-12T06:10:23Z"},
    )
    _write_json(
        truth_root / "reports" / "paper_session_ledger_v1" / day / "paper_session_ledger.v1.json",
        {"control_state": {"authority_status": "GRANTED", "system_ready": True}, "evidence_status": "READY", "evaluated_at_utc": "2026-04-12T06:10:25Z"},
    )
    _write_json(
        truth_root / "reports" / "paper_day_control_plane_v1" / day / "paper_day_control_plane.v1.json",
        {"final_start_decision": "READY_NOW", "human_readable_summary": "ready", "evaluated_at_utc": "2026-04-12T06:10:21Z"},
    )
    _write_json(
        truth_root / "session_authority_status_v1" / "current.json",
        {
            "submission_authorization_status": "AUTHORIZED",
            "traceability_status": "VALID",
            "generated_utc": "2026-04-12T14:43:17Z",
            "monitoring_checks": [{"check_name": "canonical_readiness_authority", "status": "PASS", "summary": "aligned"}],
            "top_blocker_reason_codes": [],
        },
    )
    _write_json(
        truth_root / "reports" / "execution_reconciliation_v1" / day / "execution_reconciliation.v1.json",
        {"status": "PASS", "reason_codes": []},
    )

    payload = build_operational_truth_v1(truth_root, day)

    assert payload["ok"] is True
    assert payload["summary"]["readiness_status"] == "READY_NOW"
    assert payload["summary"]["positions_total"] == 1
    assert payload["summary"]["orders_total"] == 1
    assert payload["positions_panel"]["rows"][0]["symbol"] == "SPY"
    assert payload["positions_panel"]["rows"][0]["avg_price"] == "123.45"
    assert payload["orders_panel"]["rows"][0]["status"] == "SUBMITTED"
    assert payload["orders_panel"]["rows"][0]["broker_order_id"] == 75
    assert [row["key"] for row in payload["system_state_panel"]["rows"]] == [
        "build",
        "admission",
        "boundary",
        "ledger",
        "control_plane",
        "session_status",
        "consistency_gate",
    ]
    assert payload["alerts_panel"]["rows"] == []


def test_build_operational_truth_v1_surfaces_orphan_and_stale_alerts(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day = "2026-04-13"
    submission_id = "sub999"

    _write_json(
        truth_root / "execution_stream_v1" / day / "stream1.execution_event_stream_record.v1.json",
        {
            "submission_id": submission_id,
            "canonical_json_hash": "freshhash",
            "observed_at_utc": "2026-04-12T14:23:13Z",
            "broker_ids": {"order_id": 70, "perm_id": 1870974295},
            "order_state": {"status": "PRESUBMITTED"},
            "reason_codes": ["ATTRIBUTED_BY_POST_HANDOFF_ORPHAN_FALLBACK"],
        },
    )
    _write_json(
        truth_root / "execution_evidence_v1" / "submissions" / day / submission_id / "execution_event_record.v1.json",
        {"upstream_hash": "oldhash", "raw_broker_status": "PRESUBMITTED"},
    )
    _write_json(
        truth_root / "execution_evidence_v1" / "submissions" / day / submission_id / "equity_order_plan.v1.json",
        {"symbol": "SPY", "action": "BUY", "qty_shares": 1, "order_terms": {"order_type": "LIMIT", "time_in_force": "DAY"}},
    )
    _write_json(
        truth_root / "fill_ledger_v1" / day / f"{submission_id}.fill_ledger.v1.json",
        {"lifecycle_status": "OPEN", "event_hashes": ["oldhash"]},
    )
    _write_json(truth_root / "target_day_build_v1" / f"{day}.json", {"build_status": "COMPLETE"})
    _write_json(truth_root / "target_day_admission_v1" / f"{day}.json", {"admission_status": "ADMIT"})
    _write_json(truth_root / "reports" / "submit_boundary_status_v1" / day / "submit_boundary_status.v1.json", {"boundary_status": "AUTHORIZED"})
    _write_json(truth_root / "reports" / "paper_session_ledger_v1" / day / "paper_session_ledger.v1.json", {"control_state": {"authority_status": "GRANTED"}, "evidence_status": "READY"})
    _write_json(truth_root / "reports" / "paper_day_control_plane_v1" / day / "paper_day_control_plane.v1.json", {"final_start_decision": "READY_NOW"})
    _write_json(
        truth_root / "session_authority_status_v1" / "current.json",
        {
            "submission_authorization_status": "AUTHORIZED",
            "traceability_status": "VALID",
            "monitoring_checks": [{"check_name": "canonical_readiness_authority", "status": "PASS", "summary": "aligned"}],
            "top_blocker_reason_codes": [],
        },
    )
    _write_json(truth_root / "reports" / "execution_reconciliation_v1" / day / "execution_reconciliation.v1.json", {"status": "PASS", "reason_codes": []})

    payload = build_operational_truth_v1(truth_root, day)
    codes = {row["code"] for row in payload["alerts_panel"]["rows"]}

    assert "ORPHAN_EVENT_LINEAGE" in codes
    assert "STALE_EXECUTION_EVENT" in codes
    assert "STALE_FILL_LEDGER" in codes


def test_lifecycle_authority_present_dashboard_uses_it(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day = "2026-04-13"
    submission_id = "sub_lifecycle"
    _write_ready_surfaces(truth_root, day)
    _write_order_surfaces(truth_root, day, submission_id, stream_status="SUBMITTED", fill_status="OPEN")
    _write_lifecycle_authority(
        truth_root,
        day,
        submission_id,
        state="ACKNOWLEDGED_OPEN",
        broker_order_id=701,
        perm_id=1701,
    )

    payload = build_operational_truth_v1(truth_root, day)
    row = payload["orders_panel"]["rows"][0]

    assert payload["summary"]["readiness_status"] == "READY_NOW"
    assert row["status"] == "ACKNOWLEDGED_OPEN"
    assert row["broker_order_id"] == 701
    assert row["perm_id"] == 1701


def test_lifecycle_authority_absent_legacy_readiness_does_not_regress_to_unknown(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day = "2026-04-13"
    _write_ready_surfaces(truth_root, day)

    payload = build_operational_truth_v1(truth_root, day)

    assert payload["summary"]["readiness_status"] == "READY_NOW"


def test_strategy_portfolio_and_risk_authorities_surface_in_dashboard(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day = "2026-04-13"
    _write_ready_surfaces(truth_root, day)
    _write_json(
        truth_root / "reports" / "strategy_decision_authority_v1" / day / "strategy_decision_authority.v1.json",
        {
            "strategy_decision_state": "INTENT_CREATED",
            "intent_count": 1,
            "zero_intent_reason": "",
            "produced_utc": f"{day}T14:00:00Z",
        },
    )
    _write_json(
        truth_root / "reports" / "portfolio_account_authority_v1" / day / "portfolio_account_authority.v1.json",
        {
            "account_state": "OPERATOR_STATEMENT_ONLY",
            "source_type": "OPERATOR_STATEMENT",
            "account_values": {"cash_total_cents": 10000000, "net_liquidation_cents": 10000000},
            "produced_utc": f"{day}T14:01:00Z",
        },
    )
    _write_json(
        truth_root / "reports" / "risk_sizing_authority_v1" / day / "risk_sizing_authority.v1.json",
        {
            "risk_sizing_state": "ROUNDED",
            "first_sizing_reason": "FINAL_RISK_BELOW_REQUESTED",
            "first_blocker": "",
            "final_size_summary": {"final_quantity": 1, "final_risk_cents": 43700},
            "produced_utc": f"{day}T14:02:00Z",
        },
    )

    payload = build_operational_truth_v1(truth_root, day)
    rows = {row["key"]: row for row in payload["system_state_panel"]["rows"]}

    assert rows["strategy_decision_authority"]["status"] == "INTENT_CREATED"
    assert rows["strategy_decision_authority"]["detail"] == "intent_count=1 zero_reason=<none>"
    assert rows["portfolio_account_authority"]["status"] == "OPERATOR_STATEMENT_ONLY"
    assert "cash_cents=10000000" in rows["portfolio_account_authority"]["detail"]
    assert rows["risk_sizing_authority"]["status"] == "ROUNDED"
    assert "reason=FINAL_RISK_BELOW_REQUESTED" in rows["risk_sizing_authority"]["detail"]
    assert "blocker=<none>" in rows["risk_sizing_authority"]["detail"]
    assert payload["summary"]["readiness_status"] == "READY_NOW"


def test_lifecycle_authority_conflict_with_stale_projection_authority_wins(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day = "2026-04-13"
    submission_id = "sub_conflict"
    _write_ready_surfaces(truth_root, day)
    _write_order_surfaces(truth_root, day, submission_id, stream_status="SUBMITTED", fill_status="OPEN")
    _write_lifecycle_authority(
        truth_root,
        day,
        submission_id,
        state="RECONCILED",
        broker_order_id=901,
        perm_id=1901,
    )

    payload = build_operational_truth_v1(truth_root, day)
    row = payload["orders_panel"]["rows"][0]

    assert row["status"] == "RECONCILED"
    assert row["broker_order_id"] == 901
    assert row["perm_id"] == 1901
