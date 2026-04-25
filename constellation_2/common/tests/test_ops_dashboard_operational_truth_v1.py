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
