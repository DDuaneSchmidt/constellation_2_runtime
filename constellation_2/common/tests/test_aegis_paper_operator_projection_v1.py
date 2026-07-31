from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.human_reviewed_paper_mode_v1 import build_paper_review_queue_v1, paper_trade_receipts_path_v1, write_candidate_review_packet_v1, write_paper_review_queue_v1
from ops.aegis.operator_action_command_contracts_v1 import execute_aegis_command_v1
from ops.aegis.operator_command_lifecycle_v1 import (
    command_inbox_path_v1,
    command_results_path_v1,
    command_status_v1,
    process_command_inbox_v1,
    record_paper_trade_command_v1,
)
from ops.aegis.paper_operator_projection_v1 import build_paper_operator_projection_v1
from ops.aegis.candidate_lifecycle_projection_v1 import build_candidate_lifecycle_projection_v1
from ops.aegis.trade_lifecycle.paper_trade_construction_v1 import build_and_write_paper_trade_construction_v1

DAY = "2026-05-28"
NOW = "2026-05-28T13:50:00Z"
SESSION = "PAPER-2026-05-28-0950"


def _write(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _candidate(idx: int, *, session: str = SESSION) -> dict:
    return {
        "paper_session_id": session,
        "candidate_id": f"candidate-{idx}",
        "symbol": f"T{idx}",
        "sleeve_id": "C2_CROSS_ASSET_TREND_V1",
        "strategy": "test_strategy",
        "direction": "LONG",
        "score": idx,
        "paper_trade_eligible": True,
        "live_trade_eligible": False,
        "created_at": NOW,
    }


def _seed_packet_queue_market(root: Path, *, count: int = 17, include_market: bool = True) -> None:
    candidates = [_candidate(idx) for idx in range(count)]
    packet = {
        "schema_id": "candidate_review_packet",
        "schema_version": "v1",
        "day_utc": DAY,
        "generated_at_utc": NOW,
        "run_timestamp_utc": NOW,
        "paper_session_id": SESSION,
        "paper_session_id_derivation_source": "generated_at_utc",
        "candidate_count": count,
        "review_candidates": candidates,
        "safety": {"paper_only": True, "broker_submit_transmit_allowed": False, "autonomous_execution_allowed": False},
    }
    write_candidate_review_packet_v1(truth_root=root, day_utc=DAY, payload=packet)
    queue = build_paper_review_queue_v1(truth_root=root, day_utc=DAY, packet_payload=packet)
    write_paper_review_queue_v1(truth_root=root, day_utc=DAY, payload=queue)
    records = [{"symbol": row["symbol"], "value": "100.00", "day_utc": DAY, "validation_status": "VALID"} for row in candidates] if include_market else []
    _write(root / "reports" / "market_data_inputs_v1" / DAY / "market_data_inputs.v1.json", {"schema_id": "market_data_inputs", "day_utc": DAY, "generated_at_utc": NOW, "validation_status": "VALID", "input_records": records})


def _seed_kernel(root: Path, *, paper_creation_allowed: bool = True) -> None:
    graph = {
        "PAPER_CANDIDATES_READY": {"allowed": True, "missing_or_blocking_artifacts": []},
        "PAPER_REVIEW_ALLOWED": {"allowed": True, "missing_or_blocking_artifacts": []},
        "PAPER_TRADE_READY": {"allowed": True, "missing_or_blocking_artifacts": []},
        "PAPER_TRADE_CREATION_ALLOWED": {"allowed": paper_creation_allowed, "reason": "OK" if paper_creation_allowed else "MISSING_CURRENT_MARKET_DATA", "missing_or_blocking_artifacts": [] if paper_creation_allowed else ["paper_trade_construction"]},
        "TRADE_ADVICE_ALLOWED": {"allowed": False, "reason": "PARTIAL_CONTEXT / missing advisory dependencies"},
    }
    _write(root / "reports" / "aegis_runtime_truth_kernel_v1" / DAY / "runtime_truth_kernel.v1.json", {"schema_id": "aegis_runtime_truth_kernel", "day_utc": DAY, "runtime_truth_classification": "PARTIAL_CONTEXT", "highest_readiness_layer": "BLOCKED", "dependency_graph": graph, "broker_submit_required": False, "autonomous_execution_allowed": False})


def test_paper_session_id_propagates_packet_queue_construction_projection(tmp_path: Path) -> None:
    _seed_packet_queue_market(tmp_path, count=17)
    construction, _path = build_and_write_paper_trade_construction_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    _seed_kernel(tmp_path, paper_creation_allowed=True)

    projection = build_paper_operator_projection_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert construction["paper_session_id"] == SESSION
    assert {row["paper_session_id"] for row in construction["constructed_paper_trades"]} == {SESSION}
    assert projection["sessions"][0]["paper_session_id"] == SESSION
    assert projection["sessions"][0]["current_day_candidate_count"] == 17
    assert projection["sessions"][0]["constructed_trade_count"] == 17
    assert {row["paper_session_id"] for row in projection["current_day_candidates"]} == {SESSION}




def test_projection_includes_paper_trade_construction_prices_and_risk(tmp_path: Path) -> None:
    _seed_packet_queue_market(tmp_path, count=2)
    build_and_write_paper_trade_construction_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    _seed_kernel(tmp_path, paper_creation_allowed=True)

    projection = build_paper_operator_projection_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    rows = projection["current_day_candidates"]
    assert len(rows) == 2
    assert rows[0]["entry_price"] == "100.00"
    assert rows[0]["stop_price"] == "90.00"
    assert rows[0]["quantity"] == "1"
    assert rows[0]["max_risk_amount"] == "10.00"
    assert rows[0]["construction_status"] == "CONSTRUCTED"
    assert rows[0]["construction_artifact_path"].endswith("paper_trade_construction.v1.json")
    assert rows[0]["construction_missing_fields"] == []
    assert projection["sessions"][0]["current_day_candidate_count"] == 2
    assert len(projection["carry_forward_candidates"]) == 0
    assert len(projection["open_paper_positions"]) == 0

def test_carry_forward_rows_keep_original_session_and_do_not_mix_counts(tmp_path: Path) -> None:
    _seed_packet_queue_market(tmp_path, count=2)
    queue_path = tmp_path / "reports" / "aegis_paper_review_queue_v1" / DAY / "paper_review_queue.v1.json"
    queue = json.loads(queue_path.read_text(encoding="utf-8"))
    queue["rows"].append({**_candidate(99, session="PAPER-2026-05-26-1314"), "originating_day": "2026-05-26", "rollover_status": "CARRIED_FORWARD", "status": "PAPER_POSITION_OPEN"})
    _write(queue_path, queue)
    build_and_write_paper_trade_construction_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    _seed_kernel(tmp_path, paper_creation_allowed=True)

    projection = build_paper_operator_projection_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert projection["sessions"][0]["current_day_candidate_count"] == 2
    assert len(projection["carry_forward_candidates"]) == 1
    assert projection["carry_forward_candidates"][0]["paper_session_id"] == "PAPER-2026-05-26-1314"
    assert projection["carry_forward_candidates"][0]["carry_forward"] is True






def test_paper_trade_command_accepts_active_projection_identifier_and_records_receipt(tmp_path: Path) -> None:
    _seed_packet_queue_market(tmp_path, count=1)
    build_and_write_paper_trade_construction_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    _seed_kernel(tmp_path, paper_creation_allowed=True)
    projection = build_paper_operator_projection_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    row = projection["current_day_candidates"][0]

    result = execute_aegis_command_v1(
        {
            "command_id": "PAPER_TRADE_CANDIDATE",
            "target_type": "paper_review_candidate",
            "target_id": row["candidate_id"],
            "payload": {
                "candidate_id": row["candidate_id"],
                "candidate_contract_id": row["candidate_contract_id"],
                "paper_session_id": row["paper_session_id"],
                "day_utc": DAY,
                "action": "PAPER_TRADE",
                "paper_entry_price": row["entry_price"],
                "paper_stop_price": row["stop_price"],
                "quantity": row["quantity"],
                "notional": row["notional_value"],
            },
        },
        truth_root=tmp_path,
        repo_root=ROOT,
        day_utc=DAY,
    )

    assert result["ok"] is True
    assert result["workflow_state"] == "PAPER_POSITION_OPEN"
    assert result["receipt"]["candidate_id"] == row["candidate_id"]
    assert result["command_result"]["expected_source_path"] == str(paper_trade_receipts_path_v1(truth_root=tmp_path, day_utc=DAY))
    assert paper_trade_receipts_path_v1(truth_root=tmp_path, day_utc=DAY).exists()
    refreshed = build_paper_operator_projection_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    refreshed_rows = [item for item in refreshed["current_day_candidates"] if item["candidate_id"] == row["candidate_id"]]
    if refreshed_rows:
        refreshed_row = refreshed_rows[0]
        assert refreshed_row["candidate_status"] == "PAPER_POSITION_OPEN"
        assert refreshed_row["lifecycle_state"] == "PAPER_POSITION_OPEN"
        assert refreshed_row["actionable"] is False
        assert refreshed_row["action_block_reason"] == "Paper trade already recorded"
    else:
        assert any(item.get("candidate_id") == row["candidate_id"] for item in refreshed["open_paper_positions"])

def test_projection_marks_active_review_identity_and_blocks_carry_forward_actions(tmp_path: Path) -> None:
    _seed_packet_queue_market(tmp_path, count=1)
    queue_path = tmp_path / "reports" / "aegis_paper_review_queue_v1" / DAY / "paper_review_queue.v1.json"
    queue = json.loads(queue_path.read_text(encoding="utf-8"))
    queue["rows"].append({**_candidate(99, session="PAPER-2026-05-26-1314"), "originating_day": "2026-05-26", "rollover_status": "CARRIED_FORWARD", "status": "AWAITING_REVIEW"})
    _write(queue_path, queue)
    build_and_write_paper_trade_construction_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    _seed_kernel(tmp_path, paper_creation_allowed=True)

    projection = build_paper_operator_projection_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    active = projection["current_day_candidates"][0]
    carry = projection["carry_forward_candidates"][0]
    assert active["candidate_id"] == active["candidate_contract_id"]
    assert active["source_state"] == "ACTIVE_REVIEW"
    assert active["active_review_present"] is True
    assert active["actionable"] is True
    assert active["action_endpoint"] == "/api/aegis/commands"
    assert carry["source_state"] == "CARRY_FORWARD"
    assert carry["active_review_present"] is False
    assert carry["actionable"] is False
    assert carry["action_block_reason"] == "Not in active review state"
    assert carry["action_endpoint"] == "none"

def test_projection_separates_current_carry_forward_and_open_positions(tmp_path: Path) -> None:
    _seed_packet_queue_market(tmp_path, count=3)
    _write(tmp_path / "reports" / "aegis_paper_position_ledger_v1" / DAY / "paper_position_ledger.v1.json", {"schema_id": "paper_position_ledger", "day_utc": DAY, "open_positions": [{"symbol": "T0", "position_id": "pos-1", "paper_session_id": SESSION, "originating_day": DAY, "status": "OPEN"}]})
    build_and_write_paper_trade_construction_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    _seed_kernel(tmp_path, paper_creation_allowed=True)

    projection = build_paper_operator_projection_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert len(projection["current_day_candidates"]) == 3
    assert projection["sessions"][0]["current_day_candidate_count"] == 3
    assert len(projection["carry_forward_candidates"]) == 0
    assert len(projection["open_paper_positions"]) == 1


def test_mode_split_keeps_paper_ready_while_advisory_blocked_live_disabled(tmp_path: Path) -> None:
    _seed_packet_queue_market(tmp_path, count=1)
    build_and_write_paper_trade_construction_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    _seed_kernel(tmp_path, paper_creation_allowed=True)

    projection = build_paper_operator_projection_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert projection["paper_mode"]["status"] == "READY"
    assert projection["paper_mode"]["paper_trade_creation_allowed"] is True
    assert projection["advisory_mode"]["status"] == "BLOCKED"
    assert projection["advisory_mode"]["trade_advice_allowed"] is False
    assert projection["live_mode"]["status"] == "DISABLED"
    assert projection["live_mode"]["broker_submit_transmit_allowed"] is False
    assert projection["live_mode"]["autonomous_execution_allowed"] is False


def test_missing_market_data_blocks_projection_with_precise_skipped_rows(tmp_path: Path) -> None:
    _seed_packet_queue_market(tmp_path, count=1, include_market=False)
    construction, _path = build_and_write_paper_trade_construction_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    _seed_kernel(tmp_path, paper_creation_allowed=False)

    projection = build_paper_operator_projection_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert construction["paper_submit_created"] is False
    assert construction["skipped_candidates"][0]["symbol"] == "T0"
    assert construction["skipped_candidates"][0]["missing_field"] == "market_data.current_price"
    assert projection["paper_mode"]["status"] == "BLOCKED"
    assert projection["blocked_or_skipped_candidates"][0]["blocker_reason"]



def test_projection_exposes_actionable_open_and_context_buckets_after_two_executed_commands(tmp_path: Path) -> None:
    _seed_packet_queue_market(tmp_path, count=25)
    build_and_write_paper_trade_construction_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    _seed_kernel(tmp_path, paper_creation_allowed=True)
    projection = build_paper_operator_projection_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    rows = projection["actionable_current_candidates"][:2]

    command_results = []
    receipts = []
    for idx, row in enumerate(rows):
        command_id = f"CMD-test-{idx}"
        command_results.append({
            "command_id": command_id,
            "candidate_id": row["candidate_id"],
            "candidate_contract_id": row["candidate_contract_id"],
            "paper_session_id": row["paper_session_id"],
            "status": "EXECUTED",
            "processed_at": NOW,
            "message": "Paper trade recorded",
            "receipt_id": f"paper-review:{row['candidate_id']}:test",
            "receipt_path": f"/tmp/{row['candidate_id']}.json",
            "error_code": None,
            "details": {},
        })
        receipts.append({
            "receipt_id": f"paper-review:{row['candidate_id']}:test",
            "candidate_id": row["candidate_id"],
            "symbol": row["symbol"],
            "paper_session_id": row["paper_session_id"],
            "entry_price": row["entry_price"],
            "stop_price": row["stop_price"],
            "quantity": row["quantity"],
        })
    _write(command_results_path_v1(truth_root=tmp_path, day_utc=DAY), {"schema_id": "aegis_command_results", "day_utc": DAY, "results": command_results})
    _write(paper_trade_receipts_path_v1(truth_root=tmp_path, day_utc=DAY), {"schema_id": "paper_trade_receipts", "day_utc": DAY, "receipts": receipts})

    refreshed = build_paper_operator_projection_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    executed_ids = {row["candidate_id"] for row in rows}
    actionable_ids = {row["candidate_id"] for row in refreshed["actionable_current_candidates"]}
    open_ids = {row.get("candidate_id") for row in refreshed["open_paper_positions"]}

    assert refreshed["sessions"][0]["current_day_candidate_count"] == 25
    assert refreshed["sessions"][0]["actionable_current_candidate_count"] == 23
    assert len(refreshed["actionable_current_candidates"]) == 23
    assert executed_ids.isdisjoint(actionable_ids)
    assert executed_ids.issubset(open_ids)
    assert all(row["source_state"] == "ACTIVE_REVIEW" for row in refreshed["actionable_current_candidates"])
    assert all(row["actionable"] is True for row in refreshed["actionable_current_candidates"])
    assert all(row["active_review_present"] is True for row in refreshed["actionable_current_candidates"])


def test_projection_carry_forward_context_is_separate_and_not_actionable(tmp_path: Path) -> None:
    _seed_packet_queue_market(tmp_path, count=2)
    queue_path = tmp_path / "reports" / "aegis_paper_review_queue_v1" / DAY / "paper_review_queue.v1.json"
    queue = json.loads(queue_path.read_text(encoding="utf-8"))
    queue["rows"].append({**_candidate(99, session="PAPER-2026-05-26-1314"), "originating_day": "2026-05-26", "rollover_status": "CARRIED_FORWARD", "status": "AWAITING_REVIEW"})
    _write(queue_path, queue)
    build_and_write_paper_trade_construction_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    _seed_kernel(tmp_path, paper_creation_allowed=True)

    projection = build_paper_operator_projection_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert len(projection["actionable_current_candidates"]) == 2
    assert len(projection["carry_forward_context"]) == 1
    assert projection["carry_forward_context"][0]["actionable"] is False
    assert projection["carry_forward_context"][0]["action_endpoint"] == "none"

def test_paper_operator_package_scripts_exist() -> None:
    package = json.loads((ROOT / "package.json").read_text(encoding="utf-8"))
    scripts = package["scripts"]
    assert "aegis:paper-status" in scripts
    assert "build_aegis_paper_operator_projection_v1.py" in scripts["aegis:paper-status"]
    assert "aegis:paper-open" in scripts
    assert "run_aegis_paper_open_v1.py" in scripts["aegis:paper-open"]
    assert "aegis:doctor" in scripts


def test_durable_command_inbox_records_one_received_paper_trade_command(tmp_path: Path) -> None:
    _seed_packet_queue_market(tmp_path, count=1)
    build_and_write_paper_trade_construction_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    _seed_kernel(tmp_path, paper_creation_allowed=True)
    row = build_paper_operator_projection_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)["current_day_candidates"][0]

    command, inbox_path = record_paper_trade_command_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        body={"paper_session_id": row["paper_session_id"], "candidate_id": row["candidate_id"], "candidate_contract_id": row["candidate_contract_id"], "payload": {"symbol": row["symbol"], "entry_price": row["entry_price"], "stop_price": row["stop_price"], "quantity": row["quantity"]}},
    )

    inbox = json.loads(inbox_path.read_text(encoding="utf-8"))
    assert inbox_path == command_inbox_path_v1(truth_root=tmp_path, day_utc=DAY)
    assert command["status"] == "RECEIVED"
    assert command["command_type"] == "PAPER_TRADE_REQUESTED"
    assert [item["command_id"] for item in inbox["commands"]] == [command["command_id"]]


def test_durable_command_processor_executes_valid_command_and_rebuilds_projection(tmp_path: Path) -> None:
    _seed_packet_queue_market(tmp_path, count=1)
    build_and_write_paper_trade_construction_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    _seed_kernel(tmp_path, paper_creation_allowed=True)
    row = build_paper_operator_projection_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)["current_day_candidates"][0]
    command, _path = record_paper_trade_command_v1(truth_root=tmp_path, day_utc=DAY, body={"paper_session_id": row["paper_session_id"], "candidate_id": row["candidate_id"], "candidate_contract_id": row["candidate_contract_id"], "payload": {"symbol": row["symbol"], "entry_price": row["entry_price"], "stop_price": row["stop_price"], "quantity": row["quantity"]}})

    result = process_command_inbox_v1(truth_root=tmp_path, day_utc=DAY)

    assert result["processed_count"] == 1
    assert result["processed"][0]["status"] == "EXECUTED"
    status = command_status_v1(truth_root=tmp_path, day_utc=DAY, command_id=command["command_id"])
    assert status["terminal"] is True
    assert status["result"]["status"] == "EXECUTED"
    assert status["result"]["receipt_id"].startswith(f"paper-review:{row['candidate_id']}:")
    assert command_results_path_v1(truth_root=tmp_path, day_utc=DAY).exists()
    refreshed = build_paper_operator_projection_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    refreshed_row = refreshed["current_day_candidates"][0] if refreshed["current_day_candidates"] else refreshed["open_paper_positions"][0]
    assert refreshed_row["candidate_id"] == row["candidate_id"]


def test_durable_command_processor_rejects_invalid_candidate(tmp_path: Path) -> None:
    _seed_packet_queue_market(tmp_path, count=1)
    build_and_write_paper_trade_construction_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    _seed_kernel(tmp_path, paper_creation_allowed=True)
    command, _path = record_paper_trade_command_v1(truth_root=tmp_path, day_utc=DAY, body={"paper_session_id": SESSION, "candidate_id": "missing-candidate", "candidate_contract_id": "missing-candidate", "payload": {"entry_price": "1", "stop_price": "0.9", "quantity": "1"}})

    process_command_inbox_v1(truth_root=tmp_path, day_utc=DAY)

    status = command_status_v1(truth_root=tmp_path, day_utc=DAY, command_id=command["command_id"])
    assert status["result"]["status"] == "REJECTED"
    assert status["result"]["error_code"] == "CANDIDATE_NOT_FOUND"


def test_durable_command_processor_rejects_missing_construction_and_is_idempotent(tmp_path: Path) -> None:
    _seed_packet_queue_market(tmp_path, count=1, include_market=False)
    _seed_kernel(tmp_path, paper_creation_allowed=True)
    row = build_paper_operator_projection_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)["current_day_candidates"][0]
    command, _path = record_paper_trade_command_v1(truth_root=tmp_path, day_utc=DAY, body={"paper_session_id": row["paper_session_id"], "candidate_id": row["candidate_id"], "candidate_contract_id": row["candidate_contract_id"], "payload": {"quantity": "1"}})

    first = process_command_inbox_v1(truth_root=tmp_path, day_utc=DAY)
    second = process_command_inbox_v1(truth_root=tmp_path, day_utc=DAY)

    status = command_status_v1(truth_root=tmp_path, day_utc=DAY, command_id=command["command_id"])
    assert first["processed_count"] == 1
    assert second["idempotent_skip_count"] == 1
    assert status["result"]["status"] == "REJECTED"
    assert status["result"]["error_code"] in {"CONSTRUCTION_MISSING", "CONSTRUCTION_PRICES_MISSING"}
    receipts = json.loads(paper_trade_receipts_path_v1(truth_root=tmp_path, day_utc=DAY).read_text(encoding="utf-8")) if paper_trade_receipts_path_v1(truth_root=tmp_path, day_utc=DAY).exists() else {"receipts": []}
    assert receipts["receipts"] == []


def _seed_executed_command_result_and_receipt(root: Path, row: dict, command_id: str) -> None:
    results_path = command_results_path_v1(truth_root=root, day_utc=DAY)
    results_doc = json.loads(results_path.read_text(encoding="utf-8")) if results_path.exists() else {"schema_id": "aegis_command_results", "day_utc": DAY, "results": []}
    receipt_id = f"paper-review:{row['candidate_id']}:test-{command_id}"
    results_doc.setdefault("results", []).append({
        "command_id": command_id,
        "candidate_id": row["candidate_id"],
        "candidate_contract_id": row.get("candidate_contract_id") or row["candidate_id"],
        "paper_session_id": row.get("paper_session_id") or SESSION,
        "status": "EXECUTED",
        "processed_at": NOW,
        "message": "Paper trade recorded",
        "receipt_id": receipt_id,
        "receipt_path": str(paper_trade_receipts_path_v1(truth_root=root, day_utc=DAY)),
        "error_code": None,
        "details": {},
    })
    _write(results_path, results_doc)
    receipts_path = paper_trade_receipts_path_v1(truth_root=root, day_utc=DAY)
    receipts_doc = json.loads(receipts_path.read_text(encoding="utf-8")) if receipts_path.exists() else {"schema_id": "paper_trade_receipts", "day_utc": DAY, "receipts": []}
    receipts_doc.setdefault("receipts", []).append({
        "candidate_id": row["candidate_id"],
        "symbol": row.get("symbol"),
        "paper_session_id": row.get("paper_session_id") or SESSION,
        "timestamp_utc": f"{NOW[:-1]}-{command_id}",
        "paper_entry_price": row.get("entry_price") or row.get("entry_reference_price") or "100",
        "paper_stop_price": row.get("stop_price") or "90",
        "quantity": row.get("quantity") or "1",
    })
    _write(receipts_path, receipts_doc)


def test_candidate_lifecycle_projection_keeps_25_current_session_candidates_visible_after_all_executed(tmp_path: Path) -> None:
    _seed_packet_queue_market(tmp_path, count=25)
    construction, _ = build_and_write_paper_trade_construction_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    _seed_kernel(tmp_path, paper_creation_allowed=True)
    for idx, row in enumerate(construction["constructed_paper_trades"]):
        _seed_executed_command_result_and_receipt(tmp_path, row, f"CMD-all-{idx}")

    projection = build_candidate_lifecycle_projection_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert projection["summary"]["current_session_total"] == 25
    assert len(projection["current_session_candidates"]) == 25
    assert projection["summary"]["actionable"] == 0
    assert all(row["candidate_lifecycle_state"] == "PAPER_POSITION_OPEN" for row in projection["current_session_candidates"])
    assert all("REQUEST_PAPER_TRADE" not in row["allowed_actions"] for row in projection["current_session_candidates"])


def test_candidate_lifecycle_projection_two_trades_reduce_actionable_not_visibility(tmp_path: Path) -> None:
    _seed_packet_queue_market(tmp_path, count=25)
    construction, _ = build_and_write_paper_trade_construction_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    _seed_kernel(tmp_path, paper_creation_allowed=True)
    before = build_candidate_lifecycle_projection_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    for idx, row in enumerate(construction["constructed_paper_trades"][:2]):
        _seed_executed_command_result_and_receipt(tmp_path, row, f"CMD-two-{idx}")

    after = build_candidate_lifecycle_projection_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert before["summary"]["current_session_total"] == 25
    assert before["summary"]["actionable"] == 25
    assert after["summary"]["current_session_total"] == 25
    assert after["summary"]["actionable"] == 23
    assert after["summary"]["open"] == 2
    assert len([row for row in after["current_session_candidates"] if row["candidate_lifecycle_state"] == "PAPER_POSITION_OPEN"]) == 2


def test_candidate_lifecycle_projection_command_status_drives_state_and_actions(tmp_path: Path) -> None:
    _seed_packet_queue_market(tmp_path, count=1)
    build_and_write_paper_trade_construction_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    _seed_kernel(tmp_path, paper_creation_allowed=True)
    row = build_candidate_lifecycle_projection_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)["current_session_candidates"][0]
    command, _ = record_paper_trade_command_v1(truth_root=tmp_path, day_utc=DAY, body={"paper_session_id": row["paper_session_id"], "candidate_id": row["candidate_id"], "candidate_contract_id": row["candidate_contract_id"], "payload": {"symbol": row["symbol"], "entry_price": row["entry_price"], "stop_price": row["stop_price"], "quantity": row["quantity"]}})

    projection = build_candidate_lifecycle_projection_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    updated = projection["current_session_candidates"][0]

    assert command["status"] == "RECEIVED"
    assert updated["candidate_lifecycle_state"] == "COMMAND_RECEIVED"
    assert updated["allowed_actions"] == ["VIEW_COMMAND_STATUS"]
    assert "REQUEST_PAPER_TRADE" not in updated["allowed_actions"]


def test_candidate_lifecycle_projection_duplicate_executed_results_do_not_duplicate_open_state(tmp_path: Path) -> None:
    _seed_packet_queue_market(tmp_path, count=1)
    construction, _ = build_and_write_paper_trade_construction_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    _seed_kernel(tmp_path, paper_creation_allowed=True)
    row = construction["constructed_paper_trades"][0]
    _seed_executed_command_result_and_receipt(tmp_path, row, "CMD-dup")
    _seed_executed_command_result_and_receipt(tmp_path, row, "CMD-dup")

    projection = build_candidate_lifecycle_projection_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert projection["summary"]["current_session_total"] == 1
    assert len(projection["open_paper_positions"]) == 1
    assert projection["current_session_candidates"][0]["candidate_lifecycle_state"] == "PAPER_POSITION_OPEN"
