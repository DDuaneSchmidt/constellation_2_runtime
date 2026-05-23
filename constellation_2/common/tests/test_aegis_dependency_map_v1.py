from __future__ import annotations

import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.build_aegis_dependency_map_v1 import (
    DOW_INTENT_HASH,
    DOW_TICKET_ID,
    build_dependency_map_v1,
    detect_cycles_v1,
    map_completeness_v1,
    render_dot_v1,
    validate_dependency_map_v1,
)

DAY = "2026-05-20"
SLEEVE = "C2_MEAN_REVERSION_EQ_V1"


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _seed_minimal_truth(root: Path) -> None:
    sleeve_root = root.parent / "truth_sleeves" / "PRIMARY" / "PAPER"
    ticket_dir = DOW_TICKET_ID.replace(":", "_")
    runtime_hash = "a" * 64
    _write(root / "reports/aegis_runtime_truth_kernel_v1" / DAY / "runtime_evaluation.v1.json", {"deterministic_output_hash": runtime_hash, "day_utc": DAY})
    _write(root / "reports/engine_universe_candidate_basis_v1" / DAY / SLEEVE / "engine_universe_candidate_basis.v1.json", {"day_utc": DAY, "validation_status": "VALID", "runtime_evaluation_hash": runtime_hash})
    _write(root / "reports/market_data_inputs_v1" / DAY / "market_data_inputs.v1.json", {"day_utc": DAY, "validation_status": "VALID", "runtime_evaluation_hash": runtime_hash})
    _write(root / "reports/aegis_sleeve_readiness_v1" / DAY / "sleeve_readiness.v1.json", {"day_utc": DAY, "validation_status": "VALID", "runtime_evaluation_hash": runtime_hash})
    _write(root / "reports/sleeve_evaluation_kernel_v1" / DAY / SLEEVE / "sleeve_evaluation.v1.json", {"day_utc": DAY, "validation_status": "VALID", "runtime_evaluation_hash": runtime_hash})
    _write(root / "reports/aegis_candidate_generation_diagnostics_v1" / DAY / "candidate_generation_diagnostics.v1.json", {"day_utc": DAY, "validation_status": "VALID", "runtime_evaluation_hash": runtime_hash})
    _write(root / "pointers/selected_intent_pointer.v1.json", {"day_utc": DAY, "status": "SELECTED", "runtime_evaluation_hash": runtime_hash})
    _write(root / "reports/paper_trade_construction_v1" / DAY / "paper_trade_construction.v1.json", {"day_utc": DAY, "source_day": DAY, "trade_construction_status": "complete", "runtime_evaluation_hash": runtime_hash})
    _write(root / "allocation_v1/capital_authority_allocation_v1" / DAY / "capital_authority_allocation.v1.json", {"day_utc": DAY, "validation_status": "VALID", "status": "OK", "runtime_evaluation_hash": runtime_hash})
    _write(root / "risk_definition_contract_v1" / DAY / DOW_INTENT_HASH / "risk_definition_contract.v1.json", {"day_utc": DAY, "validation_status": "PASS", "runtime_evaluation_hash": runtime_hash})
    phasec = sleeve_root / "phaseC_preflight_v1" / DAY / "attempt" / DOW_INTENT_HASH / "equity_order_plan.v2.json"
    _write(phasec, {"day_utc": DAY, "source_intent_id": "c2_mr_dow_2026-05-20_v1", "intent_hash": DOW_INTENT_HASH})
    _write(root / "reports/candidate_identity_set_v1" / DAY / DOW_INTENT_HASH / "candidate_identity_set.v1.json", {"day_utc": DAY, "validation_status": "VALID", "runtime_evaluation_hash": runtime_hash, "actual_phasec_order_plan": {"order_plan_path": str(phasec)}})
    _write(root / "target_day_admission_v1" / f"{DAY}.json", {"target_day": DAY, "admission_status": "ADMIT", "validation_status": "VALID", "runtime_evaluation_hash": runtime_hash})
    _write(sleeve_root / "day_activation_package_v1" / DAY / "ctx" / "day_activation_package.v1.json", {"day_utc": DAY, "validation_status": "VALID", "runtime_evaluation_hash": runtime_hash})
    _write(sleeve_root / "global_context_package_v1" / DAY / "ctx" / "global_context_package.v1.json", {"day_utc": DAY, "validation_status": "VALID", "runtime_evaluation_hash": runtime_hash})
    econ_pkg = sleeve_root / "economic_state_package_v1" / DAY / "ctx" / "economic_state_package.v1.json"
    first = {"dependency_id": "economic_state_package_v1", "first_real_blocker_dependency_id": "cash_ledger_snapshot_v1", "detail": "ECONOMIC_STATE_BLOCKED:first_real_blocker=cash_ledger_snapshot_v1", "path": str(econ_pkg)}
    _write(root / "reports/economic_state_build_v1" / DAY / "ctx" / "economic_state_build.v1.json", {"day_utc": DAY, "closure_status": "BLOCKED", "first_real_blocker": first})
    details = {"first_real_blocker": first, "build_path": str(root / "reports/execution_build_v1" / DAY / "sub" / "execution_build.v1.json")}
    _write(root / "reports/exposure_intent_paper_submission_package_v1" / DAY / "attempt" / "exposure_intent_paper_submission_package.v1.json", {"day_utc": DAY, "status": "BLOCKED", "blocker_code": "economic_state_package_v1", "blocker_message": json.dumps(details), "actual_phasec_order_plan_path": str(phasec), "runtime_evaluation_hash": runtime_hash})
    _write(root / "reports/paper_intent_evidence_v1" / DAY / ticket_dir / "paper_intent_evidence.v1.json", {"day_utc": DAY, "validation_status": "VALID", "runtime_evaluation_hash": runtime_hash})
    _write(root / "reports/market_freshness_evidence_v1" / DAY / ticket_dir / "market_freshness_evidence.v1.json", {"day_utc": DAY, "validation_status": "VALID", "runtime_evaluation_hash": runtime_hash})
    _write(root / "reports/paper_conversion_evidence_v1" / DAY / ticket_dir / "paper_conversion_evidence.v1.json", {"day_utc": DAY, "validation_status": "INVALID", "runtime_evaluation_hash": runtime_hash})
    _write(root / "reports/submit_boundary_precheck_v1" / DAY / ticket_dir / "submit_boundary_precheck.v1.json", {"day_utc": DAY, "validation_status": "REJECTED", "runtime_evaluation_hash": runtime_hash})
    _write(root / "reports/trade_ticket_lineage_v1" / DAY / ticket_dir / "trade_ticket_lineage.v1.json", {"day_utc": DAY, "lineage_status": "MISSING_CONVERSION", "runtime_evaluation_hash": runtime_hash})
    _write(root / "reports/operator_state_snapshot_v1" / DAY / "operator_state_snapshot.v1.json", {"day_utc": DAY, "validation_status": "VALID", "runtime_evaluation_hash": runtime_hash})
    _write(root / "reports/trade_ticket_projection_v1" / DAY / "trade_ticket_projection.v1.json", {"day_utc": DAY, "validation_status": "VALID", "runtime_evaluation_hash": runtime_hash})


def _seed_valid_economic_chain(root: Path) -> None:
    sleeve_root = root.parent / "truth_sleeves" / "PRIMARY" / "PAPER"
    _write(root / "cash_ledger_v1/snapshots" / DAY / "cash_ledger_snapshot.v1.json", {"schema_id": "C2_CASH_LEDGER_SNAPSHOT_V1", "schema_version": 1, "day_utc": DAY, "status": "OK", "source_type": "STATIC_RISK_BUDGET", "snapshot": {"cash_total_cents": 30000000}})
    _write(root / "positions_v1/snapshots" / DAY / "positions_snapshot.v5.json", {"schema_id": "C2_POSITIONS_SNAPSHOT_V5", "schema_version": 5, "day_utc": DAY, "status": "OK", "source_type": "SIMULATION_LEDGER", "items": [], "reason_codes": ["BUNDLE_A_CANONICAL_STATE_V5", "BROKER_STATEMENT_MISSING_INTERNAL_STATE_ONLY"], "reconciliation": {"positions_status": "UNKNOWN", "broker_statement_present": False}})
    _write(root / "position_lifecycle_v2" / DAY / "position_lifecycle_snapshot.v2.json", {"schema_id": "position_lifecycle_snapshot_v2", "day_utc": DAY, "status": "OK", "validation_status": "VALID"})
    _write(root / "accounting_v2/nav" / DAY / "nav.v2.json", {"schema_id": "accounting_nav_v2", "day_utc": DAY, "status": "OK", "validation_status": "VALID"})
    _write(root / "reports/economic_state_build_v1" / DAY / "ctx" / "economic_state_build.v1.json", {"day_utc": DAY, "closure_status": "COMPLETE", "validation_status": "VALID"})
    _write(sleeve_root / "economic_state_package_v1" / DAY / "ctx" / "economic_state_package.v1.json", {"day_utc": DAY, "validation_status": "VALID", "sealed": True, "mode": "PAPER", "sleeve_id": "PRIMARY", "account_id": "DUO847203", "operation_type": "fresh_paper_entry_v1"})


def test_dependency_map_includes_required_nodes_and_cash_root(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_minimal_truth(root)
    payload = build_dependency_map_v1(truth_root=root, day_utc=DAY)
    assert not validate_dependency_map_v1(payload)
    node_ids = {node["node_id"] for node in payload["nodes"]}
    assert "cash_ledger_snapshot" in node_ids
    assert "manual_capture_save_endpoint" in node_ids
    assert any(row["node_id"] == "cash_ledger_snapshot" for row in payload["root_blockers"])


def test_cycle_detection_reports_cycle() -> None:
    cycles = detect_cycles_v1([{"from": "a", "to": "b"}, {"from": "b", "to": "a"}])
    assert cycles


def test_manual_save_requires_submit_boundary_and_phasec_not_authority(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_minimal_truth(root)
    payload = build_dependency_map_v1(truth_root=root, day_utc=DAY)
    by_id = {node["node_id"]: node for node in payload["nodes"]}
    assert by_id["candidate_identity_set"]["status"] == "VALID"
    assert by_id["manual_capture_save_endpoint"]["status"] == "REJECTED"
    assert by_id["submit_boundary_precheck"]["status"] == "REJECTED"
    assert payload["assertions"]["manual_save_requires_submit_boundary"] is True


def test_static_paper_empty_positions_not_root_blocker(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_minimal_truth(root)
    _write(root / "cash_ledger_v1/snapshots" / DAY / "cash_ledger_snapshot.v1.json", {"schema_id":"C2_CASH_LEDGER_SNAPSHOT_V1","schema_version":1,"day_utc":DAY,"status":"OK","source_type":"STATIC_RISK_BUDGET","snapshot":{"cash_total_cents":30000000}})
    _write(root / "positions_v1/snapshots" / DAY / "positions_snapshot.v5.json", {"schema_id":"C2_POSITIONS_SNAPSHOT_V5","schema_version":5,"day_utc":DAY,"status":"OK","source_type":"SIMULATION_LEDGER","items":[],"reason_codes":["BUNDLE_A_CANONICAL_STATE_V5","BROKER_STATEMENT_MISSING_INTERNAL_STATE_ONLY"],"reconciliation":{"positions_status":"UNKNOWN","broker_statement_present":False}})
    payload = build_dependency_map_v1(truth_root=root, day_utc=DAY)
    by_id = {node["node_id"]: node for node in payload["nodes"]}
    assert by_id["positions_snapshot"]["status"] == "VALID"
    assert by_id["positions_snapshot"]["positions_source_classification"] == "EMPTY_POSITIONS_STATIC_PAPER"
    assert all(row["node_id"] != "positions_snapshot" for row in payload["root_blockers"])


def test_target_day_hidden_dependency_codes_are_explicit(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_minimal_truth(root)
    _write(root / "target_day_admission_v1" / f"{DAY}.json", {
        "target_day": DAY,
        "admission_status": "BLOCKED",
        "validation_status": "REJECTED",
        "blocking_reason_codes": ["HIDDEN_DEPENDENCY_DETECTED", "PARTIAL_BUILD", "REQUIRED_GATE_FAIL"],
        "hidden_dependency_check_result": {"status":"FAIL", "undeclared_dependency_artifacts":["global_kill_switch_state_v1"]},
    })
    payload = build_dependency_map_v1(truth_root=root, day_utc=DAY)
    by_id = {node["node_id"]: node for node in payload["nodes"]}
    assert by_id["target_day_admission"]["status"] == "REJECTED"
    assert "TARGET_DAY_ADMISSION_HIDDEN_DEPENDENCY" in by_id["target_day_admission"]["exact_blocker_reason"]
    assert "TARGET_DAY_ADMISSION_PARTIAL_BUILD" in by_id["target_day_admission"]["target_day_admission_blocker_states"]


def test_engine_activity_authorization_appears_in_graph_and_dot(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_minimal_truth(root)
    payload = build_dependency_map_v1(truth_root=root, day_utc=DAY)
    node_ids = {node["node_id"] for node in payload["nodes"]}
    assert "engine_activity_authorization" in node_ids
    assert {"from": "economic_state_package", "to": "engine_activity_authorization"} in payload["edges"]
    assert {"from": "engine_activity_authorization", "to": "trade_submit_readiness"} in payload["edges"]
    dot = render_dot_v1(payload)
    assert '"engine_activity_authorization"' in dot
    assert '"economic_state_package" -> "engine_activity_authorization"' in dot




def test_dependency_map_focus_prefers_active_current_ticket_over_stale_default(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_minimal_truth(root)
    active_ticket = "ticket:activecurrent1234567890"
    active_dir = active_ticket.replace(":", "_")
    _write(root / "reports/trade_ticket_lineage_v1" / DAY / active_dir / "trade_ticket_lineage.v1.json", {"day_utc": DAY, "ticket_id": active_ticket, "lineage_status": "ACTIVE_CURRENT", "runtime_evaluation_hash": "a"*64})
    _write(root / "reports/submit_boundary_precheck_v1" / DAY / active_dir / "submit_boundary_precheck.v1.json", {"day_utc": DAY, "ticket_id": active_ticket, "validation_status": "VALIDATED", "runtime_evaluation_hash": "a"*64})
    payload = build_dependency_map_v1(truth_root=root, day_utc=DAY)
    assert payload["focus"]["ticket_id"] == active_ticket
    by_id = {node["node_id"]: node for node in payload["nodes"]}
    assert active_dir in by_id["trade_ticket_lineage"]["output_paths"][0]
    assert by_id["manual_capture_save_endpoint"]["status"] == "VALID"


def test_ib_handshake_is_broker_submit_only_not_manual_capture_gate(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_minimal_truth(root)
    _seed_valid_economic_chain(root)
    sleeve_root = root.parent / "truth_sleeves" / "PRIMARY" / "PAPER"
    _write(root / "risk_v1" / "kill_switch_v1" / DAY / "global_kill_switch_state.v1.json", {"schema_id":"global_kill_switch_state","day_utc":DAY,"state":"INACTIVE","allow_entries":True})
    _write(sleeve_root / "engine_activity_v1" / "authorization_v1" / DAY / f"{DOW_INTENT_HASH}.authorization.v1.json", {"schema_id":"engine_activity_authorization","day_utc":DAY,"status":"AUTHORIZED","authorization_status":"AUTHORIZED","validation_status":"VALID","runtime_evaluation_hash":"a"*64})
    payload = build_dependency_map_v1(truth_root=root, day_utc=DAY)
    by_id = {node["node_id"]: node for node in payload["nodes"]}
    assert by_id["ib_api_handshake"]["status"] == "MISSING"
    assert by_id["ib_api_handshake"]["ib_api_handshake_classification"] == "REQUIRED_FOR_PAPER_BROKER_SIM"
    assert by_id["ib_api_handshake"]["blocks"]["conversion"] is False
    assert by_id["ib_api_handshake"]["blocks"]["manual_capture_save"] is False
    assert by_id["ib_api_handshake"]["blocks"]["broker_submit"] is True
    assert by_id["trade_submit_readiness"]["blocks"]["manual_capture_save"] is False
    assert by_id["execution_package_build"]["upstream_dependencies"] == ["candidate_identity_set", "global_context", "economic_state_package", "capital_authority_allocation", "engine_activity_authorization", "global_kill_switch_state"]
    assert payload["assertions"]["ib_not_required_for_manual_capture"] is True
    assert payload["assertions"]["broker_readiness_not_required_for_manual_capture"] is True


def test_map_fails_when_downstream_read_dependency_missing_from_graph(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_minimal_truth(root)
    payload = build_dependency_map_v1(truth_root=root, day_utc=DAY)
    payload["nodes"] = [node for node in payload["nodes"] if node["node_id"] != "engine_activity_authorization"]
    payload["map_completeness"] = map_completeness_v1(payload["nodes"])
    failures = validate_dependency_map_v1(payload)
    assert payload["map_completeness"]["status"] == "FAIL"
    assert any(item.startswith("HIDDEN_DEPENDENCY_DETECTED:engine_activity_authorization") for item in failures)


def test_root_blockers_include_missing_engine_authorization_when_upstream_clear(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_minimal_truth(root)
    _seed_valid_economic_chain(root)
    payload = build_dependency_map_v1(truth_root=root, day_utc=DAY)
    assert any(row["node_id"] == "engine_activity_authorization" for row in payload["root_blockers"])
