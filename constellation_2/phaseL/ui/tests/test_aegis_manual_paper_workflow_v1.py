from __future__ import annotations

import json
from pathlib import Path

from constellation_2.phaseL.ui.server import run_ops_dashboard_v1 as server
from ops.aegis.candidate_lifecycle_projection_v1 import build_candidate_lifecycle_projection_v1, build_and_write_candidate_lifecycle_projection_v1
from ops.aegis.paper_position_ledger_v1 import paper_position_ledger_path_v1
from ops.aegis.operator_command_lifecycle_v1 import (
    candidate_decision_ledger_path_v1,
    paper_entry_receipts_path_v1,
    paper_exit_receipts_path_v1,
    process_command_inbox_v1,
    record_operator_command_v1,
)

DAY = "2026-05-28"
SESSION = "PAPER-2026-05-28-0950"
CANDIDATE_ID = "candidate-manual-1"


def _write(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _seed_current_candidate(root: Path) -> None:
    candidate = {
        "candidate_id": CANDIDATE_ID,
        "candidate_contract_id": CANDIDATE_ID,
        "paper_session_id": SESSION,
        "symbol": "QQQ",
        "direction": "LONG",
        "entry_reference_price": "100.00",
        "entry_price": "100.00",
        "stop_price": "95.00",
        "quantity": "2",
        "rollover_status": "CURRENT_DAY",
        "status": "AWAITING_REVIEW",
    }
    _write(
        root / "reports" / "aegis_candidate_review_packet_v1" / DAY / "candidate_review_packet.v1.json",
        {
            "schema_id": "candidate_review_packet",
            "day_utc": DAY,
            "paper_session_id": SESSION,
            "candidate_count": 1,
            "review_candidates": [candidate],
            "safety": {"trade_advice_allowed": False, "broker_submit_transmit_allowed": False, "autonomous_execution_allowed": False},
        },
    )
    _write(
        root / "reports" / "aegis_paper_review_queue_v1" / DAY / "paper_review_queue.v1.json",
        {
            "schema_id": "aegis_paper_review_queue",
            "day_utc": DAY,
            "paper_session_id": SESSION,
            "rows": [candidate],
        },
    )
    _write(
        root / "reports" / "paper_trade_construction_v1" / DAY / "paper_trade_construction.v1.json",
        {
            "schema_id": "paper_trade_construction",
            "day_utc": DAY,
            "paper_session_id": SESSION,
            "constructed_paper_trades": [
                {
                    "candidate_id": CANDIDATE_ID,
                    "candidate_contract_id": CANDIDATE_ID,
                    "paper_session_id": SESSION,
                    "symbol": "QQQ",
                    "direction": "LONG",
                    "entry_price": "100.00",
                    "stop_price": "95.00",
                    "quantity": "2",
                    "construction_status": "CONSTRUCTED",
                }
            ],
            "skipped_candidates": [],
        },
    )
    _write(
        root / "reports" / "aegis_signal_evidence_boundary_v1" / DAY / "signal_evidence_boundary.v1.json",
        {
            "schema_id": "aegis_signal_evidence_boundary",
            "day_utc": DAY,
            "paper_session_id": SESSION,
            "current_session_candidate_count": 1,
            "signal_evidence_present_count": 1,
            "rejected_intent_count": 0,
            "boundary_rows": [
                {
                    "candidate_id": CANDIDATE_ID,
                    "candidate_contract_id": CANDIDATE_ID,
                    "paper_session_id": SESSION,
                    "symbol": "QQQ",
                    "boundary_status": "SIGNAL_EVIDENCE_PRESENT",
                    "in_output_intents": True,
                    "in_rejected_intents": False,
                    "boundary_reason": "Final output intent has signal evidence.",
                }
            ],
        },
    )
    _write(
        root / "reports" / "aegis_runtime_truth_kernel_v1" / DAY / "runtime_truth_kernel.v1.json",
        {
            "schema_id": "aegis_runtime_truth_kernel",
            "day_utc": DAY,
            "runtime_truth_classification": "PARTIAL_CONTEXT",
            "dependency_graph": {"PAPER_TRADE_CREATION_ALLOWED": {"allowed": True}, "TRADE_ADVICE_ALLOWED": {"allowed": False}},
            "broker_submit_required": False,
            "autonomous_execution_allowed": False,
        },
    )


def _seed_incomplete_current_candidate(root: Path) -> None:
    _seed_current_candidate(root)
    queue_path = root / "reports" / "aegis_paper_review_queue_v1" / DAY / "paper_review_queue.v1.json"
    construction_path = root / "reports" / "paper_trade_construction_v1" / DAY / "paper_trade_construction.v1.json"
    queue = json.loads(queue_path.read_text(encoding="utf-8"))
    queue["rows"][0].pop("stop_price", None)
    queue["rows"][0].pop("quantity", None)
    _write(queue_path, queue)
    construction = json.loads(construction_path.read_text(encoding="utf-8"))
    construction["constructed_paper_trades"][0].pop("stop_price", None)
    construction["constructed_paper_trades"][0].pop("quantity", None)
    _write(construction_path, construction)


def _record(root: Path, command_type: str, **payload: object) -> str:
    command, _ = record_operator_command_v1(
        truth_root=root,
        day_utc=DAY,
        body={
            "command_type": command_type,
            "candidate_id": CANDIDATE_ID,
            "candidate_contract_id": CANDIDATE_ID,
            "paper_session_id": SESSION,
            "day_utc": DAY,
            "source_ui": "positions_today_candidates",
            "payload": {"symbol": "QQQ", **payload},
        },
    )
    return str(command["command_id"])


def _row(root: Path) -> dict:
    projection = build_candidate_lifecycle_projection_v1(truth_root=root, day_utc=DAY)
    return next(row for row in projection["current_session_candidates"] if row["candidate_id"] == CANDIDATE_ID)


def test_manual_paper_workflow_writes_decision_entry_exit_ledgers_and_keeps_candidate_visible(tmp_path: Path) -> None:
    _seed_current_candidate(tmp_path)

    generated = build_candidate_lifecycle_projection_v1(truth_root=tmp_path, day_utc=DAY)
    assert generated["summary"]["current_session_total"] == 1
    assert generated["current_session_candidates"][0]["candidate_lifecycle_state"] == "GENERATED"
    assert generated["current_session_candidates"][0]["allowed_actions"] == ["CONFIRM_CAPTURED", "MARK_NOT_CAPTURED", "DEFER", "DETAILS"]

    entry_command = _record(tmp_path, "CONFIRM_CANDIDATE_CAPTURED", planned_entry="100.00", actual_entry="100.25", planned_stop="95.00", actual_stop="95.00", quantity="2")
    entry_result = process_command_inbox_v1(truth_root=tmp_path, day_utc=DAY)
    entry_row = _row(tmp_path)
    entry_receipts = json.loads(paper_entry_receipts_path_v1(truth_root=tmp_path, day_utc=DAY).read_text(encoding="utf-8"))["receipts"]
    assert entry_result["processed_count"] == 1
    assert entry_row["candidate_lifecycle_state"] == "POSITION_OPEN"
    assert entry_row["allowed_actions"] == ["VIEW_CORRECT_CAPTURE", "VIEW_ENTRY_RECEIPT", "VIEW_POSITION", "DETAILS"]
    assert entry_receipts[-1]["event_type"] == "PAPER_ENTRY_RECORDED"
    assert entry_receipts[-1]["command_id"] == entry_command
    paper_position_ledger = json.loads(paper_position_ledger_path_v1(truth_root=tmp_path, day_utc=DAY).read_text(encoding="utf-8"))
    assert paper_position_ledger["open_position_count"] == 1
    assert paper_position_ledger["open_positions"][0]["candidate_id"] == CANDIDATE_ID
    assert paper_position_ledger["open_positions"][0]["source_receipt"]["event_type"] == "PAPER_ENTRY_RECORDED"
    projection_after_entry = build_candidate_lifecycle_projection_v1(truth_root=tmp_path, day_utc=DAY)
    assert projection_after_entry["summary"]["current_session_total"] == 1
    assert projection_after_entry["summary"]["open"] == 1
    assert projection_after_entry["open_paper_positions"][0]["candidate_id"] == CANDIDATE_ID

    exit_command = _record(tmp_path, "RECORD_PAPER_EXIT", exit_price="101.00", exit_timestamp_utc="2026-05-28T20:00:00Z", exit_reason="TEST_CLOSE")
    exit_result = process_command_inbox_v1(truth_root=tmp_path, day_utc=DAY)
    exit_row = _row(tmp_path)
    exit_receipts = json.loads(paper_exit_receipts_path_v1(truth_root=tmp_path, day_utc=DAY).read_text(encoding="utf-8"))["receipts"]
    final_projection = build_candidate_lifecycle_projection_v1(truth_root=tmp_path, day_utc=DAY)
    assert exit_result["processed_count"] == 1
    assert exit_row["candidate_lifecycle_state"] == "POSITION_CLOSED"
    assert exit_row["allowed_actions"] == ["VIEW_CORRECT_CAPTURE", "VIEW_ENTRY_RECEIPT", "VIEW_EXIT_RECEIPT", "VIEW_POSITION_HISTORY", "DETAILS"]
    assert exit_receipts[-1]["event_type"] == "PAPER_EXIT_RECORDED"
    assert exit_receipts[-1]["command_id"] == exit_command
    assert final_projection["summary"]["current_session_total"] == 1
    assert final_projection["summary"]["closed"] == 1
    assert final_projection["closed_paper_positions"][0]["candidate_id"] == CANDIDATE_ID
    assert final_projection["current_session_candidates"][0]["candidate_id"] == CANDIDATE_ID


def test_reject_and_defer_keep_candidate_visible_without_entry_actions(tmp_path: Path) -> None:
    _seed_current_candidate(tmp_path)
    _record(tmp_path, "DEFER_CANDIDATE", notes="wait")
    process_command_inbox_v1(truth_root=tmp_path, day_utc=DAY)
    deferred = _row(tmp_path)
    assert deferred["candidate_lifecycle_state"] == "DEFERRED"
    assert deferred["allowed_actions"] == ["CONFIRM_CAPTURED", "MARK_NOT_CAPTURED", "DETAILS"]

    _record(tmp_path, "MARK_CANDIDATE_NOT_CAPTURED", notes="no")
    process_command_inbox_v1(truth_root=tmp_path, day_utc=DAY)
    rejected = _row(tmp_path)
    assert rejected["candidate_lifecycle_state"] == "REJECTED"
    assert rejected["allowed_actions"] == ["VIEW_CORRECT_DECISION", "REOPEN", "DETAILS"]
    assert build_candidate_lifecycle_projection_v1(truth_root=tmp_path, day_utc=DAY)["summary"]["current_session_total"] == 1


def test_command_processor_is_idempotent_for_recorded_entry(tmp_path: Path) -> None:
    _seed_current_candidate(tmp_path)
    _record(tmp_path, "CONFIRM_CANDIDATE_CAPTURED", actual_entry="100.00", actual_stop="95.00", quantity="2")
    process_command_inbox_v1(truth_root=tmp_path, day_utc=DAY)
    receipt_count = len(json.loads(paper_entry_receipts_path_v1(truth_root=tmp_path, day_utc=DAY).read_text(encoding="utf-8"))["receipts"])

    second = process_command_inbox_v1(truth_root=tmp_path, day_utc=DAY)
    second_receipt_count = len(json.loads(paper_entry_receipts_path_v1(truth_root=tmp_path, day_utc=DAY).read_text(encoding="utf-8"))["receipts"])
    paper_position_ledger = json.loads(paper_position_ledger_path_v1(truth_root=tmp_path, day_utc=DAY).read_text(encoding="utf-8"))
    assert second["processed_count"] == 0
    assert second_receipt_count == receipt_count
    assert paper_position_ledger["event_count"] == 1
    assert paper_position_ledger["open_position_count"] == 1




def test_operator_cockpit_uses_prebuilt_position_artifacts_without_rebuild() -> None:
    root = Path(__file__).resolve().parents[4]
    source = (root / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py").read_text(encoding="utf-8")
    block = source.split("def _operator_cockpit_payload", 1)[1].split("def _aegis_status", 1)[0]

    assert "read_operator_state_snapshot_v1" in block
    assert "paper_operator_projection_path_v1" in block
    assert "candidate_lifecycle_projection_path_v1" in block
    assert "signal_evidence_boundary_path_v1" in block
    assert "operator_cockpit_timing_ms" in block
    assert "build_and_write_paper_operator_projection_v1" not in block
    assert "build_and_write_candidate_lifecycle_projection_v1" not in block
    assert "load_or_build_operator_state_snapshot_response_v1" not in block


def test_portal_smoke_timeout_handles_comprehensive_cockpit_payload() -> None:
    root = Path(__file__).resolve().parents[4]
    source = (root / "ops/tools/run_aegis_portal_smoke_v1.py").read_text(encoding="utf-8")

    assert "--timeout-seconds" in source
    assert "default=30.0" in source
    assert "timeout_seconds=float(args.timeout_seconds)" in source
    assert "except (TimeoutError, URLError)" in source


def test_positions_lightweight_payload_shows_only_signal_evidence_output_candidates(tmp_path: Path) -> None:
    _seed_current_candidate(tmp_path)
    queue_path = tmp_path / "reports" / "aegis_paper_review_queue_v1" / DAY / "paper_review_queue.v1.json"
    queue = json.loads(queue_path.read_text(encoding="utf-8"))
    rejected_rows = [
        {
            "candidate_id": "candidate-rejected-aal",
            "candidate_contract_id": "candidate-rejected-aal",
            "paper_session_id": SESSION,
            "symbol": "AAL",
            "direction": "LONG",
            "entry_price": "10.00",
            "stop_price": "9.00",
            "quantity": "1",
            "rollover_status": "CURRENT_DAY",
            "status": "AWAITING_REVIEW",
        },
        {
            "candidate_id": "candidate-rejected-amd",
            "candidate_contract_id": "candidate-rejected-amd",
            "paper_session_id": SESSION,
            "symbol": "AMD",
            "direction": "LONG",
            "entry_price": "100.00",
            "stop_price": "95.00",
            "quantity": "1",
            "rollover_status": "CURRENT_DAY",
            "status": "AWAITING_REVIEW",
        },
    ]
    queue["rows"].extend(rejected_rows)
    _write(queue_path, queue)
    build_and_write_candidate_lifecycle_projection_v1(truth_root=tmp_path, day_utc=DAY)
    _write(
        tmp_path / "reports" / "aegis_signal_evidence_boundary_v1" / DAY / "signal_evidence_boundary.v1.json",
        {
            "schema_id": "aegis_signal_evidence_boundary",
            "day_utc": DAY,
            "paper_session_id": SESSION,
            "current_session_candidate_count": 3,
            "signal_evidence_present_count": 1,
            "rejected_intent_count": 2,
            "boundary_rows": [
                {"candidate_id": CANDIDATE_ID, "candidate_contract_id": CANDIDATE_ID, "paper_session_id": SESSION, "symbol": "QQQ", "boundary_status": "SIGNAL_EVIDENCE_PRESENT", "in_output_intents": True, "in_rejected_intents": False},
                {"candidate_id": "candidate-rejected-aal", "candidate_contract_id": "candidate-rejected-aal", "paper_session_id": SESSION, "symbol": "AAL", "boundary_status": "SIGNAL_EVIDENCE_REJECTED_INTENT", "in_output_intents": False, "in_rejected_intents": True, "boundary_reason": "Rejected intent lineage only."},
                {"candidate_id": "candidate-rejected-amd", "candidate_contract_id": "candidate-rejected-amd", "paper_session_id": SESSION, "symbol": "AMD", "boundary_status": "SIGNAL_EVIDENCE_REJECTED_INTENT", "in_output_intents": False, "in_rejected_intents": True, "boundary_reason": "Rejected intent lineage only."},
            ],
        },
    )

    payload = server._positions_lightweight_payload_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["summary"]["today_candidates"] == 1
    assert payload["summary"]["output_candidates_captured"] == 1
    assert payload["summary"]["current_session_lineage_count"] == 3
    assert payload["summary"]["rejected_intents_excluded"] == 2
    assert [row["symbol"] for row in payload["today_candidates"]] == ["QQQ"]
    assert "AAL" not in {row["symbol"] for row in payload["today_candidates"]}
    assert "AMD" not in {row["symbol"] for row in payload["today_candidates"]}
    assert "boundary_rows" not in payload



def test_positions_candidate_capture_status_detects_stale_read_model_mismatch(tmp_path: Path) -> None:
    projection_path = tmp_path / "reports" / "aegis_candidate_lifecycle_projection_v1" / DAY / "candidate_lifecycle_projection.v1.json"
    _write(
        projection_path,
        {
            "schema_id": "candidate_lifecycle_projection",
            "day_utc": DAY,
            "paper_session_id": SESSION,
            "summary": {"current_session_total": 26},
            "current_session_candidates": [],
            "open_paper_positions": [],
            "closed_paper_positions": [],
        },
    )
    _write(
        tmp_path / "reports" / "aegis_signal_evidence_boundary_v1" / DAY / "signal_evidence_boundary.v1.json",
        {
            "schema_id": "aegis_signal_evidence_boundary",
            "day_utc": DAY,
            "paper_session_id": SESSION,
            "current_session_candidate_count": 26,
            "signal_evidence_present_count": 26,
            "rejected_intent_count": 0,
            "boundary_rows": [
                {
                    "candidate_id": f"candidate-{idx}",
                    "candidate_contract_id": f"candidate-{idx}",
                    "paper_session_id": SESSION,
                    "symbol": f"SYM{idx}",
                    "boundary_status": "SIGNAL_EVIDENCE_PRESENT",
                    "in_output_intents": True,
                }
                for idx in range(26)
            ],
        },
    )

    payload = server._positions_lightweight_payload_v1(truth_root=tmp_path, day_utc=DAY)

    assert len(payload["today_candidates"]) == 0
    assert payload["summary"]["signal_evidence_present_count"] == 26
    assert payload["candidate_capture_status"]["classification"] == "STALE_READ_MODEL"
    assert payload["candidate_capture_status"]["status"] == "STALE"
    assert payload["candidate_capture_status"]["affected_count"] == 26
    assert "Positions read model" in payload["candidate_capture_status"]["summary"]
    assert payload["candidate_capture_status"]["next_action"]
    assert payload["candidate_capture_status"]["diagnostics_link"] == "/aegis-positions-diagnostics"


def test_positions_zero_display_rows_with_valid_signal_evidence_does_not_show_failed(tmp_path: Path) -> None:
    _write(
        tmp_path / "reports" / "aegis_candidate_lifecycle_projection_v1" / DAY / "candidate_lifecycle_projection.v1.json",
        {
            "schema_id": "candidate_lifecycle_projection",
            "day_utc": DAY,
            "paper_session_id": SESSION,
            "summary": {"current_session_total": 1},
            "current_session_candidates": [],
        },
    )
    _write(
        tmp_path / "reports" / "aegis_signal_evidence_boundary_v1" / DAY / "signal_evidence_boundary.v1.json",
        {
            "schema_id": "aegis_signal_evidence_boundary",
            "day_utc": DAY,
            "paper_session_id": SESSION,
            "current_session_candidate_count": 1,
            "signal_evidence_present_count": 1,
            "boundary_rows": [{"candidate_id": "candidate-live", "symbol": "QQQ", "paper_session_id": SESSION, "boundary_status": "SIGNAL_EVIDENCE_PRESENT"}],
        },
    )

    status = server._positions_lightweight_payload_v1(truth_root=tmp_path, day_utc=DAY)["candidate_capture_status"]

    assert status["status"] != "FAILED"
    assert status["classification"] == "STALE_READ_MODEL"
    assert status["next_action"]


def test_positions_candidate_capture_status_forbids_bare_failed(tmp_path: Path) -> None:
    _write(
        tmp_path / "reports" / "aegis_candidate_lifecycle_projection_v1" / DAY / "candidate_lifecycle_projection.v1.json",
        {"schema_id": "candidate_lifecycle_projection", "day_utc": DAY, "paper_session_id": SESSION, "current_session_candidates": []},
    )

    status = server._positions_lightweight_payload_v1(truth_root=tmp_path, day_utc=DAY)["candidate_capture_status"]

    if status["status"] == "FAILED":
        assert status["classification"]
        assert status["explanation"]
        assert "affected_count" in status
        assert status["next_action"]
    else:
        assert status["classification"] != ""
        assert status["next_action"]


def test_positions_lightweight_payload_excludes_suppressed_duplicate_candidates(tmp_path: Path) -> None:
    _seed_current_candidate(tmp_path)
    build_and_write_candidate_lifecycle_projection_v1(truth_root=tmp_path, day_utc=DAY)
    _write(
        tmp_path / "reports" / "aegis_duplicate_candidate_v1" / DAY / "duplicate_candidate.v1.json",
        {
            "schema_id": "aegis_duplicate_candidate",
            "day_utc": DAY,
            "paper_session_id": SESSION,
            "current_output_candidate_count": 1,
            "suppressed_duplicate_count": 1,
            "duplicate_rows": [
                {
                    "candidate_id": CANDIDATE_ID,
                    "candidate_contract_id": CANDIDATE_ID,
                    "paper_session_id": SESSION,
                    "symbol": "QQQ",
                    "duplicate_scope": "OPEN_POSITION",
                    "duplicate_classification": "DUPLICATE_OPEN_POSITION",
                    "suppressed": True,
                    "reviewable": False,
                    "allowed_actions": ["VIEW_EXISTING_POSITION", "DETAILS"],
                    "blocked_actions": ["CONFIRM_CAPTURED", "MARK_NOT_CAPTURED", "DEFER"],
                    "operator_message": "Duplicate of open position — view existing position.",
                }
            ],
        },
    )

    payload = server._positions_lightweight_payload_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["summary"]["output_candidates_captured"] == 1
    assert payload["summary"]["today_candidates"] == 0
    assert payload["summary"]["suppressed_duplicate_count"] == 1
    assert payload["today_candidates"] == []


def test_positions_lightweight_payload_keeps_improved_add_on_without_normal_capture(tmp_path: Path) -> None:
    _seed_current_candidate(tmp_path)
    build_and_write_candidate_lifecycle_projection_v1(truth_root=tmp_path, day_utc=DAY)
    _write(
        tmp_path / "reports" / "aegis_duplicate_candidate_v1" / DAY / "duplicate_candidate.v1.json",
        {
            "schema_id": "aegis_duplicate_candidate",
            "day_utc": DAY,
            "paper_session_id": SESSION,
            "current_output_candidate_count": 1,
            "suppressed_duplicate_count": 0,
            "duplicate_rows": [
                {
                    "candidate_id": CANDIDATE_ID,
                    "candidate_contract_id": CANDIDATE_ID,
                    "paper_session_id": SESSION,
                    "symbol": "QQQ",
                    "duplicate_scope": "OPEN_POSITION",
                    "duplicate_classification": "IMPROVED_SIGNAL_ADD_ON",
                    "suppressed": False,
                    "reviewable": True,
                    "allowed_actions": ["REVIEW_ADD_ON", "VIEW_EXISTING_POSITION", "REJECT_ADD_ON", "DEFER_ADD_ON", "DETAILS"],
                    "blocked_actions": ["CONFIRM_CAPTURED"],
                    "operator_message": "Improved signal detected — review add-on candidate.",
                }
            ],
        },
    )

    payload = server._positions_lightweight_payload_v1(truth_root=tmp_path, day_utc=DAY)
    row = payload["today_candidates"][0]

    assert payload["summary"]["today_candidates"] == 1
    assert row["duplicate_classification"] == "IMPROVED_SIGNAL_ADD_ON"
    assert row["allowed_actions"] == ["REVIEW_ADD_ON", "VIEW_EXISTING_POSITION", "REJECT_ADD_ON", "DEFER_ADD_ON", "DETAILS"]
    assert "CONFIRM_CAPTURED" in row["blocked_actions"]
    assert row["status_message"] == "Improved signal detected — review add-on candidate."

def test_positions_lightweight_payload_omits_diagnostics_and_does_not_rebuild(tmp_path: Path, monkeypatch) -> None:
    _seed_current_candidate(tmp_path)
    build_and_write_candidate_lifecycle_projection_v1(truth_root=tmp_path, day_utc=DAY)

    from constellation_2.phaseL.ui.server import run_ops_dashboard_v1 as server

    def fail_rebuild(*_args, **_kwargs):
        raise AssertionError("positions GET must read the prebuilt lifecycle projection")

    monkeypatch.setattr(server, "build_and_write_candidate_lifecycle_projection_v1", fail_rebuild)
    payload = server._positions_lightweight_payload_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["ok"] is True
    assert payload["rebuilt_on_request"] is False
    assert payload["diagnostics_omitted"] is True
    assert payload["payload_bytes"] < 50000
    assert "source_paths" not in payload
    assert "command_results" not in payload
    assert "carry_forward_context" not in payload
    assert set(payload) >= {"open_positions", "today_candidates", "closed_positions", "summary"}
    row = payload["today_candidates"][0]
    assert row["candidate_lifecycle_state"] == "GENERATED"
    assert row["allowed_actions"] == ["CONFIRM_CAPTURED", "MARK_NOT_CAPTURED", "DEFER", "DETAILS"]


def test_positions_ui_uses_lightweight_positions_api_and_operator_capture_actions() -> None:
    root = Path(__file__).resolve().parents[4]
    pages = (root / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js").read_text(encoding="utf-8")
    main = (root / "constellation_2/phaseL/ui/static/operator_shell/main.js").read_text(encoding="utf-8")
    domain = (root / "constellation_2/phaseL/ui/static/operator_shell/domain_client/index.js").read_text(encoding="utf-8")
    block = pages.split("async function renderPositionsWorkspace", 1)[1].split("function positionNumber", 1)[0]
    today_actions = pages.split("function renderPositionsTodayCandidateActions", 1)[1].split("function positionsTodayCandidateColumns", 1)[0]

    assert "fetchAegisPositions(routeParams)" in block
    assert "fetchAegisOperatorCockpit(routeParams)" not in block
    assert 'query("/api/aegis/positions", params)' in domain
    assert "renderPositionsPaperOutcomeCards(summary.manualReviewRows)" in block
    assert "Manual review only" in block
    assert "Record Capture" in today_actions
    assert "Record Not Captured" in today_actions
    assert "renderViewCorrectCaptureButton(row)" in today_actions
    assert "No candidate action" not in today_actions
    assert "data-row-command-debug" not in today_actions
    for field in ["click_received_at", "endpoint_status", "last_error"]:
        assert field not in today_actions
    assert "CONFIRM_CANDIDATE_CAPTURED" in pages
    assert "MARK_CANDIDATE_NOT_CAPTURED" in pages
    assert "data-aegis-command-status" in pages
    assert "command submitted" in main
    assert "workflowCommandLabel" in main


def test_positions_action_boundaries_keep_exit_action_on_open_positions_only() -> None:
    root = Path(__file__).resolve().parents[4]
    pages = (root / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js").read_text(encoding="utf-8")
    today_actions = pages.split("function renderPositionsTodayCandidateActions", 1)[1].split("function positionsTodayCandidateColumns", 1)[0]
    open_table = pages.split("function renderPositionsOwnershipTable", 1)[1].split("function renderReadOnlyPositionAction", 1)[0]
    closed_table = pages.split("function renderClosedPositionsTable", 1)[1].split("function positionsTodayCandidateDetails", 1)[0]

    assert "RECORD_EXIT" not in today_actions
    assert "renderRecordExitWorkflowButton" not in pages
    assert "renderRecordExitWorkflowModal" not in pages
    assert "View Position" in today_actions
    assert "View Position History" in today_actions
    assert "No candidate action:" not in today_actions

    assert "renderOpenPaperPositionExitButton(row)" in open_table
    assert "View Position" in open_table
    assert "View Entry Receipt" in open_table
    assert "function isOpenPaperPositionRow" in pages
    assert "PAPER_POSITION_OPEN" in pages

    assert "RECORD_PAPER_EXIT" not in closed_table
    assert "renderOpenPaperPositionExitButton" not in closed_table
    assert "View Position History" in closed_table
    assert "View Entry Receipt" in closed_table
    assert "View Exit Receipt" in closed_table


def test_incomplete_candidate_disables_capture_decision_actions_and_rejects_commands(tmp_path: Path) -> None:
    _seed_incomplete_current_candidate(tmp_path)

    projection = build_candidate_lifecycle_projection_v1(truth_root=tmp_path, day_utc=DAY)
    row = projection["current_session_candidates"][0]
    assert row["candidate_lifecycle_state"] == "INCOMPLETE_CANDIDATE"
    assert row["missing_required_fields"] == ["planned_stop", "quantity"]
    assert row["capture_status"] == "Incomplete candidate"
    assert not {"CONFIRM_CAPTURED", "MARK_NOT_CAPTURED", "DEFER"} & set(row["allowed_actions"])
    assert {"CONFIRM_CAPTURED", "MARK_NOT_CAPTURED", "DEFER"}.issubset(set(row["blocked_actions"]))
    assert projection["summary"]["actionable"] == 0
    assert projection["summary"]["incomplete"] == 1
    assert projection["summary"]["ready_for_operator_review"] == 0

    command_id = _record(tmp_path, "CONFIRM_CANDIDATE_CAPTURED", actual_entry="100.00", actual_stop="95.00", quantity="2")
    result = process_command_inbox_v1(truth_root=tmp_path, day_utc=DAY)
    processed = next(item for item in result["processed"] if item["command_id"] == command_id)
    assert processed["status"] == "REJECTED"

    results_path = tmp_path / "reports" / "aegis_command_results_v1" / DAY / "command_results.v1.json"
    command_result = json.loads(results_path.read_text(encoding="utf-8"))["results"][-1]
    assert command_result["error_code"] == "INCOMPLETE_CANDIDATE"
    assert command_result["details"]["missing_required_fields"] == ["planned_stop", "quantity"]
    assert not paper_entry_receipts_path_v1(truth_root=tmp_path, day_utc=DAY).exists()


def test_candidate_readiness_summary_counts_ready_and_incomplete_fields(tmp_path: Path) -> None:
    _seed_current_candidate(tmp_path)
    queue_path = tmp_path / "reports" / "aegis_paper_review_queue_v1" / DAY / "paper_review_queue.v1.json"
    construction_path = tmp_path / "reports" / "paper_trade_construction_v1" / DAY / "paper_trade_construction.v1.json"
    queue = json.loads(queue_path.read_text(encoding="utf-8"))
    construction = json.loads(construction_path.read_text(encoding="utf-8"))
    rows = []
    constructed = []
    for idx in range(25):
        row = dict(queue["rows"][0])
        row["candidate_id"] = f"candidate-ready-{idx}"
        row["candidate_contract_id"] = row["candidate_id"]
        row["symbol"] = f"T{idx:02d}"
        trade = dict(construction["constructed_paper_trades"][0])
        trade["candidate_id"] = row["candidate_id"]
        trade["candidate_contract_id"] = row["candidate_id"]
        trade["symbol"] = row["symbol"]
        if idx >= 2:
            row.pop("stop_price", None)
            row.pop("quantity", None)
            trade.pop("stop_price", None)
            trade.pop("quantity", None)
        rows.append(row)
        constructed.append(trade)
    queue["rows"] = rows
    construction["constructed_paper_trades"] = constructed
    packet_path = tmp_path / "reports" / "aegis_candidate_review_packet_v1" / DAY / "candidate_review_packet.v1.json"
    packet = json.loads(packet_path.read_text(encoding="utf-8"))
    packet["candidate_count"] = 25
    packet["review_candidates"] = rows
    _write(queue_path, queue)
    _write(construction_path, construction)
    _write(packet_path, packet)

    projection = build_candidate_lifecycle_projection_v1(truth_root=tmp_path, day_utc=DAY)
    assert projection["summary"]["current_session_total"] == 25
    assert projection["summary"]["ready_for_operator_review"] == 2
    assert projection["summary"]["incomplete"] == 23
    assert projection["summary"]["actionable"] == 2
    incomplete_rows = [row for row in projection["current_session_candidates"] if row["missing_required_fields"]]
    assert len(incomplete_rows) == 23
    assert all(row["candidate_lifecycle_state"] == "INCOMPLETE_CANDIDATE" for row in incomplete_rows)
    assert all(row["allowed_actions"] == ["DETAILS"] for row in incomplete_rows)
