from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.human_reviewed_paper_mode_v1 import (
    build_paper_review_queue_v1,
    record_paper_trade_receipt_v1,
    write_candidate_review_packet_v1,
    write_paper_review_queue_v1,
)
from ops.aegis.paper_operator_projection_v1 import build_paper_operator_projection_v1
from ops.aegis.paper_session_ledger_v1 import (
    append_paper_session_event_v1,
    resolve_scheduled_paper_session_v1,
    read_paper_session_ledger_v1,
)
from ops.aegis.trade_lifecycle.paper_trade_construction_v1 import build_and_write_paper_trade_construction_v1
from ops.tools.authorize_aegis_paper_open_v1 import main as authorize_main

DAY = "2026-05-28"
GENERATED_AT_1053 = "2026-05-28T14:53:37Z"
SESSION = "PAPER-2026-05-28-0950"


def _write(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _candidate(idx: int = 0, *, session_id: str = SESSION) -> dict:
    return {
        "paper_session_id": session_id,
        "candidate_id": f"candidate-{idx}",
        "symbol": "QQQ",
        "sleeve_id": "C2_TEST",
        "direction": "LONG",
        "entry_reference_price": "100.00",
        "entry_reference_price_source": "market_data_inputs_v1",
        "paper_trade_eligible": True,
        "live_trade_eligible": False,
    }


def _seed_packet_queue_market(root: Path, *, packet_session: str = SESSION) -> None:
    packet = {
        "schema_id": "candidate_review_packet",
        "schema_version": "v1",
        "day_utc": DAY,
        "generated_at_utc": GENERATED_AT_1053,
        "run_timestamp_utc": GENERATED_AT_1053,
        "paper_session_id": packet_session,
        "candidate_count": 1,
        "review_candidates": [_candidate(session_id=packet_session)],
        "safety": {"paper_only": True, "broker_submit_transmit_allowed": False, "autonomous_execution_allowed": False},
    }
    write_candidate_review_packet_v1(truth_root=root, day_utc=DAY, payload=packet)
    queue = build_paper_review_queue_v1(truth_root=root, day_utc=DAY, packet_payload=packet)
    write_paper_review_queue_v1(truth_root=root, day_utc=DAY, payload=queue)
    _write(root / "reports" / "market_data_inputs_v1" / DAY / "market_data_inputs.v1.json", {"schema_id": "market_data_inputs", "day_utc": DAY, "generated_at_utc": GENERATED_AT_1053, "validation_status": "VALID", "input_records": [{"symbol": "QQQ", "value": "100.00", "day_utc": DAY, "validation_status": "VALID"}]})


def _seed_runtime(root: Path) -> None:
    graph = {
        "PAPER_CANDIDATES_READY": {"allowed": True, "missing_or_blocking_artifacts": []},
        "PAPER_REVIEW_ALLOWED": {"allowed": True, "missing_or_blocking_artifacts": []},
        "PAPER_TRADE_READY": {"allowed": True, "missing_or_blocking_artifacts": []},
        "PAPER_TRADE_CREATION_ALLOWED": {"allowed": True, "missing_or_blocking_artifacts": []},
        "TRADE_ADVICE_ALLOWED": {"allowed": False, "reason": "PARTIAL_CONTEXT / missing advisory dependencies"},
    }
    _write(root / "reports" / "aegis_runtime_truth_kernel_v1" / DAY / "runtime_truth_kernel.v1.json", {"schema_id": "aegis_runtime_truth_kernel", "day_utc": DAY, "runtime_truth_classification": "PARTIAL_CONTEXT", "highest_readiness_layer": "BLOCKED", "dependency_graph": graph, "broker_submit_required": False, "autonomous_execution_allowed": False})


def test_scheduled_0950_session_creates_expected_id(tmp_path: Path) -> None:
    session = resolve_scheduled_paper_session_v1(truth_root=tmp_path, day_utc=DAY)

    assert session["paper_session_id"] == SESSION
    assert session["scheduled_run_time"] == "2026-05-28T09:50:00-04:00"
    assert session["session_timezone"] == "America/New_York"


def test_artifact_generated_at_1053_keeps_0950_session(tmp_path: Path) -> None:
    _seed_packet_queue_market(tmp_path, packet_session="PAPER-2026-05-28-1053")
    packet = json.loads((tmp_path / "reports" / "aegis_candidate_review_packet_v1" / DAY / "candidate_review_packet.v1.json").read_text(encoding="utf-8"))
    queue = json.loads((tmp_path / "reports" / "aegis_paper_review_queue_v1" / DAY / "paper_review_queue.v1.json").read_text(encoding="utf-8"))

    assert packet["generated_at_utc"] == GENERATED_AT_1053
    assert packet["paper_session_id"] == SESSION
    assert packet["review_candidates"][0]["paper_session_id"] == SESSION
    assert queue["rows"][0]["paper_session_id"] == SESSION


def test_retry_rebuild_preserves_original_session_id(tmp_path: Path) -> None:
    _seed_packet_queue_market(tmp_path, packet_session="PAPER-2026-05-28-1053")
    append_paper_session_event_v1(truth_root=tmp_path, day_utc=DAY, event_type="PAPER_SESSION_RECONSTRUCTED", source_tool="test", payload={"reconstruction_reason": "test_retry"})
    _seed_packet_queue_market(tmp_path, packet_session=SESSION)
    ledger = read_paper_session_ledger_v1(truth_root=tmp_path, day_utc=DAY)

    assert ledger["sessions"][0]["paper_session_id"] == SESSION
    assert ledger["sessions"][0]["reconstruction_count"] == 1


def test_packet_queue_construction_authorization_receipt_projection_share_session(tmp_path: Path) -> None:
    _seed_packet_queue_market(tmp_path)
    authorize_main(["--truth_root", str(tmp_path), "--day", DAY, "--operator", "test"])
    construction, _ = build_and_write_paper_trade_construction_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=GENERATED_AT_1053)
    queue_path = tmp_path / "reports" / "aegis_paper_review_queue_v1" / DAY / "paper_review_queue.v1.json"
    queue = json.loads(queue_path.read_text(encoding="utf-8"))
    queue["rows"][0]["status"] = "APPROVED_FOR_PAPER"
    _write(queue_path, queue)
    receipt = record_paper_trade_receipt_v1(truth_root=tmp_path, day_utc=DAY, candidate_id="candidate-0", action="BUY", paper_entry_price="100.00", quantity="1", operator="test")
    _seed_runtime(tmp_path)
    projection = build_paper_operator_projection_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=GENERATED_AT_1053)
    auth = json.loads((tmp_path / "reports" / "paper_open_authorization_v1" / DAY / "paper_open_authorization.v1.json").read_text(encoding="utf-8"))

    assert construction["paper_session_id"] == SESSION
    assert construction["constructed_paper_trades"][0]["paper_session_id"] == SESSION
    assert auth["paper_session_id"] == SESSION
    assert receipt["paper_session_id"] == SESSION
    assert projection["sessions"][0]["paper_session_id"] == SESSION
    assert projection["paper_mode"]["status"] == "READY"
    assert projection["advisory_mode"]["status"] == "BLOCKED"
    assert projection["live_mode"]["broker_submit_transmit_allowed"] is False
    assert projection["live_mode"]["autonomous_execution_allowed"] is False


def test_carry_forward_rows_retain_original_session_id(tmp_path: Path) -> None:
    _seed_packet_queue_market(tmp_path)
    queue_path = tmp_path / "reports" / "aegis_paper_review_queue_v1" / DAY / "paper_review_queue.v1.json"
    queue = json.loads(queue_path.read_text(encoding="utf-8"))
    queue["rows"].append({**_candidate(2, session_id="PAPER-2026-05-26-0950"), "originating_day": "2026-05-26", "rollover_status": "CARRIED_FORWARD", "status": "PAPER_POSITION_OPEN"})
    _write(queue_path, queue)
    build_and_write_paper_trade_construction_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=GENERATED_AT_1053)
    _seed_runtime(tmp_path)

    projection = build_paper_operator_projection_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=GENERATED_AT_1053)

    assert projection["current_day_candidates"][0]["paper_session_id"] == SESSION
    assert projection["carry_forward_candidates"][0]["paper_session_id"] == "PAPER-2026-05-26-0950"


def test_projection_displays_official_session_and_later_canonicalization(tmp_path: Path) -> None:
    _seed_packet_queue_market(tmp_path)
    append_paper_session_event_v1(truth_root=tmp_path, day_utc=DAY, event_type="PAPER_TRADES_CONSTRUCTED", source_tool="test", payload={"canonicalized_at": GENERATED_AT_1053}, created_at=GENERATED_AT_1053)
    _seed_runtime(tmp_path)

    projection = build_paper_operator_projection_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=GENERATED_AT_1053)
    session = projection["sessions"][0]

    assert session["paper_session_id"] == SESSION
    assert session["scheduled_run_time"] == "2026-05-28T09:50:00-04:00"
    assert session["canonicalized_at"] == GENERATED_AT_1053
