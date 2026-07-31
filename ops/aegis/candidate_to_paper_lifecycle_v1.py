from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import latest_json_v1, now_utc_v1, write_json_v1
from ops.aegis.paper_position_ledger_v1 import build_paper_position_ledger_v1, paper_position_events_path_v1, position_id_for_candidate_v1, write_paper_position_ledger_v1

REPORT_FAMILY = "aegis_candidate_to_paper_lifecycle_v1"
REPORT_FILENAME = "candidate_to_paper_lifecycle.v1.json"

BLOCKER_REVIEW_QUEUE_MISSING = "REVIEW_QUEUE_MISSING"
BLOCKER_PROMOTION_GATE_BLOCKED = "PROMOTION_GATE_BLOCKED"
BLOCKER_HYPOTHESIS_NOT_ELIGIBLE = "HYPOTHESIS_NOT_ELIGIBLE"
BLOCKER_PAPER_SESSION_MISSING = "PAPER_SESSION_MISSING"
BLOCKER_PAPER_SESSION_BLOCKED = "PAPER_SESSION_BLOCKED"
BLOCKER_PAPER_CONSTRUCTION_FAILED = "PAPER_CONSTRUCTION_FAILED"
BLOCKER_PAPER_LEDGER_WRITE_FAILED = "PAPER_LEDGER_WRITE_FAILED"
BLOCKER_DUPLICATE_POSITION_BLOCKED = "DUPLICATE_POSITION_BLOCKED"
BLOCKER_RISK_RULE_BLOCKED = "RISK_RULE_BLOCKED"
BLOCKER_INSTRUMENT_TYPE_NOT_GOVERNED = "INSTRUMENT_TYPE_NOT_GOVERNED"
BLOCKER_UNKNOWN = "UNKNOWN"
AUTO_PROMOTED_TO_PAPER_TRACKING = "AUTO_PROMOTED_TO_PAPER_TRACKING"
AUTO_PROMOTION_BLOCKED = "AUTO_PROMOTION_BLOCKED"
AUTO_PROMOTION_NOT_ELIGIBLE = "AUTO_PROMOTION_NOT_ELIGIBLE"
AUTO_PROMOTION_ALLOWED = "AUTO_PROMOTION_ALLOWED"


def _safe_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _safe_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _upper(value: Any) -> str:
    return _text(value).upper()


def _sha256(path: Path | None) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest() if path and path.exists() and path.is_file() else ""
    except Exception:
        return ""


def _by_candidate(rows: list[Any], *keys: str) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        candidate_id = ""
        for key in keys or ("candidate_id",):
            candidate_id = _text(row.get(key))
            if candidate_id:
                break
        if candidate_id and candidate_id not in out:
            out[candidate_id] = row
    return out


def _paper_positions_by_candidate(ledger: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for row in _safe_list(ledger.get("positions")) or [*_safe_list(ledger.get("open_positions")), *_safe_list(ledger.get("closed_positions"))]:
        if not isinstance(row, dict):
            continue
        candidate_id = _text(row.get("candidate_id") or row.get("linked_candidate_id"))
        if candidate_id:
            out.setdefault(candidate_id, []).append(row)
    return out


def _outcomes_by_candidate(outcomes: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out = {}
    for row in _safe_list(outcomes.get("outcomes")):
        if not isinstance(row, dict):
            continue
        candidate_id = _text(row.get("candidate_id"))
        if candidate_id:
            out[candidate_id] = row
    return out


def _session_status(session_ledger: dict[str, Any], session_id: str) -> tuple[str, list[str]]:
    if not session_id:
        return "MISSING", [BLOCKER_PAPER_SESSION_MISSING]
    for row in _safe_list(session_ledger.get("sessions")):
        if isinstance(row, dict) and _text(row.get("paper_session_id")) == session_id:
            status = _upper(row.get("status") or "UNKNOWN")
            if status in {"BLOCKED", "FAILED"}:
                return status, [BLOCKER_PAPER_SESSION_BLOCKED]
            return status or "UNKNOWN", []
    return "MISSING", [BLOCKER_PAPER_SESSION_MISSING]


def _construction_status(row: dict[str, Any]) -> tuple[str, list[str]]:
    if not row:
        return "MISSING", [BLOCKER_PAPER_CONSTRUCTION_FAILED]
    status = _upper(row.get("construction_status"))
    missing = _safe_list(row.get("missing_fields"))
    if status == "CONSTRUCTED" and not missing:
        return "CONSTRUCTED", []
    codes = [BLOCKER_PAPER_CONSTRUCTION_FAILED]
    for field in missing:
        code = f"MISSING_{str(field).upper()}"
        if code not in codes:
            codes.append(code)
    return status or "FAILED", codes


def _review_status(packet_row: dict[str, Any], queue_row: dict[str, Any]) -> tuple[str, list[str]]:
    if not packet_row and not queue_row:
        return "MISSING", [BLOCKER_REVIEW_QUEUE_MISSING]
    status = _upper(queue_row.get("status") or packet_row.get("review_status") or "AWAITING_REVIEW")
    if status == "AWAITING_REVIEW":
        return "AWAITING_REVIEW", []
    if status == "APPROVED_FOR_PAPER":
        return "APPROVED_FOR_PAPER", []
    if status in {"REJECTED_BY_OPERATOR", "EXPIRED", "INVALIDATED_BY_RUNTIME"}:
        return status, [BLOCKER_PROMOTION_GATE_BLOCKED, status]
    return status or "UNKNOWN", []


def _promotion_status(queue_row: dict[str, Any], review_status: str, positions: list[dict[str, Any]], auto_status: str = "") -> tuple[str, list[str], bool]:
    if positions:
        if any(str(pos.get("paper_tracking_mode") or "").upper() == "AUTO_PROMOTED_RESEARCH_OBSERVATION" or str(pos.get("current_state") or "").upper() == AUTO_PROMOTED_TO_PAPER_TRACKING for pos in positions):
            return AUTO_PROMOTED_TO_PAPER_TRACKING, [], False
        return "PAPER_POSITION_OPEN", [], False
    if auto_status == AUTO_PROMOTED_TO_PAPER_TRACKING:
        return AUTO_PROMOTED_TO_PAPER_TRACKING, [], False
    if auto_status == AUTO_PROMOTION_BLOCKED:
        return AUTO_PROMOTION_BLOCKED, [BLOCKER_PROMOTION_GATE_BLOCKED, AUTO_PROMOTION_BLOCKED], True
    if auto_status == AUTO_PROMOTION_NOT_ELIGIBLE:
        return AUTO_PROMOTION_NOT_ELIGIBLE, [BLOCKER_PROMOTION_GATE_BLOCKED, AUTO_PROMOTION_NOT_ELIGIBLE], True
    if review_status == "APPROVED_FOR_PAPER":
        return "PROMOTION_ELIGIBLE", [], False
    if review_status == "AWAITING_REVIEW":
        return "BLOCKED_PENDING_OPERATOR_REVIEW", [BLOCKER_PROMOTION_GATE_BLOCKED, "HUMAN_REVIEW_REQUIRED"], True
    codes = [BLOCKER_PROMOTION_GATE_BLOCKED]
    if review_status:
        codes.append(review_status)
    return "BLOCKED", codes, True


def _classification(blockers: list[str]) -> str:
    for code in [
        BLOCKER_REVIEW_QUEUE_MISSING,
        BLOCKER_PROMOTION_GATE_BLOCKED,
        BLOCKER_HYPOTHESIS_NOT_ELIGIBLE,
        BLOCKER_PAPER_SESSION_MISSING,
        BLOCKER_PAPER_SESSION_BLOCKED,
        BLOCKER_PAPER_CONSTRUCTION_FAILED,
        BLOCKER_PAPER_LEDGER_WRITE_FAILED,
        BLOCKER_DUPLICATE_POSITION_BLOCKED,
        BLOCKER_RISK_RULE_BLOCKED,
        BLOCKER_INSTRUMENT_TYPE_NOT_GOVERNED,
    ]:
        if code in blockers:
            return code
    return BLOCKER_UNKNOWN if blockers else "NONE"


def _is_expected_safety_behavior(blockers: list[str]) -> bool:
    return bool(set(blockers).intersection({BLOCKER_PROMOTION_GATE_BLOCKED, "HUMAN_REVIEW_REQUIRED", AUTO_PROMOTION_BLOCKED, AUTO_PROMOTION_NOT_ELIGIBLE, "REJECTED_BY_OPERATOR", "EXPIRED", "INVALIDATED_BY_RUNTIME", BLOCKER_RISK_RULE_BLOCKED, BLOCKER_DUPLICATE_POSITION_BLOCKED}))


def _auto_promotion_decision(candidate: dict[str, Any], construction_row: dict[str, Any], paper_session_status: str, session_blockers: list[str], ledger_path: Path | None, runtime: dict[str, Any]) -> tuple[str, list[str]]:
    not_eligible: list[str] = []
    blocked: list[str] = []
    if _upper(candidate.get("contract_validation_status")) != "VALID":
        not_eligible.append("VALID_CANDIDATE_CONTRACT_MISSING")
    if _upper(candidate.get("entry_reference_price_certification_status")) != "CERTIFIED":
        not_eligible.append("ENTRY_REFERENCE_PRICE_UNCERTIFIED")
    if _upper(candidate.get("governance_status")) != "GOVERNED":
        not_eligible.append(BLOCKER_INSTRUMENT_TYPE_NOT_GOVERNED)
    for field, code in (("hypothesis_id", "MISSING_HYPOTHESIS_LINKAGE"), ("sleeve_id", "MISSING_SLEEVE_LINKAGE"), ("thesis_id", "MISSING_THESIS_LINKAGE")):
        if not _text(candidate.get(field)):
            not_eligible.append(code)
    if not_eligible:
        return AUTO_PROMOTION_NOT_ELIGIBLE, [AUTO_PROMOTION_NOT_ELIGIBLE, *sorted(set(not_eligible))]
    if session_blockers or paper_session_status in {"MISSING", "BLOCKED", "FAILED"}:
        blocked.extend(session_blockers or [BLOCKER_PAPER_SESSION_BLOCKED])
    if not ledger_path:
        blocked.append("PAPER_LEDGER_MISSING")
    construction_status, construction_blockers = _construction_status(construction_row)
    if construction_status != "CONSTRUCTED":
        blocked.extend(construction_blockers)
    graph = runtime.get("dependency_graph") if isinstance(runtime.get("dependency_graph"), dict) else {}
    for cap in ("PAPER_CANDIDATES_READY", "PAPER_REVIEW_ALLOWED"):
        cap_payload = graph.get(cap) if isinstance(graph.get(cap), dict) else {}
        if cap_payload and cap_payload.get("allowed") is not True:
            blocked.append(f"RUNTIME_BLOCKS_{cap}")
    if blocked:
        return AUTO_PROMOTION_BLOCKED, [AUTO_PROMOTION_BLOCKED, *sorted(set(blocked))]
    return AUTO_PROMOTED_TO_PAPER_TRACKING, [AUTO_PROMOTION_ALLOWED]


def _source_paths(paths: dict[str, Path | None]) -> dict[str, str]:
    return {key: str(path or "") for key, path in paths.items()}


def _source_hashes(paths: dict[str, Path | None]) -> dict[str, str]:
    return {key: _sha256(path) for key, path in paths.items() if path}


def build_candidate_to_paper_lifecycle_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    contracts_path, contracts = latest_json_v1(root, "aegis_candidate_contracts_v1", day_utc, "candidate_contracts.v1.json")
    packet_path, packet = latest_json_v1(root, "aegis_candidate_review_packet_v1", day_utc, "candidate_review_packet.v1.json")
    queue_path, queue = latest_json_v1(root, "aegis_paper_review_queue_v1", day_utc, "paper_review_queue.v1.json")
    construction_path, construction = latest_json_v1(root, "paper_trade_construction_v1", day_utc, "paper_trade_construction.v1.json")
    ledger_path, ledger = latest_json_v1(root, "aegis_paper_position_ledger_v1", day_utc, "paper_position_ledger.v1.json")
    outcomes_path, outcomes = latest_json_v1(root, "aegis_outcome_registry_v1", day_utc, "outcome_registry.v1.json")
    session_path, session = latest_json_v1(root, "aegis_paper_session_ledger_v1", day_utc, "paper_session_ledger.v1.json")
    runtime_path, runtime = latest_json_v1(root, "aegis_runtime_truth_kernel_v1", day_utc, "runtime_truth_kernel.v1.json")

    valid_candidates = [row for row in _safe_list(contracts.get("candidate_contracts")) if isinstance(row, dict) and _upper(row.get("contract_validation_status")) == "VALID"]
    packet_by_id = _by_candidate(_safe_list(packet.get("review_candidates")), "candidate_id")
    queue_by_id = _by_candidate(_safe_list(queue.get("rows")), "candidate_id")
    constructed_by_id = _by_candidate(_safe_list(construction.get("constructed_paper_trades")), "candidate_id")
    positions_by_id = _paper_positions_by_candidate(ledger)
    outcomes_by_id = _outcomes_by_candidate(outcomes)

    rows: list[dict[str, Any]] = []
    for candidate in sorted(valid_candidates, key=lambda row: (_text(row.get("sleeve_id")), _text(row.get("symbol")), _text(row.get("candidate_id")))):
        candidate_id = _text(candidate.get("candidate_id"))
        packet_row = packet_by_id.get(candidate_id, {})
        queue_row = queue_by_id.get(candidate_id, {})
        construction_row = constructed_by_id.get(candidate_id, {})
        positions = positions_by_id.get(candidate_id, [])
        outcome = outcomes_by_id.get(candidate_id, {})
        blockers: list[str] = []
        review_status, review_blockers = _review_status(packet_row, queue_row)
        blockers.extend(review_blockers)
        session_id = _text(queue_row.get("paper_session_id") or packet_row.get("paper_session_id") or construction_row.get("paper_session_id") or packet.get("paper_session_id"))
        paper_session_status, session_blockers = _session_status(session, session_id)
        auto_status, auto_reason_codes = ("", [])
        if review_status == "AWAITING_REVIEW":
            auto_status, auto_reason_codes = _auto_promotion_decision(candidate, construction_row, paper_session_status, session_blockers, ledger_path, runtime)
        promotion_status, promotion_blockers, expected_safety = _promotion_status(queue_row, review_status, positions, auto_status)
        blockers.extend(promotion_blockers)
        blockers.extend(session_blockers)
        construction_status, construction_blockers = _construction_status(construction_row)
        if review_status == "APPROVED_FOR_PAPER" or positions:
            blockers.extend(construction_blockers)
        elif construction_status not in {"CONSTRUCTED", "MISSING"}:
            blockers.extend(construction_blockers)
        paper_position_id = _text(positions[0].get("position_id")) if positions else (position_id_for_candidate_v1(candidate_id) if auto_status == AUTO_PROMOTED_TO_PAPER_TRACKING else "")
        if review_status == "APPROVED_FOR_PAPER" and construction_status == "CONSTRUCTED" and not positions:
            blockers.append(BLOCKER_PAPER_LEDGER_WRITE_FAILED)
        if len(positions) > 1:
            blockers.append(BLOCKER_DUPLICATE_POSITION_BLOCKED)
        blockers = sorted({code for code in blockers if code})
        if not blockers and not paper_position_id:
            blockers = [BLOCKER_UNKNOWN]
        classification = _classification(blockers)
        rows.append({
            "candidate_id": candidate_id,
            "raw_signal_id": _text(candidate.get("raw_signal_id") or packet_row.get("raw_signal_id")),
            "sleeve_id": _text(candidate.get("sleeve_id") or packet_row.get("sleeve_id") or queue_row.get("sleeve_id")),
            "hypothesis_id": _text(candidate.get("hypothesis_id") or packet_row.get("hypothesis_id") or queue_row.get("hypothesis_id")),
            "thesis_id": _text(candidate.get("thesis_id") or packet_row.get("thesis_id") or queue_row.get("thesis_id")),
            "symbol": _text(candidate.get("symbol") or packet_row.get("symbol") or queue_row.get("symbol")),
            "direction": _text(candidate.get("direction") or construction_row.get("direction") or packet_row.get("direction") or queue_row.get("direction")),
            "entry_reference_price": _text(candidate.get("entry_reference_price") or construction_row.get("entry_reference_price")),
            "entry_reference_price_certification_status": _text(candidate.get("entry_reference_price_certification_status")),
            "entry_reference_price_certification_id": _text(candidate.get("entry_reference_price_certification_id")),
            "entry_reference_price_timestamp_utc": _text(candidate.get("entry_reference_price_timestamp_utc") or candidate.get("price_timestamp")),
            "candidate_snapshot_timestamp_utc": _text(candidate.get("candidate_snapshot_timestamp_utc") or candidate.get("candidate_snapshot_timestamp")),
            "contract_validation_status": _upper(candidate.get("contract_validation_status")),
            "review_status": review_status,
            "promotion_status": promotion_status,
            "auto_promotion_status": auto_status,
            "auto_promotion_reason_codes": auto_reason_codes,
            "human_approval_status": "HUMAN_APPROVED" if review_status == "APPROVED_FOR_PAPER" else "NOT_HUMAN_APPROVED",
            "paper_tracking_mode": "AUTO_PROMOTED_RESEARCH_OBSERVATION" if promotion_status == AUTO_PROMOTED_TO_PAPER_TRACKING else "",
            "paper_session_id": session_id,
            "paper_session_status": paper_session_status,
            "paper_construction_status": construction_status,
            "paper_trade_id": _text(construction_row.get("paper_trade_id")),
            "suggested_quantity": _text(construction_row.get("suggested_quantity")),
            "suggested_notional": _text(construction_row.get("suggested_notional")),
            "paper_position_id": paper_position_id,
            "outcome_id": _text(outcome.get("outcome_id")),
            "outcome_state": _text(outcome.get("outcome_state")),
            "blocker_classification": classification,
            "blocker_reason_codes": blockers,
            "expected_safety_behavior": bool(expected_safety or _is_expected_safety_behavior(blockers)),
            "repairable_system_issue": bool(classification in {BLOCKER_REVIEW_QUEUE_MISSING, BLOCKER_PAPER_SESSION_MISSING, BLOCKER_PAPER_SESSION_BLOCKED, BLOCKER_PAPER_CONSTRUCTION_FAILED, BLOCKER_PAPER_LEDGER_WRITE_FAILED, BLOCKER_UNKNOWN}),
            "source_artifacts": {
                "candidate_contracts": str(contracts_path or ""),
                "candidate_review_packet": str(packet_path or ""),
                "paper_review_queue": str(queue_path or ""),
                "paper_trade_construction": str(construction_path or ""),
                "paper_position_ledger": str(ledger_path or ""),
                "outcome_registry": str(outcomes_path or ""),
                "paper_session_ledger": str(session_path or ""),
                "runtime_truth_kernel": str(runtime_path or ""),
            },
        })

    current_ids = {row["candidate_id"] for row in rows}
    current_positions = [position for candidate_id in current_ids for position in positions_by_id.get(candidate_id, [])]
    source_paths = {
        "candidate_contracts": contracts_path,
        "candidate_review_packet": packet_path,
        "paper_review_queue": queue_path,
        "paper_trade_construction": construction_path,
        "paper_position_ledger": ledger_path,
        "outcome_registry": outcomes_path,
        "paper_session_ledger": session_path,
    }
    summary = {
        "valid_candidate_contract_count": len(valid_candidates),
        "review_eligible_count": sum(1 for row in rows if row["review_status"] != "MISSING"),
        "promotion_eligible_count": sum(1 for row in rows if row["promotion_status"] == "PROMOTION_ELIGIBLE"),
        "auto_promoted_to_paper_tracking_count": sum(1 for row in rows if row["promotion_status"] == AUTO_PROMOTED_TO_PAPER_TRACKING),
        "human_approved_for_paper_count": sum(1 for row in rows if row["review_status"] == "APPROVED_FOR_PAPER"),
        "auto_promotion_blocked_count": sum(1 for row in rows if row.get("auto_promotion_status") == AUTO_PROMOTION_BLOCKED),
        "auto_promotion_not_eligible_count": sum(1 for row in rows if row.get("auto_promotion_status") == AUTO_PROMOTION_NOT_ELIGIBLE),
        "constructed_paper_trade_count": sum(1 for row in rows if row["paper_construction_status"] == "CONSTRUCTED"),
        "paper_positions_created_count": len(current_positions) + sum(1 for row in rows if row["promotion_status"] == AUTO_PROMOTED_TO_PAPER_TRACKING and row["candidate_id"] not in positions_by_id),
        "blocked_from_paper_count": sum(1 for row in rows if not row["paper_position_id"]),
        "awaiting_review_count": sum(1 for row in rows if row["review_status"] == "AWAITING_REVIEW"),
    }
    return {
        "schema_id": "aegis_candidate_to_paper_lifecycle",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": day_utc,
        "generated_at": now_utc_v1(),
        "summary": summary,
        "rows": rows,
        "blocker_counts": {code: sum(1 for row in rows if code in row.get("blocker_reason_codes", [])) for code in sorted({code for row in rows for code in row.get("blocker_reason_codes", [])})},
        "source_artifacts": _source_paths(source_paths),
        "source_hashes": _source_hashes({**source_paths, "runtime_truth_kernel": runtime_path}),
        "safety": {"trade_advice_allowed": False, "manual_capture_allowed": False, "broker_execution_allowed": False, "autonomous_execution_allowed": False, "paper_only": True},
    }


def write_candidate_to_paper_lifecycle_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    root = Path(truth_root).expanduser().resolve()
    if _materialize_auto_promotion_events(root=root, day_utc=day_utc, payload=payload):
        ledger = build_paper_position_ledger_v1(truth_root=root, day_utc=day_utc)
        write_paper_position_ledger_v1(truth_root=root, day_utc=day_utc, payload=ledger)
        payload = build_candidate_to_paper_lifecycle_v1(truth_root=root, day_utc=day_utc)
    out_dir = root / "reports" / REPORT_FAMILY / day_utc
    path = write_json_v1(out_dir / REPORT_FILENAME, payload)
    return {"json": str(path)}


def _materialize_auto_promotion_events(*, root: Path, day_utc: str, payload: dict[str, Any]) -> bool:
    rows = [row for row in _safe_list(payload.get("rows")) if isinstance(row, dict) and row.get("promotion_status") == AUTO_PROMOTED_TO_PAPER_TRACKING]
    if not rows:
        return False
    path = paper_position_events_path_v1(truth_root=root, day_utc=day_utc)
    existing_events: list[dict[str, Any]] = []
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                item = json.loads(line)
            except Exception:
                continue
            if isinstance(item, dict):
                existing_events.append(item)
    existing_by_id = {str(item.get("event_id") or ""): item for item in existing_events if item.get("event_id")}
    wrote = False
    for row in rows:
        event = _auto_promotion_event(row=row, day_utc=day_utc)
        event_id = event["event_id"]
        existing = existing_by_id.get(event_id)
        if existing is None:
            existing_events.append(event)
            existing_by_id[event_id] = event
            wrote = True
            continue
        if not _text(existing.get("entry_price")) and _text(event.get("entry_price")):
            existing.update(event)
            wrote = True
    if wrote:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            for event in existing_events:
                handle.write(json.dumps(event, sort_keys=True) + "\n")
    return wrote


def _auto_promotion_event(*, row: dict[str, Any], day_utc: str) -> dict[str, Any]:
    candidate_id = _text(row.get("candidate_id"))
    source_artifacts = _safe_dict(row.get("source_artifacts"))
    source_hashes = _safe_dict(row.get("source_hashes")) or {path: _sha256(Path(path)) for path in source_artifacts.values() if path}
    timestamp = _text(row.get("promotion_timestamp") or row.get("candidate_snapshot_timestamp_utc") or row.get("generated_at") or row.get("entry_reference_price_timestamp_utc")) or f"{day_utc}T00:00:00Z"
    quantity = _text(row.get("suggested_quantity") or row.get("quantity") or "1")
    notional = _text(row.get("suggested_notional") or row.get("notional") or row.get("entry_reference_price"))
    return {
        "schema_id": "paper_position_event",
        "schema_version": "v1",
        "event_type": "PAPER_POSITION_OPENED",
        "event_id": f"paper_auto_promotion_opened:{candidate_id}",
        "event_time_utc": timestamp,
        "promotion_timestamp": timestamp,
        "originating_day": day_utc,
        "position_id": position_id_for_candidate_v1(candidate_id),
        "paper_session_id": _text(row.get("paper_session_id")),
        "candidate_id": candidate_id,
        "symbol": _text(row.get("symbol")).upper(),
        "side": "BUY" if _upper(row.get("direction")) in {"", "LONG", "BUY"} else _upper(row.get("direction")),
        "quantity": quantity,
        "notional": notional,
        "entry_price": _text(row.get("entry_reference_price")),
        "entry_time": timestamp,
        "operator": "aegis-paper-auto-promotion-v1",
        "paper_tracking_mode": "AUTO_PROMOTED_RESEARCH_OBSERVATION",
        "current_state": AUTO_PROMOTED_TO_PAPER_TRACKING,
        "human_approval_status": "NOT_HUMAN_APPROVED",
        "auto_promotion_reason_codes": _safe_list(row.get("auto_promotion_reason_codes")) or [AUTO_PROMOTION_ALLOWED],
        "source_artifacts": source_artifacts,
        "source_hashes": source_hashes,
        "source_receipt": {
            "receipt_type": "AUTO_PROMOTED_RESEARCH_OBSERVATION",
            "candidate_id": candidate_id,
            "symbol": _text(row.get("symbol")).upper(),
            "paper_entry_price": _text(row.get("entry_reference_price")),
            "quantity": quantity,
            "notional": notional,
            "timestamp_utc": timestamp,
            "operator": "aegis-paper-auto-promotion-v1",
            "paper_only": True,
            "human_approval_status": "NOT_HUMAN_APPROVED",
            "trade_advice_allowed": False,
            "manual_capture_allowed": False,
            "broker_execution_allowed": False,
            "broker_submit_transmit_allowed": False,
            "autonomous_execution_allowed": False,
            "live_trade_eligible": False,
        },
        "candidate_lineage": {
            "paper_session_id": _text(row.get("paper_session_id")),
            "candidate_id": candidate_id,
            "raw_signal_id": _text(row.get("raw_signal_id")),
            "sleeve_id": _text(row.get("sleeve_id")),
            "hypothesis_id": _text(row.get("hypothesis_id")),
            "thesis_id": _text(row.get("thesis_id")),
            "evidence_paths": [path for path in source_artifacts.values() if path],
            "evidence_hashes": source_hashes,
        },
        "paper_only": True,
        "trade_advice_allowed": False,
        "manual_capture_allowed": False,
        "broker_execution_allowed": False,
        "broker_submit_transmit_allowed": False,
        "autonomous_execution_allowed": False,
        "live_trade_eligible": False,
        "automatic_approval_allowed": False,
    }


def normalized_candidate_to_paper_lifecycle_v1(payload: dict[str, Any]) -> dict[str, Any]:
    out = copy.deepcopy(payload)
    out.pop("generated_at", None)
    hashes = out.get("source_hashes") if isinstance(out.get("source_hashes"), dict) else {}
    hashes.pop("outcome_registry", None)
    for row in out.get("rows") if isinstance(out.get("rows"), list) else []:
        if isinstance(row, dict):
            row.pop("outcome_id", None)
            row.pop("outcome_state", None)
    return out
