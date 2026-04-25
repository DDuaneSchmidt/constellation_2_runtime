from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from constellation_2.common.paper_session_fact_plane_v1 import (
    atomic_write_idempotent_validated_json_v1,
    now_utc_iso_v1,
    producer_block_v1,
    read_json_object_v1,
    resolve_fact_plane_truth_root_v1,
)
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_paper_session_authority_path,
    resolve_startup_materialization_path,
    resolve_trade_readiness_decision_path,
    resolve_trade_readiness_presubmit_path,
)
from constellation_2.common.sleeve_execution_root_v1 import resolve_sleeve_execution_root_v1


SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/trade_readiness_decision.v1.schema.json"
READINESS_VIEW_POST_SUBMIT = "POST_SUBMIT"
READINESS_VIEW_PRE_SUBMIT = "PRE_SUBMIT"

CANONICAL_GATES: Tuple[str, ...] = (
    "Market/Data Readiness",
    "Session Authority",
    "Intent Validity",
    "Signal Validity",
    "Sizing Validity",
    "Capital & Risk Permission",
    "Submit Permission",
    "Execution Build Readiness",
    "Broker Submission Result",
    "Lifecycle & Outcome Tracking",
)

BLOCKER_PRIORITY_ORDER: Tuple[str, ...] = (
    "WRONG_ENVIRONMENT",
    "KILL_SWITCH_ACTIVE",
    "SESSION_NOT_AUTHORIZED",
    "DATA_NOT_READY",
    "INTENT_INVALID",
    "SIGNAL_FILTERED",
    "SIZE_NOT_PROVEN",
    "NAV_INVALID",
    "RISK_POLICY_BLOCKED",
    "HEADROOM_INSUFFICIENT",
    "SUBMIT_NOT_AUTHORIZED",
    "EXECUTION_BUILD_FAILED",
    "BROKER_SUBMIT_FAILED",
    "BROKER_REJECTED",
    "BROKER_OUTCOME_NOT_RECONCILED",
    "LIFECYCLE_NOT_TRACKED",
    "OUTCOME_UNKNOWN",
)

BLOCKER_TO_GATE: Dict[str, str] = {
    "WRONG_ENVIRONMENT": "Session Authority",
    "KILL_SWITCH_ACTIVE": "Session Authority",
    "SESSION_NOT_AUTHORIZED": "Session Authority",
    "DATA_NOT_READY": "Market/Data Readiness",
    "INTENT_INVALID": "Intent Validity",
    "SIGNAL_FILTERED": "Signal Validity",
    "SIZE_NOT_PROVEN": "Sizing Validity",
    "NAV_INVALID": "Capital & Risk Permission",
    "RISK_POLICY_BLOCKED": "Capital & Risk Permission",
    "HEADROOM_INSUFFICIENT": "Capital & Risk Permission",
    "SUBMIT_NOT_AUTHORIZED": "Submit Permission",
    "EXECUTION_BUILD_FAILED": "Execution Build Readiness",
    "BROKER_SUBMIT_FAILED": "Broker Submission Result",
    "BROKER_REJECTED": "Lifecycle & Outcome Tracking",
    "BROKER_OUTCOME_NOT_RECONCILED": "Lifecycle & Outcome Tracking",
    "LIFECYCLE_NOT_TRACKED": "Lifecycle & Outcome Tracking",
    "OUTCOME_UNKNOWN": "Lifecycle & Outcome Tracking",
}


@dataclass(frozen=True)
class GateResult:
    gate: str
    status: str
    blockers: Tuple[str, ...]
    reason: str
    evidence_artifacts: Tuple[str, ...]
    evidence_hashes: Dict[str, str]


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_utc(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def _dedupe(values: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _priority_sorted_blockers(values: Iterable[str]) -> List[str]:
    unique = _dedupe(values)
    rank = {code: idx for idx, code in enumerate(BLOCKER_PRIORITY_ORDER)}
    return sorted(unique, key=lambda code: (rank.get(code, len(BLOCKER_PRIORITY_ORDER)), code))


def _normalize_reason_codes(values: Iterable[Any]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for value in values:
        code = str(value or "").strip().upper()
        if not code or code in seen:
            continue
        seen.add(code)
        out.append(code)
    return out


def _map_reasons_to_blockers(reason_codes: Iterable[str]) -> List[str]:
    blockers: List[str] = []
    normalized = _normalize_reason_codes(reason_codes)
    for code in normalized:
        if "KILL_SWITCH" in code:
            blockers.append("KILL_SWITCH_ACTIVE")
        if "SESSION" in code and ("DENIED" in code or "BLOCK" in code):
            blockers.append("SESSION_NOT_AUTHORIZED")
        if "OPTIONS" in code or "DATA" in code or "STARTUP_MATERIALIZATION" in code:
            blockers.append("DATA_NOT_READY")
        if "INTENT" in code and "SIGNAL" not in code:
            blockers.append("INTENT_INVALID")
        if "SIGNAL" in code and ("FILTER" in code or "BLOCK" in code):
            blockers.append("SIGNAL_FILTERED")
        if "ZERO_QUANTITY" in code or "AUTHORIZED_QUANTITY" in code or "SIZE" in code:
            blockers.append("SIZE_NOT_PROVEN")
        if "NAV" in code:
            blockers.append("NAV_INVALID")
        if "RISK" in code or "PAPER_POLICY_NOT_PASS" in code:
            blockers.append("RISK_POLICY_BLOCKED")
        if "HEADROOM" in code or code.startswith("AUTHZ_") or "BUNDLE_B_HEADROOM_REJECTED" in code:
            blockers.append("HEADROOM_INSUFFICIENT")
        if "READINESS" in code and "NOT_OK" in code:
            blockers.append("SUBMIT_NOT_AUTHORIZED")
        if "EXECUTION_BUILD" in code:
            blockers.append("EXECUTION_BUILD_FAILED")
        if "BROKER" in code and "SUBMIT" in code and ("FAIL" in code or "REJECT" in code):
            blockers.append("BROKER_SUBMIT_FAILED")
        if "BROKER_REJECTED" in code:
            blockers.append("BROKER_REJECTED")
        if "BROKER_OUTCOME_NOT_RECONCILED" in code:
            blockers.append("BROKER_OUTCOME_NOT_RECONCILED")
        if "LIFECYCLE" in code and ("MISSING" in code or "NOT_TRACKED" in code):
            blockers.append("LIFECYCLE_NOT_TRACKED")
        if "OUTCOME" in code and "UNKNOWN" in code:
            blockers.append("OUTCOME_UNKNOWN")
    return _priority_sorted_blockers(blockers)


def _normalize_readiness_view(value: str) -> str:
    normalized = str(value or "").strip().upper()
    if normalized == READINESS_VIEW_PRE_SUBMIT:
        return READINESS_VIEW_PRE_SUBMIT
    return READINESS_VIEW_POST_SUBMIT


def _intent_authorization_summary(
    execution_truth_root: Path,
    day_utc: str,
    *,
    intent_hash_hint: str = "",
) -> Dict[str, Any]:
    intents_dir = (execution_truth_root / "intents_v1" / "snapshots" / day_utc).resolve()
    auth_dir = (execution_truth_root / "engine_activity_v1" / "authorization_v1" / day_utc).resolve()
    intent_hashes: List[str] = []
    if intents_dir.exists() and intents_dir.is_dir():
        for intent_path in sorted(intents_dir.glob("*.exposure_intent.v1.json")):
            stem = str(intent_path.name).split(".", 1)[0].strip()
            if stem:
                intent_hashes.append(stem)
    hinted_intent_hash = str(intent_hash_hint or "").strip()
    if hinted_intent_hash:
        intent_hashes.append(hinted_intent_hash)
    intent_hashes = sorted(set(intent_hashes))
    authorized: List[Dict[str, Any]] = []
    denied_reason_codes: List[str] = []
    for intent_hash in intent_hashes:
        auth_path = (auth_dir / f"{intent_hash}.authorization.v1.json").resolve()
        if not auth_path.exists() or not auth_path.is_file():
            denied_reason_codes.append("INTENT_AUTHORIZATION_MISSING")
            continue
        try:
            payload = read_json_object_v1(auth_path)
        except Exception:
            denied_reason_codes.append("INTENT_AUTHORIZATION_PARSE_ERROR")
            continue
        auth = payload.get("authorization")
        auth_obj = auth if isinstance(auth, dict) else {}
        status = str(payload.get("status") or "").strip().upper()
        decision = str(auth_obj.get("decision") or "").strip().upper()
        try:
            qty = int(auth_obj.get("authorized_quantity") or 0)
        except Exception:
            qty = 0
        reason_codes = _normalize_reason_codes(auth_obj.get("reason_codes") or payload.get("reason_codes") or [])
        if status == "AUTHORIZED" and decision == "AUTHORIZED" and qty > 0:
            authorized.append(
                {
                    "intent_hash": intent_hash,
                    "authorized_quantity": qty,
                    "path": str(auth_path),
                }
            )
            continue
        denied_reason_codes.extend(reason_codes)
        if qty <= 0:
            denied_reason_codes.append("AUTHORIZATION_ZERO_QUANTITY")
    return {
        "intent_hashes": intent_hashes,
        "authorized": authorized,
        "denied_reason_codes": _normalize_reason_codes(denied_reason_codes),
        "intents_dir": str(intents_dir),
        "authorization_dir": str(auth_dir),
    }


def _extract_submission_id_from_pointer_payload(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    explicit_keys = (
        "submission_id",
        "current_submission_id",
        "latest_submission_id",
        "active_submission_id",
    )
    for key in explicit_keys:
        value = str(payload.get(key) or "").strip()
        if value:
            return value
    for nested_key in ("pointers", "pointer", "current", "latest", "submission"):
        nested = payload.get(nested_key)
        if not isinstance(nested, dict):
            continue
        for key in explicit_keys:
            value = str(nested.get(key) or "").strip()
            if value:
                return value
    return ""


def _resolve_pointer_submission_id(*, execution_truth_root: Path, day_utc: str) -> str:
    evidence_root = (execution_truth_root / "execution_evidence_v1").resolve()
    submissions_day_root = (evidence_root / "submissions" / day_utc).resolve()
    candidates = [
        (submissions_day_root / "latest_pointer.v1.json").resolve(),
        (submissions_day_root / "current_pointer.v1.json").resolve(),
        (submissions_day_root / "current_submission_pointer.v1.json").resolve(),
        (evidence_root / "latest_pointer.v1.json").resolve(),
    ]
    for pointer_path in candidates:
        if not pointer_path.exists() or not pointer_path.is_file():
            continue
        try:
            payload = read_json_object_v1(pointer_path)
        except Exception:
            continue
        if pointer_path.name == "latest_pointer.v1.json" and str(payload.get("day_utc") or "").strip() not in {"", day_utc}:
            continue
        submission_id = _extract_submission_id_from_pointer_payload(payload)
        if submission_id:
            return submission_id
    return ""


def _extract_artifact_timestamp(record: Dict[str, Any], path: Path) -> datetime | None:
    for key in ("submitted_at_utc", "created_at_utc", "updated_at_utc", "produced_utc", "event_time_utc"):
        parsed = _parse_utc(str(record.get(key) or ""))
        if parsed is not None:
            return parsed
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    except Exception:
        return None


def _record_submission_intent_hash(*, submissions_root: Path, submission_id: str) -> str:
    sid = str(submission_id or "").strip()
    if not sid:
        return ""
    binding_path = (submissions_root / sid / "binding_record.v2.json").resolve()
    if not binding_path.exists() or not binding_path.is_file():
        return ""
    try:
        binding = read_json_object_v1(binding_path)
    except Exception:
        return ""
    return str(binding.get("intent_hash") or "").strip()


def _status_requires_broker_ids(status: str) -> bool:
    normalized = str(status or "").strip().upper()
    return normalized in {"SUBMITTED", "PRESUBMITTED", "ACCEPTED", "ACKNOWLEDGED", "PARTIALLY_FILLED", "FILLED"}


def _status_is_immediate_broker_failure(status: str) -> bool:
    normalized = str(status or "").strip().upper()
    return normalized in {"REJECTED", "CANCELLED", "UNKNOWN"}


def _is_record_valid_for_selection(record: Dict[str, Any]) -> bool:
    status = str(record.get("status") or "").strip().upper()
    if _status_is_immediate_broker_failure(status):
        return False
    broker_ids = record.get("broker_ids")
    broker_ids_obj = broker_ids if isinstance(broker_ids, dict) else {}
    order_id = broker_ids_obj.get("order_id")
    perm_id = broker_ids_obj.get("perm_id")
    if _status_requires_broker_ids(status) and (order_id is None or perm_id is None):
        return False
    return True


def _normalize_outcome_state(value: str) -> str:
    return str(value or "").strip().upper()


def _is_outcome_pass_state(outcome_state: str) -> bool:
    return _normalize_outcome_state(outcome_state) in {
        "SUBMIT_ATTEMPTED",
        "BROKER_ID_ASSIGNED",
        "BROKER_ACCEPTED",
        "PARTIALLY_FILLED",
        "FILLED",
    }


def _is_outcome_pending_state(outcome_state: str) -> bool:
    return _normalize_outcome_state(outcome_state) in {
        "UNKNOWN_PENDING",
    }


def _gate_result(
    *,
    gate: str,
    status: str,
    blockers: Iterable[str],
    reason: str,
    evidence_artifacts: Iterable[str],
    evidence_hashes: Dict[str, str],
) -> GateResult:
    return GateResult(
        gate=gate,
        status=str(status).strip().upper(),
        blockers=tuple(_priority_sorted_blockers(blockers)),
        reason=str(reason).strip(),
        evidence_artifacts=tuple(sorted({str(item).strip() for item in evidence_artifacts if str(item).strip()})),
        evidence_hashes={key: value for key, value in sorted(evidence_hashes.items()) if key and value},
    )


def _select_canonical_blocker(blockers: Iterable[str]) -> str | None:
    ordered = _priority_sorted_blockers(blockers)
    return ordered[0] if ordered else None


def build_trade_readiness_decision_payload_v1(
    *,
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
    environment: str,
    ib_account: str,
    intent_hash: str = "",
    execution_build_ready_hint: bool | None = None,
    broker_submission_expected: bool = False,
    dry_run: bool | None = None,
    broker_transmit_enabled: bool | None = None,
    readiness_view: str = READINESS_VIEW_POST_SUBMIT,
    execution_truth_root_override: Path | None = None,
    evaluation_time_utc: str = "",
) -> Dict[str, Any]:
    root = resolve_fact_plane_truth_root_v1(truth_root)
    env = str(environment or "").strip().upper()
    day = str(day_utc or "").strip()
    account = str(ib_account or "").strip()
    view = _normalize_readiness_view(readiness_view)
    eval_time = _parse_utc(evaluation_time_utc)
    now = eval_time if eval_time is not None else _now_utc()

    global_blockers: List[str] = []
    evidence_artifacts: List[str] = []
    evidence_hashes: Dict[str, str] = {}
    gate_results: List[GateResult] = []

    if env != "PAPER":
        global_blockers.append("WRONG_ENVIRONMENT")

    session_path = resolve_paper_session_authority_path(truth_root=root, day_utc=day)
    session_payload: Dict[str, Any] | None = None
    session_blockers: List[str] = []
    session_reason = "Session authority is granted."
    if not session_path.exists() or not session_path.is_file():
        session_blockers.append("SESSION_NOT_AUTHORIZED")
        session_reason = "paper_session_authority_v1 is missing."
    else:
        try:
            session_payload = read_json_object_v1(session_path)
            evidence_artifacts.append(str(session_path))
            evidence_hashes[str(session_path)] = _sha256_file(session_path)
            if str(session_payload.get("day_utc") or "").strip() != day:
                session_blockers.append("SESSION_NOT_AUTHORIZED")
                session_reason = "paper_session_authority_v1 day mismatch."
            if str(session_payload.get("mode") or "").strip().upper() != "PAPER":
                session_blockers.append("WRONG_ENVIRONMENT")
                session_reason = "paper_session_authority_v1 mode is not PAPER."
            if str(session_payload.get("authority_status") or "").strip().upper() != "GRANTED":
                session_blockers.append("SESSION_NOT_AUTHORIZED")
                session_reason = "paper_session_authority_v1 authority_status is not GRANTED."
            safety_checks = session_payload.get("safety_checks")
            if isinstance(safety_checks, list):
                for row in safety_checks:
                    if not isinstance(row, dict):
                        continue
                    check_id = str(row.get("check_id") or "").strip()
                    status = str(row.get("status") or "").strip().upper()
                    if check_id == "CANONICAL_KILL_SWITCH_INACTIVE" and status != "PASS":
                        session_blockers.append("KILL_SWITCH_ACTIVE")
                        session_reason = "Kill switch is not inactive."
        except Exception:
            session_blockers.append("SESSION_NOT_AUTHORIZED")
            session_reason = "paper_session_authority_v1 is malformed."
    gate_results.append(
        _gate_result(
            gate="Session Authority",
            status="FAIL" if session_blockers else "PASS",
            blockers=session_blockers,
            reason=session_reason,
            evidence_artifacts=[str(session_path)],
            evidence_hashes={str(session_path): evidence_hashes.get(str(session_path), "")},
        )
    )
    global_blockers.extend(session_blockers)

    startup_path = resolve_startup_materialization_path(truth_root=root, day_utc=day)
    startup_blockers: List[str] = []
    startup_reason = "Startup materialization is successful."
    if not startup_path.exists() or not startup_path.is_file():
        startup_blockers.append("DATA_NOT_READY")
        startup_reason = "startup_materialization_v1 is missing."
    else:
        try:
            startup_payload = read_json_object_v1(startup_path)
            evidence_artifacts.append(str(startup_path))
            evidence_hashes[str(startup_path)] = _sha256_file(startup_path)
            if str(startup_payload.get("day_utc") or "").strip() != day:
                startup_blockers.append("DATA_NOT_READY")
                startup_reason = "startup_materialization_v1 day mismatch."
            if str(startup_payload.get("status") or "").strip().upper() != "SUCCESS":
                startup_blockers.append("DATA_NOT_READY")
                startup_reason = "startup_materialization_v1 status is not SUCCESS."
            startup_blockers.extend(_map_reasons_to_blockers(startup_payload.get("blocking_codes") or []))
        except Exception:
            startup_blockers.append("DATA_NOT_READY")
            startup_reason = "startup_materialization_v1 is malformed."
    startup_blockers = _priority_sorted_blockers(startup_blockers)
    gate_results.append(
        _gate_result(
            gate="Market/Data Readiness",
            status="FAIL" if startup_blockers else "PASS",
            blockers=startup_blockers,
            reason=startup_reason,
            evidence_artifacts=[str(startup_path)],
            evidence_hashes={str(startup_path): evidence_hashes.get(str(startup_path), "")},
        )
    )
    global_blockers.extend(startup_blockers)

    readiness_blockers: List[str] = []
    readiness_reason = "trade_submit_readiness_c2_v1 reports OK."
    readiness_path: Path | None = None
    execution_truth_root = None
    try:
        if execution_truth_root_override is not None:
            execution_truth_root = Path(execution_truth_root_override).resolve()
        else:
            execution_root = resolve_sleeve_execution_root_v1(
                repo_root=Path(repo_root).resolve(),
                environment=env,
                ib_account=account,
                sleeve_id="PRIMARY",
            )
            execution_truth_root = execution_root.execution_root_path.resolve()
        history_path = (
            execution_truth_root
            / "trade_submit_readiness_c2_v1"
            / "_history"
            / env
            / account
            / day
            / "status.json"
        ).resolve()
        current_path = (
            execution_truth_root
            / "trade_submit_readiness_c2_v1"
            / env
            / account
            / "status.json"
        ).resolve()
        readiness_path = history_path if history_path.exists() and history_path.is_file() else current_path
        readiness_payload = read_json_object_v1(readiness_path)
        evidence_artifacts.append(str(readiness_path))
        evidence_hashes[str(readiness_path)] = _sha256_file(readiness_path)
        if str(readiness_payload.get("environment") or "").strip().upper() != env:
            readiness_blockers.append("WRONG_ENVIRONMENT")
            readiness_reason = "trade_submit_readiness_c2_v1 environment mismatch."
        if str(readiness_payload.get("ib_account") or "").strip() != account:
            readiness_blockers.append("SUBMIT_NOT_AUTHORIZED")
            readiness_reason = "trade_submit_readiness_c2_v1 account mismatch."
        if str(readiness_payload.get("day_utc") or "").strip() != day:
            readiness_blockers.append("SUBMIT_NOT_AUTHORIZED")
            readiness_reason = "trade_submit_readiness_c2_v1 day mismatch."
        if bool(readiness_payload.get("ok") is not True):
            readiness_blockers.append("SUBMIT_NOT_AUTHORIZED")
            readiness_reason = "trade_submit_readiness_c2_v1 ok=false."
        if str(readiness_payload.get("state") or "").strip().upper() != "OK":
            readiness_blockers.append("SUBMIT_NOT_AUTHORIZED")
            readiness_reason = "trade_submit_readiness_c2_v1 state is not OK."
        expires = _parse_utc(str(readiness_payload.get("expires_utc") or ""))
        if expires is None or expires < now:
            readiness_blockers.append("SUBMIT_NOT_AUTHORIZED")
            readiness_reason = "trade_submit_readiness_c2_v1 is stale."
        readiness_blockers.extend(_map_reasons_to_blockers(readiness_payload.get("reasons") or []))
    except Exception:
        readiness_blockers.append("SUBMIT_NOT_AUTHORIZED")
        readiness_reason = "trade_submit_readiness_c2_v1 is missing or invalid."

    intent_summary = {
        "intent_hashes": [],
        "authorized": [],
        "denied_reason_codes": [],
        "intents_dir": "",
        "authorization_dir": "",
    }
    if execution_truth_root is not None:
        intent_summary = _intent_authorization_summary(
            execution_truth_root,
            day,
            intent_hash_hint=intent_hash,
        )
        evidence_artifacts.extend(
            [item for item in [intent_summary["intents_dir"], intent_summary["authorization_dir"]] if item]
        )
        if not intent_summary["intent_hashes"]:
            readiness_blockers.append("INTENT_INVALID")
        elif not intent_summary["authorized"]:
            if any("HEADROOM" in code or code.startswith("AUTHZ_") for code in intent_summary["denied_reason_codes"]):
                readiness_blockers.append("HEADROOM_INSUFFICIENT")
            else:
                readiness_blockers.append("SIZE_NOT_PROVEN")
    else:
        readiness_blockers.append("SUBMIT_NOT_AUTHORIZED")

    chosen_intent_hash = str(intent_hash or "").strip()
    if not chosen_intent_hash:
        if intent_summary["authorized"]:
            chosen_intent_hash = str(intent_summary["authorized"][0]["intent_hash"]).strip()
        elif intent_summary["intent_hashes"]:
            chosen_intent_hash = str(intent_summary["intent_hashes"][0]).strip()
        else:
            chosen_intent_hash = "UNKNOWN_INTENT"

    signal_blockers = [code for code in _priority_sorted_blockers(readiness_blockers) if code == "SIGNAL_FILTERED"]
    gate_results.append(
        _gate_result(
            gate="Signal Validity",
            status="FILTERED" if signal_blockers else "PASS",
            blockers=signal_blockers,
            reason="Signal filtered by governed checks." if signal_blockers else "Signal accepted by governed checks.",
            evidence_artifacts=[str(readiness_path)] if readiness_path is not None else [],
            evidence_hashes={str(readiness_path): evidence_hashes.get(str(readiness_path), "")}
            if readiness_path is not None
            else {},
        )
    )
    global_blockers.extend(signal_blockers)

    intent_blockers = [code for code in _priority_sorted_blockers(readiness_blockers) if code == "INTENT_INVALID"]
    gate_results.append(
        _gate_result(
            gate="Intent Validity",
            status="FAIL" if intent_blockers else "PASS",
            blockers=intent_blockers,
            reason="Intent evidence is missing or invalid." if intent_blockers else "Intent evidence is valid.",
            evidence_artifacts=[str(intent_summary.get("intents_dir") or "")],
            evidence_hashes={},
        )
    )
    global_blockers.extend(intent_blockers)

    sizing_blockers = [code for code in _priority_sorted_blockers(readiness_blockers) if code == "SIZE_NOT_PROVEN"]
    gate_results.append(
        _gate_result(
            gate="Sizing Validity",
            status="FAIL" if sizing_blockers else "PASS",
            blockers=sizing_blockers,
            reason="Authorized quantity is not proven > 0." if sizing_blockers else "Authorized quantity is proven.",
            evidence_artifacts=[str(intent_summary.get("authorization_dir") or "")],
            evidence_hashes={},
        )
    )
    global_blockers.extend(sizing_blockers)

    capital_risk_blockers = [
        code
        for code in _priority_sorted_blockers(readiness_blockers)
        if code in {"NAV_INVALID", "RISK_POLICY_BLOCKED", "HEADROOM_INSUFFICIENT"}
    ]
    gate_results.append(
        _gate_result(
            gate="Capital & Risk Permission",
            status="FAIL" if capital_risk_blockers else "PASS",
            blockers=capital_risk_blockers,
            reason="Capital/risk controls are blocking." if capital_risk_blockers else "Capital/risk controls allow submit.",
            evidence_artifacts=[str(readiness_path)] if readiness_path is not None else [],
            evidence_hashes={},
        )
    )
    global_blockers.extend(capital_risk_blockers)

    submit_blockers = [
        code
        for code in _priority_sorted_blockers(readiness_blockers)
        if code in {"SUBMIT_NOT_AUTHORIZED", "WRONG_ENVIRONMENT", "KILL_SWITCH_ACTIVE", "SESSION_NOT_AUTHORIZED", "DATA_NOT_READY"}
    ]
    if broker_submission_expected and (dry_run is True or broker_transmit_enabled is False):
        submit_blockers.append("SUBMIT_NOT_AUTHORIZED")
    submit_blockers = _priority_sorted_blockers(submit_blockers)
    gate_results.append(
        _gate_result(
            gate="Submit Permission",
            status="FAIL" if submit_blockers else "PASS",
            blockers=submit_blockers,
            reason=readiness_reason if submit_blockers else "Submit permission is authorized.",
            evidence_artifacts=[str(readiness_path)] if readiness_path is not None else [],
            evidence_hashes={},
        )
    )
    global_blockers.extend(submit_blockers)

    execution_build_blockers: List[str] = []
    execution_build_reason = "Execution build has not been attempted yet."
    build_submission_ids: List[str] = []
    if execution_build_ready_hint is True:
        execution_build_reason = "Execution build readiness provided by explicit submit-time hint."
    elif execution_build_ready_hint is False:
        execution_build_blockers.append("EXECUTION_BUILD_FAILED")
        execution_build_reason = "Execution build readiness hint reports failure."
    else:
        build_dir = (root / "reports" / "execution_build_v1" / day).resolve()
        build_candidates = sorted(build_dir.glob("*/execution_build.v1.json")) if build_dir.exists() and build_dir.is_dir() else []
        if build_candidates:
            execution_build_reason = "Execution build artifacts are present."
            ranked_build_rows: List[Tuple[int, datetime, str]] = []
            for candidate in build_candidates:
                evidence_artifacts.append(str(candidate))
                evidence_hashes[str(candidate)] = _sha256_file(candidate)
                try:
                    payload = read_json_object_v1(candidate)
                except Exception:
                    payload = {}
                submission_id = str(payload.get("submission_id") or candidate.parent.name).strip()
                if not submission_id:
                    continue
                closure_status = str(payload.get("closure_status") or payload.get("status") or "").strip().upper()
                rank = 1 if closure_status == "COMPLETE" else 0
                ts = _extract_artifact_timestamp(payload, candidate) or datetime.fromtimestamp(0, tz=timezone.utc)
                ranked_build_rows.append((rank, ts, submission_id))
            ranked_build_rows.sort(key=lambda row: (row[0], row[1], row[2]), reverse=True)
            build_submission_ids = _dedupe([row[2] for row in ranked_build_rows])
        elif intent_summary["intent_hashes"]:
            execution_build_blockers.append("EXECUTION_BUILD_FAILED")
            execution_build_reason = "Intent exists but execution_build_v1 artifact is missing."
    gate_results.append(
        _gate_result(
            gate="Execution Build Readiness",
            status="FAIL" if execution_build_blockers else ("PASS" if execution_build_reason != "Execution build has not been attempted yet." else "NOT_ATTEMPTED"),
            blockers=execution_build_blockers,
            reason=execution_build_reason,
            evidence_artifacts=[],
            evidence_hashes={},
        )
    )
    global_blockers.extend(execution_build_blockers)

    broker_blockers: List[str] = []
    broker_reason = "No broker submission attempt for this decision."
    broker_gate_evidence_artifacts: List[str] = []
    broker_gate_evidence_hashes: Dict[str, str] = {}
    selected_submission_id_for_outcome = ""
    if view == READINESS_VIEW_PRE_SUBMIT:
        broker_reason = "Pre-submit readiness defers broker submission evidence until submit is attempted."
    elif execution_truth_root is not None:
        submissions_root = (execution_truth_root / "execution_evidence_v1" / "submissions" / day).resolve()
        records = sorted(submissions_root.glob("*/broker_submission_record.v2.json")) if submissions_root.exists() else []
        if records:
            broker_reason = "Broker submission evidence exists."
            parsed_records: Dict[str, Dict[str, Any]] = {}
            malformed_submission_ids: List[str] = []
            for record_path in records:
                submission_id = str(record_path.parent.name or "").strip()
                try:
                    record = read_json_object_v1(record_path)
                except Exception:
                    if submission_id:
                        malformed_submission_ids.append(submission_id)
                    continue
                evidence_artifacts.append(str(record_path))
                evidence_hashes[str(record_path)] = _sha256_file(record_path)
                broker_gate_evidence_artifacts.append(str(record_path))
                broker_gate_evidence_hashes[str(record_path)] = evidence_hashes[str(record_path)]
                if submission_id:
                    parsed_records[submission_id] = {
                        "submission_id": submission_id,
                        "path": str(record_path),
                        "record": record,
                        "timestamp": _extract_artifact_timestamp(record, record_path),
                    }

            pointer_submission_id = _resolve_pointer_submission_id(execution_truth_root=execution_truth_root, day_utc=day)
            selected_submission_id = ""
            selection_source = "NONE"

            if pointer_submission_id:
                selected_submission_id = pointer_submission_id
                selection_source = "POINTER"

            if not selected_submission_id:
                build_records = [sid for sid in build_submission_ids if sid in parsed_records]
                build_intent_records = [
                    sid
                    for sid in build_records
                    if _record_submission_intent_hash(submissions_root=submissions_root, submission_id=sid) == chosen_intent_hash
                ]
                if build_intent_records:
                    selected_submission_id = build_intent_records[0]
                    selection_source = "EXECUTION_BUILD_INTENT"
                elif build_records:
                    selected_submission_id = build_records[0]
                    selection_source = "EXECUTION_BUILD"

            if not selected_submission_id and chosen_intent_hash:
                intent_records: List[Tuple[datetime, str]] = []
                for sid, row in parsed_records.items():
                    if _record_submission_intent_hash(submissions_root=submissions_root, submission_id=sid) != chosen_intent_hash:
                        continue
                    ts = row.get("timestamp")
                    if isinstance(ts, datetime):
                        intent_records.append((ts, sid))
                if intent_records:
                    intent_records.sort(key=lambda item: (item[0], item[1]), reverse=True)
                    selected_submission_id = intent_records[0][1]
                    selection_source = "INTENT_HASH"

            if not selected_submission_id:
                valid_records: List[Tuple[datetime, str]] = []
                for sid, row in parsed_records.items():
                    record = row.get("record")
                    if not isinstance(record, dict) or not _is_record_valid_for_selection(record):
                        continue
                    ts = row.get("timestamp")
                    if isinstance(ts, datetime):
                        valid_records.append((ts, sid))
                if valid_records:
                    valid_records.sort(key=lambda item: (item[0], item[1]), reverse=True)
                    selected_submission_id = valid_records[0][1]
                    selection_source = "LATEST_VALID"

            if not selected_submission_id and parsed_records:
                all_records: List[Tuple[datetime, str]] = []
                for sid, row in parsed_records.items():
                    ts = row.get("timestamp")
                    if isinstance(ts, datetime):
                        all_records.append((ts, sid))
                if all_records:
                    all_records.sort(key=lambda item: (item[0], item[1]), reverse=True)
                    selected_submission_id = all_records[0][1]
                    selection_source = "LATEST_ANY"

            should_have_current_submission = bool(broker_submission_expected or build_submission_ids or parsed_records or malformed_submission_ids)
            if pointer_submission_id and pointer_submission_id not in parsed_records:
                broker_blockers.append("BROKER_SUBMIT_FAILED")
                broker_reason = (
                    "Current broker submission pointer does not resolve to broker_submission_record evidence."
                )
            elif selected_submission_id and selected_submission_id in parsed_records:
                selected_row = parsed_records[selected_submission_id]
                selected_record = selected_row.get("record")
                selected_submission_id_for_outcome = selected_submission_id
                if not isinstance(selected_record, dict):
                    broker_blockers.append("BROKER_SUBMIT_FAILED")
                    broker_reason = "Selected broker submission evidence is malformed."
                else:
                    selected_status = str(selected_record.get("status") or "").strip().upper()
                    broker_ids_obj = selected_record.get("broker_ids")
                    broker_ids_obj = broker_ids_obj if isinstance(broker_ids_obj, dict) else {}
                    order_id = broker_ids_obj.get("order_id")
                    perm_id = broker_ids_obj.get("perm_id")
                    broker_env = str(((selected_record.get("broker") or {}) if isinstance(selected_record.get("broker"), dict) else {}).get("environment") or "").strip().upper()
                    record_day = str(selected_record.get("day_utc") or "").strip()
                    submitted_day = ""
                    submitted_at = str(selected_record.get("submitted_at_utc") or "").strip()
                    parsed_submitted_at = _parse_utc(submitted_at)
                    if parsed_submitted_at is not None:
                        submitted_day = parsed_submitted_at.date().isoformat()
                    if _status_is_immediate_broker_failure(selected_status):
                        broker_blockers.append("BROKER_SUBMIT_FAILED")
                        broker_reason = f"Selected broker submission {selected_submission_id} status={selected_status}."
                    elif _status_requires_broker_ids(selected_status) and (order_id is None or perm_id is None):
                        broker_blockers.append("BROKER_SUBMIT_FAILED")
                        broker_reason = (
                            f"Selected broker submission {selected_submission_id} status={selected_status} lacks broker_ids."
                        )
                    elif broker_env and broker_env != env:
                        broker_blockers.append("BROKER_SUBMIT_FAILED")
                        broker_reason = (
                            f"Selected broker submission {selected_submission_id} environment mismatch."
                        )
                    elif record_day and record_day != day:
                        broker_blockers.append("BROKER_SUBMIT_FAILED")
                        broker_reason = f"Selected broker submission {selected_submission_id} day mismatch."
                    elif str(selected_record.get("ib_account") or "").strip() and str(selected_record.get("ib_account") or "").strip() != account:
                        broker_blockers.append("BROKER_SUBMIT_FAILED")
                        broker_reason = f"Selected broker submission {selected_submission_id} account mismatch."
                    else:
                        broker_reason = (
                            f"Selected broker submission {selected_submission_id} via {selection_source} validated."
                        )
                        if not record_day and submitted_day and submitted_day != day:
                            broker_reason = (
                                f"{broker_reason} submitted_at_day={submitted_day}"
                                f" differs_from_submission_partition={day}."
                            )

                    historical_invalid_ids = [
                        sid
                        for sid, row in sorted(parsed_records.items())
                        if sid != selected_submission_id and isinstance(row.get("record"), dict) and not _is_record_valid_for_selection(row["record"])
                    ]
                    if historical_invalid_ids:
                        broker_reason = (
                            f"{broker_reason} ignored_historical_invalid_submission_ids="
                            + ",".join(historical_invalid_ids)
                        )
                    if malformed_submission_ids:
                        broker_reason = (
                            f"{broker_reason} malformed_submission_ids="
                            + ",".join(sorted(set(malformed_submission_ids)))
                        )
            elif should_have_current_submission:
                broker_blockers.append("BROKER_SUBMIT_FAILED")
                broker_reason = "No current broker submission evidence could be selected."
        elif broker_submission_expected or build_submission_ids:
            broker_blockers.append("BROKER_SUBMIT_FAILED")
            broker_reason = "No broker submission evidence found for expected current submission scope."
    gate_results.append(
        _gate_result(
            gate="Broker Submission Result",
            status=(
                "NOT_ATTEMPTED"
                if view == READINESS_VIEW_PRE_SUBMIT
                else ("FAIL" if broker_blockers else ("NOT_ATTEMPTED" if broker_reason.startswith("No broker") else "PASS"))
            ),
            blockers=broker_blockers,
            reason=broker_reason,
            evidence_artifacts=broker_gate_evidence_artifacts,
            evidence_hashes=broker_gate_evidence_hashes,
        )
    )
    global_blockers.extend(broker_blockers)

    lifecycle_blockers: List[str] = []
    lifecycle_reason = "No lifecycle closure required before first broker attempt."
    lifecycle_status = "NOT_ATTEMPTED"
    lifecycle_gate_evidence_artifacts: List[str] = []
    lifecycle_gate_evidence_hashes: Dict[str, str] = {}
    lifecycle_root = (root / "reports" / "canonical_lifecycle_closure_v1" / day).resolve()
    lifecycle_files = sorted(lifecycle_root.glob("*/canonical_lifecycle_closure.v1.json")) if lifecycle_root.exists() else []
    if view == READINESS_VIEW_PRE_SUBMIT:
        lifecycle_reason = "Pre-submit readiness defers lifecycle tracking until broker submit is attempted."
        lifecycle_status = "NOT_ATTEMPTED"
    elif broker_blockers:
        lifecycle_blockers.append("LIFECYCLE_NOT_TRACKED")
        lifecycle_reason = "Broker submission failure requires lifecycle closure tracking."
        lifecycle_status = "FAIL"
    elif execution_truth_root is not None and selected_submission_id_for_outcome:
        outcome_path = (
            execution_truth_root
            / "execution_evidence_v1"
            / "submissions"
            / day
            / selected_submission_id_for_outcome
            / "broker_order_outcome_v1.json"
        ).resolve()
        if not outcome_path.exists() or not outcome_path.is_file():
            lifecycle_blockers.append("BROKER_OUTCOME_NOT_RECONCILED")
            lifecycle_reason = (
                f"broker_order_outcome_v1 missing for selected submission {selected_submission_id_for_outcome}."
            )
            lifecycle_status = "FAIL"
        else:
            try:
                outcome_payload = read_json_object_v1(outcome_path)
                evidence_artifacts.append(str(outcome_path))
                evidence_hashes[str(outcome_path)] = _sha256_file(outcome_path)
                lifecycle_gate_evidence_artifacts.append(str(outcome_path))
                lifecycle_gate_evidence_hashes[str(outcome_path)] = evidence_hashes[str(outcome_path)]
                outcome_submission_id = str(outcome_payload.get("submission_id") or "").strip()
                outcome_day = str(outcome_payload.get("day_utc") or "").strip()
                outcome_env = str(outcome_payload.get("environment") or "").strip().upper()
                outcome_account = str(outcome_payload.get("ib_account") or "").strip()
                outcome_state = _normalize_outcome_state(str(outcome_payload.get("outcome_state") or ""))
                if outcome_submission_id != selected_submission_id_for_outcome:
                    lifecycle_blockers.append("BROKER_OUTCOME_NOT_RECONCILED")
                    lifecycle_reason = "broker_order_outcome_v1 submission_id mismatch."
                    lifecycle_status = "FAIL"
                elif outcome_day and outcome_day != day:
                    lifecycle_blockers.append("BROKER_OUTCOME_NOT_RECONCILED")
                    lifecycle_reason = "broker_order_outcome_v1 day mismatch."
                    lifecycle_status = "FAIL"
                elif outcome_env and outcome_env != env:
                    lifecycle_blockers.append("BROKER_OUTCOME_NOT_RECONCILED")
                    lifecycle_reason = "broker_order_outcome_v1 environment mismatch."
                    lifecycle_status = "FAIL"
                elif outcome_account and outcome_account != account:
                    lifecycle_blockers.append("BROKER_OUTCOME_NOT_RECONCILED")
                    lifecycle_reason = "broker_order_outcome_v1 account mismatch."
                    lifecycle_status = "FAIL"
                elif outcome_state == "BROKER_REJECTED":
                    lifecycle_blockers.append("BROKER_REJECTED")
                    lifecycle_reason = "broker_order_outcome_v1 reports BROKER_REJECTED."
                    lifecycle_status = "FAIL"
                elif outcome_state == "BROKER_CANCELLED":
                    lifecycle_blockers.append("OUTCOME_UNKNOWN")
                    lifecycle_reason = "broker_order_outcome_v1 reports BROKER_CANCELLED."
                    lifecycle_status = "FAIL"
                elif _is_outcome_pass_state(outcome_state):
                    lifecycle_reason = f"broker_order_outcome_v1 state={outcome_state}."
                    lifecycle_status = "PASS"
                elif _is_outcome_pending_state(outcome_state):
                    lifecycle_reason = "broker_order_outcome_v1 indicates pending post-submit reconciliation."
                    lifecycle_status = "PENDING"
                else:
                    lifecycle_blockers.append("BROKER_OUTCOME_NOT_RECONCILED")
                    lifecycle_reason = f"broker_order_outcome_v1 state={outcome_state or 'UNKNOWN'} is not recognized."
                    lifecycle_status = "FAIL"
            except Exception:
                lifecycle_blockers.append("BROKER_OUTCOME_NOT_RECONCILED")
                lifecycle_reason = "broker_order_outcome_v1 is malformed."
                lifecycle_status = "FAIL"
    elif lifecycle_files:
        lifecycle_reason = "Lifecycle closure artifacts are present."
        lifecycle_status = "PASS"
    else:
        lifecycle_reason = "No broker submission attempt for this decision."
        lifecycle_status = "NOT_ATTEMPTED"

    if lifecycle_files:
        for lifecycle_path in lifecycle_files:
            evidence_artifacts.append(str(lifecycle_path))
            evidence_hashes[str(lifecycle_path)] = _sha256_file(lifecycle_path)
            lifecycle_gate_evidence_artifacts.append(str(lifecycle_path))
            lifecycle_gate_evidence_hashes[str(lifecycle_path)] = evidence_hashes[str(lifecycle_path)]

    gate_results.append(
        _gate_result(
            gate="Lifecycle & Outcome Tracking",
            status=(
                "NOT_ATTEMPTED"
                if view == READINESS_VIEW_PRE_SUBMIT
                else ("FAIL" if lifecycle_blockers else lifecycle_status)
            ),
            blockers=lifecycle_blockers,
            reason=lifecycle_reason,
            evidence_artifacts=lifecycle_gate_evidence_artifacts,
            evidence_hashes=lifecycle_gate_evidence_hashes,
        )
    )
    global_blockers.extend(lifecycle_blockers)

    ordered_gate_results = sorted(
        gate_results,
        key=lambda row: CANONICAL_GATES.index(row.gate),
    )
    all_blockers = _priority_sorted_blockers(global_blockers)
    canonical_blocker = _select_canonical_blocker(all_blockers)
    canonical_gate = BLOCKER_TO_GATE.get(canonical_blocker, "NONE") if canonical_blocker else "NONE"
    canonical_reason = (
        f"{canonical_blocker} selected by blocker priority order."
        if canonical_blocker
        else "All pre-submit canonical gates pass."
    )
    decision = "NO" if canonical_blocker else "YES"
    submit_allowed = decision == "YES"
    if submit_allowed:
        if env != "PAPER":
            submit_allowed = False
        if broker_submission_expected and (dry_run is True or broker_transmit_enabled is False):
            submit_allowed = False

    payload = {
        "schema_version": "v1",
        "environment": env,
        "day_utc": day,
        "intent_hash": chosen_intent_hash,
        "decision": decision,
        "submit_allowed": bool(submit_allowed),
        "canonical_gate": canonical_gate,
        "canonical_blocker": canonical_blocker,
        "canonical_reason": canonical_reason,
        "ordered_gate_results": [
            {
                "gate": result.gate,
                "status": result.status,
                "blockers": list(result.blockers),
                "reason": result.reason,
                "evidence_artifacts": list(result.evidence_artifacts),
                "evidence_hashes": dict(result.evidence_hashes),
            }
            for result in ordered_gate_results
        ],
        "all_blockers": all_blockers,
        "evidence_artifacts": sorted({item for item in evidence_artifacts if item}),
        "evidence_hashes": {key: value for key, value in sorted(evidence_hashes.items()) if key and value},
        "decision_timestamp_utc": now_utc_iso_v1(),
        "decision_writer": "constellation_2/common/trade_readiness_reducer_v1.py",
        "truth_root": str(root),
        "policy_version": (
            "trade_readiness_reducer_v1_presubmit"
            if view == READINESS_VIEW_PRE_SUBMIT
            else "trade_readiness_reducer_v1"
        ),
        "blocker_priority_order": list(BLOCKER_PRIORITY_ORDER),
        "producer": producer_block_v1(module="constellation_2/common/trade_readiness_reducer_v1.py"),
    }
    return payload


def write_trade_readiness_decision_v1(*, truth_root: Path, payload: Dict[str, Any]) -> Path:
    root = resolve_fact_plane_truth_root_v1(truth_root)
    ref = atomic_write_idempotent_validated_json_v1(
        path=resolve_trade_readiness_decision_path(
            truth_root=root,
            day_utc=str(payload.get("day_utc") or "").strip(),
        ),
        payload=payload,
        schema_relpath=SCHEMA_RELPATH,
        volatile_field_names=("decision_timestamp_utc",),
    )
    return ref.path


def write_trade_readiness_presubmit_v1(*, truth_root: Path, payload: Dict[str, Any]) -> Path:
    root = resolve_fact_plane_truth_root_v1(truth_root)
    ref = atomic_write_idempotent_validated_json_v1(
        path=resolve_trade_readiness_presubmit_path(
            truth_root=root,
            day_utc=str(payload.get("day_utc") or "").strip(),
        ),
        payload=payload,
        schema_relpath=SCHEMA_RELPATH,
        volatile_field_names=("decision_timestamp_utc",),
    )
    return ref.path


def read_trade_readiness_decision_v1(*, truth_root: Path, day_utc: str) -> Dict[str, Any]:
    path = resolve_trade_readiness_decision_path(
        truth_root=resolve_fact_plane_truth_root_v1(truth_root),
        day_utc=day_utc,
    )
    return read_json_object_v1(path)


def read_trade_readiness_presubmit_v1(*, truth_root: Path, day_utc: str) -> Dict[str, Any]:
    path = resolve_trade_readiness_presubmit_path(
        truth_root=resolve_fact_plane_truth_root_v1(truth_root),
        day_utc=day_utc,
    )
    return read_json_object_v1(path)
