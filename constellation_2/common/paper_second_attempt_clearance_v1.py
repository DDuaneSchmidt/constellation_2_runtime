from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1


SCHEMA_ID = "paper_second_attempt_clearance"
SCHEMA_VERSION = "v1"
OPERATOR_SCHEMA_ID = "paper_second_attempt_operator_clearance"
TERMINAL_STATUSES = {"BROKER_REJECTED", "CANCELLED", "CANCELED", "REJECTED", "BROKER_CANCELLED"}
OPEN_LIFECYCLE_STATES = {"SUBMITTED_PENDING_ACK", "ACKNOWLEDGED_OPEN", "PARTIALLY_FILLED", "SUBMITTED", "PRESUBMITTED"}


class PaperSecondAttemptClearanceError(Exception):
    pass


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise PaperSecondAttemptClearanceError(f"TOP_LEVEL_NOT_OBJECT:{path}")
    return payload


def sha256_file_v1(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    out = dict(payload)
    out["canonical_json_hash"] = None
    out["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(out)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n", encoding="utf-8")


def clearance_path_v1(*, truth_root: Path, day_utc: str, prior_submission_id: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "paper_second_attempt_clearance_v1"
        / day_utc
        / prior_submission_id
        / "paper_second_attempt_clearance.v1.json"
    ).resolve()


def operator_clearance_default_path_v1(*, runtime_root: Path, day_utc: str, prior_submission_id: str) -> Path:
    return (
        Path(runtime_root).resolve()
        / "operator_inputs"
        / "paper_second_attempt_clearance_v1"
        / day_utc
        / prior_submission_id
        / "operator_clearance.v1.json"
    ).resolve()


def latest_broker_submission_record_path_v1(*, execution_root: Path, day_utc: str) -> Path | None:
    root = Path(execution_root).resolve() / "execution_evidence_v1" / "submissions" / day_utc
    if not root.exists() or not root.is_dir():
        return None
    candidates = sorted(path.resolve() for path in root.glob("*/broker_submission_record.v2.json") if path.is_file())
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _coerce_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    try:
        return int(str(value).strip())
    except Exception:
        return None


def _coerce_filled_qty(value: Any) -> int:
    parsed = _coerce_int(value)
    return parsed if parsed is not None else 0


def _prior_paths(*, execution_root: Path, day_utc: str, prior_submission_id: str) -> dict[str, Path]:
    subdir = Path(execution_root).resolve() / "execution_evidence_v1" / "submissions" / day_utc / prior_submission_id
    return {
        "submission_dir": subdir,
        "broker_submission_record": subdir / "broker_submission_record.v2.json",
        "broker_submit_attempt": subdir / "broker_submit_attempt_v1.json",
        "broker_order_outcome": subdir / "broker_order_outcome_v1.json",
        "execution_event_record": subdir / "execution_event_record.v1.json",
        "broker_acknowledgement": subdir / "broker_acknowledgement_v1.json",
        "order_plan": subdir / "order_plan.v1.json",
        "equity_order_plan_v2": subdir / "equity_order_plan.v2.json",
        "equity_order_plan_v1": subdir / "equity_order_plan.v1.json",
    }


def _plan_path(paths: dict[str, Path]) -> Path | None:
    for key in ("order_plan", "equity_order_plan_v2", "equity_order_plan_v1"):
        path = paths[key]
        if path.exists() and path.is_file():
            return path
    return None


def _load_if_present(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        return _read_json(path)
    except Exception:
        return {}


def _terminal_state(*, broker: dict[str, Any], outcome: dict[str, Any], event: dict[str, Any]) -> str:
    for value in (
        str(outcome.get("outcome_state") or "").strip().upper(),
        str(event.get("status") or "").strip().upper(),
        str(broker.get("status") or "").strip().upper(),
    ):
        if value in TERMINAL_STATUSES:
            return value
    return (
        str(outcome.get("outcome_state") or "").strip().upper()
        or str(event.get("status") or "").strip().upper()
        or str(broker.get("status") or "").strip().upper()
        or "UNKNOWN"
    )


def _broker_order_id(broker: dict[str, Any], event: dict[str, Any]) -> int | None:
    ids = broker.get("broker_ids") if isinstance(broker.get("broker_ids"), dict) else {}
    return _coerce_int(ids.get("order_id")) or _coerce_int(event.get("broker_order_id"))


def _broker_perm_id(broker: dict[str, Any], event: dict[str, Any]) -> int | None:
    ids = broker.get("broker_ids") if isinstance(broker.get("broker_ids"), dict) else {}
    broker_perm = _coerce_int(ids.get("perm_id"))
    return broker_perm if broker_perm is not None else _coerce_int(event.get("perm_id"))


def _rejection_reason(*, broker: dict[str, Any], outcome: dict[str, Any]) -> str:
    for obj in (outcome.get("error"), broker.get("error")):
        if isinstance(obj, dict):
            code = str(obj.get("code") or "").strip()
            message = str(obj.get("message") or "").strip()
            if code or message:
                return f"{code}:{message}".strip(":")
    codes = outcome.get("reason_codes")
    if isinstance(codes, list) and codes:
        return ",".join(str(code).strip() for code in codes if str(code).strip())
    return ""


def _account_from_evidence(attempt: dict[str, Any], ack: dict[str, Any], outcome: dict[str, Any]) -> str:
    for obj in (attempt, ack, outcome):
        account = str(obj.get("ib_account") or "").strip()
        if account:
            return account
    return ""


def _closure_path(truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "trading_day_closure_authority_v1" / day_utc / "trading_day_closure_authority.v1.json"


def _lifecycle_has_open_state(*, execution_root: Path, day_utc: str, prior_submission_id: str) -> bool:
    path = Path(execution_root).resolve() / "reports" / "execution_lifecycle_authority_v1" / day_utc / "execution_lifecycle_authority.v1.json"
    payload = _load_if_present(path)
    for row in payload.get("submissions") or []:
        if not isinstance(row, dict):
            continue
        if str(row.get("submission_id") or "").strip() != prior_submission_id:
            continue
        if str(row.get("current_lifecycle_state") or "").strip().upper() in OPEN_LIFECYCLE_STATES:
            return True
    return False


def _base_payload(*, day_utc: str, environment: str, sections: dict[str, Any] | None = None) -> dict[str, Any]:
    sections = sections or {}
    return {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "day_utc": day_utc,
        "environment": environment,
        "prior_submission": sections.get("prior_submission", {}),
        "closure_evidence": sections.get("closure_evidence", {}),
        "operator_clearance": sections.get("operator_clearance", {}),
        "policy": {
            "paper_only": True,
            "live_supported": False,
            "same_submission_id_reuse_allowed": False,
            "requires_fresh_snapshot_lineage": True,
            "requires_ib_preview_pass": True,
            "requires_different_plan_hash": True,
            "requires_different_structure_or_pricing": True,
        },
        "new_attempt_requirements": [
            "fresh_snapshot_lineage",
            "ib_combo_preview_pass",
            "different_plan_hash",
            "different_structure_or_pricing",
            "new_submission_id",
        ],
        "produced_at_utc": _now_iso(),
    }


def _blocked(
    *,
    day_utc: str,
    environment: str,
    blocker: str,
    operator_action: str,
    sections: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = _base_payload(day_utc=day_utc, environment=environment, sections=sections)
    payload.update({"status": "BLOCKED", "canonical_blocker": blocker, "operator_next_action": operator_action})
    return payload


def build_paper_second_attempt_clearance_v1(
    *,
    truth_root: Path,
    execution_root: Path,
    day_utc: str,
    environment: str,
    prior_submission_id: str,
    operator_clearance_path: Path,
) -> dict[str, Any]:
    environment = str(environment or "").strip().upper()
    prior_submission_id = str(prior_submission_id or "").strip()
    if environment != "PAPER":
        return _blocked(day_utc=day_utc, environment=environment, blocker="LIVE_UNSUPPORTED", operator_action="Second-attempt clearance is PAPER-only.")

    paths = _prior_paths(execution_root=execution_root, day_utc=day_utc, prior_submission_id=prior_submission_id)
    if not paths["broker_submission_record"].exists():
        return _blocked(
            day_utc=day_utc,
            environment=environment,
            blocker="PRIOR_SUBMISSION_MISSING",
            operator_action="Verify prior submission id and runtime execution root.",
            sections={"prior_submission": {"submission_id": prior_submission_id}},
        )

    broker = _read_json(paths["broker_submission_record"])
    outcome = _load_if_present(paths["broker_order_outcome"])
    event = _load_if_present(paths["execution_event_record"])
    ack = _load_if_present(paths["broker_acknowledgement"])
    attempt = _load_if_present(paths["broker_submit_attempt"])
    plan_path = _plan_path(paths)
    order_id = _broker_order_id(broker, event)
    perm_id = _broker_perm_id(broker, event)
    filled_qty = _coerce_filled_qty(event.get("filled_qty"))
    terminal_state = _terminal_state(broker=broker, outcome=outcome, event=event)
    account = _account_from_evidence(attempt, ack, outcome)
    prior_section = {
        "submission_id": prior_submission_id,
        "account": account,
        "broker_submission_record_path": str(paths["broker_submission_record"]),
        "broker_submission_record_sha256": sha256_file_v1(paths["broker_submission_record"]),
        "broker_order_outcome_path": str(paths["broker_order_outcome"]) if paths["broker_order_outcome"].exists() else "",
        "broker_order_outcome_sha256": sha256_file_v1(paths["broker_order_outcome"]) if paths["broker_order_outcome"].exists() else "",
        "execution_event_record_path": str(paths["execution_event_record"]) if paths["execution_event_record"].exists() else "",
        "execution_event_record_sha256": sha256_file_v1(paths["execution_event_record"]) if paths["execution_event_record"].exists() else "",
        "order_plan_path": str(plan_path) if plan_path is not None else "",
        "order_plan_sha256": sha256_file_v1(plan_path) if plan_path is not None else "",
        "order_id": order_id,
        "perm_id": perm_id,
        "terminal_state": terminal_state,
        "filled_qty": filled_qty,
        "rejection_or_cancel_reason": _rejection_reason(broker=broker, outcome=outcome),
    }
    sections: dict[str, Any] = {"prior_submission": prior_section}
    if terminal_state not in TERMINAL_STATUSES:
        return _blocked(day_utc=day_utc, environment=environment, blocker="PRIOR_SUBMISSION_NOT_TERMINAL", operator_action="Refresh broker lifecycle/outcome until the prior submission is terminal.", sections=sections)
    if filled_qty != 0:
        return _blocked(day_utc=day_utc, environment=environment, blocker="PRIOR_SUBMISSION_NONZERO_FILL", operator_action="Do not clear a second attempt after a filled or partially filled prior submission.", sections=sections)
    if _lifecycle_has_open_state(execution_root=execution_root, day_utc=day_utc, prior_submission_id=prior_submission_id):
        return _blocked(day_utc=day_utc, environment=environment, blocker="PRIOR_SUBMISSION_OPEN_LIFECYCLE", operator_action="Resolve open lifecycle state before clearance.", sections=sections)

    closure_path = _closure_path(truth_root, day_utc)
    if not closure_path.exists():
        return _blocked(day_utc=day_utc, environment=environment, blocker="TRADING_DAY_CLOSURE_MISSING", operator_action="Run trading day closure authority before second-attempt clearance.", sections=sections)
    closure = _read_json(closure_path)
    closure_sha = sha256_file_v1(closure_path)
    closure_section = {
        "trading_day_closure_authority_path": str(closure_path),
        "trading_day_closure_authority_sha256": closure_sha,
        "closure_state": str(closure.get("closure_state") or ""),
        "closure_status": str(closure.get("status") or ""),
    }
    sections["closure_evidence"] = closure_section
    if str(closure.get("status") or "").strip().upper() != "PASS":
        return _blocked(day_utc=day_utc, environment=environment, blocker="TRADING_DAY_CLOSURE_NOT_PASS", operator_action="Resolve trading day closure authority before clearance.", sections=sections)
    if closure.get("unresolved_submissions"):
        return _blocked(day_utc=day_utc, environment=environment, blocker="OPEN_BROKER_LIFECYCLE_REMAINS", operator_action="Resolve unresolved submissions before clearance.", sections=sections)

    op_path = Path(operator_clearance_path).expanduser().resolve()
    if not op_path.exists():
        return _blocked(day_utc=day_utc, environment=environment, blocker="OPERATOR_CLEARANCE_MISSING", operator_action="Create governed operator clearance input for the prior submission.", sections=sections)
    operator = _read_json(op_path)
    operator_section: dict[str, Any] = {"path": str(op_path), "sha256": sha256_file_v1(op_path)}
    sections["operator_clearance"] = operator_section
    mismatches: list[str] = []
    if str(operator.get("schema_id") or "") != OPERATOR_SCHEMA_ID:
        mismatches.append("schema_id")
    if str(operator.get("schema_version") or "") != SCHEMA_VERSION:
        mismatches.append("schema_version")
    if str(operator.get("environment") or "").strip().upper() != "PAPER":
        mismatches.append("environment")
    if str(operator.get("day_utc") or "").strip() != day_utc:
        mismatches.append("day_utc")
    if str(operator.get("prior_submission_id") or "").strip() != prior_submission_id:
        mismatches.append("prior_submission_id")
    if str(operator.get("ib_account") or "").strip() != account:
        mismatches.append("ib_account")
    if _coerce_int(operator.get("broker_order_id")) != order_id:
        mismatches.append("broker_order_id")
    expected_hashes = {
        "closure_artifact_sha256": closure_sha,
        "broker_submission_record_sha256": prior_section["broker_submission_record_sha256"],
        "broker_order_outcome_sha256": prior_section["broker_order_outcome_sha256"],
        "execution_event_record_sha256": prior_section["execution_event_record_sha256"],
    }
    for field, expected in expected_hashes.items():
        if str(operator.get(field) or "").strip() != str(expected):
            mismatches.append(field)
    if not str(operator.get("approved_at_utc") or "").strip():
        mismatches.append("approved_at_utc")
    if not str(operator.get("approval_reason") or "").strip():
        mismatches.append("approval_reason")
    acknowledgements = operator.get("acknowledgements") if isinstance(operator.get("acknowledgements"), dict) else {}
    for key in ("prior_broker_submit_occurred", "prior_order_zero_fill", "second_attempt_risk_understood"):
        if acknowledgements.get(key) is not True:
            mismatches.append(f"acknowledgements.{key}")
    if mismatches:
        operator_section["mismatches"] = mismatches
        return _blocked(day_utc=day_utc, environment=environment, blocker="OPERATOR_CLEARANCE_MISMATCH", operator_action="Correct operator clearance fields and hashes.", sections=sections)

    payload = _base_payload(day_utc=day_utc, environment="PAPER", sections=sections)
    payload.update(
        {
            "status": "CLEARED",
            "canonical_blocker": "",
            "operator_clearance": {
                **operator_section,
                "operator": str(operator.get("operator") or operator.get("operator_id") or "").strip(),
                "approved_at_utc": str(operator.get("approved_at_utc") or "").strip(),
                "approval_reason": str(operator.get("approval_reason") or "").strip(),
                "acknowledgements": acknowledgements,
            },
            "operator_next_action": "Rerun paper day authority and submit readiness; any second submit still requires fresh lineage and IB preview PASS.",
        }
    )
    return payload


def write_paper_second_attempt_clearance_v1(*, truth_root: Path, payload: dict[str, Any]) -> Path:
    path = clearance_path_v1(
        truth_root=truth_root,
        day_utc=str(payload.get("day_utc") or ""),
        prior_submission_id=str((payload.get("prior_submission") or {}).get("submission_id") or ""),
    )
    _write_json(path, payload)
    return path


def load_clearance_for_submission_v1(*, truth_root: Path, day_utc: str, prior_submission_id: str) -> dict[str, Any]:
    path = clearance_path_v1(truth_root=truth_root, day_utc=day_utc, prior_submission_id=prior_submission_id)
    if not path.exists() or not path.is_file():
        return {"status": "MISSING", "path": str(path), "canonical_blocker": "OPERATOR_CLEARANCE_MISSING"}
    payload = _read_json(path)
    payload["_path"] = str(path)
    return payload


def load_prior_order_plan_from_clearance_v1(clearance: dict[str, Any]) -> dict[str, Any]:
    prior = clearance.get("prior_submission") if isinstance(clearance.get("prior_submission"), dict) else {}
    path_text = str(prior.get("order_plan_path") or "").strip()
    if not path_text:
        raise PaperSecondAttemptClearanceError("PRIOR_ORDER_PLAN_PATH_MISSING")
    path = Path(path_text).resolve()
    if not path.exists() or not path.is_file():
        raise PaperSecondAttemptClearanceError(f"PRIOR_ORDER_PLAN_MISSING:path={path}")
    expected_sha = str(prior.get("order_plan_sha256") or "").strip()
    actual_sha = sha256_file_v1(path)
    if expected_sha and expected_sha != actual_sha:
        raise PaperSecondAttemptClearanceError("PRIOR_ORDER_PLAN_HASH_MISMATCH")
    return _read_json(path)


def plan_hash_v1(plan: dict[str, Any]) -> str:
    explicit = str(plan.get("plan_hash") or "").strip()
    return explicit if len(explicit) == 64 else canonical_hash_for_c2_artifact_v1(plan)


def structure_pricing_signature_v1(plan: dict[str, Any]) -> str:
    risk_proof = plan.get("risk_proof") if isinstance(plan.get("risk_proof"), dict) else {}
    payload = {
        "schema_id": plan.get("schema_id"),
        "schema_version": plan.get("schema_version"),
        "structure": plan.get("structure"),
        "underlying": plan.get("underlying"),
        "symbol": plan.get("symbol"),
        "action": plan.get("action"),
        "qty_shares": plan.get("qty_shares"),
        "legs": plan.get("legs"),
        "order_terms": plan.get("order_terms"),
        "risk_proof_contracts": risk_proof.get("contracts"),
    }
    return canonical_hash_for_c2_artifact_v1(payload)
