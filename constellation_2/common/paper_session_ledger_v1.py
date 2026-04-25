from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

from constellation_2.common.paper_session_path_alignment_v1 import resolve_paper_session_ledger_path


SCHEMA_RELPATH_V1 = "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_session_ledger.v1.schema.json"
REPO_ROOT = Path(__file__).resolve().parents[2]

STATE_ORDER_V1 = {
    "SESSION_CREATED": 0,
    "EVIDENCE_FROZEN": 1,
    "INPUTS_VALIDATED": 2,
    "EXECUTION_EVALUATED": 3,
    "SUBMIT_BOUNDARY_EVALUATED": 4,
    "AUTHORITY_DENIED": 5,
    "AUTHORITY_GRANTED": 5,
    "SUBMIT_SKIPPED": 6,
    "SUBMIT_ATTEMPTED": 6,
    "SESSION_FINALIZED": 7,
}
LEGAL_TRANSITIONS_V1 = {
    "SESSION_CREATED": {"EVIDENCE_FROZEN"},
    "EVIDENCE_FROZEN": {"INPUTS_VALIDATED"},
    "INPUTS_VALIDATED": {"EXECUTION_EVALUATED"},
    "EXECUTION_EVALUATED": {"SUBMIT_BOUNDARY_EVALUATED"},
    "SUBMIT_BOUNDARY_EVALUATED": {"AUTHORITY_GRANTED", "AUTHORITY_DENIED"},
    "AUTHORITY_GRANTED": {"SUBMIT_ATTEMPTED", "SUBMIT_SKIPPED"},
    "AUTHORITY_DENIED": {"SUBMIT_SKIPPED"},
    "SUBMIT_ATTEMPTED": {"SESSION_FINALIZED"},
    "SUBMIT_SKIPPED": {"SESSION_FINALIZED"},
}


def _sorted_codes(codes: list[str] | tuple[str, ...]) -> list[str]:
    return sorted({str(code).strip() for code in codes if str(code).strip()})


def validate_transition_history_v1(rows: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> None:
    for row in rows:
        from_state = str(row.get("from_state") or "").strip()
        to_state = str(row.get("to_state") or "").strip()
        if to_state == "SESSION_FINALIZED":
            if from_state not in {"SUBMIT_SKIPPED", "SUBMIT_ATTEMPTED"}:
                raise ValueError(f"PAPER_SESSION_LEDGER_ILLEGAL_TRANSITION:{from_state}->{to_state}")
            continue
        allowed = LEGAL_TRANSITIONS_V1.get(from_state, set())
        if to_state not in allowed:
            raise ValueError(f"PAPER_SESSION_LEDGER_ILLEGAL_TRANSITION:{from_state}->{to_state}")


def _normalize_rows(rows: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> list[dict[str, Any]]:
    return [dict(item) for item in rows]


def _normalize_section(obj: dict[str, Any]) -> dict[str, Any]:
    section = dict(obj)
    if "blocking_codes" in section:
        section["blocking_codes"] = _sorted_codes(list(section.get("blocking_codes") or []))
    if "reason_codes" in section:
        section["reason_codes"] = _sorted_codes(list(section.get("reason_codes") or []))
    if "gap_codes" in section:
        section["gap_codes"] = _sorted_codes(list(section.get("gap_codes") or []))
    return section


def _normalize_volatile_fields_v1(value: Any, *, volatile_field_names: set[str]) -> Any:
    if isinstance(value, dict):
        return {
            str(key): (
                None
                if str(key) in volatile_field_names
                else _normalize_volatile_fields_v1(item, volatile_field_names=volatile_field_names)
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [
            _normalize_volatile_fields_v1(item, volatile_field_names=volatile_field_names)
            for item in value
        ]
    return value


@dataclass(frozen=True, slots=True)
class PaperSessionLedgerV1:
    schema_id: str
    schema_version: str
    authority_scope: str
    day_utc: str
    session_id: str
    ledger_id: str
    evaluated_at_utc: str
    provenance: dict[str, Any]
    fact_refs: tuple[dict[str, Any], ...]
    evidence_freeze: dict[str, Any]
    control_state: dict[str, Any]
    submit_lifecycle: dict[str, Any]
    post_submit_lifecycle: dict[str, Any]
    operator_summary: dict[str, Any]
    constitutional_context: dict[str, Any] | None = None

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> "PaperSessionLedgerV1":
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH_V1)
        control_state = dict(obj["control_state"])
        validate_transition_history_v1(list(control_state["transition_history"]))
        return cls(
            schema_id=str(obj["schema_id"]),
            schema_version=str(obj["schema_version"]),
            authority_scope=str(obj["authority_scope"]),
            day_utc=str(obj["day_utc"]),
            session_id=str(obj["session_id"]),
            ledger_id=str(obj["ledger_id"]),
            evaluated_at_utc=str(obj["evaluated_at_utc"]),
            provenance=dict(obj["provenance"]),
            fact_refs=tuple(_normalize_rows(list(obj["fact_refs"]))),
            evidence_freeze=_normalize_section(dict(obj["evidence_freeze"])),
            control_state=_normalize_section(control_state),
            submit_lifecycle=_normalize_section(dict(obj["submit_lifecycle"])),
            post_submit_lifecycle=_normalize_section(dict(obj["post_submit_lifecycle"])),
            operator_summary=_normalize_section(dict(obj["operator_summary"])),
            constitutional_context=(
                _normalize_section(dict(obj["constitutional_context"]))
                if isinstance(obj.get("constitutional_context"), dict)
                else None
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "schema_id": self.schema_id,
            "schema_version": self.schema_version,
            "authority_scope": self.authority_scope,
            "day_utc": self.day_utc,
            "session_id": self.session_id,
            "ledger_id": self.ledger_id,
            "evaluated_at_utc": self.evaluated_at_utc,
            "provenance": dict(self.provenance),
            "fact_refs": [dict(item) for item in self.fact_refs],
            "evidence_freeze": _normalize_section(dict(self.evidence_freeze)),
            "control_state": _normalize_section(dict(self.control_state)),
            "submit_lifecycle": _normalize_section(dict(self.submit_lifecycle)),
            "post_submit_lifecycle": _normalize_section(dict(self.post_submit_lifecycle)),
            "operator_summary": _normalize_section(dict(self.operator_summary)),
        }
        if isinstance(self.constitutional_context, dict):
            payload["constitutional_context"] = _normalize_section(dict(self.constitutional_context))
        validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA_RELPATH_V1)
        validate_transition_history_v1(list(payload["control_state"]["transition_history"]))
        return payload


def build_transition_history_v1(
    *,
    evaluated_at_utc: str,
    authority_status: str,
    submit_attempted: bool,
    finalization_status: str,
    evidence_reference: str,
    execution_reference: str,
    submit_reference: str,
) -> tuple[list[dict[str, Any]], str]:
    authority_state = "AUTHORITY_GRANTED" if str(authority_status).strip().upper() == "GRANTED" else "AUTHORITY_DENIED"
    submit_state = "SUBMIT_ATTEMPTED" if bool(submit_attempted) else "SUBMIT_SKIPPED"
    rows = [
        {
            "from_state": "SESSION_CREATED",
            "to_state": "EVIDENCE_FROZEN",
            "transition_at_utc": evaluated_at_utc,
            "transition_reason_code": "LEDGER_EVIDENCE_FROZEN",
            "evidence_reference": evidence_reference,
        },
        {
            "from_state": "EVIDENCE_FROZEN",
            "to_state": "INPUTS_VALIDATED",
            "transition_at_utc": evaluated_at_utc,
            "transition_reason_code": "LEDGER_INPUTS_VALIDATED",
            "evidence_reference": evidence_reference,
        },
        {
            "from_state": "INPUTS_VALIDATED",
            "to_state": "EXECUTION_EVALUATED",
            "transition_at_utc": evaluated_at_utc,
            "transition_reason_code": "LEDGER_EXECUTION_EVALUATED",
            "evidence_reference": execution_reference,
        },
        {
            "from_state": "EXECUTION_EVALUATED",
            "to_state": "SUBMIT_BOUNDARY_EVALUATED",
            "transition_at_utc": evaluated_at_utc,
            "transition_reason_code": "LEDGER_SUBMIT_BOUNDARY_EVALUATED",
            "evidence_reference": "submit_boundary_status_v1",
        },
        {
            "from_state": "SUBMIT_BOUNDARY_EVALUATED",
            "to_state": authority_state,
            "transition_at_utc": evaluated_at_utc,
            "transition_reason_code": f"LEDGER_{authority_state}",
            "evidence_reference": evidence_reference,
        },
    ]
    current_state = authority_state
    if authority_state == "AUTHORITY_DENIED":
        rows.append(
            {
                "from_state": "AUTHORITY_DENIED",
                "to_state": "SUBMIT_SKIPPED",
                "transition_at_utc": evaluated_at_utc,
                "transition_reason_code": "LEDGER_SUBMIT_SKIPPED_AUTHORITY_DENIED",
                "evidence_reference": evidence_reference,
            }
        )
        current_state = "SUBMIT_SKIPPED"
    elif submit_attempted:
        rows.append(
            {
                "from_state": "AUTHORITY_GRANTED",
                "to_state": "SUBMIT_ATTEMPTED",
                "transition_at_utc": evaluated_at_utc,
                "transition_reason_code": "LEDGER_SUBMIT_ATTEMPT_OBSERVED",
                "evidence_reference": submit_reference,
            }
        )
        current_state = "SUBMIT_ATTEMPTED"
    if str(finalization_status).strip().upper() == "FINALIZED":
        rows.append(
            {
                "from_state": submit_state if authority_state == "AUTHORITY_GRANTED" else "SUBMIT_SKIPPED",
                "to_state": "SESSION_FINALIZED",
                "transition_at_utc": evaluated_at_utc,
                "transition_reason_code": "LEDGER_SESSION_FINALIZED",
                "evidence_reference": submit_reference,
            }
        )
        current_state = "SESSION_FINALIZED"
    validate_transition_history_v1(rows)
    return rows, current_state


def build_paper_session_ledger_v1(
    *,
    day_utc: str,
    session_id: str,
    evaluated_at_utc: str,
    provenance: dict[str, Any],
    fact_refs: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    evidence_freeze: dict[str, Any],
    authority_status: str,
    system_ready: bool,
    submission_authorized: bool,
    control_blocking_codes: list[str] | tuple[str, ...],
    submit_lifecycle: dict[str, Any],
    post_submit_lifecycle: dict[str, Any],
    operator_summary: dict[str, Any],
    constitutional_context: dict[str, Any] | None = None,
) -> PaperSessionLedgerV1:
    ledger_seed = canonical_json_bytes_v1({"day_utc": str(day_utc), "session_id": str(session_id)})
    ledger_id = f"paper_session_ledger:{str(day_utc)}:{hashlib.sha256(ledger_seed).hexdigest()[:16]}"
    submit_attempted = bool(dict(submit_lifecycle).get("submit_attempted") is True)
    finalization_status = str(dict(submit_lifecycle).get("finalization_status") or "OPEN").strip().upper()
    transition_history, current_state = build_transition_history_v1(
        evaluated_at_utc=evaluated_at_utc,
        authority_status=authority_status,
        submit_attempted=submit_attempted,
        finalization_status=finalization_status,
        evidence_reference=str(dict(evidence_freeze).get("evidence_digest") or ""),
        execution_reference=str(dict(post_submit_lifecycle).get("latest_authoritative_lineage_ref") or "sleeve_rollup_v1:not_observed"),
        submit_reference=str(dict(post_submit_lifecycle).get("latest_authoritative_lineage_ref") or "post_submit_lifecycle"),
    )
    payload = {
        "schema_id": "paper_session_ledger",
        "schema_version": "v1",
        "authority_scope": "CANONICAL_SESSION_LEDGER",
        "day_utc": str(day_utc),
        "session_id": str(session_id),
        "ledger_id": ledger_id,
        "evaluated_at_utc": str(evaluated_at_utc),
        "provenance": dict(provenance),
        "fact_refs": _normalize_rows(list(fact_refs)),
        "evidence_freeze": _normalize_section(dict(evidence_freeze)),
        "control_state": {
            "current_state": current_state,
            "authority_status": str(authority_status).strip().upper(),
            "system_ready": bool(system_ready),
            "submission_authorized": bool(submission_authorized),
            "blocking_codes": _sorted_codes(list(control_blocking_codes)),
            "transition_history": transition_history,
        },
        "submit_lifecycle": _normalize_section(
            {
                **dict(submit_lifecycle),
                "submit_attempted": bool(submit_attempted),
                "finalization_status": finalization_status,
            }
        ),
        "post_submit_lifecycle": _normalize_section(dict(post_submit_lifecycle)),
        "operator_summary": _normalize_section(dict(operator_summary)),
    }
    if isinstance(constitutional_context, dict):
        payload["constitutional_context"] = _normalize_section(dict(constitutional_context))
    return PaperSessionLedgerV1.from_dict(payload)


def write_paper_session_ledger_v1(
    *,
    truth_root: Path,
    ledger: PaperSessionLedgerV1,
) -> Path:
    path = resolve_paper_session_ledger_path(truth_root=truth_root.resolve(), day_utc=ledger.day_utc)
    payload = ledger.to_dict()
    raw = canonical_json_bytes_v1(payload) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        existing = PaperSessionLedgerV1.from_dict(json.loads(path.read_text(encoding="utf-8")))
        if existing.ledger_id != ledger.ledger_id or existing.session_id != ledger.session_id:
            raise ValueError(f"PAPER_SESSION_LEDGER_IMMUTABLE_IDENTITY_MISMATCH:path={path}")
        if str(existing.submit_lifecycle.get("finalization_status") or "").strip().upper() == "FINALIZED":
            if existing.to_dict() != payload:
                raise ValueError(f"PAPER_SESSION_LEDGER_FINALIZED_IMMUTABLE_MISMATCH:path={path}")
            return path
        existing_evaluated_at = str(existing.evaluated_at_utc).strip()
        new_evaluated_at = str(ledger.evaluated_at_utc).strip()
        if existing_evaluated_at and new_evaluated_at and new_evaluated_at < existing_evaluated_at:
            raise ValueError(f"PAPER_SESSION_LEDGER_NON_MONOTONIC_EVALUATED_AT:path={path}")
        volatile_fields = {"evaluated_at_utc", "transition_at_utc"}
        if _normalize_volatile_fields_v1(existing.to_dict(), volatile_field_names=volatile_fields) == _normalize_volatile_fields_v1(
            payload,
            volatile_field_names=volatile_fields,
        ):
            return path
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(raw)
    tmp.replace(path)
    return path


def assert_paper_session_ledger_granted_v1(
    *,
    path: Path,
    day_utc: str,
) -> PaperSessionLedgerV1:
    ledger = assert_paper_session_ledger_open_ready_v1(path=path, day_utc=day_utc)
    control = ledger.control_state
    if control.get("submission_authorized") is not True:
        raise SystemExit(f"FAIL: PAPER_SESSION_LEDGER_NOT_SUBMIT_READY path={path}")
    return ledger


def assert_paper_session_ledger_open_ready_v1(
    *,
    path: Path,
    day_utc: str,
) -> PaperSessionLedgerV1:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    ledger = PaperSessionLedgerV1.from_dict(payload)
    if str(ledger.day_utc).strip() != str(day_utc).strip():
        raise SystemExit(f"FAIL: PAPER_SESSION_LEDGER_DAY_MISMATCH path={path} day_utc={day_utc}")
    control = ledger.control_state
    if str(control.get("authority_status") or "").strip().upper() != "GRANTED":
        raise SystemExit(f"FAIL: PAPER_SESSION_LEDGER_NOT_GRANTED path={path}")
    if control.get("system_ready") is not True:
        raise SystemExit(f"FAIL: PAPER_SESSION_LEDGER_NOT_OPEN_READY path={path}")
    return ledger
