from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

from constellation_2.common.paper_session_path_alignment_v1 import resolve_paper_session_kernel_path


SCHEMA_RELPATH_V1 = "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_session_kernel.v1.schema.json"
REPO_ROOT = Path(__file__).resolve().parents[2]
NONFINAL_STATES_V1 = {
    "SESSION_CREATED",
    "EVIDENCE_FROZEN",
    "INPUTS_VALIDATED",
    "EXECUTION_EVALUATED",
    "SUBMIT_BOUNDARY_EVALUATED",
    "AUTHORITY_GRANTED",
    "AUTHORITY_DENIED",
    "SUBMIT_SKIPPED",
    "SUBMIT_ATTEMPTED",
}
LEGAL_TRANSITIONS_V1 = {
    "SESSION_CREATED": {"EVIDENCE_FROZEN"},
    "EVIDENCE_FROZEN": {"INPUTS_VALIDATED"},
    "INPUTS_VALIDATED": {"EXECUTION_EVALUATED"},
    "EXECUTION_EVALUATED": {"SUBMIT_BOUNDARY_EVALUATED"},
    "SUBMIT_BOUNDARY_EVALUATED": {"AUTHORITY_GRANTED", "AUTHORITY_DENIED"},
    "AUTHORITY_GRANTED": {"SUBMIT_SKIPPED", "SUBMIT_ATTEMPTED"},
    "AUTHORITY_DENIED": {"SUBMIT_SKIPPED"},
    "SUBMIT_SKIPPED": {"SESSION_FINALIZED"},
    "SUBMIT_ATTEMPTED": {"SESSION_FINALIZED"},
}


def _sorted_codes(codes: list[str] | tuple[str, ...]) -> list[str]:
    return sorted({str(code).strip() for code in codes if str(code).strip()})


def validate_transition_history_v1(rows: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> None:
    for row in rows:
        from_state = str(row.get("from_state") or "").strip()
        to_state = str(row.get("to_state") or "").strip()
        if to_state == "SESSION_FINALIZED":
            if from_state not in {"SUBMIT_SKIPPED", "SUBMIT_ATTEMPTED"}:
                raise ValueError(f"PAPER_SESSION_KERNEL_ILLEGAL_TRANSITION:{from_state}->{to_state}")
            continue
        allowed = LEGAL_TRANSITIONS_V1.get(from_state, set())
        if to_state not in allowed:
            raise ValueError(f"PAPER_SESSION_KERNEL_ILLEGAL_TRANSITION:{from_state}->{to_state}")


@dataclass(frozen=True, slots=True)
class PaperSessionKernelV1:
    schema_id: str
    schema_version: str
    authority_scope: str
    day_utc: str
    session_id: str
    kernel_decision_id: str
    evaluated_at_utc: str
    manifest_id: str
    evidence_digest: str
    current_state: str
    authority_status: str
    system_ready: bool
    submission_authorized: bool
    blocking_codes: tuple[str, ...]
    transition_history: tuple[dict[str, Any], ...]
    finalization_status: str
    provenance: dict[str, Any]

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> "PaperSessionKernelV1":
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH_V1)
        validate_transition_history_v1(list(obj["transition_history"]))
        return cls(
            schema_id=str(obj["schema_id"]),
            schema_version=str(obj["schema_version"]),
            authority_scope=str(obj["authority_scope"]),
            day_utc=str(obj["day_utc"]),
            session_id=str(obj["session_id"]),
            kernel_decision_id=str(obj["kernel_decision_id"]),
            evaluated_at_utc=str(obj["evaluated_at_utc"]),
            manifest_id=str(obj["manifest_id"]),
            evidence_digest=str(obj["evidence_digest"]),
            current_state=str(obj["current_state"]),
            authority_status=str(obj["authority_status"]),
            system_ready=bool(obj["system_ready"]),
            submission_authorized=bool(obj["submission_authorized"]),
            blocking_codes=tuple(_sorted_codes(list(obj["blocking_codes"]))),
            transition_history=tuple(dict(item) for item in obj["transition_history"]),
            finalization_status=str(obj["finalization_status"]),
            provenance=dict(obj["provenance"]),
        )

    def to_dict(self) -> dict[str, Any]:
        obj = {
            "schema_id": self.schema_id,
            "schema_version": self.schema_version,
            "authority_scope": self.authority_scope,
            "day_utc": self.day_utc,
            "session_id": self.session_id,
            "kernel_decision_id": self.kernel_decision_id,
            "evaluated_at_utc": self.evaluated_at_utc,
            "manifest_id": self.manifest_id,
            "evidence_digest": self.evidence_digest,
            "current_state": self.current_state,
            "authority_status": self.authority_status,
            "system_ready": self.system_ready,
            "submission_authorized": self.submission_authorized,
            "blocking_codes": list(self.blocking_codes),
            "transition_history": [dict(item) for item in self.transition_history],
            "finalization_status": self.finalization_status,
            "provenance": dict(self.provenance),
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH_V1)
        return obj


def build_transition_history_v1(
    *,
    evaluated_at_utc: str,
    authority_status: str,
    manifest_id: str,
) -> list[dict[str, Any]]:
    authority_state = "AUTHORITY_GRANTED" if str(authority_status) == "GRANTED" else "AUTHORITY_DENIED"
    rows = [
        {
            "from_state": "SESSION_CREATED",
            "to_state": "EVIDENCE_FROZEN",
            "transition_at_utc": evaluated_at_utc,
            "transition_reason_code": "MANIFEST_BOUND",
            "evidence_reference": manifest_id,
        },
        {
            "from_state": "EVIDENCE_FROZEN",
            "to_state": "INPUTS_VALIDATED",
            "transition_at_utc": evaluated_at_utc,
            "transition_reason_code": "MANIFEST_VALIDATED",
            "evidence_reference": manifest_id,
        },
        {
            "from_state": "INPUTS_VALIDATED",
            "to_state": "EXECUTION_EVALUATED",
            "transition_at_utc": evaluated_at_utc,
            "transition_reason_code": "EXECUTION_FACT_EVALUATED",
            "evidence_reference": "sleeve_rollup_v1",
        },
        {
            "from_state": "EXECUTION_EVALUATED",
            "to_state": "SUBMIT_BOUNDARY_EVALUATED",
            "transition_at_utc": evaluated_at_utc,
            "transition_reason_code": "SUBMIT_BOUNDARY_FACT_EVALUATED",
            "evidence_reference": "submit_boundary_status_v1",
        },
        {
            "from_state": "SUBMIT_BOUNDARY_EVALUATED",
            "to_state": authority_state,
            "transition_at_utc": evaluated_at_utc,
            "transition_reason_code": f"KERNEL_{authority_state}",
            "evidence_reference": manifest_id,
        },
        {
            "from_state": authority_state,
            "to_state": "SUBMIT_SKIPPED",
            "transition_at_utc": evaluated_at_utc,
            "transition_reason_code": "NO_SUBMIT_ATTEMPT_WITHIN_KERNEL",
            "evidence_reference": manifest_id,
        },
        {
            "from_state": "SUBMIT_SKIPPED",
            "to_state": "SESSION_FINALIZED",
            "transition_at_utc": evaluated_at_utc,
            "transition_reason_code": "SESSION_FINALIZED_IMMUTABLE",
            "evidence_reference": manifest_id,
        },
    ]
    validate_transition_history_v1(rows)
    return rows


def build_paper_session_kernel_v1(
    *,
    day_utc: str,
    session_id: str,
    evaluated_at_utc: str,
    manifest_id: str,
    evidence_digest: str,
    authority_status: str,
    system_ready: bool,
    submission_authorized: bool,
    blocking_codes: list[str] | tuple[str, ...],
    provenance: dict[str, Any],
) -> PaperSessionKernelV1:
    kernel_seed = canonical_json_bytes_v1(
        {
            "day_utc": str(day_utc),
            "session_id": str(session_id),
            "manifest_id": str(manifest_id),
            "evidence_digest": str(evidence_digest),
            "authority_status": str(authority_status),
            "blocking_codes": _sorted_codes(list(blocking_codes)),
        }
    )
    kernel_decision_id = f"paper_session_kernel:{str(day_utc)}:{hashlib.sha256(kernel_seed).hexdigest()[:16]}"
    return PaperSessionKernelV1.from_dict(
        {
            "schema_id": "paper_session_kernel",
            "schema_version": "v1",
            "authority_scope": "CANONICAL_SESSION_AUTHORITY",
            "day_utc": str(day_utc),
            "session_id": str(session_id),
            "kernel_decision_id": kernel_decision_id,
            "evaluated_at_utc": str(evaluated_at_utc),
            "manifest_id": str(manifest_id),
            "evidence_digest": str(evidence_digest),
            "current_state": "SESSION_FINALIZED",
            "authority_status": str(authority_status),
            "system_ready": bool(system_ready),
            "submission_authorized": bool(submission_authorized),
            "blocking_codes": _sorted_codes(list(blocking_codes)),
            "transition_history": build_transition_history_v1(
                evaluated_at_utc=evaluated_at_utc,
                authority_status=str(authority_status),
                manifest_id=str(manifest_id),
            ),
            "finalization_status": "FINALIZED",
            "provenance": dict(provenance),
        }
    )


def write_paper_session_kernel_v1(
    *,
    truth_root: Path,
    kernel: PaperSessionKernelV1,
) -> Path:
    path = resolve_paper_session_kernel_path(truth_root=truth_root.resolve(), day_utc=kernel.day_utc)
    payload = kernel.to_dict()
    raw = canonical_json_bytes_v1(payload) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        existing = PaperSessionKernelV1.from_dict(json.loads(path.read_text(encoding="utf-8")))
        if existing.kernel_decision_id != kernel.kernel_decision_id or existing.evidence_digest != kernel.evidence_digest:
            raise ValueError(f"PAPER_SESSION_KERNEL_IMMUTABLE_MISMATCH:path={path}")
        return path
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(raw)
    tmp.replace(path)
    return path


def assert_paper_session_kernel_granted_v1(
    *,
    path: Path,
    day_utc: str,
) -> PaperSessionKernelV1:
    payload = json.loads(path.read_text(encoding="utf-8"))
    kernel = PaperSessionKernelV1.from_dict(payload)
    if kernel.day_utc != str(day_utc):
        raise SystemExit(f"FAIL: PAPER_SESSION_KERNEL_DAY_MISMATCH path={path}")
    if kernel.finalization_status != "FINALIZED" or kernel.current_state != "SESSION_FINALIZED":
        raise SystemExit(f"FAIL: PAPER_SESSION_KERNEL_NOT_FINALIZED path={path}")
    if kernel.authority_status != "GRANTED" or not kernel.system_ready or not kernel.submission_authorized:
        raise SystemExit(f"FAIL: PAPER_SESSION_KERNEL_DENIED path={path}")
    return kernel
