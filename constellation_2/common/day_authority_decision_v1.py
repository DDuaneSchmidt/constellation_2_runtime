from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


SOURCE_REPO_ROOT = Path(__file__).resolve().parents[2]
DAY_AUTHORITY_DECISION_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/day_authority_decision.v1.schema.json"


@dataclass(frozen=True)
class DayAuthorityDecisionRef:
    path: Path
    payload: Dict[str, Any]
    sha256: str


def build_day_authority_decision_v1(
    *,
    trading_day: str,
    decision_state: str,
    heartbeat: Mapping[str, str],
    first_failure: Mapping[str, str] | None,
    stage: str,
    orchestrator_started: bool,
    blocking_class: str,
    blocking_evidence: Iterable[str],
    missing_or_invalid_prerequisite_refs: Iterable[str],
    authority_attestation_refs_used: Iterable[str],
    compatibility_status: Mapping[str, Any],
    diagnostic_warnings: Iterable[str],
    truth_root: Path,
    producer_module: str,
    producer_git_sha: str,
) -> Dict[str, Any]:
    day = str(trading_day or "").strip()
    if len(day) != 10:
        raise ValueError(f"DAY_AUTHORITY_DECISION_DAY_INVALID:trading_day={trading_day!r}")
    state = str(decision_state or "").strip().upper()
    if state not in {"OPEN", "BLOCKED"}:
        raise ValueError(f"DAY_AUTHORITY_DECISION_STATE_INVALID:decision_state={decision_state!r}")
    heartbeat_status = str(heartbeat.get("status") or "").strip()
    heartbeat_ref = str(heartbeat.get("source_ref") or "").strip()
    if not heartbeat_status or not heartbeat_ref:
        raise ValueError("DAY_AUTHORITY_DECISION_HEARTBEAT_INVALID")
    if first_failure is None:
        normalized_failure = None
    else:
        normalized_failure = {
            "prerequisite_class": str(first_failure.get("prerequisite_class") or "").strip(),
            "reason_code": str(first_failure.get("reason_code") or "").strip(),
            "evidence_ref": str(first_failure.get("evidence_ref") or "").strip(),
        }
        if not all(normalized_failure.values()):
            raise ValueError(f"DAY_AUTHORITY_DECISION_FIRST_FAILURE_INVALID:first_failure={first_failure!r}")
    mapping_status = str(compatibility_status.get("mapping_status") or "").strip()
    schema_status = str(compatibility_status.get("schema_status") or "").strip()
    reader_status = str(compatibility_status.get("reader_compatibility_status") or "").strip()
    detail_rows = [str(item).strip() for item in (compatibility_status.get("details") or []) if str(item).strip()]
    if not mapping_status or not schema_status or not reader_status:
        raise ValueError("DAY_AUTHORITY_DECISION_COMPATIBILITY_INVALID")
    return {
        "schema_id": "day_authority_decision",
        "schema_version": "v1",
        "decision_id": f"day-authority-{day}",
        "trading_day": day,
        "decision_state": state,
        "heartbeat": {
            "status": heartbeat_status,
            "source_ref": heartbeat_ref,
        },
        "first_failure": normalized_failure,
        "stage": str(stage or "").strip(),
        "orchestrator_started": bool(orchestrator_started),
        "blocking_class": str(blocking_class or "").strip(),
        "blocking_evidence": [str(item).strip() for item in blocking_evidence if str(item).strip()],
        "missing_or_invalid_prerequisite_refs": [
            str(item).strip() for item in missing_or_invalid_prerequisite_refs if str(item).strip()
        ],
        "authority_attestation_refs_used": [
            str(item).strip() for item in authority_attestation_refs_used if str(item).strip()
        ],
        "compatibility_status": {
            "mapping_status": mapping_status,
            "schema_status": schema_status,
            "reader_compatibility_status": reader_status,
            "details": detail_rows,
        },
        "diagnostic_warnings": [str(item).strip() for item in diagnostic_warnings if str(item).strip()],
        "emitted_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "run_metadata": {
            "truth_root": str(Path(truth_root).resolve()),
            "producer_module": str(producer_module or "").strip(),
            "producer_git_sha": str(producer_git_sha or "").strip(),
        },
    }


def canonical_day_authority_decision_path(*, truth_root: Path, trading_day: str) -> Path:
    day = str(trading_day or "").strip()
    return (
        Path(truth_root).resolve()
        / "reports"
        / "day_authority_decision_v1"
        / day
        / "day_authority_decision.v1.json"
    ).resolve()


def write_day_authority_decision_v1(*, truth_root: Path, payload: Dict[str, Any]) -> DayAuthorityDecisionRef:
    validate_against_repo_schema_v1(payload, SOURCE_REPO_ROOT, DAY_AUTHORITY_DECISION_SCHEMA)
    path = canonical_day_authority_decision_path(truth_root=truth_root, trading_day=str(payload["trading_day"]))
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = (json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(raw)
    os.replace(str(tmp), str(path))
    sha256 = hashlib.sha256(raw).hexdigest()
    return DayAuthorityDecisionRef(path=path, payload=payload, sha256=sha256)


def read_day_authority_decision_v1(*, truth_root: Path, trading_day: str) -> DayAuthorityDecisionRef:
    path = canonical_day_authority_decision_path(truth_root=truth_root, trading_day=trading_day)
    if not path.exists() or not path.is_file():
        raise ValueError(f"DAY_AUTHORITY_DECISION_MISSING:path={path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"DAY_AUTHORITY_DECISION_NOT_OBJECT:path={path}")
    validate_against_repo_schema_v1(payload, SOURCE_REPO_ROOT, DAY_AUTHORITY_DECISION_SCHEMA)
    raw = (json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")
    return DayAuthorityDecisionRef(path=path, payload=payload, sha256=hashlib.sha256(raw).hexdigest())
