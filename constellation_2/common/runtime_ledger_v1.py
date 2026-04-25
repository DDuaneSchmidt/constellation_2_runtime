from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from constellation_2.common.paper_session_path_alignment_v1 import resolve_runtime_ledger_path
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
RUNTIME_LEDGER_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/RUNTIME/runtime_ledger.v1.schema.json"


@dataclass(frozen=True)
class RuntimeLedgerAppendResultV1:
    ledger_path: Path
    ledger_sha256: str
    appended_event_ids: tuple[str, ...]
    reused_event_ids: tuple[str, ...]
    event_types: tuple[str, ...]
    event_count: int


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _sha256_file(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _normalize_runtime_ledger_event_id_payload(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_id": str(event.get("schema_id") or "runtime_ledger"),
        "schema_version": str(event.get("schema_version") or "v1"),
        "day_utc": str(event.get("day_utc") or ""),
        "event_type": str(event.get("event_type") or ""),
        "owner_plane": str(event.get("owner_plane") or ""),
        "owner_tool": str(event.get("owner_tool") or ""),
        "session_id": str(event.get("session_id") or ""),
        "submission_id": str(event.get("submission_id") or ""),
        "order_id": str(event.get("order_id") or ""),
        "perm_id": str(event.get("perm_id") or ""),
        "payload_ref": str(event.get("payload_ref") or ""),
        "payload_hash": str(event.get("payload_hash") or ""),
        "identity_key": str(event.get("identity_key") or ""),
        "identity_tuple": dict(event.get("identity_tuple") or {}),
        "event_payload_summary": dict(event.get("event_payload_summary") or {}),
    }


def _runtime_ledger_event_id(event: dict[str, Any]) -> str:
    return canonical_hash_for_c2_artifact_v1(_normalize_runtime_ledger_event_id_payload(event))


def _existing_event_ids(path: Path) -> set[str]:
    if not path.exists() or not path.is_file():
        return set()
    event_ids: set[str] = set()
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        obj = json.loads(line)
        if isinstance(obj, dict):
            event_id = str(obj.get("ledger_event_id") or "").strip()
            if event_id:
                event_ids.add(event_id)
    return event_ids


def _runtime_ledger_artifact_ref(ledger_path: Path) -> dict[str, Any]:
    if not ledger_path.exists() or not ledger_path.is_file():
        return {
            "path": str(ledger_path),
            "exists": False,
            "sha256": "",
            "status": "MISSING",
            "reason_codes": ["RUNTIME_LEDGER_MISSING"],
        }
    return {
        "path": str(ledger_path),
        "exists": True,
        "sha256": _sha256_file(ledger_path),
        "status": "PRESENT",
        "reason_codes": [],
    }


def runtime_ledger_ref_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    return _runtime_ledger_artifact_ref(resolve_runtime_ledger_path(truth_root=truth_root, day_utc=day_utc))


def read_runtime_ledger_events_v1(*, truth_root: Path, day_utc: str) -> list[dict[str, Any]]:
    ledger_path = resolve_runtime_ledger_path(truth_root=truth_root, day_utc=day_utc)
    if not ledger_path.exists() or not ledger_path.is_file():
        return []
    events: list[dict[str, Any]] = []
    for raw_line in ledger_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        obj = json.loads(line)
        if not isinstance(obj, dict):
            continue
        validate_against_repo_schema_v1(obj, REPO_ROOT, RUNTIME_LEDGER_SCHEMA_RELPATH)
        events.append(obj)
    return events


def append_runtime_ledger_events_v1(
    *,
    truth_root: Path,
    day_utc: str,
    events: Iterable[dict[str, Any]],
) -> RuntimeLedgerAppendResultV1:
    ledger_path = resolve_runtime_ledger_path(truth_root=truth_root, day_utc=day_utc)
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    existing_event_ids = _existing_event_ids(ledger_path)
    appended_event_ids: list[str] = []
    reused_event_ids: list[str] = []
    event_types: list[str] = []
    payload_chunks: list[bytes] = []

    for raw_event in events:
        event = dict(raw_event)
        event["schema_id"] = "runtime_ledger"
        event["schema_version"] = "v1"
        event["day_utc"] = str(day_utc)
        event["produced_utc"] = str(event.get("produced_utc") or "")
        event["owner_plane"] = str(event.get("owner_plane") or "")
        event["owner_tool"] = str(event.get("owner_tool") or "")
        event["owner_run_id"] = str(event.get("owner_run_id") or "")
        event["run_id"] = str(event.get("run_id") or "")
        event["session_id"] = str(event.get("session_id") or "")
        event["submission_id"] = str(event.get("submission_id") or "")
        event["order_id"] = str(event.get("order_id") or "")
        event["perm_id"] = str(event.get("perm_id") or "")
        event["payload_ref"] = str(event.get("payload_ref") or "")
        event["payload_hash"] = str(event.get("payload_hash") or "")
        event["identity_key"] = str(event.get("identity_key") or "")
        event["identity_tuple"] = dict(event.get("identity_tuple") or {})
        event["event_payload_summary"] = dict(event.get("event_payload_summary") or {})
        event["ledger_event_id"] = _runtime_ledger_event_id(event)
        validate_against_repo_schema_v1(event, REPO_ROOT, RUNTIME_LEDGER_SCHEMA_RELPATH)
        event_types.append(str(event["event_type"]))
        if event["ledger_event_id"] in existing_event_ids:
            reused_event_ids.append(str(event["ledger_event_id"]))
            continue
        payload_chunks.append(canonical_json_bytes_v1(event) + b"\n")
        appended_event_ids.append(str(event["ledger_event_id"]))
        existing_event_ids.add(str(event["ledger_event_id"]))

    if payload_chunks:
        with ledger_path.open("ab") as handle:
            for chunk in payload_chunks:
                handle.write(chunk)

    ledger_sha256 = _sha256_file(ledger_path) if ledger_path.exists() else ""
    return RuntimeLedgerAppendResultV1(
        ledger_path=ledger_path,
        ledger_sha256=ledger_sha256,
        appended_event_ids=tuple(appended_event_ids),
        reused_event_ids=tuple(reused_event_ids),
        event_types=tuple(event_types),
        event_count=len(event_types),
    )


def projection_over_runtime_ledger_v1(
    *,
    truth_root: Path,
    day_utc: str,
    append_result: RuntimeLedgerAppendResultV1 | None = None,
    matched_event_types: Iterable[str] = (),
    projection_notice: str,
) -> dict[str, Any]:
    ledger_path = resolve_runtime_ledger_path(truth_root=truth_root, day_utc=day_utc)
    ref = _runtime_ledger_artifact_ref(ledger_path)
    appended_event_ids = list(append_result.appended_event_ids) if append_result is not None else []
    reused_event_ids = list(append_result.reused_event_ids) if append_result is not None else []
    event_types = {
        str(event_type).strip()
        for event_type in matched_event_types
        if str(event_type).strip()
    }
    if append_result is not None:
        event_types.update(str(event_type).strip() for event_type in append_result.event_types if str(event_type).strip())
    return {
        "ledger_path": ref["path"],
        "ledger_sha256": ref["sha256"],
        "derived_from_runtime_ledger": bool(ref["exists"]),
        "event_types": sorted(event_types),
        "event_count": len(event_types),
        "appended_event_ids": appended_event_ids,
        "reused_event_ids": reused_event_ids,
        "projection_notice": projection_notice,
    }
