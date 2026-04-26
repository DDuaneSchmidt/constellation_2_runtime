from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .research_event_v1 import (
    ResearchEventValidationError,
    build_event_id_v1,
    utc_now_iso,
    validate_research_event_v1,
)


DEFAULT_RESEARCH_RUNTIME_ROOT = Path("/home/node/constellation_runtime_data/research_lab").resolve()
DEFAULT_EVENTS_ROOT = (DEFAULT_RESEARCH_RUNTIME_ROOT / "events").resolve()
DEFAULT_EVENT_LOG_PATH = (DEFAULT_RESEARCH_RUNTIME_ROOT / "event_log" / "research_event_transitions.v1.jsonl").resolve()
RUNTIME_ROOT_ENV = "CONSTELLATION_RESEARCH_LAB_RUNTIME_ROOT"

EVENT_BUCKETS = ("inbox", "processed", "rejected", "failed")
_STATUS_FOR_BUCKET = {
    "inbox": "NEW",
    "processed": "PROCESSED",
    "rejected": "REJECTED",
    "failed": "FAILED",
}

REPO_ROOT = Path(__file__).resolve().parents[2]


def _is_under(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def resolve_runtime_root() -> Path:
    import os

    override = str(os.getenv(RUNTIME_ROOT_ENV, "")).strip()
    root = Path(override).expanduser().resolve() if override else DEFAULT_RESEARCH_RUNTIME_ROOT
    if _is_under(root, REPO_ROOT):
        raise ValueError(f"RESEARCH_RUNTIME_ROOT_UNDER_REPO_FORBIDDEN:{root}")
    return root


def resolve_events_root() -> Path:
    return (resolve_runtime_root() / "events").resolve()


def event_log_path() -> Path:
    return (resolve_runtime_root() / "event_log" / "research_event_transitions.v1.jsonl").resolve()


def idempotency_registry_path() -> Path:
    return (resolve_events_root() / "idempotency_registry.v1.json").resolve()


def _guard_runtime_path(path: Path) -> Path:
    resolved = path.expanduser().resolve()
    if _is_under(resolved, REPO_ROOT):
        raise ValueError(f"RESEARCH_RUNTIME_WRITE_UNDER_REPO_FORBIDDEN:{resolved}")
    runtime_root = resolve_runtime_root()
    if not _is_under(resolved, runtime_root):
        raise ValueError(f"RESEARCH_RUNTIME_WRITE_OUTSIDE_ROOT_FORBIDDEN:{resolved}")
    return resolved


def _write_json(path: Path, payload: Any) -> Path:
    target = _guard_runtime_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    return target


def _read_json(path: Path) -> dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"JSON_NOT_OBJECT:{path}")
    return obj


def _append_jsonl(path: Path, payload: dict[str, Any]) -> Path:
    target = _guard_runtime_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False) + "\n")
    return target


def ensure_event_runtime_tree() -> Path:
    root = resolve_events_root()
    for bucket in EVENT_BUCKETS:
        _guard_runtime_path((root / bucket).resolve()).mkdir(parents=True, exist_ok=True)
    _guard_runtime_path(event_log_path()).parent.mkdir(parents=True, exist_ok=True)
    _guard_runtime_path(idempotency_registry_path()).parent.mkdir(parents=True, exist_ok=True)
    if not idempotency_registry_path().exists():
        _write_json(
            idempotency_registry_path(),
            {
                "schema_version": "research_event_idempotency_registry.v1",
                "updated_utc": utc_now_iso(),
                "records": {},
            },
        )
    return root


def _event_path(bucket: str, day_utc: str, event_id: str) -> Path:
    if bucket not in EVENT_BUCKETS:
        raise ValueError(f"UNKNOWN_EVENT_BUCKET:{bucket}")
    return (resolve_events_root() / bucket / str(day_utc).strip() / f"{event_id}.research_event.v1.json").resolve()


def _load_idempotency_registry() -> dict[str, Any]:
    ensure_event_runtime_tree()
    payload = _read_json(idempotency_registry_path())
    records = payload.get("records")
    if not isinstance(records, dict):
        payload["records"] = {}
    return payload


def _persist_idempotency_registry(payload: dict[str, Any]) -> Path:
    payload = dict(payload)
    payload["schema_version"] = "research_event_idempotency_registry.v1"
    payload["updated_utc"] = utc_now_iso()
    return _write_json(idempotency_registry_path(), payload)


def _find_existing_event_by_idempotency(idempotency_key: str) -> dict[str, Any] | None:
    registry = _load_idempotency_registry()
    records = registry.get("records") if isinstance(registry.get("records"), dict) else {}
    row = records.get(str(idempotency_key).strip().lower())
    return dict(row) if isinstance(row, dict) else None


def _register_idempotency(idempotency_key: str, *, event_id: str, day_utc: str, bucket: str) -> None:
    registry = _load_idempotency_registry()
    records = dict(registry.get("records") if isinstance(registry.get("records"), dict) else {})
    records[str(idempotency_key).strip().lower()] = {
        "event_id": str(event_id).strip().lower(),
        "day_utc": str(day_utc).strip(),
        "bucket": str(bucket).strip(),
        "updated_utc": utc_now_iso(),
    }
    registry["records"] = records
    _persist_idempotency_registry(registry)


def _remove_event_from_other_buckets(event_id: str, day_utc: str, keep_bucket: str) -> None:
    for bucket in EVENT_BUCKETS:
        if bucket == keep_bucket:
            continue
        path = _event_path(bucket, day_utc, event_id)
        if path.exists() and path.is_file():
            path.unlink()


def log_event_transition_v1(
    *,
    event_id: str,
    event_type: str,
    day_utc: str,
    from_status: str,
    to_status: str,
    reason: str,
    details: dict[str, Any] | None = None,
) -> Path:
    row = {
        "schema_version": "research_event_transition.v1",
        "event_id": str(event_id).strip().lower(),
        "event_type": str(event_type).strip().upper(),
        "day_utc": str(day_utc).strip(),
        "from_status": str(from_status).strip().upper(),
        "to_status": str(to_status).strip().upper(),
        "reason": str(reason).strip(),
        "details": dict(details or {}),
        "created_utc": utc_now_iso(),
    }
    return _append_jsonl(event_log_path(), row)


def publish_event_v1(event_payload: dict[str, Any]) -> dict[str, Any]:
    ensure_event_runtime_tree()
    try:
        event = validate_research_event_v1(event_payload)
    except ResearchEventValidationError as exc:
        raw_event_id = build_event_id_v1(
            event_type=str(event_payload.get("event_type") or "INVALID"),
            day_utc=str(event_payload.get("day_utc") or "1970-01-01"),
            source=str(event_payload.get("source") or "RESEARCH_LAB"),
            idempotency_key=str(event_payload.get("idempotency_key") or "0" * 64),
        )
        day_utc = str(event_payload.get("day_utc") or "1970-01-01")
        rejected_payload = {
            "schema_version": "research_event_rejected.v1",
            "event_id": raw_event_id,
            "day_utc": day_utc,
            "status": "REJECTED",
            "reason": str(exc),
            "raw_event": event_payload,
            "created_utc": utc_now_iso(),
        }
        rejected_path = _write_json(_event_path("rejected", day_utc, raw_event_id), rejected_payload)
        log_event_transition_v1(
            event_id=raw_event_id,
            event_type=str(event_payload.get("event_type") or "INVALID"),
            day_utc=day_utc,
            from_status="NEW",
            to_status="REJECTED",
            reason="EVENT_VALIDATION_FAILED",
            details={"error": str(exc)},
        )
        return {
            "accepted": False,
            "status": "REJECTED",
            "reason": str(exc),
            "event_path": str(rejected_path),
        }

    existing = _find_existing_event_by_idempotency(event["idempotency_key"])
    if existing:
        return {
            "accepted": True,
            "status": "DUPLICATE",
            "event_id": existing.get("event_id"),
            "day_utc": existing.get("day_utc"),
            "bucket": existing.get("bucket"),
        }

    path = _write_json(_event_path("inbox", event["day_utc"], event["event_id"]), event)
    _register_idempotency(
        event["idempotency_key"],
        event_id=event["event_id"],
        day_utc=event["day_utc"],
        bucket="inbox",
    )
    log_event_transition_v1(
        event_id=event["event_id"],
        event_type=event["event_type"],
        day_utc=event["day_utc"],
        from_status="",
        to_status="NEW",
        reason="EVENT_PUBLISHED",
    )
    return {
        "accepted": True,
        "status": "NEW",
        "event_id": event["event_id"],
        "event_path": str(path),
    }


def move_event_to_bucket_v1(
    event_payload: dict[str, Any],
    *,
    target_bucket: str,
    reason: str,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if target_bucket not in EVENT_BUCKETS:
        raise ValueError(f"UNKNOWN_EVENT_BUCKET:{target_bucket}")
    event = validate_research_event_v1(event_payload)
    updated = dict(event)
    prior_status = str(updated.get("status") or "NEW").strip().upper()
    updated["status"] = _STATUS_FOR_BUCKET[target_bucket]
    updated["updated_utc"] = utc_now_iso()

    target_path = _write_json(_event_path(target_bucket, updated["day_utc"], updated["event_id"]), updated)
    _remove_event_from_other_buckets(updated["event_id"], updated["day_utc"], target_bucket)
    _register_idempotency(
        updated["idempotency_key"],
        event_id=updated["event_id"],
        day_utc=updated["day_utc"],
        bucket=target_bucket,
    )
    log_event_transition_v1(
        event_id=updated["event_id"],
        event_type=updated["event_type"],
        day_utc=updated["day_utc"],
        from_status=prior_status,
        to_status=updated["status"],
        reason=reason,
        details=details,
    )
    return {
        "event": updated,
        "path": str(target_path),
    }


def list_events_in_bucket_v1(bucket: str) -> list[dict[str, Any]]:
    if bucket not in EVENT_BUCKETS:
        raise ValueError(f"UNKNOWN_EVENT_BUCKET:{bucket}")
    ensure_event_runtime_tree()
    base = (resolve_events_root() / bucket).resolve()
    rows: list[dict[str, Any]] = []
    if not base.exists() or not base.is_dir():
        return rows
    for path in sorted(base.rglob("*.research_event.v1.json")):
        try:
            rows.append(validate_research_event_v1(_read_json(path)))
        except Exception:
            continue
    rows.sort(key=lambda row: (str(row.get("created_utc") or ""), str(row.get("event_id") or "")))
    return rows


def find_event_v1(event_id: str) -> tuple[str, dict[str, Any]] | tuple[None, None]:
    token = str(event_id).strip().lower()
    for bucket in EVENT_BUCKETS:
        base = (resolve_events_root() / bucket).resolve()
        if not base.exists() or not base.is_dir():
            continue
        for path in base.rglob(f"{token}.research_event.v1.json"):
            try:
                return bucket, validate_research_event_v1(_read_json(path))
            except Exception:
                continue
    return None, None
