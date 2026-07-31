from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .memory_index import memory_root
from .memory_models import MECHANISM_TAGS, MemoryEvidenceMaturity

FAILURE_FILE = "failure_patterns.json"
RETIREMENT_ACTIVE = "ACTIVE_WARNING"
RETIREMENT_RETIRED = "RETIRED_DO_NOT_REPEAT"
RETIREMENT_REOPENED = "REOPENED_LIMITED_SCOPE"


class FailurePatternError(ValueError):
    pass


def failure_path(root: str | Path = DEFAULT_STORE_ROOT) -> Path:
    return memory_root(root) / FAILURE_FILE


def record_failure_pattern(*, root: str | Path = DEFAULT_STORE_ROOT, failure_id: str, failure_type: str, source_artifact_ids: list[str], mechanism_tags: list[str], reason: str, evidence_level: str, regime_context_ids: list[str] | None = None, first_seen_at: str = "2026-06-04T00:00:00Z", metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    _validate(mechanism_tags, evidence_level)
    rows = _read(root)
    if any(row["failure_id"] == failure_id for row in rows):
        raise FailurePatternError(f"failure exists: {failure_id}")
    row = {
        "failure_id": failure_id,
        "failure_type": failure_type,
        "source_artifact_ids": sorted(set(source_artifact_ids)),
        "mechanism_tags": sorted(set(mechanism_tags)),
        "regime_context_ids": sorted(set(regime_context_ids or [])),
        "reason": reason,
        "evidence_level": evidence_level,
        "repetition_count": 1,
        "first_seen_at": first_seen_at,
        "last_seen_at": first_seen_at,
        "retirement_status": RETIREMENT_ACTIVE,
        "metadata": dict(metadata or {}),
    }
    rows.append(row)
    _write(root, rows)
    return row


def increment_failure_repetition(root: str | Path, failure_id: str, *, source_artifact_id: str | None = None, seen_at: str = "2026-06-04T00:00:00Z") -> dict[str, Any]:
    rows = _read(root)
    for row in rows:
        if row["failure_id"] == failure_id:
            row["repetition_count"] = int(row.get("repetition_count", 1)) + 1
            row["last_seen_at"] = seen_at
            if source_artifact_id and source_artifact_id not in row.get("source_artifact_ids", []):
                row["source_artifact_ids"].append(source_artifact_id)
                row["source_artifact_ids"] = sorted(row["source_artifact_ids"])
            _write(root, rows)
            return row
    raise FailurePatternError(f"failure not found: {failure_id}")


def list_failure_patterns(root: str | Path = DEFAULT_STORE_ROOT) -> list[dict[str, Any]]:
    return _read(root)


def get_failure_patterns_by_mechanism(root: str | Path, mechanism_tag: str) -> list[dict[str, Any]]:
    return [row for row in _read(root) if mechanism_tag in row.get("mechanism_tags", [])]


def get_repeated_failures(root: str | Path = DEFAULT_STORE_ROOT, min_repetition_count: int = 2) -> list[dict[str, Any]]:
    return [row for row in _read(root) if int(row.get("repetition_count", 0)) >= min_repetition_count]


def retire_failure_pattern(root: str | Path, failure_id: str, *, retired_at: str = "2026-06-04T00:00:00Z", reason: str = "") -> dict[str, Any]:
    return _set_retirement(root, failure_id, RETIREMENT_RETIRED, retired_at, reason)


def reopen_failure_pattern(root: str | Path, failure_id: str, *, reopened_at: str = "2026-06-04T00:00:00Z", reason: str = "") -> dict[str, Any]:
    return _set_retirement(root, failure_id, RETIREMENT_REOPENED, reopened_at, reason)


def can_failure_influence_priority(failure: dict[str, Any]) -> bool:
    return failure.get("retirement_status") != RETIREMENT_RETIRED


def _set_retirement(root: str | Path, failure_id: str, status: str, at: str, reason: str) -> dict[str, Any]:
    rows = _read(root)
    for row in rows:
        if row["failure_id"] == failure_id:
            row["retirement_status"] = status
            row["last_seen_at"] = at
            row.setdefault("metadata", {})["retirement_reason"] = reason
            _write(root, rows)
            return row
    raise FailurePatternError(f"failure not found: {failure_id}")


def _validate(mechanism_tags: list[str], evidence_level: str) -> None:
    unknown = set(mechanism_tags) - MECHANISM_TAGS
    if unknown:
        raise FailurePatternError(f"unknown mechanism tags: {sorted(unknown)}")
    if evidence_level not in {item.value for item in MemoryEvidenceMaturity}:
        raise FailurePatternError(f"invalid evidence_level: {evidence_level}")


def _read(root: str | Path) -> list[dict[str, Any]]:
    path = failure_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        _write(root, [])
    return json.loads(path.read_text(encoding="utf-8"))


def _write(root: str | Path, rows: list[dict[str, Any]]) -> None:
    path = failure_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, indent=2, sort_keys=True) + "\n", encoding="utf-8")
