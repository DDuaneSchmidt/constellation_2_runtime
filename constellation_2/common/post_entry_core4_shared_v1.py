from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
CORE4_RULE_PACK_V1 = "core4_sealed_request_snapshot_binding_boundary_v1"


def canonical_text_v1(value: Any) -> str:
    return canonical_json_bytes_v1(value).decode("utf-8")


def canonical_hash_v1(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes_v1(value)).hexdigest()


def freeze_json_v1(value: Any) -> str | None:
    if value is None:
        return None
    return canonical_text_v1(value)


def thaw_json_v1(value: str | None) -> Any:
    if value is None:
        return None
    return json.loads(value)


def unique_sorted_codes_v1(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        output.append(text)
    return sorted(output)


def now_utc_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def coerce_utc_v1(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError("UTC_TEXT_MISSING")
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_json_object_v1(path: Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT:path={path}")
    return payload


def artifact_sha256_v1(*, payload: Mapping[str, Any] | None = None, path: Path | None = None) -> str:
    if path is not None:
        return hashlib.sha256(Path(path).read_bytes()).hexdigest()
    if payload is None:
        raise ValueError("ARTIFACT_REF_REQUIRES_PATH_OR_PAYLOAD")
    return canonical_hash_v1(payload)


def execution_root_ref_v1(*, environment: str, sleeve_id: str) -> str:
    return f"sleeve_execution_root_v1::{str(sleeve_id).strip().upper()}::{str(environment).strip().upper()}"
