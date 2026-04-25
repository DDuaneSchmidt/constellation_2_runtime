from __future__ import annotations

from datetime import UTC, datetime
import hashlib
from pathlib import Path
from typing import Any, Mapping

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1


def build_attempt_id_v1(*, payload: Mapping[str, Any]) -> str:
    raw = canonical_json_bytes_v1(dict(payload)) + b"\n"
    digest = hashlib.sha256(raw).hexdigest()[:12]
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    return f"{stamp}__{digest}"


def resolve_day_attempt_artifact_path_v1(
    *,
    family_root: Path,
    day_utc: str,
    attempt_id: str,
    filename: str,
) -> Path:
    return (
        Path(family_root).resolve()
        / str(day_utc).strip()
        / str(attempt_id).strip()
        / str(filename).strip()
    ).resolve()
