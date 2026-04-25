from __future__ import annotations

import json
from pathlib import Path
from typing import Any


EXPECTED_AUTHORITY_OWNER = "sleeve_execution_root_v1"


def _safe_read_json(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None, "FILE_NOT_FOUND"
    except json.JSONDecodeError:
        return None, "JSON_DECODE_ERROR"
    except Exception:
        return None, "READ_ERROR"
    if not isinstance(payload, dict):
        return None, "NOT_OBJECT"
    return payload, None


def read_execution_day_authority_state(
    *,
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
    mode: str,
) -> dict[str, Any]:
    del repo_root
    manifest_path = (Path(truth_root).resolve() / "reports" / "execution_authority_manifest_v1" / "current.json").resolve()
    manifest, error = _safe_read_json(manifest_path)
    authority_owner = str((manifest or {}).get("authority_owner") or "").strip()
    authoritative = bool(manifest is not None and authority_owner == EXPECTED_AUTHORITY_OWNER)
    reason_codes: list[str] = []
    if manifest is None:
        reason_codes.append(f"EXECUTION_AUTHORITY_MANIFEST_{error or 'UNAVAILABLE'}")
    elif authority_owner != EXPECTED_AUTHORITY_OWNER:
        reason_codes.append("EXECUTION_AUTHORITY_OWNER_INVALID")

    return {
        "state": "PASS" if authoritative else "UNKNOWN",
        "authoritative": authoritative,
        "authority_owner": authority_owner or None,
        "day_utc": str(day_utc).strip(),
        "mode": str(mode).strip().upper(),
        "path": str(manifest_path),
        "reason_codes": reason_codes,
    }
