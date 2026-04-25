from __future__ import annotations

from pathlib import Path
from typing import Iterable
import json


REPO_ROOT = Path(__file__).resolve().parents[3]
REGISTRY_PATH = REPO_ROOT / "governance" / "02_REGISTRIES" / "C2_TAX_REASON_CODE_REGISTRY_V1.json"


def _load_registry_codes_v1() -> tuple[str, ...]:
    payload = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
    codes = payload.get("codes")
    if not isinstance(codes, list) or not codes:
        raise ValueError("TAX_REASON_CODE_REGISTRY_EMPTY")
    normalized: list[str] = []
    for item in codes:
        if not isinstance(item, dict):
            raise ValueError("TAX_REASON_CODE_REGISTRY_ENTRY_INVALID")
        code = str(item.get("code") or "").strip()
        if not code:
            raise ValueError("TAX_REASON_CODE_REGISTRY_CODE_MISSING")
        normalized.append(code)
    return tuple(sorted(set(normalized)))


TAX_REASON_CODES_V1 = _load_registry_codes_v1()


def require_reason_codes_v1(reason_codes: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    normalized: list[str] = []
    allowed = set(TAX_REASON_CODES_V1)
    for raw in reason_codes:
        code = str(raw or "").strip()
        if not code:
            continue
        if code not in allowed:
            raise ValueError(f"TAX_REASON_CODE_UNKNOWN:{code}")
        if code in seen:
            continue
        seen.add(code)
        normalized.append(code)
    return tuple(sorted(normalized))

