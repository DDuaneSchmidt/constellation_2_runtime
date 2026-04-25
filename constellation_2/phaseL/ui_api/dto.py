from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional


def evidence_refs(*refs: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [ref for ref in refs if isinstance(ref, dict)]


def markers(*values: str) -> List[str]:
    out: List[str] = []
    for value in values:
        normalized = str(value or "").strip()
        if normalized and normalized not in out:
            out.append(normalized)
    return out or ["unknown"]


def view_envelope(
    *,
    view_name: str,
    as_of_utc: Optional[str],
    freshness_state: str,
    provenance_markers: Iterable[str],
    source_refs: Iterable[Dict[str, Any]],
    **fields: Any,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "view_name": view_name,
        "as_of_utc": as_of_utc,
        "freshness_state": freshness_state,
        "provenance_markers": list(provenance_markers) or ["unknown"],
        "source_refs": list(source_refs),
    }
    payload.update(fields)
    return payload
