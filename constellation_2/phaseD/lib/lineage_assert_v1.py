from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional


class LineageViolation(RuntimeError):
    pass


@dataclass(frozen=True)
class RequiredLineage:
    engine_id: str
    source_intent_id: str
    intent_sha256: str


def _req_str(o: Dict[str, Any], k: str) -> str:
    v = o.get(k, None)
    if not isinstance(v, str) or v.strip() == "":
        raise LineageViolation(f"LINEAGE_MISSING_OR_EMPTY: {k}")
    return v.strip()


def assert_required_lineage_fields(payload: Dict[str, Any]) -> RequiredLineage:
    """
    Enforces the minimal lineage required to attribute any downstream effect to an engine and intent.
    Fail-closed: raises LineageViolation if missing.
    """
    engine_id = _req_str(payload, "engine_id")
    source_intent_id = _req_str(payload, "source_intent_id")
    intent_sha256 = _req_str(payload, "intent_sha256")
    return RequiredLineage(engine_id=engine_id, source_intent_id=source_intent_id, intent_sha256=intent_sha256)


def assert_no_synth_status_in_paper(mode: str, status: Optional[str]) -> None:
    """
    PAPER readiness contract: no synthetic execution status is allowed in PAPER mode.
    """
    if str(mode).upper().strip() == "PAPER":
        s = (status or "").strip().upper()
        if s.startswith("SYNTH"):
            raise LineageViolation(f"PAPER_SYNTH_STATUS_FORBIDDEN: status={status!r}")
