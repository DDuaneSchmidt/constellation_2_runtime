from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Any, Iterable

SESSION_CONTEXTS = (
    "OPEN",
    "MORNING",
    "MIDDAY",
    "POWER_HOUR",
    "CLOSE",
    "OVERNIGHT",
    "PREMARKET",
    "POSTMARKET",
)

SESSION_CONTEXT_SET = set(SESSION_CONTEXTS)
UNKNOWN_SESSION_CONTEXT = "UNKNOWN"


def normalize_session_context(value: Any, *, timestamp: str | None = None) -> str:
    raw = str(value or "").strip().upper()
    if raw in SESSION_CONTEXT_SET:
        return raw
    if timestamp:
        inferred = infer_session_context_from_timestamp(timestamp)
        if inferred:
            return inferred
    return UNKNOWN_SESSION_CONTEXT


def infer_session_context_from_timestamp(timestamp: str) -> str | None:
    try:
        parsed = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
    except Exception:
        return None
    minutes = parsed.hour * 60 + parsed.minute
    if 4 * 60 <= minutes < 9 * 60 + 30:
        return "PREMARKET"
    if 9 * 60 + 30 <= minutes < 10 * 60:
        return "OPEN"
    if 10 * 60 <= minutes < 12 * 60:
        return "MORNING"
    if 12 * 60 <= minutes < 15 * 60:
        return "MIDDAY"
    if 15 * 60 <= minutes < 15 * 60 + 50:
        return "POWER_HOUR"
    if 15 * 60 + 50 <= minutes < 16 * 60:
        return "CLOSE"
    if 16 * 60 <= minutes < 20 * 60:
        return "POSTMARKET"
    return "OVERNIGHT"


def session_context_from_row(row: dict[str, Any]) -> str:
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    split = row.get("split_dimensions") if isinstance(row.get("split_dimensions"), dict) else {}
    candidates = (
        row.get("session_context"),
        row.get("candidate_session_context"),
        row.get("session"),
        split.get("session_context"),
        metadata.get("session_context"),
        metadata.get("candidate_session_context"),
        metadata.get("session"),
    )
    for candidate in candidates:
        normalized = normalize_session_context(candidate)
        if normalized != UNKNOWN_SESSION_CONTEXT:
            return normalized
    return normalize_session_context(None, timestamp=str(row.get("timestamp") or ""))


def session_distribution(values: Iterable[Any]) -> dict[str, int]:
    counts = Counter(normalize_session_context(value) for value in values)
    return {session: int(counts.get(session, 0)) for session in SESSION_CONTEXTS}


def dominant_session_context(values: Iterable[Any]) -> str:
    counts = Counter(normalize_session_context(value) for value in values)
    session, count = max(((session, counts.get(session, 0)) for session in SESSION_CONTEXTS), key=lambda item: (item[1], -SESSION_CONTEXTS.index(item[0])))
    return session if count else UNKNOWN_SESSION_CONTEXT
