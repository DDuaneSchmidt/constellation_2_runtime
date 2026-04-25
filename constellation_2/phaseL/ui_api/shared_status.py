from __future__ import annotations

from typing import Any, Dict


STATUS_SEMANTICS: Dict[str, Dict[str, str]] = {
    "canonical": {
        "label": "Canonical",
        "color": "green",
        "icon": "C",
        "tooltip": "Primary governed truth surface.",
    },
    "derived": {
        "label": "Derived",
        "color": "blue",
        "icon": "D",
        "tooltip": "Computed read model over governed source artifacts.",
    },
    "reconstructed": {
        "label": "Reconstructed",
        "color": "blue",
        "icon": "R",
        "tooltip": "Rebuilt or carry-forward lineage; not original untouched evidence.",
    },
    "stale": {
        "label": "Stale",
        "color": "yellow",
        "icon": "S",
        "tooltip": "Source age exceeded the presentation freshness threshold.",
    },
    "unknown": {
        "label": "Unknown",
        "color": "yellow",
        "icon": "?",
        "tooltip": "The required truth surface is missing, unreadable, or not provable.",
    },
    "healthy": {
        "label": "Healthy",
        "color": "green",
        "icon": "H",
        "tooltip": "Source status is healthy, authorized, or passing.",
    },
    "warning": {
        "label": "Warning",
        "color": "yellow",
        "icon": "W",
        "tooltip": "Source status requires awareness but is not fully blocked.",
    },
    "degraded": {
        "label": "Degraded",
        "color": "orange",
        "icon": "!",
        "tooltip": "Source status is partial, impaired, or degraded.",
    },
    "blocked": {
        "label": "Blocked",
        "color": "red",
        "icon": "B",
        "tooltip": "Source status is blocked, failed, or denied.",
    },
    "fail_closed": {
        "label": "Fail Closed",
        "color": "red",
        "icon": "F",
        "tooltip": "Read model is intentionally refusing to infer healthy state from missing or unsafe inputs.",
    },
    "advisory": {
        "label": "Advisory",
        "color": "purple",
        "icon": "A",
        "tooltip": "Recommendation-only surface; not operational authority.",
    },
}


def semantic_meta(name: str) -> Dict[str, str]:
    return STATUS_SEMANTICS.get(name, STATUS_SEMANTICS["unknown"])


def classify_health(raw_status: Any) -> str:
    value = str(raw_status or "").strip().upper()
    if value in {"PASS", "OK", "READY", "READY_NOW", "AUTHORIZED", "ADMIT", "GRANTED", "ENABLED", "ACTIVE_SESSION_CONFIRMED", "SUCCESS", "HEALTHY", "COMPLETE", "CLOSED"}:
        return "healthy"
    if value in {"WARNING", "WARN", "PENDING", "CURRENT", "VALID"}:
        return "warning"
    if value in {"DEGRADED", "PARTIAL", "ESCALATED", "NOT_AUTHORIZED", "ROLLOVER_WITHHELD", "DEPLOY_BLOCKED_VALID", "STARTUP_BLOCKED", "DENIED"}:
        return "degraded"
    if value in {"FAIL", "BLOCKED", "ERROR", "INVALID", "REJECTED", "ABORTED"}:
        return "blocked"
    if value in {"UNKNOWN", "", "UNAVAILABLE"}:
        return "unknown"
    return "warning"
