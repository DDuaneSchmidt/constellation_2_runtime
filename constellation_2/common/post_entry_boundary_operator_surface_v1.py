from __future__ import annotations

from typing import Any, Mapping, Sequence


AUTHORIZED = "AUTHORIZED"
BLOCKED = "BLOCKED"
REVIEW_REQUIRED = "REVIEW_REQUIRED"
STALE_INVALIDATED = "STALE_INVALIDATED"


def derive_post_entry_operator_surface_v1(boundaries: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    authorized: list[dict[str, str]] = []
    blocked: list[dict[str, str]] = []
    review_required: list[dict[str, str]] = []
    stale_invalidated: list[dict[str, str]] = []
    blocker_summary: dict[str, int] = {}

    for item in boundaries:
        request_ref = item.get("request_ref") if isinstance(item.get("request_ref"), dict) else {}
        entry = {
            "request_id": str(request_ref.get("request_id") or ""),
            "boundary_verdict_id": str(item.get("boundary_verdict_id") or ""),
        }
        status = str(item.get("transmission_authorization_status") or "").strip().upper()
        first_blocker = str(item.get("first_blocker") or "").strip()
        if first_blocker:
            blocker_summary[first_blocker] = blocker_summary.get(first_blocker, 0) + 1
        if status == AUTHORIZED:
            authorized.append(entry)
        elif status == REVIEW_REQUIRED:
            review_required.append(entry)
        elif status == STALE_INVALIDATED:
            stale_invalidated.append(entry)
        else:
            blocked.append(entry)

    return {
        "authorized_transmit_index": authorized,
        "blocked_transmit_index": blocked,
        "review_required_transmit_index": review_required,
        "stale_invalidated_request_index": stale_invalidated,
        "boundary_summary_dossier": {
            "total_evaluations": len(boundaries),
            "authorized_count": len(authorized),
            "blocked_count": len(blocked),
            "review_required_count": len(review_required),
            "stale_invalidated_count": len(stale_invalidated),
            "first_blocker_counts": blocker_summary,
        },
    }
