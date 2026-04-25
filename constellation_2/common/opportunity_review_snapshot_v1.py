from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Sequence

from constellation_2.common.paper_session_fact_plane_v1 import SurfaceRefV1, read_validated_surface_v1
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1


ARTIFACT_ID = "opportunity_review_snapshot_v1"
SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/RUNTIME/opportunity_review_snapshot.v1.schema.json"


def list_opportunity_review_snapshots_v1(
    *,
    canonical_truth_root: Path | str,
    scope_id: str | None = None,
) -> list[SurfaceRefV1]:
    root = (Path(canonical_truth_root).resolve() / "reports" / ARTIFACT_ID).resolve()
    refs: list[SurfaceRefV1] = []
    if not root.exists():
        return refs
    pattern = "*/*/*/opportunity_review_snapshot.v1.json" if not scope_id else f"*/{scope_id}/*/opportunity_review_snapshot.v1.json"
    for path in sorted(root.glob(pattern)):
        refs.append(read_validated_surface_v1(path=path, schema_relpath=SCHEMA_RELPATH))
    refs.sort(
        key=lambda ref: (
            str(ref.payload.get("generated_at_utc") or ""),
            str(ref.payload.get("review_snapshot_id") or ""),
            str(ref.path),
        )
    )
    return refs


def find_latest_opportunity_review_snapshot_v1(
    *,
    canonical_truth_root: Path | str,
    scope_id: str | None = None,
    day_utc: str | None = None,
    before_day_utc: str | None = None,
) -> SurfaceRefV1 | None:
    refs = list_opportunity_review_snapshots_v1(canonical_truth_root=canonical_truth_root, scope_id=scope_id)
    if day_utc:
        refs = [ref for ref in refs if str(ref.payload.get("target_day") or "") == str(day_utc)]
    if before_day_utc:
        refs = [ref for ref in refs if str(ref.payload.get("target_day") or "") < str(before_day_utc)]
    if not refs:
        return None
    return refs[-1]


def opportunity_summary_fingerprint_v1(row: Mapping[str, Any]) -> str:
    return canonical_hash_for_c2_artifact_v1(
        {
            "opportunity_id": str(row.get("opportunity_id") or ""),
            "opportunity_type": str(row.get("opportunity_type") or ""),
            "opportunity_state": str(row.get("opportunity_state") or ""),
            "review_priority": str(row.get("review_priority") or ""),
            "blocker_states": list(row.get("blocker_states") or []),
            "scenario_significance_state": str(row.get("scenario_significance_state") or ""),
        }
    )


def classify_delta_state_v1(
    *,
    prior_row: Mapping[str, Any] | None,
    current_row: Mapping[str, Any],
) -> str:
    if prior_row is None:
        return "new"
    if (
        str(current_row.get("opportunity_state") or "") == "blocked"
        and str(current_row.get("review_priority") or "") == "review_now"
    ):
        return "blocked_but_still_important"
    prior_fp = str(prior_row.get("summary_fingerprint") or "").strip()
    current_fp = opportunity_summary_fingerprint_v1(current_row)
    if prior_fp != current_fp:
        return "changed"
    if str(current_row.get("review_priority") or "") == "monitor_only":
        return "monitor_only"
    return "unchanged"


def resolved_rows_v1(
    *,
    prior_rows: Sequence[Mapping[str, Any]],
    current_ids: set[str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in prior_rows:
        opportunity_id = str(row.get("opportunity_id") or "").strip()
        if not opportunity_id or opportunity_id in current_ids:
            continue
        rows.append(
            {
                "opportunity_id": opportunity_id,
                "opportunity_type": str(row.get("opportunity_type") or ""),
                "opportunity_state": "historical_only",
                "review_priority": "historical_only",
                "delta_state": "resolved",
                "summary_fingerprint": str(row.get("summary_fingerprint") or ""),
            }
        )
    rows.sort(key=lambda item: (str(item["opportunity_type"]), str(item["opportunity_id"])))
    return rows
