from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from constellation_2.common.capability_state_v1 import resolve_policy_diff_path
from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_idempotent_validated_json_v1,
)


POLICY_DIFF_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/policy_diff.v1.schema.json"


def derive_policy_diff_payload(
    *,
    capability_ref: SurfaceRefV1,
    paper_policy_ref: SurfaceRefV1,
    production_policy_ref: SurfaceRefV1,
) -> Dict[str, Any]:
    capability_payload = capability_ref.payload
    paper_payload = paper_policy_ref.payload
    production_payload = production_policy_ref.payload

    def _item_key(row: Dict[str, Any]) -> str:
        return str(row.get("capability_id") or row.get("item_id") or "").strip()

    paper_ids = {_item_key(row) for row in (paper_payload.get("blocking_items") or []) if isinstance(row, dict)}
    production_items = [row for row in (production_payload.get("blocking_items") or []) if isinstance(row, dict)]
    production_only_items = [row for row in production_items if _item_key(row) not in paper_ids]

    shared_source_artifacts: List[Dict[str, str]] = []
    for row in capability_payload.get("source_artifacts") or []:
        if isinstance(row, dict):
            shared_source_artifacts.append(dict(row))
    for extra in (
        {"artifact_family": "capability_state_v1", "artifact_path": str(capability_ref.path), "artifact_sha256": capability_ref.sha256},
        {"artifact_family": "paper_policy_verdict_v1", "artifact_path": str(paper_policy_ref.path), "artifact_sha256": paper_policy_ref.sha256},
        {"artifact_family": "production_policy_verdict_v1", "artifact_path": str(production_policy_ref.path), "artifact_sha256": production_policy_ref.sha256},
    ):
        shared_source_artifacts.append(extra)

    shared_source_artifacts = sorted(
        {
            (row["artifact_family"], row["artifact_path"], row["artifact_sha256"]): row
            for row in shared_source_artifacts
            if isinstance(row, dict)
        }.values(),
        key=lambda row: (row["artifact_family"], row["artifact_path"]),
    )

    return {
        "schema_id": "policy_diff",
        "schema_version": "v1",
        "day_utc": str(capability_payload.get("day_utc") or "").strip(),
        "paper_policy_status": str(paper_payload.get("overall_status") or "").strip().upper(),
        "production_policy_status": str(production_payload.get("overall_status") or "").strip().upper(),
        "paper_blocking_items": list(paper_payload.get("blocking_items") or []),
        "production_blocking_items": production_items,
        "production_only_open_items": production_only_items,
        "shared_source_artifacts": shared_source_artifacts,
        "release_id": str(capability_payload.get("release_id") or "").strip(),
        "git_sha": str(capability_payload.get("git_sha") or "").strip(),
        "generated_at_utc": str(capability_payload.get("generated_at_utc") or ""),
    }


def write_policy_diff_v1(*, truth_root: Path, payload: Dict[str, Any]) -> SurfaceRefV1:
    return atomic_write_idempotent_validated_json_v1(
        path=resolve_policy_diff_path(truth_root=truth_root, day_utc=str(payload.get("day_utc") or "").strip()),
        payload=payload,
        schema_relpath=POLICY_DIFF_SCHEMA_RELPATH,
        volatile_field_names=("generated_at_utc",),
    )
