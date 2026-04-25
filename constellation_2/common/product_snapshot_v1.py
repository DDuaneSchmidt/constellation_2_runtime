from __future__ import annotations

from pathlib import Path

from constellation_2.common.paper_session_fact_plane_v1 import SurfaceRefV1, read_validated_surface_v1


ARTIFACT_ID = "product_snapshot_v1"
SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/RUNTIME/product_snapshot.v1.schema.json"


def list_product_snapshots_v1(
    *,
    canonical_truth_root: Path | str,
    scope_id: str | None = None,
) -> list[SurfaceRefV1]:
    root = (Path(canonical_truth_root).resolve() / "reports" / ARTIFACT_ID).resolve()
    refs: list[SurfaceRefV1] = []
    if not root.exists():
        return refs
    pattern = "*/*/*/product_snapshot.v1.json" if not scope_id else f"*/{scope_id}/*/product_snapshot.v1.json"
    for path in sorted(root.glob(pattern)):
        refs.append(read_validated_surface_v1(path=path, schema_relpath=SCHEMA_RELPATH))
    refs.sort(
        key=lambda ref: (
            str(ref.payload.get("generated_at_utc") or ""),
            str(ref.payload.get("snapshot_id") or ""),
            str(ref.path),
        )
    )
    return refs


def find_latest_product_snapshot_v1(
    *,
    canonical_truth_root: Path | str,
    scope_id: str | None = None,
    day_utc: str | None = None,
    before_day_utc: str | None = None,
) -> SurfaceRefV1 | None:
    refs = list_product_snapshots_v1(canonical_truth_root=canonical_truth_root, scope_id=scope_id)
    if day_utc:
        refs = [ref for ref in refs if str(ref.payload.get("target_day") or "") == str(day_utc)]
    if before_day_utc:
        refs = [ref for ref in refs if str(ref.payload.get("target_day") or "") < str(before_day_utc)]
    if not refs:
        return None
    return refs[-1]
