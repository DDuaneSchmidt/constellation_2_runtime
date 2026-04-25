from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

from constellation_2.common.refinement_state_kernel_v1 import list_refinement_states_v1

from .common import GLOBAL_TRUTH_ROOT, evidence_ref, provenance_markers, resolve_ui_day
from .dto import evidence_refs, view_envelope


def _latest_refinement_day() -> Optional[str]:
    root = (GLOBAL_TRUTH_ROOT / "reports" / "refinement_state_v1").resolve()
    if not root.exists() or not root.is_dir():
        return None
    days = [path.name for path in root.iterdir() if path.is_dir()]
    days = sorted({day for day in days if len(day) == 10 and day[4] == "-" and day[7] == "-"})
    return days[-1] if days else None


def _empty_refinement_view(day: Optional[str]) -> Dict[str, Any]:
    return view_envelope(
        view_name="refinement",
        as_of_utc=None,
        freshness_state="unknown",
        provenance_markers=provenance_markers("refinement", "missing"),
        source_refs=[],
        current_day=day,
        refinement_day=None,
        top_level_items=[],
        compressed_items=[],
        secondary_items=[],
        drilldown_only_items=[],
        trust_preserved_items=[],
        withheld_items=[],
        proof_rows=[],
        readiness_summary={},
        refinement_warnings=["REFINEMENT_STATE_ARTIFACT_MISSING"],
    )


def build_refinement_state_view(day: Optional[str] = None) -> Dict[str, Any]:
    refinement_day = resolve_ui_day(day) or _latest_refinement_day()
    if not refinement_day:
        return _empty_refinement_view(refinement_day)
    refs = list_refinement_states_v1(canonical_truth_root=GLOBAL_TRUTH_ROOT, day_utc=refinement_day)
    if not refs:
        return _empty_refinement_view(refinement_day)
    rows = [dict(ref.payload) for ref in refs]
    source_refs = evidence_refs(*[evidence_ref(ref.path, label="refinement_state_v1", artifact_type="refinement_state_v1") for ref in refs])
    top_level = [
        row
        for row in rows
        if str(row.get("visibility_effect") or "") in {"top_level", "top_level_compressed", "unchanged_due_to_withheld"}
    ]
    compressed = [row for row in rows if str(row.get("refinement_action") or "") == "compress_summary"]
    secondary = [row for row in rows if str(row.get("visibility_effect") or "") == "secondary"]
    drilldown = [row for row in rows if str(row.get("visibility_effect") or "") == "drilldown_only"]
    withheld = [row for row in rows if str(row.get("refinement_action") or "") == "refinement_withheld"]
    trust_preserved = [row for row in rows if list(row.get("protected_distinctions") or [])]
    first = rows[0]
    return view_envelope(
        view_name="refinement",
        as_of_utc=max(str(row.get("generated_at_utc") or "") for row in rows),
        freshness_state="fresh",
        provenance_markers=provenance_markers("refinement", "governed_certified_refinement"),
        source_refs=source_refs,
        current_day=refinement_day,
        refinement_day=refinement_day,
        kernel_version=first.get("kernel_version"),
        environment=first.get("environment"),
        readiness_summary=first.get("readiness_summary") or {},
        top_level_items=top_level,
        compressed_items=compressed,
        secondary_items=secondary,
        drilldown_only_items=drilldown,
        trust_preserved_items=trust_preserved,
        withheld_items=withheld,
        proof_rows=rows,
        refinement_warnings=[],
        last_refresh_utc=max(str(row.get("generated_at_utc") or "") for row in rows),
    )
