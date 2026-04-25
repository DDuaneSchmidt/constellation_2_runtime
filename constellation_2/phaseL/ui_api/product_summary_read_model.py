from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

from constellation_2.common.product_snapshot_v1 import find_latest_product_snapshot_v1
from constellation_2.common.product_summary_kernel_v1 import find_latest_product_summary_v1, list_product_summaries_v1

from .common import GLOBAL_TRUTH_ROOT, evidence_ref, provenance_markers, resolve_ui_day
from .dto import evidence_refs, view_envelope


def _latest_summary_day() -> Optional[str]:
    root = (GLOBAL_TRUTH_ROOT / "reports" / "product_summary_v1").resolve()
    if not root.exists() or not root.is_dir():
        return None
    days = [path.name for path in root.iterdir() if path.is_dir()]
    days = sorted({day for day in days if len(day) == 10 and day[4] == "-" and day[7] == "-"})
    return days[-1] if days else None


def build_product_summary_view(day: Optional[str] = None) -> Dict[str, Any]:
    summary_day = resolve_ui_day(day) or _latest_summary_day()
    if not summary_day:
        return view_envelope(
            view_name="product_summary",
            as_of_utc=None,
            freshness_state="unknown",
        provenance_markers=provenance_markers("product_summary", "missing"),
        source_refs=[],
        current_day=summary_day,
        summary_day=None,
            top_actionable_items=[],
            top_blocked_items=[],
            top_review_deltas=[],
            critical_degraded_states=[],
            readiness_summary={},
            rendered_ai_summaries=[],
            summary_warnings=["PRODUCT_SUMMARY_ARTIFACT_MISSING"],
        )
    rows = list_product_summaries_v1(canonical_truth_root=GLOBAL_TRUTH_ROOT, day_utc=summary_day)
    latest_ref = rows[-1] if rows else None
    if latest_ref is None:
        return view_envelope(
            view_name="product_summary",
            as_of_utc=None,
            freshness_state="unknown",
            provenance_markers=provenance_markers("product_summary", "missing"),
            source_refs=[],
            current_day=summary_day,
            summary_day=None,
            top_actionable_items=[],
            top_blocked_items=[],
            top_review_deltas=[],
            critical_degraded_states=[],
            readiness_summary={},
            rendered_ai_summaries=[],
            summary_warnings=["PRODUCT_SUMMARY_ARTIFACT_MISSING"],
        )
    payload = latest_ref.payload
    snapshot_ref = find_latest_product_snapshot_v1(
        canonical_truth_root=GLOBAL_TRUTH_ROOT,
        scope_id=str(payload.get("scope_id") or ""),
        day_utc=summary_day,
    )
    summary_ref = evidence_ref(latest_ref.path, label="product_summary_v1", artifact_type="product_summary_v1")
    refs = evidence_refs(
        summary_ref,
        *[
            evidence_ref(Path(row.get("artifact_path") or ""), label=row.get("artifact_id"), artifact_type=row.get("artifact_id"))
            for key in (
                "governing_opportunity_refs",
                "governing_advisory_refs",
                "governing_tax_refs",
                "governing_readiness_refs",
            )
            for row in (payload.get(key) or [])
            if isinstance(row, dict) and str(row.get("artifact_path") or "").strip()
        ],
    )
    return view_envelope(
        view_name="product_summary",
        as_of_utc=payload.get("generated_at_utc"),
        freshness_state="fresh",
        provenance_markers=provenance_markers("product_summary", "governed_product_summary"),
        source_refs=refs,
        current_day=summary_day,
        summary_day=summary_day,
        summary_id=payload.get("summary_id"),
        kernel_version=payload.get("kernel_version"),
        environment=payload.get("environment"),
        readiness_summary=payload.get("readiness_summary") or {},
        top_actionable_items=payload.get("top_actionable_items") or [],
        top_blocked_items=payload.get("top_blocked_items") or [],
        top_review_deltas=payload.get("top_review_deltas") or [],
        critical_degraded_states=payload.get("critical_degraded_states") or [],
        selection_reason_ids=payload.get("selection_reason_ids") or [],
        rendered_ai_summaries=((snapshot_ref.payload if snapshot_ref is not None else {}).get("rendered_ai_summaries") or []),
        snapshot_ref=(evidence_ref(snapshot_ref.path, label="product_snapshot_v1", artifact_type="product_snapshot_v1") if snapshot_ref is not None else None),
        summary_warnings=[],
        last_refresh_utc=payload.get("generated_at_utc"),
    )
