from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from constellation_2.common.advisory_decision_state_kernel_v1 import (
    list_advisory_decision_states_v1,
)
from constellation_2.common.opportunity_review_snapshot_v1 import (
    find_latest_opportunity_review_snapshot_v1,
)
from constellation_2.common.opportunity_state_kernel_v1 import (
    list_opportunity_states_v1,
)

from .common import GLOBAL_TRUTH_ROOT, evidence_ref, provenance_markers, resolve_ui_day
from .dto import evidence_refs, view_envelope


def _latest_opportunity_day() -> Optional[str]:
    root = (GLOBAL_TRUTH_ROOT / "reports" / "opportunity_state_v1").resolve()
    if not root.exists() or not root.is_dir():
        return None
    days = [path.name for path in root.iterdir() if path.is_dir()]
    days = sorted({day for day in days if len(day) == 10 and day[4] == "-" and day[7] == "-"})
    return days[-1] if days else None


def build_opportunity_state_view(day: Optional[str] = None) -> Dict[str, Any]:
    opportunity_day = resolve_ui_day(day) or _latest_opportunity_day()
    if not opportunity_day:
        return view_envelope(
            view_name="opportunity_state",
            as_of_utc=None,
            freshness_state="unknown",
            provenance_markers=provenance_markers("opportunity", "missing"),
            source_refs=[],
            current_day=resolve_ui_day(day),
            opportunity_day=None,
            top_opportunities=[],
            blocked_items=[],
            changed_since_last_review=[],
            scenario_items=[],
            advisory_impacts=[],
            review_snapshot_summary={},
            opportunity_warnings=["OPPORTUNITY_STATE_ARTIFACT_MISSING"],
        )

    source_rows = list_opportunity_states_v1(canonical_truth_root=GLOBAL_TRUTH_ROOT, day_utc=opportunity_day)
    if not source_rows:
        return view_envelope(
            view_name="opportunity_state",
            as_of_utc=None,
            freshness_state="unknown",
            provenance_markers=provenance_markers("opportunity", "missing"),
            source_refs=[],
            current_day=resolve_ui_day(day),
            opportunity_day=None,
            top_opportunities=[],
            blocked_items=[],
            changed_since_last_review=[],
            scenario_items=[],
            advisory_impacts=[],
            review_snapshot_summary={},
            opportunity_warnings=["OPPORTUNITY_STATE_ARTIFACT_MISSING"],
        )
    superseded_paths = {
        str((ref.payload.get("supersedes_ref") or {}).get("artifact_path") or "").strip()
        for ref in source_rows
        if isinstance(ref.payload.get("supersedes_ref"), dict)
    }
    current_rows = [ref for ref in source_rows if str(ref.path.resolve()) not in superseded_paths]
    current_rows.sort(
        key=lambda ref: (
            {"review_now": 0, "review_soon": 1, "monitor_only": 2, "historical_only": 3}.get(
                str(ref.payload.get("review_priority") or ""), 9
            ),
            str(ref.payload.get("opportunity_type") or ""),
            str(ref.payload.get("opportunity_id") or ""),
        )
    )
    snapshot_ref = find_latest_opportunity_review_snapshot_v1(
        canonical_truth_root=GLOBAL_TRUTH_ROOT,
        scope_id=(current_rows[0].payload.get("scope_id") if current_rows else None),
        day_utc=opportunity_day,
    )
    top_opportunities: List[Dict[str, Any]] = []
    blocked_items: List[Dict[str, Any]] = []
    changed_items: List[Dict[str, Any]] = []
    scenario_items: List[Dict[str, Any]] = []
    source_refs = []
    for ref in current_rows:
        payload = ref.payload
        row = {
            "opportunity_id": payload.get("opportunity_id"),
            "opportunity_type": payload.get("opportunity_type"),
            "opportunity_state": payload.get("opportunity_state"),
            "actionability_state": payload.get("actionability_state"),
            "review_priority": payload.get("review_priority"),
            "freshness_state": payload.get("freshness_state"),
            "visibility_state": payload.get("visibility_state"),
            "delta_state": payload.get("delta_state"),
            "scenario_significance_state": payload.get("scenario_significance_state"),
            "blocker_states": payload.get("blocker_states") or [],
            "summary_message": ((payload.get("primary_explanation") or {}).get("short_message")),
            "authority_label": payload.get("authority_label"),
        }
        top_opportunities.append(row)
        source_refs.append(evidence_ref(ref.path, label=payload.get("opportunity_type"), artifact_type="opportunity_state_v1"))
        if row["opportunity_state"] == "blocked" and row["review_priority"] == "review_now":
            blocked_items.append(dict(row))
        if row["delta_state"] in {"new", "changed", "blocked_but_still_important"}:
            changed_items.append(dict(row))
        if row["scenario_significance_state"] == "review_now":
            scenario_items.append(dict(row))

    advisory_impacts: List[Dict[str, Any]] = []
    for ref in list_advisory_decision_states_v1(canonical_truth_root=GLOBAL_TRUTH_ROOT, day_utc=opportunity_day):
        opportunity_refs = [row for row in (ref.payload.get("governing_opportunity_refs") or []) if isinstance(row, dict)]
        if not any(str(row.get("artifact_path") or "").strip() in {str(item.path.resolve()) for item in current_rows} for row in opportunity_refs):
            continue
        advisory_impacts.append(
            {
                "advisory_item_id": ref.payload.get("advisory_item_id"),
                "decision_state": ref.payload.get("decision_state"),
                "actionability_state": ref.payload.get("actionability_state"),
                "opportunity_binding_state": ref.payload.get("opportunity_binding_state"),
                "summary_message": ((ref.payload.get("primary_explanation") or {}).get("short_message")),
            }
        )

    as_of_utc = current_rows[-1].payload.get("generated_at_utc") if current_rows else None
    return view_envelope(
        view_name="opportunity_state",
        as_of_utc=as_of_utc,
        freshness_state=(current_rows[-1].payload.get("freshness_state") if current_rows else "unknown"),
        provenance_markers=provenance_markers("opportunity", "governed_certified_opportunity"),
        source_refs=evidence_refs(*source_refs),
        current_day=resolve_ui_day(day),
        opportunity_day=opportunity_day,
        top_opportunities=top_opportunities,
        blocked_items=blocked_items,
        changed_since_last_review=changed_items,
        scenario_items=scenario_items,
        advisory_impacts=advisory_impacts,
        review_snapshot_summary=(snapshot_ref.payload.get("summary") if snapshot_ref is not None else {}),
        opportunity_warnings=[],
    )
