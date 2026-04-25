from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from constellation_2.common.advisory_decision_state_kernel_v1 import (
    list_advisory_decision_states_v1,
)
from constellation_2.common.tax_state_kernel_v1 import list_tax_states_v1

from .common import GLOBAL_TRUTH_ROOT, evidence_ref, provenance_markers, resolve_ui_day
from .dto import evidence_refs, view_envelope


def _latest_tax_day() -> Optional[str]:
    root = (GLOBAL_TRUTH_ROOT / "reports" / "tax_state_v1").resolve()
    if not root.exists() or not root.is_dir():
        return None
    days = [path.name for path in root.iterdir() if path.is_dir()]
    days = sorted({day for day in days if len(day) == 10 and day[4] == "-" and day[7] == "-"})
    return days[-1] if days else None


def build_tax_state_view(day: Optional[str] = None) -> Dict[str, Any]:
    tax_day = resolve_ui_day(day) or _latest_tax_day()
    source_rows = list_tax_states_v1(canonical_truth_root=GLOBAL_TRUTH_ROOT, day_utc=tax_day) if tax_day else []
    latest_ref = source_rows[-1] if source_rows else None
    if latest_ref is None:
        return view_envelope(
            view_name="tax_state",
            as_of_utc=None,
            freshness_state="unknown",
            provenance_markers=provenance_markers("tax", "missing"),
            source_refs=[],
            current_day=resolve_ui_day(day),
            tax_day=tax_day,
            tax_status="MISSING",
            tax_warnings=["TAX_STATE_ARTIFACT_MISSING"],
            blocker_states=[],
            opportunity_states=[],
            account_tax_profiles=[],
            lot_level_entries={"status": "unavailable", "total_lots": 0, "items": [], "source_refs": []},
            harvesting_candidates={"status": "unavailable", "candidate_count": 0, "items": [], "source_refs": []},
            advisory_impacts=[],
        )

    payload = latest_ref.payload
    tax_ref = evidence_ref(latest_ref.path, label="tax_state_v1", artifact_type="tax_state_v1")
    advisory_impacts: List[Dict[str, Any]] = []
    for ref in list_advisory_decision_states_v1(canonical_truth_root=GLOBAL_TRUTH_ROOT, day_utc=tax_day):
        tax_refs = [row for row in (ref.payload.get("governing_tax_refs") or []) if isinstance(row, dict)]
        if not any(str(row.get("artifact_path") or "").strip() == str(latest_ref.path.resolve()) for row in tax_refs):
            continue
        advisory_impacts.append(
            {
                "advisory_item_id": ref.payload.get("advisory_item_id"),
                "decision_state": ref.payload.get("decision_state"),
                "actionability_state": ref.payload.get("actionability_state"),
                "freshness_state": ref.payload.get("freshness_state"),
                "tax_binding_state": ref.payload.get("tax_binding_state"),
                "summary_message": ((ref.payload.get("primary_explanation") or {}).get("short_message")),
            }
        )
    source_refs = evidence_refs(
        tax_ref,
        *[
            evidence_ref(
                Path(row.get("artifact_path") or ""),
                label=row.get("artifact_id"),
                artifact_type=row.get("artifact_id"),
            )
            for row in (payload.get("evidence_refs") or [])
            if isinstance(row, dict) and str(row.get("artifact_path") or "").strip()
        ],
    )
    tax_status = (
        "OK"
        if payload.get("completeness_state") == "complete" and payload.get("freshness_state") == "fresh"
        else "DEGRADED"
    )
    return view_envelope(
        view_name="tax_state",
        as_of_utc=payload.get("generated_at_utc"),
        freshness_state=payload.get("freshness_state") or "unknown",
        provenance_markers=provenance_markers("tax", "governed_deterministic_tax_state"),
        source_refs=source_refs,
        current_day=resolve_ui_day(day),
        tax_day=tax_day,
        tax_state_id=payload.get("tax_state_id"),
        tax_status=tax_status,
        completeness_state=payload.get("completeness_state"),
        visibility_state=payload.get("visibility_state"),
        blocker_states=payload.get("blocker_states") or [],
        opportunity_states=payload.get("opportunity_states") or [],
        lot_basis_state=payload.get("lot_basis_state"),
        holding_period_state=payload.get("holding_period_state"),
        wash_sale_state=payload.get("wash_sale_state"),
        primary_rule_id=payload.get("primary_rule_id"),
        primary_explanation=payload.get("primary_explanation"),
        advisory_binding_state=payload.get("advisory_binding_state"),
        account_tax_profiles=payload.get("account_tax_profiles") or [],
        realized_unrealized_tax_posture=payload.get("realized_unrealized_tax_posture") or {},
        lot_level_entries=payload.get("lot_level_entries") or {"status": "unavailable", "total_lots": 0, "items": [], "source_refs": []},
        harvesting_candidates=payload.get("harvesting_candidates") or {"status": "unavailable", "candidate_count": 0, "items": [], "source_refs": []},
        historical_visibility=payload.get("historical_visibility") or {},
        supersedes_ref=payload.get("supersedes_ref"),
        superseded_by_ref=payload.get("superseded_by_ref"),
        advisory_impacts=advisory_impacts,
        tax_warnings=[payload.get("primary_rule_id"), *(payload.get("blocker_states") or [])],
    )
