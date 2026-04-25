from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from constellation_2.common.advisory_decision_state_kernel_v1 import (
    list_advisory_decision_states_v1,
)

from .common import GLOBAL_TRUTH_ROOT, evidence_ref, provenance_markers, resolve_ui_day
from .dto import evidence_refs, markers, view_envelope


def _latest_advisory_day() -> Optional[str]:
    root = (GLOBAL_TRUTH_ROOT / "reports" / "advisory_decision_state_v1").resolve()
    if not root.exists() or not root.is_dir():
        return None
    days = [path.name for path in root.iterdir() if path.is_dir()]
    days = sorted({day for day in days if len(day) == 10 and day[4] == "-" and day[7] == "-"})
    return days[-1] if days else None


def build_advisory_view(day: Optional[str] = None) -> Dict[str, Any]:
    advisory_day = resolve_ui_day(day) or _latest_advisory_day()
    decisions: List[Dict[str, Any]] = []
    source_rows = []
    if advisory_day:
        source_rows = list_advisory_decision_states_v1(
            canonical_truth_root=GLOBAL_TRUTH_ROOT,
            day_utc=advisory_day,
        )
    for ref in source_rows:
        payload = ref.payload
        decisions.append(
            {
                "decision_id": payload.get("decision_id"),
                "advisory_item_id": payload.get("advisory_item_id"),
                "advisory_surface_label": payload.get("advisory_surface_label"),
                "decision_state": payload.get("decision_state"),
                "actionability_state": payload.get("actionability_state"),
                "freshness_state": payload.get("freshness_state"),
                "visibility_state": payload.get("visibility_state"),
                "promotion_eligibility_state": payload.get("promotion_eligibility_state"),
                "invalidation_rule_id": payload.get("invalidation_rule_id"),
                "summary_message": ((payload.get("primary_explanation") or {}).get("short_message")),
                "authority_label": payload.get("authority_label"),
                "provenance_markers": markers("advisory", "governed_certified_decision"),
                "evidence_refs": evidence_refs(
                    *[
                        evidence_ref(
                            Path(row.get("artifact_path") or ""),
                            label=row.get("artifact_id"),
                            artifact_type=row.get("artifact_id"),
                        )
                        for row in (payload.get("evidence_refs") or [])
                        if isinstance(row, dict) and str(row.get("artifact_path") or "").strip()
                    ]
                ),
            }
        )
    decisions.sort(key=lambda item: (str(item.get("advisory_item_id") or ""), str(item.get("decision_id") or "")))
    latest_decision = decisions[-1] if decisions else None
    return view_envelope(
        view_name="advisory",
        as_of_utc=(source_rows[-1].payload.get("generated_at_utc") if source_rows else None),
        freshness_state=(latest_decision or {}).get("freshness_state") or "unknown",
        provenance_markers=provenance_markers("advisory", "governed_certified_decision"),
        source_refs=evidence_refs(
            *[
                evidence_ref(ref.path, label=ref.payload.get("advisory_item_id"), artifact_type="advisory_decision_state_v1")
                for ref in source_rows
            ]
        ),
        current_day=resolve_ui_day(day),
        advisory_day=advisory_day,
        decisions=decisions,
        current_decision=latest_decision,
        advisory_warnings=[],
    )
