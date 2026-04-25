from __future__ import annotations

from typing import Any, Dict, Optional

from constellation_2.common.value_state_kernel_v1 import list_value_states_v1

from .common import GLOBAL_TRUTH_ROOT, evidence_ref, provenance_markers, resolve_ui_day
from .dto import evidence_refs, view_envelope


def _latest_value_day() -> Optional[str]:
    root = (GLOBAL_TRUTH_ROOT / "reports" / "value_state_v1").resolve()
    if not root.exists() or not root.is_dir():
        return None
    days = [path.name for path in root.iterdir() if path.is_dir()]
    days = sorted({day for day in days if len(day) == 10 and day[4] == "-" and day[7] == "-"})
    return days[-1] if days else None


def _empty_value_view(day: Optional[str]) -> Dict[str, Any]:
    return view_envelope(
        view_name="value",
        as_of_utc=None,
        freshness_state="unknown",
        provenance_markers=provenance_markers("value", "missing"),
        source_refs=[],
        current_day=day,
        value_day=None,
        value_rows=[],
        claim_strength_counts={},
        effectiveness_counts={},
        recommendation_effectiveness_scorecard=[],
        sleeve_contribution_summary=[],
        tax_effect_summary=[],
        missed_opportunity_summary=[],
        bounded_progress_summary={},
        value_warnings=["VALUE_STATE_ARTIFACT_MISSING"],
    )


def build_value_state_view(day: Optional[str] = None) -> Dict[str, Any]:
    value_day = resolve_ui_day(day) or _latest_value_day()
    if not value_day:
        return _empty_value_view(value_day)
    refs = list_value_states_v1(canonical_truth_root=GLOBAL_TRUTH_ROOT, day_utc=value_day)
    if not refs:
        return _empty_value_view(value_day)
    rows = [dict(ref.payload) for ref in refs]
    refs_payload = evidence_refs(
        *[
            evidence_ref(ref.path, label="value_state_v1", artifact_type="value_state_v1")
            for ref in refs
        ]
    )
    claim_strength_counts: dict[str, int] = {}
    effectiveness_counts: dict[str, int] = {}
    sleeve_summary: dict[str, dict[str, Any]] = {}
    for row in rows:
        claim_strength = str(row.get("claim_strength") or "unknown")
        effectiveness = str(row.get("effectiveness_state") or "unknown")
        claim_strength_counts[claim_strength] = claim_strength_counts.get(claim_strength, 0) + 1
        effectiveness_counts[effectiveness] = effectiveness_counts.get(effectiveness, 0) + 1
        for sleeve_ref in row.get("sleeve_refs") or []:
            if not isinstance(sleeve_ref, dict):
                continue
            sleeve_id = str(sleeve_ref.get("sleeve_id") or "").strip()
            if not sleeve_id:
                continue
            summary = sleeve_summary.setdefault(
                sleeve_id,
                {
                    "sleeve_id": sleeve_id,
                    "linkage_states": set(),
                    "value_rows": 0,
                    "observed_fact_count": 0,
                    "bounded_association_count": 0,
                    "withheld_count": 0,
                },
            )
            summary["linkage_states"].add(str(sleeve_ref.get("linkage_state") or "unknown"))
            summary["value_rows"] += 1
            if claim_strength == "observed_fact":
                summary["observed_fact_count"] += 1
            elif claim_strength == "bounded_association":
                summary["bounded_association_count"] += 1
            elif claim_strength in {"insufficient_evidence", "not_yet_observable"}:
                summary["withheld_count"] += 1
    sleeve_rows = []
    for sleeve_id in sorted(sleeve_summary):
        row = dict(sleeve_summary[sleeve_id])
        row["linkage_states"] = ", ".join(sorted(row["linkage_states"]))
        sleeve_rows.append(row)
    return view_envelope(
        view_name="value",
        as_of_utc=max(str(row.get("generated_at_utc") or "") for row in rows),
        freshness_state="fresh",
        provenance_markers=provenance_markers("value", "governed_certified_value"),
        source_refs=refs_payload,
        current_day=value_day,
        value_day=value_day,
        value_rows=rows,
        claim_strength_counts=claim_strength_counts,
        effectiveness_counts=effectiveness_counts,
        recommendation_effectiveness_scorecard=rows,
        sleeve_contribution_summary=sleeve_rows,
        tax_effect_summary=[row for row in rows if row.get("governing_tax_refs")],
        missed_opportunity_summary=[row for row in rows if str(row.get("effectiveness_state") or "") == "missed"],
        bounded_progress_summary={
            "observed_fact_count": claim_strength_counts.get("observed_fact", 0),
            "bounded_association_count": claim_strength_counts.get("bounded_association", 0),
            "withheld_count": claim_strength_counts.get("insufficient_evidence", 0)
            + claim_strength_counts.get("not_yet_observable", 0),
        },
        value_warnings=[],
    )
