from __future__ import annotations

from typing import Any, Dict, Optional

from .common import (
    GLOBAL_TRUTH_ROOT,
    evidence_ref,
    provenance_markers,
    read_control_plane_collection,
    resolve_ui_day,
)
from .dto import evidence_refs, view_envelope


def _latest_policy_day() -> Optional[str]:
    root = (GLOBAL_TRUTH_ROOT / "reports" / "policy_evolution_state_v1").resolve()
    if not root.exists() or not root.is_dir():
        return None
    days = [path.name for path in root.iterdir() if path.is_dir()]
    days = sorted({day for day in days if len(day) == 10 and day[4] == "-" and day[7] == "-"})
    return days[-1] if days else None


def _empty_policy_view(day: Optional[str]) -> Dict[str, Any]:
    return view_envelope(
        view_name="policy_evolution",
        as_of_utc=None,
        freshness_state="unknown",
        provenance_markers=provenance_markers("policy_evolution", "missing"),
        source_refs=[],
        current_day=day,
        policy_day=None,
        top_level_items=[],
        compressed_items=[],
        secondary_items=[],
        drilldown_only_items=[],
        trust_preserved_items=[],
        withheld_items=[],
        expiring_items=[],
        rollback_items=[],
        active_policy_evolutions=[],
        proof_rows=[],
        readiness_summary={},
        policy_warnings=["POLICY_EVOLUTION_STATE_ARTIFACT_MISSING"],
    )


def build_policy_evolution_view(day: Optional[str] = None) -> Dict[str, Any]:
    policy_day = resolve_ui_day(day) or _latest_policy_day()
    if not policy_day:
        return _empty_policy_view(policy_day)
    collection, _ = read_control_plane_collection(
        domain="operator",
        surface="policy_evolution_state_day",
        truth_root=GLOBAL_TRUTH_ROOT,
        day=policy_day,
    )
    refs = list(collection.refs) if collection is not None else []
    if not refs:
        return _empty_policy_view(policy_day)
    rows = [dict(ref.payload) for ref in refs]
    source_refs = evidence_refs(
        *[
            evidence_ref(ref.path, label="policy_evolution_state_v1", artifact_type="policy_evolution_state_v1")
            for ref in refs
        ]
    )
    top_level = [
        row
        for row in rows
        if str((row.get("proposed_policy_change") or {}).get("effective_visibility_effect") or "")
        in {"top_level", "top_level_compressed", "unchanged_due_to_withheld"}
    ]
    compressed = [
        row
        for row in rows
        if str((row.get("proposed_policy_change") or {}).get("action") or "") == "propose_more_compression"
    ]
    secondary = [
        row
        for row in rows
        if str((row.get("proposed_policy_change") or {}).get("effective_visibility_effect") or "") == "secondary"
    ]
    drilldown = [
        row
        for row in rows
        if str((row.get("proposed_policy_change") or {}).get("effective_visibility_effect") or "") == "drilldown_only"
    ]
    withheld = [
        row
        for row in rows
        if str((row.get("proposed_policy_change") or {}).get("action") or "") == "evolution_withheld"
    ]
    expiring = [
        row
        for row in rows
        if str((row.get("proposed_policy_change") or {}).get("action") or "") == "evolution_expired"
        or str(row.get("expiry_state") or "") != "active_until_next_review"
    ]
    rollback = [
        row
        for row in rows
        if str((row.get("proposed_policy_change") or {}).get("action") or "") == "rollback_to_prior_policy"
    ]
    trust_preserved = [row for row in rows if list(row.get("preserved_visibility_flags") or [])]
    active = [
        row
        for row in rows
        if str((row.get("proposed_policy_change") or {}).get("action") or "")
        in {"preserve_current_policy", "propose_strengthen_emphasis", "propose_reduce_emphasis", "propose_more_compression"}
    ]
    first = rows[0]
    return view_envelope(
        view_name="policy_evolution",
        as_of_utc=max(str(row.get("generated_at_utc") or "") for row in rows),
        freshness_state="fresh",
        provenance_markers=provenance_markers("policy_evolution", "governed_policy_evolution"),
        source_refs=source_refs,
        current_day=policy_day,
        policy_day=policy_day,
        kernel_version=first.get("kernel_version"),
        environment=first.get("environment"),
        readiness_summary=first.get("readiness_summary") or {},
        top_level_items=top_level,
        compressed_items=compressed,
        secondary_items=secondary,
        drilldown_only_items=drilldown,
        trust_preserved_items=trust_preserved,
        withheld_items=withheld,
        expiring_items=expiring,
        rollback_items=rollback,
        active_policy_evolutions=active,
        proof_rows=rows,
        policy_warnings=[],
        last_refresh_utc=max(str(row.get("generated_at_utc") or "") for row in rows),
    )
