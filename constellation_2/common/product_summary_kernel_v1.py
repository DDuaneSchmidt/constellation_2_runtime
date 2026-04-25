from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, Iterable

from constellation_2.common.bounded_product_ai_condensation_v1 import (
    ALLOWED_SUMMARY_TYPES,
    ASSIST_VERSION,
    build_bounded_product_ai_summary_v1,
)
from constellation_2.common.constitutional_runtime_v1 import (
    assert_constitutional_writer_allowed_v1,
    resolve_constitutional_artifact_path_v1,
)
from constellation_2.common.control_plane_validation_kernel_v1 import _build_scope
from constellation_2.common.opportunity_review_snapshot_v1 import find_latest_opportunity_review_snapshot_v1
from constellation_2.common.opportunity_state_kernel_v1 import list_opportunity_states_v1
from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_idempotent_validated_json_v1,
)
from constellation_2.common.product_snapshot_v1 import (
    ARTIFACT_ID as SNAPSHOT_ARTIFACT_ID,
    SCHEMA_RELPATH as SNAPSHOT_SCHEMA_RELPATH,
    find_latest_product_snapshot_v1,
)
from constellation_2.common.product_summary_precedence_v1 import (
    SELECTION_MODEL_VERSION,
    evaluate_product_summary_candidate_v1,
)
from constellation_2.common.release_baseline_gate_v1 import build_certified_operational_readiness_v1
from constellation_2.common.tax_state_kernel_v1 import find_latest_tax_state_v1
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
WRITER_ID = "constellation_2.common.product_summary_kernel_v1"
KERNEL_VERSION = WRITER_ID
ARTIFACT_ID = "product_summary_v1"
SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/RUNTIME/product_summary.v1.schema.json"


class ProductSummaryError(RuntimeError):
    pass


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _plain_ref(*, artifact_id: str, path: Path, sha256: str) -> Dict[str, str]:
    return {
        "artifact_id": str(artifact_id).strip(),
        "artifact_path": str(path.resolve()),
        "artifact_sha256": str(sha256).strip(),
    }


def _plain_ref_from_surface(surface: SurfaceRefV1, artifact_id: str) -> Dict[str, str]:
    return _plain_ref(artifact_id=artifact_id, path=surface.path, sha256=surface.sha256)


def _scope(
    *,
    day_utc: str,
    sleeve_id: str,
    environment: str,
    ib_account: str,
    operation_type: str,
    canonical_truth_root: Path | str | None,
    truth_sleeves_root: Path | str | None,
) -> Any:
    return _build_scope(
        repo_root=REPO_ROOT,
        day_utc=day_utc,
        sleeve_id=sleeve_id,
        environment=environment,
        ib_account=ib_account,
        operation_type=operation_type,
        candidate_path=None,
        canonical_truth_root=canonical_truth_root,
        truth_sleeves_root=truth_sleeves_root,
    )


def _summary_root(*, canonical_truth_root: Path, day_utc: str, scope_id: str) -> Path:
    return (canonical_truth_root / "reports" / ARTIFACT_ID / day_utc / scope_id).resolve()


def list_product_summaries_v1(
    *,
    canonical_truth_root: Path | str,
    day_utc: str,
    scope_id: str | None = None,
) -> list[SurfaceRefV1]:
    from constellation_2.common.paper_session_fact_plane_v1 import read_validated_surface_v1

    root = (Path(canonical_truth_root).resolve() / "reports" / ARTIFACT_ID / day_utc).resolve()
    refs: list[SurfaceRefV1] = []
    if not root.exists():
        return refs
    pattern = "*/*/product_summary.v1.json" if not scope_id else f"{scope_id}/*/product_summary.v1.json"
    for path in sorted(root.glob(pattern)):
        refs.append(read_validated_surface_v1(path=path, schema_relpath=SCHEMA_RELPATH))
    refs.sort(
        key=lambda ref: (
            str(ref.payload.get("generated_at_utc") or ""),
            str(ref.payload.get("summary_id") or ""),
            str(ref.path),
        )
    )
    return refs


def find_latest_product_summary_v1(
    *,
    canonical_truth_root: Path | str,
    day_utc: str,
    scope_id: str | None = None,
) -> SurfaceRefV1 | None:
    refs = list_product_summaries_v1(canonical_truth_root=canonical_truth_root, day_utc=day_utc, scope_id=scope_id)
    if not refs:
        return None
    superseded_paths = {
        str((ref.payload.get("supersedes_ref") or {}).get("artifact_path") or "").strip()
        for ref in refs
        if isinstance(ref.payload.get("supersedes_ref"), dict)
    }
    current_refs = [ref for ref in refs if str(ref.path.resolve()) not in superseded_paths]
    if current_refs:
        refs = current_refs
    return refs[-1]


def _current_refs(refs: Iterable[SurfaceRefV1]) -> list[SurfaceRefV1]:
    rows = list(refs)
    superseded_paths = {
        str((ref.payload.get("supersedes_ref") or {}).get("artifact_path") or "").strip()
        for ref in rows
        if isinstance(ref.payload.get("supersedes_ref"), dict)
    }
    current = [ref for ref in rows if str(ref.path.resolve()) not in superseded_paths]
    return current or rows


def _item_from_opportunity(ref: SurfaceRefV1) -> dict[str, Any]:
    payload = ref.payload
    return {
        "item_id": str(payload.get("opportunity_id") or ""),
        "item_kind": "opportunity",
        "item_ref": _plain_ref_from_surface(ref, "opportunity_state_v1"),
        "label": str(payload.get("opportunity_type") or "opportunity"),
        "summary_message": str(((payload.get("primary_explanation") or {}).get("short_message")) or "Opportunity available."),
        "actionability_state": str(payload.get("actionability_state") or "inspect_only"),
        "review_priority": str(payload.get("review_priority") or "monitor_only"),
        "freshness_state": str(payload.get("freshness_state") or "unknown"),
        "visibility_state": str(payload.get("visibility_state") or "current"),
        "blocker_states": list(payload.get("blocker_states") or []),
        "delta_state": str(payload.get("delta_state") or "unchanged"),
        "scenario_significance_state": str(payload.get("scenario_significance_state") or "not_evaluated"),
        "opportunity_state": str(payload.get("opportunity_state") or "monitor_only"),
        "authority_label": str(payload.get("authority_label") or ""),
        "drill_down_route": "/opportunities",
    }


def _item_from_tax(ref: SurfaceRefV1) -> dict[str, Any] | None:
    payload = ref.payload
    if (
        str(payload.get("completeness_state") or "") == "complete"
        and str(payload.get("freshness_state") or "") == "fresh"
        and str(payload.get("visibility_state") or "") == "current"
    ):
        return None
    return {
        "item_id": f"tax:{payload.get('tax_state_id')}",
        "item_kind": "degraded_state",
        "item_ref": _plain_ref_from_surface(ref, "tax_state_v1"),
        "label": "Tax state",
        "summary_message": str(((payload.get("primary_explanation") or {}).get("short_message")) or "Tax state is degraded."),
        "actionability_state": "inspect_only",
        "review_priority": "review_now",
        "freshness_state": str(payload.get("freshness_state") or "unknown"),
        "visibility_state": str(payload.get("visibility_state") or "current_downgraded"),
        "blocker_states": list(payload.get("blocker_states") or []),
        "delta_state": "changed",
        "scenario_significance_state": "not_evaluated",
        "authority_label": str(payload.get("authority_label") or ""),
        "drill_down_route": "/tax",
        "critical": True,
    }


def _item_from_readiness(readiness: dict[str, Any]) -> dict[str, Any] | None:
    if bool(readiness.get("ok")):
        return None
    blocked_state = str(readiness.get("blocked_state") or "readiness_blocked")
    refs = [dict(row) for row in readiness.get("governing_refs") or [] if isinstance(row, dict)]
    fake_ref = refs[0] if refs else {
        "artifact_id": "certified_operational_readiness_v1",
        "artifact_path": "virtual:certified_operational_readiness_v1",
        "artifact_sha256": blocked_state,
    }
    return {
        "item_id": f"readiness:{blocked_state}",
        "item_kind": "degraded_state",
        "item_ref": fake_ref,
        "label": "Release readiness",
        "summary_message": str(blocked_state),
        "actionability_state": "inspect_only",
        "review_priority": "review_now",
        "freshness_state": "fresh",
        "visibility_state": "current_downgraded",
        "blocker_states": [blocked_state],
        "delta_state": "changed",
        "scenario_significance_state": "not_evaluated",
        "authority_label": str(readiness.get("authority_label") or "governed_release_projection"),
        "drill_down_route": "/operations",
        "critical": True,
    }


def _readiness_payload(scope: Any) -> dict[str, Any]:
    readiness = build_certified_operational_readiness_v1(
        repo_root=REPO_ROOT,
        day_utc=scope.day_utc,
        sleeve_id=scope.sleeve_id,
        environment=scope.environment,
        ib_account=scope.ib_account,
        operation_type=scope.operation_type,
        canonical_truth_root=scope.canonical_truth_root,
        truth_sleeves_root=scope.truth_sleeves_root,
    )
    blocked_state = str(((readiness.get("blocked_state") or {}).get("reason")) or "")
    refs = [dict(row) for row in readiness.get("governing_refs") or [] if isinstance(row, dict)]
    return {
        "ok": bool(readiness.get("ok")),
        "blocked": bool(readiness.get("blocked")),
        "blocked_state": blocked_state or ("ready" if bool(readiness.get("ok")) else "unknown"),
        "operator_status": str(((readiness.get("baseline_gate") or {}).get("current_family_statuses") or {}).get("operator_status") or "UNKNOWN"),
        "authority_label": str(readiness.get("authority_label") or "governed_release_projection"),
        "governing_refs": refs,
    }


def _sanitize_summary_item(candidate: dict[str, Any]) -> dict[str, Any]:
    allowed = {
        "item_id",
        "item_kind",
        "item_ref",
        "label",
        "summary_message",
        "selection_reason_id",
        "actionability_state",
        "review_priority",
        "freshness_state",
        "visibility_state",
        "blocker_states",
        "delta_state",
        "scenario_significance_state",
        "authority_label",
        "drill_down_route",
    }
    return {key: value for key, value in candidate.items() if key in allowed}


def _write_projection(
    *,
    payload: dict[str, Any],
    canonical_truth_root: Path,
    day_utc: str,
    scope_id: str,
    artifact_id: str,
    schema_relpath: str,
    key_id: str,
    filename: str,
) -> SurfaceRefV1:
    assert_constitutional_writer_allowed_v1(repo_root=REPO_ROOT, artifact_id=artifact_id, writer_id=WRITER_ID)
    output_path = resolve_constitutional_artifact_path_v1(
        repo_root=REPO_ROOT,
        artifact_id=artifact_id,
        day_utc=day_utc,
        canonical_truth_root=canonical_truth_root,
        extra_variables={"scope_id": scope_id, key_id: payload[key_id]},
    )
    if output_path.name != filename:
        raise ProductSummaryError(f"UNEXPECTED_{artifact_id.upper()}_PATH_FILENAME:{output_path}")
    return atomic_write_idempotent_validated_json_v1(
        path=output_path,
        payload=payload,
        schema_relpath=schema_relpath,
        volatile_field_names=("generated_at_utc",),
    )


def materialize_product_summary_v1(
    *,
    day_utc: str,
    sleeve_id: str,
    environment: str,
    ib_account: str,
    operation_type: str = "fresh_paper_entry_v1",
    canonical_truth_root: Path | str | None = None,
    truth_sleeves_root: Path | str | None = None,
    emit_artifacts: bool = False,
    summary_types: tuple[str, ...] = ("daily_review_brief", "blocked_explanation_brief", "scenario_comparison_brief"),
    ai_available: bool = False,
) -> dict[str, Any]:
    scope = _scope(
        day_utc=day_utc,
        sleeve_id=sleeve_id,
        environment=environment,
        ib_account=ib_account,
        operation_type=operation_type,
        canonical_truth_root=canonical_truth_root,
        truth_sleeves_root=truth_sleeves_root,
    )
    opportunity_refs = _current_refs(
        [
            ref
            for ref in list_opportunity_states_v1(
                canonical_truth_root=scope.canonical_truth_root,
                day_utc=scope.day_utc,
                scope_id=scope.scope_id,
            )
            if str(ref.payload.get("visibility_state") or "") != "suppressed"
        ]
    )
    if not opportunity_refs:
        raise ProductSummaryError("PRODUCT_SUMMARY_REQUIRES_OPPORTUNITY_STATE")
    tax_ref = find_latest_tax_state_v1(
        canonical_truth_root=scope.canonical_truth_root,
        day_utc=scope.day_utc,
        scope_id=scope.scope_id,
    )
    if tax_ref is None:
        raise ProductSummaryError("PRODUCT_SUMMARY_REQUIRES_TAX_STATE")
    review_snapshot_ref = find_latest_opportunity_review_snapshot_v1(
        canonical_truth_root=scope.canonical_truth_root,
        scope_id=scope.scope_id,
        day_utc=scope.day_utc,
    )
    if review_snapshot_ref is None:
        raise ProductSummaryError("PRODUCT_SUMMARY_REQUIRES_REVIEW_SNAPSHOT")
    readiness_summary = _readiness_payload(scope)

    candidates: list[dict[str, Any]] = []
    candidates.extend(_item_from_opportunity(ref) for ref in opportunity_refs)
    tax_item = _item_from_tax(tax_ref)
    if tax_item is not None:
        candidates.append(tax_item)
    readiness_item = _item_from_readiness(readiness_summary)
    if readiness_item is not None:
        candidates.append(readiness_item)

    selected_actionable: list[dict[str, Any]] = []
    selected_blocked: list[dict[str, Any]] = []
    selected_deltas: list[dict[str, Any]] = []
    selected_degraded: list[dict[str, Any]] = []
    selection_reason_ids: list[str] = []
    for candidate in sorted(
        candidates,
        key=lambda row: (
            evaluate_product_summary_candidate_v1(row)["selection_rank"],
            str(row.get("label") or ""),
            str(row.get("item_id") or ""),
        ),
    ):
        decision = evaluate_product_summary_candidate_v1(candidate)
        candidate["selection_reason_id"] = decision["selection_reason_id"]
        selection_reason_ids.append(str(decision["selection_reason_id"]))
        if decision["suppressed"]:
            continue
        bucket = str(decision["selection_bucket"])
        if bucket == "top_actionable" and len(selected_actionable) < 3:
            selected_actionable.append(_sanitize_summary_item(candidate))
        elif bucket == "top_blocked" and len(selected_blocked) < 3:
            selected_blocked.append(_sanitize_summary_item(candidate))
        elif bucket == "top_review_deltas" and len(selected_deltas) < 3:
            selected_deltas.append(_sanitize_summary_item(candidate))
        elif bucket == "critical_degraded" and len(selected_degraded) < 3:
            selected_degraded.append(_sanitize_summary_item(candidate))

    governing_opportunity_refs = [_plain_ref_from_surface(ref, "opportunity_state_v1") for ref in opportunity_refs]
    governing_advisory_refs: list[dict[str, str]] = []
    governing_tax_refs = [_plain_ref_from_surface(tax_ref, "tax_state_v1")]
    governing_readiness_refs = [dict(row) for row in readiness_summary["governing_refs"]]
    evidence_refs = [
        *governing_opportunity_refs,
        *governing_advisory_refs,
        *governing_tax_refs,
        *governing_readiness_refs,
    ]
    summary_id = canonical_hash_for_c2_artifact_v1(
        {
            "artifact_id": ARTIFACT_ID,
            "scope_id": scope.scope_id,
            "day_utc": scope.day_utc,
            "actionable": [item["item_id"] for item in selected_actionable],
            "blocked": [item["item_id"] for item in selected_blocked],
            "deltas": [item["item_id"] for item in selected_deltas],
            "degraded": [item["item_id"] for item in selected_degraded],
            "readiness": readiness_summary,
        }
    )
    prior_summary_ref = find_latest_product_summary_v1(
        canonical_truth_root=scope.canonical_truth_root,
        day_utc=scope.day_utc,
        scope_id=scope.scope_id,
    )
    supersedes_ref = None
    if prior_summary_ref is not None and str(prior_summary_ref.payload.get("summary_id") or "") != summary_id:
        supersedes_ref = _plain_ref_from_surface(prior_summary_ref, ARTIFACT_ID)
    summary_payload = {
        "schema_id": "product_summary",
        "schema_version": "v1",
        "artifact_id": ARTIFACT_ID,
        "surface_kind": "projection",
        "summary_id": summary_id,
        "kernel_version": KERNEL_VERSION,
        "selection_model_version": SELECTION_MODEL_VERSION,
        "generated_at_utc": _utc_now(),
        "authority_label": "governed_product_summary",
        "target_day": scope.day_utc,
        "scope_id": scope.scope_id,
        "operation_type": scope.operation_type,
        "sleeve_id": scope.sleeve_id,
        "environment": scope.environment,
        "ib_account": scope.ib_account,
        "top_actionable_items": selected_actionable,
        "top_blocked_items": selected_blocked,
        "top_review_deltas": selected_deltas,
        "critical_degraded_states": selected_degraded,
        "readiness_summary": readiness_summary,
        "selection_reason_ids": sorted(set(selection_reason_ids)),
        "governing_opportunity_refs": governing_opportunity_refs,
        "governing_advisory_refs": governing_advisory_refs,
        "governing_tax_refs": governing_tax_refs,
        "governing_readiness_refs": governing_readiness_refs,
        "ai_assist_eligibility": {
            "eligible": True,
            "allowed_summary_types": list(ALLOWED_SUMMARY_TYPES),
        },
        "evidence_refs": evidence_refs,
        "semantic_events": [
            {
                "event_type": "product_summary_computed",
                "authority_label": "governed_product_summary",
                "summary_type": "product_summary",
                "selected_item_refs": [item["item_ref"] for item in selected_actionable + selected_blocked + selected_deltas],
                "critical_degraded_refs": [item["item_ref"] for item in selected_degraded],
                "governing_refs": evidence_refs,
                "kernel_version": KERNEL_VERSION,
            }
        ],
    }
    if supersedes_ref is not None:
        summary_payload["supersedes_ref"] = supersedes_ref
        summary_payload["semantic_events"].append(
            {
                "event_type": "product_summary_selection_changed",
                "authority_label": "governed_product_summary",
                "summary_type": "product_summary",
                "selected_item_refs": [item["item_ref"] for item in selected_actionable + selected_blocked + selected_deltas],
                "critical_degraded_refs": [item["item_ref"] for item in selected_degraded],
                "governing_refs": evidence_refs,
                "kernel_version": KERNEL_VERSION,
            }
        )
    if selected_degraded:
        summary_payload["semantic_events"].append(
            {
                "event_type": "critical_degraded_state_elevated",
                "authority_label": "governed_product_summary",
                "summary_type": "product_summary",
                "selected_item_refs": [item["item_ref"] for item in selected_actionable + selected_blocked + selected_deltas],
                "critical_degraded_refs": [item["item_ref"] for item in selected_degraded],
                "governing_refs": evidence_refs,
                "kernel_version": KERNEL_VERSION,
            }
        )
    if selected_blocked:
        summary_payload["semantic_events"].append(
            {
                "event_type": "top_blocked_item_elevated",
                "authority_label": "governed_product_summary",
                "summary_type": "product_summary",
                "selected_item_refs": [item["item_ref"] for item in selected_actionable + selected_blocked + selected_deltas],
                "critical_degraded_refs": [item["item_ref"] for item in selected_degraded],
                "governing_refs": evidence_refs,
                "kernel_version": KERNEL_VERSION,
            }
        )

    summary_plain_ref = {
        "artifact_id": ARTIFACT_ID,
        "artifact_path": str(
            resolve_constitutional_artifact_path_v1(
                repo_root=REPO_ROOT,
                artifact_id=ARTIFACT_ID,
                day_utc=scope.day_utc,
                canonical_truth_root=scope.canonical_truth_root,
                extra_variables={"scope_id": scope.scope_id, "summary_id": summary_id},
            ).resolve()
        ),
        "artifact_sha256": hashlib.sha256(canonical_json_bytes_v1(summary_payload)).hexdigest(),
    }
    ai_summaries = [
        build_bounded_product_ai_summary_v1(
            summary=summary_payload,
            source_summary_ref=summary_plain_ref,
            summary_type=summary_type,
            ai_available=ai_available,
        )
        for summary_type in summary_types
    ]
    prior_snapshot_ref = find_latest_product_snapshot_v1(
        canonical_truth_root=scope.canonical_truth_root,
        scope_id=scope.scope_id,
        before_day_utc=scope.day_utc,
    )
    snapshot_id = hashlib.sha256(
        canonical_json_bytes_v1(
            {
                "summary_id": summary_id,
                "day_utc": scope.day_utc,
                "ai_summaries": [row["ai_summary_id"] for row in ai_summaries],
            }
        )
    ).hexdigest()
    snapshot_payload = {
        "schema_id": "product_snapshot",
        "schema_version": "v1",
        "artifact_id": SNAPSHOT_ARTIFACT_ID,
        "surface_kind": "projection",
        "snapshot_id": snapshot_id,
        "kernel_version": KERNEL_VERSION,
        "generated_at_utc": _utc_now(),
        "authority_label": "governed_product_snapshot",
        "target_day": scope.day_utc,
        "scope_id": scope.scope_id,
        "summary_ref": summary_plain_ref,
        "selected_item_refs": [item["item_ref"] for item in selected_actionable + selected_blocked + selected_deltas],
        "critical_degraded_refs": [item["item_ref"] for item in selected_degraded],
        "rendered_ai_summaries": ai_summaries,
    }
    if prior_snapshot_ref is not None:
        snapshot_payload["prior_snapshot_ref"] = _plain_ref_from_surface(prior_snapshot_ref, SNAPSHOT_ARTIFACT_ID)

    summary_payload["semantic_events"].append(
        {
            "event_type": "product_snapshot_created",
            "authority_label": "governed_product_summary",
            "summary_type": "product_summary",
            "selected_item_refs": snapshot_payload["selected_item_refs"],
            "critical_degraded_refs": snapshot_payload["critical_degraded_refs"],
            "governing_refs": evidence_refs,
            "kernel_version": KERNEL_VERSION,
        }
    )
    for ai_summary in ai_summaries:
        summary_payload["semantic_events"].append(
            {
                "event_type": "ai_summary_generated" if ai_summary["assist_status"] == "generated" else "ai_summary_fallback_used",
                "authority_label": "governed_product_summary",
                "summary_type": ai_summary["summary_type"],
                "selected_item_refs": [item["item_ref"] for item in selected_actionable + selected_blocked + selected_deltas],
                "critical_degraded_refs": [item["item_ref"] for item in selected_degraded],
                "governing_refs": evidence_refs,
                "kernel_version": KERNEL_VERSION,
            }
        )

    summary_ref = None
    snapshot_ref = None
    if emit_artifacts:
        summary_ref = _write_projection(
            payload=summary_payload,
            canonical_truth_root=scope.canonical_truth_root,
            day_utc=scope.day_utc,
            scope_id=scope.scope_id,
            artifact_id=ARTIFACT_ID,
            schema_relpath=SCHEMA_RELPATH,
            key_id="summary_id",
            filename="product_summary.v1.json",
        )
        summary_plain_ref = _plain_ref_from_surface(summary_ref, ARTIFACT_ID)
        snapshot_payload["summary_ref"] = summary_plain_ref
        for ai_summary in ai_summaries:
            ai_summary["source_summary_ref"] = summary_plain_ref
        snapshot_ref = _write_projection(
            payload=snapshot_payload,
            canonical_truth_root=scope.canonical_truth_root,
            day_utc=scope.day_utc,
            scope_id=scope.scope_id,
            artifact_id=SNAPSHOT_ARTIFACT_ID,
            schema_relpath=SNAPSHOT_SCHEMA_RELPATH,
            key_id="snapshot_id",
            filename="product_snapshot.v1.json",
        )

    return {
        "ok": True,
        "artifact_id": ARTIFACT_ID,
        "summary": summary_payload,
        "summary_ref": summary_plain_ref if emit_artifacts else None,
        "snapshot": snapshot_payload,
        "snapshot_ref": _plain_ref_from_surface(snapshot_ref, SNAPSHOT_ARTIFACT_ID) if snapshot_ref is not None else None,
    }
