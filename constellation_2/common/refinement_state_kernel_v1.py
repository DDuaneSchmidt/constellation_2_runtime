from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

from constellation_2.common.constitutional_runtime_v1 import (
    assert_constitutional_writer_allowed_v1,
    resolve_constitutional_artifact_path_v1,
)
from constellation_2.common.control_plane_validation_kernel_v1 import _build_scope
from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_idempotent_validated_json_v1,
    read_validated_surface_v1,
)
from constellation_2.common.product_snapshot_v1 import find_latest_product_snapshot_v1
from constellation_2.common.product_summary_kernel_v1 import find_latest_product_summary_v1
from constellation_2.common.refinement_action_model_v1 import ACTION_MODEL_VERSION, build_refinement_action_v1
from constellation_2.common.refinement_threshold_v1 import THRESHOLD_MODEL_VERSION, evaluate_refinement_threshold_v1
from constellation_2.common.value_state_kernel_v1 import list_value_states_v1
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
WRITER_ID = "constellation_2.common.refinement_state_kernel_v1"
KERNEL_VERSION = WRITER_ID
ARTIFACT_ID = "refinement_state_v1"
SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/RUNTIME/refinement_state.v1.schema.json"


class RefinementStateError(RuntimeError):
    pass


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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


def _assert_allowed_truth_root(canonical_truth_root: Path) -> None:
    repo_runtime_root = (REPO_ROOT / "constellation_2" / "runtime").resolve()
    resolved = canonical_truth_root.resolve()
    if resolved == REPO_ROOT or repo_runtime_root == resolved or repo_runtime_root in resolved.parents:
        raise RefinementStateError(f"REFINEMENT_STATE_FORBIDDEN_TRUTH_ROOT:{resolved}")


def _plain_ref(*, artifact_id: str, path: Path, sha256: str) -> dict[str, str]:
    return {
        "artifact_id": str(artifact_id).strip(),
        "artifact_path": str(path.resolve()),
        "artifact_sha256": str(sha256).strip(),
    }


def _plain_ref_from_surface(surface: SurfaceRefV1, artifact_id: str) -> dict[str, str]:
    return _plain_ref(artifact_id=artifact_id, path=surface.path, sha256=surface.sha256)


def _current_refs(refs: Iterable[SurfaceRefV1]) -> list[SurfaceRefV1]:
    rows = list(refs)
    superseded_paths = {
        str((ref.payload.get("supersedes_ref") or {}).get("artifact_path") or "").strip()
        for ref in rows
        if isinstance(ref.payload.get("supersedes_ref"), dict)
    }
    current = [ref for ref in rows if str(ref.path.resolve()) not in superseded_paths]
    return current or rows


def list_refinement_states_v1(
    *,
    canonical_truth_root: Path | str,
    day_utc: str,
    scope_id: str | None = None,
) -> list[SurfaceRefV1]:
    root = (Path(canonical_truth_root).resolve() / "reports" / ARTIFACT_ID / day_utc).resolve()
    refs: list[SurfaceRefV1] = []
    if not root.exists():
        return refs
    pattern = "*/*/refinement_state.v1.json" if not scope_id else f"{scope_id}/*/refinement_state.v1.json"
    for path in sorted(root.glob(pattern)):
        refs.append(read_validated_surface_v1(path=path, schema_relpath=SCHEMA_RELPATH))
    refs.sort(
        key=lambda ref: (
            str(ref.payload.get("generated_at_utc") or ""),
            str(ref.payload.get("refinement_id") or ""),
            str(ref.path),
        )
    )
    return refs


def _find_latest_refinement_for_target(
    *,
    canonical_truth_root: Path,
    day_utc: str,
    scope_id: str,
    refinement_target: str,
) -> SurfaceRefV1 | None:
    refs = [
        ref
        for ref in list_refinement_states_v1(canonical_truth_root=canonical_truth_root, day_utc=day_utc, scope_id=scope_id)
        if str(ref.payload.get("refinement_target") or "") == refinement_target
    ]
    if not refs:
        return None
    return _current_refs(refs)[-1]


def _value_rows_by_opportunity(
    *,
    canonical_truth_root: Path,
    day_utc: str,
    scope_id: str,
) -> tuple[dict[str, list[SurfaceRefV1]], list[SurfaceRefV1]]:
    refs = _current_refs(list_value_states_v1(canonical_truth_root=canonical_truth_root, day_utc=day_utc, scope_id=scope_id))
    rows: dict[str, list[SurfaceRefV1]] = {}
    for ref in refs:
        subject = str(ref.payload.get("subject_opportunity_id") or "").strip()
        if not subject:
            continue
        rows.setdefault(subject, []).append(ref)
    return rows, refs


def _iter_summary_targets(summary_ref: SurfaceRefV1) -> Iterable[tuple[str, dict[str, Any]]]:
    payload = summary_ref.payload
    for bucket in ("top_actionable_items", "top_blocked_items", "top_review_deltas", "critical_degraded_states"):
        for row in payload.get(bucket) or []:
            if isinstance(row, dict):
                yield bucket, dict(row)


def _protected_distinctions(
    *,
    source_bucket: str,
    row: dict[str, Any],
    matched_values: list[SurfaceRefV1],
) -> list[str]:
    distinctions: list[str] = []
    if source_bucket == "critical_degraded_states":
        distinctions.append("critical_degraded_state")
    if source_bucket == "top_blocked_items" or list(row.get("blocker_states") or []):
        distinctions.append("blocked_but_important")
    if str(row.get("label") or "") == "Release readiness":
        distinctions.append("release_readiness_blocker")
    claim_strengths = {str(ref.payload.get("claim_strength") or "") for ref in matched_values}
    if claim_strengths & {"insufficient_evidence", "not_yet_observable"}:
        distinctions.append("claim_strength_withheld")
        distinctions.append("insufficient_or_not_yet_observable")
    return distinctions


def _sanitize_readiness_summary(readiness: dict[str, Any]) -> dict[str, Any]:
    return {
        "blocked": bool(readiness.get("blocked")),
        "blocked_state": str(readiness.get("blocked_state") or "READY"),
        "operator_status": str(readiness.get("operator_status") or "UNKNOWN"),
    }


def _candidate_from_summary_row(
    *,
    source_bucket: str,
    row: dict[str, Any],
    summary_ref: SurfaceRefV1,
    snapshot_ref: SurfaceRefV1 | None,
    matched_values: list[SurfaceRefV1],
    any_value_refs: list[SurfaceRefV1],
    prior_day: str | None,
) -> dict[str, Any]:
    item_ref = row.get("item_ref") if isinstance(row.get("item_ref"), dict) else None
    evidence_refs = [_plain_ref_from_surface(summary_ref, "product_summary_v1")]
    if snapshot_ref is not None:
        evidence_refs.append(_plain_ref_from_surface(snapshot_ref, "product_snapshot_v1"))
    if item_ref:
        evidence_refs.append(dict(item_ref))
    evidence_refs.extend(_plain_ref_from_surface(ref, "value_state_v1") for ref in matched_values)
    preserved_drilldown_refs = []
    if item_ref:
        preserved_drilldown_refs.append(dict(item_ref))
    preserved_drilldown_refs.extend(_plain_ref_from_surface(ref, "value_state_v1") for ref in matched_values)
    protected = _protected_distinctions(source_bucket=source_bucket, row=row, matched_values=matched_values)
    has_basis = bool(matched_values) or bool(any_value_refs)
    trust_critical = bool(protected)
    insufficient_basis = not trust_critical and not has_basis
    return {
        "refinement_target": str(row.get("item_id") or f"{source_bucket}:{row.get('label') or 'item'}"),
        "target_scope": "command_home",
        "target_label": str(row.get("label") or row.get("item_kind") or "summary_item"),
        "source_bucket": source_bucket,
        "summary_message": str(row.get("summary_message") or row.get("label") or "Summary item"),
        "review_priority": str(row.get("review_priority") or "monitor_only"),
        "has_refinement_basis": has_basis,
        "trust_critical": trust_critical,
        "insufficient_basis": insufficient_basis,
        "protected_distinctions": protected,
        "evidence_basis_refs": evidence_refs,
        "before_snapshot_ref": (_plain_ref_from_surface(snapshot_ref, "product_snapshot_v1") if snapshot_ref is not None else None),
        "review_window": {"target_day": str(summary_ref.payload.get("target_day") or ""), "prior_day": prior_day},
        "before_state": {
            "surface_bucket": source_bucket,
            "summary_message": str(row.get("summary_message") or row.get("label") or "Summary item"),
        },
        "preserved_drilldown_refs": preserved_drilldown_refs,
        "drill_down_route": str(row.get("drill_down_route") or "/"),
        "source_selection_reason_id": str(row.get("selection_reason_id") or "UNKNOWN"),
        "readiness_summary": _sanitize_readiness_summary(dict(summary_ref.payload.get("readiness_summary") or {})),
    }


def _value_summary_candidate(
    *,
    summary_ref: SurfaceRefV1,
    snapshot_ref: SurfaceRefV1 | None,
    all_value_refs: list[SurfaceRefV1],
    prior_day: str | None,
) -> dict[str, Any] | None:
    if not all_value_refs:
        return None
    claim_strengths = {str(ref.payload.get("claim_strength") or "") for ref in all_value_refs}
    protected: list[str] = []
    if claim_strengths & {"insufficient_evidence", "not_yet_observable"}:
        protected.extend(["claim_strength_withheld", "insufficient_or_not_yet_observable"])
    evidence_refs = [_plain_ref_from_surface(summary_ref, "product_summary_v1")]
    if snapshot_ref is not None:
        evidence_refs.append(_plain_ref_from_surface(snapshot_ref, "product_snapshot_v1"))
    evidence_refs.extend(_plain_ref_from_surface(ref, "value_state_v1") for ref in all_value_refs)
    return {
        "refinement_target": "value_summary",
        "target_scope": "command_home",
        "target_label": "Value proof overview",
        "source_bucket": "value_summary",
        "summary_message": "Current governed value proof remains available through the Value drill-down.",
        "review_priority": "review_soon",
        "has_refinement_basis": True,
        "trust_critical": bool(protected),
        "insufficient_basis": False,
        "protected_distinctions": protected,
        "evidence_basis_refs": evidence_refs,
        "before_snapshot_ref": (_plain_ref_from_surface(snapshot_ref, "product_snapshot_v1") if snapshot_ref is not None else None),
        "review_window": {"target_day": str(summary_ref.payload.get("target_day") or ""), "prior_day": prior_day},
        "before_state": {
            "surface_bucket": "value_drilldown",
            "summary_message": "Value proof was available only through drill-down.",
        },
        "preserved_drilldown_refs": [_plain_ref_from_surface(ref, "value_state_v1") for ref in all_value_refs],
        "drill_down_route": "/outcomes",
        "source_selection_reason_id": "VALUE_DRILLDOWN_AVAILABLE",
        "readiness_summary": _sanitize_readiness_summary(dict(summary_ref.payload.get("readiness_summary") or {})),
    }


def _write_refinement(payload: dict[str, Any], scope: Any) -> SurfaceRefV1:
    assert_constitutional_writer_allowed_v1(repo_root=REPO_ROOT, artifact_id=ARTIFACT_ID, writer_id=WRITER_ID)
    output_path = resolve_constitutional_artifact_path_v1(
        repo_root=REPO_ROOT,
        artifact_id=ARTIFACT_ID,
        day_utc=scope.day_utc,
        canonical_truth_root=scope.canonical_truth_root,
        extra_variables={"scope_id": scope.scope_id, "refinement_id": payload["refinement_id"]},
    )
    return atomic_write_idempotent_validated_json_v1(
        path=output_path,
        payload=payload,
        schema_relpath=SCHEMA_RELPATH,
        volatile_field_names=("generated_at_utc",),
    )


def materialize_refinement_state_v1(
    *,
    day_utc: str,
    sleeve_id: str,
    environment: str,
    ib_account: str,
    canonical_truth_root: Path | str,
    truth_sleeves_root: Path | str | None = None,
    operation_type: str = "fresh_paper_entry_v1",
    emit_artifacts: bool = True,
) -> dict[str, Any]:
    canonical_truth_root = Path(canonical_truth_root).resolve()
    _assert_allowed_truth_root(canonical_truth_root)
    scope = _scope(
        day_utc=day_utc,
        sleeve_id=sleeve_id,
        environment=environment,
        ib_account=ib_account,
        operation_type=operation_type,
        canonical_truth_root=canonical_truth_root,
        truth_sleeves_root=truth_sleeves_root,
    )
    summary_ref = find_latest_product_summary_v1(canonical_truth_root=canonical_truth_root, day_utc=scope.day_utc, scope_id=scope.scope_id)
    if summary_ref is None:
        raise RefinementStateError("REFINEMENT_STATE_REQUIRES_PRODUCT_SUMMARY")
    snapshot_ref = find_latest_product_snapshot_v1(canonical_truth_root=canonical_truth_root, scope_id=scope.scope_id, day_utc=scope.day_utc)
    if snapshot_ref is None:
        raise RefinementStateError("REFINEMENT_STATE_REQUIRES_PRODUCT_SNAPSHOT")
    prior_snapshot_ref = find_latest_product_snapshot_v1(
        canonical_truth_root=canonical_truth_root,
        scope_id=scope.scope_id,
        before_day_utc=scope.day_utc,
    )
    value_rows_by_opportunity, all_value_refs = _value_rows_by_opportunity(
        canonical_truth_root=canonical_truth_root,
        day_utc=scope.day_utc,
        scope_id=scope.scope_id,
    )
    rows: list[dict[str, Any]] = []
    artifact_refs: list[dict[str, str]] = []
    for source_bucket, row in _iter_summary_targets(summary_ref):
        target_id = str(row.get("item_id") or "")
        matched_values = value_rows_by_opportunity.get(target_id, [])
        candidate = _candidate_from_summary_row(
            source_bucket=source_bucket,
            row=row,
            summary_ref=summary_ref,
            snapshot_ref=snapshot_ref,
            matched_values=matched_values,
            any_value_refs=all_value_refs,
            prior_day=(str(prior_snapshot_ref.payload.get("target_day") or "") if prior_snapshot_ref is not None else None),
        )
        threshold = evaluate_refinement_threshold_v1(candidate)
        action = build_refinement_action_v1(threshold=threshold, candidate=candidate)
        refinement_id = canonical_hash_for_c2_artifact_v1(
            {
                "target_day": scope.day_utc,
                "scope_id": scope.scope_id,
                "refinement_target": candidate["refinement_target"],
                "action": action["refinement_action"],
                "visibility_effect": action["visibility_effect"],
                "reason_id": threshold["reason_id"],
            }
        )
        payload: dict[str, Any] = {
            "schema_id": "refinement_state",
            "schema_version": "v1",
            "artifact_id": ARTIFACT_ID,
            "surface_kind": "projection",
            "refinement_id": refinement_id,
            "kernel_version": KERNEL_VERSION,
            "threshold_model_version": THRESHOLD_MODEL_VERSION,
            "action_model_version": ACTION_MODEL_VERSION,
            "generated_at_utc": _utc_now(),
            "authority_label": "governed_certified_refinement",
            "target_day": scope.day_utc,
            "scope_id": scope.scope_id,
            "operation_type": scope.operation_type,
            "sleeve_id": scope.sleeve_id,
            "environment": scope.environment,
            "ib_account": scope.ib_account,
            "refinement_target": candidate["refinement_target"],
            "target_scope": candidate["target_scope"],
            "target_label": candidate["target_label"],
            "evidence_basis_refs": candidate["evidence_basis_refs"],
            "refinement_action": action["refinement_action"],
            "refinement_strength": action["refinement_strength"],
            "refinement_reason_ids": [threshold["reason_id"]],
            "review_window": candidate["review_window"],
            "reversibility_state": action["reversibility_state"],
            "visibility_effect": action["visibility_effect"],
            "protected_distinctions": candidate["protected_distinctions"],
            "source_selection_reason_id": candidate["source_selection_reason_id"],
            "before_state": candidate["before_state"],
            "after_state": {
                "surface_bucket": action["after_bucket"],
                "summary_message": action["summary_message"],
            },
            "drill_down_route": candidate["drill_down_route"],
            "readiness_summary": candidate["readiness_summary"],
            "preserved_drilldown_refs": candidate["preserved_drilldown_refs"],
            "summary_message": action["summary_message"],
            "semantic_events": [],
        }
        if candidate["before_snapshot_ref"] is not None:
            payload["before_snapshot_ref"] = candidate["before_snapshot_ref"]
        prior_ref = _find_latest_refinement_for_target(
            canonical_truth_root=canonical_truth_root,
            day_utc=scope.day_utc,
            scope_id=scope.scope_id,
            refinement_target=candidate["refinement_target"],
        )
        if prior_ref is not None and str(prior_ref.payload.get("refinement_id") or "") != refinement_id:
            payload["supersedes_ref"] = _plain_ref_from_surface(prior_ref, ARTIFACT_ID)
        event_types = {event["event_type"] for event in payload["semantic_events"]}
        if action["refinement_action"] == "refinement_withheld":
            event_types.add("refinement_withheld_due_to_insufficient_evidence")
        if action["refinement_action"] == "preserve_top_level" and candidate["protected_distinctions"]:
            event_types.add("top_level_visibility_preserved_for_trust")
        if action["refinement_action"] == "compress_summary":
            event_types.add("compression_applied")
        if action["refinement_action"] == "demote_to_secondary":
            event_types.add("demotion_applied")
        event_types.add("refinement_snapshot_created")
        payload["semantic_events"] = [
            {
                "event_type": event_type,
                "authority_label": "governed_certified_refinement",
                "refinement_target": candidate["refinement_target"],
                "refinement_action": action["refinement_action"],
                "refinement_strength": action["refinement_strength"],
                "evidence_basis_refs": candidate["evidence_basis_refs"],
                "kernel_version": KERNEL_VERSION,
                **({"before_snapshot_ref": candidate["before_snapshot_ref"]} if candidate["before_snapshot_ref"] is not None else {}),
            }
            for event_type in sorted(event_types)
        ]
        rows.append(payload)
        if emit_artifacts:
            artifact_ref = _write_refinement(payload, scope)
            artifact_refs.append(_plain_ref_from_surface(artifact_ref, ARTIFACT_ID))
            if isinstance(payload.get("supersedes_ref"), dict):
                prior_path = Path(payload["supersedes_ref"]["artifact_path"])
                prior_payload = read_validated_surface_v1(path=prior_path, schema_relpath=SCHEMA_RELPATH).payload
                updated_prior = dict(prior_payload)
                updated_prior["superseded_by_ref"] = _plain_ref_from_surface(artifact_ref, ARTIFACT_ID)
                atomic_write_idempotent_validated_json_v1(
                    path=prior_path,
                    payload=updated_prior,
                    schema_relpath=SCHEMA_RELPATH,
                    volatile_field_names=("generated_at_utc",),
                )
    value_candidate = _value_summary_candidate(
        summary_ref=summary_ref,
        snapshot_ref=snapshot_ref,
        all_value_refs=all_value_refs,
        prior_day=(str(prior_snapshot_ref.payload.get("target_day") or "") if prior_snapshot_ref is not None else None),
    )
    if value_candidate is not None:
        threshold = evaluate_refinement_threshold_v1(value_candidate)
        action = build_refinement_action_v1(threshold=threshold, candidate=value_candidate)
        refinement_id = canonical_hash_for_c2_artifact_v1(
            {
                "target_day": scope.day_utc,
                "scope_id": scope.scope_id,
                "refinement_target": value_candidate["refinement_target"],
                "action": action["refinement_action"],
                "visibility_effect": action["visibility_effect"],
                "reason_id": threshold["reason_id"],
            }
        )
        payload = {
            "schema_id": "refinement_state",
            "schema_version": "v1",
            "artifact_id": ARTIFACT_ID,
            "surface_kind": "projection",
            "refinement_id": refinement_id,
            "kernel_version": KERNEL_VERSION,
            "threshold_model_version": THRESHOLD_MODEL_VERSION,
            "action_model_version": ACTION_MODEL_VERSION,
            "generated_at_utc": _utc_now(),
            "authority_label": "governed_certified_refinement",
            "target_day": scope.day_utc,
            "scope_id": scope.scope_id,
            "operation_type": scope.operation_type,
            "sleeve_id": scope.sleeve_id,
            "environment": scope.environment,
            "ib_account": scope.ib_account,
            "refinement_target": value_candidate["refinement_target"],
            "target_scope": value_candidate["target_scope"],
            "target_label": value_candidate["target_label"],
            "evidence_basis_refs": value_candidate["evidence_basis_refs"],
            "refinement_action": action["refinement_action"],
            "refinement_strength": action["refinement_strength"],
            "refinement_reason_ids": [threshold["reason_id"]],
            "review_window": value_candidate["review_window"],
            "reversibility_state": action["reversibility_state"],
            "visibility_effect": action["visibility_effect"],
            "protected_distinctions": value_candidate["protected_distinctions"],
            "source_selection_reason_id": value_candidate["source_selection_reason_id"],
            "before_state": value_candidate["before_state"],
            "after_state": {
                "surface_bucket": action["after_bucket"],
                "summary_message": action["summary_message"],
            },
            "drill_down_route": value_candidate["drill_down_route"],
            "readiness_summary": value_candidate["readiness_summary"],
            "preserved_drilldown_refs": value_candidate["preserved_drilldown_refs"],
            "summary_message": action["summary_message"],
            "semantic_events": [
                {
                    "event_type": event_type,
                    "authority_label": "governed_certified_refinement",
                    "refinement_target": value_candidate["refinement_target"],
                    "refinement_action": action["refinement_action"],
                    "refinement_strength": action["refinement_strength"],
                    "evidence_basis_refs": value_candidate["evidence_basis_refs"],
                    **({"before_snapshot_ref": value_candidate["before_snapshot_ref"]} if value_candidate["before_snapshot_ref"] is not None else {}),
                    "kernel_version": KERNEL_VERSION,
                }
                for event_type in sorted({"refinement_state_computed", "refinement_action_selected", "compression_applied", "refinement_snapshot_created"})
            ],
        }
        if value_candidate["before_snapshot_ref"] is not None:
            payload["before_snapshot_ref"] = value_candidate["before_snapshot_ref"]
        rows.append(payload)
        if emit_artifacts:
            artifact_ref = _write_refinement(payload, scope)
            artifact_refs.append(_plain_ref_from_surface(artifact_ref, ARTIFACT_ID))
    return {
        "ok": True,
        "artifact_id": ARTIFACT_ID,
        "rows": rows,
        "artifact_refs": artifact_refs,
    }
