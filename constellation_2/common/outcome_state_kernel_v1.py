from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

from constellation_2.common.constitutional_runtime_v1 import (
    assert_constitutional_writer_allowed_v1,
    resolve_constitutional_artifact_path_v1,
)
from constellation_2.common.control_plane_validation_kernel_v1 import _build_scope
from constellation_2.common.opportunity_state_kernel_v1 import list_opportunity_states_v1
from constellation_2.common.outcome_claim_strength_v1 import (
    CLAIM_LADDER_VERSION,
    evaluate_outcome_claim_strength_v1,
)
from constellation_2.common.outcome_comparison_v1 import (
    COMPARISON_VERSION,
    evaluate_outcome_comparison_v1,
)
from constellation_2.common.outcome_effectiveness_attribution_v1 import (
    MODEL_VERSION as EFFECTIVENESS_MODEL_VERSION,
    evaluate_outcome_effectiveness_attribution_v1,
)
from constellation_2.common.outcome_explanation_mapping_v1 import (
    EXPLANATION_MAPPING_VERSION,
    build_outcome_explanation_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_idempotent_validated_json_v1,
    read_validated_surface_v1,
)
from constellation_2.common.product_snapshot_v1 import find_latest_product_snapshot_v1
from constellation_2.common.product_summary_kernel_v1 import find_latest_product_summary_v1
from constellation_2.common.tax_state_kernel_v1 import find_latest_tax_state_v1
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
WRITER_ID = "constellation_2.common.outcome_state_kernel_v1"
KERNEL_VERSION = WRITER_ID
ARTIFACT_ID = "outcome_state_v1"
SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/RUNTIME/outcome_state.v1.schema.json"
FILL_LEDGER_SCHEMA = "governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/fill_ledger.v1.schema.json"
RECON_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/reconciliation_report.v3.schema.json"


class OutcomeStateError(RuntimeError):
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
        raise OutcomeStateError(f"OUTCOME_STATE_FORBIDDEN_TRUTH_ROOT:{resolved}")


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


def list_outcome_states_v1(
    *,
    canonical_truth_root: Path | str,
    day_utc: str,
    scope_id: str | None = None,
) -> list[SurfaceRefV1]:
    root = (Path(canonical_truth_root).resolve() / "reports" / ARTIFACT_ID / day_utc).resolve()
    refs: list[SurfaceRefV1] = []
    if not root.exists():
        return refs
    pattern = "*/*/outcome_state.v1.json" if not scope_id else f"{scope_id}/*/outcome_state.v1.json"
    for path in sorted(root.glob(pattern)):
        refs.append(read_validated_surface_v1(path=path, schema_relpath=SCHEMA_RELPATH))
    refs.sort(
        key=lambda ref: (
            str(ref.payload.get("generated_at_utc") or ""),
            str(ref.payload.get("outcome_id") or ""),
            str(ref.path),
        )
    )
    return refs


def _find_latest_outcome_for_subject(
    *,
    canonical_truth_root: Path,
    day_utc: str,
    scope_id: str,
    subject_opportunity_id: str,
) -> SurfaceRefV1 | None:
    refs = [
        ref
        for ref in list_outcome_states_v1(canonical_truth_root=canonical_truth_root, day_utc=day_utc, scope_id=scope_id)
        if str(ref.payload.get("subject_opportunity_id") or "") == subject_opportunity_id
    ]
    if not refs:
        return None
    current = _current_refs(refs)
    return current[-1]


def _list_fill_ledgers(*, canonical_truth_root: Path, day_utc: str) -> list[SurfaceRefV1]:
    root = (canonical_truth_root / "fill_ledger_v1" / day_utc).resolve()
    refs: list[SurfaceRefV1] = []
    if not root.exists():
        return refs
    for path in sorted(root.glob("*.fill_ledger.v1.json")):
        refs.append(read_validated_surface_v1(path=path, schema_relpath=FILL_LEDGER_SCHEMA))
    return refs


def _load_reconciliation_ref(*, canonical_truth_root: Path, day_utc: str) -> SurfaceRefV1 | None:
    path = (canonical_truth_root / "reports" / "reconciliation_report_v3" / day_utc / "reconciliation_report.v3.json").resolve()
    if not path.exists():
        return None
    return read_validated_surface_v1(path=path, schema_relpath=RECON_SCHEMA)


def _selected_in_product_summary(opportunity_ref: SurfaceRefV1, summary_ref: SurfaceRefV1 | None) -> bool:
    if summary_ref is None:
        return False
    path_text = str(opportunity_ref.path.resolve())
    payload = summary_ref.payload
    for key in ("top_actionable_items", "top_blocked_items", "top_review_deltas", "critical_degraded_states"):
        for row in payload.get(key) or ():
            item_ref = row.get("item_ref") if isinstance(row, dict) else None
            if isinstance(item_ref, dict) and str(item_ref.get("artifact_path") or "").strip() == path_text:
                return True
    return False


def _selected_in_product_snapshot(opportunity_ref: SurfaceRefV1, snapshot_ref: SurfaceRefV1 | None) -> bool:
    if snapshot_ref is None:
        return False
    path_text = str(opportunity_ref.path.resolve())
    for row in snapshot_ref.payload.get("selected_item_refs") or ():
        if isinstance(row, dict) and str(row.get("artifact_path") or "").strip() == path_text:
            return True
    return False


def _realized_basis(
    *,
    day_utc: str,
    fill_refs: list[SurfaceRefV1],
    reconciliation_ref: SurfaceRefV1 | None,
) -> dict[str, Any]:
    filled_activity = any(int(ref.payload.get("filled_qty") or 0) > 0 for ref in fill_refs)
    no_activity_from_ledgers = bool(fill_refs) and not filled_activity
    recon_payload = reconciliation_ref.payload if reconciliation_ref is not None else {}
    recon_submission_total = int((((recon_payload.get("truth_side") or {}).get("counts") or {}).get("submissions_total")) or 0)
    recon_no_submissions = reconciliation_ref is not None and recon_submission_total == 0
    today = datetime.now(UTC).date().isoformat()
    if fill_refs or reconciliation_ref is not None:
        observability_state = "observable_now"
        basis_state = "complete_realized_basis"
    else:
        observability_state = "not_yet_observable" if str(day_utc) >= today else "insufficient_evidence"
        basis_state = "incomplete_realized_basis"
    realized_activity_observed = filled_activity or recon_submission_total > 0
    no_activity_observed = recon_no_submissions or no_activity_from_ledgers
    return {
        "observability_state": observability_state,
        "basis_state": basis_state,
        "realized_activity_observed": realized_activity_observed,
        "no_activity_observed": no_activity_observed,
    }


def _realized_state(
    *,
    opportunity_payload: dict[str, Any],
    observability_state: str,
    realized_activity_observed: bool,
    no_activity_observed: bool,
) -> str:
    if str(opportunity_payload.get("visibility_state") or "") == "historical_only":
        return "historical_only"
    if observability_state == "not_yet_observable":
        return "unknown"
    if (
        str(opportunity_payload.get("opportunity_type") or "") == "platform_blocker_review"
        and str(opportunity_payload.get("opportunity_state") or "") == "blocked"
        and no_activity_observed
        and not realized_activity_observed
    ):
        return "blocked_without_realization"
    if realized_activity_observed:
        return "realized_activity_observed"
    if no_activity_observed:
        return "no_realized_activity_observed"
    return "unknown"


def _write_outcome(payload: dict[str, Any], scope: Any) -> SurfaceRefV1:
    assert_constitutional_writer_allowed_v1(repo_root=REPO_ROOT, artifact_id=ARTIFACT_ID, writer_id=WRITER_ID)
    output_path = resolve_constitutional_artifact_path_v1(
        repo_root=REPO_ROOT,
        artifact_id=ARTIFACT_ID,
        day_utc=scope.day_utc,
        canonical_truth_root=scope.canonical_truth_root,
        extra_variables={"scope_id": scope.scope_id, "outcome_id": payload["outcome_id"]},
    )
    return atomic_write_idempotent_validated_json_v1(
        path=output_path,
        payload=payload,
        schema_relpath=SCHEMA_RELPATH,
        volatile_field_names=("generated_at_utc",),
    )


def materialize_outcome_state_v1(
    *,
    day_utc: str,
    sleeve_id: str,
    environment: str,
    ib_account: str,
    operation_type: str = "fresh_paper_entry_v1",
    canonical_truth_root: Path | str | None = None,
    truth_sleeves_root: Path | str | None = None,
    comparison_type: str | None = None,
    emit_artifacts: bool = False,
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
    _assert_allowed_truth_root(scope.canonical_truth_root)
    opportunity_refs = _current_refs(
        list_opportunity_states_v1(
            canonical_truth_root=scope.canonical_truth_root,
            day_utc=scope.day_utc,
            scope_id=scope.scope_id,
        )
    )
    if not opportunity_refs:
        raise OutcomeStateError("OUTCOME_STATE_REQUIRES_OPPORTUNITY_STATE")
    tax_ref = find_latest_tax_state_v1(
        canonical_truth_root=scope.canonical_truth_root,
        day_utc=scope.day_utc,
        scope_id=scope.scope_id,
    )
    if tax_ref is None:
        raise OutcomeStateError("OUTCOME_STATE_REQUIRES_TAX_STATE")
    product_summary_ref = find_latest_product_summary_v1(
        canonical_truth_root=scope.canonical_truth_root,
        day_utc=scope.day_utc,
        scope_id=scope.scope_id,
    )
    if product_summary_ref is None:
        raise OutcomeStateError("OUTCOME_STATE_REQUIRES_PRODUCT_SUMMARY")
    product_snapshot_ref = find_latest_product_snapshot_v1(
        canonical_truth_root=scope.canonical_truth_root,
        scope_id=scope.scope_id,
        day_utc=scope.day_utc,
    )
    if product_snapshot_ref is None:
        raise OutcomeStateError("OUTCOME_STATE_REQUIRES_PRODUCT_SNAPSHOT")

    fill_refs = _list_fill_ledgers(canonical_truth_root=scope.canonical_truth_root, day_utc=scope.day_utc)
    reconciliation_ref = _load_reconciliation_ref(canonical_truth_root=scope.canonical_truth_root, day_utc=scope.day_utc)
    basis = _realized_basis(day_utc=scope.day_utc, fill_refs=fill_refs, reconciliation_ref=reconciliation_ref)
    realized_refs = [_plain_ref_from_surface(ref, "fill_ledger_v1") for ref in fill_refs]
    if reconciliation_ref is not None:
        realized_refs.append(_plain_ref_from_surface(reconciliation_ref, "reconciliation_report_v3"))

    artifact_rows: list[dict[str, Any]] = []
    artifact_refs: list[dict[str, str]] = []
    for opportunity_ref in opportunity_refs:
        opportunity_payload = dict(opportunity_ref.payload)
        realized_state = _realized_state(
            opportunity_payload=opportunity_payload,
            observability_state=str(basis["observability_state"]),
            realized_activity_observed=bool(basis["realized_activity_observed"]),
            no_activity_observed=bool(basis["no_activity_observed"]),
        )
        comparison_state = evaluate_outcome_comparison_v1(
            requested_comparison_type=comparison_type,
            opportunity_payload=opportunity_payload,
            realized_activity_observed=bool(basis["realized_activity_observed"]),
            no_activity_observed=bool(basis["no_activity_observed"]),
        )
        effectiveness = evaluate_outcome_effectiveness_attribution_v1(
            opportunity_payload=opportunity_payload,
            realized_state=realized_state,
            observability_state=str(basis["observability_state"]),
            comparison_state=comparison_state,
        )
        claim = evaluate_outcome_claim_strength_v1(
            {
                "observability_state": basis["observability_state"],
                "basis_state": basis["basis_state"],
                "attribution_state": effectiveness["attribution_state"],
                "comparison_state": comparison_state,
            }
        )
        product_summary_refs = (
            [_plain_ref_from_surface(product_summary_ref, "product_summary_v1")] if _selected_in_product_summary(opportunity_ref, product_summary_ref) else []
        )
        product_snapshot_refs = (
            [_plain_ref_from_surface(product_snapshot_ref, "product_snapshot_v1")] if _selected_in_product_snapshot(opportunity_ref, product_snapshot_ref) else []
        )
        evidence_refs = [
            *[dict(row) for row in (opportunity_payload.get("governing_advisory_refs") or []) if isinstance(row, dict)],
            *[dict(row) for row in (opportunity_payload.get("governing_tax_refs") or []) if isinstance(row, dict)],
            _plain_ref_from_surface(opportunity_ref, "opportunity_state_v1"),
            *realized_refs,
            *product_summary_refs,
            *product_snapshot_refs,
        ]
        outcome_id = canonical_hash_for_c2_artifact_v1(
            {
                "artifact_id": ARTIFACT_ID,
                "scope_id": scope.scope_id,
                "subject_opportunity_id": str(opportunity_payload.get("opportunity_id") or ""),
                "realized_state": realized_state,
                "observability_state": basis["observability_state"],
                "effectiveness_state": effectiveness["effectiveness_state"],
                "attribution_state": effectiveness["attribution_state"],
                "claim_strength": claim["claim_strength"],
                "comparison_state": comparison_state,
            }
        )
        payload = {
            "schema_id": "outcome_state",
            "schema_version": "v1",
            "artifact_id": ARTIFACT_ID,
            "surface_kind": "projection",
            "outcome_id": outcome_id,
            "kernel_version": KERNEL_VERSION,
            "claim_ladder_version": CLAIM_LADDER_VERSION,
            "explanation_mapping_version": EXPLANATION_MAPPING_VERSION,
            "generated_at_utc": _utc_now(),
            "authority_label": "governed_certified_outcome",
            "target_day": scope.day_utc,
            "scope_id": scope.scope_id,
            "operation_type": scope.operation_type,
            "sleeve_id": scope.sleeve_id,
            "environment": scope.environment,
            "ib_account": scope.ib_account,
            "subject_opportunity_id": str(opportunity_payload.get("opportunity_id") or ""),
            "opportunity_type": str(opportunity_payload.get("opportunity_type") or ""),
            "realized_state": realized_state,
            "observability_state": str(basis["observability_state"]),
            "effectiveness_state": str(effectiveness["effectiveness_state"]),
            "attribution_state": str(effectiveness["attribution_state"]),
            "claim_strength": str(claim["claim_strength"]),
            "basis_state": str(basis["basis_state"]),
            "primary_rule_id": str(claim["primary_rule_id"]),
            "governing_advisory_refs": [dict(row) for row in (opportunity_payload.get("governing_advisory_refs") or []) if isinstance(row, dict)],
            "governing_tax_refs": [dict(row) for row in (opportunity_payload.get("governing_tax_refs") or []) if isinstance(row, dict)],
            "governing_opportunity_refs": [_plain_ref_from_surface(opportunity_ref, "opportunity_state_v1")],
            "governing_realized_refs": realized_refs,
            "governing_product_summary_refs": product_summary_refs,
            "governing_product_snapshot_refs": product_snapshot_refs,
            "comparison_state": comparison_state,
            "comparison_refs": realized_refs if comparison_state["comparison_status"] == "applied" else [],
            "evidence_refs": evidence_refs,
            "semantic_events": [],
        }
        payload["primary_explanation"] = build_outcome_explanation_v1(outcome_payload=payload)
        payload["semantic_events"] = [
            {
                "event_type": "outcome_state_computed",
                "authority_label": payload["authority_label"],
                "realized_state": payload["realized_state"],
                "observability_state": payload["observability_state"],
                "effectiveness_state": payload["effectiveness_state"],
                "attribution_state": payload["attribution_state"],
                "claim_strength": payload["claim_strength"],
                "governing_refs": evidence_refs,
                "comparison_refs": payload["comparison_refs"],
                "kernel_version": KERNEL_VERSION,
            },
            {
                "event_type": "claim_strength_assigned",
                "authority_label": payload["authority_label"],
                "realized_state": payload["realized_state"],
                "observability_state": payload["observability_state"],
                "effectiveness_state": payload["effectiveness_state"],
                "attribution_state": payload["attribution_state"],
                "claim_strength": payload["claim_strength"],
                "governing_refs": evidence_refs,
                "comparison_refs": payload["comparison_refs"],
                "kernel_version": KERNEL_VERSION,
            },
            {
                "event_type": "effectiveness_classified",
                "authority_label": payload["authority_label"],
                "realized_state": payload["realized_state"],
                "observability_state": payload["observability_state"],
                "effectiveness_state": payload["effectiveness_state"],
                "attribution_state": payload["attribution_state"],
                "claim_strength": payload["claim_strength"],
                "governing_refs": evidence_refs,
                "comparison_refs": payload["comparison_refs"],
                "kernel_version": KERNEL_VERSION,
            },
        ]
        if payload["claim_strength"] in {"insufficient_evidence", "not_yet_observable"}:
            payload["semantic_events"].append(
                {
                    "event_type": "attribution_withheld_due_to_insufficient_evidence",
                    "authority_label": payload["authority_label"],
                    "realized_state": payload["realized_state"],
                    "observability_state": payload["observability_state"],
                    "effectiveness_state": payload["effectiveness_state"],
                    "attribution_state": payload["attribution_state"],
                    "claim_strength": payload["claim_strength"],
                    "governing_refs": evidence_refs,
                    "comparison_refs": payload["comparison_refs"],
                    "kernel_version": KERNEL_VERSION,
                }
            )
        payload["semantic_events"].append(
            {
                "event_type": "comparison_applied" if comparison_state["comparison_status"] == "applied" else "comparison_rejected",
                "authority_label": payload["authority_label"],
                "realized_state": payload["realized_state"],
                "observability_state": payload["observability_state"],
                "effectiveness_state": payload["effectiveness_state"],
                "attribution_state": payload["attribution_state"],
                "claim_strength": payload["claim_strength"],
                "governing_refs": evidence_refs,
                "comparison_refs": payload["comparison_refs"],
                "kernel_version": KERNEL_VERSION,
            }
        )
        if product_snapshot_refs:
            payload["semantic_events"].append(
                {
                    "event_type": "product_outcome_snapshot_created",
                    "authority_label": payload["authority_label"],
                    "realized_state": payload["realized_state"],
                    "observability_state": payload["observability_state"],
                    "effectiveness_state": payload["effectiveness_state"],
                    "attribution_state": payload["attribution_state"],
                    "claim_strength": payload["claim_strength"],
                    "governing_refs": evidence_refs,
                    "comparison_refs": payload["comparison_refs"],
                    "kernel_version": KERNEL_VERSION,
                }
            )

        prior_ref = _find_latest_outcome_for_subject(
            canonical_truth_root=scope.canonical_truth_root,
            day_utc=scope.day_utc,
            scope_id=scope.scope_id,
            subject_opportunity_id=str(opportunity_payload.get("opportunity_id") or ""),
        )
        if prior_ref is not None and str(prior_ref.payload.get("outcome_id") or "") != outcome_id:
            payload["supersedes_ref"] = _plain_ref_from_surface(prior_ref, ARTIFACT_ID)

        if emit_artifacts:
            artifact_ref = _write_outcome(payload, scope)
            artifact_refs.append(_plain_ref_from_surface(artifact_ref, ARTIFACT_ID))
        artifact_rows.append(payload)

    return {
        "ok": True,
        "artifact_id": ARTIFACT_ID,
        "rows": artifact_rows,
        "artifact_refs": artifact_refs,
    }
