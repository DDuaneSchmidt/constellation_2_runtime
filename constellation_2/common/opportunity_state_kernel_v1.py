from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Sequence

from constellation_2.common.constitutional_runtime_v1 import (
    assert_constitutional_writer_allowed_v1,
    resolve_constitutional_artifact_path_v1,
)
from constellation_2.common.control_plane_validation_kernel_v1 import _build_scope, _check_path_boundary
from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_idempotent_validated_json_v1,
    read_validated_surface_v1,
)
from constellation_2.common.release_baseline_gate_v1 import build_certified_operational_readiness_v1
from constellation_2.common.tax_state_kernel_v1 import (
    SCHEMA_RELPATH as TAX_SCHEMA,
    find_latest_tax_state_v1,
)
from constellation_2.common.opportunity_explanation_mapping_v1 import (
    EXPLANATION_MAPPING_VERSION,
    map_opportunity_explanation_v1,
)
from constellation_2.common.opportunity_review_snapshot_v1 import (
    ARTIFACT_ID as SNAPSHOT_ARTIFACT_ID,
    SCHEMA_RELPATH as SNAPSHOT_SCHEMA_RELPATH,
    classify_delta_state_v1,
    find_latest_opportunity_review_snapshot_v1,
    opportunity_summary_fingerprint_v1,
    resolved_rows_v1,
)
from constellation_2.common.opportunity_scenario_significance_v1 import (
    evaluate_opportunity_scenario_significance_v1,
)
from constellation_2.common.opportunity_state_precedence_v1 import (
    evaluate_opportunity_state_precedence_v1,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
WRITER_ID = "constellation_2.common.opportunity_state_kernel_v1"
KERNEL_VERSION = WRITER_ID
ARTIFACT_ID = "opportunity_state_v1"
SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/RUNTIME/opportunity_state.v1.schema.json"
ADVISORY_SCHEMA = "governance/04_DATA/SCHEMAS/C2/RUNTIME/advisory_decision_state.v1.schema.json"
OPERATOR_STATUS_SCHEMA = "governance/04_DATA/SCHEMAS/C2/RUNTIME/control_plane_operator_status.v1.schema.json"
TIMELINE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/RUNTIME/transition_timeline_projection.v1.schema.json"
STRESS_CASE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/ADVISOR_KERNEL/stress_case_view.v1.schema.json"


class OpportunityStateError(RuntimeError):
    pass


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _monotonic_generated_at_utc(*, prior_ref: SurfaceRefV1 | None) -> str:
    current = datetime.now(UTC).replace(microsecond=0)
    if prior_ref is not None:
        prior_text = str(prior_ref.payload.get("generated_at_utc") or "").strip()
        if prior_text:
            try:
                prior_dt = datetime.fromisoformat(prior_text.replace("Z", "+00:00")).astimezone(UTC).replace(microsecond=0)
            except ValueError:
                prior_dt = None
            if prior_dt is not None and current <= prior_dt:
                current = prior_dt + timedelta(seconds=1)
    return current.isoformat().replace("+00:00", "Z")


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


def _read_canonical_surface(
    *,
    scope: Any,
    path: Path,
    schema_relpath: str,
    label: str,
) -> SurfaceRefV1:
    report: Dict[str, Any] = {"errors": [], "forbidden_path_hits": []}
    _check_path_boundary(
        report,
        scope=scope,
        label=label,
        path=path,
        expected_root_type="canonical_truth_root",
        reject_derived=False,
    )
    if report["errors"]:
        raise OpportunityStateError(str(report["errors"][0]))
    return read_validated_surface_v1(path=path, schema_relpath=schema_relpath)


def _opportunity_root(*, canonical_truth_root: Path, day_utc: str, scope_id: str) -> Path:
    return (canonical_truth_root / "reports" / ARTIFACT_ID / day_utc / scope_id).resolve()


def list_opportunity_states_v1(
    *,
    canonical_truth_root: Path | str,
    day_utc: str,
    scope_id: str | None = None,
) -> list[SurfaceRefV1]:
    root = (Path(canonical_truth_root).resolve() / "reports" / ARTIFACT_ID / day_utc).resolve()
    refs: list[SurfaceRefV1] = []
    if not root.exists():
        return refs
    pattern = "*/*/opportunity_state.v1.json" if not scope_id else f"{scope_id}/*/opportunity_state.v1.json"
    for path in sorted(root.glob(pattern)):
        refs.append(read_validated_surface_v1(path=path, schema_relpath=SCHEMA_RELPATH))
    refs.sort(
        key=lambda ref: (
            str(ref.payload.get("generated_at_utc") or ""),
            str(ref.payload.get("opportunity_state_id") or ""),
            str(ref.path),
        )
    )
    return refs


def find_latest_opportunity_state_v1(
    *,
    canonical_truth_root: Path | str,
    day_utc: str,
    scope_id: str | None = None,
    opportunity_id: str | None = None,
) -> SurfaceRefV1 | None:
    refs = list_opportunity_states_v1(canonical_truth_root=canonical_truth_root, day_utc=day_utc, scope_id=scope_id)
    if opportunity_id:
        refs = [
            ref for ref in refs if str(ref.payload.get("opportunity_id") or "").strip() == str(opportunity_id).strip()
        ]
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


def _latest_operator_status_ref(scope: Any) -> SurfaceRefV1 | None:
    path = (
        scope.canonical_truth_root
        / "reports"
        / "control_plane_operator_status_v1"
        / scope.day_utc
        / scope.scope_id
        / "control_plane_operator_status.v1.json"
    ).resolve()
    if not path.exists():
        return None
    return _read_canonical_surface(
        scope=scope,
        path=path,
        schema_relpath=OPERATOR_STATUS_SCHEMA,
        label="OPPORTUNITY_OPERATOR_STATUS",
    )


def _latest_timeline_ref(scope: Any) -> SurfaceRefV1 | None:
    path = (
        scope.canonical_truth_root
        / "reports"
        / "transition_timeline_projection_v1"
        / scope.day_utc
        / scope.scope_id
        / "transition_timeline_projection.v1.json"
    ).resolve()
    if not path.exists():
        return None
    return _read_canonical_surface(
        scope=scope,
        path=path,
        schema_relpath=TIMELINE_SCHEMA,
        label="OPPORTUNITY_TIMELINE",
    )


def _group_current_advisory_refs(
    *,
    canonical_truth_root: Path,
    day_utc: str,
    scope_id: str,
) -> list[SurfaceRefV1]:
    from constellation_2.common.advisory_decision_state_kernel_v1 import list_advisory_decision_states_v1

    refs = list_advisory_decision_states_v1(
        canonical_truth_root=canonical_truth_root,
        day_utc=day_utc,
        scope_id=scope_id,
    )
    by_item: dict[str, SurfaceRefV1] = {}
    for ref in refs:
        item_id = str(ref.payload.get("advisory_item_id") or "").strip()
        if not item_id:
            continue
        latest = by_item.get(item_id)
        if latest is None or (
            str(ref.payload.get("generated_at_utc") or ""),
            str(ref.payload.get("decision_id") or ""),
            str(ref.path),
        ) >= (
            str(latest.payload.get("generated_at_utc") or ""),
            str(latest.payload.get("decision_id") or ""),
            str(latest.path),
        ):
            by_item[item_id] = ref
    return [by_item[key] for key in sorted(by_item)]


def _load_optional_scenario_ref(*, scope: Any, path: Path | None) -> SurfaceRefV1 | None:
    if path is None:
        return None
    return _read_canonical_surface(
        scope=scope,
        path=path.resolve(),
        schema_relpath=STRESS_CASE_SCHEMA,
        label="OPPORTUNITY_SCENARIO_INPUT",
    )


def _candidate_tax_opportunity(
    *,
    tax_ref: SurfaceRefV1,
) -> dict[str, Any] | None:
    payload = tax_ref.payload
    blocker_states = [str(item).strip() for item in (payload.get("blocker_states") or []) if str(item).strip()]
    opportunity_states = [str(item).strip() for item in (payload.get("opportunity_states") or []) if str(item).strip()]
    if not blocker_states and not opportunity_states:
        return None
    return {
        "opportunity_id": canonical_hash_for_c2_artifact_v1({"type": "tax_harvest_review", "scope_id": payload.get("scope_id")}),
        "opportunity_type": "tax_harvest_review",
        "freshness_state": str(payload.get("freshness_state") or "unknown"),
        "blocker_states": blocker_states,
        "base_review_priority": "review_now" if opportunity_states else "monitor_only",
        "degraded_upstream_truth": str(payload.get("completeness_state") or "") != "complete",
        "release_blocked": False,
        "tax_effect_state": str(((payload.get("advisory_binding_state") or {}).get("effect_state")) or "clear"),
        "advisory_blocked": False,
        "scenario_significance_state": "not_evaluated",
        "detail_fields": {
            "opportunity_states": opportunity_states,
            "tax_state_id": str(payload.get("tax_state_id") or ""),
            "primary_rule_id": str(payload.get("primary_rule_id") or ""),
        },
        "governing_tax_refs": [_plain_ref_from_surface(tax_ref, "tax_state_v1")],
        "governing_advisory_refs": [],
        "governing_trust_refs": [],
        "governing_readiness_refs": [],
        "governing_scenario_refs": [],
        "governing_portfolio_refs": [],
    }


def _candidate_advisory_opportunities(
    *,
    advisory_refs: Sequence[SurfaceRefV1],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for ref in advisory_refs:
        payload = ref.payload
        decision_state = str(payload.get("decision_state") or "")
        if decision_state not in {"actionable", "promotion_eligible", "blocked", "stale"}:
            continue
        rows.append(
            {
                "opportunity_id": canonical_hash_for_c2_artifact_v1(
                    {"type": "advisory_action_review", "advisory_item_id": payload.get("advisory_item_id")}
                ),
                "opportunity_type": "advisory_action_review",
                "freshness_state": str(payload.get("freshness_state") or "unknown"),
                "blocker_states": [decision_state == "blocked" and str(payload.get("invalidation_rule_id") or "advisory_blocked") or ""],
                "base_review_priority": "review_now" if decision_state in {"actionable", "promotion_eligible", "blocked"} else "monitor_only",
                "degraded_upstream_truth": decision_state in {"uncertified", "incomplete_basis"},
                "release_blocked": False,
                "tax_effect_state": str(((payload.get("tax_binding_state") or {}).get("effect_state")) or "clear"),
                "advisory_blocked": decision_state == "blocked",
                "scenario_significance_state": "not_evaluated",
                "detail_fields": {
                    "advisory_item_id": str(payload.get("advisory_item_id") or ""),
                    "decision_state": decision_state,
                    "actionability_state": str(payload.get("actionability_state") or ""),
                    "invalidation_rule_id": str(payload.get("invalidation_rule_id") or ""),
                },
                "governing_tax_refs": [dict(row) for row in (payload.get("governing_tax_refs") or []) if isinstance(row, Mapping)],
                "governing_advisory_refs": [_plain_ref_from_surface(ref, "advisory_decision_state_v1")],
                "governing_trust_refs": [],
                "governing_readiness_refs": [dict(row) for row in (payload.get("governing_release_refs") or []) if isinstance(row, Mapping)],
                "governing_scenario_refs": [],
                "governing_portfolio_refs": [],
            }
        )
    return rows


def _candidate_platform_opportunity(
    *,
    readiness: Mapping[str, Any],
    operator_ref: SurfaceRefV1 | None,
    timeline_ref: SurfaceRefV1 | None,
) -> dict[str, Any] | None:
    blocked = not bool(readiness.get("ok"))
    operator_status = str(((operator_ref.payload if operator_ref is not None else {}).get("current_operator_status")) or "")
    if not blocked and operator_status not in {"BLOCKED", "PARTIAL", "UNKNOWN"}:
        return None
    blocker_states = list(readiness.get("blocked_state", {}).get("blocking_errors") or [])
    if operator_status and operator_status not in {"CERTIFIED_READY", ""}:
        blocker_states.append(f"operator_status:{operator_status}")
    refs = []
    if operator_ref is not None:
        refs.append(_plain_ref_from_surface(operator_ref, "control_plane_operator_status_v1"))
    if timeline_ref is not None:
        refs.append(_plain_ref_from_surface(timeline_ref, "transition_timeline_projection_v1"))
    return {
        "opportunity_id": canonical_hash_for_c2_artifact_v1({"type": "platform_blocker_review", "scope_id": readiness["scope"]["ib_account"]}),
        "opportunity_type": "platform_blocker_review",
        "freshness_state": "fresh",
        "blocker_states": blocker_states,
        "base_review_priority": "review_now",
        "degraded_upstream_truth": False,
        "release_blocked": blocked,
        "tax_effect_state": "clear",
        "advisory_blocked": True,
        "scenario_significance_state": "not_evaluated",
        "detail_fields": {
            "blocking_errors": list(readiness.get("blocked_state", {}).get("blocking_errors") or []),
            "operator_status": operator_status or "UNKNOWN",
        },
        "governing_tax_refs": [],
        "governing_advisory_refs": [],
        "governing_trust_refs": refs,
        "governing_readiness_refs": [],
        "governing_scenario_refs": [],
        "governing_portfolio_refs": [],
    }


def _candidate_scenario_opportunity(
    *,
    scenario_ref: SurfaceRefV1 | None,
) -> dict[str, Any] | None:
    if scenario_ref is None:
        return None
    significance = evaluate_opportunity_scenario_significance_v1(scenario_payload=scenario_ref.payload)
    if significance["state"] == "basis_unavailable":
        return {
            "opportunity_id": canonical_hash_for_c2_artifact_v1({"type": "scenario_review", "run_id": scenario_ref.payload.get("run_id")}),
            "opportunity_type": "scenario_review",
            "freshness_state": "fresh",
            "blocker_states": ["scenario_basis_unavailable"],
            "base_review_priority": "monitor_only",
            "degraded_upstream_truth": True,
            "release_blocked": False,
            "tax_effect_state": "clear",
            "advisory_blocked": False,
            "scenario_significance_state": significance["state"],
            "detail_fields": {
                "run_id": str(scenario_ref.payload.get("run_id") or ""),
                "reason_id": str(significance["reason_id"]),
            },
            "governing_tax_refs": [],
            "governing_advisory_refs": [],
            "governing_trust_refs": [],
            "governing_readiness_refs": [],
            "governing_scenario_refs": [_plain_ref_from_surface(scenario_ref, "stress_case_view_v1")],
            "governing_portfolio_refs": [],
        }
    if significance["state"] == "not_evaluated":
        return None
    return {
        "opportunity_id": canonical_hash_for_c2_artifact_v1({"type": "scenario_review", "run_id": scenario_ref.payload.get("run_id")}),
        "opportunity_type": "scenario_review",
        "freshness_state": "fresh",
        "blocker_states": [],
        "base_review_priority": "review_now" if significance["state"] == "review_now" else "monitor_only",
        "degraded_upstream_truth": False,
        "release_blocked": False,
        "tax_effect_state": "clear",
        "advisory_blocked": False,
        "scenario_significance_state": str(significance["state"]),
        "detail_fields": {
            "run_id": str(scenario_ref.payload.get("run_id") or ""),
            "scenario_ids": list(scenario_ref.payload.get("scenario_ids") or []),
            "summary": str(scenario_ref.payload.get("summary") or ""),
        },
        "governing_tax_refs": [],
        "governing_advisory_refs": [],
        "governing_trust_refs": [],
        "governing_readiness_refs": [],
        "governing_scenario_refs": [_plain_ref_from_surface(scenario_ref, "stress_case_view_v1")],
        "governing_portfolio_refs": [],
    }


def _ordered_candidates(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        (dict(row) for row in rows if row),
        key=lambda item: (
            str(item.get("opportunity_type") or ""),
            str(item.get("opportunity_id") or ""),
        ),
    )


def _semantic_event(
    *,
    event_type: str,
    authority_label: str,
    opportunity_type: str,
    opportunity_state: str,
    review_priority: str,
    blocker_states: Sequence[str],
    delta_state: str,
    scenario_significance_state: str,
    governing_refs: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    return {
        "event_type": event_type,
        "authority_label": authority_label,
        "opportunity_type": opportunity_type,
        "opportunity_state": opportunity_state,
        "review_priority": review_priority,
        "blocker_states": [str(item).strip() for item in blocker_states if str(item).strip()],
        "delta_state": delta_state,
        "scenario_significance_state": scenario_significance_state,
        "governing_refs": [dict(row) for row in governing_refs],
        "kernel_version": KERNEL_VERSION,
    }


def materialize_opportunity_state_v1(
    *,
    day_utc: str,
    sleeve_id: str,
    environment: str,
    ib_account: str,
    operation_type: str = "fresh_paper_entry_v1",
    canonical_truth_root: Path | str | None = None,
    truth_sleeves_root: Path | str | None = None,
    tax_state_path: Path | str | None = None,
    stress_case_view_path: Path | str | None = None,
    emit_artifacts: bool = False,
) -> Dict[str, Any]:
    scope = _scope(
        day_utc=day_utc,
        sleeve_id=sleeve_id,
        environment=environment,
        ib_account=ib_account,
        operation_type=operation_type,
        canonical_truth_root=canonical_truth_root,
        truth_sleeves_root=truth_sleeves_root,
    )
    tax_ref = (
        _read_canonical_surface(
            scope=scope,
            path=Path(tax_state_path).resolve(),
            schema_relpath=TAX_SCHEMA,
            label="OPPORTUNITY_TAX_INPUT",
        )
        if tax_state_path is not None and str(tax_state_path).strip()
        else find_latest_tax_state_v1(
            canonical_truth_root=scope.canonical_truth_root,
            day_utc=scope.day_utc,
            scope_id=scope.scope_id,
        )
    )
    advisory_refs = _group_current_advisory_refs(
        canonical_truth_root=scope.canonical_truth_root,
        day_utc=scope.day_utc,
        scope_id=scope.scope_id,
    )
    readiness = build_certified_operational_readiness_v1(
        day_utc=scope.day_utc,
        sleeve_id=scope.sleeve_id,
        environment=scope.environment,
        ib_account=scope.ib_account,
        operation_type=scope.operation_type,
        canonical_truth_root=scope.canonical_truth_root,
        truth_sleeves_root=scope.truth_sleeves_root,
    )
    operator_ref = _latest_operator_status_ref(scope)
    timeline_ref = _latest_timeline_ref(scope)
    scenario_ref = _load_optional_scenario_ref(
        scope=scope,
        path=Path(stress_case_view_path).resolve() if stress_case_view_path is not None and str(stress_case_view_path).strip() else None,
    )

    candidates = _ordered_candidates(
        [
            _candidate_tax_opportunity(tax_ref=tax_ref) if tax_ref is not None else None,
            *_candidate_advisory_opportunities(advisory_refs=advisory_refs),
            _candidate_platform_opportunity(readiness=readiness, operator_ref=operator_ref, timeline_ref=timeline_ref),
            _candidate_scenario_opportunity(scenario_ref=scenario_ref),
        ]
    )

    prior_snapshot_ref = find_latest_opportunity_review_snapshot_v1(
        canonical_truth_root=scope.canonical_truth_root,
        scope_id=scope.scope_id,
        before_day_utc=scope.day_utc,
    )
    prior_rows = list((prior_snapshot_ref.payload if prior_snapshot_ref is not None else {}).get("opportunity_rows") or [])
    prior_by_id = {str(row.get("opportunity_id") or ""): dict(row) for row in prior_rows if str(row.get("opportunity_id") or "")}

    authority_label = "governed_certified_opportunity"
    opportunity_payloads: list[dict[str, Any]] = []
    snapshot_rows: list[dict[str, Any]] = []
    for candidate in candidates:
        delta_state = classify_delta_state_v1(
            prior_row=prior_by_id.get(str(candidate["opportunity_id"])),
            current_row=candidate,
        )
        matrix = evaluate_opportunity_state_precedence_v1(
            freshness_state=str(candidate["freshness_state"]),
            degraded_upstream_truth=bool(candidate["degraded_upstream_truth"]),
            release_blocked=bool(candidate["release_blocked"]),
            tax_effect_state=str(candidate["tax_effect_state"]),
            advisory_blocked=bool(candidate["advisory_blocked"]),
            base_review_priority=str(candidate["base_review_priority"]),
            blocker_states=list(candidate["blocker_states"]),
            delta_state=delta_state,
            scenario_significance_state=str(candidate["scenario_significance_state"]),
        )
        opportunity_state_id = canonical_hash_for_c2_artifact_v1(
            {
                "artifact_id": ARTIFACT_ID,
                "opportunity_id": candidate["opportunity_id"],
                "scope_id": scope.scope_id,
                "day_utc": scope.day_utc,
                "primary_rule_id": matrix["primary_rule_id"],
                "delta_state": delta_state,
                "governing_refs": {
                    "advisory": candidate["governing_advisory_refs"],
                    "tax": candidate["governing_tax_refs"],
                    "trust": candidate["governing_trust_refs"],
                    "readiness": candidate["governing_readiness_refs"],
                    "scenario": candidate["governing_scenario_refs"],
                    "portfolio": candidate["governing_portfolio_refs"],
                },
            }
        )
        evidence_refs = [
            *candidate["governing_advisory_refs"],
            *candidate["governing_tax_refs"],
            *candidate["governing_trust_refs"],
            *candidate["governing_readiness_refs"],
            *candidate["governing_scenario_refs"],
            *candidate["governing_portfolio_refs"],
        ]
        explanation = map_opportunity_explanation_v1(
            opportunity_type=str(candidate["opportunity_type"]),
            opportunity_state=str(matrix["opportunity_state"]),
            actionability_state=str(matrix["actionability_state"]),
            review_priority=str(matrix["review_priority"]),
            blocker_states=list(matrix["effective_blocker_states"]),
            delta_state=delta_state,
            scenario_significance_state=str(candidate["scenario_significance_state"]),
            freshness_state=str(candidate["freshness_state"]),
            visibility_state=str(matrix["visibility_state"]),
            reason_id=str(matrix["primary_rule_id"]),
            evidence_refs=evidence_refs,
            authority_label=authority_label,
            detail_fields=dict(candidate["detail_fields"]),
        )
        prior_opportunity_ref = find_latest_opportunity_state_v1(
            canonical_truth_root=scope.canonical_truth_root,
            day_utc=scope.day_utc,
            scope_id=scope.scope_id,
            opportunity_id=str(candidate["opportunity_id"]),
        )
        generated_at_utc = _monotonic_generated_at_utc(prior_ref=prior_opportunity_ref)
        supersedes_ref = None
        if prior_opportunity_ref is not None and str(prior_opportunity_ref.payload.get("opportunity_state_id") or "") != opportunity_state_id:
            supersedes_ref = _plain_ref_from_surface(prior_opportunity_ref, ARTIFACT_ID)
        payload = {
            "schema_id": "opportunity_state",
            "schema_version": "v1",
            "artifact_id": ARTIFACT_ID,
            "surface_kind": "projection",
            "opportunity_id": str(candidate["opportunity_id"]),
            "opportunity_state_id": opportunity_state_id,
            "kernel_version": KERNEL_VERSION,
            "explanation_mapping_version": EXPLANATION_MAPPING_VERSION,
            "generated_at_utc": generated_at_utc,
            "authority_label": authority_label,
            "target_day": scope.day_utc,
            "scope_id": scope.scope_id,
            "operation_type": scope.operation_type,
            "sleeve_id": scope.sleeve_id,
            "environment": scope.environment,
            "ib_account": scope.ib_account,
            "opportunity_type": str(candidate["opportunity_type"]),
            "opportunity_state": str(matrix["opportunity_state"]),
            "actionability_state": str(matrix["actionability_state"]),
            "review_priority": str(matrix["review_priority"]),
            "freshness_state": str(candidate["freshness_state"]),
            "visibility_state": str(matrix["visibility_state"]),
            "blocker_states": list(matrix["effective_blocker_states"]),
            "delta_state": delta_state,
            "scenario_significance_state": str(candidate["scenario_significance_state"]),
            "advisory_binding_state": {
                "effect_state": str(matrix["advisory_effect_state"]),
                "opportunity_state": str(matrix["opportunity_state"]),
                "review_priority": str(matrix["review_priority"]),
                "visibility_state": str(matrix["visibility_state"]),
                "blocker_states": list(matrix["effective_blocker_states"]),
                "delta_state": delta_state,
                "scenario_significance_state": str(candidate["scenario_significance_state"]),
                "binding_reason_id": str(matrix["primary_rule_id"]),
            },
            "primary_rule_id": str(matrix["primary_rule_id"]),
            "primary_explanation": explanation,
            "historical_visibility": dict(matrix["historical_visibility"]),
            "governing_advisory_refs": [dict(row) for row in candidate["governing_advisory_refs"]],
            "governing_tax_refs": [dict(row) for row in candidate["governing_tax_refs"]],
            "governing_trust_refs": [dict(row) for row in candidate["governing_trust_refs"]],
            "governing_readiness_refs": [dict(row) for row in candidate["governing_readiness_refs"]],
            "governing_scenario_refs": [dict(row) for row in candidate["governing_scenario_refs"]],
            "governing_portfolio_refs": [dict(row) for row in candidate["governing_portfolio_refs"]],
            "evidence_refs": [dict(row) for row in evidence_refs],
            "semantic_events": [
                _semantic_event(
                    event_type="opportunity_state_computed",
                    authority_label=authority_label,
                    opportunity_type=str(candidate["opportunity_type"]),
                    opportunity_state=str(matrix["opportunity_state"]),
                    review_priority=str(matrix["review_priority"]),
                    blocker_states=list(matrix["effective_blocker_states"]),
                    delta_state=delta_state,
                    scenario_significance_state=str(candidate["scenario_significance_state"]),
                    governing_refs=evidence_refs,
                )
            ],
        }
        if prior_snapshot_ref is not None:
            payload["prior_review_snapshot_ref"] = _plain_ref_from_surface(prior_snapshot_ref, SNAPSHOT_ARTIFACT_ID)
        if supersedes_ref is not None:
            payload["supersedes_ref"] = supersedes_ref
        if str(matrix["review_priority"]) != str(candidate["base_review_priority"]):
            payload["semantic_events"].append(
                _semantic_event(
                    event_type="opportunity_priority_changed",
                    authority_label=authority_label,
                    opportunity_type=str(candidate["opportunity_type"]),
                    opportunity_state=str(matrix["opportunity_state"]),
                    review_priority=str(matrix["review_priority"]),
                    blocker_states=list(matrix["effective_blocker_states"]),
                    delta_state=delta_state,
                    scenario_significance_state=str(candidate["scenario_significance_state"]),
                    governing_refs=evidence_refs,
                )
            )
        if str(matrix["opportunity_state"]) == "blocked":
            payload["semantic_events"].append(
                _semantic_event(
                    event_type="opportunity_blocked",
                    authority_label=authority_label,
                    opportunity_type=str(candidate["opportunity_type"]),
                    opportunity_state=str(matrix["opportunity_state"]),
                    review_priority=str(matrix["review_priority"]),
                    blocker_states=list(matrix["effective_blocker_states"]),
                    delta_state=delta_state,
                    scenario_significance_state=str(candidate["scenario_significance_state"]),
                    governing_refs=evidence_refs,
                )
            )
        if str(matrix["visibility_state"]) == "historical_only":
            payload["semantic_events"].append(
                _semantic_event(
                    event_type="opportunity_historical_only",
                    authority_label=authority_label,
                    opportunity_type=str(candidate["opportunity_type"]),
                    opportunity_state=str(matrix["opportunity_state"]),
                    review_priority=str(matrix["review_priority"]),
                    blocker_states=list(matrix["effective_blocker_states"]),
                    delta_state=delta_state,
                    scenario_significance_state=str(candidate["scenario_significance_state"]),
                    governing_refs=evidence_refs,
                )
            )
        if str(candidate["scenario_significance_state"]) == "review_now":
            payload["semantic_events"].append(
                _semantic_event(
                    event_type="scenario_significance_raised",
                    authority_label=authority_label,
                    opportunity_type=str(candidate["opportunity_type"]),
                    opportunity_state=str(matrix["opportunity_state"]),
                    review_priority=str(matrix["review_priority"]),
                    blocker_states=list(matrix["effective_blocker_states"]),
                    delta_state=delta_state,
                    scenario_significance_state=str(candidate["scenario_significance_state"]),
                    governing_refs=evidence_refs,
                )
            )
        if str(matrix["advisory_effect_state"]) in {"downgrade", "block"}:
            payload["semantic_events"].append(
                _semantic_event(
                    event_type="advisory_downgraded_due_to_opportunity",
                    authority_label=authority_label,
                    opportunity_type=str(candidate["opportunity_type"]),
                    opportunity_state=str(matrix["opportunity_state"]),
                    review_priority=str(matrix["review_priority"]),
                    blocker_states=list(matrix["effective_blocker_states"]),
                    delta_state=delta_state,
                    scenario_significance_state=str(candidate["scenario_significance_state"]),
                    governing_refs=evidence_refs,
                )
            )
        opportunity_payloads.append(payload)
        snapshot_rows.append(
            {
                "opportunity_id": str(candidate["opportunity_id"]),
                "opportunity_type": str(candidate["opportunity_type"]),
                "opportunity_state": str(matrix["opportunity_state"]),
                "review_priority": str(matrix["review_priority"]),
                "delta_state": delta_state,
                "summary_fingerprint": opportunity_summary_fingerprint_v1(
                    {
                        "opportunity_id": candidate["opportunity_id"],
                        "opportunity_type": candidate["opportunity_type"],
                        "opportunity_state": matrix["opportunity_state"],
                        "review_priority": matrix["review_priority"],
                        "blocker_states": matrix["effective_blocker_states"],
                        "scenario_significance_state": candidate["scenario_significance_state"],
                    }
                ),
            }
        )

    current_ids = {str(row["opportunity_id"]) for row in snapshot_rows}
    resolved_rows = resolved_rows_v1(prior_rows=prior_rows, current_ids=current_ids)
    snapshot_id = canonical_hash_for_c2_artifact_v1(
        {
            "artifact_id": SNAPSHOT_ARTIFACT_ID,
            "scope_id": scope.scope_id,
            "target_day": scope.day_utc,
            "rows": snapshot_rows,
            "prior_review_snapshot_ref": (
                _plain_ref_from_surface(prior_snapshot_ref, SNAPSHOT_ARTIFACT_ID) if prior_snapshot_ref is not None else None
            ),
        }
    )
    snapshot_payload = {
        "schema_id": "opportunity_review_snapshot",
        "schema_version": "v1",
        "artifact_id": SNAPSHOT_ARTIFACT_ID,
        "surface_kind": "projection",
        "review_snapshot_id": snapshot_id,
        "kernel_version": KERNEL_VERSION,
        "generated_at_utc": _utc_now(),
        "authority_label": "governed_certified_opportunity_review",
        "target_day": scope.day_utc,
        "scope_id": scope.scope_id,
        "opportunity_rows": snapshot_rows,
        "resolved_rows": resolved_rows,
        "summary": {
            "current_count": len(snapshot_rows),
            "new_count": sum(1 for row in snapshot_rows if row["delta_state"] == "new"),
            "changed_count": sum(1 for row in snapshot_rows if row["delta_state"] == "changed"),
            "resolved_count": len(resolved_rows),
            "review_now_count": sum(1 for row in snapshot_rows if row["review_priority"] == "review_now"),
        },
    }
    if prior_snapshot_ref is not None:
        snapshot_payload["prior_review_snapshot_ref"] = _plain_ref_from_surface(prior_snapshot_ref, SNAPSHOT_ARTIFACT_ID)

    emitted_snapshot_ref = None
    if emit_artifacts:
        assert_constitutional_writer_allowed_v1(REPO_ROOT, SNAPSHOT_ARTIFACT_ID, WRITER_ID)
        snapshot_path = resolve_constitutional_artifact_path_v1(
            repo_root=REPO_ROOT,
            artifact_id=SNAPSHOT_ARTIFACT_ID,
            day_utc=scope.day_utc,
            canonical_truth_root=scope.canonical_truth_root,
            extra_variables={"scope_id": scope.scope_id, "review_snapshot_id": snapshot_id},
        )
        emitted_snapshot_ref = atomic_write_idempotent_validated_json_v1(
            path=snapshot_path,
            payload=snapshot_payload,
            schema_relpath=SNAPSHOT_SCHEMA_RELPATH,
            volatile_field_names=("generated_at_utc",),
        )

    emitted_refs: list[dict[str, str]] = []
    for payload in opportunity_payloads:
        if emitted_snapshot_ref is not None:
            payload["review_snapshot_ref"] = _plain_ref_from_surface(emitted_snapshot_ref, SNAPSHOT_ARTIFACT_ID)
        if emit_artifacts:
            assert_constitutional_writer_allowed_v1(REPO_ROOT, ARTIFACT_ID, WRITER_ID)
            out_path = resolve_constitutional_artifact_path_v1(
                repo_root=REPO_ROOT,
                artifact_id=ARTIFACT_ID,
                day_utc=scope.day_utc,
                canonical_truth_root=scope.canonical_truth_root,
                extra_variables={"scope_id": scope.scope_id, "opportunity_state_id": payload["opportunity_state_id"]},
            )
            emitted = atomic_write_idempotent_validated_json_v1(
                path=out_path,
                payload=payload,
                schema_relpath=SCHEMA_RELPATH,
                volatile_field_names=("generated_at_utc",),
            )
            emitted_refs.append(_plain_ref_from_surface(emitted, ARTIFACT_ID))

    return {
        "ok": True,
        "artifact_id": ARTIFACT_ID,
        "opportunities": opportunity_payloads,
        "review_snapshot": snapshot_payload,
        "artifact_refs": emitted_refs,
        "review_snapshot_ref": _plain_ref_from_surface(emitted_snapshot_ref, SNAPSHOT_ARTIFACT_ID) if emitted_snapshot_ref is not None else None,
    }
