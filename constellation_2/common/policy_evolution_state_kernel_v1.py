from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
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
from constellation_2.common.policy_evidence_window_threshold_v1 import (
    THRESHOLD_MODEL_VERSION,
    evaluate_policy_evidence_window_threshold_v1,
)
from constellation_2.common.policy_evolution_action_model_v1 import (
    ACTION_MODEL_VERSION,
    build_policy_evolution_action_v1,
)
from constellation_2.common.product_snapshot_v1 import find_latest_product_snapshot_v1
from constellation_2.common.product_summary_kernel_v1 import find_latest_product_summary_v1
from constellation_2.common.refinement_state_kernel_v1 import list_refinement_states_v1
from constellation_2.common.value_state_kernel_v1 import list_value_states_v1
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
WRITER_ID = "constellation_2.common.policy_evolution_state_kernel_v1"
KERNEL_VERSION = WRITER_ID
ARTIFACT_ID = "policy_evolution_state_v1"
SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/RUNTIME/policy_evolution_state.v1.schema.json"


class PolicyEvolutionStateError(RuntimeError):
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
        raise PolicyEvolutionStateError(f"POLICY_EVOLUTION_STATE_FORBIDDEN_TRUTH_ROOT:{resolved}")


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


def _parse_day(value: str) -> date:
    return datetime.strptime(str(value), "%Y-%m-%d").date()


def _candidate_days(day_utc: str, days: int) -> list[str]:
    target = _parse_day(day_utc)
    return sorted((target - timedelta(days=offset)).isoformat() for offset in range(days - 1, -1, -1))


def list_policy_evolution_states_v1(
    *,
    canonical_truth_root: Path | str,
    day_utc: str | None = None,
    scope_id: str | None = None,
    before_day_utc: str | None = None,
) -> list[SurfaceRefV1]:
    root = (Path(canonical_truth_root).resolve() / "reports" / ARTIFACT_ID).resolve()
    refs: list[SurfaceRefV1] = []
    if not root.exists():
        return refs
    if day_utc:
        day_roots = [root / str(day_utc)]
    else:
        day_roots = sorted(path for path in root.iterdir() if path.is_dir())
    for day_root in day_roots:
        if not day_root.exists():
            continue
        pattern = "*/*/policy_evolution_state.v1.json" if not scope_id else f"{scope_id}/*/policy_evolution_state.v1.json"
        for path in sorted(day_root.glob(pattern)):
            refs.append(read_validated_surface_v1(path=path, schema_relpath=SCHEMA_RELPATH))
    if before_day_utc:
        refs = [ref for ref in refs if str(ref.payload.get("target_day") or "") < str(before_day_utc)]
    refs.sort(
        key=lambda ref: (
            str(ref.payload.get("generated_at_utc") or ""),
            str(ref.payload.get("evolution_id") or ""),
            str(ref.path),
        )
    )
    return refs


def _find_latest_policy_for_target(
    *,
    canonical_truth_root: Path,
    scope_id: str,
    evolution_target: str,
    before_day_utc: str | None = None,
) -> SurfaceRefV1 | None:
    refs = [
        ref
        for ref in list_policy_evolution_states_v1(
            canonical_truth_root=canonical_truth_root,
            scope_id=scope_id,
            before_day_utc=before_day_utc,
        )
        if str(ref.payload.get("evolution_target") or "") == evolution_target
    ]
    if not refs:
        return None
    return _current_refs(refs)[-1]


def _history_for_target(
    *,
    canonical_truth_root: Path,
    scope_id: str,
    evolution_target: str,
    until_day_utc: str,
    window_days: int,
) -> list[SurfaceRefV1]:
    refs: list[SurfaceRefV1] = []
    for candidate_day in _candidate_days(until_day_utc, window_days):
        day_refs = [
            ref
            for ref in list_refinement_states_v1(canonical_truth_root=canonical_truth_root, day_utc=candidate_day, scope_id=scope_id)
            if str(ref.payload.get("refinement_target") or "") == evolution_target
        ]
        refs.extend(_current_refs(day_refs))
    refs.sort(
        key=lambda ref: (
            str(ref.payload.get("target_day") or ""),
            str(ref.payload.get("generated_at_utc") or ""),
            str(ref.path),
        )
    )
    return refs


def _dedupe_refs(refs: Iterable[dict[str, str]]) -> list[dict[str, str]]:
    deduped: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for ref in refs:
        artifact_id = str(ref.get("artifact_id") or "").strip()
        artifact_path = str(ref.get("artifact_path") or "").strip()
        key = (artifact_id, artifact_path)
        if not artifact_id or not artifact_path or key in seen:
            continue
        seen.add(key)
        deduped.append(dict(ref))
    return deduped


def _support_category(refinement_payload: dict[str, Any]) -> str:
    action = str(refinement_payload.get("refinement_action") or "")
    protected = list(refinement_payload.get("protected_distinctions") or [])
    if action == "compress_summary":
        return "compress"
    if action in {"demote_to_secondary", "preserve_drilldown_only"}:
        return "reduce"
    if action == "preserve_top_level" and protected:
        return "strengthen"
    if action == "refinement_withheld":
        return "withheld"
    return "preserve"


def _trust_override_state(
    *,
    protected_flags: list[str],
    support_counts: dict[str, int],
    prior_policy_ref: SurfaceRefV1 | None,
) -> str:
    trust_critical_flags = [flag for flag in protected_flags if flag != "preserved_drilldown_access"]
    if not trust_critical_flags:
        return "none"
    prior_action = ""
    if prior_policy_ref is not None:
        prior_action = str((prior_policy_ref.payload.get("proposed_policy_change") or {}).get("action") or "")
    if prior_action in {"propose_reduce_emphasis", "propose_more_compression"}:
        return "rollback_required"
    if support_counts.get("reduce", 0) >= 2:
        return "block_reduce_emphasis"
    if support_counts.get("compress", 0) >= 2:
        return "block_more_compression"
    return "none"


def _dominant_support(support_counts: dict[str, int]) -> str:
    ranked = [(name, count) for name, count in support_counts.items() if count > 0]
    if not ranked:
        return ""
    ranked.sort(key=lambda item: (-item[1], item[0]))
    if len(ranked) > 1 and ranked[0][1] == ranked[1][1]:
        return ""
    return ranked[0][0] if ranked[0][1] >= 2 else ""


def _sanitize_readiness_summary(readiness: dict[str, Any]) -> dict[str, Any]:
    return {
        "blocked": bool(readiness.get("blocked")),
        "blocked_state": str(readiness.get("blocked_state") or "READY"),
        "operator_status": str(readiness.get("operator_status") or "UNKNOWN"),
    }


def _candidate_from_refinement(
    *,
    day_utc: str,
    scope_id: str,
    refinement_ref: SurfaceRefV1,
    summary_ref: SurfaceRefV1,
    snapshot_ref: SurfaceRefV1,
    prior_policy_ref: SurfaceRefV1 | None,
    stability_history: list[SurfaceRefV1],
    short_history: list[SurfaceRefV1],
) -> dict[str, Any]:
    payload = refinement_ref.payload
    support_counts = {"strengthen": 0, "reduce": 0, "compress": 0}
    categories_seen: set[str] = set()
    for ref in stability_history:
        category = _support_category(dict(ref.payload))
        if category in support_counts:
            support_counts[category] += 1
            categories_seen.add(category)
    protected_flags = [str(flag) for flag in payload.get("protected_distinctions") or []]
    if payload.get("preserved_drilldown_refs"):
        protected_flags.append("preserved_drilldown_access")
    protected_flags = sorted({flag for flag in protected_flags if flag})
    trust_override_state = _trust_override_state(
        protected_flags=protected_flags,
        support_counts=support_counts,
        prior_policy_ref=prior_policy_ref,
    )
    dominant_support = _dominant_support(support_counts)
    insufficient_history = len(short_history) < 2 or len(stability_history) < 2
    unstable_history = (
        not insufficient_history
        and trust_override_state == "none"
        and (
            len(categories_seen) > 1
            or (not dominant_support and any(support_counts.values()))
        )
    )
    prior_policy_expired = False
    if prior_policy_ref is not None:
        prior_action = str((prior_policy_ref.payload.get("proposed_policy_change") or {}).get("action") or "")
        prior_day = str(prior_policy_ref.payload.get("target_day") or "")
        if prior_action in {"propose_strengthen_emphasis", "propose_reduce_emphasis", "propose_more_compression"}:
            prior_policy_expired = (_parse_day(day_utc) - _parse_day(prior_day)).days >= 3 and not dominant_support
    evidence_window_refs = [
        _plain_ref_from_surface(summary_ref, "product_summary_v1"),
        _plain_ref_from_surface(snapshot_ref, "product_snapshot_v1"),
    ]
    evidence_window_refs.extend(_plain_ref_from_surface(ref, "refinement_state_v1") for ref in stability_history)
    if prior_policy_ref is not None:
        evidence_window_refs.append(_plain_ref_from_surface(prior_policy_ref, ARTIFACT_ID))
    current_message = str(payload.get("summary_message") or payload.get("target_label") or "Policy target")
    current_visibility = str(payload.get("visibility_effect") or "top_level")
    current_action = str(payload.get("refinement_action") or "preserve_top_level")
    return {
        "evolution_target": str(payload.get("refinement_target") or ""),
        "target_policy_scope": "command_home_policy"
        if str(payload.get("target_scope") or "") == "command_home"
        else "refinement_policy",
        "target_label": str(payload.get("target_label") or payload.get("refinement_target") or "policy_target"),
        "drill_down_route": str(payload.get("drill_down_route") or "/"),
        "current_refinement_action": current_action,
        "current_visibility_effect": current_visibility,
        "current_summary_message": current_message,
        "evidence_basis_refs": _dedupe_refs(payload.get("evidence_basis_refs") or []),
        "evidence_window_refs": _dedupe_refs(evidence_window_refs),
        "protected_flags": protected_flags,
        "trust_override_state": trust_override_state,
        "insufficient_history": insufficient_history,
        "unstable_history": unstable_history,
        "dominant_support": dominant_support,
        "prior_policy_expired": prior_policy_expired,
        "prior_policy_ref": prior_policy_ref,
        "before_policy_state": {
            "policy_action": (
                str((prior_policy_ref.payload.get("proposed_policy_change") or {}).get("action") or "")
                if prior_policy_ref is not None
                else current_action
            ),
            "visibility_effect": (
                str((prior_policy_ref.payload.get("proposed_policy_change") or {}).get("effective_visibility_effect") or "")
                if prior_policy_ref is not None
                else current_visibility
            )
            or current_visibility,
            "summary_message": (
                str(prior_policy_ref.payload.get("summary_message") or "")
                if prior_policy_ref is not None
                else current_message
            )
            or current_message,
        },
        "readiness_summary": _sanitize_readiness_summary(dict(summary_ref.payload.get("readiness_summary") or {})),
    }


def _write_policy_evolution(payload: dict[str, Any], scope: Any) -> SurfaceRefV1:
    assert_constitutional_writer_allowed_v1(repo_root=REPO_ROOT, artifact_id=ARTIFACT_ID, writer_id=WRITER_ID)
    output_path = resolve_constitutional_artifact_path_v1(
        repo_root=REPO_ROOT,
        artifact_id=ARTIFACT_ID,
        day_utc=scope.day_utc,
        canonical_truth_root=scope.canonical_truth_root,
        extra_variables={"scope_id": scope.scope_id, "evolution_id": payload["evolution_id"]},
    )
    return atomic_write_idempotent_validated_json_v1(
        path=output_path,
        payload=payload,
        schema_relpath=SCHEMA_RELPATH,
        volatile_field_names=("generated_at_utc",),
    )


def materialize_policy_evolution_state_v1(
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
        raise PolicyEvolutionStateError("POLICY_EVOLUTION_REQUIRES_PRODUCT_SUMMARY")
    snapshot_ref = find_latest_product_snapshot_v1(canonical_truth_root=canonical_truth_root, scope_id=scope.scope_id, day_utc=scope.day_utc)
    if snapshot_ref is None:
        raise PolicyEvolutionStateError("POLICY_EVOLUTION_REQUIRES_PRODUCT_SNAPSHOT")
    value_refs = _current_refs(list_value_states_v1(canonical_truth_root=canonical_truth_root, day_utc=scope.day_utc, scope_id=scope.scope_id))
    if not value_refs:
        raise PolicyEvolutionStateError("POLICY_EVOLUTION_REQUIRES_VALUE_STATE")
    refinement_refs = _current_refs(list_refinement_states_v1(canonical_truth_root=canonical_truth_root, day_utc=scope.day_utc, scope_id=scope.scope_id))
    if not refinement_refs:
        raise PolicyEvolutionStateError("POLICY_EVOLUTION_REQUIRES_REFINEMENT_STATE")
    rows: list[dict[str, Any]] = []
    artifact_refs: list[dict[str, str]] = []
    for refinement_ref in refinement_refs:
        evolution_target = str(refinement_ref.payload.get("refinement_target") or "")
        prior_policy_ref = _find_latest_policy_for_target(
            canonical_truth_root=canonical_truth_root,
            scope_id=scope.scope_id,
            evolution_target=evolution_target,
            before_day_utc=scope.day_utc,
        )
        stability_history = _history_for_target(
            canonical_truth_root=canonical_truth_root,
            scope_id=scope.scope_id,
            evolution_target=evolution_target,
            until_day_utc=scope.day_utc,
            window_days=3,
        )
        short_history = _history_for_target(
            canonical_truth_root=canonical_truth_root,
            scope_id=scope.scope_id,
            evolution_target=evolution_target,
            until_day_utc=scope.day_utc,
            window_days=2,
        )
        candidate = _candidate_from_refinement(
            day_utc=scope.day_utc,
            scope_id=scope.scope_id,
            refinement_ref=refinement_ref,
            summary_ref=summary_ref,
            snapshot_ref=snapshot_ref,
            prior_policy_ref=prior_policy_ref,
            stability_history=stability_history,
            short_history=short_history,
        )
        threshold = evaluate_policy_evidence_window_threshold_v1(candidate)
        action = build_policy_evolution_action_v1(threshold=threshold, candidate=candidate)
        evolution_id = canonical_hash_for_c2_artifact_v1(
            {
                "target_day": scope.day_utc,
                "scope_id": scope.scope_id,
                "evolution_target": evolution_target,
                "threshold_result": threshold["threshold_result"],
                "policy_action": action["proposed_policy_change"]["action"],
                "visibility_effect": action["proposed_policy_change"]["effective_visibility_effect"],
            }
        )
        payload: dict[str, Any] = {
            "schema_id": "policy_evolution_state",
            "schema_version": "v1",
            "artifact_id": ARTIFACT_ID,
            "surface_kind": "projection",
            "evolution_id": evolution_id,
            "kernel_version": KERNEL_VERSION,
            "threshold_model_version": THRESHOLD_MODEL_VERSION,
            "action_model_version": ACTION_MODEL_VERSION,
            "generated_at_utc": _utc_now(),
            "authority_label": "governed_policy_evolution",
            "target_day": scope.day_utc,
            "scope_id": scope.scope_id,
            "operation_type": scope.operation_type,
            "sleeve_id": scope.sleeve_id,
            "environment": scope.environment,
            "ib_account": scope.ib_account,
            "evolution_target": evolution_target,
            "target_policy_scope": candidate["target_policy_scope"],
            "target_label": candidate["target_label"],
            "proposed_policy_change": action["proposed_policy_change"],
            "evidence_window_refs": candidate["evidence_window_refs"],
            "evidence_basis_refs": candidate["evidence_basis_refs"],
            "evolution_strength": threshold["evolution_strength"],
            "threshold_result": threshold["threshold_result"],
            "trust_override_state": candidate["trust_override_state"],
            "reversibility_state": action["reversibility_state"],
            "expiry_state": action["expiry_state"],
            "preserved_visibility_flags": candidate["protected_flags"],
            "readiness_summary": candidate["readiness_summary"],
            "drill_down_route": candidate["drill_down_route"],
            "before_policy_state": candidate["before_policy_state"],
            "after_policy_state": action["after_policy_state"],
            "summary_message": action["summary_message"],
            "semantic_events": [],
        }
        if prior_policy_ref is not None:
            payload["before_policy_ref"] = _plain_ref_from_surface(prior_policy_ref, ARTIFACT_ID)
            if str(prior_policy_ref.payload.get("evolution_id") or "") != evolution_id:
                payload["supersedes_ref"] = _plain_ref_from_surface(prior_policy_ref, ARTIFACT_ID)
        event_types = {"policy_evolution_state_computed", "policy_snapshot_created"}
        policy_action = action["proposed_policy_change"]["action"]
        if policy_action in {"propose_strengthen_emphasis", "propose_reduce_emphasis", "propose_more_compression"}:
            event_types.add("policy_change_proposed")
        if policy_action == "evolution_withheld":
            event_types.add("policy_change_withheld_due_to_insufficient_history")
        if candidate["trust_override_state"] != "none":
            event_types.add("policy_change_blocked_by_trust_override")
        if policy_action == "evolution_expired":
            event_types.add("policy_change_expired")
        if policy_action == "rollback_to_prior_policy":
            event_types.add("policy_rollback_selected")
        payload["semantic_events"] = [
            {
                "event_type": event_type,
                "authority_label": "governed_policy_evolution",
                "evolution_target": evolution_target,
                "proposed_policy_change": action["proposed_policy_change"],
                "evolution_strength": threshold["evolution_strength"],
                "threshold_result": threshold["threshold_result"],
                "trust_override_state": candidate["trust_override_state"],
                "evidence_window_refs": candidate["evidence_window_refs"],
                **({"before_policy_ref": payload["before_policy_ref"]} if isinstance(payload.get("before_policy_ref"), dict) else {}),
                "kernel_version": KERNEL_VERSION,
            }
            for event_type in sorted(event_types)
        ]
        rows.append(payload)
        if emit_artifacts:
            artifact_ref = _write_policy_evolution(payload, scope)
            artifact_refs.append(_plain_ref_from_surface(artifact_ref, ARTIFACT_ID))
            if isinstance(payload.get("supersedes_ref"), dict):
                prior_path = Path(payload["supersedes_ref"]["artifact_path"])
                prior_payload = read_validated_surface_v1(path=prior_path, schema_relpath=SCHEMA_RELPATH).payload
                updated_prior = dict(prior_payload)
                updated_prior["superseded_by_ref"] = _plain_ref_from_surface(artifact_ref, ARTIFACT_ID)
                updated_prior["after_policy_ref"] = _plain_ref_from_surface(artifact_ref, ARTIFACT_ID)
                atomic_write_idempotent_validated_json_v1(
                    path=prior_path,
                    payload=updated_prior,
                    schema_relpath=SCHEMA_RELPATH,
                    volatile_field_names=("generated_at_utc",),
                )
    return {
        "ok": True,
        "artifact_id": ARTIFACT_ID,
        "rows": rows,
        "artifact_refs": artifact_refs,
    }
