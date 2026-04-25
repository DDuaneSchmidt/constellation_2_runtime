from __future__ import annotations

import copy
from datetime import date, timedelta
from pathlib import Path

import pytest

from constellation_2.common.paper_session_fact_plane_v1 import atomic_write_idempotent_validated_json_v1, read_validated_surface_v1
from constellation_2.common.policy_evolution_state_kernel_v1 import (
    PolicyEvolutionStateError,
    list_policy_evolution_states_v1,
    materialize_policy_evolution_state_v1,
)
from constellation_2.common.product_summary_kernel_v1 import materialize_product_summary_v1
from constellation_2.common.refinement_state_kernel_v1 import list_refinement_states_v1, materialize_refinement_state_v1
from constellation_2.common.tests.test_product_summary_kernel_v1 import ACCOUNT, DAY, ENV, SLEEVE, _seed_product_runtime
from constellation_2.common.tests.test_value_state_kernel_v1 import _write_fill_ledger, _write_reconciliation_report
from constellation_2.common.value_state_kernel_v1 import materialize_value_state_v1


REFINEMENT_SCHEMA = "governance/04_DATA/SCHEMAS/C2/RUNTIME/refinement_state.v1.schema.json"
POLICY_SCHEMA = "governance/04_DATA/SCHEMAS/C2/RUNTIME/policy_evolution_state.v1.schema.json"


def _strip_volatile(obj: object) -> object:
    if isinstance(obj, dict):
        return {
            key: _strip_volatile(value)
            for key, value in obj.items()
            if key not in {"generated_at_utc", "artifact_sha256"}
        }
    if isinstance(obj, list):
        return [_strip_volatile(value) for value in obj]
    return copy.deepcopy(obj)


def _minus_days(day: str, days: int) -> str:
    return (date.fromisoformat(day) - timedelta(days=days)).isoformat()


def _after_bucket(visibility_effect: str) -> str:
    return {
        "top_level": "top_level",
        "top_level_compressed": "top_level_compressed",
        "secondary": "secondary",
        "drilldown_only": "drilldown_only",
        "unchanged_due_to_withheld": "top_level",
        "expired_re_review_required": "top_level",
    }.get(visibility_effect, "top_level")


def _reason_for_action(action: str) -> str:
    return {
        "preserve_top_level": "PRESERVE_TRUST_CRITICAL_TOP_LEVEL",
        "compress_summary": "COMPRESS_ACTIONABLE_SUMMARY",
        "demote_to_secondary": "DEMOTE_REVIEW_DELTA_TO_SECONDARY",
        "preserve_drilldown_only": "PRESERVE_DRILLDOWN_VALUE_ONLY",
        "refinement_withheld": "WITHHOLD_REFINEMENT_INSUFFICIENT_EVIDENCE",
    }.get(action, "WITHHOLD_REFINEMENT_INSUFFICIENT_EVIDENCE")


def _materialize_product_value_refinement(
    canonical_truth: Path,
    sleeve_root: Path,
    *,
    with_realized_activity: bool = False,
) -> list[dict]:
    materialize_product_summary_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifacts=True,
    )
    if with_realized_activity:
        _write_fill_ledger(canonical_truth, filled_qty=10)
        _write_reconciliation_report(canonical_truth, submissions_total=1, reason_codes=["RECONCILIATION_OK"])
    materialize_value_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifacts=True,
    )
    materialize_refinement_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifacts=True,
    )
    return [dict(ref.payload) for ref in list_refinement_states_v1(canonical_truth_root=canonical_truth, day_utc=DAY)]


def _find_refinement_row(rows: list[dict], *, refinement_target: str | None = None, target_label: str | None = None, action: str | None = None) -> dict:
    for row in rows:
        if refinement_target is not None and str(row.get("refinement_target") or "") != refinement_target:
            continue
        if target_label is not None and str(row.get("target_label") or "") != target_label:
            continue
        if action is not None and str(row.get("refinement_action") or "") != action:
            continue
        return row
    raise AssertionError("Expected refinement row was not found.")


def _write_refinement_history_row(
    canonical_truth: Path,
    row: dict,
    *,
    target_day: str,
    action: str,
    visibility_effect: str,
    protected_distinctions: list[str] | None = None,
) -> Path:
    payload = copy.deepcopy(row)
    payload["target_day"] = target_day
    payload["generated_at_utc"] = f"{target_day}T18:00:00Z"
    payload["refinement_id"] = f"fixture-{payload['refinement_target']}-{target_day}-{action}"
    payload["refinement_action"] = action
    payload["visibility_effect"] = visibility_effect
    payload["refinement_reason_ids"] = [_reason_for_action(action)]
    payload["protected_distinctions"] = list(protected_distinctions or [])
    payload["summary_message"] = f"Fixture {action} for {payload['target_label']}"
    payload["after_state"] = {
        "surface_bucket": _after_bucket(visibility_effect),
        "summary_message": payload["summary_message"],
    }
    payload.pop("supersedes_ref", None)
    payload.pop("superseded_by_ref", None)
    path = (
        canonical_truth
        / "reports"
        / "refinement_state_v1"
        / target_day
        / payload["scope_id"]
        / payload["refinement_id"]
        / "refinement_state.v1.json"
    )
    return atomic_write_idempotent_validated_json_v1(
        path=path,
        payload=payload,
        schema_relpath=REFINEMENT_SCHEMA,
        volatile_field_names=("generated_at_utc",),
    ).path


def _write_policy_fixture_row(
    canonical_truth: Path,
    refinement_row: dict,
    *,
    target_day: str,
    action: str,
    visibility_effect: str,
    threshold_result: str = "NO_EVOLUTION_PRESSURE",
    trust_override_state: str = "none",
    expiry_state: str = "active_until_next_review",
    reversibility_state: str = "reversible_next_review",
) -> Path:
    payload = {
        "schema_id": "policy_evolution_state",
        "schema_version": "v1",
        "artifact_id": "policy_evolution_state_v1",
        "surface_kind": "projection",
        "evolution_id": f"fixture-{refinement_row['refinement_target']}-{target_day}-{action}",
        "kernel_version": "fixture",
        "threshold_model_version": "fixture",
        "action_model_version": "fixture",
        "generated_at_utc": f"{target_day}T19:00:00Z",
        "authority_label": "governed_policy_evolution",
        "target_day": target_day,
        "scope_id": refinement_row["scope_id"],
        "operation_type": refinement_row["operation_type"],
        "sleeve_id": refinement_row["sleeve_id"],
        "environment": refinement_row["environment"],
        "ib_account": refinement_row["ib_account"],
        "evolution_target": refinement_row["refinement_target"],
        "target_policy_scope": "command_home_policy",
        "target_label": refinement_row["target_label"],
        "proposed_policy_change": {
            "action": action,
            "effective_visibility_effect": visibility_effect,
            "affected_route": refinement_row["drill_down_route"],
        },
        "evidence_window_refs": copy.deepcopy(refinement_row["evidence_basis_refs"]),
        "evidence_basis_refs": copy.deepcopy(refinement_row["evidence_basis_refs"]),
        "evolution_strength": "supported",
        "threshold_result": threshold_result,
        "trust_override_state": trust_override_state,
        "reversibility_state": reversibility_state,
        "expiry_state": expiry_state,
        "preserved_visibility_flags": copy.deepcopy(refinement_row.get("protected_distinctions") or []),
        "readiness_summary": copy.deepcopy(refinement_row.get("readiness_summary") or {"blocked": False, "blocked_state": "READY", "operator_status": "UNKNOWN"}),
        "drill_down_route": refinement_row["drill_down_route"],
        "before_policy_state": {
            "policy_action": action,
            "visibility_effect": visibility_effect,
            "summary_message": f"Fixture {action}",
        },
        "after_policy_state": {
            "policy_action": action,
            "visibility_effect": visibility_effect,
            "summary_message": f"Fixture {action}",
        },
        "summary_message": f"Fixture {action}",
        "semantic_events": [
            {
                "event_type": "policy_snapshot_created",
                "authority_label": "governed_policy_evolution",
                "evolution_target": refinement_row["refinement_target"],
                "proposed_policy_change": {
                    "action": action,
                    "effective_visibility_effect": visibility_effect,
                    "affected_route": refinement_row["drill_down_route"],
                },
                "evolution_strength": "supported",
                "threshold_result": threshold_result,
                "trust_override_state": trust_override_state,
                "evidence_window_refs": copy.deepcopy(refinement_row["evidence_basis_refs"]),
                "kernel_version": "fixture",
            }
        ],
    }
    path = (
        canonical_truth
        / "reports"
        / "policy_evolution_state_v1"
        / target_day
        / payload["scope_id"]
        / payload["evolution_id"]
        / "policy_evolution_state.v1.json"
    )
    return atomic_write_idempotent_validated_json_v1(
        path=path,
        payload=payload,
        schema_relpath=POLICY_SCHEMA,
        volatile_field_names=("generated_at_utc",),
    ).path


def test_policy_evolution_is_deterministic_for_same_governed_history(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canonical_truth, sleeve_root, *_ = _seed_product_runtime(monkeypatch, tmp_path)
    rows = _materialize_product_value_refinement(canonical_truth, sleeve_root, with_realized_activity=True)
    value_row = _find_refinement_row(rows, refinement_target="value_summary")
    _write_refinement_history_row(canonical_truth, value_row, target_day=_minus_days(DAY, 1), action="compress_summary", visibility_effect="top_level_compressed")
    _write_refinement_history_row(canonical_truth, value_row, target_day=_minus_days(DAY, 2), action="compress_summary", visibility_effect="top_level_compressed")

    first = materialize_policy_evolution_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifacts=False,
    )
    second = materialize_policy_evolution_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifacts=False,
    )

    assert _strip_volatile(first["rows"]) == _strip_volatile(second["rows"])


def test_policy_evolution_withholds_when_history_is_insufficient(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canonical_truth, sleeve_root, *_ = _seed_product_runtime(monkeypatch, tmp_path)
    _materialize_product_value_refinement(canonical_truth, sleeve_root, with_realized_activity=True)

    report = materialize_policy_evolution_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifacts=False,
    )

    assert any((row["proposed_policy_change"] or {}).get("action") == "evolution_withheld" for row in report["rows"])


def test_policy_trust_override_blocks_reduce_emphasis_for_trust_critical_targets(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canonical_truth, sleeve_root, *_ = _seed_product_runtime(monkeypatch, tmp_path, blocked_readiness=True)
    rows = _materialize_product_value_refinement(canonical_truth, sleeve_root, with_realized_activity=True)
    readiness_row = _find_refinement_row(rows, target_label="Release readiness")
    _write_refinement_history_row(canonical_truth, readiness_row, target_day=_minus_days(DAY, 1), action="demote_to_secondary", visibility_effect="secondary")
    _write_refinement_history_row(canonical_truth, readiness_row, target_day=_minus_days(DAY, 2), action="demote_to_secondary", visibility_effect="secondary")

    report = materialize_policy_evolution_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifacts=False,
    )

    row = next(item for item in report["rows"] if item["target_label"] == "Release readiness")
    assert row["threshold_result"] == "TRUST_OVERRIDE_ACTIVE"
    assert row["trust_override_state"] == "block_reduce_emphasis"
    assert row["proposed_policy_change"]["action"] == "preserve_current_policy"
    assert "release_readiness_blocker" in row["preserved_visibility_flags"]


def test_policy_action_model_proposes_compression_and_reduction_when_history_is_stable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canonical_truth, sleeve_root, *_ = _seed_product_runtime(monkeypatch, tmp_path)
    rows = _materialize_product_value_refinement(canonical_truth, sleeve_root, with_realized_activity=True)
    compression_row = next(
        row
        for row in rows
        if str(row.get("refinement_action") or "") == "compress_summary"
        and not list(row.get("protected_distinctions") or [])
    )
    reduce_row = next(
        row for row in rows if str(row.get("refinement_action") or "") in {"demote_to_secondary", "preserve_drilldown_only"}
    )
    _write_refinement_history_row(canonical_truth, compression_row, target_day=_minus_days(DAY, 1), action="compress_summary", visibility_effect="top_level_compressed")
    _write_refinement_history_row(canonical_truth, compression_row, target_day=_minus_days(DAY, 2), action="compress_summary", visibility_effect="top_level_compressed")
    reduce_visibility = "secondary" if str(reduce_row.get("refinement_action") or "") == "demote_to_secondary" else "drilldown_only"
    reduce_action = str(reduce_row.get("refinement_action") or "")
    _write_refinement_history_row(canonical_truth, reduce_row, target_day=_minus_days(DAY, 1), action=reduce_action, visibility_effect=reduce_visibility)
    _write_refinement_history_row(canonical_truth, reduce_row, target_day=_minus_days(DAY, 2), action=reduce_action, visibility_effect=reduce_visibility)

    report = materialize_policy_evolution_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifacts=False,
    )

    compression = next(item for item in report["rows"] if item["evolution_target"] == compression_row["refinement_target"])
    reduction = next(item for item in report["rows"] if item["evolution_target"] == reduce_row["refinement_target"])
    assert compression["proposed_policy_change"]["action"] == "propose_more_compression"
    assert reduction["proposed_policy_change"]["action"] == "propose_reduce_emphasis"


def test_policy_rollback_is_selected_when_prior_policy_conflicts_with_trust_override(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canonical_truth, sleeve_root, *_ = _seed_product_runtime(monkeypatch, tmp_path, blocked_readiness=True)
    rows = _materialize_product_value_refinement(canonical_truth, sleeve_root, with_realized_activity=True)
    readiness_row = _find_refinement_row(rows, target_label="Release readiness")
    _write_policy_fixture_row(
        canonical_truth,
        readiness_row,
        target_day=_minus_days(DAY, 1),
        action="propose_reduce_emphasis",
        visibility_effect="secondary",
        threshold_result="CONSISTENT_REDUCE_SUPPORT",
    )

    report = materialize_policy_evolution_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifacts=False,
    )

    row = next(item for item in report["rows"] if item["target_label"] == "Release readiness")
    assert row["trust_override_state"] == "rollback_required"
    assert row["proposed_policy_change"]["action"] == "rollback_to_prior_policy"


def test_policy_provenance_before_after_and_lineage_are_reconstructable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canonical_truth, sleeve_root, *_ = _seed_product_runtime(monkeypatch, tmp_path)
    rows = _materialize_product_value_refinement(canonical_truth, sleeve_root, with_realized_activity=True)
    value_row = _find_refinement_row(rows, refinement_target="value_summary")
    prior_path = _write_policy_fixture_row(
        canonical_truth,
        value_row,
        target_day=_minus_days(DAY, 1),
        action="propose_more_compression",
        visibility_effect="top_level_compressed",
        threshold_result="CONSISTENT_COMPRESSION_SUPPORT",
    )

    report = materialize_policy_evolution_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifacts=True,
    )

    assert report["artifact_refs"]
    refs = list_policy_evolution_states_v1(canonical_truth_root=canonical_truth, day_utc=DAY)
    row = next(ref.payload for ref in refs if ref.payload["evolution_target"] == "value_summary")
    assert row["before_policy_ref"]["artifact_id"] == "policy_evolution_state_v1"
    prior_payload = read_validated_surface_v1(path=prior_path, schema_relpath=POLICY_SCHEMA).payload
    assert prior_payload["after_policy_ref"]["artifact_id"] == "policy_evolution_state_v1"


def test_policy_rejects_forbidden_repo_local_truth_root(tmp_path: Path) -> None:
    with pytest.raises(PolicyEvolutionStateError, match="POLICY_EVOLUTION_STATE_FORBIDDEN_TRUTH_ROOT"):
        materialize_policy_evolution_state_v1(
            day_utc=DAY,
            sleeve_id=SLEEVE,
            environment=ENV,
            ib_account=ACCOUNT,
            canonical_truth_root=Path("/home/node/constellation/constellation_2/runtime/truth"),
            truth_sleeves_root=tmp_path,
            emit_artifacts=False,
        )
