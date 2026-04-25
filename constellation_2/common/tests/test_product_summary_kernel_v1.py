from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from constellation_2.common.opportunity_state_kernel_v1 import materialize_opportunity_state_v1
from constellation_2.common.paper_session_fact_plane_v1 import atomic_write_idempotent_validated_json_v1
from constellation_2.common.product_summary_kernel_v1 import (
    ProductSummaryError,
    find_latest_product_summary_v1,
    materialize_product_summary_v1,
)
from constellation_2.common.tests.test_advisory_decision_state_v1 import (
    _bundle5_session_chain,
    _seed_deployment_state,
)
from constellation_2.common.tests.test_opportunity_state_kernel_v1 import (
    ACCOUNT,
    DAY,
    ENV,
    PREV_DAY,
    SLEEVE,
    _accepted_fact,
    _materialize_advisory,
    _materialize_tax_state,
    _scenario_path,
    _seed_tax_runtime_inputs,
    _write_scenario,
    _write_tax_journal,
)


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


def _patch_readiness(monkeypatch: pytest.MonkeyPatch, *, blocked: bool) -> None:
    payload = {
        "ok": not blocked,
        "blocked": blocked,
        "blocked_state": {
            "reason": "RELEASE_BASELINE_BLOCKED" if blocked else "",
            "blocking_errors": ["RELEASE_BASELINE_BLOCKED"] if blocked else [],
        },
        "authority_label": "governed_release_projection",
        "scope": {"ib_account": ACCOUNT},
        "baseline_gate": {
            "current_family_statuses": {
                "operator_status": "BLOCKED" if blocked else "CERTIFIED_READY",
            }
        },
        "governing_refs": [
            {
                "artifact_id": "release_baseline_gate_v1",
                "artifact_path": "/tmp/release_baseline_gate.v1.json",
                "artifact_sha256": "1" * 64,
            }
        ],
    }
    monkeypatch.setattr(
        "constellation_2.common.opportunity_state_kernel_v1.build_certified_operational_readiness_v1",
        lambda **_: copy.deepcopy(payload),
    )
    monkeypatch.setattr(
        "constellation_2.common.product_summary_kernel_v1.build_certified_operational_readiness_v1",
        lambda **_: copy.deepcopy(payload),
    )


def _seed_product_runtime(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    blocked_readiness: bool = False,
    include_tax_fact: bool = True,
    positions_status: str = "OK",
) -> tuple[Path, Path, dict, dict]:
    canonical_truth, sleeve_root, stage_result, chain_path = _bundle5_session_chain(tmp_path, monkeypatch)
    _seed_tax_runtime_inputs(canonical_truth, sleeve_root, monkeypatch, positions_status=positions_status)
    if include_tax_fact:
        _write_tax_journal(
            canonical_truth,
            [
                _accepted_fact(
                    "account_classification_tax_regime",
                    {"account_id": ACCOUNT, "tax_regime": "taxable_brokerage"},
                    f"{DAY}T10:00:00Z",
                )
            ],
        )
    tax_report = _materialize_tax_state(canonical_truth, sleeve_root, emit_artifact=True)
    deployment_path = _seed_deployment_state(canonical_truth)
    _patch_readiness(monkeypatch, blocked=blocked_readiness)
    scenario_path = _write_scenario(
        _scenario_path(canonical_truth, "stress-run-product"),
        support_status="fully_supported",
        scenario_ids=["base"],
        outcomes=["review_needed"],
        summary="product scenario fixture",
    )
    opportunity_report = materialize_opportunity_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        stress_case_view_path=scenario_path,
        emit_artifacts=True,
    )
    advisory_opportunity_path = Path(opportunity_report["artifact_refs"][0]["artifact_path"])
    _materialize_advisory(
        canonical_truth=canonical_truth,
        truth_sleeves=sleeve_root,
        stage_result=stage_result,
        chain_path=chain_path,
        deployment_path=deployment_path,
        tax_state_path=Path(tax_report["artifact_ref"]["artifact_path"]),
        opportunity_state_path=advisory_opportunity_path,
        emit_artifact=True,
    )
    return canonical_truth, sleeve_root, stage_result, opportunity_report


def _write_prior_snapshot(canonical_truth: Path, scope_id: str) -> Path:
    payload = {
        "schema_id": "product_snapshot",
        "schema_version": "v1",
        "artifact_id": "product_snapshot_v1",
        "surface_kind": "projection",
        "snapshot_id": "prior-product-snapshot",
        "kernel_version": "fixture",
        "generated_at_utc": f"{PREV_DAY}T18:00:00Z",
        "authority_label": "governed_product_snapshot",
        "target_day": PREV_DAY,
        "scope_id": scope_id,
        "summary_ref": {
            "artifact_id": "product_summary_v1",
            "artifact_path": "/tmp/prior-product-summary.v1.json",
            "artifact_sha256": "2" * 64,
        },
        "selected_item_refs": [],
        "critical_degraded_refs": [],
        "rendered_ai_summaries": [
            {
                "ai_summary_id": "prior-ai-summary",
                "summary_type": "daily_review_brief",
                "rendered_text": "Prior brief",
                "degraded_state_summary": "",
                "blocked_state_summary": "",
                "provenance_refs": [],
                "assist_status": "fallback_deterministic",
                "version": "fixture",
            }
        ],
    }
    path = (
        canonical_truth
        / "reports"
        / "product_snapshot_v1"
        / PREV_DAY
        / scope_id
        / payload["snapshot_id"]
        / "product_snapshot.v1.json"
    )
    return atomic_write_idempotent_validated_json_v1(
        path=path,
        payload=payload,
        schema_relpath="governance/04_DATA/SCHEMAS/C2/RUNTIME/product_snapshot.v1.schema.json",
        volatile_field_names=("generated_at_utc",),
    ).path


def test_product_summary_is_deterministic_for_same_governed_truth(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canonical_truth, sleeve_root, *_ = _seed_product_runtime(monkeypatch, tmp_path)

    first = materialize_product_summary_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifacts=False,
    )
    second = materialize_product_summary_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifacts=False,
    )

    assert _strip_volatile(first["summary"]) == _strip_volatile(second["summary"])
    assert _strip_volatile(first["snapshot"]) == _strip_volatile(second["snapshot"])


def test_product_summary_precedence_elevates_degraded_and_blocked_items(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canonical_truth, sleeve_root, *_ = _seed_product_runtime(
        monkeypatch,
        tmp_path,
        blocked_readiness=True,
        include_tax_fact=False,
        positions_status="FAIL",
    )

    report = materialize_product_summary_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifacts=False,
    )

    assert report["summary"]["critical_degraded_states"]
    assert any(item["label"] == "Release readiness" for item in report["summary"]["critical_degraded_states"])
    assert report["summary"]["top_blocked_items"]
    assert "ELEVATE_CRITICAL_DEGRADED" in report["summary"]["selection_reason_ids"]
    assert "ELEVATE_BLOCKED_IMPORTANT" in report["summary"]["selection_reason_ids"]


def test_product_summary_fails_closed_without_required_opportunity_truth(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canonical_truth, sleeve_root, *_ = _bundle5_session_chain(tmp_path, monkeypatch)
    _seed_tax_runtime_inputs(canonical_truth, sleeve_root, monkeypatch)
    _write_tax_journal(
        canonical_truth,
        [
            _accepted_fact(
                "account_classification_tax_regime",
                {"account_id": ACCOUNT, "tax_regime": "taxable_brokerage"},
                f"{DAY}T10:00:00Z",
            )
        ],
    )
    _materialize_tax_state(canonical_truth, sleeve_root, emit_artifact=True)
    _patch_readiness(monkeypatch, blocked=False)

    with pytest.raises(ProductSummaryError, match="PRODUCT_SUMMARY_REQUIRES_OPPORTUNITY_STATE"):
        materialize_product_summary_v1(
            day_utc=DAY,
            sleeve_id=SLEEVE,
            environment=ENV,
            ib_account=ACCOUNT,
            canonical_truth_root=canonical_truth,
            truth_sleeves_root=sleeve_root,
            emit_artifacts=False,
        )


def test_bounded_ai_condensation_is_fallback_and_summary_only(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canonical_truth, sleeve_root, *_ = _seed_product_runtime(monkeypatch, tmp_path, blocked_readiness=True)

    report = materialize_product_summary_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifacts=True,
        summary_types=("daily_review_brief", "blocked_explanation_brief"),
    )

    assert report["snapshot"]["rendered_ai_summaries"]
    first = report["snapshot"]["rendered_ai_summaries"][0]
    assert first["assist_status"] == "fallback_deterministic"
    assert first["source_summary_ref"]["artifact_id"] == "product_summary_v1"
    assert first["rendered_text"]
    assert "Daily review" in first["rendered_text"]


def test_product_snapshot_preserves_provenance_and_prior_ref(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canonical_truth, sleeve_root, *_ = _seed_product_runtime(monkeypatch, tmp_path)
    preview = materialize_product_summary_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifacts=False,
    )
    scope_id = preview["summary"]["scope_id"]
    _write_prior_snapshot(canonical_truth, scope_id)

    report = materialize_product_summary_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifacts=True,
    )

    assert report["snapshot_ref"] is not None
    assert report["snapshot"]["summary_ref"]["artifact_id"] == "product_summary_v1"
    assert report["snapshot"]["prior_snapshot_ref"]["artifact_id"] == "product_snapshot_v1"
    assert report["snapshot"]["selected_item_refs"]
    assert all(row["source_summary_ref"]["artifact_id"] == "product_summary_v1" for row in report["snapshot"]["rendered_ai_summaries"])
    latest = find_latest_product_summary_v1(canonical_truth_root=canonical_truth, day_utc=DAY, scope_id=scope_id)
    assert latest is not None
    assert latest.payload["summary_id"] == report["summary"]["summary_id"]
