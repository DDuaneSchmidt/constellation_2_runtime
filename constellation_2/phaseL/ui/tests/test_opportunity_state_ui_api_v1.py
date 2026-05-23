from __future__ import annotations

from pathlib import Path

import pytest

from constellation_2.common.advisory_decision_state_kernel_v1 import materialize_advisory_decision_state_v1
from constellation_2.common.opportunity_state_kernel_v1 import materialize_opportunity_state_v1
from constellation_2.common.tests.test_advisory_decision_state_v1 import (
    _bundle5_session_chain,
    _seed_deployment_state,
)
from constellation_2.common.tests.test_opportunity_state_kernel_v1 import (
    ACCOUNT,
    DAY,
    ENV,
    SLEEVE,
    _materialize_tax_state,
    _scenario_path,
    _seed_tax_runtime_inputs,
    _write_scenario,
    _write_tax_journal,
    _accepted_fact,
)
from constellation_2.phaseL.ui_api.opportunity_state_read_model import build_opportunity_state_view


def _seed_opportunity_runtime(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> tuple[Path, Path, dict, Path, Path]:
    canonical_truth, sleeve_root, stage_result, chain_path = _bundle5_session_chain(tmp_path, monkeypatch)
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
    monkeypatch.setattr("constellation_2.phaseL.ui_api.opportunity_state_read_model.GLOBAL_TRUTH_ROOT", canonical_truth)
    readiness_payload = {
        "ok": True,
        "blocked": False,
        "blocked_state": {"reason": "", "blocking_errors": []},
        "authority_label": "governed_release_projection",
        "scope": {"ib_account": ACCOUNT},
        "baseline_gate": {"current_family_statuses": {"operator_status": "CERTIFIED_READY"}},
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
        lambda **_: dict(readiness_payload),
    )
    return canonical_truth, sleeve_root, stage_result, chain_path, _seed_deployment_state(canonical_truth)


def test_opportunity_state_view_stays_missing_without_opportunity_artifact_even_when_upstream_truth_exists(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canonical_truth, *_ = _seed_opportunity_runtime(monkeypatch, tmp_path)

    payload = build_opportunity_state_view(DAY)

    assert payload["view_name"] == "opportunity_state"
    assert payload["top_opportunities"] == []
    assert payload["opportunity_warnings"] == ["OPPORTUNITY_STATE_ARTIFACT_MISSING"]
    assert payload["opportunity_day"] is None
    assert canonical_truth.exists()


def test_opportunity_state_view_renders_governed_opportunity_artifacts_and_bound_advisory_only(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canonical_truth, sleeve_root, stage_result, chain_path, deployment_path = _seed_opportunity_runtime(monkeypatch, tmp_path)
    scenario_path = _write_scenario(
        _scenario_path(canonical_truth, "stress-run-ui"),
        support_status="fully_supported",
        scenario_ids=["base"],
        outcomes=["small_change"],
        summary="fixture monitor",
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
    scenario_ref = next(
        ref["artifact_path"]
        for ref, payload in zip(opportunity_report["artifact_refs"], opportunity_report["opportunities"])
        if payload["opportunity_type"] == "scenario_review"
    )
    materialize_advisory_decision_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        stage_path=stage_result["stage_ref"]["artifact_path"],
        transition_record_path=stage_result["transition_record_ref"]["artifact_path"],
        certification_path=stage_result["stage_certification_ref"]["artifact_path"],
        startup_chain_certification_path=str(chain_path),
        deployment_state_path=str(deployment_path),
        opportunity_state_path=scenario_ref,
        advisory_item_id="ui_opportunity_binding",
        advisory_surface_label="operator_shell_advisory",
        advisory_authority_class="recommendation",
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifact=True,
    )

    payload = build_opportunity_state_view(DAY)

    assert payload["opportunity_warnings"] == []
    assert payload["opportunity_day"] == DAY
    assert payload["top_opportunities"]
    assert any(row["opportunity_type"] == "scenario_review" for row in payload["top_opportunities"])
    assert payload["advisory_impacts"]
    assert payload["advisory_impacts"][0]["opportunity_binding_state"]["effect_state"] == "downgrade"
    assert payload["review_snapshot_summary"]["current_count"] >= 1
    assert payload["source_refs"][0]["artifact_type"] == "opportunity_state_v1"


def test_opportunities_page_reads_opportunity_projection_route() -> None:
    root = Path(__file__).resolve().parents[4]
    domain_client = (
        root / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "domain_client" / "index.js"
    ).read_text(encoding="utf-8")
    pages = (
        root / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "pages" / "index.js"
    ).read_text(encoding="utf-8")

    assert 'query("/api/opportunities")' in domain_client
    assert "async function renderOpportunitiesPage" in pages
    switch_section = pages.split("export async function loadRouteView", 1)[1]
    assert 'case "opportunities":' in switch_section
    assert "return renderOpportunitiesPage(state);" in switch_section
