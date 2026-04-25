from __future__ import annotations

from pathlib import Path

import pytest

from constellation_2.common.policy_evolution_state_kernel_v1 import materialize_policy_evolution_state_v1
from constellation_2.common.product_summary_kernel_v1 import materialize_product_summary_v1
from constellation_2.common.refinement_state_kernel_v1 import materialize_refinement_state_v1
from constellation_2.common.tests.test_product_summary_kernel_v1 import ACCOUNT, DAY, ENV, SLEEVE, _seed_product_runtime
from constellation_2.common.tests.test_value_state_kernel_v1 import _write_fill_ledger, _write_reconciliation_report
from constellation_2.common.value_state_kernel_v1 import materialize_value_state_v1
from constellation_2.phaseL.ui_api.policy_evolution_state_read_model import build_policy_evolution_view


def _materialize_policy(canonical_truth: Path, sleeve_root: Path) -> None:
    materialize_product_summary_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifacts=True,
    )
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
    materialize_policy_evolution_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifacts=True,
    )


def test_policy_view_stays_missing_without_policy_artifact_even_when_upstream_truth_exists(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canonical_truth, sleeve_root, *_ = _seed_product_runtime(monkeypatch, tmp_path)
    materialize_product_summary_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifacts=True,
    )
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
    monkeypatch.setattr("constellation_2.phaseL.ui_api.policy_evolution_state_read_model.GLOBAL_TRUTH_ROOT", canonical_truth)
    payload = build_policy_evolution_view(DAY)
    assert payload["view_name"] == "policy_evolution"
    assert payload["top_level_items"] == []
    assert payload["policy_warnings"] == ["POLICY_EVOLUTION_STATE_ARTIFACT_MISSING"]


def test_policy_view_renders_governed_policy_artifacts_only(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canonical_truth, sleeve_root, *_ = _seed_product_runtime(monkeypatch, tmp_path, blocked_readiness=True)
    _materialize_policy(canonical_truth, sleeve_root)
    monkeypatch.setattr("constellation_2.phaseL.ui_api.policy_evolution_state_read_model.GLOBAL_TRUTH_ROOT", canonical_truth)
    payload = build_policy_evolution_view(DAY)
    assert payload["policy_warnings"] == []
    assert payload["proof_rows"]
    assert payload["source_refs"][0]["artifact_type"] == "policy_evolution_state_v1"
    assert "blocked" in payload["readiness_summary"]


def test_command_and_policy_surfaces_are_policy_driven() -> None:
    root = Path(__file__).resolve().parents[4]
    pages = (root / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "pages" / "index.js").read_text(encoding="utf-8")
    system_summary = (root / "constellation_2" / "phaseL" / "ui_api" / "system_summary_read_model.py").read_text(encoding="utf-8")
    domain_client = (root / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "domain_client" / "index.js").read_text(encoding="utf-8")
    command_section = pages.split("async function renderCommandPage", 1)[1].split("async function renderPortfolioPage", 1)[0]
    policy_section = pages.split("async function renderPolicyPage", 1)[1].split("async function renderAdvisoryPage", 1)[0]
    assert "policy_evolution_state_v1" in command_section
    assert "fetchRefinement()" not in command_section
    assert "fetchFinancialState()" not in command_section
    assert 'path: "/policy"' in pages
    assert "fetchPolicyEvolution()" in policy_section
    assert "build_policy_evolution_view" in system_summary
    assert 'query("/api/policy-evolution")' in domain_client
