from __future__ import annotations

from pathlib import Path

from constellation_2.phaseL.ui.tests.operator_shell_test_sources import pages_source_v1

import pytest

from constellation_2.common.product_summary_kernel_v1 import materialize_product_summary_v1
from constellation_2.common.tests.test_outcome_state_kernel_v1 import (
    ACCOUNT,
    DAY,
    ENV,
    SLEEVE,
    _seed_product_runtime,
    _write_reconciliation_report,
)
from constellation_2.common.value_state_kernel_v1 import materialize_value_state_v1
from constellation_2.phaseL.ui_api.value_state_read_model import build_value_state_view


def test_value_view_stays_missing_without_value_artifact_even_when_upstream_truth_exists(
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
    monkeypatch.setattr("constellation_2.phaseL.ui_api.value_state_read_model.GLOBAL_TRUTH_ROOT", canonical_truth)
    payload = build_value_state_view(DAY)
    assert payload["view_name"] == "value"
    assert payload["value_rows"] == []
    assert payload["value_warnings"] == ["VALUE_STATE_ARTIFACT_MISSING"]


def test_value_view_renders_governed_value_artifacts_only(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canonical_truth, sleeve_root, *_ = _seed_product_runtime(monkeypatch, tmp_path, blocked_readiness=True)
    materialize_product_summary_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifacts=True,
    )
    _write_reconciliation_report(canonical_truth, submissions_total=0, reason_codes=["NO_SUBMISSIONS_FOUND"])
    materialize_value_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        comparison_type="blocked_vs_allowed",
        emit_artifacts=True,
    )
    monkeypatch.setattr("constellation_2.phaseL.ui_api.value_state_read_model.GLOBAL_TRUTH_ROOT", canonical_truth)
    payload = build_value_state_view(DAY)
    assert payload["value_warnings"] == []
    assert payload["value_rows"]
    assert payload["source_refs"][0]["artifact_type"] == "value_state_v1"
    assert payload["sleeve_contribution_summary"]


def test_value_route_and_pages_are_artifact_backed() -> None:
    root = Path(__file__).resolve().parents[4]
    pages = pages_source_v1(root)
    domain_client = (
        root / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "domain_client" / "index.js"
    ).read_text(encoding="utf-8")
    assert 'path: "/outcomes"' in pages
    assert "fetchValue()" in domain_client
    outcome_section = pages.split("async function renderOutcomesPage", 1)[1].split("async function renderAdvisoryPage", 1)[0]
    assert "fetchValue()" in outcome_section
    assert "fetchOpportunities()" not in outcome_section
    assert "fetchTax()" not in outcome_section
    assert "fetchAdvisory()" not in outcome_section
    sleeves_section = pages.split("async function renderSleevesPage", 1)[1].split("async function renderTaxPage", 1)[0]
    assert "fetchValue()" in sleeves_section
