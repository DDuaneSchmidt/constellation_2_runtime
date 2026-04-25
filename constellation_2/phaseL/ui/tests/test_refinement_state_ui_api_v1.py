from __future__ import annotations

from pathlib import Path

import pytest

from constellation_2.common.product_summary_kernel_v1 import materialize_product_summary_v1
from constellation_2.common.refinement_state_kernel_v1 import materialize_refinement_state_v1
from constellation_2.common.tests.test_product_summary_kernel_v1 import ACCOUNT, DAY, ENV, SLEEVE, _seed_product_runtime
from constellation_2.common.tests.test_value_state_kernel_v1 import _write_fill_ledger, _write_reconciliation_report
from constellation_2.common.value_state_kernel_v1 import materialize_value_state_v1
from constellation_2.phaseL.ui_api.refinement_state_read_model import build_refinement_state_view


def _materialize_refinement(monkeypatch: pytest.MonkeyPatch, canonical_truth: Path, sleeve_root: Path) -> None:
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


def test_refinement_view_stays_missing_without_refinement_artifact_even_when_upstream_truth_exists(
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
    monkeypatch.setattr("constellation_2.phaseL.ui_api.refinement_state_read_model.GLOBAL_TRUTH_ROOT", canonical_truth)
    payload = build_refinement_state_view(DAY)
    assert payload["view_name"] == "refinement"
    assert payload["top_level_items"] == []
    assert payload["refinement_warnings"] == ["REFINEMENT_STATE_ARTIFACT_MISSING"]


def test_refinement_view_renders_governed_refinement_artifacts_only(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canonical_truth, sleeve_root, *_ = _seed_product_runtime(monkeypatch, tmp_path, blocked_readiness=True)
    _materialize_refinement(monkeypatch, canonical_truth, sleeve_root)
    monkeypatch.setattr("constellation_2.phaseL.ui_api.refinement_state_read_model.GLOBAL_TRUTH_ROOT", canonical_truth)
    payload = build_refinement_state_view(DAY)
    assert payload["refinement_warnings"] == []
    assert payload["proof_rows"]
    assert payload["source_refs"][0]["artifact_type"] == "refinement_state_v1"
    assert payload["trust_preserved_items"]


def test_refinement_surface_remains_refinement_driven() -> None:
    root = Path(__file__).resolve().parents[4]
    pages = (root / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "pages" / "index.js").read_text(encoding="utf-8")
    refinement_section = pages.split("async function renderRefinementPage", 1)[1].split("async function renderAdvisoryPage", 1)[0]
    assert 'path: "/refinement"' in pages
    assert "fetchRefinement()" in refinement_section
