from __future__ import annotations

from pathlib import Path

import pytest

from constellation_2.common.product_summary_kernel_v1 import materialize_product_summary_v1
from constellation_2.common.tests.test_product_summary_kernel_v1 import (
    ACCOUNT,
    DAY,
    ENV,
    SLEEVE,
    _seed_product_runtime,
)
from constellation_2.phaseL.ui_api.product_summary_read_model import build_product_summary_view


def test_product_summary_view_stays_missing_without_summary_artifact_even_when_upstream_truth_exists(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canonical_truth, *_ = _seed_product_runtime(monkeypatch, tmp_path)
    monkeypatch.setattr("constellation_2.phaseL.ui_api.product_summary_read_model.GLOBAL_TRUTH_ROOT", canonical_truth)

    payload = build_product_summary_view(DAY)

    assert payload["view_name"] == "product_summary"
    assert payload["top_actionable_items"] == []
    assert payload["summary_warnings"] == ["PRODUCT_SUMMARY_ARTIFACT_MISSING"]


def test_product_summary_view_renders_governed_summary_and_snapshot_only(
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
    monkeypatch.setattr("constellation_2.phaseL.ui_api.product_summary_read_model.GLOBAL_TRUTH_ROOT", canonical_truth)

    payload = build_product_summary_view(DAY)

    assert payload["summary_warnings"] == []
    assert payload["summary_id"]
    assert payload["top_actionable_items"] or payload["top_blocked_items"] or payload["critical_degraded_states"]
    assert payload["rendered_ai_summaries"]
    assert payload["source_refs"][0]["artifact_type"] == "product_summary_v1"


def test_product_summary_shell_remains_backend_artifact_driven() -> None:
    root = Path(__file__).resolve().parents[4]
    pages = (
        root / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "pages" / "index.js"
    ).read_text(encoding="utf-8")
    main_js = (
        root / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "main.js"
    ).read_text(encoding="utf-8")

    command_section = pages.split("async function renderCommandPage", 1)[1].split("async function renderPortfolioPage", 1)[0]
    assert "policy_evolution_state_v1" in command_section
    assert "What Matters Now" in command_section
    assert "fetchOperatorHome()" not in command_section
    assert "fetchAdvisory()" not in command_section
    assert "fetchFinancialState()" not in command_section
    assert '"backend gap"' not in main_js
