from __future__ import annotations

import copy
from pathlib import Path

import pytest

from constellation_2.common.product_summary_kernel_v1 import materialize_product_summary_v1
from constellation_2.common.refinement_state_kernel_v1 import (
    RefinementStateError,
    list_refinement_states_v1,
    materialize_refinement_state_v1,
)
from constellation_2.common.tests.test_product_summary_kernel_v1 import (
    ACCOUNT,
    DAY,
    ENV,
    SLEEVE,
    _seed_product_runtime,
)
from constellation_2.common.tests.test_value_state_kernel_v1 import _write_fill_ledger, _write_reconciliation_report
from constellation_2.common.value_state_kernel_v1 import materialize_value_state_v1


def _strip_volatile(obj: object) -> object:
    if isinstance(obj, dict):
        return {key: _strip_volatile(value) for key, value in obj.items() if key not in {"generated_at_utc", "artifact_sha256"}}
    if isinstance(obj, list):
        return [_strip_volatile(value) for value in obj]
    return copy.deepcopy(obj)


def _materialize_summary_and_value(
    monkeypatch: pytest.MonkeyPatch,
    canonical_truth: Path,
    sleeve_root: Path,
    *,
    with_realized_activity: bool = False,
    comparison_type: str | None = None,
) -> None:
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
        comparison_type=comparison_type,
        emit_artifacts=True,
    )


def test_refinement_state_is_deterministic_for_same_governed_truth(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canonical_truth, sleeve_root, *_ = _seed_product_runtime(monkeypatch, tmp_path)
    _materialize_summary_and_value(monkeypatch, canonical_truth, sleeve_root, with_realized_activity=True)

    first = materialize_refinement_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifacts=False,
    )
    second = materialize_refinement_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifacts=False,
    )

    assert _strip_volatile(first["rows"]) == _strip_volatile(second["rows"])


def test_refinement_threshold_withholds_without_value_basis(
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

    report = materialize_refinement_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifacts=False,
    )

    assert any(row["refinement_action"] == "refinement_withheld" for row in report["rows"])


def test_refinement_preserves_trust_critical_visibility(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canonical_truth, sleeve_root, *_ = _seed_product_runtime(monkeypatch, tmp_path, blocked_readiness=True)
    _materialize_summary_and_value(monkeypatch, canonical_truth, sleeve_root, comparison_type="blocked_vs_allowed")

    report = materialize_refinement_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifacts=False,
    )

    trust_rows = [row for row in report["rows"] if row["refinement_action"] == "preserve_top_level"]
    assert trust_rows
    assert any("release_readiness_blocker" in row["protected_distinctions"] for row in trust_rows)


def test_refinement_action_model_compresses_and_demotes_when_evidence_allows(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canonical_truth, sleeve_root, *_ = _seed_product_runtime(monkeypatch, tmp_path)
    _materialize_summary_and_value(monkeypatch, canonical_truth, sleeve_root, with_realized_activity=True)

    report = materialize_refinement_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifacts=False,
    )

    actions = {row["refinement_action"] for row in report["rows"]}
    assert "compress_summary" in actions
    assert "demote_to_secondary" in actions or "preserve_drilldown_only" in actions


def test_refinement_provenance_before_after_and_drilldown_are_reconstructable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canonical_truth, sleeve_root, *_ = _seed_product_runtime(monkeypatch, tmp_path)
    _materialize_summary_and_value(monkeypatch, canonical_truth, sleeve_root, with_realized_activity=True)

    report = materialize_refinement_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifacts=True,
    )

    assert report["artifact_refs"]
    refs = list_refinement_states_v1(canonical_truth_root=canonical_truth, day_utc=DAY)
    row = refs[-1].payload
    assert row["before_state"]["surface_bucket"]
    assert row["after_state"]["surface_bucket"]
    assert row["preserved_drilldown_refs"]
    assert row["before_snapshot_ref"]["artifact_id"] == "product_snapshot_v1"


def test_refinement_rejects_forbidden_repo_local_truth_root(tmp_path: Path) -> None:
    with pytest.raises(RefinementStateError, match="REFINEMENT_STATE_FORBIDDEN_TRUTH_ROOT"):
        materialize_refinement_state_v1(
            day_utc=DAY,
            sleeve_id=SLEEVE,
            environment=ENV,
            ib_account=ACCOUNT,
            canonical_truth_root=Path("/home/node/constellation/constellation_2/runtime/truth"),
            truth_sleeves_root=tmp_path,
            emit_artifacts=False,
        )
