from __future__ import annotations

from pathlib import Path

from research_lab.candidates.candidate_generator import generate_drop_reversion_candidates
from research_lab.costs.cost_model_registry import create_default_cost_model_snapshot
from research_lab.regimes.regime_builder import build_regime_snapshot
from research_lab.storage.manifest_io import write_json
from research_lab.tests.test_event_study_runner import _write_dataset


def _write_evidence(store: Path, evidence_id: str = "ev_fixture") -> str:
    root = store / "evidence_packages" / evidence_id
    manifest = {
        "evidence_package_id": evidence_id,
        "hypothesis_id": "hyp_fixture",
        "research_plan_id": "btp_fixture",
        "dataset_snapshot_id": "ds_fixture_event_study",
        "universe_snapshot_id": "us_fixture",
        "runner_name": "holding_period_backtest_v1",
        "runner_version": "holding_period_backtest_v1.0",
        "runner_input_hash": "input_hash_fixture",
        "runner_output_hash": "output_hash_fixture",
        "artifact_uris": [],
        "summary_uri": f"research://evidence/{evidence_id}/summary.json",
        "manifest_hash": "manifest_hash_fixture",
        "created_at": "2024-01-01T00:00:00Z",
        "created_by": "pytest",
        "schema_version": "evidence_package.v1",
        "evidence_type": "backtest",
        "evidence_quality": "moderate",
        "event_count": 1,
        "trade_count": 1,
    }
    write_json(root / "evidence_manifest.json", manifest, overwrite=False)
    return evidence_id


def _candidate_context(store: Path) -> tuple[str, str, str, str]:
    dataset_id = _write_dataset(store)
    regime = build_regime_snapshot(dataset_snapshot_id=dataset_id, benchmark_symbol="SPY", store_root=store, created_by="pytest", allow_json_fallback=True)
    cost = create_default_cost_model_snapshot(store_root=store, actor="pytest")
    evidence_id = _write_evidence(store)
    return evidence_id, dataset_id, regime["regime_snapshot"]["regime_snapshot_id"], cost["cost_model_snapshot"]["cost_model_snapshot_id"]


def test_threshold_breach_generates_candidate(tmp_path: Path) -> None:
    store = tmp_path / "store"
    evidence_id, dataset_id, regime_id, cost_id = _candidate_context(store)

    result = generate_drop_reversion_candidates(
        source_evidence_package_id=evidence_id,
        dataset_snapshot_id=dataset_id,
        regime_snapshot_id=regime_id,
        cost_model_snapshot_id=cost_id,
        as_of_date="2024-01-02",
        threshold=-0.01,
        store_root=store,
    )

    assert result["generation_summary"]["candidate_count"] == 1
    assert result["candidates"][0]["symbol"] == "SPY"
    assert result["candidates"][0]["signal_date"] == "2024-01-02"


def test_no_breach_produces_zero_candidates_without_failure(tmp_path: Path) -> None:
    store = tmp_path / "store"
    evidence_id, dataset_id, regime_id, cost_id = _candidate_context(store)

    result = generate_drop_reversion_candidates(
        source_evidence_package_id=evidence_id,
        dataset_snapshot_id=dataset_id,
        regime_snapshot_id=regime_id,
        cost_model_snapshot_id=cost_id,
        as_of_date="2024-01-04",
        threshold=-0.90,
        store_root=store,
    )

    assert result["generation_summary"]["candidate_count"] == 0
    assert result["generation_summary"]["zero_candidate_reason"]


def test_generator_uses_only_data_up_to_as_of_date(tmp_path: Path) -> None:
    store = tmp_path / "store"
    evidence_id, dataset_id, regime_id, cost_id = _candidate_context(store)

    result = generate_drop_reversion_candidates(
        source_evidence_package_id=evidence_id,
        dataset_snapshot_id=dataset_id,
        regime_snapshot_id=regime_id,
        cost_model_snapshot_id=cost_id,
        as_of_date="2024-01-02",
        threshold=-0.01,
        store_root=store,
    )

    assert all(row["signal_date"] <= "2024-01-02" for row in result["candidates"])

