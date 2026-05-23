from __future__ import annotations

from pathlib import Path

import pytest

from ops.aegis.research_lab.research_lab_routes import (
    research_lab_challenger_blockers_v1,
    research_lab_challenger_evidence_batch_v1,
    research_lab_challenger_evidence_batches_v1,
    research_lab_challenger_evidence_completeness_v1,
    research_lab_challenger_evidence_lineage_v1,
    research_lab_challenger_variants_v1,
)
from research_lab.challengers.challenger_evidence import build_challenger_evidence_batch, evidence_quality_summary, write_challenger_evidence_batch
from research_lab.challengers.challenger_track import write_challenger_research_track
from research_lab.contracts.schemas import validate_contract
from research_lab.storage.manifest_io import read_json, read_jsonl, write_json
from research_lab.storage.parquet_io import write_parquet_records


SLEEVE_ID = "slv_fixture"
VERSION_ID = "slvv_fixture"
STABILITY_ID = "ssr_fixture"
DRIFT_ID = "edr_fixture"
FRAGILITY_ID = "rfr_fixture"
TRACK_ID = "chtrk_slv_fixture_60feb21495de"


def _write_source(path: Path, artifact_id: str) -> None:
    write_json(path, {"artifact_id": artifact_id, "content_hash": f"hash_{artifact_id}", "research_label": "RESEARCH_ONLY"}, overwrite=False)


def _outcome_rows(values: list[float] | None = None) -> list[dict]:
    vals = values or ([0.02] * 45 + [0.01] * 45)
    rows = []
    windows = ["1d", "5d", "20d"]
    for idx, value in enumerate(vals):
        for window in windows:
            rows.append(
                {
                    "candidate_id": f"cand_{idx:03d}",
                    "symbol": "SPY" if idx % 2 == 0 else "QQQ",
                    "as_of_date": f"2024-05-{(idx % 28) + 1:02d}",
                    "outcome_window": window,
                    "post_cost_return": value if window != "1d" else value / 2,
                    "excess_return": value / 3,
                    "outcome_status": "measured",
                    "risk_regime": "risk_on" if idx % 3 else "risk_off",
                    "ranking_bucket": "top" if idx % 2 == 0 else "middle",
                    "ranking_score": float(100 - idx),
                }
            )
    return rows


def _write_fixture(store: Path, *, values: list[float] | None = None) -> str:
    run_id = "lcr_fixture"
    write_json(
        store / "sleeves" / SLEEVE_ID / "versions" / f"{VERSION_ID}.json",
        {
            "sleeve_id": SLEEVE_ID,
            "sleeve_version_id": VERSION_ID,
            "dataset_snapshot_id": "ds_fixture",
            "regime_snapshot_id": "rs_fixture",
            "cost_model_snapshot_id": "cm_fixture",
            "linked_event_study_evidence_package_id": "ev_event_fixture",
            "linked_backtest_evidence_package_id": "ev_backtest_fixture",
            "linked_candidate_batch_ids": ["cb_fixture"],
            "linked_evidence_package_ids": ["ev_event_fixture", "ev_backtest_fixture"],
            "content_hash": "hash_version",
            "schema_version": "sleeve_version.v1",
        },
        overwrite=False,
    )
    write_json(store / "longitudinal_runs" / run_id / "longitudinal_run.json", {"longitudinal_run_id": run_id, "sleeve_id": SLEEVE_ID, "sleeve_version_id": VERSION_ID}, overwrite=False)
    write_parquet_records(store / "longitudinal_runs" / run_id / "candidate_batch_index.parquet", [{"candidate_count": 90}], allow_json_fallback=True)
    write_parquet_records(store / "longitudinal_runs" / run_id / "outcome_index.parquet", _outcome_rows(values), allow_json_fallback=True)
    write_json(
        store / "stability_reports" / "expectancy_drift" / f"{DRIFT_ID}.json",
        {"expectancy_drift_report_id": DRIFT_ID, "sleeve_id": SLEEVE_ID, "sleeve_version_id": VERSION_ID, "longitudinal_run_id": run_id, "research_label": "RESEARCH_ONLY", "content_hash": "hash_drift"},
        overwrite=False,
    )
    write_json(
        store / "stability_reports" / "regime_fragility" / f"{FRAGILITY_ID}.json",
        {"regime_fragility_report_id": FRAGILITY_ID, "sleeve_id": SLEEVE_ID, "sleeve_version_id": VERSION_ID, "longitudinal_run_id": run_id, "research_label": "RESEARCH_ONLY", "content_hash": "hash_fragility"},
        overwrite=False,
    )
    write_json(
        store / "stability_reports" / "sleeve_stability" / f"{STABILITY_ID}.json",
        {
            "sleeve_stability_report_id": STABILITY_ID,
            "sleeve_id": SLEEVE_ID,
            "sleeve_version_id": VERSION_ID,
            "expectancy_drift_report_id": DRIFT_ID,
            "regime_fragility_report_id": FRAGILITY_ID,
            "overall_stability_status": "degrading",
            "recommended_action": "challenge_sleeve",
            "key_findings": ["expectancy_drift_status=degrading"],
            "research_label": "RESEARCH_ONLY",
            "content_hash": "hash_stability",
        },
        overwrite=False,
    )
    _write_source(store / "datasets" / "ds_fixture" / "dataset_snapshot.json", "ds_fixture")
    _write_source(store / "regimes" / "rs_fixture" / "regime_snapshot.json", "rs_fixture")
    _write_source(store / "cost_models" / "cm_fixture" / "cost_model_snapshot.json", "cm_fixture")
    _write_source(store / "evidence_packages" / "ev_event_fixture" / "evidence_manifest.json", "ev_event_fixture")
    _write_source(store / "evidence_packages" / "ev_backtest_fixture" / "evidence_manifest.json", "ev_backtest_fixture")
    _write_source(store / "candidate_batches" / "cb_fixture" / "candidate_batch.json", "cb_fixture")
    result = write_challenger_research_track(sleeve_id=SLEEVE_ID, sleeve_stability_report_id=STABILITY_ID, store_root=store)
    return result["track"]["challenger_track_id"]


def _files(store: Path, rel: str) -> list[str]:
    root = store / rel
    return sorted(path.relative_to(root).as_posix() for path in root.rglob("*") if path.is_file()) if root.exists() else []


def test_challenger_evidence_batch_creation_one_item_per_hypothesis(tmp_path: Path) -> None:
    store = tmp_path / "store"
    track_id = _write_fixture(store)

    batch = build_challenger_evidence_batch(challenger_track_id=track_id, store_root=store)

    validate_contract("challenger_evidence_batch", batch)
    assert len(batch["source_challenger_hypothesis_ids"]) == 7
    assert len(batch["challenger_evidence_items"]) == 7
    assert len(batch["blocked_hypotheses"]) == 1
    assert all(item["research_label"] == "RESEARCH_ONLY" for item in batch["challenger_evidence_items"])
    generated = [item for item in batch["challenger_evidence_items"] if item["status"] == "generated"]
    assert generated
    assert all(item["challenger_variant_id"] for item in generated)
    assert all(item["event_study_evidence_id"] for item in generated)
    assert all(item["backtest_evidence_id"] for item in generated)
    assert all(item["longitudinal_run_id"] for item in generated)
    assert all(item["expectancy_drift_report_id"] for item in generated)
    assert all(item["regime_fragility_report_id"] for item in generated)
    assert all(item["sleeve_stability_report_id"] for item in generated)
    assert all(item["evidence_completeness"]["complete"] is True for item in generated)
    assert batch["evidence_generation_config"]["method"] == "fully_materialized_rule_delta_evidence_chain"


def test_missing_required_sources_block_hypotheses(tmp_path: Path) -> None:
    store = tmp_path / "store"
    track_id = _write_fixture(store)
    (store / "datasets" / "ds_fixture" / "dataset_snapshot.json").unlink()

    batch = build_challenger_evidence_batch(challenger_track_id=track_id, store_root=store)

    assert len(batch["blocked_hypotheses"]) == 7
    assert batch["recommended_next_action"] == "investigate_blockers"
    assert all("required_source_missing:DatasetSnapshot" in item["failure_reason"] for item in batch["blocked_hypotheses"])


def test_missing_regime_and_cost_sources_block(tmp_path: Path) -> None:
    store = tmp_path / "store"
    track_id = _write_fixture(store)
    (store / "regimes" / "rs_fixture" / "regime_snapshot.json").unlink()
    batch = build_challenger_evidence_batch(challenger_track_id=track_id, store_root=store)
    assert all("required_source_missing:RegimeSnapshot" in item["failure_reason"] for item in batch["blocked_hypotheses"])

    store2 = tmp_path / "store2"
    track_id2 = _write_fixture(store2)
    (store2 / "cost_models" / "cm_fixture" / "cost_model_snapshot.json").unlink()
    batch2 = build_challenger_evidence_batch(challenger_track_id=track_id2, store_root=store2)
    assert all("required_source_missing:CostModelSnapshot" in item["failure_reason"] for item in batch2["blocked_hypotheses"])


def test_unresolved_rule_delta_blocks(tmp_path: Path) -> None:
    store = tmp_path / "store"
    track_id = _write_fixture(store)
    track_path = store / "challenger_tracks" / f"{track_id}.json"
    track = read_json(track_path)
    track["challenger_hypothesis_set"][0]["variant_name"] = "unknown_variant"
    write_json(track_path, track, overwrite=True)

    batch = build_challenger_evidence_batch(challenger_track_id=track_id, store_root=store)

    assert any(item["failure_reason"] == "deterministic_rule_delta_unresolved" for item in batch["blocked_hypotheses"])


def test_persistence_immutable_registry_audit_and_no_mutations(tmp_path: Path) -> None:
    store = tmp_path / "store"
    track_id = _write_fixture(store)
    sleeve_before = _files(store, "sleeves")
    paper_before = _files(store, "paper_trials")
    candidate_before = _files(store, "candidate_batches")

    result = write_challenger_evidence_batch(challenger_track_id=track_id, store_root=store)

    assert read_jsonl(store / "registries" / "challenger_evidence_batches.jsonl")[-1]["challenger_evidence_batch_id"] == result["batch"]["challenger_evidence_batch_id"]
    assert any(row["entity_id"] == result["batch"]["challenger_evidence_batch_id"] for row in read_jsonl(store / "audit_log" / "audit_events.jsonl"))
    generated = [item for item in result["batch"]["challenger_evidence_items"] if item["status"] == "generated"]
    assert all((store / "challenger_variants" / f"{item['challenger_variant_id']}.json").exists() for item in generated)
    assert all((store / "event_studies" / f"{item['event_study_evidence_id']}.json").exists() for item in generated)
    assert all((store / "backtests" / f"{item['backtest_evidence_id']}.json").exists() for item in generated)
    assert all((store / "longitudinal_runs" / item["longitudinal_run_id"] / "outcome_index.parquet").exists() for item in generated)
    assert all((store / "stability_reports" / "expectancy_drift" / f"{item['expectancy_drift_report_id']}.json").exists() for item in generated)
    assert all((store / "stability_reports" / "regime_fragility" / f"{item['regime_fragility_report_id']}.json").exists() for item in generated)
    assert all((store / "stability_reports" / "sleeve_stability" / f"{item['sleeve_stability_report_id']}.json").exists() for item in generated)
    assert _files(store, "sleeves") == sleeve_before
    assert _files(store, "paper_trials") == paper_before
    assert _files(store, "candidate_batches") == candidate_before
    with pytest.raises(FileExistsError):
        write_challenger_evidence_batch(challenger_track_id=track_id, store_root=store)


def test_api_routes_are_read_only_projection(tmp_path: Path) -> None:
    store = tmp_path / "store"
    track_id = _write_fixture(store)
    result = write_challenger_evidence_batch(challenger_track_id=track_id, store_root=store)

    listing = research_lab_challenger_evidence_batches_v1(store_root=store)
    detail = research_lab_challenger_evidence_batch_v1(challenger_evidence_batch_id=result["batch"]["challenger_evidence_batch_id"], store_root=store)
    variants = research_lab_challenger_variants_v1(store_root=store)
    completeness = research_lab_challenger_evidence_completeness_v1(challenger_evidence_batch_id=result["batch"]["challenger_evidence_batch_id"], store_root=store)
    lineage = research_lab_challenger_evidence_lineage_v1(challenger_evidence_batch_id=result["batch"]["challenger_evidence_batch_id"], store_root=store)
    blockers = research_lab_challenger_blockers_v1(challenger_evidence_batch_id=result["batch"]["challenger_evidence_batch_id"], store_root=store)

    assert listing["read_only"] is True
    assert detail["read_only"] is True
    assert variants["read_only"] is True
    assert completeness["read_only"] is True
    assert lineage["read_only"] is True
    assert blockers["read_only"] is True
    assert listing["challenger_evidence_batches"][0]["challenger_evidence_batch_id"] == result["batch"]["challenger_evidence_batch_id"]
    assert variants["count"] == 6
    assert completeness["complete_count"] == 6
    assert len(lineage["lineage"]) == 7
    assert len(blockers["blockers"]) == 1
    text = str(detail).lower()
    assert "canonical_ohlcv" not in text
    assert "trading_instructions" not in text
    assert "capital_allocation_instructions" not in text


def test_determinism_and_recommended_action_rules(tmp_path: Path) -> None:
    store = tmp_path / "store"
    track_id = _write_fixture(store)

    first = build_challenger_evidence_batch(challenger_track_id=track_id, store_root=store)
    second = build_challenger_evidence_batch(challenger_track_id=track_id, store_root=store)

    assert first["determinism_fingerprint"] == second["determinism_fingerprint"]
    assert first["challenger_evidence_items"] == second["challenger_evidence_items"]
    assert first["recommended_next_action"] == "compare_challengers"

    weak_store = tmp_path / "weak"
    weak_track = _write_fixture(weak_store, values=[-0.001] * 90)
    weak_batch = build_challenger_evidence_batch(challenger_track_id=weak_track, store_root=weak_store)
    assert weak_batch["recommended_next_action"] == "collect_more_observations"

    blocked_store = tmp_path / "blocked"
    blocked_track = _write_fixture(blocked_store)
    (blocked_store / "datasets" / "ds_fixture" / "dataset_snapshot.json").unlink()
    blocked_batch = build_challenger_evidence_batch(challenger_track_id=blocked_track, store_root=blocked_store)
    assert blocked_batch["recommended_next_action"] == "investigate_blockers"


def test_no_execution_optimizer_ml_or_capital_allocation_objects(tmp_path: Path) -> None:
    store = tmp_path / "store"
    track_id = _write_fixture(store)
    batch = build_challenger_evidence_batch(challenger_track_id=track_id, store_root=store)
    keys = {str(key).lower() for key in batch}

    assert batch["governance_constraints"]["broker_execution_allowed"] is False
    assert batch["governance_constraints"]["portfolio_optimizer_allowed"] is False
    assert batch["governance_constraints"]["ml_black_box_ranking_allowed"] is False
    assert batch["governance_constraints"]["capital_allocation_allowed"] is False
    assert "broker_order" not in keys
    assert "order_management_object" not in keys
    assert "capital_allocation_object" not in keys
    assert evidence_quality_summary(batch)["blocked"] == 1
