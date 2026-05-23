from __future__ import annotations

from pathlib import Path

import pytest

from ops.aegis.research_lab.research_lab_routes import research_lab_challenger_comparison_report_v1, research_lab_challenger_comparison_reports_v1
from research_lab.challengers.challenger_comparison import build_challenger_comparison_report, write_challenger_comparison_report
from research_lab.challengers.challenger_evidence import write_challenger_evidence_batch
from research_lab.contracts.schemas import validate_contract
from research_lab.storage.manifest_io import read_json, read_jsonl, write_json
from research_lab.tests.test_challenger_evidence_batch import _write_fixture


def _write_batch(store: Path) -> str:
    track_id = _write_fixture(store)
    result = write_challenger_evidence_batch(challenger_track_id=track_id, store_root=store)
    return result["batch"]["challenger_evidence_batch_id"]


def _files(store: Path, rel: str) -> list[str]:
    root = store / rel
    return sorted(path.relative_to(root).as_posix() for path in root.rglob("*") if path.is_file()) if root.exists() else []


def test_challenger_comparison_report_creation_and_references(tmp_path: Path) -> None:
    store = tmp_path / "store"
    batch_id = _write_batch(store)

    report = build_challenger_comparison_report(challenger_evidence_batch_id=batch_id, store_root=store)

    validate_contract("challenger_comparison_report", report)
    assert report["source_incumbent_evidence_ids"]["event_study_evidence_id"] == "ev_event_fixture"
    assert report["source_incumbent_evidence_ids"]["backtest_evidence_id"] == "ev_backtest_fixture"
    assert report["source_incumbent_evidence_ids"]["expectancy_drift_report_id"] == "edr_fixture"
    assert len(report["source_challenger_evidence_ids"]) == 7
    assert report["research_label"] == "RESEARCH_ONLY"


def test_comparison_table_includes_incumbent_all_challengers_and_blocked_visible(tmp_path: Path) -> None:
    store = tmp_path / "store"
    batch_id = _write_batch(store)

    report = build_challenger_comparison_report(challenger_evidence_batch_id=batch_id, store_root=store)

    assert len(report["comparison_table"]) == 8
    assert report["comparison_table"][0]["entity_type"] == "incumbent"
    assert any(row["entity_type"] == "blocked_challenger" and row["variant_name"] == "cost_sensitivity_variant" for row in report["comparison_table"])
    assert report["blocked_hypotheses"][0]["failure_reason"] == "cost_sensitivity_requires_supported_cost_model_snapshot"


def test_rank_order_excludes_blocked_and_insufficient_and_tiebreaks_deterministically(tmp_path: Path) -> None:
    store = tmp_path / "store"
    batch_id = _write_batch(store)
    batch_path = store / "challenger_evidence_batches" / f"{batch_id}.json"
    batch = read_json(batch_path)
    for item in batch["challenger_evidence_items"]:
        if item["status"] == "generated":
            item["derived_metrics"]["mean_post_cost_return"] = 0.01
            item["derived_metrics"]["downside_tail_metric"] = 0.0
            item["derived_metrics"]["measured_candidate_count"] = 40
    write_json(batch_path, batch, overwrite=True)

    report = build_challenger_comparison_report(challenger_evidence_batch_id=batch_id, store_root=store)

    ranked_ids = [row["challenger_hypothesis_id"] for row in report["deterministic_rank_order"]]
    assert "chl_slv_etf_drop_reversion_v1_cost_sensitivity" not in ranked_ids
    assert ranked_ids == [row["challenger_hypothesis_id"] for row in build_challenger_comparison_report(challenger_evidence_batch_id=batch_id, store_root=store)["deterministic_rank_order"]]


def test_evidence_sufficiency_and_recommended_action_rules(tmp_path: Path) -> None:
    store = tmp_path / "store"
    batch_id = _write_batch(store)
    report = build_challenger_comparison_report(challenger_evidence_batch_id=batch_id, store_root=store)
    assert report["evidence_sufficiency"] == "comparison_ready"
    assert report["recommended_next_action"] == "prepare_human_review"

    blocked_store = tmp_path / "blocked"
    blocked_batch_id = _write_batch(blocked_store)
    batch_path = blocked_store / "challenger_evidence_batches" / f"{blocked_batch_id}.json"
    batch = read_json(batch_path)
    for item in batch["challenger_evidence_items"]:
        item["status"] = "blocked"
        item["evidence_quality"] = "blocked"
        item["failure_reason"] = "fixture_blocked"
    batch["blocked_hypotheses"] = batch["challenger_evidence_items"]
    write_json(batch_path, batch, overwrite=True)
    blocked = build_challenger_comparison_report(challenger_evidence_batch_id=blocked_batch_id, store_root=blocked_store)
    assert blocked["evidence_sufficiency"] == "blocked"
    assert blocked["recommended_next_action"] == "maintain_watch"


def test_persistence_registry_audit_immutability_and_no_mutations(tmp_path: Path) -> None:
    store = tmp_path / "store"
    batch_id = _write_batch(store)
    sleeve_before = _files(store, "sleeves")
    paper_before = _files(store, "paper_trials")
    candidate_before = _files(store, "candidate_batches")

    result = write_challenger_comparison_report(challenger_evidence_batch_id=batch_id, store_root=store)

    assert read_jsonl(store / "registries" / "challenger_comparison_reports.jsonl")[-1]["challenger_comparison_report_id"] == result["report"]["challenger_comparison_report_id"]
    assert any(row["entity_id"] == result["report"]["challenger_comparison_report_id"] for row in read_jsonl(store / "audit_log" / "audit_events.jsonl"))
    assert _files(store, "sleeves") == sleeve_before
    assert _files(store, "paper_trials") == paper_before
    assert _files(store, "candidate_batches") == candidate_before
    with pytest.raises(FileExistsError):
        write_challenger_comparison_report(challenger_evidence_batch_id=batch_id, store_root=store)


def test_api_routes_are_read_only_projection(tmp_path: Path) -> None:
    store = tmp_path / "store"
    batch_id = _write_batch(store)
    result = write_challenger_comparison_report(challenger_evidence_batch_id=batch_id, store_root=store)

    listing = research_lab_challenger_comparison_reports_v1(store_root=store)
    detail = research_lab_challenger_comparison_report_v1(challenger_comparison_report_id=result["report"]["challenger_comparison_report_id"], store_root=store)

    assert listing["read_only"] is True
    assert detail["read_only"] is True
    assert listing["challenger_comparison_reports"][0]["challenger_comparison_report_id"] == result["report"]["challenger_comparison_report_id"]
    text = str(detail).lower()
    assert "canonical_ohlcv" not in text
    assert "trading_instructions" not in text
    assert "order_instructions" not in text
    assert "capital_allocation_instructions" not in text


def test_repeat_run_stable_fields_and_no_optimizer_or_ml_objects(tmp_path: Path) -> None:
    store = tmp_path / "store"
    batch_id = _write_batch(store)

    first = build_challenger_comparison_report(challenger_evidence_batch_id=batch_id, store_root=store)
    second = build_challenger_comparison_report(challenger_evidence_batch_id=batch_id, store_root=store)

    assert first["comparison_table"] == second["comparison_table"]
    assert first["deterministic_rank_order"] == second["deterministic_rank_order"]
    assert first["comparison_config"]["optimizer_statement"] == "NOT_AN_OPTIMIZER"
    assert first["governance_summary"]["no_capital_allocation"] is True
    keys = {str(key).lower() for key in first}
    assert "broker_order" not in keys
    assert "order_management_object" not in keys
    assert "capital_allocation_object" not in keys
    assert "ml_ranking_model" not in keys
