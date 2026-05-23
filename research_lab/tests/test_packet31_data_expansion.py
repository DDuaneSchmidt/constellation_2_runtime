from __future__ import annotations

from pathlib import Path

import pytest

from research_lab.breadth.breadth_builder import calculate_breadth_metrics
from research_lab.breadth.breadth_registry import build_and_store_breadth_snapshot
from research_lab.breadth.breadth_snapshot import build_breadth_snapshot, classify_breadth_regime
from research_lab.contracts.schemas import validate_contract
from research_lab.macro_events.macro_event_calendar import build_macro_event_calendar_snapshot, normalize_macro_event
from research_lab.macro_events.macro_event_loader import import_macro_event_calendar
from research_lab.research_intake.proposal_queue import hypothesis_proposal_queue
from research_lab.research_intake.readiness_assessment import build_research_readiness_assessment
from research_lab.storage.manifest_io import append_jsonl, write_json
from research_lab.storage.parquet_io import write_parquet_records
from research_lab.universes.universe_builder import build_universe_snapshot
from research_lab.universes.universe_registry import store_universe_snapshot

from research_lab.tests._packet30b_helpers import seed_minimum_dataset_and_cost_model, seed_oil_proposal


ROOT = Path(__file__).resolve().parents[1]


def _append_cost_model(store: Path) -> None:
    append_jsonl(store / "registries" / "cost_model_snapshots.jsonl", {"cost_model_snapshot_id": "cm_default", "name": "Default", "content_hash": "abc123abc123", "schema_version": "cost_model_snapshot.v1"})


def _proposal(event_family_id: str, symbols: list[str] | None = None) -> dict:
    return {
        "hypothesis_proposal_id": f"ehp_{event_family_id}",
        "intent_candidate_id": "ic_test",
        "event_family_id": event_family_id,
        "title": "Test proposal",
        "hypothesis": "Research-only test proposal.",
        "proposed_event_definition": {"default_event_definitions": [{"type": "daily_return_above_threshold", "params": {"threshold": 0.02}}]},
        "proposed_universe": symbols or ["SPY"],
        "proposed_forward_windows": [1, 2, 5],
        "proposed_regime_dimensions": ["risk_regime"],
        "required_data": ["daily OHLCV"],
        "data_requirement_status": "unknown",
        "evidence_requirements": ["hypothetical research evidence only"],
        "governance_classification": "experimental",
        "proposal_status": "proposed",
        "created_at": "2026-05-20T00:00:00Z",
        "created_by": "Tester",
        "schema_version": "hypothesis_proposal.v1",
        "content_hash": "abc123abc123",
    }


def _write_dataset(store: Path, dataset_id: str, symbols: list[str], rows_per_symbol: int = 60) -> None:
    rows = []
    for sidx, symbol in enumerate(symbols):
        for day in range(rows_per_symbol):
            close = 100.0 + sidx + day
            rows.append({
                "symbol": symbol,
                "date": f"2026-01-{(day % 28) + 1:02d}",
                "open": close - 0.5,
                "high": close + 1.0,
                "low": close - 1.0,
                "close": close,
                "adj_close": close,
                "volume": 1000 + day,
            })
    manifest = {"dataset_snapshot_id": dataset_id, "symbols_loaded": symbols, "symbols": symbols, "row_count": len(rows), "quality_status": "pass", "schema_version": "ohlcv_dataset_manifest.v1"}
    write_json(store / "datasets" / dataset_id / "manifest.json", manifest)
    write_json(store / "datasets" / dataset_id / "dataset_snapshot.json", manifest)
    write_parquet_records(store / "datasets" / dataset_id / "data" / "canonical" / "daily_ohlcv.parquet", rows, allow_json_fallback=True)
    append_jsonl(store / "registries" / "dataset_snapshots.jsonl", {"dataset_snapshot_id": dataset_id, "quality_status": "pass", "row_count": len(rows), "symbol_count": len(symbols), "symbols": symbols, "schema_version": "dataset_snapshot.v1"})


def test_expanded_universe_schemas_validate() -> None:
    for filename, name in [
        ("etf_research_expanded_v1.yaml", "etf_research_expanded"),
        ("oil_energy_research_v1.yaml", "oil_energy_research"),
        ("credit_rates_research_v1.yaml", "credit_rates_research"),
        ("volatility_research_v1.yaml", "volatility_research"),
        ("sector_research_v1.yaml", "sector_research"),
    ]:
        snapshot = build_universe_snapshot(name=name, version="v1", input_path=ROOT / "universes" / filename, created_at="2026-05-20T00:00:00Z")
        validate_contract("universe_snapshot", snapshot)


def test_breadth_snapshot_schema_and_metrics_are_deterministic() -> None:
    rows = []
    for symbol in ["AAA", "BBB"]:
        for idx in range(25):
            rows.append({"symbol": symbol, "date": f"2026-01-{idx + 1:02d}", "close": 100 + idx, "adj_close": 100 + idx})
    first = calculate_breadth_metrics(rows, universe_snapshot_id="us_test")
    second = calculate_breadth_metrics(list(reversed(rows)), universe_snapshot_id="us_test")
    assert first == second
    snapshot = build_breadth_snapshot(dataset_snapshot_id="ds_test", universe_snapshot_id="us_test", metrics=first, created_at="2026-05-20T00:00:00Z")
    validate_contract("breadth_snapshot", snapshot)


def test_breadth_collapse_and_recovery_regimes_classify() -> None:
    collapse = {"symbol_count": 10, "pct_above_20dma": 0.2, "pct_above_50dma": 0.2, "pct_positive_5d": 0.2}
    recovery = {"symbol_count": 10, "pct_above_20dma": 0.6, "pct_above_50dma": 0.5, "pct_positive_5d": 0.7}
    assert classify_breadth_regime(collapse) == "breadth_collapse"
    assert classify_breadth_regime(recovery, collapse) == "breadth_recovery"


def test_macro_event_calendar_schema_and_import_are_deterministic(tmp_path: Path) -> None:
    csv = tmp_path / "macro_events.csv"
    csv.write_text("event_date,event_type,event_name,importance,source,notes\n2026-05-20,FOMC,Fed minutes,high,operator,research only\n", encoding="utf-8")
    first_events = import_macro_event_calendar(csv, created_by="Tester")[1]
    second_events = import_macro_event_calendar(csv, created_by="Tester")[1]
    assert first_events == second_events
    snapshot = build_macro_event_calendar_snapshot(events=first_events, source_path=str(csv), created_by="Tester", created_at="2026-05-20T00:00:00Z")
    validate_contract("macro_event_calendar_snapshot", snapshot)


def test_invalid_macro_event_type_fails() -> None:
    with pytest.raises(ValueError):
        normalize_macro_event({"event_date": "2026-05-20", "event_type": "BAD", "event_name": "Bad", "importance": "low", "source": "test", "notes": ""})


def test_readiness_gate_detects_missing_oil_symbols(tmp_path: Path) -> None:
    seeded = seed_oil_proposal(tmp_path)
    seed_minimum_dataset_and_cost_model(tmp_path)
    assessment = build_research_readiness_assessment(proposal=seeded["proposal"], store_root=tmp_path, assessed_at="2026-05-20T00:00:00Z")
    assert assessment["ready_for_research"] is False
    assert "missing_required_symbols" in assessment["blocking_items"]
    assert set(assessment["missing_symbols"]) >= {"USO", "XLE", "XOP", "DBC"}


def test_readiness_gate_detects_available_oil_dataset_after_universe_exists(tmp_path: Path) -> None:
    seeded = seed_oil_proposal(tmp_path)
    oil_universe = build_universe_snapshot(name="oil_energy_research", version="v1", input_path=ROOT / "universes" / "oil_energy_research_v1.yaml", created_at="2026-05-20T00:00:00Z")
    store_universe_snapshot(oil_universe, store_root=tmp_path)
    _write_dataset(tmp_path, "ds_oil", ["USO", "XLE", "XOP", "DBC", "SPY"])
    _append_cost_model(tmp_path)
    assessment = build_research_readiness_assessment(proposal=seeded["proposal"], store_root=tmp_path, assessed_at="2026-05-20T00:00:00Z")
    assert assessment["ready_for_research"] is True
    assert assessment["data_requirement_status"] == "available"
    assert assessment["missing_symbols"] == []


def test_breadth_required_hypothesis_blocks_without_breadth_snapshot(tmp_path: Path) -> None:
    _write_dataset(tmp_path, "ds_spy", ["SPY"])
    _append_cost_model(tmp_path)
    assessment = build_research_readiness_assessment(proposal=_proposal("breadth_collapse"), store_root=tmp_path, assessed_at="2026-05-20T00:00:00Z")
    assert "missing_breadth_snapshot" in assessment["blocking_items"]
    assert assessment["breadth_snapshot_available"] is False


def test_macro_required_hypothesis_blocks_without_macro_calendar(tmp_path: Path) -> None:
    _write_dataset(tmp_path, "ds_spy", ["SPY"])
    _append_cost_model(tmp_path)
    assessment = build_research_readiness_assessment(proposal=_proposal("macro_headline_shock"), store_root=tmp_path, assessed_at="2026-05-20T00:00:00Z")
    assert "missing_macro_event_calendar" in assessment["blocking_items"]
    assert assessment["macro_event_calendar_available"] is False


def test_existing_proposal_queue_still_works(tmp_path: Path) -> None:
    proposal = seed_oil_proposal(tmp_path)["proposal"]
    queue = hypothesis_proposal_queue(status="proposed", store_root=tmp_path, actor="Tester", audit_view=False)
    assert any(row["hypothesis_proposal_id"] == proposal["hypothesis_proposal_id"] for row in queue["hypothesis_proposals"])


def test_breadth_snapshot_can_be_built_from_dataset(tmp_path: Path) -> None:
    universe = build_universe_snapshot(name="etf_research_expanded", version="v1", input_path=ROOT / "universes" / "etf_research_expanded_v1.yaml", created_at="2026-05-20T00:00:00Z")
    store_universe_snapshot(universe, store_root=tmp_path)
    _write_dataset(tmp_path, "ds_breadth", ["SPY", "QQQ", "IWM"])
    result = build_and_store_breadth_snapshot(dataset_snapshot_id="ds_breadth", universe_snapshot_id=universe["universe_snapshot_id"], store_root=tmp_path, actor="Tester")
    assert result["breadth_snapshot"]["metric_count"] > 0


def test_packet31_adds_no_execution_or_canonical_ohlcv_store_code() -> None:
    checked = "\n".join((ROOT / "src" / "research_lab" / rel).read_text(encoding="utf-8") for rel in [
        "breadth/breadth_snapshot.py",
        "breadth/breadth_builder.py",
        "breadth/breadth_registry.py",
        "macro_events/macro_event_calendar.py",
        "macro_events/macro_event_loader.py",
        "macro_events/macro_event_registry.py",
    ])
    forbidden = ["broker", "live trading", "autonomous trading", "order management", "portfolio optimizer", "capital allocation", "create_sleeve"]
    assert not any(term in checked.lower() for term in forbidden)
    assert "daily_ohlcv" not in (ROOT / "src" / "research_lab" / "breadth" / "breadth_registry.py").read_text(encoding="utf-8")
