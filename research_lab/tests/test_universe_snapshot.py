from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from research_lab.contracts.schemas import validate_contract
from research_lab.universes.universe_builder import build_universe_snapshot


def _write_universe(path: Path, symbols: list[str]) -> None:
    payload = {
        "selection_policy": "test universe",
        "source_notes": "test-only",
        "symbols": [
            {
                "symbol": symbol,
                "asset_type": "ETF",
                "category": "test",
                "active": True,
                "min_start_date": "2005-01-01",
                "notes": "",
            }
            for symbol in symbols
        ],
    }
    path.write_text(yaml.safe_dump(payload, sort_keys=True), encoding="utf-8")


def test_universe_snapshot_schema_validation(tmp_path: Path) -> None:
    source = tmp_path / "universe.yaml"
    _write_universe(source, ["SPY", "QQQ"])

    snapshot = build_universe_snapshot(name="core", version="v1", input_path=source)

    validate_contract("universe_snapshot", snapshot)
    assert snapshot["universe_snapshot_id"].startswith("us_core_v1_")
    assert snapshot["symbol_count"] == 2


def test_universe_hash_is_deterministic_for_same_input(tmp_path: Path) -> None:
    source = tmp_path / "universe.yaml"
    _write_universe(source, ["QQQ", "SPY"])

    first = build_universe_snapshot(name="core", version="v1", input_path=source, created_at="2026-05-18T00:00:00Z")
    second = build_universe_snapshot(name="core", version="v1", input_path=source, created_at="2026-05-18T12:00:00Z")

    assert first["content_hash"] == second["content_hash"]
    assert first["universe_snapshot_id"] == second["universe_snapshot_id"]


def test_universe_hash_changes_when_symbols_change(tmp_path: Path) -> None:
    one = tmp_path / "one.yaml"
    two = tmp_path / "two.yaml"
    _write_universe(one, ["SPY", "QQQ"])
    _write_universe(two, ["SPY", "QQQ", "IWM"])

    first = build_universe_snapshot(name="core", version="v1", input_path=one)
    second = build_universe_snapshot(name="core", version="v1", input_path=two)

    assert first["content_hash"] != second["content_hash"]


def test_missing_required_universe_field_fails_validation(tmp_path: Path) -> None:
    source = tmp_path / "universe.yaml"
    _write_universe(source, ["SPY"])
    snapshot = build_universe_snapshot(name="core", version="v1", input_path=source)
    del snapshot["symbols"]

    with pytest.raises(Exception):
        validate_contract("universe_snapshot", snapshot)
