from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from research_lab.acquisition.csv_readiness import build_csv_readiness_report, write_csv_readiness_report
from research_lab.acquisition.expected_files import expected_csv_files_report
from research_lab.datasets.ohlcv_builder import build_ohlcv_dataset_snapshot
from research_lab.storage.paths import research_lab_root
from research_lab.universes.universe_builder import build_universe_snapshot
from research_lab.universes.universe_registry import load_universe_snapshot, store_universe_snapshot


MINIMUM_VIABLE_UNIVERSE_NAME = "local_etf_minimum_viable"
MINIMUM_VIABLE_UNIVERSE_VERSION = "v1"
MINIMUM_VALID_SYMBOLS = 3


def minimum_viable_universe_yaml() -> Path:
    return research_lab_root() / "universes" / "local_etf_minimum_viable_v1.yaml"


def create_or_reuse_minimum_viable_universe(*, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    snapshot = build_universe_snapshot(
        name=MINIMUM_VIABLE_UNIVERSE_NAME,
        version=MINIMUM_VIABLE_UNIVERSE_VERSION,
        input_path=minimum_viable_universe_yaml(),
        created_by=actor,
    )
    try:
        store_universe_snapshot(snapshot, store_root=store_root)
    except FileExistsError:
        snapshot = load_universe_snapshot(snapshot["universe_snapshot_id"], store_root=store_root)
    return snapshot


def first_dataset_status(*, csv_root: Path, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    universe = create_or_reuse_minimum_viable_universe(store_root=store_root, actor=actor)
    expected = expected_csv_files_report(
        universe_snapshot_id=universe["universe_snapshot_id"],
        csv_root=csv_root,
        store_root=store_root,
    )
    valid_present_count = len(expected["present_symbols"])
    can_build = valid_present_count >= MINIMUM_VALID_SYMBOLS
    validate_command = (
        "research_lab/.venv/bin/python -m research_lab.cli validate-local-csvs "
        f"--universe-snapshot-id {universe['universe_snapshot_id']} "
        f"--csv-root {csv_root} --start 2015-01-01 --end 2025-12-31 --allow-missing-symbols true"
    )
    build_command = (
        "research_lab/.venv/bin/python -m research_lab.cli build-from-present-csvs "
        f"--universe local_etf_minimum_viable_v1 --csv-root {csv_root} --start 2015-01-01 --end 2025-12-31"
    )
    create_study_command = (
        "research_lab/.venv/bin/python -m research_lab.cli create-standard-event-study "
        "--dataset-snapshot-id <new_dataset_snapshot_id> --hypothesis-id hyp_etf_drop_reversion_v1 "
        "--threshold -0.02 --forward-windows 1,2,5,10"
    )
    run_study_command = (
        "research_lab/.venv/bin/python -m research_lab.cli run-standard-event-study "
        "--research-plan-id <new_research_plan_id>"
    )
    next_action = "Add missing CSVs until at least 3 valid symbols are present."
    if can_build:
        next_action = "Run validate-local-csvs, then build-from-present-csvs."
    return {
        "universe_snapshot_id": universe["universe_snapshot_id"],
        "csv_root": str(csv_root.resolve()),
        "available_csv_symbols": expected["present_symbols"],
        "missing_minimum_viable_symbols": expected["missing_symbols"],
        "minimum_valid_symbols_required": MINIMUM_VALID_SYMBOLS,
        "present_symbol_count": valid_present_count,
        "can_build_minimum_viable_dataset": can_build,
        "next_operator_action": next_action,
        "commands": {
            "validate_csvs": validate_command,
            "build_dataset": build_command,
            "create_standard_event_study_after_build": create_study_command,
            "run_standard_event_study_after_plan": run_study_command,
        },
        "expected_files": expected,
        "schema_version": "first_dataset_status.v1",
    }


def build_from_present_csvs(
    *,
    universe: str,
    csv_root: Path,
    start: str,
    end: str,
    store_root: Path | None = None,
    actor: str = "Aegis",
    allow_test_parquet_fallback: bool = False,
) -> dict[str, Any]:
    if universe != "local_etf_minimum_viable_v1":
        raise ValueError("Packet 4C supports only --universe local_etf_minimum_viable_v1")
    snapshot = create_or_reuse_minimum_viable_universe(store_root=store_root, actor=actor)
    readiness = build_csv_readiness_report(
        universe_snapshot_id=snapshot["universe_snapshot_id"],
        csv_root=csv_root,
        start=start,
        end=end,
        allow_missing_symbols=True,
        store_root=store_root,
    )
    readiness_path = csv_root / "csv_readiness_report.json"
    write_csv_readiness_report(readiness, readiness_path)
    valid_symbols = readiness["symbols_present"]
    if len(valid_symbols) < MINIMUM_VALID_SYMBOLS:
        raise RuntimeError(
            f"Fewer than {MINIMUM_VALID_SYMBOLS} valid CSV symbols are present: {valid_symbols}. "
            f"Missing: {readiness['symbols_missing']}"
        )
    if not readiness["ready_to_build"]:
        raise RuntimeError(f"CSV readiness is blocked: {readiness['blocking_errors']}")
    previous_root = os.environ.get("LOCAL_CSV_OHLCV_ROOT")
    os.environ["LOCAL_CSV_OHLCV_ROOT"] = str(csv_root.resolve())
    try:
        result = build_ohlcv_dataset_snapshot(
            dataset_type="ohlcv",
            provider_name="local_csv",
            interval="1d",
            bar_policy_version="bp_daily_ohlcv_local_csv_v1",
            universe_snapshot_id=snapshot["universe_snapshot_id"],
            start_date=start,
            end_date=end,
            store_root=store_root,
            created_by=actor,
            command="python -m research_lab.cli build-from-present-csvs",
            allow_test_parquet_fallback=allow_test_parquet_fallback,
            allow_missing_symbols=True,
            require_readiness_report=True,
            readiness_report_path=readiness_path,
        )
    finally:
        if previous_root is None:
            os.environ.pop("LOCAL_CSV_OHLCV_ROOT", None)
        else:
            os.environ["LOCAL_CSV_OHLCV_ROOT"] = previous_root
    return {"universe_snapshot": snapshot, "csv_readiness_report": readiness, "dataset_build": result}
