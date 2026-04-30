from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def test_defensive_tail_uses_governed_nav_snapshot_surface(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-09"
    script_path = (
        SOURCE_ROOT
        / "constellation_2"
        / "phaseI"
        / "defensive_tail"
        / "run"
        / "run_defensive_tail_intents_day_v1.py"
    )

    _write_json(
        truth_root / "market_data_snapshot_v1" / "snapshots" / day_utc / "SPY.market_data_snapshot.v1.json",
        {
            "bars": [],
            "day_utc": day_utc,
            "schema_id": "C2_MARKET_DATA_SNAPSHOT_V1",
            "schema_version": "v1",
            "symbol": "SPY",
        },
    )
    _write_json(
        truth_root / "accounting_v1" / "nav" / day_utc / "nav_snapshot.v1.json",
        {
            "day_utc": day_utc,
            "history": {"drawdown_pct": "0.000000"},
            "schema_id": "C2_NAV_SNAPSHOT_V1",
            "schema_version": "v1",
        },
    )
    _write_json(
        truth_root / "positions_snapshot_v2" / "snapshots" / day_utc / "positions_snapshot.v2.json",
        {"positions": [], "schema_id": "C2_POSITIONS_SNAPSHOT_V2", "day_utc": day_utc},
    )
    _write_json(
        truth_root / "monitoring_v1" / "engine_correlation_matrix" / day_utc / "engine_correlation_matrix.v1.json",
        {"correlation_matrix": {}, "schema_id": "C2_ENGINE_CORRELATION_MATRIX_V1", "day_utc": day_utc},
    )
    _write_json(
        truth_root / "monitoring_v1" / "regime_snapshot_v2" / day_utc / "regime_snapshot.v2.json",
        {"regime_label": "NORMAL", "schema_id": "regime_snapshot", "day_utc": day_utc},
    )

    proc = subprocess.run(
        [sys.executable, str(script_path), "--day_utc", day_utc, "--mode", "PAPER", "--truth_root", str(truth_root), "--symbol", "SPY"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0, proc.stderr
    assert "DEF_TAIL_NO_INTENT" in proc.stdout


def test_defensive_tail_still_fails_closed_when_nav_snapshot_missing(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-09"
    script_path = (
        SOURCE_ROOT
        / "constellation_2"
        / "phaseI"
        / "defensive_tail"
        / "run"
        / "run_defensive_tail_intents_day_v1.py"
    )

    _write_json(
        truth_root / "market_data_snapshot_v1" / "snapshots" / day_utc / "SPY.market_data_snapshot.v1.json",
        {
            "bars": [],
            "day_utc": day_utc,
            "schema_id": "C2_MARKET_DATA_SNAPSHOT_V1",
            "schema_version": "v1",
            "symbol": "SPY",
        },
    )
    _write_json(
        truth_root / "positions_snapshot_v2" / "snapshots" / day_utc / "positions_snapshot.v2.json",
        {"positions": [], "schema_id": "C2_POSITIONS_SNAPSHOT_V2", "day_utc": day_utc},
    )
    _write_json(
        truth_root / "monitoring_v1" / "engine_correlation_matrix" / day_utc / "engine_correlation_matrix.v1.json",
        {"correlation_matrix": {}, "schema_id": "C2_ENGINE_CORRELATION_MATRIX_V1", "day_utc": day_utc},
    )
    _write_json(
        truth_root / "monitoring_v1" / "regime_snapshot_v2" / day_utc / "regime_snapshot.v2.json",
        {"regime_label": "NORMAL", "schema_id": "regime_snapshot", "day_utc": day_utc},
    )

    proc = subprocess.run(
        [sys.executable, str(script_path), "--day_utc", day_utc, "--mode", "PAPER", "--truth_root", str(truth_root), "--symbol", "SPY"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode != 0
    assert "MISSING_REQUIRED_INPUTS" in proc.stderr


def test_defensive_tail_requested_symbol_flows_into_emitted_intent(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-09"
    script_path = (
        SOURCE_ROOT
        / "constellation_2"
        / "phaseI"
        / "defensive_tail"
        / "run"
        / "run_defensive_tail_intents_day_v1.py"
    )
    symbol = "TLT"

    _write_json(
        truth_root / "market_data_snapshot_v1" / "snapshots" / day_utc / f"{symbol}.market_data_snapshot.v1.json",
        {
            "bars": [],
            "day_utc": day_utc,
            "schema_id": "C2_MARKET_DATA_SNAPSHOT_V1",
            "schema_version": "v1",
            "symbol": symbol,
        },
    )
    _write_json(
        truth_root / "accounting_v1" / "nav" / day_utc / "nav_snapshot.v1.json",
        {
            "day_utc": day_utc,
            "history": {"drawdown_pct": "0.000000"},
            "schema_id": "C2_NAV_SNAPSHOT_V1",
            "schema_version": "v1",
        },
    )
    _write_json(
        truth_root / "positions_snapshot_v2" / "snapshots" / day_utc / "positions_snapshot.v2.json",
        {"positions": [], "schema_id": "C2_POSITIONS_SNAPSHOT_V2", "day_utc": day_utc},
    )
    _write_json(
        truth_root / "monitoring_v1" / "engine_correlation_matrix" / day_utc / "engine_correlation_matrix.v1.json",
        {"correlation_matrix": {}, "schema_id": "C2_ENGINE_CORRELATION_MATRIX_V1", "day_utc": day_utc},
    )
    _write_json(
        truth_root / "monitoring_v1" / "regime_snapshot_v2" / day_utc / "regime_snapshot.v2.json",
        {"regime_label": "NORMAL", "schema_id": "regime_snapshot", "day_utc": day_utc},
    )

    proc = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--day_utc",
            day_utc,
            "--mode",
            "PAPER",
            "--truth_root",
            str(truth_root),
            "--symbol",
            symbol,
            "--force_enter_test_only",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0, proc.stderr
    intents = sorted((truth_root / "intents_v1" / "snapshots" / day_utc).glob("*.exposure_intent.v2.json"))
    assert len(intents) == 1
    payload = json.loads(intents[0].read_text())
    assert payload["underlying"]["symbol"] == symbol
    assert payload["inputs"]["market_data_snapshot_v1"]["path"].endswith(f"/{symbol}.market_data_snapshot.v1.json")


def test_defensive_tail_symbol_is_required_fail_closed(tmp_path: Path) -> None:
    script_path = (
        SOURCE_ROOT
        / "constellation_2"
        / "phaseI"
        / "defensive_tail"
        / "run"
        / "run_defensive_tail_intents_day_v1.py"
    )

    proc = subprocess.run(
        [sys.executable, str(script_path), "--day_utc", "2026-04-09", "--mode", "PAPER", "--truth_root", str(tmp_path / "truth")],
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode != 0
    assert "--symbol" in proc.stderr
