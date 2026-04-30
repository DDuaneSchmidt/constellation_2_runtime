from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_defensive_tail_bridge_generates_symbol_snapshot_with_source_provenance(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-30"
    symbol = "TLT"
    script_path = SOURCE_ROOT / "constellation_2/phaseJ/tools/build_defensive_tail_required_inputs_day_v1.py"
    yearly = truth_root / "market_data_snapshot_v1" / symbol / "2026.jsonl"
    yearly_row = {
        "close": 86.49,
        "dataset_version": "v1",
        "high": 86.68,
        "ingested_utc": "2026-04-30T13:30:00Z",
        "low": 86.33,
        "open": 86.6,
        "symbol": symbol,
        "timestamp_utc": "2026-04-30T00:00:00Z",
        "volume": 10015902,
    }
    _write_text(yearly, json.dumps(yearly_row, sort_keys=True) + "\n")
    _write_json(
        truth_root / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v2.json",
        {"positions": [], "schema_id": "C2_POSITIONS_SNAPSHOT_V2", "day_utc": day_utc},
    )
    _write_json(
        truth_root / "monitoring_v1" / "regime_snapshot_v2" / day_utc / "regime_snapshot.v2.json",
        {"regime_label": "NORMAL", "schema_id": "regime_snapshot", "day_utc": day_utc, "evidence": {"drawdown_pct": "0.000000"}},
    )
    env = os.environ.copy()
    env["C2_TRUTH_ROOT"] = str(truth_root)

    proc = subprocess.run(
        [sys.executable, str(script_path), "--day_utc", day_utc, "--symbol", symbol],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    assert proc.returncode == 0, proc.stderr
    snapshot_path = truth_root / "market_data_snapshot_v1" / "snapshots" / day_utc / f"{symbol}.market_data_snapshot.v1.json"
    snapshot = json.loads(snapshot_path.read_text())
    assert snapshot["symbol"] == symbol
    assert snapshot["bars"] == [yearly_row]
    ref = snapshot["source_provenance"]["market_data_yearly_jsonl"]
    assert ref["path"] == str(yearly.resolve())
    assert ref["sha256"] == hashlib.sha256(yearly.read_bytes()).hexdigest()
    assert ref["row_count"] == 1
    assert ref["matching_day_row_count"] == 1


def test_defensive_tail_bridge_requires_symbol(tmp_path: Path) -> None:
    script_path = SOURCE_ROOT / "constellation_2/phaseJ/tools/build_defensive_tail_required_inputs_day_v1.py"
    env = os.environ.copy()
    env["C2_TRUTH_ROOT"] = str(tmp_path)

    proc = subprocess.run(
        [sys.executable, str(script_path), "--day_utc", "2026-04-30"],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    assert proc.returncode != 0
    assert "--symbol" in proc.stderr
