from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


SCRIPT = Path("/home/node/constellation/ops/tools/run_accounting_attribution_v2_day_v1.py")
DAY = "2026-04-23"
ACTIVE_SLEEVES = {
    "C2_MEAN_REVERSION_EQ_V1",
    "C2_TREND_EQ_PRIMARY_V1",
    "C2_VOL_INCOME_DEFINED_RISK_V1",
    "C2_EVENT_DISLOCATION_V1",
    "C2_DEFENSIVE_TAIL_V1",
    "C2_CROSS_ASSET_TREND_V1",
    "C2_MARKET_NEUTRAL_SPREAD_V1",
}


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _run(truth_root: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--day_utc",
            DAY,
            "--producer_git_sha",
            "a" * 40,
            "--producer_repo",
            "constellation",
            "--truth_root",
            str(truth_root),
        ],
        cwd="/home/node/constellation",
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def test_accounting_attribution_empty_positions_emits_all_active_sleeves(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _write_json(
        truth / "positions_v1" / "snapshots" / DAY / "positions_snapshot.v5.json",
        {"schema_id": "positions_snapshot", "day_utc": DAY, "status": "OK", "items": []},
    )

    proc = _run(truth)
    assert proc.returncode == 0, proc.stderr

    out_path = truth / "accounting_v2" / "attribution" / DAY / "engine_attribution.v2.json"
    payload = json.loads(out_path.read_text(encoding="utf-8"))
    rows = payload["attribution"]["by_engine"]
    assert payload["status"] == "ACTIVE"
    assert set(row["engine_id"] for row in rows) == ACTIVE_SLEEVES
    assert all(row["basis"] == "SAFE_IDLE_EMPTY_POSITIONS" for row in rows)
    assert all(str(row["pnl_to_date"]) == "0" for row in rows)
    assert "C2_INTENT_SIMULATOR_V1" not in {row["engine_id"] for row in rows}


def test_accounting_attribution_open_positions_missing_marks_remains_degraded(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _write_json(
        truth / "positions_v1" / "snapshots" / DAY / "positions_snapshot.v5.json",
        {"schema_id": "positions_snapshot", "day_utc": DAY, "status": "OK", "items": [{"symbol": "SPY", "quantity": "1"}]},
    )

    proc = _run(truth)
    assert proc.returncode == 0, proc.stderr

    out_path = truth / "accounting_v2" / "attribution" / DAY / "engine_attribution.v2.json"
    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload["status"] == "DEGRADED_MISSING_INPUTS"
    assert "MISSING_INPUTS" in payload["reason_codes"]
    assert payload["attribution"]["by_engine"] == []
