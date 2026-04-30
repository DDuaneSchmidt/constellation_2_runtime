from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
SCRIPT_PATH = (
    SOURCE_ROOT
    / "constellation_2"
    / "phaseJ"
    / "monitoring"
    / "run"
    / "run_engine_correlation_matrix_day_v1.py"
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def test_engine_correlation_matrix_script_bootstraps_with_truth_root_override(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    truth_root.mkdir(parents=True, exist_ok=True)
    day_utc = "2026-04-09"

    proc = subprocess.run(
        [sys.executable, str(SCRIPT_PATH), "--day_utc", day_utc, "--truth_root", str(truth_root)],
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0, proc.stderr
    out_path = (
        truth_root
        / "monitoring_v1"
        / "engine_correlation_matrix"
        / day_utc
        / "engine_correlation_matrix.v1.json"
    )
    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload["status"] == "DEGRADED_INSUFFICIENT_HISTORY"
    assert payload["matrix"]["engine_ids"] == ["BOOTSTRAP"]
    assert payload["bootstrap_policy"]["status"] == "BLOCKED_FOR_LIVE"


def test_engine_correlation_matrix_requires_full_window_for_pass(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    end_day = "2026-04-10"
    for day in ["2026-04-08", "2026-04-09", "2026-04-10"]:
        _write_json(
            truth_root / "monitoring_v1" / "engine_daily_returns_v1" / day / "engine_daily_returns.v1.json",
            {
                "schema_id": "C2_MONITORING_ENGINE_DAILY_RETURNS_V1",
                "schema_version": "1.0.0",
                "day_utc": day,
                "status": "ACTIVE",
                "reason_codes": [],
                "returns": [{"engine_id": "ENG_A", "daily_return": "0.00000000"}],
            },
        )

    proc = subprocess.run(
        [sys.executable, str(SCRIPT_PATH), "--day_utc", end_day, "--truth_root", str(truth_root), "--window_days", "20"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0, proc.stderr
    payload = json.loads(
        (
            truth_root
            / "monitoring_v1"
            / "engine_correlation_matrix"
            / end_day
            / "engine_correlation_matrix.v1.json"
        ).read_text(encoding="utf-8")
    )
    assert payload["status"] == "DEGRADED_INSUFFICIENT_HISTORY"
    assert "INSUFFICIENT_HISTORY_LT_WINDOW" in payload["reason_codes"]
    assert payload["bootstrap_policy"]["status"] == "BOOTSTRAP_ACCEPTED_FOR_PAPER"
    assert payload["bootstrap_policy"]["return_source"] == "REALIZED_ACCOUNTING_ATTRIBUTION_INSUFFICIENT_HISTORY"
