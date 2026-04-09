from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")


def test_engine_correlation_matrix_script_bootstraps_with_truth_root_override(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    truth_root.mkdir(parents=True, exist_ok=True)
    day_utc = "2026-04-09"
    script_path = (
        SOURCE_ROOT
        / "constellation_2"
        / "phaseJ"
        / "monitoring"
        / "run"
        / "run_engine_correlation_matrix_day_v1.py"
    )

    proc = subprocess.run(
        [sys.executable, str(script_path), "--day_utc", day_utc, "--truth_root", str(truth_root)],
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
