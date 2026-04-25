from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from constellation_2.common.tests.test_lifecycle_action_authority_v1 import _write_core2_trade, DAY_UTC
from constellation_2.common.paper_session_fact_plane_v1 import read_json_object_v1


def test_run_lifecycle_action_authority_tool_v1(tmp_path: Path) -> None:
    materialization_dir = _write_core2_trade(tmp_path)
    execution_root = tmp_path / "runtime_truth_sleeves" / "PRIMARY" / "PAPER"
    tool = Path(__file__).resolve().parents[3] / "ops" / "tools" / "run_lifecycle_action_authority_v1.py"
    result = subprocess.run(
        [
            sys.executable,
            str(tool),
            "--core2_materialization_dir",
            str(materialization_dir),
            "--execution_root",
            str(execution_root),
            "--evaluated_at_utc",
            f"{DAY_UTC}T14:31:00Z",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr or result.stdout
    assert "OK: LIFECYCLE_ACTION_AUTHORITY_V1_WRITTEN" in result.stdout
    surface = read_json_object_v1(
        execution_root
        / "reports"
        / "lifecycle_action_operator_surface_v1"
        / DAY_UTC
        / ("b" * 64)
        / "lifecycle_action_operator_surface.v1.json"
    )
    assert surface["health_summary"]["trades_total"] == 1
