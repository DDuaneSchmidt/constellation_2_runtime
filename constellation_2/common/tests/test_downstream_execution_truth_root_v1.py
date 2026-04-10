from __future__ import annotations

import subprocess
import tempfile
from pathlib import Path
from unittest import mock

import ops.tools.run_c2_paper_day_orchestrator_v2 as orchestrator_v2


REPO_ROOT = Path("/home/node/constellation").resolve()
TREND_RUNNER = (
    REPO_ROOT / "constellation_2" / "phaseI" / "trend_eq_primary" / "run" / "run_trend_eq_primary_intents_day_v1.py"
).resolve()


def _index_of_pair(cmd: list[str], flag: str, value: str) -> int:
    for i in range(len(cmd) - 1):
        if cmd[i] == flag and cmd[i + 1] == value:
            return i
    return -1


def test_structural_pre_activity_producers_pass_truth_root_to_market_data_engines() -> None:
    captured: list[tuple[str, list[str]]] = []

    def fake_run_cmd(stage_id: str, cmd: list[str], env: dict[str, str]) -> int:
        captured.append((stage_id, list(cmd)))
        return 0

    active = [
        {
            "engine_id": "C2_VOL_INCOME_DEFINED_RISK_V1",
            "runner_path": "constellation_2.phaseI.vol_income_defined_risk.run.run_vol_income_defined_risk_intents_day_v1",
            "engine_runner_path": "constellation_2/phaseI/vol_income_defined_risk/run/run_vol_income_defined_risk_intents_day_v1.py",
            "engine_runner_sha256": "a" * 64,
        },
        {
            "engine_id": "C2_TREND_EQ_PRIMARY_V1",
            "runner_path": "constellation_2.phaseI.trend_eq_primary.run.run_trend_eq_primary_intents_day_v1",
            "engine_runner_path": "constellation_2/phaseI/trend_eq_primary/run/run_trend_eq_primary_intents_day_v1.py",
            "engine_runner_sha256": "b" * 64,
        },
        {
            "engine_id": "C2_MEAN_REVERSION_EQ_V1",
            "runner_path": "constellation_2.phaseI.mean_reversion.run.run_mean_reversion_intents_day_v1",
            "engine_runner_path": "constellation_2/phaseI/mean_reversion/run/run_mean_reversion_intents_day_v1.py",
            "engine_runner_sha256": "c" * 64,
        },
    ]

    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td).resolve()
        env = {"C2_TRUTH_ROOT": str(truth_root)}
        with (
            mock.patch.object(orchestrator_v2, "_load_engine_registry", return_value={"engines": []}),
            mock.patch.object(orchestrator_v2, "_active_engines_sorted", return_value=active),
            mock.patch.object(
                orchestrator_v2,
                "_build_allowed_symbols_map_fail_closed",
                return_value={row["engine_id"]: None for row in active},
            ),
            mock.patch.object(orchestrator_v2, "_sha256_file", return_value="d" * 64),
            mock.patch.object(orchestrator_v2, "_resolve_structural_activity_mode", return_value="OFF"),
            mock.patch.object(orchestrator_v2, "_run_cmd", side_effect=fake_run_cmd),
            mock.patch.object(orchestrator_v2, "_emit_missing_engine_heartbeat", return_value=None),
        ):
            orchestrator_v2._run_structural_pre_activity_producers(
                truth_root=truth_root,
                day="2026-04-09",
                mode="PAPER",
                symbol="SPY",
                produced_utc="2026-04-09T00:00:00Z",
                git_sha="e" * 40,
                env=env,
            )

    stage_map = {stage_id: cmd for stage_id, cmd in captured}
    expected_root = str(truth_root)
    assert _index_of_pair(stage_map["A0A_VOL_INCOME_DEFINED_RISK_INTENTS_DAY_V1_SPY"], "--truth_root", expected_root) > 0
    assert _index_of_pair(stage_map["A0B_TREND_EQ_PRIMARY_INTENTS_DAY_V1_SPY"], "--truth_root", expected_root) > 0
    assert _index_of_pair(stage_map["A0C_MEAN_REVERSION_INTENTS_DAY_V1_SPY"], "--truth_root", expected_root) > 0


def test_trend_engine_explicit_truth_root_wins_and_missing_manifest_still_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td).resolve()
        result = subprocess.run(
            [
                "python3",
                str(TREND_RUNNER),
                "--day_utc",
                "2026-04-09",
                "--mode",
                "PAPER",
                "--truth_root",
                str(truth_root),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
            cwd=str(REPO_ROOT),
        )

    expected_manifest = str((truth_root / "market_data_snapshot_v1" / "dataset_manifest.json").resolve())
    assert result.returncode != 0
    assert f"MARKET_DATA_MANIFEST_MISSING: {expected_manifest}" in result.stderr
