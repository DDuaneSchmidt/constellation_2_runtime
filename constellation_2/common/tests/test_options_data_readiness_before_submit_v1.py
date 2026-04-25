from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import ops.tools.run_c2_paper_day_orchestrator_v2 as orchestrator_module


def test_orchestrator_stage_order_runs_options_readiness_before_phasec() -> None:
    truth_root = Path("/tmp/constellation_options_readiness_truth")
    with patch.object(
        orchestrator_module,
        "resolve_governed_paper_execution_roots",
        return_value=SimpleNamespace(execution_root_path=Path("/tmp/constellation_options_readiness_exec")),
    ):
        stages = orchestrator_module._build_stage_defs(
            truth=truth_root,
            day="2026-04-23",
            input_day="2026-04-23",
            ib_account="DUO847203",
            git_sha="a" * 40,
            attempt_id="2026-04-23__A0001",
        )

    stage_ids = [stage.stage_id for stage in stages]
    assert stage_ids.index("A6C_OPTIONS_CHAIN_CAPTURE_IB_DAY_V1") < stage_ids.index("A6D_OPTIONS_CHAIN_TRUTH_PROMOTION_DAY_V1")
    assert stage_ids.index("A6D_OPTIONS_CHAIN_TRUTH_PROMOTION_DAY_V1") < stage_ids.index("A7_PHASEC_IDENTITY_MATERIALIZER_DAY_V1")

    capture_stage = next(stage for stage in stages if stage.stage_id == "A6C_OPTIONS_CHAIN_CAPTURE_IB_DAY_V1")
    assert capture_stage.cmd[:2] == ["python3", "ops/tools/run_options_chain_capture_ib_day_v1.py"]


def test_options_capture_stage_uses_python3_and_runs_before_submit_when_inputs_missing() -> None:
    truth_root = Path("/tmp/constellation_options_readiness_truth")
    with patch.object(orchestrator_module, "_discover_short_vol_symbols", return_value=["SPY"]), patch.object(
        orchestrator_module, "_options_snapshot_exists_for_symbol", return_value=False
    ), patch.object(
        orchestrator_module, "_options_raw_exists_for_symbol", return_value=False
    ), patch.object(
        orchestrator_module, "_run_cmd", return_value=0
    ) as run_cmd_mock:
        executed, rc, reason_codes, outputs_present = orchestrator_module._run_options_capture_stage(
            truth_root=truth_root,
            day="2026-04-23",
            produced_utc="2026-04-23T14:15:16Z",
            env={},
        )

    assert executed is True
    assert rc == 0
    assert "OPTIONS_CAPTURE_OK:SPY" in reason_codes
    assert str((truth_root / "options_chain_raw_v1" / "2026-04-23").resolve()) in outputs_present
    called_cmd = run_cmd_mock.call_args.args[1]
    assert called_cmd[0] == "python3"
    assert called_cmd[1] == "ops/tools/run_options_chain_capture_ib_day_v1.py"
    assert "2026-04-23T14:15:16Z" in called_cmd
