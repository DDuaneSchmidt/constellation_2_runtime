from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

SOURCE_ROOT = Path("/home/node/constellation")
import sys

if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_paper_startup_intent_input_convergence_v1 as convergence_tool


def _write_ref(path: Path) -> SimpleNamespace:
    return SimpleNamespace(path=path, sha256="a" * 64)


def test_convergence_tool_runs_positions_v5_before_v2(tmp_path: Path) -> None:
    decision_truth_root = (tmp_path / "truth").resolve()
    sleeve_truth_root = (tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER").resolve()
    sleeve_truth_root.mkdir(parents=True, exist_ok=True)
    captured: dict[str, object] = {}
    module_calls: list[str] = []

    def fake_module_result(module_name: str, *args: str, extra_env=None):  # noqa: ANN001
        module_calls.append(module_name)
        return {
            "script": module_name,
            "command": [module_name, *args],
            "return_code": 0,
            "stdout": "",
            "stderr": "",
        }

    def fake_tool_result(script_relpath: str, *args: str, extra_env=None):  # noqa: ANN001
        return {
            "script": script_relpath,
            "command": [script_relpath, *args],
            "return_code": 0,
            "stdout": "",
            "stderr": "",
        }

    def fake_derive(**kwargs):  # noqa: ANN003
        captured["source_refs"] = kwargs["source_refs"]
        return {"convergence_status": "SUCCESS"}

    with patch.object(convergence_tool, "resolve_decision_truth_root_v1", return_value=decision_truth_root):
        with patch.object(
            convergence_tool,
            "_resolve_primary_binding",
            return_value=SimpleNamespace(sleeve_id="PRIMARY", truth_root=str(sleeve_truth_root)),
        ):
            with patch.object(convergence_tool, "_git_sha", return_value="b" * 40):
                with patch.object(convergence_tool, "_module_result", side_effect=fake_module_result):
                    with patch.object(convergence_tool, "_tool_result", side_effect=fake_tool_result):
                        with patch.object(
                            convergence_tool,
                            "derive_paper_startup_intent_input_convergence_payload_v1",
                            side_effect=fake_derive,
                        ):
                            with patch.object(
                                convergence_tool,
                                "write_paper_startup_intent_input_convergence_v1",
                                return_value=_write_ref(decision_truth_root / "out.json"),
                            ):
                                rc = convergence_tool.main(
                                    [
                                        "--day_utc",
                                        "2026-04-24",
                                        "--truth_root",
                                        str(decision_truth_root),
                                        "--environment",
                                        "PAPER",
                                        "--ib_account",
                                        "DUO847203",
                                    ]
                                )

    assert rc == 0
    assert module_calls[:2] == [
        "constellation_2.phaseF.positions.run.run_positions_snapshot_day_v5",
        "constellation_2.phaseF.positions.run.run_positions_snapshot_day_v2",
    ]
    source_refs = captured["source_refs"]
    assert isinstance(source_refs, list)
    assert source_refs[0]["script"] == "constellation_2.phaseF.positions.run.run_positions_snapshot_day_v5"
    assert source_refs[1]["script"] == "constellation_2.phaseF.positions.run.run_positions_snapshot_day_v2"


def test_convergence_tool_skips_positions_v2_when_v5_fails(tmp_path: Path) -> None:
    decision_truth_root = (tmp_path / "truth").resolve()
    sleeve_truth_root = (tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER").resolve()
    sleeve_truth_root.mkdir(parents=True, exist_ok=True)
    captured: dict[str, object] = {}
    module_calls: list[str] = []

    def fake_module_result(module_name: str, *args: str, extra_env=None):  # noqa: ANN001
        module_calls.append(module_name)
        if module_name.endswith("run_positions_snapshot_day_v5"):
            return {
                "script": module_name,
                "command": [module_name, *args],
                "return_code": 1,
                "stdout": "",
                "stderr": "FAIL: UPSTREAM",
            }
        raise AssertionError(module_name)

    def fake_tool_result(script_relpath: str, *args: str, extra_env=None):  # noqa: ANN001
        return {
            "script": script_relpath,
            "command": [script_relpath, *args],
            "return_code": 0,
            "stdout": "",
            "stderr": "",
        }

    def fake_derive(**kwargs):  # noqa: ANN003
        captured["source_refs"] = kwargs["source_refs"]
        return {"convergence_status": "BLOCKED"}

    with patch.object(convergence_tool, "resolve_decision_truth_root_v1", return_value=decision_truth_root):
        with patch.object(
            convergence_tool,
            "_resolve_primary_binding",
            return_value=SimpleNamespace(sleeve_id="PRIMARY", truth_root=str(sleeve_truth_root)),
        ):
            with patch.object(convergence_tool, "_git_sha", return_value="c" * 40):
                with patch.object(convergence_tool, "_module_result", side_effect=fake_module_result):
                    with patch.object(convergence_tool, "_tool_result", side_effect=fake_tool_result):
                        with patch.object(
                            convergence_tool,
                            "derive_paper_startup_intent_input_convergence_payload_v1",
                            side_effect=fake_derive,
                        ):
                            with patch.object(
                                convergence_tool,
                                "write_paper_startup_intent_input_convergence_v1",
                                return_value=_write_ref(decision_truth_root / "out.json"),
                            ):
                                rc = convergence_tool.main(
                                    [
                                        "--day_utc",
                                        "2026-04-24",
                                        "--truth_root",
                                        str(decision_truth_root),
                                        "--environment",
                                        "PAPER",
                                        "--ib_account",
                                        "DUO847203",
                                    ]
                                )

    assert rc == 2
    assert module_calls == ["constellation_2.phaseF.positions.run.run_positions_snapshot_day_v5"]
    source_refs = captured["source_refs"]
    assert isinstance(source_refs, list)
    assert source_refs[0]["return_code"] == 1
    assert source_refs[1]["script"] == "constellation_2.phaseF.positions.run.run_positions_snapshot_day_v2"
    assert source_refs[1]["return_code"] == 99
    assert source_refs[1]["stderr"] == "SKIPPED_POSITIONS_SNAPSHOT_V5_FAILED"
