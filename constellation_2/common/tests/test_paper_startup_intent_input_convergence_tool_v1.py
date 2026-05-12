from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

SOURCE_ROOT = Path("/home/node/constellation")
import sys

if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_paper_startup_intent_input_convergence_v1 as convergence_tool


def _write_registry_files(repo_root: Path, *, engines: list[dict], contracts: list[dict]) -> None:
    registry_root = repo_root / "governance" / "02_REGISTRIES"
    registry_root.mkdir(parents=True, exist_ok=True)
    (registry_root / "ENGINE_MODEL_REGISTRY_V1.json").write_text(
        json.dumps(
            {
                "schema_id": "ENGINE_MODEL_REGISTRY_V1",
                "schema_version": 1,
                "engines": engines,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (registry_root / "SLEEVE_CONTRACTS_V1.json").write_text(
        json.dumps(
            {
                "schema_id": "SLEEVE_CONTRACTS_V1",
                "schema_version": 1,
                "contracts": contracts,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def _write_ref(path: Path) -> SimpleNamespace:
    return SimpleNamespace(path=path, sha256="a" * 64)


def test_required_daily_snapshot_symbols_are_registry_and_contract_driven(tmp_path: Path) -> None:
    _write_registry_files(
        tmp_path,
        engines=[
            {
                "engine_id": "C2_DEFENSIVE_TAIL_V1",
                "activation_status": "ACTIVE",
                "allowed_symbols": ["TLT"],
            },
            {
                "engine_id": "C2_SPY_REFERENCE_ONLY_V1",
                "activation_status": "ACTIVE",
                "allowed_symbols": ["SPY"],
            },
            {
                "engine_id": "C2_INACTIVE_DAILY_SNAPSHOT_V1",
                "activation_status": "INACTIVE",
                "allowed_symbols": ["GLD"],
            },
        ],
        contracts=[
            {
                "engine_id": "C2_DEFENSIVE_TAIL_V1",
                "can_generate_intents": True,
                "required_inputs": [
                    {
                        "name": "market_data_daily_snapshot",
                        "truth_owner": "sleeve_truth_root",
                        "required_scope": "allowed_symbols",
                        "expected_artifact_type": "json",
                        "path_template": convergence_tool.DAILY_MARKET_SNAPSHOT_TEMPLATE,
                        "required": True,
                    }
                ],
            },
            {
                "engine_id": "C2_SPY_REFERENCE_ONLY_V1",
                "can_generate_intents": True,
                "required_inputs": [
                    {
                        "name": "market_data_manifest",
                        "truth_owner": "sleeve_truth_root",
                        "required_scope": "global",
                        "expected_artifact_type": "manifest",
                        "path_template": "market_data_snapshot_v1/dataset_manifest.json",
                        "required": True,
                    }
                ],
            },
            {
                "engine_id": "C2_INACTIVE_DAILY_SNAPSHOT_V1",
                "can_generate_intents": True,
                "required_inputs": [
                    {
                        "name": "market_data_daily_snapshot",
                        "truth_owner": "sleeve_truth_root",
                        "required_scope": "allowed_symbols",
                        "expected_artifact_type": "json",
                        "path_template": convergence_tool.DAILY_MARKET_SNAPSHOT_TEMPLATE,
                        "required": True,
                    }
                ],
            },
        ],
    )

    diagnostics = convergence_tool._required_daily_market_snapshot_symbols_v1(repo_root=tmp_path)

    assert diagnostics["required_snapshot_symbols"] == ["TLT"]
    assert diagnostics["required_snapshot_engines"][0]["engine_id"] == "C2_DEFENSIVE_TAIL_V1"
    assert "SPY" not in diagnostics["required_snapshot_symbols"]
    assert "GLD" not in diagnostics["required_snapshot_symbols"]


def test_all_active_engine_required_daily_snapshot_symbols_are_included(tmp_path: Path) -> None:
    _write_registry_files(
        tmp_path,
        engines=[
            {
                "engine_id": "C2_DEFENSIVE_TAIL_V1",
                "activation_status": "ACTIVE",
                "allowed_symbols": ["TLT"],
            },
            {
                "engine_id": "C2_EVENT_DISLOCATION_V1",
                "activation_status": "ACTIVE",
                "allowed_symbols": ["GLD"],
            },
        ],
        contracts=[
            {
                "engine_id": "C2_DEFENSIVE_TAIL_V1",
                "can_generate_intents": True,
                "required_inputs": [
                    {
                        "name": "market_data_daily_snapshot",
                        "truth_owner": "sleeve_truth_root",
                        "required_scope": "allowed_symbols",
                        "expected_artifact_type": "json",
                        "path_template": convergence_tool.DAILY_MARKET_SNAPSHOT_TEMPLATE,
                        "required": True,
                    }
                ],
            },
            {
                "engine_id": "C2_EVENT_DISLOCATION_V1",
                "can_generate_intents": True,
                "required_inputs": [
                    {
                        "name": "market_data_daily_snapshot",
                        "truth_owner": "sleeve_truth_root",
                        "required_scope": "allowed_symbols",
                        "expected_artifact_type": "json",
                        "path_template": convergence_tool.DAILY_MARKET_SNAPSHOT_TEMPLATE,
                        "required": True,
                    }
                ],
            },
        ],
    )

    diagnostics = convergence_tool._required_daily_market_snapshot_symbols_v1(repo_root=tmp_path)

    assert diagnostics["required_snapshot_symbols"] == ["GLD", "TLT"]


def test_daily_market_snapshot_convergence_fails_closed_when_only_spy_exists(tmp_path: Path) -> None:
    sleeve_truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    spy_snapshot = (
        sleeve_truth_root
        / "market_data_snapshot_v1"
        / "snapshots"
        / "2026-05-12"
        / "SPY.market_data_snapshot.v1.json"
    )
    spy_snapshot.parent.mkdir(parents=True, exist_ok=True)
    spy_snapshot.write_text(
        json.dumps({"schema_id": "market_data_snapshot_v1", "day_utc": "2026-05-12", "status": "OK"}),
        encoding="utf-8",
    )

    aggregate, materialized, missing, per_symbol = convergence_tool._daily_market_snapshot_convergence_result(
        sleeve_truth_root=sleeve_truth_root,
        target_day="2026-05-12",
        required_symbols=["TLT"],
    )

    assert aggregate["ready"] is False
    assert aggregate["blocker_code"] == "MARKET_DATA_SNAPSHOT_V1_MISSING_SYMBOLS"
    assert aggregate["required_snapshot_symbols"] == ["TLT"]
    assert materialized == []
    assert missing == ["TLT"]
    assert per_symbol[0]["artifact_id"] == "market_data_snapshot_v1:TLT"


def test_convergence_tool_materializes_registry_required_daily_snapshots(tmp_path: Path) -> None:
    decision_truth_root = (tmp_path / "truth").resolve()
    sleeve_truth_root = (tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER").resolve()
    sleeve_truth_root.mkdir(parents=True, exist_ok=True)
    captured: dict[str, object] = {}
    tool_calls: list[tuple[str, tuple[str, ...]]] = []

    def fake_module_result(module_name: str, *args: str, extra_env=None):  # noqa: ANN001
        return {
            "script": module_name,
            "command": [module_name, *args],
            "return_code": 0,
            "stdout": "",
            "stderr": "",
        }

    def fake_tool_result(script_relpath: str, *args: str, extra_env=None):  # noqa: ANN001
        tool_calls.append((script_relpath, tuple(args)))
        return {
            "script": script_relpath,
            "command": [script_relpath, *args],
            "return_code": 0,
            "stdout": "",
            "stderr": "",
        }

    def fake_derive(**kwargs):  # noqa: ANN003
        captured["symbol_diagnostics"] = kwargs["symbol_diagnostics"]
        captured["artifact_results"] = kwargs["artifact_results"]
        return {"convergence_status": "SUCCESS"}

    with patch.object(convergence_tool, "resolve_decision_truth_root_v1", return_value=decision_truth_root):
        with patch.object(
            convergence_tool,
            "_resolve_primary_binding",
            return_value=SimpleNamespace(sleeve_id="PRIMARY", truth_root=str(sleeve_truth_root)),
        ):
            with patch.object(
                convergence_tool,
                "read_or_evaluate_trading_day_readiness_authority_v1",
                return_value=(decision_truth_root / "readiness.json", {"readiness_mode": "REGULAR_SESSION"}),
            ):
                with patch.object(
                    convergence_tool,
                    "_required_daily_market_snapshot_symbols_v1",
                    return_value={
                        "required_snapshot_symbols": ["TLT"],
                        "source_registry_paths": ["ENGINE_MODEL_REGISTRY_V1", "SLEEVE_CONTRACTS_V1"],
                        "source_registry_hashes": {},
                        "required_snapshot_engines": [{"engine_id": "C2_DEFENSIVE_TAIL_V1", "required_symbols": ["TLT"]}],
                    },
                ):
                    with patch.object(convergence_tool, "_git_sha", return_value="d" * 40):
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
                                                "2026-05-12",
                                                "--truth_root",
                                                str(decision_truth_root),
                                                "--environment",
                                                "PAPER",
                                                "--ib_account",
                                                "DUO847203",
                                                "--bridge_symbol",
                                                "SPY",
                                            ]
                                        )

    assert rc == 0
    assert (
        "constellation_2/phaseJ/tools/build_defensive_tail_required_inputs_day_v1.py",
        ("--day_utc", "2026-05-12", "--symbol", "TLT"),
    ) in tool_calls
    assert (
        "constellation_2/phaseJ/tools/build_defensive_tail_required_inputs_day_v1.py",
        ("--day_utc", "2026-05-12", "--symbol", "SPY"),
    ) not in tool_calls
    symbol_diagnostics = captured["symbol_diagnostics"]
    assert isinstance(symbol_diagnostics, dict)
    assert symbol_diagnostics["required_snapshot_symbols"] == ["TLT"]
    assert symbol_diagnostics["bridge_symbol_used_for_market_snapshot"] is False
    assert symbol_diagnostics["bridge_symbol_source"] == "DEPRECATED_ARGUMENT_IGNORED"


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
