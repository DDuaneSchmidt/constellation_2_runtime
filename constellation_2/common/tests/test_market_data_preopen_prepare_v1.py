from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import ops.tools.run_c2_market_data_preopen_prepare_v1 as preopen_module
from constellation_2.common.engine_universe_v1 import UniverseCandidateBasis


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True) + "\n", encoding="utf-8")


class MarketDataPreopenPrepareV1Tests(unittest.TestCase):
    def test_preopen_uses_persisted_pass_artifacts_when_refresh_child_rc_is_stale(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            truth_root = root / "truth"
            engine_registry = root / "ENGINE_MODEL_REGISTRY_V1.json"
            sleeve_registry = root / "C2_SLEEVE_REGISTRY_V1.json"
            policy_registry = root / "ENGINE_UNIVERSE_POLICY_V1.json"

            _write_json(engine_registry, {"engines": [{"engine_id": "CURATED_A", "activation_status": "ACTIVE"}]})
            _write_json(
                sleeve_registry,
                {"sleeves": [{"sleeve_id": "PRIMARY", "mode": "PAPER", "assigned_engine_ids": ["CURATED_A"]}]},
            )
            _write_json(
                policy_registry,
                {
                    "policies": [
                        {
                            "engine_id": "CURATED_A",
                            "universe_mode": "CURATED_SYMBOLS",
                            "symbol_source_class": "GOVERNED_CURATED",
                            "curated_symbols": ["SPY"],
                        }
                    ]
                },
            )
            _write_json(
                truth_root / "reports" / "canonical_market_data_refresh_v1" / "2026-03-25" / "canonical_market_data_refresh.v1.json",
                {"status": "PASS"},
            )
            _write_json(
                truth_root / "reports" / "market_data_truth_spine_verification_v1" / "2026-03-25" / "market_data_truth_spine_verification.v1.json",
                {"status": "PASS"},
            )

            class _RunResult:
                def __init__(self, returncode: int, stdout: str = "", stderr: str = "") -> None:
                    self.returncode = returncode
                    self.stdout = stdout
                    self.stderr = stderr

            def fake_run(cmd):
                if "run_c2_canonical_market_data_refresh_v1.py" in cmd:
                    return _RunResult(2, json.dumps({"status": "FAIL", "reason_codes": ["DYNAMIC_UNIVERSE_REFRESH_FAILED"]}))
                if "verify_market_data_truth_spine_v1.py" in cmd:
                    return _RunResult(0, json.dumps({"status": "PASS"}))
                return _RunResult(0, "")

            with patch.object(preopen_module, "active_controllable_runtime_engine_ids_for_sleeve", return_value=["CURATED_A"]), \
                patch.object(preopen_module, "POLICY_REGISTRY_PATH", policy_registry), \
                patch.object(preopen_module, "_run_cmd", side_effect=fake_run), \
                patch.object(
                    sys,
                    "argv",
                    [
                        "run_c2_market_data_preopen_prepare_v1.py",
                        "--day_utc",
                        "2026-03-25",
                        "--truth_root",
                        str(truth_root),
                    ],
                ):
                rc = preopen_module.main()

            self.assertEqual(rc, 0)

    def test_preopen_uses_same_day_ranked_basis_when_available(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            truth_root = root / "truth"
            engine_registry = root / "ENGINE_MODEL_REGISTRY_V1.json"
            sleeve_registry = root / "C2_SLEEVE_REGISTRY_V1.json"
            policy_registry = root / "ENGINE_UNIVERSE_POLICY_V1.json"

            _write_json(
                engine_registry,
                {"engines": [{"engine_id": "RANKED_A", "activation_status": "ACTIVE"}]},
            )
            _write_json(
                sleeve_registry,
                {"sleeves": [{"sleeve_id": "PRIMARY", "mode": "PAPER", "assigned_engine_ids": ["RANKED_A"]}]},
            )
            _write_json(
                policy_registry,
                {
                    "policies": [
                        {
                            "engine_id": "RANKED_A",
                            "universe_mode": "LIQUIDITY_RANKED_SYMBOLS",
                            "symbol_source_class": "DYNAMIC_SAME_DAY",
                            "same_day_symbol_basis_required": True,
                            "fallback_allowed": False,
                            "curated_symbols": [],
                        }
                    ]
                },
            )

            same_day_basis = UniverseCandidateBasis(
                engine_id="RANKED_A",
                policy_id="RANKED_A",
                day_utc="2026-03-25",
                basis_day_utc="2026-03-25",
                basis_mode="SAME_DAY_ELIGIBLE_SYMBOLS",
                configured_rule={},
                symbols_considered=["QQQ", "SPY"],
                candidate_symbols=["QQQ", "SPY"],
                exclusions_by_reason=[],
                notes=["CANDIDATE_BASIS_FROM_SAME_DAY_ELIGIBLE_SYMBOLS"],
            )

            with patch.object(preopen_module, "active_controllable_runtime_engine_ids_for_sleeve", return_value=["RANKED_A"]), \
                patch.object(preopen_module, "POLICY_REGISTRY_PATH", policy_registry), \
                patch.object(preopen_module, "resolve_engine_candidate_basis", return_value=same_day_basis), \
                patch.object(preopen_module, "_run_cmd", return_value=type("RunResult", (), {"returncode": 0, "stdout": "", "stderr": ""})()), \
                patch.object(
                    sys,
                    "argv",
                    [
                        "run_c2_market_data_preopen_prepare_v1.py",
                        "--day_utc",
                        "2026-03-25",
                        "--truth_root",
                        str(truth_root),
                    ],
                ):
                rc = preopen_module.main()

            self.assertEqual(rc, 0)

    def test_preopen_fails_closed_when_ranked_same_day_basis_is_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            truth_root = root / "truth"
            engine_registry = root / "ENGINE_MODEL_REGISTRY_V1.json"
            sleeve_registry = root / "C2_SLEEVE_REGISTRY_V1.json"
            policy_registry = root / "ENGINE_UNIVERSE_POLICY_V1.json"

            _write_json(engine_registry, {"engines": [{"engine_id": "RANKED_A", "activation_status": "ACTIVE"}]})
            _write_json(
                sleeve_registry,
                {"sleeves": [{"sleeve_id": "PRIMARY", "mode": "PAPER", "assigned_engine_ids": ["RANKED_A"]}]},
            )
            _write_json(
                policy_registry,
                {
                    "policies": [
                        {
                            "engine_id": "RANKED_A",
                            "universe_mode": "LIQUIDITY_RANKED_SYMBOLS",
                            "symbol_source_class": "DYNAMIC_SAME_DAY",
                            "same_day_symbol_basis_required": True,
                            "fallback_allowed": False,
                            "curated_symbols": [],
                        }
                    ]
                },
            )

            with patch.object(preopen_module, "active_controllable_runtime_engine_ids_for_sleeve", return_value=["RANKED_A"]), \
                patch.object(preopen_module, "POLICY_REGISTRY_PATH", policy_registry), \
                patch.object(
                    preopen_module,
                    "resolve_engine_candidate_basis",
                    side_effect=preopen_module.EngineUniverseError("CANDIDATE_BASIS_SAME_DAY_REQUIRED:RANKED_A:2026-03-25"),
                ), \
                patch.object(
                    sys,
                    "argv",
                    [
                        "run_c2_market_data_preopen_prepare_v1.py",
                        "--day_utc",
                        "2026-03-25",
                        "--truth_root",
                        str(truth_root),
                    ],
                ):
                rc = preopen_module.main()

            self.assertEqual(rc, 2)

    def test_preopen_fails_closed_on_no_intents_day_when_ranked_same_day_basis_is_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            truth_root = root / "truth"
            engine_registry = root / "ENGINE_MODEL_REGISTRY_V1.json"
            sleeve_registry = root / "C2_SLEEVE_REGISTRY_V1.json"
            policy_registry = root / "ENGINE_UNIVERSE_POLICY_V1.json"

            _write_json(engine_registry, {"engines": [{"engine_id": "RANKED_A", "activation_status": "ACTIVE"}]})
            _write_json(
                sleeve_registry,
                {"sleeves": [{"sleeve_id": "PRIMARY", "mode": "PAPER", "assigned_engine_ids": ["RANKED_A"]}]},
            )
            _write_json(
                policy_registry,
                {
                    "policies": [
                        {
                            "engine_id": "RANKED_A",
                            "universe_mode": "LIQUIDITY_RANKED_SYMBOLS",
                            "symbol_source_class": "DYNAMIC_SAME_DAY",
                            "same_day_symbol_basis_required": True,
                            "fallback_allowed": False,
                            "curated_symbols": [],
                        }
                    ]
                },
            )
            _write_json(
                truth_root / "intents_v1" / "snapshots" / "2026-03-25" / "no_intents_day.v1.json",
                {"schema_id": "no_intents_day", "day_utc": "2026-03-25", "status": "OK"},
            )

            with patch.object(preopen_module, "active_controllable_runtime_engine_ids_for_sleeve", return_value=["RANKED_A"]), \
                patch.object(preopen_module, "POLICY_REGISTRY_PATH", policy_registry), \
                patch.object(
                    preopen_module,
                    "resolve_engine_candidate_basis",
                    side_effect=preopen_module.EngineUniverseError("CANDIDATE_BASIS_SAME_DAY_REQUIRED:RANKED_A:2026-03-25"),
                ), \
                patch.object(
                    sys,
                    "argv",
                    [
                        "run_c2_market_data_preopen_prepare_v1.py",
                        "--day_utc",
                        "2026-03-25",
                        "--truth_root",
                        str(truth_root),
                    ],
                ):
                rc = preopen_module.main()

            self.assertEqual(rc, 2)

    def test_preopen_fails_closed_without_using_same_day_intent_symbols_when_ranked_basis_is_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            truth_root = root / "truth"
            engine_registry = root / "ENGINE_MODEL_REGISTRY_V1.json"
            sleeve_registry = root / "C2_SLEEVE_REGISTRY_V1.json"
            policy_registry = root / "ENGINE_UNIVERSE_POLICY_V1.json"

            _write_json(engine_registry, {"engines": [{"engine_id": "RANKED_A", "activation_status": "ACTIVE"}]})
            _write_json(
                sleeve_registry,
                {"sleeves": [{"sleeve_id": "PRIMARY", "mode": "PAPER", "assigned_engine_ids": ["RANKED_A"]}]},
            )
            _write_json(
                policy_registry,
                {
                    "policies": [
                        {
                            "engine_id": "RANKED_A",
                            "universe_mode": "LIQUIDITY_RANKED_SYMBOLS",
                            "symbol_source_class": "DYNAMIC_SAME_DAY",
                            "same_day_symbol_basis_required": True,
                            "fallback_allowed": False,
                            "curated_symbols": [],
                        }
                    ]
                },
            )
            _write_json(
                truth_root / "intents_v1" / "snapshots" / "2026-03-25" / "intent.exposure_intent.v1.json",
                {
                    "schema_id": "exposure_intent",
                    "engine": {"engine_id": "RANKED_A"},
                    "underlying": {"symbol": "SPY"},
                },
            )
            _write_json(
                truth_root / "intents_v1" / "snapshots" / "2026-03-25" / "intent.exposure_intent.v1.json.dependency_meta.v1.json",
                {
                    "schema_id": "C2_DEPENDENCY_METADATA_V1",
                    "engine_id": "RANKED_A",
                },
            )

            with patch.object(preopen_module, "active_controllable_runtime_engine_ids_for_sleeve", return_value=["RANKED_A"]), \
                patch.object(preopen_module, "POLICY_REGISTRY_PATH", policy_registry), \
                patch.object(
                    preopen_module,
                    "resolve_engine_candidate_basis",
                    side_effect=preopen_module.EngineUniverseError("CANDIDATE_BASIS_SAME_DAY_REQUIRED:RANKED_A:2026-03-25"),
                ), \
                patch.object(
                    sys,
                    "argv",
                    [
                        "run_c2_market_data_preopen_prepare_v1.py",
                        "--day_utc",
                        "2026-03-25",
                        "--truth_root",
                        str(truth_root),
                    ],
                ):
                rc = preopen_module.main()

            self.assertEqual(rc, 2)

    def test_preopen_fails_closed_without_using_curated_symbols_when_ranked_basis_missing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            truth_root = root / "truth"
            policy_registry = root / "ENGINE_UNIVERSE_POLICY_V1.json"

            _write_json(
                policy_registry,
                {
                    "policies": [
                        {
                            "engine_id": "RANKED_A",
                            "universe_mode": "LIQUIDITY_RANKED_SYMBOLS",
                            "symbol_source_class": "DYNAMIC_SAME_DAY",
                            "same_day_symbol_basis_required": True,
                            "fallback_allowed": False,
                            "curated_symbols": [],
                        },
                        {
                            "engine_id": "CURATED_OPTIONS",
                            "universe_mode": "CURATED_SYMBOLS",
                            "symbol_source_class": "GOVERNED_CURATED",
                            "curated_symbols": ["GLD", "HYG", "IWM", "QQQ", "SPY", "TLT"],
                        },
                    ]
                },
            )

            class _RunResult:
                def __init__(self, returncode: int, stdout: str = "", stderr: str = "") -> None:
                    self.returncode = returncode
                    self.stdout = stdout
                    self.stderr = stderr

            captured_cmds: list[list[str]] = []

            def fake_run(cmd):
                captured_cmds.append(list(cmd))
                if any("run_ranked_engine_candidate_basis_day_v1.py" in part for part in cmd):
                    return _RunResult(
                        2,
                        json.dumps(
                            {
                                "name": "ranked_engine_candidate_basis_day_v1",
                                "status": "FAIL",
                                "failures": ["RANKED_A"],
                            }
                        ),
                    )
                return _RunResult(0, "")

            with patch.object(
                preopen_module,
                "active_controllable_runtime_engine_ids_for_sleeve",
                return_value=["RANKED_A", "CURATED_OPTIONS"],
            ), patch.object(preopen_module, "POLICY_REGISTRY_PATH", policy_registry), patch.object(
                preopen_module,
                "resolve_engine_candidate_basis",
                side_effect=preopen_module.EngineUniverseError("CANDIDATE_BASIS_SAME_DAY_REQUIRED:RANKED_A:2026-03-25"),
            ), patch.object(preopen_module, "_run_cmd", side_effect=fake_run), patch.object(
                sys,
                "argv",
                [
                    "run_c2_market_data_preopen_prepare_v1.py",
                    "--day_utc",
                    "2026-03-25",
                    "--truth_root",
                    str(truth_root),
                ],
            ):
                rc = preopen_module.main()

            self.assertEqual(rc, 2)
            self.assertEqual(len(captured_cmds), 1)
            self.assertTrue(any("run_ranked_engine_candidate_basis_day_v1.py" in part for part in captured_cmds[0]))

    def test_preopen_non_trading_expected_no_op_allows_ranked_basis_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            truth_root = root / "truth"
            policy_registry = root / "ENGINE_UNIVERSE_POLICY_V1.json"

            _write_json(
                policy_registry,
                {
                    "policies": [
                        {
                            "engine_id": "RANKED_A",
                            "universe_mode": "LIQUIDITY_RANKED_SYMBOLS",
                            "symbol_source_class": "DYNAMIC_SAME_DAY",
                            "same_day_symbol_basis_required": True,
                            "fallback_allowed": False,
                            "curated_symbols": [],
                        }
                    ]
                },
            )
            _write_json(
                truth_root / "reports" / "paper_trading_posture_v1" / "2026-03-25" / "paper_trading_posture.v1.json",
                {
                    "posture_class": "PAPER_READY_NO_OP",
                    "blocking_family": "EXPECTED_NO_OP",
                    "blocking_reason_codes": ["MARKET_CALENDAR_NON_TRADING_SESSION"],
                    "expected_no_op_today": True,
                },
            )

            class _RunResult:
                def __init__(self, returncode: int, stdout: str = "", stderr: str = "") -> None:
                    self.returncode = returncode
                    self.stdout = stdout
                    self.stderr = stderr

            def fake_run(cmd):
                if any("run_ranked_engine_candidate_basis_day_v1.py" in part for part in cmd):
                    return _RunResult(
                        2,
                        json.dumps(
                            {
                                "name": "ranked_engine_candidate_basis_day_v1",
                                "status": "FAIL",
                                "failures": ["RANKED_A"],
                            }
                        ),
                    )
                return _RunResult(0, "")

            with patch.object(
                preopen_module,
                "active_controllable_runtime_engine_ids_for_sleeve",
                return_value=["RANKED_A"],
            ), patch.object(
                preopen_module, "POLICY_REGISTRY_PATH", policy_registry
            ), patch.object(
                preopen_module, "_run_cmd", side_effect=fake_run
            ), patch.object(
                sys,
                "argv",
                [
                    "run_c2_market_data_preopen_prepare_v1.py",
                    "--day_utc",
                    "2026-03-25",
                    "--truth_root",
                    str(truth_root),
                ],
            ):
                rc = preopen_module.main()

            self.assertEqual(rc, 0)


if __name__ == "__main__":
    unittest.main()
