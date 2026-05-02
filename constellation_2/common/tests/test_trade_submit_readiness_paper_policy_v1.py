from __future__ import annotations

import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_trade_submit_readiness_c2_v1 as readiness_module


DAY = "2026-04-13"
ACCOUNT = "DUO847203"
ENV = "PAPER"


def test_day_authority_constitutional_ref_builder_requires_complete_ref() -> None:
    with pytest.raises(ValueError, match="DAY_AUTHORITY_DECISION_REF_MISSING"):
        readiness_module._build_day_authority_constitutional_ref(
            day_utc=DAY,
            day_authority_path=Path(),
            day_authority_sha256=None,
        )


def test_day_authority_constitutional_ref_builder_emits_artifact_id_path_sha256() -> None:
    ref = readiness_module._build_day_authority_constitutional_ref(
        day_utc=DAY,
        day_authority_path=Path("/tmp/day_authority_decision.v1.json"),
        day_authority_sha256="a" * 64,
    )

    assert ref["artifact_id"] == "day_authority_decision_v1"
    assert ref["path"] == "/tmp/day_authority_decision.v1.json"
    assert ref["sha256"] == "a" * 64
    assert ref["artifact_class"] == "admission_result"
    assert ref["finality_state"] == "provisional"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _write_intraday_readiness_authority(truth_root: Path) -> tuple[Path, dict]:
    path = (
        truth_root
        / "reports"
        / "trading_day_readiness_authority_v1"
        / DAY
        / "trading_day_readiness_authority.v1.json"
    )
    payload = {
        "schema_id": "C2_TRADING_DAY_READINESS_AUTHORITY_V1",
        "schema_version": 1,
        "target_day": DAY,
        "readiness_mode": "INTRADAY_SUBMIT_READY",
        "submit_allowed_by_mode": True,
        "requires_live_account_truth": True,
    }
    _write_json(path, payload)
    return path, payload


def test_trade_submit_readiness_emits_paper_policy_not_pass_for_startup_materialization_failure() -> None:
    with tempfile.TemporaryDirectory(dir=str(SOURCE_ROOT / "tmp")) as td:
        root = Path(td)
        truth_root = root / "truth"
        execution_truth_root = root / "truth_sleeves" / "PRIMARY" / ENV

        capability_path = truth_root / "reports" / "capability_state_v1" / DAY / "capability_state.v1.json"
        paper_policy_path = truth_root / "reports" / "paper_policy_verdict_v1" / DAY / "paper_policy_verdict.v1.json"
        production_policy_path = truth_root / "reports" / "production_policy_verdict_v1" / DAY / "production_policy_verdict.v1.json"
        policy_diff_path = truth_root / "reports" / "policy_diff_v1" / DAY / "policy_diff.v1.json"
        day_authority_path = truth_root / "reports" / "day_authority_decision_v1" / DAY / "day_authority_decision.v1.json"
        handshake_pointer_path = execution_truth_root / "ib_api_handshake" / "latest_pointer.v1.json"
        handshake_path = execution_truth_root / "ib_api_handshake" / DAY / "ib_api_handshake.v1.json"
        authorization_path = execution_truth_root / "reports" / "authorization_gate_verdict_v1" / DAY / "authorization_gate_verdict.v1.json"

        _write_json(capability_path, {"schema_id": "capability_state", "schema_version": "v1", "day_utc": DAY})
        _write_json(
            paper_policy_path,
            {
                "schema_id": "paper_policy_verdict",
                "schema_version": "v1",
                "day_utc": DAY,
                "overall_status": "FAIL",
                "blocking_items": [{"capability_id": "startup_materialization_ready", "status": "FAIL"}],
            },
        )
        _write_json(
            production_policy_path,
            {
                "schema_id": "production_policy_verdict",
                "schema_version": "v1",
                "day_utc": DAY,
                "overall_status": "FAIL",
            },
        )
        _write_json(
            policy_diff_path,
            {
                "schema_id": "policy_diff",
                "schema_version": "v1",
                "day_utc": DAY,
                "production_only_open_items": [{"capability_id": "production_certification_gate_set_complete"}],
            },
        )
        _write_json(
            day_authority_path,
            {
                "schema_id": "day_authority_decision",
                "schema_version": "v1",
                "day_utc": DAY,
                "decision_state": "OPEN",
                "stage": "PRE_ORCHESTRATION_PREFLIGHT",
            },
        )
        _write_json(
            handshake_pointer_path,
            {
                "schema_id": "C2_IB_API_HANDSHAKE_LATEST_POINTER_V1",
                "schema_version": 1,
                "day_utc": DAY,
                "pointers": {"handshake_path": str(handshake_path), "handshake_sha256": "a" * 64},
            },
        )
        _write_json(handshake_path, {"schema_id": "C2_IB_API_HANDSHAKE_V1", "schema_version": 1, "day_utc": DAY})
        _write_json(
            authorization_path,
            {
                "schema_id": "authorization_gate_verdict_v1",
                "schema_version": 1,
                "day_utc": DAY,
                "status": "PASS",
            },
        )

        history_path = execution_truth_root / "trade_submit_readiness_c2_v1" / "_history" / ENV / ACCOUNT / DAY / "status.json"
        fixed_now = datetime(2026, 4, 13, 14, 30, 0, tzinfo=timezone.utc)
        readiness_ref = _write_intraday_readiness_authority(truth_root)

        with patch.object(readiness_module, "REPO_ROOT", root), patch.object(readiness_module, "TRUTH_ROOT", truth_root), patch.object(
            readiness_module,
            "resolve_governed_account_binding",
            return_value=SimpleNamespace(
                account_registry_sha256="a" * 64,
                sleeve_registry_sha256="b" * 64,
                account_registry_path=root / "governance" / "02_REGISTRIES" / "C2_IB_ACCOUNT_REGISTRY_V1.json",
                sleeve_registry_path=root / "governance" / "02_REGISTRIES" / "C2_SLEEVE_REGISTRY_V1.json",
            ),
        ), patch.object(
            readiness_module,
            "resolve_sleeve_execution_root_v1",
            return_value=SimpleNamespace(execution_root_path=execution_truth_root),
        ), patch.object(
            readiness_module,
            "resolve_pointer_bound_handshake_state",
            return_value=SimpleNamespace(
                pointer_path=handshake_pointer_path,
                pointer_sha256="c" * 64,
                handshake_path=handshake_path,
                handshake_sha256="d" * 64,
            ),
        ), patch.object(
            readiness_module,
            "_refresh_authorization_convergence_for_day",
            return_value=0,
        ), patch.object(
            readiness_module,
            "_load_primary_scoped_authorization_snapshot",
            return_value={
                "binding": SimpleNamespace(sleeve_id="PRIMARY"),
                "authorization_path": authorization_path,
                "authorization_sha256": "e" * 64,
                "authorization_status": "PASS",
                "input_manifest": [],
            },
        ), patch.object(
            readiness_module,
            "_refresh_policy_stack_for_day",
            lambda **kwargs: None,
        ), patch.object(
            readiness_module,
            "_load_day_authority",
            return_value=(
                {"decision_state": "OPEN", "stage": "PRE_ORCHESTRATION_PREFLIGHT"},
                day_authority_path,
                "f" * 64,
            ),
        ), patch.object(
            readiness_module,
            "read_or_evaluate_trading_day_readiness_authority_v1",
            return_value=readiness_ref,
        ), patch.object(
            readiness_module,
            "validate_against_repo_schema_v1",
            lambda *args, **kwargs: None,
        ), patch.object(
            readiness_module,
            "validate_trade_submit_readiness_status_obj",
            lambda *args, **kwargs: None,
        ), patch.object(
            readiness_module,
            "_now_utc",
            return_value=fixed_now,
        ), patch(
            "sys.argv",
            ["run_trade_submit_readiness_c2_v1.py", "--day_utc", DAY, "--ib_account", ACCOUNT, "--environment", ENV],
        ):
            rc = readiness_module.main()

        assert rc == 2
        status = json.loads(history_path.read_text(encoding="utf-8"))
        assert status["ok"] is False
        assert "FAIL:PAPER_POLICY_NOT_PASS" in status["reasons"]
        assert "FAIL:PAPER_POLICY_NOT_PASS:startup_materialization_ready:status=FAIL" in status["reasons"]
        assert "PAPER_POLICY_VERDICT_OK" not in status["reasons"]
        assert status["as_of_utc"] == "2026-04-13T14:30:00Z"
        assert status["expires_utc"] == "2026-04-13T14:45:00Z"
        assert status["expires_utc"] > status["as_of_utc"]
        assert status["expires_utc"] != f"{DAY}T00:02:00Z"
        assert status["constitutional_lineage"]["artifact_type"] == "trade_submit_readiness_c2_v1"
        assert isinstance(status["constitutional_dependency_declaration"]["declared_dependency_artifacts"], list)


def test_trade_submit_readiness_blocks_on_previous_day_bundle_c_drawdown() -> None:
    with tempfile.TemporaryDirectory(dir=str(SOURCE_ROOT / "tmp")) as td:
        root = Path(td)
        truth_root = root / "truth"
        execution_truth_root = root / "truth_sleeves" / "PRIMARY" / ENV
        prev_day = readiness_module._prior_trading_day_utc(DAY)

        capability_path = truth_root / "reports" / "capability_state_v1" / DAY / "capability_state.v1.json"
        paper_policy_path = truth_root / "reports" / "paper_policy_verdict_v1" / DAY / "paper_policy_verdict.v1.json"
        production_policy_path = truth_root / "reports" / "production_policy_verdict_v1" / DAY / "production_policy_verdict.v1.json"
        policy_diff_path = truth_root / "reports" / "policy_diff_v1" / DAY / "policy_diff.v1.json"
        day_authority_path = truth_root / "reports" / "day_authority_decision_v1" / DAY / "day_authority_decision.v1.json"
        handshake_pointer_path = execution_truth_root / "ib_api_handshake" / "latest_pointer.v1.json"
        handshake_path = execution_truth_root / "ib_api_handshake" / DAY / "ib_api_handshake.v1.json"
        authorization_path = execution_truth_root / "reports" / "authorization_gate_verdict_v1" / DAY / "authorization_gate_verdict.v1.json"
        economic_build_path = truth_root / "reports" / "economic_state_build_v1" / prev_day / "ctx-test" / "economic_state_build.v1.json"
        economic_package_path = execution_truth_root / "economic_state_package_v1" / prev_day / "ctx-test" / "economic_state_package.v1.json"

        _write_json(capability_path, {"schema_id": "capability_state", "schema_version": "v1", "day_utc": DAY})
        _write_json(
            paper_policy_path,
            {
                "schema_id": "paper_policy_verdict",
                "schema_version": "v1",
                "day_utc": DAY,
                "overall_status": "PASS",
                "blocking_items": [],
            },
        )
        _write_json(
            production_policy_path,
            {
                "schema_id": "production_policy_verdict",
                "schema_version": "v1",
                "day_utc": DAY,
                "overall_status": "PASS",
            },
        )
        _write_json(
            policy_diff_path,
            {
                "schema_id": "policy_diff",
                "schema_version": "v1",
                "day_utc": DAY,
                "production_only_open_items": [],
            },
        )
        _write_json(
            day_authority_path,
            {
                "schema_id": "day_authority_decision",
                "schema_version": "v1",
                "day_utc": DAY,
                "decision_state": "OPEN",
                "stage": "PRE_ORCHESTRATION_PREFLIGHT",
            },
        )
        _write_json(
            handshake_pointer_path,
            {
                "schema_id": "C2_IB_API_HANDSHAKE_LATEST_POINTER_V1",
                "schema_version": 1,
                "day_utc": DAY,
                "pointers": {"handshake_path": str(handshake_path), "handshake_sha256": "a" * 64},
            },
        )
        _write_json(handshake_path, {"schema_id": "C2_IB_API_HANDSHAKE_V1", "schema_version": 1, "day_utc": DAY})
        _write_json(
            authorization_path,
            {
                "schema_id": "authorization_gate_verdict_v1",
                "schema_version": 1,
                "day_utc": DAY,
                "status": "PASS",
            },
        )
        _write_json(
            economic_build_path,
            {
                "schema_id": "economic_state_build",
                "schema_version": "v1",
                "day_utc": prev_day,
                "closure_status": "COMPLETE",
                "economic_evaluation": {
                    "benchmark_state": {
                        "policy_baseline": {"comparison_vs_portfolio_return": "-0.020000"},
                        "external_benchmarks": [
                            {"benchmark_id": "SPY", "comparison_vs_portfolio_return": "-0.015000"}
                        ],
                    },
                    "risk_state": {"drawdown_pct": "-0.120000"},
                },
            },
        )
        _write_json(
            economic_package_path,
            {
                "schema_id": "economic_state_package",
                "schema_version": "v1",
                "day_utc": prev_day,
                "sealed": True,
                "build_ref": {"path": str(economic_build_path), "sha256": "b" * 64},
            },
        )

        history_path = execution_truth_root / "trade_submit_readiness_c2_v1" / "_history" / ENV / ACCOUNT / DAY / "status.json"
        readiness_ref = _write_intraday_readiness_authority(truth_root)

        with patch.object(readiness_module, "REPO_ROOT", root), patch.object(readiness_module, "TRUTH_ROOT", truth_root), patch.object(
            readiness_module,
            "resolve_governed_account_binding",
            return_value=SimpleNamespace(
                account_registry_sha256="a" * 64,
                sleeve_registry_sha256="b" * 64,
                account_registry_path=root / "governance" / "02_REGISTRIES" / "C2_IB_ACCOUNT_REGISTRY_V1.json",
                sleeve_registry_path=root / "governance" / "02_REGISTRIES" / "C2_SLEEVE_REGISTRY_V1.json",
            ),
        ), patch.object(
            readiness_module,
            "resolve_sleeve_execution_root_v1",
            return_value=SimpleNamespace(execution_root_path=execution_truth_root),
        ), patch.object(
            readiness_module,
            "resolve_pointer_bound_handshake_state",
            return_value=SimpleNamespace(
                pointer_path=handshake_pointer_path,
                pointer_sha256="c" * 64,
                handshake_path=handshake_path,
                handshake_sha256="d" * 64,
            ),
        ), patch.object(
            readiness_module,
            "_refresh_authorization_convergence_for_day",
            return_value=0,
        ), patch.object(
            readiness_module,
            "_load_primary_scoped_authorization_snapshot",
            return_value={
                "binding": SimpleNamespace(sleeve_id="PRIMARY"),
                "authorization_path": authorization_path,
                "authorization_sha256": "e" * 64,
                "authorization_status": "PASS",
                "input_manifest": [],
            },
        ), patch.object(
            readiness_module,
            "_refresh_policy_stack_for_day",
            lambda **kwargs: None,
        ), patch.object(
            readiness_module,
            "_load_day_authority",
            return_value=(
                {"decision_state": "OPEN", "stage": "PRE_ORCHESTRATION_PREFLIGHT"},
                day_authority_path,
                "f" * 64,
            ),
        ), patch.object(
            readiness_module,
            "read_or_evaluate_trading_day_readiness_authority_v1",
            return_value=readiness_ref,
        ), patch.object(
            readiness_module,
            "_load_previous_day_economic_package_state",
            return_value={
                "status": "OK",
                "source_day_utc": prev_day,
                "package_path": str(economic_package_path),
                "package_sha256": "c" * 64,
                "build_path": str(economic_build_path),
                "build_sha256": "d" * 64,
                "drawdown_pct": "-0.120000",
                "drawdown_guard_status": "BLOCKED",
                "policy_baseline_comparison_vs_portfolio_return": "-0.020000",
                "external_benchmark_underperformer_count": 1,
                "reason_codes": [],
            },
        ), patch.object(
            readiness_module,
            "validate_against_repo_schema_v1",
            lambda *args, **kwargs: None,
        ), patch.object(
            readiness_module,
            "validate_trade_submit_readiness_status_obj",
            lambda *args, **kwargs: None,
        ), patch(
            "sys.argv",
            ["run_trade_submit_readiness_c2_v1.py", "--day_utc", DAY, "--ib_account", ACCOUNT, "--environment", ENV],
        ):
            rc = readiness_module.main()

        assert rc == 2
        status = json.loads(history_path.read_text(encoding="utf-8"))
        assert status["ok"] is False
        assert status["economic_state"]["status"] == "OK"
        assert status["economic_state"]["drawdown_guard_status"] == "BLOCKED"
        assert status["economic_state"]["package_path"] == str(economic_package_path)
        assert "FAIL:BUNDLE_C_DRAWDOWN_LIMIT_EXCEEDED" in status["reasons"]
        assert "INFO:BUNDLE_C_POLICY_BASELINE_UNDERPERFORMANCE" in status["reasons"]


def test_trade_submit_readiness_requires_previous_day_bundle_c_package() -> None:
    with tempfile.TemporaryDirectory(dir=str(SOURCE_ROOT / "tmp")) as td:
        root = Path(td)
        truth_root = root / "truth"
        execution_truth_root = root / "truth_sleeves" / "PRIMARY" / ENV

        capability_path = truth_root / "reports" / "capability_state_v1" / DAY / "capability_state.v1.json"
        paper_policy_path = truth_root / "reports" / "paper_policy_verdict_v1" / DAY / "paper_policy_verdict.v1.json"
        production_policy_path = truth_root / "reports" / "production_policy_verdict_v1" / DAY / "production_policy_verdict.v1.json"
        policy_diff_path = truth_root / "reports" / "policy_diff_v1" / DAY / "policy_diff.v1.json"
        day_authority_path = truth_root / "reports" / "day_authority_decision_v1" / DAY / "day_authority_decision.v1.json"
        handshake_pointer_path = execution_truth_root / "ib_api_handshake" / "latest_pointer.v1.json"
        handshake_path = execution_truth_root / "ib_api_handshake" / DAY / "ib_api_handshake.v1.json"
        authorization_path = execution_truth_root / "reports" / "authorization_gate_verdict_v1" / DAY / "authorization_gate_verdict.v1.json"

        _write_json(capability_path, {"schema_id": "capability_state", "schema_version": "v1", "day_utc": DAY})
        _write_json(
            paper_policy_path,
            {
                "schema_id": "paper_policy_verdict",
                "schema_version": "v1",
                "day_utc": DAY,
                "overall_status": "PASS",
                "blocking_items": [],
            },
        )
        _write_json(
            production_policy_path,
            {
                "schema_id": "production_policy_verdict",
                "schema_version": "v1",
                "day_utc": DAY,
                "overall_status": "PASS",
            },
        )
        _write_json(
            policy_diff_path,
            {
                "schema_id": "policy_diff",
                "schema_version": "v1",
                "day_utc": DAY,
                "production_only_open_items": [],
            },
        )
        _write_json(
            day_authority_path,
            {
                "schema_id": "day_authority_decision",
                "schema_version": "v1",
                "day_utc": DAY,
                "decision_state": "OPEN",
                "stage": "PRE_ORCHESTRATION_PREFLIGHT",
            },
        )
        _write_json(
            handshake_pointer_path,
            {
                "schema_id": "C2_IB_API_HANDSHAKE_LATEST_POINTER_V1",
                "schema_version": 1,
                "day_utc": DAY,
                "pointers": {"handshake_path": str(handshake_path), "handshake_sha256": "a" * 64},
            },
        )
        _write_json(handshake_path, {"schema_id": "C2_IB_API_HANDSHAKE_V1", "schema_version": 1, "day_utc": DAY})
        _write_json(
            authorization_path,
            {
                "schema_id": "authorization_gate_verdict_v1",
                "schema_version": 1,
                "day_utc": DAY,
                "status": "PASS",
            },
        )

        out_dir = execution_truth_root / "trade_submit_readiness_c2_v1" / ENV / ACCOUNT
        history_path = execution_truth_root / "trade_submit_readiness_c2_v1" / "_history" / ENV / ACCOUNT / DAY / "status.json"
        readiness_ref = _write_intraday_readiness_authority(truth_root)

        with patch.object(readiness_module, "REPO_ROOT", root), patch.object(readiness_module, "TRUTH_ROOT", truth_root), patch.object(
            readiness_module,
            "resolve_governed_account_binding",
            return_value=SimpleNamespace(
                account_registry_sha256="a" * 64,
                sleeve_registry_sha256="b" * 64,
                account_registry_path=root / "governance" / "02_REGISTRIES" / "C2_IB_ACCOUNT_REGISTRY_V1.json",
                sleeve_registry_path=root / "governance" / "02_REGISTRIES" / "C2_SLEEVE_REGISTRY_V1.json",
            ),
        ), patch.object(
            readiness_module,
            "resolve_sleeve_execution_root_v1",
            return_value=SimpleNamespace(execution_root_path=execution_truth_root),
        ), patch.object(
            readiness_module,
            "resolve_pointer_bound_handshake_state",
            return_value=SimpleNamespace(
                pointer_path=handshake_pointer_path,
                pointer_sha256="c" * 64,
                handshake_path=handshake_path,
                handshake_sha256="d" * 64,
            ),
        ), patch.object(
            readiness_module,
            "_refresh_authorization_convergence_for_day",
            return_value=0,
        ), patch.object(
            readiness_module,
            "_load_primary_scoped_authorization_snapshot",
            return_value={
                "binding": SimpleNamespace(sleeve_id="PRIMARY"),
                "authorization_path": authorization_path,
                "authorization_sha256": "e" * 64,
                "authorization_status": "PASS",
                "input_manifest": [],
            },
        ), patch.object(
            readiness_module,
            "_refresh_policy_stack_for_day",
            lambda **kwargs: None,
        ), patch.object(
            readiness_module,
            "_load_day_authority",
            return_value=(
                {"decision_state": "OPEN", "stage": "PRE_ORCHESTRATION_PREFLIGHT"},
                day_authority_path,
                "f" * 64,
            ),
        ), patch.object(
            readiness_module,
            "read_or_evaluate_trading_day_readiness_authority_v1",
            return_value=readiness_ref,
        ), patch.object(
            readiness_module,
            "validate_against_repo_schema_v1",
            lambda *args, **kwargs: None,
        ), patch.object(
            readiness_module,
            "validate_trade_submit_readiness_status_obj",
            lambda *args, **kwargs: None,
        ), patch(
            "sys.argv",
            ["run_trade_submit_readiness_c2_v1.py", "--day_utc", DAY, "--ib_account", ACCOUNT, "--environment", ENV],
        ):
            rc = readiness_module.main()

        assert rc == 2
        status = json.loads(history_path.read_text(encoding="utf-8"))
        assert status["ok"] is False
        assert status["economic_state"]["status"] == "UNKNOWN"
        assert "BUNDLE_C_PREVIOUS_DAY_ECONOMIC_BOOTSTRAP_REQUIRED" in status["economic_state"]["reason_codes"]
        assert "FAIL:BUNDLE_C_PREVIOUS_DAY_ECONOMIC_STATE_REQUIRED" in status["reasons"]
        assert not (truth_root / "trade_submit_readiness_c2_v1" / "status.json").exists()
        assert (out_dir / "status.json").exists()


def test_trade_submit_readiness_materializes_previous_day_bundle_c_package_when_data_exists() -> None:
    with tempfile.TemporaryDirectory(dir=str(SOURCE_ROOT / "tmp")) as td:
        root = Path(td)
        execution_truth_root = root / "truth_sleeves" / "PRIMARY" / ENV
        prev_day = readiness_module._prior_trading_day_utc(DAY)

        truth_root = root / "truth"
        economic_build_path = truth_root / "reports" / "economic_state_build_v1" / prev_day / "ctx-test" / "economic_state_build.v1.json"
        economic_package_path = execution_truth_root / "economic_state_package_v1" / prev_day / "ctx-test" / "economic_state_package.v1.json"

        def _materialize_previous_day_package(**kwargs):
            assert kwargs["day_utc"] == prev_day
            assert kwargs["sleeve_id"] == "PRIMARY"
            assert kwargs["environment"] == ENV
            assert kwargs["ib_account"] == ACCOUNT
            _write_json(
                economic_build_path,
                {
                    "schema_id": "economic_state_build",
                    "schema_version": "v1",
                    "day_utc": prev_day,
                    "closure_status": "COMPLETE",
                    "economic_evaluation": {
                        "benchmark_state": {
                            "policy_baseline": {"comparison_vs_portfolio_return": "0.000000"},
                            "external_benchmarks": [],
                        },
                        "risk_state": {"drawdown_pct": "-0.020000"},
                    },
                },
            )
            _write_json(
                economic_package_path,
                {
                    "schema_id": "economic_state_package",
                    "schema_version": "v1",
                    "day_utc": prev_day,
                    "sealed": True,
                    "build_ref": {"path": str(economic_build_path), "sha256": "b" * 64},
                },
            )
            return {
                "build_obj": {"closure_status": "COMPLETE"},
                "package_path": economic_package_path,
            }

        with patch.object(
            readiness_module,
            "run_economic_state_authority_v1",
            side_effect=_materialize_previous_day_package,
        ):
            materialization = readiness_module._materialize_previous_day_economic_package(
                repo_root=root,
                day_utc=DAY,
                sleeve_id="PRIMARY",
                environment=ENV,
                ib_account=ACCOUNT,
            )
        status = readiness_module._load_previous_day_economic_package_state(
            execution_truth_root=execution_truth_root,
            day_utc=DAY,
        )

        assert materialization["reason_codes"] == []
        assert status["status"] == "OK"
        assert status["package_path"] == str(economic_package_path)
        assert economic_package_path.exists()
        assert economic_build_path.exists()


def test_trade_submit_readiness_requires_bootstrap_mode_when_previous_day_bundle_c_cannot_be_materialized() -> None:
    with tempfile.TemporaryDirectory(dir=str(SOURCE_ROOT / "tmp")) as td:
        root = Path(td)
        truth_root = root / "truth"
        execution_truth_root = root / "truth_sleeves" / "PRIMARY" / ENV

        capability_path = truth_root / "reports" / "capability_state_v1" / DAY / "capability_state.v1.json"
        paper_policy_path = truth_root / "reports" / "paper_policy_verdict_v1" / DAY / "paper_policy_verdict.v1.json"
        production_policy_path = truth_root / "reports" / "production_policy_verdict_v1" / DAY / "production_policy_verdict.v1.json"
        policy_diff_path = truth_root / "reports" / "policy_diff_v1" / DAY / "policy_diff.v1.json"
        day_authority_path = truth_root / "reports" / "day_authority_decision_v1" / DAY / "day_authority_decision.v1.json"
        handshake_pointer_path = execution_truth_root / "ib_api_handshake" / "latest_pointer.v1.json"
        handshake_path = execution_truth_root / "ib_api_handshake" / DAY / "ib_api_handshake.v1.json"
        authorization_path = execution_truth_root / "reports" / "authorization_gate_verdict_v1" / DAY / "authorization_gate_verdict.v1.json"

        _write_json(capability_path, {"schema_id": "capability_state", "schema_version": "v1", "day_utc": DAY})
        _write_json(
            paper_policy_path,
            {
                "schema_id": "paper_policy_verdict",
                "schema_version": "v1",
                "day_utc": DAY,
                "overall_status": "PASS",
                "blocking_items": [],
            },
        )
        _write_json(
            production_policy_path,
            {
                "schema_id": "production_policy_verdict",
                "schema_version": "v1",
                "day_utc": DAY,
                "overall_status": "PASS",
            },
        )
        _write_json(
            policy_diff_path,
            {
                "schema_id": "policy_diff",
                "schema_version": "v1",
                "day_utc": DAY,
                "production_only_open_items": [],
            },
        )
        _write_json(
            day_authority_path,
            {
                "schema_id": "day_authority_decision",
                "schema_version": "v1",
                "day_utc": DAY,
                "decision_state": "OPEN",
                "stage": "PRE_ORCHESTRATION_PREFLIGHT",
            },
        )
        _write_json(
            handshake_pointer_path,
            {
                "schema_id": "C2_IB_API_HANDSHAKE_LATEST_POINTER_V1",
                "schema_version": 1,
                "day_utc": DAY,
                "pointers": {"handshake_path": str(handshake_path), "handshake_sha256": "a" * 64},
            },
        )
        _write_json(handshake_path, {"schema_id": "C2_IB_API_HANDSHAKE_V1", "schema_version": 1, "day_utc": DAY})
        _write_json(
            authorization_path,
            {
                "schema_id": "authorization_gate_verdict_v1",
                "schema_version": 1,
                "day_utc": DAY,
                "status": "PASS",
            },
        )

        history_path = execution_truth_root / "trade_submit_readiness_c2_v1" / "_history" / ENV / ACCOUNT / DAY / "status.json"
        readiness_ref = _write_intraday_readiness_authority(truth_root)

        with patch.object(readiness_module, "REPO_ROOT", root), patch.object(readiness_module, "TRUTH_ROOT", truth_root), patch.object(
            readiness_module,
            "resolve_governed_account_binding",
            return_value=SimpleNamespace(
                account_registry_sha256="a" * 64,
                sleeve_registry_sha256="b" * 64,
                account_registry_path=root / "governance" / "02_REGISTRIES" / "C2_IB_ACCOUNT_REGISTRY_V1.json",
                sleeve_registry_path=root / "governance" / "02_REGISTRIES" / "C2_SLEEVE_REGISTRY_V1.json",
            ),
        ), patch.object(
            readiness_module,
            "resolve_sleeve_execution_root_v1",
            return_value=SimpleNamespace(execution_root_path=execution_truth_root),
        ), patch.object(
            readiness_module,
            "resolve_pointer_bound_handshake_state",
            return_value=SimpleNamespace(
                pointer_path=handshake_pointer_path,
                pointer_sha256="c" * 64,
                handshake_path=handshake_path,
                handshake_sha256="d" * 64,
            ),
        ), patch.object(
            readiness_module,
            "_refresh_authorization_convergence_for_day",
            return_value=0,
        ), patch.object(
            readiness_module,
            "_load_primary_scoped_authorization_snapshot",
            return_value={
                "binding": SimpleNamespace(sleeve_id="PRIMARY"),
                "authorization_path": authorization_path,
                "authorization_sha256": "e" * 64,
                "authorization_status": "PASS",
                "input_manifest": [],
            },
        ), patch.object(
            readiness_module,
            "_refresh_policy_stack_for_day",
            lambda **kwargs: None,
        ), patch.object(
            readiness_module,
            "_load_day_authority",
            return_value=(
                {"decision_state": "OPEN", "stage": "PRE_ORCHESTRATION_PREFLIGHT"},
                day_authority_path,
                "f" * 64,
            ),
        ), patch.object(
            readiness_module,
            "run_economic_state_authority_v1",
            return_value={
                "build_obj": {
                    "closure_status": "BLOCKED",
                    "first_real_blocker": {"dependency_id": "positions_snapshot_v5"},
                },
                "package_path": None,
            },
        ), patch.object(
            readiness_module,
            "read_or_evaluate_trading_day_readiness_authority_v1",
            return_value=readiness_ref,
        ), patch.object(
            readiness_module,
            "validate_against_repo_schema_v1",
            lambda *args, **kwargs: None,
        ), patch.object(
            readiness_module,
            "validate_trade_submit_readiness_status_obj",
            lambda *args, **kwargs: None,
        ), patch(
            "sys.argv",
            ["run_trade_submit_readiness_c2_v1.py", "--day_utc", DAY, "--ib_account", ACCOUNT, "--environment", ENV],
        ):
            rc = readiness_module.main()

        assert rc == 2
        status = json.loads(history_path.read_text(encoding="utf-8"))
        assert status["ok"] is False
        assert status["economic_state"]["status"] == "UNKNOWN"
        assert "BUNDLE_C_PREVIOUS_DAY_ECONOMIC_BOOTSTRAP_REQUIRED" in status["economic_state"]["reason_codes"]
        assert "BUNDLE_C_PREVIOUS_DAY_ECONOMIC_FIRST_BLOCKER:positions_snapshot_v5" in status["economic_state"]["reason_codes"]
        assert "FAIL:BUNDLE_C_PREVIOUS_DAY_ECONOMIC_STATE_REQUIRED" in status["reasons"]
