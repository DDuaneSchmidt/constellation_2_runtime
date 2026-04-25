from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

REPO_ROOT = Path("/home/node/constellation")
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import ops.tools.run_session_readiness_refresh_v1 as session_refresh_module
from constellation_2.common.constitutional_review_resolution_v1 import (
    build_constitutional_operator_decision_v1,
    write_constitutional_operator_decision_v1,
)
from constellation_2.phaseD.lib.submit_boundary_paper_v4 import _read_authorization


DAY = "2026-03-16"


def _binding(truth_root: Path) -> SimpleNamespace:
    return SimpleNamespace(
        sleeve_id="PRIMARY",
        environment="PAPER",
        truth_partition="truth_sleeves/PRIMARY/PAPER",
        truth_root=truth_root,
    )


def _write_intent(intent_path: Path, *, intent_id: str = "intent-1") -> None:
    intent_path.parent.mkdir(parents=True, exist_ok=True)
    intent_path.write_text(
        json.dumps(
            {
                "schema_id": "exposure_intent_v1",
                "schema_version": 1,
                "intent_id": intent_id,
                "engine": {"engine_id": "ENGINE_SPY_01"},
                "symbol": "SPY",
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def _write_canonical_capital_authority_allocation(path: Path, *, intent_hash: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema_id": "C2_CAPITAL_AUTHORITY_ALLOCATION_V1",
                "schema_version": 1,
                "produced_utc": f"{DAY}T00:00:00Z",
                "day_utc": DAY,
                "producer": {
                    "repo": "constellation_2_runtime",
                    "git_sha": "a" * 40,
                    "module": "ops/tools/run_capital_authority_allocation_day_v1.py",
                },
                "status": "OK",
                "reason_codes": [],
                "input_manifest": [
                    {
                        "type": "exposure_net",
                        "path": "/tmp/exposure_net.v1.json",
                        "sha256": "1" * 64,
                        "day_utc": DAY,
                        "producer": "risk_v1",
                    }
                ],
                "portfolio": {
                    "allowed_capital_at_risk_cents": 100000,
                    "used_capital_at_risk_cents": 1000,
                    "headroom_cents": 99000,
                },
                "allocation_state": {
                    "target_basis": "INTENT_TARGET_NOTIONAL_PCT",
                    "actual_basis": "POSITIONS_COST_BASIS_ABS_OVER_NAV_BASIS",
                    "portfolio_nav_basis_cents": 10000000,
                    "portfolio_target_notional_pct": "0.010000",
                    "portfolio_actual_notional_pct": "0.000000",
                    "max_abs_drift_notional_pct": "0.010000",
                    "reallocation_state": {
                        "status": "ACTION_NEEDED",
                        "reason_codes": ["BUNDLE_B_DRIFT_DETECTED"],
                    },
                    "target_rows": [
                        {
                            "account_id": "DUO847203",
                            "action_type": "OPEN",
                            "actual_notional_pct": "0.000000",
                            "drift_notional_pct": "0.010000",
                            "engine_id": "ENGINE_SPY_01",
                            "execution_sleeve_id": "PRIMARY",
                            "intent_hash": intent_hash,
                            "intent_id": "intent-1",
                            "position_id": "",
                            "strategy_sleeve_id": "C2_TREND_EQ_PRIMARY",
                            "symbol": "SPY",
                            "target_notional_pct": "0.010000",
                        }
                    ],
                },
                "sleeve_account_authority_state": {
                    "mode_source": "C2_SLEEVE_REGISTRY_V1",
                    "bindings": [
                        {
                            "account_id": "DUO847203",
                            "allowed_engine_ids": ["ENGINE_SPY_01"],
                            "allowed_execution_sleeve_ids": ["PRIMARY"],
                            "enabled": True,
                            "execution_sleeve_id": "PRIMARY",
                            "mode": "PAPER",
                        }
                    ],
                },
                "decision_chain": {
                    "candidate_actions": [
                        {
                            "account_id": "DUO847203",
                            "action_type": "OPEN",
                            "actual_notional_pct": "0.000000",
                            "candidate_id": "c" * 64,
                            "drift_notional_pct": "0.010000",
                            "economic_signal": "UNKNOWN",
                            "economic_signal_reason_code": "",
                            "economic_signal_source_day_utc": DAY,
                            "engine_id": "ENGINE_SPY_01",
                            "execution_sleeve_id": "PRIMARY",
                            "intent_hash": intent_hash,
                            "intent_id": "intent-1",
                            "lifecycle_intent": "OPEN",
                            "position_id": "",
                            "reason_codes": [],
                            "requested_mode": "PAPER",
                            "strategy_sleeve_id": "C2_TREND_EQ_PRIMARY",
                            "symbol": "SPY",
                            "target_notional_pct": "0.010000",
                        }
                    ],
                    "trade_intents": [
                        {
                            "account_id": "DUO847203",
                            "action_type": "OPEN",
                            "actual_notional_pct": "0.000000",
                            "candidate_id": "c" * 64,
                            "drift_notional_pct": "0.010000",
                            "economic_signal": "UNKNOWN",
                            "economic_signal_reason_code": "",
                            "economic_signal_source_day_utc": DAY,
                            "engine_id": "ENGINE_SPY_01",
                            "execution_sleeve_id": "PRIMARY",
                            "intent_hash": intent_hash,
                            "intent_id": "intent-1",
                            "lifecycle_intent": "OPEN",
                            "position_id": "",
                            "reason_codes": ["BUNDLE_B_REQUESTED_QUANTITY_UNPROVEN"],
                            "requested_quantity": 0,
                            "requested_quantity_basis": "UNPROVEN_INTENT_RISK_BUDGET",
                            "strategy_sleeve_id": "C2_TREND_EQ_PRIMARY",
                            "symbol": "SPY",
                            "target_notional_pct": "0.010000",
                            "trade_intent_id": "d" * 64,
                        }
                    ],
                    "authorized_trade_intents": [
                        {
                            "account_id": "DUO847203",
                            "action_type": "OPEN",
                            "authorization_outcome": "APPROVED",
                            "authorized_quantity": 1,
                            "authorized_trade_intent_id": "e" * 64,
                            "candidate_id": "c" * 64,
                            "economic_signal": "UNKNOWN",
                            "economic_signal_reason_code": "",
                            "economic_signal_source_day_utc": DAY,
                            "engine_id": "ENGINE_SPY_01",
                            "execution_sleeve_id": "PRIMARY",
                            "intent_hash": intent_hash,
                            "intent_id": "intent-1",
                            "lifecycle_intent": "OPEN",
                            "position_id": "",
                            "reason_codes": ["CAPAUTH_AUTHORIZED"],
                            "requested_quantity": 1,
                            "requested_quantity_basis": "UNPROVEN_INTENT_RISK_BUDGET",
                            "strategy_sleeve_id": "C2_TREND_EQ_PRIMARY",
                            "symbol": "SPY",
                            "trade_intent_id": "d" * 64,
                        }
                    ],
                },
                "per_sleeve": [
                    {
                        "sleeve_id": "C2_TREND_EQ_PRIMARY",
                        "engine_ids": ["ENGINE_SPY_01"],
                        "allowed_capital_at_risk_cents": 100000,
                        "used_capital_at_risk_cents": 1000,
                        "headroom_cents": 99000,
                    }
                ],
                "per_intent": [
                    {
                        "intent_hash": intent_hash,
                        "intent_id": "intent-1",
                        "engine_id": "ENGINE_SPY_01",
                        "sleeve_id": "C2_TREND_EQ_PRIMARY",
                        "decision": "AUTHORIZED",
                        "authorized_quantity": 1,
                        "reason_codes": ["CAPAUTH_AUTHORIZED"],
                    }
                ],
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def test_primary_execution_authorization_refresh_runs_exposure_net_allocation_and_authorization() -> None:
    calls: list[list[str]] = []

    with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
        truth_root = Path(td) / "truth_sleeves" / "PRIMARY" / "PAPER"
        binding = _binding(truth_root)
        intent_path = truth_root / "intents_v1" / "snapshots" / DAY / "intent.exposure_intent.v1.json"
        _write_intent(intent_path)
        intent_hash = session_refresh_module._sha256_file(intent_path)
        exposure_path = truth_root / "risk_v1" / "exposure_net_v1" / DAY / "exposure_net.v1.json"
        allocation_path = truth_root / "allocation_v1" / "capital_authority_allocation_v1" / DAY / "capital_authority_allocation.v1.json"
        expected_auth_path = truth_root / "engine_activity_v1" / "authorization_v1" / DAY / f"{intent_hash}.authorization.v1.json"

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            target = str(cmd[1]) if len(cmd) > 1 else ""
            if target == str(session_refresh_module.EXPOSURE_NET_TOOL):
                exposure_path.parent.mkdir(parents=True, exist_ok=True)
                exposure_path.write_text("{}", encoding="utf-8")
            if target == str(session_refresh_module.CAPITAL_AUTHORITY_ALLOCATION_TOOL):
                assert cmd[cmd.index("--canonical_sequence_owner") + 1] == session_refresh_module.CANONICAL_SEQUENCE_OWNER
                allocation_path.parent.mkdir(parents=True, exist_ok=True)
                allocation_path.write_text("{}", encoding="utf-8")
            if target == str(session_refresh_module.AUTHORIZATION_ARTIFACTS_TOOL):
                expected_auth_path.parent.mkdir(parents=True, exist_ok=True)
                expected_auth_path.write_text(
                    json.dumps(
                        {
                            "schema_id": "C2_AUTHORIZATION_V1",
                            "schema_version": 1,
                            "day_utc": DAY,
                            "status": "AUTHORIZED",
                            "engine_id": "ENGINE_SPY_01",
                            "intent_id": "intent-1",
                            "intent_hash": intent_hash,
                            "authorization": {"decision": "AUTHORIZED", "authorized_quantity": 1},
                        },
                        sort_keys=True,
                    ),
                    encoding="utf-8",
                )
            return {"cmd": cmd, "returncode": 0, "stdout": "", "stderr": ""}

        with patch.object(session_refresh_module, "_run", side_effect=fake_run):
            result = session_refresh_module._refresh_primary_execution_authorization(day_utc=DAY, binding=binding)

    assert result["status"] == "OK"
    assert result["reason_codes"] == []
    targets = [str(cmd[1]) for cmd in calls if len(cmd) > 1]
    assert targets == [
        str(session_refresh_module.EXPOSURE_NET_TOOL),
        str(session_refresh_module.CAPITAL_AUTHORITY_ALLOCATION_TOOL),
        str(session_refresh_module.AUTHORIZATION_ARTIFACTS_TOOL),
    ]
    assert result["exposure_net_path"] == str(exposure_path)
    assert result["allocation_path"] == str(allocation_path)
    assert result["expected_authorization_paths"] == [str(expected_auth_path)]
    assert result["missing_authorization_paths"] == []


def test_primary_execution_authorization_refresh_initializes_sleeve_allocation_from_canonical_when_missing() -> None:
    calls: list[list[str]] = []

    with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
        global_truth = Path(td) / "truth"
        truth_root = Path(td) / "truth_sleeves" / "PRIMARY" / "PAPER"
        binding = _binding(truth_root)
        intent_path = truth_root / "intents_v1" / "snapshots" / DAY / "intent.exposure_intent.v1.json"
        _write_intent(intent_path)
        intent_hash = session_refresh_module._sha256_file(intent_path)
        exposure_path = truth_root / "risk_v1" / "exposure_net_v1" / DAY / "exposure_net.v1.json"
        exposure_path.parent.mkdir(parents=True, exist_ok=True)
        exposure_path.write_text("{}", encoding="utf-8")
        allocation_path = truth_root / "allocation_v1" / "capital_authority_allocation_v1" / DAY / "capital_authority_allocation.v1.json"
        canonical_allocation_path = (
            global_truth / "allocation_v1" / "capital_authority_allocation_v1" / DAY / "capital_authority_allocation.v1.json"
        )
        _write_canonical_capital_authority_allocation(canonical_allocation_path, intent_hash=intent_hash)
        expected_auth_path = truth_root / "engine_activity_v1" / "authorization_v1" / DAY / f"{intent_hash}.authorization.v1.json"

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            target = str(cmd[1]) if len(cmd) > 1 else ""
            if target == str(session_refresh_module.EXPOSURE_NET_TOOL):
                return {
                    "cmd": cmd,
                    "returncode": 1,
                    "stdout": "",
                    "stderr": f"FAIL: REFUSE_OVERWRITE_EXISTING_FILE: {exposure_path}",
                }
            if target == str(session_refresh_module.CAPITAL_AUTHORITY_ALLOCATION_TOOL):
                raise AssertionError("allocation tool should not run when sleeve allocation is initialized from canonical truth")
            if target == str(session_refresh_module.AUTHORIZATION_ARTIFACTS_TOOL):
                assert allocation_path.is_file()
                expected_auth_path.parent.mkdir(parents=True, exist_ok=True)
                expected_auth_path.write_text(
                    json.dumps(
                        {
                            "schema_id": "C2_AUTHORIZATION_V1",
                            "schema_version": 1,
                            "day_utc": DAY,
                            "status": "AUTHORIZED",
                            "engine_id": "ENGINE_SPY_01",
                            "intent_id": "intent-1",
                            "intent_hash": intent_hash,
                            "authorization": {"decision": "AUTHORIZED", "authorized_quantity": 1},
                        },
                        sort_keys=True,
                    ),
                    encoding="utf-8",
                )
            return {"cmd": cmd, "returncode": 0, "stdout": "", "stderr": ""}

        with patch.object(session_refresh_module, "GLOBAL_TRUTH_ROOT", global_truth), patch.object(
            session_refresh_module, "_run", side_effect=fake_run
        ):
            result = session_refresh_module._refresh_primary_execution_authorization(day_utc=DAY, binding=binding)
        assert result["status"] == "OK"
        assert result["reason_codes"] == []
        assert allocation_path.is_file()
        assert json.loads(allocation_path.read_text(encoding="utf-8")) == json.loads(
            canonical_allocation_path.read_text(encoding="utf-8")
        )
        targets = [str(cmd[1]) for cmd in calls if len(cmd) > 1]
        assert targets == [
            str(session_refresh_module.EXPOSURE_NET_TOOL),
            str(session_refresh_module.AUTHORIZATION_ARTIFACTS_TOOL),
        ]
        assert result["missing_authorization_paths"] == []


def test_primary_authority_pointer_refresh_materializes_pointer_spine_under_global_and_sleeve_truth() -> None:
    calls: list[list[str]] = []

    with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / 'tmp')) as td:
        global_truth = Path(td) / 'truth'
        sleeve_truth = Path(td) / 'truth_sleeves' / 'PRIMARY' / 'PAPER'
        binding = _binding(sleeve_truth)
        authorization_path = sleeve_truth / 'reports' / 'authorization_gate_verdict_v1' / DAY / 'authorization_gate_verdict.v1.json'
        authorization_path.parent.mkdir(parents=True, exist_ok=True)
        authorization_path.write_text(
            json.dumps(
                {
                    'schema_id': 'authorization_gate_verdict_v1',
                    'schema_version': 1,
                    'day_utc': DAY,
                    'status': 'PASS',
                },
                sort_keys=True,
            ),
            encoding='utf-8',
        )

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            target = str(cmd[1]) if len(cmd) > 1 else ''
            if target == str(session_refresh_module.POINTER_ATTEMPT_ALLOC_TOOL):
                truth_root = cmd[cmd.index('--truth_root') + 1]
                assert truth_root in {str(global_truth.resolve()), str(sleeve_truth.resolve())}
                return {
                    'cmd': cmd,
                    'returncode': 0,
                    'stdout': json.dumps({'attempt_id': f'{DAY}__A0001__{Path(truth_root).name}__123456789abc', 'attempt_seq': 1}),
                    'stderr': '',
                }
            if target == str(session_refresh_module.POINTER_APPEND_TOOL):
                truth_root = cmd[cmd.index('--truth_root') + 1]
                assert truth_root in {str(global_truth.resolve()), str(sleeve_truth.resolve())}
                assert cmd[cmd.index('--points_to') + 1] == str(authorization_path.resolve())
                return {'cmd': cmd, 'returncode': 0, 'stdout': '{}', 'stderr': ''}
            if target == str(session_refresh_module.POINTER_HEADS_MATERIALIZE_TOOL):
                truth_root = cmd[cmd.index('--truth_root') + 1]
                assert truth_root in {str(global_truth.resolve()), str(sleeve_truth.resolve())}
                return {'cmd': cmd, 'returncode': 0, 'stdout': '{}', 'stderr': ''}
            raise AssertionError(f'unexpected command: {cmd}')

        with patch.object(session_refresh_module, 'GLOBAL_TRUTH_ROOT', global_truth), patch.object(
            session_refresh_module, '_run', side_effect=fake_run
        ):
            result = session_refresh_module._refresh_primary_authority_pointer(
                day_utc=DAY,
                binding=binding,
                git_sha='abcdef0123456789',
            )

    assert result['status'] == 'OK'
    assert result['authorization_path'] == str(authorization_path.resolve())
    assert result['pointer_truth_root'] == str(global_truth.resolve())
    assert result['scoped_pointer_truth_root'] == str(sleeve_truth.resolve())
    assert result['canonical_pointer_refresh']['status'] == 'OK'
    assert result['scoped_pointer_refresh']['status'] == 'OK'
    targets = [str(cmd[1]) for cmd in calls if len(cmd) > 1]
    assert targets == [
        str(session_refresh_module.POINTER_ATTEMPT_ALLOC_TOOL),
        str(session_refresh_module.POINTER_APPEND_TOOL),
        str(session_refresh_module.POINTER_HEADS_MATERIALIZE_TOOL),
        str(session_refresh_module.POINTER_ATTEMPT_ALLOC_TOOL),
        str(session_refresh_module.POINTER_APPEND_TOOL),
        str(session_refresh_module.POINTER_HEADS_MATERIALIZE_TOOL),
    ]



def test_primary_execution_authorization_refresh_fails_closed_when_exposure_net_missing() -> None:
    with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
        truth_root = Path(td) / "truth_sleeves" / "PRIMARY" / "PAPER"
        binding = _binding(truth_root)
        intent_path = truth_root / "intents_v1" / "snapshots" / DAY / "intent.exposure_intent.v1.json"
        _write_intent(intent_path)

        def fake_run(cmd, **kwargs):
            return {"cmd": cmd, "returncode": 0, "stdout": "", "stderr": ""}

        with patch.object(session_refresh_module, "_run", side_effect=fake_run):
            result = session_refresh_module._refresh_primary_execution_authorization(day_utc=DAY, binding=binding)

    assert result["status"] == "ERROR"
    assert "PRIMARY_EXPOSURE_NET_ARTIFACT_MISSING" in result["reason_codes"]
    assert "PRIMARY_CAPITAL_AUTHORITY_ALLOCATION_ARTIFACT_MISSING" in result["reason_codes"]
    assert "PRIMARY_ENGINE_ACTIVITY_AUTHORIZATION_ARTIFACT_MISSING" in result["reason_codes"]


def test_refresh_positions_snapshots_materializes_v5_before_v2() -> None:
    calls: list[list[str]] = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        module_name = str(cmd[2]) if len(cmd) > 2 else ""
        if module_name == "constellation_2.phaseF.positions.run.run_positions_snapshot_day_v5":
            assert cmd[cmd.index("--truth_root") + 1] == str(session_refresh_module.GLOBAL_TRUTH_ROOT)
            assert cmd[cmd.index("--ib_account") + 1] == "DUO847203"
        return {"cmd": cmd, "returncode": 0, "stdout": "", "stderr": ""}

    with patch.object(session_refresh_module, "_run", side_effect=fake_run):
        result = session_refresh_module._refresh_positions_snapshots(
            day_utc=DAY,
            producer_git_sha="a" * 40,
            paper_account="DUO847203",
            python_bin="/usr/bin/python3",
        )

    assert result["v5"]["returncode"] == 0
    assert result["v2"]["returncode"] == 0
    assert [cmd[2] for cmd in calls] == [
        "constellation_2.phaseF.positions.run.run_positions_snapshot_day_v5",
        "constellation_2.phaseF.positions.run.run_positions_snapshot_day_v2",
    ]


def test_primary_execution_authorization_surfaces_nested_allocation_fail_code() -> None:
    with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
        truth_root = Path(td) / "truth_sleeves" / "PRIMARY" / "PAPER"
        binding = _binding(truth_root)
        intent_path = truth_root / "intents_v1" / "snapshots" / DAY / "intent.exposure_intent.v1.json"
        _write_intent(intent_path)
        exposure_path = truth_root / "risk_v1" / "exposure_net_v1" / DAY / "exposure_net.v1.json"

        def fake_run(cmd, **kwargs):
            target = str(cmd[1]) if len(cmd) > 1 else ""
            if target == str(session_refresh_module.EXPOSURE_NET_TOOL):
                exposure_path.parent.mkdir(parents=True, exist_ok=True)
                exposure_path.write_text("{}", encoding="utf-8")
                return {"cmd": cmd, "returncode": 0, "stdout": "", "stderr": ""}
            if target == str(session_refresh_module.CAPITAL_AUTHORITY_ALLOCATION_TOOL):
                return {
                    "cmd": cmd,
                    "returncode": 1,
                    "stdout": "",
                    "stderr": "FAIL: POSITIONS_SNAPSHOT_V5_MISSING: /tmp/positions_snapshot.v5.json",
                }
            return {"cmd": cmd, "returncode": 99, "stdout": "", "stderr": "SKIPPED_ALLOCATION_FAILED"}

        with patch.object(session_refresh_module, "_run", side_effect=fake_run):
            result = session_refresh_module._refresh_primary_execution_authorization(day_utc=DAY, binding=binding)

    assert result["status"] == "ERROR"
    assert "PRIMARY_CAPITAL_AUTHORITY_ALLOCATION_FAILED" in result["reason_codes"]
    assert "PRIMARY_CAPITAL_AUTHORITY_ALLOCATION_FAILED:POSITIONS_SNAPSHOT_V5_MISSING" in result["reason_codes"]


def test_refresh_local_day_authority_writer_routes_through_canonical_owner() -> None:
    report_path = Path("/tmp/session_readiness_refresh.v1.json")
    calls: list[dict[str, object]] = []

    def fake_writer(**kwargs):
        calls.append(kwargs)
        return {"status": "OK", "path": "/tmp/day_authority_decision.v1.json", "sha256": "a" * 64, "decision_state": "BLOCKED"}

    with patch.object(session_refresh_module, "write_day_authority_decision_from_refresh_report_v1", side_effect=fake_writer):
        result = session_refresh_module._write_day_authority_decision_from_report(
            day_utc=DAY,
            truth_root=Path("/tmp/truth"),
            report_path=report_path,
            producer_git_sha="b" * 40,
        )

    assert result["status"] == "OK"
    assert calls == [
        {
            "day_utc": DAY,
            "truth_root": Path("/tmp/truth"),
            "report_path": report_path,
            "producer_git_sha": "b" * 40,
        }
    ]


def test_refresh_local_day_authority_writer_propagates_canonical_failure() -> None:
    with patch.object(
        session_refresh_module,
        "write_day_authority_decision_from_refresh_report_v1",
        side_effect=SystemExit("FAIL: day_authority_decision_write_failed status=ERROR reason=boom"),
    ):
        try:
            session_refresh_module._write_day_authority_decision_from_report(
                day_utc=DAY,
                truth_root=Path("/tmp/truth"),
                report_path=Path("/tmp/session_readiness_refresh.v1.json"),
                producer_git_sha="c" * 40,
            )
        except SystemExit as exc:
            assert str(exc) == "FAIL: day_authority_decision_write_failed status=ERROR reason=boom"
        else:
            raise AssertionError("expected SystemExit")


def test_submit_boundary_reads_materialized_sleeve_authorization() -> None:
    with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
        truth_root = Path(td)
        auth_path = truth_root / "engine_activity_v1" / "authorization_v1" / DAY / "abc.authorization.v1.json"
        auth_path.parent.mkdir(parents=True, exist_ok=True)
        auth_path.write_text(
            json.dumps(
                {
                    "schema_id": "C2_AUTHORIZATION_V1",
                    "schema_version": 1,
                    "day_utc": DAY,
                    "status": "AUTHORIZED",
                    "authorization": {"decision": "AUTHORIZED", "authorized_quantity": 1},
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )

        status, decision, qty, sha256, resolved_path = _read_authorization(truth_root, DAY, "abc")

    assert status == "AUTHORIZED"
    assert decision == "AUTHORIZED"
    assert qty == 1
    assert sha256
    assert resolved_path == auth_path.resolve()


def test_collect_authorization_shadow_summary_counts_decisions_missing_facts_and_mismatches() -> None:
    with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
        root = Path(td)
        path_a = root / "engine_activity_v1" / "authorization_v1" / DAY / "a.authorization.v1.json"
        path_b = root / "engine_activity_v1" / "authorization_v1" / DAY / "b.authorization.v1.json"
        for path, payload in (
            (
                path_a,
                {
                    "proposal_hash": "1" * 64,
                    "fact_bundle_hash": "2" * 64,
                    "decision_enum": "AUTO_EXECUTE_PROTECTIVE",
                    "legacy_constitutional_comparison": {"comparison_status": "CONSISTENT"},
                    "constitutional_shadow": {
                        "fact_bundle": {
                            "fact_records": [
                                {"fact_id": "fact-a", "dependency_health": "DEGRADED_NON_BLOCKING"},
                            ]
                        },
                        "decision": {"negative_evidence": []},
                    },
                },
            ),
            (
                path_b,
                {
                    "proposal_hash": "3" * 64,
                    "fact_bundle_hash": "4" * 64,
                    "decision_enum": "BLOCK",
                    "legacy_constitutional_comparison": {"comparison_status": "MISMATCH"},
                    "constitutional_shadow": {
                        "fact_bundle": {
                            "fact_records": [
                                {"fact_id": "fact-b", "dependency_health": "HEALTHY"},
                            ]
                        },
                        "decision": {
                            "negative_evidence": [
                                {
                                    "type": "MISSING_FACT",
                                    "fact": "policy_binding_fact",
                                    "severity": "BLOCKING",
                                    "detail": "required fact type missing: policy_binding_fact",
                                }
                            ]
                        },
                    },
                },
            ),
        ):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")

        summary = session_refresh_module._collect_authorization_shadow_summary(
            expected_paths=[path_a.resolve(), path_b.resolve()]
        )

    assert summary["proposal_count_expected"] == 2
    assert summary["proposal_count_captured"] == 2
    assert summary["decision_counts"] == {
        "AUTO_EXECUTE_PROTECTIVE": 1,
        "BLOCK": 1,
    }
    assert summary["decision_percentages"] == {
        "AUTO_EXECUTE_PROTECTIVE": 50.0,
        "BLOCK": 50.0,
    }
    assert summary["missing_fact_count"] == 1
    assert summary["dependency_degradation_count"] == 1
    assert summary["legacy_constitutional_mismatch_count"] == 1
    assert summary["review_required_count"] == 0
    assert summary["review_completed_count"] == 0
    assert summary["approve_rate"] == 0.0
    assert summary["proposal_rows"][0]["captured"] is True
    assert summary["proposal_rows"][1]["comparison_status"] == "MISMATCH"


def test_collect_authorization_shadow_summary_counts_review_completion_rates() -> None:
    with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
        root = Path(td)
        auth_path = root / "engine_activity_v1" / "authorization_v1" / DAY / "review.authorization.v1.json"
        payload = {
            "day_utc": DAY,
            "proposal_hash": "7" * 64,
            "fact_bundle_hash": "8" * 64,
            "decision_enum": "REQUIRE_HUMAN_REVIEW",
            "legacy_constitutional_comparison": {"comparison_status": "CONSISTENT"},
            "constitutional_shadow": {
                "review_packet": {
                    "packet_hash": "9" * 64,
                },
                "fact_bundle": {
                    "fact_records": [
                        {"fact_id": "fact-review", "dependency_health": "HEALTHY"},
                    ]
                },
                "decision": {
                    "negative_evidence": [],
                },
            },
        }
        auth_path.parent.mkdir(parents=True, exist_ok=True)
        auth_path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")

        decision_record = build_constitutional_operator_decision_v1(
            review_packet={
                "schema_id": "constitutional_review_packet",
                "schema_version": "v1",
                "packet_hash": "9" * 64,
                "proposal_hash": "7" * 64,
                "fact_bundle_hash": "8" * 64,
                "policy_version": "constitutional_shadow_v1",
                "created_at": f"{DAY}T12:00:00Z",
                "action_type": "CLOSE_TRADE",
                "action_class": "PROTECTIVE",
                "target_entities": ["DU1234567"],
                "expected_economic_effect": {},
                "expected_tax_effect": {},
                "expected_risk_effect": {},
                "admissibility_summary": {
                    "general_admissibility": "VERIFIED_PARTIAL",
                    "tax_admissibility": "INCOMPLETE",
                    "dependency_health": "HEALTHY",
                    "state_coherence": "PARTIAL",
                },
                "missing_facts": ["lot_basis_fact"],
                "dependency_issues": [],
                "decision_enum": "REQUIRE_HUMAN_REVIEW",
                "blocker_rules": ["POLICY_REQUIRES_HUMAN_REVIEW"],
                "negative_evidence": [
                    {
                        "type": "MISSING_FACT",
                        "fact": "lot_basis_fact",
                        "severity": "BLOCKING",
                        "detail": "required fact type missing: lot_basis_fact",
                    }
                ],
                "consequence_of_no_action": "Protective action remains blocked until an operator decides.",
                "suggested_options": ["APPROVE", "REJECT", "DEFER"],
                "effective_scope": {
                    "global": "PAPER",
                    "domain": "TRADING",
                    "account": "DU1234567",
                    "sleeve": "PRIMARY",
                    "action_class": "PROTECTIVE",
                    "effective_authority": "REQUIRE_HUMAN_REVIEW",
                },
                "authorization_expires_at": f"{DAY}T23:59:59Z",
                "visible_fact_summary": {
                    "required_fact_types": ["policy_binding_fact"],
                    "fact_types_present": ["policy_binding_fact"],
                },
                "visible_facts": [
                    {
                        "fact_id": "fact-review",
                        "fact_type": "policy_binding_fact",
                        "logical_name": "policy_binding",
                        "observed_at": f"{DAY}T12:00:00Z",
                        "captured_at": f"{DAY}T12:00:01Z",
                        "freshness_class": "CURRENT",
                        "provenance_class": "AUTHORITATIVE_FILE",
                        "content_hash": "a" * 64,
                        "general_admissibility": "VERIFIED_COMPLETE",
                        "tax_admissibility": "UNKNOWN",
                        "dependency_health": "HEALTHY",
                        "state_coherence": "COHERENT",
                        "artifact_path": "/tmp/policy_binding.json",
                    }
                ],
            },
            operator_action="APPROVE",
            operator_id="ops-reviewer",
            decided_at=f"{DAY}T12:30:00Z",
            source_artifact_type="authorization_v1",
            source_artifact_path=str(auth_path.resolve()),
            source_artifact_hash="b" * 64,
        )
        write_constitutional_operator_decision_v1(
            truth_root=root,
            day_utc=DAY,
            decision_record=decision_record,
        )

        summary = session_refresh_module._collect_authorization_shadow_summary(
            expected_paths=[auth_path.resolve()]
        )

    assert summary["review_required_count"] == 1
    assert summary["review_completed_count"] == 1
    assert summary["approve_rate"] == 100.0
    assert summary["reject_rate"] == 0.0
    assert summary["defer_rate"] == 0.0
    assert summary["proposal_rows"][0]["review_packet_hash"] == "9" * 64
    assert summary["proposal_rows"][0]["operator_final_action"] == "APPROVE"


def test_write_session_report_canonicalizes_nested_float_values(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(session_refresh_module, "GLOBAL_TRUTH_ROOT", tmp_path)
    monkeypatch.setattr(session_refresh_module, "_git_sha", lambda: "a" * 40)

    report_path, report_status = session_refresh_module._write_session_report(
        day_utc=DAY,
        truth_root=tmp_path,
        failures=[],
        results={
            "paper_session_ledger": {
                "stdout": json.dumps(
                    {
                        "authority_status": "GRANTED",
                        "path": "/tmp/ledger.json",
                        "ledger_id": "ledger-1",
                    },
                    sort_keys=True,
                )
            },
            "scoped_gate_refresh": [
                {
                    "execution_authorization_refresh": {
                        "constitutional_shadow_summary": {
                            "approve_rate": 100.0,
                            "decision_percentages": {"APPROVE": 50.0},
                        }
                    }
                }
            ],
        },
    )

    assert report_status == "OK"
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    summary = payload["results"]["scoped_gate_refresh"][0]["execution_authorization_refresh"]["constitutional_shadow_summary"]
    assert summary["approve_rate"] == "100"
    assert summary["decision_percentages"]["APPROVE"] == "50"
