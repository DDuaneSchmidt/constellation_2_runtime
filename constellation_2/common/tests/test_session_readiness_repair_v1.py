from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import ops.tools.run_ib_api_handshake_spine_v1 as handshake_module
import ops.tools.run_c2_paper_day_orchestrator_v2 as orchestrator_module
import ops.tools.run_operator_daily_gate_v3 as operator_gate_module
import ops.tools.run_session_readiness_refresh_v1 as session_refresh_module
import ops.tools.run_trade_submit_readiness_c2_v1 as readiness_module
from constellation_2.common.trade_submit_readiness_authority_v1 import read_trade_submit_readiness_authority_state


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
DAY = "2026-03-16"


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = ''.join(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n" for row in rows)
    path.write_text(payload, encoding='utf-8')


def _read_trade_status_for_day(*, truth_root: Path, day: str, environment: str = "PAPER", ib_account: str = "DUO847203") -> dict:
    current_path = truth_root / "trade_submit_readiness_c2_v1" / environment / ib_account / "status.json"
    history_path = truth_root / "trade_submit_readiness_c2_v1" / "_history" / environment / ib_account / day / "status.json"
    path = current_path if current_path.exists() else history_path
    return json.loads(path.read_text(encoding="utf-8"))


def _write_gate_stack_authority_day(*, truth_root: Path, day: str, status: str = "PASS") -> Path:
    gate_path = truth_root / "reports" / "gate_stack_verdict_v1" / day / "gate_stack_verdict.v1.json"
    operator_gate_path = truth_root / "reports" / "operator_daily_gate_v3" / day / "operator_daily_gate.v3.json"
    heartbeat_gate_path = truth_root / "reports" / "heartbeat_gate_v1" / day / "heartbeat_gate.v1.json"
    _write_json(operator_gate_path, {"status": "PASS"})
    _write_json(heartbeat_gate_path, {"status": "PASS"})
    _write_json(
        gate_path,
        {
            "schema_id": "gate_stack_verdict",
            "schema_version": "v1",
            "day_utc": day,
            "produced_utc": f"{day}T00:00:00Z",
            "status": status,
            "gates": [
                {"gate_id": "operator_daily_gate_v3", "status": "PASS", "artifact_path": str(operator_gate_path)},
                {"gate_id": "heartbeat_gate_v1", "status": "PASS", "artifact_path": str(heartbeat_gate_path)},
            ],
        },
    )
    return gate_path


def _write_scoped_gate_stack_authority_day(*, root: Path, sleeve_id: str, day: str, status: str = "PASS") -> Path:
    sleeve_truth_root = root / "constellation_2" / "runtime" / "truth_sleeves" / sleeve_id / "PAPER"
    gate_path = _write_gate_stack_authority_day(truth_root=sleeve_truth_root, day=day, status=status)
    gate_sha = readiness_module._sha256_file(gate_path)
    _write_jsonl(
        sleeve_truth_root / "run_pointer_v1" / "canonical_pointer_index.v1.jsonl",
        [
            {
                "schema_id": "C2_RUN_POINTER_CANONICAL_POINTER_INDEX_V1",
                "pointer_seq": 1,
                "day_utc": day,
                "attempt_id": f"{day}__A0001__abc1234__000000000000",
                "attempt_seq": 1,
                "mode": "PAPER",
                "status": status,
                "authoritative": status == "PASS",
                "produced_utc": f"{day}T00:00:00Z",
                "producer_git_sha": "abc1234",
                "points_to": str(gate_path),
                "points_to_sha256": gate_sha,
            }
        ],
    )
    if status == "PASS":
        _write_json(
            sleeve_truth_root / "run_pointer_v2" / "canonical_authority_head.v1.json",
            {
                "schema_id": "c2_run_pointer_canonical_authority_head",
                "schema_version": "v1",
                "day_utc": day,
                "status": "PASS",
                "authoritative": True,
                "produced_utc": f"{day}T00:00:00Z",
                "points_to": str(gate_path),
                "points_to_sha256": gate_sha,
            },
        )
    return gate_path


class SessionReadinessRepairTests(unittest.TestCase):
    def _write_minimal_registries(self, root: Path) -> None:
        _write_json(
            root / "governance" / "02_REGISTRIES" / "C2_IB_ACCOUNT_REGISTRY_V1.json",
            {
                "schema_id": "c2_ib_account_registry",
                "schema_version": "v1",
                "accounts": [
                    {
                        "account_id": "DUO847203",
                        "environment": "PAPER",
                        "enabled_for_submission": True,
                        "allowed_sleeve_ids": ["PRIMARY"],
                    }
                ],
            },
        )
        _write_json(
            root / "governance" / "02_REGISTRIES" / "C2_SLEEVE_REGISTRY_V1.json",
            {
                "schema_id": "c2_sleeve_registry",
                "schema_version": "v1",
                "sleeves": [
                    {
                        "sleeve_id": "PRIMARY",
                        "enabled": True,
                        "mode": "PAPER",
                        "execution_mode": "AUTO",
                        "status": "PRODUCTION",
                        "ib_account": "DUO847203",
                        "truth_partition": "truth_sleeves/PRIMARY/PAPER",
                        "assigned_engine_ids": ["C2_MEAN_REVERSION_EQ_V1", "C2_TREND_EQ_PRIMARY_V1", "C2_VOL_INCOME_DEFINED_RISK_V1"],
                        "active_controllable_engine_ids": ["C2_MEAN_REVERSION_EQ_V1", "C2_TREND_EQ_PRIMARY_V1", "C2_VOL_INCOME_DEFINED_RISK_V1"],
                        "disabled_by_default_engine_ids": [],
                        "support_only_engine_ids": [],
                    }
                ],
            },
        )

    def test_handshake_missing_broker_events_writes_fail_without_crash(self) -> None:
        with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
            root = Path(td)
            truth_root = root / "constellation_2" / "runtime" / "truth"
            self._write_minimal_registries(root)
            with patch.object(handshake_module, "REPO_ROOT", root), patch.object(
                handshake_module, "TRUTH_ROOT", truth_root
            ), patch.object(
                handshake_module, "AUTH_BROKER_EVENTS_ROOT", truth_root / "execution_evidence_v1" / "broker_events"
            ), patch.object(
                handshake_module, "validate_against_repo_schema_v1", lambda *args, **kwargs: None
            ):
                rc = handshake_module.main(["--day_utc", DAY])
            self.assertEqual(rc, 2)
            out = json.loads((truth_root / "ib_api_handshake" / DAY / "ib_api_handshake.v1.json").read_text(encoding="utf-8"))
            self.assertEqual(out["status"], "FAIL")
            self.assertIn("BROKER_EVENTS_MISSING", out["reason_codes"])

    def test_trade_submit_readiness_uses_day_scoped_handshake_when_latest_pointer_is_stale(self) -> None:
        with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
            root = Path(td)
            truth_root = root / "constellation_2" / "runtime" / "truth"
            self._write_minimal_registries(root)
            _write_scoped_gate_stack_authority_day(root=root, sleeve_id="PRIMARY", day=DAY)
            _write_json(
                truth_root / "ib_api_handshake" / "latest_pointer.v1.json",
                {
                    "schema_id": "C2_IB_API_HANDSHAKE_LATEST_POINTER_V1",
                    "schema_version": 1,
                    "day_utc": "2026-03-13",
                    "pointers": {
                        "handshake_path": str(truth_root / "ib_api_handshake" / "2026-03-13" / "ib_api_handshake.v1.json"),
                        "handshake_sha256": "a" * 64,
                    },
                },
            )
            _write_json(
                truth_root / "ib_api_handshake" / DAY / "ib_api_handshake.v1.json",
                {
                    "schema_id": "C2_IB_API_HANDSHAKE_V1",
                    "schema_version": 1,
                    "day_utc": DAY,
                    "status": "OK",
                    "ok": True,
                    "environment": "PAPER",
                    "ib_account": "DUO847203",
                    "reason_codes": ["HANDSHAKE_OK_NEXTVALIDID_SEEN_NO_504_AFTER"],
                    "inputs": {},
                    "observations": {},
                },
            )
            with patch.object(readiness_module, "REPO_ROOT", root), patch.object(
                readiness_module, "TRUTH_ROOT", truth_root
            ), patch.object(
                readiness_module, "OUT_ROOT", truth_root / "trade_submit_readiness_c2_v1"
            ), patch.object(
                readiness_module, "validate_against_repo_schema_v1", lambda *args, **kwargs: None
            ), patch(
                "sys.argv",
                ["run_trade_submit_readiness_c2_v1.py", "--day_utc", DAY, "--ib_account", "DUO847203", "--environment", "PAPER"],
            ):
                rc = readiness_module.main()
            self.assertEqual(rc, 0)
            out = _read_trade_status_for_day(truth_root=truth_root, day=DAY)
            self.assertTrue(out["ok"])
            self.assertIn("IB_API_HANDSHAKE_POINTER_OK", out["reasons"])

    def test_trade_submit_readiness_fails_on_handshake_account_mismatch(self) -> None:
        with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
            root = Path(td)
            truth_root = root / "constellation_2" / "runtime" / "truth"
            self._write_minimal_registries(root)
            _write_scoped_gate_stack_authority_day(root=root, sleeve_id="PRIMARY", day=DAY)
            handshake_path = truth_root / "ib_api_handshake" / DAY / "ib_api_handshake.v1.json"
            _write_json(
                handshake_path,
                {
                    "schema_id": "C2_IB_API_HANDSHAKE_V1",
                    "schema_version": 1,
                    "day_utc": DAY,
                    "status": "OK",
                    "ok": True,
                    "environment": "PAPER",
                    "ib_account": "DUQ154892",
                    "reason_codes": ["HANDSHAKE_OK_NEXTVALIDID_SEEN_NO_504_AFTER"],
                    "inputs": {},
                    "observations": {},
                },
            )
            _write_json(
                truth_root / "ib_api_handshake" / "latest_pointer.v1.json",
                {
                    "schema_id": "C2_IB_API_HANDSHAKE_LATEST_POINTER_V1",
                    "schema_version": 1,
                    "day_utc": DAY,
                    "pointers": {
                        "handshake_path": str(handshake_path),
                        "handshake_sha256": readiness_module._sha256_file(handshake_path),
                    },
                },
            )
            with patch.object(readiness_module, "REPO_ROOT", root), patch.object(
                readiness_module, "TRUTH_ROOT", truth_root
            ), patch.object(
                readiness_module, "OUT_ROOT", truth_root / "trade_submit_readiness_c2_v1"
            ), patch.object(
                readiness_module, "validate_against_repo_schema_v1", lambda *args, **kwargs: None
            ), patch(
                "sys.argv",
                ["run_trade_submit_readiness_c2_v1.py", "--day_utc", DAY, "--ib_account", "DUO847203", "--environment", "PAPER"],
            ):
                rc = readiness_module.main()
            self.assertEqual(rc, 2)
            out = _read_trade_status_for_day(truth_root=truth_root, day=DAY)
            self.assertTrue(any(reason.startswith("FAIL:IB_API_HANDSHAKE_ACCOUNT_MISMATCH:") for reason in out["reasons"]))

    def test_trade_submit_readiness_fails_when_gate_stack_authority_missing(self) -> None:
        with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
            root = Path(td)
            truth_root = root / "constellation_2" / "runtime" / "truth"
            self._write_minimal_registries(root)
            (root / "constellation_2" / "runtime" / "truth_sleeves" / "PRIMARY" / "PAPER").mkdir(parents=True, exist_ok=True)
            handshake_path = truth_root / "ib_api_handshake" / DAY / "ib_api_handshake.v1.json"
            _write_json(
                handshake_path,
                {
                    "schema_id": "C2_IB_API_HANDSHAKE_V1",
                    "schema_version": 1,
                    "day_utc": DAY,
                    "status": "OK",
                    "ok": True,
                    "environment": "PAPER",
                    "ib_account": "DUO847203",
                    "reason_codes": ["HANDSHAKE_OK_NEXTVALIDID_SEEN_NO_504_AFTER"],
                    "inputs": {},
                    "observations": {},
                },
            )
            _write_json(
                truth_root / "ib_api_handshake" / "latest_pointer.v1.json",
                {
                    "schema_id": "C2_IB_API_HANDSHAKE_LATEST_POINTER_V1",
                    "schema_version": 1,
                    "day_utc": DAY,
                    "pointers": {
                        "handshake_path": str(handshake_path),
                        "handshake_sha256": readiness_module._sha256_file(handshake_path),
                    },
                },
            )
            with patch.object(readiness_module, "REPO_ROOT", root), patch.object(
                readiness_module, "TRUTH_ROOT", truth_root
            ), patch.object(
                readiness_module, "OUT_ROOT", truth_root / "trade_submit_readiness_c2_v1"
            ), patch.object(
                readiness_module, "validate_against_repo_schema_v1", lambda *args, **kwargs: None
            ), patch(
                "sys.argv",
                ["run_trade_submit_readiness_c2_v1.py", "--day_utc", DAY, "--ib_account", "DUO847203", "--environment", "PAPER"],
            ):
                rc = readiness_module.main()
            self.assertEqual(rc, 2)
            out = _read_trade_status_for_day(truth_root=truth_root, day=DAY)
            self.assertTrue(any(reason.startswith("FAIL:FINAL_GATE_STACK_NOT_PASS:") for reason in out["reasons"]))

    def test_trade_submit_readiness_fails_when_handshake_pointer_missing(self) -> None:
        with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
            root = Path(td)
            truth_root = root / "constellation_2" / "runtime" / "truth"
            truth_root.mkdir(parents=True, exist_ok=True)
            self._write_minimal_registries(root)
            _write_scoped_gate_stack_authority_day(root=root, sleeve_id="PRIMARY", day=DAY)
            with patch.object(readiness_module, "REPO_ROOT", root), patch.object(
                readiness_module, "TRUTH_ROOT", truth_root
            ), patch.object(
                readiness_module, "OUT_ROOT", truth_root / "trade_submit_readiness_c2_v1"
            ), patch.object(
                readiness_module, "validate_against_repo_schema_v1", lambda *args, **kwargs: None
            ), patch(
                "sys.argv",
                ["run_trade_submit_readiness_c2_v1.py", "--day_utc", DAY, "--ib_account", "DUO847203", "--environment", "PAPER"],
            ):
                rc = readiness_module.main()
            self.assertEqual(rc, 2)
            out = _read_trade_status_for_day(truth_root=truth_root, day=DAY)
            self.assertIn("FAIL:IB_API_HANDSHAKE_POINTER_MISSING", out["reasons"])

    def test_trade_submit_readiness_does_not_fallback_to_global_pass_when_scoped_paper_truth_fails(self) -> None:
        with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
            root = Path(td)
            truth_root = root / "constellation_2" / "runtime" / "truth"
            self._write_minimal_registries(root)
            gate_path = _write_gate_stack_authority_day(truth_root=truth_root, day=DAY)
            _write_json(
                truth_root / "run_pointer_v2" / "canonical_authority_head.v1.json",
                {
                    "schema_id": "c2_run_pointer_canonical_authority_head",
                    "schema_version": "v1",
                    "day_utc": DAY,
                    "status": "PASS",
                    "authoritative": True,
                    "points_to": str(gate_path),
                    "points_to_sha256": readiness_module._sha256_file(gate_path),
                },
            )
            _write_scoped_gate_stack_authority_day(root=root, sleeve_id="PRIMARY", day=DAY, status="FAIL")
            handshake_path = truth_root / "ib_api_handshake" / DAY / "ib_api_handshake.v1.json"
            _write_json(
                handshake_path,
                {
                    "schema_id": "C2_IB_API_HANDSHAKE_V1",
                    "schema_version": 1,
                    "day_utc": DAY,
                    "status": "OK",
                    "ok": True,
                    "environment": "PAPER",
                    "ib_account": "DUO847203",
                    "reason_codes": ["HANDSHAKE_OK_NEXTVALIDID_SEEN_NO_504_AFTER"],
                    "inputs": {},
                    "observations": {},
                },
            )
            _write_json(
                truth_root / "ib_api_handshake" / "latest_pointer.v1.json",
                {
                    "schema_id": "C2_IB_API_HANDSHAKE_LATEST_POINTER_V1",
                    "schema_version": 1,
                    "day_utc": DAY,
                    "pointers": {
                        "handshake_path": str(handshake_path),
                        "handshake_sha256": readiness_module._sha256_file(handshake_path),
                    },
                },
            )
            with patch.object(readiness_module, "REPO_ROOT", root), patch.object(
                readiness_module, "TRUTH_ROOT", truth_root
            ), patch.object(
                readiness_module, "OUT_ROOT", truth_root / "trade_submit_readiness_c2_v1"
            ), patch.object(
                readiness_module, "validate_against_repo_schema_v1", lambda *args, **kwargs: None
            ), patch(
                "sys.argv",
                ["run_trade_submit_readiness_c2_v1.py", "--day_utc", DAY, "--ib_account", "DUO847203", "--environment", "PAPER"],
            ):
                rc = readiness_module.main()
            self.assertEqual(rc, 2)
            out = _read_trade_status_for_day(truth_root=truth_root, day=DAY)
            self.assertTrue(any("sleeve_id=PRIMARY" in reason and "GATE_STACK_STATUS_NOT_PASS" in reason for reason in out["reasons"]))

    def test_session_refresh_bootstraps_broker_events_before_handshake(self) -> None:
        calls = []

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            return {"cmd": cmd, "returncode": 0, "stdout": "", "stderr": ""}

        with patch.object(session_refresh_module, "resolve_single_paper_ib_account_from_sleeve_registry", return_value="DUO847203"), patch.object(
            session_refresh_module, "_load_accounts", return_value={"PAPER": ["DUO847203"], "LIVE": []}
        ), patch.object(
            session_refresh_module, "_run", side_effect=fake_run
        ), patch.object(
            session_refresh_module, "_git_sha", return_value="abc1234"
        ), patch.object(
            session_refresh_module, "_resolve_paper_sleeve_truth_bindings", return_value=[]
        ), patch.object(
            session_refresh_module, "_authority_lifecycle_result", return_value={"status": "OK", "incident_count": 0, "incidents": []}
        ), patch(
            "sys.argv", ["run_session_readiness_refresh_v1.py", "--day_utc", DAY]
        ):
            rc = session_refresh_module.main()

        self.assertEqual(rc, 0)
        self.assertGreaterEqual(len(calls), 5)
        operator_statement_cmd = calls[0]
        bootstrap_index = next(i for i, cmd in enumerate(calls) if len(cmd) > 1 and str(cmd[1]) == str(session_refresh_module.BROKER_EVENTS_BOOTSTRAP_TOOL))
        manifest_index = next(i for i, cmd in enumerate(calls) if len(cmd) > 1 and str(cmd[1]) == str(session_refresh_module.BROKER_EVENTS_MANIFEST_TOOL))
        handshake_index = next(i for i, cmd in enumerate(calls) if len(cmd) > 1 and str(cmd[1]) == str(session_refresh_module.HANDSHAKE_TOOL))
        bootstrap_cmd = calls[bootstrap_index]
        manifest_cmd = calls[manifest_index]
        handshake_cmd = calls[handshake_index]
        self.assertIn("ensure_cash_ledger_operator_statement_v1.py", str(operator_statement_cmd[1]))
        self.assertIn("BROKER_ACCOUNT_VALUES", operator_statement_cmd)
        self.assertIn("ops/ib/c2_execution_observer_v1.py", str(bootstrap_cmd[1]))
        self.assertIn("--bootstrap-handshake-only", bootstrap_cmd)
        self.assertIn("run_broker_event_day_manifest_v1.py", str(manifest_cmd[1]))
        self.assertEqual(handshake_cmd[1], str(session_refresh_module.HANDSHAKE_TOOL))
        self.assertLess(bootstrap_index, handshake_index)
        self.assertLess(manifest_index, handshake_index)

    def test_session_refresh_runs_global_gate_refresh_before_trade_submit_readiness(self) -> None:
        calls = []

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            return {"cmd": cmd, "returncode": 0, "stdout": "", "stderr": ""}

        with patch.object(session_refresh_module, "resolve_single_paper_ib_account_from_sleeve_registry", return_value="DUO847203"), patch.object(
            session_refresh_module, "_load_accounts", return_value={"PAPER": ["DUO847203"], "LIVE": []}
        ), patch.object(
            session_refresh_module, "_run", side_effect=fake_run
        ), patch.object(
            session_refresh_module, "_resolve_paper_sleeve_truth_bindings", return_value=[]
        ), patch.object(
            session_refresh_module, "_authority_lifecycle_result", return_value={"status": "OK", "incident_count": 0, "incidents": []}
        ), patch(
            "sys.argv", ["run_session_readiness_refresh_v1.py", "--day_utc", DAY]
        ):
            rc = session_refresh_module.main()

        self.assertEqual(rc, 0)
        global_gate_refresh_index = next(
            i for i, cmd in enumerate(calls) if len(cmd) > 1 and str(cmd[1]) == str(session_refresh_module.GLOBAL_GATE_REFRESH_TOOL)
        )
        trade_submit_index = next(
            i for i, cmd in enumerate(calls) if len(cmd) > 1 and str(cmd[1]) == str(session_refresh_module.READINESS_TOOL)
        )
        self.assertLess(global_gate_refresh_index, trade_submit_index)

    def test_session_refresh_treats_global_only_paper_rollup_failures_as_monitoring(self) -> None:
        calls = []

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            if len(cmd) > 1 and str(cmd[1]) == str(session_refresh_module.GLOBAL_GATE_REFRESH_TOOL):
                truth_root = cmd[cmd.index('--truth_root') + 1] if '--truth_root' in cmd else ''
                if truth_root == str(session_refresh_module.GLOBAL_TRUTH_ROOT):
                    return {
                        "cmd": cmd,
                        "returncode": 2,
                        "stdout": json.dumps({"hard_failures": ["exit_reconciliation_v1", "pointer_heads_materialize_v1"]}),
                        "stderr": "",
                    }
            return {"cmd": cmd, "returncode": 0, "stdout": "", "stderr": ""}

        with patch.object(session_refresh_module, "resolve_single_paper_ib_account_from_sleeve_registry", return_value="DUO847203"), patch.object(
            session_refresh_module, "_load_accounts", return_value={"PAPER": ["DUO847203"], "LIVE": []}
        ), patch.object(
            session_refresh_module, "_run", side_effect=fake_run
        ), patch.object(
            session_refresh_module, "_resolve_paper_sleeve_truth_bindings", return_value=[]
        ), patch.object(
            session_refresh_module, "_authority_lifecycle_result", return_value={"status": "OK", "incident_count": 0, "incidents": []}
        ), patch(
            "sys.argv", ["run_session_readiness_refresh_v1.py", "--day_utc", DAY]
        ):
            rc = session_refresh_module.main()

        self.assertEqual(rc, 0)

    def test_session_refresh_runs_scoped_gate_refresh_before_trade_submit_readiness(self) -> None:
        calls = []
        primary_truth = REPO_ROOT / "tmp" / "fake_primary_truth"
        tail_truth = REPO_ROOT / "tmp" / "fake_tail_truth"

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            return {"cmd": cmd, "returncode": 0, "stdout": "", "stderr": ""}

        class _Binding:
            def __init__(self, sleeve_id: str, truth_root: Path) -> None:
                self.sleeve_id = sleeve_id
                self.environment = 'PAPER'
                self.truth_root = truth_root
                self.truth_partition = f'truth_sleeves/{sleeve_id}/PAPER'

        with patch.object(session_refresh_module, "resolve_single_paper_ib_account_from_sleeve_registry", return_value="DUO847203"), patch.object(
            session_refresh_module, "_load_accounts", return_value={"PAPER": ["DUO847203"], "LIVE": []}
        ), patch.object(
            session_refresh_module, "_run", side_effect=fake_run
        ), patch.object(
            session_refresh_module, "_authority_lifecycle_result", return_value={"status": "OK", "incident_count": 0, "incidents": []}
        ), patch.object(
            session_refresh_module,
            "_resolve_paper_sleeve_truth_bindings",
            return_value=[_Binding('PRIMARY', primary_truth), _Binding('C2_DEFENSIVE_TAIL', tail_truth)],
        ), patch(
            "sys.argv", ["run_session_readiness_refresh_v1.py", "--day_utc", DAY]
        ):
            rc = session_refresh_module.main()

        self.assertEqual(rc, 0)
        trade_submit_index = next(
            i for i, cmd in enumerate(calls) if len(cmd) > 1 and str(cmd[1]) == str(session_refresh_module.READINESS_TOOL)
        )
        scoped_gate_calls = [
            (i, cmd)
            for i, cmd in enumerate(calls)
            if len(cmd) > 1
            and str(cmd[1]) == str(session_refresh_module.GLOBAL_GATE_REFRESH_TOOL)
            and "--truth_root" in cmd
            and cmd[cmd.index("--truth_root") + 1] in {str(primary_truth), str(tail_truth)}
        ]
        self.assertEqual(len(scoped_gate_calls), 2)
        for idx, _cmd in scoped_gate_calls:
            self.assertLess(idx, trade_submit_index)

    def test_session_refresh_materializes_b2_inputs_before_global_gate_refresh(self) -> None:
        calls = []

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            return {"cmd": cmd, "returncode": 0, "stdout": "", "stderr": ""}

        with patch.object(session_refresh_module, "resolve_single_paper_ib_account_from_sleeve_registry", return_value="DUO847203"), patch.object(
            session_refresh_module, "_load_accounts", return_value={"PAPER": ["DUO847203"], "LIVE": []}
        ), patch.object(
            session_refresh_module, "_run", side_effect=fake_run
        ), patch.object(
            session_refresh_module, "_git_sha", return_value="abc1234"
        ), patch.object(
            session_refresh_module, "_resolve_paper_sleeve_truth_bindings", return_value=[]
        ), patch.object(
            session_refresh_module, "_authority_lifecycle_result", return_value={"status": "OK", "incident_count": 0, "incidents": []}
        ), patch(
            "sys.argv", ["run_session_readiness_refresh_v1.py", "--day_utc", DAY]
        ):
            rc = session_refresh_module.main()

        self.assertEqual(rc, 0)
        positions_index = next(
            i for i, cmd in enumerate(calls) if len(cmd) > 2 and cmd[1] == "-m" and cmd[2] == "constellation_2.phaseF.positions.run.run_positions_snapshot_day_v2"
        )
        cash_index = next(
            i for i, cmd in enumerate(calls) if len(cmd) > 2 and cmd[1] == "-m" and cmd[2] == "constellation_2.phaseF.cash_ledger.run.run_cash_ledger_snapshot_day_v1"
        )
        nav_index = next(
            i for i, cmd in enumerate(calls) if len(cmd) > 1 and str(cmd[1]) == str(session_refresh_module.ACCOUNTING_NAV_TOOL)
        )
        global_gate_refresh_index = next(
            i for i, cmd in enumerate(calls) if len(cmd) > 1 and str(cmd[1]) == str(session_refresh_module.GLOBAL_GATE_REFRESH_TOOL)
        )
        self.assertLess(positions_index, global_gate_refresh_index)
        self.assertLess(cash_index, global_gate_refresh_index)
        self.assertLess(nav_index, global_gate_refresh_index)

    def test_session_refresh_seeds_missing_sleeve_positions_snapshot_from_canonical_before_operator_gate(self) -> None:
        with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
            root = Path(td)
            global_truth = root / "constellation_2" / "runtime" / "truth"
            sleeve_truth = root / "constellation_2" / "runtime" / "truth_sleeves" / "PRIMARY" / "PAPER"
            day = DAY
            sleeve_truth.mkdir(parents=True, exist_ok=True)

            canonical_positions = global_truth / "positions_v1" / "snapshots" / day / "positions_snapshot.v2.json"
            _write_json(
                canonical_positions,
                {
                    "schema_id": "C2_POSITIONS_SNAPSHOT_V2",
                    "schema_version": 2,
                    "day_utc": day,
                    "produced_utc": f"{day}T00:00:00Z",
                    "producer": {"repo": "constellation_2_runtime", "git_sha": "abc1234", "module": "test"},
                    "status": "OK",
                    "reason_codes": ["NO_SUBMISSIONS_EMPTY_POSITIONS_V2"],
                    "input_manifest": [],
                    "positions": {"currency": "USD", "asof_utc": f"{day}T00:00:00Z", "items": [], "notes": []},
                },
            )

            binding = SimpleNamespace(
                sleeve_id="PRIMARY",
                environment="PAPER",
                truth_root=sleeve_truth,
                truth_partition="truth_sleeves/PRIMARY/PAPER",
            )

            with patch.object(session_refresh_module, "REPO_ROOT", root), patch.object(
                session_refresh_module, "GLOBAL_TRUTH_ROOT", global_truth
            ), patch.object(
                session_refresh_module, "_resolve_paper_sleeve_truth_bindings", return_value=[binding]
            ), patch.object(
                orchestrator_module, "REPO_ROOT", root
            ), patch.object(
                orchestrator_module, "DEFAULT_TRUTH_ROOT", global_truth
            ):
                result = session_refresh_module._seed_sleeve_positions_snapshots(day_utc=day, paper_account="DUO847203")

            seeded_path = sleeve_truth / "positions_v1" / "snapshots" / day / "positions_snapshot.v2.json"
            self.assertEqual(result["status"], "OK")
            self.assertEqual(result["seeded_count"], 1)
            self.assertTrue(seeded_path.exists())
            self.assertEqual(
                json.loads(seeded_path.read_text(encoding="utf-8")),
                json.loads(canonical_positions.read_text(encoding="utf-8")),
            )

            _write_json(
                sleeve_truth / "reports" / "reconciliation_report_v3" / day / "reconciliation_report.v3.json",
                {"status": "OK", "reason_codes": [], "truth_side": {"counts": {"submissions_total": 0}}},
            )
            _write_json(
                sleeve_truth / "allocation_v1" / "summary" / day / "summary.json",
                {"status": "OK"},
            )
            _write_json(
                sleeve_truth / "reports" / "capital_risk_envelope_v2" / day / "capital_risk_envelope.v2.json",
                {"status": "PASS"},
            )
            _write_json(
                sleeve_truth / "cash_ledger_v1" / "snapshots" / day / "cash_ledger_snapshot.v1.json",
                {
                    "produced_utc": f"{day}T00:00:00Z",
                    "snapshot": {"observed_at_utc": f"{day}T00:00:00Z"},
                },
            )
            _write_json(
                sleeve_truth / "exit_reconciliation_v1" / day / "exit_reconciliation.v1.json",
                {"obligations": []},
            )

            with patch.object(operator_gate_module, "REPO_ROOT", root), patch.object(
                operator_gate_module, "_git_sha", return_value="abc1234"
            ), patch.object(
                operator_gate_module, "validate_against_repo_schema_v1", lambda *args, **kwargs: None
            ), patch(
                "sys.argv",
                [
                    "run_operator_daily_gate_v3.py",
                    "--day_utc",
                    day,
                    "--truth_root",
                    str(sleeve_truth),
                    "--produced_utc",
                    f"{day}T00:00:00Z",
                    "--mode",
                    "PAPER",
                ],
            ):
                rc = operator_gate_module.main()

            self.assertEqual(rc, 0)
            gate = json.loads(
                (sleeve_truth / "reports" / "operator_daily_gate_v3" / day / "operator_daily_gate.v3.json").read_text(encoding="utf-8")
            )
            self.assertTrue(gate["checks"]["positions_snapshot_present"])
            self.assertEqual(gate["status"], "PASS")
            self.assertNotIn("MISSING_POSITIONS_SNAPSHOT", gate["reason_codes"])

    def test_session_refresh_includes_pnl_attribution_pending_when_nav_missing(self) -> None:
        calls = []

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            return {"cmd": cmd, "returncode": 0, "stdout": "", "stderr": ""}

        with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
            root = Path(td)
            global_truth = root / "constellation_2" / "runtime" / "truth"
            with patch.object(session_refresh_module, "REPO_ROOT", root), patch.object(
                session_refresh_module, "GLOBAL_TRUTH_ROOT", global_truth
            ), patch.object(
                session_refresh_module, "resolve_single_paper_ib_account_from_sleeve_registry", return_value="DUO847203"
            ), patch.object(
                session_refresh_module, "_load_accounts", return_value={"PAPER": ["DUO847203"], "LIVE": []}
            ), patch.object(
                session_refresh_module, "_run", side_effect=fake_run
            ), patch.object(
                session_refresh_module, "_resolve_paper_sleeve_truth_bindings", return_value=[]
            ), patch.object(
                session_refresh_module, "validate_against_repo_schema_v1", lambda *args, **kwargs: None
            ), patch(
                "sys.argv", ["run_session_readiness_refresh_v1.py", "--day_utc", DAY]
            ):
                rc = session_refresh_module.main()

            self.assertEqual(rc, 0)
            report = json.loads((global_truth / "reports" / "session_readiness_refresh_v1" / DAY / "session_readiness_refresh.v1.json").read_text(encoding="utf-8"))
            self.assertEqual(report["results"]["pnl_attribution"]["status"], "PENDING_NAV_NOT_AVAILABLE")
            self.assertFalse(any(str(session_refresh_module.PNL_ATTRIBUTION_TOOL) == str(cmd[1]) for cmd in calls if len(cmd) > 1))
            self.assertTrue(any(str(session_refresh_module.STARTUP_PROOF_VALIDATION_TOOL) == str(cmd[1]) for cmd in calls if len(cmd) > 1))

    def test_session_refresh_returns_zero_for_monitoring_only_startup_proof_gap(self) -> None:
        calls = []

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            target = str(cmd[1]) if len(cmd) > 1 else ""
            if target == str(session_refresh_module.STARTUP_PROOF_VALIDATION_TOOL):
                return {"cmd": cmd, "returncode": 2, "stdout": "{\"status\":\"STARTUP_BLOCKED\"}", "stderr": ""}
            return {"cmd": cmd, "returncode": 0, "stdout": "", "stderr": ""}

        with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
            root = Path(td)
            global_truth = root / "constellation_2" / "runtime" / "truth"
            with patch.object(session_refresh_module, "REPO_ROOT", root), patch.object(
                session_refresh_module, "GLOBAL_TRUTH_ROOT", global_truth
            ), patch.object(
                session_refresh_module, "resolve_single_paper_ib_account_from_sleeve_registry", return_value="DUO847203"
            ), patch.object(
                session_refresh_module, "_load_accounts", return_value={"PAPER": ["DUO847203"], "LIVE": []}
            ), patch.object(
                session_refresh_module, "_run", side_effect=fake_run
            ), patch.object(
                session_refresh_module, "_git_sha", return_value="abc1234"
            ), patch.object(
                session_refresh_module, "_resolve_paper_sleeve_truth_bindings", return_value=[]
            ), patch.object(
                session_refresh_module, "validate_against_repo_schema_v1", lambda *args, **kwargs: None
            ), patch(
                "sys.argv", ["run_session_readiness_refresh_v1.py", "--day_utc", DAY]
            ):
                rc = session_refresh_module.main()

            self.assertEqual(rc, 0)
            report = json.loads((global_truth / "reports" / "session_readiness_refresh_v1" / DAY / "session_readiness_refresh.v1.json").read_text(encoding="utf-8"))
            self.assertEqual(report["status"], "OK_WITH_MONITORING_GAPS")
            self.assertIn("startup_proof_validation", report["failures"])

    def test_session_refresh_writes_current_report_before_startup_proof_validation(self) -> None:
        call_order = []
        observed_session_statuses = []

        def fake_run(cmd, **kwargs):
            call_order.append(cmd)
            target = str(cmd[1]) if len(cmd) > 1 else ""
            if target == str(session_refresh_module.STARTUP_PROOF_VALIDATION_TOOL):
                session_path = global_truth / "reports" / "session_readiness_refresh_v1" / DAY / "session_readiness_refresh.v1.json"
                observed_session_statuses.append(json.loads(session_path.read_text(encoding="utf-8"))["status"])
                return {"cmd": cmd, "returncode": 2, "stdout": "{\"status\":\"STARTUP_BLOCKED\"}", "stderr": ""}
            return {"cmd": cmd, "returncode": 0, "stdout": "", "stderr": ""}

        with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
            root = Path(td)
            global_truth = root / "constellation_2" / "runtime" / "truth"
            with patch.object(session_refresh_module, "REPO_ROOT", root), patch.object(
                session_refresh_module, "GLOBAL_TRUTH_ROOT", global_truth
            ), patch.object(
                session_refresh_module, "resolve_single_paper_ib_account_from_sleeve_registry", return_value="DUO847203"
            ), patch.object(
                session_refresh_module, "_load_accounts", return_value={"PAPER": ["DUO847203"], "LIVE": []}
            ), patch.object(
                session_refresh_module, "_run", side_effect=fake_run
            ), patch.object(
                session_refresh_module, "_git_sha", return_value="abc1234"
            ), patch.object(
                session_refresh_module, "_resolve_paper_sleeve_truth_bindings", return_value=[]
            ), patch.object(
                session_refresh_module, "validate_against_repo_schema_v1", lambda *args, **kwargs: None
            ), patch(
                "sys.argv", ["run_session_readiness_refresh_v1.py", "--day_utc", DAY]
            ):
                rc = session_refresh_module.main()

            self.assertEqual(rc, 0)
            self.assertEqual(observed_session_statuses, ["OK"])
            report = json.loads((global_truth / "reports" / "session_readiness_refresh_v1" / DAY / "session_readiness_refresh.v1.json").read_text(encoding="utf-8"))
            self.assertEqual(report["status"], "OK_WITH_MONITORING_GAPS")
            self.assertIn("startup_proof_validation", report["failures"])

    def test_trade_readiness_authority_fails_closed_on_same_day_alias_drift(self) -> None:
        with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
            root = Path(td)
            truth_root = root / "constellation_2" / "runtime" / "truth"
            current_path = truth_root / "trade_submit_readiness_c2_v1" / "PAPER" / "DUO847203" / "status.json"
            history_path = truth_root / "trade_submit_readiness_c2_v1" / "_history" / "PAPER" / "DUO847203" / DAY / "status.json"
            base_payload = {
                "schema_id": "trade_submit_readiness_c2",
                "schema_version": "v1",
                "day_utc": DAY,
                "as_of_utc": f"{DAY}T00:00:00Z",
                "expires_utc": f"{DAY}T00:02:00Z",
                "environment": "PAPER",
                "ib_account": "DUO847203",
                "input_manifest": [],
                "producer": {"repo": "constellation_2_runtime", "git_sha": "abc1234", "module": "test"},
                "provenance": {
                    "truth_root": str(truth_root),
                    "registry_sha256": "a" * 64,
                    "sleeve_registry_sha256": "b" * 64,
                },
            }
            _write_json(
                current_path,
                {
                    **base_payload,
                    "ok": False,
                    "state": "FAIL",
                    "reasons": ["STALE_CURRENT"],
                },
            )
            _write_json(
                history_path,
                {
                    **base_payload,
                    "ok": True,
                    "state": "OK",
                    "reasons": ["FRESH_HISTORY"],
                },
            )
            with self.assertRaisesRegex(ValueError, "TRADE_SUBMIT_READINESS_ALIAS_DRIFT"):
                read_trade_submit_readiness_authority_state(
                    repo_root=root,
                    environment="PAPER",
                    ib_account="DUO847203",
                    day_utc=DAY,
                )

    def test_trade_readiness_authority_fails_closed_when_same_day_history_missing(self) -> None:
        with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
            root = Path(td)
            truth_root = root / "constellation_2" / "runtime" / "truth"
            current_path = truth_root / "trade_submit_readiness_c2_v1" / "PAPER" / "DUO847203" / "status.json"
            _write_json(
                current_path,
                {
                    "schema_id": "trade_submit_readiness_c2",
                    "schema_version": "v1",
                    "day_utc": "2026-03-15",
                    "as_of_utc": "2026-03-15T00:00:00Z",
                    "expires_utc": "2026-03-15T00:02:00Z",
                    "ok": False,
                    "state": "FAIL",
                    "environment": "PAPER",
                    "ib_account": "DUO847203",
                    "reasons": ["STALE_CURRENT"],
                    "input_manifest": [],
                    "producer": {"repo": "constellation_2_runtime", "git_sha": "abc1234", "module": "test"},
                    "provenance": {
                        "truth_root": str(truth_root),
                        "registry_sha256": "a" * 64,
                        "sleeve_registry_sha256": "b" * 64,
                    },
                },
            )
            with self.assertRaisesRegex(ValueError, "MISSING_FILE:path="):
                read_trade_submit_readiness_authority_state(
                    repo_root=root,
                    environment="PAPER",
                    ib_account="DUO847203",
                    day_utc=DAY,
                )

    def test_session_refresh_treats_global_gate_refresh_without_hard_failures_as_monitoring_only(self) -> None:
        def fake_run(cmd, **kwargs):
            target = str(cmd[1]) if len(cmd) > 1 else ''
            if target == str(session_refresh_module.GLOBAL_GATE_REFRESH_TOOL):
                return {
                    'cmd': cmd,
                    'returncode': 2,
                    'stdout': json.dumps({'hard_failures': []}),
                    'stderr': '',
                }
            return {'cmd': cmd, 'returncode': 0, 'stdout': '', 'stderr': ''}

        with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / 'tmp')) as td:
            root = Path(td)
            global_truth = root / 'constellation_2' / 'runtime' / 'truth'
            with patch.object(session_refresh_module, 'REPO_ROOT', root), patch.object(
                session_refresh_module, 'GLOBAL_TRUTH_ROOT', global_truth
            ), patch.object(
                session_refresh_module, 'resolve_single_paper_ib_account_from_sleeve_registry', return_value='DUO847203'
            ), patch.object(
                session_refresh_module, '_load_accounts', return_value={'PAPER': ['DUO847203'], 'LIVE': []}
            ), patch.object(
                session_refresh_module, '_run', side_effect=fake_run
            ), patch.object(
                session_refresh_module, '_git_sha', return_value='abc1234'
            ), patch.object(
                session_refresh_module, '_resolve_paper_sleeve_truth_bindings', return_value=[]
            ), patch.object(
                session_refresh_module, 'validate_against_repo_schema_v1', lambda *args, **kwargs: None
            ), patch(
                'sys.argv', ['run_session_readiness_refresh_v1.py', '--day_utc', DAY]
            ):
                rc = session_refresh_module.main()

            self.assertEqual(rc, 0)
            report = json.loads((global_truth / 'reports' / 'session_readiness_refresh_v1' / DAY / 'session_readiness_refresh.v1.json').read_text(encoding='utf-8'))
            self.assertEqual(report['status'], 'OK')
            self.assertNotIn('global_gate_refresh', report['failures'])

    def test_paper_global_gate_refresh_nonblocking_requires_expected_noop_for_canonical_failures(self) -> None:
        result = {
            "returncode": 2,
            "stdout": json.dumps(
                {
                    "hard_failures": [
                        "canonical_intent_publication_v1",
                        "canonical_market_data_preopen_prepare_v1",
                    ]
                }
            ),
            "stderr": "",
        }
        self.assertFalse(
            session_refresh_module._paper_global_gate_refresh_nonblocking(
                result, expected_calendar_no_op=False
            )
        )
        self.assertTrue(
            session_refresh_module._paper_global_gate_refresh_nonblocking(
                result, expected_calendar_no_op=True
            )
        )

    def test_scope_summary_ignores_primary_nonblocking_override(self) -> None:
        binding = SimpleNamespace(
            sleeve_id="PRIMARY",
            environment="PAPER",
            truth_root=Path("/tmp/primary"),
            truth_partition="truth_sleeves/PRIMARY/PAPER",
        )
        result = {
            "cmd": [],
            "returncode": 2,
            "stdout": json.dumps(
                {
                    "hard_failures": [
                        "canonical_intent_publication_v1",
                        "canonical_market_data_preopen_prepare_v1",
                        "signal_proof_publication_v1",
                    ]
                }
            ),
            "stderr": "",
        }
        annotated = session_refresh_module._annotate_scoped_gate_refresh_result(
            binding=binding,
            result=result,
            expected_calendar_no_op=True,
        )
        self.assertTrue(annotated["nonblocking_override"])
        self.assertEqual(annotated["state"], "READY_WITH_EXPECTED_NO_OP_GAPS")
        scope = session_refresh_module._scope_summary_from_results([annotated])
        self.assertTrue(scope["primary_ready"])
        self.assertEqual(scope["global_readiness_state"], "PASS")

    def test_session_refresh_treats_non_trading_primary_gate_failures_as_nonblocking(self) -> None:
        class _Binding:
            def __init__(self, sleeve_id: str, truth_root: Path) -> None:
                self.sleeve_id = sleeve_id
                self.environment = "PAPER"
                self.truth_root = truth_root
                self.truth_partition = f"truth_sleeves/{sleeve_id}/PAPER"

        primary_truth = REPO_ROOT / "tmp" / "session_noop_primary_truth"

        def fake_run(cmd, **kwargs):
            target = str(cmd[1]) if len(cmd) > 1 else ""
            if target == str(session_refresh_module.GLOBAL_GATE_REFRESH_TOOL) and "--truth_root" in cmd:
                scope_truth_root = cmd[cmd.index("--truth_root") + 1]
                if scope_truth_root == str(session_refresh_module.GLOBAL_TRUTH_ROOT):
                    return {
                        "cmd": cmd,
                        "returncode": 2,
                        "stdout": json.dumps(
                            {
                                "hard_failures": [
                                    "canonical_intent_publication_v1",
                                    "canonical_market_data_preopen_prepare_v1",
                                    "pointer_heads_materialize_v1",
                                ]
                            }
                        ),
                        "stderr": "",
                    }
                if scope_truth_root == str(primary_truth):
                    return {
                        "cmd": cmd,
                        "returncode": 2,
                        "stdout": json.dumps(
                            {
                                "hard_failures": [
                                    "canonical_intent_publication_v1",
                                    "canonical_market_data_preopen_prepare_v1",
                                    "signal_proof_publication_v1",
                                ]
                            }
                        ),
                        "stderr": "",
                    }
            return {"cmd": cmd, "returncode": 0, "stdout": "", "stderr": ""}

        with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
            root = Path(td)
            global_truth = root / "constellation_2" / "runtime" / "truth"
            with patch.object(session_refresh_module, "REPO_ROOT", root), patch.object(
                session_refresh_module, "GLOBAL_TRUTH_ROOT", global_truth
            ), patch.object(
                session_refresh_module, "resolve_single_paper_ib_account_from_sleeve_registry", return_value="DUO847203"
            ), patch.object(
                session_refresh_module, "_load_accounts", return_value={"PAPER": ["DUO847203"], "LIVE": []}
            ), patch.object(
                session_refresh_module, "_run", side_effect=fake_run
            ), patch.object(
                session_refresh_module, "_resolve_paper_sleeve_truth_bindings",
                return_value=[_Binding("PRIMARY", primary_truth)],
            ), patch.object(
                session_refresh_module, "_paper_expected_calendar_no_op", return_value=True
            ), patch.object(
                session_refresh_module, "_git_sha", return_value="abc1234"
            ), patch.object(
                session_refresh_module, "validate_against_repo_schema_v1", lambda *args, **kwargs: None
            ), patch(
                "sys.argv", ["run_session_readiness_refresh_v1.py", "--day_utc", DAY]
            ):
                rc = session_refresh_module.main()

            self.assertEqual(rc, 0)
            report = json.loads(
                (global_truth / "reports" / "session_readiness_refresh_v1" / DAY / "session_readiness_refresh.v1.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(report["status"], "OK")
            self.assertNotIn("global_gate_refresh", report["failures"])
            self.assertFalse(any(item.startswith("scoped_gate_refresh:PRIMARY:") for item in report["failures"]))
            scoped_rows = {row["sleeve_id"]: row for row in report["results"]["scoped_gate_refresh"]}
            self.assertEqual(scoped_rows["PRIMARY"]["state"], "READY_WITH_EXPECTED_NO_OP_GAPS")
            self.assertTrue(scoped_rows["PRIMARY"]["nonblocking_override"])
            self.assertEqual(report["results"]["scope_summary"]["global_readiness_state"], "PASS")
            self.assertTrue(report["results"]["scope_summary"]["primary_ready"])

    def test_session_refresh_treats_non_primary_scoped_failures_as_monitoring_only(self) -> None:
        class _Binding:
            def __init__(self, sleeve_id: str, truth_root: Path) -> None:
                self.sleeve_id = sleeve_id
                self.environment = 'PAPER'
                self.truth_root = truth_root
                self.truth_partition = f'truth_sleeves/{sleeve_id}/PAPER'

        primary_truth = REPO_ROOT / 'tmp' / 'session_scope_primary_truth'
        tail_truth = REPO_ROOT / 'tmp' / 'session_scope_tail_truth'

        def fake_run(cmd, **kwargs):
            target = str(cmd[1]) if len(cmd) > 1 else ''
            if target == str(session_refresh_module.GLOBAL_GATE_REFRESH_TOOL) and '--truth_root' in cmd:
                scope_truth_root = cmd[cmd.index('--truth_root') + 1]
                if scope_truth_root == str(tail_truth):
                    return {
                        'cmd': cmd,
                        'returncode': 2,
                        'stdout': json.dumps({'hard_failures': ['operator_daily_gate_v3']}),
                        'stderr': '',
                    }
            if target == str(session_refresh_module.STARTUP_PROOF_VALIDATION_TOOL):
                return {'cmd': cmd, 'returncode': 0, 'stdout': json.dumps({'status': 'STARTUP_READY_WITH_PENDING_MONITORING'}), 'stderr': ''}
            return {'cmd': cmd, 'returncode': 0, 'stdout': '', 'stderr': ''}

        with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / 'tmp')) as td:
            root = Path(td)
            global_truth = root / 'constellation_2' / 'runtime' / 'truth'
            with patch.object(session_refresh_module, 'REPO_ROOT', root), patch.object(
                session_refresh_module, 'GLOBAL_TRUTH_ROOT', global_truth
            ), patch.object(
                session_refresh_module, 'resolve_single_paper_ib_account_from_sleeve_registry', return_value='DUO847203'
            ), patch.object(
                session_refresh_module, '_load_accounts', return_value={'PAPER': ['DUO847203'], 'LIVE': []}
            ), patch.object(
                session_refresh_module, '_run', side_effect=fake_run
            ), patch.object(
                session_refresh_module, '_resolve_paper_sleeve_truth_bindings',
                return_value=[_Binding('PRIMARY', primary_truth), _Binding('C2_DEFENSIVE_TAIL', tail_truth)],
            ), patch.object(
                session_refresh_module, '_git_sha', return_value='abc1234'
            ), patch.object(
                session_refresh_module, 'validate_against_repo_schema_v1', lambda *args, **kwargs: None
            ), patch(
                'sys.argv', ['run_session_readiness_refresh_v1.py', '--day_utc', DAY]
            ):
                rc = session_refresh_module.main()

            self.assertEqual(rc, 0)
            report = json.loads((global_truth / 'reports' / 'session_readiness_refresh_v1' / DAY / 'session_readiness_refresh.v1.json').read_text(encoding='utf-8'))
            self.assertEqual(report['status'], 'OK_WITH_MONITORING_GAPS')
            self.assertEqual(report['results']['scope_summary']['global_readiness_state'], 'DEGRADED')
            self.assertEqual(report['results']['scope_summary']['nonblocking_blocked_sleeve_ids'], ['C2_DEFENSIVE_TAIL'])
            self.assertFalse(any('C2_DEFENSIVE_TAIL' in failure for failure in report['failures']))
            scoped_rows = {row['sleeve_id']: row for row in report['results']['scoped_gate_refresh']}
            self.assertEqual(scoped_rows['PRIMARY']['state'], 'READY')
            self.assertEqual(scoped_rows['C2_DEFENSIVE_TAIL']['state'], 'BLOCKED')
            self.assertEqual(scoped_rows['C2_DEFENSIVE_TAIL']['blocking_scope'], 'SLEEVE')


if __name__ == "__main__":
    unittest.main()
