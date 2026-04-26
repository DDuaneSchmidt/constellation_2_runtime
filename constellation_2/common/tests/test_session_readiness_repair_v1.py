from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
(REPO_ROOT / "tmp").mkdir(parents=True, exist_ok=True)

import ops.tools.run_ib_api_handshake_spine_v1 as handshake_module
import ops.tools.run_c2_paper_day_orchestrator_v2 as orchestrator_module
import ops.tools.run_operator_daily_gate_v3 as operator_gate_module
import ops.tools.run_session_readiness_refresh_v1 as session_refresh_module
import ops.tools.run_trade_submit_readiness_c2_v1 as readiness_module
from constellation_2.common.trade_submit_readiness_authority_v1 import read_trade_submit_readiness_authority_state
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


def _readiness_day_authority_tuple(*, truth_root: Path, day: str) -> tuple[dict, Path, str]:
    path = (truth_root / "reports" / "day_authority_decision_v1" / day / "day_authority_decision.v1.json").resolve()
    payload = {
        "decision_state": "OPEN",
        "stage": "PRE_ORCHESTRATION_PREFLIGHT",
        "blocking_evidence": [],
        "emitted_at": f"{day}T00:00:00Z",
    }
    return payload, path, "a" * 64


def _readiness_economic_ok(*, day: str) -> dict:
    return {
        "status": "OK",
        "source_day_utc": "2026-03-15",
        "package_path": "",
        "package_sha256": "",
        "build_path": "",
        "build_sha256": "",
        "drawdown_pct": None,
        "drawdown_guard_status": "PASS",
        "policy_baseline_comparison_vs_portfolio_return": None,
        "external_benchmark_underperformer_count": 0,
        "reason_codes": [],
    }


def _readiness_authorization_pass_snapshot(*, truth_root: Path, day: str) -> dict:
    return {
        "binding": SimpleNamespace(sleeve_id="PRIMARY"),
        "authorization_path": (truth_root / "reports" / "authorization_gate_verdict_v1" / day / "authorization_gate_verdict.v1.json"),
        "authorization_sha256": "c" * 64,
        "authorization_payload": {"status": "PASS", "day_utc": day},
        "authorization_status": "PASS",
        "input_manifest": [],
    }


def _readiness_policy_state(*, schema_id: str, day: str) -> dict:
    if schema_id == "policy_diff":
        payload = {"day_utc": day, "production_only_open_items": []}
    elif schema_id in {"paper_policy_verdict", "production_policy_verdict"}:
        payload = {"day_utc": day, "overall_status": "PASS", "blocking_items": []}
    else:
        payload = {"day_utc": day}
    return {"path": Path(f"/tmp/{schema_id}.json"), "sha256": "d" * 64, "payload": payload}


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
                        "ib_gateway_profile": {
                            "host": "127.0.0.1",
                            "port": 4002,
                            "client_id_orders": 178,
                            "client_id_observer": 179,
                        },
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
            latest = json.loads((truth_root / "ib_api_handshake" / "latest_pointer.v1.json").read_text(encoding="utf-8"))
            self.assertEqual(latest["day_utc"], DAY)
            self.assertEqual(latest["pointers"]["handshake_path"], str(truth_root / "ib_api_handshake" / DAY / "ib_api_handshake.v1.json"))

    def test_broker_bootstrap_python_preserves_venv_symlink_path(self) -> None:
        with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
            root = Path(td)
            real_python = root / "python-real"
            real_python.write_text("#!/bin/sh\n", encoding="utf-8")
            symlink_python = root / "python"
            symlink_python.symlink_to(real_python)
            with patch.object(session_refresh_module, "BROKER_EVENTS_BOOTSTRAP_PYTHON", symlink_python):
                resolved = session_refresh_module._resolve_broker_events_bootstrap_python()
            self.assertEqual(resolved, symlink_python)

    def test_broker_bootstrap_python_defaults_to_current_execution_python(self) -> None:
        with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
            root = Path(td)
            current_python = root / "python-current"
            current_python.write_text("#!/bin/sh\n", encoding="utf-8")
            with patch.object(session_refresh_module.sys, "executable", str(current_python)), patch.object(
                session_refresh_module, "BROKER_EVENTS_BOOTSTRAP_PYTHON", root / "missing-python"
            ):
                resolved = session_refresh_module._resolve_broker_events_bootstrap_python()
            self.assertEqual(resolved, current_python)

    def test_broker_bootstrap_reason_codes_surface_ibapi_import_failure(self) -> None:
        result = {
            "returncode": 1,
            "stdout": "",
            "stderr": "Traceback...\nModuleNotFoundError: No module named 'ibapi'",
        }
        codes = session_refresh_module._broker_bootstrap_reason_codes(result)
        self.assertIn("READINESS_BOOTSTRAP_IBAPI_IMPORT_FAILED", codes)

    def test_positions_snapshot_v2_skip_safe_rejects_zero_qty_open_positions(self) -> None:
        with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
            truth_root = Path(td) / "truth"
            snap_path = truth_root / "positions_v1" / "snapshots" / DAY / "positions_snapshot.v2.json"
            _write_json(
                snap_path,
                {
                    "schema_id": "C2_POSITIONS_SNAPSHOT_V2",
                    "schema_version": 2,
                    "day_utc": DAY,
                    "produced_utc": f"{DAY}T00:00:00Z",
                    "producer": {
                        "repo": "constellation",
                        "git_sha": "abc1234",
                        "module": "constellation_2/phaseF/positions/run/run_positions_snapshot_day_v2.py",
                    },
                    "status": "OK",
                    "reason_codes": ["CARRY_FORWARD_OPEN_POSITIONS_V2"],
                    "input_manifest": [],
                    "positions": {
                        "currency": "USD",
                        "asof_utc": f"{DAY}T00:00:00Z",
                        "items": [
                            {
                                "position_id": "p1",
                                "engine_id": "C2_TREND_EQ_PRIMARY_V1",
                                "instrument": {
                                    "kind": "EQUITY",
                                    "underlying": "SPY",
                                    "expiry": None,
                                    "strike": None,
                                    "right": None,
                                },
                                "qty": 0,
                                "avg_cost_cents": 0,
                                "market_exposure_type": "UNDEFINED_RISK",
                                "max_loss_cents": None,
                                "opened_day_utc": DAY,
                                "status": "OPEN",
                            }
                        ],
                        "notes": ["seeded"],
                    },
                },
            )

            self.assertFalse(orchestrator_module._positions_snapshot_v2_skip_safe(truth_root, DAY))

    def test_handshake_refreshes_same_day_fail_to_ok_when_broker_events_arrive(self) -> None:
        with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
            root = Path(td)
            truth_root = root / "constellation_2" / "runtime" / "truth"
            handshake_path = truth_root / "ib_api_handshake" / DAY / "ib_api_handshake.v1.json"
            existing_fail = {
                "schema_id": "C2_IB_API_HANDSHAKE_V1",
                "schema_version": 1,
                "day_utc": DAY,
                "status": "FAIL",
                "ok": False,
                "reason_codes": ["BROKER_EVENTS_MISSING"],
                "inputs": {"broker_event_log": str(truth_root / "execution_evidence_v1" / "broker_events" / DAY / "broker_event_log.v1.jsonl")},
                "observations": {},
            }
            _write_json(handshake_path, existing_fail)
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
            _write_jsonl(
                truth_root / "execution_evidence_v1" / "broker_events" / DAY / "broker_event_log.v1.jsonl",
                [
                    {"event_type": "starting", "ib_fields": {"args": [{"value": "host=127.0.0.1"}]}},
                    {"event_type": "nextValidId", "ib_fields": {"args": [{"value": "orderId=44"}]}},
                    {"event_type": "openOrderEnd", "ib_fields": {"args": [{"value": "openOrderEnd()"}]}},
                ],
            )
            with patch.object(handshake_module, "REPO_ROOT", root), patch.object(
                handshake_module, "validate_against_repo_schema_v1", lambda *args, **kwargs: None
            ):
                rc = handshake_module.main(["--day_utc", DAY, "--truth_root", str(truth_root)])
            self.assertEqual(rc, 0)
            out = json.loads(handshake_path.read_text(encoding="utf-8"))
            self.assertEqual(out["status"], "OK")
            self.assertTrue(out["ok"])
            self.assertIn("HANDSHAKE_OK_NEXTVALIDID_SEEN_NO_504_AFTER", out["reason_codes"])
            latest = json.loads((truth_root / "ib_api_handshake" / "latest_pointer.v1.json").read_text(encoding="utf-8"))
            self.assertEqual(latest["day_utc"], DAY)
            self.assertEqual(latest["pointers"]["handshake_path"], str(handshake_path))
            self.assertEqual(latest["pointers"]["handshake_sha256"], readiness_module._sha256_file(handshake_path))

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
                readiness_module,
                "resolve_sleeve_execution_root_v1",
                return_value=SimpleNamespace(execution_root_path=truth_root),
            ), patch.object(
                readiness_module,
                "_load_day_authority",
                return_value=_readiness_day_authority_tuple(truth_root=truth_root, day=DAY),
            ), patch.object(
                readiness_module,
                "_load_primary_scoped_authorization_snapshot",
                return_value=_readiness_authorization_pass_snapshot(truth_root=truth_root, day=DAY),
            ), patch.object(
                readiness_module, "_refresh_policy_stack_for_day", lambda **kwargs: None
            ), patch.object(
                readiness_module,
                "_read_policy_artifact",
                side_effect=lambda **kwargs: _readiness_policy_state(
                    schema_id=str(kwargs.get("expected_schema_id") or ""),
                    day=DAY,
                ),
            ), patch.object(
                readiness_module, "_load_previous_day_economic_package_state", return_value=_readiness_economic_ok(day=DAY)
            ), patch.object(
                readiness_module, "validate_against_repo_schema_v1", lambda *args, **kwargs: None
            ), patch(
                "sys.argv",
                ["run_trade_submit_readiness_c2_v1.py", "--day_utc", DAY, "--ib_account", "DUO847203", "--environment", "PAPER"],
            ):
                rc = readiness_module.main()
            self.assertEqual(rc, 2)
            out = _read_trade_status_for_day(truth_root=truth_root, day=DAY)
            self.assertFalse(out["ok"])
            self.assertTrue(any(reason.startswith("FAIL:") for reason in out["reasons"]))

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
                readiness_module,
                "resolve_sleeve_execution_root_v1",
                return_value=SimpleNamespace(execution_root_path=truth_root),
            ), patch.object(
                readiness_module,
                "_load_day_authority",
                return_value=_readiness_day_authority_tuple(truth_root=truth_root, day=DAY),
            ), patch.object(
                readiness_module,
                "_load_primary_scoped_authorization_snapshot",
                return_value=_readiness_authorization_pass_snapshot(truth_root=truth_root, day=DAY),
            ), patch.object(
                readiness_module, "_refresh_policy_stack_for_day", lambda **kwargs: None
            ), patch.object(
                readiness_module,
                "_read_policy_artifact",
                side_effect=lambda **kwargs: _readiness_policy_state(
                    schema_id=str(kwargs.get("expected_schema_id") or ""),
                    day=DAY,
                ),
            ), patch.object(
                readiness_module, "_load_previous_day_economic_package_state", return_value=_readiness_economic_ok(day=DAY)
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
                readiness_module,
                "resolve_sleeve_execution_root_v1",
                return_value=SimpleNamespace(execution_root_path=truth_root),
            ), patch.object(
                readiness_module,
                "_load_day_authority",
                return_value=_readiness_day_authority_tuple(truth_root=truth_root, day=DAY),
            ), patch.object(
                readiness_module, "_load_previous_day_economic_package_state", return_value=_readiness_economic_ok(day=DAY)
            ), patch.object(
                readiness_module, "validate_against_repo_schema_v1", lambda *args, **kwargs: None
            ), patch(
                "sys.argv",
                ["run_trade_submit_readiness_c2_v1.py", "--day_utc", DAY, "--ib_account", "DUO847203", "--environment", "PAPER"],
            ):
                rc = readiness_module.main()
            self.assertEqual(rc, 2)
            out = _read_trade_status_for_day(truth_root=truth_root, day=DAY)
            self.assertFalse(out["ok"])
            self.assertTrue(any(reason.startswith("FAIL:") for reason in out["reasons"]))

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
                readiness_module,
                "resolve_sleeve_execution_root_v1",
                return_value=SimpleNamespace(execution_root_path=truth_root),
            ), patch.object(
                readiness_module,
                "_load_day_authority",
                return_value=_readiness_day_authority_tuple(truth_root=truth_root, day=DAY),
            ), patch.object(
                readiness_module,
                "_load_primary_scoped_authorization_snapshot",
                return_value=_readiness_authorization_pass_snapshot(truth_root=truth_root, day=DAY),
            ), patch.object(
                readiness_module, "_refresh_policy_stack_for_day", lambda **kwargs: None
            ), patch.object(
                readiness_module,
                "_read_policy_artifact",
                side_effect=lambda **kwargs: _readiness_policy_state(
                    schema_id=str(kwargs.get("expected_schema_id") or ""),
                    day=DAY,
                ),
            ), patch.object(
                readiness_module, "_load_previous_day_economic_package_state", return_value=_readiness_economic_ok(day=DAY)
            ), patch.object(
                readiness_module, "validate_against_repo_schema_v1", lambda *args, **kwargs: None
            ), patch(
                "sys.argv",
                ["run_trade_submit_readiness_c2_v1.py", "--day_utc", DAY, "--ib_account", "DUO847203", "--environment", "PAPER"],
            ):
                rc = readiness_module.main()
            self.assertEqual(rc, 2)
            out = _read_trade_status_for_day(truth_root=truth_root, day=DAY)
            self.assertTrue(any(reason.startswith("FAIL:IB_API_HANDSHAKE_NOT_OK:") for reason in out["reasons"]))

    def test_trade_submit_readiness_prefers_current_day_scoped_gate_over_stale_head(self) -> None:
        with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
            root = Path(td)
            truth_root = root / "constellation_2" / "runtime" / "truth"
            self._write_minimal_registries(root)
            _write_scoped_gate_stack_authority_day(root=root, sleeve_id="PRIMARY", day="2026-03-13", status="PASS")
            _write_gate_stack_authority_day(
                truth_root=root / "constellation_2" / "runtime" / "truth_sleeves" / "PRIMARY" / "PAPER",
                day=DAY,
                status="FAIL",
            )
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
                readiness_module,
                "resolve_sleeve_execution_root_v1",
                return_value=SimpleNamespace(execution_root_path=truth_root),
            ), patch.object(
                readiness_module,
                "_load_day_authority",
                return_value=_readiness_day_authority_tuple(truth_root=truth_root, day=DAY),
            ), patch.object(
                readiness_module, "_load_previous_day_economic_package_state", return_value=_readiness_economic_ok(day=DAY)
            ), patch.object(
                readiness_module, "validate_against_repo_schema_v1", lambda *args, **kwargs: None
            ), patch(
                "sys.argv",
                ["run_trade_submit_readiness_c2_v1.py", "--day_utc", DAY, "--ib_account", "DUO847203", "--environment", "PAPER"],
            ):
                rc = readiness_module.main()
            self.assertEqual(rc, 2)
            out = _read_trade_status_for_day(truth_root=truth_root, day=DAY)
            self.assertFalse(out["ok"])
            self.assertTrue(any(reason.startswith("FAIL:") for reason in out["reasons"]))
            self.assertFalse(any("GATE_STACK_DAY_MISMATCH" in reason for reason in out["reasons"]))

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
                readiness_module,
                "resolve_sleeve_execution_root_v1",
                return_value=SimpleNamespace(execution_root_path=truth_root),
            ), patch.object(
                readiness_module,
                "_load_day_authority",
                return_value=_readiness_day_authority_tuple(truth_root=truth_root, day=DAY),
            ), patch.object(
                readiness_module, "_load_previous_day_economic_package_state", return_value=_readiness_economic_ok(day=DAY)
            ), patch.object(
                readiness_module, "validate_against_repo_schema_v1", lambda *args, **kwargs: None
            ), patch(
                "sys.argv",
                ["run_trade_submit_readiness_c2_v1.py", "--day_utc", DAY, "--ib_account", "DUO847203", "--environment", "PAPER"],
            ):
                rc = readiness_module.main()
            self.assertEqual(rc, 2)
            out = _read_trade_status_for_day(truth_root=truth_root, day=DAY)
            self.assertFalse(out["ok"])
            self.assertTrue(any(reason.startswith("FAIL:") for reason in out["reasons"]))

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
            session_refresh_module, "validate_against_repo_schema_v1", lambda *args, **kwargs: None
        ), patch.object(
            session_refresh_module, "_authority_lifecycle_result", return_value={"status": "OK", "incident_count": 0, "incidents": []}
        ), patch(
            "sys.argv", ["run_session_readiness_refresh_v1.py", "--day_utc", DAY]
        ):
            rc = session_refresh_module.main()

        self.assertIn(rc, (0, 2))
        self.assertGreaterEqual(len(calls), 5)
        operator_statement_cmd = next(
            cmd for cmd in calls if len(cmd) > 1 and "ensure_cash_ledger_operator_statement_v1.py" in str(cmd[1])
        )
        bootstrap_index = next(i for i, cmd in enumerate(calls) if len(cmd) > 1 and str(cmd[1]) == str(session_refresh_module.BROKER_EVENTS_BOOTSTRAP_TOOL))
        manifest_index = next(i for i, cmd in enumerate(calls) if len(cmd) > 1 and str(cmd[1]) == str(session_refresh_module.BROKER_EVENTS_MANIFEST_TOOL))
        handshake_index = next(i for i, cmd in enumerate(calls) if len(cmd) > 1 and str(cmd[1]) == str(session_refresh_module.HANDSHAKE_TOOL))
        bootstrap_cmd = calls[bootstrap_index]
        manifest_cmd = calls[manifest_index]
        handshake_cmd = calls[handshake_index]
        self.assertIn("ensure_cash_ledger_operator_statement_v1.py", str(operator_statement_cmd[1]))
        self.assertIn("ops/ib/c2_execution_observer_v1.py", str(bootstrap_cmd[1]))
        self.assertIn("--bootstrap-handshake-only", bootstrap_cmd)
        self.assertIn("--truth_root", bootstrap_cmd)
        bootstrap_truth_root = bootstrap_cmd[bootstrap_cmd.index("--truth_root") + 1]
        self.assertIn("run_broker_event_day_manifest_v1.py", str(manifest_cmd[1]))
        self.assertIn("--truth_root", manifest_cmd)
        self.assertEqual(manifest_cmd[manifest_cmd.index("--truth_root") + 1], bootstrap_truth_root)
        self.assertEqual(handshake_cmd[1], str(session_refresh_module.HANDSHAKE_TOOL))
        self.assertIn("--truth_root", handshake_cmd)
        self.assertEqual(handshake_cmd[handshake_cmd.index("--truth_root") + 1], bootstrap_truth_root)
        self.assertLess(bootstrap_index, handshake_index)
        self.assertLess(manifest_index, handshake_index)

    def test_session_refresh_runs_global_gate_refresh_before_trade_submit_readiness(self) -> None:
        calls = []

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            target = str(cmd[1]) if len(cmd) > 1 else ""
            if target == str(session_refresh_module.PAPER_SESSION_LEDGER_TOOL):
                return {"cmd": cmd, "returncode": 0, "stdout": json.dumps({"authority_status": "GRANTED"}), "stderr": ""}
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
        self.assertEqual(session_refresh_module.GLOBAL_GATE_REFRESH_TOOL.name, "run_gate_authority_plane_v1.py")
        global_gate_refresh_index = next(
            i for i, cmd in enumerate(calls) if len(cmd) > 1 and str(cmd[1]) == str(session_refresh_module.GLOBAL_GATE_REFRESH_TOOL)
        )
        global_gate_cmd = calls[global_gate_refresh_index]
        trade_submit_index = next(
            i for i, cmd in enumerate(calls) if len(cmd) > 1 and str(cmd[1]) == str(session_refresh_module.READINESS_TOOL)
        )
        self.assertIn("--produced_utc", global_gate_cmd)
        self.assertEqual(global_gate_cmd[global_gate_cmd.index("--produced_utc") + 1], f"{DAY}T00:00:00Z")
        self.assertEqual(global_gate_cmd[global_gate_cmd.index("--mode") + 1], "PAPER")
        self.assertLess(global_gate_refresh_index, trade_submit_index)

    def test_session_refresh_rebuilds_canonical_target_day_before_submit_boundary(self) -> None:
        calls = []
        envs = []

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            envs.append(kwargs.get("extra_env"))
            target = str(cmd[1]) if len(cmd) > 1 else ""
            if target == str(session_refresh_module.PAPER_SESSION_LEDGER_TOOL):
                return {"cmd": cmd, "returncode": 0, "stdout": json.dumps({"authority_status": "GRANTED"}), "stderr": ""}
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
        build_index = next(
            i for i, cmd in enumerate(calls) if len(cmd) > 1 and str(cmd[1]) == str(session_refresh_module.SESSION_AUTHORITY_TOOL) and "--phase" in cmd and cmd[cmd.index("--phase") + 1] == "build"
        )
        admit_index = next(
            i for i, cmd in enumerate(calls) if len(cmd) > 1 and str(cmd[1]) == str(session_refresh_module.SESSION_AUTHORITY_TOOL) and "--phase" in cmd and cmd[cmd.index("--phase") + 1] == "admit"
        )
        submit_boundary_index = next(
            i for i, cmd in enumerate(calls) if len(cmd) > 1 and str(cmd[1]) == str(session_refresh_module.SUBMIT_BOUNDARY_STATUS_TOOL)
        )
        self.assertLess(build_index, admit_index)
        self.assertLess(admit_index, submit_boundary_index)
        self.assertEqual(envs[build_index], session_refresh_module._monitoring_refresh_extra_env())
        self.assertEqual(envs[admit_index], session_refresh_module._monitoring_refresh_extra_env())

    def test_session_refresh_reruns_trade_submit_readiness_after_canonical_target_day_refresh(self) -> None:
        calls = []

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            target = str(cmd[1]) if len(cmd) > 1 else ""
            if target == str(session_refresh_module.PAPER_SESSION_LEDGER_TOOL):
                return {"cmd": cmd, "returncode": 0, "stdout": json.dumps({"authority_status": "GRANTED"}), "stderr": ""}
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
        readiness_indexes = [
            i for i, cmd in enumerate(calls) if len(cmd) > 1 and str(cmd[1]) == str(session_refresh_module.READINESS_TOOL)
        ]
        self.assertEqual(len(readiness_indexes), 2)
        build_index = next(
            i for i, cmd in enumerate(calls) if len(cmd) > 1 and str(cmd[1]) == str(session_refresh_module.SESSION_AUTHORITY_TOOL) and "--phase" in cmd and cmd[cmd.index("--phase") + 1] == "build"
        )
        admit_index = next(
            i for i, cmd in enumerate(calls) if len(cmd) > 1 and str(cmd[1]) == str(session_refresh_module.SESSION_AUTHORITY_TOOL) and "--phase" in cmd and cmd[cmd.index("--phase") + 1] == "admit"
        )
        submit_boundary_index = next(
            i for i, cmd in enumerate(calls) if len(cmd) > 1 and str(cmd[1]) == str(session_refresh_module.SUBMIT_BOUNDARY_STATUS_TOOL)
        )
        self.assertLess(readiness_indexes[0], build_index)
        self.assertLess(admit_index, readiness_indexes[1])
        self.assertLess(readiness_indexes[1], submit_boundary_index)

    def test_session_refresh_reactivates_active_session_after_canonical_target_day_refresh(self) -> None:
        calls = []
        envs = []

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            envs.append(kwargs.get("extra_env"))
            target = str(cmd[1]) if len(cmd) > 1 else ""
            if target == str(session_refresh_module.PAPER_SESSION_LEDGER_TOOL):
                return {"cmd": cmd, "returncode": 0, "stdout": json.dumps({"authority_status": "GRANTED"}), "stderr": ""}
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
        build_index = next(
            i for i, cmd in enumerate(calls) if len(cmd) > 1 and str(cmd[1]) == str(session_refresh_module.SESSION_AUTHORITY_TOOL) and "--phase" in cmd and cmd[cmd.index("--phase") + 1] == "build"
        )
        admit_index = next(
            i for i, cmd in enumerate(calls) if len(cmd) > 1 and str(cmd[1]) == str(session_refresh_module.SESSION_AUTHORITY_TOOL) and "--phase" in cmd and cmd[cmd.index("--phase") + 1] == "admit"
        )
        activate_index = next(
            i for i, cmd in enumerate(calls) if len(cmd) > 1 and str(cmd[1]) == str(session_refresh_module.SESSION_AUTHORITY_TOOL) and "--phase" in cmd and cmd[cmd.index("--phase") + 1] == "activate"
        )
        submit_boundary_index = next(
            i for i, cmd in enumerate(calls) if len(cmd) > 1 and str(cmd[1]) == str(session_refresh_module.SUBMIT_BOUNDARY_STATUS_TOOL)
        )
        self.assertLess(build_index, admit_index)
        self.assertLess(admit_index, activate_index)
        self.assertLess(activate_index, submit_boundary_index)
        self.assertEqual(envs[activate_index], session_refresh_module._monitoring_refresh_extra_env())

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

        self.assertIn(rc, (0, 2))

    def test_session_refresh_runs_scoped_gate_refresh_before_trade_submit_readiness(self) -> None:
        calls = []
        primary_truth = REPO_ROOT / "tmp" / "fake_primary_truth"
        tail_truth = REPO_ROOT / "tmp" / "fake_tail_truth"

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            target = str(cmd[1]) if len(cmd) > 1 else ""
            if target == str(session_refresh_module.PAPER_SESSION_LEDGER_TOOL):
                return {"cmd": cmd, "returncode": 0, "stdout": json.dumps({"authority_status": "GRANTED"}), "stderr": ""}
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

        self.assertIn(rc, (0, 2))
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
        self.assertGreaterEqual(len(scoped_gate_calls), 1)
        for idx, _cmd in scoped_gate_calls:
            self.assertLess(idx, trade_submit_index)

    def test_session_refresh_materializes_b2_inputs_before_global_gate_refresh(self) -> None:
        calls = []

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            target = str(cmd[1]) if len(cmd) > 1 else ""
            if target == str(session_refresh_module.PAPER_SESSION_LEDGER_TOOL):
                return {"cmd": cmd, "returncode": 0, "stdout": json.dumps({"authority_status": "GRANTED"}), "stderr": ""}
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

            canonical_positions = global_truth / "positions_v1" / "snapshots" / day / "positions_snapshot.v5.json"
            _write_json(
                canonical_positions,
                {
                    "schema_id": "C2_POSITIONS_SNAPSHOT_V5",
                    "schema_version": 5,
                    "day_utc": day,
                    "produced_utc": f"{day}T00:00:00Z",
                    "producer": {"repo": "constellation_2_runtime", "git_sha": "abc1234", "module": "test"},
                    "status": "OK",
                    "reason_codes": ["BUNDLE_A_CANONICAL_STATE_V5"],
                    "input_manifest": [],
                    "accounts": [
                        {
                            "account_id": "DUO847203",
                            "currency": "USD",
                            "cash_total_cents": 100000,
                            "broker_cash_cents": 100000,
                            "cash_source": "CASH_LEDGER_ONLY",
                            "reason_codes": [],
                        }
                    ],
                    "items": [],
                    "reconciliation": {
                        "broker_statement_present": True,
                        "broker_statement_path": "/tmp/broker.json",
                        "cash_status": "MATCH",
                        "cash_delta_cents": 0,
                        "positions_status": "MATCH",
                        "reason_codes": [],
                        "position_mismatches": [],
                    },
                    "canonical_json_hash": "1" * 64,
                },
            )

            binding = SimpleNamespace(
                sleeve_id="PRIMARY",
                environment="PAPER",
                truth_root=sleeve_truth,
                truth_partition="truth_sleeves/PRIMARY/PAPER",
            )
            seeded_path = sleeve_truth / "positions_v1" / "snapshots" / day / "positions_snapshot.v5.json"

            with patch.object(session_refresh_module, "REPO_ROOT", root), patch.object(
                session_refresh_module, "GLOBAL_TRUTH_ROOT", global_truth
            ), patch.object(
                session_refresh_module, "_resolve_paper_sleeve_truth_bindings", return_value=[binding]
            ), patch.object(
                session_refresh_module,
                "_run",
                side_effect=lambda cmd, extra_env=None: (
                    seeded_path.parent.mkdir(parents=True, exist_ok=True),
                    seeded_path.write_text(canonical_positions.read_text(encoding="utf-8"), encoding="utf-8"),
                    {"cmd": cmd, "returncode": 0, "stdout": "OK", "stderr": ""},
                )[-1],
            ), patch.object(
                orchestrator_module, "REPO_ROOT", root
            ):
                result = session_refresh_module._seed_sleeve_positions_snapshots(day_utc=day, paper_account="DUO847203")
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
                operator_gate_module,
                "assert_constitutional_writer_allowed_v1",
                return_value={"artifact_class": "report", "required_upstream_dependencies": []},
            ), patch.object(
                operator_gate_module,
                "build_artifact_dependency_declaration_v1",
                return_value={"dependency_refs": []},
            ), patch.object(
                operator_gate_module,
                "build_governed_artifact_lineage_v1",
                return_value={"generated_at_utc": f"{day}T00:00:00Z"},
            ), patch.object(
                operator_gate_module,
                "build_governed_dependency_ref_v1",
                return_value={
                    "artifact_id": "test_dependency",
                    "path": "/tmp/test_dependency.json",
                    "sha256": "0" * 64,
                    "artifact_class": "report",
                    "finality_state": "provisional",
                },
            ), patch.object(
                operator_gate_module,
                "validate_governed_artifact_payload_v1",
                lambda **kwargs: None,
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

            gate = json.loads(
                (sleeve_truth / "reports" / "operator_daily_gate_v3" / day / "operator_daily_gate.v3.json").read_text(encoding="utf-8")
            )
            self.assertTrue(gate["checks"]["positions_snapshot_present"])
            self.assertNotIn("MISSING_POSITIONS_SNAPSHOT", gate["reason_codes"])

    def test_session_refresh_includes_pnl_attribution_pending_when_nav_missing(self) -> None:
        calls = []

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            return {"cmd": cmd, "returncode": 0, "stdout": "", "stderr": ""}

        with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
            root = Path(td)
            global_truth = root / "constellation_2" / "runtime" / "truth"
            self._write_minimal_registries(root)
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

            self.assertIn(rc, (0, 2))
            report = json.loads((global_truth / "reports" / "session_readiness_refresh_v1" / DAY / "session_readiness_refresh.v1.json").read_text(encoding="utf-8"))
            self.assertIn(
                report["results"]["pnl_attribution"]["status"],
                {"PENDING_NAV_NOT_AVAILABLE", "SKIPPED_OPTIONAL_TOOL_MISSING"},
            )
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
            self._write_minimal_registries(root)
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

            self.assertIn(rc, (0, 2))
            report = json.loads((global_truth / "reports" / "session_readiness_refresh_v1" / DAY / "session_readiness_refresh.v1.json").read_text(encoding="utf-8"))
            self.assertEqual(report["status"], "FAIL")
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
            self._write_minimal_registries(root)
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

            self.assertIn(rc, (0, 2))
            self.assertEqual(observed_session_statuses, ["FAIL"])
            report = json.loads((global_truth / "reports" / "session_readiness_refresh_v1" / DAY / "session_readiness_refresh.v1.json").read_text(encoding="utf-8"))
            self.assertEqual(report["status"], "FAIL")
            self.assertIn("startup_proof_validation", report["failures"])

    def test_trade_readiness_authority_fails_closed_on_same_day_alias_drift(self) -> None:
        with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
            root = Path(td)
            truth_root = root / "constellation_2" / "runtime" / "truth"
            self._write_minimal_registries(root)
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
            with self.assertRaisesRegex(ValueError, "(TRADE_SUBMIT_READINESS_ALIAS_DRIFT|TRADE_SUBMIT_READINESS_CONSTITUTIONAL_INVALID)"):
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
            self._write_minimal_registries(root)
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
            with self.assertRaisesRegex(ValueError, "(MISSING_FILE:path=|TRADE_SUBMIT_READINESS_CONSTITUTIONAL_INVALID)"):
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
            if target == str(session_refresh_module.PAPER_SESSION_LEDGER_TOOL):
                return {'cmd': cmd, 'returncode': 0, 'stdout': json.dumps({'authority_status': 'GRANTED'}), 'stderr': ''}
            return {'cmd': cmd, 'returncode': 0, 'stdout': '', 'stderr': ''}

        with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / 'tmp')) as td:
            root = Path(td)
            global_truth = root / 'constellation_2' / 'runtime' / 'truth'
            self._write_minimal_registries(root)
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
            if target == str(session_refresh_module.PAPER_SESSION_LEDGER_TOOL):
                return {"cmd": cmd, "returncode": 0, "stdout": json.dumps({"authority_status": "GRANTED"}), "stderr": ""}
            return {"cmd": cmd, "returncode": 0, "stdout": "", "stderr": ""}

        with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
            root = Path(td)
            global_truth = root / "constellation_2" / "runtime" / "truth"
            self._write_minimal_registries(root)
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
                session_refresh_module,
                "_run_primary_startup_authorization_refresh",
                return_value={
                    "sleeve_id": "PRIMARY",
                    "state": "READY_WITH_EXPECTED_NO_OP_GAPS",
                    "nonblocking_override": True,
                    "returncode": 2,
                    "blocking_scope": "PRIMARY_SLEEVE",
                    "reason_codes": [
                        "canonical_intent_publication_v1",
                        "canonical_market_data_preopen_prepare_v1",
                        "signal_proof_publication_v1",
                    ],
                    "scope_truth_root": str(primary_truth),
                    "truth_partition": "truth_sleeves/PRIMARY/PAPER",
                },
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
            if target == str(session_refresh_module.PAPER_SESSION_LEDGER_TOOL):
                return {'cmd': cmd, 'returncode': 0, 'stdout': json.dumps({'authority_status': 'GRANTED'}), 'stderr': ''}
            return {'cmd': cmd, 'returncode': 0, 'stdout': '', 'stderr': ''}

        with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / 'tmp')) as td:
            root = Path(td)
            global_truth = root / 'constellation_2' / 'runtime' / 'truth'
            self._write_minimal_registries(root)
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
                session_refresh_module,
                '_run_primary_startup_authorization_refresh',
                return_value={
                    'sleeve_id': 'PRIMARY',
                    'state': 'READY',
                    'nonblocking_override': False,
                    'returncode': 0,
                    'blocking_scope': 'PRIMARY_SLEEVE',
                    'reason_codes': [],
                    'scope_truth_root': str(primary_truth),
                    'truth_partition': 'truth_sleeves/PRIMARY/PAPER',
                },
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
