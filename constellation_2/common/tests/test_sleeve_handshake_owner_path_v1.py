from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import ops.tools.run_session_readiness_refresh_v1 as session_refresh_module
import ops.tools.run_pre_open_materializer_v1 as pre_open_tool
import constellation_2.common.pre_open_materializer_v1 as pre_open_common


DAY = "2026-03-16"


class SleeveHandshakeOwnerPathTests(unittest.TestCase):
    def test_session_refresh_bootstraps_handshake_under_execution_root(self) -> None:
        calls: list[list[str]] = []
        execution_root = Path("/tmp/test_sleeve_execution_root").resolve()

        def fake_run(cmd, **kwargs):
            calls.append(list(cmd))
            return {"cmd": list(cmd), "returncode": 0, "stdout": "", "stderr": ""}

        fake_profile = SimpleNamespace(
            host="127.0.0.1",
            port=4002,
            client_id_observer=179,
        )
        fake_roots = SimpleNamespace(execution_root_path=execution_root)
        fake_authority_result = {
            "status": "OK",
            "diagnostics": {
                "validation_state": "PASS",
                "blocking_class": "NONE",
                "first_failure": None,
            },
        }

        with patch.object(
            session_refresh_module,
            "resolve_single_paper_ib_account_from_sleeve_registry",
            return_value="DUO847203",
        ), patch.object(
            session_refresh_module,
            "_load_accounts",
            return_value={"PAPER": ["DUO847203"], "LIVE": []},
        ), patch.object(
            session_refresh_module,
            "resolve_governed_paper_execution_profile",
            return_value=fake_profile,
        ), patch.object(
            session_refresh_module,
            "resolve_governed_paper_execution_roots",
            return_value=fake_roots,
        ), patch.object(
            session_refresh_module,
            "_run",
            side_effect=fake_run,
        ), patch.object(
            session_refresh_module,
            "_seed_sleeve_positions_snapshots",
            return_value={"status": "OK", "seeded_count": 0, "seeded_paths": []},
        ), patch.object(
            session_refresh_module,
            "_resolve_paper_sleeve_truth_bindings",
            return_value=[],
        ), patch.object(
            session_refresh_module,
            "run_day_authority_preflight_v1",
            return_value=fake_authority_result,
        ), patch.object(
            session_refresh_module,
            "_write_day_authority_decision_from_validation",
            return_value={"status": "OK", "path": "/tmp/fake_day_authority.json", "sha256": "0" * 64},
        ), patch.object(
            session_refresh_module,
            "_authority_lifecycle_result",
            return_value={"status": "OK", "incident_count": 0, "incidents": []},
        ), patch.object(
            session_refresh_module,
            "_write_session_report",
            return_value=(Path("/tmp/fake_session_readiness.json"), "CREATED"),
        ), patch.object(
            session_refresh_module,
            "_write_operator_summary_safe",
            return_value={"status": "OK"},
        ), patch(
            "sys.argv",
            ["run_session_readiness_refresh_v1.py", "--day_utc", DAY],
        ):
            rc = session_refresh_module.main()

        self.assertIn(rc, (0, 2))
        bootstrap_cmd = next(
            cmd for cmd in calls if len(cmd) > 1 and str(cmd[1]) == str(session_refresh_module.BROKER_EVENTS_BOOTSTRAP_TOOL)
        )
        manifest_cmd = next(
            cmd for cmd in calls if len(cmd) > 1 and str(cmd[1]) == str(session_refresh_module.BROKER_EVENTS_MANIFEST_TOOL)
        )
        handshake_cmd = next(
            cmd for cmd in calls if len(cmd) > 1 and str(cmd[1]) == str(session_refresh_module.HANDSHAKE_TOOL)
        )

        self.assertIn("--truth_root", bootstrap_cmd)
        self.assertEqual(bootstrap_cmd[bootstrap_cmd.index("--truth_root") + 1], str(execution_root))
        self.assertIn("--host", bootstrap_cmd)
        self.assertEqual(bootstrap_cmd[bootstrap_cmd.index("--host") + 1], "127.0.0.1")
        self.assertIn("--port", bootstrap_cmd)
        self.assertEqual(bootstrap_cmd[bootstrap_cmd.index("--port") + 1], "4002")
        self.assertIn("--client-id", bootstrap_cmd)
        self.assertEqual(bootstrap_cmd[bootstrap_cmd.index("--client-id") + 1], "179")
        self.assertIn("--sleeve-id", bootstrap_cmd)
        self.assertEqual(bootstrap_cmd[bootstrap_cmd.index("--sleeve-id") + 1], session_refresh_module.PRIMARY_SLEEVE_ID)
        self.assertIn("--bootstrap-handshake-only", bootstrap_cmd)

        self.assertIn("--truth_root", manifest_cmd)
        self.assertEqual(manifest_cmd[manifest_cmd.index("--truth_root") + 1], str(execution_root))
        self.assertIn("--truth_root", handshake_cmd)
        self.assertEqual(handshake_cmd[handshake_cmd.index("--truth_root") + 1], str(execution_root))

    def test_observer_service_unit_uses_governed_sleeve_truth_and_profile(self) -> None:
        service_path = REPO_ROOT / "ops" / "systemd" / "user" / "c2-execution-observer.service"
        text = service_path.read_text(encoding="utf-8")

        self.assertIn("WorkingDirectory=/home/node/constellation", text)
        self.assertIn("/home/node/constellation_2_runtime/.venv_c2/bin/python", text)
        self.assertIn("/home/node/constellation/ops/ib/c2_execution_observer_v1.py", text)
        self.assertIn("--truth_root /home/node/constellation_runtime_data/truth_sleeves/PRIMARY/PAPER", text)
        self.assertIn("--host 127.0.0.1", text)
        self.assertIn("--port 4002", text)
        self.assertIn("--client-id 179", text)
        self.assertIn("--sleeve-id PRIMARY", text)

    def test_observer_source_bootstraps_repo_root_before_constellation_imports(self) -> None:
        observer_path = REPO_ROOT / "ops" / "ib" / "c2_execution_observer_v1.py"
        text = observer_path.read_text(encoding="utf-8")

        self.assertIn("_REPO_ROOT_FROM_FILE = _THIS_FILE.parents[2]", text)
        self.assertIn("sys.path.insert(0, str(_REPO_ROOT_FROM_FILE))", text)
        self.assertIn("repo_root_missing_constellation_2", text)
        self.assertIn("repo_root_missing_governance", text)


    def test_pre_open_tool_runs_handshake_under_primary_sleeve_truth_root(self) -> None:
        calls: list[dict[str, object]] = []
        canonical_truth_root = Path("/tmp/test_pre_open_canonical_truth").resolve()
        primary_truth_root = Path("/tmp/test_pre_open_primary_truth").resolve()
        execution_truth_root = Path("/tmp/test_pre_open_execution_truth").resolve()
        fake_profile = SimpleNamespace(host="127.0.0.1", port=4002, client_id_observer=179)
        fake_execution_roots = SimpleNamespace(execution_root_path=execution_truth_root)
        fake_ref = SimpleNamespace(
            path=Path("/tmp/fake_pre_open_bundle.json").resolve(),
            sha256="0" * 64,
            payload={"materialization_state": "COMPLETE", "blocking_reason_codes": []},
        )

        def fake_run_tool(cmd, *, script, env=None):
            calls.append({"cmd": list(cmd), "script": script, "env": dict(env or {})})
            return {
                "script": script,
                "command": list(cmd),
                "return_code": 0,
                "stdout": "",
                "stderr": "",
                "required_for_completion": True,
            }

        with patch.object(
            pre_open_tool,
            "_resolve_truth_root",
            return_value=canonical_truth_root,
        ), patch.object(
            pre_open_tool,
            "resolve_session_authority_target_day_v1",
            return_value=DAY,
        ), patch.object(
            pre_open_tool,
            "_resolve_ib_account",
            return_value="DUO847203",
        ), patch.object(
            pre_open_tool,
            "_resolve_primary_sleeve_truth_root",
            return_value=primary_truth_root,
        ), patch.object(
            pre_open_tool,
            "resolve_governed_paper_execution_profile",
            return_value=fake_profile,
        ), patch.object(
            pre_open_tool,
            "resolve_governed_paper_execution_roots",
            return_value=fake_execution_roots,
        ), patch.object(
            pre_open_tool,
            "_run_tool",
            side_effect=fake_run_tool,
        ), patch.object(
            pre_open_tool,
            "derive_pre_open_bundle_payload_v1",
            return_value={"materialization_state": "COMPLETE", "blocking_reason_codes": []},
        ), patch.object(
            pre_open_tool,
            "write_pre_open_bundle_v1",
            return_value=fake_ref,
        ), patch(
            "sys.argv",
            ["run_pre_open_materializer_v1.py", "--day_utc", DAY, "--truth_root", str(canonical_truth_root)],
        ):
            rc = pre_open_tool.main()

        self.assertEqual(rc, 0)
        bootstrap_cmd = next(
            row["cmd"]
            for row in calls
            if row["script"] == "ops/ib/c2_execution_observer_v1.py"
        )
        manifest_cmd = next(
            row["cmd"]
            for row in calls
            if row["script"] == "ops/ib/run_broker_event_day_manifest_v1.py"
        )
        handshake_cmd = next(
            row["cmd"]
            for row in calls
            if row["script"] == "ops/tools/run_ib_api_handshake_spine_v1.py"
        )
        self.assertIn("--truth_root", bootstrap_cmd)
        self.assertEqual(
            bootstrap_cmd[bootstrap_cmd.index("--truth_root") + 1],
            str(execution_truth_root),
        )
        self.assertIn("--truth_root", manifest_cmd)
        self.assertEqual(
            manifest_cmd[manifest_cmd.index("--truth_root") + 1],
            str(execution_truth_root),
        )
        self.assertIn("--truth_root", handshake_cmd)
        self.assertEqual(handshake_cmd[handshake_cmd.index("--truth_root") + 1], str(primary_truth_root))

    def test_pre_open_tool_attempts_rollover_before_broker_checks_when_stale(self) -> None:
        calls: list[dict[str, object]] = []
        canonical_truth_root = Path("/tmp/test_rollover_before_preopen_truth").resolve()
        primary_truth_root = Path("/tmp/test_rollover_before_preopen_primary").resolve()
        execution_truth_root = Path("/tmp/test_rollover_before_preopen_execution").resolve()
        fake_profile = SimpleNamespace(host="127.0.0.1", port=4002, client_id_observer=179)
        fake_execution_roots = SimpleNamespace(execution_root_path=execution_truth_root)
        fake_ref = SimpleNamespace(
            path=Path("/tmp/fake_pre_open_bundle_rollover.json").resolve(),
            sha256="0" * 64,
            payload={"materialization_state": "COMPLETE", "blocking_reason_codes": []},
        )

        def fake_run_tool(cmd, *, script, env=None):
            calls.append({"cmd": list(cmd), "script": script, "env": dict(env or {})})
            return {
                "script": script,
                "command": list(cmd),
                "return_code": 0,
                "stdout": "",
                "stderr": "",
                "required_for_completion": True,
            }

        with patch.object(
            pre_open_tool,
            "_resolve_truth_root",
            return_value=canonical_truth_root,
        ), patch.object(
            pre_open_tool,
            "resolve_session_authority_target_day_v1",
            return_value=DAY,
        ), patch.object(
            pre_open_tool,
            "_resolve_ib_account",
            return_value="DUO847203",
        ), patch.object(
            pre_open_tool,
            "_resolve_primary_sleeve_truth_root",
            return_value=primary_truth_root,
        ), patch.object(
            pre_open_tool,
            "_stale_authority_head_detected",
            return_value=True,
        ), patch.object(
            pre_open_tool,
            "resolve_governed_paper_execution_profile",
            return_value=fake_profile,
        ), patch.object(
            pre_open_tool,
            "resolve_governed_paper_execution_roots",
            return_value=fake_execution_roots,
        ), patch.object(
            pre_open_tool,
            "_run_tool",
            side_effect=fake_run_tool,
        ), patch.object(
            pre_open_tool,
            "derive_pre_open_bundle_payload_v1",
            return_value={
                "materialization_state": "COMPLETE",
                "blocking_reason_codes": [],
                "prerequisite_checks": [
                    {
                        "artifact_id": "primary_scoped_canonical_authority_head_v1",
                        "result_status": "PASS",
                        "target_day_observed": DAY,
                    }
                ],
            },
        ), patch.object(
            pre_open_tool,
            "write_pre_open_bundle_v1",
            return_value=fake_ref,
        ), patch(
            "sys.argv",
            ["run_pre_open_materializer_v1.py", "--day_utc", DAY, "--truth_root", str(canonical_truth_root)],
        ):
            rc = pre_open_tool.main()

        self.assertEqual(rc, 0)
        scripts = [str(row["script"]) for row in calls]
        self.assertGreaterEqual(len(scripts), 3)
        self.assertEqual(
            scripts[:3],
            [
                "ops/tools/run_session_authority_v1.py",
                "ops/tools/run_session_authority_v1.py",
                "ops/tools/run_session_authority_v1.py",
            ],
        )
        phases = []
        for row in calls[:3]:
            cmd = list(row["cmd"])
            self.assertIn("--phase", cmd)
            phases.append(cmd[cmd.index("--phase") + 1])
        self.assertEqual(phases, ["build", "admit", "activate"])
        self.assertGreater(
            scripts.index("ops/tools/run_ib_api_handshake_spine_v1.py"),
            scripts.index("ops/tools/run_session_authority_v1.py"),
        )

    def test_pre_open_tool_fails_closed_when_rollover_stays_stale(self) -> None:
        canonical_truth_root = Path("/tmp/test_rollover_fail_closed_truth").resolve()
        primary_truth_root = Path("/tmp/test_rollover_fail_closed_primary").resolve()
        execution_truth_root = Path("/tmp/test_rollover_fail_closed_execution").resolve()
        fake_profile = SimpleNamespace(host="127.0.0.1", port=4002, client_id_observer=179)
        fake_execution_roots = SimpleNamespace(execution_root_path=execution_truth_root)
        written_payload: dict[str, object] = {}

        def fake_run_tool(cmd, *, script, env=None):
            return {
                "script": script,
                "command": list(cmd),
                "return_code": 0,
                "stdout": "",
                "stderr": "",
                "required_for_completion": True,
            }

        def fake_write_pre_open_bundle_v1(*, truth_root, payload):
            written_payload["payload"] = payload
            return SimpleNamespace(
                path=Path("/tmp/fake_pre_open_bundle_rollover_fail.json").resolve(),
                sha256="0" * 64,
                payload=payload,
            )

        with patch.object(
            pre_open_tool,
            "_resolve_truth_root",
            return_value=canonical_truth_root,
        ), patch.object(
            pre_open_tool,
            "resolve_session_authority_target_day_v1",
            return_value=DAY,
        ), patch.object(
            pre_open_tool,
            "_resolve_ib_account",
            return_value="DUO847203",
        ), patch.object(
            pre_open_tool,
            "_resolve_primary_sleeve_truth_root",
            return_value=primary_truth_root,
        ), patch.object(
            pre_open_tool,
            "_stale_authority_head_detected",
            return_value=True,
        ), patch.object(
            pre_open_tool,
            "resolve_governed_paper_execution_profile",
            return_value=fake_profile,
        ), patch.object(
            pre_open_tool,
            "resolve_governed_paper_execution_roots",
            return_value=fake_execution_roots,
        ), patch.object(
            pre_open_tool,
            "_run_tool",
            side_effect=fake_run_tool,
        ), patch.object(
            pre_open_tool,
            "derive_pre_open_bundle_payload_v1",
            return_value={
                "materialization_state": "INCOMPLETE",
                "completion_state": "INCOMPLETE",
                "blocking_reason_codes": ["TARGET_DAY_DATE_MISMATCH"],
                "prerequisite_checks": [
                    {
                        "artifact_id": "primary_scoped_canonical_authority_head_v1",
                        "result_status": "FAIL",
                        "target_day_observed": "2026-03-15",
                        "blocking_reason_code": "TARGET_DAY_DATE_MISMATCH",
                        "blocker_codes": ["TARGET_DAY_DATE_MISMATCH"],
                        "closure_status": "OPEN",
                    }
                ],
            },
        ), patch.object(
            pre_open_tool,
            "write_pre_open_bundle_v1",
            side_effect=fake_write_pre_open_bundle_v1,
        ), patch(
            "sys.argv",
            ["run_pre_open_materializer_v1.py", "--day_utc", DAY, "--truth_root", str(canonical_truth_root)],
        ):
            rc = pre_open_tool.main()

        self.assertEqual(rc, 2)
        payload = dict(written_payload["payload"])
        self.assertEqual(payload["materialization_state"], "BLOCKED")
        self.assertIn("ROLLOVER_FAILED_STALE_AUTHORITY_HEAD", payload["blocking_reason_codes"])
        head_row = payload["prerequisite_checks"][0]
        self.assertEqual(head_row["blocking_reason_code"], "ROLLOVER_FAILED_STALE_AUTHORITY_HEAD")

    def test_pre_open_tool_stops_before_broker_checks_when_rollover_phase_fails(self) -> None:
        calls: list[dict[str, object]] = []
        canonical_truth_root = Path("/tmp/test_rollover_phase_fail_truth").resolve()
        primary_truth_root = Path("/tmp/test_rollover_phase_fail_primary").resolve()
        written_payload: dict[str, object] = {}

        def fake_run_tool(cmd, *, script, env=None):
            cmd_list = list(cmd)
            calls.append({"cmd": cmd_list, "script": script})
            if script == "ops/tools/run_session_authority_v1.py":
                return {
                    "script": script,
                    "command": cmd_list,
                    "return_code": 2,
                    "stdout": "",
                    "stderr": "blocked",
                    "required_for_completion": True,
                }
            raise AssertionError(f"unexpected non-rollover invocation: {script}")

        def fake_write_pre_open_bundle_v1(*, truth_root, payload):
            written_payload["payload"] = payload
            return SimpleNamespace(
                path=Path("/tmp/fake_pre_open_bundle_rollover_phase_fail.json").resolve(),
                sha256="0" * 64,
                payload=payload,
            )

        with patch.object(
            pre_open_tool,
            "_resolve_truth_root",
            return_value=canonical_truth_root,
        ), patch.object(
            pre_open_tool,
            "resolve_session_authority_target_day_v1",
            return_value=DAY,
        ), patch.object(
            pre_open_tool,
            "_resolve_ib_account",
            return_value="DUO847203",
        ), patch.object(
            pre_open_tool,
            "_resolve_primary_sleeve_truth_root",
            return_value=primary_truth_root,
        ), patch.object(
            pre_open_tool,
            "_stale_authority_head_detected",
            return_value=True,
        ), patch.object(
            pre_open_tool,
            "_run_tool",
            side_effect=fake_run_tool,
        ), patch.object(
            pre_open_tool,
            "derive_pre_open_bundle_payload_v1",
            return_value={
                "materialization_state": "INCOMPLETE",
                "completion_state": "INCOMPLETE",
                "blocking_reason_codes": [],
                "prerequisite_checks": [
                    {
                        "artifact_id": "primary_scoped_canonical_authority_head_v1",
                        "result_status": "FAIL",
                        "target_day_observed": "",
                        "blocking_reason_code": "TARGET_DAY_ARTIFACT_MISSING",
                        "blocker_codes": ["TARGET_DAY_ARTIFACT_MISSING"],
                        "closure_status": "OPEN",
                    }
                ],
            },
        ), patch.object(
            pre_open_tool,
            "write_pre_open_bundle_v1",
            side_effect=fake_write_pre_open_bundle_v1,
        ), patch(
            "sys.argv",
            ["run_pre_open_materializer_v1.py", "--day_utc", DAY, "--truth_root", str(canonical_truth_root)],
        ):
            rc = pre_open_tool.main()

        self.assertEqual(rc, 2)
        scripts = [str(row["script"]) for row in calls]
        self.assertEqual(
            scripts,
            [
                "ops/tools/run_session_authority_v1.py",
                "ops/tools/run_session_authority_v1.py",
                "ops/tools/run_session_authority_v1.py",
            ],
        )
        phases = []
        for row in calls:
            cmd = list(row["cmd"])
            self.assertIn("--phase", cmd)
            phases.append(cmd[cmd.index("--phase") + 1])
        self.assertEqual(phases, ["build", "admit", "activate"])
        payload = dict(written_payload["payload"])
        self.assertIn("ROLLOVER_FAILED_STALE_AUTHORITY_HEAD", payload["blocking_reason_codes"])

    def test_pre_open_tool_skips_rollover_attempt_when_primary_head_already_aligned(self) -> None:
        calls: list[dict[str, object]] = []
        canonical_truth_root = Path("/tmp/test_rollover_skip_truth").resolve()
        primary_truth_root = Path("/tmp/test_rollover_skip_primary").resolve()
        execution_truth_root = Path("/tmp/test_rollover_skip_execution").resolve()
        written_payload: dict[str, object] = {}
        fake_profile = SimpleNamespace(host="127.0.0.1", port=4002, client_id_observer=179)
        fake_execution_roots = SimpleNamespace(execution_root_path=execution_truth_root)

        def fake_run_tool(cmd, *, script, env=None):
            cmd_list = list(cmd)
            calls.append({"cmd": cmd_list, "script": script})
            return {
                "script": script,
                "command": cmd_list,
                "return_code": 0,
                "stdout": "",
                "stderr": "",
                "required_for_completion": True,
            }

        def fake_write_pre_open_bundle_v1(*, truth_root, payload):
            written_payload["payload"] = payload
            return SimpleNamespace(
                path=Path("/tmp/fake_pre_open_bundle_rollover_skip.json").resolve(),
                sha256="0" * 64,
                payload=payload,
            )

        with patch.object(
            pre_open_tool,
            "_resolve_truth_root",
            return_value=canonical_truth_root,
        ), patch.object(
            pre_open_tool,
            "resolve_session_authority_target_day_v1",
            return_value=DAY,
        ), patch.object(
            pre_open_tool,
            "_resolve_ib_account",
            return_value="DUO847203",
        ), patch.object(
            pre_open_tool,
            "_resolve_primary_sleeve_truth_root",
            return_value=primary_truth_root,
        ), patch.object(
            pre_open_tool,
            "_stale_authority_head_detected",
            return_value=True,
        ), patch.object(
            pre_open_tool,
            "_primary_scoped_head_matches_target_day_from_sleeve_truth_root",
            return_value=True,
        ), patch.object(
            pre_open_tool,
            "resolve_governed_paper_execution_profile",
            return_value=fake_profile,
        ), patch.object(
            pre_open_tool,
            "resolve_governed_paper_execution_roots",
            return_value=fake_execution_roots,
        ), patch.object(
            pre_open_tool,
            "_run_tool",
            side_effect=fake_run_tool,
        ), patch.object(
            pre_open_tool,
            "derive_pre_open_bundle_payload_v1",
            return_value={
                "materialization_state": "COMPLETE",
                "completion_state": "COMPLETE",
                "blocking_reason_codes": [],
                "prerequisite_checks": [
                    {
                        "artifact_id": "primary_scoped_canonical_authority_head_v1",
                        "result_status": "PASS",
                        "target_day_observed": DAY,
                        "blocking_reason_code": "",
                        "blocker_codes": [],
                        "closure_status": "CLOSED",
                    }
                ],
            },
        ), patch.object(
            pre_open_tool,
            "write_pre_open_bundle_v1",
            side_effect=fake_write_pre_open_bundle_v1,
        ), patch(
            "sys.argv",
            ["run_pre_open_materializer_v1.py", "--day_utc", DAY, "--truth_root", str(canonical_truth_root)],
        ):
            rc = pre_open_tool.main()

        self.assertEqual(rc, 0)
        scripts = [str(row["script"]) for row in calls]
        self.assertNotIn("ops/tools/run_session_authority_v1.py", scripts)
        payload = dict(written_payload["payload"])
        self.assertNotIn("ROLLOVER_FAILED_STALE_AUTHORITY_HEAD", payload["blocking_reason_codes"])

    def test_pre_open_tool_does_not_force_stale_block_when_head_aligned_after_rollover_attempt(self) -> None:
        calls: list[dict[str, object]] = []
        canonical_truth_root = Path("/tmp/test_rollover_align_after_attempt_truth").resolve()
        primary_truth_root = Path("/tmp/test_rollover_align_after_attempt_primary").resolve()
        written_payload: dict[str, object] = {}

        def fake_run_tool(cmd, *, script, env=None):
            cmd_list = list(cmd)
            calls.append({"cmd": cmd_list, "script": script})
            if script == "ops/tools/run_session_authority_v1.py":
                return {
                    "script": script,
                    "command": cmd_list,
                    "return_code": 2,
                    "stdout": "",
                    "stderr": "blocked",
                    "required_for_completion": True,
                }
            raise AssertionError(f"unexpected non-rollover invocation: {script}")

        def fake_write_pre_open_bundle_v1(*, truth_root, payload):
            written_payload["payload"] = payload
            return SimpleNamespace(
                path=Path("/tmp/fake_pre_open_bundle_rollover_align_after_attempt.json").resolve(),
                sha256="0" * 64,
                payload=payload,
            )

        with patch.object(
            pre_open_tool,
            "_resolve_truth_root",
            return_value=canonical_truth_root,
        ), patch.object(
            pre_open_tool,
            "resolve_session_authority_target_day_v1",
            return_value=DAY,
        ), patch.object(
            pre_open_tool,
            "_resolve_ib_account",
            return_value="DUO847203",
        ), patch.object(
            pre_open_tool,
            "_resolve_primary_sleeve_truth_root",
            return_value=primary_truth_root,
        ), patch.object(
            pre_open_tool,
            "_stale_authority_head_detected",
            return_value=True,
        ), patch.object(
            pre_open_tool,
            "_primary_scoped_head_matches_target_day_from_sleeve_truth_root",
            return_value=False,
        ), patch.object(
            pre_open_tool,
            "_run_tool",
            side_effect=fake_run_tool,
        ), patch.object(
            pre_open_tool,
            "derive_pre_open_bundle_payload_v1",
            return_value={
                "materialization_state": "COMPLETE",
                "completion_state": "COMPLETE",
                "blocking_reason_codes": [],
                "prerequisite_checks": [
                    {
                        "artifact_id": "primary_scoped_canonical_authority_head_v1",
                        "result_status": "PASS",
                        "target_day_observed": DAY,
                        "blocking_reason_code": "",
                        "blocker_codes": [],
                        "closure_status": "CLOSED",
                    }
                ],
            },
        ), patch.object(
            pre_open_tool,
            "write_pre_open_bundle_v1",
            side_effect=fake_write_pre_open_bundle_v1,
        ), patch(
            "sys.argv",
            ["run_pre_open_materializer_v1.py", "--day_utc", DAY, "--truth_root", str(canonical_truth_root)],
        ):
            rc = pre_open_tool.main()

        self.assertEqual(rc, 0)
        scripts = [str(row["script"]) for row in calls]
        self.assertEqual(
            scripts,
            [
                "ops/tools/run_session_authority_v1.py",
                "ops/tools/run_session_authority_v1.py",
                "ops/tools/run_session_authority_v1.py",
            ],
        )
        payload = dict(written_payload["payload"])
        self.assertNotIn("ROLLOVER_FAILED_STALE_AUTHORITY_HEAD", payload["blocking_reason_codes"])

    def test_pre_open_common_reads_handshake_from_primary_sleeve_truth_root(self) -> None:
        canonical_truth_root = Path("/tmp/test_common_canonical_truth").resolve()
        primary_truth_root = Path("/tmp/test_common_primary_truth").resolve()
        fake_binding = SimpleNamespace(sleeve_id="PRIMARY", truth_root=primary_truth_root)
        fake_state = SimpleNamespace(
            pointer_path=(primary_truth_root / "ib_api_handshake" / "latest_pointer.v1.json").resolve(),
            pointer_day_utc=DAY,
            pointer_sha256="1" * 64,
            handshake_path=(primary_truth_root / "ib_api_handshake" / DAY / "ib_api_handshake.v1.json").resolve(),
            handshake_day_utc=DAY,
            handshake_sha256="2" * 64,
        )
        fake_state.pointer_path.parent.mkdir(parents=True, exist_ok=True)
        fake_state.pointer_path.write_text("{}\n", encoding="utf-8")
        fake_state.handshake_path.parent.mkdir(parents=True, exist_ok=True)
        fake_state.handshake_path.write_text("{}\n", encoding="utf-8")
        seen: dict[str, Path] = {}

        def fake_resolve_pointer_bound_handshake_state(**kwargs):
            seen["truth_root"] = Path(kwargs["truth_root"]).resolve()
            return fake_state

        def fake_read_json_object(path: Path):
            resolved = Path(path).resolve()
            if resolved == fake_state.pointer_path:
                return {
                    "schema_id": "C2_IB_API_HANDSHAKE_LATEST_POINTER_V1",
                    "schema_version": 1,
                    "day_utc": DAY,
                    "produced_utc": f"{DAY}T00:00:00Z",
                    "status": "OK",
                    "pointers": {
                        "handshake_path": str(fake_state.handshake_path),
                        "handshake_sha256": fake_state.handshake_sha256,
                    },
                }
            if resolved == fake_state.handshake_path:
                return {
                    "schema_id": "C2_IB_API_HANDSHAKE_V1",
                    "schema_version": 1,
                    "day_utc": DAY,
                    "produced_utc": f"{DAY}T00:00:00Z",
                    "status": "OK",
                    "reason_codes": [],
                }
            raise AssertionError(f"unexpected path: {resolved}")

        with patch.object(
            pre_open_common,
            "resolve_governed_sleeve_truth_bindings",
            return_value=[fake_binding],
        ), patch.object(
            pre_open_common,
            "resolve_pointer_bound_handshake_state",
            side_effect=fake_resolve_pointer_bound_handshake_state,
        ), patch.object(
            pre_open_common,
            "read_json_object_v1",
            side_effect=fake_read_json_object,
        ):
            rows = pre_open_common._collect_handshake_rows(
                truth_root=canonical_truth_root,
                environment="PAPER",
                ib_account="DUO847203",
                target_day=DAY,
            )

        self.assertEqual(seen["truth_root"], primary_truth_root)
        self.assertEqual(rows[0]["authority_path"], str(fake_state.pointer_path))
        self.assertEqual(rows[1]["authority_path"], str(fake_state.handshake_path))


    def test_capability_state_uses_primary_sleeve_handshake_truth_root(self) -> None:
        from constellation_2.common import capability_state_v1 as capability_state_common

        canonical_truth_root = Path("/tmp/test_capability_state_truth").resolve()
        primary_truth_root = Path("/tmp/test_capability_state_sleeve").resolve()
        primary_truth_root.mkdir(parents=True, exist_ok=True)

        auth_path = (primary_truth_root / "reports" / "authorization_gate_verdict_v1" / DAY / "authorization_gate_verdict.v1.json").resolve()
        auth_path.parent.mkdir(parents=True, exist_ok=True)
        auth_path.write_text(json.dumps({"status": "PASS", "reason_codes": []}) + "\n", encoding="utf-8")
        econ_path = (primary_truth_root / "reports" / "economic_health_gate_verdict_v1" / DAY / "economic_health_gate_verdict.v1.json").resolve()
        econ_path.parent.mkdir(parents=True, exist_ok=True)
        econ_path.write_text(json.dumps({"status": "PASS", "reason_codes": []}) + "\n", encoding="utf-8")

        fake_binding = SimpleNamespace(sleeve_id="PRIMARY", truth_root=primary_truth_root)
        fake_state = SimpleNamespace(
            pointer_path=(primary_truth_root / "ib_api_handshake" / "latest_pointer.v1.json").resolve(),
            pointer_day_utc=DAY,
            pointer_sha256="1" * 64,
            handshake_path=(primary_truth_root / "ib_api_handshake" / DAY / "ib_api_handshake.v1.json").resolve(),
            handshake_day_utc=DAY,
            handshake_sha256="2" * 64,
        )
        fake_state.pointer_path.parent.mkdir(parents=True, exist_ok=True)
        fake_state.pointer_path.write_text("{}\n", encoding="utf-8")
        fake_state.handshake_path.parent.mkdir(parents=True, exist_ok=True)
        fake_state.handshake_path.write_text("{}\n", encoding="utf-8")
        seen: dict[str, Path] = {}

        def fake_resolve_pointer_bound_handshake_state(**kwargs):
            seen["truth_root"] = Path(kwargs["truth_root"]).resolve()
            return fake_state

        def fake_read_json(path: Path):
            resolved = Path(path).resolve()
            if resolved == fake_state.handshake_path:
                return {"status": "OK", "ok": True, "reason_codes": []}
            if resolved == auth_path:
                return {"status": "PASS", "reason_codes": []}
            if resolved == econ_path:
                return {"status": "PASS", "reason_codes": []}
            raise AssertionError(f"unexpected path: {resolved}")

        with patch.object(
            capability_state_common,
            "_release_metadata",
            return_value={"release_id": "r1", "git_sha": "a" * 40},
        ), patch.object(
            capability_state_common,
            "_load_gate_hierarchy",
            return_value={},
        ), patch.object(
            capability_state_common,
            "_load_capability_policy_registry",
            return_value=({"schema_id": "capability_policy_registry", "schema_version": "v1"}, REPO_ROOT / "governance/02_REGISTRIES/CAPABILITY_POLICY_REGISTRY_V1.json", "3" * 64),
        ), patch.object(
            capability_state_common,
            "resolve_decision_truth_root_v1",
            return_value=canonical_truth_root,
        ), patch.object(
            capability_state_common,
            "resolve_governed_account_binding",
            return_value=SimpleNamespace(account_registry_path=REPO_ROOT / "governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json", sleeve_registry_path=REPO_ROOT / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json"),
        ), patch.object(
            capability_state_common,
            "_resolve_primary_binding",
            return_value=fake_binding,
        ), patch.object(
            capability_state_common,
            "read_startup_materialization_ref_v1",
            return_value=SimpleNamespace(payload={"status": "SUCCESS", "blocking_codes": []}, path=Path("/tmp/startup_materialization.json")),
        ), patch.object(
            capability_state_common,
            "read_paper_trading_posture_ref_v1",
            return_value=SimpleNamespace(payload={"posture_status": "ENABLED", "system_ready": True, "freshness_verdict": "CURRENT", "linkage_verdict": "LINKED", "blocking_codes": []}, path=Path("/tmp/paper_trading_posture.json")),
        ), patch.object(
            capability_state_common,
            "resolve_pointer_bound_handshake_state",
            side_effect=fake_resolve_pointer_bound_handshake_state,
        ), patch.object(
            capability_state_common,
            "_read_json",
            side_effect=fake_read_json,
        ), patch.object(
            capability_state_common,
            "resolve_canonical_governed_sleeve_truth_root",
            return_value=primary_truth_root,
        ), patch.object(
            capability_state_common,
            "CORE_GATE_IDS",
            (),
        ), patch.object(
            capability_state_common,
            "PRODUCTION_CERT_GATE_IDS",
            (),
        ):
            payload = capability_state_common.derive_capability_state_payload(
                repo_root=REPO_ROOT,
                truth_root=canonical_truth_root,
                day_utc=DAY,
                ib_account="DUO847203",
                environment="PAPER",
            )

        self.assertEqual(seen["truth_root"], primary_truth_root)
        by_id = {row["capability_id"]: row for row in payload["capabilities"]}
        self.assertEqual(by_id["broker_connectivity_available"]["status"], "PASS")
        self.assertEqual(
            by_id["broker_connectivity_available"]["source_artifacts"][1]["artifact_path"],
            str(fake_state.handshake_path),
        )


if __name__ == "__main__":
    unittest.main()
