from __future__ import annotations

import io
import json
import sys
import tempfile
import time
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
(REPO_ROOT / "tmp").mkdir(parents=True, exist_ok=True)

if "constellation_2.common.accounting_authority_v1" not in sys.modules:
    accounting_stub = types.ModuleType("constellation_2.common.accounting_authority_v1")
    accounting_stub.read_accounting_authority_state = lambda **_: {
        "basis_class": "UNKNOWN",
        "authoritative": False,
        "reason_codes": ["ACCOUNTING_AUTHORITY_HELPER_UNAVAILABLE_IN_TEST"],
        "cash_authority_basis": "UNKNOWN",
    }
    sys.modules["constellation_2.common.accounting_authority_v1"] = accounting_stub

if "constellation_2.common.execution_day_authority_v1" not in sys.modules:
    execution_stub = types.ModuleType("constellation_2.common.execution_day_authority_v1")
    execution_stub.read_execution_day_authority_state = lambda **_: {
        "state": "UNKNOWN",
        "reason_codes": ["EXECUTION_DAY_AUTHORITY_HELPER_UNAVAILABLE_IN_TEST"],
    }
    sys.modules["constellation_2.common.execution_day_authority_v1"] = execution_stub

from constellation_2.common import bounded_obligation_pipeline_v1 as bounded_pipeline
from constellation_2.common import cockpit_status_obligation_pipeline_v1 as cockpit_pipeline
from constellation_2.common import paper_day_orchestrator_pipeline_v1 as orchestrator_pipeline
from constellation_2.phaseL.ui.server import c2_ops_cockpit_status_v2_collector_v1 as cockpit_collector
import ops.tools.run_c2_paper_day_orchestrator_v2 as orchestrator_cli


DAY = "2026-04-19"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


class Bundle9ObligationPipelineTests(unittest.TestCase):
    def test_execute_bounded_obligation_pipeline_records_deterministic_phase_proof(self) -> None:
        report = bounded_pipeline.execute_bounded_obligation_pipeline_v1(
            pipeline_id="c2_ops_cockpit_status_v2_collector_v1",
            pipeline_mode="normal",
            target_path_family="constellation_2/phaseL/ui/server/c2_ops_cockpit_status_v2_collector_v1.py",
            budget_profile="contract_default",
            resolve_inputs=lambda: {"governing_refs": [{"artifact_id": "seed", "artifact_path": "/tmp/seed", "artifact_sha256": ""}]},
            evaluate=lambda resolved: {"payload": {"resolved": dict(resolved)}, "governing_refs": list(resolved["governing_refs"])},
            persist_outputs=lambda resolved, evaluated: {"payload": dict(evaluated["payload"]), "governing_refs": list(evaluated["governing_refs"])},
            project_emit=lambda resolved, evaluated, persisted: {
                "payload": dict(persisted["payload"]),
                "governing_refs": list(persisted["governing_refs"]),
            },
        )

        self.assertTrue(report["ok"])
        proof = report["proof"]
        self.assertEqual(
            [row["phase_name"] for row in proof["phase_results"]],
            list(bounded_pipeline.PIPELINE_PHASES),
        )
        self.assertEqual(proof["status"], "OK")
        self.assertEqual(proof["authority_label"], bounded_pipeline.PIPELINE_AUTHORITY_LABEL)
        self.assertEqual(set(proof["phase_timings"].keys()), set(bounded_pipeline.PIPELINE_PHASES))
        self.assertEqual(report["projected"]["payload"]["resolved"]["governing_refs"][0]["artifact_id"], "seed")

    def test_execute_bounded_obligation_pipeline_blocks_on_hard_budget(self) -> None:
        report = bounded_pipeline.execute_bounded_obligation_pipeline_v1(
            pipeline_id="c2_ops_cockpit_status_v2_collector_v1",
            pipeline_mode="normal",
            target_path_family="constellation_2/phaseL/ui/server/c2_ops_cockpit_status_v2_collector_v1.py",
            budget_profile="strict_validation",
            resolve_inputs=lambda: {},
            evaluate=lambda resolved: (time.sleep(0.01), {"payload": dict(resolved)})[1],
            persist_outputs=lambda resolved, evaluated: dict(evaluated),
            project_emit=lambda resolved, evaluated, persisted: dict(persisted),
        )

        self.assertFalse(report["ok"])
        self.assertTrue(str(report["proof"]["blocked_reason"]).startswith("PIPELINE_PERFORMANCE_HARD_FAIL"))
        self.assertEqual(report["proof"]["phase_results"][-1]["phase_name"], "measure_phase_boundaries")
        self.assertEqual(report["proof"]["phase_results"][-1]["status"], "BLOCKED")

    def test_cockpit_status_pipeline_wraps_core_builder_without_extra_semantics(self) -> None:
        payload = {
            "ok": True,
            "meta": {"selected_day": DAY},
            "provenance": {"source_paths": ["/tmp/source_a.json", "/tmp/source_b.json"]},
        }
        with patch.object(cockpit_pipeline.collector_tool, "_build_status_v2_core", return_value=payload) as core:
            report = cockpit_pipeline.run_cockpit_status_obligation_pipeline_v1(
                truth_root=REPO_ROOT,
                instance_config_path=REPO_ROOT / "governance/00_INDEX.md",
                day=DAY,
                attempt_id="attempt-1",
            )

        core.assert_called_once()
        self.assertTrue(report["ok"])
        self.assertEqual(report["payload"]["meta"]["selected_day"], DAY)
        self.assertEqual(report["payload"]["pipeline_proof"]["pipeline_id"], cockpit_pipeline.PIPELINE_ID)
        self.assertEqual(report["payload"]["pipeline_proof"]["status"], "OK")

    def test_cockpit_status_pipeline_rejects_unsupported_modes(self) -> None:
        report = cockpit_pipeline.run_cockpit_status_obligation_pipeline_v1(
            truth_root=REPO_ROOT,
            instance_config_path=REPO_ROOT / "governance/00_INDEX.md",
            day=DAY,
            attempt_id=None,
            pipeline_mode="exact_ref_replay",
        )
        self.assertFalse(report["ok"])
        self.assertEqual(
            report["proof"]["blocked_reason"],
            "PIPELINE_MODE_UNSUPPORTED:c2_ops_cockpit_status_v2_collector_v1:exact_ref_replay",
        )

    def test_cockpit_collector_public_wrapper_renders_pipeline_payload_only(self) -> None:
        with patch(
            "constellation_2.common.cockpit_status_obligation_pipeline_v1.run_cockpit_status_obligation_pipeline_v1",
            return_value={
                "ok": True,
                "payload": {"ok": True, "authority_label": "governed_surface"},
                "proof": {"pipeline_id": cockpit_pipeline.PIPELINE_ID},
            },
        ) as runner:
            rendered = cockpit_collector.build_status_v2(REPO_ROOT, REPO_ROOT / "governance/00_INDEX.md", DAY, None, None)

        runner.assert_called_once()
        self.assertEqual(rendered, {"ok": True, "authority_label": "governed_surface"})

    def test_orchestrator_exact_ref_replay_is_bounded_and_nonpublishing(self) -> None:
        with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
            root = Path(td)
            manifest_path = root / "reports" / "orchestrator_run_verdict_v2" / DAY / "attempt-1" / "orchestrator_attempt_manifest.v2.json"
            verdict_path = manifest_path.parent / "orchestrator_run_verdict.v2.json"
            _write_json(
                manifest_path,
                {
                    "schema_id": "C2_ORCHESTRATOR_ATTEMPT_MANIFEST_V2",
                    "day_utc": DAY,
                    "attempt_id": "attempt-1",
                    "attempt_seq": 1,
                    "mode": "PAPER",
                },
            )
            _write_json(verdict_path, {"status": "PASS", "reason_codes": ["PASS"]})
            args = SimpleNamespace(
                pipeline_mode="exact_ref_replay",
                replay_attempt_manifest_path=str(manifest_path),
                truth_root=str(root),
                day_utc=DAY,
                mode="PAPER",
                input_day_utc="",
                symbol="SPY",
                ib_account="DUO847203",
                produced_utc=f"{DAY}T00:00:00Z",
                paper_session_ledger_path=str(root / "paper_session_ledger.v1.json"),
            )
            with patch.object(orchestrator_pipeline._module(), "_require_repo_root_cwd", lambda: None):
                resolved = orchestrator_pipeline.resolve_paper_day_orchestrator_pipeline_inputs_v1(args)
                evaluated = orchestrator_pipeline.evaluate_paper_day_orchestrator_pipeline_v1(resolved)
                with patch.object(orchestrator_pipeline._module(), "_publish_pipeline_manifest_v3", side_effect=AssertionError("unexpected publish")), patch.object(
                    orchestrator_pipeline._module(), "_publish_pipeline_manifest_v2_compat", side_effect=AssertionError("unexpected publish")
                ), patch.object(
                    orchestrator_pipeline._module(), "_publish_pipeline_manifest_v1_compat", side_effect=AssertionError("unexpected publish")
                ):
                    persisted = orchestrator_pipeline.persist_paper_day_orchestrator_pipeline_outputs_v1(resolved, evaluated)

        self.assertEqual(resolved["pipeline_mode"], "exact_ref_replay")
        self.assertEqual(evaluated["status"], "PASS")
        self.assertEqual(persisted["attempt_manifest_path"], str(manifest_path.resolve()))
        self.assertEqual(persisted["verdict_path"], str(verdict_path.resolve()))

    def test_orchestrator_bounded_recompute_is_explicit_and_auditable(self) -> None:
        with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
            root = Path(td)
            manifest_path = root / "reports" / "orchestrator_run_verdict_v2" / DAY / "attempt-2" / "orchestrator_attempt_manifest.v2.json"
            _write_json(
                manifest_path,
                {
                    "schema_id": "C2_ORCHESTRATOR_ATTEMPT_MANIFEST_V2",
                    "day_utc": DAY,
                    "attempt_id": "attempt-2",
                    "attempt_seq": 2,
                    "mode": "PAPER",
                },
            )
            args = SimpleNamespace(
                pipeline_mode="bounded_recompute",
                replay_attempt_manifest_path=str(manifest_path),
                truth_root=str(root),
                day_utc=DAY,
                mode="PAPER",
                input_day_utc="",
                symbol="SPY",
                ib_account="DUO847203",
                produced_utc=f"{DAY}T00:00:00Z",
                paper_session_ledger_path=str(root / "paper_session_ledger.v1.json"),
            )
            orch = orchestrator_pipeline._module()
            with patch.object(orch, "_require_repo_root_cwd", lambda: None), patch.object(
                orch, "_publish_pipeline_manifest_v3", return_value=(True, 0)
            ) as pub3, patch.object(
                orch, "_publish_pipeline_manifest_v2_compat", return_value=(True, 0)
            ) as pub2, patch.object(
                orch, "_publish_pipeline_manifest_v1_compat", return_value=(True, 0)
            ) as pub1:
                resolved = orchestrator_pipeline.resolve_paper_day_orchestrator_pipeline_inputs_v1(args)
                evaluated = orchestrator_pipeline.evaluate_paper_day_orchestrator_pipeline_v1(resolved)
                persisted = orchestrator_pipeline.persist_paper_day_orchestrator_pipeline_outputs_v1(resolved, evaluated)

        pub3.assert_called_once()
        pub2.assert_called_once()
        pub1.assert_called_once()
        self.assertEqual(persisted["attempt_manifest_path"], str(manifest_path.resolve()))
        self.assertEqual(persisted["status"], "REPLAY_ONLY")

    def test_orchestrator_forensic_replay_is_rejected(self) -> None:
        report = orchestrator_pipeline.run_paper_day_orchestrator_obligation_pipeline_v1(
            SimpleNamespace(
                pipeline_mode="forensic_replay",
                replay_attempt_manifest_path="",
                truth_root=str(REPO_ROOT),
                day_utc=DAY,
                mode="PAPER",
                input_day_utc="",
                symbol="SPY",
                ib_account="DUO847203",
                produced_utc=f"{DAY}T00:00:00Z",
                paper_session_ledger_path=str(REPO_ROOT / "tmp" / "missing-ledger.json"),
                budget_profile="contract_default",
            )
        )

        self.assertFalse(report["ok"])
        self.assertEqual(
            report["pipeline_proof"]["blocked_reason"],
            "PIPELINE_MODE_UNSUPPORTED:paper_day_orchestrator_v2:forensic_replay",
        )

    def test_orchestrator_entrypoint_is_thin_pipeline_launcher(self) -> None:
        fake_report = {
            "ok": True,
            "exit_code": 7,
            "result": {"status": "BLOCKED"},
            "pipeline_proof": {"pipeline_id": "paper_day_orchestrator_v2"},
        }
        stdout = io.StringIO()
        argv = [
            "run_c2_paper_day_orchestrator_v2.py",
            "--day_utc",
            DAY,
            "--mode",
            "PAPER",
            "--ib_account",
            "DUO847203",
            "--produced_utc",
            f"{DAY}T00:00:00Z",
            "--paper_session_ledger_path",
            str(REPO_ROOT / "tmp" / "dummy-ledger.json"),
            "--emit_pipeline_report",
        ]
        with patch(
            "constellation_2.common.paper_day_orchestrator_pipeline_v1.run_paper_day_orchestrator_obligation_pipeline_v1",
            return_value=fake_report,
        ) as runner, patch.object(sys, "argv", argv), patch("sys.stdout", stdout):
            rc = orchestrator_cli.main()

        runner.assert_called_once()
        self.assertEqual(rc, 7)
        rendered = json.loads(stdout.getvalue())
        self.assertEqual(rendered["pipeline_proof"]["pipeline_id"], "paper_day_orchestrator_v2")


if __name__ == "__main__":
    unittest.main()
