from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import ops.tools.run_startup_materialization_v1 as startup_module


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _write_startup_inputs_prep(truth_root: Path, *, day_utc: str, status: str, blocking_codes: list[str] | None = None) -> None:
    _write_json(
        truth_root / "reports" / "startup_materialization_inputs_prep_v1" / day_utc / "startup_materialization_inputs_prep.v1.json",
        {
            "schema_id": "startup_materialization_inputs_prep",
            "schema_version": "v1",
            "authority_scope": "NON_AUTHORITY_FACT",
            "day_utc": day_utc,
            "status": status,
            "default_equity_reference_price": "655.83" if status == "PASS" else "",
            "blocking_codes": list(blocking_codes or []),
            "produced_at_utc": f"{day_utc}T00:00:00Z",
            "producer": {"repo": "constellation", "module": "test", "git_sha": "a" * 40},
        },
    )


def _write_input_convergence(truth_root: Path, *, day_utc: str, status: str = "SUCCESS") -> None:
    _write_json(
        truth_root / "reports" / "startup_materialization_input_convergence_v1" / day_utc / "startup_materialization_input_convergence.v1.json",
        {
            "schema_id": "startup_materialization_input_convergence_v1",
            "schema_version": 1,
            "generated_utc": f"{day_utc}T00:00:00Z",
            "day_utc": day_utc,
            "convergence_status": status,
            "artifact_results": [],
            "blocker_chain": [],
            "source_refs": [],
        },
    )


def _write_phasec_risk_inputs_prep(truth_root: Path, *, day_utc: str, status: str, blocking_codes: list[str] | None = None) -> None:
    _write_json(
        truth_root / "reports" / "phasec_risk_inputs_prep_v1" / day_utc / "phasec_risk_inputs_prep.v1.json",
        {
            "schema_id": "phasec_risk_inputs_prep",
            "schema_version": "v1",
            "authority_scope": "NON_AUTHORITY_FACT",
            "day_utc": day_utc,
            "status": status,
            "compatible_nav_path": str(truth_root / "accounting_compat_v1" / "nav" / day_utc / "nav_snapshot.v1.json"),
            "drawdown_pct": "0.000000" if status == "PASS" else "",
            "required_inputs_checked": [],
            "blocking_codes": list(blocking_codes or []),
            "producer": {"repo": "constellation", "module": "test", "git_sha": "a" * 40},
            "produced_at_utc": f"{day_utc}T00:00:00Z",
            "bridge_result": {"returncode": 0 if status == "PASS" else 1, "stdout": "", "stderr": "", "artifact_path": "", "artifact_status": "PRESENT"},
            "human_readable_summary": "test",
        },
    )


def test_startup_materialization_succeeds_when_phasec_risk_inputs_pass() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        execution_truth_root = Path(td) / "truth_sleeves" / "PRIMARY" / "PAPER"
        day_utc = "2026-04-14"
        _write_json(truth_root / "intents_v1" / "snapshots" / day_utc / "spy.exposure_intent.v1.json", {"schema_id": "exposure_intent"})
        phasec_root = execution_truth_root / "phaseC_preflight_v1" / day_utc
        identity_dir = phasec_root / "attempt_A0001" / "spy"
        identity_dir.mkdir(parents=True, exist_ok=True)
        for filename in ("equity_order_plan.v2.json", "mapping_ledger_record.v2.json", "binding_record.v2.json"):
            _write_json(identity_dir / filename, {"ok": True})
        _write_json(
            phasec_root / "latest_active_attempt.v1.json",
            {
                "schema_id": "phasec_latest_active_attempt.v1",
                "schema_version": "v1",
                "day_utc": day_utc,
                "attempt_id": "A0001",
                "attempt_dir": str((phasec_root / "attempt_A0001").resolve()),
            },
        )

        with patch.object(startup_module, "resolve_decision_truth_root_v1", return_value=truth_root), patch.object(
            startup_module, "_resolve_execution_truth_root", return_value=execution_truth_root
        ), patch.object(startup_module, "_run_input_convergence", return_value={"returncode": 0, "stdout": "", "stderr": "", "cmd": []}), patch.object(
            startup_module, "_load_input_convergence_payload", return_value={"convergence_status": "SUCCESS", "blocker_chain": []}
        ), patch.object(
            startup_module, "_run_inputs_prep", return_value={"returncode": 0, "stdout": "", "stderr": "", "cmd": []}
        ), patch.object(
            startup_module,
            "_load_inputs_prep_payload",
            return_value={"status": "PASS", "blocking_codes": [], "default_equity_reference_price": "655.83"},
        ), patch.object(
            startup_module, "_run_phasec_risk_inputs_prep", return_value={"returncode": 0, "stdout": "", "stderr": "", "cmd": []}
        ), patch.object(
            startup_module, "_load_phasec_risk_inputs_prep_payload", return_value={"status": "PASS", "blocking_codes": []}
        ), patch.object(
            startup_module, "_run_phasec_materializer", return_value={"returncode": 0, "stdout": "", "stderr": "", "cmd": []}
        ), patch.object(
            startup_module, "discover_phasec_identity_dirs_v1", return_value=[identity_dir]
        ):
            rc = startup_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

        payload = json.loads(
            (truth_root / "reports" / "startup_materialization_v1" / day_utc / "startup_materialization.v1.json").read_text(encoding="utf-8")
        )
        assert rc == 0
        assert payload["status"] == "SUCCESS"


def test_startup_materialization_fails_closed_when_phasec_risk_inputs_fail() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        execution_truth_root = Path(td) / "truth_sleeves" / "PRIMARY" / "PAPER"
        day_utc = "2026-04-14"
        _write_json(truth_root / "intents_v1" / "snapshots" / day_utc / "spy.exposure_intent.v1.json", {"schema_id": "exposure_intent"})
        with patch.object(startup_module, "resolve_decision_truth_root_v1", return_value=truth_root), patch.object(
            startup_module, "_resolve_execution_truth_root", return_value=execution_truth_root
        ), patch.object(startup_module, "_run_input_convergence", return_value={"returncode": 0, "stdout": "", "stderr": "", "cmd": []}), patch.object(
            startup_module, "_load_input_convergence_payload", return_value={"convergence_status": "SUCCESS", "blocker_chain": []}
        ), patch.object(
            startup_module, "_run_inputs_prep", return_value={"returncode": 0, "stdout": "", "stderr": "", "cmd": []}
        ), patch.object(
            startup_module,
            "_load_inputs_prep_payload",
            return_value={"status": "PASS", "blocking_codes": [], "default_equity_reference_price": "655.83"},
        ), patch.object(
            startup_module, "_run_phasec_risk_inputs_prep", return_value={"returncode": 2, "stdout": "", "stderr": "FAIL", "cmd": []}
        ), patch.object(
            startup_module,
            "_load_phasec_risk_inputs_prep_payload",
            return_value={
                "status": "BLOCKED_BY_DEFECT",
                "blocking_codes": ["PHASEC_RISK_INPUTS_PREP_FAIL:ACCOUNTING_NAV_COMPAT_BRIDGE_NONZERO"],
            },
        ), patch.object(
            startup_module, "_run_phasec_materializer", side_effect=AssertionError("phasec should not run")
        ):
            rc = startup_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

        payload = json.loads(
            (truth_root / "reports" / "startup_materialization_v1" / day_utc / "startup_materialization.v1.json").read_text(encoding="utf-8")
        )
        assert rc == 2
        assert payload["status"] != "SUCCESS"
        assert "STARTUP_MATERIALIZATION_FAIL:PHASEC_RISK_INPUTS_PREP_FAIL:ACCOUNTING_NAV_COMPAT_BRIDGE_NONZERO" in payload["blocking_codes"]


def test_startup_materialization_attempts_options_promotion_for_short_vol_intent_when_raw_exists() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        execution_truth_root = Path(td) / "truth_sleeves" / "PRIMARY" / "PAPER"
        day_utc = "2026-04-14"
        _write_json(
            truth_root / "intents_v1" / "snapshots" / day_utc / "spy.exposure_intent.v1.json",
            {"schema_id": "exposure_intent", "exposure_type": "SHORT_VOL_DEFINED", "underlying": {"symbol": "SPY"}},
        )
        phasec_root = execution_truth_root / "phaseC_preflight_v1" / day_utc
        identity_dir = phasec_root / "attempt_A0001" / "spy"
        identity_dir.mkdir(parents=True, exist_ok=True)
        for filename in ("equity_order_plan.v2.json", "mapping_ledger_record.v2.json", "binding_record.v2.json"):
            _write_json(identity_dir / filename, {"ok": True})
        _write_json(
            phasec_root / "latest_active_attempt.v1.json",
            {
                "schema_id": "phasec_latest_active_attempt.v1",
                "schema_version": "v1",
                "day_utc": day_utc,
                "attempt_id": "A0001",
                "attempt_dir": str((phasec_root / "attempt_A0001").resolve()),
            },
        )

        with patch.object(startup_module, "resolve_decision_truth_root_v1", return_value=truth_root), patch.object(
            startup_module, "_resolve_execution_truth_root", return_value=execution_truth_root
        ), patch.object(startup_module, "_run_input_convergence", return_value={"returncode": 0, "stdout": "", "stderr": "", "cmd": []}), patch.object(
            startup_module, "_load_input_convergence_payload", return_value={"convergence_status": "SUCCESS", "blocker_chain": []}
        ), patch.object(
            startup_module, "_run_inputs_prep", return_value={"returncode": 0, "stdout": "", "stderr": "", "cmd": []}
        ), patch.object(
            startup_module,
            "_load_inputs_prep_payload",
            return_value={"status": "PASS", "blocking_codes": [], "default_equity_reference_price": ""},
        ), patch.object(
            startup_module, "_run_phasec_risk_inputs_prep", return_value={"returncode": 0, "stdout": "", "stderr": "", "cmd": []}
        ), patch.object(
            startup_module, "_load_phasec_risk_inputs_prep_payload", return_value={"status": "PASS", "blocking_codes": []}
        ), patch.object(
            startup_module, "_run_phasec_materializer", return_value={"returncode": 0, "stdout": "", "stderr": "", "cmd": []}
        ), patch.object(
            startup_module, "discover_phasec_identity_dirs_v1", return_value=[identity_dir]
        ), patch.object(
            startup_module, "_options_snapshot_exists_for_symbol", return_value=False
        ), patch.object(
            startup_module, "_options_raw_exists_for_symbol", return_value=True
        ), patch.object(
            startup_module,
            "_run_options_raw_capture_for_symbol",
            side_effect=AssertionError("capture should not run when raw already exists"),
        ), patch.object(
            startup_module,
            "_run_options_truth_promotion_for_symbol",
            return_value={"returncode": 0, "stdout": "OK", "stderr": "", "cmd": []},
        ) as promotion_mock:
            rc = startup_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

        payload = json.loads(
            (truth_root / "reports" / "startup_materialization_v1" / day_utc / "startup_materialization.v1.json").read_text(encoding="utf-8")
        )
        assert rc == 0
        assert payload["status"] == "SUCCESS"
        assert promotion_mock.call_count == 1


def test_startup_materialization_attempts_options_capture_then_promotion_when_raw_missing() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        execution_truth_root = Path(td) / "truth_sleeves" / "PRIMARY" / "PAPER"
        day_utc = "2026-04-14"
        _write_json(
            truth_root / "intents_v1" / "snapshots" / day_utc / "spy.exposure_intent.v1.json",
            {"schema_id": "exposure_intent", "exposure_type": "SHORT_VOL_DEFINED", "underlying": {"symbol": "SPY"}},
        )
        phasec_root = execution_truth_root / "phaseC_preflight_v1" / day_utc
        identity_dir = phasec_root / "attempt_A0001" / "spy"
        identity_dir.mkdir(parents=True, exist_ok=True)
        for filename in ("equity_order_plan.v2.json", "mapping_ledger_record.v2.json", "binding_record.v2.json"):
            _write_json(identity_dir / filename, {"ok": True})
        _write_json(
            phasec_root / "latest_active_attempt.v1.json",
            {
                "schema_id": "phasec_latest_active_attempt.v1",
                "schema_version": "v1",
                "day_utc": day_utc,
                "attempt_id": "A0001",
                "attempt_dir": str((phasec_root / "attempt_A0001").resolve()),
            },
        )

        with patch.object(startup_module, "resolve_decision_truth_root_v1", return_value=truth_root), patch.object(
            startup_module, "_resolve_execution_truth_root", return_value=execution_truth_root
        ), patch.object(startup_module, "_run_input_convergence", return_value={"returncode": 0, "stdout": "", "stderr": "", "cmd": []}), patch.object(
            startup_module, "_load_input_convergence_payload", return_value={"convergence_status": "SUCCESS", "blocker_chain": []}
        ), patch.object(
            startup_module, "_run_inputs_prep", return_value={"returncode": 0, "stdout": "", "stderr": "", "cmd": []}
        ), patch.object(
            startup_module,
            "_load_inputs_prep_payload",
            return_value={"status": "PASS", "blocking_codes": [], "default_equity_reference_price": ""},
        ), patch.object(
            startup_module, "_run_phasec_risk_inputs_prep", return_value={"returncode": 0, "stdout": "", "stderr": "", "cmd": []}
        ), patch.object(
            startup_module, "_load_phasec_risk_inputs_prep_payload", return_value={"status": "PASS", "blocking_codes": []}
        ), patch.object(
            startup_module, "_run_phasec_materializer", return_value={"returncode": 0, "stdout": "", "stderr": "", "cmd": []}
        ), patch.object(
            startup_module, "discover_phasec_identity_dirs_v1", return_value=[identity_dir]
        ), patch.object(
            startup_module, "_options_snapshot_exists_for_symbol", return_value=False
        ), patch.object(
            startup_module, "_options_raw_exists_for_symbol", side_effect=[False, True]
        ), patch.object(
            startup_module,
            "_run_options_raw_capture_for_symbol",
            return_value={"returncode": 0, "stdout": "CAPTURE_OK", "stderr": "", "cmd": []},
        ) as capture_mock, patch.object(
            startup_module,
            "_run_options_truth_promotion_for_symbol",
            return_value={"returncode": 0, "stdout": "PROMOTION_OK", "stderr": "", "cmd": []},
        ) as promotion_mock:
            rc = startup_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

        payload = json.loads(
            (truth_root / "reports" / "startup_materialization_v1" / day_utc / "startup_materialization.v1.json").read_text(encoding="utf-8")
        )
        assert rc == 0
        assert payload["status"] == "SUCCESS"
        assert capture_mock.call_count == 1
        assert promotion_mock.call_count == 1


def test_startup_materialization_keeps_phasec_veto_fail_closed_when_options_raw_missing() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        execution_truth_root = Path(td) / "truth_sleeves" / "PRIMARY" / "PAPER"
        day_utc = "2026-04-14"
        _write_json(
            truth_root / "intents_v1" / "snapshots" / day_utc / "spy.exposure_intent.v1.json",
            {"schema_id": "exposure_intent", "exposure_type": "SHORT_VOL_DEFINED", "underlying": {"symbol": "SPY"}},
        )
        phasec_root = execution_truth_root / "phaseC_preflight_v1" / day_utc
        (phasec_root / "attempt_A0001").mkdir(parents=True, exist_ok=True)
        _write_json(
            phasec_root / "latest_active_attempt.v1.json",
            {
                "schema_id": "phasec_latest_active_attempt.v1",
                "schema_version": "v1",
                "day_utc": day_utc,
                "attempt_id": "A0001",
                "attempt_dir": str((phasec_root / "attempt_A0001").resolve()),
            },
        )

        with patch.object(startup_module, "resolve_decision_truth_root_v1", return_value=truth_root), patch.object(
            startup_module, "_resolve_execution_truth_root", return_value=execution_truth_root
        ), patch.object(startup_module, "_run_input_convergence", return_value={"returncode": 0, "stdout": "", "stderr": "", "cmd": []}), patch.object(
            startup_module, "_load_input_convergence_payload", return_value={"convergence_status": "SUCCESS", "blocker_chain": []}
        ), patch.object(
            startup_module, "_run_inputs_prep", return_value={"returncode": 0, "stdout": "", "stderr": "", "cmd": []}
        ), patch.object(
            startup_module,
            "_load_inputs_prep_payload",
            return_value={"status": "PASS", "blocking_codes": [], "default_equity_reference_price": ""},
        ), patch.object(
            startup_module, "_run_phasec_risk_inputs_prep", return_value={"returncode": 0, "stdout": "", "stderr": "", "cmd": []}
        ), patch.object(
            startup_module, "_load_phasec_risk_inputs_prep_payload", return_value={"status": "PASS", "blocking_codes": []}
        ), patch.object(
            startup_module, "_run_phasec_materializer", return_value={"returncode": 0, "stdout": "", "stderr": "", "cmd": []}
        ), patch.object(
            startup_module, "discover_phasec_identity_dirs_v1", return_value=[]
        ), patch.object(
            startup_module, "_options_snapshot_exists_for_symbol", return_value=False
        ), patch.object(
            startup_module, "_options_raw_exists_for_symbol", return_value=False
        ), patch.object(
            startup_module,
            "_run_options_raw_capture_for_symbol",
            return_value={"returncode": 2, "stdout": "", "stderr": "CAPTURE_FAIL", "cmd": []},
        ) as capture_mock, patch.object(
            startup_module, "_run_options_truth_promotion_for_symbol", side_effect=AssertionError("promotion should not run without raw")
        ), patch.object(
            startup_module,
            "_phasec_veto_evaluation",
            return_value={
                "blocking_codes": ["STARTUP_MATERIALIZATION_FAIL:PHASEC_VETO:C2_SUBMIT_FAIL_CLOSED_REQUIRED"],
                "ignored_rows": [],
            },
        ):
            rc = startup_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

        payload = json.loads(
            (truth_root / "reports" / "startup_materialization_v1" / day_utc / "startup_materialization.v1.json").read_text(encoding="utf-8")
        )
        assert rc == 2
        assert payload["status"] != "SUCCESS"
        assert capture_mock.call_count == 1
        assert "STARTUP_MATERIALIZATION_FAIL:PHASEC_VETO:C2_SUBMIT_FAIL_CLOSED_REQUIRED" in payload["blocking_codes"]
