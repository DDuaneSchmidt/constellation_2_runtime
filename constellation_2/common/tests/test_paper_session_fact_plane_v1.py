from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_paper_trading_posture_v1 as posture_module
import ops.tools.run_startup_materialization_v1 as startup_module
import ops.tools.run_submit_boundary_status_v1 as boundary_module
from constellation_2.common.paper_session_fact_plane_v1 import canonical_paper_session_id_v1


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _write_market_calendar_day(truth_root: Path, *, day_utc: str, is_trading_session: bool) -> None:
    manifest_path = truth_root / "market_calendar_v1" / "dataset_manifest.json"
    year_path = truth_root / "market_calendar_v1" / "NYSE" / f"{day_utc[:4]}.jsonl"
    year_path.parent.mkdir(parents=True, exist_ok=True)
    row = {"day_utc": day_utc, "is_trading_session": is_trading_session}
    year_payload = json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n"
    year_path.write_text(year_payload, encoding="utf-8")
    import hashlib

    _write_json(
        manifest_path,
        {
            "files": [
                {
                    "exchange": "NYSE",
                    "year": int(day_utc[:4]),
                    "file": f"NYSE/{day_utc[:4]}.jsonl",
                    "sha256": hashlib.sha256(year_payload.encode("utf-8")).hexdigest(),
                }
            ]
        },
    )


def _write_trade_submit_status(truth_root: Path, *, day_utc: str, ib_account: str, ok: bool, state: str) -> Path:
    path = truth_root / "trade_submit_readiness_c2_v1" / "_history" / "PAPER" / ib_account / day_utc / "status.json"
    _write_json(
        path,
        {
            "schema_id": "trade_submit_readiness_c2",
            "schema_version": "v1",
            "day_utc": day_utc,
            "as_of_utc": f"{day_utc}T00:00:00Z",
            "expires_utc": f"{day_utc}T00:02:00Z",
            "ok": ok,
            "state": state,
            "environment": "PAPER",
            "ib_account": ib_account,
            "reasons": [] if ok else ["READINESS_NOT_OK"],
            "input_manifest": [],
            "producer": {"repo": "constellation", "module": "test", "git_sha": "abc1234"},
            "provenance": {
                "truth_root": str(truth_root.resolve()),
                "registry_sha256": "a" * 64,
                "sleeve_registry_sha256": "b" * 64,
            },
            "session_authority_attestation": {
                "decision_artifact_path": "/tmp/day_authority.json",
                "decision_artifact_sha256": "c" * 64,
                "policy_version": "validation_result_only",
                "evaluator_version": "validation_result_only",
                "venue": "C2",
                "session_date": day_utc,
                "decision_status": "OK",
                "session_class": None,
                "stage_id": "PRE_ORCHESTRATION_PREFLIGHT",
                "policy_action": "SKIP",
                "stage_execution_status": state,
                "reason_codes": [],
            },
            "run_state_authority_attestation": {
                "authority_family": "day_authority_decision_v1",
                "authority_artifact_path": "/tmp/day_authority.json",
                "authority_artifact_sha256": "d" * 64,
                "policy_version": "validation_result_only",
                "evaluator_version": "validation_result_only",
                "decision_status": "OK",
                "classification_field": "decision_state",
                "classification_value": "OPEN",
                "cycle_snapshot_family": "gate_stack_verdict_v1",
                "cycle_snapshot_artifact_path": "/tmp/gate_stack.json",
                "cycle_snapshot_artifact_sha256": "e" * 64,
                "cycle_id": f"{day_utc}:OK",
                "cycle_coherence_status": "COHERENT",
                "stage_id": "TRADE_SUBMIT_READINESS",
                "stage_execution_status": state,
                "reason_codes": [],
                "upstream_authority_refs": [],
            },
        },
    )
    return path


def _write_startup_inputs_prep(
    truth_root: Path,
    *,
    day_utc: str,
    status: str,
    blocking_codes: list[str] | None = None,
    default_equity_reference_price: str = "",
) -> None:
    _write_json(
        truth_root / "reports" / "startup_materialization_inputs_prep_v1" / day_utc / "startup_materialization_inputs_prep.v1.json",
        {
            "schema_id": "startup_materialization_inputs_prep",
            "schema_version": "v1",
            "authority_scope": "NON_AUTHORITY_FACT",
            "day_utc": day_utc,
            "session_id": canonical_paper_session_id_v1(day_utc),
            "status": status,
            "equity_entry_symbols": ["SPY"],
            "default_equity_reference_price": default_equity_reference_price,
            "default_equity_reference_price_source": "LIQUIDITY_GATE" if default_equity_reference_price else "NONE",
            "default_equity_reference_price_artifact_path": "/tmp/liquidity.json" if default_equity_reference_price else "",
            "required_inputs_checked": [],
            "blocking_codes": list(blocking_codes or []),
            "producer": {"repo": "constellation", "module": "test", "git_sha": "abc1234"},
            "produced_at_utc": f"{day_utc}T00:00:00Z",
            "liquidity_gate_result": {
                "returncode": 0 if status == "PASS" else 1,
                "stdout": "",
                "stderr": "",
                "artifact_path": "/tmp/liquidity.json",
                "artifact_status": "PASS" if status == "PASS" else "FAIL",
            },
            "human_readable_summary": "test",
        },
    )


def _write_phasec_risk_inputs_prep(
    truth_root: Path,
    *,
    day_utc: str,
    status: str,
    blocking_codes: list[str] | None = None,
    drawdown_pct: str = "0.000000",
) -> None:
    _write_json(
        truth_root / "reports" / "phasec_risk_inputs_prep_v1" / day_utc / "phasec_risk_inputs_prep.v1.json",
        {
            "schema_id": "phasec_risk_inputs_prep",
            "schema_version": "v1",
            "authority_scope": "NON_AUTHORITY_FACT",
            "day_utc": day_utc,
            "session_id": canonical_paper_session_id_v1(day_utc),
            "status": status,
            "compatible_nav_path": str((truth_root / "accounting_compat_v1" / "nav" / day_utc / "nav_snapshot.v1.json").resolve()),
            "drawdown_pct": drawdown_pct if status == "PASS" else "",
            "required_inputs_checked": [],
            "blocking_codes": list(blocking_codes or []),
            "producer": {"repo": "constellation", "module": "test", "git_sha": "abc1234"},
            "produced_at_utc": f"{day_utc}T00:00:00Z",
            "bridge_result": {
                "returncode": 0 if status == "PASS" else 1,
                "stdout": "",
                "stderr": "",
                "artifact_path": str((truth_root / "accounting_compat_v1" / "nav" / day_utc / "nav_snapshot.v1.json").resolve()),
                "artifact_status": "PRESENT" if status == "PASS" else "MISSING",
            },
            "human_readable_summary": "test",
        },
    )


def test_startup_materialization_happy_path_writes_success() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        day_utc = "2026-04-08"
        intents_dir = truth_root / "intents_v1" / "snapshots" / day_utc
        _write_json(intents_dir / "spy.exposure_intent.v1.json", {"schema_id": "exposure_intent", "schema_version": "v1"})
        _write_startup_inputs_prep(
            truth_root,
            day_utc=day_utc,
            status="PASS",
            default_equity_reference_price="655.83",
        )
        _write_phasec_risk_inputs_prep(truth_root, day_utc=day_utc, status="PASS")
        phasec_root = truth_root / "phaseC_preflight_v1" / day_utc
        identity_dir = phasec_root / "attempt_A0001" / "spy"
        identity_dir.mkdir(parents=True, exist_ok=True)
        for name in ("equity_order_plan.v2.json", "mapping_ledger_record.v2.json", "binding_record.v2.json"):
            _write_json(identity_dir / name, {"ok": True})
        _write_json(
            phasec_root / "latest_active_attempt.v1.json",
            {"schema_id": "phasec_latest_active_attempt.v1", "schema_version": "v1", "day_utc": day_utc, "attempt_id": "A0001", "attempt_dir": str((phasec_root / "attempt_A0001").resolve())},
        )
        with patch.object(startup_module, "_run_inputs_prep", return_value={"returncode": 0, "stdout": "", "stderr": "", "cmd": []}), patch.object(startup_module, "_run_phasec_risk_inputs_prep", return_value={"returncode": 0, "stdout": "", "stderr": "", "cmd": []}), patch.object(startup_module, "_run_phasec_materializer", return_value={"returncode": 0, "stdout": "", "stderr": "", "cmd": []}):
            rc = startup_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
        assert rc == 0
        payload = json.loads((truth_root / "reports" / "startup_materialization_v1" / day_utc / "startup_materialization.v1.json").read_text(encoding="utf-8"))
        assert payload["status"] == "SUCCESS"
        assert payload["session_id"] == canonical_paper_session_id_v1(day_utc)
        assert payload["authority_scope"] == "NON_AUTHORITY_FACT"


def test_startup_materialization_missing_inputs_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        day_utc = "2026-04-08"
        _write_startup_inputs_prep(
            truth_root,
            day_utc=day_utc,
            status="BLOCKED_VALID",
            blocking_codes=["STARTUP_MATERIALIZATION_MISSING_DEPENDENCY:INTENTS_DAY_DIR_EMPTY_OR_ABSENT"],
        )
        with patch.object(startup_module, "_run_inputs_prep", return_value={"returncode": 2, "stdout": "", "stderr": "FAIL", "cmd": []}), patch.object(startup_module, "_run_phasec_risk_inputs_prep", side_effect=AssertionError("risk prep should not run")), patch.object(startup_module, "_run_phasec_materializer", side_effect=AssertionError("phasec should not run")):
            rc = startup_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
        assert rc == 2
        payload = json.loads((truth_root / "reports" / "startup_materialization_v1" / day_utc / "startup_materialization.v1.json").read_text(encoding="utf-8"))
        assert payload["status"] != "SUCCESS"
        assert "STARTUP_MATERIALIZATION_MISSING_DEPENDENCY:INTENTS_DAY_DIR_EMPTY_OR_ABSENT" in payload["blocking_codes"]


def test_startup_materialization_fails_closed_on_phasec_risk_inputs_blocker() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        day_utc = "2026-04-08"
        intents_dir = truth_root / "intents_v1" / "snapshots" / day_utc
        _write_json(intents_dir / "spy.exposure_intent.v1.json", {"schema_id": "exposure_intent", "schema_version": "v1"})
        _write_startup_inputs_prep(
            truth_root,
            day_utc=day_utc,
            status="PASS",
            default_equity_reference_price="655.83",
        )
        _write_phasec_risk_inputs_prep(
            truth_root,
            day_utc=day_utc,
            status="BLOCKED_BY_DEFECT",
            blocking_codes=["PHASEC_RISK_INPUTS_PREP_FAIL:ACCOUNTING_NAV_COMPAT_BRIDGE_NONZERO"],
        )
        with patch.object(startup_module, "_run_inputs_prep", return_value={"returncode": 0, "stdout": "", "stderr": "", "cmd": []}), patch.object(startup_module, "_run_phasec_risk_inputs_prep", return_value={"returncode": 2, "stdout": "", "stderr": "FAIL", "cmd": []}), patch.object(startup_module, "_run_phasec_materializer", side_effect=AssertionError("phasec should not run")):
            rc = startup_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
        assert rc == 2
        payload = json.loads((truth_root / "reports" / "startup_materialization_v1" / day_utc / "startup_materialization.v1.json").read_text(encoding="utf-8"))
        assert payload["status"] != "SUCCESS"
        assert "STARTUP_MATERIALIZATION_FAIL:PHASEC_RISK_INPUTS_PREP_FAIL:ACCOUNTING_NAV_COMPAT_BRIDGE_NONZERO" in payload["blocking_codes"]


def test_startup_materialization_prep_block_prevents_old_phasec_pointer_blockers() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        day_utc = "2026-04-08"
        intents_dir = truth_root / "intents_v1" / "snapshots" / day_utc
        _write_json(
            intents_dir / "spy.exposure_intent.v1.json",
            {
                "schema_id": "exposure_intent",
                "schema_version": "v1",
                "underlying": {"symbol": "SPY"},
                "target_notional_pct": "0.01",
            },
        )
        _write_startup_inputs_prep(
            truth_root,
            day_utc=day_utc,
            status="BLOCKED_VALID",
            blocking_codes=["STARTUP_MATERIALIZATION_MISSING_DEPENDENCY:DEFAULT_EQUITY_REFERENCE_PRICE_UNAVAILABLE"],
        )
        with patch.object(startup_module, "_run_inputs_prep", return_value={"returncode": 2, "stdout": "", "stderr": "FAIL", "cmd": []}), patch.object(startup_module, "_run_phasec_risk_inputs_prep", side_effect=AssertionError("risk prep should not run")), patch.object(startup_module, "_run_phasec_materializer", side_effect=AssertionError("phasec should not run")):
            rc = startup_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
        assert rc == 2
        payload = json.loads(
            (
                truth_root
                / "reports"
                / "startup_materialization_v1"
                / day_utc
                / "startup_materialization.v1.json"
            ).read_text(encoding="utf-8")
        )
        assert "STARTUP_MATERIALIZATION_MISSING_DEPENDENCY:DEFAULT_EQUITY_REFERENCE_PRICE_UNAVAILABLE" in payload["blocking_codes"]
        assert "STARTUP_MATERIALIZATION_UNKNOWN:LATEST_ACTIVE_ATTEMPT_POINTER_MISSING" not in payload["blocking_codes"]
        assert "STARTUP_MATERIALIZATION_FAIL:NO_SUPPORTED_IDENTITY_OUTPUTS" not in payload["blocking_codes"]


def test_startup_materialization_surfaces_latest_phasec_veto_reason() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        day_utc = "2026-04-08"
        intents_dir = truth_root / "intents_v1" / "snapshots" / day_utc
        _write_json(
            intents_dir / "spy.exposure_intent.v1.json",
            {
                "schema_id": "exposure_intent",
                "schema_version": "v1",
                "underlying": {"symbol": "SPY"},
                "target_notional_pct": "0.01",
            },
        )
        _write_startup_inputs_prep(
            truth_root,
            day_utc=day_utc,
            status="PASS",
            default_equity_reference_price="655.83",
        )
        _write_phasec_risk_inputs_prep(truth_root, day_utc=day_utc, status="PASS")
        veto_path = (
            truth_root
            / "phaseC_preflight_v1"
            / day_utc
            / "attempt_A0001"
            / "spy.veto_record.v1.json"
        )
        _write_json(
            veto_path,
            {
                "reason_code": "C2_SUBMIT_FAIL_CLOSED_REQUIRED",
                "reason_detail": "DRAWDOWN_MISSING_FAIL_CLOSED",
            },
        )
        with patch.object(startup_module, "_run_inputs_prep", return_value={"returncode": 0, "stdout": "", "stderr": "", "cmd": []}), patch.object(startup_module, "_run_phasec_risk_inputs_prep", return_value={"returncode": 0, "stdout": "", "stderr": "", "cmd": []}), patch.object(startup_module, "_run_phasec_materializer", return_value={"returncode": 0, "stdout": "", "stderr": "", "cmd": []}):
            rc = startup_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
        assert rc == 2
        payload = json.loads(
            (
                truth_root
                / "reports"
                / "startup_materialization_v1"
                / day_utc
                / "startup_materialization.v1.json"
            ).read_text(encoding="utf-8")
        )
        assert "STARTUP_MATERIALIZATION_FAIL:PHASEC_VETO:C2_SUBMIT_FAIL_CLOSED_REQUIRED" in payload["blocking_codes"]
        assert "STARTUP_MATERIALIZATION_MISSING_DEPENDENCY:DRAWDOWN_MISSING_FAIL_CLOSED" in payload["blocking_codes"]
        assert "STARTUP_MATERIALIZATION_UNKNOWN:LATEST_ACTIVE_ATTEMPT_POINTER_MISSING" not in payload["blocking_codes"]
        assert "STARTUP_MATERIALIZATION_FAIL:NO_SUPPORTED_IDENTITY_OUTPUTS" not in payload["blocking_codes"]


def test_startup_materialization_ignores_stale_phasec_veto_when_newer_active_attempt_exists() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        day_utc = "2026-04-08"
        intents_dir = truth_root / "intents_v1" / "snapshots" / day_utc
        _write_json(
            intents_dir / "spy.exposure_intent.v1.json",
            {
                "schema_id": "exposure_intent",
                "schema_version": "v1",
                "underlying": {"symbol": "SPY"},
                "target_notional_pct": "0.01",
            },
        )
        _write_startup_inputs_prep(
            truth_root,
            day_utc=day_utc,
            status="PASS",
            default_equity_reference_price="655.83",
        )
        _write_phasec_risk_inputs_prep(truth_root, day_utc=day_utc, status="PASS")
        phasec_root = truth_root / "phaseC_preflight_v1" / day_utc
        _write_json(
            phasec_root / "attempt_A0001" / "spy.veto_record.v1.json",
            {
                "reason_code": "C2_SUBMIT_FAIL_CLOSED_REQUIRED",
                "reason_detail": "DRAWDOWN_MISSING_FAIL_CLOSED",
            },
        )
        identity_dir = phasec_root / "attempt_A0002" / "spy"
        identity_dir.mkdir(parents=True, exist_ok=True)
        for name in ("equity_order_plan.v2.json", "mapping_ledger_record.v2.json", "binding_record.v2.json"):
            _write_json(identity_dir / name, {"ok": True})
        _write_json(
            phasec_root / "latest_active_attempt.v1.json",
            {
                "schema_id": "phasec_latest_active_attempt.v1",
                "schema_version": "v1",
                "day_utc": day_utc,
                "attempt_id": "A0002",
                "attempt_dir": str((phasec_root / "attempt_A0002").resolve()),
            },
        )
        with patch.object(startup_module, "_run_inputs_prep", return_value={"returncode": 0, "stdout": "", "stderr": "", "cmd": []}), patch.object(startup_module, "_run_phasec_risk_inputs_prep", return_value={"returncode": 0, "stdout": "", "stderr": "", "cmd": []}), patch.object(startup_module, "_run_phasec_materializer", return_value={"returncode": 0, "stdout": "", "stderr": "", "cmd": []}):
            rc = startup_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
        assert rc == 0
        payload = json.loads(
            (
                truth_root
                / "reports"
                / "startup_materialization_v1"
                / day_utc
                / "startup_materialization.v1.json"
            ).read_text(encoding="utf-8")
        )
        assert payload["status"] == "SUCCESS"
        assert "STARTUP_MATERIALIZATION_FAIL:PHASEC_VETO:C2_SUBMIT_FAIL_CLOSED_REQUIRED" not in payload["blocking_codes"]
        assert "STARTUP_MATERIALIZATION_MISSING_DEPENDENCY:DRAWDOWN_MISSING_FAIL_CLOSED" not in payload["blocking_codes"]


def test_paper_trading_posture_non_trading_day_is_valid_no_op_fact() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        day_utc = "2026-04-08"
        _write_market_calendar_day(truth_root, day_utc=day_utc, is_trading_session=False)
        rc = posture_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
        assert rc == 0
        payload = json.loads((truth_root / "reports" / "paper_trading_posture_v1" / day_utc / "paper_trading_posture.v1.json").read_text(encoding="utf-8"))
        assert payload["posture_status"] == "DISABLED"
        assert payload["expected_no_op_today"] is True
        assert payload["blocking_reason_codes"] == ["MARKET_CALENDAR_NON_TRADING_SESSION"]
        assert payload["authority_scope"] == "NON_AUTHORITY_FACT"
        assert payload["binding_classification"] == "NON_BINDING_DIAGNOSTIC"


def test_paper_trading_posture_missing_calendar_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        day_utc = "2026-04-08"
        rc = posture_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
        assert rc == 2
        payload = json.loads((truth_root / "reports" / "paper_trading_posture_v1" / day_utc / "paper_trading_posture.v1.json").read_text(encoding="utf-8"))
        assert payload["posture_status"] == "UNKNOWN"
        assert payload["system_ready"] is False


def test_submit_boundary_authorized_happy_path() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        truth_root = root / "truth"
        day_utc = "2026-04-08"
        _write_json(
            root / "governance" / "02_REGISTRIES" / "C2_SLEEVE_REGISTRY_V1.json",
            {
                "schema_id": "c2_sleeve_registry",
                "schema_version": "v1",
                "sleeves": [{"sleeve_id": "PRIMARY", "enabled": True, "mode": "PAPER", "ib_account": "DUO847203"}],
            },
        )
        _write_json(
            truth_root / "reports" / "startup_materialization_v1" / day_utc / "startup_materialization.v1.json",
            {
                "schema_id": "startup_materialization",
                "schema_version": "v1",
                "authority_scope": "NON_AUTHORITY_FACT",
                "day_utc": day_utc,
                "session_id": canonical_paper_session_id_v1(day_utc),
                "status": "SUCCESS",
                "required_inputs_checked": [],
                "materialized_outputs": [],
                "blocking_codes": [],
                "producer": {"repo": "constellation", "module": "test", "git_sha": "abc1234"},
                "produced_at_utc": f"{day_utc}T00:00:00Z",
                "freshness_verdict": "CURRENT",
                "linkage_verdict": "LINKED",
                "path_resolution_evidence": {"phasec_root": "/tmp/phasec", "latest_active_attempt_path": "/tmp/latest.json"},
                "producer_run_id": "startup:test",
                "phasec_materializer_result": {"returncode": 0, "stdout": "", "stderr": ""},
            },
        )
        _write_market_calendar_day(truth_root, day_utc=day_utc, is_trading_session=True)
        posture_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
        _write_trade_submit_status(truth_root, day_utc=day_utc, ib_account="DUO847203", ok=True, state="OK")
        with patch.object(boundary_module, "REPO_ROOT", root):
            rc = boundary_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
        assert rc == 0
        payload = json.loads((truth_root / "reports" / "submit_boundary_status_v1" / day_utc / "submit_boundary_status.v1.json").read_text(encoding="utf-8"))
        assert payload["boundary_status"] == "AUTHORIZED"
        assert payload["submission_authorized"] is True
        assert payload["session_id"] == canonical_paper_session_id_v1(day_utc)


def test_submit_boundary_missing_dependency_blocks() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        truth_root = root / "truth"
        day_utc = "2026-04-08"
        _write_json(
            root / "governance" / "02_REGISTRIES" / "C2_SLEEVE_REGISTRY_V1.json",
            {
                "schema_id": "c2_sleeve_registry",
                "schema_version": "v1",
                "sleeves": [{"sleeve_id": "PRIMARY", "enabled": True, "mode": "PAPER", "ib_account": "DUO847203"}],
            },
        )
        with patch.object(boundary_module, "REPO_ROOT", root):
            rc = boundary_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
        assert rc == 2
        payload = json.loads((truth_root / "reports" / "submit_boundary_status_v1" / day_utc / "submit_boundary_status.v1.json").read_text(encoding="utf-8"))
        assert payload["boundary_status"] == "BLOCKED"
        assert payload["submission_authorized"] is False


def test_submit_boundary_refreshes_missing_trade_submit_readiness_and_stays_current_linked() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        truth_root = root / "truth"
        day_utc = "2026-04-08"
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
                        "assigned_engine_ids": ["C2_DEFENSIVE_TAIL_V1"],
                        "active_controllable_engine_ids": ["C2_DEFENSIVE_TAIL_V1"],
                        "disabled_by_default_engine_ids": [],
                        "support_only_engine_ids": [],
                    }
                ],
            },
        )
        _write_json(
            truth_root / "reports" / "startup_materialization_v1" / day_utc / "startup_materialization.v1.json",
            {
                "schema_id": "startup_materialization",
                "schema_version": "v1",
                "authority_scope": "NON_AUTHORITY_FACT",
                "day_utc": day_utc,
                "session_id": canonical_paper_session_id_v1(day_utc),
                "status": "SUCCESS",
                "required_inputs_checked": [],
                "materialized_outputs": [],
                "blocking_codes": [],
                "producer": {"repo": "constellation", "module": "test", "git_sha": "abc1234"},
                "produced_at_utc": f"{day_utc}T00:00:00Z",
                "freshness_verdict": "CURRENT",
                "linkage_verdict": "LINKED",
                "path_resolution_evidence": {"phasec_root": "/tmp/phasec", "latest_active_attempt_path": "/tmp/latest.json"},
                "producer_run_id": "startup:test",
                "phasec_materializer_result": {"returncode": 0, "stdout": "", "stderr": ""},
            },
        )
        _write_market_calendar_day(truth_root, day_utc=day_utc, is_trading_session=True)
        posture_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
        with patch.object(boundary_module, "REPO_ROOT", root):
            rc = boundary_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
        assert rc == 0
        payload = json.loads((truth_root / "reports" / "submit_boundary_status_v1" / day_utc / "submit_boundary_status.v1.json").read_text(encoding="utf-8"))
        assert payload["boundary_status"] == "DENIED"
        assert payload["submission_authorized"] is False
        assert payload["freshness_verdict"] == "CURRENT"
        assert payload["linkage_verdict"] == "LINKED"
        assert "trade_submit_readiness_c2_v1" in {row["logical_name"] for row in payload["required_boundary_checks"]}
        readiness_path = truth_root / "trade_submit_readiness_c2_v1" / "_history" / "PAPER" / "DUO847203" / day_utc / "status.json"
        readiness = json.loads(readiness_path.read_text(encoding="utf-8"))
        assert readiness["environment"] == "PAPER"
        assert readiness["ib_account"] == "DUO847203"
        assert readiness["day_utc"] == day_utc
        assert readiness["state"] == "FAIL"
        assert readiness["provenance"]["truth_root"] == str(truth_root.resolve())


def test_submit_boundary_denied_when_posture_disabled() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        truth_root = root / "truth"
        day_utc = "2026-04-08"
        _write_json(
            root / "governance" / "02_REGISTRIES" / "C2_SLEEVE_REGISTRY_V1.json",
            {
                "schema_id": "c2_sleeve_registry",
                "schema_version": "v1",
                "sleeves": [{"sleeve_id": "PRIMARY", "enabled": True, "mode": "PAPER", "ib_account": "DUO847203"}],
            },
        )
        _write_json(
            truth_root / "reports" / "startup_materialization_v1" / day_utc / "startup_materialization.v1.json",
            {
                "schema_id": "startup_materialization",
                "schema_version": "v1",
                "authority_scope": "NON_AUTHORITY_FACT",
                "day_utc": day_utc,
                "session_id": canonical_paper_session_id_v1(day_utc),
                "status": "SUCCESS",
                "required_inputs_checked": [],
                "materialized_outputs": [],
                "blocking_codes": [],
                "producer": {"repo": "constellation", "module": "test", "git_sha": "abc1234"},
                "produced_at_utc": f"{day_utc}T00:00:00Z",
                "freshness_verdict": "CURRENT",
                "linkage_verdict": "LINKED",
                "path_resolution_evidence": {"phasec_root": "/tmp/phasec", "latest_active_attempt_path": "/tmp/latest.json"},
                "producer_run_id": "startup:test",
                "phasec_materializer_result": {"returncode": 0, "stdout": "", "stderr": ""},
            },
        )
        _write_market_calendar_day(truth_root, day_utc=day_utc, is_trading_session=False)
        posture_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
        _write_trade_submit_status(truth_root, day_utc=day_utc, ib_account="DUO847203", ok=True, state="OK")
        with patch.object(boundary_module, "REPO_ROOT", root):
            rc = boundary_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
        assert rc == 0
        payload = json.loads((truth_root / "reports" / "submit_boundary_status_v1" / day_utc / "submit_boundary_status.v1.json").read_text(encoding="utf-8"))
        assert payload["boundary_status"] == "DENIED"
        assert payload["submission_authorized"] is False


def test_fact_plane_outputs_are_deterministic_for_fixed_inputs() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        day_utc = "2026-04-08"
        _write_market_calendar_day(truth_root, day_utc=day_utc, is_trading_session=False)
        with patch.object(posture_module, "now_utc_iso_v1", side_effect=["2026-04-08T00:00:00Z", "2026-04-08T00:00:01Z"]):
            posture_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
            first = (truth_root / "reports" / "paper_trading_posture_v1" / day_utc / "paper_trading_posture.v1.json").read_text(encoding="utf-8")
            posture_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
            second = (truth_root / "reports" / "paper_trading_posture_v1" / day_utc / "paper_trading_posture.v1.json").read_text(encoding="utf-8")
        assert first == second


def test_startup_materialization_noop_preserves_bytes_when_only_timestamp_changes() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        day_utc = "2026-04-08"
        intents_dir = truth_root / "intents_v1" / "snapshots" / day_utc
        _write_json(intents_dir / "spy.exposure_intent.v1.json", {"schema_id": "exposure_intent", "schema_version": "v1"})
        _write_startup_inputs_prep(
            truth_root,
            day_utc=day_utc,
            status="PASS",
            default_equity_reference_price="655.83",
        )
        _write_phasec_risk_inputs_prep(truth_root, day_utc=day_utc, status="PASS")
        phasec_root = truth_root / "phaseC_preflight_v1" / day_utc
        identity_dir = phasec_root / "attempt_A0001" / "spy"
        identity_dir.mkdir(parents=True, exist_ok=True)
        for name in ("equity_order_plan.v2.json", "mapping_ledger_record.v2.json", "binding_record.v2.json"):
            _write_json(identity_dir / name, {"ok": True})
        _write_json(
            phasec_root / "latest_active_attempt.v1.json",
            {"schema_id": "phasec_latest_active_attempt.v1", "schema_version": "v1", "day_utc": day_utc, "attempt_id": "A0001", "attempt_dir": str((phasec_root / "attempt_A0001").resolve())},
        )
        with patch.object(startup_module, "_run_inputs_prep", return_value={"returncode": 0, "stdout": "", "stderr": "", "cmd": []}), patch.object(startup_module, "_run_phasec_risk_inputs_prep", return_value={"returncode": 0, "stdout": "", "stderr": "", "cmd": []}), patch.object(startup_module, "_run_phasec_materializer", return_value={"returncode": 0, "stdout": "", "stderr": "", "cmd": []}), patch.object(startup_module, "now_utc_iso_v1", side_effect=["2026-04-08T00:00:00Z", "2026-04-08T00:00:01Z"]):
            startup_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
            first = (truth_root / "reports" / "startup_materialization_v1" / day_utc / "startup_materialization.v1.json").read_text(encoding="utf-8")
            startup_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
            second = (truth_root / "reports" / "startup_materialization_v1" / day_utc / "startup_materialization.v1.json").read_text(encoding="utf-8")
        assert first == second


def test_submit_boundary_noop_preserves_bytes_when_only_timestamp_changes() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        truth_root = root / "truth"
        day_utc = "2026-04-08"
        _write_json(
            root / "governance" / "02_REGISTRIES" / "C2_SLEEVE_REGISTRY_V1.json",
            {
                "schema_id": "c2_sleeve_registry",
                "schema_version": "v1",
                "sleeves": [{"sleeve_id": "PRIMARY", "enabled": True, "mode": "PAPER", "ib_account": "DUO847203"}],
            },
        )
        _write_json(
            truth_root / "reports" / "startup_materialization_v1" / day_utc / "startup_materialization.v1.json",
            {
                "schema_id": "startup_materialization",
                "schema_version": "v1",
                "authority_scope": "NON_AUTHORITY_FACT",
                "day_utc": day_utc,
                "session_id": canonical_paper_session_id_v1(day_utc),
                "status": "SUCCESS",
                "required_inputs_checked": [],
                "materialized_outputs": [],
                "blocking_codes": [],
                "producer": {"repo": "constellation", "module": "test", "git_sha": "abc1234"},
                "produced_at_utc": f"{day_utc}T00:00:00Z",
                "freshness_verdict": "CURRENT",
                "linkage_verdict": "LINKED",
                "path_resolution_evidence": {"phasec_root": "/tmp/phasec", "latest_active_attempt_path": "/tmp/latest.json"},
                "producer_run_id": "startup:test",
                "phasec_materializer_result": {"returncode": 0, "stdout": "", "stderr": ""},
            },
        )
        _write_market_calendar_day(truth_root, day_utc=day_utc, is_trading_session=True)
        posture_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
        _write_trade_submit_status(truth_root, day_utc=day_utc, ib_account="DUO847203", ok=True, state="OK")
        with patch.object(boundary_module, "REPO_ROOT", root), patch.object(boundary_module, "now_utc_iso_v1", side_effect=["2026-04-08T00:00:00Z", "2026-04-08T00:00:01Z"]):
            boundary_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
            first = (truth_root / "reports" / "submit_boundary_status_v1" / day_utc / "submit_boundary_status.v1.json").read_text(encoding="utf-8")
            boundary_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
            second = (truth_root / "reports" / "submit_boundary_status_v1" / day_utc / "submit_boundary_status.v1.json").read_text(encoding="utf-8")
        assert first == second
