from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from unittest.mock import patch

SOURCE_ROOT = Path("/home/node/constellation").resolve()
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from ops.aegis.canonical_operator_state_v1 import _noon_preflight_alert
from ops.tools.run_aegis_noon_preflight_rehearsal_v1 import (
    _build_noon_preflight_report,
    _deliver_noon_preflight_email,
    _validate_candidate_generation_readiness,
    write_noon_preflight_report_v1,
)


DAY = "2026-05-18"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _candidate_diagnostics(root: Path, **overrides: object) -> Path:
    payload = {
        "schema_id": "aegis_candidate_generation_diagnostics",
        "schema_version": "v1",
        "day_utc": DAY,
        "candidate_generation_status": "RAN",
        "total_sleeves_expected": 1,
        "total_sleeves_run": 1,
        "total_raw_signals": 0,
        "total_candidates_generated": 0,
        "total_candidates_rejected": 0,
        "operator_interpretation": "NORMAL_NO_SIGNAL",
        "sleeves": [
            {
                "sleeve_id": "SLEEVE_A",
                "run_status": "RAN",
                "raw_signal_count": 0,
                "candidate_count": 0,
                "rejected_count": 0,
                "rejection_reasons": [],
                "data_status": "OK",
                "reason_no_candidate": "No qualifying setup.",
            }
        ],
        "trigger_evaluation": {"ran": True, "trigger_count": 1, "activated_count": 0, "skipped_count": 0, "reason": "No trigger activated."},
        "recommended_next_steps": ["No action needed."],
    }
    payload.update(overrides)
    path = root / "reports" / "aegis_candidate_generation_diagnostics_v1" / DAY / "candidate_generation_diagnostics.v1.json"
    _write_json(path, payload)
    return path


def _candidate_readiness(root: Path) -> tuple[list[dict], list[str], dict]:
    validations: list[dict] = []
    blockers: list[str] = []
    readiness = _validate_candidate_generation_readiness(
        validations=validations,
        blockers=blockers,
        source_refs=[],
        truth_root=root,
        day_utc=DAY,
    )
    return validations, blockers, readiness


def test_noon_preflight_requires_candidate_diagnostics(tmp_path: Path) -> None:
    validations, blockers, readiness = _candidate_readiness(tmp_path)

    assert readiness["candidate_generation_ready"] is False
    assert "CANDIDATE_DIAGNOSTICS_MISSING" in blockers
    assert validations[-1]["reason_code"] == "CANDIDATE_DIAGNOSTICS_MISSING"


def test_noon_preflight_fails_on_data_blocked(tmp_path: Path) -> None:
    _candidate_diagnostics(
        tmp_path,
        candidate_generation_status="UNKNOWN",
        total_sleeves_run=0,
        operator_interpretation="DATA_BLOCKED",
        sleeves=[{"sleeve_id": "SLEEVE_A", "run_status": "NOT_RUN", "data_status": "MISSING"}],
        trigger_evaluation={"ran": False, "trigger_count": 0, "activated_count": 0, "skipped_count": 0, "reason": "Missing event packet."},
    )

    _validations, blockers, readiness = _candidate_readiness(tmp_path)
    report = _build_noon_preflight_report(
        day_utc=DAY,
        generated_at_utc=f"{DAY}T16:00:00Z",
        status={"result": "FAIL"},
        validations=[],
        blockers=blockers,
        next_action="Run candidate diagnostics.",
        candidate_readiness=readiness,
        email_attempt={"email_attempted": False, "email_status": "NOT_CONFIGURED", "email_error": "EMAIL_NOT_CONFIGURED", "email_alert_sent": False},
    )

    assert report["status"] == "FAILED"
    assert report["operator_alert_required"] is True
    assert report["operator_interpretation"] == "DATA_BLOCKED"
    assert report["candidate_generation_ready"] is False
    assert report["data_ready"] is False
    assert "DATA_BLOCKED" in report["classifications"]
    assert any(row["code"] == "CANDIDATE_GENERATION_NOT_READY" for row in report["blocking_items"])


def test_noon_preflight_fails_when_expected_sleeves_never_run(tmp_path: Path) -> None:
    _candidate_diagnostics(tmp_path, candidate_generation_status="RAN", total_sleeves_expected=2, total_sleeves_run=0)

    _validations, blockers, readiness = _candidate_readiness(tmp_path)

    assert readiness["candidate_generation_ready"] is False
    assert "SLEEVE_RUNS_MISSING" in blockers


def test_noon_preflight_fails_when_enabled_sleeves_are_not_enumerated(tmp_path: Path) -> None:
    _candidate_diagnostics(
        tmp_path,
        candidate_generation_status="RAN",
        total_sleeves_enabled=7,
        total_sleeves_expected_today=1,
        total_sleeves_expected=1,
        total_sleeves_run=1,
        sleeves=[{"sleeve_id": "ONLY_ONE", "expected_today": True, "run_status": "RAN", "data_status": "OK"}],
    )

    _validations, blockers, readiness = _candidate_readiness(tmp_path)

    assert readiness["candidate_generation_ready"] is False
    assert "ENABLED_SLEEVES_NOT_ENUMERATED" in blockers
    assert "CANDIDATE_DIAGNOSTICS_INCOMPLETE" in blockers


def test_noon_preflight_warns_on_partial_vix_block_when_ready_sleeves_ran(tmp_path: Path) -> None:
    sleeves = [
        {"sleeve_id": f"SLEEVE_{idx}", "expected_today": True, "run_status": "RAN", "data_status": "OK"}
        for idx in range(6)
    ]
    sleeves.append({"sleeve_id": "VIX_SLEEVE", "expected_today": True, "run_status": "BLOCKED", "data_status": "MISSING", "reason_no_candidate": "Blocked by sleeve input contract: market.volatility.VIX."})
    _candidate_diagnostics(
        tmp_path,
        candidate_generation_status="PARTIAL",
        operator_interpretation="PARTIAL_RUN",
        total_sleeves_enabled=7,
        total_sleeves_expected_today=7,
        total_sleeves_expected=7,
        total_sleeves_run=6,
        total_sleeves_ready=0,
        total_sleeves_ready_with_warnings=6,
        total_sleeves_blocked=1,
        sleeves=sleeves,
        input_artifacts={
            "data_registry": "data_registry.v1.json",
            "sleeve_input_contracts": "sleeve_input_contracts.v1.json",
            "sleeve_readiness": "sleeve_readiness.v1.json",
        },
        market_data_summary={
            "provider_config": {"configured": True, "primary": "LOCAL_CACHE", "fallback": "STOOQ"},
            "missing_symbols": ["VIX"],
            "stale_symbols": [],
            "mapping_missing_symbols": [],
            "failure_reason": None,
            "missing_symbol_explanations": {
                "VIX": {
                    "canonical_symbol": "VIX",
                    "operator_message": "VIX is missing from LOCAL_CACHE canonical/alias paths and fallback provider lookup failed; C2_VOL_INCOME_DEFINED_RISK_V1 remains blocked and the run remains READY_PARTIAL. Other ready sleeves may proceed to operator review. No execution is authorized.",
                    "synthetic_data_allowed": False,
                }
            },
        },
        trigger_evaluation={"ran": False, "trigger_count": 0, "activated_count": 0, "skipped_count": 0, "reason": "No event trigger artifact."},
    )

    validations, blockers, readiness = _candidate_readiness(tmp_path)

    assert readiness["candidate_generation_ready"] is True
    assert readiness["operator_interpretation"] == "PARTIAL_RUN"
    assert "SYMBOL_DATA_MISSING" not in blockers
    assert "CANDIDATE_GENERATION_STATUS_PARTIAL" in readiness["warning_codes"]
    assert "SYMBOL_DATA_MISSING" in readiness["warning_codes"]
    assert readiness["missing_symbol_explanations"]["VIX"]["canonical_symbol"] == "VIX"
    assert readiness["missing_symbol_explanations"]["VIX"]["synthetic_data_allowed"] is False
    assert readiness["blocked_sleeve_explanations"][0]["required_symbol"] == "VIX"
    assert validations[-1]["status"] == "WARN"


def test_partial_noon_preflight_report_surfaces_vix_explanation(tmp_path: Path) -> None:
    report = _build_noon_preflight_report(
        day_utc=DAY,
        generated_at_utc=f"{DAY}T16:00:00Z",
        status={"result": "WARN", "readiness_status": "READY_PARTIAL"},
        validations=[],
        blockers=[],
        next_action="Review listed WARN items before canonical 09:50 UTC and 14:50 UTC sleeve run.",
        candidate_readiness={
            "candidate_generation_ready": True,
            "data_ready": True,
            "sleeves_enabled": 7,
            "sleeves_expected": 7,
            "sleeves_run": 6,
            "operator_interpretation": "PARTIAL_RUN",
            "candidate_generation_status": "PARTIAL",
            "warning_codes": ["SYMBOL_DATA_MISSING"],
            "blocking_codes": [],
            "recommended_action": ["No action needed."],
            "missing_symbol_explanations": {
                "VIX": {
                    "canonical_symbol": "VIX",
                    "operator_message": "VIX is missing from LOCAL_CACHE canonical/alias paths and fallback provider lookup failed; C2_VOL_INCOME_DEFINED_RISK_V1 remains blocked and the run remains READY_PARTIAL. Other ready sleeves may proceed to operator review. No execution is authorized.",
                }
            },
            "blocked_sleeve_explanations": [
                {
                    "sleeve_id": "C2_VOL_INCOME_DEFINED_RISK_V1",
                    "required_symbol": "VIX",
                    "explanation": "C2_VOL_INCOME_DEFINED_RISK_V1 requires canonical VIX.",
                    "fail_closed": True,
                }
            ],
        },
        email_attempt={"email_attempted": True, "email_status": "SENT", "email_error": None, "email_alert_sent": True},
    )
    paths = write_noon_preflight_report_v1(truth_root=tmp_path, day_utc=DAY, payload=report)
    summary = Path(paths["summary"]).read_text(encoding="utf-8")

    assert report["readiness_status"] == "READY_PARTIAL"
    assert report["missing_symbol_explanations"]["VIX"]["canonical_symbol"] == "VIX"
    assert "No execution is authorized" in summary


def test_noon_preflight_report_files_generate(tmp_path: Path) -> None:
    payload = _build_noon_preflight_report(
        day_utc=DAY,
        generated_at_utc=f"{DAY}T16:00:00Z",
        status={"result": "FAIL"},
        validations=[],
        blockers=["DATA_BLOCKED"],
        next_action="Run candidate diagnostics.",
        candidate_readiness={
            "candidate_generation_ready": False,
            "data_ready": False,
            "sleeves_expected": 1,
            "sleeves_run": 0,
            "operator_interpretation": "DATA_BLOCKED",
            "candidate_generation_status": "UNKNOWN",
            "blocking_codes": ["DATA_BLOCKED"],
            "recommended_action": ["npm run aegis:candidate-diagnostics"],
        },
        email_attempt={"email_attempted": False, "email_status": "NOT_CONFIGURED", "email_error": "EMAIL_NOT_CONFIGURED", "email_alert_sent": False},
    )

    paths = write_noon_preflight_report_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)

    assert Path(paths["json"]).is_file()
    assert Path(paths["summary"]).is_file()
    assert Path(paths["matrix"]).is_file()
    assert json.loads(Path(paths["json"]).read_text(encoding="utf-8"))["status"] == "FAILED"


class _FakeSmtp:
    sent = False

    def __init__(self, *_args, **_kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def starttls(self):
        return None

    def login(self, *_args):
        return None

    def send_message(self, _message):
        type(self).sent = True


def test_noon_preflight_email_attempted_when_configured() -> None:
    env = {
        "C2_EMAIL_SMTP_HOST": "smtp.example.test",
        "C2_EMAIL_SMTP_PORT": "587",
        "C2_EMAIL_USERNAME": "operator",
        "C2_EMAIL_PASSWORD": "secret",
        "C2_EMAIL_FROM": "aegis@example.test",
        "C2_EMAIL_TO": "operator@example.test",
    }
    with patch.dict(os.environ, env, clear=False), patch("ops.tools.run_aegis_noon_preflight_rehearsal_v1.smtplib.SMTP", _FakeSmtp):
        result = _deliver_noon_preflight_email(
            alert_required=True,
            alert_transport={**env, "configured": True, "from": env["C2_EMAIL_FROM"], "to": env["C2_EMAIL_TO"], "smtp_host": env["C2_EMAIL_SMTP_HOST"], "smtp_port": env["C2_EMAIL_SMTP_PORT"], "use_tls": True},
            subject="Aegis noon preflight failed — candidate generation blocked",
            body="status: FAILED",
        )

    assert result["email_attempted"] is True
    assert result["email_status"] == "SENT"
    assert _FakeSmtp.sent is True


def test_noon_preflight_email_not_configured_is_surfaced() -> None:
    result = _deliver_noon_preflight_email(
        alert_required=True,
        alert_transport={"configured": False, "missing": ["C2_EMAIL_TO"]},
        subject="subject",
        body="body",
    )

    assert result["email_attempted"] is False
    assert result["email_status"] == "NOT_CONFIGURED"
    assert "EMAIL_NOT_CONFIGURED" in str(result["email_error"])


def test_passing_noon_preflight_does_not_alert() -> None:
    report = _build_noon_preflight_report(
        day_utc=DAY,
        generated_at_utc=f"{DAY}T16:00:00Z",
        status={"result": "PASS"},
        validations=[],
        blockers=[],
        next_action="No operator action.",
        candidate_readiness={
            "candidate_generation_ready": True,
            "data_ready": True,
            "sleeves_expected": 1,
            "sleeves_run": 1,
            "operator_interpretation": "NORMAL_NO_SIGNAL",
            "candidate_generation_status": "RAN",
            "blocking_codes": [],
            "recommended_action": ["No action needed."],
        },
        email_attempt={"email_attempted": False, "email_status": "DISABLED", "email_error": None, "email_alert_sent": False},
    )

    assert report["status"] == "PASSED"
    assert report["readiness_status"] == "READY_FULL"
    assert report["operator_alert_required"] is False
    assert report["alert_severity"] == "NONE"
    assert report["classifications"] == ["READY_FULL"]


def test_partial_noon_preflight_is_ready_partial_not_blocked() -> None:
    report = _build_noon_preflight_report(
        day_utc=DAY,
        generated_at_utc=f"{DAY}T16:00:00Z",
        status={"result": "WARN", "readiness_status": "READY_PARTIAL"},
        validations=[],
        blockers=[],
        next_action="Review listed WARN items before canonical 09:50 UTC and 14:50 UTC sleeve run.",
        candidate_readiness={
            "candidate_generation_ready": True,
            "data_ready": True,
            "sleeves_enabled": 7,
            "sleeves_expected": 7,
            "sleeves_run": 6,
            "operator_interpretation": "PARTIAL_RUN",
            "candidate_generation_status": "PARTIAL",
            "warning_codes": ["SYMBOL_DATA_MISSING"],
            "blocking_codes": [],
            "recommended_action": ["No action needed."],
        },
        email_attempt={"email_attempted": True, "email_status": "SENT", "email_error": None, "email_alert_sent": True},
    )

    assert report["status"] == "PARTIAL"
    assert report["readiness_status"] == "READY_PARTIAL"
    assert report["operator_alert_required"] is True
    assert report["alert_severity"] == "WARNING"
    assert "READY_PARTIAL" in report["classifications"]
    assert "BLOCKED" not in report["classifications"]


def test_blocked_noon_preflight_is_blocked() -> None:
    report = _build_noon_preflight_report(
        day_utc=DAY,
        generated_at_utc=f"{DAY}T16:00:00Z",
        status={"result": "FAIL"},
        validations=[],
        blockers=["DATA_BLOCKED"],
        next_action="Run candidate diagnostics.",
        candidate_readiness={
            "candidate_generation_ready": False,
            "data_ready": False,
            "sleeves_expected": 7,
            "sleeves_run": 0,
            "operator_interpretation": "DATA_BLOCKED",
            "candidate_generation_status": "NOT_RUN",
            "blocking_codes": ["DATA_BLOCKED"],
        },
        email_attempt={"email_attempted": True, "email_status": "SENT", "email_error": None, "email_alert_sent": True},
    )

    assert report["status"] == "FAILED"
    assert report["readiness_status"] == "BLOCKED"
    assert "BLOCKED" in report["classifications"]
    assert "READY_PARTIAL" not in report["classifications"]


def test_ui_projection_surfaces_failed_preflight_without_email() -> None:
    alert = _noon_preflight_alert(
        {
            "status": "FAILED",
            "operator_alert_required": True,
            "alert_reason": "DATA_BLOCKED",
            "generated_at_utc": f"{DAY}T16:00:00Z",
            "next_safe_command": "npm run aegis:candidate-diagnostics",
            "email_status": "NOT_CONFIGURED",
        }
    )

    assert alert["available"] is True
    assert alert["title"] == "Noon preflight failed and no email alert was delivered."
    assert alert["email_status"] == "NOT_CONFIGURED"


def test_noon_preflight_safety_does_not_enable_broker_or_autonomous_execution() -> None:
    report = _build_noon_preflight_report(
        day_utc=DAY,
        generated_at_utc=f"{DAY}T16:00:00Z",
        status={"result": "FAIL"},
        validations=[],
        blockers=["DATA_BLOCKED"],
        next_action="Run candidate diagnostics.",
        candidate_readiness={"candidate_generation_ready": False, "data_ready": False, "sleeves_expected": 1, "sleeves_run": 0, "operator_interpretation": "DATA_BLOCKED", "candidate_generation_status": "UNKNOWN"},
        email_attempt={"email_attempted": False, "email_status": "NOT_CONFIGURED", "email_error": "EMAIL_NOT_CONFIGURED", "email_alert_sent": False},
    )

    assert report["safety"]["broker_execution_allowed"] is False
    assert report["safety"]["autonomous_execution_allowed"] is False
    assert report["safety"]["live_trading_allowed"] is False
    assert report["safety"]["automatic_approval_allowed"] is False
    assert report["safety"]["automatic_sleeve_mutation_allowed"] is False
