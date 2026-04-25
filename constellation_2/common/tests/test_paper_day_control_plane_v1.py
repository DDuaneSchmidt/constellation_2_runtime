from __future__ import annotations

import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_intents_day_completeness_v1 as intents_day_completeness_module
import ops.tools.run_paper_day_control_plane_v1 as control_plane_module
import ops.tools.run_paper_session_ledger_v1 as ledger_module
import ops.tools.run_startup_proof_validation_v1 as startup_proof_module
from constellation_2.common.paper_session_fact_plane_v1 import canonical_paper_session_id_v1
from constellation_2.common.paper_session_ledger_v1 import build_paper_session_ledger_v1, write_paper_session_ledger_v1
from constellation_2.common.paper_session_path_alignment_v1 import resolve_paper_day_control_plane_attempt_path
from constellation_2.common.tests.test_next_day_readiness_consistency_gate_v1 import (
    _operator_summary_dossier,
    _write_build_admission_active as _write_consistency_build_admission_active,
    _write_control_plane as _write_consistency_control_plane,
    _write_kill_switch as _write_consistency_kill_switch,
    _write_status as _write_consistency_status,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _producer() -> dict:
    return {"repo": "constellation", "module": "test", "git_sha": "a" * 40}


def _fact_dep(day_utc: str, logical_name: str) -> dict:
    return {
        "logical_name": logical_name,
        "absolute_path": f"/tmp/{logical_name}.json",
        "sha256": "b" * 64,
        "day_utc": day_utc,
        "status": "PASS",
        "reason_codes": [],
    }


def _write_startup(truth_root: Path, day_utc: str, *, status: str = "SUCCESS") -> None:
    _write_json(
        truth_root / "reports" / "startup_materialization_v1" / day_utc / "startup_materialization.v1.json",
        {
            "schema_id": "startup_materialization",
            "schema_version": "v1",
            "authority_scope": "NON_AUTHORITY_FACT",
            "day_utc": day_utc,
            "session_id": canonical_paper_session_id_v1(day_utc),
            "status": status,
            "required_inputs_checked": [_fact_dep(day_utc, "intent_file:spy.json")],
            "materialized_outputs": [_fact_dep(day_utc, "phasec_materialized_output:spy:binding_record.v2.json")],
            "blocking_codes": [] if status == "SUCCESS" else ["STARTUP_DENIED"],
            "producer": _producer(),
            "produced_at_utc": f"{day_utc}T00:00:00Z",
            "freshness_verdict": "CURRENT",
            "linkage_verdict": "LINKED",
            "path_resolution_evidence": {
                "phasec_root": "/tmp/phasec",
                "latest_active_attempt_path": "/tmp/latest_active_attempt.v1.json",
            },
            "producer_run_id": f"startup_materialization_v1:{day_utc}",
            "phasec_materializer_result": {"returncode": 0, "stdout": "", "stderr": ""},
        },
    )


def _write_posture(truth_root: Path, day_utc: str, *, enabled: bool = True) -> None:
    _write_json(
        truth_root / "reports" / "paper_trading_posture_v1" / day_utc / "paper_trading_posture.v1.json",
        {
            "schema_id": "paper_trading_posture",
            "schema_version": "v1",
            "authority_scope": "NON_AUTHORITY_FACT",
            "day_utc": day_utc,
            "session_id": canonical_paper_session_id_v1(day_utc),
            "system_ready": enabled,
            "posture_status": "ENABLED" if enabled else "DISABLED",
            "posture_class": "PAPER_READY_ACTIVE" if enabled else "PAPER_READY_NO_OP",
            "blocking_family": "NONE" if enabled else "EXPECTED_NO_OP",
            "expected_no_op_today": not enabled,
            "policy_reasons": ["MARKET_CALENDAR_TRADING_SESSION"] if enabled else ["MARKET_CALENDAR_NON_TRADING_SESSION"],
            "blocking_codes": [] if enabled else ["MARKET_CALENDAR_NON_TRADING_SESSION"],
            "blocking_reason_codes": [] if enabled else ["MARKET_CALENDAR_NON_TRADING_SESSION"],
            "source_dependencies": [
                {
                    "logical_name": "market_calendar_manifest",
                    "absolute_path": "/tmp/market_calendar_manifest.json",
                    "sha256": "c" * 64,
                    "day_utc": day_utc,
                    "status": "OK",
                    "reason_codes": [],
                    "producer": "market_calendar_v1",
                }
            ],
            "producer": _producer(),
            "produced_at_utc": f"{day_utc}T00:01:00Z",
            "freshness_verdict": "CURRENT",
            "linkage_verdict": "LINKED",
        },
    )


def _write_boundary(truth_root: Path, day_utc: str, *, authorized: bool = True) -> None:
    check = _fact_dep(day_utc, "trade_submit_readiness_c2_v1")
    _write_json(
        truth_root / "reports" / "submit_boundary_status_v1" / day_utc / "submit_boundary_status.v1.json",
        {
            "schema_id": "submit_boundary_status",
            "schema_version": "v1",
            "authority_scope": "NON_AUTHORITY_FACT",
            "day_utc": day_utc,
            "session_id": canonical_paper_session_id_v1(day_utc),
            "submission_authorized": authorized,
            "boundary_status": "AUTHORIZED" if authorized else "DENIED",
            "required_boundary_checks": [check],
            "failed_checks": [] if authorized else [check],
            "blocking_codes": [] if authorized else ["SUBMIT_BOUNDARY_TRADE_SUBMIT_READINESS_DENIED"],
            "producer": _producer(),
            "produced_at_utc": f"{day_utc}T00:02:00Z",
            "freshness_verdict": "CURRENT",
            "linkage_verdict": "LINKED",
            "paper_account": "DUO847203",
        },
    )


def _minimal_fact_row(day_utc: str, logical_name: str) -> dict:
    return {
        "logical_name": logical_name,
        "required_for_authority": logical_name != "sleeve_rollup_v1",
        "absolute_path": f"/tmp/{logical_name}.json",
        "schema_id": logical_name.replace("_v1", ""),
        "schema_version": "v1",
        "producer": _producer(),
        "artifact_timestamp_utc": f"{day_utc}T00:00:00Z",
        "artifact_day_utc": day_utc,
        "artifact_session_id": canonical_paper_session_id_v1(day_utc),
        "content_hash": "f" * 64,
        "presence_verdict": "PRESENT",
        "schema_verdict": "VALID",
        "linkage_verdict": "LINKED",
        "freshness_verdict": "CURRENT",
        "duplicate_resolution_verdict": "SINGLE_CANONICAL_PATH",
        "blocking_codes": [],
        "lookup_evidence": [f"resolved_path=/tmp/{logical_name}.json"],
        "fact_snapshot": {},
    }


def _write_ledger(truth_root: Path, day_utc: str, *, authority_status: str) -> None:
    fact_rows = [
        _minimal_fact_row(day_utc, "paper_trading_posture_v1"),
        _minimal_fact_row(day_utc, "startup_materialization_v1"),
        _minimal_fact_row(day_utc, "submit_boundary_status_v1"),
        _minimal_fact_row(day_utc, "sleeve_rollup_v1"),
    ]
    ledger = build_paper_session_ledger_v1(
        day_utc=day_utc,
        session_id=canonical_paper_session_id_v1(day_utc),
        evaluated_at_utc=f"{day_utc}T00:00:00Z",
        provenance=_producer(),
        fact_refs=fact_rows,
        evidence_freeze={
            "evidence_digest": "b" * 64,
            "overall_evidence_status": "READY" if authority_status == "GRANTED" else "DENY",
            "blocking_codes": [] if authority_status == "GRANTED" else ["BLOCKED"],
            "inputs": [row for row in fact_rows if row["required_for_authority"]],
        },
        authority_status=authority_status,
        system_ready=authority_status == "GRANTED",
        submission_authorized=authority_status == "GRANTED",
        control_blocking_codes=[] if authority_status == "GRANTED" else ["BLOCKED"],
        submit_lifecycle={
            "submit_attempt_status": "ATTEMPTED" if authority_status == "GRANTED" else "SKIPPED",
            "submit_attempted": authority_status == "GRANTED",
            "submit_result_status": "PASS" if authority_status == "GRANTED" else "NOT_AUTHORIZED",
            "reason_codes": [] if authority_status == "GRANTED" else ["BLOCKED"],
            "submit_evidence_refs": ["/tmp/sleeve_rollup.v1.json"] if authority_status == "GRANTED" else [],
            "finalization_status": "OPEN",
        },
        post_submit_lifecycle={
            "lineage_status": "NOT_OBSERVED_AT_EVALUATION",
            "latest_authoritative_lineage_ref": "",
            "latest_authoritative_lineage_sha256": "",
            "execution_evidence_refs": [],
            "reconciliation_refs": [],
            "gap_codes": ["PAPER_SESSION_LEDGER_POST_SUBMIT_NOT_OBSERVED"],
        },
        operator_summary={
            "authority_scope": "DERIVED_ONLY_VIEW",
            "ledger_ref": "/tmp/paper_session_ledger.v1.json",
            "summary_state": "AUTHORIZED_TO_PROCEED" if authority_status == "GRANTED" else "AUTHORITY_DENIED",
            "authority_status": authority_status,
            "submission_authorized": authority_status == "GRANTED",
            "non_authority_notice": "Derived only.",
            "blocking_codes": [] if authority_status == "GRANTED" else ["BLOCKED"],
        },
    )
    write_paper_session_ledger_v1(truth_root=truth_root, ledger=ledger)


def _write_startup_proof(truth_root: Path, day_utc: str, *, ready: bool) -> None:
    _write_json(
        truth_root / "reports" / "startup_proof_validation_v1" / day_utc / "startup_proof_validation.v1.json",
        {
            "schema_id": "startup_proof_validation",
            "schema_version": "v1",
            "authority_scope": "NON_AUTHORITY_STARTUP_VALIDATION",
            "validation_scope": "BOD_CONTROL_PLANE_ONLY",
            "day_utc": day_utc,
            "session_id": canonical_paper_session_id_v1(day_utc),
            "status": "STARTUP_READY" if ready else "STARTUP_BLOCKED",
            "ledger_ref": str(
                truth_root / "reports" / "paper_session_ledger_v1" / day_utc / "paper_session_ledger.v1.json"
            ),
            "ledger_id": f"paper_session_ledger:{day_utc}:test",
            "ledger_authority_status": "GRANTED" if ready else "DENIED",
            "checks": [
                {"logical_name": "repo_authority", "status": "PASS", "evidence_ref": "/tmp/repo_role.v1.json", "blocking_codes": [], "detail": {}},
                {"logical_name": "startup_materialization", "status": "PASS" if ready else "FAIL", "evidence_ref": "/tmp/startup_materialization.v1.json", "blocking_codes": [] if ready else ["BLOCKED"], "detail": {}},
                {"logical_name": "paper_session_ledger", "status": "PASS" if ready else "FAIL", "evidence_ref": "/tmp/paper_session_ledger.v1.json", "blocking_codes": [] if ready else ["BLOCKED"], "detail": {}},
            ],
            "blocking_codes": [] if ready else ["BLOCKED"],
            "producer": _producer(),
            "produced_at_utc": f"{day_utc}T00:05:00Z",
            "non_authority_notice": "paper_session_ledger_v1 remains the only authoritative paper-session control owner for the touched startup path.",
        },
    )


def _write_complete_prerequisite(truth_root: Path, day_utc: str) -> None:
    _write_json(
        truth_root / "reports" / "intents_day_completeness_v1" / day_utc / "intents_day_completeness.v1.json",
        {
            "schema_id": "intents_day_completeness",
            "schema_version": "v1",
            "authority_scope": "NON_AUTHORITY_PREREQUISITE_FACT",
            "day_utc": day_utc,
            "session_id": canonical_paper_session_id_v1(day_utc),
            "completeness_status": "COMPLETE",
            "required_inputs_checked": [_fact_dep(day_utc, "intents_day_directory")],
            "missing_inputs": [],
            "freshness_verdict": "CURRENT",
            "linkage_verdict": "LINKED",
            "blocking_codes": [],
            "producer": _producer(),
            "produced_at_utc": f"{day_utc}T00:00:00Z",
        },
    )


def _write_intent_sample(
    truth_root: Path,
    day_utc: str,
    *,
    filename_digest_override: str | None = None,
) -> Path:
    sample_path = SOURCE_ROOT / "constellation_2/phaseH/acceptance/samples/sample_exposure_intent_long_equity.v1.json"
    raw = sample_path.read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    filename_digest = filename_digest_override or digest
    target = truth_root / "intents_v1" / "snapshots" / day_utc / f"{filename_digest}.exposure_intent.v1.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(raw)
    return target


def test_intents_day_completeness_missing_day_dir_blocks_immediately(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"

    rc = intents_day_completeness_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

    assert rc == 2
    payload = json.loads(
        (
            truth_root / "reports" / "intents_day_completeness_v1" / day_utc / "intents_day_completeness.v1.json"
        ).read_text(encoding="utf-8")
    )
    assert payload["completeness_status"] == "INCOMPLETE"
    assert payload["blocking_codes"] == ["INTENTS_DAY_COMPLETENESS_MISSING_DAY_DIR"]


def test_intents_day_completeness_complete_day_passes(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"
    _write_intent_sample(truth_root, day_utc)

    rc = intents_day_completeness_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

    assert rc == 0
    payload = json.loads(
        (
            truth_root / "reports" / "intents_day_completeness_v1" / day_utc / "intents_day_completeness.v1.json"
        ).read_text(encoding="utf-8")
    )
    assert payload["completeness_status"] == "COMPLETE"
    assert payload["blocking_codes"] == []


def test_intents_day_completeness_filename_hash_mismatch_blocks_with_invalid_file(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"
    wrong_path = _write_intent_sample(truth_root, day_utc, filename_digest_override="0" * 64)

    rc = intents_day_completeness_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

    assert rc == 2
    payload = json.loads(
        (
            truth_root / "reports" / "intents_day_completeness_v1" / day_utc / "intents_day_completeness.v1.json"
        ).read_text(encoding="utf-8")
    )
    assert payload["completeness_status"] == "MALFORMED"
    assert payload["blocking_codes"] == ["INTENTS_DAY_COMPLETENESS_INVALID_INTENT_FILE"]
    invalid_rows = [
        row
        for row in payload["required_inputs_checked"]
        if row["logical_name"] == f"intent_snapshot:{wrong_path.name}"
    ]
    assert len(invalid_rows) == 1
    assert invalid_rows[0]["status"] == "INVALID"
    assert invalid_rows[0]["absolute_path"] == str(wrong_path.resolve())
    assert invalid_rows[0]["reason_codes"] == [
        f"INTENTS_DAY_COMPLETENESS_FILENAME_HASH_MISMATCH:path={wrong_path.resolve()}"
    ]


def test_intents_day_completeness_rename_to_content_hash_repairs_day(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"
    wrong_path = _write_intent_sample(truth_root, day_utc, filename_digest_override="0" * 64)
    actual_digest = hashlib.sha256(wrong_path.read_bytes()).hexdigest()
    repaired_path = wrong_path.with_name(f"{actual_digest}.exposure_intent.v1.json")

    first_rc = intents_day_completeness_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
    assert first_rc == 2

    wrong_path.rename(repaired_path)

    second_rc = intents_day_completeness_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

    assert second_rc == 0
    payload = json.loads(
        (
            truth_root / "reports" / "intents_day_completeness_v1" / day_utc / "intents_day_completeness.v1.json"
        ).read_text(encoding="utf-8")
    )
    assert payload["completeness_status"] == "COMPLETE"
    assert payload["blocking_codes"] == []
    repaired_rows = [
        row
        for row in payload["required_inputs_checked"]
        if row["logical_name"] == f"intent_snapshot:{repaired_path.name}"
    ]
    assert len(repaired_rows) == 1
    assert repaired_rows[0]["status"] == "PRESENT"
    assert repaired_rows[0]["reason_codes"] == []


def test_paper_day_control_plane_missing_intents_blocks_before_posture_reason(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"

    rc = control_plane_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

    assert rc == 2
    payload = json.loads(
        (
            truth_root / "reports" / "paper_day_control_plane_v1" / day_utc / "paper_day_control_plane.v1.json"
        ).read_text(encoding="utf-8")
    )
    assert payload["final_start_decision"] == "BLOCKED_VALID"
    assert payload["authority_result"]["first_true_blocker_code"] == "INTENTS_DAY_COMPLETENESS_MISSING_DAY_DIR"
    assert [row["status"] for row in payload["canonical_regeneration_results"]] == [
        "SKIPPED_PREREQUISITE_BLOCKED",
        "SKIPPED_PREREQUISITE_BLOCKED",
        "SKIPPED_PREREQUISITE_BLOCKED",
        "SKIPPED_PREREQUISITE_BLOCKED",
        "SKIPPED_PREREQUISITE_BLOCKED",
    ]
    assert "PAPER_TRADING_POSTURE:OPERATIONAL_READINESS" not in payload["blocking_codes"]
    ignored = {row["logical_name"] for row in payload["ignored_legacy_surfaces"]}
    assert "trading_day_state_v1" in ignored
    assert "session_readiness_refresh_v1" in ignored
    assert "paper_session_admission_certificate_v1" in ignored


def test_paper_day_control_plane_runs_regeneration_in_order_and_can_ready_now(
    tmp_path: Path,
    monkeypatch,
) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"
    order: list[str] = []
    _write_consistency_build_admission_active(truth_root, day_utc=day_utc)
    _write_consistency_kill_switch(truth_root, day_utc=day_utc, canonical_active=False)
    _write_boundary(truth_root, day_utc, authorized=True)
    _write_ledger(truth_root, day_utc, authority_status="GRANTED")
    _write_startup(truth_root, day_utc, status="SUCCESS")
    _write_startup_proof(truth_root, day_utc, ready=True)
    _write_consistency_control_plane(
        truth_root,
        day_utc,
        final_start_decision="READY_NOW",
        ledger_authority_status="GRANTED",
    )
    _write_consistency_status(
        truth_root,
        monkeypatch=monkeypatch,
        day_utc=day_utc,
        current_state="READY",
        submission_authorization_status="AUTHORIZED",
        submission_authorized=True,
        generated_at=datetime(2099, 1, 1, 0, 0, 0, tzinfo=UTC),
    )

    def fake_run(cmd: list[str], *, truth_root: Path) -> dict:
        tool_name = Path(cmd[1]).name
        order.append(tool_name)
        if tool_name == "run_intents_day_completeness_v1.py":
            _write_complete_prerequisite(truth_root, day_utc)
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_startup_materialization_v1.py":
            _write_startup(truth_root, day_utc, status="SUCCESS")
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_paper_trading_posture_v1.py":
            _write_posture(truth_root, day_utc, enabled=True)
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_submit_boundary_status_v1.py":
            _write_boundary(truth_root, day_utc, authorized=True)
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_paper_session_ledger_v1.py":
            _write_ledger(truth_root, day_utc, authority_status="GRANTED")
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_startup_proof_validation_v1.py":
            _write_startup_proof(truth_root, day_utc, ready=True)
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        raise AssertionError(tool_name)

    with patch.object(control_plane_module, "_run", side_effect=fake_run):
        rc = control_plane_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

    assert rc == 0
    assert order == [
        "run_intents_day_completeness_v1.py",
        "run_startup_materialization_v1.py",
        "run_paper_trading_posture_v1.py",
        "run_submit_boundary_status_v1.py",
        "run_paper_session_ledger_v1.py",
        "run_startup_proof_validation_v1.py",
    ]
    payload = json.loads(
        (
            truth_root / "reports" / "paper_day_control_plane_v1" / day_utc / "paper_day_control_plane.v1.json"
        ).read_text(encoding="utf-8")
    )
    assert payload["final_start_decision"] == "READY_NOW"
    assert payload["authority_result"]["ledger_authority_status"] == "GRANTED"
    assert payload["startup_proof_result"]["agreement_with_ledger"] is True


def test_paper_day_control_plane_refreshes_session_authority_status_after_write(
    tmp_path: Path,
    monkeypatch,
) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"
    _write_consistency_build_admission_active(truth_root, day_utc=day_utc)
    _write_consistency_kill_switch(truth_root, day_utc=day_utc, canonical_active=False)
    _write_boundary(truth_root, day_utc, authorized=True)
    _write_ledger(truth_root, day_utc, authority_status="GRANTED")
    _write_startup(truth_root, day_utc, status="SUCCESS")
    _write_startup_proof(truth_root, day_utc, ready=True)
    _write_consistency_control_plane(
        truth_root,
        day_utc,
        final_start_decision="READY_NOW",
        ledger_authority_status="GRANTED",
    )
    _write_consistency_status(
        truth_root,
        monkeypatch=monkeypatch,
        day_utc=day_utc,
        current_state="READY",
        submission_authorization_status="AUTHORIZED",
        submission_authorized=True,
        generated_at=datetime(2099, 1, 1, 0, 0, 0, tzinfo=UTC),
    )

    def fake_run(cmd: list[str], *, truth_root: Path) -> dict:
        tool_name = Path(cmd[1]).name
        if tool_name == "run_intents_day_completeness_v1.py":
            _write_complete_prerequisite(truth_root, day_utc)
        elif tool_name == "run_startup_materialization_v1.py":
            _write_startup(truth_root, day_utc, status="SUCCESS")
        elif tool_name == "run_paper_trading_posture_v1.py":
            _write_posture(truth_root, day_utc, enabled=True)
        elif tool_name == "run_submit_boundary_status_v1.py":
            _write_boundary(truth_root, day_utc, authorized=True)
        elif tool_name == "run_paper_session_ledger_v1.py":
            _write_ledger(truth_root, day_utc, authority_status="GRANTED")
        elif tool_name == "run_startup_proof_validation_v1.py":
            _write_startup_proof(truth_root, day_utc, ready=True)
        else:
            raise AssertionError(tool_name)
        return {"return_code": 0, "stdout": "{}", "stderr": ""}

    refreshed: dict[str, object] = {}

    def fake_build_status(*, truth_root: Path, environment: str):
        refreshed["build_called"] = True
        return {
            "schema_id": "session_authority_status",
            "schema_version": "v1",
            "generated_utc": f"{day_utc}T00:00:00Z",
            "environment": environment,
            "target_day": day_utc,
            "active_day": "2026-04-07",
            "next_target_day": day_utc,
            "blocked_target_day": "",
            "rollover_status": "ROLLED_OVER",
            "rollover_reason_code": "",
            "target_day_admission_status": "ADMIT",
            "target_day_build_status": "COMPLETE",
            "closure_status": "CLOSED",
            "hidden_dependency_check_result": "PASS",
            "traceability_status": "VALID",
            "active_session_generated_utc": f"{day_utc}T00:00:00Z",
            "target_day_admission_generated_utc": f"{day_utc}T00:00:00Z",
            "target_day_build_generated_utc": f"{day_utc}T00:00:00Z",
            "market_calendar_coverage_status": "HEALTHY",
            "market_calendar_coverage_severity": "INFO",
            "market_calendar_required_target_day": day_utc,
            "market_calendar_warning_target_day": "",
            "market_calendar_source_coverage_end": "2099-12-31",
            "market_calendar_runtime_coverage_end": "2099-12-31",
            "market_calendar_source_status": "HEALTHY",
            "market_calendar_runtime_status": "HEALTHY",
            "market_calendar_source_covers_required_target_day": True,
            "market_calendar_runtime_covers_required_target_day": True,
            "market_calendar_operator_action_code": "NONE",
            "market_calendar_coverage_ref": {"artifact_path": "", "artifact_sha256": ""},
            "submission_authorization_status": "AUTHORIZED",
            "submission_authorized": True,
            "first_real_blocker_code": "",
            "first_real_blocker_summary": "",
            "operator_summary_authority_owner": "session_authority_status_v1",
            "operator_summary_authority_manifest_ref": {"artifact_path": "", "artifact_sha256": ""},
            "operator_summary_dossier_ref": {"artifact_path": "", "artifact_sha256": ""},
            "advisory_only_signals": [],
            "ambiguity_state": {"status": "CLEAR", "summary": "", "reason_codes": [], "conflicting_authorities": []},
            "required_operator_action": "",
            "recommended_operator_action": "",
            "rollover_withheld_seconds": 0,
            "status_severity": "INFO",
            "top_blocker_reason_codes": [],
            "active_ref": {"artifact_path": "", "artifact_sha256": ""},
            "target_day_admission_ref": {"artifact_path": "", "artifact_sha256": ""},
            "target_day_build_ref": {"artifact_path": "", "artifact_sha256": ""},
            "monitoring_checks": [],
            "canonical_readiness_authority": None,
            "is_canonical": False,
            "authority_level": "derived",
            "derived_from": ["submit_boundary_status_v1", "paper_session_ledger_v1", "paper_day_control_plane_v1"],
        }

    def fake_write_status(*, truth_root: Path, payload):
        refreshed["write_called"] = True
        refreshed["payload"] = dict(payload)

    with patch.object(control_plane_module, "_run", side_effect=fake_run), patch.object(
        control_plane_module,
        "build_session_authority_status_payload_v1",
        side_effect=fake_build_status,
    ), patch.object(
        control_plane_module,
        "write_session_authority_status_v1",
        side_effect=fake_write_status,
    ):
        rc = control_plane_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

    assert rc == 0
    assert refreshed["build_called"] is True
    assert refreshed["write_called"] is True
    assert refreshed["payload"]["submission_authorization_status"] == "AUTHORIZED"
    assert refreshed["payload"]["target_day"] == day_utc


def test_paper_day_control_plane_ready_now_recomputes_status_before_consistency_gate(
    tmp_path: Path,
    monkeypatch,
) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"
    _write_consistency_build_admission_active(truth_root, day_utc=day_utc)
    _write_consistency_kill_switch(truth_root, day_utc=day_utc, canonical_active=False)
    _write_boundary(truth_root, day_utc, authorized=True)
    _write_ledger(truth_root, day_utc, authority_status="GRANTED")
    _write_startup(truth_root, day_utc, status="SUCCESS")
    _write_startup_proof(truth_root, day_utc, ready=True)
    _write_consistency_control_plane(
        truth_root,
        day_utc,
        final_start_decision="BLOCKED_BY_DEFECT",
        ledger_authority_status="GRANTED",
    )
    _write_consistency_status(
        truth_root,
        monkeypatch=monkeypatch,
        day_utc=day_utc,
        current_state="BLOCKED",
        submission_authorization_status="UNKNOWN",
        submission_authorized=False,
        first_blocker_code="SESSION_AUTHORITY_CANONICAL_MISMATCH",
        first_blocker_summary="stale prior state",
        generated_at=datetime(2099, 1, 1, 0, 0, 0, tzinfo=UTC),
    )
    monkeypatch.undo()
    monkeypatch.setattr(
        "constellation_2.common.session_authority_monitor_v1.build_operator_summary_dossier_v1",
        lambda **kwargs: _operator_summary_dossier(
            day_utc=str(kwargs.get("day_utc") or day_utc),
            current_state="READY",
            submission_authorization_status="AUTHORIZED",
            submission_authorized=True,
        ),
    )

    def fake_run(cmd: list[str], *, truth_root: Path) -> dict:
        tool_name = Path(cmd[1]).name
        if tool_name == "run_intents_day_completeness_v1.py":
            _write_complete_prerequisite(truth_root, day_utc)
        elif tool_name == "run_startup_materialization_v1.py":
            _write_startup(truth_root, day_utc, status="SUCCESS")
        elif tool_name == "run_paper_trading_posture_v1.py":
            _write_posture(truth_root, day_utc, enabled=True)
        elif tool_name == "run_submit_boundary_status_v1.py":
            _write_boundary(truth_root, day_utc, authorized=True)
        elif tool_name == "run_paper_session_ledger_v1.py":
            _write_ledger(truth_root, day_utc, authority_status="GRANTED")
        elif tool_name == "run_startup_proof_validation_v1.py":
            _write_startup_proof(truth_root, day_utc, ready=True)
        else:
            raise AssertionError(tool_name)
        return {"return_code": 0, "stdout": "{}", "stderr": ""}

    with patch.object(control_plane_module, "_run", side_effect=fake_run):
        rc = control_plane_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

    assert rc == 0
    payload = json.loads(
        (
            truth_root / "reports" / "paper_day_control_plane_v1" / day_utc / "paper_day_control_plane.v1.json"
        ).read_text(encoding="utf-8")
    )
    status_payload = json.loads(
        (truth_root / "session_authority_status_v1" / "current.json").read_text(encoding="utf-8")
    )
    assert payload["final_start_decision"] == "READY_NOW"
    assert payload["blocking_codes"] == []
    assert status_payload["submission_authorization_status"] == "AUTHORIZED"
    assert status_payload["first_real_blocker_code"] == ""


def test_paper_day_control_plane_still_blocks_when_recomputed_status_remains_inconsistent(
    tmp_path: Path,
    monkeypatch,
) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"
    _write_consistency_build_admission_active(truth_root, day_utc=day_utc)
    _write_consistency_kill_switch(truth_root, day_utc=day_utc, canonical_active=False)
    _write_complete_prerequisite(truth_root, day_utc)
    _write_startup(truth_root, day_utc, status="SUCCESS")
    _write_posture(truth_root, day_utc, enabled=True)
    _write_boundary(truth_root, day_utc, authorized=True)
    _write_ledger(truth_root, day_utc, authority_status="GRANTED")
    _write_startup_proof(truth_root, day_utc, ready=True)
    monkeypatch.setattr(
        "constellation_2.common.session_authority_monitor_v1.build_operator_summary_dossier_v1",
        lambda **kwargs: _operator_summary_dossier(
            day_utc=str(kwargs.get("day_utc") or day_utc),
            current_state="BLOCKED",
            submission_authorization_status="UNKNOWN",
            submission_authorized=False,
            first_blocker_code="SESSION_AUTHORITY_CANONICAL_MISMATCH",
            first_blocker_summary="forced inconsistency",
        ),
    )

    def fake_run(cmd: list[str], *, truth_root: Path) -> dict:
        tool_name = Path(cmd[1]).name
        if tool_name == "run_intents_day_completeness_v1.py":
            _write_complete_prerequisite(truth_root, day_utc)
        elif tool_name == "run_startup_materialization_v1.py":
            _write_startup(truth_root, day_utc, status="SUCCESS")
        elif tool_name == "run_paper_trading_posture_v1.py":
            _write_posture(truth_root, day_utc, enabled=True)
        elif tool_name == "run_submit_boundary_status_v1.py":
            _write_boundary(truth_root, day_utc, authorized=True)
        elif tool_name == "run_paper_session_ledger_v1.py":
            _write_ledger(truth_root, day_utc, authority_status="GRANTED")
        elif tool_name == "run_startup_proof_validation_v1.py":
            _write_startup_proof(truth_root, day_utc, ready=True)
        else:
            raise AssertionError(tool_name)
        return {"return_code": 0, "stdout": "{}", "stderr": ""}

    with patch.object(control_plane_module, "_run", side_effect=fake_run):
        rc = control_plane_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

    assert rc == 3
    payload = json.loads(
        (
            truth_root / "reports" / "paper_day_control_plane_v1" / day_utc / "paper_day_control_plane.v1.json"
        ).read_text(encoding="utf-8")
    )
    assert payload["final_start_decision"] == "BLOCKED_BY_DEFECT"
    assert payload["authority_result"]["first_true_blocker_code"] == "CONSISTENCY_GATE_FAILURE"
    assert "CONSISTENCY_GATE_FAILURE" in payload["blocking_codes"]


def test_paper_day_control_plane_marks_missing_ledger_output_as_defect(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"

    def fake_run(cmd: list[str], *, truth_root: Path) -> dict:
        tool_name = Path(cmd[1]).name
        if tool_name == "run_intents_day_completeness_v1.py":
            _write_complete_prerequisite(truth_root, day_utc)
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_startup_materialization_v1.py":
            _write_startup(truth_root, day_utc, status="SUCCESS")
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_paper_trading_posture_v1.py":
            _write_posture(truth_root, day_utc, enabled=True)
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_submit_boundary_status_v1.py":
            _write_boundary(truth_root, day_utc, authorized=True)
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_paper_session_ledger_v1.py":
            return {"return_code": 1, "stdout": "", "stderr": "missing ledger output"}
        if tool_name == "run_startup_proof_validation_v1.py":
            _write_startup_proof(truth_root, day_utc, ready=False)
            return {"return_code": 2, "stdout": "{}", "stderr": ""}
        raise AssertionError(tool_name)

    with patch.object(control_plane_module, "_run", side_effect=fake_run):
        rc = control_plane_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

    assert rc == 3
    payload = json.loads(
        (
            truth_root / "reports" / "paper_day_control_plane_v1" / day_utc / "paper_day_control_plane.v1.json"
        ).read_text(encoding="utf-8")
    )
    assert payload["final_start_decision"] == "BLOCKED_BY_DEFECT"
    assert "PAPER_DAY_CONTROL_PLANE_OUTPUT_MISSING:paper_session_ledger_v1" in payload["blocking_codes"]


def test_paper_day_control_plane_refreshes_stale_ledger_and_startup_proof_after_upstream_success(
    tmp_path: Path,
    monkeypatch,
) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"
    _write_consistency_build_admission_active(truth_root, day_utc=day_utc)
    _write_consistency_kill_switch(truth_root, day_utc=day_utc, canonical_active=False)
    _write_boundary(truth_root, day_utc, authorized=True)
    _write_ledger(truth_root, day_utc, authority_status="GRANTED")
    _write_consistency_control_plane(
        truth_root,
        day_utc,
        final_start_decision="READY_NOW",
        ledger_authority_status="GRANTED",
    )
    _write_consistency_status(
        truth_root,
        monkeypatch=monkeypatch,
        day_utc=day_utc,
        current_state="READY",
        submission_authorization_status="AUTHORIZED",
        submission_authorized=True,
        generated_at=datetime(2099, 1, 1, 0, 0, 0, tzinfo=UTC),
    )
    _write_complete_prerequisite(truth_root, day_utc)
    _write_startup(truth_root, day_utc, status="MISSING_DEPENDENCY")
    _write_posture(truth_root, day_utc, enabled=True)
    _write_boundary(truth_root, day_utc, authorized=False)
    _write_ledger(truth_root, day_utc, authority_status="DENIED")
    _write_startup_proof(truth_root, day_utc, ready=False)

    order: list[str] = []

    def fake_run(cmd: list[str], *, truth_root: Path) -> dict:
        tool_name = Path(cmd[1]).name
        order.append(tool_name)
        if tool_name == "run_intents_day_completeness_v1.py":
            _write_complete_prerequisite(truth_root, day_utc)
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_startup_materialization_v1.py":
            _write_startup(truth_root, day_utc, status="SUCCESS")
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_paper_trading_posture_v1.py":
            _write_posture(truth_root, day_utc, enabled=True)
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_submit_boundary_status_v1.py":
            _write_boundary(truth_root, day_utc, authorized=True)
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_paper_session_ledger_v1.py":
            _write_ledger(truth_root, day_utc, authority_status="GRANTED")
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_startup_proof_validation_v1.py":
            _write_startup_proof(truth_root, day_utc, ready=True)
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        raise AssertionError(tool_name)

    with patch.object(control_plane_module, "_run", side_effect=fake_run):
        rc = control_plane_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

    assert rc == 0
    assert order == [
        "run_intents_day_completeness_v1.py",
        "run_startup_materialization_v1.py",
        "run_paper_trading_posture_v1.py",
        "run_submit_boundary_status_v1.py",
        "run_paper_session_ledger_v1.py",
        "run_startup_proof_validation_v1.py",
    ]
    ledger_payload = json.loads(
        (truth_root / "reports" / "paper_session_ledger_v1" / day_utc / "paper_session_ledger.v1.json").read_text(
            encoding="utf-8"
        )
    )
    proof_payload = json.loads(
        (
            truth_root / "reports" / "startup_proof_validation_v1" / day_utc / "startup_proof_validation.v1.json"
        ).read_text(encoding="utf-8")
    )
    control_payload = json.loads(
        (
            truth_root / "reports" / "paper_day_control_plane_v1" / day_utc / "paper_day_control_plane.v1.json"
        ).read_text(encoding="utf-8")
    )
    assert ledger_payload["control_state"]["authority_status"] == "GRANTED"
    assert ledger_payload["evidence_freeze"]["overall_evidence_status"] == "READY"
    assert proof_payload["status"] == "STARTUP_READY"
    assert control_payload["final_start_decision"] == "READY_NOW"


def test_c2_preopen_preflight_prefers_trading_day_state_machine() -> None:
    text = (SOURCE_ROOT / "ops/run/c2_preopen_preflight_v1.sh").read_text(encoding="utf-8")
    assert "run_trading_day_state_machine_v1.py" in text
    assert "TRADING_DAY_STATE_MACHINE" in text
    assert "final_start_decision" in text
    assert "run_trading_day_state_v1.py" not in text


def test_paper_day_control_plane_attempt_history_is_immutable_and_day_file_tracks_latest(
    tmp_path: Path,
    monkeypatch,
) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"
    _write_consistency_build_admission_active(truth_root, day_utc=day_utc)
    _write_consistency_kill_switch(truth_root, day_utc=day_utc, canonical_active=False)
    _write_boundary(truth_root, day_utc, authorized=True)
    _write_ledger(truth_root, day_utc, authority_status="GRANTED")
    _write_consistency_control_plane(
        truth_root,
        day_utc,
        final_start_decision="READY_NOW",
        ledger_authority_status="GRANTED",
    )
    _write_consistency_status(
        truth_root,
        monkeypatch=monkeypatch,
        day_utc=day_utc,
        current_state="READY",
        submission_authorization_status="AUTHORIZED",
        submission_authorized=True,
        generated_at=datetime(2099, 1, 1, 0, 0, 0, tzinfo=UTC),
    )
    _write_complete_prerequisite(truth_root, day_utc)
    _write_startup(truth_root, day_utc, status="MISSING_DEPENDENCY")
    _write_posture(truth_root, day_utc, enabled=True)
    _write_boundary(truth_root, day_utc, authorized=False)
    _write_ledger(truth_root, day_utc, authority_status="DENIED")
    _write_startup_proof(truth_root, day_utc, ready=False)

    blocked_attempt_id = "ATTEMPT_BLOCKED"
    ready_attempt_id = "ATTEMPT_READY"

    def fake_run_blocked(cmd: list[str], *, truth_root: Path) -> dict:
        tool_name = Path(cmd[1]).name
        if tool_name == "run_intents_day_completeness_v1.py":
            _write_complete_prerequisite(truth_root, day_utc)
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_startup_materialization_v1.py":
            _write_startup(truth_root, day_utc, status="SUCCESS")
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_paper_trading_posture_v1.py":
            _write_posture(truth_root, day_utc, enabled=True)
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_submit_boundary_status_v1.py":
            _write_boundary(truth_root, day_utc, authorized=True)
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_paper_session_ledger_v1.py":
            return {"return_code": 1, "stdout": "", "stderr": "missing ledger output"}
        if tool_name == "run_startup_proof_validation_v1.py":
            _write_startup_proof(truth_root, day_utc, ready=False)
            return {"return_code": 2, "stdout": "{}", "stderr": ""}
        raise AssertionError(tool_name)

    with patch.object(control_plane_module, "build_attempt_id_v1", return_value=blocked_attempt_id), patch.object(
        control_plane_module, "_run", side_effect=fake_run_blocked
    ):
        rc = control_plane_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

    assert rc == 2
    blocked_attempt_path = resolve_paper_day_control_plane_attempt_path(
        truth_root=truth_root,
        day_utc=day_utc,
        attempt_id=blocked_attempt_id,
    )
    assert blocked_attempt_path.exists()
    blocked_bytes = blocked_attempt_path.read_text(encoding="utf-8")
    assert '"final_start_decision":"BLOCKED_VALID"' in blocked_bytes

    def fake_run_ready(cmd: list[str], *, truth_root: Path) -> dict:
        tool_name = Path(cmd[1]).name
        if tool_name == "run_intents_day_completeness_v1.py":
            _write_complete_prerequisite(truth_root, day_utc)
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_startup_materialization_v1.py":
            _write_startup(truth_root, day_utc, status="SUCCESS")
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_paper_trading_posture_v1.py":
            _write_posture(truth_root, day_utc, enabled=True)
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_submit_boundary_status_v1.py":
            _write_boundary(truth_root, day_utc, authorized=True)
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_paper_session_ledger_v1.py":
            _write_ledger(truth_root, day_utc, authority_status="GRANTED")
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_startup_proof_validation_v1.py":
            _write_startup_proof(truth_root, day_utc, ready=True)
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        raise AssertionError(tool_name)

    with patch.object(control_plane_module, "build_attempt_id_v1", return_value=ready_attempt_id), patch.object(
        control_plane_module, "_run", side_effect=fake_run_ready
    ):
        rc = control_plane_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

    assert rc in {0, 3}
    ready_attempt_path = resolve_paper_day_control_plane_attempt_path(
        truth_root=truth_root,
        day_utc=day_utc,
        attempt_id=ready_attempt_id,
    )
    assert ready_attempt_path.exists()
    assert blocked_attempt_path.read_text(encoding="utf-8") == blocked_bytes

    current_path = truth_root / "reports" / "paper_day_control_plane_v1" / day_utc / "paper_day_control_plane.v1.json"
    assert current_path.read_text(encoding="utf-8") == ready_attempt_path.read_text(encoding="utf-8")
    assert current_path.read_text(encoding="utf-8") != blocked_bytes
