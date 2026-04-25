from __future__ import annotations

import json
import sys
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_trading_day_control_plane_v1 as control_module
from constellation_2.common.paper_session_fact_plane_v1 import canonical_paper_session_id_v1
from constellation_2.common.paper_session_ledger_v1 import build_paper_session_ledger_v1, write_paper_session_ledger_v1


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


def _write_startup(truth_root: Path, day_utc: str) -> None:
    _write_json(
        truth_root / "reports" / "startup_materialization_v1" / day_utc / "startup_materialization.v1.json",
        {
            "schema_id": "startup_materialization",
            "schema_version": "v1",
            "authority_scope": "NON_AUTHORITY_FACT",
            "day_utc": day_utc,
            "session_id": canonical_paper_session_id_v1(day_utc),
            "status": "SUCCESS",
            "required_inputs_checked": [_fact_dep(day_utc, "intent_file:spy.json")],
            "materialized_outputs": [_fact_dep(day_utc, "phasec_materialized_output:spy:binding_record.v2.json")],
            "blocking_codes": [],
            "producer": _producer(),
            "produced_at_utc": f"{day_utc}T00:01:00Z",
            "freshness_verdict": "CURRENT",
            "linkage_verdict": "LINKED",
        },
    )


def _write_posture(truth_root: Path, day_utc: str) -> None:
    _write_json(
        truth_root / "reports" / "paper_trading_posture_v1" / day_utc / "paper_trading_posture.v1.json",
        {
            "schema_id": "paper_trading_posture",
            "schema_version": "v1",
            "authority_scope": "NON_AUTHORITY_FACT",
            "day_utc": day_utc,
            "session_id": canonical_paper_session_id_v1(day_utc),
            "system_ready": True,
            "posture_status": "ENABLED",
            "posture_class": "PAPER_READY_ACTIVE",
            "blocking_family": "NONE",
            "expected_no_op_today": False,
            "policy_reasons": ["MARKET_CALENDAR_TRADING_SESSION"],
            "blocking_codes": [],
            "blocking_reason_codes": [],
            "source_dependencies": [_fact_dep(day_utc, "market_calendar_manifest")],
            "producer": _producer(),
            "produced_at_utc": f"{day_utc}T00:02:00Z",
            "freshness_verdict": "CURRENT",
            "linkage_verdict": "LINKED",
        },
    )


def _write_boundary(truth_root: Path, day_utc: str) -> None:
    _write_json(
        truth_root / "reports" / "submit_boundary_status_v1" / day_utc / "submit_boundary_status.v1.json",
        {
            "schema_id": "submit_boundary_status",
            "schema_version": "v1",
            "authority_scope": "NON_AUTHORITY_FACT",
            "day_utc": day_utc,
            "session_id": canonical_paper_session_id_v1(day_utc),
            "submission_authorized": True,
            "boundary_status": "AUTHORIZED",
            "required_boundary_checks": [_fact_dep(day_utc, "trade_submit_readiness_c2_v1")],
            "failed_checks": [],
            "blocking_codes": [],
            "producer": _producer(),
            "produced_at_utc": f"{day_utc}T00:03:00Z",
            "freshness_verdict": "CURRENT",
            "linkage_verdict": "LINKED",
            "paper_account": "DUO847203",
        },
    )


def _write_ledger(truth_root: Path, day_utc: str) -> None:
    fact_rows = [
        _minimal_fact_row(day_utc, "paper_trading_posture_v1"),
        _minimal_fact_row(day_utc, "startup_materialization_v1"),
        _minimal_fact_row(day_utc, "submit_boundary_status_v1"),
        _minimal_fact_row(day_utc, "sleeve_rollup_v1"),
    ]
    ledger = build_paper_session_ledger_v1(
        day_utc=day_utc,
        session_id=canonical_paper_session_id_v1(day_utc),
        evaluated_at_utc=f"{day_utc}T00:04:00Z",
        provenance=_producer(),
        fact_refs=fact_rows,
        evidence_freeze={
            "evidence_digest": "b" * 64,
            "overall_evidence_status": "READY",
            "blocking_codes": [],
            "inputs": [row for row in fact_rows if row["required_for_authority"]],
        },
        authority_status="GRANTED",
        system_ready=True,
        submission_authorized=True,
        control_blocking_codes=[],
        submit_lifecycle={
            "submit_attempt_status": "ATTEMPTED",
            "submit_attempted": True,
            "submit_result_status": "PASS",
            "reason_codes": [],
            "submit_evidence_refs": ["/tmp/sleeve_rollup.v1.json"],
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
            "summary_state": "AUTHORIZED_TO_PROCEED",
            "authority_status": "GRANTED",
            "submission_authorized": True,
            "non_authority_notice": "Derived only.",
            "blocking_codes": [],
        },
    )
    write_paper_session_ledger_v1(truth_root=truth_root, ledger=ledger)


def _write_startup_proof(truth_root: Path, day_utc: str) -> None:
    _write_json(
        truth_root / "reports" / "startup_proof_validation_v1" / day_utc / "startup_proof_validation.v1.json",
        {
            "schema_id": "startup_proof_validation",
            "schema_version": "v1",
            "authority_scope": "NON_AUTHORITY_STARTUP_VALIDATION",
            "validation_scope": "BOD_CONTROL_PLANE_ONLY",
            "day_utc": day_utc,
            "session_id": canonical_paper_session_id_v1(day_utc),
            "status": "STARTUP_READY",
            "ledger_ref": str(
                truth_root / "reports" / "paper_session_ledger_v1" / day_utc / "paper_session_ledger.v1.json"
            ),
            "ledger_id": f"paper_session_ledger:{day_utc}:test",
            "ledger_authority_status": "GRANTED",
            "checks": [],
            "blocking_codes": [],
            "producer": _producer(),
            "produced_at_utc": f"{day_utc}T00:05:00Z",
        },
    )


def _paper_day_control_plane_ref(truth_root: Path, day_utc: str) -> SimpleNamespace:
    path = truth_root / "reports" / "paper_day_control_plane_v1" / day_utc / "paper_day_control_plane.v1.json"
    payload = {
        "schema_id": "paper_day_control_plane",
        "schema_version": "v1",
        "authority_scope": "SUPPORTING_DAY_CONTROL_ARTIFACT",
        "day_utc": day_utc,
        "startup_attempt_id": f"paper_day_start_attempt:{day_utc}:test",
        "control_plane_id": f"paper_day_control_plane:{day_utc}:test",
        "evaluated_at_utc": f"{day_utc}T00:06:00Z",
        "producer": _producer(),
        "prerequisite_gate": {
            "prerequisite_status": "PASS",
            "prerequisite_blocking_codes": [],
            "prerequisite_artifact_refs": [
                str(truth_root / "reports" / "intents_day_completeness_v1" / day_utc / "intents_day_completeness.v1.json")
            ],
            "first_missing_prerequisite": "",
        },
        "canonical_regeneration_results": [],
        "authority_result": {
            "ledger_path": str(truth_root / "reports" / "paper_session_ledger_v1" / day_utc / "paper_session_ledger.v1.json"),
            "ledger_id": f"paper_session_ledger:{day_utc}:test",
            "ledger_authority_status": "GRANTED",
            "ledger_evidence_status": "READY",
            "first_true_blocker_code": "",
            "first_true_blocker_artifact_path": str(
                truth_root / "reports" / "paper_session_ledger_v1" / day_utc / "paper_session_ledger.v1.json"
            ),
        },
        "startup_proof_result": {
            "startup_proof_validation_path": str(
                truth_root / "reports" / "startup_proof_validation_v1" / day_utc / "startup_proof_validation.v1.json"
            ),
            "startup_proof_validation_status": "STARTUP_READY",
            "agreement_with_ledger": True,
        },
        "final_start_decision": "READY_NOW",
        "blocking_codes": [],
        "human_readable_summary": "test",
    }
    return SimpleNamespace(path=path, payload=payload, sha256="c" * 64)


def _surface_ref(path: Path) -> SimpleNamespace:
    return SimpleNamespace(path=path, payload=json.loads(path.read_text(encoding="utf-8")), sha256="d" * 64)


def test_trading_day_control_plane_uses_nested_ledger_control_state_for_ready_now(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"

    def fake_run(cmd: list[str], *, truth_root: Path) -> dict:
        tool_name = Path(cmd[1]).name
        if tool_name == "run_intents_day_completeness_v1.py":
            _write_complete_prerequisite(truth_root, day_utc)
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_paper_day_control_plane_v1.py":
            _write_startup(truth_root, day_utc)
            _write_posture(truth_root, day_utc)
            _write_boundary(truth_root, day_utc)
            _write_ledger(truth_root, day_utc)
            _write_startup_proof(truth_root, day_utc)
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        raise AssertionError(tool_name)

    with ExitStack() as stack:
        stack.enter_context(patch.object(control_module, "_run", side_effect=fake_run))
        stack.enter_context(
            patch.object(
                control_module,
                "read_paper_day_control_plane_ref_v1",
                return_value=_paper_day_control_plane_ref(truth_root, day_utc),
            )
        )
        stack.enter_context(
            patch.object(
                control_module,
                "read_paper_session_ledger_ref_v1",
                side_effect=lambda *args, **kwargs: _surface_ref(
                    truth_root / "reports" / "paper_session_ledger_v1" / day_utc / "paper_session_ledger.v1.json"
                ),
            )
        )
        stack.enter_context(
            patch.object(
                control_module,
                "read_startup_proof_validation_ref_v1",
                side_effect=lambda *args, **kwargs: _surface_ref(
                    truth_root
                    / "reports"
                    / "startup_proof_validation_v1"
                    / day_utc
                    / "startup_proof_validation.v1.json"
                ),
            )
        )
        rc = control_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

    assert rc == 0
    payload = json.loads(
        (
            truth_root
            / "reports"
            / "trading_day_control_plane_v1"
            / day_utc
            / "trading_day_control_plane.v1.json"
        ).read_text(encoding="utf-8")
    )
    assert payload["final_start_decision"] == "READY_NOW"
    assert payload["supporting_session_authority"]["ledger_authority_status"] == "GRANTED"
    assert payload["supporting_session_authority"]["system_ready"] is True
    assert payload["supporting_session_authority"]["submission_authorized"] is True
    assert payload["startup_proof_result"]["agreement_with_supporting_authority"] is True


def test_trading_day_control_plane_ready_now_clears_stale_blockers(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"

    def fake_run(cmd: list[str], *, truth_root: Path) -> dict:
        tool_name = Path(cmd[1]).name
        if tool_name == "run_intents_day_completeness_v1.py":
            _write_complete_prerequisite(truth_root, day_utc)
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_paper_day_control_plane_v1.py":
            _write_startup(truth_root, day_utc)
            _write_posture(truth_root, day_utc)
            _write_boundary(truth_root, day_utc)
            _write_ledger(truth_root, day_utc)
            _write_startup_proof(truth_root, day_utc)
            startup_path = (
                truth_root
                / "reports"
                / "startup_proof_validation_v1"
                / day_utc
                / "startup_proof_validation.v1.json"
            )
            startup_payload = json.loads(startup_path.read_text(encoding="utf-8"))
            startup_payload["blocking_codes"] = ["HIDDEN_DEPENDENCY_DETECTED"]
            _write_json(startup_path, startup_payload)
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        raise AssertionError(tool_name)

    with ExitStack() as stack:
        stack.enter_context(patch.object(control_module, "_run", side_effect=fake_run))
        stack.enter_context(
            patch.object(
                control_module,
                "read_intents_day_completeness_ref_v1",
                side_effect=lambda *args, **kwargs: _surface_ref(
                    truth_root / "reports" / "intents_day_completeness_v1" / day_utc / "intents_day_completeness.v1.json"
                ),
            )
        )
        stack.enter_context(
            patch.object(
                control_module,
                "read_paper_day_control_plane_ref_v1",
                return_value=_paper_day_control_plane_ref(truth_root, day_utc),
            )
        )
        stack.enter_context(
            patch.object(
                control_module,
                "read_paper_session_ledger_ref_v1",
                side_effect=lambda *args, **kwargs: _surface_ref(
                    truth_root / "reports" / "paper_session_ledger_v1" / day_utc / "paper_session_ledger.v1.json"
                ),
            )
        )
        stack.enter_context(
            patch.object(
                control_module,
                "read_startup_proof_validation_ref_v1",
                side_effect=lambda *args, **kwargs: _surface_ref(
                    truth_root
                    / "reports"
                    / "startup_proof_validation_v1"
                    / day_utc
                    / "startup_proof_validation.v1.json"
                ),
            )
        )
        rc = control_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

    assert rc == 0
    payload = json.loads(
        (
            truth_root
            / "reports"
            / "trading_day_control_plane_v1"
            / day_utc
            / "trading_day_control_plane.v1.json"
        ).read_text(encoding="utf-8")
    )
    assert payload["final_start_decision"] == "READY_NOW"
    assert payload["blocking_codes"] == []
    assert payload["first_true_blocker"]["first_true_blocker_code"] == ""
