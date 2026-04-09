from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_paper_session_ledger_v1 as ledger_tool
from constellation_2.common.paper_session_fact_plane_v1 import canonical_paper_session_id_v1
from constellation_2.common.paper_session_ledger_v1 import (
    PaperSessionLedgerV1,
    build_paper_session_ledger_v1,
    validate_transition_history_v1,
    write_paper_session_ledger_v1,
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


def _write_startup(
    truth_root: Path,
    day_utc: str,
    *,
    status: str = "SUCCESS",
    freshness: str = "CURRENT",
    linkage: str = "LINKED",
) -> None:
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
            "freshness_verdict": freshness,
            "linkage_verdict": linkage,
            "path_resolution_evidence": {
                "phasec_root": "/tmp/phasec",
                "latest_active_attempt_path": "/tmp/latest_active_attempt.v1.json",
            },
            "producer_run_id": f"startup_materialization_v1:{day_utc}",
            "phasec_materializer_result": {"returncode": 0, "stdout": "", "stderr": ""},
        },
    )


def _write_posture(truth_root: Path, day_utc: str, *, enabled: bool = True) -> None:
    status = "ENABLED" if enabled else "DISABLED"
    codes = [] if enabled else ["MARKET_CALENDAR_NON_TRADING_SESSION"]
    _write_json(
        truth_root / "reports" / "paper_trading_posture_v1" / day_utc / "paper_trading_posture.v1.json",
        {
            "schema_id": "paper_trading_posture",
            "schema_version": "v1",
            "authority_scope": "NON_AUTHORITY_FACT",
            "day_utc": day_utc,
            "session_id": canonical_paper_session_id_v1(day_utc),
            "system_ready": enabled,
            "posture_status": status,
            "posture_class": "PAPER_READY_ACTIVE" if enabled else "PAPER_READY_NO_OP",
            "blocking_family": "NONE" if enabled else "EXPECTED_NO_OP",
            "expected_no_op_today": not enabled,
            "policy_reasons": ["MARKET_CALENDAR_TRADING_SESSION"] if enabled else codes,
            "blocking_codes": codes,
            "blocking_reason_codes": codes,
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
    status = "AUTHORIZED" if authorized else "DENIED"
    codes = [] if authorized else ["SUBMIT_BOUNDARY_TRADE_SUBMIT_READINESS_DENIED"]
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
            "boundary_status": status,
            "required_boundary_checks": [check],
            "failed_checks": [] if authorized else [check],
            "blocking_codes": codes,
            "producer": _producer(),
            "produced_at_utc": f"{day_utc}T00:02:00Z",
            "freshness_verdict": "CURRENT",
            "linkage_verdict": "LINKED",
            "paper_account": "DUO847203",
        },
    )


def _write_rollup(truth_root: Path, day_utc: str, *, status: str = "PASS") -> None:
    _write_json(
        truth_root / "reports" / "sleeve_rollup_v1" / day_utc / "sleeve_rollup.v1.json",
        {
            "schema_id": "C2_SLEEVE_ROLLUP_V1",
            "schema_version": "v1",
            "day_utc": day_utc,
            "input_day_utc": day_utc,
            "produced_utc": f"{day_utc}T00:03:00Z",
            "status": status,
            "registry_path": "/tmp/C2_SLEEVE_REGISTRY_V1.json",
            "session_id": canonical_paper_session_id_v1(day_utc),
            "paper_session_ledger_path": "/tmp/paper_session_ledger.v1.json",
            "ledger_id": "paper_session_ledger:seed",
            "authority_status": "GRANTED" if status == "PASS" else "DENIED",
            "sleeves": [
                {
                    "sleeve_id": "PRIMARY",
                    "enabled": True,
                    "status": "PASS" if status == "PASS" else "FAIL",
                    "mode": "PAPER",
                    "ib_account": "DUO847203",
                    "truth_root": "/tmp/truth_sleeves/PRIMARY/PAPER",
                    "orchestrator_rc": 0 if status == "PASS" else 2,
                    "verdict_status": status,
                    "verdict_reason_codes": [] if status == "PASS" else ["REQUIRED_FAILURE"],
                    "verdict_safety_breaches": [],
                    "verdict_pointer_seq": 1,
                    "verdict_pointer_index_path": "/tmp/pointer.jsonl",
                    "verdict_points_to": "/tmp/orchestrator_run_verdict.v2.json",
                    "verdict_points_to_sha256": "d" * 64,
                    "cmd": "python3 fake.py",
                }
            ],
            "producer": _producer(),
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


def _build_manual_ledger(*, day_utc: str, authority_status: str, finalization_status: str, evidence_digest: str) -> object:
    fact_rows = [
        _minimal_fact_row(day_utc, "paper_trading_posture_v1"),
        _minimal_fact_row(day_utc, "startup_materialization_v1"),
        _minimal_fact_row(day_utc, "submit_boundary_status_v1"),
        _minimal_fact_row(day_utc, "sleeve_rollup_v1"),
    ]
    return build_paper_session_ledger_v1(
        day_utc=day_utc,
        session_id=canonical_paper_session_id_v1(day_utc),
        evaluated_at_utc=f"{day_utc}T00:00:00Z",
        provenance=_producer(),
        fact_refs=fact_rows,
        evidence_freeze={
            "evidence_digest": evidence_digest,
            "overall_evidence_status": "READY",
            "blocking_codes": [],
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
            "finalization_status": finalization_status,
        },
        post_submit_lifecycle={
            "lineage_status": "BOUND" if authority_status == "GRANTED" else "GAP",
            "latest_authoritative_lineage_ref": "/tmp/latest_pointer.v1.json" if authority_status == "GRANTED" else "",
            "latest_authoritative_lineage_sha256": "e" * 64 if authority_status == "GRANTED" else "",
            "execution_evidence_refs": ["/tmp/sleeve_rollup.v1.json"] if authority_status == "GRANTED" else [],
            "reconciliation_refs": ["/tmp/execution_reconciliation.v1.json"] if authority_status == "GRANTED" else [],
            "gap_codes": [] if authority_status == "GRANTED" else ["POST_SUBMIT_GAP"],
        },
        operator_summary={
            "authority_scope": "DERIVED_ONLY_VIEW",
            "ledger_ref": "/tmp/paper_session_ledger.v1.json",
            "summary_state": "AUTHORIZED_TO_PROCEED" if authority_status == "GRANTED" else "DENIED",
            "authority_status": authority_status,
            "submission_authorized": authority_status == "GRANTED",
            "non_authority_notice": "Derived only.",
            "blocking_codes": [] if authority_status == "GRANTED" else ["BLOCKED"],
        },
    )


def test_ledger_grants_authority_for_valid_pre_submit_facts(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"
    _write_startup(truth_root, day_utc)
    _write_posture(truth_root, day_utc, enabled=True)
    _write_boundary(truth_root, day_utc, authorized=True)

    with patch.object(ledger_tool, "now_utc_iso_v1", return_value="2026-04-08T00:04:00Z"):
        rc = ledger_tool.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

    assert rc == 0
    payload = json.loads(
        (truth_root / "reports" / "paper_session_ledger_v1" / day_utc / "paper_session_ledger.v1.json").read_text(encoding="utf-8")
    )
    assert payload["evidence_freeze"]["overall_evidence_status"] == "READY"
    assert payload["control_state"]["authority_status"] == "GRANTED"
    assert payload["control_state"]["submission_authorized"] is True
    assert payload["control_state"]["system_ready"] is True
    assert payload["submit_lifecycle"]["submit_attempt_status"] == "NOT_OBSERVED_AT_EVALUATION"
    assert payload["post_submit_lifecycle"]["lineage_status"] == "NOT_OBSERVED_AT_EVALUATION"


def test_ledger_evidence_denies_when_required_input_is_unlinked(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"
    _write_startup(truth_root, day_utc, linkage="UNLINKED")
    _write_posture(truth_root, day_utc, enabled=True)
    _write_boundary(truth_root, day_utc, authorized=True)

    rc = ledger_tool.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
    payload = json.loads(
        (truth_root / "reports" / "paper_session_ledger_v1" / day_utc / "paper_session_ledger.v1.json").read_text(encoding="utf-8")
    )
    assert rc == 2
    assert payload["evidence_freeze"]["overall_evidence_status"] == "DENY"
    assert payload["control_state"]["authority_status"] == "DENIED"
    assert "LEDGER_EVIDENCE_LINKAGE_INVALID:startup_materialization_v1" in payload["evidence_freeze"]["blocking_codes"]


def test_ledger_denies_when_submit_boundary_is_denied(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"
    _write_startup(truth_root, day_utc)
    _write_posture(truth_root, day_utc, enabled=True)
    _write_boundary(truth_root, day_utc, authorized=False)

    rc = ledger_tool.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
    payload = json.loads(
        (truth_root / "reports" / "paper_session_ledger_v1" / day_utc / "paper_session_ledger.v1.json").read_text(encoding="utf-8")
    )
    assert rc == 2
    assert payload["control_state"]["authority_status"] == "DENIED"
    assert "SUBMIT_BOUNDARY_TRADE_SUBMIT_READINESS_DENIED" in payload["control_state"]["blocking_codes"]


def test_ledger_digest_is_deterministic_for_fixed_inputs(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"
    _write_startup(truth_root, day_utc)
    _write_posture(truth_root, day_utc, enabled=True)
    _write_boundary(truth_root, day_utc, authorized=True)

    with patch.object(ledger_tool, "now_utc_iso_v1", return_value="2026-04-08T00:04:00Z"):
        ledger_tool.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
    first = json.loads(
        (truth_root / "reports" / "paper_session_ledger_v1" / day_utc / "paper_session_ledger.v1.json").read_text(encoding="utf-8")
    )
    with patch.object(ledger_tool, "now_utc_iso_v1", return_value="2026-04-08T00:04:00Z"):
        ledger_tool.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
    second = json.loads(
        (truth_root / "reports" / "paper_session_ledger_v1" / day_utc / "paper_session_ledger.v1.json").read_text(encoding="utf-8")
    )
    assert first["evidence_freeze"]["evidence_digest"] == second["evidence_freeze"]["evidence_digest"]
    assert first["ledger_id"] == second["ledger_id"]


def test_ledger_illegal_transition_rejected() -> None:
    with pytest.raises(ValueError, match="PAPER_SESSION_LEDGER_ILLEGAL_TRANSITION"):
        validate_transition_history_v1(
            [
                {
                    "from_state": "SESSION_CREATED",
                    "to_state": "AUTHORITY_GRANTED",
                    "transition_at_utc": "2026-04-08T00:00:00Z",
                    "transition_reason_code": "BAD",
                    "evidence_reference": "bad",
                }
            ]
        )


def test_ledger_write_is_immutable_for_conflicting_evidence_rewrite_when_finalized(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    ledger_a = _build_manual_ledger(
        day_utc="2026-04-08",
        authority_status="GRANTED",
        finalization_status="FINALIZED",
        evidence_digest="f" * 64,
    )
    write_paper_session_ledger_v1(truth_root=truth_root, ledger=ledger_a)
    ledger_b = _build_manual_ledger(
        day_utc="2026-04-08",
        authority_status="DENIED",
        finalization_status="OPEN",
        evidence_digest="e" * 64,
    )
    with pytest.raises(ValueError, match="PAPER_SESSION_LEDGER_FINALIZED_IMMUTABLE_MISMATCH"):
        write_paper_session_ledger_v1(truth_root=truth_root, ledger=ledger_b)


def test_ledger_write_allows_nonfinalized_same_day_evidence_refresh(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    ledger_a = _build_manual_ledger(
        day_utc="2026-04-08",
        authority_status="DENIED",
        finalization_status="OPEN",
        evidence_digest="f" * 64,
    )
    write_paper_session_ledger_v1(truth_root=truth_root, ledger=ledger_a)
    ledger_b = _build_manual_ledger(
        day_utc="2026-04-08",
        authority_status="GRANTED",
        finalization_status="OPEN",
        evidence_digest="e" * 64,
    )
    refreshed = PaperSessionLedgerV1.from_dict(
        {
            **ledger_b.to_dict(),
            "evaluated_at_utc": "2026-04-08T00:01:00Z",
        }
    )
    write_paper_session_ledger_v1(truth_root=truth_root, ledger=refreshed)
    payload = json.loads(
        (truth_root / "reports" / "paper_session_ledger_v1" / "2026-04-08" / "paper_session_ledger.v1.json").read_text(
            encoding="utf-8"
        )
    )
    assert payload["evidence_freeze"]["evidence_digest"] == "e" * 64
    assert payload["control_state"]["authority_status"] == "GRANTED"


def test_ledger_write_is_noop_for_equivalent_same_day_refresh(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    ledger_a = _build_manual_ledger(
        day_utc="2026-04-08",
        authority_status="DENIED",
        finalization_status="OPEN",
        evidence_digest="f" * 64,
    )
    write_paper_session_ledger_v1(truth_root=truth_root, ledger=ledger_a)
    ledger_path = truth_root / "reports" / "paper_session_ledger_v1" / "2026-04-08" / "paper_session_ledger.v1.json"
    first = ledger_path.read_text(encoding="utf-8")
    ledger_b = PaperSessionLedgerV1.from_dict(
        {
            **ledger_a.to_dict(),
            "evaluated_at_utc": "2026-04-08T00:01:00Z",
        }
    )
    write_paper_session_ledger_v1(truth_root=truth_root, ledger=ledger_b)
    second = ledger_path.read_text(encoding="utf-8")
    assert first == second


def test_ledger_binds_post_submit_lineage_when_rollup_and_lineage_exist(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"
    _write_startup(truth_root, day_utc)
    _write_posture(truth_root, day_utc, enabled=True)
    _write_boundary(truth_root, day_utc, authorized=True)
    _write_rollup(truth_root, day_utc, status="PASS")
    submissions_dir = truth_root / "execution_evidence_v1" / "submissions" / day_utc
    submissions_dir.mkdir(parents=True, exist_ok=True)
    (submissions_dir / "submission.v1.json").write_text("{}\n", encoding="utf-8")
    latest_pointer = truth_root / "execution_evidence_v1" / "latest_pointer.v1.json"
    _write_json(latest_pointer, {"day_utc": day_utc, "points_to": str(submissions_dir / "submission.v1.json")})
    _write_json(
        truth_root / "reports" / "execution_reconciliation_v1" / day_utc / "execution_reconciliation.v1.json",
        {"status": "OK"},
    )

    rc = ledger_tool.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
    payload = json.loads(
        (truth_root / "reports" / "paper_session_ledger_v1" / day_utc / "paper_session_ledger.v1.json").read_text(encoding="utf-8")
    )
    assert rc == 0
    assert payload["submit_lifecycle"]["submit_attempt_status"] == "ATTEMPTED"
    assert payload["post_submit_lifecycle"]["lineage_status"] == "BOUND"
    assert payload["post_submit_lifecycle"]["gap_codes"] == []
