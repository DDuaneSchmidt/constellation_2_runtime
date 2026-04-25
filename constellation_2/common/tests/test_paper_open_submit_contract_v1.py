from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import constellation_2.common.paper_day_orchestrator_pipeline_v1 as orchestrator_pipeline
from constellation_2.common.paper_session_fact_plane_v1 import canonical_paper_session_id_v1
from constellation_2.common.paper_session_ledger_v1 import (
    assert_paper_session_ledger_granted_v1,
    assert_paper_session_ledger_open_ready_v1,
    build_paper_session_ledger_v1,
    write_paper_session_ledger_v1,
)


DAY = "2026-04-22"


def _producer() -> dict:
    return {
        "repo": "constellation",
        "module": "test_paper_open_submit_contract_v1.py",
        "git_sha": "a" * 40,
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


def _write_ledger(*, truth_root: Path, submission_authorized: bool) -> Path:
    fact_rows = [
        _minimal_fact_row(DAY, "paper_trading_posture_v1"),
        _minimal_fact_row(DAY, "startup_materialization_v1"),
        _minimal_fact_row(DAY, "submit_boundary_status_v1"),
        _minimal_fact_row(DAY, "sleeve_rollup_v1"),
    ]
    ledger = build_paper_session_ledger_v1(
        day_utc=DAY,
        session_id=canonical_paper_session_id_v1(DAY),
        evaluated_at_utc=f"{DAY}T00:04:00Z",
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
        submission_authorized=submission_authorized,
        control_blocking_codes=[] if submission_authorized else ["SUBMISSION_BLOCKED"],
        submit_lifecycle={
            "submit_attempt_status": "SKIPPED",
            "submit_attempted": False,
            "submit_result_status": "PASS" if submission_authorized else "NOT_AUTHORIZED",
            "reason_codes": [] if submission_authorized else ["SUBMISSION_BLOCKED"],
            "submit_evidence_refs": [],
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
            "submission_authorized": submission_authorized,
            "non_authority_notice": "Derived only.",
            "blocking_codes": [] if submission_authorized else ["SUBMISSION_BLOCKED"],
        },
    )
    return write_paper_session_ledger_v1(truth_root=truth_root, ledger=ledger)


def test_paper_open_assert_allows_submission_not_authorized(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    ledger_path = _write_ledger(truth_root=truth_root, submission_authorized=False)

    ledger = assert_paper_session_ledger_open_ready_v1(path=ledger_path, day_utc=DAY)

    assert ledger.control_state["authority_status"] == "GRANTED"
    assert ledger.control_state["system_ready"] is True
    assert ledger.control_state["submission_authorized"] is False


def test_submit_assert_remains_fail_closed_when_submission_not_authorized(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    ledger_path = _write_ledger(truth_root=truth_root, submission_authorized=False)

    with pytest.raises(SystemExit, match="PAPER_SESSION_LEDGER_NOT_SUBMIT_READY"):
        assert_paper_session_ledger_granted_v1(path=ledger_path, day_utc=DAY)


def test_orchestrator_pipeline_uses_open_contract_for_paper_and_strict_for_live(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    truth_root.mkdir(parents=True, exist_ok=True)
    ledger_path = _write_ledger(truth_root=truth_root, submission_authorized=False)
    orch = orchestrator_pipeline._module()
    sentinel = SimpleNamespace(ledger_id="paper_session_ledger:test", session_id="paper_session:test")

    def _run(mode: str) -> tuple[object, int, int]:
        args = SimpleNamespace(
            pipeline_mode="normal",
            truth_root=str(truth_root),
            day_utc=DAY,
            mode=mode,
            input_day_utc="",
            symbol="SPY",
            ib_account="DUO847203",
            produced_utc=f"{DAY}T00:00:00Z",
            paper_session_ledger_path=str(ledger_path),
        )
        with (
            patch.object(orch, "_require_repo_root_cwd", lambda: None),
            patch.object(orch, "_git_sha", return_value="b" * 40, create=True),
            patch.object(orch, "_resolve_expected_sleeve_account", return_value=("PRIMARY", "DUO847203")),
            patch.object(orch.subprocess, "check_output", return_value='{"attempt_id":"attempt-1","attempt_seq":1}'),
            patch.object(orch, "_resolve_session_state", return_value={"session_state": "TRADING_SESSION"}),
            patch.object(orchestrator_pipeline, "assert_paper_session_ledger_open_ready_v1", return_value=sentinel) as open_assert,
            patch.object(orchestrator_pipeline, "assert_paper_session_ledger_granted_v1", return_value=sentinel) as strict_assert,
        ):
            resolved = orchestrator_pipeline.resolve_paper_day_orchestrator_pipeline_inputs_v1(args)
        return resolved["ledger"], open_assert.call_count, strict_assert.call_count

    paper_ledger, paper_open_calls, paper_strict_calls = _run("PAPER")
    live_ledger, live_open_calls, live_strict_calls = _run("LIVE")

    assert paper_ledger is sentinel
    assert paper_open_calls == 1
    assert paper_strict_calls == 0

    assert live_ledger is sentinel
    assert live_open_calls == 0
    assert live_strict_calls == 1


def test_pipeline_skips_sleeve_edge_when_core2_summary_missing_and_reaches_governed_submit(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    truth_root.mkdir(parents=True, exist_ok=True)
    orch = orchestrator_pipeline._module()
    sentinel = SimpleNamespace(ledger_id="paper_session_ledger:test", session_id="paper_session:test")

    resolved = {
        "pipeline_mode": "normal",
        "truth_root": truth_root,
        "day": DAY,
        "input_day": DAY,
        "mode": "PAPER",
        "symbol": "SPY",
        "ib_account": "DUO847203",
        "produced_utc": f"{DAY}T00:00:00Z",
        "git_sha": "b" * 7,
        "attempt_id": "attempt-1",
        "attempt_seq": 1,
        "session_info": {"session_state": "TRADING_SESSION"},
        "stage_env": {},
        "cfg_hash": "0" * 64,
        "ledger": sentinel,
        "paper_session_ledger_path": truth_root / "reports" / "paper_session_ledger_v1" / DAY / "paper_session_ledger.v1.json",
        "governing_refs": [],
    }

    stages = [
        orch.StageDef(
            stage_id=orch.SLEEVE_EDGE_PUBLICATION_STAGE_ID,
            cmd=["python3", "ops/tools/run_sleeve_edge_measurement_v1.py"],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=True,
            skip_if_exists_paths=[],
        ),
        orch.StageDef(
            stage_id="A7A_GOVERNED_SUBMIT_V5",
            cmd=["python3", "constellation_2/phaseD/tools/c2_submit_paper_v5.py"],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=True,
            skip_if_exists_paths=[],
        ),
    ]

    with (
        patch.object(orch, "resolve_governed_paper_execution_roots", return_value=SimpleNamespace(execution_root_path=truth_root)),
        patch.object(orch, "_run_structural_pre_activity_producers", return_value=None),
        patch.object(orch, "_detect_activity", return_value={"activity": True}),
        patch.object(orch, "_build_stage_defs", return_value=stages),
        patch.object(orch, "_validate_sleeve_edge_publication_order", return_value=None),
        patch.object(orch, "_sleeve_edge_core2_summary_ready", return_value=(False, "SKIP_SLEEVE_EDGE_CORE2_SUMMARY_MISSING")),
        patch.object(orch, "_run_governed_submit_stage", return_value=(0, ["GOV_SUBMIT_SUBMISSION_COUNT=1"])) as run_submit,
    ):
        evaluated = orchestrator_pipeline.evaluate_paper_day_orchestrator_pipeline_v1(resolved)

    stage_rows = {row["stage_id"]: row for row in evaluated["stage_results"]}
    assert stage_rows[orch.SLEEVE_EDGE_PUBLICATION_STAGE_ID]["status"] == "SKIP"
    assert stage_rows[orch.SLEEVE_EDGE_PUBLICATION_STAGE_ID]["reason_codes"] == ["SKIP_SLEEVE_EDGE_CORE2_SUMMARY_MISSING"]
    assert stage_rows["A7A_GOVERNED_SUBMIT_V5"]["status"] == "OK"
    assert run_submit.call_count == 1


def test_pipeline_execution_stream_snapshot_runs_when_existing_stream_outputs_do_not_cover_submissions(
    tmp_path: Path,
) -> None:
    truth_root = tmp_path / "truth"
    truth_root.mkdir(parents=True, exist_ok=True)
    orch = orchestrator_pipeline._module()
    sentinel = SimpleNamespace(ledger_id="paper_session_ledger:test", session_id="paper_session:test")
    stream_glob = str((truth_root / "execution_stream_v1" / DAY / "*.execution_event_stream_record.v1.json").resolve())
    stage = orch.StageDef(
        stage_id="B0_EXECUTION_STREAM_SNAPSHOT_V1",
        cmd=["python3", "ops/tools/run_execution_stream_snapshot_day_v1.py"],
        required_for_paper=True,
        required_for_live=True,
        required_if_activity=True,
        blocking=False,
        skip_if_exists_paths=[stream_glob],
    )
    resolved = {
        "pipeline_mode": "normal",
        "truth_root": truth_root,
        "day": DAY,
        "input_day": DAY,
        "mode": "PAPER",
        "symbol": "SPY",
        "ib_account": "DUO847203",
        "produced_utc": f"{DAY}T00:00:00Z",
        "git_sha": "b" * 7,
        "attempt_id": "attempt-1",
        "attempt_seq": 1,
        "session_info": {"session_state": "TRADING_SESSION"},
        "stage_env": {},
        "cfg_hash": "0" * 64,
        "ledger": sentinel,
        "paper_session_ledger_path": truth_root / "reports" / "paper_session_ledger_v1" / DAY / "paper_session_ledger.v1.json",
        "governing_refs": [],
    }

    with (
        patch.object(orch, "resolve_governed_paper_execution_roots", return_value=SimpleNamespace(execution_root_path=truth_root)),
        patch.object(orch, "_run_structural_pre_activity_producers", return_value=None),
        patch.object(orch, "_detect_activity", return_value={"activity": True}),
        patch.object(orch, "_build_stage_defs", return_value=[stage]),
        patch.object(orch, "_validate_sleeve_edge_publication_order", return_value=None),
        patch.object(orch, "_count_submission_dirs", return_value=1),
        patch.object(orch, "_path_exists", return_value=True),
        patch.object(orch, "_execution_stream_snapshot_skip_safe", return_value=False),
        patch.object(orch, "_run_cmd", return_value=0) as run_cmd,
    ):
        evaluated = orchestrator_pipeline.evaluate_paper_day_orchestrator_pipeline_v1(resolved)

    stage_row = evaluated["stage_results"][0]
    assert stage_row["stage_id"] == "B0_EXECUTION_STREAM_SNAPSHOT_V1"
    assert stage_row["status"] == "OK"
    assert stage_row["executed"] is True
    run_cmd.assert_called_once_with("B0_EXECUTION_STREAM_SNAPSHOT_V1", stage.cmd, env={})


def test_pipeline_execution_stream_snapshot_skips_when_stream_outputs_cover_submissions(
    tmp_path: Path,
) -> None:
    truth_root = tmp_path / "truth"
    truth_root.mkdir(parents=True, exist_ok=True)
    orch = orchestrator_pipeline._module()
    sentinel = SimpleNamespace(ledger_id="paper_session_ledger:test", session_id="paper_session:test")
    stream_glob = str((truth_root / "execution_stream_v1" / DAY / "*.execution_event_stream_record.v1.json").resolve())
    stage = orch.StageDef(
        stage_id="B0_EXECUTION_STREAM_SNAPSHOT_V1",
        cmd=["python3", "ops/tools/run_execution_stream_snapshot_day_v1.py"],
        required_for_paper=True,
        required_for_live=True,
        required_if_activity=True,
        blocking=False,
        skip_if_exists_paths=[stream_glob],
    )
    resolved = {
        "pipeline_mode": "normal",
        "truth_root": truth_root,
        "day": DAY,
        "input_day": DAY,
        "mode": "PAPER",
        "symbol": "SPY",
        "ib_account": "DUO847203",
        "produced_utc": f"{DAY}T00:00:00Z",
        "git_sha": "b" * 7,
        "attempt_id": "attempt-1",
        "attempt_seq": 1,
        "session_info": {"session_state": "TRADING_SESSION"},
        "stage_env": {},
        "cfg_hash": "0" * 64,
        "ledger": sentinel,
        "paper_session_ledger_path": truth_root / "reports" / "paper_session_ledger_v1" / DAY / "paper_session_ledger.v1.json",
        "governing_refs": [],
    }

    with (
        patch.object(orch, "resolve_governed_paper_execution_roots", return_value=SimpleNamespace(execution_root_path=truth_root)),
        patch.object(orch, "_run_structural_pre_activity_producers", return_value=None),
        patch.object(orch, "_detect_activity", return_value={"activity": True}),
        patch.object(orch, "_build_stage_defs", return_value=[stage]),
        patch.object(orch, "_validate_sleeve_edge_publication_order", return_value=None),
        patch.object(orch, "_count_submission_dirs", return_value=1),
        patch.object(orch, "_path_exists", return_value=True),
        patch.object(orch, "_execution_stream_snapshot_skip_safe", return_value=True),
        patch.object(orch, "_run_cmd", return_value=0) as run_cmd,
    ):
        evaluated = orchestrator_pipeline.evaluate_paper_day_orchestrator_pipeline_v1(resolved)

    stage_row = evaluated["stage_results"][0]
    assert stage_row["stage_id"] == "B0_EXECUTION_STREAM_SNAPSHOT_V1"
    assert stage_row["status"] == "SKIP"
    assert stage_row["reason_codes"] == ["SKIP_EXISTING_OUTPUTS_AVOID_REWRITE"]
    run_cmd.assert_not_called()


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def test_governed_submit_idempotency_ignores_veto_only_history(tmp_path: Path) -> None:
    orch = orchestrator_pipeline._module()
    truth_root = tmp_path / "truth_sleeve"
    day = "2026-04-23"
    intent_sha = "4d246a5a31d6c40d74d5af509c549a4daa950b540f293cdc9de8390051e6b184"

    # Historical veto-only submission dir for the same intent must not trip
    # idempotent already-submitted logic.
    veto_dir = (
        truth_root
        / "execution_evidence_v1"
        / "submissions"
        / day
        / "old-veto-only"
    )
    _write_json(
        veto_dir / "equity_order_plan.v1.json",
        {"schema_id": "equity_order_plan", "schema_version": "v1", "intent_sha256": intent_sha},
    )
    _write_json(
        veto_dir / "veto_record.v1.json",
        {"schema_id": "veto_record", "schema_version": "v1", "reason_code": "C2_SUBMIT_AUTHZ_NOT_AUTHORIZED"},
    )

    assert (
        orch._has_submission_evidence_for_intent_sha(  # noqa: SLF001
            truth_root=truth_root,
            day=day,
            intent_sha=intent_sha,
        )
        is False
    )


def test_governed_submit_idempotency_requires_broker_submission_record(tmp_path: Path) -> None:
    orch = orchestrator_pipeline._module()
    truth_root = tmp_path / "truth_sleeve"
    day = "2026-04-23"
    intent_sha = "4d246a5a31d6c40d74d5af509c549a4daa950b540f293cdc9de8390051e6b184"
    subdir = truth_root / "execution_evidence_v1" / "submissions" / day / "attempted"

    _write_json(
        subdir / "equity_order_plan.v1.json",
        {"schema_id": "equity_order_plan", "schema_version": "v1", "intent_sha256": intent_sha},
    )
    _write_json(
        subdir / "broker_submission_record.v2.json",
        {"schema_id": "broker_submission_record", "schema_version": "v2", "status": "SUBMITTED"},
    )

    assert (
        orch._has_submission_evidence_for_intent_sha(  # noqa: SLF001
            truth_root=truth_root,
            day=day,
            intent_sha=intent_sha,
        )
        is True
    )


def test_governed_submit_idempotency_non_dry_run_ignores_records_without_broker_ids(tmp_path: Path) -> None:
    orch = orchestrator_pipeline._module()
    truth_root = tmp_path / "truth_sleeve"
    day = "2026-04-23"
    intent_sha = "4d246a5a31d6c40d74d5af509c549a4daa950b540f293cdc9de8390051e6b184"
    subdir = truth_root / "execution_evidence_v1" / "submissions" / day / "attempted"

    _write_json(
        subdir / "equity_order_plan.v1.json",
        {"schema_id": "equity_order_plan", "schema_version": "v1", "intent_sha256": intent_sha},
    )
    _write_json(
        subdir / "broker_submission_record.v2.json",
        {
            "schema_id": "broker_submission_record",
            "schema_version": "v2",
            "status": "SUBMITTED",
            "broker_ids": {"order_id": None, "perm_id": None},
        },
    )

    assert (
        orch._has_submission_evidence_for_intent_sha(  # noqa: SLF001
            truth_root=truth_root,
            day=day,
            intent_sha=intent_sha,
            require_real_broker_ids=True,
        )
        is False
    )


def test_governed_submit_idempotency_non_dry_run_accepts_records_with_broker_ids(tmp_path: Path) -> None:
    orch = orchestrator_pipeline._module()
    truth_root = tmp_path / "truth_sleeve"
    day = "2026-04-23"
    intent_sha = "4d246a5a31d6c40d74d5af509c549a4daa950b540f293cdc9de8390051e6b184"
    subdir = truth_root / "execution_evidence_v1" / "submissions" / day / "attempted"

    _write_json(
        subdir / "equity_order_plan.v1.json",
        {"schema_id": "equity_order_plan", "schema_version": "v1", "intent_sha256": intent_sha},
    )
    _write_json(
        subdir / "broker_submission_record.v2.json",
        {
            "schema_id": "broker_submission_record",
            "schema_version": "v2",
            "status": "SUBMITTED",
            "broker_ids": {"order_id": 101, "perm_id": None},
        },
    )

    assert (
        orch._has_submission_evidence_for_intent_sha(  # noqa: SLF001
            truth_root=truth_root,
            day=day,
            intent_sha=intent_sha,
            require_real_broker_ids=True,
        )
        is True
    )


def test_governed_submit_attempts_when_authorized_after_veto_only_history(tmp_path: Path) -> None:
    orch = orchestrator_pipeline._module()
    truth_root = tmp_path / "truth_sleeve"
    day = "2026-04-23"
    intent_sha = "4d246a5a31d6c40d74d5af509c549a4daa950b540f293cdc9de8390051e6b184"
    ib_account = "DUO847203"

    # Authorization now positive.
    _write_json(
        truth_root / "engine_activity_v1" / "authorization_v1" / day / f"{intent_sha}.authorization.v1.json",
        {
            "schema_id": "authorization",
            "schema_version": "v1",
            "status": "AUTHORIZED",
            "authorization": {"decision": "AUTHORIZED", "authorized_quantity": 1},
        },
    )

    # Keep prior veto-only history for same intent.
    veto_dir = (
        truth_root
        / "execution_evidence_v1"
        / "submissions"
        / day
        / "old-veto-only"
    )
    _write_json(
        veto_dir / "equity_order_plan.v1.json",
        {"schema_id": "equity_order_plan", "schema_version": "v1", "intent_sha256": intent_sha},
    )
    _write_json(
        veto_dir / "veto_record.v1.json",
        {"schema_id": "veto_record", "schema_version": "v1", "reason_code": "C2_SUBMIT_AUTHZ_NOT_AUTHORIZED"},
    )

    identity_dir = tmp_path / "identity" / intent_sha
    _write_json(
        identity_dir / "equity_order_plan.v1.json",
        {
            "schema_id": "equity_order_plan",
            "schema_version": "v1",
            "intent_sha256": intent_sha,
            "source_intent_id": "c2_trend_eq_spy_2026-04-23_v1",
        },
    )
    _write_json(identity_dir / "binding_record.v2.json", {"schema_id": "binding_record", "schema_version": "v2"})
    _write_json(identity_dir / "mapping_ledger_record.v2.json", {"schema_id": "mapping_ledger_record", "schema_version": "v2"})

    with (
        patch.object(orch, "_discover_same_day_identity_dirs", return_value=[identity_dir]),
        patch.object(
            orch,
            "_ensure_governed_submit_inputs",
            return_value={
                "ok": True,
                "submission_id": "submission-new",
                "execution_package_path": tmp_path / "pkg.json",
                "submission_record_path": tmp_path / "subrec.json",
            },
        ),
        patch.object(orch, "_build_governed_submit_cmd", return_value=["python3", "noop"]),
        patch.object(orch, "_run_cmd", return_value=0) as run_cmd,
        patch.object(orch, "_count_broker_submission_records", return_value=1),
    ):
        rc, reason_codes = orch._run_governed_submit_stage(  # noqa: SLF001
            truth_root=truth_root,
            day=day,
            produced_utc=f"{day}T00:00:00Z",
            ib_account=ib_account,
            env={},
        )

    assert rc == 0
    assert "GOV_SUBMIT_SUBMISSION_COUNT=1" in reason_codes
    assert run_cmd.call_count == 1


def test_governed_submit_non_dry_run_attempts_when_only_dry_run_submission_history_exists(tmp_path: Path) -> None:
    orch = orchestrator_pipeline._module()
    truth_root = tmp_path / "truth_sleeve"
    day = "2026-04-23"
    intent_sha = "4d246a5a31d6c40d74d5af509c549a4daa950b540f293cdc9de8390051e6b184"
    ib_account = "DUO847203"

    _write_json(
        truth_root / "engine_activity_v1" / "authorization_v1" / day / f"{intent_sha}.authorization.v1.json",
        {
            "schema_id": "authorization",
            "schema_version": "v1",
            "status": "AUTHORIZED",
            "authorization": {"decision": "AUTHORIZED", "authorized_quantity": 1},
        },
    )

    prior_submit_dir = truth_root / "execution_evidence_v1" / "submissions" / day / "old-submit-no-broker-id"
    _write_json(
        prior_submit_dir / "equity_order_plan.v1.json",
        {"schema_id": "equity_order_plan", "schema_version": "v1", "intent_sha256": intent_sha},
    )
    _write_json(
        prior_submit_dir / "broker_submission_record.v2.json",
        {
            "schema_id": "broker_submission_record",
            "schema_version": "v2",
            "status": "SUBMITTED",
            "broker_ids": {"order_id": None, "perm_id": None},
        },
    )

    identity_dir = tmp_path / "identity" / intent_sha
    _write_json(
        identity_dir / "equity_order_plan.v1.json",
        {
            "schema_id": "equity_order_plan",
            "schema_version": "v1",
            "intent_sha256": intent_sha,
            "source_intent_id": "c2_trend_eq_spy_2026-04-23_v1",
        },
    )
    _write_json(identity_dir / "binding_record.v2.json", {"schema_id": "binding_record", "schema_version": "v2"})
    _write_json(identity_dir / "mapping_ledger_record.v2.json", {"schema_id": "mapping_ledger_record", "schema_version": "v2"})

    with (
        patch.object(orch, "_discover_same_day_identity_dirs", return_value=[identity_dir]),
        patch.object(
            orch,
            "_ensure_governed_submit_inputs",
            return_value={
                "ok": True,
                "submission_id": "submission-nondryrun",
                "execution_package_path": tmp_path / "pkg.json",
                "submission_record_path": tmp_path / "subrec.json",
            },
        ),
        patch.object(orch, "_build_governed_submit_cmd", return_value=["python3", "noop"]),
        patch.object(orch, "_run_cmd", return_value=0) as run_cmd,
        patch.object(orch, "_count_broker_submission_records", return_value=1),
    ):
        rc, reason_codes = orch._run_governed_submit_stage(  # noqa: SLF001
            truth_root=truth_root,
            day=day,
            produced_utc=f"{day}T00:00:00Z",
            ib_account=ib_account,
            env={"C2_GOVERNED_SUBMIT_DRY_RUN": "NO"},
        )

    assert rc == 0
    assert "GOV_SUBMIT_SUBMISSION_COUNT=1" in reason_codes
    assert run_cmd.call_count == 1
