from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import ops.tools.run_operator_daily_gate_v3 as operator_gate_module
import ops.tools.run_gate_stack_verdict_v1 as gate_stack_module
import ops.tools.run_reconciliation_report_v3 as reconciliation_module
from constellation_2.common.broker_fact_spine_v1 import materialize_broker_fact_spine_v1
from constellation_2.common.constitutional_runtime_v1 import (
    FINALITY_PROVISIONAL,
    build_artifact_dependency_declaration_v1,
    build_governed_artifact_lineage_v1,
    build_machine_blocker_envelope_v1,
    get_constitutional_artifact_contract_v1,
    validate_governed_artifact_payload_v1,
)


DAY = "2026-04-13"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = "".join(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n" for row in rows)
    path.write_text(payload, encoding="utf-8")


def _governed_empty_dependency_payload(
    *,
    artifact_id: str,
    payload: dict,
    producer_module: str,
    generated_at_utc: str,
) -> dict:
    contract = get_constitutional_artifact_contract_v1(REPO_ROOT, artifact_id)
    blocker_envelope = build_machine_blocker_envelope_v1(
        closure_state="COMPLETE",
        reason_codes=list(payload.get("reason_codes") or []),
        missing_dependency_artifacts=[],
    )
    payload["blocking_codes"] = list(blocker_envelope["blocking_codes"])
    payload["closure_state"] = str(blocker_envelope["closure_state"])
    payload["first_blocker_code"] = str(blocker_envelope["first_blocker_code"])
    payload["missing_dependency_artifacts"] = list(blocker_envelope["missing_dependency_artifacts"])
    payload["constitutional_dependency_declaration"] = build_artifact_dependency_declaration_v1(
        artifact_type=artifact_id,
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id=artifact_id,
        declared_dependency_artifacts=[],
        dependency_refs=[],
    )
    payload["constitutional_lineage"] = build_governed_artifact_lineage_v1(
        artifact_type=artifact_id,
        artifact_version=str(payload.get("schema_version") or ""),
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id=artifact_id,
        producer_id=producer_module,
        generated_at_utc=generated_at_utc,
        effective_at_utc=generated_at_utc,
        finality_state=FINALITY_PROVISIONAL,
        input_artifact_refs=[],
        policy_snapshot_refs=[],
        code_version="a" * 40,
        run_id=f"{artifact_id}:{DAY}",
    )
    return payload


def _governed_identity() -> SimpleNamespace:
    return SimpleNamespace(
        authority_owner="execution_identity_binding_v1",
        sleeve_id="PRIMARY",
        environment="PAPER",
        account_id="DUO847203",
        client_id_orders=7,
        client_id_observer=179,
        host="127.0.0.1",
        port=4002,
    )


def _governed_roots(execution_root: Path) -> SimpleNamespace:
    return SimpleNamespace(
        authority_owner="sleeve_execution_root_v1",
        execution_root_path=execution_root,
        sleeve_id="PRIMARY",
        mode="PAPER",
    )


def _legacy_event(*, event_type: str, args: list[str], received_utc: str = f"{DAY}T00:00:00Z") -> dict[str, object]:
    return {
        "schema_id": "BROKER_EVENT_RAW",
        "schema_version": 1,
        "sequence_number": 1,
        "broker": {
            "client_id": 179,
            "environment": "PAPER",
            "name": "INTERACTIVE_BROKERS",
        },
        "event_type": event_type,
        "ib_fields": {"args": [{"value": item} for item in args]},
        "received_utc": received_utc,
        "sha256": "a" * 64,
    }


def _write_submission_day(
    truth_root: Path,
    *,
    submission_only: bool,
) -> None:
    submission_dir = truth_root / "execution_evidence_v1" / "submissions" / DAY / "submission-1"
    submission_dir.mkdir(parents=True, exist_ok=True)
    _write_json(
        submission_dir / "broker_submission_record.v2.json",
        {
            "schema_id": "broker_submission_record",
            "schema_version": "v2",
            "submission_id": "submission-1",
            "submitted_at_utc": f"{DAY}T00:00:00Z",
            "binding_hash": "a" * 64,
            "broker": {"name": "INTERACTIVE_BROKERS", "environment": "PAPER"},
            "status": "SUBMITTED",
            "broker_ids": {
                "order_id": None if submission_only else 101,
                "perm_id": None if submission_only else 202,
            },
            "error": None,
            "canonical_json_hash": "b" * 64,
        },
    )
    if not submission_only:
        _write_json(
            submission_dir / "execution_event_record.v1.json",
            {
                "schema_id": "execution_event_record",
                "schema_version": "v1",
                "created_at_utc": f"{DAY}T00:00:00Z",
                "event_time_utc": f"{DAY}T00:00:00Z",
                "binding_hash": "a" * 64,
                "broker_submission_hash": "b" * 64,
                "broker_order_id": "101",
                "perm_id": "202",
                "status": "SUBMITTED",
                "filled_qty": 0,
                "avg_price": "0",
                "raw_broker_status": None,
                "raw_payload_digest": None,
                "sequence_num": None,
                "canonical_json_hash": "c" * 64,
                "upstream_hash": None,
            },
        )


def _write_broker_event_day(
    truth_root: Path,
    *,
    execdetails_total: int,
    include_cash_capture: bool,
    include_position_capture: bool,
) -> Path:
    broker_day_dir = truth_root / "execution_evidence_v1" / "broker_events" / DAY
    rows = [
        _legacy_event(event_type="nextValidId", args=["orderId=44"]),
        _legacy_event(event_type="reqAllOpenOrders", args=["reqAllOpenOrders()"]),
        _legacy_event(event_type="reqExecutions", args=["reqExecutions(reqId=9001, ExecutionFilter())"]),
        _legacy_event(event_type="openOrderEnd", args=["openOrderEnd()"]),
        _legacy_event(event_type="execDetailsEnd", args=["reqId=9001"]),
        _legacy_event(event_type="bootstrapHandshakeComplete", args=["timeoutSeconds=15"]),
    ]
    if include_position_capture:
        rows.extend(
            [
                _legacy_event(event_type="reqPositions", args=["reqPositions()"]),
                _legacy_event(
                    event_type="position",
                    args=[
                        "account=DUO847203",
                        "symbol=SPY",
                        "secType=STK",
                        "exchange=SMART",
                        "currency=USD",
                        "contract=SPY-STK-SMART-USD",
                        "position=1",
                        "avgCost=500.30",
                        "marketPrice=501.00",
                    ],
                ),
                _legacy_event(event_type="positionEnd", args=["positionEnd()"]),
            ]
        )
    if include_cash_capture:
        rows.extend(
            [
                _legacy_event(
                    event_type="reqAccountSummary",
                    args=["reqId=9003", "groupName=All", "tags=TotalCashValue,TotalCashBalance,NetLiquidation"],
                ),
                _legacy_event(
                    event_type="accountSummary",
                    args=[
                        "reqId=9003",
                        "account=DUO847203",
                        "tag=TotalCashValue",
                        "value=100000.00",
                        "currency=USD",
                    ],
                ),
                _legacy_event(event_type="accountSummaryEnd", args=["reqId=9003"]),
            ]
        )
    log_path = broker_day_dir / "broker_event_log.v1.jsonl"
    _write_jsonl(log_path, rows)
    _write_json(
        broker_day_dir / "broker_event_day_manifest.v1.json",
        {
            "status": "OK",
            "log": {
                "line_count": len(rows),
                "event_type_counts": {
                    "execDetails": execdetails_total,
                },
            },
        },
    )
    return log_path


def _materialize_broker_fact_spine(*, truth_root: Path, source_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        "constellation_2.common.broker_fact_spine_v1.resolve_governed_execution_identity_v1",
        lambda **_: _governed_identity(),
    )
    monkeypatch.setattr(
        "constellation_2.common.broker_fact_spine_v1.resolve_governed_paper_execution_roots",
        lambda **_: _governed_roots(truth_root),
    )
    materialize_broker_fact_spine_v1(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc=DAY,
        environment="PAPER",
        sleeve_id="PRIMARY",
        evaluation_utc=f"{DAY}T00:00:00Z",
        source_path=source_path,
    )


def _run_reconciliation(*, truth_root: Path) -> dict:
    with patch.object(sys, "argv", ["run_reconciliation_report_v3.py", "--day_utc", DAY, "--truth_root", str(truth_root)]):
        rc = reconciliation_module.main()
    report_path = truth_root / "reports" / "reconciliation_report_v3" / DAY / "reconciliation_report.v3.json"
    return {"returncode": rc, "payload": json.loads(report_path.read_text(encoding="utf-8"))}


def _write_day_open_attempt_with_runtime_lifecycle_ref(truth_root: Path) -> None:
    _write_json(
        truth_root / "reports" / "day_open_attempt_v1" / DAY / "day_open_attempt.v1.json",
        {
            "schema_id": "day_open_attempt",
            "schema_version": "v1",
            "day_utc": DAY,
            "attempt_sequence": 1,
            "trigger_kind": "INITIAL_BOD_TRIGGER",
            "started_at_utc": f"{DAY}T13:30:00Z",
            "finished_at_utc": f"{DAY}T13:30:10Z",
            "trigger_ref": "/tmp/day_open_trigger.v1.json",
            "consumed_trigger_key": "trigger-key",
            "actor_identity": {"actor_name": "test", "actor_path": "/tmp/test"},
            "open_command_executed": True,
            "submit_stage_entered": True,
            "submit_stage_owner": "test",
            "result_code": "OPEN_SUCCEEDED",
            "reason_codes": [],
            "consumed_authority_refs": {
                "paper_session_ledger_v1": "/tmp/paper_session_ledger.v1.json",
                "day_open_trigger_v1": "/tmp/day_open_trigger.v1.json",
            },
            "direct_evidence_refs": {
                "sleeve_rollup_v1": "/tmp/sleeve_rollup.v1.json",
                "orchestrator_run_verdict_v2": [],
                "run_pointer_v1": [],
            },
            "runtime_lifecycle_ref": {
                "run_id": "20260418T150000Z__c2_paper_day_orchestrator_service__pid5150",
                "runtime_identity_contract_path": "/tmp/active_runtime_contract.v1.json",
                "runtime_identity_contract_sha256": "e" * 64,
                "startup_identity_receipt_path": "/tmp/runtime_startup_identity.v1.json",
                "lifecycle_start_receipt_path": "/tmp/runtime_lifecycle_receipt.v1.json",
            },
            "final_classification": "OPEN_SUCCEEDED",
            "attempt_history": [],
            "environment": "PAPER",
            "open_policy": {
                "policy_mode": "PAPER_READY_WHEN_GRANTED_UNBOUNDED_SAME_DAY",
                "environment": "PAPER",
                "max_successful_opens_per_day": 0,
                "max_late_open_attempts_per_day": 0,
                "same_day_open_cap_enforced": False,
                "time_window_enforced": False,
                "successful_open_already_recorded": False,
                "prior_success_ref": "",
                "initial_open_consumed": True,
                "late_open_available": False,
                "late_open_consumed": False,
                "late_open_consumed_ref": "",
                "open_terminal": False,
                "terminal_reason_code": "",
                "policy_status": "PAPER_REPEAT_OPEN_ALLOWED_WHEN_GRANTED",
                "attempt_count": 1,
                "latest_attempt_kind": "INITIAL_BOD_TRIGGER",
                "latest_attempt_result": "OPEN_SUCCEEDED",
                "prior_trigger_kind": "INITIAL_BOD_TRIGGER",
                "prior_trigger_status": "EMITTED",
            },
            "producer": {"repo": "constellation", "module": "test.module", "git_sha": "a" * 40},
        },
    )


def _write_operator_gate_inputs(truth_root: Path) -> None:
    _write_json(
        truth_root / "positions_v1" / "snapshots" / DAY / "positions_snapshot.v5.json",
        {"schema_id": "C2_POSITIONS_SNAPSHOT_V5", "schema_version": 5, "day_utc": DAY, "status": "OK", "items": []},
    )
    _write_json(
        truth_root / "reports" / "capital_risk_envelope_v2" / DAY / "capital_risk_envelope.v2.json",
        _governed_empty_dependency_payload(
            artifact_id="capital_risk_envelope_v2",
            producer_module="ops/tools/run_c2_capital_risk_envelope_gate_v2.py",
            generated_at_utc=f"{DAY}T00:00:00Z",
            payload={
                "schema_id": "capital_risk_envelope",
                "schema_version": "v2",
                "day_utc": DAY,
                "produced_utc": f"{DAY}T00:00:00Z",
                "producer": {
                    "repo": "constellation_2_runtime",
                    "module": "ops/tools/run_c2_capital_risk_envelope_gate_v2.py",
                    "git_sha": "a" * 40,
                },
                "status": "PASS",
                "reason_codes": [],
                "input_manifest": [
                    {
                        "type": "positions_snapshot",
                        "path": str((truth_root / "positions_v1" / "snapshots" / DAY / "positions_snapshot.v5.json").resolve()),
                        "sha256": "b" * 64,
                    }
                ],
                "checks": {
                    "allocation_summary_present": True,
                    "nav_present": True,
                    "positions_present": True,
                    "drawdown_present": True,
                    "positions_all_have_max_loss": True,
                    "portfolio_within_envelope": True,
                },
                "envelope": {
                    "contracts": {
                        "drawdown_contract": {"path": "drawdown.contract", "sha256": "c" * 64},
                        "capital_risk_envelope_contract": {"path": "cap.contract", "sha256": "d" * 64},
                    },
                    "drawdown_multiplier_table": [
                        {"threshold_drawdown_pct": "0.000000", "multiplier": "1.00"}
                    ],
                    "base_envelope_pct": "0.010000",
                    "nav_total": 100000,
                    "nav_total_cents": 10000000,
                    "peak_nav": 100000,
                    "drawdown_abs": 0,
                    "drawdown_pct": "0.000000",
                    "multiplier": "1.00",
                    "allowed_capital_at_risk_cents": 10000,
                    "portfolio_capital_at_risk_cents": 0,
                    "headroom_cents": 10000,
                    "positions": [],
                },
            },
        ),
    )
    _write_json(
        truth_root / "cash_ledger_v1" / "snapshots" / DAY / "cash_ledger_snapshot.v1.json",
        {
            "produced_utc": f"{DAY}T00:00:00Z",
            "snapshot": {"observed_at_utc": f"{DAY}T00:00:00Z"},
        },
    )
    _write_json(
        truth_root / "exit_reconciliation_v1" / DAY / "exit_reconciliation.v1.json",
        _governed_empty_dependency_payload(
            artifact_id="exit_reconciliation_v1",
            producer_module="constellation_2.phaseI.exit_reconciliation.run.run_exit_reconciliation_day_v1",
            generated_at_utc=f"{DAY}T00:00:00Z",
            payload={
                "schema_id": "C2_EXIT_RECONCILIATION_V1",
                "schema_version": 1,
                "produced_utc": f"{DAY}T00:00:00Z",
                "day_utc": DAY,
                "producer": {
                    "repo": "constellation_2_runtime",
                    "module": "constellation_2.phaseI.exit_reconciliation.run.run_exit_reconciliation_day_v1",
                    "git_sha": "a" * 40,
                },
                "status": "OK",
                "reason_codes": [],
                "input_manifest": [
                    {
                        "type": "positions_snapshot",
                        "path": str((truth_root / "positions_v1" / "snapshots" / DAY / "positions_snapshot.v5.json").resolve()),
                        "sha256": "e" * 64,
                        "day_utc": DAY,
                        "producer": "positions_snapshot_v5",
                    }
                ],
                "obligations": [],
            },
        ),
    )


def _write_previous_day_economic_build(truth_root: Path, *, prev_day: str, drawdown_pct: str = "-0.010000") -> None:
    _write_json(
        truth_root / "reports" / "economic_state_build_v1" / prev_day / "ctx-test" / "economic_state_build.v1.json",
        {
            "schema_id": "economic_state_build",
            "schema_version": "v1",
            "day_utc": prev_day,
            "closure_status": "COMPLETE",
            "economic_evaluation": {
                "benchmark_state": {
                    "policy_baseline": {"comparison_vs_portfolio_return": "-0.020000"},
                    "external_benchmarks": [{"benchmark_id": "SPY", "comparison_vs_portfolio_return": "-0.015000"}],
                },
                "risk_state": {"drawdown_pct": drawdown_pct},
            },
        },
    )


def test_reconciliation_report_uses_real_broker_cash_and_position_capture(monkeypatch, tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _write_submission_day(truth_root, submission_only=False)
    source_path = _write_broker_event_day(
        truth_root,
        execdetails_total=1,
        include_cash_capture=True,
        include_position_capture=True,
    )
    _materialize_broker_fact_spine(truth_root=truth_root, source_path=source_path, monkeypatch=monkeypatch)

    result = _run_reconciliation(truth_root=truth_root)

    assert result["returncode"] == 0
    assert result["payload"]["status"] == "OK"
    assert result["payload"]["comparisons"]["cash"]["status"] == "OK"
    assert result["payload"]["comparisons"]["positions"]["status"] == "OK"
    assert "MISSING_CASH_BROKER_TRUTH_CAPTURE" not in result["payload"]["reason_codes"]
    assert "MISSING_POSITIONS_BROKER_TRUTH_CAPTURE" not in result["payload"]["reason_codes"]


def test_reconciliation_report_converges_to_execdetails_gap_without_missing_capture_blockers(monkeypatch, tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _write_submission_day(truth_root, submission_only=False)
    source_path = _write_broker_event_day(
        truth_root,
        execdetails_total=0,
        include_cash_capture=True,
        include_position_capture=True,
    )
    _materialize_broker_fact_spine(truth_root=truth_root, source_path=source_path, monkeypatch=monkeypatch)

    result = _run_reconciliation(truth_root=truth_root)

    assert result["returncode"] == 1
    assert result["payload"]["status"] == "FAIL"
    assert result["payload"]["comparisons"]["truth_submissions_vs_broker_execdetails"]["status"] == "FAIL"
    assert result["payload"]["comparisons"]["cash"]["status"] == "OK"
    assert result["payload"]["comparisons"]["positions"]["status"] == "OK"
    assert "MISSING_CASH_BROKER_TRUTH_CAPTURE" not in result["payload"]["reason_codes"]
    assert "MISSING_POSITIONS_BROKER_TRUTH_CAPTURE" not in result["payload"]["reason_codes"]


def test_reconciliation_report_accepts_submission_only_dry_run_without_execdetails(monkeypatch, tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _write_submission_day(truth_root, submission_only=True)
    source_path = _write_broker_event_day(
        truth_root,
        execdetails_total=0,
        include_cash_capture=True,
        include_position_capture=True,
    )
    _materialize_broker_fact_spine(truth_root=truth_root, source_path=source_path, monkeypatch=monkeypatch)

    result = _run_reconciliation(truth_root=truth_root)

    assert result["returncode"] == 0
    assert result["payload"]["status"] == "OK"
    assert result["payload"]["comparisons"]["truth_submissions_vs_broker_execdetails"]["status"] == "OK"
    assert "not yet required" in result["payload"]["comparisons"]["truth_submissions_vs_broker_execdetails"]["reason"]
    assert result["payload"]["comparisons"]["cash"]["status"] == "OK"
    assert result["payload"]["comparisons"]["positions"]["status"] == "OK"


def test_reconciliation_report_emits_runtime_lifecycle_ref_when_day_open_attempt_carries_it(monkeypatch, tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _write_submission_day(truth_root, submission_only=True)
    _write_day_open_attempt_with_runtime_lifecycle_ref(truth_root)
    source_path = _write_broker_event_day(
        truth_root,
        execdetails_total=0,
        include_cash_capture=True,
        include_position_capture=True,
    )
    _materialize_broker_fact_spine(truth_root=truth_root, source_path=source_path, monkeypatch=monkeypatch)

    result = _run_reconciliation(truth_root=truth_root)

    assert result["returncode"] == 0
    assert result["payload"]["runtime_lifecycle_ref"]["run_id"] == "20260418T150000Z__c2_paper_day_orchestrator_service__pid5150"
    assert any(item["type"] == "day_open_attempt_v1" for item in result["payload"]["input_manifest"])


def test_reconciliation_report_prefers_ok_manifest_matching_current_broker_log(monkeypatch, tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _write_submission_day(truth_root, submission_only=False)
    source_path = _write_broker_event_day(
        truth_root,
        execdetails_total=1,
        include_cash_capture=True,
        include_position_capture=True,
    )
    _materialize_broker_fact_spine(truth_root=truth_root, source_path=source_path, monkeypatch=monkeypatch)

    broker_day_dir = truth_root / "execution_evidence_v1" / "broker_events" / DAY
    fixed_manifest = broker_day_dir / "broker_event_day_manifest.v1.json"
    matching_log_sha = reconciliation_module._sha256_file(source_path)
    matching_manifest = broker_day_dir / f"broker_event_day_manifest.v1.{matching_log_sha}.json"

    _write_json(
        fixed_manifest,
        {
            "status": "FAIL",
            "produced_utc": f"{DAY}T00:00:00Z",
            "log": {"line_count": 0, "event_type_counts": {}, "log_sha256": "0" * 64},
        },
    )
    _write_json(
        matching_manifest,
        {
            "status": "OK",
            "produced_utc": f"{DAY}T00:05:00Z",
            "input_manifest": [
                {
                    "type": "broker_event_log_v1_jsonl",
                    "path": str(source_path),
                    "sha256": matching_log_sha,
                }
            ],
            "log": {
                "line_count": 12,
                "event_type_counts": {"execDetails": 1},
                "log_sha256": matching_log_sha,
            },
        },
    )

    result = _run_reconciliation(truth_root=truth_root)

    assert result["returncode"] == 0
    assert result["payload"]["status"] == "OK"
    assert result["payload"]["broker_side"]["broker_event_manifest_path"] == str(matching_manifest)
    assert result["payload"]["broker_side"]["counts"]["execDetails_total"] == 1
    assert result["payload"]["comparisons"]["truth_submissions_vs_broker_execdetails"]["status"] == "OK"


def test_operator_daily_gate_passes_when_reconciliation_prereqs_are_present(monkeypatch, tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    prev_day = "2026-04-12"
    _write_submission_day(truth_root, submission_only=True)
    source_path = _write_broker_event_day(
        truth_root,
        execdetails_total=0,
        include_cash_capture=True,
        include_position_capture=True,
    )
    _materialize_broker_fact_spine(truth_root=truth_root, source_path=source_path, monkeypatch=monkeypatch)
    _write_operator_gate_inputs(truth_root)
    _write_previous_day_economic_build(truth_root, prev_day=prev_day)
    recon = _run_reconciliation(truth_root=truth_root)
    assert recon["payload"]["status"] == "OK"

    with patch.object(
        sys,
        "argv",
        [
            "run_operator_daily_gate_v3.py",
            "--day_utc",
            DAY,
            "--truth_root",
            str(truth_root),
            "--produced_utc",
            f"{DAY}T00:00:00Z",
            "--mode",
            "PAPER",
        ],
    ):
        rc = operator_gate_module.main()

    gate_path = truth_root / "reports" / "operator_daily_gate_v3" / DAY / "operator_daily_gate.v3.json"
    gate = json.loads(gate_path.read_text(encoding="utf-8"))
    assert rc == 0
    assert gate["status"] == "PASS"
    assert gate["closure_state"] == "COMPLETE"
    assert gate["blocking_codes"] == []
    assert gate["missing_dependency_artifacts"] == []
    assert gate["checks"]["positions_snapshot_present"] is True
    assert gate["checks"]["previous_day_economic_state_status"] == "OK"
    assert gate["checks"]["reconciliation_v3_status"] == "OK"
    validated = validate_governed_artifact_payload_v1(
        repo_root=REPO_ROOT,
        artifact_id="operator_daily_gate_v3",
        payload=gate,
    )
    assert validated["constitutional_lineage"]["artifact_type"] == "operator_daily_gate_v3"
    assert validated["constitutional_dependency_declaration"]["declared_dependency_artifacts"] == [
        "reconciliation_report_v3",
        "positions_snapshot_v5",
        "capital_risk_envelope_v2",
        "cash_ledger_snapshot_v1",
        "exit_reconciliation_v1",
        "economic_state_build_v1",
    ]


def test_operator_daily_gate_fails_closed_when_previous_day_bundle_c_missing(monkeypatch, tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _write_submission_day(truth_root, submission_only=True)
    source_path = _write_broker_event_day(
        truth_root,
        execdetails_total=0,
        include_cash_capture=True,
        include_position_capture=True,
    )
    _materialize_broker_fact_spine(truth_root=truth_root, source_path=source_path, monkeypatch=monkeypatch)
    _write_operator_gate_inputs(truth_root)
    recon = _run_reconciliation(truth_root=truth_root)
    assert recon["payload"]["status"] == "OK"

    with patch.object(
        sys,
        "argv",
        [
            "run_operator_daily_gate_v3.py",
            "--day_utc",
            DAY,
            "--truth_root",
            str(truth_root),
            "--produced_utc",
            f"{DAY}T00:00:00Z",
            "--mode",
            "PAPER",
        ],
    ):
        rc = operator_gate_module.main()

    gate_path = truth_root / "reports" / "operator_daily_gate_v3" / DAY / "operator_daily_gate.v3.json"
    gate = json.loads(gate_path.read_text(encoding="utf-8"))
    assert rc == 1
    assert gate["status"] == "FAIL"
    assert gate["closure_state"] == "BLOCKED"
    assert gate["first_blocker_code"] == "MISSING_PREVIOUS_DAY_ECONOMIC_STATE_FAILCLOSED"
    assert gate["missing_dependency_artifacts"] == ["economic_state_build_v1"]
    assert gate["checks"]["previous_day_economic_state_status"] == "UNKNOWN"
    assert "MISSING_PREVIOUS_DAY_ECONOMIC_STATE_FAILCLOSED" in gate["reason_codes"]
    validated = validate_governed_artifact_payload_v1(
        repo_root=REPO_ROOT,
        artifact_id="operator_daily_gate_v3",
        payload=gate,
    )
    assert validated["constitutional_dependency_declaration"]["declared_dependency_artifacts"] == [
        "reconciliation_report_v3",
        "positions_snapshot_v5",
        "capital_risk_envelope_v2",
        "cash_ledger_snapshot_v1",
        "exit_reconciliation_v1",
        "economic_state_build_v1",
    ]


def test_operator_daily_gate_requires_canonical_positions_snapshot_v5(monkeypatch, tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    prev_day = "2026-04-12"
    _write_submission_day(truth_root, submission_only=True)
    source_path = _write_broker_event_day(
        truth_root,
        execdetails_total=0,
        include_cash_capture=True,
        include_position_capture=True,
    )
    _materialize_broker_fact_spine(truth_root=truth_root, source_path=source_path, monkeypatch=monkeypatch)
    _write_json(
        truth_root / "positions_v1" / "snapshots" / DAY / "positions_snapshot.v2.json",
        {"schema_id": "positions_snapshot", "day_utc": DAY},
    )
    _write_previous_day_economic_build(truth_root, prev_day=prev_day)
    _write_json(
        truth_root / "reports" / "capital_risk_envelope_v2" / DAY / "capital_risk_envelope.v2.json",
        {"schema_id": "capital_risk_envelope", "schema_version": "v2", "day_utc": DAY, "status": "PASS"},
    )
    _write_json(
        truth_root / "cash_ledger_v1" / "snapshots" / DAY / "cash_ledger_snapshot.v1.json",
        {"produced_utc": f"{DAY}T00:00:00Z", "snapshot": {"observed_at_utc": f"{DAY}T00:00:00Z"}},
    )
    _write_json(
        truth_root / "exit_reconciliation_v1" / DAY / "exit_reconciliation.v1.json",
        {"schema_id": "exit_reconciliation", "day_utc": DAY, "obligations": []},
    )
    recon = _run_reconciliation(truth_root=truth_root)
    assert recon["payload"]["status"] == "OK"

    with patch.object(
        sys,
        "argv",
        [
            "run_operator_daily_gate_v3.py",
            "--day_utc",
            DAY,
            "--truth_root",
            str(truth_root),
            "--produced_utc",
            f"{DAY}T00:00:00Z",
            "--mode",
            "PAPER",
        ],
    ):
        rc = operator_gate_module.main()

    gate_path = truth_root / "reports" / "operator_daily_gate_v3" / DAY / "operator_daily_gate.v3.json"
    gate = json.loads(gate_path.read_text(encoding="utf-8"))
    assert rc == 1
    assert gate["status"] == "FAIL"
    assert gate["checks"]["positions_snapshot_present"] is False
    assert "MISSING_POSITIONS_SNAPSHOT_V5" in gate["reason_codes"]


def test_operator_daily_gate_fails_on_previous_day_bundle_c_drawdown(monkeypatch, tmp_path: Path) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    prev_day = "2026-04-12"
    _write_submission_day(truth_root, submission_only=True)
    source_path = _write_broker_event_day(
        truth_root,
        execdetails_total=0,
        include_cash_capture=True,
        include_position_capture=True,
    )
    _materialize_broker_fact_spine(truth_root=truth_root, source_path=source_path, monkeypatch=monkeypatch)
    _write_operator_gate_inputs(truth_root)
    _write_previous_day_economic_build(truth_root, prev_day=prev_day, drawdown_pct="-0.110000")
    recon = _run_reconciliation(truth_root=truth_root)
    assert recon["payload"]["status"] == "OK"

    with patch.object(
        sys,
        "argv",
        [
            "run_operator_daily_gate_v3.py",
            "--day_utc",
            DAY,
            "--truth_root",
            str(truth_root),
            "--produced_utc",
            f"{DAY}T00:00:00Z",
            "--mode",
            "PAPER",
        ],
    ):
        rc = operator_gate_module.main()

    gate_path = truth_root / "reports" / "operator_daily_gate_v3" / DAY / "operator_daily_gate.v3.json"
    gate = json.loads(gate_path.read_text(encoding="utf-8"))
    assert rc == 1
    assert gate["status"] == "FAIL"
    assert gate["checks"]["previous_day_economic_state_status"] == "OK"
    assert gate["checks"]["previous_day_drawdown_guard_status"] == "BLOCKED"
    assert gate["economic_state"]["drawdown_pct"] == "-0.110000"
    assert "BUNDLE_C_DRAWDOWN_LIMIT_EXCEEDED" in gate["reason_codes"]

    for gate_id, filename in {
        "feed_attestation_gate_v1": "feed_attestation_gate.v1.json",
        "heartbeat_gate_v1": "heartbeat_gate.v1.json",
        "correlation_envelope_gate_v1": "correlation_envelope_gate.v1.json",
        "liquidity_slippage_gate_v1": "liquidity_slippage_gate.v1.json",
        "replay_certification_gate_v1": "replay_certification_gate.v1.json",
    }.items():
        _write_json(
            truth_root / "reports" / gate_id / DAY / filename,
            {"schema_id": gate_id, "schema_version": "v1", "day_utc": DAY, "status": "PASS", "reason_codes": []},
        )

    with patch.object(
        sys,
        "argv",
        [
            "run_gate_stack_verdict_v1.py",
            "--day_utc",
            DAY,
            "--truth_root",
            str(truth_root),
            "--produced_utc",
            f"{DAY}T00:00:00Z",
            "--mode",
            "PAPER",
        ],
    ):
        gate_stack_rc = gate_stack_module.main()

    gate_stack_path = truth_root / "reports" / "gate_stack_verdict_v1" / DAY / "gate_stack_verdict.v1.json"
    gate_stack = json.loads(gate_stack_path.read_text(encoding="utf-8"))
    operator_row = next(row for row in gate_stack["gates"] if row["gate_id"] == "operator_daily_gate_v3")
    assert gate_stack_rc == 1
    assert gate_stack["status"] == "FAIL"
    assert gate_stack["closure_state"] == "BLOCKED"
    assert gate_stack["missing_dependency_artifacts"] == []
    assert gate_stack["first_blocker_code"].startswith("GATE_REQUIRED_NOT_PASS:operator_daily_gate_v3:FAIL")
    assert operator_row["status"] == "FAIL"
    validated_gate_stack = validate_governed_artifact_payload_v1(
        repo_root=REPO_ROOT,
        artifact_id="gate_stack_verdict_v1",
        payload=gate_stack,
    )
    assert validated_gate_stack["constitutional_dependency_declaration"]["declared_dependency_artifacts"] == [
        "operator_daily_gate_v3",
        "feed_attestation_gate_v1",
        "heartbeat_gate_v1",
        "capital_risk_envelope_v2",
        "correlation_envelope_gate_v1",
        "liquidity_slippage_gate_v1",
        "replay_certification_gate_v1",
    ]
    assert validated_gate_stack["constitutional_lineage"]["artifact_type"] == "gate_stack_verdict_v1"


def test_operator_daily_gate_fails_closed_when_capital_risk_envelope_is_invalid_governed_payload(
    monkeypatch,
    tmp_path: Path,
) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    prev_day = "2026-04-12"
    _write_submission_day(truth_root, submission_only=True)
    source_path = _write_broker_event_day(
        truth_root,
        execdetails_total=0,
        include_cash_capture=True,
        include_position_capture=True,
    )
    _materialize_broker_fact_spine(truth_root=truth_root, source_path=source_path, monkeypatch=monkeypatch)
    _write_operator_gate_inputs(truth_root)
    _write_previous_day_economic_build(truth_root, prev_day=prev_day)
    recon = _run_reconciliation(truth_root=truth_root)
    assert recon["payload"]["status"] == "OK"

    cap_path = truth_root / "reports" / "capital_risk_envelope_v2" / DAY / "capital_risk_envelope.v2.json"
    cap_payload = json.loads(cap_path.read_text(encoding="utf-8"))
    cap_payload.pop("constitutional_lineage", None)
    cap_path.write_text(json.dumps(cap_payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")

    with patch.object(
        sys,
        "argv",
        [
            "run_operator_daily_gate_v3.py",
            "--day_utc",
            DAY,
            "--truth_root",
            str(truth_root),
            "--produced_utc",
            f"{DAY}T00:00:00Z",
            "--mode",
            "PAPER",
        ],
    ):
        rc = operator_gate_module.main()

    gate_path = truth_root / "reports" / "operator_daily_gate_v3" / DAY / "operator_daily_gate.v3.json"
    gate = json.loads(gate_path.read_text(encoding="utf-8"))
    assert rc == 1
    assert gate["status"] == "FAIL"
    assert gate["closure_state"] == "BLOCKED"
    assert "INVALID_GOVERNED_DEPENDENCY:capital_risk_envelope_v2" in gate["blocking_codes"]
    assert gate["first_blocker_code"] == "INVALID_GOVERNED_DEPENDENCY:capital_risk_envelope_v2"


def test_gate_stack_verdict_fails_closed_when_capital_risk_envelope_is_invalid_governed_payload(
    monkeypatch,
    tmp_path: Path,
) -> None:
    truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    prev_day = "2026-04-12"
    _write_submission_day(truth_root, submission_only=True)
    source_path = _write_broker_event_day(
        truth_root,
        execdetails_total=0,
        include_cash_capture=True,
        include_position_capture=True,
    )
    _materialize_broker_fact_spine(truth_root=truth_root, source_path=source_path, monkeypatch=monkeypatch)
    _write_operator_gate_inputs(truth_root)
    _write_previous_day_economic_build(truth_root, prev_day=prev_day)
    recon = _run_reconciliation(truth_root=truth_root)
    assert recon["payload"]["status"] == "OK"

    with patch.object(
        sys,
        "argv",
        [
            "run_operator_daily_gate_v3.py",
            "--day_utc",
            DAY,
            "--truth_root",
            str(truth_root),
            "--produced_utc",
            f"{DAY}T00:00:00Z",
            "--mode",
            "PAPER",
        ],
    ):
        assert operator_gate_module.main() == 0

    for gate_id, filename in {
        "feed_attestation_gate_v1": "feed_attestation_gate.v1.json",
        "heartbeat_gate_v1": "heartbeat_gate.v1.json",
        "correlation_envelope_gate_v1": "correlation_envelope_gate.v1.json",
        "liquidity_slippage_gate_v1": "liquidity_slippage_gate.v1.json",
        "replay_certification_gate_v1": "replay_certification_gate.v1.json",
    }.items():
        _write_json(
            truth_root / "reports" / gate_id / DAY / filename,
            {"schema_id": gate_id, "schema_version": "v1", "day_utc": DAY, "status": "PASS", "reason_codes": []},
        )

    cap_path = truth_root / "reports" / "capital_risk_envelope_v2" / DAY / "capital_risk_envelope.v2.json"
    cap_payload = json.loads(cap_path.read_text(encoding="utf-8"))
    cap_payload.pop("constitutional_dependency_declaration", None)
    cap_path.write_text(json.dumps(cap_payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")

    with patch.object(
        sys,
        "argv",
        [
            "run_gate_stack_verdict_v1.py",
            "--day_utc",
            DAY,
            "--truth_root",
            str(truth_root),
            "--produced_utc",
            f"{DAY}T00:00:00Z",
            "--mode",
            "PAPER",
        ],
    ):
        rc = gate_stack_module.main()

    gate_stack_path = truth_root / "reports" / "gate_stack_verdict_v1" / DAY / "gate_stack_verdict.v1.json"
    gate_stack = json.loads(gate_stack_path.read_text(encoding="utf-8"))
    assert rc == 1
    assert gate_stack["status"] == "FAIL"
    assert gate_stack["closure_state"] == "BLOCKED"
    assert "INVALID_GOVERNED_DEPENDENCY:capital_risk_envelope_v2" in gate_stack["blocking_codes"]
    assert gate_stack["first_blocker_code"] in {
        "INVALID_GOVERNED_DEPENDENCY:operator_daily_gate_v3",
        "INVALID_GOVERNED_DEPENDENCY:capital_risk_envelope_v2",
    }
