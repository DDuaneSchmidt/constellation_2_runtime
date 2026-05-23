from __future__ import annotations

import json
from pathlib import Path

import pytest

from ops.aegis.evidence_event_store_v1 import append_evidence_event_v1, evidence_events_path_v1, rebuild_evidence_snapshot_v1
from ops.aegis.intelligence_common_v1 import write_json_v1
from ops.aegis.pure_runtime_evaluator_v1 import evaluate_runtime, runtime_policy_bundle_v1
import ops.aegis.repair_orchestrator_v1 as repair_runtime
from ops.aegis.repair_orchestrator_v1 import run_repair_orchestration_v1
from ops.aegis.runtime_evaluation_v1 import validate_runtime_evaluation_v1
from ops.aegis.runtime_evaluation_v1 import stable_json_bytes_v1
from ops.tools import capture_manual_trade_receipt_v1 as capture_cli
from ops.aegis.runtime_truth_kernel_v1 import build_ledger_runtime_evaluation_v1, build_runtime_truth_kernel_v1, runtime_evaluation_path_v1, runtime_policy_bundle_path_v1
from constellation_2.common.aegis_chatgpt_control_packet_v1 import build_aegis_chatgpt_control_packet_v1
from constellation_2.phaseL.ui_api.readiness_kernel_v1 import build_readiness_kernel_v1
from ops.tools.build_aegis_audit_bundle_v1 import build_aegis_audit_bundle_v1
from ops.tools.replay_aegis_runtime_v1 import replay_aegis_runtime_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


DAY = "2026-05-15"
NOW = "2026-05-15T20:55:00Z"
REPO_ROOT = Path(__file__).resolve().parents[3]


def _event(schema_id: str, *, day: str = DAY, created_at: str = NOW, validation_status: str = "VALID") -> dict:
    return {
        "event_id": f"event-{schema_id}",
        "event_type": "EvidenceValidated",
        "run_id": "run-1",
        "parent_run_id": "",
        "day_utc": day,
        "created_at_utc": created_at,
        "producer": "ops.aegis.runtime_truth_kernel_v1.artifact_scan",
        "producer_version": "aegis_runtime_truth_kernel.v1",
        "git_sha": "abc1234",
        "schema_id": schema_id,
        "schema_version": "v1",
        "input_hashes": {},
        "output_hashes": {f"{schema_id}.json": "a" * 64},
        "artifact_paths": [f"/tmp/reports/{schema_id}_v1/{day}/{schema_id}.v1.json"],
        "validation_status": validation_status,
        "previous_event_hash": "",
        "event_hash": "",
    }


def _write_ready_events(root: Path) -> None:
    for schema_id in (
        "aegis_lite_operating_status",
        "aegis_lite_eod_report",
        "operator_execution_queue",
        "event_market_snapshot",
        "research_dataset_binding",
        "research_task_queue",
        "event_monitoring_status",
        "event_rules_registry",
        "event_validity_gate",
        "ai_feedback_review",
        "manual_execution_receipt",
        "manual_trade_packet",
        "promoted_candidate_evidence",
        "alert_transport_proof",
    ):
        append_evidence_event_v1(truth_root=root, day_utc=DAY, event=_event(schema_id))


def test_runtime_evaluation_hash_is_deterministic_for_same_bundle(tmp_path: Path) -> None:
    _write_ready_events(tmp_path)
    snapshot = rebuild_evidence_snapshot_v1(truth_root=tmp_path, day_utc=DAY)
    policy = runtime_policy_bundle_v1(run_id="run-1", parent_run_id="", generated_at_utc=NOW, git_sha="abc1234")

    first = evaluate_runtime(DAY, snapshot, policy)
    second = evaluate_runtime(DAY, snapshot, policy)

    validate_runtime_evaluation_v1(first)
    assert first["deterministic_output_hash"] == second["deterministic_output_hash"]
    assert first["capabilities"]["TRADE_ADVICE_ALLOWED"]["allowed"] is False
    assert first["capabilities"]["AUTONOMOUS_EXECUTION_ALLOWED"]["allowed"] is False


def test_event_store_rejects_tampered_previous_event(tmp_path: Path) -> None:
    append_evidence_event_v1(truth_root=tmp_path, day_utc=DAY, event=_event("event_market_snapshot"))
    path = evidence_events_path_v1(truth_root=tmp_path, day_utc=DAY)
    text = path.read_text(encoding="utf-8")
    path.write_text(text.replace("event_market_snapshot", "event_market_tampered", 1), encoding="utf-8")

    with pytest.raises(ValueError):
        rebuild_evidence_snapshot_v1(truth_root=tmp_path, day_utc=DAY)


def test_event_store_rejects_wrong_day_event(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        append_evidence_event_v1(truth_root=tmp_path, day_utc=DAY, event=_event("event_market_snapshot", day="2026-05-14"))


def test_pure_evaluator_fails_closed_for_stale_future_and_missing(tmp_path: Path) -> None:
    append_evidence_event_v1(truth_root=tmp_path, day_utc=DAY, event=_event("event_market_snapshot", created_at="2026-05-16T00:00:00Z"))
    snapshot = rebuild_evidence_snapshot_v1(truth_root=tmp_path, day_utc=DAY)
    policy = runtime_policy_bundle_v1(run_id="run-1", parent_run_id="", generated_at_utc=NOW, git_sha="abc1234")

    evaluation = evaluate_runtime(DAY, snapshot, policy)

    assert evaluation["runtime_truth_classification"] == "PARTIAL_CONTEXT"
    assert evaluation["highest_readiness_layer"] == "BLOCKED"
    assert evaluation["capabilities"]["DATA_READY"]["allowed"] is False
    assert any("FUTURE_DATED" in row["blocker_id"] for row in evaluation["blockers"])


def test_repair_dry_run_writes_no_evidence_events(tmp_path: Path) -> None:
    before_path = evidence_events_path_v1(truth_root=tmp_path, day_utc=DAY)
    result = run_repair_orchestration_v1(
        truth_root=tmp_path,
        repo_root=Path.cwd(),
        day_utc=DAY,
        mode="dry-run",
        repair_class="",
        run_id="repair-run",
        parent_run_id="",
        generated_at_utc=NOW,
        git_sha="abc1234",
    )

    assert result["attempts"] == []
    assert before_path.exists() is False
    assert Path(result["paths"]["repair_plan_json"]).exists()


def test_audit_bundle_replay_reproduces_runtime_evaluation_hash(tmp_path: Path) -> None:
    _write_ready_events(tmp_path)
    bundle = build_aegis_audit_bundle_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        run_id="bundle-run",
        parent_run_id="",
        generated_at_utc=NOW,
        command_args=["--day-utc", DAY],
    )

    replay = replay_aegis_runtime_v1(audit_bundle=Path(bundle["bundle_path"]))

    assert replay["ok"] is True
    assert replay["runtime_evaluation_hash"] == bundle["runtime_evaluation_hash"]


def test_manual_capture_blocked_when_runtime_evaluation_missing_or_partial(tmp_path: Path, capsys) -> None:
    rc = capture_cli.main(
        [
            "--truth_root",
            str(tmp_path),
            "--symbol",
            "SPY",
            "--side",
            "BUY",
            "--quantity",
            "1",
            "--price",
            "500",
            "--trade-date",
            DAY,
            "--operator-attestation",
            "true",
        ]
    )

    assert rc == 3
    err = json.loads(capsys.readouterr().err)
    assert err["validation_status"] == "BLOCKED_BY_RUNTIME_EVALUATION"

def test_common_json_writer_emits_produced_and_validated_events(tmp_path: Path) -> None:
    artifact = tmp_path / "reports" / "event_market_snapshot_v1" / DAY / "event_market_snapshot.v1.json"

    write_json_v1(
        artifact,
        {
            "schema_id": "event_market_snapshot",
            "artifact_id": "event_market_snapshot",
            "schema_version": "v1",
            "day_utc": DAY,
            "generated_at_utc": NOW,
            "status": "PASS",
        },
    )

    snapshot = rebuild_evidence_snapshot_v1(truth_root=tmp_path, day_utc=DAY)
    event_types = [row["event_type"] for row in snapshot["events"]]
    assert event_types == ["EvidenceProduced", "EvidenceValidated"]
    assert snapshot["latest_by_schema_id"]["event_market_snapshot"]["validation_status"] == "VALID"


def test_repair_execute_requires_native_evidence_event_ids(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    producer = tmp_path / "repair_producer.py"
    producer.write_text("import sys\nsys.exit(0)\n", encoding="utf-8")
    monkeypatch.setattr(
        repair_runtime,
        "repairable_contracts_by_schema_v1",
        lambda _registry=None: {
            "event_market_snapshot": {
                "producer_id": "test.native.producer",
                "producer_version": "v1",
                "repair_class": "AUTO_DETERMINISTIC",
                "command": f"python3 {producer}",
                "inputs": [],
                "outputs": ["reports/event_market_snapshot_v1/{day_utc}/event_market_snapshot.v1.json"],
            }
        },
    )

    result = run_repair_orchestration_v1(
        truth_root=tmp_path,
        repo_root=Path.cwd(),
        day_utc=DAY,
        mode="execute",
        repair_class="AUTO_DETERMINISTIC",
        run_id="repair-execute-run",
        parent_run_id="",
        generated_at_utc=NOW,
        git_sha="abc1234",
    )

    assert result["attempts"]
    events = rebuild_evidence_snapshot_v1(truth_root=tmp_path, day_utc=DAY)["events"]
    repair_types = [row["event_type"] for row in events if row["producer"] == "ops.aegis.repair_orchestrator_v1"]
    assert "RepairPlanned" in repair_types
    assert "RepairAttempted" in repair_types
    assert "RepairFailed" in repair_types
    assert result["attempts"][0]["validation_status"] == "NO_EVIDENCE_EVENT_EMITTED"

def test_all_authority_consumers_reference_same_canonical_runtime_evaluation_hash(tmp_path: Path, capsys) -> None:
    append_evidence_event_v1(truth_root=tmp_path, day_utc=DAY, event=_event("event_market_snapshot"))
    evaluation, policy = build_ledger_runtime_evaluation_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        generated_at_utc=NOW,
        run_id="canonical-run",
        parent_run_id="",
    )
    eval_path = runtime_evaluation_path_v1(truth_root=tmp_path, day_utc=DAY)
    policy_path = runtime_policy_bundle_path_v1(truth_root=tmp_path, day_utc=DAY)
    eval_path.parent.mkdir(parents=True, exist_ok=True)
    eval_path.write_bytes(stable_json_bytes_v1(evaluation) + b"\n")
    policy_path.write_bytes(stable_json_bytes_v1(policy) + b"\n")
    expected_hash = evaluation["deterministic_output_hash"]

    kernel = build_runtime_truth_kernel_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    ui = build_readiness_kernel_v1(DAY, truth_root=tmp_path, sleeve_truth_root=tmp_path / "sleeve")
    packet = build_aegis_chatgpt_control_packet_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)
    bundle = build_aegis_audit_bundle_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        run_id="canonical-bundle",
        parent_run_id="",
        generated_at_utc=NOW,
        command_args=["--day-utc", DAY],
    )
    replay = replay_aegis_runtime_v1(audit_bundle=Path(bundle["bundle_path"]))

    assert kernel["runtime_evaluation_hash"] == expected_hash
    assert ui["runtime_evaluation_hash"] == expected_hash
    assert packet["reason_if_blocked"]
    assert bundle["runtime_evaluation_hash"] == expected_hash
    assert replay["runtime_evaluation_hash"] == expected_hash

    rc = capture_cli.main(
        [
            "--truth_root",
            str(tmp_path),
            "--symbol",
            "SPY",
            "--side",
            "BUY",
            "--quantity",
            "1",
            "--price",
            "500",
            "--trade-date",
            DAY,
            "--operator-attestation",
            "true",
        ]
    )
    assert rc == 3
    manual = json.loads(capsys.readouterr().err)
    assert manual["runtime_evaluation_hash"] == expected_hash

from ops.aegis.producer_contract_validator_v1 import validate_evidence_event_contract_v1


def test_producer_contract_validator_rejects_unknown_wrong_schema_forbidden_manual_and_external() -> None:
    valid = _event("event_rules_registry")
    valid["producer"] = "ops/tools/run_aegis_event_monitor_v1.py:event_rules_registry"
    valid["artifact_paths"] = [f"/tmp/reports/event_rules_registry_v1/{DAY}/run/event_rules_registry.v1.json"]
    assert validate_evidence_event_contract_v1(valid)["contract_valid"] is True

    unknown = {**valid, "producer": "unknown.producer"}
    assert validate_evidence_event_contract_v1(unknown)["status"] == "UNKNOWN_PRODUCER"

    wrong_schema = {**valid, "schema_id": "event_market_snapshot"}
    assert validate_evidence_event_contract_v1(wrong_schema)["status"] in {"UNKNOWN_PRODUCER", "WRONG_SCHEMA"}

    external = _event("event_market_snapshot")
    external["producer"] = "ops/tools/build_event_market_snapshot_v1.py"
    external["artifact_paths"] = [f"/tmp/reports/event_market_snapshot_v1/{DAY}/event_market_snapshot.v1.json"]
    external["input_hashes"] = {}
    assert validate_evidence_event_contract_v1(external)["status"] == "SOURCE_HASH_MISSING"

    manual = _event("manual_execution_receipt")
    manual["producer"] = "ops/tools/write_manual_intent_v1.py:manual_execution_receipt"
    manual["artifact_paths"] = [f"/tmp/reports/manual_execution_receipt_v1/{DAY}/index/manual_execution_receipt.v1.json"]
    manual["input_hashes"] = {}
    assert validate_evidence_event_contract_v1(manual)["status"] == "OPERATOR_INTENT_MISSING"

from ops.aegis.event_append_transaction_v1 import append_event_transaction_v1, quarantine_events_path_v1, read_quarantined_events_v1, canonical_vs_quarantine_report_v1
from ops.aegis.manual_intent_v1 import build_manual_intent_v1, emit_manual_action_event_v1, validate_manual_intent_v1, write_manual_intent_v1
from ops.aegis.producer_contract_reports_v1 import critical_bridge_usage_report_v1


def _contract_event(schema_id: str = "event_rules_registry", *, event_id: str = "native-event-1", producer: str = "ops/tools/run_aegis_event_monitor_v1.py:event_rules_registry", day: str = DAY) -> dict:
    return {
        "event_id": event_id,
        "event_type": "EvidenceValidated",
        "run_id": "native-run",
        "parent_run_id": "",
        "day_utc": day,
        "created_at_utc": NOW,
        "producer": producer,
        "producer_version": "v1",
        "git_sha": "abc1234",
        "schema_id": schema_id,
        "schema_version": "v1",
        "input_hashes": {"input:registry": "b" * 64},
        "output_hashes": {f"/tmp/reports/{schema_id}_v1/{DAY}/run/{schema_id}.v1.json": "a" * 64},
        "artifact_paths": [f"/tmp/reports/{schema_id}_v1/{DAY}/run/{schema_id}.v1.json"],
        "validation_status": "VALID",
        "previous_event_hash": "",
        "event_hash": "",
    }


def test_event_append_transaction_appends_valid_event_and_quarantines_invalid(tmp_path: Path) -> None:
    valid = append_event_transaction_v1(truth_root=tmp_path, day_utc=DAY, event=_contract_event())
    assert valid["ok"] is True
    assert valid["canonical_event_validation_status"] == "CANONICAL_VALID"
    assert valid["event"]["canonical_event_validation_status"] == "CANONICAL_VALID"

    invalid = append_event_transaction_v1(truth_root=tmp_path, day_utc=DAY, event=_contract_event(event_id="bad-native", producer="unknown.producer"))
    assert invalid["ok"] is False
    assert "CONTRACT:UNKNOWN_PRODUCER" in invalid["reasons"]
    assert quarantine_events_path_v1(truth_root=tmp_path, day_utc=DAY).exists()

    snapshot = rebuild_evidence_snapshot_v1(truth_root=tmp_path, day_utc=DAY)
    assert [row["event_id"] for row in snapshot["events"]] == [valid["event"]["event_id"]]


def test_event_append_transaction_rejects_duplicate_and_wrong_day(tmp_path: Path) -> None:
    first = append_event_transaction_v1(truth_root=tmp_path, day_utc=DAY, event=_contract_event(event_id="dupe-native"))
    assert first["ok"] is True
    duplicate = append_event_transaction_v1(truth_root=tmp_path, day_utc=DAY, event=_contract_event(event_id="dupe-native"))
    wrong_day = append_event_transaction_v1(truth_root=tmp_path, day_utc=DAY, event=_contract_event(event_id="wrong-day", day="2026-05-14"))

    assert duplicate["ok"] is False
    assert "DUPLICATE_EVENT_ID" in duplicate["reasons"]
    assert wrong_day["ok"] is False
    assert "WRONG_DAY" in wrong_day["reasons"]


    malformed = append_event_transaction_v1(truth_root=tmp_path, day_utc=DAY, event={"event_id": "malformed", "day_utc": DAY})
    assert malformed["ok"] is False
    assert malformed["canonical_event_validation_status"] == "QUARANTINED_INVALID"
    assert any(str(reason).startswith("MALFORMED:") for reason in malformed["reasons"])
    report = canonical_vs_quarantine_report_v1(truth_root=tmp_path, day_utc=DAY)
    wrong_day_report = canonical_vs_quarantine_report_v1(truth_root=tmp_path, day_utc="2026-05-14")
    assert report["canonical_event_count"] == 1
    assert report["quarantined_event_count"] == 2
    assert wrong_day_report["quarantined_event_count"] == 1


def test_manual_intent_none_declared_is_valid_and_deterministic(tmp_path: Path) -> None:
    payload = build_manual_intent_v1(
        operator_id="operator-1",
        operator_intent="NONE_DECLARED",
        intent_timestamp_utc=NOW,
        day_utc=DAY,
        reason="No manual action asserted.",
        runtime_evaluation_hash="h" * 64,
    )

    assert validate_manual_intent_v1(payload, day_utc=DAY, runtime_evaluation_hash="h" * 64, generated_at_utc=NOW) == []
    first_path = write_manual_intent_v1(truth_root=tmp_path / "first", payload=payload)
    second_path = write_manual_intent_v1(truth_root=tmp_path / "second", payload=payload)
    assert first_path.read_bytes() == second_path.read_bytes()


def test_manual_intent_missing_malformed_and_stale_block(tmp_path: Path) -> None:
    runtime_hash = "h" * 64
    missing = {"schema_id": "manual_intent", "day_utc": DAY, "runtime_evaluation_hash": runtime_hash}
    missing_reasons = validate_manual_intent_v1(missing, day_utc=DAY, runtime_evaluation_hash=runtime_hash, generated_at_utc=NOW)
    assert "OPERATOR_ID_MISSING" in missing_reasons
    assert "OPERATOR_INTENT_NOT_ALLOWED" in missing_reasons

    malformed = build_manual_intent_v1(
        operator_id="operator-1",
        operator_intent="REVIEW_COMPLETED",
        intent_timestamp_utc="not-a-time",
        day_utc=DAY,
        reason="Malformed timestamp test.",
        runtime_evaluation_hash=runtime_hash,
    )
    assert "INTENT_TIMESTAMP_MALFORMED" in validate_manual_intent_v1(malformed, day_utc=DAY, runtime_evaluation_hash=runtime_hash, generated_at_utc=NOW)

    stale = build_manual_intent_v1(
        operator_id="operator-1",
        operator_intent="REPORT_ACKNOWLEDGED",
        intent_timestamp_utc="2026-05-01T00:00:00Z",
        day_utc=DAY,
        reason="Stale intent test.",
        runtime_evaluation_hash=runtime_hash,
    )
    assert "INTENT_TIMESTAMP_STALE" in validate_manual_intent_v1(stale, day_utc=DAY, runtime_evaluation_hash=runtime_hash, generated_at_utc=NOW)

    write_manual_intent_v1(truth_root=tmp_path, payload=stale)
    blocked = emit_manual_action_event_v1(truth_root=tmp_path, payload=stale, runtime_evaluation_hash=runtime_hash, generated_at_utc=NOW, git_sha="abc1234")
    assert blocked["event_type"] == "ManualActionBlocked"
    assert "INTENT_TIMESTAMP_STALE" in blocked["manual_intent_validation_reasons"]


def test_manual_action_accepted_event_for_valid_intent(tmp_path: Path) -> None:
    runtime_hash = "h" * 64
    payload = build_manual_intent_v1(
        operator_id="operator-1",
        operator_intent="NONE_DECLARED",
        intent_timestamp_utc=NOW,
        day_utc=DAY,
        reason="No manual declaration.",
        runtime_evaluation_hash=runtime_hash,
    )
    write_manual_intent_v1(truth_root=tmp_path, payload=payload)

    event = emit_manual_action_event_v1(truth_root=tmp_path, payload=payload, runtime_evaluation_hash=runtime_hash, generated_at_utc=NOW, git_sha="abc1234")

    assert event["event_type"] == "ManualActionAccepted"
    assert event["input_hashes"]["runtime_evaluation_hash"] == runtime_hash
    assert event["input_hashes"]["operator_intent_hash"] == payload["acknowledgment_hash"]



def test_critical_bridge_usage_reports_only_active_latest_bridge_dependency() -> None:
    bridge = _contract_event(schema_id="event_rules_registry", event_id="bridge-old", producer="ops.aegis.runtime_truth_kernel_v1.artifact_scan")
    bridge["created_at_utc"] = "2026-05-15T00:00:00Z"
    native = _contract_event(schema_id="event_rules_registry", event_id="native-new", producer="ops/tools/run_aegis_event_monitor_v1.py:event_rules_registry")
    native["created_at_utc"] = "2026-05-15T01:00:00Z"

    report = critical_bridge_usage_report_v1(events=[bridge, native])

    assert report["critical_bridge_usage_count"] == 0
    assert report["historical_critical_bridge_event_count"] == 1


def test_target_producers_use_native_event_transaction_hooks() -> None:
    targets = [
        Path("ops/tools/build_event_market_snapshot_v1.py"),
        Path("ops/tools/build_ai_eod_feedback_review_v1.py"),
        Path("ops/tools/audit_research_dataset_bindings_v1.py"),
        Path("ops/tools/run_research_test_queue_v1.py"),
        Path("ops/tools/run_aegis_lite_eod_pipeline_v1.py"),
        Path("ops/tools/run_aegis_event_monitor_v1.py"),
    ]
    for path in targets:
        source = path.read_text(encoding="utf-8")
        assert "emit_artifact_evidence_transaction_v1" in source



def _repair_attempt_for(result: dict, schema_id: str) -> dict:
    return next(row for row in result["attempts"] if row["schema_id"] == schema_id)


def _patch_event_validity_repair_contract(monkeypatch: pytest.MonkeyPatch, producer: Path) -> None:
    monkeypatch.setattr(
        repair_runtime,
        "repairable_contracts_by_schema_v1",
        lambda _registry=None: {
            "event_validity_gate": {
                "producer_id": "ops/tools/write_event_validity_evidence_v1.py",
                "producer_version": "v1",
                "repair_class": "AUTO_DETERMINISTIC",
                "command": f"python3 {producer} --truth_root {{truth_root}} --day_utc {{day_utc}}",
                "inputs": [],
                "outputs": ["reports/event_validity_gate_v1/{day_utc}/index/event_validity_gate.v1.json"],
            }
        },
    )


def _write_fake_repair_producer(path: Path, *, event_type: str = "EvidenceValidated", exit_code: int = 0, emit_event: bool = True) -> None:
    validation_literal = "VALID" if event_type == "EvidenceValidated" else "INVALID"
    body = f"""
from pathlib import Path
import sys
sys.path.insert(0, "{Path.cwd()}")
from ops.aegis.evidence_event_store_v1 import append_evidence_event_v1

root = Path(sys.argv[sys.argv.index("--truth_root") + 1]).resolve()
day = sys.argv[sys.argv.index("--day_utc") + 1]
if {repr(emit_event)}:
    append_evidence_event_v1(
        truth_root=root,
        day_utc=day,
        event={{
            "event_id": "fake-repair-{event_type}",
            "event_type": "{event_type}",
            "run_id": "fake-repair-run",
            "parent_run_id": "",
            "day_utc": day,
            "created_at_utc": "{NOW}",
            "producer": "ops/tools/write_event_validity_evidence_v1.py",
            "producer_version": "v1",
            "git_sha": "abc1234",
            "schema_id": "event_validity_gate",
            "schema_version": "v1",
            "input_hashes": {{"input": "b" * 64}},
            "output_hashes": {{str(root / "reports/event_validity_gate_v1" / day / "index/event_validity_gate.v1.json"): "a" * 64}},
            "artifact_paths": [str(root / "reports/event_validity_gate_v1" / day / "index/event_validity_gate.v1.json")],
            "validation_status": "{validation_literal}",
            "previous_event_hash": "",
            "event_hash": "",
        }},
    )
raise SystemExit({exit_code})
"""
    path.write_text(body, encoding="utf-8")


def test_repair_exit_zero_rejected_evidence_is_not_succeeded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    producer = tmp_path / "producer_rejected.py"
    _write_fake_repair_producer(producer, event_type="EvidenceRejected", exit_code=0)
    _patch_event_validity_repair_contract(monkeypatch, producer)

    result = run_repair_orchestration_v1(
        truth_root=tmp_path,
        repo_root=Path.cwd(),
        day_utc=DAY,
        mode="execute",
        repair_class="AUTO_DETERMINISTIC",
        run_id="repair-rejected-run",
        parent_run_id="",
        generated_at_utc=NOW,
        git_sha="abc1234",
    )

    attempt = _repair_attempt_for(result, "event_validity_gate")
    assert attempt["command_success"] is True
    assert attempt["event_emission_success"] is True
    assert attempt["evidence_validation_status"] == "REJECTED"
    assert attempt["status"] == "RepairCompletedWithRejectedEvidence"
    assert attempt["blocker_state_after"] == "REJECTED"
    repair_types = [row["event_type"] for row in rebuild_evidence_snapshot_v1(truth_root=tmp_path, day_utc=DAY)["events"] if row["producer"] == "ops.aegis.repair_orchestrator_v1"]
    assert "RepairCompletedWithRejectedEvidence" in repair_types
    assert "RepairSucceeded" not in repair_types


def test_repair_exit_zero_validated_evidence_and_blocker_cleared_succeeds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    producer = tmp_path / "producer_validated.py"
    _write_fake_repair_producer(producer, event_type="EvidenceValidated", exit_code=0)
    _patch_event_validity_repair_contract(monkeypatch, producer)

    result = run_repair_orchestration_v1(
        truth_root=tmp_path,
        repo_root=Path.cwd(),
        day_utc=DAY,
        mode="execute",
        repair_class="AUTO_DETERMINISTIC",
        run_id="repair-success-run",
        parent_run_id="",
        generated_at_utc=NOW,
        git_sha="abc1234",
    )

    attempt = _repair_attempt_for(result, "event_validity_gate")
    assert attempt["command_success"] is True
    assert attempt["event_emission_success"] is True
    assert attempt["evidence_validation_status"] == "VALIDATED"
    assert attempt["blocker_state_after"] == "VALIDATED"
    assert attempt["status"] == "RepairSucceeded"


def test_repair_exit_nonzero_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    producer = tmp_path / "producer_nonzero.py"
    _write_fake_repair_producer(producer, event_type="EvidenceValidated", exit_code=7)
    _patch_event_validity_repair_contract(monkeypatch, producer)

    result = run_repair_orchestration_v1(
        truth_root=tmp_path,
        repo_root=Path.cwd(),
        day_utc=DAY,
        mode="execute",
        repair_class="AUTO_DETERMINISTIC",
        run_id="repair-nonzero-run",
        parent_run_id="",
        generated_at_utc=NOW,
        git_sha="abc1234",
    )

    attempt = _repair_attempt_for(result, "event_validity_gate")
    assert attempt["command_success"] is False
    assert attempt["event_emission_success"] is True
    assert attempt["status"] == "RepairFailed"
    assert attempt["validation_status"] == "COMMAND_FAILED"


def test_repair_exit_zero_without_event_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    producer = tmp_path / "producer_no_event.py"
    _write_fake_repair_producer(producer, exit_code=0, emit_event=False)
    _patch_event_validity_repair_contract(monkeypatch, producer)

    result = run_repair_orchestration_v1(
        truth_root=tmp_path,
        repo_root=Path.cwd(),
        day_utc=DAY,
        mode="execute",
        repair_class="AUTO_DETERMINISTIC",
        run_id="repair-no-event-run",
        parent_run_id="",
        generated_at_utc=NOW,
        git_sha="abc1234",
    )

    attempt = _repair_attempt_for(result, "event_validity_gate")
    assert attempt["command_success"] is True
    assert attempt["event_emission_success"] is False
    assert attempt["evidence_validation_status"] == "NO_EVENT"
    assert attempt["status"] == "RepairFailed"
    assert attempt["validation_status"] == "NO_EVIDENCE_EVENT_EMITTED"

from ops.tools import write_manual_intent_v1 as manual_intent_cli


def _persist_canonical_runtime_for_manual_intent(root: Path, *, generated_at: str = NOW) -> dict:
    evaluation, policy = build_ledger_runtime_evaluation_v1(
        truth_root=root,
        day_utc=DAY,
        generated_at_utc=generated_at,
        run_id="manual-intent-runtime",
        parent_run_id="",
    )
    eval_path = runtime_evaluation_path_v1(truth_root=root, day_utc=DAY)
    policy_path = runtime_policy_bundle_path_v1(truth_root=root, day_utc=DAY)
    eval_path.parent.mkdir(parents=True, exist_ok=True)
    eval_path.write_bytes(stable_json_bytes_v1(evaluation) + b"\n")
    policy_path.write_bytes(stable_json_bytes_v1(policy) + b"\n")
    return evaluation


def _run_manual_intent(root: Path, *, intent: str, runtime_hash: str, mode: str = "execute", operator_id: str = "operator-1", timestamp: str = NOW) -> tuple[int, dict]:
    rc = manual_intent_cli.main(
        [
            "--truth-root",
            str(root),
            "--day-utc",
            DAY,
            "--operator-id",
            operator_id,
            "--operator-intent",
            intent,
            "--reason",
            "Test operator intent assertion.",
            "--runtime-evaluation-hash",
            runtime_hash,
            "--intent-timestamp-utc",
            timestamp,
            "--mode",
            mode,
        ]
    )
    return rc, {}


def test_write_manual_intent_dry_run_writes_no_events(tmp_path: Path, capsys) -> None:
    evaluation = _persist_canonical_runtime_for_manual_intent(tmp_path)

    rc = manual_intent_cli.main(
        [
            "--truth-root",
            str(tmp_path),
            "--day-utc",
            DAY,
            "--operator-id",
            "operator-1",
            "--operator-intent",
            "NONE_DECLARED",
            "--reason",
            "No manual execution occurred.",
            "--runtime-evaluation-hash",
            evaluation["deterministic_output_hash"],
            "--intent-timestamp-utc",
            NOW,
            "--mode",
            "dry-run",
        ]
    )

    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["ok"] is True
    assert out["target_schema_id"] == "manual_execution_receipt"
    assert evidence_events_path_v1(truth_root=tmp_path, day_utc=DAY).exists() is False


def test_write_manual_intent_rejects_missing_operator_wrong_hash_and_future(tmp_path: Path, capsys) -> None:
    evaluation = _persist_canonical_runtime_for_manual_intent(tmp_path)
    base = [
        "--truth-root",
        str(tmp_path),
        "--day-utc",
        DAY,
        "--operator-id",
        "",
        "--operator-intent",
        "NONE_DECLARED",
        "--reason",
        "No manual execution occurred.",
        "--runtime-evaluation-hash",
        evaluation["deterministic_output_hash"],
        "--intent-timestamp-utc",
        NOW,
        "--mode",
        "dry-run",
    ]
    assert manual_intent_cli.main(base) == 2
    missing = json.loads(capsys.readouterr().out)
    assert "OPERATOR_ID_MISSING" in missing["validation_errors"]

    wrong_hash = list(base)
    wrong_hash[wrong_hash.index(evaluation["deterministic_output_hash"])] = "bad" * 20
    wrong_hash[wrong_hash.index("")]= "operator-1"
    assert manual_intent_cli.main(wrong_hash) == 2
    wrong = json.loads(capsys.readouterr().out)
    assert "RUNTIME_EVALUATION_HASH_MISMATCH" in wrong["validation_errors"]

    future = list(base)
    future[future.index("")] = "operator-1"
    future[future.index(NOW)] = "2999-01-01T00:00:00Z"
    assert manual_intent_cli.main(future) == 2
    future_out = json.loads(capsys.readouterr().out)
    assert "INTENT_TIMESTAMP_FUTURE" in future_out["validation_errors"]


def test_none_declared_manual_intent_clears_manual_execution_receipt_only(tmp_path: Path, capsys) -> None:
    evaluation = _persist_canonical_runtime_for_manual_intent(tmp_path)

    rc = manual_intent_cli.main(
        [
            "--truth-root",
            str(tmp_path),
            "--day-utc",
            DAY,
            "--operator-id",
            "operator-1",
            "--operator-intent",
            "NONE_DECLARED",
            "--reason",
            "No manual execution occurred.",
            "--runtime-evaluation-hash",
            evaluation["deterministic_output_hash"],
            "--intent-timestamp-utc",
            NOW,
            "--mode",
            "execute",
        ]
    )

    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["target_schema_id"] == "manual_execution_receipt"
    assert out["manual_action_event_id"].startswith("ManualActionAccepted:")
    assert len(out["target_evidence_event_ids"]) == 2
    snapshot = rebuild_evidence_snapshot_v1(truth_root=tmp_path, day_utc=DAY)
    policy = runtime_policy_bundle_v1(run_id="after-manual", parent_run_id="", generated_at_utc=NOW, git_sha="abc1234")
    after = evaluate_runtime(DAY, snapshot, policy)
    receipt_path = next(tmp_path.rglob("manual_execution_receipt.v1.json"))
    validate_against_repo_schema_v1(json.loads(receipt_path.read_text(encoding="utf-8")), REPO_ROOT, "governance/04_DATA/SCHEMAS/C2/REPORTS/manual_execution_receipt.v1.schema.json")
    assert any(ref["schema_id"] == "manual_execution_receipt" for ref in after["source_evidence_refs"])
    assert not any(row.get("schema_id") == "manual_execution_receipt" for row in after["blocker_state"])
    assert any(row.get("schema_id") == "aegis_lite_eod_report" for row in after["blocker_state"])
    assert after["capabilities"]["TRADE_ADVICE_ALLOWED"]["allowed"] is False
    assert after["capabilities"]["AUTONOMOUS_EXECUTION_ALLOWED"]["allowed"] is False


def test_report_acknowledged_manual_intent_clears_aegis_lite_eod_report_only(tmp_path: Path, capsys) -> None:
    evaluation = _persist_canonical_runtime_for_manual_intent(tmp_path)

    rc = manual_intent_cli.main(
        [
            "--truth-root",
            str(tmp_path),
            "--day-utc",
            DAY,
            "--operator-id",
            "operator-1",
            "--operator-intent",
            "REPORT_ACKNOWLEDGED",
            "--reason",
            "EOD report reviewed.",
            "--runtime-evaluation-hash",
            evaluation["deterministic_output_hash"],
            "--intent-timestamp-utc",
            NOW,
            "--mode",
            "execute",
        ]
    )

    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["target_schema_id"] == "aegis_lite_eod_report"
    snapshot = rebuild_evidence_snapshot_v1(truth_root=tmp_path, day_utc=DAY)
    policy = runtime_policy_bundle_v1(run_id="after-eod", parent_run_id="", generated_at_utc=NOW, git_sha="abc1234")
    after = evaluate_runtime(DAY, snapshot, policy)
    report_path = next(tmp_path.rglob("aegis_lite_eod_report.v1.json"))
    validate_against_repo_schema_v1(json.loads(report_path.read_text(encoding="utf-8")), REPO_ROOT, "governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_lite_eod_report.v1.schema.json")
    assert any(ref["schema_id"] == "aegis_lite_eod_report" for ref in after["source_evidence_refs"])
    assert not any(row.get("schema_id") == "aegis_lite_eod_report" for row in after["blocker_state"])
    assert any(row.get("schema_id") == "manual_execution_receipt" for row in after["blocker_state"])


def test_malformed_manual_intent_execute_emits_blocked_event(tmp_path: Path, capsys) -> None:
    evaluation = _persist_canonical_runtime_for_manual_intent(tmp_path)

    rc = manual_intent_cli.main(
        [
            "--truth-root",
            str(tmp_path),
            "--day-utc",
            DAY,
            "--operator-id",
            "",
            "--operator-intent",
            "NONE_DECLARED",
            "--reason",
            "Malformed missing operator.",
            "--runtime-evaluation-hash",
            evaluation["deterministic_output_hash"],
            "--intent-timestamp-utc",
            NOW,
            "--mode",
            "execute",
        ]
    )

    assert rc == 2
    err = json.loads(capsys.readouterr().err)
    assert err["manual_action_event_id"].startswith("ManualActionBlocked:")
    events = rebuild_evidence_snapshot_v1(truth_root=tmp_path, day_utc=DAY)["events"]
    assert any(row["event_type"] == "ManualActionBlocked" for row in events)
    assert not any(row["schema_id"] == "manual_execution_receipt" and row["event_type"] == "EvidenceValidated" for row in events)


def test_replay_after_manual_intents_passes_and_authority_hashes_match(tmp_path: Path, capsys) -> None:
    evaluation = _persist_canonical_runtime_for_manual_intent(tmp_path)
    for intent in ("NONE_DECLARED", "REVIEW_COMPLETED"):
        assert manual_intent_cli.main(
            [
                "--truth-root",
                str(tmp_path),
                "--day-utc",
                DAY,
                "--operator-id",
                "operator-1",
                "--operator-intent",
                intent,
                "--reason",
                f"{intent} test assertion.",
                "--runtime-evaluation-hash",
                evaluation["deterministic_output_hash"],
                "--intent-timestamp-utc",
                NOW,
                "--mode",
                "execute",
            ]
        ) == 0
        capsys.readouterr()
    after_eval, after_policy = build_ledger_runtime_evaluation_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW, run_id="manual-after", parent_run_id="")
    runtime_evaluation_path_v1(truth_root=tmp_path, day_utc=DAY).write_bytes(stable_json_bytes_v1(after_eval) + b"\n")
    runtime_policy_bundle_path_v1(truth_root=tmp_path, day_utc=DAY).write_bytes(stable_json_bytes_v1(after_policy) + b"\n")
    bundle = build_aegis_audit_bundle_v1(truth_root=tmp_path, day_utc=DAY, run_id="manual-bundle", parent_run_id="", generated_at_utc=NOW, command_args=["--day-utc", DAY])
    replay = replay_aegis_runtime_v1(audit_bundle=Path(bundle["bundle_path"]))
    ui = build_readiness_kernel_v1(DAY, truth_root=tmp_path, sleeve_truth_root=tmp_path / "sleeve")
    packet = build_aegis_chatgpt_control_packet_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert replay["ok"] is True
    assert replay["runtime_evaluation_hash"] == after_eval["deterministic_output_hash"]
    assert ui["runtime_evaluation_hash"] == after_eval["deterministic_output_hash"]
    assert packet["runtime_evaluation_hash"] == after_eval["deterministic_output_hash"]
    assert after_eval["capabilities"]["TRADE_ADVICE_ALLOWED"]["allowed"] is False
    assert after_eval["capabilities"]["AUTONOMOUS_EXECUTION_ALLOWED"]["allowed"] is False
