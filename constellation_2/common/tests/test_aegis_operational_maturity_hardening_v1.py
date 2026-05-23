from __future__ import annotations

import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.evidence_event_store_v1 import append_evidence_event_v1  # noqa: E402
from ops.aegis.final_eod_orchestrator_v1 import final_run_outcome_v1, ledger_jsonl_path_v1, ledger_latest_path_v1  # noqa: E402
from ops.aegis.operational_maturity_hardening_v1 import (  # noqa: E402
    build_evidence_lifecycle_projection_v1,
    build_capture_ticket_projection_v1,
    build_orchestrator_resilience_projection_v1,
    build_promoted_candidate_lifecycle_proof_v1,
    build_provider_resilience_projection_v1,
    classify_operational_readiness_v1,
)


DAY = "2026-05-22"
NOW = "2026-05-22T21:00:00Z"


def _event(schema_id: str, event_type: str = "EvidenceValidated", validation_status: str = "VALID", created_at: str = NOW) -> dict:
    return {
        "event_id": f"{event_type}:{schema_id}:{created_at}",
        "event_type": event_type,
        "run_id": "run-1",
        "parent_run_id": "",
        "day_utc": DAY,
        "created_at_utc": created_at,
        "producer": "ops.aegis.runtime_truth_kernel_v1.artifact_scan",
        "producer_version": "aegis_runtime_truth_kernel.v1",
        "git_sha": "abc123",
        "schema_id": schema_id,
        "schema_version": "v1",
        "input_hashes": {},
        "output_hashes": {f"{schema_id}.json": "a" * 64},
        "artifact_paths": [f"/tmp/reports/{schema_id}_v1/{DAY}/{schema_id}.v1.json"],
        "validation_status": validation_status,
        "previous_event_hash": "",
        "event_hash": "",
    }


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def test_evidence_lifecycle_registry_marks_consumed_verified_and_missing_required(tmp_path: Path) -> None:
    append_evidence_event_v1(truth_root=tmp_path, day_utc=DAY, event=_event("research_task_queue", "EvidenceProduced", "PRODUCED", "2026-05-22T20:59:00Z"))
    append_evidence_event_v1(truth_root=tmp_path, day_utc=DAY, event=_event("research_task_queue", "EvidenceValidated", "VALID", "2026-05-22T21:00:00Z"))
    append_evidence_event_v1(truth_root=tmp_path, day_utc=DAY, event=_event("event_market_snapshot", "EvidenceValidated", "VALID", "2026-05-22T21:00:01Z"))
    append_evidence_event_v1(truth_root=tmp_path, day_utc=DAY, event=_event("alert_transport_proof", "EvidenceRejected", "INVALID", "2026-05-22T21:00:02Z"))

    projection = build_evidence_lifecycle_projection_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        runtime_evaluation={"source_evidence_refs": [{"schema_id": "research_task_queue"}], "capabilities": {}},
        generated_at_utc=NOW,
    )

    statuses = {item["lineage"]["event_id"].split(":")[1]: item["status"] for item in projection["items"]}
    assert statuses["research_task_queue"] == "CONSUMED"
    assert statuses["event_market_snapshot"] == "VERIFIED"
    assert statuses["alert_transport_proof"] == "REJECTED"
    assert "manual_execution_receipt" in projection["missing_required_replay_evidence"]


def test_promoted_candidate_lifecycle_distinguishes_normal_no_op_from_blocked(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "reports" / "candidate_consumption_audit_v1" / DAY / "run-1" / "candidate_consumption_audit.v1.json",
        {
            "raw_candidate_count": 3,
            "promoted_candidate_count": 0,
            "excluded_candidate_count": 3,
            "consumption_counts": {"EXCLUDED_LOW_SCORE": 3},
            "normal_no_op": True,
        },
    )
    _write_json(
        tmp_path / "reports" / "aegis_lite_eod_report_v1" / DAY / "run-1" / "aegis_lite_eod_report.v1.json",
        {"eod_outcome_status": "NORMAL_NO_OP_NO_PROMOTED_CANDIDATES"},
    )

    proof = build_promoted_candidate_lifecycle_proof_v1(truth_root=tmp_path, day_utc=DAY)

    assert proof["status"] == "NORMAL_NO_OP_NO_PROMOTABLE_CANDIDATES"
    assert proof["raw_candidate_count"] == 3
    assert proof["excluded_count_by_reason"] == {"EXCLUDED_LOW_SCORE": 3}
    assert proof["broker_submit_required"] is False


def test_provider_resilience_uses_certified_final_eod_artifact_not_stale_registry(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "reports" / "aegis_data_registry_v1" / DAY / "data_registry.v1.json",
        {
            "provider_status": [{"provider": "OLD", "request_status": "FAILED"}],
            "missing_symbols": ["XLB"],
            "fetched_symbols": [],
        },
    )
    _write_json(
        tmp_path / "reports" / "final_eod_market_data_v1" / DAY / "final_eod_market_data.v1.json",
        {
            "status": "CURRENT",
            "validation_status": "VALID",
            "symbols": ["SPY", "VIX"],
            "fetched_symbols": ["SPY", "VIX"],
            "missing_symbols": [],
            "stale_symbols": [],
            "provider_failed_symbols": [],
            "provider_results": [
                {"provider": "TIINGO", "request_status": "SUCCESS", "missing_symbols": ["VIX"]},
                {"provider": "CBOE", "request_status": "SUCCESS", "fetched_symbols": ["VIX"]},
            ],
        },
    )

    provider = build_provider_resilience_projection_v1(truth_root=tmp_path, day_utc=DAY)

    assert provider["status"] == "HEALTHY"
    assert provider["rolling_failure_count"] == 0
    assert provider["coverage"]["missing_symbols"] == []


def test_orchestrator_outcome_classifies_partial_abort_and_completion(tmp_path: Path) -> None:
    assert final_run_outcome_v1(status="SUCCEEDED", stages=[{"status": "SUCCEEDED"}]) == "RUN_COMPLETED"
    assert final_run_outcome_v1(status="INTERRUPTED", stages=[{"status": "SUCCEEDED"}]) == "RUN_ABORTED"
    assert final_run_outcome_v1(status="FAILED", stages=[{"status": "SUCCEEDED"}, {"status": "FAILED"}]) == "RUN_PARTIAL"

    latest = {
        "run_id": "run-1",
        "day_utc": DAY,
        "status": "FAILED",
        "final_run_outcome": "RUN_PARTIAL",
        "failure_reason": "DOMAIN_CERTIFICATION failed",
    }
    _write_json(ledger_latest_path_v1(truth_root=tmp_path, day_utc=DAY), latest)
    ledger = ledger_jsonl_path_v1(truth_root=tmp_path, day_utc=DAY)
    ledger.parent.mkdir(parents=True, exist_ok=True)
    ledger.write_text(
        json.dumps({"run_id": "run-1", "status": "SUCCEEDED", "stage": "EOD_SOURCE_CERTIFY"}) + "\n"
        + json.dumps({"run_id": "run-1", "status": "FAILED", "stage": "DOMAIN_CERTIFICATION"}) + "\n",
        encoding="utf-8",
    )

    projection = build_orchestrator_resilience_projection_v1(truth_root=tmp_path, day_utc=DAY)

    assert projection["final_run_outcome"] == "RUN_PARTIAL"
    assert projection["stage_checkpointing_present"] is True
    assert projection["stale_run_cleanup_needed"] is True


def test_operational_readiness_uses_explicit_categories_not_vague_blocked() -> None:
    kernel = {"blocked_capabilities": ["DATA_READY"], "allowed_capabilities": []}
    runtime_eval = {"capabilities": {"DATA_READY": {"allowed": False}}, "deterministic_output_hash": "a" * 64}
    replay = {"status": "DETERMINISTIC"}
    evidence = {"missing_required_replay_evidence": []}

    result = classify_operational_readiness_v1(
        kernel=kernel,
        runtime_evaluation=runtime_eval,
        evidence_projection=evidence,
        replay_projection=replay,
    )

    assert result["classification"] == "BLOCKED_DATA"
    assert result["classification"] != "BLOCKED"



def test_no_promotable_candidates_split_capability_from_ticket_availability() -> None:
    runtime_eval = {"capabilities": {"MANUAL_TRADE_CAPTURE_ALLOWED": {"allowed": True}}}
    promoted = {"promoted_candidate_count": 0}
    receipts = {"recommendation_count": 0, "status": "NO_MANUAL_EXECUTION_DECLARED"}

    ticket = build_capture_ticket_projection_v1(runtime_evaluation=runtime_eval, promoted=promoted, manual_receipts=receipts)
    readiness = classify_operational_readiness_v1(
        kernel={"blocked_capabilities": []},
        runtime_evaluation=runtime_eval,
        evidence_projection={"missing_required_replay_evidence": []},
        replay_projection={"status": "DETERMINISTIC"},
        capture_ticket_projection=ticket,
    )

    assert ticket["platform_capture_capability"] == "READY"
    assert ticket["capture_ticket_count"] == 0
    assert ticket["capture_ticket_status"] == "NONE_AVAILABLE"
    assert ticket["operator_next_action"] == "No action required"
    assert readiness["classification"] == "MANUAL_CAPTURE_CAPABILITY_READY"
    assert "capture_ticket_count=0" in readiness["reason"]


def test_promoted_candidate_creates_ticket_ready_semantics() -> None:
    runtime_eval = {"capabilities": {"MANUAL_TRADE_CAPTURE_ALLOWED": {"allowed": True}}}

    ticket = build_capture_ticket_projection_v1(
        runtime_evaluation=runtime_eval,
        promoted={"promoted_candidate_count": 1},
        manual_receipts={"recommendation_count": 1, "status": "RECEIPT_REQUIRED"},
    )

    assert ticket["platform_capture_capability"] == "READY"
    assert ticket["capture_ticket_count"] == 1
    assert ticket["capture_ticket_status"] == "TICKET_READY"


def test_manual_receipt_completed_ticket_semantics() -> None:
    runtime_eval = {"capabilities": {"MANUAL_TRADE_CAPTURE_ALLOWED": {"allowed": True}}}

    ticket = build_capture_ticket_projection_v1(
        runtime_evaluation=runtime_eval,
        promoted={"promoted_candidate_count": 1},
        manual_receipts={"recommendation_count": 1, "status": "VERIFIED"},
    )

    assert ticket["capture_ticket_status"] == "TICKET_COMPLETED"
    assert ticket["operator_next_action"] == "No action required"

def test_policy_disabled_trade_advice_evidence_is_not_required_for_current_mode(tmp_path: Path) -> None:
    from ops.aegis.evidence_event_store_v1 import rebuild_evidence_snapshot_v1
    from ops.aegis.pure_runtime_evaluator_v1 import evaluate_runtime, runtime_policy_bundle_v1

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
        "alert_transport_proof",
    ):
        append_evidence_event_v1(truth_root=tmp_path, day_utc=DAY, event=_event(schema_id))

    snapshot = rebuild_evidence_snapshot_v1(truth_root=tmp_path, day_utc=DAY)
    policy = runtime_policy_bundle_v1(run_id="run-policy", parent_run_id="", generated_at_utc=NOW, git_sha="abc123")
    evaluation = evaluate_runtime(DAY, snapshot, policy)

    assert evaluation["runtime_truth_classification"] == "REAL_RUNTIME"
    assert evaluation["highest_readiness_layer"] == "MANUAL_TRADE_CAPTURE_ALLOWED"
    blocker_ids = {row["blocker_id"] for row in evaluation["blockers"]}
    assert "EVIDENCE_MISSING:promoted_candidate_evidence" not in blocker_ids
    assert "EVIDENCE_MISSING:manual_trade_packet" not in blocker_ids
    assert evaluation["capabilities"]["TRADE_ADVICE_ALLOWED"]["allowed"] is False
    assert evaluation["capabilities"]["AUTONOMOUS_EXECUTION_ALLOWED"]["allowed"] is False


def test_alert_transport_proof_cli_emits_canonical_events(tmp_path: Path, capsys) -> None:
    from ops.aegis.evidence_event_store_v1 import rebuild_evidence_snapshot_v1
    from ops.tools import write_alert_transport_proof_v1 as alert_cli

    rc = alert_cli.main(["--truth_root", str(tmp_path), "--day_utc", DAY, "--transport_mode", "GATE_ONLY"])

    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["live_transport_claim_allowed"] is False
    events = rebuild_evidence_snapshot_v1(truth_root=tmp_path, day_utc=DAY)["events"]
    assert [row["event_type"] for row in events] == ["EvidenceProduced", "EvidenceValidated"]
    assert events[-1]["schema_id"] == "alert_transport_proof"
