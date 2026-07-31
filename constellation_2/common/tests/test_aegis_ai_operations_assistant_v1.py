from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.ai_operations_assistant_v1 import (
    answer_ai_operations_question_v1,
    build_ai_operations_context_v1,
    build_ai_operations_response_v1,
    run_ai_operations_self_check_v1,
    write_ai_operations_context_v1,
    write_ai_operations_response_v1,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _seed_operational_sources(root: Path, day: str = "2026-05-29") -> None:
    reports = root / "reports"
    _write_json(reports / "aegis_verified_runtime_graph_v1" / day / "verified_runtime_graph.v1.json", {
        "day_utc": day,
        "graph_status": "READY",
        "runtime_readiness_status": "BLOCKED",
        "active_mode_readiness_status": "BLOCKED",
        "runtime_blockers": ["runtime_truth_kernel:DATA_READY:BLOCKED_BY_KERNEL"],
        "audit_blockers": [],
        "policy_gates": {
            "trade_advice_allowed": False,
            "broker_submit_transmit_policy": "DISABLED_BY_DESIGN",
            "autonomous_execution_policy": "DISABLED_BY_DESIGN",
        },
    })
    _write_json(reports / "aegis_chatgpt_control_packet_v1" / day / "aegis_chatgpt_control_packet.v1.json", {
        "day_utc": day,
        "runtime_truth_classification": "PARTIAL_CONTEXT",
        "reason_if_blocked": "KERNEL_BLOCKED_TRADE_ADVICE:RUNTIME_EVALUATION",
        "trade_advice_allowed": False,
        "manual_trade_capture_allowed": False,
        "autonomous_execution_allowed": False,
        "next_operator_actions": ["Regenerate market data coverage; validate with npm run aegis:audit."],
        "stale_or_missing_sources": [{"artifact_id": "market_data_coverage", "status": "MISSING"}],
    })
    handoff = reports / "aegis_audit_handoff_v1" / day / "aegis_audit_handoff.txt"
    handoff.parent.mkdir(parents=True, exist_ok=True)
    handoff.write_text("runtime_truth_classification: PARTIAL_CONTEXT\nadvisory_status: ADVISORY_BLOCKED_BY_STALE_EVIDENCE\noperator_action_required: true\n", encoding="utf-8")
    hydrate = reports / "aegis_verified_runtime_graph_v1" / day / "chatgpt_hydrate_packet.v1.md"
    hydrate.write_text("# hydrate\nVerified runtime graph summary\n", encoding="utf-8")
    _write_json(reports / "aegis_command_center_queue_audit_v1" / day / "command_center_queue_audit.v1.json", {
        "summary": {"total_rows": 40, "operator_action_required_count": 1, "needs_attention_rows_audited": 1}
    })
    _write_json(reports / "aegis_canonical_operator_state_v1" / day / "canonical_operator_state.v1.json", {
        "day_utc": day,
        "generated_at_utc": "2026-05-29T14:00:00Z",
        "actions_required": [{"title": "Review blocker"}],
        "missing_inputs": [],
        "warnings": [],
    })


def test_ai_operations_context_is_deterministic_and_records_sources(tmp_path: Path):
    _seed_operational_sources(tmp_path)
    first = build_ai_operations_context_v1(truth_root=tmp_path, day_utc="2026-05-29")
    second = build_ai_operations_context_v1(truth_root=tmp_path, day_utc="2026-05-29")

    assert first["context_hash"] == second["context_hash"]
    assert first["data_quality_status"] == "CANONICAL"
    assert first["summary"]["evidence_count"] >= 6
    assert "verified_runtime_graph" in first["source_artifact_hashes"]
    assert first["safety"]["trade_advice_allowed"] is False


def test_ai_operations_response_answers_operational_question_from_evidence(tmp_path: Path):
    _seed_operational_sources(tmp_path)
    payload = build_ai_operations_response_v1(
        truth_root=tmp_path,
        day_utc="2026-05-29",
        question="What should be fixed first?",
    )
    response = payload["latest_response"]

    assert response["intent"] == "FIX_FIRST"
    assert response["confidence"] == "HIGH"
    assert response["context_day"] == "2026-05-29"
    assert response["source_day"] == "2026-05-29"
    assert "Regenerate market data coverage" in response["answer"]
    assert response["unsupported_claims"] == []
    assert response["source_artifacts"]
    assert response["safety"]["broker_execution_allowed"] is False


def test_ai_operations_phase1_rejects_entity_specific_questions(tmp_path: Path):
    _seed_operational_sources(tmp_path)
    context = build_ai_operations_context_v1(truth_root=tmp_path, day_utc="2026-05-29")
    response = answer_ai_operations_question_v1(context=context, question="Why is this sleeve underperforming?")

    assert response["intent"] == "UNSUPPORTED_PHASE1_SCOPE"
    assert response["confidence"] == "LOW"
    assert response["unsupported_claims"]
    assert "Phase 1" in response["answer"]


def test_ai_operations_self_check_passes_for_grounded_operational_response(tmp_path: Path):
    _seed_operational_sources(tmp_path)
    context = build_ai_operations_context_v1(truth_root=tmp_path, day_utc="2026-05-29")
    write_ai_operations_context_v1(truth_root=tmp_path, day_utc="2026-05-29", payload=context)
    response = build_ai_operations_response_v1(truth_root=tmp_path, day_utc="2026-05-29", question="What happened?")
    write_ai_operations_response_v1(truth_root=tmp_path, day_utc="2026-05-29", payload=response)

    check = run_ai_operations_self_check_v1(truth_root=tmp_path, day_utc="2026-05-29")

    assert check["ok"] is True
    assert check["failures"] == []


def test_ai_operations_self_check_refreshes_stale_saved_context(tmp_path: Path):
    day = "2026-05-29"
    _seed_operational_sources(tmp_path, day=day)
    context = build_ai_operations_context_v1(truth_root=tmp_path, day_utc=day)
    write_ai_operations_context_v1(truth_root=tmp_path, day_utc=day, payload=context)
    response = build_ai_operations_response_v1(truth_root=tmp_path, day_utc=day, question="What happened?")
    write_ai_operations_response_v1(truth_root=tmp_path, day_utc=day, payload=response)

    queue_path = tmp_path / "reports" / "aegis_command_center_queue_audit_v1" / day / "command_center_queue_audit.v1.json"
    _write_json(queue_path, {"summary": {"total_rows": 99, "operator_action_required_count": 0, "needs_attention_rows_audited": 0}})

    check = run_ai_operations_self_check_v1(truth_root=tmp_path, day_utc=day)
    refreshed_context = json.loads((tmp_path / "reports" / "aegis_ai_operations_context_v1" / day / "ai_operations_context.v1.json").read_text())

    assert check["ok"] is True
    assert check["failures"] == []
    assert refreshed_context["context_hash"] != context["context_hash"]


def test_ai_operations_response_refreshes_stale_context(tmp_path: Path):
    day = "2026-05-29"
    _seed_operational_sources(tmp_path, day=day)
    context = build_ai_operations_context_v1(truth_root=tmp_path, day_utc=day)
    write_ai_operations_context_v1(truth_root=tmp_path, day_utc=day, payload=context)
    queue_path = tmp_path / "reports" / "aegis_command_center_queue_audit_v1" / day / "command_center_queue_audit.v1.json"
    _write_json(queue_path, {"summary": {"total_rows": 4, "operator_action_required_count": 0, "needs_attention_rows_audited": 0}})

    response = build_ai_operations_response_v1(truth_root=tmp_path, day_utc=day, question="Is operator action required?")

    latest = response["latest_response"]
    assert latest["context_day"] == day
    assert latest["source_day"] == day
    assert latest["unsupported_claims"] == []
    assert "No row-level operator action" in latest["answer"]
