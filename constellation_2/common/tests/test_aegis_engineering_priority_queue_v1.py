from __future__ import annotations

import json
from pathlib import Path

from ops.aegis.engineering_priority_queue_v1 import (
    build_engineering_priority_queue_v1,
    engineering_priority_queue_path_v1,
    write_engineering_priority_queue_v1,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _seed_sources(root: Path, day: str = "2026-05-30") -> None:
    reports = root / "reports"
    _write_json(reports / "aegis_runtime_truth_kernel_v1" / day / "runtime_truth_kernel.v1.json", {
        "day_utc": day,
        "runtime_truth_classification": "PARTIAL_CONTEXT",
        "highest_readiness_layer": "BLOCKED",
        "missing_or_stale_source_count": 3,
        "blocked_capabilities": ["DATA_READY", "EVENT_READY", "TRADE_ADVICE_ALLOWED"],
        "trade_advice_allowed": False,
        "broker_submit_required": False,
        "autonomous_execution_allowed": False,
    })
    _write_json(reports / "aegis_verified_runtime_graph_v1" / day / "verified_runtime_graph.v1.json", {
        "day_utc": day,
        "graph_status": "READY",
        "audit_blockers": [],
    })
    _write_json(reports / "aegis_chatgpt_control_packet_v1" / day / "aegis_chatgpt_control_packet.v1.json", {
        "day_utc": day,
        "runtime_truth_classification": "PARTIAL_CONTEXT",
        "reason_if_blocked": "KERNEL_BLOCKED_TRADE_ADVICE:RUNTIME_EVALUATION",
        "next_operator_actions": ["Regenerate aegis_lite_operating_status: run `python3 ops/tools/build_aegis_operator_status_v1.py --truth_root /tmp/truth --day_utc 2026-05-30`; validate with `npm run aegis:audit`; blocks DATA_READY."],
        "trade_advice_allowed": False,
    })
    _write_json(reports / "aegis_canonical_operator_state_v1" / day / "canonical_operator_state.v1.json", {
        "day_utc": day,
        "generated_at_utc": "2026-05-30T10:00:00Z",
        "candidate_ui_projection": {"run_visibility": {"run_visibility_status": "RUN_HISTORY_AVAILABLE", "last_successful_run_at": "2026-05-30T09:50:00-04:00"}},
    })
    _write_json(reports / "aegis_candidate_generation_diagnostics_v1" / day / "candidate_generation_diagnostics.v1.json", {
        "candidate_generation_status": "UNKNOWN",
        "operator_interpretation": "DATA_BLOCKED",
    })
    _write_json(reports / "aegis_command_center_queue_audit_v1" / day / "command_center_queue_audit.v1.json", {
        "summary": {"incorrectly_shown_as_awaiting_review_count": 2, "incorrectly_shown_as_needs_attention_count": 1}
    })
    handoff = reports / "aegis_audit_handoff_v1" / day / "aegis_audit_handoff.txt"
    handoff.parent.mkdir(parents=True, exist_ok=True)
    handoff.write_text("runtime_truth_classification: PARTIAL_CONTEXT\n", encoding="utf-8")


def test_engineering_priority_queue_classifies_and_prioritizes_issues(tmp_path: Path) -> None:
    _seed_sources(tmp_path)
    payload = build_engineering_priority_queue_v1(truth_root=tmp_path, day_utc="2026-05-30")

    assert payload["schema_id"] == "aegis_engineering_priority_queue_v1"
    assert payload["summary"]["blocking_issues"] >= 1
    assert payload["summary"]["waiting_for_data"] >= 1
    assert payload["summary"]["graph_validation_status"] == "READY"
    assert payload["summary"]["runtime_readiness_status"] == "BLOCKED"
    assert "Graph validation passed" in payload["summary"]["runtime_readiness_explanation"]
    assert payload["fix_first"][0]["priority"] in {"P0", "P1"}
    assert payload["fix_first"][0]["issue"] == "Runtime truth is PARTIAL_CONTEXT"
    assert payload["fix_first"][0]["operator_issue"] == "Today's runtime evidence is incomplete"
    assert payload["fix_first"][0]["ask_aegis_question"].startswith("Why is this blocked")
    assert payload["fix_first"][0]["repair_status"] == "REPAIR_NEEDS_RECOVERY_PLAN"
    assert payload["fix_first"][0]["repair_command"] == ""
    assert payload["fix_first"][0]["verification_command"] == "TARGET_DAY=2026-05-30 npm run aegis:audit"
    assert payload["fix_first"][0]["recovery_plan_path"].endswith("recovery_plan.v1.txt")
    assert payload["fix_first"][0]["human_evidence_summary"] == "3 required runtime sources are stale or missing."
    assert payload["system_repair_actions"]
    assert payload["summary"]["system_repair_actions"] == len(payload["system_repair_actions"])
    assert payload["summary"]["operator_actions_required"] == len(payload["operator_actions_required"])
    assert payload["active_blockers"]
    assert payload["degraded_but_functional"]
    for issue in payload["issues"]:
        assert issue["classification"] in {"BLOCKING", "DEGRADED", "WAITING_FOR_DATA", "WAITING_FOR_TIME", "WAITING_FOR_OPERATOR", "INFORMATIONAL"}
        assert issue["priority"] in {"P0", "P1", "P2", "P3"}
        assert issue["issue"]
        assert issue["operator_issue"]
        assert issue["operator_summary"]
        assert issue["ask_aegis_question"]
        assert issue["cause"]
        assert issue["evidence"]
        assert issue["repair_action"]
        assert issue["repair_status"] in {"REPAIR_AVAILABLE", "REPAIR_UNAVAILABLE", "REPAIR_NEEDS_RECOVERY_PLAN", "REPAIR_MANUAL_INVESTIGATION", "VERIFY_ONLY"}
        assert issue["action_type"] in {"USER_ACTION", "SYSTEM_REPAIR", "WAITING_FOR_DATA", "WAITING_FOR_TIME", "VERIFY_ONLY"}
        assert issue["human_evidence_summary"]
        assert issue["raw_evidence"]
        assert issue["verification_command"]
        assert issue["repair_command"] != issue["verification_command"]
        assert "npm run aegis:audit" not in issue["repair_command"]
        assert "npm run aegis:portal-smoke" not in issue["repair_command"]
        if issue["repair_status"] == "REPAIR_AVAILABLE":
            assert issue["repair_command"]
        else:
            assert not issue["repair_command"]
            assert issue["repair_unavailable_reason"]
        if "recovery plan" in issue["repair_unavailable_reason"].lower():
            assert issue["recovery_plan_id"] or issue["recovery_plan_path"]
        assert issue["source_artifacts"]
        assert issue["timestamp"]
    assert payload["safety"]["trade_advice_allowed"] is False
    assert payload["safety"]["broker_submit_transmit_allowed"] is False


def test_engineering_priority_queue_writes_expected_artifact(tmp_path: Path) -> None:
    _seed_sources(tmp_path)
    payload = build_engineering_priority_queue_v1(truth_root=tmp_path, day_utc="2026-05-30")
    path = write_engineering_priority_queue_v1(truth_root=tmp_path, day_utc="2026-05-30", payload=payload)

    assert path == engineering_priority_queue_path_v1(truth_root=tmp_path, day_utc="2026-05-30")
    written = json.loads(path.read_text(encoding="utf-8"))
    assert written["summary"]["total_issues"] == payload["summary"]["total_issues"]
    assert written["day_clarity"]["requested_day"] == "2026-05-30"
