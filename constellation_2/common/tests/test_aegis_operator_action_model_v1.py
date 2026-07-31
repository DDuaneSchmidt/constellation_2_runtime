from __future__ import annotations

import json
from pathlib import Path

from ops.aegis.operator_action_model_v1 import build_operator_action_model_v1, write_operator_action_model_v1
from ops.aegis.operator_action_model_self_check_v1 import build_operator_action_model_self_check_v1

DAY = "2026-06-01"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _seed_truth(root: Path, *, trade_advice_allowed: bool = False, manual_capture_allowed: bool = False, manual_packet_count: int = 0, operator_tasks: int = 0, candidate_count: int = 0, open_positions: int = 36, closed_positions: int = 0, validation_samples: int = 0) -> None:
    reports = root / "reports"
    _write_json(reports / "aegis_runtime_truth_kernel_v1" / DAY / "runtime_truth_kernel.v1.json", {
        "artifact_id": "aegis_runtime_truth_kernel_v1",
        "day_utc": DAY,
        "generated_at_utc": "2026-06-01T10:00:00Z",
        "runtime_truth_classification": "PARTIAL_CONTEXT" if not trade_advice_allowed else "REAL_RUNTIME",
        "highest_readiness_layer": "BLOCKED" if not trade_advice_allowed else "READY",
        "trade_advice_allowed": trade_advice_allowed,
        "manual_trade_capture_allowed": manual_capture_allowed,
        "autonomous_execution_allowed": False,
        "broker_submit_required": False,
        "blocked_capabilities": [] if trade_advice_allowed else ["TRADE_ADVICE_ALLOWED", "MANUAL_TRADE_CAPTURE_ALLOWED"],
    })
    _write_json(reports / "aegis_chatgpt_control_packet_v1" / DAY / "aegis_chatgpt_control_packet.v1.json", {
        "artifact_id": "aegis_chatgpt_control_packet_v1",
        "day_utc": DAY,
        "generated_at": "2026-06-01T10:01:00Z",
        "runtime_truth_classification": "PARTIAL_CONTEXT" if not trade_advice_allowed else "REAL_RUNTIME",
        "trade_advice_allowed": trade_advice_allowed,
        "manual_trade_capture_allowed": manual_capture_allowed,
        "autonomous_execution_allowed": False,
        "aegis_lite_status": {"manual_packet_actionable_count": manual_packet_count, "manual_trade_packet_present": manual_packet_count > 0},
    })
    _write_json(reports / "aegis_canonical_operator_state_v1" / DAY / "canonical_operator_state.v1.json", {
        "artifact_id": "aegis_canonical_operator_state_v1",
        "day_utc": DAY,
        "generated_at_utc": "2026-06-01T10:02:00Z",
        "candidate_ui_projection": {
            "diagnostics_status": "AVAILABLE",
            "diagnostic_candidate_outputs": candidate_count,
            "diagnostic_rejection_count": 42,
            "canonical_generated_at": "2026-06-01T10:02:00Z",
            "run_summary": {"diagnostic_candidate_outputs": candidate_count, "diagnostics_completed_at": "2026-06-01T09:50:00Z"},
        },
    })
    _write_json(reports / "aegis_candidate_state_v1" / DAY / "candidate_state.v1.json", {"active_candidate_count": open_positions, "generated_at_utc": "2026-06-01T10:03:00Z"})
    _write_json(reports / "aegis_candidate_generation_diagnostics_v1" / DAY / "candidate_generation_diagnostics.v1.json", {"candidate_count": candidate_count, "generated_at": "2026-06-01T10:03:00Z"})
    _write_json(reports / "aegis_sleeve_analytics_v1" / DAY / "sleeve_analytics.v1.json", {"generated_at": "2026-06-01T10:03:00Z"})
    _write_json(reports / "aegis_outcome_registry_v1" / DAY / "outcome_registry.v1.json", {"generated_at": "2026-06-01T10:04:00Z", "summary": {"open_outcome_count": open_positions, "closed_outcome_count": closed_positions, "validation_sample_count": validation_samples}})
    _write_json(reports / "aegis_statistical_sufficiency_v1" / DAY / "statistical_sufficiency.v1.json", {"generated_at": "2026-06-01T10:05:00Z", "summary": {"UNDERPOWERED": 8}, "hypotheses": [{}] * 8})
    _write_json(reports / "aegis_research_portfolio_v1" / DAY / "research_portfolio.v1.json", {"generated_at": "2026-06-01T10:05:00Z"})
    _write_json(reports / "aegis_research_capital_allocation_v1" / DAY / "research_capital_allocation.v1.json", {"generated_at": "2026-06-01T10:05:00Z"})
    _write_json(reports / "aegis_paper_position_ledger_v1" / DAY / "paper_position_ledger.v1.json", {"generated_at_utc": "2026-06-01T10:06:00Z", "open_position_count": open_positions, "closed_position_count": closed_positions})
    _write_json(reports / "aegis_command_center_queue_audit_v1" / DAY / "command_center_queue_audit.v1.json", {"generated_at": "2026-06-01T10:07:00Z", "summary": {"operator_action_required_count": operator_tasks}})
    _write_json(reports / "aegis_verified_runtime_graph_v1" / DAY / "portal_runtime_model.v1.json", {"generated_at": "2026-06-01T10:08:00Z"})


def _cap(model: dict, capability_id: str) -> dict:
    return model["capabilities_by_id"][capability_id]


def test_normal_monitoring_state(tmp_path: Path) -> None:
    _seed_truth(tmp_path)
    model = build_operator_action_model_v1(truth_root=tmp_path, day_utc=DAY)
    assert model["summary"]["aegis_can_operate"] is True
    assert _cap(model, "PAPER_MONITORING")["status"] == "ACTIVE"
    assert model["summary"]["top_level_summary"] == "Monitoring only. No David action required."


def test_zero_candidate_completed_run_is_complete_not_blocked(tmp_path: Path) -> None:
    _seed_truth(tmp_path, candidate_count=0)
    row = _cap(build_operator_action_model_v1(truth_root=tmp_path, day_utc=DAY), "CANDIDATE_GENERATION")
    assert row["status"] == "COMPLETE"
    assert "ZERO_OUTPUT_CANDIDATES" in row["reason_codes"]


def test_blocked_trade_recommendation_when_trade_advice_false(tmp_path: Path) -> None:
    _seed_truth(tmp_path, trade_advice_allowed=False)
    row = _cap(build_operator_action_model_v1(truth_root=tmp_path, day_utc=DAY), "TRADE_RECOMMENDATION")
    assert row["status"] == "BLOCKED"
    assert "TRADE_ADVICE_DISABLED_BY_RUNTIME_TRUTH" in row["reason_codes"]


def test_no_david_action_required_without_real_queue_task(tmp_path: Path) -> None:
    _seed_truth(tmp_path, operator_tasks=0)
    row = _cap(build_operator_action_model_v1(truth_root=tmp_path, day_utc=DAY), "DAVID_ACTION")
    assert row["status"] == "COMPLETE"
    assert row["david_action_required"] is False


def test_manual_trade_capture_ready_only_with_eligible_packet_and_runtime_permission(tmp_path: Path) -> None:
    _seed_truth(tmp_path, manual_capture_allowed=False, manual_packet_count=0)
    model = build_operator_action_model_v1(truth_root=tmp_path, day_utc=DAY)
    assert _cap(model, "MANUAL_TRADE_CAPTURE")["status"] == "NOT_APPLICABLE"
    _seed_truth(tmp_path, manual_capture_allowed=True, manual_packet_count=1)
    model = build_operator_action_model_v1(truth_root=tmp_path, day_utc=DAY)
    assert _cap(model, "MANUAL_TRADE_CAPTURE")["status"] == "READY"


def test_broker_execution_disabled_by_policy(tmp_path: Path) -> None:
    _seed_truth(tmp_path)
    row = _cap(build_operator_action_model_v1(truth_root=tmp_path, day_utc=DAY), "BROKER_EXECUTION")
    assert row["status"] == "DISABLED_BY_POLICY"


def test_missing_reason_code_failure(tmp_path: Path) -> None:
    _seed_truth(tmp_path)
    model = build_operator_action_model_v1(truth_root=tmp_path, day_utc=DAY)
    model["capabilities_by_id"]["TRADE_RECOMMENDATION"]["reason_codes"] = []
    for row in model["capability_matrix"]:
        if row["capability_id"] == "TRADE_RECOMMENDATION":
            row["reason_codes"] = []
    write_operator_action_model_v1(truth_root=tmp_path, day_utc=DAY, payload=model)
    check = build_operator_action_model_self_check_v1(truth_root=tmp_path, day_utc=DAY)
    assert check["ok"] is False
    assert any(f["failure_code"] == "MISSING_REASON_CODES" for f in check["failures"])


def test_deterministic_rerun_stability(tmp_path: Path) -> None:
    _seed_truth(tmp_path)
    first = build_operator_action_model_v1(truth_root=tmp_path, day_utc=DAY)
    second = build_operator_action_model_v1(truth_root=tmp_path, day_utc=DAY)
    assert first == second


def test_ui_data_shape_contains_all_required_capabilities(tmp_path: Path) -> None:
    _seed_truth(tmp_path)
    model = build_operator_action_model_v1(truth_root=tmp_path, day_utc=DAY)
    assert set(model["capabilities_by_id"]) == {
        "CANDIDATE_GENERATION",
        "PAPER_MONITORING",
        "OUTCOME_REALIZATION",
        "HYPOTHESIS_VALIDATION",
        "TRADE_RECOMMENDATION",
        "MANUAL_TRADE_CAPTURE",
        "BROKER_EXECUTION",
        "DAVID_ACTION",
    }
    for row in model["capability_matrix"]:
        assert {"capability_id", "status", "status_label", "reason_codes", "human_readable_reason", "source_artifacts", "source_hashes", "next_expected_event", "david_action_required"} <= set(row)


def test_outcome_summary_current_aliases_drive_operator_counts(tmp_path: Path) -> None:
    _seed_truth(tmp_path, open_positions=55, closed_positions=0, validation_samples=0)
    outcome_path = tmp_path / "reports" / "aegis_outcome_registry_v1" / DAY / "outcome_registry.v1.json"
    payload = json.loads(outcome_path.read_text(encoding="utf-8"))
    payload["summary"] = {"open_outcomes": 53, "closed_outcomes": 2}
    _write_json(outcome_path, payload)
    _write_json(tmp_path / "reports" / "aegis_validation_samples_v1" / DAY / "validation_samples.v1.json", {"summary": {"included_samples": 2}})
    model = build_operator_action_model_v1(truth_root=tmp_path, day_utc=DAY)
    assert model["summary"]["open_paper_position_count"] == 53
    assert model["summary"]["closed_paper_position_count"] == 2
    assert model["summary"]["validation_sample_count"] == 2


def test_operator_action_model_write_is_byte_stable(tmp_path: Path) -> None:
    _seed_truth(tmp_path)
    first = build_operator_action_model_v1(truth_root=tmp_path, day_utc=DAY)
    write_operator_action_model_v1(truth_root=tmp_path, day_utc=DAY, payload=first)
    path = tmp_path / "reports" / "aegis_operator_action_model_v1" / DAY / "operator_action_model.v1.json"
    first_bytes = path.read_bytes()
    second = build_operator_action_model_v1(truth_root=tmp_path, day_utc=DAY)
    write_operator_action_model_v1(truth_root=tmp_path, day_utc=DAY, payload=second)
    assert path.read_bytes() == first_bytes


def test_self_check_ignores_provenance_only_source_hash_drift(tmp_path: Path) -> None:
    _seed_truth(tmp_path)
    model = build_operator_action_model_v1(truth_root=tmp_path, day_utc=DAY)
    write_operator_action_model_v1(truth_root=tmp_path, day_utc=DAY, payload=model)
    control_path = tmp_path / "reports" / "aegis_chatgpt_control_packet_v1" / DAY / "aegis_chatgpt_control_packet.v1.json"
    control = json.loads(control_path.read_text(encoding="utf-8"))
    control["generated_at"] = "2026-06-01T10:09:00Z"
    _write_json(control_path, control)

    check = build_operator_action_model_self_check_v1(truth_root=tmp_path, day_utc=DAY)

    assert check["ok"] is True
    assert check["byte_stable_match"] is False
    assert check["semantic_stable_match"] is True
    assert check["provenance_only_drift"] is True
    assert not any(f["failure_code"] == "NON_DETERMINISTIC_OUTPUT" for f in check["failures"])


def test_self_check_still_flags_semantic_drift(tmp_path: Path) -> None:
    _seed_truth(tmp_path)
    model = build_operator_action_model_v1(truth_root=tmp_path, day_utc=DAY)
    write_operator_action_model_v1(truth_root=tmp_path, day_utc=DAY, payload=model)
    outcome_path = tmp_path / "reports" / "aegis_outcome_registry_v1" / DAY / "outcome_registry.v1.json"
    outcome = json.loads(outcome_path.read_text(encoding="utf-8"))
    outcome["summary"]["closed_outcome_count"] = 3
    _write_json(outcome_path, outcome)

    check = build_operator_action_model_self_check_v1(truth_root=tmp_path, day_utc=DAY)

    assert check["ok"] is False
    assert check["semantic_stable_match"] is False
    assert any(f["failure_code"] == "NON_DETERMINISTIC_OUTPUT" for f in check["failures"])
