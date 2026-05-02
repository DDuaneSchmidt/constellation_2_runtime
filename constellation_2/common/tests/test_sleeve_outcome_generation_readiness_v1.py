from __future__ import annotations

import json
from pathlib import Path

from ops.tools import run_sleeve_outcome_generation_readiness_v1 as report


DAY = "2026-05-01"


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _report(root: Path, family: str, filename: str) -> Path:
    return root / "reports" / family / DAY / filename


def _seed(root: Path, execution_root: Path) -> None:
    _write(
        _report(root, "sleeve_intent_quality_diagnostics_v1", "sleeve_intent_quality_diagnostics.v1.json"),
        {
            "intent_diagnostics": [
                {"sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "intent_id": "trend", "signal_trigger_reason": "SIGNAL_CHANGED", "missing_inputs": [], "symbol": "SPY"},
                {"sleeve_id": "C2_VOL_INCOME_DEFINED_RISK_V1", "intent_id": "vol", "signal_trigger_reason": "SIGNAL_CHANGED", "missing_inputs": ["OPTIONS_CHAIN_SNAPSHOT_MISSING"], "symbol": "IWM"},
                {"sleeve_id": "C2_CROSS_ASSET_TREND_V1", "intent_id": "cross", "signal_trigger_reason": "SIGNAL_CHANGED", "missing_inputs": [], "symbol": "DBC"},
            ]
        },
    )
    _write(
        _report(root, "portfolio_scoring_v1", "portfolio_scoring.v1.json"),
        {
            "rankings": [
                {"rank": 1, "sleeve_id": "C2_VOL_INCOME_DEFINED_RISK_V1", "intent_id": "vol", "symbol": "IWM", "executable_eligible": True, "reason_codes": ["SCORING_EXECUTABLE_ELIGIBLE"]},
                {"rank": 2, "sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "intent_id": "trend", "symbol": "SPY", "executable_eligible": True, "reason_codes": ["SCORING_EXECUTABLE_ELIGIBLE"]},
                {"rank": 3, "sleeve_id": "C2_CROSS_ASSET_TREND_V1", "intent_id": "cross", "symbol": "DBC", "executable_eligible": False, "reason_codes": ["SCORING_NOT_EXECUTABLE_SUPPRESS"]},
            ]
        },
    )
    _write(
        _report(root, "risk_sizing_authority_v1", "risk_sizing_authority.v1.json"),
        {
            "sizing_decisions": [
                {"intent_id": "trend", "intent_hash": "a" * 64, "intent_path": "/intent/trend.json", "sizing_state": "SIZED", "execution_package_path": "", "final_quantity": None, "reason_code": "", "account_net_liquidation_cents": 1000000, "allowed_risk_cents": 20000},
                {"intent_id": "vol", "intent_hash": "b" * 64, "intent_path": "/intent/vol.json", "sizing_state": "SIZED", "execution_package_path": "", "final_quantity": None, "reason_code": "", "account_net_liquidation_cents": 1000000, "allowed_risk_cents": 20000},
            ]
        },
    )
    _write(_report(root, "market_data_authority_v1", "market_data_authority.v1.json"), {"status": "FAIL", "first_blocker": "OPTIONS_CHAIN_SNAPSHOT_MISSING", "required_symbols": ["IWM"]})
    _write(_report(root, "submit_boundary_status_v1", "submit_boundary_status.v1.json"), {"submit_allowed": False, "canonical_blocker": "AEGIS_NOT_READY"})
    _write(_report(root, "trade_outcome_v1", "trade_outcome.v1.json"), {"intent_id": "vol", "outcome_status": "UNKNOWN"})
    _write(_report(root, "sleeve_intent_trade_attribution_v1", "sleeve_intent_trade_attribution.v1.json"), {"opportunities": []})
    _write(
        execution_root / "allocation_v1" / "capital_authority_allocation_v1" / DAY / "capital_authority_allocation.v1.json",
        {
            "decision_chain": {
                "authorized_trade_intents": [
                    {
                        "intent_id": "trend",
                        "requested_quantity": 1,
                        "requested_quantity_basis": "INTENT_RISK_BUDGET",
                        "authorized_quantity": 0,
                        "target_notional_pct": "0.01",
                        "risk_per_unit_cents": 10000,
                        "required_risk_cents": 10000,
                        "available_sleeve_headroom_cents": 5000,
                        "available_portfolio_headroom_cents": 20000,
                        "reason_codes": ["BUNDLE_B_HEADROOM_REJECTED"],
                    },
                    {
                        "intent_id": "vol",
                        "requested_quantity": 0,
                        "requested_quantity_basis": "UNPROVEN_INTENT_RISK_BUDGET",
                        "authorized_quantity": 0,
                        "target_notional_pct": "0.01",
                        "risk_per_unit_cents": 0,
                        "required_risk_cents": 0,
                        "available_sleeve_headroom_cents": 5000,
                        "available_portfolio_headroom_cents": 20000,
                        "reason_codes": [
                            "AUTHZ_MISSING_DEFINED_RISK_EVIDENCE",
                            "BUNDLE_B_REQUESTED_QUANTITY_UNPROVEN",
                            "BUNDLE_B_REQUESTED_QUANTITY_ZERO",
                        ],
                    },
                ]
            },
            "governed_evaluation_control_state": {
                "sleeve_controls": [
                    {
                        "scope_id": "C2_TREND_EQ_PRIMARY",
                        "artifact_status": "MISSING",
                        "control_state": "fail_safe_block_new_risk",
                        "diagnostic": "MISSING_ACTION_ARTIFACT:/tmp/sleeve_governance_action_state.v1.json",
                        "effective_headroom_cents": 5000,
                        "reason_codes": ["SLEEVE_ACTION_ARTIFACT_FAILSAFE_BLOCK"],
                    },
                    {
                        "scope_id": "C2_VOL_INCOME_DEFINED_RISK",
                        "artifact_status": "MISSING",
                        "control_state": "fail_safe_block_new_risk",
                        "diagnostic": "MISSING_ACTION_ARTIFACT:/tmp/sleeve_governance_action_state.v1.json",
                        "effective_headroom_cents": 5000,
                        "reason_codes": ["SLEEVE_ACTION_ARTIFACT_FAILSAFE_BLOCK"],
                    },
                ]
            },
        },
    )
    for intent_hash in ["a" * 64, "b" * 64]:
        _write(execution_root / "engine_activity_v1" / "authorization_v1" / DAY / f"{intent_hash}.authorization.v1.json", {"status": "REJECTED", "authorization": {"decision": "REJECTED", "authorized_quantity": 0}, "reason_codes": ["BUNDLE_B_HEADROOM_REJECTED"]})


def test_only_scoring_eligible_sleeves_are_reported(tmp_path: Path) -> None:
    _seed(tmp_path, tmp_path / "exec")

    payload = report.build_sleeve_outcome_generation_readiness_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path / "exec")

    assert {row["sleeve_id"] for row in payload["sleeve_results"]} == {"C2_TREND_EQ_PRIMARY_V1", "C2_VOL_INCOME_DEFINED_RISK_V1"}
    assert payload["summary"]["eligible_intent_count"] == 2


def test_missing_execution_submit_and_outcome_evidence_blocks_readiness(tmp_path: Path) -> None:
    _seed(tmp_path, tmp_path / "exec")

    rows = {row["intent_id"]: row for row in report.build_sleeve_outcome_generation_readiness_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path / "exec")["sleeve_results"]}

    assert rows["trend"]["execution_ready_if_aegis_ready"] is False
    assert "EXECUTION_PACKAGE_MISSING" in rows["trend"]["outcome_generation_blockers"]
    assert "SUBMIT_DECISION_TRACE_MISSING" in rows["trend"]["outcome_generation_blockers"]
    assert "COMPLETED_OUTCOME_MISSING" in rows["trend"]["outcome_generation_blockers"]
    assert "AUTHORIZATION_REJECTED" in rows["trend"]["outcome_generation_blockers"]
    assert rows["trend"]["authorization_details"]["authorization_status"] == "REJECTED"
    assert rows["trend"]["authorization_details"]["authorized_quantity"] == 0
    assert rows["trend"]["execution_package_readiness"]["status"] == "BLOCKED_BY_AUTHORIZATION_REJECTED"
    assert rows["trend"]["submit_trace_readiness"]["status"] == "EXPECTED_MISSING_SUBMIT_BOUNDARY_NOT_REACHED"
    assert "EXECUTION_PACKAGE_DOWNSTREAM_OF_AUTHORIZATION" in rows["trend"]["root_cause_classification"]


def test_trend_sizing_audit_exposes_formula_and_headroom_reject(tmp_path: Path) -> None:
    _seed(tmp_path, tmp_path / "exec")

    rows = {row["intent_id"]: row for row in report.build_sleeve_outcome_generation_readiness_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path / "exec")["sleeve_results"]}
    audit = rows["trend"]["sizing_audit"]

    assert audit["requested_target_pct"] == "0.01"
    assert audit["nav_basis"]["account_net_liquidation_cents"] == 1000000
    assert audit["risk_per_unit_cents"] == 10000
    assert audit["requested_quantity"] == 1
    assert audit["required_risk_cents"] == 10000
    assert audit["sleeve_headroom_cents"] == 5000
    assert audit["root_cause"] == "REAL_HEADROOM_POLICY_REJECT_WITH_GOVERNANCE_FAILSAFE"
    assert audit["sleeve_governance_control"]["control_state"] == "fail_safe_block_new_risk"


def test_options_market_data_blocker_is_kept_on_vol_income(tmp_path: Path) -> None:
    _seed(tmp_path, tmp_path / "exec")

    rows = {row["intent_id"]: row for row in report.build_sleeve_outcome_generation_readiness_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path / "exec")["sleeve_results"]}

    assert "OPTIONS_CHAIN_SNAPSHOT_MISSING" in rows["vol"]["outcome_generation_blockers"]
    assert rows["vol"]["market_inputs"]["missing_inputs"] == ["OPTIONS_CHAIN_SNAPSHOT_MISSING"]
    assert rows["vol"]["market_inputs"]["options_chain_recovery"]["required"] is True
    assert rows["vol"]["market_inputs"]["options_chain_recovery"]["expected_artifact_pattern"].endswith(
        "options_chain_snapshot_v1/2026-05-01/<capture_id>/options_chain_snapshot.v1.json"
    )
    assert "run_options_chain_snapshot_required_day_v1.py" in rows["vol"]["market_inputs"]["options_chain_recovery"]["recovery_command"]
    assert "OPTIONS_CHAIN_SNAPSHOT_MISSING" not in rows["trend"]["outcome_generation_blockers"]
    assert rows["trend"]["market_inputs"]["missing_inputs"] == []
    assert rows["trend"]["market_inputs"]["options_chain_recovery"]["required"] is False
    assert rows["vol"]["next_governed_producer"] == "ops/tools/run_options_chain_snapshot_required_day_v1.py"
    assert "MISSING_OPTIONS_CHAIN_INPUT" in rows["vol"]["root_cause_classification"]
    assert rows["vol"]["sizing_audit"]["root_cause"] == "DEFINED_RISK_EVIDENCE_MISSING"


def test_existing_execution_package_is_detected_from_execution_root(tmp_path: Path) -> None:
    execution_root = tmp_path / "exec"
    _seed(tmp_path, execution_root)
    _write(
        execution_root / "engine_activity_v1" / "authorization_v1" / DAY / f"{'a' * 64}.authorization.v1.json",
        {"status": "APPROVED", "authorization": {"decision": "APPROVED", "authorized_quantity": 1}, "reason_codes": []},
    )
    package_path = execution_root / "execution_package_v1" / DAY / "submission" / "execution_package.v1.json"
    _write(package_path, {"intent_id": "trend", "intent_hash": "a" * 64})

    rows = {
        row["intent_id"]: row
        for row in report.build_sleeve_outcome_generation_readiness_v1(
            day_utc=DAY,
            truth_root=tmp_path,
            execution_root=execution_root,
        )["sleeve_results"]
    }

    assert rows["trend"]["execution_package_readiness"]["status"] == "PRESENT"
    assert rows["trend"]["risk_sizing_result"]["execution_package_path"] == str(package_path.resolve())
    assert "EXECUTION_PACKAGE_MISSING" not in rows["trend"]["outcome_generation_blockers"]
    assert "SUBMIT_DECISION_TRACE_MISSING" in rows["trend"]["outcome_generation_blockers"]
    assert "COMPLETED_OUTCOME_MISSING" in rows["trend"]["outcome_generation_blockers"]


def test_blocked_execution_build_is_reported_in_package_readiness(tmp_path: Path) -> None:
    execution_root = tmp_path / "exec"
    _seed(tmp_path, execution_root)
    _write(
        execution_root / "engine_activity_v1" / "authorization_v1" / DAY / f"{'a' * 64}.authorization.v1.json",
        {"status": "APPROVED", "authorization": {"decision": "APPROVED", "authorized_quantity": 1}, "reason_codes": []},
    )
    build_path = tmp_path / "reports" / "execution_build_v1" / DAY / "submission" / "execution_build.v1.json"
    _write(
        tmp_path / "target_day_admission_v1" / f"{DAY}.json",
        {
            "admission_status": "BLOCKED",
            "blocking_reason_codes": ["REQUIRED_GATE_FAIL"],
            "blocker_chain": [{"artifact_id": "startup_materialization_input_convergence_v1", "blocker_code": "REQUIRED_GATE_FAIL"}],
        },
    )
    _write(
        build_path,
        {
            "intent_id": "trend",
            "generated_utc": f"{DAY}T12:00:00Z",
            "closure_status": "BLOCKED",
            "submission_id": "submission",
            "candidate_ref": {
                "canonical_truth_root": str(tmp_path),
                "execution_truth_root": str(execution_root),
                "sleeve_id": "PRIMARY",
                "environment": "PAPER",
            },
            "first_real_blocker": {"dependency_id": "global_context_package_v1", "status": "FAILED"},
            "blocking_chain": [{"dependency_id": "global_context_package_v1", "status": "FAILED"}],
            "materializable_now": ["global_context_package_v1"],
        },
    )

    rows = {
        row["intent_id"]: row
        for row in report.build_sleeve_outcome_generation_readiness_v1(
            day_utc=DAY,
            truth_root=tmp_path,
            execution_root=execution_root,
        )["sleeve_results"]
    }

    readiness = rows["trend"]["execution_package_readiness"]
    assert readiness["status"] == "BLOCKED_BY_EXECUTION_BUILD"
    assert readiness["latest_execution_build"]["build_path"] == str(build_path.resolve())
    assert readiness["latest_execution_build"]["first_real_blocker"]["dependency_id"] == "global_context_package_v1"
    assert readiness["latest_execution_build"]["chain_map"][0]["artifact"] == "target_day_admission_v1"
    assert readiness["latest_execution_build"]["chain_map"][0]["blocker"] == "REQUIRED_GATE_FAIL"
    assert "run_session_authority_v1.py" in readiness["latest_execution_build"]["chain_map"][0]["recovery_command"]


def test_stale_same_day_broker_evidence_is_marked_non_recoverable_for_past_day(tmp_path: Path) -> None:
    _seed(tmp_path, tmp_path / "exec")
    _write(_report(tmp_path, "submit_boundary_status_v1", "submit_boundary_status.v1.json"), {"submit_allowed": False, "canonical_blocker": "BROKER_EVENT_LOG_STALE"})

    payload = report.build_sleeve_outcome_generation_readiness_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path / "exec")
    rows = {row["intent_id"]: row for row in payload["sleeve_results"]}

    assert rows["trend"]["same_day_evidence_status"]["status"] == "NON_RECOVERABLE_STALE_SAME_DAY_EVIDENCE"
    assert rows["trend"]["same_day_evidence_status"]["must_be_produced_during_target_day"] is True
    assert "backfill" in rows["trend"]["same_day_evidence_status"]["recovery_command"].lower()
    requirement_ids = {item["requirement_id"] for item in payload["same_day_execution_requirements"]}
    assert {
        "broker_event_observer",
        "broker_supply_v1",
        "runtime_resilience_authority_v1",
        "session_readiness_refresh_v1",
        "day_authority_decision_v1",
        "target_day_admission_v1",
        "day_activation_package_v1",
        "global_context_package_v1",
        "execution_package_v1",
    }.issubset(requirement_ids)


def test_report_is_diagnostic_only_and_cannot_change_allocation_or_submit(tmp_path: Path) -> None:
    _seed(tmp_path, tmp_path / "exec")

    payload = report.build_sleeve_outcome_generation_readiness_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path / "exec")

    assert payload["authority"] == "DIAGNOSTIC_ONLY"
    assert payload["readiness_effect"] == "NONE"
    assert payload["submit_effect"] == "NONE"
    assert payload["allocation_effect"] == "NONE"
    assert payload["summary"]["execution_ready_count"] == 0
