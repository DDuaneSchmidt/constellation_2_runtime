from __future__ import annotations

import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import ops.tools.run_aegis_control_plane_v1 as cp  # noqa: E402
import ops.tools.run_aegis_operator_projection_v1 as projection  # noqa: E402
import ops.tools.run_aegis_requirement_graph_v1 as graph  # noqa: E402
from constellation_2.phaseB.lib.validate_against_schema_v1 import SchemaValidationError, validate_against_repo_schema_v1  # noqa: E402
from ops.tools import run_aegis_bod_prepare_v1 as bod  # noqa: E402

DAY = "2026-05-04"
KNOWN_FAILURE_FIXTURE = Path(__file__).resolve().parent / "fixtures" / "readiness_domain_ownership_known_failure_v1.json"
DOMAIN_DRIFT_FIXTURE = Path(__file__).resolve().parent / "fixtures" / "readiness_domain_drift_known_good_vs_current_v1.json"
REPORT_CONTRACT_SCHEMAS = [
    "governance/04_DATA/SCHEMAS/C2/REPORTS/runtime_resilience_authority.v1.schema.json",
    "governance/04_DATA/SCHEMAS/C2/REPORTS/broker_supply.v1.schema.json",
    "governance/04_DATA/SCHEMAS/C2/REPORTS/authorization_supply.v1.schema.json",
]


def _ctx(tmp_path: Path) -> bod.BodContext:
    truth = tmp_path / "truth"
    execution = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    runtime = tmp_path
    operator = tmp_path / "operator"
    for path in (truth, execution, operator):
        path.mkdir(parents=True, exist_ok=True)
    return bod.BodContext(
        day_utc=DAY,
        environment="PAPER",
        truth_root=truth,
        execution_root=execution,
        runtime_root=runtime,
        operator_input_root=operator,
        ib_account="DU123456",
    )


def _ctx_with_truth(tmp_path: Path, truth: Path) -> bod.BodContext:
    execution = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    runtime = tmp_path
    operator = tmp_path / "operator"
    for path in (truth, execution, operator):
        path.mkdir(parents=True, exist_ok=True)
    return bod.BodContext(
        day_utc=DAY,
        environment="PAPER",
        truth_root=truth,
        execution_root=execution,
        runtime_root=runtime,
        operator_input_root=operator,
        ib_account="DU123456",
    )


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _producer_contract() -> dict:
    return {
        "producer_contract_v1": {
            "code_version_git_commit": cp._current_git_commit_v1(),
            "source_dirty_status": "CLEAN",
            "generated_at_utc": f"{DAY}T13:00:00Z",
            "producer_name": "test",
            "producer_command": "test",
        }
    }


def _self_bound_control_payload(
    *,
    actual_path: Path,
    truth_root: Path,
    runtime_root: Path,
    runtime_mode: str,
    output_path: Path | None = None,
) -> dict:
    output = (output_path or actual_path).resolve()
    return {
        "schema_id": "aegis_control_plane",
        "schema_version": cp.SCHEMA_VERSION,
        "day_utc": DAY,
        "truth_root": str(truth_root.resolve()),
        "runtime_root": str(runtime_root.resolve()),
        "runtime_mode": runtime_mode,
        "artifact_path": str(actual_path.resolve()),
        "actual_artifact_path": str(actual_path.resolve()),
        "producer_contract_output_artifact_path": str(output),
        "environment": "PAPER",
        "final_status": "READY",
        "current_phase": "",
        "current_domain": "",
        "canonical_blocker": "",
        "submit_allowed": True,
        "evidence_paths": [],
        "producer_contract_v1": {
            "code_version_git_commit": cp._current_git_commit_v1(),
            "source_dirty_status": "CLEAN",
            "generated_at_utc": f"{DAY}T13:00:00Z",
            "output_artifacts": [{"path": str(output)}],
        },
    }


def _artifact_meta(ctx: bod.BodContext) -> dict:
    return {"truth_root": str(ctx.truth_root), **_producer_contract()}


def _bootstrap_meta(ctx: bod.BodContext) -> dict:
    return {
        "truth_roots": {
            "canonical_truth_root": str(ctx.truth_root),
            "sleeve_truth_root": str(ctx.execution_root),
            "operator_input_root": str(ctx.operator_input_root),
        },
        **_producer_contract(),
    }


def _source_pass(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        cp,
        "_source_repo_status",
        lambda: {
            "git_dirty_status": "CLEAN",
            "dirty_path_count": 0,
            "canonical_repo_protection_status": "PROTECTED",
            "canonical_repo_protection_status_path": "/tmp/protected.json",
        },
    )


def _session_pass(ctx: bod.BodContext) -> None:
    _write(
        ctx.truth_root / "active_session_v1" / "current.json",
        {
            "active_day": ctx.day_utc,
            "target_day": ctx.day_utc,
            "promotion_state": "PROMOTED",
            "rollover_status": "ROLLED_OVER",
            "target_day_admission_status": "PASS",
        },
    )
    _write(ctx.truth_root / "target_day_build_v1" / f"{ctx.day_utc}.json", {"target_day": ctx.day_utc, "build_status": "PASS"})
    _write(ctx.truth_root / "target_day_admission_v1" / f"{ctx.day_utc}.json", {"target_day": ctx.day_utc, "admission_status": "PASS"})
    _write(
        ctx.truth_root / "reports" / "session_promotion_decision_v1" / ctx.day_utc / "session_promotion_decision.v1.json",
        {"target_day": ctx.day_utc, "promotion_state": "PROMOTED", "target_day_admission_status": "PASS"},
    )
    _write(
        ctx.truth_root / "reports" / "paper_session_authority_v1" / ctx.day_utc / "paper_session_authority.v1.json",
        {"day_utc": ctx.day_utc, "authority_status": "GRANTED"},
    )
    _write(
        ctx.truth_root / "reports" / "paper_session_bootstrap_v1" / ctx.day_utc / "paper_session_bootstrap.v1.json",
        {"day_utc": ctx.day_utc, "bootstrap_status": "PASS", **_bootstrap_meta(ctx)},
    )
    _write(ctx.truth_root / "market_calendar_v1" / "dataset_manifest.json", {"day_utc": ctx.day_utc, "coverage_status": "HEALTHY"})
    _write(ctx.truth_root / "market_calendar_v1" / "NYSE" / f"{ctx.day_utc[:4]}.jsonl", json.dumps({"day": ctx.day_utc, "status": "TRADING_DAY"}))


def _session_supporting_authorities(ctx: bod.BodContext) -> None:
    _write(
        ctx.truth_root / "reports" / "paper_session_authority_v1" / ctx.day_utc / "paper_session_authority.v1.json",
        {"day_utc": ctx.day_utc, "authority_status": "GRANTED"},
    )
    _write(
        ctx.truth_root / "reports" / "paper_session_bootstrap_v1" / ctx.day_utc / "paper_session_bootstrap.v1.json",
        {"day_utc": ctx.day_utc, "bootstrap_status": "PASS", **_bootstrap_meta(ctx)},
    )


def _session_blocked_artifacts(ctx: bod.BodContext) -> None:
    _write(
        ctx.truth_root / "active_session_v1" / "current.json",
        {
            "active_day": "2026-05-02",
            "target_day": ctx.day_utc,
            "promotion_state": "BLOCKED",
            "rollover_status": "ROLLOVER_WITHHELD",
            "rollover_reason_code": "HIDDEN_DEPENDENCY_DETECTED",
            "blocking_codes": ["HIDDEN_DEPENDENCY_DETECTED", "PARTIAL_BUILD", "REQUIRED_GATE_FAIL"],
        },
    )
    _write(
        ctx.truth_root / "target_day_admission_v1" / f"{ctx.day_utc}.json",
        {
            "target_day": ctx.day_utc,
            "admission_status": "BLOCKED",
            "blocking_reason_codes": ["HIDDEN_DEPENDENCY_DETECTED", "PARTIAL_BUILD", "REQUIRED_GATE_FAIL"],
            "hidden_dependency_check_result": {
                "status": "FAIL",
                "undeclared_dependency_artifacts": [
                    "runtime_resilience_authority_v1",
                    "safety_state_authority_v1",
                    "trading_day_readiness_authority_v1",
                ],
                "failing_producers": ["ops/tools/run_startup_materialization_input_convergence_v1.py"],
            },
            "blocker_chain": [
                {
                    "artifact_id": "startup_materialization_input_convergence_v1",
                    "artifact_path": str(
                        ctx.truth_root
                        / "reports"
                        / "startup_materialization_input_convergence_v1"
                        / ctx.day_utc
                        / "startup_materialization_input_convergence.v1.json"
                    ),
                    "blocker_code": "REQUIRED_GATE_FAIL",
                }
            ],
        },
    )
    _write(
        ctx.truth_root / "target_day_build_v1" / f"{ctx.day_utc}.json",
        {
            "target_day": ctx.day_utc,
            "build_status": "BLOCKED",
            "artifact_results": [
                {
                    "artifact_id": "startup_materialization_input_convergence_v1",
                    "required": True,
                    "result_status": "FAIL",
                    "blocker_codes": ["REQUIRED_GATE_FAIL"],
                    "producer": {"module": "ops/tools/run_startup_materialization_input_convergence_v1.py"},
                }
            ],
        },
    )
    _session_supporting_authorities(ctx)
    _write(
        ctx.truth_root / "reports" / "session_promotion_decision_v1" / ctx.day_utc / "session_promotion_decision.v1.json",
        {
            "target_day": ctx.day_utc,
            "promotion_state": "BLOCKED",
            "blocked_reason_codes": ["HIDDEN_DEPENDENCY_DETECTED", "PARTIAL_BUILD", "REQUIRED_GATE_FAIL"],
        },
    )
    _write(
        ctx.truth_root / "reports" / "pre_open_bundle_v1" / ctx.day_utc / "pre_open_bundle.v1.json",
        {
            "target_day": ctx.day_utc,
            "materialization_state": "INCOMPLETE",
            "blocking_reason_codes": ["REQUIRED_GATE_FAIL", "TARGET_DAY_DATE_MISMATCH"],
        },
    )
    _write(
        ctx.truth_root / "reports" / "paper_session_bootstrap_v1" / ctx.day_utc / "paper_session_bootstrap.v1.json",
        {"day_utc": ctx.day_utc, "bootstrap_status": "READY", **_bootstrap_meta(ctx)},
    )


def _broker_pass(ctx: bod.BodContext) -> None:
    _write(
        ctx.truth_root / "reports" / "runtime_resilience_authority_v1" / ctx.day_utc / "runtime_resilience_authority.v1.json",
        {"day_utc": ctx.day_utc, "status": "PASS", "canonical_blocker": "", **_artifact_meta(ctx)},
    )
    log = ctx.execution_root / "execution_evidence_v1" / "broker_events" / ctx.day_utc / "broker_event_log.v1.jsonl"
    log.parent.mkdir(parents=True, exist_ok=True)
    log.write_text("", encoding="utf-8")
    _write(log.parent / "broker_event_day_manifest.v1.json", {"day_utc": ctx.day_utc, "status": "PASS", "truth_root": str(ctx.execution_root), **_producer_contract()})
    _write(ctx.truth_root / "reports" / "broker_supply_v1" / ctx.day_utc / "broker_supply.v1.json", {"day_utc": ctx.day_utc, "status": "PASS", "canonical_blocker": "", **_artifact_meta(ctx)})


def _bod_pass(ctx: bod.BodContext) -> None:
    _write(ctx.operator_input_root / "operator_inputs" / "paper_capital_seed_v1" / ctx.day_utc / "paper_capital_seed.v1.json", {"day_utc": ctx.day_utc, "truth_root": str(ctx.operator_input_root), **_producer_contract()})
    _write(ctx.operator_input_root / "operator_inputs" / "cash_ledger_operator_statements" / ctx.day_utc / "operator_statement.v1.json", {"day_utc": ctx.day_utc, "truth_root": str(ctx.operator_input_root), **_producer_contract()})
    _write(ctx.truth_root / "reports" / "pre_open_bundle_v1" / ctx.day_utc / "pre_open_bundle.v1.json", {"target_day": ctx.day_utc, "producer_contract_v1": {"deterministic_fingerprint": "x"}})
    _write(
        ctx.truth_root / "reports" / "startup_materialization_input_convergence_v1" / ctx.day_utc / "startup_materialization_input_convergence.v1.json",
        {"day_utc": ctx.day_utc, "status": "PASS", "canonical_blocker": "", **_artifact_meta(ctx)},
    )
    _write(
        ctx.truth_root / "reports" / "safety_state_authority_v1" / ctx.day_utc / "safety_state_authority.v1.json",
        {"day_utc": ctx.day_utc, "status": "PASS", "canonical_blocker": "", **_artifact_meta(ctx)},
    )


def _market_pass(ctx: bod.BodContext) -> None:
    _write(ctx.truth_root / "reports" / "market_data_supply_v1" / ctx.day_utc / "market_data_supply.v1.json", {"day_utc": ctx.day_utc, "status": "PASS", "canonical_blocker": "", **_artifact_meta(ctx)})
    _write(ctx.truth_root / "reports" / "market_data_authority_v1" / ctx.day_utc / "market_data_authority.v1.json", {"day_utc": ctx.day_utc, "status": "PASS", "canonical_blocker": "", **_artifact_meta(ctx)})


def _feed_pass(ctx: bod.BodContext) -> None:
    _write(ctx.execution_root / "reports" / "feed_attestation_gate_v1" / ctx.day_utc / "feed_attestation_gate.v1.json", {"day_utc": ctx.day_utc, "status": "PASS", "canonical_blocker": "", "truth_root": str(ctx.execution_root), **_producer_contract()})


def _auth_pass(ctx: bod.BodContext) -> None:
    _write(ctx.truth_root / "reports" / "authorization_supply_v1" / ctx.day_utc / "authorization_supply.v1.json", {"day_utc": ctx.day_utc, "status": "PASS", "canonical_blocker": "", **_artifact_meta(ctx)})
    _write(ctx.execution_root / "reports" / "authorization_gate_verdict_v1" / ctx.day_utc / "authorization_gate_verdict.v1.json", {"day_utc": ctx.day_utc, "status": "PASS", "canonical_blocker": ""})


def _strategy_pass(ctx: bod.BodContext) -> None:
    _write(ctx.truth_root / "reports" / "trading_day_intent_generation_v1" / ctx.day_utc / "trading_day_intent_generation.v1.json", {"day_utc": ctx.day_utc, "status": "PASS", "canonical_blocker": "", **_artifact_meta(ctx)})


def _authorization_kill_pass(ctx: bod.BodContext) -> None:
    _auth_pass(ctx)
    _write(ctx.truth_root / "risk_v1" / "kill_switch_v1" / ctx.day_utc / "global_kill_switch_state.v1.json", {"day_utc": ctx.day_utc, "state": "INACTIVE", "status": "PASS", "canonical_blocker": "", **_artifact_meta(ctx)})


def _submit_boundary_pass(ctx: bod.BodContext) -> None:
    _write(ctx.truth_root / "reports" / "trading_day_readiness_authority_v1" / ctx.day_utc / "trading_day_readiness_authority.v1.json", {"day_utc": ctx.day_utc, "status": "PASS", "canonical_blocker": "", **_artifact_meta(ctx)})
    _write(ctx.truth_root / "reports" / "submit_boundary_status_v1" / ctx.day_utc / "submit_boundary_status.v1.json", {"day_utc": ctx.day_utc, "status": "PASS", "submit_allowed": True, "canonical_blocker": "", **_artifact_meta(ctx)})


def test_session_failure_defers_broker_bod_feed_and_submit(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _write(ctx.truth_root / "reports" / "pre_open_bundle_v1" / ctx.day_utc / "pre_open_bundle.v1.json", {"target_day": ctx.day_utc, "blocking_reason_codes": ["TARGET_DAY_DATE_MISMATCH"]})

    payload = cp.build_control_plane_v1(ctx)

    assert payload["current_phase"] == "SESSION_AUTHORITY"
    assert payload["canonical_blocker"] == "SESSION_IDENTITY_PRECHECK_FAILED"
    assert {"BROKER_CONNECTIVITY", "CAPITAL_SAFETY", "MARKET_FEED", "SUBMIT_BOUNDARY"} <= set(payload["deferred_domains"])


def test_skipped_or_missing_session_authority_blocks_before_bod(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _broker_pass(ctx)
    _bod_pass(ctx)
    phase_results = {"SESSION_AUTHORITY": {"status": "SKIPPED"}}
    monkeypatch.setattr(cp, "_evaluate_broker_health", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("broker should be deferred")))

    payload = cp.build_control_plane_v1(ctx, phase_results=phase_results)

    assert payload["current_phase"] == "SESSION_AUTHORITY"
    assert payload["canonical_blocker"] == "SESSION_IDENTITY_PRECHECK_FAILED"
    assert "BOD_INPUTS" in payload["deferred_phases"]


def test_control_plane_does_not_use_legacy_session_root_for_production(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    production_truth = tmp_path / "production_truth"
    legacy_truth = tmp_path / "truth"
    ctx = _ctx_with_truth(tmp_path, production_truth)
    legacy_ctx = _ctx_with_truth(tmp_path, legacy_truth)
    _session_pass(legacy_ctx)
    _broker_pass(ctx)
    _bod_pass(ctx)

    payload = cp.build_control_plane_v1(ctx)

    assert payload["current_phase"] == "SESSION_AUTHORITY"
    assert payload["canonical_blocker"] == "SESSION_IDENTITY_PRECHECK_FAILED"
    assert all(str(production_truth) in path or "repo_protection" in path for path in payload["evidence_paths"])


def test_session_hidden_dependency_is_decomposed_and_defers_downstream(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _session_blocked_artifacts(ctx)
    monkeypatch.setattr(cp, "_evaluate_broker_health", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("downstream phase evaluated")))

    payload = cp.build_control_plane_v1(ctx)

    assert payload["current_phase"] == "SESSION_AUTHORITY"
    assert payload["current_domain"] == "SESSION_IDENTITY"
    assert payload["canonical_blocker"] == "SESSION_IDENTITY_PRECHECK_FAILED"
    assert "target_day_admission_v1" in {row["dependency_id"] for row in payload["session_precheck_failures"]}
    assert any("run_session_authority_v1.py" in command for command in payload["recovery_commands"])
    assert {"BROKER_CONNECTIVITY", "MARKET_FEED", "AUTHORIZATION_KILL_SWITCH", "SUBMIT_BOUNDARY"} <= set(payload["deferred_domains"])
    assert any(row["blocking_reason"] == "HIDDEN_DEPENDENCY_DETECTED" for row in payload["session_precheck_failures"])


def test_session_identity_does_not_own_runtime_resilience_missing(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _session_pass(ctx)

    payload = cp.build_control_plane_v1(ctx)

    assert payload["current_domain"] == "BROKER_CONNECTIVITY"
    assert payload["current_phase"] == "BROKER_HEALTH"
    assert "runtime_resilience_authority_v1" in {row["dependency_id"] for row in payload["failed_current_domain_dependencies"]}
    assert "runtime_resilience_authority_v1" not in {row["dependency_id"] for row in payload["session_precheck_failures"]}


def test_session_identity_does_not_treat_blocked_bootstrap_as_satisfied(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _session_pass(ctx)
    _write(
        ctx.truth_root / "reports" / "paper_session_bootstrap_v1" / ctx.day_utc / "paper_session_bootstrap.v1.json",
        {"day_utc": ctx.day_utc, "bootstrap_status": "BLOCKED", "blocker_chain": ["NON_TRADING_DAY"]},
    )

    payload = cp.build_control_plane_v1(ctx)

    assert payload["current_domain"] == "SESSION_IDENTITY"
    assert payload["canonical_blocker"] == "PAPER_SESSION_BOOTSTRAP_NOT_READY"
    failed = payload["failed_current_domain_dependencies"]
    assert [row["dependency_id"] for row in failed] == ["paper_session_bootstrap_v1"]
    assert failed[0]["status"] == "FAIL"


def test_non_trading_day_blocks_session_identity_without_fake_readiness(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _session_pass(ctx)
    _write(
        ctx.truth_root / "reports" / "paper_session_authority_v1" / ctx.day_utc / "paper_session_authority.v1.json",
        {"day_utc": ctx.day_utc, "authority_status": "DENIED", "blocking_reason_codes": ["NON_TRADING_DAY"]},
    )
    _write(
        ctx.truth_root / "reports" / "paper_session_bootstrap_v1" / ctx.day_utc / "paper_session_bootstrap.v1.json",
        {
            "day_utc": ctx.day_utc,
            "bootstrap_status": "BLOCKED",
            "blocker_chain": ["NON_TRADING_DAY"],
            "canonical_stop_surface": "market_calendar_day",
            **_producer_contract(),
        },
    )

    payload = cp.build_control_plane_v1(ctx)

    assert payload["current_domain"] == "SESSION_IDENTITY"
    assert payload["current_phase"] == "SESSION_AUTHORITY"
    assert payload["canonical_blocker"] == "SESSION_IDENTITY_PRECHECK_FAILED"
    assert payload["final_status"] == "NOT_READY"
    assert payload["submit_allowed"] is False
    assert {"BROKER_CONNECTIVITY", "CAPITAL_SAFETY", "SUBMIT_BOUNDARY"} <= set(payload["deferred_domains"])
    failed = {row["dependency_id"]: row for row in payload["failed_current_domain_dependencies"]}
    assert failed["paper_session_authority_v1"]["reason_codes"] == ["NON_TRADING_DAY"]
    assert failed["paper_session_bootstrap_v1"]["reason_codes"] == ["NON_TRADING_DAY"]
    assert failed["paper_session_authority_v1"]["artifact_path"].endswith("paper_session_authority.v1.json")
    assert "run_session_authority_v1.py" in failed["paper_session_authority_v1"]["producer"]
    assert "run_paper_session_bootstrap_v1.py" in failed["paper_session_bootstrap_v1"]["recovery_command"]


def test_non_trading_day_remains_root_cause_when_session_artifacts_are_stale(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _session_pass(ctx)
    stale_contract = {
        "producer_contract_v1": {
            "code_version_git_commit": cp._current_git_commit_v1(),
            "source_dirty_status": "CLEAN",
            "generated_at_utc": "2026-05-03T13:00:00Z",
            "producer_name": "test",
            "producer_command": "test",
        }
    }
    _write(
        ctx.truth_root / "target_day_build_v1" / f"{ctx.day_utc}.json",
        {"target_day": ctx.day_utc, "build_status": "BLOCKED", "blocking_reason_codes": ["NON_TRADING_DAY"], **stale_contract},
    )
    _write(
        ctx.truth_root / "reports" / "paper_session_authority_v1" / ctx.day_utc / "paper_session_authority.v1.json",
        {"day_utc": ctx.day_utc, "authority_status": "DENIED", "blocking_reason_codes": ["NON_TRADING_DAY"], **stale_contract},
    )
    _write(
        ctx.truth_root / "reports" / "paper_session_bootstrap_v1" / ctx.day_utc / "paper_session_bootstrap.v1.json",
        {"day_utc": ctx.day_utc, "bootstrap_status": "BLOCKED", "blocker_chain": ["NON_TRADING_DAY"], **stale_contract},
    )

    payload = cp.build_control_plane_v1(ctx)
    monkeypatch.setattr(projection.bod, "_resolve_context", lambda *_args, **_kwargs: ctx)
    monkeypatch.setattr(projection, "control_plane_acceptance_issues_v1", lambda *_args, **_kwargs: [])
    _write(cp.control_plane_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc), payload)
    _out_path, projected = projection.run_operator_projection_v1(ctx.day_utc, ctx.environment, str(ctx.truth_root))

    assert payload["current_domain"] == "SESSION_IDENTITY"
    assert payload["current_session_sub_blocker"]["sub_blocker_code"] == "NON_TRADING_DAY"
    assert payload["blocker_reason"] == "NON_TRADING_DAY"
    assert payload["supporting_stale_dependencies"]
    assert any(row["blocking_reason"] == "STALE_ARTIFACT_GENERATED_AT_DAY_MISMATCH" for row in payload["failed_current_domain_dependencies"])
    assert projected["why_not_ready_summary"] == "SYSTEM NOT READY BECAUSE: SESSION_IDENTITY -> NON_TRADING_DAY"


def test_domain_precheck_surfaces_only_current_domain_failures_at_once(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    runtime_path = (
        ctx.truth_root
        / "reports"
        / "runtime_resilience_authority_v1"
        / ctx.day_utc
        / "runtime_resilience_authority.v1.json"
    )
    _session_pass(ctx)
    _write(
        runtime_path,
        {"day_utc": ctx.day_utc, "status": "BLOCKED", "reason_codes": ["IB_DISCONNECTED"], "canonical_blocker": "IB_DISCONNECTED", **_artifact_meta(ctx)},
    )

    payload = cp.build_control_plane_v1(ctx)

    assert payload["current_domain"] == "BROKER_CONNECTIVITY"
    assert payload["canonical_blocker"] == "BROKER_CONNECTIVITY_PRECHECK_FAILED"
    failed_ids = {row["dependency_id"] for row in payload["failed_current_domain_dependencies"]}
    assert {"runtime_resilience_authority_v1", "broker_event_log"} <= failed_ids
    assert not payload["session_precheck_failures"]
    assert "CAPITAL_SAFETY" in payload["deferred_domains"]


def test_broker_health_failure_defers_downstream(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _session_pass(ctx)

    payload = cp.build_control_plane_v1(ctx)

    assert payload["current_phase"] == "BROKER_HEALTH"
    assert payload["current_domain"] == "BROKER_CONNECTIVITY"
    assert payload["canonical_blocker"] == "BROKER_CONNECTIVITY_PRECHECK_FAILED"
    assert "broker_event_log" in {row["dependency_id"] for row in payload["failed_current_domain_dependencies"]}
    assert {"MARKET_FEED", "AUTHORIZATION_KILL_SWITCH", "SUBMIT_BOUNDARY"} <= set(payload["deferred_domains"])


def test_feed_attestation_is_current_only_after_earlier_phases_pass(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _session_pass(ctx)
    _broker_pass(ctx)
    _bod_pass(ctx)
    _market_pass(ctx)
    _write(ctx.execution_root / "reports" / "feed_attestation_gate_v1" / ctx.day_utc / "feed_attestation_gate.v1.json", {"day_utc": ctx.day_utc, "status": "FAIL", "reason_codes": ["FAL_STALE"], "truth_root": str(ctx.execution_root), **_producer_contract()})

    payload = cp.build_control_plane_v1(ctx)

    assert payload["current_phase"] == "MARKET_DATA"
    assert payload["current_domain"] == "MARKET_FEED"
    assert payload["canonical_blocker"] == "FAL_STALE"


def test_capital_safety_owns_nav_and_cash_failures_after_broker_passes(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _session_pass(ctx)
    _broker_pass(ctx)
    _write(
        ctx.truth_root / "reports" / "safety_state_authority_v1" / ctx.day_utc / "safety_state_authority.v1.json",
        {"day_utc": ctx.day_utc, "status": "BLOCKED", "reason_codes": ["NAV_INVALID"], "canonical_blocker": "NAV_INVALID", **_artifact_meta(ctx)},
    )
    _write(
        ctx.truth_root / "reports" / "startup_materialization_input_convergence_v1" / ctx.day_utc / "startup_materialization_input_convergence.v1.json",
        {"day_utc": ctx.day_utc, "status": "BLOCKED", "reason_codes": ["CASH_LEDGER_SNAPSHOT_V1_MISSING"], "canonical_blocker": "CASH_LEDGER_SNAPSHOT_V1_MISSING", **_artifact_meta(ctx)},
    )

    payload = cp.build_control_plane_v1(ctx)

    assert payload["current_domain"] == "CAPITAL_SAFETY"
    assert payload["canonical_blocker"] == "CAPITAL_SAFETY_PRECHECK_FAILED"
    failed = {row["blocking_reason"] for row in payload["failed_current_domain_dependencies"]}
    assert {"NAV_INVALID", "CASH_LEDGER_SNAPSHOT_V1_MISSING"} <= failed
    assert not payload["session_precheck_failures"]


def test_strategy_intent_owns_missing_intent_inputs_after_feed_passes(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _session_pass(ctx)
    _broker_pass(ctx)
    _bod_pass(ctx)
    _market_pass(ctx)
    _feed_pass(ctx)
    _write(
        ctx.truth_root / "reports" / "trading_day_intent_generation_v1" / ctx.day_utc / "trading_day_intent_generation.v1.json",
        {"day_utc": ctx.day_utc, "status": "BLOCKED", "reason_codes": ["MISSING_REQUIRED_INPUTS"], "canonical_blocker": "MISSING_REQUIRED_INPUTS", **_artifact_meta(ctx)},
    )

    payload = cp.build_control_plane_v1(ctx)

    assert payload["current_domain"] == "STRATEGY_INTENT"
    assert payload["canonical_blocker"] == "MISSING_REQUIRED_INPUTS"


def test_submit_boundary_owns_submit_mode_after_prior_domains_pass(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _session_pass(ctx)
    _broker_pass(ctx)
    _bod_pass(ctx)
    _market_pass(ctx)
    _feed_pass(ctx)
    _strategy_pass(ctx)
    _authorization_kill_pass(ctx)
    _write(
        ctx.truth_root / "reports" / "trading_day_readiness_authority_v1" / ctx.day_utc / "trading_day_readiness_authority.v1.json",
        {"day_utc": ctx.day_utc, "status": "BLOCKED", "canonical_blocker": "SUBMIT_NOT_ALLOWED_BY_TRADING_DAY_MODE", **_artifact_meta(ctx)},
    )

    payload = cp.build_control_plane_v1(ctx)

    assert payload["current_domain"] == "SUBMIT_BOUNDARY"
    assert payload["canonical_blocker"] == "SUBMIT_BOUNDARY_PRECHECK_FAILED"
    assert "trading_day_readiness_authority_v1" in {row["dependency_id"] for row in payload["failed_current_domain_dependencies"]}


def test_readiness_domain_registry_has_single_owner_for_key_dependencies() -> None:
    domains = cp.load_readiness_domain_registry_v1()
    owners: dict[str, list[str]] = {}
    for domain in domains:
        for dep in domain.get("dependencies", []):
            owners.setdefault(dep["dependency_id"], []).append(dep["domain_owner"])
    assert owners["runtime_resilience_authority_v1"] == ["BROKER_CONNECTIVITY"]
    assert owners["safety_state_authority_v1"] == ["CAPITAL_SAFETY"]
    assert owners["startup_materialization_input_convergence_v1"] == ["CAPITAL_SAFETY"]
    assert owners["trading_day_intent_generation_v1"] == ["STRATEGY_INTENT"]
    assert owners["trading_day_readiness_authority_v1"] == ["SUBMIT_BOUNDARY"]


def test_kill_switch_blocker_is_owned_by_kill_switch(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _session_pass(ctx)
    _broker_pass(ctx)
    _bod_pass(ctx)
    _market_pass(ctx)
    _feed_pass(ctx)
    _strategy_pass(ctx)
    _auth_pass(ctx)
    _write(ctx.truth_root / "risk_v1" / "kill_switch_v1" / ctx.day_utc / "global_kill_switch_state.v1.json", {"day_utc": ctx.day_utc, "state": "ACTIVE", "reason_codes": ["C2_KILL_SWITCH_ACTIVE"], **_artifact_meta(ctx)})

    payload = cp.build_control_plane_v1(ctx)

    assert payload["current_phase"] == "KILL_SWITCH"
    assert payload["current_domain"] == "AUTHORIZATION_KILL_SWITCH"
    assert payload["canonical_blocker"] == "C2_KILL_SWITCH_ACTIVE"


def test_operator_projection_uses_one_control_plane_blocker(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _session_pass(ctx)
    monkeypatch.setattr(cp.bod, "_resolve_context", lambda *_args, **_kwargs: ctx)
    path, control = cp.run_control_plane_v1(ctx.day_utc, ctx.environment, str(ctx.truth_root))
    assert path.exists()
    monkeypatch.setattr(projection.bod, "_resolve_context", lambda *_args, **_kwargs: ctx)

    _out_path, payload = projection.run_operator_projection_v1(ctx.day_utc, ctx.environment, str(ctx.truth_root))

    assert payload["phase"] == "BROKER_HEALTH"
    assert payload["current_domain"] == "BROKER_CONNECTIVITY"
    assert payload["canonical_blocker"] == "BROKER_CONNECTIVITY_PRECHECK_FAILED"
    assert payload["operator_next_action"] == control["recovery_action"]
    assert payload["evidence_paths"]
    assert "SUBMIT_BOUNDARY" in payload["deferred_downstream_phases"]


def test_operator_projection_shows_exact_session_recovery_without_downstream_actions(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _session_blocked_artifacts(ctx)
    monkeypatch.setattr(cp.bod, "_resolve_context", lambda *_args, **_kwargs: ctx)
    _cp_path, control = cp.run_control_plane_v1(ctx.day_utc, ctx.environment, str(ctx.truth_root))
    monkeypatch.setattr(projection.bod, "_resolve_context", lambda *_args, **_kwargs: ctx)

    _out_path, payload = projection.run_operator_projection_v1(ctx.day_utc, ctx.environment, str(ctx.truth_root))

    assert payload["canonical_blocker"] == "SESSION_IDENTITY_PRECHECK_FAILED"
    assert payload["operator_next_action"] == "Resolve all listed SESSION_IDENTITY precheck failures, then rerun the control plane."
    assert payload["session_precheck_failures"]
    assert any(row["dependency_id"] == "target_day_admission_v1" for row in payload["session_precheck_failures"])
    assert str(ctx.truth_root / "target_day_admission_v1" / f"{ctx.day_utc}.json") in payload["evidence_paths"]
    assert payload["next_valid_actions"]
    assert not [action for action in payload["next_valid_actions"] if "broker" in action.lower() or "kill" in action.lower() or "submit" in action.lower()]


def test_requirement_graph_defers_downstream_missing_artifacts_when_broker_is_current(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _session_pass(ctx)
    monkeypatch.setattr(cp.bod, "_resolve_context", lambda *_args, **_kwargs: ctx)
    control_path, control = cp.run_control_plane_v1(ctx.day_utc, ctx.environment, str(ctx.truth_root))
    assert control_path.exists()
    assert control["current_domain"] == "BROKER_CONNECTIVITY"

    payload = graph.build_requirement_graph(ctx)
    broker_node = next(row for row in payload["requirements"] if row["requirement_id"] == "BROKER_HEALTH:broker_event_log")
    deferred_market = [row for row in payload["requirements"] if row["owner_phase"] in {"MARKET_DATA", "AUTHORIZATION", "SUBMIT_BOUNDARY"} and row["status"] == "DEFERRED_BY_UPSTREAM_DOMAIN"]

    assert broker_node["status"] == "BLOCKING_CURRENT_DOMAIN"
    assert broker_node["canonical_blocker"] == "BROKER_EVENT_LOG_MISSING"
    assert deferred_market


def test_readiness_registry_contract_is_mandatory_and_single_owner() -> None:
    domains = cp.load_readiness_domain_registry_v1()
    owners: dict[str, str] = {}
    for domain in domains:
        assert domain["domain_id"]
        for dep in domain["dependencies"]:
            dependency_id = dep["dependency_id"]
            assert dep["domain_owner"] == domain["domain_id"]
            assert dep["owning_domain"] == domain["domain_id"]
            assert dependency_id not in owners
            owners[dependency_id] = dep["domain_owner"]
            for key in (
                "expected_path",
                "artifact_path",
                "schema_path",
                "producer_command",
                "governed_producer",
                "recovery_action",
                "recovery_command",
                "blocking_scope",
            ):
                assert dep[key]
            assert dep["artifact_path"] == dep["expected_path"]
            assert dep["governed_producer"] == dep["producer_command"]
            if dep.get("schema_exempt") is True:
                assert dep.get("schema_exempt_reason")
            else:
                assert (REPO_ROOT / dep["schema_path"]).exists()
            if dep.get("metadata_exempt") is True:
                assert dep.get("metadata_exempt_reason")


def test_readiness_registry_completeness_scanner_covers_dependency_usages() -> None:
    domains = cp.load_readiness_domain_registry_v1()
    registered = {
        str(dep.get("dependency_id") or "").strip()
        for domain in domains
        for dep in domain.get("dependencies", [])
        if isinstance(dep, dict)
    }
    source_paths = [
        REPO_ROOT / "ops" / "tools" / "run_aegis_control_plane_v1.py",
        REPO_ROOT / "governance" / "02_REGISTRIES" / "aegis_readiness_domain_registry_v1.json",
    ]
    used: set[str] = set()
    for path in source_paths:
        text = path.read_text(encoding="utf-8")
        used.update(re.findall(r'dependency_id["\']?\s*(?:==|:)\s*["\']([A-Za-z0-9_]+)["\']', text))
        used.update(re.findall(r'["\'](broker_event_log|market_calendar_day|paper_capital_seed|operator_statement)["\']', text))
    readiness_like = {item for item in used if item in registered or item.endswith("_v1") or item in {"broker_event_log", "market_calendar_day", "paper_capital_seed", "operator_statement"}}
    unknown = sorted(item for item in readiness_like if item not in registered)
    assert unknown == []


def test_required_readiness_dependencies_have_schema_validation_or_explicit_exemption() -> None:
    missing: list[str] = []
    for domain in cp.load_readiness_domain_registry_v1():
        for dep in domain.get("dependencies", []):
            if not isinstance(dep, dict) or dep.get("required") is not True or dep.get("diagnostic_only") is True:
                continue
            dep_id = str(dep.get("dependency_id") or "")
            if dep.get("schema_exempt") is True:
                if not str(dep.get("schema_exempt_reason") or "").strip():
                    missing.append(dep_id)
                continue
            schema_path = str(dep.get("schema_path") or "").strip()
            if not schema_path or not (REPO_ROOT / schema_path).exists():
                missing.append(dep_id)
            if dep.get("schema_instance_exempt") is True and not str(dep.get("schema_instance_exempt_reason") or "").strip():
                missing.append(dep_id)
    assert missing == []


def test_readiness_contract_schemas_reject_missing_contract_fields() -> None:
    for schema_path in REPORT_CONTRACT_SCHEMAS:
        with pytest.raises(SchemaValidationError):
            validate_against_repo_schema_v1({"day_utc": DAY, "status": "PASS"}, REPO_ROOT, schema_path)
        with pytest.raises(SchemaValidationError):
            validate_against_repo_schema_v1(
                {
                    "day_utc": DAY,
                    "status": "BLOCKED",
                    "generated_at_utc": f"{DAY}T13:00:00Z",
                    "producer": {"module": "ops/tools/example.py", "git_sha": "a" * 40},
                    "truth_root": "/tmp/truth",
                },
                REPO_ROOT,
                schema_path,
            )

    with pytest.raises(SchemaValidationError):
        validate_against_repo_schema_v1(
            {"day_utc": DAY, "cash_total": 100000, "nlv_total": 100000},
            REPO_ROOT,
            "governance/04_DATA/SCHEMAS/C2/INPUTS/paper_capital_seed.v1.schema.json",
        )
    with pytest.raises(SchemaValidationError):
        validate_against_repo_schema_v1(
            {"account_id": "DU123456", "observed_at_utc": f"{DAY}T13:00:00Z"},
            REPO_ROOT,
            "governance/04_DATA/SCHEMAS/C2/INPUTS/operator_statement.v1.schema.json",
        )


def test_readiness_contract_schemas_accept_governed_artifact_shapes() -> None:
    base_report = {
        "day_utc": DAY,
        "status": "BLOCKED",
        "canonical_blocker": "TEST_BLOCKER",
        "reason_codes": ["TEST_BLOCKER"],
        "generated_at_utc": f"{DAY}T13:00:00Z",
        "producer": {"repo": "constellation", "module": "ops/tools/example.py", "git_sha": "a" * 40},
        "truth_root": "/tmp/truth",
    }
    for schema_path in REPORT_CONTRACT_SCHEMAS:
        validate_against_repo_schema_v1(base_report, REPO_ROOT, schema_path)

    validate_against_repo_schema_v1(
        {
            "schema_id": "paper_capital_seed",
            "schema_version": "v1",
            "day_utc": DAY,
            "environment": "PAPER",
            "ib_account": "DU123456",
            "produced_utc": f"{DAY}T13:00:00Z",
            "seed_mode": "OPERATOR_SUPPLIED",
            "cash_total": 100000,
            "nlv_total": 100000,
            "currency": "USD",
        },
        REPO_ROOT,
        "governance/04_DATA/SCHEMAS/C2/INPUTS/paper_capital_seed.v1.schema.json",
    )
    validate_against_repo_schema_v1(
        {
            "account_id": "DU123456",
            "observed_at_utc": f"{DAY}T13:00:00Z",
            "nlv_total": 100000,
            "cash_total": 100000,
            "available_funds": 100000,
            "excess_liquidity": 100000,
            "currency": "USD",
        },
        REPO_ROOT,
        "governance/04_DATA/SCHEMAS/C2/INPUTS/operator_statement.v1.schema.json",
    )


def test_control_plane_schema_requires_self_binding_fields(tmp_path: Path) -> None:
    schema_path = "governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_control_plane.v1.schema.json"
    with pytest.raises(SchemaValidationError):
        validate_against_repo_schema_v1(
            {
                "schema_id": "aegis_control_plane",
                "schema_version": cp.SCHEMA_VERSION,
                "day_utc": DAY,
                "final_status": "READY",
                "current_domain": "",
                "current_phase": "",
                "canonical_blocker": "",
                "submit_allowed": True,
                "producer_contract_v1": {
                    "code_version_git_commit": cp._current_git_commit_v1(),
                    "source_dirty_status": "CLEAN",
                    "generated_at_utc": f"{DAY}T13:00:00Z",
                },
            },
            REPO_ROOT,
            schema_path,
        )

    truth_root = tmp_path / "candidate_truth"
    path = cp.control_plane_path(truth_root=truth_root, day_utc=DAY)
    payload = _self_bound_control_payload(
        actual_path=path,
        truth_root=truth_root,
        runtime_root=tmp_path,
        runtime_mode="CANDIDATE",
    )
    validate_against_repo_schema_v1(payload, REPO_ROOT, schema_path)


def test_control_plane_self_binding_accepts_only_actual_output_path(tmp_path: Path) -> None:
    truth_root = tmp_path / "candidate_truth"
    path = cp.control_plane_path(truth_root=truth_root, day_utc=DAY)
    payload = _self_bound_control_payload(
        actual_path=path,
        truth_root=truth_root,
        runtime_root=tmp_path,
        runtime_mode="CANDIDATE",
    )
    assert cp.control_plane_self_binding_issues_v1(payload, actual_path=path) == []

    copied_path = cp.control_plane_path(truth_root=tmp_path / "production_truth", day_utc=DAY)
    codes = {row["code"] for row in cp.control_plane_self_binding_issues_v1(payload, actual_path=copied_path)}
    assert "CONTROL_PLANE_OUTPUT_PATH_MISMATCH" in codes
    assert "CONTROL_PLANE_TRUTH_ROOT_MISMATCH" in codes


def test_production_control_plane_rejects_candidate_runtime_mode_and_paths(tmp_path: Path) -> None:
    production_truth = tmp_path / "production_truth"
    candidate_truth = tmp_path / "candidate_truth"
    path = cp.control_plane_path(truth_root=production_truth, day_utc=DAY)
    payload = _self_bound_control_payload(
        actual_path=path,
        truth_root=production_truth,
        runtime_root=tmp_path,
        runtime_mode="CANDIDATE",
        output_path=cp.control_plane_path(truth_root=candidate_truth, day_utc=DAY),
    )
    payload["evidence_paths"] = [str(candidate_truth / "target_day_build_v1" / f"{DAY}.json")]
    payload["readiness_dependency_inventory"] = [
        {"dependency_id": "stale_downstream", "upstream_artifacts": [str(candidate_truth / "reports" / "stale.json")]}
    ]

    codes = {row["code"] for row in cp.control_plane_self_binding_issues_v1(payload, actual_path=path)}

    assert "CONTROL_PLANE_RUNTIME_MODE_MISMATCH" in codes
    assert "CONTROL_PLANE_OUTPUT_PATH_MISMATCH" in codes
    assert "CONTROL_PLANE_TRUTH_ROOT_MISMATCH" in codes


def test_control_plane_self_binding_rejects_wrong_truth_root_runtime_root_mode_and_metadata(tmp_path: Path) -> None:
    truth_root = tmp_path / "candidate_truth"
    path = cp.control_plane_path(truth_root=truth_root, day_utc=DAY)
    payload = _self_bound_control_payload(
        actual_path=path,
        truth_root=tmp_path / "other_truth",
        runtime_root=tmp_path / "other_runtime",
        runtime_mode="PRODUCTION",
    )
    payload["producer_contract_v1"]["code_version_git_commit"] = "0" * 40

    codes = {row["code"] for row in cp.control_plane_self_binding_issues_v1(payload, actual_path=path)}

    assert "CONTROL_PLANE_TRUTH_ROOT_MISMATCH" in codes
    assert "CONTROL_PLANE_RUNTIME_ROOT_MISMATCH" in codes
    assert "CONTROL_PLANE_RUNTIME_MODE_MISMATCH" in codes
    assert "CONTROL_PLANE_PRODUCER_METADATA_STALE" in codes

    payload.pop("producer_contract_v1")
    codes = {row["code"] for row in cp.control_plane_self_binding_issues_v1(payload, actual_path=path)}
    assert "CONTROL_PLANE_PRODUCER_METADATA_MISSING" in codes


def test_control_plane_validates_required_dependency_schema_instances(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    dep = next(
        row
        for domain in cp.load_readiness_domain_registry_v1()
        for row in domain["dependencies"]
        if row["dependency_id"] == "runtime_resilience_authority_v1"
    )
    path = cp._dependency_path(dep, ctx)
    _write(
        path,
        {
            "schema_id": "runtime_resilience_authority",
            "schema_version": "v1",
            "day_utc": ctx.day_utc,
            "status": "PASS",
            "generated_at_utc": f"{ctx.day_utc}T13:00:00Z",
            "producer": {"repo": "constellation", "module": "ops/tools/run_runtime_resilience_authority_v1.py", "git_sha": cp._current_git_commit_v1()},
            "truth_root": str(ctx.truth_root),
        },
    )

    result = cp._evaluate_domain_dependency(dep, ctx)

    assert result["status"] == "SATISFIED"

    _write(
        path,
        {
            "schema_id": "runtime_resilience_authority",
            "schema_version": "v1",
            "day_utc": ctx.day_utc,
            "generated_at_utc": f"{ctx.day_utc}T13:00:00Z",
            "producer": {"repo": "constellation", "module": "ops/tools/run_runtime_resilience_authority_v1.py", "git_sha": cp._current_git_commit_v1()},
            "truth_root": str(ctx.truth_root),
        },
    )

    result = cp._evaluate_domain_dependency(dep, ctx)

    assert result["status"] == "FAIL"
    assert result["blocking_reason"] == "SCHEMA_INSTANCE_INVALID"


def test_control_plane_dependency_result_includes_lineage_when_available(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    input_path = ctx.truth_root / "inputs" / "runtime_resilience_input.v1.json"
    _write(input_path, {"day_utc": ctx.day_utc})
    dep = next(
        row
        for domain in cp.load_readiness_domain_registry_v1()
        for row in domain["dependencies"]
        if row["dependency_id"] == "runtime_resilience_authority_v1"
    )
    path = cp._dependency_path(dep, ctx)
    _write(
        path,
        {
            "schema_id": "runtime_resilience_authority",
            "schema_version": "v1",
            "day_utc": ctx.day_utc,
            "status": "PASS",
            "generated_at_utc": f"{ctx.day_utc}T13:00:00Z",
            "producer": {"repo": "constellation", "module": "ops/tools/run_runtime_resilience_authority_v1.py", "git_sha": cp._current_git_commit_v1()},
            "truth_root": str(ctx.truth_root),
            "producer_contract_v1": {
                "producer_name": "ops/tools/run_runtime_resilience_authority_v1.py",
                "producer_command": "run runtime resilience",
                "code_version_git_commit": cp._current_git_commit_v1(),
                "source_dirty_status": "CLEAN",
                "input_artifacts": [{"path": str(input_path)}],
            },
        },
    )

    result = cp._evaluate_domain_dependency(dep, ctx)

    assert str(input_path) in result["upstream_artifacts"]
    assert "ops/tools/run_runtime_resilience_authority_v1.py" in result["upstream_producers"]
    assert str(ctx.truth_root) in result["upstream_truth_roots"]


def test_registry_missing_schema_path_and_exemptions_are_explicit() -> None:
    bad = [
        {
            "domain_id": "BROKER_CONNECTIVITY",
            "domain_order": 2,
            "dependencies": [
                {
                    "dependency_id": "runtime_resilience_authority_v1",
                    "domain_owner": "BROKER_CONNECTIVITY",
                    "owning_domain": "BROKER_CONNECTIVITY",
                    "expected_path": "x",
                    "artifact_path": "x",
                    "schema_path": "governance/04_DATA/SCHEMAS/C2/REPORTS/missing.schema.json",
                    "producer_command": "x",
                    "governed_producer": "x",
                    "recovery_action": "x",
                    "recovery_command": "x",
                    "blocking_scope": "BROKER_CONNECTIVITY",
                }
            ],
        }
    ]
    with pytest.raises(RuntimeError, match="READINESS_DEPENDENCY_SCHEMA_PATH_MISSING"):
        cp._validate_readiness_domain_registry_v1(bad)

    domains = cp.load_readiness_domain_registry_v1()
    broker_event = next(
        row
        for domain in domains
        for row in domain["dependencies"]
        if row["dependency_id"] == "broker_event_log"
    )
    market_calendar = next(
        row
        for domain in domains
        for row in domain["dependencies"]
        if row["dependency_id"] == "market_calendar_day"
    )
    assert broker_event["schema_instance_exempt_reason"]
    assert market_calendar["schema_instance_exempt_reason"]


def test_broker_event_log_requires_paired_day_manifest(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _session_pass(ctx)
    _broker_pass(ctx)
    manifest = (
        ctx.execution_root
        / "execution_evidence_v1"
        / "broker_events"
        / ctx.day_utc
        / "broker_event_day_manifest.v1.json"
    )
    manifest.unlink()

    payload = cp.build_control_plane_v1(ctx)
    broker_event = next(row for row in payload["readiness_dependency_inventory"] if row["dependency_id"] == "broker_event_log")

    assert payload["current_domain"] == "BROKER_CONNECTIVITY"
    assert broker_event["status"] == "FAIL"
    assert broker_event["blocking_reason"] == "BROKER_EVENT_DAY_MANIFEST_MISSING"

    _write(manifest, {"day_utc": "2026-05-01", "status": "PASS", "truth_root": str(ctx.execution_root), **_producer_contract()})
    payload = cp.build_control_plane_v1(ctx)
    broker_event = next(row for row in payload["readiness_dependency_inventory"] if row["dependency_id"] == "broker_event_log")
    assert broker_event["blocking_reason"] == "BROKER_EVENT_DAY_MANIFEST_DAY_MISMATCH"


def test_market_calendar_day_requires_paired_jsonl_coverage(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _session_pass(ctx)
    calendar_year = ctx.truth_root / "market_calendar_v1" / "NYSE" / f"{ctx.day_utc[:4]}.jsonl"
    calendar_year.unlink()

    payload = cp.build_control_plane_v1(ctx)
    calendar_day = next(row for row in payload["readiness_dependency_inventory"] if row["dependency_id"] == "market_calendar_day")

    assert payload["current_domain"] == "SESSION_IDENTITY"
    assert calendar_day["status"] == "FAIL"
    assert calendar_day["blocking_reason"] == "MARKET_CALENDAR_YEAR_MISSING"

    _write(calendar_year, json.dumps({"day": "2026-05-01", "status": "TRADING_DAY"}))
    payload = cp.build_control_plane_v1(ctx)
    calendar_day = next(row for row in payload["readiness_dependency_inventory"] if row["dependency_id"] == "market_calendar_day")
    assert calendar_day["blocking_reason"] == "MARKET_CALENDAR_DAY_MISSING"


def test_control_plane_freshness_policy_fails_closed(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):  # noqa: ANN001
            return datetime(2026, 5, 4, 13, 0, 0, tzinfo=UTC)

    monkeypatch.setattr(cp, "datetime", FixedDateTime)
    dep = {
        "dependency_id": "runtime_resilience_authority_v1",
        "domain_owner": "BROKER_CONNECTIVITY",
        "owning_domain": "BROKER_CONNECTIVITY",
        "required_for": ["broker_health"],
        "blocking_scope": "BROKER_CONNECTIVITY",
        "expected_path": "{truth_root}/reports/runtime_resilience_authority_v1/{day_utc}/runtime_resilience_authority.v1.json",
        "artifact_path": "{truth_root}/reports/runtime_resilience_authority_v1/{day_utc}/runtime_resilience_authority.v1.json",
        "schema_path": "governance/04_DATA/SCHEMAS/C2/REPORTS/runtime_resilience_authority.v1.schema.json",
        "producer_command": "x",
        "governed_producer": "x",
        "recovery_action": "x",
        "recovery_command": "x",
        "required": True,
        "diagnostic_only": False,
        "freshness_policy": {"require_timestamp": True, "max_age_seconds": 1},
    }
    payload = {
        "schema_id": "runtime_resilience_authority",
        "schema_version": "v1",
        "day_utc": ctx.day_utc,
        "status": "PASS",
        "generated_at_utc": f"{ctx.day_utc}T00:00:00Z",
        "producer": {"repo": "constellation", "module": "ops/tools/run_runtime_resilience_authority_v1.py", "git_sha": cp._current_git_commit_v1()},
        "truth_root": str(ctx.truth_root),
    }

    blocker, _detail = cp._artifact_freshness_issue_v1(dep=dep, payload=payload, ctx=ctx, require_freshness_metadata=True)
    assert blocker == "ARTIFACT_FRESHNESS_EXPIRED"

    missing_timestamp = dict(payload)
    missing_timestamp.pop("generated_at_utc")
    blocker, _detail = cp._artifact_freshness_issue_v1(dep=dep, payload=missing_timestamp, ctx=ctx, require_freshness_metadata=True)
    assert blocker == "FRESHNESS_TIMESTAMP_MISSING"

    wrong_session_dep = dict(dep)
    wrong_session_dep["freshness_policy"] = {"session_field": "session_id", "expected_session": "{day_utc}:PAPER", "require_session": True}
    wrong_session = dict(payload)
    wrong_session["session_id"] = "2026-05-03:PAPER"
    blocker, _detail = cp._artifact_freshness_issue_v1(dep=wrong_session_dep, payload=wrong_session, ctx=ctx, require_freshness_metadata=True)
    assert blocker == "SESSION_SCOPE_MISMATCH"

    exempt_dep = dict(dep)
    exempt_dep["freshness_exempt"] = True
    blocker, _detail = cp._artifact_freshness_issue_v1(dep=exempt_dep, payload={}, ctx=ctx, require_freshness_metadata=True)
    assert blocker == ""


def test_control_plane_build_is_reproducible_after_stable_normalization(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _session_pass(ctx)

    first = cp.build_control_plane_v1(ctx)
    second = cp.build_control_plane_v1(ctx)

    def stable(payload: dict) -> dict:
        copy = json.loads(json.dumps(payload, sort_keys=True))
        copy["generated_at_utc"] = "<normalized>"
        return copy

    assert stable(first) == stable(second)


def test_stale_deferred_downstream_artifacts_are_quarantined_not_actionable(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _session_pass(ctx)
    _write(
        ctx.truth_root / "reports" / "paper_session_bootstrap_v1" / ctx.day_utc / "paper_session_bootstrap.v1.json",
        {"day_utc": ctx.day_utc, "bootstrap_status": "BLOCKED", "blocker_chain": ["NON_TRADING_DAY"], **_producer_contract()},
    )
    _write(
        ctx.truth_root / "reports" / "runtime_resilience_authority_v1" / ctx.day_utc / "runtime_resilience_authority.v1.json",
        {
            "schema_id": "runtime_resilience_authority",
            "schema_version": "v1",
            "day_utc": ctx.day_utc,
            "generated_at_utc": f"{ctx.day_utc}T13:00:00Z",
            "producer": {"repo": "constellation", "module": "ops/tools/run_runtime_resilience_authority_v1.py", "git_sha": cp._current_git_commit_v1()},
            "truth_root": str(ctx.truth_root),
        },
    )

    payload = cp.build_control_plane_v1(ctx)

    assert payload["current_domain"] == "SESSION_IDENTITY"
    assert payload["canonical_blocker"] == "PAPER_SESSION_BOOTSTRAP_NOT_READY"
    findings = [row for row in payload["diagnostic_findings"] if row.get("dependency_id") == "runtime_resilience_authority_v1"]
    assert findings
    assert findings[0]["classification"] == "DEFERRED_EVIDENCE_ONLY"
    assert findings[0]["actionable"] is False


def test_registry_rejects_session_identity_leakage(monkeypatch) -> None:  # noqa: ANN001
    bad = [
        {
            "domain_id": "SESSION_IDENTITY",
            "domain_order": 1,
            "dependencies": [
                {
                    "dependency_id": "active_session_v1",
                    "domain_owner": "SESSION_IDENTITY",
                    "owning_domain": "SESSION_IDENTITY",
                    "expected_path": "x",
                    "artifact_path": "x",
                    "schema_path": "governance/04_DATA/SCHEMAS/C2/REPORTS/active_session.v1.schema.json",
                    "producer_command": "x",
                    "governed_producer": "x",
                    "recovery_action": "x",
                    "recovery_command": "x",
                    "blocking_scope": "SESSION_IDENTITY",
                    "blocker_codes_owned": ["IB_DISCONNECTED"],
                }
            ],
        }
    ]
    with pytest.raises(RuntimeError, match="SESSION_IDENTITY_FORBIDDEN_BLOCKER"):
        cp._validate_readiness_domain_registry_v1(bad)


def test_session_identity_uses_positive_dependency_allowlist() -> None:
    bad = [
        {
            "domain_id": "SESSION_IDENTITY",
            "domain_order": 1,
            "dependencies": [
                {
                    "dependency_id": "new_downstream_readiness_v1",
                    "domain_owner": "SESSION_IDENTITY",
                    "owning_domain": "SESSION_IDENTITY",
                    "expected_path": "x",
                    "artifact_path": "x",
                    "schema_path": "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_session_bootstrap.v1.schema.json",
                    "producer_command": "x",
                    "governed_producer": "x",
                    "recovery_action": "x",
                    "recovery_command": "x",
                    "blocking_scope": "SESSION_IDENTITY",
                    "blocker_codes_owned": ["NEW_DOWNSTREAM_BLOCKER"],
                }
            ],
        }
    ]
    with pytest.raises(RuntimeError, match="SESSION_IDENTITY_DEPENDENCY_NOT_ALLOWLISTED"):
        cp._validate_readiness_domain_registry_v1(bad)


def test_unknown_evaluated_dependency_fails_closed(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _session_pass(ctx)
    original = cp._evaluate_domain_dependency

    def fake(dep, context):  # noqa: ANN001
        row = original(dep, context)
        if row["dependency_id"] == "runtime_resilience_authority_v1":
            row["dependency_id"] = "unregistered_runtime_dependency_v1"
        return row

    monkeypatch.setattr(cp, "_evaluate_domain_dependency", fake)
    with pytest.raises(RuntimeError, match="READINESS_DEPENDENCY_NOT_REGISTERED"):
        cp.build_control_plane_v1(ctx)


def test_registry_rejects_missing_producer_contract_metadata() -> None:
    bad = [
        {
            "domain_id": "SESSION_IDENTITY",
            "domain_order": 1,
            "dependencies": [
                {
                    "dependency_id": "paper_session_bootstrap_v1",
                    "domain_owner": "SESSION_IDENTITY",
                    "owning_domain": "SESSION_IDENTITY",
                    "expected_path": "x",
                    "artifact_path": "x",
                    "schema_path": "x",
                    "producer_command": "x",
                    "governed_producer": "x",
                    "recovery_action": "x",
                    "blocking_scope": "SESSION_IDENTITY",
                    "blocker_codes_owned": ["PAPER_SESSION_BOOTSTRAP_V1_MISSING"],
                }
            ],
        }
    ]
    with pytest.raises(RuntimeError, match="READINESS_DEPENDENCY_CONTRACT_INCOMPLETE:paper_session_bootstrap_v1:recovery_command"):
        cp._validate_readiness_domain_registry_v1(bad)


def test_stale_artifact_git_commit_is_rejected(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _session_pass(ctx)
    _write(
        ctx.truth_root / "reports" / "paper_session_bootstrap_v1" / ctx.day_utc / "paper_session_bootstrap.v1.json",
        {
            "day_utc": ctx.day_utc,
            "bootstrap_status": "PASS",
            "producer_contract_v1": {"code_version_git_commit": "0000000000000000000000000000000000000000", "source_dirty_status": "CLEAN"},
        },
    )

    payload = cp.build_control_plane_v1(ctx)

    assert payload["current_domain"] == "SESSION_IDENTITY"
    assert payload["canonical_blocker"] == "STALE_ARTIFACT_GIT_COMMIT_MISMATCH"
    assert payload["failed_current_domain_dependencies"][0]["dependency_id"] == "paper_session_bootstrap_v1"


def test_truth_root_mismatch_is_rejected(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _session_pass(ctx)
    _write(
        ctx.truth_root / "reports" / "paper_session_bootstrap_v1" / ctx.day_utc / "paper_session_bootstrap.v1.json",
        {
            "day_utc": ctx.day_utc,
            "truth_root": str(tmp_path / "candidate_truth"),
            "bootstrap_status": "PASS",
            **_producer_contract(),
        },
    )

    payload = cp.build_control_plane_v1(ctx)

    assert payload["current_domain"] == "SESSION_IDENTITY"
    assert payload["canonical_blocker"] == "TRUTH_ROOT_MISMATCH"


def test_manual_bootstrap_pass_without_governed_metadata_is_rejected(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _session_pass(ctx)
    _write(
        ctx.truth_root / "reports" / "paper_session_bootstrap_v1" / ctx.day_utc / "paper_session_bootstrap.v1.json",
        {"day_utc": ctx.day_utc, "bootstrap_status": "PASS"},
    )

    payload = cp.build_control_plane_v1(ctx)

    assert payload["current_domain"] == "SESSION_IDENTITY"
    assert payload["canonical_blocker"] == "PRODUCER_METADATA_MISSING"


def test_generic_required_dependency_without_metadata_fails(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _session_pass(ctx)
    _broker_pass(ctx)
    _write(
        ctx.truth_root / "reports" / "broker_supply_v1" / ctx.day_utc / "broker_supply.v1.json",
        {"day_utc": ctx.day_utc, "status": "PASS", "canonical_blocker": ""},
    )

    payload = cp.build_control_plane_v1(ctx)

    assert payload["current_domain"] == "BROKER_CONNECTIVITY"
    assert payload["canonical_blocker"] == "PRODUCER_METADATA_MISSING"
    assert [row["dependency_id"] for row in payload["failed_current_domain_dependencies"]] == ["broker_supply_v1"]


def test_projection_renders_control_plane_readiness_without_recomputing(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _session_pass(ctx)
    monkeypatch.setattr(cp.bod, "_resolve_context", lambda *_args, **_kwargs: ctx)
    _cp_path, control = cp.run_control_plane_v1(ctx.day_utc, ctx.environment, str(ctx.truth_root))
    monkeypatch.setattr(projection.bod, "_resolve_context", lambda *_args, **_kwargs: ctx)

    _out_path, payload = projection.run_operator_projection_v1(ctx.day_utc, ctx.environment, str(ctx.truth_root))

    for key in ("current_domain", "current_phase", "canonical_blocker", "submit_allowed"):
        assert payload[key] == control[key]
    assert payload["phase"] == control["current_phase"]
    assert payload["final_status"] == control["final_status"]
    assert payload["failed_current_domain_dependencies"] == control["failed_current_domain_dependencies"]
    assert payload["deferred_downstream_domains"] == control["deferred_domains"]
    assert payload["deferred_domains"] == control["deferred_domains"]
    assert payload["why_not_ready_summary"].startswith("SYSTEM NOT READY BECAUSE:")
    assert payload["deferred_domain_note"] == "Deferred domains are not failed and are not actionable until the current domain clears."


def test_projection_why_not_ready_summary_uses_current_domain_reason(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _session_pass(ctx)
    _write(
        ctx.truth_root / "reports" / "paper_session_authority_v1" / ctx.day_utc / "paper_session_authority.v1.json",
        {"day_utc": ctx.day_utc, "authority_status": "DENIED", "reason_codes": ["NON_TRADING_DAY"]},
    )
    _write(
        ctx.truth_root / "reports" / "paper_session_bootstrap_v1" / ctx.day_utc / "paper_session_bootstrap.v1.json",
        {"day_utc": ctx.day_utc, "bootstrap_status": "BLOCKED", "reason_codes": ["NON_TRADING_DAY"], **_bootstrap_meta(ctx)},
    )
    monkeypatch.setattr(cp.bod, "_resolve_context", lambda *_args, **_kwargs: ctx)
    cp.run_control_plane_v1(ctx.day_utc, ctx.environment, str(ctx.truth_root))
    monkeypatch.setattr(projection.bod, "_resolve_context", lambda *_args, **_kwargs: ctx)

    _out_path, payload = projection.run_operator_projection_v1(ctx.day_utc, ctx.environment, str(ctx.truth_root))

    assert payload["current_domain"] == "SESSION_IDENTITY"
    assert payload["why_not_ready_summary"] == "SYSTEM NOT READY BECAUSE: SESSION_IDENTITY -> NON_TRADING_DAY"
    assert payload["deferred_domain_note"] == "Deferred domains are not failed and are not actionable until the current domain clears."


def test_projection_blocks_when_control_plane_wrong_day_without_kernel_fallback(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    cp_path = cp.control_plane_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    _write(
        cp_path,
        {
            "day_utc": "2026-05-03",
            "final_status": "READY",
            "canonical_blocker": "",
            "submit_allowed": True,
        },
    )
    _write(
        ctx.truth_root / "reports" / "unified_truth_kernel_v1" / ctx.day_utc / "unified_truth_kernel.v1.json",
        {
            "day_utc": ctx.day_utc,
            "final_status": "READY",
            "canonical_blocker": "",
            "operator_next_action": "kernel fallback should not be used",
        },
    )
    monkeypatch.setattr(projection.bod, "_resolve_context", lambda *_args, **_kwargs: ctx)
    monkeypatch.setattr(projection, "control_plane_acceptance_issues_v1", lambda *_args, **_kwargs: [])

    _out_path, payload = projection.run_operator_projection_v1(ctx.day_utc, ctx.environment, str(ctx.truth_root))

    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "CONTROL_PLANE_UNAVAILABLE"
    assert payload["final_status"] == "UNKNOWN"
    assert payload["submit_allowed"] is False
    assert "kernel fallback should not be used" not in payload["operator_next_action"]


def test_projection_rejects_control_plane_output_path_mismatch(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    ctx = _ctx_with_truth(tmp_path, tmp_path / "candidate_truth")
    cp_path = cp.control_plane_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    payload = _self_bound_control_payload(
        actual_path=cp_path,
        truth_root=ctx.truth_root,
        runtime_root=ctx.runtime_root,
        runtime_mode="CANDIDATE",
        output_path=ctx.runtime_root / "candidate_truth" / "wrong" / "control_plane.v1.json",
    )
    _write(cp_path, payload)
    monkeypatch.setattr(projection.bod, "_resolve_context", lambda *_args, **_kwargs: ctx)

    _out_path, observed = projection.run_operator_projection_v1(ctx.day_utc, ctx.environment, str(ctx.truth_root))

    assert observed["status"] == "BLOCKED"
    assert observed["canonical_blocker"] == "CONTROL_PLANE_OUTPUT_PATH_MISMATCH"
    assert observed["submit_allowed"] is False
    assert observed["control_plane_integrity_issues"]


def test_projection_rejects_copied_candidate_control_plane_under_production_truth(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    production_truth = tmp_path / "production_truth"
    candidate_truth = tmp_path / "candidate_truth"
    ctx = _ctx_with_truth(tmp_path, production_truth)
    actual_path = cp.control_plane_path(truth_root=production_truth, day_utc=ctx.day_utc)
    source_path = cp.control_plane_path(truth_root=candidate_truth, day_utc=ctx.day_utc)
    payload = _self_bound_control_payload(
        actual_path=source_path,
        truth_root=candidate_truth,
        runtime_root=tmp_path,
        runtime_mode="CANDIDATE",
    )
    payload["evidence_paths"] = [str(candidate_truth / "target_day_build_v1" / f"{ctx.day_utc}.json")]
    _write(actual_path, payload)
    monkeypatch.setattr(projection.bod, "_resolve_context", lambda *_args, **_kwargs: ctx)

    _out_path, observed = projection.run_operator_projection_v1(ctx.day_utc, ctx.environment, str(production_truth))

    assert observed["status"] == "BLOCKED"
    assert observed["canonical_blocker"] in {
        "CONTROL_PLANE_OUTPUT_PATH_MISMATCH",
        "CONTROL_PLANE_TRUTH_ROOT_MISMATCH",
        "CONTROL_PLANE_RUNTIME_MODE_MISMATCH",
    }
    assert {row["code"] for row in observed["control_plane_integrity_issues"]} >= {
        "CONTROL_PLANE_OUTPUT_PATH_MISMATCH",
        "CONTROL_PLANE_TRUTH_ROOT_MISMATCH",
    }


def test_projection_has_no_kernel_readiness_fallback_path() -> None:
    text = (REPO_ROOT / "ops" / "tools" / "run_aegis_operator_projection_v1.py").read_text(encoding="utf-8")

    assert "_projection_from_kernel" not in text
    assert "run_unified_truth_kernel_v1" not in text
    assert "unified_truth_kernel_path" not in text


def test_projection_integrity_context_names_control_plane_as_readiness_source(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _session_pass(ctx)
    monkeypatch.setattr(cp.bod, "_resolve_context", lambda *_args, **_kwargs: ctx)
    cp.run_control_plane_v1(ctx.day_utc, ctx.environment, str(ctx.truth_root))
    monkeypatch.setattr(projection.bod, "_resolve_context", lambda *_args, **_kwargs: ctx)

    _out_path, payload = projection.run_operator_projection_v1(ctx.day_utc, ctx.environment, str(ctx.truth_root))

    assert payload["integrity_context"]["final_status_source"] == "aegis_control_plane_v1"
    assert payload["integrity_context"]["readiness_source"] == "aegis_control_plane_v1"
    assert "day_run_ledger" not in payload["authority_note"]


def test_projection_renders_promotion_validation_mismatch_without_deciding_readiness(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _session_pass(ctx)
    _write(
        ctx.truth_root / "reports" / "aegis_promotion_validation_ledger_v1" / ctx.day_utc / "promotion_validation_ledger.v1.json",
        {
            "candidate_commit": "candidate-commit",
            "promoted_commit": "promoted-commit",
            "truth_root": str(tmp_path / "other_truth"),
            "runtime_root": str(tmp_path / "other_runtime"),
            "promotion_status": "CANDIDATE",
            "blockers": [{"code": "PROMOTION_VALIDATION_TRUTH_ROOT_MISMATCH"}],
        },
    )
    monkeypatch.setattr(cp.bod, "_resolve_context", lambda *_args, **_kwargs: ctx)
    _cp_path, control = cp.run_control_plane_v1(ctx.day_utc, ctx.environment, str(ctx.truth_root))
    monkeypatch.setattr(projection.bod, "_resolve_context", lambda *_args, **_kwargs: ctx)

    _out_path, payload = projection.run_operator_projection_v1(ctx.day_utc, ctx.environment, str(ctx.truth_root))

    assert payload["promotion_status"] == "CANDIDATE"
    assert payload["promotion_blockers"] == [{"code": "PROMOTION_VALIDATION_TRUTH_ROOT_MISMATCH"}]
    assert payload["truth_root_consistency"]["consistent"] is False
    assert payload["final_status"] == control["final_status"]


def test_projection_control_plane_blocker_not_obscured_by_promotion_visibility(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _write(
        cp.control_plane_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc),
        {
            "day_utc": ctx.day_utc,
            "truth_root": str(ctx.truth_root),
            "runtime_root": str(ctx.runtime_root),
            "runtime_mode": "CANDIDATE",
            "artifact_path": str(cp.control_plane_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)),
            "actual_artifact_path": str(cp.control_plane_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)),
            "producer_contract_output_artifact_path": str(cp.control_plane_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)),
            "final_status": "NOT_READY",
            "current_domain": "SESSION_IDENTITY",
            "current_phase": "SESSION_AUTHORITY",
            "canonical_blocker": "SESSION_IDENTITY_PRECHECK_FAILED",
            "submit_allowed": False,
            "failed_current_domain_dependencies": [],
            "producer_contract_v1": {
                "code_version_git_commit": cp._current_git_commit_v1(),
                "source_dirty_status": "CLEAN",
                "generated_at_utc": f"{ctx.day_utc}T13:00:00Z",
                "output_artifacts": [{"path": str(cp.control_plane_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc))}],
            },
        },
    )
    _write(
        ctx.truth_root / "reports" / "aegis_promotion_validation_ledger_v1" / ctx.day_utc / "promotion_validation_ledger.v1.json",
        {
            "candidate_commit": "candidate-commit",
            "promoted_commit": "promoted-commit",
            "truth_root": str(tmp_path / "other_truth"),
            "runtime_root": str(tmp_path / "other_runtime"),
            "promotion_status": "CANDIDATE",
            "blockers": [{"code": "PROMOTION_VALIDATION_TRUTH_ROOT_MISMATCH"}],
        },
    )
    monkeypatch.setattr(projection.bod, "_resolve_context", lambda *_args, **_kwargs: ctx)
    monkeypatch.setattr(projection, "control_plane_acceptance_issues_v1", lambda *_args, **_kwargs: [])

    _out_path, payload = projection.run_operator_projection_v1(ctx.day_utc, ctx.environment, str(ctx.truth_root))

    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "SESSION_IDENTITY_PRECHECK_FAILED"
    assert payload["promotion_blockers"] == [{"code": "PROMOTION_VALIDATION_TRUTH_ROOT_MISMATCH"}]
    assert payload["integrity_context"]["readiness_source"] == "aegis_control_plane_v1"


def test_projection_renders_promotion_gate_status_when_present(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    production_truth = tmp_path / "production_truth"
    candidate_truth = tmp_path / "candidate_truth"
    ctx = _ctx_with_truth(tmp_path, production_truth)
    _session_pass(ctx)
    _write(
        candidate_truth / "reports" / "aegis_promotion_validation_ledger_v1" / ctx.day_utc / "promotion_validation_ledger.v1.json",
        {
            "candidate_commit": "candidate-commit",
            "promoted_commit": "promoted-commit",
            "truth_root": str(candidate_truth),
            "runtime_root": str(tmp_path),
            "promotion_status": "CANDIDATE",
            "blockers": [],
        },
    )
    _write(
        candidate_truth / "reports" / "aegis_production_promotion_gate_v1" / ctx.day_utc / "promo-1.json",
        {
            "promotion_status": "BLOCKED",
            "blockers": [{"code": "HUMAN_APPROVAL_MISSING"}],
        },
    )
    monkeypatch.setattr(cp.bod, "_resolve_context", lambda *_args, **_kwargs: ctx)
    cp.run_control_plane_v1(ctx.day_utc, ctx.environment, str(ctx.truth_root))
    monkeypatch.setattr(projection.bod, "_resolve_context", lambda *_args, **_kwargs: ctx)

    _out_path, payload = projection.run_operator_projection_v1(ctx.day_utc, ctx.environment, str(ctx.truth_root))

    assert payload["promotion_status"] == "BLOCKED"
    assert payload["promotion_blockers"] == [{"code": "HUMAN_APPROVAL_MISSING"}]
    assert payload["promotion_state"]["promotion_visibility_source"] == "aegis_production_promotion_gate_v1"
    assert payload["candidate_commit"] == "candidate-commit"


def test_projection_prefers_current_promoted_production_state_over_prior_gate(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    production_truth = tmp_path / "production_truth"
    candidate_truth = tmp_path / "candidate_truth"
    ctx = _ctx_with_truth(tmp_path, production_truth)
    control_path = cp.control_plane_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    commit = projection.git_commit_v1()
    _write(
        control_path,
        {
            "day_utc": ctx.day_utc,
            "truth_root": str(ctx.truth_root),
            "runtime_root": str(ctx.runtime_root),
            "runtime_mode": "PRODUCTION",
            "artifact_path": str(control_path),
            "actual_artifact_path": str(control_path),
            "producer_contract_output_artifact_path": str(control_path),
            "final_status": "NOT_READY",
            "current_domain": "SESSION_IDENTITY",
            "current_phase": "SESSION_AUTHORITY",
            "canonical_blocker": "SESSION_IDENTITY_PRECHECK_FAILED",
            "submit_allowed": False,
            "failed_current_domain_dependencies": [],
            "producer_contract_v1": {
                "code_version_git_commit": commit,
                "source_dirty_status": "CLEAN",
                "generated_at_utc": f"{ctx.day_utc}T13:00:00Z",
                "output_artifacts": [{"path": str(control_path)}],
            },
        },
    )
    _write(
        ctx.truth_root / "governance" / "production_version.v1.json",
        {"schema_version": "production_version.v1", "promoted_commit": commit, "status": "ACTIVE"},
    )
    _write(
        ctx.truth_root / "reports" / "aegis_promotion_validation_ledger_v1" / ctx.day_utc / "promotion_validation_ledger.v1.json",
        {
            "candidate_commit": commit,
            "promoted_commit": commit,
            "truth_root": str(ctx.truth_root),
            "runtime_root": str(ctx.runtime_root),
            "promotion_status": "PROMOTED",
            "blockers": [],
        },
    )
    _write(
        candidate_truth / "reports" / "aegis_production_promotion_gate_v1" / ctx.day_utc / "promo-approved.json",
        {"promotion_status": "APPROVED_FOR_PROMOTION", "blockers": []},
    )
    monkeypatch.setattr(projection.bod, "_resolve_context", lambda *_args, **_kwargs: ctx)
    monkeypatch.setattr(projection, "control_plane_acceptance_issues_v1", lambda *_args, **_kwargs: [])

    _out_path, payload = projection.run_operator_projection_v1(ctx.day_utc, ctx.environment, str(ctx.truth_root))

    assert payload["promotion_status"] == "PROMOTED"
    assert payload["promotion_blockers"] == []
    assert payload["promotion_state"]["promotion_visibility_source"] == "aegis_promotion_validation_ledger_v1"


def test_submit_allowed_false_when_control_plane_not_ready(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)

    payload = cp.build_control_plane_v1(ctx)

    assert payload["final_status"] == "NOT_READY"
    assert payload["submit_allowed"] is False


def test_known_failure_golden_fixture_preserves_domain_ownership(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    fixture = json.loads(KNOWN_FAILURE_FIXTURE.read_text(encoding="utf-8"))["expected"]
    ctx = _ctx(tmp_path)
    _session_pass(ctx)
    bootstrap = ctx.truth_root / "reports" / "paper_session_bootstrap_v1" / ctx.day_utc / "paper_session_bootstrap.v1.json"
    bootstrap.unlink()
    _write(
        ctx.truth_root / "reports" / "runtime_resilience_authority_v1" / ctx.day_utc / "runtime_resilience_authority.v1.json",
        {"day_utc": ctx.day_utc, "status": "BLOCKED", "canonical_blocker": "IB_DISCONNECTED", **_artifact_meta(ctx)},
    )
    _write(
        ctx.truth_root / "reports" / "safety_state_authority_v1" / ctx.day_utc / "safety_state_authority.v1.json",
        {"day_utc": ctx.day_utc, "status": "BLOCKED", "canonical_blocker": "NAV_INVALID", **_artifact_meta(ctx)},
    )
    _write(
        ctx.truth_root / "reports" / "startup_materialization_input_convergence_v1" / ctx.day_utc / "startup_materialization_input_convergence.v1.json",
        {"day_utc": ctx.day_utc, "status": "BLOCKED", "canonical_blocker": "CASH_LEDGER_SNAPSHOT_V1_MISSING", **_artifact_meta(ctx)},
    )
    _write(
        ctx.truth_root / "reports" / "trading_day_intent_generation_v1" / ctx.day_utc / "trading_day_intent_generation.v1.json",
        {"day_utc": ctx.day_utc, "status": "BLOCKED", "canonical_blocker": "MISSING_REQUIRED_INPUTS", **_artifact_meta(ctx)},
    )
    _write(
        ctx.truth_root / "reports" / "trading_day_readiness_authority_v1" / ctx.day_utc / "trading_day_readiness_authority.v1.json",
        {"day_utc": ctx.day_utc, "status": "BLOCKED", "canonical_blocker": "SUBMIT_NOT_ALLOWED_BY_TRADING_DAY_MODE", **_artifact_meta(ctx)},
    )

    payload = cp.build_control_plane_v1(ctx)

    assert payload["current_domain"] == fixture["current_domain"]
    assert payload["current_phase"] == fixture["current_phase"]
    assert payload["canonical_blocker"] == fixture["canonical_blocker"]
    assert [row["dependency_id"] for row in payload["failed_current_domain_dependencies"]] == [fixture["failed_current_domain_dependency"]]
    blockers_to_domains = {
        row["blocking_reason"]: row["domain_owner"]
        for row in payload["readiness_dependency_inventory"]
        if row.get("blocking_reason")
    }
    for blocker, owner in fixture["ownership"].items():
        assert blockers_to_domains[blocker] == owner
    assert not [
        row
        for row in payload["readiness_dependency_inventory"]
        if row["domain_owner"] == "SESSION_IDENTITY"
        and row.get("blocking_reason") in set(fixture["ownership"].keys())
    ]


def test_domain_drift_fixture_preserves_paths_producers_and_statuses(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    fixture = json.loads(DOMAIN_DRIFT_FIXTURE.read_text(encoding="utf-8"))["expected_current_failure"]
    ctx = _ctx(tmp_path)
    _session_pass(ctx)
    _write(
        ctx.truth_root / "reports" / "paper_session_bootstrap_v1" / ctx.day_utc / "paper_session_bootstrap.v1.json",
        {"day_utc": ctx.day_utc, "bootstrap_status": "BLOCKED", "blocker_chain": ["NON_TRADING_DAY"], **_producer_contract()},
    )

    payload = cp.build_control_plane_v1(ctx)
    by_id = {row["dependency_id"]: row for row in payload["readiness_dependency_inventory"]}

    assert payload["current_domain"] == fixture["current_domain"]
    assert payload["canonical_blocker"] == fixture["canonical_blocker"]
    for expected in fixture["dependencies"]:
        row = by_id[expected["dependency_id"]]
        assert row["owning_domain"] == expected["owning_domain"]
        assert row["artifact_path"].endswith(expected["artifact_path_suffix"])
        assert expected["producer_contains"] in row["producer"]
        assert expected["recovery_command_contains"] in row["recovery_command"]
        assert row["status"] == expected["status"]
        assert row["blocking_reason"] == expected["blocker_code"]
