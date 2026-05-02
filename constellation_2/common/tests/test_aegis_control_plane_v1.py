from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import ops.tools.run_aegis_control_plane_v1 as cp  # noqa: E402
import ops.tools.run_aegis_operator_projection_v1 as projection  # noqa: E402
import ops.tools.run_aegis_requirement_graph_v1 as graph  # noqa: E402
from ops.tools import run_aegis_bod_prepare_v1 as bod  # noqa: E402

DAY = "2026-05-04"


def _ctx(tmp_path: Path) -> bod.BodContext:
    truth = tmp_path / "truth"
    execution = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    runtime = tmp_path / "runtime"
    operator = tmp_path / "operator"
    for path in (truth, execution, runtime, operator):
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
    runtime = tmp_path / "runtime"
    operator = tmp_path / "operator"
    for path in (truth, execution, runtime, operator):
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
        {"day_utc": ctx.day_utc, "bootstrap_status": "PASS"},
    )


def _session_supporting_authorities(ctx: bod.BodContext) -> None:
    _write(
        ctx.truth_root / "reports" / "paper_session_authority_v1" / ctx.day_utc / "paper_session_authority.v1.json",
        {"day_utc": ctx.day_utc, "authority_status": "GRANTED"},
    )
    _write(
        ctx.truth_root / "reports" / "paper_session_bootstrap_v1" / ctx.day_utc / "paper_session_bootstrap.v1.json",
        {"day_utc": ctx.day_utc, "bootstrap_status": "PASS"},
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
        {"day_utc": ctx.day_utc, "bootstrap_status": "READY"},
    )


def _broker_pass(ctx: bod.BodContext) -> None:
    log = ctx.execution_root / "execution_evidence_v1" / "broker_events" / ctx.day_utc / "broker_event_log.v1.jsonl"
    log.parent.mkdir(parents=True, exist_ok=True)
    log.write_text("", encoding="utf-8")
    _write(log.parent / "broker_event_day_manifest.v1.json", {"day_utc": ctx.day_utc, "status": "PASS"})
    _write(ctx.truth_root / "reports" / "broker_supply_v1" / ctx.day_utc / "broker_supply.v1.json", {"day_utc": ctx.day_utc, "status": "PASS", "canonical_blocker": ""})


def _bod_pass(ctx: bod.BodContext) -> None:
    _write(ctx.operator_input_root / "operator_inputs" / "paper_capital_seed_v1" / ctx.day_utc / "paper_capital_seed.v1.json", {"day_utc": ctx.day_utc})
    _write(ctx.operator_input_root / "operator_inputs" / "cash_ledger_operator_statements" / ctx.day_utc / "operator_statement.v1.json", {"day_utc": ctx.day_utc})
    _write(ctx.truth_root / "reports" / "pre_open_bundle_v1" / ctx.day_utc / "pre_open_bundle.v1.json", {"target_day": ctx.day_utc, "producer_contract_v1": {"deterministic_fingerprint": "x"}})


def _market_pass(ctx: bod.BodContext) -> None:
    _write(ctx.truth_root / "reports" / "market_data_supply_v1" / ctx.day_utc / "market_data_supply.v1.json", {"day_utc": ctx.day_utc, "status": "PASS", "canonical_blocker": ""})
    _write(ctx.truth_root / "reports" / "market_data_authority_v1" / ctx.day_utc / "market_data_authority.v1.json", {"day_utc": ctx.day_utc, "status": "PASS", "canonical_blocker": ""})


def _feed_pass(ctx: bod.BodContext) -> None:
    _write(ctx.execution_root / "reports" / "feed_attestation_gate_v1" / ctx.day_utc / "feed_attestation_gate.v1.json", {"day_utc": ctx.day_utc, "status": "PASS", "canonical_blocker": ""})


def _auth_pass(ctx: bod.BodContext) -> None:
    _write(ctx.truth_root / "reports" / "authorization_supply_v1" / ctx.day_utc / "authorization_supply.v1.json", {"day_utc": ctx.day_utc, "status": "PASS", "canonical_blocker": ""})
    _write(ctx.execution_root / "reports" / "authorization_gate_verdict_v1" / ctx.day_utc / "authorization_gate_verdict.v1.json", {"day_utc": ctx.day_utc, "status": "PASS", "canonical_blocker": ""})


def test_session_failure_defers_broker_bod_feed_and_submit(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _write(ctx.truth_root / "reports" / "pre_open_bundle_v1" / ctx.day_utc / "pre_open_bundle.v1.json", {"target_day": ctx.day_utc, "blocking_reason_codes": ["TARGET_DAY_DATE_MISMATCH"]})

    payload = cp.build_control_plane_v1(ctx)

    assert payload["current_phase"] == "SESSION_AUTHORITY"
    assert payload["canonical_blocker"] == "SESSION_AUTHORITY_PRECHECK_FAILED"
    assert {"BROKER_HEALTH", "BOD_INPUTS", "FEED_ATTESTATION", "SUBMIT_BOUNDARY"} <= set(payload["deferred_phases"])


def test_skipped_or_missing_session_authority_blocks_before_bod(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _broker_pass(ctx)
    _bod_pass(ctx)
    phase_results = {"SESSION_AUTHORITY": {"status": "SKIPPED"}}
    monkeypatch.setattr(cp, "_evaluate_broker_health", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("broker should be deferred")))

    payload = cp.build_control_plane_v1(ctx, phase_results=phase_results)

    assert payload["current_phase"] == "SESSION_AUTHORITY"
    assert payload["canonical_blocker"] == "SESSION_AUTHORITY_PRECHECK_FAILED"
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
    assert payload["canonical_blocker"] == "SESSION_AUTHORITY_PRECHECK_FAILED"
    assert all(str(production_truth) in path or "repo_protection" in path for path in payload["evidence_paths"])


def test_session_hidden_dependency_is_decomposed_and_defers_downstream(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _session_blocked_artifacts(ctx)
    monkeypatch.setattr(cp, "_evaluate_broker_health", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("downstream phase evaluated")))

    payload = cp.build_control_plane_v1(ctx)

    assert payload["current_phase"] == "SESSION_AUTHORITY"
    assert payload["canonical_blocker"] == "SESSION_AUTHORITY_PRECHECK_FAILED"
    assert str(ctx.truth_root / "target_day_build_v1" / f"{ctx.day_utc}.json") in payload["evidence_paths"]
    assert payload["current_session_sub_blocker"]["owning_artifact"] == "SESSION_AUTHORITY_PRECHECK"
    assert "hidden_dependency_check" in {row["dependency_id"] for row in payload["session_precheck_failures"]}
    assert any("run_session_authority_v1.py" in command for command in payload["recovery_commands"])
    assert {"BROKER_HEALTH", "FEED_ATTESTATION", "KILL_SWITCH", "SUBMIT_BOUNDARY"} <= set(payload["deferred_phases"])
    sub_codes = {row["sub_blocker_code"] for row in payload["session_sub_blockers"]}
    assert any(str(code).startswith("HIDDEN_DEPENDENCY_DETECTED") for code in sub_codes)


def test_session_partial_build_can_be_primary_when_hidden_dependency_absent(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _write(ctx.truth_root / "active_session_v1" / "current.json", {"target_day": ctx.day_utc, "promotion_state": "BLOCKED", "blocking_codes": ["PARTIAL_BUILD"]})
    _write(
        ctx.truth_root / "target_day_admission_v1" / f"{ctx.day_utc}.json",
        {
            "target_day": ctx.day_utc,
            "blocking_reason_codes": ["PARTIAL_BUILD"],
            "hidden_dependency_check_result": {
                "status": "FAIL",
                "blocking_reason_code": "PARTIAL_BUILD",
                "undeclared_dependency_artifacts": [],
                "failing_producers": ["ops/tools/run_session_readiness_refresh_v1.py"],
            },
        },
    )
    _session_supporting_authorities(ctx)
    _write(ctx.truth_root / "target_day_build_v1" / f"{ctx.day_utc}.json", {"target_day": ctx.day_utc, "build_status": "BLOCKED", "artifact_results": [{"artifact_id": "capability_state_v1", "required": True, "result_status": "FAIL", "blocker_codes": ["PARTIAL_BUILD"]}]})

    payload = cp.build_control_plane_v1(ctx)

    assert payload["canonical_blocker"] == "PARTIAL_BUILD"
    assert payload["current_session_sub_blocker"]["missing_or_failed_dependency"] == "capability_state_v1"
    assert payload["current_session_sub_blocker"]["evidence_path"].endswith("/target_day_build_v1/2026-05-04.json")


def test_declared_session_dependency_missing_becomes_specific_blocker(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    missing_path = (
        ctx.truth_root
        / "reports"
        / "runtime_resilience_authority_v1"
        / ctx.day_utc
        / "runtime_resilience_authority.v1.json"
    )
    _write(ctx.truth_root / "active_session_v1" / "current.json", {"target_day": ctx.day_utc, "promotion_state": "BLOCKED", "blocking_codes": ["PARTIAL_BUILD"]})
    _write(ctx.truth_root / "target_day_admission_v1" / f"{ctx.day_utc}.json", {"target_day": ctx.day_utc, "blocking_reason_codes": ["PARTIAL_BUILD"]})
    _session_supporting_authorities(ctx)
    _write(
        ctx.truth_root / "target_day_build_v1" / f"{ctx.day_utc}.json",
        {
            "target_day": ctx.day_utc,
            "build_status": "BLOCKED",
            "artifact_results": [
                {
                    "artifact_id": "runtime_resilience_authority_v1",
                    "required": True,
                    "classification": "SESSION_AUTHORITY_DECLARED_DEPENDENCY",
                    "result_status": "FAIL",
                    "blocker_codes": ["RUNTIME_RESILIENCE_AUTHORITY_V1_MISSING"],
                    "canonical_path": str(missing_path),
                    "producer": {
                        "command": (
                            f'PYTHONPATH="$PWD" python3 ops/tools/run_runtime_resilience_authority_v1.py '
                            f"--day_utc {ctx.day_utc} --truth_root {ctx.truth_root}"
                        )
                    },
                }
            ],
        },
    )

    payload = cp.build_control_plane_v1(ctx)

    assert payload["current_phase"] == "SESSION_AUTHORITY"
    assert payload["canonical_blocker"] == "RUNTIME_RESILIENCE_AUTHORITY_V1_MISSING"
    assert payload["current_session_sub_blocker"]["missing_or_failed_dependency"] == "runtime_resilience_authority_v1"
    assert payload["evidence_paths"] == [str(missing_path)]
    assert "run_runtime_resilience_authority_v1.py" in payload["recovery_commands"][0]


def test_session_precheck_surfaces_multiple_required_failures_at_once(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    market_path = ctx.truth_root / "market_calendar_v1" / "dataset_manifest.json"
    runtime_path = (
        ctx.truth_root
        / "reports"
        / "runtime_resilience_authority_v1"
        / ctx.day_utc
        / "runtime_resilience_authority.v1.json"
    )
    _write(ctx.truth_root / "active_session_v1" / "current.json", {"target_day": ctx.day_utc, "promotion_state": "BLOCKED", "blocking_codes": ["PARTIAL_BUILD"]})
    _write(ctx.truth_root / "target_day_admission_v1" / f"{ctx.day_utc}.json", {"target_day": ctx.day_utc, "blocking_reason_codes": ["PARTIAL_BUILD"]})
    _session_supporting_authorities(ctx)
    _write(
        ctx.truth_root / "target_day_build_v1" / f"{ctx.day_utc}.json",
        {
            "target_day": ctx.day_utc,
            "build_status": "BLOCKED",
            "hidden_dependency_check_result": {"status": "PASS", "undeclared_dependency_artifacts": []},
            "artifact_results": [
                {
                    "artifact_id": "market_calendar_day",
                    "required": True,
                    "result_status": "FAIL",
                    "observed_status": "MISSING",
                    "schema_status": "MISSING",
                    "date_binding_status": "MISSING",
                    "freshness_status": "STALE",
                    "blocker_codes": ["MARKET_CALENDAR_DAY_MISSING"],
                    "canonical_path": str(market_path),
                },
                {
                    "artifact_id": "runtime_resilience_authority_v1",
                    "required": True,
                    "result_status": "FAIL",
                    "observed_status": "MISSING",
                    "schema_status": "MISSING",
                    "date_binding_status": "MISSING",
                    "freshness_status": "STALE",
                    "blocker_codes": ["RUNTIME_RESILIENCE_AUTHORITY_V1_MISSING"],
                    "canonical_path": str(runtime_path),
                },
                {
                    "artifact_id": "pre_open_bundle_v1",
                    "required": True,
                    "result_status": "PASS",
                    "canonical_path": str(ctx.truth_root / "reports" / "pre_open_bundle_v1" / ctx.day_utc / "pre_open_bundle.v1.json"),
                },
            ],
        },
    )

    payload = cp.build_control_plane_v1(ctx)

    assert payload["current_phase"] == "SESSION_AUTHORITY"
    assert payload["canonical_blocker"] == "SESSION_AUTHORITY_PRECHECK_FAILED"
    failed_ids = {row["dependency_id"] for row in payload["session_precheck_failures"]}
    assert {"market_calendar_day", "runtime_resilience_authority_v1"} <= failed_ids
    satisfied = [row for row in payload["session_dependency_inventory"] if row["dependency_id"] == "pre_open_bundle_v1"]
    assert satisfied and satisfied[0]["status"] == "SATISFIED"
    assert "BROKER_HEALTH" in payload["deferred_phases"]


def test_session_required_gate_fail_can_be_primary(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    gate_path = ctx.truth_root / "reports" / "startup_materialization_input_convergence_v1" / ctx.day_utc / "startup_materialization_input_convergence.v1.json"
    _write(ctx.truth_root / "active_session_v1" / "current.json", {"target_day": ctx.day_utc, "promotion_state": "BLOCKED", "blocking_codes": ["REQUIRED_GATE_FAIL"]})
    _write(
        ctx.truth_root / "target_day_admission_v1" / f"{ctx.day_utc}.json",
        {"target_day": ctx.day_utc, "blocking_reason_codes": ["REQUIRED_GATE_FAIL"], "blocker_chain": [{"artifact_id": "startup_materialization_input_convergence_v1", "artifact_path": str(gate_path), "blocker_code": "REQUIRED_GATE_FAIL"}]},
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
                    "canonical_path": str(gate_path),
                }
            ],
        },
    )
    _session_supporting_authorities(ctx)

    payload = cp.build_control_plane_v1(ctx)

    assert payload["canonical_blocker"] == "REQUIRED_GATE_FAIL"
    assert payload["current_session_sub_blocker"]["missing_or_failed_dependency"] == "startup_materialization_input_convergence_v1"
    assert payload["current_session_sub_blocker"]["evidence_path"] == str(gate_path)


def test_broker_health_failure_defers_downstream(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _session_pass(ctx)

    payload = cp.build_control_plane_v1(ctx)

    assert payload["current_phase"] == "BROKER_HEALTH"
    assert payload["canonical_blocker"] == "BROKER_EVENT_LOG_MISSING"
    assert {"FEED_ATTESTATION", "KILL_SWITCH", "SUBMIT_BOUNDARY"} <= set(payload["deferred_phases"])


def test_feed_attestation_is_current_only_after_earlier_phases_pass(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _session_pass(ctx)
    _broker_pass(ctx)
    _bod_pass(ctx)
    _market_pass(ctx)
    _write(ctx.execution_root / "reports" / "feed_attestation_gate_v1" / ctx.day_utc / "feed_attestation_gate.v1.json", {"day_utc": ctx.day_utc, "status": "FAIL", "reason_codes": ["FAL_STALE"]})

    payload = cp.build_control_plane_v1(ctx)

    assert payload["current_phase"] == "FEED_ATTESTATION"
    assert payload["canonical_blocker"] == "FAL_STALE"


def test_kill_switch_blocker_is_owned_by_kill_switch(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _session_pass(ctx)
    _broker_pass(ctx)
    _bod_pass(ctx)
    _market_pass(ctx)
    _feed_pass(ctx)
    _auth_pass(ctx)
    _write(ctx.truth_root / "risk_v1" / "kill_switch_v1" / ctx.day_utc / "global_kill_switch_state.v1.json", {"day_utc": ctx.day_utc, "state": "ACTIVE", "reason_codes": ["C2_KILL_SWITCH_ACTIVE"]})

    payload = cp.build_control_plane_v1(ctx)

    assert payload["current_phase"] == "KILL_SWITCH"
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
    assert payload["canonical_blocker"] == "BROKER_EVENT_LOG_MISSING"
    assert payload["operator_next_action"] == control["recovery_action"]
    assert payload["evidence_paths"]
    assert "SUBMIT_BOUNDARY" in payload["deferred_downstream_phases"]


def test_operator_projection_shows_exact_session_recovery_without_downstream_actions(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _session_blocked_artifacts(ctx)
    monkeypatch.setattr(cp.bod, "_resolve_context", lambda *_args, **_kwargs: ctx)
    cp.run_control_plane_v1(ctx.day_utc, ctx.environment, str(ctx.truth_root))
    monkeypatch.setattr(projection.bod, "_resolve_context", lambda *_args, **_kwargs: ctx)

    _out_path, payload = projection.run_operator_projection_v1(ctx.day_utc, ctx.environment, str(ctx.truth_root))

    assert payload["canonical_blocker"] == "SESSION_AUTHORITY_PRECHECK_FAILED"
    assert payload["operator_next_action"] == "Resolve all listed SESSION_AUTHORITY precheck failures, then rerun session authority."
    assert payload["session_precheck_failures"]
    assert any(row["dependency_id"] == "hidden_dependency_check" for row in payload["session_precheck_failures"])
    assert str(ctx.truth_root / "target_day_build_v1" / f"{ctx.day_utc}.json") in payload["evidence_paths"]
    assert payload["next_valid_actions"]
    assert not [action for action in payload["next_valid_actions"] if "broker" in action.lower() or "kill" in action.lower() or "submit" in action.lower()]


def test_requirement_graph_defers_downstream_missing_artifacts_when_broker_is_current(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    _source_pass(monkeypatch)
    ctx = _ctx(tmp_path)
    _session_pass(ctx)
    monkeypatch.setattr(cp.bod, "_resolve_context", lambda *_args, **_kwargs: ctx)
    control_path, control = cp.run_control_plane_v1(ctx.day_utc, ctx.environment, str(ctx.truth_root))
    assert control_path.exists()
    assert control["current_phase"] == "BROKER_HEALTH"

    payload = graph.build_requirement_graph(ctx)
    broker_node = next(row for row in payload["requirements"] if row["requirement_id"] == "BROKER_HEALTH:broker_event_log")
    deferred_market = [row for row in payload["requirements"] if row["owner_phase"] in {"MARKET_DATA", "AUTHORIZATION", "SUBMIT_BOUNDARY"} and row["status"] == "DEFERRED_BY_UPSTREAM_BLOCKER"]

    assert broker_node["status"] == "BLOCKING_CURRENT_RUN"
    assert broker_node["canonical_blocker"] == "BROKER_EVENT_LOG_MISSING"
    assert deferred_market
