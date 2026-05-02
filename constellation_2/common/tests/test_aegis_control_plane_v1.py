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
        ctx.truth_root / "reports" / "paper_session_authority_v1" / ctx.day_utc / "paper_session_authority.v1.json",
        {"day_utc": ctx.day_utc, "authority_status": "GRANTED"},
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
    assert payload["canonical_blocker"] == "TARGET_DAY_DATE_MISMATCH"
    assert {"BROKER_HEALTH", "BOD_INPUTS", "FEED_ATTESTATION", "SUBMIT_BOUNDARY"} <= set(payload["deferred_phases"])


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
