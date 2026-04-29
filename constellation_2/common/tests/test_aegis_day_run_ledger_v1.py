from __future__ import annotations

from pathlib import Path
import sys
import json

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import ops.tools.aegis_chatgpt_packet as packet  # noqa: E402
import ops.tools.run_aegis_day_v1 as day_run  # noqa: E402
import ops.tools.run_market_open_data_gate_v1 as open_gate  # noqa: E402


def _ctx(tmp_path: Path, day: str = "2026-04-29") -> day_run.PhaseContext:
    truth = tmp_path / "truth"
    execution = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    runtime = tmp_path / "runtime"
    operator = tmp_path / "operator"
    for path in (truth, execution, runtime, operator):
        path.mkdir(parents=True, exist_ok=True)
    return day_run.PhaseContext(
        day_utc=day,
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


def _phase(status: str = "PASS", blocker: str = "", phase: str = "X") -> dict:
    return day_run._empty_phase(
        phase,
        status=status,
        canonical_blocker=blocker,
        blocker_detail=blocker,
        producer_command=f"test:{phase}",
        exit_code=0 if status in {"PASS", "SKIPPED", "PENDING"} else 2,
    )


def _install_phase_runners(
    monkeypatch: pytest.MonkeyPatch,
    blocked_phase: str | None = None,
    blocker: str = "BLOCKED",
    pending_phase: str | None = None,
    pending_blocker: str = "MARKET_NOT_OPEN",
) -> None:
    runners = {}
    for phase in day_run.PHASE_ORDER:
        def _runner(ctx, env, phase=phase):  # noqa: ANN001
            if phase == blocked_phase:
                return _phase("BLOCKED", blocker, phase)
            if phase == pending_phase:
                return _phase("PENDING", pending_blocker, phase)
            if phase == "EXECUTION":
                return _phase("SKIPPED", "", phase)
            if phase == "EOD_RECONCILIATION":
                return _phase("SKIPPED", "", phase)
            return _phase("PASS", "", phase)

        runners[phase] = _runner
    monkeypatch.setattr(day_run, "PHASE_RUNNERS", runners)
    monkeypatch.setattr(
        day_run,
        "_source_repo_status",
        lambda: {
            "git_dirty_status": "CLEAN",
            "dirty_path_count": 0,
            "source_reproducibility_status": "REPRODUCIBLE",
            "canonical_repo_protection_status": "PROTECTED",
        },
    )


def test_source_integrity_block_is_canonical(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _install_phase_runners(monkeypatch, blocked_phase="SOURCE_INTEGRITY", blocker="SOURCE_REPRODUCIBILITY_BLOCKED")
    payload = day_run.build_day_run_payload(_ctx(tmp_path))
    assert payload["final_status"] == "NOT_READY"
    assert payload["canonical_phase"] == "SOURCE_INTEGRITY"
    assert payload["canonical_blocker"] == "SOURCE_REPRODUCIBILITY_BLOCKED"


def test_broker_block_makes_later_failures_downstream_only(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _install_phase_runners(monkeypatch, blocked_phase="BROKER_HEALTH", blocker="IB_EVENT_TIMEOUT")
    payload = day_run.build_day_run_payload(_ctx(tmp_path))
    assert payload["canonical_phase"] == "BROKER_HEALTH"
    assert payload["canonical_blocker"] == "IB_EVENT_TIMEOUT"
    assert payload["phase_results"]["MARKET_DATA_BOD_PREP"]["status"] == "SKIPPED"
    assert payload["phase_results"]["SUBMIT_BOUNDARY"]["status"] == "SKIPPED"
    assert all(item["phase"] != "MARKET_DATA_BOD_PREP" or "Skipped because BROKER_HEALTH" in item["reason"] for item in payload["downstream_consequences"])


def test_market_data_block_after_session_passes_is_canonical(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _install_phase_runners(monkeypatch, blocked_phase="MARKET_DATA_BOD_PREP", blocker="OPTIONS_SNAPSHOT_ROOT_MISSING")
    payload = day_run.build_day_run_payload(_ctx(tmp_path))
    assert payload["phase_results"]["SESSION_AUTHORITY"]["status"] == "PASS"
    assert payload["canonical_phase"] == "MARKET_DATA_BOD_PREP"
    assert payload["canonical_blocker"] == "OPTIONS_SNAPSHOT_ROOT_MISSING"


def test_session_authority_true_target_day_admission_failure_remains_canonical(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    ctx = _ctx(tmp_path)
    _write(
        ctx.truth_root / "reports" / "paper_session_authority_v1" / ctx.day_utc / "paper_session_authority.v1.json",
        {"authority_status": "DENIED", "submission_authorized": False},
    )
    _write(
        ctx.truth_root / "reports" / "paper_session_bootstrap_v1" / ctx.day_utc / "paper_session_bootstrap.v1.json",
        {"bootstrap_status": "BLOCKED", "bootstrap_semantic_status": "BLOCKED"},
    )
    _write(
        ctx.truth_root / "risk_v1" / "kill_switch_v1" / ctx.day_utc / "global_kill_switch_state.v1.json",
        {"state": "INACTIVE"},
    )

    monkeypatch.setattr(
        day_run,
        "_run_steps",
        lambda *_args, **_kwargs: (
            [{"step_name": "paper_trading_day_authority", "status": "BLOCKED", "blocker": "TARGET_DAY_ADMISSION_NOT_READY"}],
            [],
            ["TARGET_DAY_ADMISSION_NOT_READY"],
        ),
    )

    row = day_run._phase_session_authority(ctx, {})

    assert row["status"] == "BLOCKED"
    assert row["canonical_blocker"] == "TARGET_DAY_ADMISSION_NOT_READY"


def test_session_authority_does_not_fail_on_quote_dependent_legacy_day_authority_when_session_granted(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    ctx = _ctx(tmp_path)
    _write(
        ctx.truth_root / "reports" / "paper_session_authority_v1" / ctx.day_utc / "paper_session_authority.v1.json",
        {"authority_status": "GRANTED", "submission_authorized": False},
    )
    _write(
        ctx.truth_root / "reports" / "paper_session_bootstrap_v1" / ctx.day_utc / "paper_session_bootstrap.v1.json",
        {"bootstrap_status": "READY", "bootstrap_semantic_status": "READY_PAPER_ONLY"},
    )
    _write(
        ctx.truth_root / "risk_v1" / "kill_switch_v1" / ctx.day_utc / "global_kill_switch_state.v1.json",
        {"state": "INACTIVE"},
    )

    monkeypatch.setattr(
        day_run,
        "_run_steps",
        lambda *_args, **_kwargs: (
            [{"step_name": "paper_trading_day_authority", "status": "BLOCKED", "blocker": "TARGET_DAY_ADMISSION_NOT_READY"}],
            [],
            ["TARGET_DAY_ADMISSION_NOT_READY"],
        ),
    )

    row = day_run._phase_session_authority(ctx, {})

    assert row["status"] == "PASS"
    assert row["canonical_blocker"] == ""
    assert "TARGET_DAY_ADMISSION_NOT_READY" in row["downstream_consequences"]


def test_market_data_missing_snapshot_without_capture_diagnostic_stays_generic() -> None:
    steps = [
        {
            "step_name": "options_chain_snapshot",
            "status": "BLOCKED",
            "blocker": "OPTIONS_CHAIN_SNAPSHOT_MISSING",
            "stdout_summary": '{"results":[{"reason_code":"OPTIONS_CHAIN_SNAPSHOT_MISSING"}]}',
        }
    ]

    assert day_run._specific_market_data_blocker_from_steps(steps) == ""


def test_market_data_specific_capture_blocker_wins_over_generic_authority(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)

    def _fake_run_steps(*_args, **_kwargs):  # noqa: ANN002, ANN003
        return (
            [
                {
                    "step_name": "market_data_supply",
                    "status": "BLOCKED",
                    "blocker": "OPTIONS_MARKET_DATA_PERMISSION_DENIED",
                    "duration_ms": 1,
                }
            ],
            [str(day_run._market_data_supply_path(ctx))],
            ["OPTIONS_MARKET_DATA_PERMISSION_DENIED"],
        )

    supply_path = day_run._market_data_supply_path(ctx)
    supply_path.parent.mkdir(parents=True)
    supply_path.write_text(
        '{"status":"BLOCKED","canonical_blocker":"OPTIONS_MARKET_DATA_PERMISSION_DENIED","requirements":[{"requirement_id":"REQ1","instrument":"SPY"}],"provider_checks":[{"provider":"IBKR","capability":"OPTIONS_BID_ASK_QUOTES","status":"UNAVAILABLE","blocker":"OPTIONS_MARKET_DATA_PERMISSION_DENIED"}]}\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(day_run, "_run_steps", _fake_run_steps)

    row = day_run._phase_market_data(ctx, {})

    assert row["status"] == "BLOCKED"
    assert row["canonical_blocker"] == "OPTIONS_MARKET_DATA_PERMISSION_DENIED"


def test_market_data_successful_snapshot_phase_passes(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)

    def _fake_run_steps(*_args, **_kwargs):  # noqa: ANN002, ANN003
        return ([{"step_name": "market_data_supply", "status": "PASS", "duration_ms": 1}], [str(day_run._market_data_supply_path(ctx))], [])

    supply_path = day_run._market_data_supply_path(ctx)
    supply_path.parent.mkdir(parents=True)
    supply_path.write_text('{"status":"PASS","canonical_blocker":"","requirements":[],"provider_checks":[]}\n', encoding="utf-8")
    monkeypatch.setattr(day_run, "_run_steps", _fake_run_steps)

    row = day_run._phase_market_data(ctx, {})

    assert row["status"] == "PASS"
    assert row["canonical_blocker"] == ""


def test_market_data_wrong_day_snapshot_does_not_satisfy_current_day(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _install_phase_runners(monkeypatch, blocked_phase="MARKET_DATA_BOD_PREP", blocker="OPTIONS_CHAIN_SNAPSHOT_MISSING")
    ctx = _ctx(tmp_path, "2026-04-29")
    wrong_day = ctx.execution_root / "options_chain_snapshot_v1" / "2026-04-28" / "capture" / "options_chain_snapshot.v1.json"
    wrong_day.parent.mkdir(parents=True)
    wrong_day.write_text('{"day_utc":"2026-04-28"}\n', encoding="utf-8")

    payload = day_run.build_day_run_payload(ctx)

    assert payload["canonical_phase"] == "MARKET_DATA_BOD_PREP"
    assert payload["canonical_blocker"] == "OPTIONS_CHAIN_SNAPSHOT_MISSING"
    assert payload["phase_results"]["STRATEGY_AND_RISK"]["status"] == "SKIPPED"


def test_specific_market_data_blocker_is_ledger_canonical_and_skips_downstream(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _install_phase_runners(monkeypatch, blocked_phase="MARKET_DATA_BOD_PREP", blocker="OPTIONS_QUOTES_MISSING_BID_ASK")

    payload = day_run.build_day_run_payload(_ctx(tmp_path))

    assert payload["canonical_phase"] == "MARKET_DATA_BOD_PREP"
    assert payload["canonical_blocker"] == "OPTIONS_QUOTES_MISSING_BID_ASK"
    assert payload["phase_results"]["STRATEGY_AND_RISK"]["status"] == "SKIPPED"
    assert payload["phase_results"]["AUTHORIZATION_PREP"]["status"] == "SKIPPED"


def test_all_pre_ready_phases_pass_produces_paper_ready(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _install_phase_runners(monkeypatch)
    payload = day_run.build_day_run_payload(_ctx(tmp_path))
    assert payload["final_status"] == "PAPER_READY"
    assert payload["canonical_phase"] == ""
    assert payload["phase_results"]["EXECUTION"]["status"] == "SKIPPED"
    assert payload["phase_results"]["EOD_RECONCILIATION"]["status"] == "SKIPPED"


def test_all_pre_ready_phases_pass_with_delayed_market_data_marks_delayed_ready(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _install_phase_runners(monkeypatch)
    ctx = _ctx(tmp_path)
    path = day_run._market_data_supply_path(ctx)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('{"day_utc":"2026-04-29","status":"PASS","delayed_data_used":true,"market_data_mode":"DELAYED"}\n', encoding="utf-8")

    payload = day_run.build_day_run_payload(ctx)

    assert payload["final_status"] == "PAPER_READY_WITH_DELAYED_DATA"
    assert payload["canonical_phase"] == ""


def test_day_ledger_reaches_pre_market_ready_before_market_open(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _install_phase_runners(monkeypatch, pending_phase="MARKET_OPEN_DATA_GATE", pending_blocker="MARKET_NOT_OPEN")

    payload = day_run.build_day_run_payload(_ctx(tmp_path))

    assert payload["final_status"] == "PRE_MARKET_READY"
    assert payload["canonical_phase"] == "MARKET_OPEN_DATA_GATE"
    assert payload["canonical_blocker"] == "MARKET_NOT_OPEN"
    assert payload["phase_results"]["AUTHORIZATION_FINAL"]["status"] == "SKIPPED"
    assert payload["phase_results"]["PAPER_READY"]["status"] == "SKIPPED"


def test_market_data_bod_prep_runs_and_passes_when_supply_is_pre_market_pending(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    ctx = _ctx(tmp_path)
    supply_path = day_run._market_data_supply_path(ctx)
    _write(
        supply_path,
        {
            "day_utc": ctx.day_utc,
            "status": "PRE_MARKET_PENDING",
            "canonical_blocker": "MARKET_OPEN_DATA_PENDING",
            "requirements": [{"requirement_id": "REQ1", "instrument": "SPY"}],
            "provider_checks": [],
        },
    )
    monkeypatch.setattr(
        day_run,
        "_run_steps",
        lambda *_args, **_kwargs: (
            [{"step_name": "market_data_supply", "status": "PASS", "blocker": ""}],
            [str(supply_path)],
            [],
        ),
    )

    row = day_run._phase_market_data_bod_prep(ctx, {})

    assert row["status"] == "PASS"
    assert row["canonical_blocker"] == ""
    assert row["outputs"] == [str(supply_path)]


def test_market_data_bod_prep_demotes_stale_snapshot_to_market_open_gate(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    ctx = _ctx(tmp_path)
    supply_path = day_run._market_data_supply_path(ctx)
    _write(
        supply_path,
        {
            "day_utc": ctx.day_utc,
            "status": "BLOCKED",
            "canonical_blocker": "OPTIONS_SNAPSHOT_STALE",
            "requirements": [{"requirement_id": "REQ1", "instrument": "SPY"}],
            "provider_checks": [],
        },
    )
    monkeypatch.setattr(
        day_run,
        "_run_steps",
        lambda *_args, **_kwargs: (
            [{"step_name": "market_data_supply", "status": "BLOCKED", "blocker": "OPTIONS_SNAPSHOT_STALE"}],
            [str(supply_path)],
            ["OPTIONS_SNAPSHOT_STALE"],
        ),
    )

    row = day_run._phase_market_data_bod_prep(ctx, {})

    assert row["status"] == "PASS"
    assert row["canonical_blocker"] == ""
    assert "OPTIONS_SNAPSHOT_STALE" in row["downstream_consequences"]


def test_day_ledger_cannot_reach_paper_ready_before_market_gate_passes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _install_phase_runners(monkeypatch, pending_phase="MARKET_OPEN_DATA_GATE", pending_blocker="MARKET_NOT_OPEN")

    payload = day_run.build_day_run_payload(_ctx(tmp_path))

    assert payload["final_status"] != "PAPER_READY"
    assert payload["phase_results"]["MARKET_OPEN_DATA_GATE"]["status"] == "PENDING"


def test_market_open_gate_before_0930_returns_market_not_open(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    monkeypatch.setattr(open_gate, "_market_session_state", lambda: "PRE_MARKET")

    payload = open_gate.build_market_open_data_gate(ctx)

    assert payload["status"] == "PENDING"
    assert payload["canonical_blocker"] == "MARKET_NOT_OPEN"


def test_market_open_gate_during_market_hours_with_valid_quotes_passes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    ctx = _ctx(tmp_path)
    supply_path = day_run._market_data_supply_path(ctx)
    supply_path.parent.mkdir(parents=True, exist_ok=True)
    supply_path.write_text('{"status":"PASS","canonical_blocker":"","artifacts":[]}\n', encoding="utf-8")
    monkeypatch.setattr(open_gate, "_market_session_state", lambda: "REGULAR")
    monkeypatch.setattr(open_gate, "_run_market_data_supply", lambda _ctx: ({"exit_code": 0}, ""))

    payload = open_gate.build_market_open_data_gate(ctx)

    assert payload["status"] == "PASS"
    assert payload["canonical_blocker"] == ""


def test_packet_without_ledger_reports_day_run_missing_not_downstream(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    roots = packet.RootResolution(
        canonical_truth_root=tmp_path / "truth",
        runtime_truth_root=tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER",
        truth_sleeves_root=tmp_path / "truth_sleeves",
        authority_source="test",
        evidence="test",
        error="",
    )
    roots.canonical_truth_root.mkdir(parents=True)
    roots.runtime_truth_root.mkdir(parents=True)
    monkeypatch.setattr(
        packet,
        "_build_current_calendar_day_runtime_status",
        lambda roots: packet.CurrentCalendarDayStatus(
            day_utc="2026-04-29",
            is_trading_session="true",
            expected_non_trading_day=False,
            paper_session_authority_path="x",
            paper_session_status="GRANTED",
            status="NOT_READY",
            canonical_blocker="OPTIONS_SNAPSHOT_ROOT_MISSING",
            service_status_summary="legacy",
            evidence="legacy",
        ),
    )
    monkeypatch.setattr(
        packet,
        "_build_latest_trading_day_evidence_status",
        lambda roots: packet.LatestTradingDayEvidenceStatus(
            evidence_day_utc="2026-04-29",
            status="NOT_READY",
            canonical_blocker="AUTHZ_MISSING_DEFINED_RISK_EVIDENCE",
            submit_boundary_status_path="x",
            closure_authority_path="x",
            trade_lineage_graph_path="x",
            execution_lifecycle_authority_path="x",
            runtime_service_authority_path="x",
            market_data_authority_path="x",
            strategy_decision_authority_path="x",
            portfolio_account_authority_path="x",
            risk_sizing_authority_path="x",
            execution_mode_authority_path="x",
            trading_day_closure_authority_path="x",
            runtime_service_state="MANUAL_MODE_READY",
            market_data_state="MISSING_REQUIRED_DATA",
            market_data_operator_impact="PRE_SUBMIT_BLOCKER",
            strategy_decision_state="MARKET_DATA_BLOCKED",
            strategy_intent_count="1",
            strategy_zero_intent_reason="<none>",
            portfolio_account_state="OPERATOR_STATEMENT_ONLY",
            portfolio_cash_total_cents="None",
            portfolio_net_liquidation_cents="None",
            risk_sizing_state="RISK_BLOCKED",
            risk_final_size="{}",
            execution_mode_state="DRY_RUN_LOCKED",
            execution_mode_environment="PAPER",
            broker_transmit_enabled="false",
            trading_day_closure_state="NO_TRADES_CLOSED",
            current_head_path="x",
            submission_index_path="x",
            evidence="legacy",
        ),
    )
    section = packet._build_paper_status(roots).section
    paper_section = section.split("## Aegis Paper-Trading Status", 1)[1]
    assert "- canonical_blocker: DAY_RUN_LEDGER_MISSING" in paper_section
    assert "- canonical_blocker: OPTIONS_SNAPSHOT_ROOT_MISSING" not in paper_section.split("## Aegis Brittleness Map", 1)[0]
    assert not (roots.canonical_truth_root / "reports" / "aegis_day_run_v1" / "2026-04-29" / "day_run.v1.json").exists()


def test_ledger_write_is_idempotent_and_preserves_prior_day(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _install_phase_runners(monkeypatch, blocked_phase="MARKET_DATA_BOD_PREP", blocker="OPTIONS_SNAPSHOT_ROOT_MISSING")
    ctx = _ctx(tmp_path, "2026-04-29")
    payload1 = day_run.build_day_run_payload(ctx)
    path = day_run._ledger_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    day_run._write_json(path, payload1)
    payload2 = day_run.build_day_run_payload(ctx)
    day_run._write_json(path, payload2)
    prior = day_run._ledger_path(truth_root=ctx.truth_root, day_utc="2026-04-28")
    day_run._write_json(prior, {"day_utc": "2026-04-28", "final_status": "PAPER_READY"})
    assert path.exists()
    assert prior.exists()
    assert day_run._read_json(prior)["day_utc"] == "2026-04-28"


def test_wrong_day_ledger_is_rejected_by_packet_loader(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    path = truth / "reports" / "aegis_day_run_v1" / "2026-04-29" / "day_run.v1.json"
    path.parent.mkdir(parents=True)
    path.write_text('{"day_utc":"2026-04-28","final_status":"PAPER_READY"}\n', encoding="utf-8")
    roots = packet.RootResolution(
        canonical_truth_root=truth,
        runtime_truth_root=tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER",
        truth_sleeves_root=tmp_path / "truth_sleeves",
        authority_source="test",
        evidence="test",
        error="",
    )
    status = packet._load_day_run_ledger_status(roots, "2026-04-29")
    assert status.canonical_blocker == "WRONG_DAY_RUN_LEDGER"


def test_downstream_blockers_do_not_replace_canonical(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _install_phase_runners(monkeypatch, blocked_phase="BOD_INPUTS", blocker="OPERATOR_STATEMENT_MISSING")
    payload = day_run.build_day_run_payload(_ctx(tmp_path))
    assert payload["canonical_blocker"] == "OPERATOR_STATEMENT_MISSING"
    assert payload["phase_results"]["MARKET_DATA_BOD_PREP"]["status"] == "SKIPPED"
    assert payload["phase_results"]["AUTHORIZATION_PREP"]["status"] == "SKIPPED"


def test_submit_decision_trace_only_in_mutating_submit_phase() -> None:
    command = day_run._phase_submit_boundary.__code__.co_consts
    assert any("run_submit_decision_trace_v1.py" in str(item) for item in command)


def test_day_run_ledger_path_is_under_truth_reports_not_repo(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    path = day_run._ledger_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    assert path == ctx.truth_root / "reports" / "aegis_day_run_v1" / ctx.day_utc / "day_run.v1.json"
    assert not str(path).startswith(str(day_run.REPO_ROOT))
