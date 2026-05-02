from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCE_ROOT = Path(__file__).resolve().parents[3]
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.aegis_chatgpt_packet as packet
import ops.tools.run_aegis_paper_preflight_v1 as preflight


DAY = "2026-04-28"


def _roots() -> packet.RootResolution:
    return packet.RootResolution(
        canonical_truth_root=Path("/home/node/constellation_runtime_data/truth"),
        runtime_truth_root=Path("/home/node/constellation_runtime_data/truth_sleeves/PRIMARY/PAPER"),
        truth_sleeves_root=Path("/home/node/constellation_runtime_data/truth_sleeves"),
        authority_source="test",
        evidence="test",
        error="",
    )


def _current_day(status: str = "READY", blocker: str = "") -> packet.CurrentCalendarDayStatus:
    return packet.CurrentCalendarDayStatus(
        day_utc=DAY,
        is_trading_session="true",
        expected_non_trading_day=False,
        paper_session_authority_path="test",
        paper_session_status="GRANTED",
        status=status,
        canonical_blocker=blocker,
        service_status_summary="test",
        evidence="test",
    )


def _latest(
    *,
    status: str = "READY",
    runtime_service_state: str = "OK",
    execution_mode_state: str = "PAPER_TRANSMIT_ENABLED",
    execution_mode_environment: str = "PAPER",
    broker_transmit_enabled: str = "false",
    market_data_state: str = "FRESH",
    market_data_operator_impact: str = "NONE",
) -> packet.LatestTradingDayEvidenceStatus:
    return packet.LatestTradingDayEvidenceStatus(
        evidence_day_utc=DAY,
        status=status,
        canonical_blocker="",
        submit_boundary_status_path="test",
        closure_authority_path="test",
        trade_lineage_graph_path="test",
        execution_lifecycle_authority_path="test",
        runtime_service_authority_path="test",
        market_data_authority_path="test",
        strategy_decision_authority_path="test",
        portfolio_account_authority_path="test",
        risk_sizing_authority_path="test",
        execution_mode_authority_path="test",
        trading_day_closure_authority_path="test",
        runtime_service_state=runtime_service_state,
        market_data_state=market_data_state,
        market_data_operator_impact=market_data_operator_impact,
        strategy_decision_state="INTENT_CREATED",
        strategy_intent_count="1",
        strategy_zero_intent_reason="<none>",
        portfolio_account_state="OK",
        portfolio_cash_total_cents="10000000",
        portfolio_net_liquidation_cents="10000000",
        risk_sizing_state="ROUNDED",
        risk_final_size="{}",
        execution_mode_state=execution_mode_state,
        execution_mode_environment=execution_mode_environment,
        broker_transmit_enabled=broker_transmit_enabled,
        trading_day_closure_state="OPEN",
        current_head_path="test",
        submission_index_path="test",
        evidence="test",
    )


def _gate(
    *,
    dirty: str = "CLEAN",
    reproducibility: str = "REPRODUCIBLE",
    protection: str = "PROTECTED",
) -> packet.SourceIntegrityGate:
    return packet._build_source_integrity_gate(  # noqa: SLF001
        git_dirty_status=dirty,
        dirty_path_count=1 if dirty == "DIRTY" else 0,
        source_reproducibility_status=reproducibility,
        canonical_repo_protection_status=protection,
    )


def _signals(
    *,
    current_day: packet.CurrentCalendarDayStatus | None = None,
    latest: packet.LatestTradingDayEvidenceStatus | None = None,
    gate: packet.SourceIntegrityGate | None = None,
    status_lines: list[str] | None = None,
) -> list[packet.ReadinessSignal]:
    return packet._collect_readiness_signals(  # noqa: SLF001
        roots=_roots(),
        current_day=current_day or _current_day(),
        latest_trading_day=latest or _latest(),
        source_integrity_gate=gate or _gate(),
        freshness_status="FRESH",
        status_lines=status_lines or [],
    )


def test_harmless_ui_source_edit_blocks_source_only_without_unrelated_blockers() -> None:
    signals = _signals(
        gate=_gate(dirty="DIRTY", reproducibility="NOT_REPRODUCIBLE_DIRTY_WORKTREE"),
        status_lines=[" M constellation_2/phaseL/ui/static/index.html"],
    )
    decision = packet._decide_final_readiness(  # noqa: SLF001
        current_day=_current_day(),
        latest_trading_day=_latest(),
        signals=signals,
    )
    enforcing = packet._dedupe_enforcing_signals(signals)  # noqa: SLF001
    assert decision.status == "BLOCKED"
    assert decision.canonical_blocker == "SOURCE_REPRODUCIBILITY_BLOCKED"
    assert not any(s.root_cause_id == "runtime_service_missing" for s in enforcing)
    assert not any(s.root_cause_id == "market_data_stale" for s in enforcing)
    assert not any(s.root_cause_id == "broker_observation" for s in enforcing)


def test_repo_runtime_artifact_creates_one_source_integrity_root_cause() -> None:
    signals = _signals(
        gate=_gate(dirty="DIRTY", reproducibility="NOT_REPRODUCIBLE_DIRTY_WORKTREE"),
        status_lines=["?? runtime/process_state/service_status.json"],
    )
    enforcing = packet._dedupe_enforcing_signals(signals)  # noqa: SLF001
    source_integrity = [s for s in enforcing if s.root_cause_id == "source_integrity"]
    assert len(source_integrity) == 1
    assert source_integrity[0].code == "SOURCE_REPRODUCIBILITY_BLOCKED"
    assert packet._build_grade_profile(signals).unique_root_cause_count == 1  # noqa: SLF001


def test_missing_runtime_service_cannot_coexist_with_ready() -> None:
    signals = _signals(latest=_latest(runtime_service_state="MISSING_REQUIRED_SERVICE"))
    decision = packet._decide_final_readiness(  # noqa: SLF001
        current_day=_current_day(),
        latest_trading_day=_latest(runtime_service_state="MISSING_REQUIRED_SERVICE"),
        signals=signals,
    )
    assert decision.status == "BLOCKED"
    assert decision.status != "READY"
    assert decision.canonical_blocker == "MISSING_REQUIRED_RUNTIME_SERVICE"


def test_execution_mode_unknown_blocks_ready_and_degraded_ready() -> None:
    signals = _signals(latest=_latest(execution_mode_state="UNKNOWN"))
    decision = packet._decide_final_readiness(  # noqa: SLF001
        current_day=_current_day(),
        latest_trading_day=_latest(execution_mode_state="UNKNOWN"),
        signals=signals,
    )
    assert decision.status == "BLOCKED"
    assert decision.status not in {"READY", "DEGRADED_READY"}
    assert decision.canonical_blocker == "EXECUTION_MODE_UNKNOWN_BLOCKED"


def test_stale_diagnostic_market_data_uses_degradation_cap() -> None:
    latest = _latest(market_data_state="STALE", market_data_operator_impact="POST_SUBMIT_DIAGNOSTIC")
    signals = _signals(latest=latest)
    decision = packet._decide_final_readiness(  # noqa: SLF001
        current_day=_current_day(),
        latest_trading_day=latest,
        signals=signals,
    )
    assert decision.status == "DEGRADED_READY"
    assert decision.grade_profile.grade_cap == 85
    assert decision.grade_profile.overall_grade == 85


def test_same_root_cause_is_not_double_counted_in_grade() -> None:
    signals = _signals(
        gate=_gate(dirty="DIRTY", reproducibility="NOT_REPRODUCIBLE_DIRTY_WORKTREE"),
        status_lines=["?? runtime/logs/supervisor.log"],
    )
    profile = packet._build_grade_profile(signals)  # noqa: SLF001
    assert profile.cap_reason == "HARD_SAFETY_INVARIANT"
    assert profile.grade_cap == 40
    assert profile.unique_root_cause_count == 1


def test_packet_build_is_source_repo_read_only(monkeypatch) -> None:
    monkeypatch.setattr(packet, "_git_status_short_lines", lambda: [])
    monkeypatch.setattr(packet, "_git_diff_name_only_lines", lambda: [])
    monkeypatch.setattr(packet, "evaluate_canonical_cleanliness_v1", lambda _repo: {"status": "CLEAN", "dirty_path_count": 0})
    monkeypatch.setattr(packet, "read_protection_status_v1", lambda: {"status": "PROTECTED"})
    monkeypatch.setattr(packet, "_resolve_truth_roots", _roots)
    monkeypatch.setattr(packet, "_build_current_calendar_day_runtime_status", lambda _roots: _current_day())
    monkeypatch.setattr(packet, "_build_latest_trading_day_evidence_status", lambda _roots: _latest())
    _export_id, text = packet._build_packet()  # noqa: SLF001
    assert "## Aegis Paper-Trading Status" in text
    assert str(packet.LATEST_PACKET_PATH).startswith("/home/node/constellation_runtime_data/")


def test_packet_exports_unified_truth_kernel_fields(monkeypatch, tmp_path: Path) -> None:
    roots = packet.RootResolution(
        canonical_truth_root=tmp_path / "truth",
        runtime_truth_root=tmp_path / "runtime",
        truth_sleeves_root=tmp_path / "sleeves",
        authority_source="test",
        evidence="test",
        error="",
    )
    day = "2026-04-28"
    ledger_path = roots.canonical_truth_root / "reports" / "aegis_day_run_v1" / day / "day_run.v1.json"
    kernel_path = roots.canonical_truth_root / "reports" / "unified_truth_kernel_v1" / day / "unified_truth_kernel.v1.json"
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    kernel_path.parent.mkdir(parents=True, exist_ok=True)
    ledger_path.write_text(json.dumps({"day_utc": day, "final_status": "NOT_READY", "canonical_phase": "MARKET_OPEN_DATA_GATE", "canonical_blocker": "MARKET_CLOSED"}), encoding="utf-8")
    kernel_path.write_text(json.dumps({"day_utc": day, "final_status": "NOT_READY", "final_status_source": "aegis_day_run_ledger_v1", "first_blocker": "MARKET_CLOSED", "first_blocker_phase": "MARKET_OPEN_DATA_GATE", "canonical_blocker": "MARKET_CLOSED", "truth_confidence": "MEDIUM", "allowed_operator_actions": [{"action_id": "inspect_market_data_artifacts", "label": "Inspect market data artifacts"}], "forbidden_operator_actions": [{"action_id": "submit_paper_order", "label": "Submit paper order"}], "trade_health": {"edge_status": "UNPROVEN"}}), encoding="utf-8")
    monkeypatch.setattr(packet, "_build_current_calendar_day_runtime_status", lambda _roots: _current_day(status="BLOCKED", blocker="MARKET_CLOSED"))
    monkeypatch.setattr(packet, "_build_latest_trading_day_evidence_status", lambda _roots: _latest(status="BLOCKED"))

    text = packet._build_paper_status(roots).section

    assert "## Aegis Unified Truth Kernel" in text
    assert "- kernel_final_status: NOT_READY" in text
    assert "- kernel_final_status_source: aegis_day_run_ledger_v1" in text
    assert "Inspect market data artifacts" in text
    assert "- older_surface_role: supporting/diagnostic evidence only" in text


def test_preflight_runtime_outputs_default_outside_source_repo() -> None:
    source_root = Path("/home/node/constellation").resolve()
    marker_path = preflight._dry_run_reset_marker_path_v1(  # noqa: SLF001
        execution_truth_root=Path("/home/node/constellation_runtime_data/truth_sleeves/PRIMARY/PAPER"),
        day_utc=DAY,
    )
    assert not str(marker_path).startswith(str(source_root) + "/")
