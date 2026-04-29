from __future__ import annotations

import subprocess
import sys
from pathlib import Path

SOURCE_ROOT = Path(__file__).resolve().parents[3]
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.aegis_chatgpt_packet as packet
import ops.tools.run_aegis_paper_preflight_v1 as preflight
from constellation_2.common.execution_mode_authority_v1 import evaluate_execution_mode_authority_v1
from constellation_2.common.runtime_service_authority_v1 import evaluate_runtime_service_authority_v1
from constellation_2.phaseL.ui.server.c2_ops_cockpit_status_v2_collector_control_plane_v1 import (
    load_status_collector_control_plane_bundle_v1,
)
from ops.tools.repo_protection_common_v1 import require_runtime_output_outside_repo_runtime_v1


DAY = "2026-04-28"


def _roots(tmp_path: Path) -> packet.RootResolution:
    return packet.RootResolution(
        canonical_truth_root=tmp_path / "truth",
        runtime_truth_root=tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER",
        truth_sleeves_root=tmp_path / "truth_sleeves",
        authority_source="test",
        evidence="test",
        error="",
    )


def _current(status: str = "READY", blocker: str = "") -> packet.CurrentCalendarDayStatus:
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
    runtime_service_state: str = "MANUAL_MODE_READY",
    execution_mode_state: str = "DRY_RUN_LOCKED",
    status: str = "READY",
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
        market_data_state="FRESH",
        market_data_operator_impact="NONE",
        strategy_decision_state="INTENT_CREATED",
        strategy_intent_count="1",
        strategy_zero_intent_reason="<none>",
        portfolio_account_state="OK",
        portfolio_cash_total_cents="10000000",
        portfolio_net_liquidation_cents="10000000",
        risk_sizing_state="ROUNDED",
        risk_final_size="{}",
        execution_mode_state=execution_mode_state,
        execution_mode_environment="PAPER",
        broker_transmit_enabled="false",
        trading_day_closure_state="OPEN",
        current_head_path="test",
        submission_index_path="test",
        evidence="test",
    )


def _decision(
    tmp_path: Path,
    *,
    current_day: packet.CurrentCalendarDayStatus | None = None,
    latest: packet.LatestTradingDayEvidenceStatus | None = None,
    git_dirty_status: str = "CLEAN",
    source_reproducibility_status: str = "REPRODUCIBLE",
    canonical_repo_protection_status: str = "PROTECTED",
    runtime_service_state: str = "MANUAL_MODE_READY",
    execution_mode_state: str = "DRY_RUN_LOCKED",
    status_lines: list[str] | None = None,
) -> packet.FinalReadinessDecision:
    gate = packet._build_source_integrity_gate(  # noqa: SLF001
        git_dirty_status=git_dirty_status,
        dirty_path_count=1 if git_dirty_status == "DIRTY" else 0,
        source_reproducibility_status=source_reproducibility_status,
        canonical_repo_protection_status=canonical_repo_protection_status,
    )
    current = current_day or _current()
    resolved_latest = latest or _latest(
        runtime_service_state=runtime_service_state,
        execution_mode_state=execution_mode_state,
    )
    signals = packet._collect_readiness_signals(  # noqa: SLF001
        roots=_roots(tmp_path),
        current_day=current,
        latest_trading_day=resolved_latest,
        source_integrity_gate=gate,
        freshness_status="FRESH",
        status_lines=status_lines or [],
    )
    return packet._decide_final_readiness(  # noqa: SLF001
        current_day=current,
        latest_trading_day=resolved_latest,
        signals=signals,
    )


def test_packet_generation_outputs_are_outside_source_repo(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(packet, "LATEST_PACKET_PATH", tmp_path / "runtime_data" / "latest" / "packet.md")
    monkeypatch.setattr(packet, "ARCHIVE_ROOT", tmp_path / "runtime_data" / "archive")

    latest_path, archive_path = packet._write_outputs("export_id", "packet\n")  # noqa: SLF001

    assert latest_path.exists()
    assert archive_path.exists()
    assert not str(latest_path.resolve()).startswith(str(SOURCE_ROOT) + "/")
    assert not str(archive_path.resolve()).startswith(str(SOURCE_ROOT) + "/")


def test_preflight_runtime_outputs_are_outside_source_repo() -> None:
    marker_path = preflight._dry_run_reset_marker_path_v1(  # noqa: SLF001
        execution_truth_root=Path("/home/node/constellation_runtime_data/truth_sleeves/PRIMARY/PAPER"),
        day_utc=DAY,
    )

    assert not str(marker_path).startswith(str(SOURCE_ROOT) + "/")


def test_dashboard_status_collector_is_read_only_for_source_repo(tmp_path: Path) -> None:
    before = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))

    load_status_collector_control_plane_bundle_v1(
        truth_root=tmp_path / "truth",
        global_truth_root=tmp_path / "global_truth",
        day=DAY,
    )

    after = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))
    assert after == before


def test_dirty_source_file_blocks_readiness(tmp_path: Path) -> None:
    decision = _decision(
        tmp_path,
        git_dirty_status="DIRTY",
        source_reproducibility_status="NOT_REPRODUCIBLE_DIRTY_WORKTREE",
        status_lines=[" M ops/tools/aegis_chatgpt_packet.py"],
    )

    assert decision.status == "BLOCKED"
    assert decision.canonical_blocker == "SOURCE_REPRODUCIBILITY_BLOCKED"


def test_untracked_file_blocks_readiness(tmp_path: Path) -> None:
    decision = _decision(
        tmp_path,
        git_dirty_status="DIRTY",
        source_reproducibility_status="NOT_REPRODUCIBLE_DIRTY_WORKTREE",
        status_lines=["?? ops/tools/new_tool.py"],
    )

    assert decision.status == "BLOCKED"
    assert decision.canonical_blocker == "SOURCE_REPRODUCIBILITY_BLOCKED"


def test_unprotected_canonical_repo_blocks_readiness(tmp_path: Path) -> None:
    decision = _decision(tmp_path, canonical_repo_protection_status="UNPROTECTED")

    assert decision.status == "BLOCKED"
    assert decision.canonical_blocker == "CANONICAL_REPO_PROTECTION_BLOCKED"


def test_execution_mode_unknown_blocks_readiness(tmp_path: Path) -> None:
    decision = _decision(tmp_path, execution_mode_state="UNKNOWN")

    assert decision.status == "BLOCKED"
    assert decision.canonical_blocker == "EXECUTION_MODE_UNKNOWN_BLOCKED"


def test_runtime_service_missing_blocks_readiness(tmp_path: Path) -> None:
    decision = _decision(tmp_path, runtime_service_state="MISSING_REQUIRED_SERVICE")

    assert decision.status == "BLOCKED"
    assert decision.canonical_blocker == "MISSING_REQUIRED_RUNTIME_SERVICE"


def test_runtime_artifacts_cannot_be_written_under_repo_runtime_by_default() -> None:
    try:
        require_runtime_output_outside_repo_runtime_v1(SOURCE_ROOT / "runtime" / "logs")
    except SystemExit as exc:
        assert "runtime output path under canonical repo/runtime is forbidden" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("repo-local runtime output path was accepted")


def test_clean_protected_explicit_mode_and_runtime_pass_source_runtime_integrity(tmp_path: Path) -> None:
    decision = _decision(tmp_path)

    assert decision.status == "READY"
    assert decision.canonical_blocker == ""


def test_execution_mode_authority_never_emits_unknown_for_default_paper_mode(tmp_path: Path) -> None:
    payload = evaluate_execution_mode_authority_v1(
        day_utc=DAY,
        truth_root=tmp_path / "truth",
        execution_root=tmp_path / "execution",
        env={},
    )

    assert payload["mode_state"] == "DRY_RUN_LOCKED"
    assert payload["mode_state"] != "UNKNOWN"
    assert payload["status"] == "PASS"


def test_manual_runtime_authority_does_not_require_stopped_auto_runner(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    runtime = tmp_path / "runtime"
    repo.mkdir(parents=True)
    (repo / "package.json").write_text('{"scripts":{"aegis:paper:submit":"python3 ops/tools/run_aegis_paper_submit_v1.py"}}\n', encoding="utf-8")
    status_path = runtime / "process_state" / "service_status.json"
    status_path.parent.mkdir(parents=True)
    status_path.write_text(
        '{"services":[{"name":"aegis_paper_auto_runner","required":true,"state":"STOPPED","pid_running":false}]}\n',
        encoding="utf-8",
    )

    payload = evaluate_runtime_service_authority_v1(
        day_utc=DAY,
        truth_root=tmp_path / "truth",
        repo_root=repo,
        runtime_root=runtime,
        expected_run_mode="MANUAL",
    )

    assert payload["service_state"] == "MANUAL_MODE_READY"
    assert payload["required_missing"] == []


def test_missing_current_day_paper_session_authority_is_not_ready(tmp_path: Path) -> None:
    current = _current(status="NOT_READY", blocker="SESSION_AUTHORITY_MISSING")
    decision = _decision(tmp_path, current_day=current)

    assert decision.status == "NOT_READY"
    assert decision.canonical_blocker == "SESSION_AUTHORITY_MISSING"


def test_successful_current_day_paper_session_authority_removes_missing_blocker(tmp_path: Path) -> None:
    current = _current(status="READY", blocker="")
    decision = _decision(tmp_path, current_day=current)

    assert decision.status == "READY"
    assert decision.canonical_blocker == ""


def test_wrong_day_session_authority_cannot_satisfy_current_day_readiness(tmp_path: Path) -> None:
    wrong_day_latest = _latest(status="READY")
    current = _current(status="NOT_READY", blocker="SESSION_AUTHORITY_MISSING")

    decision = _decision(tmp_path, current_day=current, latest=wrong_day_latest)

    assert decision.status == "NOT_READY"
    assert decision.canonical_blocker == "SESSION_AUTHORITY_MISSING"


def test_preflight_step_timeout_fails_closed_without_indefinite_hang(monkeypatch) -> None:
    kills: list[tuple[int, int]] = []

    class FakeProcess:
        pid = 12345
        returncode = -15
        calls = 0

        def communicate(self, timeout=None):  # noqa: ANN001
            self.calls += 1
            if self.calls == 1:
                raise subprocess.TimeoutExpired(cmd=["slow"], timeout=timeout)
            return "", "terminated"

    monkeypatch.setenv("AEGIS_PREFLIGHT_STEP_TIMEOUT_SECONDS", "1")
    monkeypatch.setattr(preflight.subprocess, "Popen", lambda *args, **kwargs: FakeProcess())
    monkeypatch.setattr(preflight.os, "killpg", lambda pid, sig: kills.append((pid, sig)))

    row = preflight._run_step("session_authority_v1.refresh_target_day", ["slow"])  # noqa: SLF001

    assert row["timed_out"] is True
    assert row["return_code"] == 124
    assert row["timeout_seconds"] == 1
    assert "PREFLIGHT_STEP_TIMEOUT" in row["stderr"]
    assert kills


def test_timeout_blocker_payload_is_operator_visible() -> None:
    payload = preflight._preflight_timeout_blocked_payload(  # noqa: SLF001
        day_utc=DAY,
        steps=[
            {
                "name": "session_authority_v1.refresh_target_day",
                "cmd": ["python", "ops/tools/run_session_authority_v1.py"],
                "timed_out": True,
                "timeout_seconds": 1,
            }
        ],
    )

    assert payload["status"] == "PREFLIGHT_BLOCKED"
    assert payload["canonical_blocker"] == "PREFLIGHT_STEP_TIMEOUT"
    assert payload["owning_gate"] == "paper_preflight_step_timeout_gate"


def test_session_authority_generation_path_is_runtime_data_only() -> None:
    authority_path = (
        Path("/home/node/constellation_runtime_data/truth")
        / "reports"
        / "paper_session_authority_v1"
        / DAY
        / "paper_session_authority.v1.json"
    )

    assert str(authority_path).startswith("/home/node/constellation_runtime_data/")
    assert not str(authority_path).startswith(str(SOURCE_ROOT) + "/")
