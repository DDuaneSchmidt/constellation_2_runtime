from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_paper_session_admission_v1 as admission_module


class _LedgerRef:
    def __init__(self, path: Path, payload: dict[str, object]) -> None:
        self.path = path
        self.payload = payload


def _write_json(path: Path, obj: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def test_admission_runner_invokes_current_day_control_plane_in_order(monkeypatch) -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        sleeve_truth_root = Path(td) / "truth_sleeves" / "PRIMARY" / "PAPER"
        day_utc = "2026-04-09"
        ledger_path = admission_module.resolve_paper_session_ledger_path(truth_root=truth_root, day_utc=day_utc)
        commands: list[str] = []
        downloader_cmds: list[list[str]] = []

        def _fake_run(cmd: list[str], *, truth_root: Path) -> dict[str, object]:
            if len(cmd) > 2 and cmd[1] == "-m":
                commands.append(cmd[2])
            else:
                commands.append(Path(cmd[1]).name)
            if len(cmd) > 1 and Path(cmd[1]).name == "ib_historical_market_data_snapshot_downloader_v1.py":
                downloader_cmds.append(list(cmd))
            return {"cmd": cmd, "returncode": 0, "stdout": "", "stderr": ""}

        for path in (
            admission_module.resolve_startup_proof_validation_path(truth_root=truth_root, day_utc=day_utc),
            admission_module.resolve_deployment_state_machine_path(truth_root=truth_root, day_utc=day_utc),
            admission_module.resolve_trading_day_state_machine_path(truth_root=truth_root, day_utc=day_utc),
            admission_module.resolve_execution_journal_path(truth_root=truth_root, day_utc=day_utc),
            admission_module.resolve_current_system_projection_path(truth_root=truth_root, day_utc=day_utc),
        ):
            _write_json(path, {"ok": True})
        _write_json(
            truth_root / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v2.json",
            {"schema_id": "C2_POSITIONS_SNAPSHOT_V2", "status": "OK"},
        )
        _write_json(
            truth_root / "cash_ledger_v1" / "snapshots" / day_utc / "cash_ledger_snapshot.v1.json",
            {"schema_id": "C2_CASH_LEDGER_SNAPSHOT_V1", "status": "OK"},
        )
        _write_json(
            truth_root / "intents_v1" / "snapshots" / day_utc / "intent.json",
            {"schema_id": "exposure_intent", "schema_version": "v1"},
        )

        monkeypatch.setattr(admission_module, "_run", _fake_run)
        monkeypatch.setattr(admission_module, "_producer_git_sha", lambda: "a" * 40)
        monkeypatch.setattr(admission_module, "_active_market_data_symbols", lambda: ["GLD", "IWM", "QQQ", "SPY"])
        monkeypatch.setattr(admission_module, "resolve_single_paper_ib_account_from_sleeve_registry", lambda _: "DU1234567")
        monkeypatch.setattr(admission_module, "_resolve_primary_paper_sleeve_truth_root", lambda **_: sleeve_truth_root)
        monkeypatch.setattr(
            admission_module,
            "read_paper_session_ledger_ref_v1",
            lambda **_: _LedgerRef(
                ledger_path,
                {
                    "ledger_id": "paper_session_ledger:2026-04-09:test",
                    "control_state": {
                        "authority_status": "DENIED",
                        "system_ready": False,
                        "submission_authorized": False,
                        "current_state": "SUBMIT_SKIPPED",
                    },
                },
            ),
        )
        monkeypatch.setattr(
            admission_module.sys,
            "argv",
            [
                "run_paper_session_admission_v1.py",
                "--day_utc",
                day_utc,
                "--truth_root",
                str(truth_root),
            ],
        )

        rc = admission_module.main()

        assert rc == 2
        assert commands == [
            "run_startup_materialization_v1.py",
            "run_paper_trading_posture_v1.py",
            "run_submit_boundary_status_v1.py",
            "ensure_cash_ledger_operator_statement_v1.py",
            "ib_historical_market_data_snapshot_downloader_v1.py",
            "constellation_2.phaseF.positions.run.run_positions_snapshot_day_v2",
            "constellation_2.phaseF.cash_ledger.run.run_cash_ledger_snapshot_day_v1",
            "run_accounting_nav_v2_day_v1.py",
            "run_regime_snapshot_v2.py",
            "build_defensive_tail_required_inputs_day_v1.py",
            "run_engine_correlation_matrix_day_v1.py",
            "run_accounting_nav_v2_day_v1.py",
            "bridge_accounting_nav_v2_to_compat_v1.py",
            "run_engine_correlation_matrix_day_v1.py",
            "run_reconciliation_report_v3.py",
            "run_exit_reconciliation_day_v1.py",
            "run_allocation_day_v2.py",
            "run_liquidity_slippage_gate_v1.py",
            "run_c2_capital_risk_envelope_gate_v2.py",
            "run_feed_attestation_gate_v1.py",
            "run_operator_daily_gate_v3.py",
            "run_heartbeat_gate_v1.py",
            "run_correlation_envelope_gate_v1.py",
            "run_replay_certification_gate_v1.py",
            "run_gate_stack_verdict_v1.py",
            "run_submit_boundary_status_v1.py",
            "run_paper_session_ledger_v1.py",
            "run_startup_proof_validation_v1.py",
            "run_deployment_state_machine_v1.py",
            "run_trading_day_state_machine_v1.py",
            "run_execution_journal_v1.py",
            "run_current_system_projection_v1.py",
        ]
        assert len(downloader_cmds) == 1
        assert downloader_cmds[0] == [
            admission_module.sys.executable,
            str(admission_module.MARKET_DATA_DOWNLOADER_TOOL),
            "--run_utc",
            f"{day_utc}T00:00:00Z",
            "--dataset_version",
            "v1",
            "--symbol",
            "GLD",
            "--symbol",
            "IWM",
            "--symbol",
            "QQQ",
            "--symbol",
            "SPY",
            "--start_year",
            "2026",
            "--end_year",
            "2026",
            "--host",
            "127.0.0.1",
            "--port",
            "4002",
            "--client_id",
            "7",
            "--sleep_sec",
            "0.1",
            "--use_rth",
            "1",
        ]


def test_mirror_canonical_file_copies_missing_target_and_preserves_identical_bytes(tmp_path: Path) -> None:
    source = tmp_path / "canonical" / "positions_snapshot.v2.json"
    target = tmp_path / "sleeve" / "positions_snapshot.v2.json"
    _write_json(source, {"schema_id": "C2_POSITIONS_SNAPSHOT_V2", "status": "OK"})

    copied = admission_module._mirror_canonical_file(
        source_path=source,
        target_path=target,
        artifact_id="positions",
    )
    existing = admission_module._mirror_canonical_file(
        source_path=source,
        target_path=target,
        artifact_id="positions",
    )

    assert copied["status"] == "COPIED"
    assert existing["status"] == "EXISTS_IDENTICAL"
    assert target.read_text(encoding="utf-8") == source.read_text(encoding="utf-8")


def test_mirror_canonical_day_json_dir_fails_closed_on_destination_extra_files(tmp_path: Path) -> None:
    source_dir = tmp_path / "canonical" / "intents_v1" / "snapshots" / "2026-04-09"
    target_dir = tmp_path / "sleeve" / "intents_v1" / "snapshots" / "2026-04-09"
    _write_json(source_dir / "a.json", {"schema_id": "exposure_intent", "schema_version": "v1"})
    _write_json(target_dir / "extra.json", {"schema_id": "exposure_intent", "schema_version": "v1"})

    result = admission_module._mirror_canonical_day_json_dir(
        source_dir=source_dir,
        target_dir=target_dir,
        artifact_id="intents",
    )

    assert result["status"] == "TARGET_EXTRA_FILES"
    assert result["extra_files"] == ["extra.json"]


def test_admission_runner_fails_closed_when_required_control_plane_artifact_missing(monkeypatch) -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        sleeve_truth_root = Path(td) / "truth_sleeves" / "PRIMARY" / "PAPER"
        day_utc = "2026-04-09"
        ledger_path = admission_module.resolve_paper_session_ledger_path(truth_root=truth_root, day_utc=day_utc)
        captured: list[dict[str, object]] = []

        def _fake_run(cmd: list[str], *, truth_root: Path) -> dict[str, object]:
            return {"cmd": cmd, "returncode": 0, "stdout": "", "stderr": ""}

        for path in (
            admission_module.resolve_startup_proof_validation_path(truth_root=truth_root, day_utc=day_utc),
            admission_module.resolve_deployment_state_machine_path(truth_root=truth_root, day_utc=day_utc),
            admission_module.resolve_trading_day_state_machine_path(truth_root=truth_root, day_utc=day_utc),
            admission_module.resolve_execution_journal_path(truth_root=truth_root, day_utc=day_utc),
        ):
            _write_json(path, {"ok": True})

        monkeypatch.setattr(admission_module, "_run", _fake_run)
        monkeypatch.setattr(admission_module, "_producer_git_sha", lambda: "a" * 40)
        monkeypatch.setattr(admission_module, "_active_market_data_symbols", lambda: ["GLD", "IWM", "QQQ", "SPY"])
        monkeypatch.setattr(admission_module, "resolve_single_paper_ib_account_from_sleeve_registry", lambda _: "DU1234567")
        monkeypatch.setattr(admission_module, "_resolve_primary_paper_sleeve_truth_root", lambda **_: sleeve_truth_root)
        monkeypatch.setattr(admission_module, "_print_payload", lambda payload: captured.append(dict(payload)))
        monkeypatch.setattr(
            admission_module,
            "read_paper_session_ledger_ref_v1",
            lambda **_: _LedgerRef(
                ledger_path,
                {
                    "ledger_id": "paper_session_ledger:2026-04-09:test",
                    "control_state": {
                        "authority_status": "DENIED",
                        "system_ready": False,
                        "submission_authorized": False,
                        "current_state": "SUBMIT_SKIPPED",
                    },
                },
            ),
        )
        monkeypatch.setattr(
            admission_module.sys,
            "argv",
            [
                "run_paper_session_admission_v1.py",
                "--day_utc",
                day_utc,
                "--truth_root",
                str(truth_root),
            ],
        )

        rc = admission_module.main()

        assert rc == 4
        assert captured[-1]["status"] == "CONTROL_PLANE_INCOMPLETE"
        assert "current_system_projection_v1" in captured[-1]["missing_control_plane_artifacts"]
