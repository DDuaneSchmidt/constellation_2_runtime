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
        day_utc = "2026-04-09"
        ledger_path = admission_module.resolve_paper_session_ledger_path(truth_root=truth_root, day_utc=day_utc)
        commands: list[str] = []

        def _fake_run(cmd: list[str]) -> dict[str, object]:
            commands.append(Path(cmd[1]).name)
            return {"cmd": cmd, "returncode": 0, "stdout": "", "stderr": ""}

        for path in (
            admission_module.resolve_startup_proof_validation_path(truth_root=truth_root, day_utc=day_utc),
            admission_module.resolve_deployment_state_machine_path(truth_root=truth_root, day_utc=day_utc),
            admission_module.resolve_trading_day_state_machine_path(truth_root=truth_root, day_utc=day_utc),
            admission_module.resolve_execution_journal_path(truth_root=truth_root, day_utc=day_utc),
            admission_module.resolve_current_system_projection_path(truth_root=truth_root, day_utc=day_utc),
        ):
            _write_json(path, {"ok": True})

        monkeypatch.setattr(admission_module, "_run", _fake_run)
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
            "run_paper_session_ledger_v1.py",
            "run_startup_proof_validation_v1.py",
            "run_deployment_state_machine_v1.py",
            "run_trading_day_state_machine_v1.py",
            "run_execution_journal_v1.py",
            "run_current_system_projection_v1.py",
        ]


def test_admission_runner_fails_closed_when_required_control_plane_artifact_missing(monkeypatch) -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        day_utc = "2026-04-09"
        ledger_path = admission_module.resolve_paper_session_ledger_path(truth_root=truth_root, day_utc=day_utc)
        captured: list[dict[str, object]] = []

        def _fake_run(cmd: list[str]) -> dict[str, object]:
            return {"cmd": cmd, "returncode": 0, "stdout": "", "stderr": ""}

        for path in (
            admission_module.resolve_startup_proof_validation_path(truth_root=truth_root, day_utc=day_utc),
            admission_module.resolve_deployment_state_machine_path(truth_root=truth_root, day_utc=day_utc),
            admission_module.resolve_trading_day_state_machine_path(truth_root=truth_root, day_utc=day_utc),
            admission_module.resolve_execution_journal_path(truth_root=truth_root, day_utc=day_utc),
        ):
            _write_json(path, {"ok": True})

        monkeypatch.setattr(admission_module, "_run", _fake_run)
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
