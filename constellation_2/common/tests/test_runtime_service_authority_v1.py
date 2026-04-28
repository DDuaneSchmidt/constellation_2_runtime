from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.runtime_service_authority_v1 import evaluate_runtime_service_authority_v1


DAY = "2026-04-27"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _seed_package(repo_root: Path) -> None:
    _write_json(repo_root / "package.json", {"scripts": {"aegis:paper:submit": "python3 ops/tools/run_aegis_paper_submit_v1.py"}})


def test_manual_one_shot_no_submit_service_is_ready_when_command_exists(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    runtime = repo / "runtime"
    _seed_package(repo)
    _write_json(runtime / "process_state" / "service_status.json", {"services": []})

    payload = evaluate_runtime_service_authority_v1(day_utc=DAY, truth_root=tmp_path / "truth", repo_root=repo, runtime_root=runtime, expected_run_mode="MANUAL")

    assert payload["service_state"] == "MANUAL_MODE_READY"
    assert payload["submit_creator_available"] is True
    assert payload["submit_creator_required"] is False


def test_automatic_missing_auto_runner_is_required_failure(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    runtime = repo / "runtime"
    _seed_package(repo)
    _write_json(runtime / "process_state" / "service_status.json", {"services": []})

    payload = evaluate_runtime_service_authority_v1(day_utc=DAY, truth_root=tmp_path / "truth", repo_root=repo, runtime_root=runtime, expected_run_mode="AUTOMATIC")

    assert payload["service_state"] == "MISSING_REQUIRED_SERVICE"
    assert payload["submit_creator_required"] is True
    assert payload["auto_runner_required"] is True
    assert payload["first_blocker"] == "aegis_paper_auto_runner"


def test_manual_mode_stopped_auto_runner_is_not_required(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    runtime = repo / "runtime"
    _seed_package(repo)
    _write_json(
        runtime / "process_state" / "service_status.json",
        {"services": [{"name": "aegis_paper_auto_runner", "required": True, "state": "STOPPED", "pid_running": False}]},
    )

    payload = evaluate_runtime_service_authority_v1(day_utc=DAY, truth_root=tmp_path / "truth", repo_root=repo, runtime_root=runtime, expected_run_mode="MANUAL")

    assert payload["service_state"] == "MANUAL_MODE_READY"
    assert payload["required_missing"] == []
    assert payload["services"][0]["required"] is False
    assert payload["services"][0]["raw_required"] is True
    assert payload["services"][0]["requirement_basis"] == "not_required_for_manual_or_one_shot_paper_mode"


def test_automatic_passes_when_auto_runner_active_and_submit_command_exists(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    runtime = repo / "runtime"
    _seed_package(repo)
    _write_json(
        runtime / "process_state" / "service_status.json",
        {"services": [{"name": "aegis_paper_auto_runner", "required": True, "state": "RUNNING", "pid_running": True}]},
    )

    payload = evaluate_runtime_service_authority_v1(day_utc=DAY, truth_root=tmp_path / "truth", repo_root=repo, runtime_root=runtime, expected_run_mode="AUTOMATIC")

    assert payload["service_state"] == "READY"
    assert payload["status"] == "PASS"
    assert payload["auto_runner_running"] is True


def test_observer_dashboard_missing_is_diagnostic_unless_required(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    runtime = repo / "runtime"
    _seed_package(repo)
    _write_json(
        runtime / "process_state" / "service_status.json",
        {"services": [{"name": "observer", "required": False, "state": "STOPPED", "pid_running": False}]},
    )

    payload = evaluate_runtime_service_authority_v1(day_utc=DAY, truth_root=tmp_path / "truth", repo_root=repo, runtime_root=runtime, expected_run_mode="MANUAL")

    assert payload["service_state"] == "MANUAL_MODE_READY"
    assert payload["diagnostics"][0]["name"] == "observer"


def test_required_dashboard_missing_is_failure(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    runtime = repo / "runtime"
    _seed_package(repo)
    _write_json(
        runtime / "process_state" / "service_status.json",
        {"services": [{"name": "ops_dashboard", "required": True, "state": "STOPPED", "pid_running": False}]},
    )

    payload = evaluate_runtime_service_authority_v1(day_utc=DAY, truth_root=tmp_path / "truth", repo_root=repo, runtime_root=runtime, expected_run_mode="MANUAL")

    assert payload["service_state"] == "MISSING_REQUIRED_SERVICE"
    assert payload["first_blocker"] == "ops_dashboard"
