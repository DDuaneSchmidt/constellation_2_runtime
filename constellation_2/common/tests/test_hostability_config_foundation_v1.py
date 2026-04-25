from __future__ import annotations

import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]


def _read(relpath: str) -> str:
    return (REPO_ROOT / relpath).read_text(encoding="utf-8")


def test_paper_day_wrapper_reads_active_runtime_contract() -> None:
    text = _read("ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh")
    assert "load_active_runtime_identity_snapshot_v1" in text
    assert "run_single_node_hosted_preflight_v1.py" in text
    assert "run_runtime_lifecycle_v1.py" in text
    assert "run_runtime_startup_identity_v1.py" in text
    assert 'RUNTIME_DATA_ROOT="/home/node/constellation_runtime_data"' not in text
    assert 'RUNTIME_TRUTH_ROOT="${RUNTIME_DATA_ROOT}/truth"' not in text


def test_session_authority_monitor_requires_contract_truth_root() -> None:
    text = _read("ops/run/c2_session_authority_monitor_v1.sh")
    assert "load_active_runtime_identity_snapshot_v1" in text
    assert 'RUNTIME_DATA_ROOT="/home/node/constellation_runtime_data"' not in text
    assert 'RUNTIME_TRUTH_ROOT="${C2_TRUTH_ROOT:-${RUNTIME_DATA_ROOT}/truth}"' not in text


def test_execution_observer_wrapper_resolves_governed_runtime_config() -> None:
    text = _read("ops/run/c2_execution_observer_v1.sh")
    assert "load_active_runtime_identity_snapshot_v1" in text
    assert "run_single_node_hosted_preflight_v1.py" in text
    assert "run_runtime_lifecycle_v1.py" in text
    assert "run_runtime_startup_identity_v1.py" in text
    assert "cd /home/node/constellation_2_runtime" not in text
    assert 'PY="${PYTHON_BIN_OVERRIDE:-/home/node/constellation_2_runtime/.venv_c2/bin/python}"' not in text
    assert '--host 127.0.0.1' not in text
    assert '--port 4002' not in text
    assert '--client-id 79' not in text


def test_execution_observer_service_uses_canonical_wrapper() -> None:
    text = _read("ops/systemd/user/c2-execution-observer.service")
    assert "ops/run/c2_execution_observer_v1.sh" in text
    assert "/home/node/constellation_2_runtime/.venv_c2/bin/python" not in text
    assert "--host 127.0.0.1" not in text
    assert "--port 4002" not in text
    assert "--client-id 179" not in text


def test_touched_wrappers_are_shell_parseable() -> None:
    for relpath in (
        "ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh",
        "ops/run/c2_session_authority_monitor_v1.sh",
        "ops/run/c2_execution_observer_v1.sh",
    ):
        subprocess.run(["bash", "-n", str(REPO_ROOT / relpath)], check=True)


def test_canonical_wrappers_gate_launch_through_runtime_lifecycle() -> None:
    paper = _read("ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh")
    observer = _read("ops/run/c2_execution_observer_v1.sh")
    assert 'run_single_node_hosted_preflight_v1.py' in paper
    assert 'run_runtime_lifecycle_v1.py" admit' in paper
    assert 'run_runtime_lifecycle_v1.py" record-start' in paper
    assert 'run_runtime_lifecycle_v1.py" record-stop' in paper
    assert "--runtime_run_id" in paper
    assert "--runtime_identity_contract_path" in paper
    assert "--runtime_lifecycle_start_receipt_path" in paper
    assert 'run_single_node_hosted_preflight_v1.py' in observer
    assert 'run_runtime_lifecycle_v1.py" admit' in observer
    assert 'run_runtime_lifecycle_v1.py" record-start' in observer
    assert 'run_runtime_lifecycle_v1.py" record-stop' in observer
