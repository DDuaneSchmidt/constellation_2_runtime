from __future__ import annotations

import sys
from pathlib import Path


SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))


def _read(relpath: str) -> str:
    return (SOURCE_ROOT / relpath).resolve().read_text(encoding="utf-8")


def test_systemd_entry_uses_runtime_identity_api_without_contract_helper() -> None:
    text = _read("ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh")
    assert "load_active_runtime_identity_snapshot_v1" in text
    assert "load_active_runtime_identity_snapshot_v1(repo_root=Path().resolve())" in text
    assert "load_active_runtime_contract_or_fail" not in text
    assert "--surface release_manifest_active" not in text
    assert "load_release_current_runtime_authority_v1" in text


def test_orchestrator_remains_root_bridge_migrated() -> None:
    text = _read("ops/tools/run_c2_paper_day_orchestrator_v2.py")
    assert "resolve_canonical_truth_root_bridge_v1" in text
    assert "resolve_truth_root_bridge_v1" in text
    assert "resolve_truth_sleeves_root_bridge_v1" in text
    assert 'caller="ops/tools/run_c2_paper_day_orchestrator_v2.py"' in text


def test_bootstrap_remains_bridge_migrated() -> None:
    text = _read("ops/tools/run_paper_session_bootstrap_v1.py")
    assert "resolve_decision_truth_root_bridge_v1" in text
    assert "load_runtime_path_authority_bridge_v1" in text
    assert "resolve_decision_truth_root_v1(" not in text
    assert "load_runtime_path_authority_v1(" not in text


def test_runtime_contract_helper_still_present() -> None:
    text = _read("constellation_2/common/runtime_contract_v1.py")
    assert "def load_active_runtime_contract_or_fail()" in text


def test_systemd_entry_hosted_preflight_runs_before_runtime_identity_load() -> None:
    text = _read("ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh")
    assert text.index("run_single_node_hosted_preflight_v1.py") < text.index(
        "load_active_runtime_identity_snapshot_v1"
    )
