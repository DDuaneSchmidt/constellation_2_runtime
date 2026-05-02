from __future__ import annotations

import sys
from pathlib import Path


SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))


def _read(relpath: str) -> str:
    return (SOURCE_ROOT / relpath).resolve().read_text(encoding="utf-8")


def test_bootstrap_uses_bridge_helpers_only_at_authority_boundary() -> None:
    relpath = "ops/tools/run_paper_session_bootstrap_v1.py"
    text = _read(relpath)
    assert "resolve_decision_truth_root_bridge_v1" in text
    assert "load_runtime_path_authority_bridge_v1" in text
    assert 'caller="ops/tools/run_paper_session_bootstrap_v1.py"' in text
    assert "resolve_decision_truth_root_v1(" not in text
    assert "load_runtime_path_authority_v1(" not in text


def test_bootstrap_canonical_truth_guard_semantics_remain_identical() -> None:
    relpath = "ops/tools/run_paper_session_bootstrap_v1.py"
    text = _read(relpath)
    assert "_allowed_paper_bootstrap_truth_root_v1" in text
    assert "PHASE_CONTROLLED_PRODUCTION_TRUTH_ROOT" in text
    assert "PHASE_CONTROLLED_CANDIDATE_TRUTH_ROOT" in text
    assert "PAPER_BOOTSTRAP_CANONICAL_TRUTH_REQUIRED" in text


def test_orchestrator_and_systemd_entry_remain_unchanged_for_this_pass() -> None:
    orchestrator = _read("ops/tools/run_c2_paper_day_orchestrator_v2.py")
    systemd_entry = _read("ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh")

    assert "resolve_decision_truth_root_bridge_v1" not in orchestrator
    assert "load_runtime_path_authority_bridge_v1" not in orchestrator
    assert "runtime_authority_snapshot_bridge_v1" not in orchestrator

    assert "resolve_decision_truth_root_bridge_v1" not in systemd_entry
    assert "load_release_current_runtime_authority_v1" in systemd_entry
    assert "runtime_authority_snapshot_bridge_v1" not in systemd_entry


def test_load_active_runtime_contract_helper_unchanged_in_this_pass() -> None:
    text = _read("constellation_2/common/runtime_contract_v1.py")
    assert "def load_active_runtime_contract_or_fail()" in text
    assert "load_release_current_runtime_authority_v1" not in text
    assert "runtime_identity_bridge_v1" not in text
