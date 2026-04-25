from __future__ import annotations

import re
import sys
from pathlib import Path


SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))


MIGRATED_RUNTIME_CRITICAL_ROOT_ONLY_SET = {
    "ops/run/c2_supervisor_paper_v2.py": "ops/run/c2_supervisor_paper_v2.py",
    "ops/tools/run_c2_capital_risk_envelope_gate_v2.py": "ops/tools/run_c2_capital_risk_envelope_gate_v2.py",
    "ops/tools/run_c2_daily_operator_gate_v1.py": "ops/tools/run_c2_daily_operator_gate_v1.py",
    "ops/tools/run_c2_multi_sleeve_orchestrator_v1.py": "ops/tools/run_c2_multi_sleeve_orchestrator_v1.py",
    "ops/tools/run_c2_paper_day_orchestrator_v2.py": "ops/tools/run_c2_paper_day_orchestrator_v2.py",
    "ops/tools/run_correlation_preconditions_gate_v1.py": "ops/tools/run_correlation_preconditions_gate_v1.py",
    "ops/tools/run_correlation_preconditions_gate_v2.py": "ops/tools/run_correlation_preconditions_gate_v2.py",
    "ops/tools/run_engine_model_registry_gate_v1.py": "ops/tools/run_engine_model_registry_gate_v1.py",
    "ops/tools/run_execution_readiness_gate_v1.py": "ops/tools/run_execution_readiness_gate_v1.py",
    "ops/tools/run_feed_attestation_gate_v1.py": "ops/tools/run_feed_attestation_gate_v1.py",
    "ops/tools/run_gate_authority_plane_v1.py": "ops/tools/run_gate_authority_plane_v1.py",
    "ops/tools/run_gate_completeness_gate_v1.py": "ops/tools/run_gate_completeness_gate_v1.py",
    "ops/tools/run_heartbeat_gate_v1.py": "ops/tools/run_heartbeat_gate_v1.py",
    "ops/tools/run_liquidity_slippage_gate_v1.py": "ops/tools/run_liquidity_slippage_gate_v1.py",
    "ops/tools/run_operator_daily_gate_v1.py": "ops/tools/run_operator_daily_gate_v1.py",
    "ops/tools/run_operator_daily_gate_v2.py": "ops/tools/run_operator_daily_gate_v2.py",
    "ops/tools/run_operator_gate_verdict_v2.py": "ops/tools/run_operator_gate_verdict_v2.py",
    "ops/tools/run_operator_gate_verdict_v3.py": "ops/tools/run_operator_gate_verdict_v3.py",
    "ops/tools/run_recurrence_kill_gate_v1.py": "ops/tools/run_recurrence_kill_gate_v1.py",
    "ops/tools/run_systemic_risk_gate_v1.py": "ops/tools/run_systemic_risk_gate_v1.py",
    "ops/tools/run_systemic_risk_gate_v2.py": "ops/tools/run_systemic_risk_gate_v2.py",
    "ops/tools/run_systemic_risk_gate_v3.py": "ops/tools/run_systemic_risk_gate_v3.py",
    "ops/tools/run_trade_submit_readiness_c2_v1.py": "ops/tools/run_trade_submit_readiness_c2_v1.py",
    "ops/tools/run_truth_surface_authority_gate_v1.py": "ops/tools/run_truth_surface_authority_gate_v1.py",
}


DECISION_ROOT_RUNTIME_CRITICAL_UNCHANGED_SET = {
    "ops/tools/run_global_kill_switch_v1.py",
    "ops/tools/run_gate_stack_verdict_v1.py",
}


SNAPSHOT_RUNTIME_CRITICAL_UNCHANGED_SET = {
    "constellation_2/common/fresh_day_admission_v1.py",
    "constellation_2/common/runtime_identity_v1.py",
}


BOOTSTRAP_UNCHANGED_SET = {
    "ops/tools/run_paper_session_bootstrap_v1.py",
}


def _read(relpath: str) -> str:
    return (SOURCE_ROOT / relpath).resolve().read_text(encoding="utf-8")


def _assert_no_legacy_root_imports(text: str, relpath: str) -> None:
    runtime_contract_pattern = (
        r"from\s+constellation_2\.common\.runtime_contract_v1\s+import"
        r"(?:\s*\([^)]*resolve_canonical_truth_root|[^\n]*resolve_canonical_truth_root)"
    )
    truth_root_pattern = (
        r"from\s+constellation_2\.common\.truth_root_v1\s+import"
        r"(?:\s*\([^)]*resolve_truth_root|[^\n]*resolve_truth_root)"
    )
    assert not re.search(runtime_contract_pattern, text, flags=re.MULTILINE), relpath
    assert not re.search(truth_root_pattern, text, flags=re.MULTILINE), relpath


def test_runtime_critical_root_only_cutover_callers_use_bridge_helpers() -> None:
    for relpath, caller_label in MIGRATED_RUNTIME_CRITICAL_ROOT_ONLY_SET.items():
        text = _read(relpath)
        assert "runtime_authority_bridge_v1" in text, relpath
        assert (
            "resolve_canonical_truth_root_bridge_v1" in text
            or "resolve_truth_root_bridge_v1" in text
        ), relpath
        assert f'caller="{caller_label}"' in text, relpath
        assert "resolve_decision_truth_root_v1(" not in text, relpath
        assert "resolve_runtime_path_authority_snapshot_v1(" not in text, relpath
        _assert_no_legacy_root_imports(text, relpath)


def test_decision_root_runtime_critical_callers_remain_unchanged() -> None:
    for relpath in DECISION_ROOT_RUNTIME_CRITICAL_UNCHANGED_SET:
        text = _read(relpath)
        assert "runtime_authority_bridge_v1" not in text, relpath
        assert "resolve_canonical_truth_root_bridge_v1" not in text, relpath
        assert "resolve_truth_root_bridge_v1" not in text, relpath


def test_snapshot_runtime_critical_callers_remain_unchanged() -> None:
    for relpath in SNAPSHOT_RUNTIME_CRITICAL_UNCHANGED_SET:
        text = _read(relpath)
        assert "runtime_authority_bridge_v1" not in text, relpath


def test_bootstrap_paths_remain_legacy() -> None:
    for relpath in BOOTSTRAP_UNCHANGED_SET:
        text = _read(relpath)
        assert "runtime_authority_bridge_v1" not in text, relpath
        assert "resolve_canonical_truth_root_bridge_v1" not in text, relpath
        assert "resolve_truth_root_bridge_v1" not in text, relpath
