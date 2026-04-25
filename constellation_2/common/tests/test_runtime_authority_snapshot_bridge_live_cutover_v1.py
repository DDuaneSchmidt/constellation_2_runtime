from __future__ import annotations

import sys
from pathlib import Path


SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))


MIGRATED_SNAPSHOT_CONSUMER_SET = {
    "constellation_2/common/next_day_readiness_probe_v1.py": "constellation_2/common/next_day_readiness_probe_v1.py",
    "constellation_2/common/fresh_day_admission_v1.py": "constellation_2/common/fresh_day_admission_v1.py",
    "ops/tools/run_paper_session_bootstrap_v1.py": "ops/tools/run_paper_session_bootstrap_v1.py",
}


BLOCKED_SNAPSHOT_RUNTIME_SET = {
    "constellation_2/common/runtime_identity_v1.py",
}


def _assert_cutover_to_snapshot_bridge(relpath: str, caller_label: str) -> None:
    path = (SOURCE_ROOT / relpath).resolve()
    text = path.read_text(encoding="utf-8")
    assert "runtime_authority_snapshot_bridge_v1" in text, relpath
    assert (
        "resolve_runtime_path_authority_snapshot_bridge_v1" in text
        or "load_runtime_path_authority_bridge_v1" in text
    ), relpath
    assert f'caller="{caller_label}"' in text, relpath
    assert "resolve_runtime_path_authority_snapshot_v1(" not in text, relpath


def _assert_not_importing_snapshot_bridge(relpath: str) -> None:
    path = (SOURCE_ROOT / relpath).resolve()
    text = path.read_text(encoding="utf-8")
    assert "runtime_authority_snapshot_bridge_v1" not in text, relpath
    assert "resolve_runtime_path_authority_snapshot_bridge_v1" not in text, relpath


def test_migrated_snapshot_consumer_set_uses_snapshot_bridge() -> None:
    for relpath, caller_label in MIGRATED_SNAPSHOT_CONSUMER_SET.items():
        _assert_cutover_to_snapshot_bridge(relpath, caller_label)


def test_bootstrap_and_runtime_blocked_set_remains_legacy() -> None:
    for relpath in BLOCKED_SNAPSHOT_RUNTIME_SET:
        _assert_not_importing_snapshot_bridge(relpath)


def test_bootstrap_tool_uses_snapshot_bridge_runtime_path_authority() -> None:
    relpath = "ops/tools/run_paper_session_bootstrap_v1.py"
    text = (SOURCE_ROOT / relpath).resolve().read_text(encoding="utf-8")
    assert "load_runtime_path_authority_bridge_v1" in text, relpath
    assert "runtime_authority_snapshot_bridge_v1" in text, relpath
    assert "load_runtime_path_authority_v1(" not in text, relpath
