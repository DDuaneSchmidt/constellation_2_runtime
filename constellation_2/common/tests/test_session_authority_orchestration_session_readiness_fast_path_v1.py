from __future__ import annotations

import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_session_authority_orchestration_v1 as orchestration_module


def test_collect_target_day_build_source_refs_passes_build_fast_path_flag(monkeypatch, tmp_path: Path) -> None:
    captured_calls: list[tuple[str, tuple[str, ...]]] = []

    def fake_run_tool_v1(*, repo_root: Path, script_relpath: str, downstream_build_cycle_scripts, args=()):  # noqa: ANN001
        captured_calls.append((script_relpath, tuple(str(arg) for arg in args)))
        return {
            "script": script_relpath,
            "command": [sys.executable, str((repo_root / script_relpath).resolve()), *list(args)],
            "return_code": 0,
            "stdout": "",
            "stderr": "",
            "required_for_closure": True,
        }

    monkeypatch.setattr(orchestration_module, "run_tool_v1", fake_run_tool_v1)
    monkeypatch.delenv("C2_SKIP_STARTUP_MATERIALIZATION_REENTRY", raising=False)
    monkeypatch.delenv("C2_SKIP_SESSION_AUTHORITY_REENTRY", raising=False)

    orchestration_module.collect_target_day_build_source_refs_v1(
        repo_root=SOURCE_ROOT,
        truth_root=tmp_path,
        target_day="2026-04-24",
        environment="PAPER",
        ib_account="DUO847203",
        downstream_build_cycle_scripts=set(),
        run_primary_sleeve_capability_initialization_fn=lambda **_: [],
    )

    session_refresh_calls = [
        args for script, args in captured_calls if script == "ops/tools/run_session_readiness_refresh_v1.py"
    ]
    assert len(session_refresh_calls) == 1
    args = session_refresh_calls[0]
    assert "--build_fast_path" in args
    idx = args.index("--build_fast_path")
    assert idx + 1 < len(args)
    assert args[idx + 1] == "YES"


def test_broker_fact_spine_timeout_floor_is_applied(monkeypatch) -> None:
    env_name = orchestration_module.SESSION_AUTHORITY_TOOL_TIMEOUT_ENV
    monkeypatch.delenv(env_name, raising=False)
    assert orchestration_module._resolve_timeout_seconds_for_script("ops/tools/run_broker_fact_spine_v1.py") == 45.0
    assert orchestration_module._resolve_timeout_seconds_for_script("ops/tools/run_submit_boundary_status_v1.py") == 15.0

    monkeypatch.setenv(env_name, "10")
    assert orchestration_module._resolve_timeout_seconds_for_script("ops/tools/run_broker_fact_spine_v1.py") == 45.0
    assert orchestration_module._resolve_timeout_seconds_for_script("ops/tools/run_submit_boundary_status_v1.py") == 10.0

    monkeypatch.setenv(env_name, "120")
    assert orchestration_module._resolve_timeout_seconds_for_script("ops/tools/run_broker_fact_spine_v1.py") == 120.0
