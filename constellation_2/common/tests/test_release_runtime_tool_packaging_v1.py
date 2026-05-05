from __future__ import annotations

from pathlib import Path
import subprocess
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
import ops.tools.build_constellation_release_v1 as build_release


REQUIRED_CURRENT_RELEASE_TOOLS = (
    "ops/tools/run_aegis_alert_sink_v1.py",
    "ops/tools/run_aegis_paper_ready_kernel_v1.py",
    "ops/tools/write_portal_healthz_v1.py",
    "ops/tools/run_portal_availability_probe_v1.py",
    "ops/tools/run_aegis_post_trade_lifecycle_v1.py",
    "ops/tools/run_sleeve_economic_truth_pipeline_v1.py",
    "ops/tools/run_aegis_projection_refresh_v1.py",
    "ops/tools/run_aegis_truth_resolver_v1.py",
    "ops/tools/run_market_open_data_gate_v1.py",
)


def test_timer_invoked_runtime_tools_exist_in_source() -> None:
    missing = [rel for rel in REQUIRED_CURRENT_RELEASE_TOOLS if not (REPO_ROOT / rel).is_file()]

    assert missing == []


def test_release_packaging_includes_timer_invoked_runtime_tools(tmp_path: Path) -> None:
    release_root = tmp_path / "release"

    copied = set(build_release._copy_tree_filtered(build_release.REPO_ROOT / "ops", release_root))

    missing = [rel for rel in REQUIRED_CURRENT_RELEASE_TOOLS if rel not in copied]
    assert missing == []


def test_timer_invoked_runtime_tool_imports_resolve_dependencies() -> None:
    code = "\n".join(
        [
            "import importlib",
            "for module in (",
            "    'ops.tools.run_aegis_paper_ready_kernel_v1',",
            "    'ops.tools.run_aegis_post_trade_lifecycle_v1',",
            "    'ops.tools.run_sleeve_economic_truth_pipeline_v1',",
            "):",
            "    importlib.import_module(module)",
        ]
    )

    proc = subprocess.run(
        [sys.executable, "-B", "-c", code],
        cwd=str(REPO_ROOT),
        env={"PYTHONDONTWRITEBYTECODE": "1"},
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0, proc.stderr
