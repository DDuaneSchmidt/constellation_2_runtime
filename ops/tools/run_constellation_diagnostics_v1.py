#!/usr/bin/env python3

import argparse
import json
import subprocess
import sys
from pathlib import Path

THIS_FILE = Path(__file__).resolve()
REPO_ROOT = THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.release_baseline_common_v1 import resolve_release_baseline_roots_v1  # noqa: E402


ROOTS = resolve_release_baseline_roots_v1(REPO_ROOT)
REPO_ROOT = ROOTS.repo_root

RUNTIME_STATE_SCRIPT = REPO_ROOT / "ops/tools/run_constellation_runtime_state_snapshot_v1.py"
ROOT_CAUSE_SCRIPT = REPO_ROOT / "ops/tools/run_constellation_root_cause_classifier_v1.py"
REPAIR_PLAN_SCRIPT = REPO_ROOT / "ops/tools/run_constellation_repair_plan_v1.py"
BUG_METRICS_SCRIPT = REPO_ROOT / "ops/tools/run_constellation_bug_metrics_v1.py"
PLATFORM_READINESS_SCRIPT = REPO_ROOT / "ops/tools/run_constellation_platform_readiness_v1.py"
SELF_HEAL_SCRIPT = REPO_ROOT / "ops/tools/run_constellation_self_heal_candidate_v1.py"
MEMO_SCRIPT = REPO_ROOT / "ops/tools/run_constellation_diagnostics_memo_v1.py"
CONTROL_PANEL_SCRIPT = REPO_ROOT / "ops/tools/run_constellation_ai_control_panel_v1.py"

ARTIFACT_ROOT = ROOTS.system_snapshot_root


def run(script: Path, extra_args: list[str] | None = None) -> None:
    print(f"\n=== RUNNING: {script.name} ===")

    if not script.exists():
        print(f"FAIL_CLOSED: missing script {script}")
        sys.exit(1)

    cmd = ["python3", str(script)]
    if extra_args:
        cmd.extend(extra_args)

    result = subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True
    )

    print(result.stdout)

    if result.returncode != 0:
        print(result.stderr)
        print(f"FAIL_CLOSED: {script.name} failed")
        sys.exit(1)


def verify_artifact(path: Path) -> None:
    print(f"VERIFY: {path}")
    if not path.exists():
        print(f"FAIL_CLOSED: missing artifact {path}")
        sys.exit(1)


def load_latest_operating_day() -> str:
    runtime_state_path = ARTIFACT_ROOT / "constellation_runtime_state.v1.json"
    if not runtime_state_path.exists():
        raise SystemExit(f"FAIL_CLOSED: missing runtime state for day binding: {runtime_state_path}")
    try:
        obj = json.loads(runtime_state_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise SystemExit(f"FAIL_CLOSED: cannot parse runtime state for day binding: {exc}")
    day = str(obj.get("latest_operating_day") or "").strip()
    if len(day) != 10 or day[4] != "-" or day[7] != "-":
        raise SystemExit(f"FAIL_CLOSED: invalid latest_operating_day in runtime state: {day}")
    return day


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(prog="run_constellation_diagnostics_v1")
    ap.add_argument(
        "--skip-self-heal",
        action="store_true",
        help="Skip self-heal candidate generation (used to avoid recursive invocation paths).",
    )
    return ap.parse_args()


def main() -> None:
    args = _parse_args()
    print("=== CONSTELLATION DIAGNOSTICS RUN ===")

    run(RUNTIME_STATE_SCRIPT)
    latest_operating_day = load_latest_operating_day()
    run(ROOT_CAUSE_SCRIPT)
    run(REPAIR_PLAN_SCRIPT)
    run(BUG_METRICS_SCRIPT, ["--day_utc", latest_operating_day])
    run(PLATFORM_READINESS_SCRIPT, ["--day_utc", latest_operating_day])
    if not args.skip_self_heal:
        run(SELF_HEAL_SCRIPT)
    else:
        print("\n=== SKIP: run_constellation_self_heal_candidate_v1.py (--skip-self-heal) ===")
    run(MEMO_SCRIPT)
    run(CONTROL_PANEL_SCRIPT)

    print("\n=== VERIFY ARTIFACTS ===")
    verify_artifact(ARTIFACT_ROOT / "constellation_runtime_state.v1.json")
    verify_artifact(ARTIFACT_ROOT / "constellation_root_cause_report.v1.json")
    verify_artifact(ARTIFACT_ROOT / "constellation_repair_plan.v1.json")
    verify_artifact(ROOTS.readiness_root / "constellation_bug_metrics_v1" / latest_operating_day / "constellation_bug_metrics.v1.json")
    verify_artifact(ROOTS.readiness_root / "constellation_platform_readiness_v1" / latest_operating_day / "constellation_platform_readiness.v1.json")
    if not args.skip_self_heal:
        verify_artifact(ARTIFACT_ROOT / "constellation_self_heal_packet.v1.json")
        verify_artifact(ARTIFACT_ROOT / "constellation_self_heal_memo.v1.md")
    verify_artifact(ARTIFACT_ROOT / "constellation_diagnostics_memo.v1.md")
    verify_artifact(ARTIFACT_ROOT / "constellation_ai_control_panel.v1.json")

    print("\n=== DIAGNOSTICS COMPLETE ===")
    print("\nArtifacts produced:")
    print(ARTIFACT_ROOT / "constellation_runtime_state.v1.json")
    print(ARTIFACT_ROOT / "constellation_root_cause_report.v1.json")
    print(ARTIFACT_ROOT / "constellation_repair_plan.v1.json")
    print(ROOTS.readiness_root / "constellation_bug_metrics_v1" / latest_operating_day / "constellation_bug_metrics.v1.json")
    print(ROOTS.readiness_root / "constellation_platform_readiness_v1" / latest_operating_day / "constellation_platform_readiness.v1.json")
    if not args.skip_self_heal:
        print(ARTIFACT_ROOT / "constellation_self_heal_packet.v1.json")
        print(ARTIFACT_ROOT / "constellation_self_heal_memo.v1.md")
    print(ARTIFACT_ROOT / "constellation_diagnostics_memo.v1.md")
    print(ARTIFACT_ROOT / "constellation_ai_control_panel.v1.json")


if __name__ == "__main__":
    main()
