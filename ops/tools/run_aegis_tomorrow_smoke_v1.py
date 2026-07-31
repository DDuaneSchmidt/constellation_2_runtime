#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.operator_action_command_contracts_v1 import command_registry_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402

SCHEMA_ID = "aegis_tomorrow_readiness_smoke"
ARTIFACT_ID = "aegis_tomorrow_smoke"


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _artifact(path: Path, logical_name: str) -> dict[str, Any]:
    exists = path.exists()
    return {
        "logical_name": logical_name,
        "path": str(path),
        "exists": exists,
        "sha256": _sha256(path) if exists and path.is_file() else "",
    }


def _run_step(name: str, argv: list[str], *, day_utc: str, truth_root: Path) -> dict[str, Any]:
    env = {key: value for key, value in os.environ.items() if key in {"PATH", "HOME", "LANG", "LC_ALL", "TZ"} and value}
    env["TARGET_DAY"] = day_utc
    env["AEGIS_TRUTH_ROOT"] = str(truth_root)
    env["CI"] = "1"
    completed = subprocess.run(
        argv,
        cwd=str(REPO_ROOT),
        env=env,
        text=True,
        capture_output=True,
        timeout=180,
        check=False,
        shell=False,
    )
    return {
        "name": name,
        "argv": argv,
        "shell": False,
        "exit_code": int(completed.returncode),
        "ok": completed.returncode == 0,
        "stdout_tail": str(completed.stdout or "")[-2000:],
        "stderr_tail": str(completed.stderr or "")[-2000:],
    }


def _truth_policy(day_utc: str, truth_root: Path) -> dict[str, Any]:
    kernel_path = truth_root / "reports" / "aegis_runtime_truth_kernel_v1" / day_utc / "runtime_truth_kernel.v1.json"
    kernel = _read_json(kernel_path)
    return {
        "runtime_truth_kernel_path": str(kernel_path),
        "runtime_truth_classification": kernel.get("runtime_truth_classification"),
        "trade_advice_allowed": kernel.get("trade_advice_allowed") is True,
        "broker_submit_transmit_allowed": str(kernel.get("broker_submit_transmit_policy") or "").upper() not in {"", "DISABLED_BY_DESIGN", "DISABLED"},
        "autonomous_execution_allowed": kernel.get("autonomous_execution_allowed") is True,
        "manual_trade_capture_allowed": kernel.get("manual_trade_capture_allowed") is True,
        "paper_review_allowed": kernel.get("paper_review_allowed") is True,
        "source_hash": _sha256(kernel_path) if kernel_path.exists() else "",
    }


def build_tomorrow_smoke_v1(*, truth_root: Path, day_utc: str, run_commands: bool = True) -> dict[str, Any]:
    generated_at = _now()
    steps = []
    if run_commands:
        steps = [
            _run_step("candidate_diagnostics", ["npm", "run", "aegis:candidate-diagnostics"], day_utc=day_utc, truth_root=truth_root),
            _run_step("signal_evidence_graph", ["npm", "run", "aegis:signal-evidence-graph"], day_utc=day_utc, truth_root=truth_root),
            _run_step("candidate_contracts", ["npm", "run", "aegis:candidate-contracts"], day_utc=day_utc, truth_root=truth_root),
            _run_step("paper_review_queue", ["npm", "run", "aegis:paper:review-queue"], day_utc=day_utc, truth_root=truth_root),
        ]
    registry = command_registry_v1()["commands_by_id"]
    command = registry.get("RUN_CONTEXT_READINESS_COMMAND") or {}
    package = _read_json(REPO_ROOT / "package.json")
    scripts = package.get("scripts") if isinstance(package.get("scripts"), dict) else {}
    policy = _truth_policy(day_utc, truth_root)
    source_artifacts = [
        _artifact(truth_root / "reports" / "aegis_candidate_generation_diagnostics_v1" / day_utc / "candidate_generation_diagnostics.v1.json", "candidate_generation_diagnostics"),
        _artifact(truth_root / "reports" / "aegis_signal_evidence_graph_v1" / day_utc / "signal_evidence_graph.v1.json", "signal_evidence_graph"),
        _artifact(truth_root / "reports" / "aegis_candidate_contracts_v1" / day_utc / "candidate_contracts.v1.json", "candidate_contracts"),
        _artifact(truth_root / "reports" / "aegis_paper_review_queue_v1" / day_utc / "paper_review_queue.v1.json", "paper_review_queue"),
        _artifact(truth_root / "reports" / "aegis_runtime_truth_kernel_v1" / day_utc / "runtime_truth_kernel.v1.json", "runtime_truth_kernel"),
    ]
    checks = {
        "candidate_diagnostics_command_works": any(step["name"] == "candidate_diagnostics" and step["ok"] for step in steps) if run_commands else True,
        "signal_evidence_graph_command_works": any(step["name"] == "signal_evidence_graph" and step["ok"] for step in steps) if run_commands else True,
        "candidate_contracts_command_works": any(step["name"] == "candidate_contracts" and step["ok"] for step in steps) if run_commands else True,
        "paper_review_queue_command_works": any(step["name"] == "paper_review_queue" and step["ok"] for step in steps) if run_commands else True,
        "repair_context_readiness_package_command_exists": "aegis:repair-context-readiness" in scripts,
        "run_context_readiness_command_registered": "RUN_CONTEXT_READINESS_COMMAND" in registry,
        "run_context_readiness_maps_to_repair_action": command.get("safety_classification", {}).get("portal_action_id") == "repair_context_readiness",
        "run_context_readiness_is_api_command": command.get("action_type") == "API_COMMAND",
        "trade_advice_remains_runtime_truth_false": policy["trade_advice_allowed"] is False,
        "broker_submit_transmit_remains_runtime_truth_false": policy["broker_submit_transmit_allowed"] is False,
        "autonomous_execution_remains_runtime_truth_false": policy["autonomous_execution_allowed"] is False,
    }
    ok = all(checks.values())
    return {
        "schema_id": SCHEMA_ID,
        "schema_version": "v1",
        "artifact_id": ARTIFACT_ID,
        "day_utc": day_utc,
        "generated_at": generated_at,
        "generator": "ops.tools.run_aegis_tomorrow_smoke_v1",
        "truth_root": str(truth_root),
        "ok": ok,
        "checks": checks,
        "steps": steps,
        "policy_from_runtime_truth": policy,
        "source_artifacts": source_artifacts,
        "safety_summary": {
            "broker_execution_changed": False,
            "autonomous_execution_changed": False,
            "trade_advice_policy_changed": False,
            "candidate_logic_changed": False,
            "runtime_policy_changed": False,
        },
    }


def write_tomorrow_smoke_v1(*, truth_root: Path, day_utc: str, run_commands: bool = True) -> Path:
    payload = build_tomorrow_smoke_v1(truth_root=truth_root, day_utc=day_utc, run_commands=run_commands)
    out_dir = truth_root / "reports" / "aegis_tomorrow_smoke_v1" / day_utc
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "aegis_tomorrow_smoke.v1.json"
    out_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if not payload["ok"]:
        raise SystemExit(f"FAIL: tomorrow smoke failed: {out_path}")
    return out_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_tomorrow_smoke_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--no-run", action="store_true", help="Build report from existing artifacts without running producer commands")
    args = parser.parse_args(argv)
    path = write_tomorrow_smoke_v1(truth_root=Path(args.truth_root).expanduser().resolve(), day_utc=str(args.day_utc), run_commands=not args.no_run)
    print(json.dumps({"ok": True, "path": str(path)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
