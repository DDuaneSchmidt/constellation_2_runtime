#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_day_lifecycle_v1 import write_lifecycle_transition_v1
from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1
from constellation_2.common.runtime_contract_v1 import resolve_runtime_data_root
from ops.tools import run_aegis_bod_prepare_v1 as bod


def _manifest_path(ctx: bod.BodContext) -> Path:
    return (
        ctx.truth_root
        / "reports"
        / "aegis_day_prepare_v1"
        / ctx.day_utc
        / "aegis_day_prepare.v1.json"
    ).resolve()


def _fallback_manifest_path(day_utc: str) -> Path:
    return (
        resolve_runtime_data_root().resolve()
        / "truth"
        / "reports"
        / "aegis_day_prepare_v1"
        / day_utc
        / "aegis_day_prepare.v1.json"
    ).resolve()


def _row_status(row: dict[str, Any]) -> str:
    if row.get("status") == "PASS":
        return "PASS"
    if row.get("exit_code") == 0:
        return "PASS"
    if row.get("status") == "TIMEOUT":
        return "TIMEOUT"
    return "BLOCKED"


def _canonical_blocker(steps: list[dict[str, Any]]) -> str:
    for row in steps:
        if _row_status(row) in {"BLOCKED", "TIMEOUT"}:
            blocker = str(row.get("blocker") or "").strip()
            if blocker:
                return blocker
            return f"{str(row.get('step_name') or 'STEP').upper()}_BLOCKED"
    return ""


def run_prepare_for_trading_day_v1(ctx: bod.BodContext) -> dict[str, Any]:
    py = sys.executable
    env = dict(os.environ)
    env.setdefault("C2_GOVERNED_SUBMIT_DRY_RUN", "YES")
    write_lifecycle_transition_v1(
        truth_root=ctx.truth_root,
        day_utc=ctx.day_utc,
        state="BOD_PENDING",
        producer="ops/tools/run_aegis_prepare_for_trading_day_v1.py",
        reason="Starting daily paper-trading preparation",
    )
    commands: list[tuple[str, list[str]]] = [
        ("bod_prepare", [py, "ops/tools/run_aegis_bod_prepare_v1.py", "--day_utc", ctx.day_utc, "--environment", ctx.environment]),
        ("pre_open_verify", [py, "ops/tools/run_aegis_pre_open_verify_v1.py", "--day_utc", ctx.day_utc, "--environment", ctx.environment]),
        ("market_and_strategy_prepare", [py, "ops/tools/run_aegis_market_and_strategy_prepare_v1.py", "--day_utc", ctx.day_utc, "--environment", ctx.environment]),
        ("paper_ready", [py, "ops/tools/run_aegis_paper_ready_v1.py", "--day_utc", ctx.day_utc, "--environment", ctx.environment]),
        ("chatgpt_packet", [py, "ops/tools/aegis_chatgpt_packet.py"]),
    ]
    steps = [bod._run_child(name, cmd, env=env) for name, cmd in commands]
    blocker = _canonical_blocker(steps)
    final_ready_step = next((row for row in steps if row.get("step_name") == "paper_ready"), {})
    final_status = "PAPER_READY" if _row_status(final_ready_step) == "PASS" and not blocker else "NOT_READY"
    return {
        "schema_id": "aegis_day_prepare",
        "schema_version": "v1",
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "status": final_status,
        "canonical_blocker": "" if final_status == "PAPER_READY" else blocker,
        "steps": steps,
        "truth_root": str(ctx.truth_root),
        "execution_root": str(ctx.execution_root),
        "operator_input_root": str(ctx.operator_input_root),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_prepare_for_trading_day_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default="PAPER", choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)

    day_utc = parse_day_utc_v1(args.day_utc)
    environment = str(args.environment).strip().upper()
    try:
        ctx = bod._resolve_context(day_utc, environment, args.truth_root)
    except Exception as exc:  # noqa: BLE001
        path = _fallback_manifest_path(day_utc)
        payload: dict[str, Any] = {
            "schema_id": "aegis_day_prepare",
            "schema_version": "v1",
            "day_utc": day_utc,
            "environment": environment,
            "status": "NOT_READY",
            "canonical_blocker": "TRUTH_ROOT_RUNTIME_CONTRACT_BLOCKED",
            "error": f"{type(exc).__name__}:{exc}",
            "path": str(path),
        }
        bod._write_json(path, payload)
        print(json.dumps({"status": "NOT_READY", "canonical_blocker": "TRUTH_ROOT_RUNTIME_CONTRACT_BLOCKED", "manifest_path": str(path)}, sort_keys=True))
        return 2

    payload = run_prepare_for_trading_day_v1(ctx)
    path = _manifest_path(ctx)
    payload["manifest_path"] = str(path)
    bod._write_json(path, payload)
    print(json.dumps({"status": payload["status"], "canonical_blocker": payload["canonical_blocker"], "manifest_path": str(path)}, sort_keys=True))
    return 0 if payload["status"] == "PAPER_READY" else 2


if __name__ == "__main__":
    raise SystemExit(main())
