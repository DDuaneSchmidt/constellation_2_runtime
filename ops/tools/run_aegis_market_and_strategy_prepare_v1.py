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

from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1
from ops.tools import run_aegis_bod_prepare_v1 as bod


def _manifest_path(ctx: bod.BodContext) -> Path:
    return (
        ctx.truth_root
        / "reports"
        / "aegis_market_and_strategy_prepare_v1"
        / ctx.day_utc
        / "aegis_market_and_strategy_prepare.v1.json"
    ).resolve()


def _classification(step_name: str, blocker: str) -> str:
    text = f"{step_name} {blocker}".upper()
    if any(token in text for token in ("OPTIONS", "MARKET_DATA", "IB", "HANDSHAKE", "SNAPSHOT")):
        return "MARKET_DATA_UNAVAILABLE"
    if "STRATEGY" in text:
        return "STRATEGY_BLOCKED"
    if any(token in text for token in ("PHASEC", "DEFINED_RISK", "RISK")):
        return "RISK_EVIDENCE_MISSING"
    return "SYSTEM_FAILURE"


def run_market_and_strategy_prepare_v1(ctx: bod.BodContext) -> dict[str, Any]:
    py = sys.executable
    env = dict(os.environ)
    env.setdefault("C2_GOVERNED_SUBMIT_DRY_RUN", "YES")
    commands: list[tuple[str, list[str], int]] = [
        (
            "options_chain_snapshot",
            [
                py,
                "ops/tools/run_options_chain_snapshot_required_day_v1.py",
                "--day_utc",
                ctx.day_utc,
                "--truth_root",
                str(ctx.execution_root),
                "--symbols_from_intents",
                "YES",
            ],
            2,
        ),
        (
            "market_data_authority",
            [py, "ops/tools/run_market_data_authority_v1.py", "--day_utc", ctx.day_utc, "--truth_root", str(ctx.truth_root), "--execution_root", str(ctx.execution_root)],
            2,
        ),
        (
            "strategy_decision_authority",
            [py, "ops/tools/run_strategy_decision_authority_v1.py", "--day_utc", ctx.day_utc, "--truth_root", str(ctx.truth_root), "--execution_root", str(ctx.execution_root)],
            1,
        ),
        (
            "defined_risk_phasec_evidence",
            [
                py,
                "ops/tools/run_phasec_identity_materializer_day_v1.py",
                "--day_utc",
                ctx.day_utc,
                "--eval_time_utc",
                f"{ctx.day_utc}T00:00:00Z",
                "--truth_root",
                str(ctx.truth_root),
                "--execution_truth_root",
                str(ctx.execution_root),
            ],
            1,
        ),
        (
            "risk_sizing_authority",
            [py, "ops/tools/run_risk_sizing_authority_v1.py", "--day_utc", ctx.day_utc, "--truth_root", str(ctx.truth_root), "--execution_root", str(ctx.execution_root)],
            1,
        ),
    ]
    steps = [bod._run_child_with_retries(name, cmd, env=env, max_attempts=attempts) for name, cmd, attempts in commands]
    first_blocked = next((row for row in steps if row.get("status") in {"BLOCKED", "TIMEOUT"}), {})
    blocker = str(first_blocked.get("blocker") or "")
    failure_class = _classification(str(first_blocked.get("step_name") or ""), blocker) if first_blocked else ""
    return {
        "schema_id": "aegis_market_and_strategy_prepare",
        "schema_version": "v1",
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "status": "READY" if not first_blocked else "BLOCKED",
        "canonical_blocker": blocker or failure_class,
        "failure_classification": failure_class,
        "steps": steps,
        "truth_root": str(ctx.truth_root),
        "execution_root": str(ctx.execution_root),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_market_and_strategy_prepare_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default="PAPER", choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)

    day_utc = parse_day_utc_v1(args.day_utc)
    ctx = bod._resolve_context(day_utc, str(args.environment).strip().upper(), args.truth_root)
    payload = run_market_and_strategy_prepare_v1(ctx)
    path = _manifest_path(ctx)
    payload["path"] = str(path)
    bod._write_json(path, payload)
    print(json.dumps({"status": payload["status"], "canonical_blocker": payload["canonical_blocker"], "path": str(path)}, sort_keys=True))
    return 0 if payload["status"] == "READY" else 2


if __name__ == "__main__":
    raise SystemExit(main())
