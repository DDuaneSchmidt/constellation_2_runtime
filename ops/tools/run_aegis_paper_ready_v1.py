#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_day_lifecycle_v1 import read_lifecycle_state_v1, write_lifecycle_transition_v1
from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1
from constellation_2.common.stale_artifact_guard_v1 import (
    MISSING_EVIDENCE,
    STALE_ARTIFACT,
    classify_artifact_freshness_v1,
)
from ops.tools import run_aegis_bod_prepare_v1 as bod


def _manifest_path(ctx: bod.BodContext) -> Path:
    return (
        ctx.truth_root
        / "reports"
        / "aegis_paper_ready_v1"
        / ctx.day_utc
        / "aegis_paper_ready.v1.json"
    ).resolve()


def _state(payload: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = str(payload.get(key) or "").strip().upper()
        if value:
            return value
    return "UNKNOWN"


def _status(payload: dict[str, Any]) -> str:
    return str(payload.get("status") or "").strip().upper()


def evaluate_paper_ready_v1(ctx: bod.BodContext) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []

    def add_check(name: str, path: Path, ok: bool, blocker: str, observed: str = "") -> None:
        checks.append(
            {
                "name": name,
                "status": "PASS" if ok else "BLOCKED",
                "path": str(path),
                "observed_state": observed,
                "blocker": "" if ok else blocker,
            }
        )

    lifecycle = read_lifecycle_state_v1(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    lifecycle_state = str(lifecycle.get("state") or "").strip().upper()
    lifecycle_path = ctx.truth_root / "reports" / "aegis_day_lifecycle_v1" / ctx.day_utc / "aegis_day_lifecycle.v1.json"
    pre_open_verify_path = ctx.truth_root / "reports" / "aegis_pre_open_verify_v1" / ctx.day_utc / "aegis_pre_open_verify.v1.json"
    lifecycle_freshness = classify_artifact_freshness_v1(
        artifact_path=lifecycle_path,
        day_utc=ctx.day_utc,
        dependency_paths=[pre_open_verify_path],
        payload=lifecycle,
    )
    if lifecycle_freshness["status"] == "MISSING":
        add_check("pre_open_lifecycle", lifecycle_path, False, MISSING_EVIDENCE, lifecycle_state)
        checks[-1]["artifact_freshness"] = lifecycle_freshness
    elif lifecycle_freshness["status"] == "STALE":
        add_check("pre_open_lifecycle", lifecycle_path, False, STALE_ARTIFACT, lifecycle_state)
        checks[-1]["artifact_freshness"] = lifecycle_freshness
    else:
        add_check("pre_open_lifecycle", lifecycle_path, lifecycle_state in {"PRE_OPEN_READY", "PAPER_READY"}, "PRE_OPEN_NOT_READY", lifecycle_state)
        checks[-1]["artifact_freshness"] = lifecycle_freshness

    market_path = ctx.truth_root / "reports" / "market_data_authority_v1" / ctx.day_utc / "market_data_authority.v1.json"
    market = bod._read_json(market_path)
    market_state = _state(market, "market_data_state", "status")
    add_check("market_data", market_path, market_state in {"READY", "VALID", "PASS", "NO_REQUIRED_DATA", "NOT_REQUIRED"}, "MARKET_DATA_UNAVAILABLE", market_state)

    strategy_path = ctx.truth_root / "reports" / "strategy_decision_authority_v1" / ctx.day_utc / "strategy_decision_authority.v1.json"
    strategy = bod._read_json(strategy_path)
    strategy_state = _state(strategy, "strategy_decision_state", "status")
    strategy_ok = strategy_state in {"READY", "VALID", "PASS", "NO_INTENTS", "NO_INTENT"} or (
        strategy_state == "INTENT_CREATED" and _status(strategy) == "PASS"
    )
    add_check("strategy_decision", strategy_path, strategy_ok, "STRATEGY_BLOCKED", strategy_state)

    risk_path = ctx.truth_root / "reports" / "risk_sizing_authority_v1" / ctx.day_utc / "risk_sizing_authority.v1.json"
    risk = bod._read_json(risk_path)
    risk_state = _state(risk, "risk_sizing_state", "status")
    risk_blocker = str(risk.get("canonical_blocker") or risk.get("first_blocker") or "").strip() or "RISK_EVIDENCE_MISSING"
    risk_ok = risk_state in {"READY", "VALID", "PASS", "NO_RISK_REQUIRED"} or (
        risk_state == "SIZED" and _status(risk) == "PASS"
    )
    add_check("risk_sizing", risk_path, risk_ok, risk_blocker, risk_state)

    submit_path = ctx.truth_root / "reports" / "submit_boundary_status_v1" / ctx.day_utc / "submit_boundary_status.v1.json"
    submit = bod._read_json(submit_path)
    submit_state = _state(submit, "boundary_status", "status")
    submit_ok = submit_state in {"AUTHORIZED", "PASS"} and submit.get("submission_authorized") is True
    submit_blocker = str(submit.get("first_blocker_code") or submit.get("canonical_blocker") or "SUBMIT_BOUNDARY_BLOCKED").strip()
    add_check("submit_boundary", submit_path, submit_ok, submit_blocker, submit_state)

    execution_path = ctx.truth_root / "reports" / "execution_mode_authority_v1" / ctx.day_utc / "execution_mode_authority.v1.json"
    execution = bod._read_json(execution_path)
    execution_state = _state(execution, "mode_state", "mode")
    execution_ok = execution_state in {"DRY_RUN_LOCKED", "PAPER_READY_NOT_TRANSMITTED", "PAPER_TRANSMIT_ENABLED"}
    add_check("execution_mode", execution_path, execution_ok, "EXECUTION_MODE_UNKNOWN", execution_state)

    first_blocked = next((row for row in checks if row["status"] == "BLOCKED"), {})
    status = "PAPER_READY" if not first_blocked else "NOT_READY"
    return {
        "schema_id": "aegis_paper_ready",
        "schema_version": "v1",
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "status": status,
        "canonical_blocker": str(first_blocked.get("blocker") or ""),
        "checks": checks,
        "truth_root": str(ctx.truth_root),
        "execution_root": str(ctx.execution_root),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_paper_ready_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default="PAPER", choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)

    day_utc = parse_day_utc_v1(args.day_utc)
    ctx = bod._resolve_context(day_utc, str(args.environment).strip().upper(), args.truth_root)
    payload = evaluate_paper_ready_v1(ctx)
    path = _manifest_path(ctx)
    payload["path"] = str(path)
    pre_open_check = next((row for row in payload["checks"] if row.get("name") == "pre_open_lifecycle"), {})
    lifecycle_path = ctx.truth_root / "reports" / "aegis_day_lifecycle_v1" / ctx.day_utc / "aegis_day_lifecycle.v1.json"
    if payload["status"] == "PAPER_READY" or pre_open_check.get("status") == "BLOCKED":
        lifecycle_path = write_lifecycle_transition_v1(
            truth_root=ctx.truth_root,
            day_utc=ctx.day_utc,
            state="PAPER_READY" if payload["status"] == "PAPER_READY" else "PRE_OPEN_BLOCKED",
            producer="ops/tools/run_aegis_paper_ready_v1.py",
            blocker=str(payload.get("canonical_blocker") or ""),
            reason="Paper readiness gate complete",
            evidence_paths=[str(path)],
        )
    payload["lifecycle_path"] = str(lifecycle_path)
    bod._write_json(path, payload)
    print(json.dumps({"status": payload["status"], "canonical_blocker": payload["canonical_blocker"], "path": str(path)}, sort_keys=True))
    return 0 if payload["status"] == "PAPER_READY" else 2


if __name__ == "__main__":
    raise SystemExit(main())
