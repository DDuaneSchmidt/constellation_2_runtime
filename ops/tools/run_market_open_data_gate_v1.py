#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1
from ops.tools import run_aegis_bod_prepare_v1 as bod
from ops.tools.run_market_data_supply_v1 import market_data_supply_path

SCHEMA_VERSION = "market_open_data_gate.v1"


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n", encoding="utf-8")


def market_open_data_gate_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "reports" / "market_open_data_gate_v1" / day_utc / "market_open_data_gate.v1.json").resolve()


def _market_session_state(now_utc: datetime | None = None) -> str:
    now = now_utc or datetime.now(UTC)
    local = now.astimezone(ZoneInfo("America/New_York"))
    if local.weekday() >= 5:
        return "NON_TRADING_DAY"
    minutes = local.hour * 60 + local.minute
    if minutes < (9 * 60 + 30):
        return "PRE_MARKET"
    if minutes < (16 * 60):
        return "REGULAR"
    return "AFTER_HOURS"


def _run_market_data_supply(ctx: bod.BodContext) -> dict[str, Any]:
    cmd = [
        sys.executable,
        "ops/tools/run_market_data_supply_v1.py",
        "--day_utc",
        ctx.day_utc,
        "--environment",
        ctx.environment,
    ]
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, check=False, timeout=300)
    return {
        "command": " ".join(cmd),
        "exit_code": int(proc.returncode),
        "stdout_summary": str(proc.stdout or "").strip()[-1200:],
        "stderr_summary": str(proc.stderr or "").strip()[-1200:],
    }


def build_market_open_data_gate(ctx: bod.BodContext) -> dict[str, Any]:
    generated_at = _now_iso()
    session_state = _market_session_state()
    supply_path = market_data_supply_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    command_result: dict[str, Any] = {}
    if session_state != "REGULAR":
        status = "PENDING"
        blocker = "MARKET_NOT_OPEN"
        action = "Rerun market-open data gate after 09:30 ET during regular US options market hours."
    else:
        command_result = _run_market_data_supply(ctx)
        supply = _read_json(supply_path)
        supply_status = str(supply.get("status") or "").strip().upper()
        blocker = str(supply.get("canonical_blocker") or "").strip()
        status = "PASS" if supply_status == "PASS" and not blocker else "BLOCKED"
        if status == "BLOCKED" and blocker in {"OPTIONS_QUOTES_MISSING", "OPTIONS_QUOTES_UNAVAILABLE_OUTSIDE_MARKET_HOURS"}:
            blocker = "OPTIONS_QUOTES_MISSING_BID_ASK"
        if status == "BLOCKED" and not blocker:
            blocker = "OPTIONS_SNAPSHOT_CAPTURE_FAILED"
        action = "" if status == "PASS" else "Capture current SPY option bid/ask quotes and freshness certificate during regular market hours, then rerun this gate."
    return {
        "schema_id": "market_open_data_gate",
        "schema_version": SCHEMA_VERSION,
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "generated_at_utc": generated_at,
        "market_session_state": session_state,
        "status": status,
        "canonical_blocker": blocker,
        "market_data_supply_path": str(supply_path),
        "command_result": command_result,
        "operator_next_action": action,
    }


def run_market_open_data_gate_v1(day_utc: str, environment: str, truth_root: str = "") -> tuple[Path, dict[str, Any]]:
    ctx = bod._resolve_context(day_utc, environment, truth_root)
    payload = build_market_open_data_gate(ctx)
    path = market_open_data_gate_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    previous = _read_json(path)
    if previous and str(previous.get("day_utc") or "") != ctx.day_utc:
        raise SystemExit(f"FAIL: WRONG_DAY_MARKET_OPEN_DATA_GATE_COLLISION: {path}")
    _write_json(path, payload)
    return path, payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_market_open_data_gate_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default="PAPER", choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)
    day_utc = parse_day_utc_v1(args.day_utc)
    environment = str(args.environment or "PAPER").strip().upper()
    path, payload = run_market_open_data_gate_v1(day_utc, environment, str(args.truth_root or ""))
    print(json.dumps({"status": payload["status"], "canonical_blocker": payload["canonical_blocker"], "market_open_data_gate_path": str(path)}, sort_keys=True))
    return 0 if payload.get("status") in {"PASS", "PENDING"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
