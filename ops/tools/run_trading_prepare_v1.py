#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.repo_protection_common_v1 import RUNTIME_DATA_ROOT

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
NY_TZ = ZoneInfo("America/New_York")
DECISION_PATH = (REPO_ROOT / "runtime" / "process_state" / "trading_readiness_decision.json").resolve()


def _utc_now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def _iso_utc(ts: datetime) -> str:
    return ts.isoformat().replace("+00:00", "Z")


def _today_local_day() -> str:
    return datetime.now(NY_TZ).date().isoformat()


def _parse_day(raw: str) -> datetime | None:
    value = str(raw or "").strip()
    if not DATE_RE.match(value):
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=UTC)
    except ValueError:
        return None


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp_path.replace(path)


def _run_step(name: str, cmd: list[str], ok_return_codes: set[int]) -> dict[str, Any]:
    if len(cmd) > 1 and cmd[1].endswith(".py"):
        script_path = (REPO_ROOT / cmd[1]).resolve()
        if not script_path.exists() or not script_path.is_file():
            return {
                "name": name,
                "cmd": cmd,
                "returncode": 127,
                "ok": False,
                "stdout": "",
                "stderr": f"MISSING_COMMAND_PATH:{script_path}",
            }

    proc = subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    return {
        "name": name,
        "cmd": cmd,
        "returncode": int(proc.returncode),
        "ok": int(proc.returncode) in ok_return_codes,
        "stdout": str(proc.stdout or "").strip(),
        "stderr": str(proc.stderr or "").strip(),
    }


def _parse_json_stdout(step: dict[str, Any]) -> dict[str, Any] | None:
    raw = str(step.get("stdout") or "").strip()
    if not raw:
        return None
    try:
        obj = json.loads(raw)
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


def _gate_row(gates: list[dict[str, Any]], key: str) -> dict[str, Any] | None:
    for gate in gates:
        if str(gate.get("gate") or "").strip() == key:
            return gate
    return None


def _gate_status(gates: list[dict[str, Any]], key: str) -> str:
    gate = _gate_row(gates, key)
    if gate is None:
        return "NOT_READY"
    return "READY" if bool(gate.get("passed")) else "NOT_READY"


def _gate_evidence(gates: list[dict[str, Any]], key: str) -> str:
    gate = _gate_row(gates, key)
    if gate is None:
        return ""
    return str(gate.get("evidence_path") or "").strip()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manual paper trading readiness preparation controller")
    parser.add_argument("--day_utc", default="", help="Target trading day UTC (YYYY-MM-DD)")
    parser.add_argument("--day", default="", help="Alias for --day_utc")
    parser.add_argument("--mode", default="PAPER", help="Trading mode. LIVE is always NO_GO.")
    parser.add_argument("--truth_root", default=str((RUNTIME_DATA_ROOT / "truth").resolve()))
    parser.add_argument("--packet_max_age_hours", type=int, default=48)
    parser.add_argument("--capital_max_age_days", type=int, default=2)
    parser.add_argument("--json", action="store_true", help="Print decision payload as JSON.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    blockers: list[dict[str, str]] = []
    mode = str(args.mode or "").strip().upper()

    day_values = [value for value in [str(args.day_utc or "").strip(), str(args.day or "").strip()] if value]
    if len(day_values) > 1 and day_values[0] != day_values[1]:
        blockers.append(
            {
                "code": "TRADING_DAY_ARGUMENT_CONFLICT",
                "detail": f"--day_utc={day_values[0]} --day={day_values[1]}",
            }
        )
        trading_day = day_values[0]
    elif day_values:
        trading_day = day_values[0]
    else:
        # No stronger repo-wide day selector exists; default to local market day.
        trading_day = _today_local_day()

    day_dt = _parse_day(trading_day)
    if day_dt is None:
        blockers.append({"code": "INVALID_TRADING_DAY", "detail": f"trading_day={trading_day!r}"})
        day_dt = _utc_now().replace(hour=0, minute=0, second=0)

    if mode != "PAPER":
        blockers.append({"code": "MODE_NOT_PAPER", "detail": f"mode={mode}"})

    truth_root = Path(str(args.truth_root or "").strip() or str((RUNTIME_DATA_ROOT / "truth").resolve())).resolve()

    producer_steps = [
        (
            "submit_boundary_status",
            [
                "python3",
                "ops/tools/run_submit_boundary_status_v1.py",
                "--day_utc",
                trading_day,
                "--truth_root",
                str(truth_root),
            ],
            {0, 2},
        ),
        (
            "aegis_day_closure_authority",
            [
                "python3",
                "ops/tools/run_aegis_day_closure_authority_v1.py",
                "--day_utc",
                trading_day,
                "--truth_root",
                str(truth_root),
            ],
            {0, 2},
        ),
        (
            "kill_switch",
            ["python3", "ops/tools/run_global_kill_switch_v1.py", "--day_utc", trading_day],
            {0, 2},
        ),
        (
            "capital_snapshot",
            ["python3", "ops/tools/run_capital_domain_v1.py", "overview"],
            {0},
        ),
        (
            "chatgpt_packet",
            ["python3", "ops/tools/aegis_chatgpt_packet.py"],
            {0},
        ),
    ]

    producer_results: list[dict[str, Any]] = []
    for name, cmd, ok_codes in producer_steps:
        result = _run_step(name, cmd, ok_codes)
        producer_results.append(result)
        if not bool(result.get("ok")):
            blockers.append(
                {
                    "code": f"PREPARE_PRODUCER_FAILED_{name.upper()}",
                    "detail": f"rc={result.get('returncode')} stderr={result.get('stderr')}",
                }
            )

    preflight_cmd = [
        "python3",
        "ops/tools/run_trading_preflight_v1.py",
        "--day_utc",
        trading_day,
        "--mode",
        mode,
        "--capital_max_age_days",
        str(int(args.capital_max_age_days)),
        "--packet_max_age_hours",
        str(int(args.packet_max_age_hours)),
        "--json",
        "--skip-decision-gate",
        "--allow-missing-decision",
    ]
    preflight_result = _run_step("trading_preflight", preflight_cmd, {0, 2})
    preflight_payload = _parse_json_stdout(preflight_result) or {}
    if not bool(preflight_result.get("ok")):
        blockers.append(
            {
                "code": "PREPARE_PREFLIGHT_EXECUTION_FAILED",
                "detail": f"rc={preflight_result.get('returncode')} stderr={preflight_result.get('stderr')}",
            }
        )
    elif not preflight_payload:
        blockers.append(
            {
                "code": "PREPARE_PREFLIGHT_OUTPUT_INVALID",
                "detail": "preflight stdout did not parse as JSON",
            }
        )

    preflight_gates = preflight_payload.get("gates")
    if not isinstance(preflight_gates, list):
        preflight_gates = []
    capital_snapshot_gate = _gate_row(preflight_gates, "capital_snapshot_fresh")
    capital_snapshot_ready = bool(capital_snapshot_gate and bool(capital_snapshot_gate.get("passed")))
    preflight_blockers = preflight_payload.get("blocking_reason_codes")
    if isinstance(preflight_blockers, list):
        for code in preflight_blockers:
            token = str(code or "").strip()
            if token:
                blockers.append({"code": token, "detail": "reported by trading:preflight"})

    submit_boundary_path = (
        truth_root / "reports" / "submit_boundary_status_v1" / trading_day / "submit_boundary_status.v1.json"
    ).resolve()
    closure_path = (
        truth_root / "reports" / "aegis_day_closure_authority_v1" / trading_day / "aegis_day_closure_authority.v1.json"
    ).resolve()
    kill_switch_path = (truth_root / "risk_v1" / "kill_switch_v1" / trading_day / "global_kill_switch_state.v1.json").resolve()
    packet_path = (RUNTIME_DATA_ROOT / "exports" / "aegis_state" / "latest" / "chatgpt_aegis_packet.md").resolve()

    submit_boundary_obj = _read_json(submit_boundary_path) or {}
    closure_obj = _read_json(closure_path) or {}
    kill_switch_obj = _read_json(kill_switch_path) or {}

    runtime = {
        "app_status": _gate_status(preflight_gates, "app_runtime_status_ready"),
        "health_status": _gate_status(preflight_gates, "health_endpoint_ready"),
        "capital_api_status": _gate_status(preflight_gates, "capital_api_http_200"),
    }

    decision_status = str(preflight_payload.get("status") or "").strip().upper()
    go = decision_status == "GO" and not blockers and mode == "PAPER"
    decision = "GO" if go else "NO_GO"

    # Conservative expiry policy: decision expires at local market-day boundary (NY midnight).
    day_for_expiry = trading_day if DATE_RE.match(trading_day) else day_dt.astimezone(NY_TZ).date().isoformat()
    local_day_start = datetime.strptime(day_for_expiry, "%Y-%m-%d").replace(tzinfo=NY_TZ)
    expires_at_utc = (local_day_start + timedelta(days=1)).astimezone(UTC).replace(microsecond=0)
    decision_payload: dict[str, Any] = {
        "schema": "trading_readiness_decision.v1",
        "decision": decision,
        "mode": "PAPER" if mode == "PAPER" else mode,
        "trading_day": trading_day,
        "generated_at_utc": _iso_utc(_utc_now()),
        "expires_at_utc": _iso_utc(expires_at_utc),
        "controller": "ops/tools/run_trading_prepare_v1.py",
        "runtime": runtime,
        "gates": {
            "submit_boundary_status": {
                "status": str(
                    submit_boundary_obj.get("boundary_status") or submit_boundary_obj.get("status") or "UNKNOWN"
                ).strip(),
                "evidence": str(submit_boundary_path),
            },
            "aegis_day_closure_authority": {
                "status": str(closure_obj.get("status") or "UNKNOWN").strip(),
                "evidence": str(closure_path),
            },
            "kill_switch": {
                "status": str(kill_switch_obj.get("state") or "UNKNOWN").strip(),
                "allow_entries": bool(kill_switch_obj.get("allow_entries") is True),
                "evidence": str(kill_switch_path),
            },
            "capital_snapshot": {
                "status": "READY" if capital_snapshot_ready else "NOT_READY",
                "fresh": bool(capital_snapshot_ready),
                "evidence": _gate_evidence(preflight_gates, "capital_snapshot_fresh")
                or "http://127.0.0.1:8787/api/capital/overview",
            },
            "packet_freshness": {
                "status": _gate_status(preflight_gates, "packet_freshness_valid"),
                "evidence": str(packet_path),
            },
            "paper_mode": {
                "status": "PASS" if mode == "PAPER" else "FAIL",
                "mode": mode,
            },
        },
        "blockers": blockers,
    }
    _write_json_atomic(DECISION_PATH, decision_payload)

    print(f"Trading readiness: {decision}")
    print(f"Trading day: {trading_day}")
    print(f"Mode: {decision_payload.get('mode')}")
    print(f"Expires: {decision_payload.get('expires_at_utc')}")
    print("Blockers:")
    if blockers:
        for blocker in blockers:
            print(f"- {blocker.get('code')}: {blocker.get('detail')}")
    else:
        print("- none")

    if args.json:
        print(json.dumps(decision_payload, indent=2, sort_keys=True))
    return 0 if go else 2


if __name__ == "__main__":
    raise SystemExit(main())
