#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

DEFAULT_TRUTH_ROOT = Path("/home/node/constellation_runtime_data/truth")
PYTHON = sys.executable
RUN_MODE = "INTRADAY_OPERATIONAL"
SLEEVE_SYMBOLS = "SPY,QQQ,IWM,DIA,TLT,IEF,LQD,HYG,GLD,DBC,UUP,VIX"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _now_utc() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _safe_run_id(run_id: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_", ".", ":"} else "_" for ch in str(run_id or "").strip())


def _run_id(day: str) -> str:
    stamp = datetime.now(UTC).replace(microsecond=0).strftime("%Y%m%dT%H%M%SZ")
    return f"aegis_intraday_sleeves_now:{day}:{stamp}"


def _json_or_text(text: str) -> Any:
    raw = str(text or "").strip()
    if not raw:
        return {}
    try:
        return json.loads(raw.splitlines()[-1])
    except Exception:
        return {"text": raw[-4000:]}


def _run(cmd: list[str], *, env: dict[str, str]) -> dict[str, Any]:
    started = _now_utc()
    proc = subprocess.run(cmd, cwd=str(_repo_root()), env=env, capture_output=True, text=True)
    ended = _now_utc()
    return {
        "command": cmd,
        "returncode": int(proc.returncode),
        "started_at_utc": started,
        "ended_at_utc": ended,
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
        "payload": _json_or_text(proc.stdout),
    }


def _read(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _candidate_counts(root: Path, day: str, run_id: str) -> dict[str, object]:
    path = root / "reports" / "candidate_generation_manifest_v1" / day / _safe_run_id(run_id) / "candidate_generation_manifest.v1.json"
    if not path.exists():
        path = root / "reports" / "candidate_generation_manifest_v1" / day / f"sleeve_evaluation_kernel_v1:{day}" / "candidate_generation_manifest.v1.json"
    payload = _read(path)
    rows = payload.get("candidate_rows") if isinstance(payload.get("candidate_rows"), list) else []
    by_sleeve: dict[str, dict[str, int]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        sleeve = str(row.get("sleeve_id") or row.get("engine_id") or "UNKNOWN")
        status = str(row.get("status") or "UNKNOWN")
        by_sleeve.setdefault(sleeve, {})[status] = by_sleeve.setdefault(sleeve, {}).get(status, 0) + 1
    selected = next((row for row in rows if isinstance(row, dict) and str(row.get("status") or "") == "CANDIDATE_CREATED"), {})
    return {
        "path": str(path),
        "by_sleeve": by_sleeve,
        "summary": payload.get("summary") if isinstance(payload.get("summary"), dict) else {},
        "selected_candidate": selected,
        "candidate_count": len(rows),
        "provisional_count": sum(1 for row in rows if isinstance(row, dict) and row.get("candidate_data_status") == "PROVISIONAL_CANDIDATE"),
    }



def _selected_symbols(root: Path, day: str) -> list[str]:
    path = root / "reports" / "intent_arbitration_v1" / day / "intent_arbitration.v1.json"
    payload = _read(path)
    selected = payload.get("selected_intent") if isinstance(payload.get("selected_intent"), dict) else {}
    symbol = str(selected.get("symbol") or "").strip().upper()
    return [symbol] if symbol else []


def _symbol_list(symbols: str, extras: list[str]) -> str:
    seen: set[str] = set()
    out: list[str] = []
    for raw in [*str(symbols or "").split(","), *extras]:
        symbol = str(raw or "").strip().upper()
        if symbol and symbol not in seen:
            seen.add(symbol)
            out.append(symbol)
    return ",".join(out)

def _first_path(payload: Any, *keys: str) -> str:
    if not isinstance(payload, dict):
        return ""
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str) and value:
            return value
    paths = payload.get("paths")
    if isinstance(paths, dict):
        for key in keys:
            value = paths.get(key)
            if isinstance(value, str) and value:
                return value
    return ""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_intraday_sleeves_now_v1")
    parser.add_argument("--truth-root", "--truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day-utc", "--day_utc", "--day", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--environment", default="PAPER")
    parser.add_argument("--symbols", default=SLEEVE_SYMBOLS)
    parser.add_argument("--run-id", "--run_id", default="")
    args = parser.parse_args(argv)

    root = Path(args.truth_root).expanduser().resolve()
    day = str(args.day_utc)
    environment = str(args.environment or "PAPER").strip().upper()
    run_id = str(args.run_id or _run_id(day)).strip()
    safe_run_id = _safe_run_id(run_id)
    rollup_path = root / "reports" / "sleeve_evaluation_kernel_v1" / day / safe_run_id / "sleeve_evaluation_rollup.v1.json"
    portfolio_gate_path = root / "reports" / "portfolio_activation_gate_v1" / day / "portfolio_activation_gate.v1.json"
    portfolio_scoring_path = root / "reports" / "portfolio_scoring_v1" / day / "portfolio_scoring.v1.json"

    env = dict(os.environ)
    env["AEGIS_MARKET_DATA_MODE"] = RUN_MODE
    env["AEGIS_LITE_MANUAL_ONLY"] = "1"
    env.setdefault("AEGIS_MARKET_DATA_INTRADAY_PROVIDER", "YAHOO_CHART")
    env.setdefault("AEGIS_MARKET_DATA_PROVIDER_PRIMARY", "LOCAL_CACHE")
    env.setdefault("AEGIS_MARKET_DATA_PROVIDER_FALLBACK", "STOOQ")
    env.setdefault("AEGIS_MARKET_DATA_TOTAL_TIMEOUT_SECONDS", "180")
    env.setdefault("AEGIS_MARKET_DATA_PER_SYMBOL_TIMEOUT_SECONDS", "12")
    env.setdefault("AEGIS_MARKET_DATA_TIMEOUT_SECONDS", "12")

    pre_arbitration_commands = [
        [PYTHON, "ops/tools/refresh_aegis_market_data_v1.py", "--truth_root", str(root), "--day", day, "--market-data-mode", RUN_MODE, "--symbols", str(args.symbols)],
        [PYTHON, "ops/tools/build_aegis_market_data_inputs_v1.py", "--truth-root", str(root), "--day-utc", day, "--market-data-mode", RUN_MODE, "--emit-events"],
        [PYTHON, "ops/tools/build_aegis_data_registry_v1.py", "--truth_root", str(root), "--day", day],
        [PYTHON, "ops/tools/build_canonical_symbol_universe_authority_report_v1.py", "--truth-root", str(root), "--day-utc", day, "--market-data-mode", RUN_MODE],
        [PYTHON, "ops/tools/build_aegis_sleeve_readiness_v1.py", "--truth_root", str(root), "--day", day],
        [PYTHON, "ops/tools/run_sleeve_evaluation_kernel_v1.py", "--day_utc", day, "--truth_root", str(root), "--environment", environment, "--run-mode", RUN_MODE, "--run-id", run_id],
        [PYTHON, "ops/tools/run_portfolio_activation_gate_v1.py", "--day_utc", day, "--truth_root", str(root), "--environment", environment, "--source_rollup_path", str(rollup_path), "--run-mode", RUN_MODE, "--run-id", run_id],
        [PYTHON, "ops/tools/run_portfolio_scoring_v1.py", "--day_utc", day, "--truth_root", str(root), "--environment", environment, "--source_rollup_path", str(rollup_path), "--portfolio_gate_path", str(portfolio_gate_path)],
        [PYTHON, "ops/tools/run_intent_arbitration_v1.py", "--day_utc", day, "--truth_root", str(root), "--environment", environment, "--source-rollup-path", str(rollup_path), "--portfolio-gate-path", str(portfolio_gate_path), "--portfolio-scoring-path", str(portfolio_scoring_path)],
    ]

    results: list[dict[str, Any]] = []
    for command in pre_arbitration_commands:
        result = _run(command, env=env)
        results.append(result)
        if int(result["returncode"]) not in {0, 2}:
            break

    if results and all(int(row["returncode"]) in {0, 2} for row in results):
        selected_symbols = _selected_symbols(root, day)
        combined_symbols = _symbol_list(str(args.symbols), selected_symbols)
        if combined_symbols != _symbol_list(str(args.symbols), []):
            for command in (
                [PYTHON, "ops/tools/refresh_aegis_market_data_v1.py", "--truth_root", str(root), "--day", day, "--market-data-mode", RUN_MODE, "--symbols", combined_symbols],
                [PYTHON, "ops/tools/build_aegis_market_data_inputs_v1.py", "--truth-root", str(root), "--day-utc", day, "--market-data-mode", RUN_MODE, "--emit-events"],
                [PYTHON, "ops/tools/build_aegis_data_registry_v1.py", "--truth_root", str(root), "--day", day],
            ):
                result = _run(command, env=env)
                results.append(result)
                if int(result["returncode"]) not in {0, 2}:
                    break

    post_arbitration_commands = [
        [PYTHON, "ops/tools/promote_aegis_selected_intent_v1.py", "--truth-root", str(root), "--day", day],
        [PYTHON, "ops/tools/build_candidate_promotion_map_v1.py", "--truth-root", str(root), "--day", day],
        [PYTHON, "ops/tools/build_operator_state_snapshot_v1.py", "--truth-root", str(root), "--day", day],
        [PYTHON, "ops/tools/run_aegis_runtime_truth_kernel_v1.py", "--truth_root", str(root), "--day", day, "--json"],
        [PYTHON, "ops/tools/run_aegis_projection_refresh_v1.py", "--truth-root", str(root), "--target-day", day, "--environment", environment],
        [PYTHON, "ops/tools/build_aegis_audit_bundle_v1.py", "--truth-root", str(root), "--day-utc", day, "--run-id", run_id],
    ]
    if results and all(int(row["returncode"]) in {0, 2} for row in results):
        for command in post_arbitration_commands:
            result = _run(command, env=env)
            results.append(result)
            if int(result["returncode"]) not in {0, 2}:
                break

    audit_path = _first_path(results[-1].get("payload") if results else {}, "bundle_path", "audit_bundle_path", "manifest_path", "artifact_path", "json")
    replay_result: dict[str, Any] = {}
    if audit_path and Path(audit_path).exists():
        replay_result = _run([PYTHON, "ops/tools/replay_aegis_runtime_v1.py", "--audit-bundle", audit_path], env=env)
        results.append(replay_result)

    refresh_payload = results[0].get("payload") if results and isinstance(results[0].get("payload"), dict) else {}
    universe_authority_payload = results[3].get("payload") if len(results) > 3 and isinstance(results[3].get("payload"), dict) else {}
    readiness_payload = results[4].get("payload") if len(results) > 4 and isinstance(results[4].get("payload"), dict) else {}
    counts = _candidate_counts(root, day, run_id)
    all_ok = all(int(row["returncode"]) in {0, 2} for row in results)
    out = {
        "schema_id": "aegis_intraday_sleeves_now_run",
        "schema_version": "v1",
        "day_utc": day,
        "environment": environment,
        "run_id": run_id,
        "run_mode": RUN_MODE,
        "market_data_mode": RUN_MODE,
        "underlying_command": " ".join([PYTHON, "ops/tools/run_aegis_intraday_sleeves_now_v1.py", "--truth-root", str(root), "--day-utc", day, "--environment", environment, "--run-id", run_id]),
        "status": "PASS" if all_ok else "FAIL",
        "market_data_status": refresh_payload.get("status") if isinstance(refresh_payload, dict) else "UNKNOWN",
        "operator_market_data_state": refresh_payload.get("operator_market_data_state") if isinstance(refresh_payload, dict) else "UNKNOWN",
        "market_data_usable_for_candidate_generation": refresh_payload.get("usable_for_candidate_generation") if isinstance(refresh_payload, dict) else False,
        "final_eod_certification_status": refresh_payload.get("final_eod_certification_status") if isinstance(refresh_payload, dict) else "PENDING",
        "canonical_symbol_universe_authority_report": universe_authority_payload,
        "sleeve_readiness": readiness_payload,
        "candidate_counts": counts,
        "audit_bundle_path": audit_path,
        "replay_result": replay_result.get("payload") if isinstance(replay_result, dict) else {},
        "commands": results,
        "broker_submit_enabled": False,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }
    print(json.dumps(out, sort_keys=True))
    return 0 if out["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
